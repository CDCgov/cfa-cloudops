import datetime
import io
import json
import logging
from dataclasses import dataclass
from enum import Enum

import pandas as pd
from azure.batch.models import BatchMetadataItem

from cfa.cloudops import batch_helpers

from .blob import create_storage_container_if_not_exists

logger = logging.getLogger(__name__)


class NodeMonitoringMode(str, Enum):
    """Monitoring modes available for Azure Batch compute nodes."""

    MONITOR = "monitor"
    BENCHMARK = "benchmark"
    BOTH = "both"


class TaskMonitoringMode(str, Enum):
    """Monitoring modes available for an individual Azure Batch task."""

    NONE = "none"
    TIME = "time"
    SUMMARY = "summary"
    TIMESERIES = "timeseries"
    FULL = "full"


@dataclass
class TaskMonitoringConfig:
    """Configuration for task-level resource monitoring."""

    mode: TaskMonitoringMode | str = TaskMonitoringMode.SUMMARY
    interval_seconds: int = 5
    output_mount: str = "output"
    output_folder: str = "default"
    script_url: str | None = None


def normalize_node_monitoring_mode(
    mode: NodeMonitoringMode | str | None,
) -> NodeMonitoringMode | None:
    if mode is None:
        return None
    if isinstance(mode, NodeMonitoringMode):
        return mode
    try:
        return NodeMonitoringMode(mode)
    except ValueError:
        valid = ", ".join(m.value for m in NodeMonitoringMode)
        raise ValueError(
            f"Invalid node monitoring mode '{mode}'. Expected one of: {valid}."
        ) from None


def normalize_task_monitoring_mode(
    mode: TaskMonitoringMode | str,
) -> TaskMonitoringMode:
    if isinstance(mode, TaskMonitoringMode):
        return mode
    try:
        return TaskMonitoringMode(mode)
    except ValueError:
        valid = ", ".join(m.value for m in TaskMonitoringMode)
        raise ValueError(
            f"Invalid task monitoring mode '{mode}'. Expected one of: {valid}."
        ) from None


def build_metadata_items(
    code_version: str | None = None,
    model_version: str | None = None,
    workload_metadata: dict | None = None,
    extra_metadata: dict[str, str] | None = None,
) -> list[BatchMetadataItem]:
    """Build Azure Batch metadata items used by jobs and tasks."""
    values: dict[str, str] = {}

    if code_version:
        values["code_version"] = code_version
    if model_version:
        values["model_version"] = model_version
    if workload_metadata:
        values["workload_metadata"] = json.dumps(
            workload_metadata,
            separators=(",", ":"),
            default=str,
        )
    if extra_metadata:
        values.update({k: str(v) for k, v in extra_metadata.items()})

    return [BatchMetadataItem(name=name, value=value) for name, value in values.items()]


class CloudMetrics:
    def __init__(
        self,
        batch_service_client: object,
        blob_service_client: object,
        credentials: object,
    ):
        self.batch_service_client = batch_service_client
        self.blob_service_client = blob_service_client
        self.cred = credentials

    @staticmethod
    def _calculate_cpu_efficiency_pct(
        average_cores_used: float | None,
        workload_metadata: dict,
    ) -> float | None:
        if average_cores_used is None:
            return None

        # Accept several useful names without imposing model-specific terminology.
        for key in (
            "allocated_cores",
            "available_cores",
            "cores",
            "configured_threads",
        ):
            value = workload_metadata.get(key)
            try:
                capacity = float(value)
            except (TypeError, ValueError):
                continue
            if capacity > 0:
                return round((average_cores_used / capacity) * 100.0, 2)
        return None

    @staticmethod
    def _metadata_to_dict(metadata) -> dict[str, str]:
        if not metadata:
            return {}
        return {
            getattr(item, "name", ""): getattr(item, "value", "")
            for item in metadata
            if getattr(item, "name", None)
        }

    @staticmethod
    def _decode_workload_metadata(metadata: dict[str, str]) -> dict:
        raw = metadata.get("workload_metadata")
        if not raw:
            return {}
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {"value": value}
        except (TypeError, json.JSONDecodeError):
            return {"raw": raw}

    @staticmethod
    def _calculate_average_cores(
        cpu_usage_usec: int | float | None,
        runtime_sec: int | float | None,
    ) -> float | None:
        if cpu_usage_usec is None or runtime_sec is None:
            return None
        try:
            runtime = float(runtime_sec)
            if runtime <= 0:
                return None
            return round((float(cpu_usage_usec) / 1_000_000.0) / runtime, 4)
        except (TypeError, ValueError):
            return None

    def _download_json_blob(
        self,
        container_name: str,
        blob_name: str,
    ) -> dict | None:
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name,
        )
        try:
            content = blob_client.download_blob().readall()
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            return json.loads(content)
        except Exception:
            logger.warning(
                "Unable to read metrics blob '%s/%s'.",
                container_name,
                blob_name,
                exc_info=True,
            )
            return None

    def _find_task_summary_blob(
        self,
        container_name: str,
        pool_id: str | None,
        job_name: str,
        task_id: str,
        metrics_output_folder: str | None,
        metrics_prefix: str = "task-metrics",
    ) -> str | None:
        """Find summary.json for a task without requiring the report process to know the date."""
        container_client = self.blob_service_client.get_container_client(container_name)

        prefix_parts = [metrics_prefix.rstrip("/")]
        if pool_id:
            prefix_parts.append(pool_id)
        if metrics_output_folder:
            prefix_parts.append(metrics_output_folder)
        prefix = "/".join(prefix_parts).rstrip("/") + "/"

        suffix = f"/{job_name}/{task_id}/summary.json"
        matches = [
            item.name
            for item in container_client.list_blobs(name_starts_with=prefix)
            if item.name.endswith(suffix)
        ]

        if not matches:
            return None

        # A retry/re-run may leave multiple dated summaries. Lexicographic ordering
        # works for the YYYY-MM-DD path emitted by task-monitor.sh.
        return sorted(matches)[-1]

    def generate_run_report(
        self,
        job_name: str,
        report_container: str,
        metrics_container: str,
        report_prefix: str = "run-reports",
        metrics_prefix: str = "task-metrics",
    ) -> dict:
        """Aggregate one Batch job/run and upload JSON/CSV reports to Azure Blob Storage.

        The run is the Batch job and each Batch task is treated as one component.
        Task resource metrics are read from ``task-monitor.sh`` summary.json files.
        Job/task metadata supplies code version, model version, and workload metadata.
        """
        logger.info("Generating run report for Batch job '%s'.", job_name)

        job = self.batch_service_client.get_job(job_name)
        job_metadata = self._metadata_to_dict(getattr(job, "metadata", None))
        job_workload = self._decode_workload_metadata(job_metadata)

        pool_id = None
        execution_info = getattr(job, "execution_info", None)
        pool_info = getattr(job, "pool_info", None)
        if execution_info is not None:
            pool_id = getattr(execution_info, "pool_id", None)
        if pool_id is None and pool_info is not None:
            pool_id = getattr(pool_info, "pool_id", None)

        vm_size = None
        if pool_id:
            try:
                pool = batch_helpers.get_pool_full_info(
                    self.cred.azure_resource_group_name,
                    self.cred.azure_batch_account,
                    pool_id,
                    self.batch_mgmt_client,
                )
                vm_size = getattr(pool, "vm_size", None)
            except Exception:
                logger.warning(
                    "Could not retrieve VM size for pool '%s'.",
                    pool_id,
                    exc_info=True,
                )

        tasks = list(self.batch_service_client.list_tasks(job_name))
        components: list[dict] = []
        run_start = None
        run_end = None
        total_cpu_usec = 0.0
        max_component_peak_memory_bytes = None

        for task in tasks:
            task_id = str(getattr(task, "id", "unknown-task"))
            execution = getattr(task, "execution_info", None)
            task_metadata = self._metadata_to_dict(getattr(task, "metadata", None))
            task_workload = self._decode_workload_metadata(task_metadata)
            combined_workload = {**job_workload, **task_workload}

            start_time = getattr(execution, "start_time", None) if execution else None
            end_time = getattr(execution, "end_time", None) if execution else None
            exit_code = getattr(execution, "exit_code", None) if execution else None
            retry_count = getattr(execution, "retry_count", None) if execution else None
            requeue_count = (
                getattr(execution, "requeue_count", None) if execution else None
            )

            if start_time is not None and (run_start is None or start_time < run_start):
                run_start = start_time
            if end_time is not None and (run_end is None or end_time > run_end):
                run_end = end_time

            batch_runtime_sec = None
            if start_time is not None and end_time is not None:
                batch_runtime_sec = (end_time - start_time).total_seconds()

            metrics_output_folder = task_metadata.get("metrics_output_folder")
            summary_blob = self._find_task_summary_blob(
                container_name=metrics_container,
                pool_id=pool_id,
                job_name=job_name,
                task_id=task_id,
                metrics_output_folder=metrics_output_folder,
                metrics_prefix=metrics_prefix,
            )
            task_metrics = (
                self._download_json_blob(metrics_container, summary_blob)
                if summary_blob
                else None
            ) or {}

            runtime_sec = task_metrics.get("runtime_sec")
            if runtime_sec is None:
                runtime_sec = batch_runtime_sec

            cpu_usage_usec = task_metrics.get("cpu_usage_usec")
            try:
                if cpu_usage_usec is not None:
                    total_cpu_usec += float(cpu_usage_usec)
            except (TypeError, ValueError):
                pass

            peak_memory = task_metrics.get("memory_peak_bytes")
            try:
                if peak_memory is not None:
                    peak_memory = int(peak_memory)
                    if (
                        max_component_peak_memory_bytes is None
                        or peak_memory > max_component_peak_memory_bytes
                    ):
                        max_component_peak_memory_bytes = peak_memory
            except (TypeError, ValueError):
                pass

            average_cores_used = self._calculate_average_cores(
                cpu_usage_usec=cpu_usage_usec,
                runtime_sec=runtime_sec,
            )
            cpu_efficiency_pct = self._calculate_cpu_efficiency_pct(
                average_cores_used,
                combined_workload,
            )

            performance = dict(task_metrics)
            performance["average_cores_used"] = average_cores_used
            performance["cpu_efficiency_pct"] = cpu_efficiency_pct

            components.append(
                {
                    "task_id": task_id,
                    "code_version": task_metadata.get("code_version")
                    or job_metadata.get("code_version"),
                    "model_version": task_metadata.get("model_version")
                    or job_metadata.get("model_version"),
                    "workload": combined_workload,
                    "batch": {
                        "start_time": start_time.isoformat() if start_time else None,
                        "end_time": end_time.isoformat() if end_time else None,
                        "runtime_sec": batch_runtime_sec,
                        "exit_code": exit_code,
                        "retry_count": retry_count,
                        "requeue_count": requeue_count,
                    },
                    "performance": performance,
                    "metrics_blob": summary_blob,
                }
            )

        overall_runtime_sec = None
        if run_start is not None and run_end is not None:
            overall_runtime_sec = (run_end - run_start).total_seconds()

        report = {
            "job_id": job_name,
            "pool_id": pool_id,
            "vm_size": vm_size,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "code_version": job_metadata.get("code_version"),
            "model_version": job_metadata.get("model_version"),
            "workload": job_workload,
            "overall_runtime_sec": overall_runtime_sec,
            "component_count": len(components),
            "summary": {
                "total_cpu_sec": round(total_cpu_usec / 1_000_000.0, 6),
                "max_component_peak_memory_bytes": max_component_peak_memory_bytes,
            },
            "components": components,
        }

        create_storage_container_if_not_exists(
            report_container, self.blob_service_client
        )

        report_base = f"{report_prefix.strip('/')}/{job_name}"
        json_blob_name = f"{report_base}/report.json"
        json_blob_client = self.blob_service_client.get_blob_client(
            container=report_container,
            blob=json_blob_name,
        )
        json_blob_client.upload_blob(
            json.dumps(report, indent=2, default=str),
            overwrite=True,
        )

        csv_blob_name = None
        rows = []
        for component in components:
            performance = component["performance"]
            batch = component["batch"]
            workload = component["workload"]
            rows.append(
                {
                    "job_id": job_name,
                    "pool_id": pool_id,
                    "vm_size": vm_size,
                    "component": component["component"],
                    "task_id": component["task_id"],
                    "code_version": component["code_version"],
                    "model_version": component["model_version"],
                    "batch_runtime_sec": batch.get("runtime_sec"),
                    "runtime_sec": performance.get("runtime_sec")
                    or batch.get("runtime_sec"),
                    "exit_code": batch.get("exit_code"),
                    "memory_peak_bytes": performance.get("memory_peak_bytes"),
                    "cpu_usage_usec": performance.get("cpu_usage_usec"),
                    "average_cores_used": performance.get("average_cores_used"),
                    "cpu_efficiency_pct": performance.get("cpu_efficiency_pct"),
                    "pids_peak": performance.get("pids_peak"),
                    "threads_observed_end": performance.get("threads_observed_end"),
                    "io_read_bytes": performance.get("io_read_bytes"),
                    "io_write_bytes": performance.get("io_write_bytes"),
                    "workload_metadata": json.dumps(workload, default=str),
                }
            )

        frame = pd.DataFrame(rows)
        csv_buffer = io.StringIO()
        frame.to_csv(csv_buffer, index=False)
        csv_blob_name = f"{report_base}/components.csv"
        csv_blob_client = self.blob_service_client.get_blob_client(
            container=report_container,
            blob=csv_blob_name,
        )
        csv_blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

        report["report_location"] = {
            "container": report_container,
            "json_blob": json_blob_name,
            "csv_blob": csv_blob_name,
        }
        logger.info(
            "Uploaded run report for job '%s' to container '%s'.",
            job_name,
            report_container,
        )
        return report
