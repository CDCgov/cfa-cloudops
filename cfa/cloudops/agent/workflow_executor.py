import datetime as dt
from graphlib import CycleError, TopologicalSorter
from typing import Any

import yaml

from cfa.cloudops import CloudClient


def _timedelta_from_yaml(value: dict | None):
    if value is None:
        return None
    normalized = {}
    time_parts = ["minutes", "hours", "days"]
    for part in time_parts:
        if value[part] and value[part] != "NoneType":
            normalized[part] = value[part]
        else:
            normalized[part] = 0
    return dt.timedelta(**normalized)


SPECIAL_ARG_CONVERTERS = {
    "recurrence_interval": _timedelta_from_yaml,
    "start_window": _timedelta_from_yaml,
}


class WorkflowExecutor:
    def __init__(
        self, client: CloudClient, dependency_yaml_path: str, dry_run: bool = False
    ):
        self.client = client
        self.dry_run = dry_run
        with open(dependency_yaml_path, "r") as f:
            self.dependency_spec = yaml.safe_load(f)

    def validate_methods(self, workflow: list[dict[str, Any]]):
        allowed = self.dependency_spec["operations"]

        for step in workflow:
            method = step["method"]
            if method not in allowed:
                raise ValueError(f"Unsupported method: {method}")
            if not hasattr(self.client, method):
                raise ValueError(f"CloudClient has no method: {method}")

    def validate_dependencies(self, workflow: list[dict[str, Any]]):
        step_ids = {step["id"] for step in workflow}

        for step in workflow:
            for dep in step.get("depends_on", []):
                if dep not in step_ids:
                    raise ValueError(f"{step['id']} depends on missing step {dep}")

        sorter = TopologicalSorter()
        for step in workflow:
            sorter.add(step["id"], *step.get("depends_on", []))

        try:
            return list(sorter.static_order())
        except CycleError as e:
            raise ValueError(f"Workflow has a dependency cycle: {e}")

    def normalize_args(self, args: dict[str, Any]):
        normalized = dict(args)
        for key, converter in SPECIAL_ARG_CONVERTERS.items():
            if key in normalized:
                normalized[key] = converter(normalized[key])
        return normalized

    def execute(self, workflow_yaml_path: str):
        with open(workflow_yaml_path, "r") as f:
            spec = yaml.safe_load(f)

        workflow = spec["workflow"]
        self.validate_methods(workflow)
        ordered_ids = self.validate_dependencies(workflow)

        by_id = {step["id"]: step for step in workflow}
        results = {}

        for step_id in ordered_ids:
            step = by_id[step_id]
            method = getattr(self.client, step["method"])
            args = self.normalize_args(step.get("args", {}))

            print(f"Running step: {step_id}")
            if self.dry_run:
                print(f'Running method "{step["method"]}" with args: {args}')
            else:
                results[step_id] = method(**args)

        return results

    def execute_plan(self, spec: str):
        spec_yaml = yaml.load(spec, Loader=yaml.SafeLoader)
        workflow = spec_yaml["workflow"]
        self.validate_methods(workflow)
        ordered_ids = self.validate_dependencies(workflow)

        by_id = {step["id"]: step for step in workflow}
        results = {}
        log_entries = []

        for step_id in ordered_ids:
            step = by_id[step_id]
            method = getattr(self.client, step["method"])
            args = self.normalize_args(step.get("args", {}))

            log_entries.append(f"Running step: {step_id}")
            if self.dry_run:
                log_entries.append(
                    f'Running method:\n\t"{step["method"]}" with args: {args}'
                )
            else:
                results[step_id] = method(**args)

        return results, log_entries
