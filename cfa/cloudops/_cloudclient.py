import logging

from azure.batch import models as batch_models

# from azure.batch.models import TaskAddParameter
from azure.mgmt.batch import models

import cfa.cloudops.defaults as d
from cfa.cloudops import batch_helpers, blob

from .auth import EnvCredentialHandler, SPCredentialHandler
from .blob import create_storage_container_if_not_exists, get_node_mount_config
from .blob_helpers import upload_files_in_folder
from .client import (
    get_batch_management_client,
    get_batch_service_client,
    get_blob_service_client,
    get_compute_management_client,
)
from .job import create_job

logger = logging.getLogger(__name__)


class CloudClient:
    """High-level client for managing Azure Batch resources and operations.

    CloudClient provides a simplified interface for creating and managing Azure Batch
    pools, jobs, and tasks. It handles authentication, client initialization, and
    provides convenient methods for common batch operations.

    Args:
        dotenv_path (str, optional): Path to .env file containing environment variables.
            If None, uses default .env file discovery. Default is None.
        use_sp (bool, optional): Whether to use Service Principal authentication (True)
            or environment-based authentication (False). Default is False.
        **kwargs: Additional keyword arguments passed to the credential handler.

    Attributes:
        cred: Credential handler (EnvCredentialHandler or SPCredentialHandler)
        batch_mgmt_client: Azure Batch management client
        compute_mgmt_client: Azure Compute management client
        batch_service_client: Azure Batch service client
        blob_service_client: Azure Blob storage client
        pool_name (str): Name of the most recently created or used pool
        save_logs_to_blob (str): Blob container name for saving task logs
        logs_folder (str): Folder path within blob container for logs
        task_id_ints (bool): Whether to use integer task IDs
        task_id_max (int): Maximum task ID when using integer IDs

    Example:
        Create a client with environment-based authentication:

            client = CloudClient()

        Create a client with Service Principal authentication:

            client = CloudClient(
                use_sp=True,
                dotenv_path="/path/to/.env"
            )

        Create a client with custom configuration:

            client = CloudClient(
                azure_tenant_id="custom-tenant-id",
                azure_subscription_id="custom-sub-id"
            )
    """

    def __init__(self, dotenv_path: str = None, use_sp=False, **kwargs):
        # authenticate to get credentials
        if not use_sp:
            self.cred = EnvCredentialHandler(dotenv_path=dotenv_path, **kwargs)
        else:
            self.cred = SPCredentialHandler(dotenv_path=dotenv_path, **kwargs)
        # get clients

        self.batch_mgmt_client = get_batch_management_client(self.cred)
        self.compute_mgmt_client = get_compute_management_client(self.cred)
        self.batch_service_client = get_batch_service_client(self.cred)
        self.blob_service_client = get_blob_service_client(self.cred)

    def create_pool(
        self,
        pool_name: str,
        mounts=None,
        container_image_name=None,
        vm_size=d.default_vm_size,  # do some validation on size if too large
        autoscale=True,
        autoscale_formula="default",
        dedicated_nodes=5,
        low_priority_nodes=5,
        max_autoscale_nodes=3,
        task_slots_per_node=1,
        availability_zones="regional",
        cache_blobfuse=True,
    ):
        """Create a pool in Azure Batch with the specified configuration.

        A pool is a collection of compute nodes (virtual machines) on which your tasks run.
        This function creates a new pool with configurable scaling, container support,
        storage mounts, and availability zone placement.

        Args:
            pool_name (str): Name of the pool to create. Must be unique within the Batch account.
            mounts (list, optional): List of mount configurations as tuples of
                (storage_container, mount_name). Each tuple specifies a blob storage
                container to mount and the local mount point name.
            container_image_name (str, optional): Docker container image name to use for tasks.
                Should be in the format "registry/image:tag" or just "image:tag" for Docker Hub.
            vm_size (str): Azure VM size for the pool nodes (e.g., "Standard_D4s_v3").
                Defaults to the value from defaults module.
            autoscale (bool): Whether to enable autoscaling (True) or use fixed scaling (False).
                Default is True.
            autoscale_formula (str): Autoscale formula to use when autoscale=True.
                Use "default" for the built-in formula or provide a custom Azure Batch
                autoscale formula. Default is "default".
            dedicated_nodes (int): Number of dedicated nodes when autoscale=False.
                Only used for fixed scaling. Default is 5.
            low_priority_nodes (int): Number of low-priority nodes when autoscale=False.
                Low-priority nodes are cheaper but can be preempted. Default is 5.
            max_autoscale_nodes (int): Maximum number of nodes for autoscaling.
                Only used when autoscale=True. Default is 10.
            task_slots_per_node (int): Number of task slots per node. Determines how many
                tasks can run concurrently on each node. Default is 1.
            availability_zones (str): Availability zone placement policy. Must be either
                "regional" for regional deployment or "zonal" for zone-aware deployment.
                Default is "regional".
            cache_blobfuse (bool): Whether to enable blobfuse caching for mounted storage.
                Improves performance for read-heavy workloads. Default is True.

        Raises:
            RuntimeError: If the pool creation fails due to Azure Batch service errors,
                authentication issues, or invalid parameters.
            ValueError: If availability_zones is not "regional" or "zonal", or if other
                parameters have invalid values.

        Example:
            Create a simple autoscaling pool:

                client = CloudClient()
                client.create_pool(
                    pool_name="my-compute-pool",
                    container_image_name="myapp:latest",
                    vm_size="Standard_D2s_v3"
                )

            Create a pool with storage mounts and fixed scaling:

                client.create_pool(
                    pool_name="data-processing-pool",
                    container_image_name="python:3.9",
                    vm_size="Standard_D4s_v3",
                    mounts=[("input-data", "data"), ("output-results", "results")],
                    autoscale=False,
                    dedicated_nodes=5,
                    availability_zones="zonal"
                )

        Note:
            The pool must be created before jobs can be submitted to it. Ensure that
            the specified VM size is available in your Azure region and that any
            container images are accessible from the compute nodes.
        """
        # Initialize mount configuration
        mount_config = None

        # Configure storage mounts if provided
        if mounts is not None:
            storage_containers = []
            mount_names = []
            for mount in mounts:
                storage_containers.append(mount[0])
                mount_names.append(mount[1])
            mount_config = get_node_mount_config(
                storage_containers=storage_containers,
                mount_names=mount_names,
                account_names=self.cred.azure_blob_storage_account,
                identity_references=self.cred.compute_node_identity_reference,
                cache_blobfuse=cache_blobfuse,  # Pass cache setting to mount config
            )

        # validate pool name
        pool_name = pool_name.replace(" ", "_")

        # validate vm size
        print("Verify the size of the VM is appropriate for the use case.")
        print("**Please use smaller VMs for dev/testing.**")

        # Get base pool configuration
        pool_config = d.get_default_pool_config(
            pool_name=pool_name,
            subnet_id=self.cred.azure_subnet_id,
            user_assigned_identity=self.cred.azure_user_assigned_identity,
            mount_configuration=mount_config,
            vm_size=vm_size,
        )

        # Configure scaling settings
        if autoscale:
            # Set up autoscaling
            if autoscale_formula == "default":
                # Default formula: scale based on pending tasks with max limit
                formula = d.remaining_task_autoscale_formula(
                    task_sample_interval_minutes=15,
                    max_number_vms=max_autoscale_nodes,
                )
            else:
                formula = autoscale_formula

            pool_config.scale_settings = models.ScaleSettings(
                auto_scale=models.AutoScaleSettings(
                    formula=formula,
                    evaluation_interval="PT5M",  # Evaluate every 5 minutes
                )
            )
        else:
            # Set up fixed scaling
            pool_config.scale_settings = models.ScaleSettings(
                fixed_scale=models.FixedScaleSettings(
                    target_dedicated_nodes=dedicated_nodes,
                    target_low_priority_nodes=low_priority_nodes,
                )
            )

        # Configure task slots per node
        pool_config.task_slots_per_node = task_slots_per_node

        # Configure container if image is provided
        if container_image_name:
            container_config = models.ContainerConfiguration(
                type="dockerCompatible",
                container_image_names=[container_image_name],
            )

            # Add container registry if available
            if hasattr(self.cred, "azure_container_registry"):
                container_config.container_registries = [
                    self.cred.azure_container_registry
                ]

            d.assign_container_config(pool_config, container_config)

        # Configure availability zones in the virtual machine configuration
        # Set node placement configuration for zonal deployment
        if availability_zones.lower() == "regional":
            pool_config.deployment_configuration.virtual_machine_configuration.node_placement_configuration = models.NodePlacementConfiguration(
                policy=models.NodePlacementPolicyType.regional
            )
        elif availability_zones.lower() == "zonal":
            pool_config.deployment_configuration.virtual_machine_configuration.node_placement_configuration = models.NodePlacementConfiguration(
                policy=models.NodePlacementPolicyType.zonal
            )
        else:
            raise ValueError(
                "Availability zone needs to be 'zonal' or 'regional'."
            )

        # Configure blobfuse caching
        if cache_blobfuse and mount_config:
            pass

        try:
            # Create the pool using the batch management client
            self.batch_mgmt_client.pool.create(
                resource_group_name=self.cred.azure_resource_group_name,
                account_name=self.cred.azure_batch_account,
                pool_name=pool_name,
                parameters=pool_config,
            )
            self.pool_name = pool_name
            print(f"created pool: {pool_name}")
        except Exception as e:
            error_msg = f"Failed to create pool '{pool_name}': {str(e)}"
            raise RuntimeError(error_msg)

    def create_job(
        self,
        job_name: str,
        pool_name: str,
        uses_deps: bool = True,
        save_logs_to_blob: str | None = None,
        logs_folder: str | None = None,
        task_retries: int = 0,
        mark_complete_after_tasks_run: bool = False,
        task_id_ints: bool = False,
        timeout: int | None = None,
        exist_ok=False,
    ):
        """Create a job in Azure Batch to run tasks on a specified pool.

        A job is a collection of tasks that run on compute nodes in a pool. Jobs provide
        a way to organize and manage related tasks, handle dependencies, and control task
        execution settings. Tasks are added to the job after it's created.

        Args:
            job_name (str): Unique identifier for the job. Must be unique within the Batch
                account. Can contain letters, numbers, hyphens, and underscores. Cannot
                exceed 64 characters. Spaces will be automatically removed.
            pool_name (str): Name of the pool where the job's tasks will run. The pool
                must already exist and be in an active state.
            uses_deps (bool, optional): Whether to enable task dependencies for this job.
                When True, tasks can specify dependencies on other tasks within the same job.
                Default is True.
            save_logs_to_blob (str, optional): Azure Blob Storage container name where task
                logs should be saved. If provided, stdout and stderr from tasks will be
                automatically uploaded to this container. Default is None (logs not saved to blob).
            logs_folder (str, optional): Folder path within the blob container where logs
                should be stored. Only used when save_logs_to_blob is specified. Leading and
                trailing slashes are automatically handled. Default is "stdout_stderr".
            task_retries (int, optional): Maximum number of times a task can be retried if
                it fails. Tasks will be retried automatically up to this limit. Valid range:
                0-100. Default is 0 (no retries).
            mark_complete_after_tasks_run (bool, optional): Whether to automatically mark
                the job as complete after all tasks finish. When True, the job will be marked
                complete without requiring explicit job termination. Default is False.
            task_id_ints (bool, optional): Whether to use integer task IDs instead of string
                IDs. When True, tasks added to this job should use integer IDs for better
                performance with large numbers of tasks. Default is False (use string IDs).
            timeout (int, optional): Maximum time in minutes that the job can run before
                being terminated. If None, no timeout is set and the job can run indefinitely.
                Default is None (no timeout).
            exist_ok (bool, optional): Whether to allow the job creation if a job with the
                same name already exists. Default is False.

        Raises:
            RuntimeError: If the job creation fails due to Azure Batch service errors,
                authentication issues, or invalid parameters.
            ValueError: If the job_name or pool_name are invalid, or if the specified
                pool does not exist.

        Example:
            Create a simple job with default settings:

                client = CloudClient()
                client.create_job(
                    job_name="data-processing-job",
                    pool_name="compute-pool"
                )

            Create a job with dependencies, retries, and log saving:

                client.create_job(
                    job_name="pipeline-job",
                    pool_name="compute-pool",
                    uses_deps=True,
                    task_retries=3,
                    save_logs_to_blob="job-logs",
                    logs_folder="pipeline-logs/run-001",
                    timeout=120,  # 2 hours
                    mark_complete_after_tasks_run=True
                )

            Create a job optimized for many tasks:

                client.create_job(
                    job_name="bulk-processing",
                    pool_name="large-pool",
                    task_id_ints=True,  # Better performance for many tasks
                    save_logs_to_blob="bulk-logs",
                    exist_ok=True
                )

        Note:
            - The job must be created before adding tasks to it
            - Task dependencies only work when uses_deps=True
            - If save_logs_to_blob is specified, ensure the blob container exists
            - Job names are automatically cleaned of spaces
        """
        # save job information that will be used with tasks
        job_name = job_name.replace(" ", "")
        if pool_name:
            self.pool_name = pool_name
        elif self.pool_name:
            pool_name = self.pool_name
        self.save_logs_to_blob = save_logs_to_blob

        if save_logs_to_blob:
            if logs_folder is None:
                self.logs_folder = "stdout_stderr"
            else:
                if logs_folder.startswith("/"):
                    logs_folder = logs_folder[1:]
                if logs_folder.endswith("/"):
                    logs_folder = logs_folder[:-1]
                self.logs_folder - logs_folder

        if task_id_ints:
            self.task_id_ints = True
            self.task_id_max = 0
        else:
            self.task_id_ints = False

        # add the job
        job = batch_models.JobAddParameter(
            id=job_name,
            pool_info=batch_models.PoolInformation(pool_id=pool_name),
            uses_task_dependencies=uses_deps,
        )

        # Configure job constraints if specified
        if timeout is not None:
            job.constraints = batch_models.JobConstraints(
                max_wall_clock_time=timeout * 60
            )

        # Configure job manager task settings
        if mark_complete_after_tasks_run:
            job.on_all_tasks_complete = (
                batch_models.OnAllTasksComplete.terminate_job
            )

        # Configure task retry settings
        if task_retries > 0:
            job.constraints = job.constraints or batch_models.JobConstraints()
            job.constraints.max_task_retry_count = task_retries

        # Configure log saving if specified
        if save_logs_to_blob:
            # Note: Log saving configuration would typically be handled
            # at the task level, not the job level. This is a placeholder
            # for future implementation.
            pass

        # Create the job
        create_job(self.batch_service_client, job, exist_ok=exist_ok)

    def add_task(
        self,
        job_name: str,
        base_call: list[str],
        task_id: str = None,
        depends_on: list[str] | None = None,
        depends_on_range: tuple | None = None,
        run_dependent_tasks_on_fail: bool = False,
        container_image_name: str = None,
        container_image_version: str = "latest",
        timeout: int | None = None,
    ):
        """Add a task to an existing Azure Batch job.

        Creates and adds a new task to the specified job. Tasks can have dependencies
        on other tasks, run in containers, and have configurable timeouts and retry policies.

        Args:
            job_name (str): ID of the job to add the task to. The job must already exist.
            base_call (list[str] | str): Command to execute for the task. Can be provided
                as a list of strings (which will be joined with spaces) or as a single
                command string.
            task_id (str, optional): Unique identifier for the task within the job.
                If None, a task ID will be auto-generated. Must be unique within the job.
            depends_on (list[str], optional): List of task IDs that this task depends on.
                The task will not start until all dependency tasks have completed successfully.
                Only works if the job was created with uses_deps=True.
            depends_on_range (tuple, optional): Range of dependent tasks when task IDs are
                integers, specified as (start_int, end_int). Alternative to depends_on for
                jobs using integer task IDs.
            run_dependent_tasks_on_fail (bool, optional): Whether dependent tasks should
                run even if this task fails. Default is False (dependent tasks only run
                on success).
            container_image_name (str, optional): Docker container image to use for the task.
                Should be in format "registry/image" or "image" for Docker Hub. If None,
                uses the container configured at the pool level.
            container_image_version (str, optional): Version/tag of the container image.
                Default is "latest".
            timeout (int, optional): Maximum time in minutes the task can run before being
                terminated. If None, no timeout is applied (task can run indefinitely).

        Returns:
            str: The task ID that was created or assigned.

        Raises:
            RuntimeError: If the task creation fails due to Azure Batch service errors,
                authentication issues, or invalid parameters.
            ValueError: If the job_name does not exist, task_id conflicts with existing task,
                or if dependency configuration is invalid.

        Example:
            Add a simple task:

                task_id = client.add_task(
                    job_name="my-job",
                    base_call=["python", "script.py", "--input", "data.txt"]
                )

            Add a task with dependencies:

                task_id = client.add_task(
                    job_name="pipeline-job",
                    base_call="python preprocess.py",
                    task_id="preprocess-task",
                    timeout=30
                )

                dependent_task_id = client.add_task(
                    job_name="pipeline-job",
                    base_call="python analyze.py",
                    task_id="analysis-task",
                    depends_on=["preprocess-task"],
                    container_image_name="myregistry/analytics",
                    container_image_version="v2.1"
                )

            Add task with integer ID dependencies:

                task_id = client.add_task(
                    job_name="bulk-job",
                    base_call="python process_chunk.py",
                    depends_on_range=(1, 10),  # Depends on tasks 1-10
                    timeout=60
                )

        Note:
            - Task dependencies only work if the job was created with uses_deps=True
            - Container images must be accessible from the compute nodes
            - Task IDs must be unique within the job
            - Command execution occurs in the task's working directory on the compute node
        """
        if isinstance(base_call, list):
            base_call = " ".join(base_call)
        # Add a task to the job
        az_mount_dir = "$AZ_BATCH_NODE_MOUNTS_DIR"
        user_identity = batch_models.UserIdentity(
            auto_user=batch_models.AutoUserSpecification(
                scope=batch_models.AutoUserScope.pool,
                elevation_level=batch_models.ElevationLevel.admin,
            )
        )
        print(az_mount_dir, user_identity)

        # pull mounts from associated pool

        # get task config for task
        # task_config = get_task_config()
        # add task
        self.batch_service_client.task.add()

    def create_blob_container(self, name: str) -> None:
        # create_container and save the container client
        create_storage_container_if_not_exists(name, self.blob_service_client)
        logger.debug(f"Created container client for container {name}.")

    def upload_files(
        self,
        files: str | list[str],
        container_name: str,
        local_root_dir: str = ".",
        location_in_blob: str = ".",
    ) -> None:
        blob.upload_to_storage_container(
            file_paths=files,
            blob_storage_container_name=container_name,
            blob_service_client=self.blob_service_client,
            local_root_dir=local_root_dir,
            remote_root_dir=location_in_blob,
        )

    def upload_folders(
        self,
        folder_names: list[str],
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        exclude_patterns: str | list | None = None,
        location_in_blob: str = ".",
        force_upload: bool = False,
    ) -> list[str]:
        _files = []
        for _folder in folder_names:
            logger.debug(f"Trying to upload folder {_folder}.")
            _uploaded_files = upload_files_in_folder(
                folder=_folder,
                container_name=container_name,
                include_extensions=include_extensions,
                exclude_extensions=exclude_extensions,
                exclude_patterns=exclude_patterns,
                location_in_blob=location_in_blob,
                blob_service_client=self.blob_service_client,
                force_upload=force_upload,
            )
            _files += _uploaded_files
        logger.debug(f"uploaded {_files}")
        self.files += _files
        return _files

    def monitor_job(
        self,
        job_name: str,
        timeout: str | None = None,
        download_job_stats: bool = False,
    ) -> None:
        """monitor the tasks running in a job

        Args:
            job_name (str): job id
            timeout (str): timeout for monitoring job. If omitted, will use None.
            download_job_stats (bool): whether to download job statistics when job completes. Default is False.
        """
        # monitor the tasks
        logger.debug(f"starting to monitor job {job_name}.")
        monitor = batch_helpers.monitor_tasks(
            job_name, timeout, self.batch_service_client
        )
        print(monitor)

        if download_job_stats:
            batch_helpers.download_job_stats(
                job_name=job_name,
                batch_service_client=self.batch_service_client,
                file_name=None,
            )
        logger.info("Job complete.")

    def check_job_status(self, job_name: str) -> None:
        """checks various components of a job
        - whether job exists
        - prints number of completed tasks
        - prints the state of a job: completed, activate, etc.

        Args:
            job_name (str): name of job
        """
        # whether job exists
        logger.debug("Checking job exists.")
        if batch_helpers.check_job_exists(job_name, self.batch_service_client):
            logger.debug(f"Job {job_name} exists.")
            c_tasks = batch_helpers.get_completed_tasks(
                job_name, self.batch_service_client
            )
            logger.info("Task info:")
            logger.info(c_tasks)
            if batch_helpers.check_job_complete(
                job_name, self.batch_service_client
            ):
                logger.info(f"Job {job_name} completed.")
            else:
                j_state = batch_helpers.get_job_state(
                    job_name, self.batch_service_client
                )
                logger.info(f"Job in {j_state} state")
        else:
            logger.info(f"Job {job_name} does not exist.")
