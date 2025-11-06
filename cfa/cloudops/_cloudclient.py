import datetime
import logging
import os
from graphlib import CycleError, TopologicalSorter

import pandas as pd
from azure.batch import models as batch_models
from azure.batch.models import (
    JobConstraints,
    MetadataItem,
    OnAllTasksComplete,
    OnTaskFailure,
)

# from azure.batch.models import TaskAddParameter
from azure.mgmt.batch import models
from azure.mgmt.resource import SubscriptionClient

import cfa.cloudops.defaults as d
from cfa.cloudops import batch_helpers, blob, blob_helpers, helpers

from .auth import (
    DefaultCredentialHandler,
    EnvCredentialHandler,
    SPCredentialHandler,
)
from .blob import create_storage_container_if_not_exists, get_node_mount_config
from .blob_helpers import upload_files_in_folder
from .client import (
    get_batch_management_client,
    get_batch_service_client,
    get_blob_service_client,
    get_compute_management_client,
)
from .job import create_job, create_job_schedule

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
        cred: Credential handler (EnvCredentialHandler, SPCredentialHandler, or DefaultCredentialHandler)
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

    def __init__(
        self,
        dotenv_path: str = None,
        use_sp: bool = False,
        use_federated: bool = False,
        **kwargs,
    ):
        # authenticate to get credentials
        if not use_sp and not use_federated:
            self.cred = EnvCredentialHandler(dotenv_path=dotenv_path, **kwargs)
            self.method = "env"
            logger.info("Using environment-based credentials.")
        elif use_federated:
            self.cred = DefaultCredentialHandler(dotenv_path=dotenv_path, **kwargs)
            self.method = "default"
            logger.info("Using default credentials.")
        else:
            self.cred = SPCredentialHandler(dotenv_path=dotenv_path, **kwargs)
            self.method = "sp"
            logger.info("Using service principal credentials.")
        # get clients

        self.batch_mgmt_client = get_batch_management_client(self.cred)
        self.compute_mgmt_client = get_compute_management_client(self.cred)
        self.batch_service_client = get_batch_service_client(self.cred)
        self.blob_service_client = get_blob_service_client(self.cred)
        self.full_container_name = None
        self.save_logs_to_blob = None
        self.logs_folder = "stdout_stderr"
        self.task_id_ints = False
        self.task_id_max = 0

    def check_credentials(self):
        if self.method == "env":
            cred = self.cred.user_credential
        elif self.method == "default":
            cred = self.cred.client_secret_sp_credential
        else:
            cred = self.cred.client_secret_credential

        try:
            subscription_client = SubscriptionClient(cred)
            # List subscriptions
            sub_list = [sub for sub in subscription_client.subscriptions.list()]
            for subscription in sub_list:
                print("Found subscription via credential.")
                print(f"Subscription ID: {subscription.subscription_id}")
                print(f"Subscription Name: {subscription.display_name}")
                print(f"State: {subscription.state}")
                print("-" * 30)
        except Exception as e:
            print(f"An error occurred: {e}")

    def create_pool(
        self,
        pool_name: str,
        mounts=None,
        container_image_name=None,
        vm_size=d.default_vm_size,  # do some validation on size if too large
        autoscale=True,
        autoscale_formula="default",
        dedicated_nodes=0,
        low_priority_nodes=1,
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
                Only used for fixed scaling. Default is 0.
            low_priority_nodes (int): Number of low-priority nodes when autoscale=False.
                Low-priority nodes are cheaper but can be preempted. Default is 1.
            max_autoscale_nodes (int): Maximum number of nodes for autoscaling.
                Only used when autoscale=True. Default is 3.
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
        else:
            raise ValueError("container_image_name not provided.")

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
            raise ValueError("Availability zone needs to be 'zonal' or 'regional'.")

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
        verify_pool: bool = True,
        verbose=False,
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
        logger.debug(f"job_name: {job_name}")

        if pool_name:
            self.pool_name = pool_name
        elif self.pool_name:
            pool_name = self.pool_name
        else:
            logger.error("Please specify a pool for the job and try again.")
            raise Exception("Please specify a pool for the job and try again.")

        self.save_logs_to_blob = save_logs_to_blob

        if save_logs_to_blob:
            if logs_folder is None:
                self.logs_folder = "stdout_stderr"
            else:
                if logs_folder.startswith("/"):
                    logs_folder = logs_folder[1:]
                if logs_folder.endswith("/"):
                    logs_folder = logs_folder[:-1]
                self.logs_folder = logs_folder
        if timeout is None:
            _to = None
        else:
            _to = datetime.timedelta(minutes=timeout)

        on_all_tasks_complete = (
            OnAllTasksComplete.terminate_job
            if mark_complete_after_tasks_run
            else OnAllTasksComplete.no_action
        )

        job_constraints = JobConstraints(
            max_task_retry_count=task_retries,
            max_wall_clock_time=_to,
        )
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
            on_all_tasks_complete=on_all_tasks_complete,
            on_task_failure=OnTaskFailure.perform_exit_options_job_action,
            constraints=job_constraints,
            metadata=[
                MetadataItem(name="mark_complete", value=mark_complete_after_tasks_run)
            ],
        )

        # Configure task retry settings
        if task_retries > 0:
            job.constraints = job.constraints or batch_models.JobConstraints()
            job.constraints.max_task_retry_count = task_retries

        # Create the job
        create_job(
            self.batch_service_client,
            job,
            exist_ok=exist_ok,
            verify_pool=verify_pool,
            verbose=verbose,
        )

    def create_job_schedule(
        self,
        job_schedule_name: str,
        pool_name: str,
        command: str,
        timeout: int = 30,
        start_window: datetime.timedelta = None,
        recurrence_interval: datetime.timedelta = None,
        do_not_run_until: str = None,
        do_not_run_after: str = None,
        exist_ok=False,
        verify_pool: bool = True,
        verbose=False,
    ):
        """Create a job schedule in Azure Batch to run a job on a specified pool.

        An job schedule is a service resource that automates the creation of recurring jobs.
        Instead of manually submitting the same job each time it needs to run,
        you can create a job schedule that handles the process automatically on a defined cadence

        Args:
            job_schedule_name (str): Unique display name for the job. Must be unique within the Batch
                account. Can contain letters, numbers, hyphens, and underscores. Cannot
                exceed 1024 characters. Spaces will be automatically replaced with dashes.
            pool_name (str): Name of Azure batch pool where the job's tasks will run. The pool must exist before job schedule is created.
            command (str): Docker command that will be run by the job manager task of the job created by the job schedule.
            timeout (int, optional): The maximum time that the server can spend processing the request, in seconds.
                Default is 30 seconds.
            start_window (timedelta): If a Job is not created within the startWindow interval, then the 'opportunity' is lost;
                no Job will be created until the next recurrence of the schedule.
            recurrence_interval (timedelta): Specify a recurring interval for running the specified job
            do_not_run_until (str): Disable the schedule until the specified time
            do_not_run_after (str): Disable the schedule after the specified time
            exist_ok (bool, optional): Whether to allow the job schedule creation if a job schedule with the
                same name already exists. Default is False.

        Raises:
            RuntimeError: If the job schedule creation fails due to Azure Batch service errors,
                authentication issues, or invalid parameters.
            ValueError: If the job schedule ID is invalid

        Example:
            Create a simple job schedule with default timeout of 30 seconds and recurrence interval of 10 minutes

                client = CloudClient()
                client.create_job_schedule(
                    job_schedule_name="Data Processing Job Schedule",
                    pool_name="my-test-pool-1",
                    command="python process_data.py",
                    recurrence_interval=datetime.timedelta(minutes=10)
                )

            Create a simple job schedule with timeout of 900 seconds, recurrence interval of 2 hours. Job must be run before 11 PM on December 31st, 2025.

                client = CloudClient()
                client.create_job_schedule(
                    job_schedule_name="Data Processing Job Schedule",
                    pool_name="my-test-pool-2",
                    command="python process_data.py",
                    timeout=900,
                    recurrence_interval=datetime.timedelta(hours=2),
                    do_not_run_after="2025-12-31 23:00:00"
                )
        """
        job_schedule_id = job_schedule_name.replace(" ", "-").lower()
        logger.debug(f"job_schedule_id: {job_schedule_id}")

        job_specification = batch_models.JobSpecification(
            pool_info=batch_models.PoolInformation(pool_id=pool_name),
            on_all_tasks_complete=batch_models.OnAllTasksComplete.terminate_job,
            job_manager_task=batch_models.JobManagerTask(
                id=f"{job_schedule_id}-job", command_line=command
            ),
        )

        do_not_run_after_datetime = None
        if do_not_run_after:
            do_not_run_after_datetime = datetime.datetime.strptime(
                do_not_run_after, d.default_datetime_format
            )
        do_not_run_until_datetime = None
        if do_not_run_until:
            do_not_run_until_datetime = datetime.datetime.strptime(
                do_not_run_until, d.default_datetime_format
            )
        schedule = batch_models.Schedule(
            start_window=start_window,
            recurrence_interval=recurrence_interval,
            do_not_run_until=do_not_run_until_datetime,
            do_not_run_after=do_not_run_after_datetime,
        )

        # add the job schedule
        job_schedule_add_param = batch_models.JobScheduleAddParameter(
            id=job_schedule_id,
            display_name=job_schedule_name,
            schedule=schedule,
            job_specification=job_specification,
        )

        job_schedule_add_options = batch_models.JobScheduleAddOptions(
            timeout=timeout,
        )

        # Create the job
        create_job_schedule(
            self.batch_service_client,
            job_schedule_add_param,
            exist_ok=exist_ok,
            verify_pool=verify_pool,
            verbose=verbose,
            job_schedule_add_options=job_schedule_add_options,
        )

    def add_task(
        self,
        job_name: str,
        command_line: str,
        name_suffix: str = "",
        depends_on: str | None = None,
        depends_on_range: tuple | None = None,
        run_dependent_tasks_on_fail: bool = False,
        container_image_name: str = None,
        timeout: int | None = None,
    ):
        """
        Add a task to an Azure Batch job.

        Args:
            job_name (str): Name of the job to add the task to.
            command_line (str): Command line arguments for the task.
            name_suffix (str, optional): Suffix to append to the task ID.
            depends_on (list[str], optional): List of task IDs this task depends on.
            depends_on_range (tuple, optional): Range of task IDs this task depends on.
            run_dependent_tasks_on_fail (bool, optional): Whether to run dependent tasks if this task fails.
            container_image_name (str, optional): Container image to use for the task.
            timeout (int, optional): Maximum time in minutes for the task to run.
        """
        # get pool info for related job
        job_info = self.batch_service_client.job.get(job_name)
        pool_name = job_info.as_dict()["execution_info"]["pool_id"]

        if container_image_name is None:
            if self.full_container_name is None:
                logger.debug("Gettting full pool info")
                pool_info = batch_helpers.get_pool_full_info(
                    self.cred.azure_resource_group_name,
                    self.cred.azure_batch_account,
                    pool_name,
                    self.batch_mgmt_client,
                )
                logger.debug("Generated full pool info.")
                vm_config = (
                    pool_info.deployment_configuration.virtual_machine_configuration
                )
                logger.debug("Generated VM config.")
                pool_container = vm_config.container_configuration.container_image_names
                container_name = pool_container[0].split("://")[-1]
                logger.debug(f"Container name set to {container_name}.")
            else:
                container_name = self.full_container_name
                logger.debug(f"Container name set to {container_name}.")
        else:
            container_name = container_image_name

        if self.save_logs_to_blob:
            rel_mnt_path = batch_helpers.get_rel_mnt_path(
                blob_name=self.save_logs_to_blob,
                pool_name=pool_name,
                resource_group_name=self.cred.azure_resource_group_name,
                account_name=self.cred.azure_batch_account,
                batch_mgmt_client=self.batch_mgmt_client,
            )
            if rel_mnt_path != "ERROR!":
                rel_mnt_path = "/" + helpers.format_rel_path(rel_path=rel_mnt_path)
        else:
            rel_mnt_path = None

        # get all mounts from pool info
        self.mounts = batch_helpers.get_pool_mounts(
            pool_name,
            self.cred.azure_resource_group_name,
            self.cred.azure_batch_account,
            self.batch_mgmt_client,
        )

        logger.debug("Adding tasks to job.")
        tid = batch_helpers.add_task(
            job_name=job_name,
            task_id_base=job_name,
            command_line=command_line,
            save_logs_rel_path=rel_mnt_path,
            logs_folder=self.logs_folder,
            name_suffix=name_suffix,
            mounts=self.mounts,
            depends_on=depends_on,
            depends_on_range=depends_on_range,
            run_dependent_tasks_on_fail=run_dependent_tasks_on_fail,
            batch_client=self.batch_service_client,
            full_container_name=container_name,
            task_id_max=self.task_id_max,
            task_id_ints=self.task_id_ints,
            timeout=timeout,
        )
        self.task_id_max += 1
        print(f"Added task {tid} to job {job_name}.")
        return tid

    def create_blob_container(self, name: str) -> None:
        """Create a blob storage container if it doesn't already exist.

        Creates a new Azure Blob Storage container with the specified name. If the
        container already exists, this operation completes successfully without error.

        Args:
            name (str): Name of the blob storage container to create. Must follow Azure
                naming conventions: lowercase letters, numbers, and hyphens only, must
                start and end with letter or number, 3-63 characters long.

        Example:
            Create a container for storing input data:

                client = CloudClient()
                client.create_blob_container("input-data")

            Create a container for job outputs:

                client.create_blob_container("job-results-2024")

        Note:
            Container names must be globally unique within the storage account and
            follow Azure naming rules. The operation is idempotent - calling it
            multiple times with the same name is safe.
        """
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
        """Upload files to an Azure Blob Storage container.

        Uploads one or more files from the local filesystem to a blob storage container.
        The files maintain their relative directory structure within the container.

        Args:
            files (str | list[str]): Path(s) to file(s) to upload. Can be a single file
                path as a string or a list of file paths. Paths can be relative or absolute.
            container_name (str): Name of the blob storage container to upload to. The
                container must already exist.
            local_root_dir (str, optional): Local directory to use as the base path for
                relative file paths. Files will be uploaded relative to this directory.
                Default is "." (current directory).
            location_in_blob (str, optional): Remote directory path within the blob container
                where files should be uploaded. Default is "." (container root).

        Example:
            Upload a single file:

                client = CloudClient()
                client.upload_files(
                    files="data/input.csv",
                    container_name="job-data"
                )

            Upload multiple files with custom paths:

                client.upload_files(
                    files=["config.json", "scripts/process.py", "data/input.txt"],
                    container_name="job-data",
                    local_root_dir="/home/user/project",
                    location_in_blob="job-123"
                )

        Note:
            The blob container must exist before uploading files. Use create_blob_container()
            to create it if needed. Files are uploaded with their directory structure preserved.
        """
        blob.upload_to_storage_container(
            file_paths=files,
            blob_storage_container_name=container_name,
            blob_service_client=self.blob_service_client,
            local_root_dir=local_root_dir,
            remote_root_dir=location_in_blob,
        )

    def upload_folders(
        self,
        folder_names: str | list[str],
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        exclude_patterns: str | list | None = None,
        location_in_blob: str = ".",
        force_upload: bool = False,
    ) -> list[str]:
        """Upload entire folders to an Azure Blob Storage container with filtering options.

        Recursively uploads all files from specified folders to a blob storage container.
        Supports filtering by file extensions and patterns to control which files are uploaded.

        Args:
            folder_names (list[str]): List of local folder paths to upload. Each folder
                will be recursively uploaded with its directory structure preserved.
            container_name (str): Name of the blob storage container to upload to. The
                container must already exist.
            include_extensions (str | list, optional): File extensions to include in the
                upload. Can be a single extension string (e.g., ".py") or list of extensions
                (e.g., [".py", ".txt"]). If None, all extensions are included.
            exclude_extensions (str | list, optional): File extensions to exclude from
                the upload. Can be a single extension string or list. Takes precedence
                over include_extensions if a file matches both.
            exclude_patterns (str | list, optional): Filename patterns to exclude using
                glob-style matching (e.g., "*.tmp", "__pycache__"). Can be a single pattern
                string or list of patterns.
            location_in_blob (str, optional): Remote directory path within the blob container
                where folders should be uploaded. Default is "." (container root).
            force_upload (bool, optional): Whether to force upload files even if they
                already exist in the container with the same size. Default is False
                (skip existing files with same size).

        Returns:
            list[str]: List of file paths that were successfully uploaded to the container.

        Example:
            Upload Python source folders:

                client = CloudClient()
                uploaded_files = client.upload_folders(
                    folder_names=["src", "tests"],
                    container_name="code-repo",
                    include_extensions=[".py", ".yaml"],
                    exclude_patterns=["__pycache__", "*.pyc"]
                )

            Upload data folders with custom location:

                uploaded_files = client.upload_folders(
                    folder_names=["data/input", "data/config"],
                    container_name="job-data",
                    location_in_blob="run-001",
                    exclude_extensions=[".tmp", ".log"],
                    force_upload=True
                )

        Note:
            The blob container must exist before uploading. Directory structure is
            preserved in the container. Use filtering options to avoid uploading
            unnecessary files like temporary files or build artifacts.
        """
        _files = []
        if isinstance(folder_names, str):
            folder_names = [folder_names]
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
        return _files

    def monitor_job(
        self,
        job_name: str,
        timeout: int | None = None,
        download_job_stats: bool = False,
    ) -> None:
        """Monitor the execution of tasks in an Azure Batch job.

        Continuously monitors the progress of all tasks in a job until they complete
        or a timeout is reached. Provides real-time status updates and optionally
        downloads job statistics when complete.

        Args:
            job_name (str): ID of the job to monitor. The job must exist and be in
                an active state.
            timeout (int, optional): Maximum time in minutes to monitor the job before giving up.
                If None, monitoring continues indefinitely until all tasks complete.
            download_job_stats (bool, optional): Whether to download comprehensive job
                statistics when the job completes. Statistics include task execution
                times, resource usage, and success/failure rates. Default is False.

        Example:
            Monitor a job with default settings:

                client = CloudClient()
                client.monitor_job("data-processing-job")

            Monitor with timeout and statistics download:

                client.monitor_job(
                    job_name="long-running-job",
                    timeout=120,  # 2 hours in minutes
                    download_job_stats=True
                )

        Note:
            This method blocks until the job completes or times out. For non-blocking
            job status checks, use check_job_status() instead. Job statistics are
            saved to the current working directory when downloaded.
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

    def check_job_status(self, job_name: str) -> str:
        """Check the current status and progress of an Azure Batch job.

        Performs a comprehensive status check of a job including existence verification,
        task completion counts, and overall job state. Provides detailed logging of
        the job's current status without blocking execution.

        Args:
            job_name (str): Name/ID of the job to check. The job may or may not exist.

        Returns:
            str: job status info

        Example:
            Check status of a running job:

                client = CloudClient()
                client.check_job_status("data-processing-job")

            Check multiple jobs in a loop:

                job_names = ["job-1", "job-2", "job-3"]
                for job_name in job_names:
                    client.check_job_status(job_name)

        Note:
            This method is non-blocking and provides a point-in-time status check.
            For continuous monitoring, use monitor_job() instead. Status information
            is logged at info level and printed to the console.
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
            if batch_helpers.check_job_complete(job_name, self.batch_service_client):
                logger.info(f"Job {job_name} completed.")
                return "complete"
            else:
                j_state = batch_helpers.get_job_state(
                    job_name, self.batch_service_client
                )
                logger.info(f"Job in {j_state} state")
                return j_state
        else:
            logger.info(f"Job {job_name} does not exist.")
            return "does not exist"

    def delete_job(self, job_name: str) -> None:
        """Delete an Azure Batch job and all its associated tasks.

        Permanently removes a job from the Batch account. This operation also deletes
        all tasks associated with the job and any stored task execution data.

        Args:
            job_name (str): Name/ID of the job to delete. The job must exist.

        Raises:
            RuntimeError: If the job deletion fails due to Azure Batch service errors
                or if the job does not exist.

        Example:
            Delete a completed job:

                client = CloudClient()
                client.delete_job("completed-job")

            Clean up multiple jobs:

                job_names = ["old-job-1", "old-job-2", "failed-job"]
                for job_name in job_names:
                    try:
                        client.delete_job(job_name)
                        print(f"Deleted {job_name}")
                    except RuntimeError as e:
                        print(f"Failed to delete {job_name}: {e}")

        Warning:
            This operation is irreversible. All task data, logs, and job metadata
            will be permanently lost. Ensure you have downloaded any needed outputs
            or logs before deleting the job.
        """
        logger.debug(f"Attempting to delete {job_name}.")
        self.batch_service_client.job.delete(job_name)
        logger.info(f"Job {job_name} deleted.")

    def delete_job_schedule(self, job_schedule_id: str) -> None:
        """Delete an Azure Batch job schedule.

        Permanently removes a job schedule from the Batch account.

        Args:
            job_schedule_id (str): Name/ID of the job schedule to delete. The job schedule must exist.

        Raises:
            RuntimeError: If the job schedule deletion fails due to Azure Batch service errors
                or if the job schedule does not exist.

        Example:
            Delete a completed job chedule:

                client = CloudClient()
                client.delete_job_schedule("my-job-schedule")

        Warning:
            This operation is irreversible.
        """
        logger.debug(f"Attempting to delete schedule {job_schedule_id}.")
        self.batch_service_client.job_schedule.delete(job_schedule_id)
        logger.info(f"Job schedule {job_schedule_id} deleted.")

    def resume_job_schedule(self, job_schedule_id: str) -> None:
        """Resumes a suspended Azure Batch job schedule.

        Enables a job schedule in the Batch account.

        Args:
            job_schedule_id (str): Name/ID of the job schedule to resume. The job schedule must exist.

        Raises:
            RuntimeError: If the job schedule suspension fails due to Azure Batch service errors
                or if the job schedule does not exist.

        Example:
            Delete a completed job chedule:

                client = CloudClient()
                client.resume_job_schedule("my-job-schedule")
        """
        logger.debug(f"Attempting to resume schedule {job_schedule_id}.")
        self.batch_service_client.job_schedule.enable(job_schedule_id)
        logger.info(f"Job schedule {job_schedule_id} resumed.")

    def suspend_job_schedule(self, job_schedule_id: str) -> None:
        """Suspends an active Azure Batch job schedule until it is resumed.

        Disables a job schedule in the Batch account.

        Args:
            job_schedule_id (str): Name/ID of the job schedule to suspend. The job schedule must exist.

        Raises:
            RuntimeError: If the job schedule suspension fails due to Azure Batch service errors
                or if the job schedule does not exist.

        Example:
            Delete a completed job chedule:

                client = CloudClient()
                client.suspend_job_schedule("my-job-schedule")
        """
        logger.debug(f"Attempting to suspend schedule {job_schedule_id}.")
        self.batch_service_client.job_schedule.disable(job_schedule_id)
        logger.info(f"Job schedule {job_schedule_id} suspended.")

    def package_and_upload_dockerfile(
        self,
        registry_name: str,
        repo_name: str,
        tag: str,
        path_to_dockerfile: str = "./Dockerfile",
        use_device_code: bool = False,
    ) -> str:
        """Build a Docker image from a Dockerfile and upload it to Azure Container Registry.

        Takes a Dockerfile, builds it into a Docker image, and uploads the resulting
        image to the specified Azure Container Registry. This is useful for creating
        custom container images for Azure Batch tasks.

        Args:
            registry_name (str): Name of the Azure Container Registry (without .azurecr.io).
                The registry must already exist and be accessible.
            repo_name (str): Name of the repository within the container registry where
                the image will be stored.
            tag (str): Tag to assign to the uploaded Docker image (e.g., "latest", "v1.0").
            path_to_dockerfile (str, optional): Path to the Dockerfile to build. Can be
                relative or absolute. Default is "./Dockerfile" (Dockerfile in current directory).
            use_device_code (bool, optional): Whether to use device code authentication
                for Azure CLI login during the upload process. Useful for environments
                without a web browser. Default is False.

        Returns:
            str: Full container image name that was uploaded, in the format
                "registry.azurecr.io/repo:tag".

        Example:
            Build and upload from default Dockerfile:

                client = CloudClient()
                image_name = client.package_and_upload_dockerfile(
                    registry_name="myregistry",
                    repo_name="batch-app",
                    tag="v1.0"
                )
                print(f"Uploaded: {image_name}")

            Build from custom Dockerfile location:

                image_name = client.package_and_upload_dockerfile(
                    registry_name="myregistry",
                    repo_name="data-processor",
                    tag="latest",
                    path_to_dockerfile="./docker/worker/Dockerfile",
                    use_device_code=True
                )

        Note:
            This method requires Docker to be installed and the Azure CLI to be
            available and authenticated. The resulting image name is stored in
            self.full_container_name for later use.
        """
        self.full_container_name = helpers.package_and_upload_dockerfile(
            registry_name, repo_name, tag, path_to_dockerfile, use_device_code
        )
        logger.debug("Completed package_and_upload_dockerfile() function.")
        self.container_registry_server = f"{registry_name}.azurecr.io"
        self.registry_url = f"https://{self.container_registry_server}"
        self.container_image_name = f"https://{self.full_container_name}"
        return self.full_container_name

    def upload_docker_image(
        self,
        image_name: str,
        registry_name: str,
        repo_name: str,
        tag: str,
        use_device_code: bool = False,
    ) -> str:
        """Upload an existing Docker image to Azure Container Registry.

        Takes a Docker image that already exists locally and uploads it to the specified
        Azure Container Registry. This is useful when you have pre-built images that
        you want to use for Azure Batch tasks.

        Args:
            image_name (str): Name of the local Docker image to upload. Should be the
                full image name as it appears in "docker images" output.
            registry_name (str): Name of the Azure Container Registry (without .azurecr.io).
                The registry must already exist and be accessible.
            repo_name (str): Name of the repository within the container registry where
                the image will be stored.
            tag (str): Tag to assign to the uploaded Docker image (e.g., "latest", "v1.0").
            use_device_code (bool, optional): Whether to use device code authentication
                for Azure CLI login during the upload process. Useful for environments
                without a web browser. Default is False.

        Returns:
            str: Full container image name that was uploaded, in the format
                "registry.azurecr.io/repo:tag".

        Example:
            Upload a locally built image:

                client = CloudClient()
                image_name = client.upload_docker_image(
                    image_name="my-local-app:latest",
                    registry_name="myregistry",
                    repo_name="batch-app",
                    tag="v1.0"
                )

            Upload with device code authentication:

                image_name = client.upload_docker_image(
                    image_name="data-processor:dev",
                    registry_name="myregistry",
                    repo_name="processors",
                    tag="development",
                    use_device_code=True
                )

        Note:
            This method requires Docker to be installed and the Azure CLI to be
            available and authenticated. The local image must exist before calling
            this method. The resulting image name is stored in self.full_container_name.
        """
        self.full_container_name = helpers.upload_docker_image(
            image_name, registry_name, repo_name, tag, use_device_code
        )
        logger.debug("Completed package_and_upload_docker_image() function.")
        self.container_registry_server = f"{registry_name}.azurecr.io"
        self.registry_url = f"https://{self.container_registry_server}"
        self.container_image_name = f"https://{self.full_container_name}"
        return self.full_container_name

    def download_file(
        self,
        src_path: str,
        dest_path: str,
        container_name: str = None,
        do_check: bool = True,
        check_size: bool = True,
    ) -> None:
        """Download a single file from Azure Blob Storage to the local filesystem.

        Downloads a file from a blob storage container to a local destination path.
        Supports verification of the download to ensure data integrity.

        Args:
            src_path (str): Path of the file within the blob container to download.
                Should be the full blob path including any directory structure.
            dest_path (str): Local filesystem path where the file should be saved.
                Can be relative or absolute. Parent directories will be created if needed.
            container_name (str, optional): Name of the blob storage container containing
                the file. If None, uses the default container associated with the client.
            do_check (bool, optional): Whether to perform verification checks after
                download. Default is True.
            check_size (bool, optional): Whether to verify that the downloaded file
                size matches the source file size. Only used if do_check is True.
                Default is True.

        Example:
            Download a file with default settings:

                client = CloudClient()
                client.download_file(
                    src_path="data/results.csv",
                    dest_path="./local_results.csv",
                    container_name="job-outputs"
                )

            Download without verification:

                client.download_file(
                    src_path="logs/job.log",
                    dest_path="/tmp/job.log",
                    container_name="job-logs",
                    do_check=False
                )

        Note:
            If the destination directory doesn't exist, it will be created automatically.
            The download will overwrite any existing file at the destination path.
        """
        # use the output container client by default for downloading files
        logger.debug(f"Creating container client for {container_name}.")
        c_client = self.blob_service_client.get_container_client(
            container=container_name
        )

        logger.debug("Attempting to download file.")
        blob_helpers.download_file(c_client, src_path, dest_path, do_check, check_size)

    def download_folder(
        self,
        src_path: str,
        dest_path: str,
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        verbose=True,
        check_size=True,
    ) -> None:
        """Download an entire folder from Azure Blob Storage to the local filesystem.

        Recursively downloads all files from a directory in a blob storage container,
        preserving the directory structure. Supports filtering by file extensions.

        Args:
            src_path (str): Path of the directory within the blob container to download.
                Should be the directory path within the container (e.g., "data/outputs").
            dest_path (str): Local filesystem path where the directory should be saved.
                The directory structure will be recreated under this path.
            container_name (str): Name of the blob storage container containing the directory.
            include_extensions (str | list, optional): File extensions to include in the
                download. Can be a single extension string (e.g., ".csv") or list of
                extensions (e.g., [".csv", ".json"]). If None, all files are included.
            exclude_extensions (str | list, optional): File extensions to exclude from
                the download. Can be a single extension string or list. Takes precedence
                over include_extensions if a file matches both.
            verbose (bool, optional): Whether to print progress information during
                download. Default is True.
            check_size (bool, optional): Whether to verify that downloaded file sizes
                match the source file sizes. Default is True.

        Example:
            Download entire results directory:

                client = CloudClient()
                client.download_folder(
                    src_path="job-123/outputs",
                    dest_path="./results",
                    container_name="job-outputs"
                )

            Download only specific file types:

                client.download_folder(
                    src_path="logs",
                    dest_path="./local_logs",
                    container_name="job-logs",
                    include_extensions=[".log", ".txt"],
                    verbose=False
                )

        Note:
            The destination folder will be created if it doesn't exist. The source
            folder structure is preserved in the destination. Large downloads may
            take considerable time depending on file sizes and network speed.
        """
        logger.debug("Attempting to download folder.")
        blob_helpers.download_folder(
            container_name,
            src_path,
            dest_path,
            self.blob_service_client,
            include_extensions,
            exclude_extensions,
            verbose,
            check_size,
        )
        logger.debug("finished call to download")

    def async_download_folder(
        self,
        src_path: str,
        dest_path: str,
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        check_size=True,
        max_concurrent_downloads: int = 20,
    ) -> None:
        """Download an entire folder from Azure Blob Storage to the local filesystem.

        Recursively downloads all files from a directory in a blob storage container,
        preserving the directory structure. Supports filtering by file extensions.

        Args:
            src_path (str): Path of the directory within the blob container to download.
                Should be the directory path within the container (e.g., "data/outputs").
            dest_path (str): Local filesystem path where the directory should be saved.
                The directory structure will be recreated under this path.
            container_name (str): Name of the blob storage container containing the directory.
            include_extensions (str | list, optional): File extensions to include in the
                download. Can be a single extension string (e.g., ".csv") or list of
                extensions (e.g., [".csv", ".json"]). If None, all files are included.
            exclude_extensions (str | list, optional): File extensions to exclude from
                the download. Can be a single extension string or list. Takes precedence
                over include_extensions if a file matches both.
            check_size (bool, optional): Whether to verify that downloaded file sizes
                match the source file sizes. Default is True.
            max_concurrent_downloads (int, optional): Maximum number of concurrent
                downloads to perform. Higher values may increase speed but use more RAM.
                Default is 20.

        Example:
            Download entire results directory:

                client = CloudClient()
                client.async_download_folder(
                    src_path="job-123/outputs",
                    dest_path="./results",
                    container_name="job-outputs"
                )

            Download only specific file types:

                client.async_download_folder(
                    src_path="logs",
                    dest_path="./local_logs",
                    container_name="job-logs",
                    include_extensions=[".log", ".txt"],
                )

        Note:
            The destination folder will be created if it doesn't exist. The source
            folder structure is preserved in the destination. Large downloads may
            take considerable time depending on file sizes and network speed.
        """
        logger.debug("Attempting to download folder.")
        if self.method == "default":
            cred = self.cred.client_secret_sp_credential
        elif self.method == "sp":
            cred = self.cred.client_secret_credential
        else:
            cred = self.cred.user_credential
        blob.async_download_blob_folder(
            container_name=container_name,
            local_folder=dest_path,
            name_starts_with=src_path,
            storage_account_url=self.cred.azure_blob_storage_endpoint,
            include_extensions=include_extensions,
            exclude_extensions=exclude_extensions,
            check_size=check_size,
            max_concurrent_downloads=max_concurrent_downloads,
            credential=cred,
        )
        logger.debug("finished call to download")

    def async_upload_folder(
        self,
        folders: str | list[str],
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        location_in_blob: str = ".",
        max_concurrent_uploads: int = 20,
    ):
        """Upload entire folders to an Azure Blob Storage container asynchronously.

        Recursively uploads all files from specified folders to a blob storage container
        using asynchronous operations for improved performance. Supports filtering by
        file extensions and patterns to control which files are uploaded.

        Args:
            folders (str | list[str]): List of local folder paths to upload. Each folder
            container_name (str): Name of the blob storage container to upload to. The
                container must already exist.
            include_extensions (str | list, optional): File extensions to include in the
                upload. Can be a single extension string (e.g., ".py") or list of extensions
                (e.g., [".py", ".txt"]). If None, all extensions are included.
            exclude_extensions (str | list, optional): File extensions to exclude from
                the upload. Can be a single extension string or list.
            location_in_blob (str, optional): Remote directory path within the blob container
                where folders should be uploaded. Default is "." (container root).
            max_concurrent_uploads (int, optional): Maximum number of concurrent
                uploads to perform. Higher values may increase speed but use more RAM.

        Returns:
            list[str]: List of file paths that were successfully uploaded to the container.

        Example:
            Upload Python source folders asynchronously:
                client = CloudClient()
                uploaded_files = client.async_upload_folder(
                    folders=["src", "tests"],
                    container_name="code-repo",
                    include_extensions=[".py", ".yaml"],
                    location_in_blob="project")

        Note:
            The blob container must exist before uploading. Directory structure is
            preserved in the container. Use filtering options to avoid uploading
            unnecessary files like temporary files or build artifacts.
        """
        logger.debug("Attempting to upload folder(s).")
        if self.method == "default":
            cred = self.cred.client_secret_sp_credential
        elif self.method == "sp":
            cred = self.cred.client_secret_credential
        else:
            cred = self.cred.user_credential
        if isinstance(folders, str):
            folders = [folders]
        for folder in folders:
            logger.debug(f"Trying to upload folder {folder}.")
            blob.async_upload_folder(
                folder=folder,
                container_name=container_name,
                storage_account_url=self.cred.azure_blob_storage_endpoint,
                include_extensions=include_extensions,
                exclude_extensions=exclude_extensions,
                location_in_blob=location_in_blob,
                max_concurrent_uploads=max_concurrent_uploads,
                credential=cred,
            )

    def delete_pool(self, pool_name: str) -> None:
        """Delete an Azure Batch pool and all its compute nodes.

        Permanently removes a pool from the Batch account. This operation stops all
        running tasks on the pool's nodes and deallocates all compute resources.

        Args:
            pool_name (str): Name of the pool to delete. The pool must exist.

        Raises:
            RuntimeError: If the pool deletion fails due to Azure Batch service errors
                or if the pool does not exist.

        Example:
            Delete a completed pool:

                client = CloudClient()
                client.delete_pool("old-compute-pool")

            Clean up test pools:

                test_pools = ["test-pool-1", "test-pool-2"]
                for pool_name in test_pools:
                    try:
                        client.delete_pool(pool_name)
                        print(f"Deleted pool: {pool_name}")
                    except RuntimeError as e:
                        print(f"Failed to delete {pool_name}: {e}")

        Warning:
            This operation is irreversible and will terminate any running tasks.
            Ensure all important work is complete before deleting the pool.
            Pool deletion may take several minutes to complete.
        """
        batch_helpers.delete_pool(
            resource_group_name=self.cred.azure_resource_group_name,
            account_name=self.cred.azure_batch_account,
            pool_name=pool_name,
            batch_mgmt_client=self.batch_mgmt_client,
        )

    def list_blob_files(self, blob_container: str = None):
        """List all files in blob storage containers associated with the client.

        Retrieves a list of all blob files from either a specified container or from
        all containers associated with the client's mounts. This is useful for
        discovering available data files before processing.

        Args:
            blob_container (str, optional): Name of a specific blob storage container
                to list files from. If None, will list files from all containers
                in the client's mounts. Default is None.

        Returns:
            list[str] | None: List of blob file paths found in the container(s).
                Returns None if no container is specified and no mounts are configured.

        Example:
            List files from a specific container:

                client = CloudClient()
                files = client.list_blob_files("input-data")
                print(f"Found {len(files)} files: {files}")

            List files from all mounted containers:

                files = client.list_blob_files()
                if files:
                    print(f"Total files across all mounts: {len(files)}")

        Note:
            Either blob_container must be specified or the client must have mounts
            configured. If neither condition is met, a warning is logged and None
            is returned.
        """
        if blob_container:
            logger.debug(f"Listing blobs in {blob_container}")
            filenames = blob_helpers.list_blobs_flat(
                container_name=blob_container,
                blob_service_client=self.blob_service_client,
                verbose=False,
            )
        elif self.mounts:
            logger.debug("Looping through mounts.")
            filenames = []
            for mount in self.mounts:
                _files = blob_helpers.list_blobs_flat(
                    container_name=mount[0],
                    blob_service_client=self.blob_service_client,
                    verbose=False,
                )
                filenames += _files
        return filenames

    def delete_blob_file(self, blob_name: str, container_name: str):
        """Delete a specific file from Azure Blob Storage.

        Permanently removes a file and all its snapshots from the specified blob
        storage container. This operation cannot be undone.

        Args:
            blob_name (str): Name/path of the blob file to delete within the container.
                Should include any directory structure (e.g., "data/file.txt").
            container_name (str): Name of the blob storage container containing the file.

        Example:
            Delete a specific output file:

                client = CloudClient()
                client.delete_blob_file(
                    blob_name="results/output.csv",
                    container_name="job-outputs"
                )

            Delete a log file:

                client.delete_blob_file(
                    blob_name="logs/job-123.log",
                    container_name="system-logs"
                )

        Warning:
            This operation permanently deletes the file and all its snapshots.
            Ensure you have backed up any important data before deletion.
        """
        logger.debug(f"Deleting blob {blob_name} from {container_name}.")
        blob_helpers.delete_blob_snapshots(
            blob_name, container_name, self.blob_service_client
        )
        logger.debug(f"Deleted {blob_name}.")

    def delete_blob_folder(self, folder_path: str, container_name: str):
        """Delete an entire folder and all its contents from Azure Blob Storage.

        Recursively removes all files within the specified folder path from the blob
        storage container. This operation deletes all files that have the folder path
        as a prefix in their blob names.

        Args:
            folder_path (str): Path of the folder to delete within the container.
                Should be the folder prefix (e.g., "data/temp" will delete all blobs
                starting with "data/temp/").
            container_name (str): Name of the blob storage container containing the folder.

        Example:
            Delete a temporary data folder:

                client = CloudClient()
                client.delete_blob_folder(
                    folder_path="temp/job-123",
                    container_name="workspace"
                )

            Delete all log files from a specific run:

                client.delete_blob_folder(
                    folder_path="logs/2024-01-15",
                    container_name="system-logs"
                )

        Warning:
            This operation permanently deletes all files within the specified folder.
            There is no way to recover deleted files. Ensure you have backed up any
            important data before deletion.
        """
        logger.debug(f"Deleting files in {folder_path} folder.")
        blob_helpers.delete_blob_folder(
            folder_path, container_name, self.blob_service_client
        )
        logger.debug(f"Deleted folder {folder_path}.")

    def download_job_stats(self, job_name: str, file_name: str | None = None):
        """Download job statistics for a completed Azure Batch job.

        Downloads detailed statistics for all tasks in the specified job and saves them
        to a CSV file. The statistics include task execution times, exit codes, and node info.

        Args:
            job_name (str): Name of the job to download statistics for. The job must exist.
            file_name (str, optional): Name of the output CSV file (without extension).
                If None, defaults to "{job_name}-stats.csv".

        Example:
            Download stats for a job:

                client = CloudClient()
                client.download_job_stats(job_name="my-job")

            Download with custom filename:

                client.download_job_stats(job_name="my-job", file_name="run42_stats")

        Note:
            The CSV file will be created in the current working directory. The job must
            be completed before statistics are available for all tasks.
        """
        batch_helpers.download_job_stats(
            job_name=job_name,
            batch_service_client=self.batch_service_client,
            file_name=file_name,
        )

    def add_tasks_from_yaml(
        self, job_name: str, base_cmd: str, file_path: str, **kwargs
    ) -> list[str]:
        """Add multiple tasks to a job from a YAML file.

        Reads a YAML file describing tasks, constructs the corresponding commands, and
        submits each as a task to the specified job. Returns the list of created task IDs.

        Args:
            job_name (str): ID of the job to add tasks to. The job must exist.
            base_cmd (str): Base command to prepend to each task command from the YAML file.
            file_path (str): Path to the YAML file describing the tasks.
            **kwargs: Additional keyword arguments passed to add_task().

        Returns:
            list[str]: List of task IDs created from the YAML file.

        Example:
            Add tasks from a YAML file:

                client = CloudClient()
                task_ids = client.add_tasks_from_yaml(
                    job_name="my-job",
                    base_cmd="python run.py",
                    file_path="tasks.yaml"
                )
                print(f"Added {len(task_ids)} tasks from YAML.")

        Note:
            The YAML file should define the commands or parameters for each task. The
            base_cmd is prepended to each command from the YAML file.
        """
        # get tasks from yaml
        task_strs = batch_helpers.get_tasks_from_yaml(
            base_cmd=base_cmd, file_path=file_path
        )
        # submit tasks
        task_list = []
        for task_str in task_strs:
            tid = self.add_task(job_name=job_name, command_line=task_str, **kwargs)
            task_list.append(tid)
        return task_list

    def download_after_job(
        self,
        job_name: str,
        blob_paths: list[str],
        target: str,
        container_name: str,
        **kwargs,
    ):
        """Download files or directories from blob storage after a job completes.

        Waits for the specified job to complete, then downloads the specified files or
        directories from blob storage to a local target directory. Handles both single
        files and directories.

        Args:
            job_name (str): Name/ID of the job to monitor for completion.
            blob_paths (list[str]): List of blob paths (files or directories) to download.
            target (str): Local directory where files/directories will be downloaded.
            container_name (str): Name of the blob storage container containing the files.
            **kwargs: Additional keyword arguments passed to download_folder().

        Example:
            Download results after job completion:

                client = CloudClient()
                client.download_after_job(
                    job_name="my-job",
                    blob_paths=["results/output.csv", "logs/"],
                    target="./outputs",
                    container_name="job-outputs"
                )

        Note:
            This method blocks until the job completes. Files are downloaded to the
            specified target directory, preserving directory structure for folders.
        """
        # check job for completion
        batch_helpers.monitor_tasks(
            job_name=job_name,
            timeout=None,
            batch_client=self.batch_service_client,
        )

        # loop through blob_paths:
        os.makedirs(target, exist_ok=True)

        for path in blob_paths:
            if "." in path:
                self.download_file(
                    src_path=path,
                    dest_path=os.path.join(target, path),
                    container_name=container_name,
                )
            else:
                self.download_folder(
                    src_path=path,
                    dest_path=os.path.join(target),
                    container_name=container_name,
                    **kwargs,
                )

    def run_dag(self, *args: batch_helpers.Task, job_name: str, **kwargs):
        """Run a set of tasks as a directed acyclic graph (DAG) in the correct order.

        Accepts multiple Task objects, determines their execution order using topological
        sorting, and submits them to Azure Batch as a dependency graph. Raises an error
        if the tasks do not form a valid DAG.

        Args:
            *args: batch_helpers.Task objects representing tasks and their dependencies.
            job_name (str): Name/ID of the job to add tasks to.
            **kwargs: Additional keyword arguments passed to add_task().

        Raises:
            CycleError: If the submitted tasks do not form a valid DAG (contain cycles).

        Example:
            Run a DAG of tasks:

                client = CloudClient()
                client.create_job("dag_job", pool_name = "test_pool")
                t1 = Task("python step1.py")
                t2 = Task("python step2.py")
                t3 = Task("python step3.py")
                t4 = Task("python step4.py")
                t2.after(t1)
                t3.after(t1)
                t4.after([t2, t3])
                client.run_dag(t1, t2, t3, t4, job_name="dag_job")

        Note:
            The tasks must form a valid DAG (no cycles). Task dependencies are resolved
            automatically and tasks are submitted in the correct order. Task IDs and
            dependencies are updated as tasks are submitted.
        """
        # get topologicalsorter opject
        ts = TopologicalSorter()
        tasks = args
        for task in tasks:
            ts.add(task, *task.deps)
        try:
            task_order = [*ts.static_order()]
        except CycleError as ce:
            logger.warn("Submitted tasks do not form a DAG.")
            raise ce
        task_df = pd.DataFrame(columns=["id", "cmd", "deps"])
        # initialize df for task execution
        for i, task in enumerate(task_order):
            task_df.loc[i] = [task.id, task.cmd, task.deps]
        for task in task_order:
            tid = self.add_task(
                job_name=job_name,
                command_line=task.cmd,
                depends_on=task_df[task_df["id"] == task.id]["deps"].values[0],
                **kwargs,
            )
            for i, dep in enumerate(task_df["deps"]):
                dlist = []
                for dp in dep:
                    if str(dp) == str(task.id):
                        dlist.append(tid)
                    else:
                        dlist.append(str(dp))
                task_df.at[i, "deps"] = dlist
