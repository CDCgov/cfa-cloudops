import datetime
import logging
import os
import subprocess as sp
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

import docker
import pandas as pd

from cfa.cloudops.local import batch, helpers

logger = logging.getLogger(__name__)


class CloudClient:
    def __init__(
        self,
        dotenv_path: str = None,
        use_sp=False,
        use_federated=False,
        **kwargs,
    ):
        """
        Initialize a CloudClient instance for managing Azure Batch, Blob Storage, and Docker operations.

        Args:
            dotenv_path (str, optional): Path to dotenv file for environment variables.
            use_sp (bool, optional): Use service principal authentication.
            use_federated (bool, optional): Use federated authentication.
            **kwargs: Additional keyword arguments for future extensibility.
        """
        # authenticate to get credentials
        if not use_sp and not use_federated:
            self.cred = "envcred"
        if use_federated:
            self.cred = "fedcred"
        else:
            self.cred = "spcred"
        # get clients

        self.batch_mgmt_client = "bmc"
        self.compute_mgmt_client = "cmc"
        self.batch_service_client = "batchsc"
        self.blob_service_client = "blobsc"
        self.full_container_name = None
        self.save_logs_to_blob = None
        self.logs_folder = "stdout_stderr"
        self.task_id_ints = False
        self.task_id_max = 0
        self.jobs = set()

    def create_pool(
        self,
        pool_name: str,
        mounts=None,
        container_image_name=None,
        vm_size="standard_d3s_v3",  # do some validation on size if too large
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
        start_time = datetime.datetime.now()
        # Configure storage mounts if provided
        if mounts is not None:
            storage_containers = []
            mount_names = []
            for mount in mounts:
                storage_containers.append(mount[0])
                mount_names.append(mount[1])

        # validate pool name
        pool_name = pool_name.replace(" ", "_")

        # validate vm size
        print("Verify the size of the VM is appropriate for the use case.")
        print("**Please use smaller VMs for dev/testing.**")

        # Configure container if image is provided
        if container_image_name:
            pass
        else:
            raise ValueError("container_image_name not provided.")

        # Configure availability zones in the virtual machine configuration
        # Set node placement configuration for zonal deployment
        if availability_zones.lower() == "regional":
            pass
        elif availability_zones.lower() == "zonal":
            pass
        else:
            raise ValueError(
                "Availability zone needs to be 'zonal' or 'regional'."
            )

        # see if docker is running
        try:
            docker_env = docker.from_env(timeout=8)
            docker_env.ping()
        except Exception:
            print("can't find docker daemon")
            return None
        # check if image exists
        try:
            docker_env.images.get(self.full_container_name)
        except docker.errors.NotFound:
            print(
                f"image not found... make sure image {self.full_container_name} exists."
            )
            return None

        print("Verify the size of the VM is appropriate for the use case.")
        print("**Please use smaller VMs for dev/testing.**")
        try:
            self.pool = batch.Pool(pool_name, self.cont_name)
            logger.info(f"Pool {pool_name!r} created.")
        except Exception:
            logger.warning(f"Pool {pool_name!r} already exists")

        # get mnt string
        mount_str = ""
        if mounts is not None:
            for mount in mounts:
                mount_str = (
                    mount_str
                    + " --mount type=bind,source="
                    + os.path.abspath(mount[0])
                    + f",target=/{mount[1]}"
                )
        # format pool info to save
        pool_info = {
            "image_name": self.full_container_name,
            "mount_str": mount_str,
        }
        # save pool info
        os.makedirs("tmp/pools", exist_ok=True)
        save_path = Path(f"tmp/pools/{pool_name}.txt")
        save_path.write_text(str(pool_info))
        end_time = datetime.datetime.now()
        return {
            "pool_id": pool_name,
            "creation_time": round((end_time - start_time).total_seconds(), 2),
        }

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
        logger.debug(f"job_id: {job_name}")

        if pool_name:
            self.pool_name = pool_name
        elif self.pool_name:
            pool_name = self.pool_name
        else:
            logger.error("Please specify a pool for the job and try again.")
            raise Exception("Please specify a pool for the job and try again.")

        # check pool exists:
        if not os.path.exists(f"tmp/pools/{pool_name}.txt"):
            print(f"Pool {pool_name} does not exist.")
            return None
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

        if task_id_ints:
            self.task_id_ints = True
            self.task_id_max = 0
        else:
            self.task_id_ints = False

        # check image for pool exists
        p_path = Path(f"tmp/pools/{pool_name}.txt")
        pool_info = eval(p_path.read_text())
        image_name = pool_info["image_name"]
        mount_str = pool_info["mount_str"]
        # check if exists:
        dock_env = docker.from_env()
        try:
            d = dock_env.images.get(image_name)
            print(d.short_id)
        except Exception:
            print(f"Container {image_name} for pool could not be found.")

        # add the job to the pool
        logger.debug(f"Attempting to add job {job_name}.")
        helpers.add_job(
            job_id=job_name,
            pool_id=pool_name,
            task_retries=task_retries,
            mark_complete=mark_complete_after_tasks_run,
        )
        # get container name and run infinitely
        self.cont_name = (
            image_name.replace("/", "_").replace(":", "_") + f".{job_name}"
        )
        sp.run(
            f"docker run -d --rm {mount_str} --name {self.cont_name} {image_name} sleep inf",
            shell=True,
        )

        self.jobs.add(job_name)

    def add_task(
        self,
        job_name: str,
        command_line: list[str],
        name_suffix: str = "",
        depends_on: list[str] | None = None,
        depends_on_range: tuple | None = None,
        run_dependent_tasks_on_fail: bool = False,
    ):
        """
        Add a task to an Azure Batch job.

        Args:
            job_name (str): Name of the job to add the task to.
            command_line (list[str]): Command line arguments for the task.
            name_suffix (str, optional): Suffix to append to the task ID.
            depends_on (list[str], optional): List of task IDs this task depends on.
            depends_on_range (tuple, optional): Range of task IDs this task depends on.
            run_dependent_tasks_on_fail (bool, optional): Whether to run dependent tasks if this task fails.
            container_image_name (str, optional): Container image to use for the task.
            timeout (int, optional): Maximum time in minutes for the task to run.

        Returns:
            str: The ID of the added task.
        """
        # run tasks for input files
        logger.debug("Adding task to job.")
        task_id = self.task_id_max
        print(f"Running {task_id}.")
        sp.run(
            f"""docker exec -i {self.cont_name} {command_line}""", shell=True
        )

        self.task_id_max += 1
        return task_id

    def create_blob_container(self, name: str):
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
        helpers.create_container(name)

    def upload_files(
        self,
        files: str | list[str],
        container_name: str,
        local_root_dir: str = ".",
        location_in_blob: str = ".",
    ):
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
        for file_name in files:
            helpers.upload_to_storage_container(
                filepath=file_name,
                location=location_in_blob,
                container_name=container_name,
                verbose=False,
            )
            logger.debug("Finished running helpers.upload_blob_file().")
        logger.debug("Uploaded all files in files list.")

    def upload_folders(
        self,
        folder_names: list[str],
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        exclude_patterns: str | list | None = None,
        location_in_blob: str = ".",
        force_upload: bool = False,
    ):
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
        for _folder in folder_names:
            logger.debug(f"Trying to upload folder {_folder}.")
            _uploaded_files = helpers.upload_folder(
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
    ):
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
        pass

    def check_job_status(self, job_name: str):
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
        job_path = Path(f"tmp/jobs/{job_name}.txt")

        if job_path.exists():
            return "exists"
        else:
            logger.info(f"Job {job_name} does not exist.")
            return "does not exist"

    def delete_job(self, job_id: str):
        """
        Delete a job and its associated Docker container.

        Args:
            job_id (str): ID of the job to delete.

        Example:
            Delete a job and stop its container:

                client = CloudClient()
                client.delete_job("my-job-id")

        Note:
            This operation will remove the job record and stop the related Docker container.
        """
        # delete the file
        job_id_r = job_id.replace(" ", "")
        os.remove(f"tmp/jobs/{job_id_r}.txt")

        # delete the container
        sp.run(f"docker stop {self.cont_name}", shell=True)

    def package_and_upload_dockerfile(
        self,
        registry_name: str,
        repo_name: str,
        tag: str,
        path_to_dockerfile: str = "./Dockerfile",
        use_device_code: bool = False,
    ):
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
    ):
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
    ):
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
        logger.debug("Attempting to download file.")
        c_client = None
        helpers.download_file(
            c_client, src_path, dest_path, do_check, check_size
        )

    def download_folder(
        self,
        src_path: str,
        dest_path: str,
        container_name: str,
        include_extensions: str | list | None = None,
        exclude_extensions: str | list | None = None,
        verbose=True,
        check_size=True,
    ):
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
                    exclude_extensions=[".tmp"],
                    verbose=False
                )

        Note:
            The destination folder will be created if it doesn't exist. The source
            folder structure is preserved in the destination. Large downloads may
            take considerable time depending on file sizes and network speed.
        """
        logger.debug("Attempting to download folder.")
        helpers.download_folder(
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

    def delete_pool(self, pool_name: str):
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
        pool_id_r = pool_name.replace(" ", "")
        os.remove(f"tmp/pools/{pool_id_r}.txt")

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
            filenames = os.listdir(blob_container)
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
        os.remove(f"{container_name}/{blob_name}")
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
        os.remove(f"{container_name}/{folder_path}")
        logger.debug(f"Deleted folder {folder_path}.")

    def download_job_stats(self, job_name: str, file_name: str | None = None):
        """Download job statistics for a completed Azure Batch job.

        Downloads detailed statistics for all tasks in the specified job and saves them
        to a CSV file. The statistics include task execution times, exit codes, and node info.

        Args:
            job_id (str): ID of the job to download statistics for. The job must exist.
            file_name (str, optional): Name of the output CSV file (without extension).
                If None, defaults to "{job_id}-stats.csv".

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
        pass

    def add_tasks_from_yaml(
        self, job_name: str, base_cmd: str, file_path: str, **kwargs
    ):
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
        task_strs = helpers.get_tasks_from_yaml(
            base_cmd=base_cmd, file_path=file_path
        )
        # submit tasks
        task_list = []
        for task_str in task_strs:
            tid = self.add_task(
                job_name=job_name, command_line=task_str, **kwargs
            )
            task_list.append(tid)
        return task_list

    def run_dag(self, *args: batch.Task, job_name: str, **kwargs):
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
