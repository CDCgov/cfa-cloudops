<<<<<<< HEAD
import argparse
import textwrap

from cfa.cloudops import CloudClient


def hello():
    parser = argparse.ArgumentParser(description="CloudOps parser")
    parser.add_argument("--name", type=str, default="World", help="Name to greet")
    args = parser.parse_args()
    print(f"Hello, {args.name}!")


def create_pool():
    parser = argparse.ArgumentParser(description="Create a resource pool")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--pool_name",
        type=str,
        required=True,
        help="Name of the resource pool",
    )
    parser.add_argument(
        "-m",
        "--mounts",
        nargs="+",
        required=False,
        default=None,
        help="List of mount points",
    )
    parser.add_argument(
        "-c",
        "--container_image_name",
        type=str,
        required=True,
        help="Container image name",
    )
    parser.add_argument(
        "-v",
        "--vm_size",
        type=str,
        required=False,
        default="standard_d4s_v3",
        help="VM size",
    )
    parser.add_argument(
        "-a", "--autoscale", action="store_true", help="Enable autoscaling"
    )
    parser.add_argument(
        "-d",
        "--dedicated_nodes",
        type=int,
        default=0,
        help="Number of dedicated nodes",
    )
    parser.add_argument(
        "-l",
        "--low_priority_nodes",
        type=int,
        default=1,
        help="Number of low priority nodes",
    )
    parser.add_argument(
        "-max",
        "--max_autoscale_nodes",
        type=int,
        default=3,
        help="Maximum number of nodes for autoscaling",
    )
    parser.add_argument(
        "-t",
        "--task_slots_per_node",
        type=int,
        default=1,
        help="Task slots per node",
    )
    parser.add_argument(
        "-az",
        "--availability_zones",
        type=str,
        default="regional",
        help="Availability zones",
    )
    parser.add_argument(
        "-cache",
        "--cache_blobfuse",
        action="store_true",
        help="Enable blobfuse caching",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    if args.mounts is None:
        new_mounts = None
    else:
        new_mounts = [(m, m) for m in args.mounts]
    client.create_pool(
        pool_name=args.pool_name,
        mounts=new_mounts,
        container_image_name=args.container_image_name,
        vm_size=args.vm_size,
        autoscale=args.autoscale,
        dedicated_nodes=args.dedicated_nodes,
        low_priority_nodes=args.low_priority_nodes,
        max_autoscale_nodes=args.max_autoscale_nodes,
        task_slots_per_node=args.task_slots_per_node,
        availability_zones=args.availability_zones,
        cache_blobfuse=args.cache_blobfuse,
    )


def create_job():
    parser = argparse.ArgumentParser(description="Create a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job",
    )
    parser.add_argument(
        "-pn",
        "--pool_name",
        type=str,
        required=True,
        help="Name of the resource pool to use",
    )
    parser.add_argument(
        "-dep", "--uses_deps", action="store_true", help="Use dependencies"
    )
    parser.add_argument(
        "-s",
        "--save_logs_to_blob",
        type=str,
        default=None,
        help="Blob container to save logs",
    )
    parser.add_argument(
        "-l",
        "--logs_folder",
        type=str,
        default=None,
        help="Folder in blob container to save logs",
    )
    parser.add_argument(
        "-r",
        "--task_retries",
        type=int,
        default=0,
        help="Number of task retries on failure",
    )

    parser.add_argument(
        "-m",
        "--mark_complete",
        action="store_true",
        help="Mark job as complete after it finishes",
    )
    parser.add_argument(
        "-i",
        "--task_id_ints",
        action="store_true",
        help="Use integer task IDs",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=None,
        help="Job timeout in seconds",
    )
    parser.add_argument(
        "-e",
        "--exist_ok",
        action="store_true",
        help="If job with same name exists, do not create a new one",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.create_job(
        job_name=args.job_name,
        pool_name=args.pool_name,
        uses_deps=args.uses_deps,
        save_logs_to_blob=args.save_logs_to_blob,
        logs_folder=args.logs_folder,
        task_retries=args.task_retries,
        mark_complete_after_tasks_run=args.mark_complete,
        task_id_ints=args.task_id_ints,
        timeout=args.timeout,
        exist_ok=args.exist_ok,
        verbose=args.verbose,
    )


def add_task():
    parser = argparse.ArgumentParser(description="Add a task to a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-jn",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to add the task to",
    )
    parser.add_argument(
        "-c",
        "--command_line",
        type=str,
        required=True,
        help="Command line to execute",
    )
    parser.add_argument(
        "-n",
        "--name_suffix",
        type=str,
        default="",
        help="Suffix for the task name",
    )
    parser.add_argument(
        "-d",
        "--depends_on",
        nargs="+",
        default=None,
        help="List of task dependencies",
    )
    parser.add_argument(
        "-dr",
        "--depends_on_range",
        type=str,
        default=None,
        help="Range of task dependencies",
    )
    parser.add_argument(
        "-r",
        "--run_dependent_tasks_on_fail",
        action="store_true",
        help="Run dependent tasks even if this task fails",
    )
    parser.add_argument(
        "-ci",
        "--container_image_name",
        type=str,
        default=None,
        help="Container image name for the task",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=None,
        help="Task timeout in seconds",
    )

    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.add_task(
        job_name=args.job_name,
        command_line=args.command_line,
        name_suffix=args.name_suffix,
        depends_on=args.depends_on,
        depends_on_range=args.depends_on_range,
        run_dependent_tasks_on_fail=args.run_dependent_tasks_on_fail,
        container_image_name=args.container_image_name,
        timeout=args.timeout,
    )


def create_blob_container():
    parser = argparse.ArgumentParser(description="Create a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to create",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.create_blob_container(container_name=args.container_name)


def upload_file():
    parser = argparse.ArgumentParser(description="Upload files to a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-s",
        "--source_path",
        type=str,
        required=True,
        help="Path to the source file",
    )

    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to upload files to",
    )
    parser.add_argument(
        "-l",
        "--local_root_dir",
        type=str,
        default=".",
        required=False,
        help="Path to the local root directory",
    )
    parser.add_argument(
        "-loc",
        "--location_in_blob",
        type=str,
        default=".",
        help="Destination path in the blob container",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_files(
        files=args.source_path,
        container_name=args.container_name,
        local_root_dir=args.local_root_dir,
        location_in_blob=args.location_in_blob,
    )


def upload_folder():
    parser = argparse.ArgumentParser(description="Upload folder(s) to Blob")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--folder_name",
        type=str,
        required=True,
        help="Name of the folder to upload",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to upload folders to",
    )
    parser.add_argument(
        "-i",
        "--include_extensions",
        nargs="+",
        default=None,
        required=False,
        help="List of file extensions to include",
    )
    parser.add_argument(
        "-e",
        "--exclude_extensions",
        nargs="+",
        default=None,
        required=False,
        help="List of file extensions to exclude",
    )
    parser.add_argument(
        "-l",
        "--location_in_blob",
        type=str,
        default=".",
        help="Destination path in the blob container",
    )
    parser.add_argument(
        "-fu",
        "--force_upload",
        action="store_true",
        help="Force upload even if files exist",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_folders(
        folder_names=args.folder_name,
        container_name=args.container_name,
        include_extensions=args.include_extensions,
        exclude_extensions=args.exclude_extensions,
        location_in_blob=args.location_in_blob,
        force_upload=args.force_upload,
    )


def monitor_job():
    parser = argparse.ArgumentParser(description="Monitor a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to monitor",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds for monitoring the job",
    )
    parser.add_argument(
        "-d",
        "--download_job_stats",
        action="store_true",
        help="Download job statistics",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.monitor_job(
        job_name=args.job_name,
        timeout=args.timeout,
        download_job_stats=args.download_job_stats,
    )


def check_job_status():
    parser = argparse.ArgumentParser(description="Check job status")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to check status for",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    print(client.check_job_status(job_name=args.job_name))


def delete_job():
    parser = argparse.ArgumentParser(description="Delete a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_job(job_name=args.job_name)


def package_and_upload_dockerfile():
    parser = argparse.ArgumentParser(description="Package and upload Dockerfile")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-r",
        "--registry_name",
        type=str,
        required=True,
        help="Name of the Azure registry",
    )
    parser.add_argument(
        "-n",
        "--repo_name",
        type=str,
        required=True,
        help="Name of the repository to upload the package to",
    )
    parser.add_argument(
        "-t",
        "--tag",
        type=str,
        default=".",
        help="Tag for the container image",
    )
    parser.add_argument(
        "-d",
        "--path_to_dockerfile",
        type=str,
        required=False,
        default="./Dockerfile",
        help="Path to the Dockerfile",
    )
    parser.add_argument(
        "-u",
        "--use_device_code",
        action="store_true",
        help="Use device code for authentication",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.package_and_upload_dockerfile(
        registry_name=args.registry_name,
        repo_name=args.repo_name,
        tag=args.tag,
        path_to_dockerfile=args.path_to_dockerfile,
        use_device_code=args.use_device_code,
    )


def upload_docker_image():
    parser = argparse.ArgumentParser(
        description="Upload Docker image to Azure Container Registry"
    )
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-i",
        "--image_name",
        type=str,
        required=True,
        help="Name of the Docker image to upload",
    )
    parser.add_argument(
        "-r",
        "--registry_name",
        type=str,
        required=True,
        help="Name of the Azure Container Registry",
    )
    parser.add_argument(
        "-n",
        "--repo_name",
        type=str,
        required=True,
        help="Name of the repository to upload the image to",
    )
    parser.add_argument(
        "-t",
        "--tag",
        type=str,
        default="latest",
        help="Tag for the container image",
    )
    parser.add_argument(
        "-u",
        "--use_device_code",
        action="store_true",
        help="Use device code for authentication",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_docker_image(
        image_name=args.image_name,
        registry_name=args.registry_name,
        repo_name=args.repo_name,
        tag=args.tag,
        use_device_code=args.use_device_code,
    )


def download_file():
    parser = argparse.ArgumentParser(description="Download a file from Blob storage")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to download the file from",
    )
    parser.add_argument(
        "-b",
        "--blob_name",
        type=str,
        required=True,
        help="Name of the blob to download",
    )
    parser.add_argument(
        "-d",
        "--destination_path",
        type=str,
        required=True,
        help="Local path to save the downloaded file",
    )
    parser.add_argument(
        "-check",
        "--check_size",
        action="store_true",
        help="Check file size before downloading",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_file(
        src_path=args.blob_name,
        dest_path=args.destination_path,
        container_name=args.container_name,
        do_check=True,
        check_size=args.check_size,
    )


def download_folder():
    parser = argparse.ArgumentParser(description="Download a folder from Blob storage")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-s",
        "--src_path",
        type=str,
        required=True,
        help="source path",
    )
    parser.add_argument(
        "-d",
        "--dest_path",
        type=str,
        required=True,
        help="destination path",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to download the folder from",
    )
    parser.add_argument(
        "-i",
        "--include_extensions",
        nargs="+",
        required=False,
        default=None,
        help="List of file extensions to include",
    )
    parser.add_argument(
        "-e",
        "--exclude_extensions",
        nargs="+",
        required=False,
        default=None,
        help="List of file extensions to exclude",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "-check",
        "--check_size",
        action="store_true",
        help="Check file size before downloading",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_folder(  # type: ignore
        src_path=args.src_path,
        dest_path=args.dest_path,
        container_name=args.container_name,
        include_extensions=args.include_extensions,
        exclude_extensions=args.exclude_extensions,
        verbose=args.verbose,
        check_size=args.check_size,
    )


def delete_pool():
    parser = argparse.ArgumentParser(description="Delete a resource pool")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--pool_name",
        type=str,
        required=True,
        help="Name of the resource pool to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_pool(pool_name=args.pool_name)


def list_blob_files():
    parser = argparse.ArgumentParser(description="List files in a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to list files from",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    files = client.list_blob_files(blob_container=args.container_name)
    for file in files:
        print(file)


def delete_blob_file():
    parser = argparse.ArgumentParser(description="Delete a file from a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to delete the file from",
    )
    parser.add_argument(
        "-b",
        "--blob_name",
        type=str,
        required=True,
        help="Name of the blob to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_blob_file(
        container_name=args.container_name, blob_name=args.blob_name
    )


def delete_blob_folder():
    parser = argparse.ArgumentParser(
        description="Delete a folder from a blob container"
    )
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to delete the folder from",
    )
    parser.add_argument(
        "-b",
        "--blob_folder_name",
        type=str,
        required=True,
        help="Name of the blob folder to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_blob_folder(
        container_name=args.container_name, folder_path=args.blob_folder_name
    )


def download_job_stats():
    parser = argparse.ArgumentParser(description="Download job stats from Blob storage")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-j",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to download stats for",
    )
    parser.add_argument(
        "-path",
        "--file_name",
        type=str,
        default=None,
        required=False,
        help="path to the downloaded file",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_job_stats(job_name=args.job_name, file_name=args.file_name)


def download_after_job():
    parser = argparse.ArgumentParser(
        description="Download files from Blob storage after job completion"
    )
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-j",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to monitor and download files after completion",
    )
    parser.add_argument(
        "-b",
        "--blob_paths",
        nargs="+",
        required=True,
        help="Name of the blob to download",
    )
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        required=True,
        help="Local path to save the downloaded file",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to download the file from",
    )

    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_after_job(
        job_name=args.job_name,
        blob_paths=args.blob_paths,
        target=args.target,
        container_name=args.container_name,
    )


def add_tasks_from_yaml():
    parser = argparse.ArgumentParser(description="Add tasks to a job from a YAML file")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-j",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to add tasks to",
    )
    parser.add_argument(
        "-c",
        "--base_cmd",
        type=str,
        required=True,
        help="Base command for the tasks",
    )
    parser.add_argument(
        "-f",
        "--file_path",
        type=str,
        required=True,
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.add_tasks_from_yaml(
        job_name=args.job_name,
        base_cmd=args.base_cmd,
        file_path=args.file_path,
    )


def generate_sample_env():
    text = """
    # This file is saved as cloudops-sample.env. Rename it to .env (or your desired name) and fill in the values.

    # Azure account info
    AZURE_BATCH_ACCOUNT="your azure batch account name"
    AZURE_BATCH_LOCATION="azure batch location"
    AZURE_USER_ASSIGNED_IDENTITY="/subscriptions/xxxxxxxxx/resourcegroups/xxxxxxxx/Microsoft.ManagedIdentity/userAssignedIdentities/xxxxxxxxxx"
    AZURE_SUBNET_ID="/subscriptions/xxxxxxxx/resourceGroups/xxxxxxxx/providers/Microsoft.Network/virtualNetworks/xxxxxxxx/subnets/xxxxxxxx"
    AZURE_SP_CLIENT_ID="your sp client id"
    AZURE_KEYVAULT_NAME="your keyvault name"
    AZURE_KEYVAULT_SP_SECRET_ID="your keyvault secret id"

    # Azure Blob storage config
    AZURE_BLOB_STORAGE_ACCOUNT="your azure blob storage account"

    # Azure container registry config
    AZURE_CONTAINER_REGISTRY_ACCOUNT="your azure container registry name"
    """
    try:
        with open("cloudops-sample.env", "w") as file:
            file.write(textwrap.dedent(text).strip() + "\n")
        print("Sample .env file 'cloudops-sample.env' created successfully.")
    except Exception as e:
        print(f"Error creating sample .env file: {e}")
=======
import argparse
import textwrap

from cfa.cloudops import CloudClient


def hello():
    parser = argparse.ArgumentParser(description="CloudOps parser")
    parser.add_argument("--name", type=str, default="World", help="Name to greet")
    args = parser.parse_args()
    print(f"Hello, {args.name}!")


def create_pool():
    parser = argparse.ArgumentParser(description="Create a resource pool")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--pool_name",
        type=str,
        required=True,
        help="Name of the resource pool",
    )
    parser.add_argument(
        "-m",
        "--mounts",
        nargs="+",
        required=False,
        default=None,
        help="List of mount points",
    )
    parser.add_argument(
        "-c",
        "--container_image_name",
        type=str,
        required=True,
        help="Container image name",
    )
    parser.add_argument(
        "-v",
        "--vm_size",
        type=str,
        required=False,
        default="standard_d4s_v3",
        help="VM size",
    )
    parser.add_argument(
        "-a", "--autoscale", action="store_true", help="Enable autoscaling"
    )
    parser.add_argument(
        "-d",
        "--dedicated_nodes",
        type=int,
        default=0,
        help="Number of dedicated nodes",
    )
    parser.add_argument(
        "-l",
        "--low_priority_nodes",
        type=int,
        default=1,
        help="Number of low priority nodes",
    )
    parser.add_argument(
        "-max",
        "--max_autoscale_nodes",
        type=int,
        default=3,
        help="Maximum number of nodes for autoscaling",
    )
    parser.add_argument(
        "-t",
        "--task_slots_per_node",
        type=int,
        default=1,
        help="Task slots per node",
    )
    parser.add_argument(
        "-az",
        "--availability_zones",
        type=str,
        default="regional",
        help="Availability zones",
    )
    parser.add_argument(
        "-cache",
        "--cache_blobfuse",
        action="store_true",
        help="Enable blobfuse caching",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    if args.mounts is None:
        new_mounts = None
    else:
        new_mounts = [(m, m) for m in args.mounts]
    client.create_pool(
        pool_name=args.pool_name,
        mounts=new_mounts,
        container_image_name=args.container_image_name,
        vm_size=args.vm_size,
        autoscale=args.autoscale,
        dedicated_nodes=args.dedicated_nodes,
        low_priority_nodes=args.low_priority_nodes,
        max_autoscale_nodes=args.max_autoscale_nodes,
        task_slots_per_node=args.task_slots_per_node,
        availability_zones=args.availability_zones,
        cache_blobfuse=args.cache_blobfuse,
    )


def create_job():
    parser = argparse.ArgumentParser(description="Create a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job",
    )
    parser.add_argument(
        "-pn",
        "--pool_name",
        type=str,
        required=True,
        help="Name of the resource pool to use",
    )
    parser.add_argument(
        "-dep", "--uses_deps", action="store_true", help="Use dependencies"
    )
    parser.add_argument(
        "-s",
        "--save_logs_to_blob",
        type=str,
        default=None,
        help="Blob container to save logs",
    )
    parser.add_argument(
        "-l",
        "--logs_folder",
        type=str,
        default=None,
        help="Folder in blob container to save logs",
    )
    parser.add_argument(
        "-r",
        "--task_retries",
        type=int,
        default=0,
        help="Number of task retries on failure",
    )

    parser.add_argument(
        "-m",
        "--mark_complete",
        action="store_true",
        help="Mark job as complete after it finishes",
    )
    parser.add_argument(
        "-i",
        "--task_id_ints",
        action="store_true",
        help="Use integer task IDs",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=None,
        help="Job timeout in seconds",
    )
    parser.add_argument(
        "-e",
        "--exist_ok",
        action="store_true",
        help="If job with same name exists, do not create a new one",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.create_job(
        job_name=args.job_name,
        pool_name=args.pool_name,
        uses_deps=args.uses_deps,
        save_logs_to_blob=args.save_logs_to_blob,
        logs_folder=args.logs_folder,
        task_retries=args.task_retries,
        mark_complete_after_tasks_run=args.mark_complete,
        task_id_ints=args.task_id_ints,
        timeout=args.timeout,
        exist_ok=args.exist_ok,
        verbose=args.verbose,
    )


def add_task():
    parser = argparse.ArgumentParser(description="Add a task to a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-jn",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to add the task to",
    )
    parser.add_argument(
        "-c",
        "--command_line",
        type=str,
        required=True,
        help="Command line to execute",
    )
    parser.add_argument(
        "-n",
        "--name_suffix",
        type=str,
        default="",
        help="Suffix for the task name",
    )
    parser.add_argument(
        "-d",
        "--depends_on",
        nargs="+",
        default=None,
        help="List of task dependencies",
    )
    parser.add_argument(
        "-dr",
        "--depends_on_range",
        type=str,
        default=None,
        help="Range of task dependencies",
    )
    parser.add_argument(
        "-r",
        "--run_dependent_tasks_on_fail",
        action="store_true",
        help="Run dependent tasks even if this task fails",
    )
    parser.add_argument(
        "-ci",
        "--container_image_name",
        type=str,
        default=None,
        help="Container image name for the task",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=None,
        help="Task timeout in seconds",
    )

    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.add_task(
        job_name=args.job_name,
        command_line=args.command_line,
        name_suffix=args.name_suffix,
        depends_on=args.depends_on,
        depends_on_range=args.depends_on_range,
        run_dependent_tasks_on_fail=args.run_dependent_tasks_on_fail,
        container_image_name=args.container_image_name,
        timeout=args.timeout,
    )


def create_blob_container():
    parser = argparse.ArgumentParser(description="Create a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to create",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.create_blob_container(container_name=args.container_name)


def upload_file():
    parser = argparse.ArgumentParser(description="Upload files to a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-s",
        "--source_path",
        type=str,
        required=True,
        help="Path to the source file",
    )

    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to upload files to",
    )
    parser.add_argument(
        "-l",
        "--local_root_dir",
        type=str,
        default=".",
        required=False,
        help="Path to the local root directory",
    )
    parser.add_argument(
        "-loc",
        "--location_in_blob",
        type=str,
        default=".",
        help="Destination path in the blob container",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_files(
        files=args.source_path,
        container_name=args.container_name,
        local_root_dir=args.local_root_dir,
        location_in_blob=args.location_in_blob,
    )


def upload_folder():
    parser = argparse.ArgumentParser(description="Upload folder(s) to Blob")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--folder_name",
        type=str,
        required=True,
        help="Name of the folder to upload",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to upload folders to",
    )
    parser.add_argument(
        "-i",
        "--include_extensions",
        nargs="+",
        default=None,
        required=False,
        help="List of file extensions to include",
    )
    parser.add_argument(
        "-e",
        "--exclude_extensions",
        nargs="+",
        default=None,
        required=False,
        help="List of file extensions to exclude",
    )
    parser.add_argument(
        "-l",
        "--location_in_blob",
        type=str,
        default=".",
        help="Destination path in the blob container",
    )
    parser.add_argument(
        "-fu",
        "--force_upload",
        action="store_true",
        help="Force upload even if files exist",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_folders(
        folder_names=args.folder_name,
        container_name=args.container_name,
        include_extensions=args.include_extensions,
        exclude_extensions=args.exclude_extensions,
        location_in_blob=args.location_in_blob,
        force_upload=args.force_upload,
    )


def monitor_job():
    parser = argparse.ArgumentParser(description="Monitor a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to monitor",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds for monitoring the job",
    )
    parser.add_argument(
        "-d",
        "--download_job_stats",
        action="store_true",
        help="Download job statistics",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.monitor_job(
        job_name=args.job_name,
        timeout=args.timeout,
        download_job_stats=args.download_job_stats,
    )


def check_job_status():
    parser = argparse.ArgumentParser(description="Check job status")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to check status for",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    print(client.check_job_status(job_name=args.job_name))


def delete_job():
    parser = argparse.ArgumentParser(description="Delete a job")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_job(job_name=args.job_name)


def package_and_upload_dockerfile():
    parser = argparse.ArgumentParser(description="Package and upload Dockerfile")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-r",
        "--registry_name",
        type=str,
        required=True,
        help="Name of the Azure registry",
    )
    parser.add_argument(
        "-n",
        "--repo_name",
        type=str,
        required=True,
        help="Name of the repository to upload the package to",
    )
    parser.add_argument(
        "-t",
        "--tag",
        type=str,
        default=".",
        help="Tag for the container image",
    )
    parser.add_argument(
        "-d",
        "--path_to_dockerfile",
        type=str,
        required=False,
        default="./Dockerfile",
        help="Path to the Dockerfile",
    )
    parser.add_argument(
        "-u",
        "--use_device_code",
        action="store_true",
        help="Use device code for authentication",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.package_and_upload_dockerfile(
        registry_name=args.registry_name,
        repo_name=args.repo_name,
        tag=args.tag,
        path_to_dockerfile=args.path_to_dockerfile,
        use_device_code=args.use_device_code,
    )


def upload_docker_image():
    parser = argparse.ArgumentParser(
        description="Upload Docker image to Azure Container Registry"
    )
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-i",
        "--image_name",
        type=str,
        required=True,
        help="Name of the Docker image to upload",
    )
    parser.add_argument(
        "-r",
        "--registry_name",
        type=str,
        required=True,
        help="Name of the Azure Container Registry",
    )
    parser.add_argument(
        "-n",
        "--repo_name",
        type=str,
        required=True,
        help="Name of the repository to upload the image to",
    )
    parser.add_argument(
        "-t",
        "--tag",
        type=str,
        default="latest",
        help="Tag for the container image",
    )
    parser.add_argument(
        "-u",
        "--use_device_code",
        action="store_true",
        help="Use device code for authentication",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_docker_image(
        image_name=args.image_name,
        registry_name=args.registry_name,
        repo_name=args.repo_name,
        tag=args.tag,
        use_device_code=args.use_device_code,
    )


def download_file():
    parser = argparse.ArgumentParser(description="Download a file from Blob storage")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to download the file from",
    )
    parser.add_argument(
        "-b",
        "--blob_name",
        type=str,
        required=True,
        help="Name of the blob to download",
    )
    parser.add_argument(
        "-d",
        "--destination_path",
        type=str,
        required=True,
        help="Local path to save the downloaded file",
    )
    parser.add_argument(
        "-check",
        "--check_size",
        action="store_true",
        help="Check file size before downloading",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_file(
        src_path=args.blob_name,
        dest_path=args.destination_path,
        container_name=args.container_name,
        do_check=True,
        check_size=args.check_size,
    )


def download_folder():
    parser = argparse.ArgumentParser(description="Download a folder from Blob storage")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-s",
        "--src_path",
        type=str,
        required=True,
        help="source path",
    )
    parser.add_argument(
        "-d",
        "--dest_path",
        type=str,
        required=True,
        help="destination path",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to download the folder from",
    )
    parser.add_argument(
        "-i",
        "--include_extensions",
        nargs="+",
        required=False,
        default=None,
        help="List of file extensions to include",
    )
    parser.add_argument(
        "-e",
        "--exclude_extensions",
        nargs="+",
        required=False,
        default=None,
        help="List of file extensions to exclude",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "-check",
        "--check_size",
        action="store_true",
        help="Check file size before downloading",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_folder(  # type: ignore
        src_path=args.src_path,
        dest_path=args.dest_path,
        container_name=args.container_name,
        include_extensions=args.include_extensions,
        exclude_extensions=args.exclude_extensions,
        verbose=args.verbose,
        check_size=args.check_size,
    )


def delete_pool():
    parser = argparse.ArgumentParser(description="Delete a resource pool")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-n",
        "--pool_name",
        type=str,
        required=True,
        help="Name of the resource pool to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_pool(pool_name=args.pool_name)


def list_blob_files():
    parser = argparse.ArgumentParser(description="List files in a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to list files from",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    files = client.list_blob_files(blob_container=args.container_name)
    for file in files:
        print(file)


def delete_blob_file():
    parser = argparse.ArgumentParser(description="Delete a file from a blob container")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to delete the file from",
    )
    parser.add_argument(
        "-b",
        "--blob_name",
        type=str,
        required=True,
        help="Name of the blob to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_blob_file(
        container_name=args.container_name, blob_name=args.blob_name
    )


def delete_blob_folder():
    parser = argparse.ArgumentParser(
        description="Delete a folder from a blob container"
    )
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to delete the folder from",
    )
    parser.add_argument(
        "-b",
        "--blob_folder_name",
        type=str,
        required=True,
        help="Name of the blob folder to delete",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_blob_folder(
        container_name=args.container_name, folder_path=args.blob_folder_name
    )


def download_job_stats():
    parser = argparse.ArgumentParser(description="Download job stats from Blob storage")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-j",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to download stats for",
    )
    parser.add_argument(
        "-path",
        "--file_name",
        type=str,
        default=None,
        required=False,
        help="path to the downloaded file",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_job_stats(job_name=args.job_name, file_name=args.file_name)


def download_after_job():
    parser = argparse.ArgumentParser(
        description="Download files from Blob storage after job completion"
    )
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-j",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to monitor and download files after completion",
    )
    parser.add_argument(
        "-b",
        "--blob_paths",
        nargs="+",
        required=True,
        help="Name of the blob to download",
    )
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        required=True,
        help="Local path to save the downloaded file",
    )
    parser.add_argument(
        "-c",
        "--container_name",
        type=str,
        required=True,
        help="Name of the blob container to download the file from",
    )

    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.download_after_job(
        job_name=args.job_name,
        blob_paths=args.blob_paths,
        target=args.target,
        container_name=args.container_name,
    )


def add_tasks_from_yaml():
    parser = argparse.ArgumentParser(description="Add tasks to a job from a YAML file")
    parser.add_argument(
        "-p", "--dotenv_path", type=str, default=None, help="Path to .env file"
    )
    parser.add_argument(
        "-sp",
        "--use_sp",
        action="store_true",
        help="Use service principal for authentication",
    )
    parser.add_argument(
        "-f",
        "--use_federated",
        action="store_true",
        help="Use federated identity for authentication",
    )
    parser.add_argument(
        "-j",
        "--job_name",
        type=str,
        required=True,
        help="Name of the job to add tasks to",
    )
    parser.add_argument(
        "-c",
        "--base_cmd",
        type=str,
        required=True,
        help="Base command for the tasks",
    )
    parser.add_argument(
        "-f",
        "--file_path",
        type=str,
        required=True,
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.add_tasks_from_yaml(
        job_name=args.job_name,
        base_cmd=args.base_cmd,
        file_path=args.file_path,
    )


def generate_sample_env():
    text = """
    # This file is saved as cloudops-sample.env. Rename it to .env (or your desired name) and fill in the values.

    # Azure account info
    AZURE_BATCH_ACCOUNT="your azure batch account name"
    AZURE_BATCH_LOCATION="azure batch location"
    AZURE_USER_ASSIGNED_IDENTITY="/subscriptions/xxxxxxxxx/resourcegroups/xxxxxxxx/Microsoft.ManagedIdentity/userAssignedIdentities/xxxxxxxxxx"
    AZURE_SUBNET_ID="/subscriptions/xxxxxxxx/resourceGroups/xxxxxxxx/providers/Microsoft.Network/virtualNetworks/xxxxxxxx/subnets/xxxxxxxx"
    AZURE_SP_CLIENT_ID="your sp client id"
    AZURE_KEYVAULT_NAME="your keyvault name"
    AZURE_KEYVAULT_SP_SECRET_ID="your keyvault secret id"

    # Azure Blob storage config
    AZURE_BLOB_STORAGE_ACCOUNT="your azure blob storage account"

    # Azure container registry config
    AZURE_CONTAINER_REGISTRY_ACCOUNT="your azure container registry name"
    """
    try:
        with open("cloudops-sample.env", "w") as file:
            file.write(textwrap.dedent(text).strip() + "\n")
        print("Sample .env file 'cloudops-sample.env' created successfully.")
    except Exception as e:
        print(f"Error creating sample .env file: {e}")
>>>>>>> 6ea9bf37aae39826d9cbc2fbb45d49d457801069
