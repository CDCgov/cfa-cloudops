import argparse
import datetime
import json
import logging
import sys
import textwrap

from cfa.cloudops import CloudClient


def setup_logging():
    level = logging.INFO

    handlers = [logging.StreamHandler(sys.stdout)]

    logging.basicConfig(
        level=level,
        format="%(asctime)s | [%(levelname)-8s] | %(name)s | %(message)s",
        handlers=handlers,
    )

    # Keep package logs visible while reducing Azure SDK noise.
    logging.getLogger("cfa.cloudops").setLevel(logging.INFO)
    logging.getLogger("azure").setLevel(logging.ERROR)


logger = logging.getLogger(__name__)

# Configure logging once on module import so all script entry points inherit it.
setup_logging()


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
        default=True,
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
        "-fp",
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


def check_credentials():
    parser = argparse.ArgumentParser(description="Check CloudClient credentials")
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
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.check_credentials()


def create_job_schedule():
    parser = argparse.ArgumentParser(description="Create a job schedule")
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
    parser.add_argument("-n", "--job_schedule_name", type=str, required=True)
    parser.add_argument("-pn", "--pool_name", type=str, required=True)
    parser.add_argument("-c", "--command", type=str, required=True)
    parser.add_argument("-t", "--timeout", type=int, default=30)
    parser.add_argument(
        "-sw",
        "--start_window_minutes",
        type=int,
        default=None,
        help="Job schedule start window in minutes",
    )
    parser.add_argument(
        "-ri",
        "--recurrence_interval_minutes",
        type=int,
        default=None,
        help="Job schedule recurrence interval in minutes",
    )
    parser.add_argument(
        "-dnu",
        "--do_not_run_until",
        type=str,
        default=None,
        help="Do not run until datetime string",
    )
    parser.add_argument(
        "-dna",
        "--do_not_run_after",
        type=str,
        default=None,
        help="Do not run after datetime string",
    )
    parser.add_argument(
        "-e",
        "--exist_ok",
        action="store_true",
        help="Do not fail if job schedule already exists",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    start_window = None
    if args.start_window_minutes is not None:
        start_window = datetime.timedelta(minutes=args.start_window_minutes)
    recurrence_interval = None
    if args.recurrence_interval_minutes is not None:
        recurrence_interval = datetime.timedelta(
            minutes=args.recurrence_interval_minutes
        )
    client.create_job_schedule(
        job_schedule_name=args.job_schedule_name,
        pool_name=args.pool_name,
        command=args.command,
        timeout=args.timeout,
        start_window=start_window,
        recurrence_interval=recurrence_interval,
        do_not_run_until=args.do_not_run_until,
        do_not_run_after=args.do_not_run_after,
        exist_ok=args.exist_ok,
        verbose=args.verbose,
    )


def delete_job_schedule():
    parser = argparse.ArgumentParser(description="Delete a job schedule")
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
    parser.add_argument("-n", "--job_schedule_id", type=str, required=True)
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.delete_job_schedule(job_schedule_id=args.job_schedule_id)


def resume_job_schedule():
    parser = argparse.ArgumentParser(description="Resume a suspended job schedule")
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
    parser.add_argument("-n", "--job_schedule_id", type=str, required=True)
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.resume_job_schedule(job_schedule_id=args.job_schedule_id)


def suspend_job_schedule():
    parser = argparse.ArgumentParser(description="Suspend an active job schedule")
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
    parser.add_argument("-n", "--job_schedule_id", type=str, required=True)
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.suspend_job_schedule(job_schedule_id=args.job_schedule_id)


def list_available_images():
    parser = argparse.ArgumentParser(description="List available Azure Batch images")
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
        "-os",
        "--operating_system",
        type=str,
        default=None,
        help="Optional operating system filter (linux/windows)",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    for image in client.list_available_images(operating_system=args.operating_system):
        print(image)


def update_blob_protection():
    parser = argparse.ArgumentParser(
        description="Update legal hold or read-only on blobs"
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
    parser.add_argument("-s", "--source_path", nargs="+", required=True)
    parser.add_argument("-c", "--container_name", type=str, required=True)
    parser.add_argument("-lh", "--legal_hold", action="store_true")
    parser.add_argument("-ro", "--read_only", action="store_true")
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    files = args.source_path if len(args.source_path) > 1 else args.source_path[0]
    client.update_blob_protection(
        files=files,
        container_name=args.container_name,
        legal_hold=args.legal_hold,
        read_only=args.read_only,
    )


def list_acr_tags():
    parser = argparse.ArgumentParser(description="List tags in an ACR repository")
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
    parser.add_argument("-r", "--registry_name", type=str, required=True)
    parser.add_argument("-n", "--repo_name", type=str, required=True)
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    for tag in client.list_acr_tags(
        registry_name=args.registry_name, repo_name=args.repo_name
    ):
        print(tag)


def get_task_status():
    parser = argparse.ArgumentParser(description="Get task status for a job")
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
    parser.add_argument("-j", "--job_name", type=str, required=True)
    parser.add_argument("-t", "--task_id", type=str, default=None)
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    print(client.get_task_status(job_name=args.job_name, task_id=args.task_id))


def get_kv_secret():
    parser = argparse.ArgumentParser(description="Get a secret from Azure Key Vault")
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
    parser.add_argument("-s", "--secret_name", type=str, required=True)
    parser.add_argument("-k", "--keyvault", type=str, required=True)
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    print(client.get_kv_secret(secret_name=args.secret_name, keyvault=args.keyvault))


def get_all_vm_quotas():
    parser = argparse.ArgumentParser(description="Get all available VM quotas")
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
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    for quota in client.get_all_vm_quotas():
        print(quota)


def get_vm_series_quotas():
    parser = argparse.ArgumentParser(description="Get VM quotas filtered by series")
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
        "--series",
        nargs="+",
        required=True,
        help="VM series values, e.g., D E",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    series = args.series if len(args.series) > 1 else args.series[0]
    for quota in client.get_vm_series_quotas(series=series):
        print(quota)


def get_vm_name():
    parser = argparse.ArgumentParser(
        description="Get a VM name matching selection criteria"
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
    parser.add_argument("-s", "--series", type=str, default="D")
    parser.add_argument("-c", "--cores", type=int, default=4)
    parser.add_argument("-amd", "--amd", action="store_true")
    parser.add_argument("-ntd", "--no_temp_disk", action="store_true")
    parser.add_argument("-ssd", "--ssd", action="store_true")
    parser.add_argument("-v", "--version", type=int, default=5)
    parser.add_argument("-nv", "--no_verify", action="store_true")
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    print(
        client.get_vm_name(
            series=args.series,
            cores=args.cores,
            amd=args.amd,
            temp_disk=not args.no_temp_disk,
            ssd=args.ssd,
            version=args.version,
            verify=not args.no_verify,
        )
    )


def add_task_collection():
    parser = argparse.ArgumentParser(description="Add a task collection to a job")
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
    parser.add_argument("-j", "--job_name", type=str, required=True)
    parser.add_argument(
        "-tf",
        "--tasks_file",
        type=str,
        required=True,
        help="Path to JSON file containing a list of task objects",
    )
    parser.add_argument("-n", "--name_suffix", type=str, default="")
    args = parser.parse_args()

    with open(args.tasks_file, "r") as f:
        tasks = json.load(f)

    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.add_task_collection(
        job_name=args.job_name,
        tasks=tasks,
        name_suffix=args.name_suffix,
    )


def async_download_folder():
    parser = argparse.ArgumentParser(description="Asynchronously download a folder")
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
    parser.add_argument("-s", "--src_path", type=str, required=True)
    parser.add_argument("-d", "--dest_path", type=str, required=True)
    parser.add_argument("-c", "--container_name", type=str, required=True)
    parser.add_argument("-i", "--include_extensions", nargs="+", default=None)
    parser.add_argument("-e", "--exclude_extensions", nargs="+", default=None)
    parser.add_argument("-check", "--check_size", action="store_true")
    parser.add_argument(
        "-mcd",
        "--max_concurrent_downloads",
        type=int,
        default=20,
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.async_download_folder(
        src_path=args.src_path,
        dest_path=args.dest_path,
        container_name=args.container_name,
        include_extensions=args.include_extensions,
        exclude_extensions=args.exclude_extensions,
        check_size=args.check_size,
        max_concurrent_downloads=args.max_concurrent_downloads,
    )


def async_upload_folder():
    parser = argparse.ArgumentParser(description="Asynchronously upload folder(s)")
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
        "--folders",
        nargs="+",
        required=True,
        help="Folder path(s) to upload",
    )
    parser.add_argument("-c", "--container_name", type=str, required=True)
    parser.add_argument("-i", "--include_extensions", nargs="+", default=None)
    parser.add_argument("-e", "--exclude_extensions", nargs="+", default=None)
    parser.add_argument("-l", "--location_in_blob", type=str, default=".")
    parser.add_argument(
        "-mcu",
        "--max_concurrent_uploads",
        type=int,
        default=20,
    )
    parser.add_argument("-lh", "--legal_hold", action="store_true")
    parser.add_argument("-ild", "--immutability_lock_days", type=int, default=0)
    parser.add_argument("-ro", "--read_only", action="store_true")
    args = parser.parse_args()
    folders = args.folders if len(args.folders) > 1 else args.folders[0]
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.async_upload_folder(
        folders=folders,
        container_name=args.container_name,
        include_extensions=args.include_extensions,
        exclude_extensions=args.exclude_extensions,
        location_in_blob=args.location_in_blob,
        max_concurrent_uploads=args.max_concurrent_uploads,
        legal_hold=args.legal_hold,
        immutability_lock_days=args.immutability_lock_days,
        read_only=args.read_only,
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


def test():
    try:
        import pytest
    except ImportError as exc:
        raise RuntimeError(
            "pytest is not installed. Run `uv run test` from the project root, "
            "or install the dev dependencies first with `uv sync`."
        ) from exc

    raise SystemExit(pytest.main(sys.argv[1:]))
