import argparse

from cfa.cloudops import CloudClient


def hello():
    parser = argparse.ArgumentParser(description="CloudOps parser")
    parser.add_argument(
        "--name", type=str, default="World", help="Name to greet"
    )
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
        "-m", "--mounts", type=list, default=[], help="List of mount points"
    )
    parser.add_argument(
        "-c",
        "--container_image_name",
        type=str,
        required=True,
        help="Container image name",
    )
    parser.add_argument("-v", "--vm_size", type=str, help="VM size")
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
    client.create_pool(
        pool_name=args.pool_name,
        mounts=args.mounts,
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
        "-dep", "--use_deps", action="store_true", help="Use dependencies"
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
        use_deps=args.use_deps,
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
        type=list,
        default=[],
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


def upload_files():
    parser = argparse.ArgumentParser(
        description="Upload files to a blob container"
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
        help="Name of the blob container to upload files to",
    )
    parser.add_argument(
        "-s",
        "--source",
        type=str,
        required=True,
        help="Source file or directory to upload",
    )
    parser.add_argument(
        "-d",
        "--destination",
        type=str,
        default="",
        help="Destination path in the blob container",
    )
    args = parser.parse_args()
    client = CloudClient(
        dotenv_path=args.dotenv_path,
        use_sp=args.use_sp,
        use_federated=args.use_federated,
    )
    client.upload_files_to_blob_container(
        container_name=args.container_name,
        source=args.source,
        destination=args.destination,
    )
