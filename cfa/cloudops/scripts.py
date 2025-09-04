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
