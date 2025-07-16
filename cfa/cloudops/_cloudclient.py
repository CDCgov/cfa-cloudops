from azure.mgmt.batch import models

import cfa.cloudops.defaults as d

from .auth import EnvCredentialHandler
from .blob import get_node_mount_config
from .client import (
    get_batch_management_client,
    get_batch_service_client,
    get_blob_service_client,
    get_compute_management_client,
)


class CloudClient:
    def __init__(self, dotenv_path: str = None):
        # authenticate to get credentials
        self.cred = EnvCredentialHandler(dotenv_path=dotenv_path)
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
        max_autoscale_nodes=10,
        task_slots_per_node=1,
        availability_zones="regional",
        cache_blobfuse=True,
    ):
        """
        Create a pool in Azure Batch with the specified configuration.

        Parameters
        ----------
        pool_name : str
            Name of the pool to create
        mounts : list, optional
            List of mount configurations as tuples of (storage_container, mount_name)
        container_image_name : str, optional
            Docker container image name to use
        vm_size : str
            Azure VM size for the pool nodes
        autoscale : bool
            Whether to enable autoscaling (True) or use fixed scaling (False)
        autoscale_formula : str
            Autoscale formula to use when autoscale=True
        dedicated_nodes : int
            Number of dedicated nodes when autoscale=False
        low_priority_nodes : int
            Number of low priority nodes when autoscale=False
        max_autoscale_nodes : int
            Maximum number of nodes for autoscaling
        task_slots_per_node : int
            Number of task slots per node
        availability_zones : str
            Whether to use 'zonal' or 'regional' policy for availability zones.
        cache_blobfuse: bool
            Whether to enable blobfuse caching
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
        if availability_zones == "regional":
            pool_config.deployment_configuration.virtual_machine_configuration.node_placement_configuration = models.NodePlacementConfiguration(
                policy=models.NodePlacementPolicyType.regional
            )
        elif availability_zones == "zonal":
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
        except Exception as e:
            error_msg = f"Failed to create pool '{pool_name}': {str(e)}"
            raise RuntimeError(error_msg)
