from azure.mgmt.batch import models
import numpy as np
import toml
from cfa.cloudops.batch_helpers import check_pool_exists
from cfa.cloudops.auth import SPCredentialHandler
from cfa.cloudops.blob import get_node_mount_config
from cfa.cloudops.client import get_batch_management_client
from cfa.cloudops.defaults import (
    assign_container_config,
    get_default_pool_config,
    remaining_task_autoscale_formula, 
)

DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"

class CFABatchPoolService:
    def __init__(self, dotenv_path):
        self.batch_pools = []
        self.resource_group_name = None
        self.account_name = None
        self.cred = SPCredentialHandler(dotenv_path)
        self.batch_mgmt_client = get_batch_management_client(self.cred)


    def setup_pools(self, config_file_path):
        attributes = toml.load(config_file_path)
        self.parallel_pool_limit = int(attributes['Batch'].get("parallel_pool_limit", "1"))
        pool_name_prefix = attributes['Batch'].get("pool_name_prefix", "cfa_pool_")
        self.cred.azure_user_assigned_identity = attributes['Authentication'].get("azure_user_assigned_identity")
        self.cred.azure_resource_group_name = attributes['Authentication'].get("resource_group")
        self.cred.azure_batch_account =  attributes['Batch'].get("batch_account_name")
        for i in range(self.parallel_pool_limit):
            pool_name = f"{pool_name_prefix}{i}"
            if check_pool_exists(self.cred.azure_resource_group_name, self.cred.azure_batch_account, pool_name, self.batch_mgmt_client):
                print(f'Existing Azure batch pool {pool_name} is being reused')
                continue
            mount_config = self.__create_containers()
            pool_config = self.__create_pool_configuration(pool_name, mount_config, attributes)
            self.__create_pool(pool_name, pool_config)
            self.batch_pools.append(pool_name)
        

    def __create_containers(self):
        storage_containers = []
        mount_names = []
        mounts=['input', 'output'],
        for mount in mounts:
            storage_containers.append(mount[0])
            mount_names.append(mount[1])
        mount_config = get_node_mount_config(
            storage_containers=storage_containers,
            mount_names=mount_names,
            account_names=self.cred.azure_blob_storage_account,
            identity_references=self.cred.compute_node_identity_reference,
            cache_blobfuse=True,
        )
        return mount_config


    def __create_pool_configuration(self, pool_name, mount_config, attributes):
        pool_config = get_default_pool_config(
            pool_name=pool_name,
            subnet_id=self.cred.azure_subnet_id,
            user_assigned_identity=self.cred.azure_user_assigned_identity,
            mount_configuration=mount_config,
            vm_size=attributes['Batch'].get("pool_vm_size"),
        )
        formula = remaining_task_autoscale_formula(
            task_sample_interval_minutes=15,
            max_number_vms=int(attributes['Batch'].get("max_autoscale_nodes", "3")),
        )
        pool_config.scale_settings = models.ScaleSettings(
            auto_scale=models.AutoScaleSettings(
                formula=formula,
                evaluation_interval="PT5M",  # Evaluate every 5 minutes
            )
        )
        pool_config.task_slots_per_node = int(attributes['Batch'].get("task_slots_per_node", "1"))

        container_config = models.ContainerConfiguration(
            type="dockerCompatible",
            container_image_names=[attributes['Batch'].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME)],
        )

        if hasattr(self.cred, "azure_container_registry"):
            container_config.container_registries = [
                self.cred.azure_container_registry
            ]
            assign_container_config(pool_config, container_config)

        pool_config.deployment_configuration.virtual_machine_configuration.node_placement_configuration = models.NodePlacementConfiguration(
            policy=models.NodePlacementPolicyType.regional
        )
        return pool_config


    def __create_pool(self, pool_name, pool_config):
        try:
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


    def setup_step_parameters(self, items):
         item_chunks = np.array_split(items, self.parallel_pool_limit)
         step_parameters = []
         for i in range(self.parallel_pool_limit):
            pool_name = self.batch_pools[i] if i < len(self.batch_pools) else None
            step_parameters.append(
                {'pool_name': pool_name, 'cred': self.cred, 'parameters': item_chunks[i]}
            )
         return step_parameters


    def delete_all_pools(self):
        for pool_name in self.batch_pools:
            self.cloud_client.delete_pool(pool_name)
            print(f"Deleted Azure Batch Pool: {pool_name}")
        return True
