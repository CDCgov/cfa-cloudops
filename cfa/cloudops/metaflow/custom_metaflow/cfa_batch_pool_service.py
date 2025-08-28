from azure.mgmt.batch import models
from dotenv import dotenv_values
import numpy as np
import cfa.cloudops.batch_helpers as bh
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
        self.attributes = dotenv_values(dotenv_path)
        self.cred = SPCredentialHandler(
            azure_tenant_id=self.attributes['AZURE_TENANT_ID'],
            azure_subscription_id=self.attributes['AZURE_SUBSCRIPTION_ID'],
            azure_sp_client_id=self.attributes['AZURE_SP_CLIENT_ID'],
            azure_client_secret=self.attributes['AZURE_CLIENT_SECRET'],
            azure_keyvault_endpoint=self.attributes['AZURE_KEYVAULT_ENDPOINT'],
            azure_keyvault_sp_secret_id=self.attributes['AZURE_KEYVAULT_SP_SECRET_ID']
        )
        self.cred.azure_user_assigned_identity = self.attributes.get("AZURE_USER_ASSIGNED_IDENTITY")
        self.cred.azure_resource_group_name = self.attributes.get("AZURE_RESOURCE_GROUP")
        self.cred.azure_batch_account = self.attributes.get("AZURE_BATCH_ACCOUNT")
        self.cred.azure_blob_storage_account = self.attributes.get("AZURE_BLOB_STORAGE_ACCOUNT")
        self.cred.azure_subnet_id = self.attributes.get("AZURE_SUBNET_ID")
        self.batch_mgmt_client = get_batch_management_client(self.cred)


    def setup_pools(self):
        self.parallel_pool_limit = int(self.attributes.get("PARALLEL_POOL_LIMIT", "1"))
        pool_name_prefix = self.attributes.get("POOL_NAME_PREFIX", "cfa_pool_")
        for i in range(self.parallel_pool_limit):
            pool_name = f"{pool_name_prefix}{i}"
            if bh.check_pool_exists(self.cred.azure_resource_group_name, self.cred.azure_batch_account, pool_name, self.batch_mgmt_client):
                print(f'Existing Azure batch pool {pool_name} is being reused')
            else:
                mount_config = self.__create_containers()
                pool_config = self.__create_pool_configuration(pool_name, mount_config)
                self.__create_pool(pool_name, pool_config)
            self.batch_pools.append(pool_name)


    def __create_containers(self):
        storage_containers = []
        mount_names = []
        mounts=[('input','input'), ('output', 'output')]
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


    def __create_pool_configuration(self, pool_name, mount_config):
        pool_config = get_default_pool_config(
            pool_name=pool_name,
            subnet_id=self.cred.azure_subnet_id,
            user_assigned_identity=self.cred.azure_user_assigned_identity,
            mount_configuration=mount_config,
            vm_size=self.attributes.get("POOL_VM_SIZE"),
        )
        formula = remaining_task_autoscale_formula(
            task_sample_interval_minutes=15,
            max_number_vms=int(self.attributes.get("MAX_AUTOSCALE_NODES", "3")),
        )
        pool_config.scale_settings = models.ScaleSettings(
            auto_scale=models.AutoScaleSettings(
                formula=formula,
                evaluation_interval="PT5M",  # Evaluate every 5 minutes
            )
        )
        pool_config.task_slots_per_node = int(self.attributes.get("TASK_SLOTS_PER_NODE", "1"))

        container_config = models.ContainerConfiguration(
            type="dockerCompatible",
            container_image_names=[self.attributes.get("CONTAINER_IMAGE_NAME", DEFAULT_CONTAINER_IMAGE_NAME)],
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
                {'pool_name': pool_name, 'cred': self.cred, 'attributes': self.attributes, 'parameters': item_chunks[i]}
            )
         return step_parameters


    def delete_all_pools(self):
        for pool_name in self.batch_pools:
            bh.delete_pool(
                resource_group_name=self.cred.azure_resource_group_name,
                account_name=self.cred.azure_batch_account,
                pool_name=pool_name,
                batch_mgmt_client=self.batch_mgmt_client,
            )
            print(f"Deleted Azure Batch Pool: {pool_name}")
        return True
