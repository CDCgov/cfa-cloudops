import logging

import numpy as np
import toml
from azure.mgmt.batch import models
from dotenv import dotenv_values

import cfa.cloudops.batch_helpers as bh
from cfa.cloudops.auth import SPCredentialHandler
from cfa.cloudops.blob import get_node_mount_config
from cfa.cloudops.client import get_batch_management_client
from cfa.cloudops.defaults import (
    assign_container_config,
    default_azure_batch_endpoint_subdomain,
    default_azure_batch_location,
    get_default_pool_config,
    remaining_task_autoscale_formula,
)

DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"
DEFAULT_POOL_LIMIT = "1"

logger = logging.getLogger(__name__)


class CFABatchPoolService:
    def __init__(self, dotenv_path, job_config_file="job.toml"):
        self.batch_pools = []
        self.attributes = dotenv_values(dotenv_path)
        self.job_configuration = toml.load(job_config_file)
        self.parallel_pool_limit = int(
            self.job_configuration["Pool"].get(
                "parallel_pool_limit", DEFAULT_POOL_LIMIT
            )
        )
        self.__setup_credentials()
        self.batch_mgmt_client = get_batch_management_client(self.cred)

    def __setup_credentials(self):
        self.cred = SPCredentialHandler(
            azure_tenant_id=self.attributes["AZURE_TENANT_ID"],
            azure_subscription_id=self.attributes["AZURE_SUBSCRIPTION_ID"],
            azure_sp_client_id=self.attributes["AZURE_SP_CLIENT_ID"],
            azure_client_secret=self.attributes["AZURE_CLIENT_SECRET"],
            azure_keyvault_endpoint=self.attributes["AZURE_KEYVAULT_ENDPOINT"],
            azure_keyvault_sp_secret_id=self.attributes[
                "AZURE_KEYVAULT_SP_SECRET_ID"
            ],
        )
        self.cred.azure_container_registry_account = self.attributes.get(
            "AZURE_CONTAINER_REGISTRY_ACCOUNT"
        )
        self.cred.azure_user_assigned_identity = self.attributes.get(
            "AZURE_USER_ASSIGNED_IDENTITY"
        )
        self.cred.azure_resource_group_name = self.attributes.get(
            "AZURE_RESOURCE_GROUP"
        )
        self.cred.azure_blob_storage_account = self.attributes.get(
            "AZURE_BLOB_STORAGE_ACCOUNT"
        )
        self.cred.azure_subnet_id = self.attributes.get("AZURE_SUBNET_ID")
        self.cred.azure_batch_account = self.attributes.get(
            "AZURE_BATCH_ACCOUNT"
        )
        self.cred.azure_batch_location = default_azure_batch_location
        self.cred.azure_batch_endpoint_subdomain = (
            default_azure_batch_endpoint_subdomain
        )

    def __setup_pool(self, pool_name):
        if bh.check_pool_exists(
            self.cred.azure_resource_group_name,
            self.cred.azure_batch_account,
            pool_name,
            self.batch_mgmt_client,
        ):
            logger.info(
                f"Existing Azure batch pool {pool_name} is being reused"
            )
        else:
            mount_config = self.__create_containers()
            pool_config = self.__create_pool_configuration(
                pool_name, mount_config
            )
            self.__create_pool(pool_name, pool_config)
        if pool_name not in self.batch_pools:
            self.batch_pools.append(pool_name)

    def setup_pools(self, pools: list[str] = None):
        pool_name = self.job_configuration["Pool"].get("pool_name")
        if pool_name:
            self.__setup_pool(pool_name)
        elif pools:
            for pool_name in pools:
                self.__setup_pool(pool_name)
        else:
            pool_name_prefix = self.job_configuration["Pool"].get(
                "pool_name_prefix", "cfa_pool_"
            )
            for i in range(self.parallel_pool_limit):
                pool_name = f"{pool_name_prefix}{i}"
                self.__setup_pool(pool_name)

    def __create_containers(self):
        storage_containers = []
        mount_names = []
        input_mount_name = self.job_configuration["Pool"].get(
            "input_mount", "input-test"
        )
        output_mount_name = self.job_configuration["Pool"].get(
            "output_mount", "output-test"
        )
        mounts = [(input_mount_name, "input"), (output_mount_name, "output")]
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

    def __setup_fixedscale_configuration(self, pool_config):
        pool_config.scale_settings = models.ScaleSettings(
            fixed_scale=models.FixedScaleSettings(
                target_dedicated_nodes=int(
                    self.job_configuration["Pool"].get("dedicated_nodes", "3")
                ),
                target_low_priority_nodes=int(
                    self.job_configuration["Pool"].get(
                        "low_priority_nodes", "3"
                    )
                ),
            )
        )
        return pool_config

    def __setup_autoscaled_configuration(self, pool_config):
        formula = remaining_task_autoscale_formula(
            task_sample_interval_minutes=15,
            max_number_vms=int(
                self.job_configuration["Pool"].get("max_autoscale_nodes", "3")
            ),
        )
        pool_config.scale_settings = models.ScaleSettings(
            auto_scale=models.AutoScaleSettings(
                formula=formula,
                evaluation_interval="PT5M",  # Evaluate every 5 minutes
            )
        )
        return pool_config

    def __create_pool_configuration(self, pool_name, mount_config):
        pool_config = get_default_pool_config(
            pool_name=pool_name,
            subnet_id=self.cred.azure_subnet_id,
            user_assigned_identity=self.cred.azure_user_assigned_identity,
            mount_configuration=mount_config,
            vm_size=self.job_configuration["Pool"].get("vm_size"),
        )
        autoscale = self.job_configuration["Pool"].get("autoscale", "True")
        if autoscale.lower() == "true":
            pool_config = self.__setup_autoscaled_configuration(pool_config)
        else:
            pool_config = self.__setup_fixedscale_configuration(pool_config)

        pool_config.task_slots_per_node = int(
            self.job_configuration["Pool"].get("task_slots_per_node", "1")
        )
        container_config = models.ContainerConfiguration(
            type="dockerCompatible",
            container_image_names=[
                self.job_configuration["Pool"].get("container_image_name")
            ],
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
            logger.info(f"created pool: {pool_name}")
        except Exception as e:
            error_msg = f"Failed to create pool '{pool_name}': {str(e)}"
            raise RuntimeError(error_msg)

    def setup_step_parameters(self, items, pools: list[str] = None):
        if "Job" in self.job_configuration:
            docker_command = self.job_configuration["Job"].get(
                "docker_command", "python main.py"
            )
        if pools:
            item_chunks = np.array_split(items, len(pools))
        else:
            item_chunks = np.array_split(items, self.parallel_pool_limit)
        step_parameters = []
        for i in range(len(item_chunks)):
            pool_name = (
                self.batch_pools[i] if i < len(self.batch_pools) else None
            )
            step_parameters.append(
                {
                    "pool_name": pool_name,
                    "attributes": self.attributes,
                    "job_configuration": self.job_configuration,
                    "task_parameters": item_chunks[i],
                    "docker_command": docker_command,
                }
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
            logger.info(f"Deleted Azure Batch Pool: {pool_name}")
        return True
