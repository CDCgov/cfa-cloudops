import numpy as np
from cfa_azure.batch_helpers import (
    check_pool_exists,
    create_batch_pool, 
    delete_pool,
    get_pool_parameters,
    get_batch_mgmt_client
)
from cfa_azure.blob_helpers import (
    get_blob_config,
    get_mount_config,
    get_blob_service_client
)
from cfa_azure.helpers import (
    create_container,
    format_rel_path,
    get_sp_secret,
    get_batch_service_client,
    read_config
)
from azure.identity import (
    ClientSecretCredential,
    ManagedIdentityCredential,
)
from azure.common.credentials import ServicePrincipalCredentials


DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"

class CFABatchPoolService:
    def __init__(self):
        self.attributes = read_config("client_config_states.toml")
        self.resource_group_name = self.attributes["Authentication"]["resource_group"]
        self.account_name = self.attributes["Batch"]["batch_account_name"]
        self.batch_pools = []

    def __get_secret_credentials(self):
        self.sp_secret = get_sp_secret(self.attributes, ManagedIdentityCredential())
        self.secret_cred = ClientSecretCredential(
            tenant_id=self.attributes["Authentication"]["tenant_id"],
            client_id=self.attributes["Authentication"]["sp_application_id"],
            client_secret=self.sp_secret,
        )
        self.batch_cred = ServicePrincipalCredentials(
            client_id=self.attributes["Authentication"]["sp_application_id"],
            tenant=self.attributes["Authentication"]["tenant_id"],
            secret=self.sp_secret,
            resource="https://batch.core.windows.net/",
        )

    def __setup_clients(self):
        self.batch_client = get_batch_service_client(self.attributes, self.batch_cred)
        self.batch_mgmt_client = get_batch_mgmt_client(config=self.attributes, credential=self.secret_cred)
        self.blob_service_client = get_blob_service_client(self.attributes, self.secret_cred)

    def setup_pools(self):
        self.parallel_pool_limit = int(self.attributes.get("ParallelPoolLimit", "1"))
        pool_name_prefix = self.attributes.get("PoolNamePrefix", "cfa_pool_")
        self.__get_secret_credentials()
        self.__setup_clients()
        for i in range(self.parallel_pool_limit):
            pool_name = f"{pool_name_prefix}{i}"
            self.__fetch_or_create_pool(pool_name)

    def setup_step_parameters(self, items):
         item_chunks = np.array_split(items, self.parallel_pool_limit)
         step_parameters = []
         for i in range(self.parallel_pool_limit):
            pool_name = self.batch_pools[i] if i < len(self.batch_pools) else None
            step_parameters.append(
                {'pool_name': pool_name, 'sp_secret': self.sp_secret, 'parameters': item_chunks[i]}
            )
         return step_parameters

    def __create_containers(self) -> list:
        mounts = []
        container_names = ['input', 'output']
        for name in container_names:
            rel_mount_dir = format_rel_path(f"/{name}")
            mounts.append((f"cfa{name}", rel_mount_dir))
            create_container(f"cfa{name}", self.blob_service_client)
        return mounts

    def __fetch_or_create_pool(self, pool_name) -> bool:
        if check_pool_exists(self.resource_group_name, self.account_name, pool_name, self.batch_mgmt_client):
            print(f'Existing Azure batch pool {self.pool_name} is being reused')
        else:
            mounts = self.__create_containers()
            blob_config = []
            if mounts:
                for mount in mounts:
                    blob_config.append(
                        get_blob_config(
                            mount[0], mount[1], True, self.attributes
                        )
                    )
            mount_config = get_mount_config(blob_config)
            pool_parameters = get_pool_parameters(
                mode="autoscale",
                container_image_name=self.attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME),
                container_registry_url=self.attributes["Container"]["container_registry_url"],
                container_registry_server=self.attributes["Container"]["container_registry_server"],
                config=self.attributes,
                mount_config=mount_config,
                credential=self.secret_cred,
                use_default_autoscale_formula=True
            )
            batch_json = {
                "account_name": self.account_name,
                "pool_id": pool_name,
                "pool_parameters": pool_parameters,
                "resource_group_name": self.resource_group_name
            }
            pool_name = create_batch_pool(self.batch_mgmt_client, batch_json)
        self.batch_pools.append(pool_name)
        return pool_name

    def delete_all_pools(self):
        for pool_name in self.batch_pools:
            delete_pool(self.resource_group_name, self.account_name, pool_name, self.batch_mgmt_client)
            print(f"Deleted Azure Batch Pool: {pool_name}")
        return True