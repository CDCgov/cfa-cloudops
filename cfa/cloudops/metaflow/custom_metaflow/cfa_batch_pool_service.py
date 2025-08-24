import numpy as np
from cfa.cloudops.batch_helpers import check_pool_exists
import toml
from cfa.cloudops import CloudClient


DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"

class CFABatchPoolService:
    def __init__(self):
        self.cloud_client = CloudClient()
        self.batch_pools = []

    def setup_pools(self, config_file_path):
        attributes = toml.load(config_file_path)
        self.parallel_pool_limit = int(attributes['Batch'].get("parallel_pool_limit", "1"))
        pool_name_prefix = attributes['Batch'].get("pool_name_prefix", "cfa_pool_")
        for i in range(self.parallel_pool_limit):
            pool_name = f"{pool_name_prefix}{i}"
            if check_pool_exists(self.cloud_client.cred.azure_resource_group_name, self.cloud_client.cred.azure_batch_account, pool_name, self.cloud_client.batch_mgmt_client):
                print(f'Existing Azure batch pool {pool_name} is being reused')
                continue
            self.cloud_client.create_pool(
                pool_name=pool_name,
                mounts=[('input', 'input'), ('output', 'output')],
                container_image_name=attributes["Container"].get("container_image_name", DEFAULT_CONTAINER_IMAGE_NAME),
                vm_size=attributes['Batch'].get("pool_vm_size")
            )
            self.batch_pools.append(pool_name)


    def setup_step_parameters(self, items):
         item_chunks = np.array_split(items, self.parallel_pool_limit)
         step_parameters = []
         for i in range(self.parallel_pool_limit):
            pool_name = self.batch_pools[i] if i < len(self.batch_pools) else None
            step_parameters.append(
                {'pool_name': pool_name, 'sp_secret': self.cloud_client.cred, 'parameters': item_chunks[i]}
            )
         return step_parameters

    def delete_all_pools(self):
        for pool_name in self.batch_pools:
            self.cloud_client.delete_pool(pool_name)
            print(f"Deleted Azure Batch Pool: {pool_name}")
        return True