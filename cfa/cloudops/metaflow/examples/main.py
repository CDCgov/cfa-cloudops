from dotenv import dotenv_values
import logging
from metaflow import FlowSpec, step
from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import (
    CFAAzureBatchDecorator
)
from custom_metaflow.cfa_batch_pool_service import (
    CFABatchPoolService
)
logger = logging.getLogger(__name__)

my_dotenv_path = 'metaflow.env'

attributes = dotenv_values(my_dotenv_path)

attributes_1 = attributes.copy()
attributes_1['CONTAINER_IMAGE_NAME'] = 'r-base:latest'

attributes_2 = attributes.copy()
attributes_2['CONTAINER_IMAGE_NAME'] = 'python:3.10.17-slim-bullseye'


class MyFlow(FlowSpec):
    @step
    def start(self):
        logger.info("Starting the flow...")
        self.batch_pool_service = CFABatchPoolService(dotenv_path=my_dotenv_path)
        self.batch_pool_service.setup_pools()
        self.attributes = self.batch_pool_service.attributes
        self.next(self.perform_remote_read_arizona, self.perform_remote_read_california)

    @step
    @CFAAzureBatchDecorator(
        pool_name=attributes['POOL_NAME'],
        attributes=attributes_1,
        docker_command='Rscript main.r'
    )
    def perform_remote_read_arizona(self):
        print("Running the perform_remote_read_arizona step in Azure Batch...")
        self.next(self.join)

    @step
    @CFAAzureBatchDecorator(
        pool_name=attributes['POOL_NAME'],
        attributes=attributes_2,
        docker_command='python3 main.py'
    )
    def perform_remote_read_california(self):
        print(
            "Running the perform_remote_read_california step in Azure Batch..."
        )
        self.next(self.join)

    @step
    def join(self, inputs):
        print("Flow joined.")
        self.merge_artifacts(inputs)
        self.next(self.end)

    @step
    def end(self):
        self.batch_pool_service.delete_all_pools()
        print("Flow completed.")


if __name__ == "__main__":
    MyFlow()
