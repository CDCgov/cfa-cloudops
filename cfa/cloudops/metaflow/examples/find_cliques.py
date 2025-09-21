import logging

import numpy as np
from custom_metaflow.cfa_batch_pool_service import CFABatchPoolService
from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import (
    CFAAzureBatchDecorator,
)
from metaflow import FlowSpec, step

logger = logging.getLogger(__name__)


class CliqueFlow(FlowSpec):
    @step
    def start(self):
        logger.info("Starting the flow...")
        self.batch_pool_service = CFABatchPoolService(
            dotenv_path="cliques.env", job_config_file="bk_job.toml"
        )
        random_integers_list = [
            np.random.randint(2000, 3000)
            for _ in range(self.parallel_pool_limit)
        ]
        self.batch_pool_service.setup_pools()
        self.split_nodes = self.batch_pool_service.setup_step_parameters(
            random_integers_list
        )
        self.next(self.process_state, foreach="split_nodes")

    @step
    def process_state(self):
        # Dynamically apply the decorator
        decorator = CFAAzureBatchDecorator(
            pool_name=self.input["pool_name"],
            attributes=self.input["attributes"],
            job_configuration=self.input["job_configuration"],
            task_parameters=self.input["task_parameters"],
            docker_command=self.input["docker_command"],
        )
        decorator(self._process_state)()
        self.next(self.join)

    def _process_state(self):
        step_pool_name = self.input["pool_name"]
        step_parameters = self.input["task_parameters"]
        logger.info(
            f"Running the _process_state step in Azure Batch for pool {step_pool_name} with {len(step_parameters)} parameters which are {step_parameters}"
        )

    @step
    def join(self, inputs):
        logger.info("Flow joined.")
        self.merge_artifacts(inputs)
        self.next(self.end)

    @step
    def end(self):
        # self.batch_pool_service.delete_all_pools()
        logger.info("Flow completed.")


if __name__ == "__main__":
    CliqueFlow()
