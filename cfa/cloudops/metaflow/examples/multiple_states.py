import logging
from metaflow import FlowSpec, step
from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import CFAAzureBatchDecorator
from custom_metaflow.cfa_batch_pool_service import CFABatchPoolService

logger = logging.getLogger(__name__)

class MyFlow(FlowSpec):
    @step
    def start(self):
        logger.info("Starting the flow...")
        self.all_states = []
        with open('states.txt', 'r') as file:
            all_states = file.read().splitlines()
        self.batch_pool_service = CFABatchPoolService(dotenv_path='metaflow.env')
        self.batch_pool_service.setup_pools()
        self.split_lists = self.batch_pool_service.setup_step_parameters(all_states)
        self.next(self.process_state, foreach='split_lists')
        
    @step
    def process_state(self):
        # Dynamically apply the decorator
        decorator = CFAAzureBatchDecorator(
            pool_name=self.input['pool_name'],
            attributes=self.input['attributes'],
            docker_command=f'python /input/exp/outlook_2507/eli_test_viz_US002/run_projection.py -s {",".join(self.input["parameters"])} log -l info -o both'
        )
        decorator(self._process_state)()
        self.next(self.join)

    def _process_state(self):
        step_pool_name = self.input['pool_name']
        step_parameters = self.input['parameters']
        logger.info(f"Running the _process_state step in Azure Batch for pool {step_pool_name} with {len(step_parameters)} parameters which are {step_parameters}")

    @step
    def join(self, inputs):
        logger.info("Flow joined.")
        self.merge_artifacts(inputs) 
        self.next(self.end)

    @step
    def end(self):
        self.batch_pool_service.delete_all_pools()
        logger.info("Flow completed.")


if __name__ == "__main__":
    MyFlow()