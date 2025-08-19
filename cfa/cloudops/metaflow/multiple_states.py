from metaflow import FlowSpec, step
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
plugins_folder = os.path.join(current_dir, "custom_metaflow", "plugins", "decorators")
if plugins_folder not in sys.path:
    sys.path.insert(0, plugins_folder)

from custom_metaflow.plugins.decorators.cfa_azure_batch_decorator import CFAAzureBatchDecorator
from custom_metaflow.cfa_batch_pool_service import CFABatchPoolService

class MyFlow(FlowSpec):
    @step
    def start(self):
        print("Starting the flow...")
        self.all_states = []
        with open('states.txt', 'r') as file:
            all_states = file.read().splitlines()
        self.batch_pool_service = CFABatchPoolService()
        self.batch_pool_service.setup_pools()
        self.split_lists = self.batch_pool_service.setup_step_parameters(all_states)
        self.next(self.process_state, foreach='split_lists')

    @step
    def process_state(self):
        # Dynamically apply the decorator
        decorator = CFAAzureBatchDecorator(
            pool_name=self.input['pool_name'],
            sp_secret=self.input['sp_secret'],
            config_file="client_config_states.toml", 
            docker_command=f'echo {self.input["parameters"]}'
        )
        decorator(self._process_state)()
        self.next(self.join)

    def _process_state(self):
        print(f"Running the _process_state step in Azure Batch for {self.input}...")

    @step
    def join(self, inputs):
        print("Flow joined.")
        self.next(self.end)

    @step
    def end(self):
        self.batch_pool_service.delete_all_pools()
        print("Flow completed.")


if __name__ == "__main__":
    MyFlow()