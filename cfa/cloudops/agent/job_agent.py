import secrets

import yaml
from workflow_executor import WorkflowExecutor
from workflow_planner import generate_workflow

from cfa.cloudops import CloudClient

goal_1 = """
Create a job with 3 tasks using the cfaprdbatchcr.azurecr.io/simple_test_app:latest container.
The second task task depends on first task. Third task depend on the second task. Each task
will run the following command: "/bin/bash -c 'for i in $(seq 1 19); do echo \"$i\"; sleep 5; done'"
Monitor the job after creating it.
I want the output from each task to be stored in output-test blob container in a folder named stderror_stdoutput.
Create a subfolder in that folder for each run date in YYYY-MMM-DD format.
"""

goal_2 = """
Schedule a job that runs daily at 9 PM except weekends and computes the Measles
inference using daily partition stored in input-test folder of CFAAzureBatchPrd
storage account. Use the measles-script:v1 container for this job.
"""


def create_plan(goal):
    plan = generate_workflow(goal)
    goal_id = secrets.token_hex(4)
    with open(f"output/generated_workflow_{goal_id}.yaml", "w") as f:
        yaml.safe_dump(plan.model_dump(), f, sort_keys=False)
    print("Generated the following workflow:\n")
    print(plan.model_dump())
    return goal_id


def run_plan(goal_id: str):
    client = CloudClient(dotenv_path="metaflow.env", use_sp=True)
    client.cred.azure_resource_group_name = "EXT-EDAV-CFA-PRD"
    executor = WorkflowExecutor(
        client=client,
        dependency_yaml_path="config/operational_dependencies.yaml",
        dry_run=False,
    )
    executor.execute(f"output/generated_workflow_{goal_id}.yaml")


if __name__ == "__main__":
    goal_id = create_plan(goal_1)
    choice = input("\nProceed with execution? (Y/N): ").lower()
    if choice == "y":
        run_plan(goal_id)
