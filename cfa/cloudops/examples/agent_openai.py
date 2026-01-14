import os

from openai import OpenAI

from cfa.cloudops import CloudClient

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# Initialize the CloudClient
cloud_client = CloudClient(
    dotenv_path="metaflow.env", use_sp=True
)  # Update with your .env path

FUNCTION_REGISTRY = {
    "create_pool": CloudClient.create_pool,
    "create_job": CloudClient.create_job,
    "add_task": CloudClient.add_task,
    # Add other CloudClient methods here
    "delete_pool": CloudClient.delete_pool,
    "delete_job": CloudClient.delete_job,
    "monitor_job": CloudClient.monitor_job,
}

# System prompt for the agent
SYSTEM_PROMPT = """
You are an intelligent Azure Batch orchestration agent. Your job is to manage Azure Batch resources using the CloudClient class.
You can create pools, jobs, and tasks by invoking the following functions:
1. create_pool(pool_name, vm_size, container_image_name, ...)
2. create_job(job_name, pool_name, ...)
3. add_task(job_name, command_line, ...)

Your task is to analyze the user's goal, determine the required sequence of actions, and execute them in the correct order.
Always ensure that:
- A pool exists before creating a job.
- A job exists before adding tasks.
- The schedule and recurrence requirements are respected.
"""


def interpret_goal(goal: str) -> dict:
    """
    Use OpenAI to interpret the user's goal and generate a plan.

    Args:
        goal (str): The user's goal as a natural language instruction.

    Returns:
        dict: A structured plan with actions and parameters.
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"User Goal: {goal}\n\nGenerate a structured plan as a Python dictionary.",
        },
    ]
    response = client.chat.completions.create(
        model="gpt-4",  # Use the appropriate model (e.g., gpt-4 or gpt-3.5-turbo)
        messages=messages,
        max_tokens=500,
        temperature=0,
    )
    plan = response.choices[0].message.content.strip()
    return eval(plan)  # Convert the generated string to a Python dictionary


def execute_plan(plan: dict, cloud_client: CloudClient):
    """
    Execute the plan by invoking the appropriate CloudClient methods.

    Args:
        plan (dict): The structured plan with actions and parameters.
    """
    for step in plan.get("actions", []):
        action = step["action"]
        params = step["parameters", {}]

        if action in FUNCTION_REGISTRY:
            print(f"Executing action: {action} with parameters: {params}")
            # Dynamically call the registered function with the CloudClient instance
            # FUNCTION_REGISTRY[action](cloud_client, **params)
        else:
            print(f"Unknown action: {action}")


def main():
    # Example goal
    goal = """
    Schedule a job that runs daily at 9 PM except weekends and computes the Measles inference
    using daily partition stored in input-test folder of CFAAzureBatchPrd storage account.
    Use the measles-script:v1 container for this job.
    """

    # Step 1: Interpret the goal
    print("Interpreting the goal...")
    plan = interpret_goal(goal)
    print("Generated Plan:", plan)

    # Step 2: Execute the plan
    print("Executing the plan...")
    execute_plan(plan, cloud_client)


if __name__ == "__main__":
    main()
