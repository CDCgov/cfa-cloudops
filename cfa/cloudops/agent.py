import json

from openai import AzureOpenAI

from cfa.cloudops import CloudClient

SYSTEM_PROMPT = """
You are an autonomous cloud orchestration agent.
Your job is to create and manage Azure Batch pools, jobs and tasks.
When infrastructure actions are needed, call the appropriate tools.
Plan before acting. Act until the user goal is achieved.
"""

MODEL_NAME = "gpt-4.1"

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "create_pool",
            "description": "Create a compute pool",
            "parameters": {
                "type": "object",
                "properties": {
                    "pool_id": {"type": "string"},
                    "vm_size": {"type": "string"},
                    "node_count": {"type": "integer"},
                },
                "required": ["pool_id", "vm_size", "node_count"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_job",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "pool_id": {"type": "string"},
                },
                "required": ["job_id", "pool_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_task",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "task_id": {"type": "string"},
                    "command": {"type": "string"},
                },
                "required": ["job_id", "task_id", "command"],
            },
        },
    },
]


cloud_client = CloudClient()

client = AzureOpenAI(
    api_key="YOUR_KEY",  # pragma: allowlist secret
    azure_endpoint="YOUR_ENDPOINT",
    api_version="2024-02-01",
)


def run_agent(user_goal):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_goal},
    ]

    while True:
        response = client.chat.completions.create(
            model=MODEL_NAME, messages=messages, tools=TOOLS
        )

        msg = response.choices[0].message

        if msg.tool_calls:
            for call in msg.tool_calls:
                name = call.function.name
                args = json.loads(call.function.arguments)

                print(f"\nAgent calls: {name}({args})")

                if name == "create_pool":
                    cloud_client.create_pool(**args)
                elif name == "create_job":
                    cloud_client.create_job(**args)
                elif name == "add_task":
                    cloud_client.add_task(**args)

            messages.append(msg)

        else:
            print("\nAgent:", msg.content)
            break
