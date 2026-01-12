# agent_azure_openai_only.py

import json

from CloudClient import CloudClient
from openai import AzureOpenAI

cloud = CloudClient()

client = AzureOpenAI(
    api_key="KEY",  # pragma: allowlist secret
    azure_endpoint="ENDPOINT",
    api_version="2024-02-01",
)

SYSTEM = (
    "You are an agent that manages Azure Batch using create_pool, create_job, add_task."
)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "create_pool",
            "parameters": {
                "type": "object",
                "properties": {
                    "pool_id": {"type": "string"},
                    "vm_size": {"type": "string"},
                    "node_count": {"type": "integer"},
                },
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
            },
        },
    },
]

messages = [
    {"role": "system", "content": SYSTEM},
    {"role": "user", "content": "Create GPU pool and run 3 tasks"},
]

while True:
    r = client.chat.completions.create(
        model="agent-gpt4", messages=messages, tools=TOOLS
    )

    msg = r.choices[0].message

    if not msg.tool_calls:
        break

    for call in msg.tool_calls:
        args = json.loads(call.function.arguments)
        if call.function.name == "create_pool":
            cloud.create_pool(**args)
        elif call.function.name == "create_job":
            cloud.create_job(**args)
        elif call.function.name == "add_task":
            cloud.add_task(**args)

    messages.append(msg)
