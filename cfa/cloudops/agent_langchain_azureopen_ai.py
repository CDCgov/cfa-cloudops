# agent_langchain.py

from CloudClient import CloudClient
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.chat_models import AzureChatOpenAI
from langchain.tools import tool

cloud = CloudClient()


@tool
def create_pool(pool_id: str, vm_size: str, node_count: int):
    cloud.create_pool(pool_id, vm_size, node_count)


@tool
def create_job(job_id: str, pool_id: str):
    cloud.create_job(job_id, pool_id)


@tool
def add_task(job_id: str, task_id: str, command: str):
    cloud.add_task(job_id, task_id, command)


llm = AzureChatOpenAI(
    azure_endpoint="ENDPOINT",
    api_key="KEY",  # pragma: allowlist secret
    deployment_name="agent-gpt4",
    api_version="2024-02-01",
)

agent = create_openai_tools_agent(
    llm, tools=[create_pool, create_job, add_task], prompt="You manage Azure Batch."
)

executor = AgentExecutor(
    agent=agent, tools=[create_pool, create_job, add_task], verbose=True
)

executor.invoke({"input": "Create GPU pool and run 3 tasks"})
