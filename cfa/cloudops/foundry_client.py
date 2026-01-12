# agent_azure_foundry.py

from azure.ai.foundry import FoundryClient

client = FoundryClient(subscription_id="SUB_ID")

response = client.run_agent(
    agent_name="BatchOrchestrator", input="Create GPU pool and run 3 tasks"
)

print(response.status)
