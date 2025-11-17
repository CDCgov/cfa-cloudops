# ContainerAppClient Overview

The `ContainerAppClient` class provides a Python interface for managing Azure Container Apps jobs using the Azure SDK. It supports inspection, listing, existence checks, and starting jobs with custom commands and environment variables.

## Features
- Authenticate using Azure Managed Identity and environment variables
- List all jobs in a resource group
- Retrieve detailed job information
- Inspect container commands, images, and environment variables
- Check if a job exists
- Start jobs with custom commands, arguments, and environment variables
- Stop a running container app job

## Setup

1. **Install dependencies**
   - The ContainerAppClient is available in the `cfa-cloudops` package. Install the package by executing the following code in your terminal.

   ```bash
   pip install git+https://github.com/CDCgov/cfa-cloudops.git
   ```

2. **Environment variables**
   - The client uses Managed Identity credentialing by default, which can then pull in the subscription ID, resource group name, and tenant Id.
   - It's possible to pass in specific variables or use environment variables for Azure authentication:
     - `AZURE_SUBSCRIPTION_ID`
     - `AZURE_RESOURCE_GROUP_NAME`
     - `AZURE_TENANT_ID`
   - You can use a `.env` file and pass its path to the client, or set these variables manually.

## Usage Example

```python
from cfa.cloudops import ContainerAppClient

# Simplest instantiation using Managed Identity
client = ContainerAppClient()

# Initialize the client (dotenv_path is optional)
client = ContainerAppClient(dotenv_path=".env", resource_group="my-rg", subscription_id="xxxx-xxxx", job_name="my-job")

# List all jobs in the resource group
jobs = client.list_jobs()
print("Jobs:", jobs)

# Check if a job exists
exists = client.check_job_exists("my-job")
print("Job exists:", exists)

# Get job information
info = client.get_job_info("my-job")
print("Job info:", info)

# Get command and environment info for containers in a job
cmd_info = client.get_command_info("my-job")
print("Command info:", cmd_info)

# Start a job (optionally override command, args, env)
client.start_job(
    job_name="my-job",
    command=["python", "main.py"],
    args=["--input", "data.csv"],
    env=[{"name": "ENV_VAR", "value": "value"}]
)

# Stop a job
client.stop_job(
    job_name="my-job",
    job_execution_name="my-job-xxxxxxx"
)
```

## Method Reference

- `__init__(dotenv_path, resource_group, subscription_id, job_name)`
  - Initializes the client and loads environment variables.
- `list_jobs()`
  - Returns a list of job names in the resource group.
- `check_job_exists(job_name)`
  - Returns `True` if the job exists, `False` otherwise.
- `get_job_info(job_name)`
  - Returns a dictionary of job details.
- `get_command_info(job_name)`
  - Returns a list of container info dicts (name, image, command, args, env).
- `start_job(job_name, command, args, env)`
  - Starts a job, optionally overriding command, args, and environment variables.

- `stop_job(job_name, job_execution_name)`
  - Stops the specified job execution.

## Notes

- The client uses Azure Managed Identity for authentication. Ensure your environment supports this (e.g., Azure VM, App Service, or configure credentials).
- If you do not provide `resource_group`, `subscription_id`, or `job_name`, the client will attempt to use environment variables or values from the `.env` file.
- All operations are logged using Python's `logging` module for easier debugging.
