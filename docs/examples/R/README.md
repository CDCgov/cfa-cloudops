# Running R Scripts with cfa-cloudops (launch_job.py Example)

This example shows how to use `cfa-cloudops` to run R scripts in Azure Batch using the
`launch_job.py` example script in this folder.

## What this example does

The `launch_job.py` script:

1. Instantiates a `CloudClient`.
2. Packages and uploads the local `Dockerfile` to Azure Container Registry (ACR).
3. Creates an Azure Batch pool using that container image.
4. Creates an Azure Batch job.
5. Adds task(s) that run `Rscript` commands.
6. Monitors the job until task completion.

## Prerequisites

- Python 3.10+
- Azure CLI installed and authenticated (`az login`)
- Access to the required Azure resources (Key Vault, ACR, Batch account)

## 1. Create a virtual environment and install cfa-cloudops

From this `docs/examples/R` directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install git+https://github.com/CDCgov/cfa-cloudops.git
```

## 2. Review and update launch_job.py

Open `launch_job.py` and replace placeholder values with your environment values:

- `keyvault="my_keyvault"`
- `registry_name="my_azure_registry"`

Also verify task commands match scripts available in your container image. As written,
the script submits:

- `Rscript /app/r_helloworld.r`
- `Rscript /app/r_helloworld.r --user 'CloudOps User'`


## 3. Run the example

```bash
python launch_job.py
```

You should see output for Docker upload/build, pool creation, job creation, task
submission, and monitoring updates.

## 4. Clean up resources (recommended)

After your run, delete test Batch resources to avoid ongoing costs.

You can do this in Python (for example, with `CloudClient` methods such as
`delete_job` and `delete_pool`) or via Azure CLI/Portal according to your team
workflow.
