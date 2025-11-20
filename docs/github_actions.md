# Using `cfa-cloudops` with GitHub Actions

The `cfa-cloudops` package was designed to require minimal changes when switching from local development to GitHub Actions execution. Please refer to [this example repo](https://github.com/cdcent/cfa-cloudops-example) for help structuring your GitHub repo to use `cfa-cloudops`. For this documentation, it is assumed the main way of using `cfa-cloudops` is with the `CloudClient`.

## Initializing

Existing code from a local workflow can easily be transferred to run in GitHub Actions. The main difference between execution environments is the initialization of the `CloudClient`. We use the parameter `use_federated=True` when initializing the client to coordinate with GitHub to use federated credentials. When we login with federated credentials during the workflow (more on this later), the client will then pick these up to use for authentication.

### Example

```python
cc = CloudClient(use_federated = True)
```

## Secrets

Just like using a .env file for local `cloudops` execution, GitHub needs a way to store/access environment variables. Because of the possible sensitivity of some of these values, we cannot store the .env file directly as a file in the repo. The values in the .env need to be included as Secrets in your repository's Actions Secrets And Variables section within the repo Settings. The names of these secrets should match the keys in the .env file. More info on this can be found [here](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets).

## Workflows

To run a workflow in github actions that utilizes `cfa-cloudops` we first need a yaml file in the .github/workflows folder. Then we provide several key components to the workflow file as follows. For more information see [here](https://github.com/cdcent/cfa-cloudops-example/blob/main/.github/workflows/azure_workflow.yaml).

- when to run the workflows
- permissions and runner
    - note that you need a self hosted runner with access to Azure for the workflow to succeed. Please reach out to the CFA Tools Team for any help using a self-hosted runner.
- checkout the repo
- login with federated credentials and the secrets stored as AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID
- setup the environment, such as python or installing dependencies
- run the script using `cfa-cloudops`
    - environment variables need to be passed in this step

### Example workflow file

```yaml
name: Run Azure Python Script

on:
  push:
    branches:
      - main
  workflow_dispatch:

jobs:
  run-python:
    permissions:
      id-token: write
      contents: read
    runs-on: [self-hosted, Linux, X64, <runner name>]

    steps:
      - name: Checkout Repo
        id: checkout_repo
        uses: actions/checkout@v5

      - name: Azure CLI Login
        uses: azure/login@v2
        with:
          client-id: ${{secrets.AZURE_CLIENT_ID}}
          tenant-id: ${{secrets.AZURE_TENANT_ID}}
          subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID}}

      - name: Setup Python Env
        uses: actions/setup-python@v6
        with:
          python-version: "3.11"

      - name: Install Python Packages
        run: |
          python3 -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Execute Python Script
        env:
          AZURE_BATCH_ACCOUNT: ${{ secrets.AZURE_BATCH_ACCOUNT }}
          AZURE_BLOB_STORAGE_ACCOUNT: ${{ secrets.AZURE_BLOB_STORAGE_ACCOUNT }}
          AZURE_CONTAINER_REGISTRY_ACCOUNT: ${{ secrets.AZURE_CONTAINER_REGISTRY_ACCOUNT }}
          AZURE_KEYVAULT_NAME: ${{ secrets.AZURE_KEYVAULT_NAME }}
          AZURE_KEYVAULT_SP_SECRET_ID: ${{ secrets.AZURE_KEYVAULT_SP_SECRET_ID }}
          AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_SUBNET_ID: ${{ secrets.AZURE_SUBNET_ID }}
          AZURE_SUBSCRIPTION_ID: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
          AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
          AZURE_USER_ASSIGNED_IDENTITY: ${{ secrets.AZURE_USER_ASSIGNED_IDENTITY }}
        run: python3 <python file name>
```
