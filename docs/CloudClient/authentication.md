# Authentication with `cfa.cloudops.CloudClient`

Authentication with the `CloudClient` class is meant to be user-friendly while maintaining flexibility. The main authentication to the Azure environment is handled by the DefaultAzureCredential which uses either a Key Vault or environment variables for Azure account information. A key vault name can be provided to pull necessary values for instantiating the CloudClient, thus removing the need for a .env. Or these environment variables can be pulled from the local environment or instantiated from a .env file specified during the `CloudClient` instantiation.

## Using Key Vault Setup

When the `CloudClient` class gets instantiated, one way it attempts to create a valid credential listed above is by pulling values from the specified `keyvault`. The Key Vault to be used by CFA individuals can be found in the documentation [here](https://github.com/cdcent/cfa-cloudops-example). This will then pull the following values from the Key Vault:

- azure_batch_account
- azure_batch_location
- azure_user_assigned_identity
- azure_subnet_id
- azure_client_id
- azure_keyvault_sp_secret_id
- azure_blob_storage_account
- azure_container_registry_account
- azure_tenant_id
- azure_subscription_id
- azure_resource_group_name

If the Key Vault is setup with these keys/values (the correct CFA key vault is), then no .env file is necessary. If a .env is still provided, then values from the .env will be used over what is stored in the key vault. If you desire to use values in the keyvault over the .env, provide the flag `force_keyvault=True` when instantiating the `CloudClient`. Note that if you desire to use a service principal then "AZURE_TENANT_ID","AZURE_SUBSCRIPTION_ID", "AZURE_CLIENT_ID", and "AZURE_CLIENT_SECRET" need to be in the .env file, saved as local environment variables, or passed to the `CloudClient`.


The following way pulls values from our Key Vault called 'my-key-vault'.

```python3
client = CloudClient(keyvault="my-key-vault")
```

If we want to force the use of Key Vault values, the following should be run:

```python3
client = CloudClient(keyvault="my-key-vault", force_keyvault=True)
```

## Environment Variable Setup

When the `CloudClient` class gets instantiated, the other way it attempts to get one of the three credentials listed above is based on environment variables. These environment variables can be stored locally on your system before calling out to the `CloudClient` class. A potentially easier way is to store the required variables in a .env file. This allows for easier changing of variables or sharing between individuals.

The path to the .env file can be provided via the `dotenv_path` parameter when calling `CloudClient()`. By default, it looks for a file called `.env`. If the name of the file is anything else, it should be passed to `dotenv_path`. For example, instantiating the client in the following ways would be identical:
```python
client = CloudClient()
client = CloudClient(dotenv_path=".env")
```

If the .env file is called "my_azure.env" then the following should be run:
```python
client = CloudClient(dotenv_path="my_azure.env")
```

During instantiation of the `CloudClient`, values from the .env file are loaded into the process environment.
By default (`override_env=False`), existing environment variables are preserved and only missing values are populated.
If you set `override_env=True`, .env values overwrite existing environment variables.
Then all environment variables are used to build credentials.

An example .env file can be found [here](../files/sample.env).

## Advanced: Default Credential Chain Options

`CloudClient` authentication is powered by `DefaultCredentialHandler`, which builds a chained Azure credential very similar to the DefaultAzureCredential.
You can tune the credential chain with keyword arguments when creating a `DefaultCredentialHandler` to exclude certain credentials or specify which Managed Identity to use.

Supported options:

- `exclude_environment_credential`
- `exclude_workload_identity_credential`
- `exclude_managed_identity_credential`
- `exclude_visual_studio_code_credential`
- `exclude_azure_cli_credential`
- `exclude_azure_developer_cli_credential`
- `managed_identity_client_id` (user-assigned managed identity)

Note that direct instantiation of the DefaultCredentialHandler is usually not recommended. It is primarily used as the underlying credential handler for the CloudClient.

Example:

```python
from cfa.cloudops.auth import DefaultCredentialHandler

handler = DefaultCredentialHandler(
    dotenv_path=".env",
    exclude_environment_credential=True,
    exclude_azure_cli_credential=True,
    managed_identity_client_id="<managed-identity-client-id>",
)
```

These options are useful when you want deterministic credential selection in environments where multiple Azure login methods are available.

## Logging During Initialization

Package logging can still be controlled with the `LOG_LEVEL` and `LOG_OUTPUT` environment variables, but `CloudClient` now also supports direct logging overrides during initialization. This is useful when you want debug output from the client constructor itself without setting shell-level environment variables first.

For example, to enable debug logs to stdout during client creation:

```python
client = CloudClient(
    log_level="debug",
    log_output="stdout",
)
```

Supported `log_level` values are `none`, `debug`, `info`, `warning`/`warn`, `error`, and `critical`. Supported `log_output` values are `stdout`, `file`, and `both`.

## Using Different Authentication Methods

There are a few different methods for authentication within the DefaultAzureCredential. These are environment credentials, workload credentials, managed identity credentials, and Azure CLI credentials. The DefaultAzurecredential will choose the method to authenticate based on the environment variables and system settings available.

### Managed Identity

The most common method for authenticating to the Azure environment via the `CloudClient` is likely a Managed Identity. Data Scientists at CFA should already have identities associated with Azure in their development environment (VAP). Because of this, we can reduce the number of inputs to authenticate with Azure because your machine is already approved. This is the encouraged method when possible. When this method is used, we are able to pull in AZURE_SUBSCRIPTION_ID, AZURE_TENANT_ID, and AZURE_RESOURCE_GROUP_NAME from the linked subscription. Therefore, these values do not need to exist in the local environment or .env file.

To instantiate a `CloudClient` object using a Managed Identity credential, no additional arguments need to be passed in, except from `dotenv_path` if needed. If you want to specify which managed identity is used, you can set the client ID for the Managed Identity with the parameter `managed_identity_client_id`. For example:

### Service Principal

Sometimes there are cases when a Managed Identity won't work or is not ideal. In this situation it is possible to authenticate with a Service Principal. This method requires the existence of AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET to exist in the local environment variables or .env file, or these can be passed in to the `CloudClient` as lowercase parameters of the same name.

Check [here](../files/sp_sample.env) for an example .env to be used with a service principal.

The following is an example of including these variables as part of the instantiation:
```python
client = CloudClient(
    azure_tenant_id="my_tenant_id",
    azure_subscription_id="my_subscription_id",
    azure_client_id="my_client_id",
    azure_client_secret="my_sp_secret",  # pragma: allowlist secret
)
```

### Federated Tokens

Federated token credentials are useful for interacting with Azure from GitHub Actions. Because you have to be signed into GitHub to authorize Actions, that authorization can be passed along to Azure with the right permissions configured in Azure. Federated token credentials are automatically picked up for the credential if they are granted to the GitHub repo.

### Azure CLI

Another common method for creating a credential is using the Azure CLI. Simply run `az login` or `az login --identity` in your terminal before running cloudops code and the CLI credential will be picked up by the CloudClient.

#### Example
In practice, there are a few steps required for using the CloudClient in GitHub Actions. In your repo, create a workflow file that contains the steps for your workflow. The workflow will need to run on a self-hosted runner with access to Azure in order to pull information from Azure back to the runner. We also need to use OIDC Federated login using the azure/login@v3 action. Secrets typically found in your .env file will need to be added as secrets to your GitHub repository, especially AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_SUBSCRIPTION_ID. In each of the action steps, the appropriate environment variables will need to be loaded in the `env:` section of the action. Then the correct Python version and requirements can be loaded. Lastly, run the Python script using `cfa-cloudops`; the credential chain selects the workload identity automatically.

For a specific example, check [here](https://github.com/cdcent/cfa-cloudops-example).
