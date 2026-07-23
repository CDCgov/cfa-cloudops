# Authentication with `cfa.cloudops.CloudClient`

Authentication with the `CloudClient` class is meant to be user-friendly while maintaining flexibility. There are three different ways to authenticate to the Azure environment, all of which center around either a Key Vault or environment variables for Azure account information. A key vault name can be provided to pull necessary values for instantiating the CloudClient. Or these environment variables can be pulled from the local environment or instantiated from a .env file specified during the `CloudClient` instantiation.

The three authentication methods available are:

- Managed Identity credential (default)
- Service Principal credential
- Federated Token credential

## Using Key Vault Setup

When the `CloudClient` class gets instantiated, one way it attempts to get one of the three credentials listed above is by pulling values from the specified `keyvault`. The Key Vault to be used by CFA individuals can be found in the documentation [here](https://github.com/cdcent/cfa-cloudops-example). This will then pull the following values from the Key Vault:

- azure_batch_account
- azure_batch_location
- azure_user_assigned_identity
- azure_subnet_id
- azure_client_id
- azure_keyvault_sp_secret_id
- azure_blob_storage_account
- azure_container_registry_account

If the Key Vault is set up with these keys/values (the correct CFA Key Vault is), then no .env file is necessary. If a .env file is still provided, values from the .env file are used over what is stored in the Key Vault. If you want to use values in the Key Vault over the .env file, provide the flag `force_keyvault=True` when instantiating `CloudClient`. Note that if you are using a service principal, then "AZURE_TENANT_ID", "AZURE_SUBSCRIPTION_ID", "AZURE_CLIENT_ID", and "AZURE_CLIENT_SECRET" need to be in the .env file, saved as local environment variables, or passed to `CloudClient`.

For managed identity and service principal authentication, pass the Key Vault name directly with `keyvault="..."` when instantiating `CloudClient`.

For federated authentication (`use_federated=True`), if `keyvault` is not passed explicitly, `CloudClient` will check for `AZURE_KEYVAULT_NAME` in the environment and use that value if available.

For example, the following way pulls values from our Key Vault called 'my-key-vault'.

```python3
client = CloudClient(keyvault = "my-key-vault")
```

If we want to force the use of Key Vault values, the following should be run:

```python3
client = CloudClient(keyvault = "my-key-vault", force_keyvault = True)
```

## Environment Variable Setup

When the `CloudClient` class gets instantiated, another way it attempts to get one of the three credentials listed above is based on environment variables. These environment variables can be stored locally on your system before calling `CloudClient`. A potentially easier way is to store the required variables in a .env file. This allows for easier updates and sharing between individuals.

The path to the .env file can be provided via the `dotenv_path` parameter when calling `CloudClient()`. By default, it looks for a file called `.env`. If the file name is anything else, pass it to `dotenv_path`. For example, instantiating the client in the following ways is identical:
```python
client = CloudClient()
client = CloudClient(dotenv_path = ".env")
```

If the .env file is called "my_azure.env", then run the following:
```python
client = CloudClient(dotenv_path = "my_azure.env")
```

During instantiation of `CloudClient`, variables from the .env file are added to local environment variables, overriding any variables with the same name. Then all environment variables from the local environment are used to create a credential.

An example .env file can be found [here](../files/sample.env).

## Using Different Authentication Methods

### Managed Identity

The default method for authenticating to the Azure environment via the `CloudClient` is a Managed Identity. Data Scientists at CFA should already have identities associated with Azure in their development environment (VAP). Because of this, we can reduce the number of inputs to authenticate with Azure because your machine is already approved. This is the encouraged method when possible. When this method is used, we are able to pull in AZURE_SUBSCRIPTION_ID, AZURE_TENANT_ID, and AZURE_RESOURCE_GROUP_NAME from the linked subscription. Therefore, these values do not need to exist in the local environment or .env file.

To instantiate a `CloudClient` object using a Managed Identity credential, no additional arguments need to be passed, except `dotenv_path` if needed. For example:
```python
client = CloudClient()
```

### Service Principal

Sometimes a Managed Identity will not work or is not ideal. In this situation, you can authenticate with a Service Principal. If this is the case, set the `use_sp` parameter to `True` when instantiating `CloudClient`. This method requires AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET to exist in local environment variables or a .env file, or these can be passed to `CloudClient` as lowercase parameters of the same name.

Check [here](../files/sp_sample.env) for an example .env to be used with a service principal.

The following is an example of including the additional environment variables in the .env file.
```python
client = CloudClient(use_sp = True)
```

The following is an example of including these variables as part of the instantiation:
```python
client = CloudClient(
    use_sp = True,
    azure_tenant_id = "my_tenant_id",
    azure_subscription_id = "my_subscription_id",
    azure_client_id = "my_client_id",
    azure_client_secret = "my_sp_secret" #pragma: allowlist secret
)
```

### Federated Tokens

Federated token credentials are useful for interacting with Azure from GitHub Actions. Because you have to be signed into GitHub to authorize Actions, that authorization can be passed along to Azure with the right permissions configured in Azure. Federated token credentials can be passed to the `CloudClient` by setting the `use_federated` parameter to `True` when instantiating the `CloudClient`.

The following is an example of including the values as parameters in the instantiation:
```python
client = CloudClient(
    use_federated = True
)
```

#### Example
In practice, there are a few steps required for using CloudClient in GitHub Actions. In your repo, create a workflow file that contains the steps for your workflow. The workflow will need to run on a self-hosted runner with access to Azure in order to pull information from Azure back to the runner. We also need to use OIDC federated login with the `azure/login@v3` action. Secrets typically found in your .env file will need to be added as secrets to your GitHub repository, especially AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_SUBSCRIPTION_ID. In each action step, the appropriate environment variables will need to be loaded in the `env:` section of the action. Then the correct Python version and requirements can be loaded. Lastly, you can run a Python script using `cfa-cloudops` and the `use_federated` parameter mentioned above.

For a specific example, check [here](https://github.com/cdcent/cfa-cloudops-example).

## Checking Credentials

If you want to quickly verify that the active credential can access your Azure subscription(s), use `check_credentials`.

This method attempts to list subscriptions visible to the current credential and logs subscription information. It is useful for validating `.env` values, Key Vault setup, or federated login before creating pools/jobs.

### Example

```python
client = CloudClient()
client.check_credentials()
```

## Retrieving Secrets from Key Vault

If you need a single secret value at runtime, use `get_kv_secret`.

Inputs:

- `secret_name`: the secret key to retrieve
- `keyvault`: the Key Vault name (without `.vault.azure.net`)

This returns the secret value if found, or `None` if the lookup fails.

### Example

```python
client = CloudClient(keyvault="my-key-vault")
token = client.get_kv_secret(
    secret_name="my-secret-name", #pragma: allowlist secret
    keyvault="my-key-vault"
)
print(token is not None)
```
