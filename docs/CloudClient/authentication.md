# Authentication with `cfa.cloudops.CloudClient`

Authentication with the `CloudClient` class is meant to be user-friendly while maintaining flexibility. There are three different ways to authenticate to the Azure environment, all of which center around environment variables for Azure account information. These environment variables can be pulled from the local environment or instantiated from a .env file specified during the `CloudClient` instantiation.

The three authentication methods available are:
- Managed Identity credential (default)
- Service Principal credential
- Federated Token credential

## Environment Variable Setup

When the `CloudClient` class gets instantiated, it attempts to get one of the three credentials listed above based on environment variables. These environment variables can be stored locally on your system before calling out to the `CloudClient` class. A potentially easier way is to store the required variables is in a .env file. This allows for easier changing of variables or sharing between individuals.

The path to the .env file can be provided via the `dotenv_path` parameter when calling `CloudClient()`. By default, it looks for a file called `.env`. If the name of the file is anything else, it should be passed to `dotenv_path`. For example, instantiating the client in the following ways would be identical:
```python
client = CloudClient()
client = CloudClient(dotenv_path = ".env")
```

If the .env file is called "my_azure.env" then the following should be run:
```python
client = CloudClient(dotenv_path = "my_azure.env")
```

During instantiation of the `CloudClient`, the variables from the .env file get added to the local environment variables, overriding any variables with the same name. Then all the environment variables from the local environment are used to create a cre
dential.

An example .env file can be found [here](../files/sample.env).

## Using Different Authentication Methods

### Managed Identity

The default method for authenticating to the Azure environment via the `CloudClient` is a Managed Identity. Data Scientists at CFA should already have identities associated with Azure in their development environment (VAP). Because of this, we can reduce the number of inputs to authenticate with Azure because your machine is already approved. This is the encouraged method when possible. When this method is used, we are able to pull in AZURE_SUBSCRIPTION_ID, AZURE_TENANT_ID, and AZURE_RESOURCE_GROUP_NAME from the linked subscription. Therefore, these values do not need to exist in the local environment or .env file.

To instantiate a `CloudClient` object using a Managed Identity credential, no additional arguments need to be passed in, except from `dotenv_path` if needed. For example:
```python3
client = CloudClient()
```

### Service Principal

Sometimes there are cases when a Managed Identity won't work or is not ideal. In this situation it is possible to authenticate with a Service Principal. If this is the case, set the `use_sp` parameter to `True` when instantiating the `CloudClient`. This method requires the existence of AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET to exist in the local environment variables or .env file, or these can be passed in to the `CloudClient` as lowercase parameters of the same name.

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

Federated token credentials are useful for interacting with Azure from GitHub Actions. Because you have to be signed into GitHub to authorize Actions, that authorization can be passed along to Azure with the right permissions configured in Azure. Federated token credentials can be used by setting the `use_federated` parameter to `True` when instantiating the `CloudClient`. Similar to the Service Principal authentication, the following additional environment variables or parameters are needed: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_SUBSCRIPTION_ID (all lowercase if passed as a parameter.)

The following is an example of including the values as parameters in the instantiation:
```python
client = CloudClient(
    use_federated = True,
    azure_tenant_id = "my_tenant_id",
    azure_client_id = "my_client_id"
)
```
