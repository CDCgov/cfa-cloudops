"""
Helper functions for Azure authentication.
"""

import logging
import os
from dataclasses import dataclass
from functools import cached_property, partial

from azure.batch import models
from azure.common.credentials import ServicePrincipalCredentials
from azure.core.pipeline import PipelineContext, PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline.transport import HttpRequest
from azure.identity import (
    ClientSecretCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.keyvault.secrets import SecretClient
from azure.mgmt.resource import SubscriptionClient
from dotenv import load_dotenv
from msrest.authentication import BasicTokenAuthentication

import cfa.cloudops.defaults as d
from cfa.cloudops.config import get_config_val
from cfa.cloudops.endpoints import (
    construct_azure_container_registry_endpoint,
    construct_batch_endpoint,
    construct_blob_account_endpoint,
    is_valid_acr_endpoint,
)
from cfa.cloudops.util import ensure_listlike

logger = logging.getLogger(__name__)


@dataclass
class CredentialHandler:
    """Data structure for Azure credentials.

    Lazy and cached: credentials are retrieved from a keyvault only when needed
    and are cached thereafter.
    """

    azure_subscription_id: str = None
    azure_resource_group_name: str = None
    azure_user_assigned_identity: str = None
    azure_subnet_id: str = None

    azure_keyvault_endpoint: str = None
    azure_keyvault_sp_secret_id: str = None
    azure_tenant_id: str = None
    azure_client_id: str = None
    azure_batch_endpoint_subdomain: str = d.default_azure_batch_endpoint_subdomain
    azure_batch_account: str = None
    azure_batch_location: str = d.default_azure_batch_location
    azure_batch_resource_url: str = d.default_azure_batch_resource_url
    azure_blob_storage_endpoint_subdomain: str = (
        d.default_azure_blob_storage_endpoint_subdomain
    )
    azure_blob_storage_account: str = None

    azure_container_registry_account: str = None
    azure_container_registry_domain: str = d.default_azure_container_registry_domain
    method: str = None

    def require_attr(self, attributes: str | list[str], goal: str = None):
        """Check that attributes required for a given operation are defined.

        Raises an informative error message if the required attribute is not defined.

        Args:
            attributes: String or list of strings naming the required attribute(s).
            goal: String naming the value that the attributes are required for obtaining,
                to make error messages more informative. If None, use a more generic message.

        Raises:
            AttributeError: If any required ``attributes`` are None.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.require_attr(["azure_tenant_id"], "authentication")
            AttributeError: A non-None value for attribute azure_tenant_id is required...
        """
        attributes = ensure_listlike(attributes)
        err_msgs = []
        for attr in attributes:
            attr_val = getattr(self, attr)
            if attr_val is None:
                err_msg = (f"A non-None value for attribute {attr} is required ") + (
                    f"to obtain a value for {goal}."
                    if goal is not None
                    else "for this operation."
                )
                err_msgs.append(err_msg)
        if err_msgs:
            raise AttributeError("\n".join(err_msgs))

    @property
    def azure_batch_endpoint(self) -> str:
        """Azure batch endpoint URL.

        Constructed programmatically from account name, location, and subdomain.

        Returns:
            str: The endpoint URL.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_batch_account = "mybatchaccount"
            >>> handler.azure_batch_location = "eastus"
            >>> handler.azure_batch_endpoint_subdomain = "batch.azure.com"
            >>> handler.azure_batch_endpoint
            'https://mybatchaccount.eastus.batch.azure.com'
        """
        logger.debug("Constructing Azure Batch endpoint URL.")
        self.require_attr(
            [
                "azure_batch_account",
                "azure_batch_endpoint_subdomain",
            ],
            goal="Azure batch endpoint URL",
        )
        logger.debug(
            "All required attributes present for Azure Batch endpoint URL. Constructing..."
        )
        endpoint = construct_batch_endpoint(
            self.azure_batch_account,
            self.azure_batch_location,
            self.azure_batch_endpoint_subdomain,
        )
        logger.debug(f"Constructed Azure Batch endpoint URL: {endpoint}")
        return endpoint

    @property
    def azure_blob_storage_endpoint(self) -> str:
        """Azure blob storage endpoint URL.

        Constructed programmatically from the account name and endpoint subdomain.

        Returns:
            str: The endpoint URL.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_blob_storage_account = "mystorageaccount"
            >>> handler.azure_blob_storage_endpoint_subdomain = "blob.core.windows.net"
            >>> handler.azure_blob_storage_endpoint
            'https://mystorageaccount.blob.core.windows.net'
        """
        logger.debug("Constructing Azure Blob account endpoint URL.")
        self.require_attr(
            [
                "azure_blob_storage_account",
                "azure_blob_storage_endpoint_subdomain",
            ],
            goal="Azure blob storage endpoint URL",
        )
        logger.debug(
            "All required attributes present for Azure Blob endpoint URL. Constructing..."
        )
        endpoint = construct_blob_account_endpoint(
            self.azure_blob_storage_account,
            self.azure_blob_storage_endpoint_subdomain,
        )
        logger.debug(f"Constructed Azure Blob endpoint URL: {endpoint}")
        return endpoint

    @property
    def azure_container_registry_endpoint(self) -> str:
        """Azure container registry endpoint URL.

        Constructed programmatically from the account name and registry domain.

        Returns:
            str: The endpoint URL.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_container_registry_account = "myregistry"
            >>> handler.azure_container_registry_domain = "azurecr.io"
            >>> handler.azure_container_registry_endpoint
            'myregistry.azurecr.io'
        """
        logger.debug("Constructing Azure Container Registry endpoint URL.")
        self.require_attr(
            [
                "azure_container_registry_account",
                "azure_container_registry_domain",
            ],
            goal="Azure container registry endpoint URL",
        )
        logger.debug(
            "All required attributes present for Azure Container Registry endpoint URL. Constructing..."
        )
        registry_endpoint = construct_azure_container_registry_endpoint(
            self.azure_container_registry_account,
            self.azure_container_registry_domain,
        )
        logger.debug(
            f"Constructed Azure Container Registry endpoint URL: {registry_endpoint}"
        )
        return registry_endpoint

    @cached_property
    def user_credential(self) -> ManagedIdentityCredential:
        """Azure user credential.

        Returns:
            ManagedIdentityCredential: The Azure user credential using ManagedIdentityCredential.

        Example:
            >>> handler = CredentialHandler()
            >>> credential = handler.user_credential
            >>> # Use credential with Azure SDK clients
        """
        logger.debug("Creating ManagedIdentityCredential for user.")
        return ManagedIdentityCredential()

    @cached_property
    def service_principal_secret(self):
        """A service principal secret retrieved from Azure Key Vault.

        Returns:
            str: The secret value.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_keyvault_endpoint = "https://myvault.vault.azure.net/"
            >>> handler.azure_keyvault_sp_secret_id = "my-secret"
            >>> secret = handler.service_principal_secret
        """
        logger.debug("Retrieving service principal secret from Azure Key Vault.")
        self.require_attr(
            ["azure_keyvault_endpoint", "azure_keyvault_sp_secret_id"],
            goal="service_principal_secret",
        )
        if self.method == "default":
            logger.debug(
                "Using default credential method for service principal secret."
            )
            cred = self.default_credential
        elif self.method == "sp":
            logger.debug(
                "Using service principal credential method for service principal secret."
            )
            return self.azure_client_secret
        else:
            logger.debug("Using user credential method for service principal secret.")
            cred = self.user_credential
        logger.debug(
            "All required attributes present for service principal secret. Retrieving..."
        )
        secret = get_sp_secret(
            self.azure_keyvault_endpoint,
            self.azure_keyvault_sp_secret_id,
            cred,
        )
        logger.debug("Retrieved service principal secret from Azure Key Vault.")
        logger.info(
            f"Retrieved secret '{self.azure_keyvault_sp_secret_id}' from Azure Key Vault."
        )
        return secret

    @cached_property
    def default_credential(self):
        logger.debug("Creating DefaultCredential.")
        return DefaultCredential()

    @cached_property
    def batch_service_principal_credentials(self):
        """Service Principal credentials for authenticating to Azure Batch.

        Returns:
            ServicePrincipalCredentials: The credentials configured for Azure Batch access.

        Example:
            >>> handler = CredentialHandler()
            >>> # Set required attributes...
            >>> credentials = handler.batch_service_principal_credentials
            >>> # Use with Azure Batch client
        """
        logger.debug("Creating ServicePrincipalCredentials for Azure Batch.")
        self.require_attr(
            [
                "azure_tenant_id",
                "azure_client_id",
                "azure_batch_resource_url",
            ],
            goal="batch_service_principal_credentials",
        )
        logger.debug(
            "All required attributes present for Azure Batch Service Principal credentials. Creating..."
        )
        spcred = ServicePrincipalCredentials(
            client_id=self.azure_client_id,
            tenant=self.azure_tenant_id,
            secret=self.service_principal_secret,
            resource=self.azure_batch_resource_url,
        )
        logger.debug("Created ServicePrincipalCredentials for Azure Batch.")
        return spcred

    @cached_property
    def client_secret_sp_credential(self):
        """A client secret credential created using the service principal secret.

        Returns:
            ClientSecretCredential: The credential configured with service principal details.

        Example:
            >>> handler = CredentialHandler()
            >>> # Set required attributes...
            >>> credential = handler.client_secret_sp_credential
            >>> # Use with Azure SDK clients
        """
        logger.debug("Creating ClientSecretCredential using service principal secret.")
        self.require_attr(["azure_tenant_id", "azure_client_id"])
        logger.debug(
            "All required attributes present for ClientSecretCredential. Creating..."
        )
        cscred = ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_secret=self.service_principal_secret,
            client_id=self.azure_client_id,
        )
        logger.debug("Created ClientSecretCredential using service principal secret.")
        return cscred

    @cached_property
    def client_secret_credential(self):
        """A client secret credential created using the azure_client_secret attribute.

        Returns:
            ClientSecretCredential: The credential configured with client secret details.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_tenant_id = "tenant-id"
            >>> handler.azure_client_id = "client-id"
            >>> handler.azure_client_secret = "client-secret" #pragma: allowlist secret
            >>> credential = handler.client_secret_credential
        """
        logger.debug("Creating ClientSecretCredential using azure_client_secret.")
        self.require_attr(
            [
                "azure_tenant_id",
                "azure_client_id",
                "azure_client_secret",
            ]
        )
        logger.debug(
            "All required attributes present for ClientSecretCredential. Creating..."
        )
        client_sec_cred = ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_secret=self.azure_client_secret,
            client_id=self.azure_client_id,
        )
        logger.debug("Created ClientSecretCredential using azure_client_secret.")
        return client_sec_cred

    @cached_property
    def compute_node_identity_reference(self):
        """An object defining a compute node identity reference.

        Specifically, a ComputeNodeIdentityReference object associated to the
        CredentialHandler's user-assigned identity.

        Returns:
            models.ComputeNodeIdentityReference: The identity reference.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_user_assigned_identity = "/subscriptions/.../resourceGroups/..."
            >>> identity_ref = handler.compute_node_identity_reference
        """
        logger.debug("Creating ComputeNodeIdentityReference.")
        self.require_attr(
            ["azure_user_assigned_identity"],
            goal="Compute node identity reference",
        )
        logger.debug(
            "All required attributes present for ComputeNodeIdentityReference. Creating..."
        )
        comp_id_ref = models.ComputeNodeIdentityReference(
            resource_id=self.azure_user_assigned_identity
        )
        logger.debug("Created ComputeNodeIdentityReference.")
        return comp_id_ref

    @cached_property
    def azure_container_registry(self):
        """An object pointing to an Azure Container Registry.

        Specifically, a ContainerRegistry instance corresponding to the particular
        Azure Container Registry account specified in the CredentialHandler, if any,
        with authentication via the compute_node_identity_reference defined by
        CredentialHandler, if any.

        Returns:
            models.ContainerRegistry: A properly instantiated ContainerRegistry object.

        Raises:
            ValueError: If the container registry endpoint is invalid.

        Example:
            >>> handler = CredentialHandler()
            >>> # Set required attributes...
            >>> registry = handler.azure_container_registry
        """
        logger.debug("Creating Azure Container Registry ContainerRegistry instance.")
        self.require_attr(
            [
                "azure_container_registry_account",
                "azure_container_registry_domain",
                "azure_user_assigned_identity",
            ],
            goal=("Azure Container Registry `ContainerRegistry` instance"),
        )
        logger.debug(
            "All required attributes present for Azure Container Registry. Validating endpoint..."
        )
        valid, msg = is_valid_acr_endpoint(self.azure_container_registry_endpoint)
        if not valid:
            logger.error(f"Invalid Azure Container Registry endpoint: {msg}")
            raise ValueError(msg)
        logger.debug(
            "Azure Container Registry endpoint is valid. Creating ContainerRegistry instance..."
        )
        cont_reg = models.ContainerRegistry(
            user_name=self.azure_container_registry_account,
            registry_server=self.azure_container_registry_endpoint,
            identity_reference=self.compute_node_identity_reference,
        )
        logger.debug("Created Azure Container Registry ContainerRegistry instance.")
        return cont_reg


class DefaultCredential(BasicTokenAuthentication):
    def __init__(
        self,
        credential=None,
        resource_id="https://batch.core.windows.net/.default",
        **kwargs,
    ):
        """Initialize a DefaultCredential.

        Args:
            credential: Azure credential instance. If None, uses DefaultAzureCredential.
            resource_id: Azure resource ID for authentication scope.
                Default is "https://batch.core.windows.net/.default".
            **kwargs: Additional keyword arguments passed to BearerTokenCredentialPolicy.
        """
        logger.debug("Initializing DefaultCredential.")
        super(DefaultCredential, self).__init__(None)
        if credential is None:
            logger.debug("No credential provided, using DefaultAzureCredential.")
            credential = DefaultAzureCredential()
        self.credential = credential
        self._policy = BearerTokenCredentialPolicy(credential, resource_id, **kwargs)

    def _make_request(self):
        logger.debug("Making fake PipelineRequest to obtain token.")
        return PipelineRequest(
            HttpRequest("CredentialWrapper", "https://batch.core.windows.net"),
            PipelineContext(None),
        )

    def set_token(self):
        """Ask the azure-core BearerTokenCredentialPolicy policy to get a token.
        Using the policy gives us for free the caching system of azure-core.
        """
        logger.debug("Setting token using BearerTokenCredentialPolicy.")
        request = self._make_request()
        self._policy.on_request(request)
        # Read Authorization, and get the second part after Bearer
        token = request.http_request.headers["Authorization"].split(" ", 1)[1]
        self.token = {"access_token": token}
        logger.debug("Set the token.")

    def get_token(self, *scopes, **kwargs):
        """Get an access token for the specified scopes.

        Args:
            *scopes: Variable number of scope strings to request access for.
            **kwargs: Additional keyword arguments passed to the underlying credential.

        Returns:
            AccessToken: Token object with access token and expiration information.
        """
        # Pass get_token call to credential
        logger.debug("Getting token from underlying credential.")
        return self.credential.get_token(*scopes, **kwargs)

    def signed_session(self, session=None):
        """Create a signed session with authentication token.

        Args:
            session: Optional existing session to modify. If None, creates a new session.

        Returns:
            Session: A signed session object with authentication headers.
        """
        logger.debug("Creating signed session with updated token.")
        self.set_token()
        return super(DefaultCredential, self).signed_session(session)


class EnvCredentialHandler(CredentialHandler):
    """Azure Credentials populated from available environment variables.

    Subclass of CredentialHandler that populates attributes from environment
    variables at instantiation, with the opportunity to override those values
    via keyword arguments passed to the constructor.

    Args:
        dotenv_path (str, optional): Path to .env file to load environment variables from.
            If None, uses default .env file discovery.
        **kwargs: Keyword arguments defining additional attributes or overriding
            those set in the environment variables. Passed as the ``config_dict``
            argument to ``config.get_config_val``.

    Example:
        >>> # Load from environment variables
        >>> handler = EnvCredentialHandler()

        >>> # Override specific values
        >>> handler = EnvCredentialHandler(azure_tenant_id="custom-tenant-id")

        >>> # Load from custom .env file
        >>> handler = EnvCredentialHandler(dotenv_path="/path/to/.env")
    """

    def __init__(
        self,
        dotenv_path: str = None,
        keyvault: str = None,
        force_keyvault: bool = False,
        **kwargs,
    ) -> None:
        """Initialize the EnvCredentialHandler.

        Loads environment variables from .env file and populates credential attributes from them.

        Args:
            dotenv_path (str, optional): Path to .env file to load environment variables from.
                If None, uses default .env file discovery.
            keyvault (str, optional): Name of the Azure Key Vault to use for secrets.
            force_keyvault (bool, optional): If True, forces loading of Key Vault secrets even if they are already set in the environment.
            **kwargs: Additional keyword arguments to override specific credential attributes.
        """
        logger.debug("Initializing EnvCredentialHandler.")
        load_env_vars(
            dotenv_path=dotenv_path,
            keyvault_name=keyvault,
            force_keyvault=force_keyvault,
        )

        get_conf = partial(get_config_val, config_dict=kwargs, try_env=True)

        for key in self.__dataclass_fields__.keys():
            self.__setattr__(key, get_conf(key))
        # set method to "env"
        self.__setattr__("method", "env")
        # check for azure batch location
        if self.__getattribute__("azure_batch_location") is None:
            self.__setattr__("azure_batch_location", d.default_azure_batch_location)


def load_env_vars(
    dotenv_path=None, keyvault_name: str = None, force_keyvault: bool = False
):
    """Load environment variables and Azure subscription information.

    Loads variables from a .env file (if specified), retrieves Azure subscription
    information using ManagedIdentityCredential, and sets default environment variables.

    Args:
        dotenv_path: Path to .env file to load. If None, uses default .env file discovery.
        keyvault_name: Name of the Azure Key Vault to use for secrets.
        force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.

    Example:
        >>> load_env_vars()  # Load from default .env
        >>> load_env_vars("/path/to/.env")  # Load from specific file
    """
    # get ManagedIdentityCredential
    mid_cred = ManagedIdentityCredential()

    # delete existing kv keys
    for key in d.default_kv_keys:
        del os.environ[key]

    logger.debug("Loading environment variables.")
    load_dotenv(dotenv_path=dotenv_path, override=True)

    sub_c = SubscriptionClient(mid_cred)
    # pull in account info and save to environment vars
    account_info = list(sub_c.subscriptions.list())[0]
    os.environ["AZURE_SUBSCRIPTION_ID"] = account_info.subscription_id
    os.environ["AZURE_TENANT_ID"] = account_info.tenant_id
    os.environ["AZURE_RESOURCE_GROUP_NAME"] = account_info.display_name

    # delete existing kv keys
    for key in d.default_kv_keys:
        del os.environ[key]

    # get Key Vault secrets
    if keyvault_name is not None:
        get_keyvault_vars(
            keyvault_name=keyvault_name,
            credential=mid_cred,
            force_keyvault=force_keyvault,
        )

    # save default values
    d.set_env_vars()


class SPCredentialHandler(CredentialHandler):
    def __init__(
        self,
        azure_tenant_id: str = None,
        azure_subscription_id: str = None,
        azure_client_id: str = None,
        azure_client_secret: str = None,
        dotenv_path: str = None,
        keyvault: str = None,
        force_keyvault: bool = False,
        **kwargs,
    ):
        """Initialize a Service Principal Credential Handler.

        Creates a credential handler that uses Azure Service Principal authentication
        for accessing Azure resources. Credentials can be provided directly as parameters
        or loaded from environment variables. If not provided directly, the handler will
        attempt to load credentials from environment variables or a .env file.

        Args:
            azure_tenant_id: Azure Active Directory tenant ID. If None, will attempt
                to load from AZURE_TENANT_ID environment variable.
            azure_subscription_id: Azure subscription ID. If None, will attempt
                to load from AZURE_SUBSCRIPTION_ID environment variable.
            azure_client_id: Azure Service Principal client ID (application ID).
                If None, will attempt to load from AZURE_CLIENT_ID environment variable.
            azure_client_secret: Azure Service Principal client secret. If None, will
                attempt to load from AZURE_CLIENT_SECRET environment variable.
            dotenv_path: Path to .env file to load environment variables from.
                If None, uses default .env file discovery.
            keyvault: Name of the Azure Key Vault to use for secrets.
            force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.
            **kwargs: Additional keyword arguments to override specific credential attributes.

        Raises:
            ValueError: If AZURE_TENANT_ID is not found in environment variables
                and not provided as parameter.
            ValueError: If AZURE_SUBSCRIPTION_ID is not found in environment variables
                and not provided as parameter.
            ValueError: If AZURE_CLIENT_ID is not found in environment variables
                and not provided as parameter.
            ValueError: If AZURE_CLIENT_SECRET is not found in environment variables
                and not provided as parameter.

        Example:
            >>> # Using direct parameters
            >>> handler = SPCredentialHandler(
            ...     azure_tenant_id="12345678-1234-1234-1234-123456789012",
            ...     azure_subscription_id="87654321-4321-4321-4321-210987654321",
            ...     azure_client_id="abcdef12-3456-7890-abcd-ef1234567890",
            ...     azure_client_secret="your-secret-here" #pragma: allowlist secret
            ... )

            >>> # Using environment variables
            >>> handler = SPCredentialHandler()  # Loads from env vars

            >>> # Using custom .env file
            >>> handler = SPCredentialHandler(dotenv_path="/path/to/.env")
        """
        logger.debug("Initializing SPCredentialHandler.")
        # load env vars, including client secret if available
        load_dotenv(dotenv_path=dotenv_path, override=True)

        mandatory_environment_variables = [
            "AZURE_TENANT_ID",
            "AZURE_SUBSCRIPTION_ID",
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
        ]
        for mandatory in mandatory_environment_variables:
            if mandatory not in os.environ:
                logger.warning(f"Environment variable {mandatory} was not provided")

        # check if tenant_id, client_id, subscription_id, and client_secret_id exist, else find in os env vars
        logger.debug(
            "Setting azure_tenant_id, azure_subscription_id, azure_client_id, and azure_client_secret."
        )
        self.azure_tenant_id = (
            azure_tenant_id
            if azure_tenant_id is not None
            else os.getenv("AZURE_TENANT_ID", None)
        )
        self.azure_subscription_id = (
            azure_subscription_id
            if azure_subscription_id is not None
            else os.getenv("AZURE_SUBSCRIPTION_ID", None)
        )
        self.azure_client_id = (
            azure_client_id
            if azure_client_id is not None
            else os.getenv("AZURE_CLIENT_ID", None)
        )
        self.azure_client_secret = (
            azure_client_secret
            if azure_client_secret is not None
            else os.getenv("AZURE_CLIENT_SECRET", None)
        )

        self.require_attr(
            [x.lower() for x in mandatory_environment_variables],
            goal="service principal credentials",
        )
        sp_cred = ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_id=self.azure_client_id,
            client_secret=self.azure_client_secret,
        )
        # load keyvault secrets
        if keyvault is not None:
            get_keyvault_vars(
                keyvault_name=keyvault,
                credential=sp_cred,
                force_keyvault=force_keyvault,
            )

        d.set_env_vars()

        get_conf = partial(get_config_val, config_dict=kwargs, try_env=True)

        for key in self.__dataclass_fields__.keys():
            self.__setattr__(key, get_conf(key))
        # set method to "sp"
        self.__setattr__("method", "sp")
        # check for azure batch location
        if self.__getattribute__("azure_batch_location") is None:
            self.__setattr__("azure_batch_location", d.default_azure_batch_location)


class DefaultCredentialHandler(CredentialHandler):
    def __init__(
        self,
        dotenv_path: str | None = None,
        keyvault: str = None,
        force_keyvault: bool = False,
        **kwargs,
    ) -> None:
        """Initialize a Default Credential Handler.

        Creates a credential handler that uses DefaultAzureCredential for accessing
        Azure resources. This handler automatically discovers and uses the most appropriate
        credential type available in the environment (managed identity, service principal,
        Azure CLI, etc.).

        Args:
            dotenv_path: Path to .env file to load environment variables from.
                If None, uses default .env file discovery.
            keyvault: Name of the Azure Key Vault to use for secrets.
            force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.
            **kwargs: Additional keyword arguments to override specific credential attributes.

        Raises:
            ValueError: If AZURE_SUBSCRIPTION_ID is not found in environment variables.
            ValueError: If the subscription matching AZURE_SUBSCRIPTION_ID is not found.

        Example:
            >>> # Using default credential discovery
            >>> handler = DefaultCredentialHandler()

            >>> # Using custom .env file
            >>> handler = DefaultCredentialHandler(dotenv_path="/path/to/.env")
        """
        logger.debug("Initializing DefaultCredentialHandler.")
        logger.debug("Loading environment variables.")
        load_dotenv(dotenv_path=dotenv_path)
        logger.debug(
            "Retrieving Azure subscription information using DefaultCredential."
        )
        d_cred = DefaultCredential()
        sub_c = SubscriptionClient(d_cred)

        # load keyvault secrets
        if keyvault is not None:
            get_keyvault_vars(
                keyvault_name=keyvault,
                credential=d_cred,
                force_keyvault=force_keyvault,
            )
        # pull subscription id from env vars
        sub_id = os.getenv("AZURE_SUBSCRIPTION_ID", None)
        if sub_id is None:
            logger.error("AZURE_SUBSCRIPTION_ID not found in environment variables.")
            raise ValueError("AZURE_SUBSCRIPTION_ID not found in env variables.")
        subscription = [
            sub for sub in sub_c.subscriptions.list() if sub.subscription_id == sub_id
        ]
        # pull info if sub exists
        logger.debug("Pulling subscription information.")
        if subscription:
            subscription = subscription[0]
            os.environ["AZURE_RESOURCE_GROUP_NAME"] = subscription.display_name
            logger.debug("Set AZURE_RESOURCE_GROUP_NAME from subscription information.")
        else:
            logger.error(
                f"Subscription matching AZURE_SUBSCRIPTION_ID ({sub_id}) not found."
            )
            raise ValueError(
                f"Subscription matching AZURE_SUBSCRIPTION_ID ({sub_id}) not found."
            )
        logger.debug("Setting environment variables.")
        d.set_env_vars()

        get_conf = partial(get_config_val, config_dict=kwargs, try_env=True)

        for key in self.__dataclass_fields__.keys():
            self.__setattr__(key, get_conf(key))
        # set method to "default"
        self.__setattr__("method", "default")
        # check for azure batch location
        if self.__getattribute__("azure_batch_location") is None:
            self.__setattr__("azure_batch_location", d.default_azure_batch_location)


def get_sp_secret(
    vault_url: str,
    vault_sp_secret_id: str,
    user_credential=None,
) -> str:
    """Get a service principal secret from an Azure keyvault.

    Args:
        vault_url: URL for the Azure keyvault to access.
        vault_sp_secret_id: Service principal secret ID within the keyvault.
        user_credential: User credential for the Azure user, as an azure-identity
            credential class instance. If None, will use a ManagedIdentityCredential
            instantiated at runtime.

    Returns:
        str: The retrieved value of the service principal secret.

    Example:
        >>> secret = get_sp_secret(
        ...     "https://myvault.vault.azure.net/",
        ...     "my-secret-id"
        ... )
    """
    if user_credential is None:
        logger.debug("No user_credential provided, using ManagedIdentityCredential.")
        user_credential = ManagedIdentityCredential()

    secret_client = SecretClient(vault_url=vault_url, credential=user_credential)
    sp_secret = secret_client.get_secret(vault_sp_secret_id).value
    logger.debug("Retrieved service principal secret from Azure Key Vault.")

    return sp_secret


def get_client_secret_sp_credential(
    vault_url: str,
    vault_sp_secret_id: str,
    tenant_id: str,
    application_id: str,
    user_credential=None,
) -> ClientSecretCredential:
    """Get a ClientSecretCredential for a given Azure service principal.

    Args:
        vault_url: URL for the Azure keyvault to access.
        vault_sp_secret_id: Service principal secret ID within the keyvault.
        tenant_id: Tenant ID for the service principal credential.
        application_id: Application ID for the service principal credential.
        user_credential: User credential for the Azure user, as an azure-identity
            credential class instance. Passed to ``get_sp_secret``. If None,
            ``get_sp_secret`` will use a ManagedIdentityCredential instantiated
            at runtime. See its documentation for more.

    Returns:
        ClientSecretCredential: A ClientSecretCredential for the given service principal.

    Example:
        >>> credential = get_client_secret_sp_credential(
        ...     "https://myvault.vault.azure.net/",
        ...     "my-secret-id",
        ...     "tenant-id",
        ...     "application-id"
        ... )
    """
    logger.debug("Getting SP secret for service principal.")
    sp_secret = get_sp_secret(
        vault_url, vault_sp_secret_id, user_credential=user_credential
    )
    logger.debug("Creating ClientSecretCredential for service principal using secret.")
    sp_credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=application_id,
        client_secret=sp_secret,
    )
    logger.debug("Created ClientSecretCredential for service principal.")
    return sp_credential


def get_service_principal_credentials(
    vault_url: str,
    vault_sp_secret_id: str,
    tenant_id: str,
    application_id: str,
    resource_url: str = d.default_azure_batch_resource_url,
    user_credential=None,
) -> ServicePrincipalCredentials:
    """Get a ServicePrincipalCredentials object for a given Azure service principal.

    Args:
        vault_url: URL for the Azure keyvault to access.
        vault_sp_secret_id: Service principal secret ID within the keyvault.
        tenant_id: Tenant ID for the service principal credential.
        application_id: Application ID for the service principal credential.
        resource_url: URL of the Azure resource. Defaults to the value of
            ``defaults.default_azure_batch_resource_url``.
        user_credential: User credential for the Azure user, as an azure-identity
            credential class instance. Passed to ``get_sp_secret``. If None,
            ``get_sp_secret`` will use a ManagedIdentityCredential instantiated
            at runtime. See the ``get_sp_secret`` documentation for details.

    Returns:
        ServicePrincipalCredentials: A ServicePrincipalCredentials object for the
            service principal.

    Example:
        >>> credentials = get_service_principal_credentials(
        ...     "https://myvault.vault.azure.net/",
        ...     "my-secret-id",
        ...     "tenant-id",
        ...     "application-id"
        ... )
    """
    logger.debug("Getting SP secret for service principal.")
    sp_secret = get_sp_secret(
        vault_url, vault_sp_secret_id, user_credential=user_credential
    )
    logger.debug(
        "Creating ServicePrincipalCredentials for service principal using secret."
    )
    sp_credential = ServicePrincipalCredentials(
        tenant=tenant_id,
        client_id=application_id,
        secret=sp_secret,
        resource=resource_url,
    )
    logger.debug("Created ServicePrincipalCredentials for service principal.")

    return sp_credential


def get_compute_node_identity_reference(
    credential_handler: CredentialHandler = None,
) -> models.ComputeNodeIdentityReference:
    """Get a valid ComputeNodeIdentityReference using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank EnvCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).

    Returns:
        models.ComputeNodeIdentityReference: A ComputeNodeIdentityReference created
            according to the specified configuration.

    Example:
        >>> # Using default environment-based handler
        >>> identity_ref = get_compute_node_identity_reference()

        >>> # Using custom handler
        >>> handler = CredentialHandler()
        >>> identity_ref = get_compute_node_identity_reference(handler)
    """
    logger.debug("Getting ComputeNodeIdentityReference from CredentialHandler.")
    ch = credential_handler
    if ch is None:
        logger.debug("No CredentialHandler provided, using EnvCredentialHandler.")
        ch = EnvCredentialHandler()
    logger.debug("Retrieving compute_node_identity_reference from CredentialHandler.")
    return ch.compute_node_identity_reference


def get_secret_client(keyvault: str, credential: object) -> SecretClient:
    """Get an Azure Key Vault SecretClient using a CredentialHandler.

    Args:
        keyvault: Name of the Azure Key Vault to connect to.
        credential: Credential handler for connecting and authenticating to Azure resources.

    Returns:
        SecretClient: An authenticated SecretClient for the specified Key Vault.

    Example:
        >>> handler = CredentialHandler()
        >>> secret_client = get_secret_client("myvault", handler)
    """
    logger.debug("Creating SecretClient for Azure Key Vault.")
    vault_url = f"https://{keyvault}.{d.default_azure_keyvault_endpoint_subdomain}"
    secret_client = SecretClient(vault_url=vault_url, credential=credential)
    logger.debug("Created SecretClient for Azure Key Vault.")
    return secret_client


def load_keyvault_vars(
    secret_client: SecretClient,
    force_keyvault: bool = False,
):
    """Load secrets from an Azure Key Vault into environment variables.

    Args:
        secret_client: SecretClient for accessing the Azure Key Vault.
        force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.
    """
    kv_keys = d.default_kv_keys

    for key in kv_keys:
        if force_keyvault:
            logger.debug(
                "Force Key Vault load enabled; loading secret regardless of existing environment variable."
            )
            try:
                secret = secret_client.get_secret(key.replace("_", "-")).value
                os.environ[key] = secret
                logger.debug(
                    f"Loaded secret '{key}' from Key Vault into environment variable."
                )
            except Exception as e:
                logger.warning(f"Could not load secret '{key}' from Key Vault: {e}")
        else:
            if key in os.environ:
                logger.debug(
                    f"Environment variable '{key}' already set; skipping Key Vault load."
                )
                continue
            else:
                try:
                    secret = secret_client.get_secret(key).value
                    os.environ[key] = secret
                    logger.debug(
                        f"Loaded secret '{key}' from Key Vault into environment variable."
                    )
                except Exception as e:
                    logger.warning(f"Could not load secret '{key}' from Key Vault: {e}")


def get_keyvault_vars(
    keyvault_name: str,
    credential: object,
    force_keyvault: bool = False,
):
    """Retrieve secrets from an Azure Key Vault and save to environment.

    Args:
        keyvault_name: Name of the Azure Key Vault to connect to.
        credential: Credential handler for connecting and authenticating to Azure resources.
        force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.
    """
    if keyvault_name is None:
        logger.debug("No Key Vault name provided; skipping Key Vault variable loading.")
        return None
    logger.debug("Getting SecretClient for Azure Key Vault.")
    secret_client = get_secret_client(
        keyvault=keyvault_name,
        credential=credential,
    )
    logger.debug("Loading Key Vault secrets into environment variables.")
    load_keyvault_vars(secret_client, force_keyvault=force_keyvault)
