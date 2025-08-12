"""
Helper functions for Azure authentication.
"""

import logging
import os
from dataclasses import dataclass
from functools import cached_property, partial

from azure.batch import models
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import (
    ClientSecretCredential,
    ManagedIdentityCredential,
    WorkloadIdentityCredential,
)
from azure.keyvault.secrets import SecretClient
from azure.mgmt.resource import SubscriptionClient
from dotenv import load_dotenv

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
    azure_sp_client_id: str = None
    azure_batch_endpoint_subdomain: str = (
        d.default_azure_batch_endpoint_subdomain
    )
    azure_batch_account: str = None
    azure_batch_location: str = None
    azure_batch_resource_url: str = d.default_azure_batch_resource_url
    azure_blob_storage_endpoint_subdomain: str = (
        d.default_azure_blob_storage_endpoint_subdomain
    )
    azure_blob_storage_account: str = None

    azure_container_registry_account: str = None
    azure_container_registry_domain: str = (
        d.default_azure_container_registry_domain
    )
    azure_federated_token_file: str = None
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
        for attr in attributes:
            attr_val = getattr(self, attr)
            if attr_val is None:
                err_msg = (
                    f"A non-None value for attribute {attr} is required "
                ) + (
                    f"to obtain a value for {goal}."
                    if goal is not None
                    else "for this operation."
                )
                raise AttributeError(err_msg)

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
        self.require_attr(
            [
                "azure_batch_account",
                "azure_batch_location",
                "azure_batch_endpoint_subdomain",
            ],
            goal="Azure batch endpoint URL",
        )
        return construct_batch_endpoint(
            self.azure_batch_account,
            self.azure_batch_location,
            self.azure_batch_endpoint_subdomain,
        )

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
        self.require_attr(
            [
                "azure_blob_storage_account",
                "azure_blob_storage_endpoint_subdomain",
            ],
            goal="Azure blob storage endpoint URL",
        )
        return construct_blob_account_endpoint(
            self.azure_blob_storage_account,
            self.azure_blob_storage_endpoint_subdomain,
        )

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
        self.require_attr(
            [
                "azure_container_registry_account",
                "azure_container_registry_domain",
            ],
            goal="Azure container registry endpoint URL",
        )
        return construct_azure_container_registry_endpoint(
            self.azure_container_registry_account,
            self.azure_container_registry_domain,
        )

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
        self.require_attr(
            ["azure_keyvault_endpoint", "azure_keyvault_sp_secret_id"],
            goal="service_principal_secret",
        )

        return get_sp_secret(
            self.azure_keyvault_endpoint,
            self.azure_keyvault_sp_secret_id,
            self.user_credential,
        )

    @cached_property
    def federated_credential(self) -> WorkloadIdentityCredential:
        self.require_attr(
            [
                "azure_tenant_id",
                "azure_sp_client_id",
                "azure_federated_token_file",
            ]
        )
        return WorkloadIdentityCredential(
            tenant_id=self.azure_tenant_id,
            client_id=self.azure_sp_client_id,
            token_file_path=self.azure_federated_token_file,
        )

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
        self.require_attr(
            [
                "azure_tenant_id",
                "azure_sp_client_id",
                "azure_batch_resource_url",
            ],
            goal="batch_service_principal_credentials",
        )
        return ServicePrincipalCredentials(
            client_id=self.azure_sp_client_id,
            tenant=self.azure_tenant_id,
            secret=self.service_principal_secret,
            resource=self.azure_batch_resource_url,
        )

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
        self.require_attr(["azure_tenant_id", "azure_sp_client_id"])
        return ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_secret=self.service_principal_secret,
            client_id=self.azure_sp_client_id,
        )

    @cached_property
    def client_secret_credential(self):
        """A client secret credential created using the azure_client_secret attribute.

        Returns:
            ClientSecretCredential: The credential configured with client secret details.

        Example:
            >>> handler = CredentialHandler()
            >>> handler.azure_tenant_id = "tenant-id"
            >>> handler.azure_sp_client_id = "client-id"
            >>> handler.azure_client_secret = "client-secret" #pragma: allowlist secret
            >>> credential = handler.client_secret_credential
        """
        self.require_attr(
            [
                "azure_tenant_id",
                "azure_sp_client_id",
                "azure_client_secret",
            ]
        )
        return ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_secret=self.azure_client_secret,
            client_id=self.azure_sp_client_id,
        )

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
        self.require_attr(
            ["azure_user_assigned_identity"],
            goal="Compute node identity reference",
        )
        return models.ComputeNodeIdentityReference(
            resource_id=self.azure_user_assigned_identity
        )

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
        self.require_attr(
            [
                "azure_container_registry_account",
                "azure_container_registry_domain",
                "azure_user_assigned_identity",
            ],
            goal=("Azure Container Registry `ContainerRegistry` instance"),
        )

        valid, msg = is_valid_acr_endpoint(
            self.azure_container_registry_endpoint
        )
        if not valid:
            raise ValueError(msg)

        return models.ContainerRegistry(
            user_name=self.azure_container_registry_account,
            registry_server=self.azure_container_registry_endpoint,
            identity_reference=self.compute_node_identity_reference,
        )


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

    def __init__(self, dotenv_path: str = None, **kwargs) -> None:
        """Initialize the EnvCredentialHandler.

        Loads environment variables from .env file and populates credential attributes from them.

        Args:
            dotenv_path (str, optional): Path to .env file to load environment variables from.
                If None, uses default .env file discovery.
            **kwargs: Additional keyword arguments to override specific credential attributes.
        """
        load_env_vars(dotenv_path=dotenv_path)
        get_conf = partial(get_config_val, config_dict=kwargs, try_env=True)

        for key in self.__dataclass_fields__.keys():
            self.__setattr__(key, get_conf(key))


def load_env_vars(dotenv_path=None):
    """Load environment variables and Azure subscription information.

    Loads variables from a .env file (if specified), retrieves Azure subscription
    information using ManagedIdentityCredential, and sets default environment variables.

    Args:
        dotenv_path: Path to .env file to load. If None, uses default .env file discovery.

    Example:
        >>> load_env_vars()  # Load from default .env
        >>> load_env_vars("/path/to/.env")  # Load from specific file
    """
    load_dotenv(dotenv_path=dotenv_path, override=True)
    # get ManagedIdentityCredential to pull SubscriptionClient
    mid_cred = ManagedIdentityCredential()
    sub_c = SubscriptionClient(mid_cred)
    # pull in account info and save to environment vars
    account_info = list(sub_c.subscriptions.list())[0]
    os.environ["AZURE_SUBSCRIPTION_ID"] = account_info.subscription_id
    os.environ["AZURE_TENANT_ID"] = account_info.tenant_id
    os.environ["AZURE_RESOURCE_GROUP_NAME"] = account_info.display_name
    # save default values
    d.set_env_vars()


class SPCredentialHandler(CredentialHandler):
    def __init__(
        self,
        azure_tenant_id: str = None,
        azure_subscription_id: str = None,
        azure_sp_client_id: str = None,
        azure_client_secret: str = None,
        dotenv_path: str = None,
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
            azure_sp_client_id: Azure Service Principal client ID (application ID).
                If None, will attempt to load from AZURE_SP_CLIENT_ID environment variable.
            azure_client_secret: Azure Service Principal client secret. If None, will
                attempt to load from AZURE_CLIENT_SECRET environment variable.
            dotenv_path: Path to .env file to load environment variables from.
                If None, uses default .env file discovery.

        Raises:
            ValueError: If AZURE_TENANT_ID is not found in environment variables
                and not provided as parameter.
            ValueError: If AZURE_SUBSCRIPTION_ID is not found in environment variables
                and not provided as parameter.
            ValueError: If AZURE_SP_CLIENT_ID is not found in environment variables
                and not provided as parameter.
            ValueError: If AZURE_CLIENT_SECRET is not found in environment variables
                and not provided as parameter.

        Example:
            >>> # Using direct parameters
            >>> handler = SPCredentialHandler(
            ...     azure_tenant_id="12345678-1234-1234-1234-123456789012",
            ...     azure_subscription_id="87654321-4321-4321-4321-210987654321",
            ...     azure_sp_client_id="abcdef12-3456-7890-abcd-ef1234567890",
            ...     azure_client_secret="your-secret-here" #pragma: allowlist secret
            ... )

            >>> # Using environment variables
            >>> handler = SPCredentialHandler()  # Loads from env vars

            >>> # Using custom .env file
            >>> handler = SPCredentialHandler(dotenv_path="/path/to/.env")
        """
        self.method = "sp"
        # load env vars, including client secret if available
        load_dotenv(dotenv_path=dotenv_path, override=True)

        self.azure_tenant_id = (
            azure_tenant_id
            if azure_tenant_id is not None
            else os.environ["AZURE_TENANT_ID"]
        )
        self.azure_subscription_id = (
            azure_subscription_id
            if azure_subscription_id is not None
            else os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self.azure_sp_client_id = (
            azure_sp_client_id
            if azure_sp_client_id is not None
            else os.environ["AZURE_SP_CLIENT_ID"]
        )
        self.azure_client_secret = (
            azure_client_secret
            if azure_client_secret is not None
            else os.environ["AZURE_CLIENT_SECRET"]
        )

        # check if tenant_id, client_id, subscription_id, and client_secret_id exist, else find in os env vars
        if "AZURE_TENANT_ID" not in os.environ and not azure_tenant_id:
            raise ValueError(
                "AZURE_TENANT_ID not found in env variables and not provided."
            )
        if (
            "AZURE_SUBSCRIPTION_ID" not in os.environ
            and not azure_subscription_id
        ):
            raise ValueError(
                "AZURE_SUBSCRIPTION_ID not found in env variables and not provided."
            )
        if "AZURE_SP_CLIENT_ID" not in os.environ and not azure_sp_client_id:
            raise ValueError(
                "AZURE_SP_CLIENT_ID not found in env variables and not provided."
            )
        if "AZURE_CLIENT_SECRET" not in os.environ and not azure_client_secret:
            raise ValueError(
                "AZURE_CLIENT_SECRET not found in env variables and not provided."
            )
        d.set_env_vars()

        get_conf = partial(get_config_val, config_dict=kwargs, try_env=True)

        for key in self.__dataclass_fields__.keys():
            self.__setattr__(key, get_conf(key))


class FederatedCredentialHandler(CredentialHandler):
    def __init__(
        self,
        azure_tenant_id: str | None = None,
        azure_sp_client_id: str | None = None,
        azure_federated_token_file: str | None = None,
        dotenv_path: str | None = None,
        **kwargs,
    ) -> None:
        self.method = "fc"
        load_env_vars(dotenv_path=dotenv_path)

        if "AZURE_TENANT_ID" not in os.environ and not azure_tenant_id:
            raise ValueError(
                "AZURE_TENANT_ID not found in env variables and not provided."
            )
        if "AZURE_SP_CLIENT_ID" not in os.environ and not azure_sp_client_id:
            raise ValueError(
                "AZURE_SP_CLIENT_ID not found in env variables and not provided."
            )
        if (
            "AZURE_FEDERATED_TOKEN_FILE" not in os.environ
            and not azure_federated_token_file
        ):
            raise ValueError(
                "AZURE_FEDERATED_TOKEN_FILE not found in env variables and not provided."
            )
        d.set_env_vars()

        get_conf = partial(get_config_val, config_dict=kwargs, try_env=True)

        for key in self.__dataclass_fields__.keys():
            self.__setattr__(key, get_conf(key))


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
        user_credential = ManagedIdentityCredential()

    secret_client = SecretClient(
        vault_url=vault_url, credential=user_credential
    )
    sp_secret = secret_client.get_secret(vault_sp_secret_id).value

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
    sp_secret = get_sp_secret(
        vault_url, vault_sp_secret_id, user_credential=user_credential
    )
    sp_credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=application_id,
        client_secret=sp_secret,
    )

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
    sp_secret = get_sp_secret(
        vault_url, vault_sp_secret_id, user_credential=user_credential
    )
    sp_credential = ServicePrincipalCredentials(
        tenant=tenant_id,
        client_id=application_id,
        secret=sp_secret,
        resource=resource_url,
    )

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
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()

    return ch.compute_node_identity_reference
