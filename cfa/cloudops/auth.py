"""
Helper functions for Azure authentication.
"""

import logging
import os
from dataclasses import dataclass
from functools import cached_property

from azure.core.pipeline import PipelineContext, PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline.transport import HttpRequest
from azure.identity import (
    ChainedTokenCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.keyvault.secrets import SecretClient
from azure.mgmt.batch import models as batch_mgmt_models
from azure.mgmt.resource.subscriptions import SubscriptionClient
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
    azure_tenant_id: str = None
    azure_client_id: str = None
    azure_client_secret: str = None
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
    def default_credential(self):
        credential_kwargs = getattr(self, "_default_credential_kwargs", {}) or {}
        logger.debug(
            "Creating DefaultAzureCredential for ARM/Key Vault/data-plane usage."
        )
        if credential_kwargs:
            logger.debug(
                "DefaultAzureCredential configured with options: %s",
                sorted(credential_kwargs.keys()),
            )
        return ChainedTokenCredential(
            DefaultAzureCredential(**credential_kwargs),
            ManagedIdentityCredential(**credential_kwargs),
        )

    @cached_property
    def batch_credential(self):
        """Credential wrapper for SDKs expecting BasicTokenAuthentication semantics.

        This wrapper should be used only where a legacy/msrest-style auth object is
        required. For ARM/Key Vault and modern Azure SDK clients, use
        ``default_credential`` directly.
        """
        logger.debug("Creating batch-compatible DefaultCredential wrapper.")
        resource_url = (
            self.azure_batch_resource_url or d.default_azure_batch_resource_url
        )
        resource_scope = f"{resource_url.rstrip('/')}/.default"
        return DefaultCredential(
            credential=self.default_credential,
            resource_id=resource_scope,
        )

    @cached_property
    def compute_node_identity_reference(self):
        """An object defining a compute node identity reference.

        Specifically, a ComputeNodeIdentityReference object associated to the
        CredentialHandler's user-assigned identity.

        Returns:
            azure.mgmt.batch.models.ComputeNodeIdentityReference: The identity reference.

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
        comp_id_ref = batch_mgmt_models.ComputeNodeIdentityReference(
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
            goal="Azure Container Registry ContainerRegistry instance",
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
        cont_reg = batch_mgmt_models.ContainerRegistry(
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
            credential = ChainedTokenCredential(
                DefaultAzureCredential(), ManagedIdentityCredential()
            )
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


def load_env_vars(
    dotenv_path=None,
    override_env=False,
    keyvault_name: str = None,
    force_keyvault: bool = False,
):
    """Load environment variables and Azure subscription information.

    Loads variables from a .env file (if specified), retrieves Azure subscription
    and tenant information using DefaultAzureCredential, and sets default environment
    variables.

    Args:
        dotenv_path: Path to .env file to load. If None, uses default .env file discovery.
        override_env: If True, overrides existing environment variables with values from the .env file.
        keyvault_name: Name of the Azure Key Vault to use for secrets.
        force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.

    Example:
        >>> load_env_vars()  # Load from default .env
        >>> load_env_vars("/path/to/.env")  # Load from specific file
    """
    # get DefaultAzureCredential
    def_cred = ChainedTokenCredential(
        DefaultAzureCredential(), ManagedIdentityCredential()
    )

    logger.debug("Loading environment variables.")
    load_dotenv(dotenv_path=dotenv_path, override=override_env)

    needs_subscriptions = (
        "AZURE_SUBSCRIPTION_ID" not in os.environ
        or "AZURE_TENANT_ID" not in os.environ
        or "AZURE_RESOURCE_GROUP_NAME" not in os.environ
    )

    if needs_subscriptions:
        sub_c = SubscriptionClient(def_cred)
        subscriptions = list(sub_c.subscriptions.list())
        if not subscriptions:
            raise ValueError(
                "No Azure subscriptions were found for the current credential."
            )

        configured_sub_id = os.getenv("AZURE_SUBSCRIPTION_ID")
        if configured_sub_id is None:
            account_info = subscriptions[0]
            os.environ["AZURE_SUBSCRIPTION_ID"] = account_info.subscription_id
            logger.debug(
                "AZURE_SUBSCRIPTION_ID not found in environment; using first available subscription."
            )
        else:
            account_info = next(
                (
                    sub
                    for sub in subscriptions
                    if sub.subscription_id == configured_sub_id
                ),
                None,
            )
            if account_info is None:
                raise ValueError(
                    f"Subscription matching AZURE_SUBSCRIPTION_ID ({configured_sub_id}) not found."
                )

        if "AZURE_TENANT_ID" not in os.environ:
            os.environ["AZURE_TENANT_ID"] = account_info.tenant_id

        if "AZURE_RESOURCE_GROUP_NAME" not in os.environ:
            os.environ["AZURE_RESOURCE_GROUP_NAME"] = account_info.display_name

    # get Key Vault secrets
    if keyvault_name is not None:
        get_keyvault_vars(
            keyvault_name=keyvault_name,
            credential=def_cred,
            force_keyvault=force_keyvault,
        )

    # save default values
    d.set_env_vars()


class DefaultCredentialHandler(CredentialHandler):
    def __init__(
        self,
        dotenv_path: str | None = ".env",
        override_env: bool = False,
        keyvault: str = None,
        force_keyvault: bool = False,
        default_credential_kwargs: dict | None = None,
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
            override_env: If True, overrides existing environment variables with values from the
                .env file and persists resolved ``azure_*`` handler values and derived defaults
                into process environment variables.
            keyvault: Name of the Azure Key Vault to use for secrets.
            force_keyvault: If True, forces loading of Key Vault secrets even if they are already set in the environment.
            default_credential_kwargs: Optional keyword arguments passed directly to
                ``DefaultAzureCredential`` to tune credential chain behavior in CI/
                headless environments (for example
                ``{"exclude_interactive_browser_credential": True}``).
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
        # load the .env values
        load_dotenv(dotenv_path=dotenv_path, override=override_env)

        # gather any default_credential_kwargs for DefaultAzureCredential
        self._default_credential_kwargs = default_credential_kwargs or {}
        if self._default_credential_kwargs:
            logger.debug(
                "DefaultCredentialHandler received DefaultAzureCredential options: %s",
                sorted(self._default_credential_kwargs.keys()),
            )

        # Explicit kwargs should override .env/env values for this handler instance
        # without mutating process-level environment by default.
        azure_kwargs = {
            key: val
            for key, val in kwargs.items()
            if key.startswith("azure_") and val is not None
        }

        credential_env_mapping = {
            "azure_tenant_id": "AZURE_TENANT_ID",
            "azure_client_id": "AZURE_CLIENT_ID",
            "azure_client_secret": "AZURE_CLIENT_SECRET",  # pragma: allowlist secret
        }
        credential_env_overrides = {
            env_key: str(azure_kwargs[key])
            for key, env_key in credential_env_mapping.items()
            if key in azure_kwargs
        }
        # stores original env values to restore after handler initialization if override_env is False
        original_credential_env = {
            env_key: os.environ.get(env_key) for env_key in credential_env_overrides
        }

        # override environment variables with explicit azure_kwargs
        for env_key, env_val in credential_env_overrides.items():
            os.environ[env_key] = env_val

        # helper function to get resolved value for a key, checking azure_kwargs first, then falling back to environment or .env
        def get_resolved(key: str):
            if key in azure_kwargs:
                return azure_kwargs[key]
            return get_config_val(key, config_dict=kwargs, try_env=True)

        # try to retrieve Azure subscription information using DefaultAzureCredential
        try:
            logger.debug(
                "Retrieving Azure subscription information using DefaultCredential."
            )
            d_cred = ChainedTokenCredential(
                DefaultAzureCredential(**self._default_credential_kwargs),
                ManagedIdentityCredential(**self._default_credential_kwargs),
            )

            # Reuse the same credential object for downstream SDK clients.
            self.__dict__["default_credential"] = d_cred

            # load keyvault secrets
            if keyvault is None:
                try:
                    # if missing from arg, get kv name from env
                    keyvault = os.environ["AZURE_KEYVAULT_NAME"]
                except KeyError:
                    keyvault = None
            if keyvault is not None:
                # pull from kv if name provided or found in env
                get_keyvault_vars(
                    keyvault_name=keyvault,
                    credential=d_cred,
                    force_keyvault=force_keyvault,
                )

            try:
                # create SubscriptionClient to retrieve subscription info
                sub_c = SubscriptionClient(d_cred)
            except Exception as e:
                logger.error(f"Failed to create SubscriptionClient: {e}")
                raise
            # get list of subscriptions
            subscriptions = list(sub_c.subscriptions.list())
            if not subscriptions:
                raise ValueError(
                    "No Azure subscriptions were found for the current credential."
                )
            # try to get subscription ID from environment or .env
            sub_id = get_resolved("azure_subscription_id")
            if sub_id is None:
                logger.debug(
                    "AZURE_SUBSCRIPTION_ID not found; using first available subscription."
                )
                # use first subscription if none specified
                subscription = subscriptions[0]
                azure_kwargs["azure_subscription_id"] = subscription.subscription_id
            else:
                # find the subscription matching the specified subscription ID
                subscription = next(
                    (sub for sub in subscriptions if sub.subscription_id == sub_id),
                    None,
                )

            # pull info if sub exists
            logger.debug("Pulling subscription information.")
            if subscription is not None:
                # use env resource group name if present, otherwise use subscription display name
                if "AZURE_RESOURCE_GROUP_NAME" in os.environ:
                    logger.debug(
                        "Using AZURE_RESOURCE_GROUP_NAME from environment/.env/key vault."
                    )
                else:
                    azure_kwargs["azure_resource_group_name"] = (
                        subscription.display_name
                    )
                # use env tenant ID if present, otherwise use subscription tenant ID
                if "AZURE_TENANT_ID" in os.environ:
                    logger.debug(
                        "Using AZURE_TENANT_ID from environment/.env/key vault."
                    )
                else:
                    azure_kwargs["azure_tenant_id"] = subscription.tenant_id
            else:
                logger.error(
                    f"Subscription matching AZURE_SUBSCRIPTION_ID ({sub_id}) not found."
                )
                raise ValueError(
                    f"Subscription matching AZURE_SUBSCRIPTION_ID ({sub_id}) not found."
                )

            # iterate over dataclass fields and set azure kwargs
            for key in self.__dataclass_fields__.keys():
                resolved_val = get_resolved(key)
                if resolved_val is not None:
                    self.__setattr__(key, resolved_val)

            # persist resolved values into environment variables if override_env is True
            if override_env:
                logger.debug(
                    "Persisting resolved handler values into environment variables."
                )
                # iterate over dataclass fields and set environment variables
                for key in self.__dataclass_fields__.keys():
                    val = self.__getattribute__(key)
                    if val is not None:
                        os.environ[key.upper()] = str(val)
            # set the environment variables for the defaults
            d.set_env_vars(override_env=override_env)
        finally:
            if not override_env:
                for env_key, original_val in original_credential_env.items():
                    if original_val is None:
                        os.environ.pop(env_key, None)
                    else:
                        os.environ[env_key] = original_val


class EnvCredentialHandler(DefaultCredentialHandler):
    """Backward-compatible alias for environment-based handler behavior.

    The project now standardizes on ``DefaultCredentialHandler``. This class is
    retained for compatibility with older imports and tests.
    """


class SPCredentialHandler(DefaultCredentialHandler):
    """Backward-compatible alias for service-principal handler behavior.

    The project now standardizes on ``DefaultCredentialHandler``. This class is
    retained for compatibility with older imports and tests.
    """


def get_compute_node_identity_reference(
    credential_handler: CredentialHandler = None,
) -> batch_mgmt_models.ComputeNodeIdentityReference:
    """Get a valid ComputeNodeIdentityReference using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank DefaultCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).

    Returns:
        azure.mgmt.batch.models.ComputeNodeIdentityReference: A ComputeNodeIdentityReference created
            according to the specified configuration.

    Example:
        >>> # Using default environment-based handler
        >>> identity_ref = get_compute_node_identity_reference()

        >>> # Using custom handler
        >>> handler = CredentialHandler()
        >>> identity_ref = get_compute_node_identity_reference(handler)
    """
    logger.debug("Getting ComputeNodeIdentityReference from CredentialHandler.")
    if credential_handler is None:
        logger.debug("No CredentialHandler provided, using DefaultCredentialHandler.")
        credential_handler = DefaultCredentialHandler()
    logger.debug("Retrieving compute_node_identity_reference from CredentialHandler.")
    return credential_handler.compute_node_identity_reference


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
                    secret = secret_client.get_secret(key.replace("_", "-")).value
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
    else:
        os.environ["AZURE_KEYVAULT_NAME"] = keyvault_name
    logger.debug("Getting SecretClient for Azure Key Vault.")
    try:
        secret_client = get_secret_client(
            keyvault=keyvault_name,
            credential=credential,
        )
    except Exception as e:
        logger.error(f"Failed to get SecretClient: {e}")
        raise
    logger.debug("Loading Key Vault secrets into environment variables.")
    load_keyvault_vars(secret_client, force_keyvault=force_keyvault)
