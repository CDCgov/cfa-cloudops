"""
Helper functions for setting up valid Azure clients.
"""

import logging

from azure.batch import BatchServiceClient
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.storage.blob import BlobServiceClient

from .auth import CredentialHandler, EnvCredentialHandler

logger = logging.getLogger(__name__)


def get_batch_management_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BatchManagementClient:
    """Get an Azure Batch management client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank EnvCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).
        **kwargs: Additional keyword arguments passed to the BatchManagementClient constructor.

    Returns:
        BatchManagementClient: A client instantiated according to the specified configuration.

    Example:
        >>> # Using default environment-based credentials
        >>> client = get_batch_management_client()

        >>> # Using custom credential handler
        >>> handler = CredentialHandler()
        >>> client = get_batch_management_client(credential_handler=handler)
    """
    logger.debug(
        f"Creating BatchManagementClient with credential handler: {type(credential_handler).__name__ if credential_handler else 'None'}"
    )

    ch = credential_handler
    if ch is None:
        logger.debug("No credential handler provided, creating EnvCredentialHandler")
        ch = EnvCredentialHandler()

    logger.debug(f"Selected authentication method: '{ch.method}'")

    if ch.method == "sp":
        logger.debug("Using service principal credentials for BatchManagementClient")
        client = BatchManagementClient(
            credential=ch.client_secret_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )
    elif ch.method == "default":
        logger.debug("Using default credentials for BatchManagementClient")
        client = BatchManagementClient(
            credential=ch.client_secret_sp_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )
    else:
        logger.debug("Using user credentials for BatchManagementClient")
        client = BatchManagementClient(
            credential=ch.user_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )

    logger.debug("BatchManagementClient created successfully")
    return client


def get_compute_management_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> ComputeManagementClient:
    """Get an Azure compute management client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank EnvCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).
        **kwargs: Additional keyword arguments passed to the ComputeManagementClient constructor.

    Returns:
        ComputeManagementClient: A client instantiated according to the specified configuration.

    Example:
        >>> # Using default environment-based credentials
        >>> client = get_compute_management_client()

        >>> # Using custom credential handler
        >>> handler = CredentialHandler()
        >>> client = get_compute_management_client(credential_handler=handler)
    """
    logger.debug(
        f"Creating ComputeManagementClient with credential handler: {type(credential_handler).__name__ if credential_handler else 'None'}"
    )

    ch = credential_handler
    if ch is None:
        logger.debug("No credential handler provided, creating EnvCredentialHandler")
        ch = EnvCredentialHandler()

    logger.debug(f"Selected authentication method: '{ch.method}'")

    if ch.method == "sp":
        logger.debug("Using service principal credentials for ComputeManagementClient")
        client = ComputeManagementClient(
            credential=ch.client_secret_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )
    elif ch.method == "default":
        logger.debug("Using default credentials for ComputeManagementClient")
        client = ComputeManagementClient(
            credential=ch.client_secret_sp_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )
    else:
        logger.debug("Using user credentials for ComputeManagementClient")
        client = ComputeManagementClient(
            credential=ch.user_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )

    logger.debug("ComputeManagementClient created successfully")
    return client


def get_batch_service_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BatchServiceClient:
    """Get an Azure batch service client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank EnvCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).
        **kwargs: Additional keyword arguments passed to the BatchServiceClient constructor.

    Returns:
        BatchServiceClient: A client instantiated according to the specified configuration.

    Example:
        >>> # Using default environment-based credentials
        >>> client = get_batch_service_client()

        >>> # Using custom credential handler
        >>> handler = CredentialHandler()
        >>> client = get_batch_service_client(credential_handler=handler)
    """
    logger.debug(
        f"Creating BatchServiceClient with credential handler: {type(credential_handler).__name__ if credential_handler else 'None'}"
    )

    ch = credential_handler
    if ch is None:
        logger.debug("No credential handler provided, creating EnvCredentialHandler")
        ch = EnvCredentialHandler()

    logger.debug(f"Selected authentication method: '{ch.method}'")
    logger.debug(f"Using batch endpoint: {ch.azure_batch_endpoint}")

    if ch.method == "sp":
        logger.info("Using service principal credentials for BatchServiceClient")
        logger.debug("Creating BatchServiceClient with service principal credentials")
        client = BatchServiceClient(
            credentials=ch.batch_service_principal_credentials,
            batch_url=ch.azure_batch_endpoint,
            **kwargs,
        )
    elif ch.method == "default":
        logger.info("Using default credentials for BatchServiceClient")
        logger.debug("Creating BatchServiceClient with default credentials")
        client = BatchServiceClient(
            credentials=ch.batch_service_principal_credentials,
            batch_url=ch.azure_batch_endpoint,
            **kwargs,
        )
    else:
        logger.info("Using user credentials for BatchServiceClient")
        logger.debug("Creating BatchServiceClient with user credentials")
        client = BatchServiceClient(
            credentials=ch.batch_service_principal_credentials,
            batch_url=ch.azure_batch_endpoint,
            **kwargs,
        )

    logger.debug("BatchServiceClient created successfully")
    return client


def get_blob_service_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BlobServiceClient:
    """Get an Azure blob service client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank EnvCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).
        **kwargs: Additional keyword arguments passed to the BlobServiceClient constructor.

    Returns:
        BlobServiceClient: A client instantiated according to the specified configuration.

    Example:
        >>> # Using default environment-based credentials
        >>> client = get_blob_service_client()

        >>> # Using custom credential handler
        >>> handler = CredentialHandler()
        >>> client = get_blob_service_client(credential_handler=handler)
    """
    logger.debug(
        f"Creating BlobServiceClient with credential handler: {type(credential_handler).__name__ if credential_handler else 'None'}"
    )

    ch = credential_handler
    if ch is None:
        logger.debug("No credential handler provided, creating EnvCredentialHandler")
        ch = EnvCredentialHandler()

    logger.debug(f"Selected authentication method: '{ch.method}'")
    logger.debug(f"Using blob storage endpoint: {ch.azure_blob_storage_endpoint}")

    if ch.method == "sp":
        logger.debug("Using service principal credentials for BlobServiceClient")
        client = BlobServiceClient(
            account_url=ch.azure_blob_storage_endpoint,
            credential=ch.client_secret_credential,
            **kwargs,
        )
    elif ch.method == "default":
        logger.debug("Using default credentials for BlobServiceClient")
        client = BlobServiceClient(
            credential=ch.client_secret_sp_credential,
            account_url=ch.azure_blob_storage_endpoint,
            **kwargs,
        )
    else:
        logger.debug("Using user credentials for BlobServiceClient")
        client = BlobServiceClient(
            account_url=ch.azure_blob_storage_endpoint,
            credential=ch.user_credential,
            **kwargs,
        )

    logger.debug("BlobServiceClient created successfully")
    return client
