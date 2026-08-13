"""
Helper functions for setting up valid Azure clients.
"""

import logging

from azure.batch import BatchClient
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.storage.blob import BlobServiceClient

from .auth import CredentialHandler, DefaultCredentialHandler

logger = logging.getLogger(__name__)


def get_batch_management_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BatchManagementClient:
    """Get an Azure Batch management client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank DefaultCredentialHandler, which
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
        logger.debug(
            "No credential handler provided, creating DefaultCredentialHandler"
        )
        ch = DefaultCredentialHandler()

    logger.debug("Using user credentials for BatchManagementClient")
    client = BatchManagementClient(
        credential=ch.default_credential,
        subscription_id=ch.azure_subscription_id,
        **kwargs,
    )

    logger.info("BatchManagementClient created.")
    return client


def get_compute_management_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> ComputeManagementClient:
    """Get an Azure compute management client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank DefaultCredentialHandler, which
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
        logger.debug(
            "No credential handler provided, creating DefaultCredentialHandler"
        )
        ch = DefaultCredentialHandler()

    logger.debug("Using user credentials for ComputeManagementClient")
    client = ComputeManagementClient(
        credential=ch.default_credential,
        subscription_id=ch.azure_subscription_id,
        **kwargs,
    )

    logger.info("ComputeManagementClient created.")
    return client


def get_batch_service_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BatchClient:
    """Get an Azure batch service client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Requires azure-batch>=15.1.0 (uses the new BatchClient from azure-core).

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank DefaultCredentialHandler, which
            attempts to obtain needed credentials using information available in
            local environment variables (see its documentation for details).
        **kwargs: Additional keyword arguments passed to the BatchClient constructor.

    Returns:
        BatchClient: A client instantiated according to the specified configuration.

    Example:
        >>> # Using default environment-based credentials
        >>> client = get_batch_service_client()

        >>> # Using custom credential handler
        >>> handler = CredentialHandler()
        >>> client = get_batch_service_client(credential_handler=handler)
    """
    logger.debug(
        f"Creating BatchClient with credential handler: {type(credential_handler).__name__ if credential_handler else 'None'}"
    )

    ch = credential_handler
    if ch is None:
        logger.debug(
            "No credential handler provided, creating DefaultCredentialHandler"
        )
        ch = DefaultCredentialHandler()

    logger.debug(f"Using batch endpoint: {ch.azure_batch_endpoint}")

    # BatchClient (15.0.0+) uses endpoint and credential parameters

    logger.info("Using user credentials for BatchClient")
    logger.debug("Creating BatchClient with user credentials")
    batch_credential = getattr(ch, "batch_credential", ch.default_credential)
    client = BatchClient(
        endpoint=ch.azure_batch_endpoint,
        credential=batch_credential,
        **kwargs,
    )

    logger.debug("BatchClient created successfully")
    return client


def get_blob_service_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BlobServiceClient:
    """Get an Azure blob service client using credentials from a CredentialHandler.

    Uses credentials obtained via a CredentialHandler: either a user-provided one
    or a default based on environment variables.

    Args:
        credential_handler: Credential handler for connecting and authenticating to
            Azure resources. If None, create a blank DefaultCredentialHandler, which
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
        logger.debug(
            "No credential handler provided, creating DefaultCredentialHandler"
        )
        ch = DefaultCredentialHandler()

    logger.debug(f"Using blob storage endpoint: {ch.azure_blob_storage_endpoint}")

    logger.debug("Using user credentials for BlobServiceClient")
    client = BlobServiceClient(
        account_url=ch.azure_blob_storage_endpoint,
        credential=ch.default_credential,
        **kwargs,
    )

    logger.info(
        f"BlobServiceClient created at endpoint '{ch.azure_blob_storage_endpoint}'."
    )
    return client
