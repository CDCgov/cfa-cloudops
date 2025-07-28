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

    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()
    if ch.method == "sp":
        return BatchManagementClient(
            credential=ch.client_secret_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )
    else:
        return BatchManagementClient(
            credential=ch.user_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )


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
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()
    if ch.method == "sp":
        return ComputeManagementClient(
            credential=ch.client_secret_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )
    else:
        return ComputeManagementClient(
            credential=ch.user_credential,
            subscription_id=ch.azure_subscription_id,
            **kwargs,
        )


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
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()
    if ch.method == "sp":
        return BatchServiceClient(
            credentials=ch.client_secret_credential,
            batch_url=ch.azure_batch_endpoint,
            **kwargs,
        )
    else:
        return BatchServiceClient(
            credentials=ch.user_credential,
            batch_url=ch.azure_batch_endpoint,
            **kwargs,
        )


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
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()
    if ch.method == "sp":
        return BatchServiceClient(
            credentials=ch.client_secret_credential,
            batch_url=ch.azure_batch_endpoint,
            **kwargs,
        )
    else:
        return BlobServiceClient(
            account_url=ch.azure_blob_storage_endpoint,
            credential=ch.user_credential,
            **kwargs,
        )
