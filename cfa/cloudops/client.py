"""
Helper functions for setting up valid Azure clients.
"""

from azure.batch import BatchServiceClient
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.storage.blob import BlobServiceClient

from .auth import CredentialHandler, EnvCredentialHandler


def get_batch_management_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BatchManagementClient:
    """
    Get an Azure Batch management client using
    credentials obtained via a :class:`CredentialHandler`,
    either a user-provided one or a default based on
    environment variables.

    Parameters
    ----------
    credential_handler
       Credential handler for connecting and
       authenticating to Azure resources.
       If ``None``, create a blank
       :class:`EnvCredentialHandler`, which
       attempts to obtain needed credentials
       using information available in local
       environment variables (see its documentation
       for details).

    **kwargs
       Additional keyword arguments passed to
       the :class:`BatchManagementClient` constructor.

    Returns
    -------
    BatchManagementClient
        A client instaniated according to
        the specified configuration.
    """

    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()

    return BatchManagementClient(
        credential=ch.client_secret_sp_credential,
        subscription_id=ch.azure_subscription_id,
        **kwargs,
    )


def get_compute_management_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> ComputeManagementClient:
    """
    Get an Azure compute management client using
    credentials obtained via a :class:`CredentialHandler`,
    either a user-provided one or a default based on
    environment variables.

    Parameters
    ----------
    credential_handler
       Credential handler for connecting and
       authenticating to Azure resources.
       If ``None``, create a blank
       :class:`EnvCredentialHandler`, which
       attempts to obtain needed credentials
       using information available in local
       environment variables (see its documentation
       for details).

    **kwargs
       Additional keyword arguments passed to
       the :class:`ComputeManagementClient` constructor.

    Returns
    -------
    ComputeManagementClient
        A client instaniated according to
        the specified configuration.
    """
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()

    return ComputeManagementClient(
        credential=ch.client_secret_sp_credential,
        subscription_id=ch.azure_subscription_id,
        **kwargs,
    )


def get_batch_service_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BatchServiceClient:
    """
    Get an Azure batch service client using
    credentials obtained via a :class:`CredentialHandler`,
    either a user-provided one or a default based on
    environment variables.

    Parameters
    ----------
    credential_handler
       Credential handler for connecting and
       authenticating to Azure resources.
       If ``None``, create a blank
       :class:`EnvCredentialHandler`, which
       attempts to obtain needed credentials
       using information available in local
       environment variables (see its documentation
       for details).

    **kwargs
       Additional keyword arguments passed to
       the :class:`BatchServiceClient` constructor.

    Returns
    -------
    BatchServiceClient
        A client instaniated according to
        the specified configuration.
    """
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()

    return BatchServiceClient(
        credentials=ch.batch_service_principal_credentials,
        batch_url=ch.azure_batch_endpoint,
        **kwargs,
    )


def get_blob_service_client(
    credential_handler: CredentialHandler = None, **kwargs
) -> BlobServiceClient:
    """
    Get an Azure blob service client using
    credentials obtained via a :class:`CredentialHandler`,
    either a user-provided one or a default based on
    environment variables.

    Parameters
    ----------
    credential_handler
       Credential handler for connecting and
       authenticating to Azure resources.
       If ``None``, create a blank
       :class:`EnvCredentialHandler`, which
       attempts to obtain needed credentials
       using information available in local
       environment variables (see its documentation
       for details).

    **kwargs
       Additional keyword arguments passed to
       the :class:`BlobServiceClient` constructor.

    Returns
    -------
    BlobServiceClient
        A client instaniated according to
        the specified configuration.
    """
    ch = credential_handler
    if ch is None:
        ch = EnvCredentialHandler()

    return BlobServiceClient(
        account_url=ch.azure_blob_storage_endpoint,
        credential=ch.client_secret_sp_credential,
        **kwargs,
    )
