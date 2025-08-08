"""
Helper functions for constructing Azure endpoint URLs.
"""

import logging
from urllib.parse import quote, urljoin, urlparse, urlunparse

import cfa.cloudops.defaults as d

logger = logging.getLogger(__name__)


def _construct_https_url(netloc: str, path: str = "") -> str:
    """Construct a simple https URL via urllib.parse.urlunparse.

    Args:
        netloc: netloc value for urlunparse (subdomains and domain).
        path: path value for urlunparse (path after the domain).

    Returns:
        str: The URL, as a string.

    Example:
        >>> url = _construct_https_url("example.com", "/api/v1")
        >>> print(url)
        'https://example.com/api/v1'

        >>> url = _construct_https_url("subdomain.example.com")
        >>> print(url)
        'https://subdomain.example.com'
    """
    return urlunparse(
        [
            "https",
            quote(netloc),
            path,
            "",
            "",
            "",
        ]
    )


def construct_batch_endpoint(
    batch_account: str,
    batch_location: str,
    batch_endpoint_subdomain: str = d.default_azure_batch_endpoint_subdomain,
) -> str:
    """Construct an Azure Batch endpoint URL from the account name, location, and subdomain.

    Args:
        batch_account: Name of the Azure batch account.
        batch_location: Location of the Azure batch servers, e.g. "eastus".
        batch_endpoint_subdomain: Azure batch endpoint subdomains and domains
            that follow the account and location, e.g. "batch.azure.com/", the default.

    Returns:
        str: The endpoint URL.

    Example:
        >>> url = construct_batch_endpoint("mybatch", "eastus")
        >>> print(url)
        'https://mybatch.eastus.batch.azure.com/'

        >>> url = construct_batch_endpoint("mybatch", "westus", "custom.domain.com/")
        >>> print(url)
        'https://mybatch.westus.custom.domain.com/'
    """
    return _construct_https_url(
        f"{batch_account}.{batch_location}.{batch_endpoint_subdomain}"
    )


def construct_azure_container_registry_endpoint(
    azure_container_registry_account: str,
    azure_container_registry_domain: str = d.default_azure_container_registry_domain,
) -> str:
    """Construct an Azure container registry endpoint URL from the account name and domain.

    Args:
        azure_container_registry_account: Name of the Azure container registry account.
        azure_container_registry_domain: Domain for the Azure container registry.
            Typically "azurecr.io", the default.

    Returns:
        str: The registry endpoint URL.

    Example:
        >>> url = construct_azure_container_registry_endpoint("myregistry")
        >>> print(url)
        'https://myregistry.azurecr.io'

        >>> url = construct_azure_container_registry_endpoint("myregistry", "custom.domain.io")
        >>> print(url)
        'https://myregistry.custom.domain.io'
    """
    return _construct_https_url(
        f"{azure_container_registry_account}.{azure_container_registry_domain}"
    )


def construct_blob_account_endpoint(
    blob_account: str,
    blob_endpoint_subdomain: str = d.default_azure_blob_storage_endpoint_subdomain,
) -> str:
    """Construct an Azure blob storage account endpoint URL.

    Args:
        blob_account: Name of the Azure blob storage account.
        blob_endpoint_subdomain: Azure blob endpoint subdomains and domains
            that follow the account, e.g. "blob.core.windows.net/", the default.

    Returns:
        str: The endpoint URL.

    Example:
        >>> url = construct_blob_account_endpoint("mystorageaccount")
        >>> print(url)
        'https://mystorageaccount.blob.core.windows.net/'

        >>> url = construct_blob_account_endpoint("mystorageaccount", "custom.blob.domain/")
        >>> print(url)
        'https://mystorageaccount.custom.blob.domain/'
    """
    return _construct_https_url(f"{blob_account}.{blob_endpoint_subdomain}")


def construct_blob_container_endpoint(
    blob_container: str,
    blob_account: str,
    blob_endpoint_subdomain: str = d.default_azure_blob_storage_endpoint_subdomain,
) -> str:
    """Construct an endpoint URL for a blob storage container.

    Constructs the URL from the container name, account name, and endpoint subdomain.

    Args:
        blob_container: Name of the blob storage container.
        blob_account: Name of the Azure blob storage account.
        blob_endpoint_subdomain: Azure Blob endpoint subdomains and domains
            that follow the account name, e.g. "blob.core.windows.net/", the default.

    Returns:
        str: The endpoint URL.

    Example:
        >>> url = construct_blob_container_endpoint("mycontainer", "mystorageaccount")
        >>> print(url)
        'https://mystorageaccount.blob.core.windows.net/mycontainer'

        >>> url = construct_blob_container_endpoint("data", "storage", "custom.blob.domain/")
        >>> print(url)
        'https://storage.custom.blob.domain/data'
    """
    return urljoin(
        construct_blob_account_endpoint(blob_account, blob_endpoint_subdomain),
        quote(blob_container),
    )


def is_valid_acr_endpoint(endpoint: str) -> tuple[bool, str | None]:
    """Check whether an Azure container registry endpoint is valid given CFA ACR configurations.

    Args:
        endpoint: Azure Container Registry endpoint to validate.

    Returns:
        tuple[bool, str | None]: First entry: True if validation passes, else False.
            Second entry: None if validation passes, else a string indicating
            what failed validation.

    Example:
        >>> valid, error = is_valid_acr_endpoint("https://myregistry.azurecr.io")
        >>> print(valid)  # True
        >>> print(error)  # None

        >>> valid, error = is_valid_acr_endpoint("https://myregistry.azurecr.io/")
        >>> print(valid)  # False
        >>> print("trailing slash" in error)  # True

        >>> valid, error = is_valid_acr_endpoint("https://azurecr.io")
        >>> print(valid)  # False
        >>> print("subdomain" in error)  # True
    """
    if endpoint.endswith("/"):
        return (
            False,
            (
                "Azure Container Registry URLs "
                "must not end with a trailing "
                "slash, as this can hamper DNS "
                "lookups of the private registry endpoint. "
                f"Got {endpoint}"
            ),
        )

    domain = urlparse(endpoint).netloc

    if not domain.endswith("azurecr.io"):
        return (
            False,
            (
                "Azure Container Registry URLs "
                "must have the domain "
                f"`azurecr.io`. Got `{domain}`."
            ),
        )

    if domain.startswith("azurecr.io"):
        return (
            False,
            (
                "Azure container registry URLs "
                "must have a subdomain, typically "
                "corresponding to the particular "
                "private registry name."
                f"Got {endpoint}"
            ),
        )

    return (True, None)
