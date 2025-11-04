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
    logger.debug(f"Constructing HTTPS URL with netloc: '{netloc}', path: '{path}'")

    quoted_netloc = quote(netloc)
    logger.debug(f"URL-encoded netloc: '{quoted_netloc}'")

    url_components = [
        "https",
        quoted_netloc,
        path,
        "",
        "",
        "",
    ]
    logger.debug(f"URL components: {url_components}")

    constructed_url = urlunparse(url_components)
    logger.debug(f"Successfully constructed URL: '{constructed_url}'")

    return constructed_url


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

    is_default_subdomain = (
        batch_endpoint_subdomain == d.default_azure_batch_endpoint_subdomain
    )
    logger.debug(
        f"Using {'default' if is_default_subdomain else 'custom'} batch endpoint subdomain"
    )

    netloc = f"{batch_account}.{batch_location}.{batch_endpoint_subdomain}"
    logger.debug(f"Assembled batch endpoint netloc: '{netloc}'")

    endpoint_url = _construct_https_url(netloc)
    logger.debug(f"Successfully constructed Azure Batch endpoint: '{endpoint_url}'")

    return endpoint_url


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
    logger.debug(
        f"Constructing Azure Container Registry endpoint: account='{azure_container_registry_account}', domain='{azure_container_registry_domain}'"
    )

    is_default_domain = (
        azure_container_registry_domain == d.default_azure_container_registry_domain
    )
    logger.debug(
        f"Using {'default' if is_default_domain else 'custom'} container registry domain"
    )

    netloc = f"{azure_container_registry_account}.{azure_container_registry_domain}"
    logger.debug(f"Assembled container registry netloc: '{netloc}'")

    endpoint_url = _construct_https_url(netloc)
    logger.debug(
        f"Successfully constructed Azure Container Registry endpoint: '{endpoint_url}'"
    )

    return endpoint_url


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
    logger.debug(
        f"Constructing Azure Blob account endpoint: account='{blob_account}', subdomain='{blob_endpoint_subdomain}'"
    )

    is_default_subdomain = (
        blob_endpoint_subdomain == d.default_azure_blob_storage_endpoint_subdomain
    )
    logger.debug(
        f"Using {'default' if is_default_subdomain else 'custom'} blob storage subdomain"
    )

    netloc = f"{blob_account}.{blob_endpoint_subdomain}"
    logger.debug(f"Assembled blob account netloc: '{netloc}'")

    endpoint_url = _construct_https_url(netloc)
    logger.debug(
        f"Successfully constructed Azure Blob account endpoint: '{endpoint_url}'"
    )

    return endpoint_url


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
    logger.debug(
        f"Constructing Azure Blob container endpoint: container='{blob_container}', account='{blob_account}', subdomain='{blob_endpoint_subdomain}'"
    )

    logger.debug("Getting blob account endpoint for container URL construction")
    account_endpoint = construct_blob_account_endpoint(
        blob_account, blob_endpoint_subdomain
    )
    logger.debug(f"Blob account endpoint: '{account_endpoint}'")

    quoted_container = quote(blob_container)
    logger.debug(f"URL-encoded container name: '{quoted_container}'")

    container_endpoint = urljoin(account_endpoint, quoted_container)
    logger.debug(
        f"Successfully constructed Azure Blob container endpoint: '{container_endpoint}'"
    )

    return container_endpoint


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
    logger.debug(f"Validating Azure Container Registry endpoint: '{endpoint}'")

    logger.debug("Checking for trailing slash in ACR endpoint")
    if endpoint.endswith("/"):
        error_msg = (
            "Azure Container Registry URLs "
            "must not end with a trailing "
            "slash, as this can hamper DNS "
            "lookups of the private registry endpoint. "
            f"Got {endpoint}"
        )
        logger.debug(f"ACR validation failed: trailing slash found - {error_msg}")
        return (False, error_msg)

    logger.debug("Parsing URL to extract domain information")
    domain = urlparse(endpoint).netloc
    logger.debug(f"Extracted domain: '{domain}'")

    logger.debug("Checking if domain ends with 'azurecr.io'")
    if not domain.endswith("azurecr.io"):
        error_msg = (
            "Azure Container Registry URLs "
            "must have the domain "
            f"`azurecr.io`. Got `{domain}`."
        )
        logger.debug(f"ACR validation failed: invalid domain - {error_msg}")
        return (False, error_msg)

    logger.debug("Checking for required subdomain in ACR URL")
    if domain.startswith("azurecr.io"):
        error_msg = (
            "Azure container registry URLs "
            "must have a subdomain, typically "
            "corresponding to the particular "
            "private registry name."
            f"Got {endpoint}"
        )
        logger.debug(f"ACR validation failed: missing subdomain - {error_msg}")
        return (False, error_msg)

    logger.debug(f"ACR endpoint validation passed: '{endpoint}' is valid")
    return (True, None)
