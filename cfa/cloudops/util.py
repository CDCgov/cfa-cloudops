"""
Miscellaneous utilities for interacting with Azure.
"""

import json
import subprocess
from collections.abc import MutableSequence
from urllib.parse import quote, urljoin, urlparse, urlunparse

from .defaults import (
    default_azure_batch_endpoint_subdomain,
    default_azure_blob_storage_endpoint_subdomain,
    default_azure_container_registry_domain,
)


def lookup_service_principal(display_name: str) -> list:
    """
    Look up an Azure service principal from its display name.

    Requires the Azure CLI.

    Parameters
    ----------
    display_name
        The display name of the service
        principal to look up.

    Returns
    -------
    list
        The results, if any, or an empty
        list if no match was found.
    """
    try:
        command = [f"az ad sp list --display-name {display_name}"]
        result = subprocess.check_output(
            command, shell=True, universal_newlines=True, text=True
        )
    except Exception as e:
        raise RuntimeError(
            "Attempt to search available Azure "
            "service principals via the "
            "`az ad sp list` command produced an "
            "error. Check that you have the Azure "
            "command line interface (CLI) installed "
            "and in your PATH as `az`, and that you "
            "are authenticated via `az login`"
        ) from e
    parsed = json.loads(result)
    return parsed


def ensure_listlike(x: any) -> MutableSequence:
    """
    Ensure that an object either behaves like a
    :class:`MutableSequence` and if not return a
    one-item :class:`list` containing the object.

    Useful for handling list-of-strings inputs
    alongside single strings.

    Based on this `StackOverflow approach
    <https://stackoverflow.com/a/66485952>`_.

    Parameters
    ----------
    x
        The item to ensure is :class:`list`-like.

    Returns
    -------
    MutableSequence
        ``x`` if ``x`` is a :class:`MutableSequence`
        otherwise ``[x]`` (i.e. a one-item list containing
        ``x``).
    """
    return x if isinstance(x, MutableSequence) else [x]


def is_valid_acr_endpoint(endpoint: str) -> tuple[bool, str]:
    """
    Check whether an Azure container
    registry endpoint is valid given
    CFA ACR configurations.

    Parameters
    ----------
    endpoint
        Azure Container Registry endpoint to validate.

    Returns
    -------
    tuple[bool, str]
        First entry: ``True`` if validation passes, else ``False``.
        Second entry: ``None`` if validation passes, else
        a string indicating what failed validation.
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


def _construct_https_url(netloc: str, path: str = "") -> str:
    """
    Construct a simple https URL via
    :func:`urllib.parse.urlunparse`.

    Parameters
    ----------
    netloc
        ``netloc`` value for :func:`~urllib.parse.urlunparse`
        (subdomains and domain).

    path
        ``path`` value for :func:`~urllib.parse.urlunparse`
        (path after the domain).

    Returns
    -------
    str
        The URL, as a string.
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
    batch_endpoint_subdomain: str = default_azure_batch_endpoint_subdomain,
) -> str:
    """
    Construct an Azure Batch endpoint URL from
    the account name, location, and subdomain.

    Parameters
    ----------
    batch_account
        Name of the Azure batch account.

    batch_location
        Location of the Azure batch servers,
        e.g. ``"eastus"``.

    batch_endpoint_subdomain
        Azure batch endpoint subdomains and domains
        that follow the account and location, e.g.
        ``"batch.azure.com/"``, the default.

    Returns
    -------
    str
        The endpoint URL.
    """
    return _construct_https_url(
        f"{batch_account}.{batch_location}.{batch_endpoint_subdomain}"
    )


def construct_azure_container_registry_endpoint(
    azure_container_registry_account: str,
    azure_container_registry_domain: str = default_azure_container_registry_domain,
) -> str:
    """
    Construct an Azure container registry endpoint URL
    from the account name, location, and subdomain.

    Parameters
    ----------
    azure_container_registry_account
        Name of the Azure container registry account.

    azure_container_registry_domain
        Domain for the Azure container registry. Typically
        ``"azurecr.io"``, the default.

    Returns
    -------
    str
        The registry endpoint URL.
    """
    return _construct_https_url(
        f"{azure_container_registry_account}.{azure_container_registry_domain}"
    )


def construct_blob_account_endpoint(
    blob_account: str,
    blob_endpoint_subdomain: str = default_azure_blob_storage_endpoint_subdomain,
) -> str:
    """
    Construct an Azure blob storage account endpoint URL.

    Parameters
    ----------
    blob_account
        Name of the Azure blob storage account.

    blob_endpoint_subdomain
        Azure batch endpoint subdomains and domains
        that follow the account, e.g.
        ``"blob.core.windows.net/"``, the default.

    Returns
    -------
    str
       The endpoint URL.
    """
    return _construct_https_url(f"{blob_account}.{blob_endpoint_subdomain}")


def construct_blob_container_endpoint(
    blob_container: str,
    blob_account: str,
    blob_endpoint_subdomain: str = default_azure_blob_storage_endpoint_subdomain,
) -> str:
    """
    Construct an endpoint URL for a blob storage container
    from the container name, account name, and endpoint subdomain.

    Parameters
    ----------
    blob_container
        Name of the blob storage container.

    blob_account
        Name of the Azure blob storage account.

    blob_endpoint_subdomain
        Azure Blob endpoint subdomains and domains
        that follow the account name, e.g.
        ``"blob.core.windows.net/"``, the default.

    Returns
    -------
    str
       The endpoint URL.
    """
    return urljoin(
        construct_blob_account_endpoint(blob_account, blob_endpoint_subdomain),
        quote(blob_container),
    )
