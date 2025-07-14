"""
Miscellaneous utilities for interacting with Azure.
"""

import json
import subprocess
from collections.abc import MutableSequence
from urllib.parse import urlparse


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
