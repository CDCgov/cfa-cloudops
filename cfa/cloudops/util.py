"""
Miscellaneous utilities for interacting with Azure.
"""

import json
import logging
import subprocess
from collections.abc import MutableSequence

from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.batch.models import SupportedSku

from .config import get_config_val

logger = logging.getLogger(__name__)


def lookup_service_principal(display_name: str) -> list:
    """Look up an Azure service principal from its display name.

    Requires the Azure CLI.

    Args:
        display_name: The display name of the service principal to look up.

    Returns:
        list: The results, if any, or an empty list if no match was found.

    Raises:
        RuntimeError: If the Azure CLI command fails or is not available.

    Example:
        >>> # Look up a service principal by display name
        >>> sp_list = lookup_service_principal("my-service-principal")
        >>> if sp_list:
        ...     print(f"Found {len(sp_list)} service principal(s)")
        ...     print(f"App ID: {sp_list[0]['appId']}")
        >>> else:
        ...     print("No service principal found")
    """
    logger.debug(
        f"Looking up Azure service principal with display name: '{display_name}'"
    )

    try:
        command = [f"az ad sp list --display-name {display_name}"]
        logger.debug(f"Executing Azure CLI command: {command[0]}")

        result = subprocess.check_output(
            command, shell=True, universal_newlines=True, text=True
        )
        logger.debug(
            f"Azure CLI command executed successfully, result length: {len(result)} characters"
        )

    except Exception as e:
        error_msg = (
            "Attempt to search available Azure "
            "service principals via the "
            "`az ad sp list` command produced an "
            "error. Check that you have the Azure "
            "command line interface (CLI) installed "
            "and in your PATH as `az`, and that you "
            "are authenticated via `az login`"
        )
        logger.debug(f"Azure CLI command failed: {str(e)}")
        raise RuntimeError(error_msg) from e

    logger.debug("Parsing JSON response from Azure CLI")
    parsed = json.loads(result)
    logger.debug(f"Successfully parsed JSON, found {len(parsed)} service principal(s)")

    if parsed:
        logger.debug(
            f"Service principal details: {[sp.get('appId', 'No appId') for sp in parsed]}"
        )
    else:
        logger.debug("No service principals found matching the display name")

    return parsed


def ensure_listlike(x: any) -> MutableSequence:
    """Ensure that an object either behaves like a MutableSequence or return a one-item list.

    If the object is not a MutableSequence, return a one-item list containing the object.
    Useful for handling list-of-strings inputs alongside single strings.

    Based on this `StackOverflow approach <https://stackoverflow.com/a/66485952>`_.

    Args:
        x: The item to ensure is list-like.

    Returns:
        MutableSequence: ``x`` if ``x`` is a MutableSequence, otherwise ``[x]``
            (i.e. a one-item list containing ``x``).

    Example:
        >>> # Single string becomes a list
        >>> result = ensure_listlike("hello")
        >>> print(result)
        ['hello']

        >>> # List stays a list
        >>> result = ensure_listlike(["hello", "world"])
        >>> print(result)
        ['hello', 'world']

        >>> # Works with other types too
        >>> result = ensure_listlike(42)
        >>> print(result)
        [42]
    """
    logger.debug(f"Ensuring input is list-like: type={type(x)}, value={repr(x)}")

    is_mutable_sequence = isinstance(x, MutableSequence)
    logger.debug(f"Input is MutableSequence: {is_mutable_sequence}")

    if is_mutable_sequence:
        logger.debug(f"Returning original MutableSequence with {len(x)} items")
        return x
    else:
        logger.debug("Converting single item to list")
        result = [x]
        logger.debug(f"Created list with 1 item: {repr(result)}")
        return result


def sku_to_dict(sku: SupportedSku):
    """Convert a SupportedSku object to a flat dictionary of property names and values.

    Args:
        sku: The SupportedSku object to convert.

    Returns:
        dict: A flat dictionary with keys 'name', 'family_name',
            'batch_support_end_of_life', 'additional_properties', as well as keys
            and values corresponding to any SkuCapability objects associated to
            the SupportedSku.

    Example:
        >>> from azure.mgmt.batch.models import SupportedSku
        >>> # Assuming we have a SupportedSku object
        >>> sku_dict = sku_to_dict(some_sku)
        >>> print(sku_dict['name'])  # e.g., 'Standard_D2s_v3'
        >>> print(sku_dict['family_name'])  # e.g., 'standardDSv3Family'
        >>> print(sku_dict.get('vCPUs'))  # e.g., '2' (from capabilities)
    """
    logger.debug(
        f"Converting SupportedSku to dictionary: name='{sku.name}', family='{sku.family_name}'"
    )
    logger.debug(f"SKU batch support end of life: {sku.batch_support_end_of_life}")

    if hasattr(sku, "capabilities") and sku.capabilities:
        capabilities_count = len(sku.capabilities)
        logger.debug(f"Processing {capabilities_count} SKU capabilities")
        capabilities_dict = {c.name: c.value for c in sku.capabilities}
        logger.debug(f"SKU capabilities: {list(capabilities_dict.keys())}")
    else:
        logger.debug("No SKU capabilities found")
        capabilities_dict = {}

    if hasattr(sku, "additional_properties") and sku.additional_properties:
        logger.debug(
            f"Additional properties present: {list(sku.additional_properties.keys())}"
        )
    else:
        logger.debug("No additional properties found")

    result_dict = dict(
        name=sku.name,
        family_name=sku.family_name,
        batch_support_end_of_life=sku.batch_support_end_of_life,
        additional_properties=sku.additional_properties,
        **capabilities_dict,
    )

    logger.debug(
        f"Successfully converted SKU to dictionary with {len(result_dict)} keys"
    )

    return result_dict


def lookup_available_vm_skus_for_batch(
    client: BatchManagementClient = None,
    config_dict: dict = None,
    try_env: bool = True,
    to_dict: bool = True,
    **kwargs,
):
    """Look up available VM image SKUs for the given batch service.

    Args:
        client: BatchManagementClient to use when looking up the available images.
            If None, use the output of ``get_batch_management_client()``. Defaults to None.
        config_dict: Configuration dictionary. Passed as the ``config_dict`` argument
            to any internal ``config.get_config_val`` calls. See that function's
            documentation for additional details.
        try_env: Whether to look for configuration values in the available environment
            variables. Passed as the ``try_env`` argument to any internal
            ``config.get_config_val`` calls. See that function's documentation for
            additional details.
        to_dict: Apply ``sku_to_dict`` to the list of results? Defaults to True.
            If False, the result will be a list of SupportedSku objects.
        **kwargs: Additional keyword arguments passed to
            ``BatchManagementClient.location.list_supported_virtual_machine_skus``.

    Returns:
        list: Of supported SKUs, either as dictionaries of property names and values
            (default) or as raw SupportedSku objects (if ``to_dict`` is False).

    Example:
        >>> from azure.mgmt.batch import BatchManagementClient
        >>>
        >>> # Get SKUs as dictionaries (default)
        >>> skus = lookup_available_vm_skus_for_batch()
        >>> for sku in skus[:3]:  # Show first 3
        ...     print(f"Name: {sku['name']}, vCPUs: {sku.get('vCPUs', 'N/A')}")

        >>> # Get raw SupportedSku objects
        >>> raw_skus = lookup_available_vm_skus_for_batch(to_dict=False)
        >>> print(f"Found {len(raw_skus)} available VM SKUs")
        >>> print(f"First SKU: {raw_skus[0].name}")
    """
    logger.debug("Looking up available VM SKUs for batch service")
    logger.debug(
        f"Parameters: client={client is not None}, to_dict={to_dict}, try_env={try_env}"
    )

    if kwargs:
        logger.debug(f"Additional kwargs provided: {list(kwargs.keys())}")

    if client is None:
        logger.debug("No client provided, creating BatchManagementClient")
        from .client import get_batch_management_client

        client = get_batch_management_client(config_dict=config_dict, try_env=try_env)
        logger.debug("Successfully created BatchManagementClient")
    else:
        logger.debug("Using provided BatchManagementClient")

    batch_location = get_config_val(
        "azure_batch_location",
        config_dict=config_dict,
        try_env=try_env,
    )
    logger.debug(f"Using Azure Batch location: '{batch_location}'")

    logger.debug("Calling Azure API to list supported virtual machine SKUs")
    try:
        sku_iterator = client.location.list_supported_virtual_machine_skus(
            location_name=batch_location,
            **kwargs,
        )
        result = [item for item in sku_iterator]
        logger.debug(f"Successfully retrieved {len(result)} VM SKUs from Azure API")

    except Exception as e:
        logger.debug(f"Failed to retrieve VM SKUs from Azure API: {str(e)}")
        raise

    if result:
        sample_sku_names = [sku.name for sku in result[:3]]
        logger.debug(f"Sample SKU names: {sample_sku_names}")
    else:
        logger.debug("No VM SKUs returned from Azure API")

    if to_dict:
        logger.debug("Converting SupportedSku objects to dictionaries")
        result = [sku_to_dict(x) for x in result]
        logger.debug(f"Successfully converted {len(result)} SKUs to dictionaries")
    else:
        logger.debug("Returning raw SupportedSku objects")

    logger.debug(
        f"Returning {len(result)} VM SKUs (as {'dictionaries' if to_dict else 'SupportedSku objects'})"
    )

    return result
