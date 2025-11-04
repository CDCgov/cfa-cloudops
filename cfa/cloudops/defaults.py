"""
Default configurations for Azure resources.
"""

import logging
import os

from azure.mgmt.batch import models

logger = logging.getLogger(__name__)


# autoscale default formula
def remaining_task_autoscale_formula(
    task_sample_interval_minutes: int = 15,
    max_number_vms: int = 10,
):
    """Get an autoscaling formula that rescales pools based on the remaining task count.

    Args:
        task_sample_interval_minutes: Task sampling interval, in minutes, as an integer.
            Defaults to 15.
        max_number_vms: Maximum number of virtual machines to spin up, regardless of
            the number of remaining tasks. Defaults to 10.

    Returns:
        str: The autoscale formula, as a string.

    Example:
        >>> # Default settings (15 min interval, max 10 VMs)
        >>> formula = remaining_task_autoscale_formula()
        >>> print(type(formula))  # <class 'str'>

        >>> # Custom settings
        >>> formula = remaining_task_autoscale_formula(
        ...     task_sample_interval_minutes=30,
        ...     max_number_vms=20
        ... )
        >>> print("cappedPoolSize = 20" in formula)  # True
    """
    logger.debug(
        f"Generating autoscale formula with parameters: task_sample_interval_minutes={task_sample_interval_minutes}, max_number_vms={max_number_vms}"
    )

    autoscale_formula_template = """// In this example, the pool size
    // is adjusted based on the number of tasks in the queue.
    // Note that both comments and line breaks are acceptable in formula strings.

    // Get pending tasks for the past 15 minutes.
    $samples = $ActiveTasks.GetSamplePercent(TimeInterval_Minute * {task_sample_interval_minutes});
    // If we have fewer than 70 percent data points, we use the last sample point, otherwise we use the maximum of last sample point and the history average.
    $tasks = $samples < 70 ? max(0, $ActiveTasks.GetSample(1)) :
    max( $ActiveTasks.GetSample(1), avg($ActiveTasks.GetSample(TimeInterval_Minute * {task_sample_interval_minutes})));
    // If number of pending tasks is not 0, set targetVM to pending tasks, otherwise half of current dedicated.
    $targetVMs = $tasks > 0 ? $tasks : max(0, $TargetDedicatedNodes / 2);
    // The pool size is capped at {max_number_vms}, if target VM value is more than that, set it to {max_number_vms}.
    cappedPoolSize = {max_number_vms};
    $TargetDedicatedNodes = max(0, min($targetVMs, cappedPoolSize));
    // Set node deallocation mode - keep nodes active only until tasks finish
    $NodeDeallocationOption = taskcompletion;"""

    logger.debug("Formatting autoscale formula template with provided parameters")
    autoscale_formula = autoscale_formula_template.format(
        task_sample_interval_minutes=task_sample_interval_minutes,
        max_number_vms=max_number_vms,
    )

    logger.debug(
        f"Generated autoscale formula with {len(autoscale_formula)} characters, capped at {max_number_vms} VMs with {task_sample_interval_minutes}min intervals"
    )

    return autoscale_formula


# image defaults
default_image_publisher = "microsoft-dsvm"
default_image_offer = "ubuntu-hpc"
default_image_sku = "2204"
default_node_agent_sku_id = "batch.node.ubuntu 22.04"

# batch info
default_azure_batch_resource_url = "https://batch.core.windows.net/"
default_azure_batch_endpoint_subdomain = "batch.azure.com/"
default_azure_batch_location = "eastus"
default_azure_blob_storage_endpoint_subdomain = "blob.core.windows.net/"
default_azure_container_registry_domain = "azurecr.io"
default_azure_keyvault_endpoint_subdomain = "vault.azure.net"
default_image_reference = models.ImageReference(
    publisher=default_image_publisher,
    offer=default_image_offer,
    sku=default_image_sku,
    version="latest",
)

# this default sets up pools to use containers but does not
# pre-fetch any
default_container_configuration = models.ContainerConfiguration(
    type="dockerCompatible",
)

default_vm_configuration = models.VirtualMachineConfiguration(
    image_reference=default_image_reference,
    container_configuration=default_container_configuration,
    node_agent_sku_id=default_node_agent_sku_id,
)


default_vm_size = "standard_d4s_v3"  # 4 core D-series VM

default_autoscale_evaluation_interval = "PT5M"

default_autoscale_formula = remaining_task_autoscale_formula(
    task_sample_interval_minutes=15,
    max_number_vms=10,
)

default_network_config_dict = dict(
    public_ip_address_configuration=models.PublicIPAddressConfiguration(
        provision="NoPublicIPAddresses"
    )
)


default_pool_config_dict = dict(
    deployment_configuration=models.DeploymentConfiguration(
        virtual_machine_configuration=default_vm_configuration
    ),
    vm_size=default_vm_size,
    target_node_communication_mode="Simplified",
    scale_settings=models.ScaleSettings(
        auto_scale=models.AutoScaleSettings(
            formula=default_autoscale_formula,
            evaluation_interval=default_autoscale_evaluation_interval,
        )
    ),
)


def set_env_vars():
    """Set default Azure environment variables.

    Sets default values for Azure service endpoints and creates new variables
    as a function of existing environment variables.

    Example:
        >>> import os
        >>> set_env_vars()
        >>> print(os.environ["AZURE_BATCH_ENDPOINT_SUBDOMAIN"])
        'batch.azure.com/'
        >>> print(os.environ["AZURE_CONTAINER_REGISTRY_DOMAIN"])
        'azurecr.io'
    """
    logger.debug("Setting default Azure environment variables")

    # save default values
    logger.debug("Setting Azure service endpoint subdomains and URLs")
    os.environ["AZURE_BATCH_ENDPOINT_SUBDOMAIN"] = "batch.azure.com/"
    logger.debug(
        f"Set AZURE_BATCH_ENDPOINT_SUBDOMAIN = {os.environ['AZURE_BATCH_ENDPOINT_SUBDOMAIN']}"
    )

    os.environ["AZURE_BATCH_RESOURCE_URL"] = "https://batch.core.windows.net/"
    logger.debug(
        f"Set AZURE_BATCH_RESOURCE_URL = {os.environ['AZURE_BATCH_RESOURCE_URL']}"
    )

    os.environ["AZURE_KEYVAULT_ENDPOINT_SUBDOMAIN"] = "vault.azure.net"
    logger.debug(
        f"Set AZURE_KEYVAULT_ENDPOINT_SUBDOMAIN = {os.environ['AZURE_KEYVAULT_ENDPOINT_SUBDOMAIN']}"
    )

    os.environ["AZURE_BLOB_STORAGE_ENDPOINT_SUBDOMAIN"] = "blob.core.windows.net/"
    logger.debug(
        f"Set AZURE_BLOB_STORAGE_ENDPOINT_SUBDOMAIN = {os.environ['AZURE_BLOB_STORAGE_ENDPOINT_SUBDOMAIN']}"
    )

    os.environ["AZURE_CONTAINER_REGISTRY_DOMAIN"] = "azurecr.io"
    logger.debug(
        f"Set AZURE_CONTAINER_REGISTRY_DOMAIN = {os.environ['AZURE_CONTAINER_REGISTRY_DOMAIN']}"
    )

    # create new variables as a function of env vars
    logger.debug(
        "Creating derived environment variables from existing Azure account settings"
    )

    batch_account = os.getenv("AZURE_BATCH_ACCOUNT")
    batch_location = os.getenv("AZURE_BATCH_LOCATION")
    os.environ["AZURE_BATCH_ENDPOINT"] = (
        f"https://{batch_account}.{batch_location}.{default_azure_batch_endpoint_subdomain}"
    )
    logger.debug(
        f"Set AZURE_BATCH_ENDPOINT = {os.environ['AZURE_BATCH_ENDPOINT']} (from account: {batch_account}, location: {batch_location})"
    )

    keyvault_name = os.getenv("AZURE_KEYVAULT_NAME")
    os.environ["AZURE_KEYVAULT_ENDPOINT"] = (
        f"https://{keyvault_name}.{default_azure_keyvault_endpoint_subdomain}"
    )
    logger.debug(
        f"Set AZURE_KEYVAULT_ENDPOINT = {os.environ['AZURE_KEYVAULT_ENDPOINT']} (from keyvault: {keyvault_name})"
    )

    blob_account = os.getenv("AZURE_BLOB_STORAGE_ACCOUNT")
    os.environ["AZURE_BLOB_STORAGE_ENDPOINT"] = (
        f"https://{blob_account}.{default_azure_blob_storage_endpoint_subdomain}"
    )
    logger.debug(
        f"Set AZURE_BLOB_STORAGE_ENDPOINT = {os.environ['AZURE_BLOB_STORAGE_ENDPOINT']} (from account: {blob_account})"
    )

    registry_account = os.getenv("AZURE_CONTAINER_REGISTRY_ACCOUNT")
    os.environ["ACR_TAG_PREFIX"] = (
        f"{registry_account}.{default_azure_container_registry_domain}/"
    )
    logger.debug(
        f"Set ACR_TAG_PREFIX = {os.environ['ACR_TAG_PREFIX']} (from registry: {registry_account})"
    )

    logger.debug("Completed setting all default Azure environment variables")


def get_default_pool_identity(
    user_assigned_identity: str,
) -> models.BatchPoolIdentity:
    """Get the default BatchPoolIdentity instance for azuretools.

    Associates a blank UserAssignedIdentities instance to the provided
    user_assigned_identity string.

    Args:
        user_assigned_identity: User-assigned identity, as a string.

    Returns:
        models.BatchPoolIdentity: Instantiated BatchPoolIdentity instance
            using the provided user-assigned identity.

    Example:
        >>> identity = get_default_pool_identity(
        ...     "/subscriptions/.../resourceGroups/.../providers/..."
        ... )
        >>> print(identity.type)
        <PoolIdentityType.user_assigned: 'UserAssigned'>
    """
    logger.debug(
        f"Creating default pool identity for user-assigned identity: {user_assigned_identity}"
    )

    logger.debug("Setting pool identity type to UserAssigned")
    logger.debug("Creating UserAssignedIdentities object for the provided identity")

    pool_identity = models.BatchPoolIdentity(
        type=models.PoolIdentityType.user_assigned,
        user_assigned_identities={
            user_assigned_identity: models.UserAssignedIdentities()
        },
    )

    logger.debug(
        f"Successfully created BatchPoolIdentity with type: {pool_identity.type}"
    )

    return pool_identity


def get_default_pool_config(
    pool_name: str, subnet_id: str, user_assigned_identity: str, **kwargs
) -> models.Pool:
    """Instantiate a Pool instance with default configuration.

    Creates a Pool with the given pool name and subnet id, the default pool identity
    given by get_default_pool_identity, and other defaults specified in
    default_pool_config_dict and default_network_config_dict.

    Args:
        pool_name: Name for the pool. Passed as the ``display_name`` argument
            to the Pool constructor.
        subnet_id: Subnet id for the pool, as a string. Should typically be obtained
            from a configuration file or an environment variable, often via a
            CredentialHandler instance.
        user_assigned_identity: User-assigned identity for the pool, as a string.
            Passed to get_default_pool_identity.
        **kwargs: Additional keyword arguments passed to the Pool constructor,
            potentially overriding settings from default_pool_config_dict.

    Returns:
        models.Pool: The instantiated Pool object.

    Example:
        >>> pool = get_default_pool_config(
        ...     pool_name="my-batch-pool",
        ...     subnet_id="/subscriptions/.../subnets/default",
        ...     user_assigned_identity="/subscriptions/.../resourceGroups/..."
        ... )
        >>> print(pool.display_name)
        'my-batch-pool'
        >>> print(pool.vm_size)
        'standard_d4s_v3'
    """
    logger.debug(f"Creating default pool configuration with name: '{pool_name}'")
    logger.debug(
        f"Additional kwargs provided: {list(kwargs.keys()) if kwargs else 'None'}"
    )

    logger.debug("Getting default pool identity")
    pool_identity = get_default_pool_identity(user_assigned_identity)

    logger.debug(
        f"Using default pool config with: {list(default_pool_config_dict.keys())}"
    )

    # Merge configurations: defaults first, then kwargs overrides
    merged_config = {**default_pool_config_dict, **kwargs}
    logger.debug(f"Final configuration keys: {list(merged_config.keys())}")

    pool = models.Pool(
        identity=pool_identity,
        display_name=pool_name,
        network_configuration=models.NetworkConfiguration(
            subnet_id=subnet_id, **default_network_config_dict
        ),
        **merged_config,
    )

    logger.debug(
        f"Successfully created Pool with display_name: '{pool.display_name}', vm_size: {getattr(pool, 'vm_size', 'unknown')}"
    )

    return pool


def assign_container_config(
    pool_config: models.Pool, container_config: models.ContainerConfiguration
) -> models.Pool:
    """Assign a container configuration to a Pool object (in place).

    Args:
        pool_config: Pool configuration object to modify.
        container_config: ContainerConfiguration object to add to the Pool
            configuration object.

    Returns:
        models.Pool: The modified Pool object.

    Example:
        >>> from azure.mgmt.batch import models
        >>> pool = get_default_pool_config("test", "subnet", "identity")
        >>> container_config = models.ContainerConfiguration(type="dockerCompatible")
        >>> modified_pool = assign_container_config(pool, container_config)
        >>> # Pool is modified in place and returned
        >>> assert modified_pool is pool
    """
    logger.debug(
        f"Assigning container configuration to pool: {getattr(pool_config, 'display_name', 'unknown')}"
    )
    logger.debug(
        f"Container configuration type: {getattr(container_config, 'type', 'unknown')}"
    )

    logger.debug(
        "Accessing pool deployment configuration virtual machine configuration"
    )

    (
        pool_config.deployment_configuration.virtual_machine_configuration.container_configuration
    ) = container_config

    logger.debug(
        "Successfully assigned container configuration to pool (in-place modification)"
    )
    logger.debug("Pool object modified and will be returned")

    return pool_config
