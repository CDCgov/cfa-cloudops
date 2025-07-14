"""
Default configurations for Azure resources.
"""

from azure.mgmt.batch import models


# autoscale default formula
def remaining_task_autoscale_formula(
    task_sample_interval_minutes: int = 15,
    max_number_vms: int = 10,
):
    """
    Get an autoscaling formula that rescales pools based on the remaining task count.

    Parameters
    ----------
    task_sample_interval_minutes
        Task sampling interval, in minutes, as an integer.
        Default 15.

    max_number_vms
        Maximum number of virtual machines to spin
        up, regardless of the number of remaining
        tasks. Default 10.

    Returns
    -------
    str
        The autoscale formula, as a string.
    """
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

    autoscale_formula = autoscale_formula_template.format(
        task_sample_interval_minutes=task_sample_interval_minutes,
        max_number_vms=max_number_vms,
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
    evaluation_interval=default_autoscale_evaluation_interval,
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
