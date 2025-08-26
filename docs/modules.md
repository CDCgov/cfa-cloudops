# `cfa-cloudops` Modules

The various modules of `cfa-cloudops` are listed below. Apart from the CloudClient, they help with low-level functionality with the Cloud environment. While some of these functions and classes are used in the CloudClient class, there is much greater flexibility available when using these other modules. The CloudClient provides much more ease of use to the end-user while maintaining a good amount of customizability. These modules are imported in your python script via `from cfa.cloudops import <module_name>`. Each will be discussed in more detail below.
- CloudClient
- auth
- automation
- autoscale
- blob
- client
- defaults
- endpoints
- job
- task
- util


## CloudClient

This class is discussed in detail [here](./CloudClient/index.md).

## auth

This module assists with authentication to the Cloud. It contains a base class called `CredentialHandler`, which holds and creates various Azure information and credentials. This class should not be called directly. The following three classes are built on the `CredentialHandler` and should be used for authentication to the Cloud:

- EnvCredentialHandler: uses a managed identity to authenticate to the Cloud based on environment variables or values in a .env file. 
- SPCredentialHandler: uses a service principal to authenticate to the Cloud based on environment variables or values in a .env file. 
- FederatedCredentialHandler: uses a federated token credential to authenticate to the Cloud based on environment variables or values in a .env file. This is credential method is preferred when using `cfa-cloudops` via GitHub Actions. 

These handlers are discussed in more detail [here](./CloudClient/authentication.md).

## automation

This module is discussed in detail [here](./automation.md).

## autoscale

This module contains two autoscale formulas: `dev_autoscale_formula` and `prod_autoscale_formula`. These are available to use directly as your autoscale formula or as a starting place for you to tweak. They can be imported from `cfa.cloudops.autoscale`. 

The `dev_autoscale_formula` makes use of a combination of half low priority nodes and half dedicated nodes up to a maximum of 10 nodes total.

The `prod_autoscale_formula` uses only dedicated nodes up to a maximum of 25 nodes. 

### Example

The following code creates a pool using the CloudClient and dev_autoscale_formula.

```
from cfa.cloudops.autoscale import dev_autoscale_formula
from cfa.cloudops import CloudClient
cc = CloudClient()
cc.create_pool(
    "test_pool",
    autoscale_formula = dev_autoscale_formula
)

```

## blob

This module assists in interacting with Blob Storage. The following functions are available. Most functions require a Blob Service Client which can be created using the `client` module described below.

- create_storage_container_if_not_exists
- upload_to_storage_container
- download_from_storage_container
- get_node_mount_config

## client

This module helps with the creation of various clients to interact with the Cloud. These clients are the core components that allow you to do this. The following functions are available and return the corresponding client.

- get_batch_management_client
- get_compute_management_client
- get_batch_service_client
- get_blob_service_client

## defaults

This module contains functions that return default values for various Cloud services. 

- remaining_task_autoscale_formula
- default_image_publisher
- default_image_offer
- default_image_sku
- default_node_agent_sku_id
- default_azure_batch_resource_url
- default_azure_batch_endpoint_subdomain
- default_azure_blob_storage_endpoint_subdomain
- default_azure_container_registry_domain
- default_azure_keyvault_endpoint_subdomain
- default_image_reference
- default_container_configuration
- default_vm_configuration
- default_vm_size
- default_autoscale_evaluation_interval
- default_autoscale_formula
- default_network_config_dict
- default_pool_config_dict
- set_env_vars
- get_default_pool_identity
- get_default_pool_config
- assign_container_config


## endpoints

The functions in this module help form endpoints for interacting with the Cloud.

- _construct_https_url
- construct_batch_endpoint
- construct_azure_container_registry_endpoint
- construct_blob_account_endpoint
- construct_blob_container_endpoint
- is_valid_acr_endpoint


## job

This module contains the function `create_job` for creating a job in the Batch environment.

## task

This module helps with tasks in the Cloud environment.

- create_bind_mount_string
- get_container_settings
- output_task_files_to_blob
- get_task_config

## util

This module contains functions that assist in general utility of the Cloud or other functions that interact with the Cloud.

- lookup_service_principal
- ensure_listlike
- sku_to_dict
- lookup_available_vm_skus_for_batch
