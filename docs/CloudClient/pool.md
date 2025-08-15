# Pool creation with `cfa.cloudops.CloudClient`

Pools in Azure Batch form the compute component which jobs and tasks use to execute commands. A pool is a collection of nodes which can autoscale or be fixed to a certain number of nodes. It can be set with certain properties such as connections to Blob Storage and Container Registry to use for the node. It is encouraged to use autoscaling when possible to avoid additional charges by leaving fixed pools running.

Pools can easily be created with a `CloudClient` object while maintaining the flexibility needed for each unique use case. To create a pool using an instantiated `CloudClient` object, simply use the `create_pool()` method. The following parameters are available:
- pool_name: name to call the pool. Any spaces with be replaced with "_". Required.
- container_image_name: full name of the container image to use. Can be from Azure Container Registry, GitHub, or DockerHub. Optional but pretty necessary.
- mounts: list of mounts to Blob Storage. The mounts are in a tuple form, where the first entry is the name of the Blob Container name, followed by the name to use as referenced in your code. Optional if not connecting to Blob Storage.
- vm_size: the name of the VM size to use. Default is standard_d4s_v3.
- autoscale: either True or False, depending on if autoscale or fixed number of nodes should be used. Default is True.
- autoscale_formula: the full text of an autoscale formula. If not provided a default autoscale formula will be used if autoscale is set to True.
- dedicated_nodes: the number of dedicated nodes to use if not autoscaling. Default is 0.
- low_priority_nodes: the number of low priority nodes to use if not autoscaling. Default is 1.
- max_autoscale_nodes: the maximum number of nodes to autoscale to if using the default autoscale formula. Default is 3.
- task_slots_per_node: the number of task slots per node. Default is 1.
- availability_zones: either regional or zonal policy. Default is regional.
- cache_blobfuse: whether to cache the blobfuse connection on the node. Default is True.

A very basic example of creating a pool with the `CloudClient` is as follows:
```python
client = CloudClient()
client.create_pool("sample_pool_name")
```

## Autoscale Pool Example

For this example, suppose we want to create an autoscale pool with the following setup:
- pool name "sample-pool"
- use the Docker container python:3.10 as the base OS
- connect to two Blob Storage containers
    - the first container is called "input-test" but we reference it as "/input" in our code
    - the second container is called -output-test" but we reference it as "/results" in our code
- use the default autoscale formula
- use up to 10 nodes when scaling
- don't cache the blobfuse

The pool can bet setup in the following way:
```python
client = CloudClient()
client.create_pool(
    pool_name = "sample-pool",
    container_image_name = "python:3.10",
    mounts = [("input-test", "input"), ("output-test", "results")],
    autoscale = True, # this line actually unnecessary
    max_autoscale_nodes = 10,
    cache_blobfuse = False
)
```

## Fixed Pool Example

Say we are performing a lot of debugging in Azure Batch and need a pool to keep its nodes available to run multiple jobs/tasks as we troubleshoot. This is the perfect example of using a fixed pool. For this example, suppose we want to create a fixed pool with the following characteristics:
- pool name "sample-pool-debug"
- container_image_name = "my_azure_registry/azurecr.io/test_repo:latest"
- one connection to Blob Storage to the container "input-files" which we refer to as "/data" in the code
- fixed number of 3 low priority nodes
- 2 task slots per node since the tasks are small

In this case we could run the following:
```python
client = CloudClient()
client.create_pool(
    pool_name = "sample-pool-debug",
    container_image_name = "my_azure_registry/azurecr.io/test_repo:latest",
    mounts = [("input-files", "data")],
    autoscale = False,
    low_priority_nodes = 3,
    task_slots_per_node = 2
)
```

## Containers for Pools

Containers are central to pool creation and task execution. Pools in Azure Batch use containers as the image when spinning up a node. These container images can be pulled from Azure Container Registry, GitHub Container Registry, and DockerHub. There are a couple methods available within `CloudClient` to help with this. Both of these functions can upload local containers to Azure Container Registry, in slightly different use cases.
- package_and_upload_dockerfile: this method packages a local Dockerfile into a Docker image, which gets uploaded to the Azure Container Registry
- upload_docker_image: this method uploads a Docker image to the Azure Container Registry

### `package_and_upload_dockerfile()` in action

This method takes a Dockerfile at the specified path, builds the Docker image, then uploads to the location specified in Azure Container Registry.
```python
client = CloudClient()
client.package_and_upload_dockerfile(
    registry_name = "my_azure_registry",
    repo_name = "test-repo",
    tag = "latest",
    path_to_dockerfile = "./Dockerfile"
)
```


### `upload_docker_image()` in action

This method functions similar to above but takes the already built Docker image, referenced by its full name, and uploads it to the specified location in the Azure Container Registry.
```python
client = CloudClient()
client.upload_docker_image(
    image_name = "local_docker_registry/repo-name:latest",
    registry_name = "my_azure_registry",
    repo_name = "test-repo",
    tag = "latest:
)
```
