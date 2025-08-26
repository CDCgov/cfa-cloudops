# Local Execution

There are cases when developers will need to switch between Cloud execution and local execution. Due to the intricacies of Blob Storage, Azure Batch, and other Azure services, it is often difficult to switch between the two environments. The `local` module in `cfa-cloudops` allows for a seamless transition between environments. All that is needed is a change in the imports of your python library. Instead of importing `from cfa.cloudops import ...` we simply run `from cfa.cloudops.local import ...`.

The local module helps with debugging or troubleshooting errors that are appearing in Azure. The local module uses the same python script you would use for Cloud interaction to mimic various Azure services locally. Azure Batch jobs and tasks are executed via Docker containers and Blob Storage containers are local directories.

## A Simple Example

Suppose we have the following python script to start execute a job in Azure Batch.
```
from cfa.cloudops import CloudClient

cc = CloudClient()
cc.create_pool(
    "example-pool",
    container_image_name = "python:3.10-slim",
    mounts = [("input-test", "data")]
    )
cc.create_job(
    "example-job",
    pool_name = "example-pool"
    )
cc.add_task(
    "python3 /data/run-local.py"
    )
```

This same script can be run in a local environment by only switching the import line of the code. Note that the folder 'input-test' needs to exist locally, or you can include the line `cc.create_blob_container("input-test")` to mimic creating a Blob container. The following will now run the same code but in a terminal.
```
from cfa.cloudops.local import CloudClient

cc = CloudClient()
cc.create_pool(
    "example-pool",
    container_image_name = "python:3.10-slim",
    mounts = [("input-test", "data")]
    )
cc.create_job(
    "example-job",
    pool_name = "example-pool"
    )
cc.add_task(
    "python3 /data/run-local.py",
    job_name = "example-job"
    )
```
The above code will create a pool (which is essentially a text file containing various information), a job (another text file with information plus a Docker container), and a task that executes in the job's Docker container.

## Notes
There isn't a one-to-one mapping of every functionality in the `cfa-cloudops` repo to the `local` module, but it covers enough bases of the `CloudClient` and `automation` modules to cover a general workflow. Some parameters in various methods of the `CloudClient` do not affect local execution like they do in Cloud execution because of the constrained environment. Lastly, note that tasks are run sequentially when running locally, rather than in parallel like in the Cloud.
