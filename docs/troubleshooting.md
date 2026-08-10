# Troubleshooting


## Purpose

This guide is intended to improve onboarding to cfa-cloudops client capabilities.  The guide documents common issues encountered by CFA users when using cfa-cloudops. It provides guidance on troubleshooting and best practices, and focuses on practical solutions that can be implemented immediately to resolve issues rather than targeting changes to the cfa-cloudops code base.

## Potential Issues and Solutions

### 1\. Authentication/Credential Error

The default authentication method for the `CloudClient` is a Managed Identity. If your Managed Identity on your VM is not setup at all or missing certain permissions, you will experience issues authenticating.

Solution: confirm your VM has the right Managed Identity setup for the Azure environment. If working at CFA, please reach out to the CFA Tools Teams. An easy way to check your Managed Identity is to run `az login --identity` in your terminal.

### 2\. CloudClient and Key Vault Configuration

**Problem**

Calling `CloudClient()` without explicitly passing a Key Vault may fail with an error similar to:

**AttributeError:** A non-None value for attribute azure_batch_account is required to obtain a value for Azure batch endpoint URL.


This indicates that cloudops did not resolve the Azure Batch account configuration needed by the endpoint layer.

**Common Causes**

- An incorrect Key Vault reference. Refer to <https://github.com/cdcent/cfa-cloudops-example> for help with the Key Vault.

- A `.env` file or manually set environment variable may not be sufficient if the rest of the Azure Batch configuration is not loaded successfully.


**Best Practice**

Use the explicit Key Vault argument. Initialize the client by explicitly specifying the Key Vault name. The Key Vault name for CFA use is found [here](https://github.com/cdcent/cfa-cloudops-example).


**Python Script**

`from cloudops import CloudClient`

`cc = CloudClient(keyvault = <cfa_keyvault_name>)`

This is the preferred approach because it avoids ambiguity about whether environment variables are being read correctly.

**Decision Tree**

CloudClient() fails with Azure Batch account error?
Did you pass Key Vault explicitly?

   No ------------------> Use `CloudClient(keyvault = <cfa_keyvault_name>)`

   Yes -----------------> Confirm your Key Vault and Azure Batch access



### 3\. Azure Container Registry (ACR) Configuration

**Problem**

Pushing a container image may fail with errors such as:

`ERROR: Registry names may contain only alpha numeric characters and must be between 5 and 50 characters`

or

`ERROR: Could not connect to the registry login server`


**Common Cause**

A placeholder registry name was copied literally, or the registry name does not correspond to an existing Azure Container Registry available to the user.

For example, names like the following should be treated as placeholders and replaced with your own values:

my_azure_registry

my-azure-registry

**Best Practice**

Use the actual Azure Container Registry assigned for your cloudops environment.

Then use that value consistently in image build, tag, and push steps.

**Decision tree**

Image push fails?
Does the registry name contain underscores?

   Yes ------------------> Replace with a valid ACR name

   No -------------------> Does the registry exist and belong to your environment?

        No ----> Use your assigned registry name
        Yes ---> Check login/network access



### 4\.	Running Python and R Scripts in cloudops Tasks

**Problem**

A task may fail because the script cannot be found inside the container.

**Example**

`Fatal error: cannot open file '/input-test-edp/cloudops_helloworld.r': No such file or directory`

**Common Causes**

- The container cannot locate the script at the specified path.
- The path used in the task command does not match the file location inside the Docker image or mounted input directory.
- Local paths on your computer are not automatically available inside the container. The task command must reference paths that exist inside the container at runtime.
- Incorrect permission privileges preventing reading from the blob.

**Best Practices**

Before running the actual script, run a diagnostic task that prints the working directory and available files.

- Verify the script exists inside the Docker image.
- Verify the path passed to cloudops exactly matches the container path.
- Do not assume the container working directory matches your local project structure.
- If troubleshooting, temporarily print the working directory and directory contents.

**Example**

`pwd`

`ls -R`

This confirms where files are located inside the container.

Use the output to determine the correct path to your script.

**Python Example Pattern**

If your Dockerfile copies a Python script into `/app`, use a command that references `/app`.

`WORKDIR /app
COPY main.py /app/main.py`

Then the task command should use:

`python /app/main.py`


**R Example Pattern**

If your Dockerfile copies an R script into `/app`, use the same pattern.

`WORKDIR /app
COPY cloudops_helloworld.R /app/cloudops_helloworld.R`

Then the task command should use:

`Rscript /app/cloudops_helloworld.R`

**cloudops task example pattern**

Use the appropriate cloudops task submission method for your workflow. The important part is that the command references a valid in-container path.

\# Example pattern only: adapt argument names to the current cloudops API.

`cc.add_task(
    job_name="my-job",
    command="Rscript /app/cloudops_helloworld.R",
)`

**Decision tree**

Script not found?

Did the Dockerfile copy the script into the image?

   No ------------------> Add COPY instruction and rebuild image

   Yes -----------------> Does the task command use the same path?

        No ----> Update task command path
        Yes ---> Run `pwd` and `ls -R` inside the task

Is the blob container mounted to the pool?

   No -------------------> Mount the blob container to the pool before submitting the task

   Yes ------------------> Can the task read the file?

           No -----> Check blob access and permissions
           Yes-----> Confirm the file name and path




### 5\. Dockerfile and Working Directory Consistency

**Problem**

A Dockerfile may define:

`WORKDIR /app`

but the cloudops task command may not reference `/app`, making it unclear where scripts are expected to live.

**Best Practices**

Keep the Dockerfile, task command, and script paths aligned.


**For Python**

`FROM python:3.11-slim`

`WORKDIR /app
COPY main.py /app/main.py`

`CMD ["python", "/app/main.py"]`


**For R:**

`FROM rocker/r-ver:4.3.0`


`WORKDIR /app
COPY cloudops_helloworld.R /app/cloudops_helloworld.R`

`CMD ["Rscript", "/app/cloudops_helloworld.R"]`

When submitting a cloudops task, use paths that match the Dockerfile.

`python /app/main.py`

or:

`Rscript /app/cloudops_helloworld.R`


**Decision tree**

Dockerfile uses `WORKDIR /app`?
Does the task command reference files in /app?

   No ------------------> Update task command or Dockerfile

   Yes -----------------> Rebuild image and rerun a small test task


### 6\. Deleting Pools

**Problem**

Calling pool deletion and then immediately recreating a pool with the same name may fail or skip creation with a message similar to:

`Pool with name demo-pool already exists. Skipping pool creation.`

**Common Cause**

Pool deletion may not complete immediately.  Azure Batch has accepted the delete request, but the pool may still exist for a short period before deletion fully completes.

If you need to recreate a pool with the same name:

- Call the pool deletion method.
- Wait a minutes.
- Verify that the pool no longer exists.
- Recreate the pool.
- If you need to continue immediately, use a new pool name.

**\# Example pattern only: adapt argument names to the current cloudops API.**

`cc.delete_pool(pool_name="my-debug-pool")`

\# Wait and verify deletion before reusing the same name.

\# If continuing immediately, choose a new pool name.

`cc.create_pool(pool_name="my-debug-pool-v2")`


**Decision Tree**

Need to recreate a deleted pool?
Must you reuse the same name?

   No ------------------> Create a pool with a new name

   Yes -----------------> Wait, verify deletion, then recreate


### 7\.  Pool Autoscaling

**Problem**

Users observe that pools do not automatically increase in size despite specifying

`max_autoscale_nodes=5`

**Note**

`max_autoscale_nodes` only applies if the default autoscale formula is used. Otherwise, whatever is specified in the user-specified autoscale formula will take precedent. If number of tasks is less than max autoscale nodes, the number of nodes may be less than the maximum you set.

**Best Practices**

Setting a maximum node count alone does not guarantee that autoscaling will occur.

When creating a pool for debugging or small tests:

- Confirm whether the pool is expected to use autoscaling or fixed nodes.
- Use the simplest pool configuration recommended by the project for small test jobs.
- Submit a small test task before launching a larger workload.
- If tasks stay queued, check pool existence and node availability before assuming the task code is wrong.

**\# Example pattern only: adapt argument names to the current cloudops API.**

`cc.create_pool(
    pool_name="my-debug-pool",
    vm_size="xsmall",
    max_autoscale_nodes=5,
)`


**Decision tree**

Tasks stay queued after pool creation?
Does the pool exist?

   No ------------------> Create the pool first

   Yes -----------------> Are nodes allocated or allocating?

        No ----> Review pool autoscale/fixed-node settings
        Yes ---> Wait for startup, then monitor task state



### 8\.	`cc.monitor_job() fails while downloading task output.`

**Problem**

`cc.monitor_job
(download_task_output=True)` fails when tasks no longer have nodes


**Common Cause**

Azure Batch tasks execute on compute nodes. If the node has been removed (for example, because the pool autoscaled down or was deleted), cloudops cannot retrieve files directly from that node.

**Best Practices**

- Monitor tasks while nodes are still available.
- Download outputs immediately after task completion.
- Avoid deleting pools until outputs have been retrieved.
- If output download fails, recreate the job and rerun the task if the output cannot be recovered elsewhere.

**Example**

\# Monitor the job until it completes

`cc.monitor_job(download_task_output=True)`


**Decision Tree**

Did `monitor_job()` fail?

Did the task(s) finish?

 Yes ----------------------------> Does the pool still have nodes?

        Yes -------------------> Download outputs
        No --------------------> Recreate job and rerun task

No ----------------------------> Continue monitoring



### 9\. Jobs Remain Queued Forever

**Problem**

- Submitted tasks never get started
- Task state remains queued

**Common Causes**

- The Azure Batch pool does not exist or contains no available compute nodes.
- The job has already been marked complete.

**How to Verify**

Before creating a job:
- Verify the pool exists with an appropriate autoscaling formula.
- Job is not marked complete.


**Best Practices**

\# Create or verify the pool first

`cc.create_pool(...)`


\# Then create the job

`cc.create_job(...)`

\# Finally submit tasks

`cc.add_task(...)`


**Decision Tree**

Task queued?

Does pool exist?

Yes --------------------------------> Has the pool finished allocating?

        Yes --------------------> Submit task again
        No ---------------------> Wait for nodes to finish allocating

No -------------------------------> Create pool


### 10\. Intrepreting Cloudops Error Messages

**Problem**

Some CloudOps errors are generic and do not clearly identify the underlying Azure Batch issue.

**Best Practices**

Rather than relying only on the exception:

- Check pool status
- Check node availability
- Check job status
- Check task status

Often the Azure Batch resources state explains the error better than the cloudops exception.

If the message is still unclear, submit an issue on the GitHub repo.


### 11\. Reducing Debugging Time

**Problem**

Debugging is slow.   Even using an extra small VM size, Azure Batch typically requires several minutes to provision compute resources before tasks begin running.

**Common Causes**

This behavior is expected and is not unique to cloudops. All pools take several minutes to spin up nodes no matter the size of the VM.

**Best Practices**

- Expect approximately 4 - 5 minutes before the first task begins.
- Plan to test multiple code changes in a single debugging session rather than repeatedly creating and deleting pools.
- Reuse an existing pool whenever possible.
- Submit small test tasks before launching full production runs.

**Decision Tree**

Create Pool

     |
     v

Submit small test task

      |
      v

  Verify output

      |
      v

Submit larger workload


## General Recommendations

- Use explicit CloudOps configuration over implicit environment discovery when getting started.
- Treat registry names, pool names, and paths in examples as placeholders unless the documentation says otherwise.
- Keep Dockerfile paths and task commands consistent.
- For R and Python tasks, start with a minimal hello-world script before running a full model.
- Use diagnostic commands such as `pwd` and `ls -r` when debugging path problems.
- Do not immediately reuse a pool name after deletion unless you have verified deletion is complete.
- If a task is queued, check pool and node state before changing the task node.


## Quick Troubleshooting Reference

| **Issue**                                  | **Most Likely Cause**              | **Recommended Action**                    |
| ------------------------------------------ | ---------------------------------- | ----------------------------------------- |
| 1. Authentication/Credential Error                          | Managed Identity on your VM is not setup at all or not setup correctly       | Confirm your VM has the right Managed Identity setup for the Azure environment |
| 2. `CloudClient()` initialization fails with Azure Batch account error                          | Cloudops could not resolve Azure Batch configuration from the environment           | Use `CloudClient(keyvault = <cfa_keyvault>)`  <- make sure to use the appropriate keyvault listed in the repo documented above |
| 3. Container image push fails                          | Placeholder or invalid Azure Container Registry name           | Use your actual ACR name of your container registry |
| 4. Python or R scripts cannot be found                          | Script is not present at the path used inside the container           | Run `pwd` and `ls -R`; update Dockerfile or task command   |
| 5.	Dockerfile path does not match task command                          | `WORKDIR`, `COPY`, and command paths are inconsistent           | Align Dockerfile and task command paths   |
| 6. Pool cannot be recreated immediately after deletion                          | Azure Batch has not fully completed deletion           | Wait and verify deletion, or use a new pool name   |
| 7. Pool does not autoscale as expected                          | Pool configuration may define a maximum but not trigger node allocation           | Verify autoscale/fixed-node configuration and node availability   |
| 8. Tasks stay queued                          | Pool missing or no nodes           | Verify pool exists and has active nodes   |
| 9. `monitor_task()` fails                       | Node removed                       | Download outputs before deleting pools    |
| 10. Generic CloudOps error                     | Azure Batch resource issue         | Check pool, node, job, and task status    |
| 11. Slow debugging                             | Azure Batch provisioning           | Reuse pools and batch test changes        |
| 12. Download output fails after one task fails | Tasks completed at different times | Monitor and retrieve outputs individually |
