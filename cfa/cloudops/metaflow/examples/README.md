# Examples

## Setup
1. Set up the following environment variables in your Linux shell or Dockerfile:
 ```text
 export USERNAME="YOUR_USER_NAME"
 export PYTHONPATH="/path_to_cfa-cloudops/cfa/cloud ops/metaflow:$PYTHONPATH"
 ```

2. Create a `metaflow.env` file and add the following parameters:
```text
# Authentication info
AZURE_CLIENT_SECRET="REPLACE_WITH_SP_CLIENT_SECRET"
AZURE_KEYVAULT_ENDPOINT="REPLACE_WITH_KEYVAULT_ENDPOINT"
AZURE_KEYVAULT_SP_SECRET_ID="REPLACE_WITH_SECRET_ID"
AZURE_SP_CLIENT_ID="REPLACE_WITH_SP_CLIENT_ID"
AZURE_SUBSCRIPTION_ID="REPLACE_WITH_AZURE_SUBSCRIPTION_ID"
AZURE_TENANT_ID="REPLACE_WITH_TENANT_ID"

# Azure account info
AZURE_BATCH_ACCOUNT="REPLACE_WITH_BATCH_ACCOUNT"
AZURE_RESOURCE_GROUP="REPLACE_WITH_RESOURCE_GROUP_NAME"
AZURE_SUBNET_ID="REPLACE_WITH_AZURE_SUBNET_ID"
AZURE_USER_ASSIGNED_IDENTITY="REPLACE_WITH_USER_ASSIGNED_ID"

# Azure Blob storage config
AZURE_BLOB_STORAGE_ACCOUNT="REPLACE_WITH_AZURE_BLOB_STORAGE_ACCOUNT"

# Azure Pool info
POOL_VM_SIZE="STANDARD_A2_V2"
TASK_INTERVAL="10"
```
3. If you will be running the flow spec from a Linux environment, you need to set up the Python virtual environment:
```shell
 python3 -m venv metaflow_env
 source metaflow_env/bin/activate
 pip install -r requirements.txt
```

## Parallel Steps with Distinct Datasets: Read Different Blobs
This is an example of creating one flow spec that contains 2 steps that runs remotely in Azure Batch. Both steps use the same batch pool but use a different Docker container image and command to run the batch task. When the 2 steps complete successfully, the flow spec terminates the batch pool. 

### Steps 
1. Add the following parameters to `metaflow.env` file:
 ```text
 JOB_ID="my_remote_job"
 POOL_NAME="my_pool_1"
 ```
 
The flow spec will read `POOL_NAME` proprty and create a pool with that name. Both steps will create a job with name specified in `JOB_ID` property. Tasks will be added each to the job and execute the Docker container specified in `CONTAINER_IMAGE_NAME`. 


2. To execute the flow spec in Linux shell, run the following command: `python main.py run`

3. To execute the flow spec in Docker container, replace username in `Dockerfile` with your CDC EXT username. Also copy the custom_metaflow folder to the /examples subfolder. 

 ```text
 ENV USERNAME="YOUR_USER_NAME"
 ```

 Then build and run Docker container:
 ```shell
 docker build . -t my_remote_container
 docker run my_remote_container
 ```

## Multiple Parallel Steps with Partitioned Dataset: Process a list of 50 U.S. states
This is an example of creating one flow spec that contains 1 step that runs remotely in Azure Batch. The step processes 50 U.S. states in one Azure batch pool. 

The flow spec contains 4 steps: 
- `start`: initializes 1 batch pool 
- `process_state`: invoked once and receives the list of 50 U.S. states as input parameter
- `join`: executed when the process_state step completes
- `end`: deletes the pool created in start step

This example consumes the least amount of Azure Batch resources. However runtime is longer because the 50 U.S. states are sequentially executed within one job queue in one pool. To achieve higher concurrency, you need to enable autoscaling within the pool.  

### Steps 
1. Modify the `metaflow.env` file and add the following parameters:
 ```text
 JOB_ID="my_parallel_states_job"
 POOL_NAME_PREFIX="my_parallel_states_pool_"
 PARALLEL_POOL_LIMIT="1"
 ```
 
 The `parallel_pool_limit` of 1 means only one Azure batch pool and one step will be spawned. The pool will be assigned the prefix specified in `pool_name_prefix` property followed by an ordinal number (e.g. my_parallel_states_pool_1).  

 The step will create a job with name specified in `JOB_ID` property. Tasks will be added each to the job to process the U.S. states assigned to that step. 

2. To execute the flow spec in Linux shell, run the following command: `python multiple_states.py run`

3. To execute the flow spec in Docker container, replace username in `DockerfileMultipleParallel` with your CDC EXT username
 ```text
 ENV USERNAME="YOUR_USER_NAME"
 ```

 Then build and run Docker container:
 ```shell
 docker build . -t my_parallel_states_container -f DockerfileMultipleParallel
 docker run my_parallel_states_container
 ```


## Multiple Parallel Steps with Partitioned Dataset: Process a list of 50 U.S. states concurrently
This is an example of creating one flow spec that contains 50 steps that run remotely in Azure Batch. Each step processes one U.S. state in a separate Azure batch pool. 

The flow spec contains 4 steps: 
- `start`: initializes 50 batch pools and splits the list of 50 U.S. states into 50 separate lists, one per pool 
- `process_state`: invoked concurrently 50 times, once per pool
- `join`: executed when the 50 concurrent executions of process_state step are complete
- `end`: deletes the 50 pools created in start step

### Steps 
1. Add the following parameters to `metaflow.env`:
 ```text
 JOB_ID="my_parallel_states_job"
 POOL_NAME_PREFIX="my_parallel_states_pool_"
 PARALLEL_POOL_LIMIT="50"
 ```
 
 The `parallel_pool_limit` of 50 means one Azure batch pool will be spawned per step. Each pool will be assigned the prefix specified in `pool_name_prefix` property followed by an ordinal number (e.g. my_parallel_states_pool_1, my_parallel_states_pool_2, my_parallel_states_pool_50, etc.). This will achieve maximum concurrency. However, there is a possibility of the flow spec being terminated if the Azure subscription quota of 100 pools is exceeded. 

 Each step will create a job with name specified in `JOB_ID` property. Tasks will be added each to the job to process the U.S. states assigned to each step. 

2. To execute the flow spec in Linux shell, run the following command: `python multiple_states.py run`

3. To execute the flow spec in Docker container, replace username in `DockerfileMultipleParallel` with your CDC EXT username
 ```text
 ENV USERNAME="YOUR_USER_NAME"
 ```

 Then build and run Docker container:
 ```shell
 docker build . -t my_parallel_states_container -f DockerfileMultipleParallel
 docker run my_parallel_states_container
 ```


## Multiple Parallel Steps with Partitioned Dataset: Process a list of 50 U.S. states in 5 pools with 10 states per pool
This is a variation of the previous example. To avoid exceeding the quota of 100 batch pools in Azure subscription, this example creates 5 distinct pools and distributes the 50 U.S. states uniformly across these pools. 

The flow spec contains 4 steps: 
- `start`: initializes 5 batch pools and splits the list of 50 U.S. states into 5 separate lists, one per pool 
- `process_state`: invoked concurrently 5 times, once per pool
- `join`: executed when the 5 concurrent steps are complete
- `end`: deletes the 5 pools created in start step


### Steps 
1. Add the following parameters to `metaflow.env`:
 ```text
 JOB_ID="my_parallel_states_job"
 POOL_NAME_PREFIX="my_parallel_states_pool_"
 PARALLEL_POOL_LIMIT="5"
 ```
 
2. To execute the flow spec in Linux shell, run the following command: `python multiple_states.py run`

3. To execute the flow spec in Docker container, replace username in `DockerfileMultipleParallel` with your CDC EXT username
 ```text
 ENV USERNAME="YOUR_USER_NAME"
 ```

 Then build and run Docker container:
 ```shell
 docker build . -t my_parallel_states_container -f DockerfileMultipleParallel
 docker run my_parallel_states_container
 ```