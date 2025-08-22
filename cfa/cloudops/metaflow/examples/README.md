# Examples

## Parallel Steps with Distinct Datases: Read Different Blobs
This is an example of creating one flowspec that contains 2 steps that runs remotely in Azure Batch. Both steps use the same batch pool but read data from different Azure blob locations and process it. When the 2 steps complete successfully, the flowspec terminates the batch pool. 

### Steps 
1. Modify the `client_config.toml` file and add the following parameters:
  ```text
  [Batch]
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  job_id="my_remote_job"
  pool_name_prefix="my_parallel_states_pool_"
  pool_vm_size="STANDARD_A2_V2"
  parallel_pool_limit="1"
  ```
 
  The `parallel_pool_limit` of 1 means only one Azure batch pool and one step will be spawned. The pool will be assigned the prefix specified in `pool_name_prefix` property followed by an ordinal number (e.g. my_parallel_states_pool_1).  

  The step will create a job with name specified in `job_id` property. Tasks will be added each to the job to process the U.S. states assigned to that step. 

2. To execute the flowspec n Linux shell, run the following commands:
  ```shell
  python3 -m venv metaflow_env
  source metaflow_env/bin/activate
  pip install -r requirements.txt

  export USERNAME="YOUR_USER_NAME"

  python main.py run
  ```

3. To execute the flowspec in Docker container, replace user name in `DockerfileMultipleParallel` with your CDC EXT user name
  ```text
  ENV USERNAME="YOUR_USER_NAME"
  ```

  Then build and run Docker container:
  ```shell
  docker build . -t my_remote_container
  docker run my_remote_container
  ```


## Multiple Parallel Steps with Partitioned Dataset: Process a list of 50 U.S. states
This is an example of creating one flowspec that contains 1 step that runs remotely in Azure Batch. The step processes 50 U.S. states in one Azure batch pool. When the step has completed successfully, the flowspec deletes the batch pool. This example consumes the least amount of Azure Batch resources. However runtime is longer because the 50 U.S. states are sequentially executed within one job queue in one pool. To achieve higher concurrency, you need to enable autoscaling within the pool.  

### Steps 
1. Modify the `client_config_states.toml` file and add the following parameters:
  ```text
  [Batch]
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  job_id="my_parallel_states_job"
  pool_name_prefix="my_parallel_states_pool_"
  pool_vm_size="STANDARD_A2_V2"
  parallel_pool_limit="1"
  ```
 
  The `parallel_pool_limit` of 1 means only one Azure batch pool and one step will be spawned. The pool will be assigned the prefix specified in `pool_name_prefix` property followed by an ordinal number (e.g. my_parallel_states_pool_1).  

  The step will create a job with name specified in `job_id` property. Tasks will be added each to the job to process the U.S. states assigned to that step. 

2. To execute the flowspec n Linux shell, run the following commands:
  ```shell
  python3 -m venv metaflow_env
  source metaflow_env/bin/activate
  pip install -r requirements.txt

  export USERNAME="YOUR_USER_NAME"

  python multiple_states.py run
  ```

3. To execute the flowspec in Docker container, replace user name in `DockerfileMultipleParallel` with your CDC EXT user name
  ```text
  ENV USERNAME="YOUR_USER_NAME"
  ```

  Then build and run Docker container:
  ```shell
  docker build . -t my_parallel_states_container -f DockerfileMultipleParallel
  docker run my_parallel_states_container
  ```


## Multiple Parallel Steps with Partitioned Dataset: Process a list of 50 U.S. states concurrently
This is an example of creating one flowspec that contains 50 steps that run remotely in Azure Batch. Each step processes one U.S. state in a separate Azure batch pool. When all the steps have completed successfully, the flowspec deletes the 50 batch pools. 

### Steps 
1. Modify the `client_config_states.toml` file and add the following parameters:
  ```text
  [Batch]
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  job_id="my_parallel_states_job"
  pool_name_prefix="my_parallel_states_pool_"
  pool_vm_size="STANDARD_A2_V2"
  parallel_pool_limit="50"
  ```
 
  The `parallel_pool_limit` of 50 means one Azure batch pool will be spawned per step. Each pool will be assigned the prefix specified in `pool_name_prefix` property followed by an ordinal number (e.g. my_parallel_states_pool_1, my_parallel_states_pool_2, my_parallel_states_pool_50, etc). This will achieve maximum concurrency. However, there is a possibility of the flowspec being terminated if the Azure subscription quota of 100 pools is exceeded. 

  Each step will create a job with name specified in `job_id` property. Tasks will be added each to the job to process the U.S. states assigned to each step. 

2. To execute the flowspec n Linux shell, run the following commands:
  ```shell
  python3 -m venv metaflow_env
  source metaflow_env/bin/activate
  pip install -r requirements.txt

  export USERNAME="YOUR_USER_NAME"

  python multiple_states.py run
  ```

3. To execute the flowspec in Docker container, replace user name in `DockerfileMultipleParallel` with your CDC EXT user name
  ```text
  ENV USERNAME="YOUR_USER_NAME"
  ```

  Then build and run Docker container:
  ```shell
  docker build . -t my_parallel_states_container -f DockerfileMultipleParallel
  docker run my_parallel_states_container
  ```


## Multiple Parallel Steps with Partitioned Dataset: Process a list of 50 U.S. states in 5 pools with 10 states per pool
This is a variation of the previous example. To avoid exceeding the quota of 100 batch pools in Azure subscription, this example creates 5 distinct pools and distributes the 50 U.S. states to each pool. The flowspec shall contain 5 remote parallel steps and execute 10 U.S. states per step. When all the steps have completed successfully, the flowspec deletes the 5 batch pools. 

### Steps 
1. Modify the `client_config_states.toml` file and add the following parameters:
  ```text
  [Batch]
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  job_id="my_parallel_states_job"
  pool_name_prefix="my_parallel_states_pool_"
  pool_vm_size="STANDARD_A2_V2"
  parallel_pool_limit="5"
  ```
 
2. To execute the flowspec n Linux shell, run the following commands:
  ```shell
  python3 -m venv metaflow_env
  source metaflow_env/bin/activate
  pip install -r requirements.txt

  export USERNAME="YOUR_USER_NAME"

  python multiple_states.py run
  ```

3. To execute the flowspec in Docker container, replace user name in `DockerfileMultipleParallel` with your CDC EXT user name
  ```text
  ENV USERNAME="YOUR_USER_NAME"
  ```

  Then build and run Docker container:
  ```shell
  docker build . -t my_parallel_states_container -f DockerfileMultipleParallel
  docker run my_parallel_states_container
  ```









# Steps
1. Add a step to the `main.py` workflow for the operation you want to perform in the Azure Batch. Decorate that method with `@cfa_azure_batch`. 
  ```python
  @step
  @cfa_azure_batch
  def perform_remote_task(self):
      # YOUR CODE GOES IN HERE
      self.next(self.end)
  ```
2. Provide the Azure batch configuration in a `client_config.toml` file:
  ```text
  [Authentication]
  subscription_id="REPLACE_WITH_AZURE_SUBSCRIPTION_ID"
  resource_group="REPLACE_WITH_AZURE_RESOURCE_GROUP"
  user_assigned_identity="REPLACE_WITH_USER_ASSIGNED_ID"
  tenant_id="REPLACE_WITH_TENANT_ID"
  batch_application_id="REPLACE_WITH_BATCH_APP_ID"
  batch_object_id="REPLACE_WITH_BATCH_OBJECT_ID"
  sp_application_id="REPLACE_WITH_SERVICE_PRINCIPAL_APP_ID"
  vault_url="REPLACE_WITH_AZURE_VAULT_URL"
  vault_sp_secret_id="REPLACE_WITH_SECRET_ID"
  subnet_id="REPLACE_WITH_AZURE_SUBNET_ID"

  [Batch]
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  pool_vm_size="STANDARD_A2_V2"
  pool_name="REPLACE_WITH_POOL_NAME"
  scaling_mode="fixed"

  [Container]
  container_registry_url="URL_FOR_CONTAINER_REGISTRY"
  container_registry_server="CONTAINER_REGISTRY_SERVER"
  container_registry_username="USER_NAME_FOR_CONTAINER_REGISTRY"
  container_registry_password="PASSWORD_FOR_CONTAINER_REGISTRY"
  container_image_name="CONTAINER_IMAGE_NAME"

  [Storage]
  storage_account_name="AZURE_BLOB_STORAGE_ACCOUNT"
  storage_account_url="AZURE_BLOB_STORAGE_URL"
  ```

