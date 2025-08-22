# Run a hybrid Metaflow job 
## Created by Fawad Rafi (Peraton) for CFA

# Outline
Metaflow is an open-source job orchestration framework that can run tasks locally or remotely in the Cloud as part of a job. 
Each job is defined as a class that inherits from the FlowSpec class and overrides its methods. Tasks within a job are decorated with @step. By default all tasks run locally but we can also run these in a Kubernetes cluster and AWS Batch. Since Azure Batch is not supported in Metaflow currently, this example illustrates how to use a custom Azure Batch Decorator that can run step within Metaflow job remotely using the Azure Batch service. 

A singleton class CFA Azure Batch Pool Service has been added to the custom_metaflow package to encapsulate the API calls needed for creating batch pools and mounting containers. 

![image info](./MetaflowAndParallelTasks.png)

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
  job_id="REPLACE_WITH_JOB_NAME"
  batch_account_name="REPLACE_WITH_BATCH_ACCOUNT"
  batch_service_url="REPLACE_WITH_BATCH_SERVICE_URL"
  pool_vm_size="STANDARD_A2_V2"
  pool_name="REPLACE_WITH_POOL_NAME"
  parallel_pool_limit="1"
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

3. There are two options available for running the job: in a Windows Subsystem for Linux (WSL) or a Docker container from a Windows environment. Metaflow does not offer native support for Windows. 

### Running as Docker container
Replace user name in `Dockerfile` with your CDC EXT user name
```text
ENV USERNAME="YOUR_USER_NAME"
```

Build and run Docker container
```shell
docker build . -t my_metaflow_container
docker run my_metaflow_container
```

### Running in WSL  
```shell
python3 -m venv metaflow_env
source metaflow_env/bin/activate
pip install -r requirements.txt

export USERNAME="YOUR_USER_NAME"

python main.py run
```