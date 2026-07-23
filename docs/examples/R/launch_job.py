# instantiate the CoudClient
from cfa.cloudops import CloudClient

cc = CloudClient(keyvault="my_keyvault")  # replace with CFA keyvault name

# package and upload the Dockerfile to Azure Container Registry
# this will make the Dockerfile available to the Azure Batch pool when it is created
container_name = cc.package_and_upload_dockerfile(
    registry_name="my_azure_registry",  # replace with Azure Container Registry name
    repo_name="simple_test",
    tag="R",
    path_to_dockerfile="./Dockerfile",  # this line only needed if Dockerfile
)

# create the pool
cc.create_pool(
    pool_name="r_test_pool",
    vm_size="small",
    container_image_name=container_name,
)

# create the job
cc.create_job(job_name="r_test_job", pool_name="r_test_pool")

# add the tasks, one with arg and one without
cc.add_task(job_name="r_test_job", command_line="Rscript /app/r_helloworld.r")

cc.add_task(
    job_name="r_test_job",
    command_line="Rscript /app/r_helloworld.r --user 'CloudOps User'",
)

# monitor tasks
cc.monitor_job(job_name="r_test_job")
