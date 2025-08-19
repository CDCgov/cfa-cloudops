from metaflow.decorators import StepDecorator

from cfa_azure.helpers import (
    add_job, 
    add_task_to_job,
    check_job_exists,
    read_config
)
from azure.identity import (
    ClientSecretCredential,
    ManagedIdentityCredential,
)
from azure.common.credentials import ServicePrincipalCredentials
from cfa_azure.helpers import (
    get_batch_service_client,
    get_sp_secret
)
from cfa_azure.batch_helpers import get_batch_mgmt_client
from cfa_azure.blob_helpers import get_blob_service_client
from functools import wraps
import random
import time
import string

DEFAULT_CONTAINER_IMAGE_NAME = "python:latest"
DEFAULT_TASK_INTERVAL = '10' # Default task interval in seconds

def generate_random_string(length):
    """Generates a random string of specified length using letters and digits."""
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string

class CFAAzureBatchDecorator(StepDecorator):
    """
    Specifies that this step should execute on Azure Batch.

    Parameters
    ----------
    config_file : str
        Path to the JSON configuration file containing Azure Batch settings.
    """

    name = "cfa_azure_batch"
    defaults = {
        'Authentication': None,
        'Batch': None,
        'Container': None,
        'Storage': None
    }

    def __init__(self, batch_pool_service, config_file=None, **kwargs):
        super(CFAAzureBatchDecorator, self).__init__()
        self.attributes = self.defaults.copy()
        self.batch_pool_service = batch_pool_service
        # Load configuration from the JSON file if provided
        if config_file:
            self.attributes.update(read_config(config_file))
        self.docker_command = kwargs.get('docker_command', 'python main.py')

    def setup_secret_credentials(self):
        sp_secret = get_sp_secret(self.attributes, ManagedIdentityCredential())        
        self.secret_cred = ClientSecretCredential(
            tenant_id=self.attributes["Authentication"]["tenant_id"],
            client_id=self.attributes["Authentication"]["sp_application_id"],
            client_secret=sp_secret,
        )
        self.batch_cred = ServicePrincipalCredentials(
            client_id=self.attributes["Authentication"]["sp_application_id"],
            tenant=self.attributes["Authentication"]["tenant_id"],
            secret=sp_secret,
            resource="https://batch.core.windows.net/",
        )
        print("Secret credentials setup complete.")

    def setup_clients(self):
        self.batch_client = get_batch_service_client(self.attributes, self.batch_cred)
        self.batch_mgmt_client = get_batch_mgmt_client(config=self.attributes, credential=self.secret_cred)
        self.blob_service_client = get_blob_service_client(self.attributes, self.secret_cred)

    def fetch_or_create_job(self):
        if 'job_id_prefix' in self.attributes['Batch'] and self.attributes['Batch']['job_id_prefix']:
            job_id_prefix = self.attributes['Batch']['job_id_prefix']
            job_id = f'{job_id_prefix}{generate_random_string(5)}'
        else:
            job_id=self.attributes['Batch']['job_id']
        if check_job_exists(job_id=job_id, batch_client=self.batch_client):
            print(f'Existing Azure batch job {job_id} is being reused')
        else:
            add_job(job_id=job_id, pool_id=self.pool_id, batch_client=self.batch_client, mark_complete=True)
            print("Azure Batch Job created")
        return job_id

    def fetch_or_create_batch_pool(self):
        self.setup_secret_credentials()
        self.setup_clients()
        if 'pool_name_prefix' in self.attributes['Batch'] and self.attributes['Batch']['pool_name_prefix']:
            pool_name_prefix = self.attributes['Batch']['pool_name_prefix']
            pool_name = f'{pool_name_prefix}{generate_random_string(5)}'
        else:
            pool_name = self.attributes['Batch']['pool_name']
        
        task_interval = int(self.attributes['Batch'].get('task_interval', DEFAULT_TASK_INTERVAL))
        time.sleep(task_interval)

        self.pool_id = self.batch_pool_service.fetch_or_create_pool(pool_name, self.attributes, self.batch_mgmt_client, self.blob_service_client, self.secret_cred)
        print(f'Azure batch pool {self.pool_id} was created')

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self.fetch_or_create_batch_pool()
            job_id = self.fetch_or_create_job()
            task_dependencies = None
            if 'parent_task' in self.attributes["Batch"] and self.attributes["Batch"]["parent_task"]:
                task_dependencies = self.attributes["Batch"]["parent_task"].split(",")
            self.task_id = add_task_to_job(
                job_id=job_id, 
                task_id_base=f"{job_id}_task_{generate_random_string(3)}_", 
                docker_command=self.docker_command, 
                batch_client=self.batch_client, 
                full_container_name=DEFAULT_CONTAINER_IMAGE_NAME,
                depends_on=task_dependencies
            )
            return func(*args, **kwargs)
        return wrapper