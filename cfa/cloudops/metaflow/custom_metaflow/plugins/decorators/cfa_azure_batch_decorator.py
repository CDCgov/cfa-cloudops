from metaflow.decorators import StepDecorator

from cfa.cloudops.helpers import (
    add_job, 
    add_task_to_job,
    check_job_exists,
    read_config
)
from azure.common.credentials import ServicePrincipalCredentials
from cfa.cloudops.helpers import (
    get_batch_service_client
)
from functools import wraps
import random
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

    def __init__(self, pool_name, sp_secret, config_file=None, **kwargs):
        super(CFAAzureBatchDecorator, self).__init__()
        self.attributes = self.defaults.copy()
        # Load configuration from the JSON file if provided
        if config_file:
            self.attributes.update(read_config(config_file))
        self.sp_secret = sp_secret
        self.pool_name = pool_name
        self.docker_command = kwargs.get('docker_command', 'python main.py')


    def fetch_or_create_job(self):
        batch_cred = ServicePrincipalCredentials(
            client_id=self.attributes["Authentication"]["sp_application_id"],
            tenant=self.attributes["Authentication"]["tenant_id"],
            secret=self.sp_secret,
            resource="https://batch.core.windows.net/",
        )
        self.batch_client = get_batch_service_client(self.attributes, batch_cred)
        if 'job_id_prefix' in self.attributes['Batch'] and self.attributes['Batch']['job_id_prefix']:
            job_id_prefix = self.attributes['Batch']['job_id_prefix']
            job_id = f'{job_id_prefix}{generate_random_string(5)}'
        else:
            job_id=self.attributes['Batch']['job_id']
        if check_job_exists(job_id=job_id, batch_client=self.batch_client):
            print(f'Existing Azure batch job {job_id} is being reused')
        else:
            add_job(job_id=job_id, pool_id=self.pool_name, batch_client=self.batch_client, mark_complete=True)
            print("Azure Batch Job created")
        return job_id

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
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