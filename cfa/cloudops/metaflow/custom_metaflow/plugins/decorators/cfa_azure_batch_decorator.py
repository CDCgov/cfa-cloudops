from metaflow.decorators import StepDecorator
from azure.batch import models as batch_models
from azure.batch.models import (
    JobConstraints,
    MetadataItem,
    OnAllTasksComplete,
    OnTaskFailure,
)
import datetime
from functools import wraps
import random
import string
from cfa.cloudops import batch_helpers, helpers
from cfa.cloudops.client import get_batch_service_client, get_batch_management_client
from cfa.cloudops.job import create_job

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

    def __init__(self, pool_name, cred, attributes, **kwargs):
        super(CFAAzureBatchDecorator, self).__init__()
        self.attributes = self.defaults.copy()
        self.attributes.update(attributes)
        self.cred = cred
        self.pool_name = pool_name
        self.docker_command = kwargs.get('docker_command', 'python main.py')


    def __create_job(
        self,
        job_name: str,
        task_retries: int = 0,
        mark_complete_after_tasks_run: bool = False,
        timeout: int | None = None,
        uses_deps: bool = True,
        exist_ok=False,
        verify_pool: bool = True,
        verbose=False,
    ):
        job_name = job_name.replace(" ", "")

        if timeout is None:
            _to = None
        else:
            _to = datetime.timedelta(minutes=timeout)

        on_all_tasks_complete = (
            OnAllTasksComplete.terminate_job
            if mark_complete_after_tasks_run
            else OnAllTasksComplete.no_action
        )

        job_constraints = JobConstraints(
            max_task_retry_count=task_retries,
            max_wall_clock_time=_to,
        )

        # add the job
        job = batch_models.JobAddParameter(
            id=job_name,
            pool_info=batch_models.PoolInformation(pool_id=self.pool_name),
            uses_task_dependencies=uses_deps,
            on_all_tasks_complete=on_all_tasks_complete,
            on_task_failure=OnTaskFailure.perform_exit_options_job_action,
            constraints=job_constraints,
            metadata=[
                MetadataItem(
                    name="mark_complete", value=mark_complete_after_tasks_run
                )
            ],
        )

        # Configure task retry settings
        if task_retries > 0:
            job.constraints = job.constraints or batch_models.JobConstraints()
            job.constraints.max_task_retry_count = task_retries

        # Create the job
        create_job(
            self.batch_service_client,
            job,
            exist_ok=exist_ok,
            verify_pool=verify_pool,
            verbose=verbose,
        )


    def add_task(
        self,
        job_name: str,
        command_line: list[str],
        name_suffix: str = "",
        save_logs_to_blob: str | None = None,
        logs_folder: str | None = None,
        depends_on: list[str] | None = None,
        depends_on_range: tuple | None = None,
        run_dependent_tasks_on_fail: bool = False,
        container_image_name: str = None,
        timeout: int | None = None,
    ):
        container_name = container_image_name

        if save_logs_to_blob:
            logs_folder = "stdout_stderr"

        if self.save_logs_to_blob:
            rel_mnt_path = batch_helpers.get_rel_mnt_path(
                blob_name=save_logs_to_blob,
                pool_name=self.pool_name,
                resource_group_name=self.cred.azure_resource_group_name,
                account_name=self.cred.azure_batch_account,
                batch_mgmt_client=self.batch_mgmt_client,
            )
            if rel_mnt_path != "ERROR!":
                rel_mnt_path = "/" + helpers.format_rel_path(
                    rel_path=rel_mnt_path
                )
        else:
            rel_mnt_path = None

        # get all mounts from pool info
        mounts = batch_helpers.get_pool_mounts(
            self.pool_name,
            self.cred.azure_resource_group_name,
            self.cred.azure_batch_account,
            self.batch_mgmt_client,
        )
        tid = batch_helpers.add_task(
            job_name=job_name,
            task_id_base=job_name,
            command_line=command_line,
            save_logs_rel_path=rel_mnt_path,
            logs_folder=logs_folder,
            name_suffix=name_suffix,
            mounts=mounts,
            depends_on=depends_on,
            depends_on_range=depends_on_range,
            run_dependent_tasks_on_fail=run_dependent_tasks_on_fail,
            batch_client=self.batch_client,
            full_container_name=container_name,
            timeout=timeout,
        )
        self.task_id_max += 1
        print(f"Added task {tid} to job {job_name}.")
        return tid


    def fetch_or_create_job(self):
        self.batch_client = get_batch_service_client(self.cred)
        self.batch_mgmt_client = get_batch_management_client(self.cred)

        job_id = self.attributes.get('JOB_ID')
        job_id_prefix = self.attributes.get('JOB_ID_PREFIX')
        if job_id_prefix:
            job_id = f'{job_id_prefix}{generate_random_string(5)}'

        if batch_helpers.check_job_exists(job_id, self.batch_client):
            print(f'Existing Azure batch job {job_id} is being reused')
        else:
            self.__create_job(job_name=job_id, mark_complete_after_tasks_run=True)
            print("Azure Batch Job created")
        return job_id


    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            job_id = self.fetch_or_create_job()
            task_dependencies = None
            parent_tasks = self.attributes.get('PARENT_TASK')
            if parent_tasks:
                task_dependencies = parent_tasks.split(",")
            self.task_id = self.add_task(
                job_name=job_id, 
                command_line=self.docker_command,
                name_suffix=f"{job_id}_task_{generate_random_string(3)}_", 
                depends_on=task_dependencies,
                container_image_name=self.attributes.get("CONTAINER_IMAGE_NAME", DEFAULT_CONTAINER_IMAGE_NAME)
            )
            return func(*args, **kwargs)
        return wrapper
