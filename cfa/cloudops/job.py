"""
Utilities for working with Azure Batch jobs.
"""

import logging

from azure.batch import BatchServiceClient, models

logger = logging.getLogger(__name__)


def create_job(
    client: BatchServiceClient,
    job: models.JobAddParameter,
    verify_pool: bool = True,
    exist_ok: bool = False,
    verbose: bool = False,
    **kwargs,
) -> bool:
    """Create an Azure Batch job if it does not already exist.

    Returns True if the job was created successfully. By default, verifies that the
    Azure Batch pool specified for the job exists, erroring if the pool cannot be found.

    If the job itself already exists, errors by default but can also be configured to
    proceed without modifying or deleting the existing job.

    Args:
        client: BatchServiceClient to use when creating the job.
        job: JobAddParameter instance defining the job to add.
        verify_pool: Verify that the specified pool for the job exists before
            attempting to create the job, and error if it cannot be found.
            Defaults to True.
        exist_ok: Proceed if the job already exists (without attempting to
            update/modify/overwrite it)? Defaults to False (error if the job already exists).
        verbose: Message to stdout on success or failure due to job already existing?
            Defaults to False.
        **kwargs: Additional keyword arguments passed to
            ``azure.batch.BatchServiceClient.job.add``.

    Returns:
        bool: True if the job is successfully created. False if the job already
            exists and ``exist_ok`` is set to True.

    Raises:
        ValueError: If the pool for the job cannot be found and ``verify_pool`` is True.
        models.BatchErrorException: If the job exists and ``exist_ok`` is not True.

    Example:
        >>> from azure.batch import BatchServiceClient, models
        >>> client = BatchServiceClient(credentials=..., batch_url=...)
        >>> job = models.JobAddParameter(
        ...     id="my-job",
        ...     pool_info=models.PoolInformation(pool_id="my-pool")
        ... )
        >>>
        >>> # Create job with pool verification
        >>> success = create_job(client, job)
        >>> print(success)  # True if created, False if already exists with exist_ok=True
        >>>
        >>> # Create job allowing existing jobs
        >>> success = create_job(client, job, exist_ok=True, verbose=True)
        Job my-job exists.
    """
    if verify_pool:
        pool_id = job.pool_info.pool_id
        if not client.pool.exists(pool_id):
            raise ValueError(
                f"Attempt to create job {job.id} on "
                f"pool {pool_id}, but could not find "
                "the requested pool. Check that this "
                "pool id is correct and that a pool "
                "with that id exists"
            )
    try:
        client.job.add(job, **kwargs)
        if verbose:
            print(f"Created job {job.id}.")
        return True
    except models.BatchErrorException as e:
        if not (e.error.code == "JobExists" and exist_ok):
            raise e
        if verbose:
            print(f"Job {job.id} exists.")
        return False


def create_job_schedule(
    client: BatchServiceClient,
    cloud_job_schedule: models.JobScheduleAddParameter,
    verify_pool: bool = True,
    exist_ok: bool = False,
    verbose: bool = False,
    **kwargs,
) -> bool:
    """Create an Azure Batch job schedule if it does not already exist.

    Returns True if the job schedule was created successfully. By default, verifies that the
    Azure Batch pool specified for the job schedule exists, erroring if the pool cannot be found.

    Args:
        client: BatchServiceClient to use when creating the job.
        cloud_job_schedule: JobAdJobScheduleAddParameter instance defining the job schedule to add.
        verify_pool: Verify that the specified pool for the job exists before
            attempting to create the job schedule, and error if it cannot be found.
            Defaults to True.
        exist_ok: Proceed if the job schedule already exists (without attempting to
            update/modify/overwrite it)? Defaults to False (error if the job schedule already exists).
        verbose: Message to stdout on success or failure due to job already existing?
            Defaults to False.
        **kwargs: Additional keyword arguments passed to
            ``azure.batch.BatchServiceClient.job_schedule.add``.

    Returns:
        bool: True if the job schedule is successfully created. False if the job schedule already
            exists and ``exist_ok`` is set to True.

    Raises:
        ValueError: If the pool for the job cannot be found and ``verify_pool`` is True.
        models.BatchErrorException: If the job exists and ``exist_ok`` is not True.

    Example:
        >>> from azure.batch import BatchServiceClient, models
        >>> client = BatchServiceClient(credentials=..., batch_url=...)
        >>> schedule = models.Schedule(
        ...     recurrence_interval=datetime.timedelta(hours=1),
        ...     do_not_run_until=datetime.datetime.strptime("2025-01-01 08:00:00", "%Y-%m-%d %H:%M:%S")
        ...     do_not_run_after=datetime.datetime.strptime("2025-01-01 17:00:00", "%Y-%m-%d %H:%M:%S")
        >>> )
        >>> job_manager_task = models.JobManagerTask(
        ...     id="my-job-manager-task",
        ...     command_line="/bin/bash -c 'printenv; echo Job manager task starting.'",
        ...     authentication_token_settings=models.AuthenticationTokenSettings(
        ...         access="job"
        ...     )
        ... )
        >>> job_specification = models.JobSpecification(
        ...     pool_info=models.PoolInformation(pool_id="my-pool"),
        ...     job_manager_task=job_manager_task
        >>> )
        >>> job_schedule_add_param = models.JobScheduleAddParameter(
        ...     id="my-job-schedule",
        ...     display_name="My Job Schedule",
        ...     schedule=schedule,
        ...     job_specification=job_specification,
        >>> )
        >>>
        >>> # Create job with pool verification
        >>> success = create_job_schedule(client, job_schedule_add_param)
        >>> print(success)  # True if created, False if already exists with exist_ok=True
        >>>
        >>> # Create job allowing existing job schedule
        >>> success = create_job_schedule(client, job_schedule_add_param, exist_ok=True, verbose=True)
        Job schedule my-job-schedule" exists.
    """
    if verify_pool:
        pool_id = cloud_job_schedule.job_specification.pool_info.pool_id
        if not client.pool.exists(pool_id):
            raise ValueError(
                f"Attempt to create job schedule {cloud_job_schedule.id} on "
                f"pool {pool_id}, but could not find "
                "the requested pool. Check that this "
                "pool id is correct and that a pool "
                "with that id exists"
            )
    try:
        client.job_schedule.add(cloud_job_schedule, **kwargs)
        if verbose:
            print(f"Created job schedule {cloud_job_schedule.id}.")
        return True
    except models.BatchErrorException as e:
        if not exist_ok:
            raise e
        if verbose:
            print(f"Job schedule {cloud_job_schedule.id} exists.")
        return False
