"""
Utilities for working with Azure Batch jobs.
"""

import logging

from azure.batch import BatchClient, models

logger = logging.getLogger(__name__)


def create_job(
    client: BatchClient,
    job: models.BatchJobCreateOptions,
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

    Requires azure-batch>=15.0.0.

    Args:
        client: BatchClient to use when creating the job.
        job: BatchJobCreateOptions instance defining the job to add.
        verify_pool: Verify that the specified pool for the job exists before
            attempting to create the job, and error if it cannot be found.
            Defaults to True.
        exist_ok: Proceed if the job already exists (without attempting to
            update/modify/overwrite it)? Defaults to False (error if the job already exists).
        verbose: Message to stdout on success or failure due to job already existing?
            Defaults to False.
        **kwargs: Additional keyword arguments passed to
            ``azure.batch.BatchClient.create_job``.

    Returns:
        bool: True if the job is successfully created. False if the job already
            exists and ``exist_ok`` is set to True.

    Raises:
        ValueError: If the pool for the job cannot be found and ``verify_pool`` is True.
        Exception: If the job exists and ``exist_ok`` is not True.

    Example:
        >>> from cfa.cloudops.client import get_batch_service_client
        >>> from azure.batch import models
        >>> client = get_batch_service_client()
        >>> job = models.BatchJobCreateOptions(
        ...     id="my-job",
        ...     pool_info=models.BatchPoolInfo(pool_id="my-pool")
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
    logger.debug(f"Starting create_job for job ID: '{job.id}'")
    logger.debug(
        f"Parameters: verify_pool={verify_pool}, exist_ok={exist_ok}, verbose={verbose}"
    )

    pool_id = job.pool_info.pool_id
    logger.debug(f"Job '{job.id}' configured to use pool: '{pool_id}'")

    if verify_pool:
        logger.debug(f"Pool verification enabled, checking if pool '{pool_id}' exists")
        pool_exists = client.pool_exists(pool_id)
        logger.debug(f"Pool '{pool_id}' exists: {pool_exists}")

        if not pool_exists:
            error_msg = (
                f"Attempt to create job {job.id} on "
                f"pool {pool_id}, but could not find "
                "the requested pool. Check that this "
                "pool id is correct and that a pool "
                "with that id exists"
            )
            logger.debug(f"Pool verification failed: {error_msg}")
            raise ValueError(error_msg)
        else:
            logger.debug(f"Pool verification successful for pool '{pool_id}'")
    else:
        logger.debug("Pool verification disabled, skipping pool existence check")

    logger.debug(f"Attempting to create job '{job.id}' on Azure Batch service")
    if kwargs:
        logger.debug(f"Additional kwargs provided: {list(kwargs.keys())}")
    else:
        logger.debug("No additional kwargs provided")

    try:
        logger.debug(f"Calling client.create_job() for job '{job.id}'")
        client.create_job(job, **kwargs)
        logger.debug(f"Successfully created job '{job.id}' on pool '{pool_id}'")

        if verbose:
            print(f"Created job {job.id}.")

        logger.debug("Job creation completed successfully, returning True")
        return True

    except Exception as e:
        # Handle job already exists error
        error_code = getattr(e, "error_code", None) or (
            getattr(e.error, "code", None) if hasattr(e, "error") else None
        )

        logger.debug(f"Exception caught: {type(e).__name__}, error_code={error_code}")
        logger.debug(
            f"Job exists check: error_code is 'JobExists' = {error_code == 'JobExists'}, exist_ok = {exist_ok}"
        )

        if not (error_code == "JobExists" and exist_ok):
            logger.debug(f"Re-raising exception for job '{job.id}': {error_code}")
            raise e

        logger.debug(
            f"Job '{job.id}' already exists and exist_ok=True, proceeding without error"
        )
        if verbose:
            print(f"Job {job.id} exists.")

        logger.debug("Job already exists scenario, returning False")
        return False


def create_job_schedule(
    client: BatchClient,
    cloud_job_schedule: models.BatchJobScheduleCreateOptions,
    verify_pool: bool = True,
    exist_ok: bool = False,
    verbose: bool = False,
    **kwargs,
) -> bool:
    """Create an Azure Batch job schedule if it does not already exist.

    Returns True if the job schedule was created successfully. By default, verifies that the
    Azure Batch pool specified for the job schedule exists, erroring if the pool cannot be found.

    Requires azure-batch>=15.0.0.

    Args:
        client: BatchClient to use when creating the job schedule.
        cloud_job_schedule: BatchJobScheduleCreateOptions instance defining the job schedule to add.
        verify_pool: Verify that the specified pool for the job exists before
            attempting to create the job schedule, and error if it cannot be found.
            Defaults to True.
        exist_ok: Proceed if the job schedule already exists (without attempting to
            update/modify/overwrite it)? Defaults to False (error if the job schedule already exists).
        verbose: Message to stdout on success or failure due to job schedule already existing?
            Defaults to False.
        **kwargs: Additional keyword arguments passed to
            ``azure.batch.BatchClient.create_job_schedule``.

    Returns:
        bool: True if the job schedule is successfully created. False if the job schedule already
            exists and ``exist_ok`` is set to True.

    Raises:
        ValueError: If the pool for the job cannot be found and ``verify_pool`` is True.
        Exception: If the job schedule exists and ``exist_ok`` is not True.

    Example:
        >>> from cfa.cloudops.client import get_batch_service_client
        >>> from azure.batch import models
        >>> import datetime
        >>> client = get_batch_service_client()
        >>> schedule = models.BatchJobSchedule(
        ...     recurrence_interval=datetime.timedelta(hours=1),
        ...     do_not_run_until=datetime.datetime.strptime("2025-01-01 08:00:00", "%Y-%m-%d %H:%M:%S"),
        ...     do_not_run_after=datetime.datetime.strptime("2025-01-01 17:00:00", "%Y-%m-%d %H:%M:%S")
        ... )
        >>> job_specification = models.BatchJobSpecification(
        ...     pool_info=models.BatchPoolInfo(pool_id="my-pool")
        ... )
        >>> job_schedule = models.BatchJobScheduleCreateOptions(
        ...     id="my-job-schedule",
        ...     display_name="My Job Schedule",
        ...     schedule=schedule,
        ...     job_specification=job_specification,
        ... )
        >>>
        >>> # Create job schedule with pool verification
        >>> success = create_job_schedule(client, job_schedule)
        >>> print(success)  # True if created, False if already exists with exist_ok=True
        >>>
        >>> # Create job schedule allowing existing ones
        >>> success = create_job_schedule(client, job_schedule, exist_ok=True, verbose=True)
        Job schedule my-job-schedule exists.
    """
    logger.debug(
        f"Starting create_job_schedule for schedule ID: '{cloud_job_schedule.id}'"
    )
    logger.debug(
        f"Parameters: verify_pool={verify_pool}, exist_ok={exist_ok}, verbose={verbose}"
    )

    if verify_pool:
        pool_id = cloud_job_schedule.job_specification.pool_info.pool_id
        logger.debug(
            f"Job schedule '{cloud_job_schedule.id}' configured to use pool: '{pool_id}'"
        )
        logger.debug(f"Pool verification enabled, checking if pool '{pool_id}' exists")

        if not client.pool_exists(pool_id):
            error_msg = (
                f"Attempt to create job schedule {cloud_job_schedule.id} on "
                f"pool {pool_id}, but could not find "
                "the requested pool. Check that this "
                "pool id is correct and that a pool "
                "with that id exists"
            )
            logger.debug(f"Pool verification failed: {error_msg}")
            raise ValueError(error_msg)
        else:
            logger.debug(f"Pool verification successful for pool '{pool_id}'")
    else:
        logger.debug("Pool verification disabled, skipping pool existence check")

    logger.debug(
        f"Attempting to create job schedule '{cloud_job_schedule.id}' on Azure Batch service"
    )
    if kwargs:
        logger.debug(f"Additional kwargs provided: {list(kwargs.keys())}")

    try:
        logger.debug(
            f"Calling client.create_job_schedule() for schedule '{cloud_job_schedule.id}'"
        )
        client.create_job_schedule(cloud_job_schedule, **kwargs)
        logger.debug(f"Successfully created job schedule '{cloud_job_schedule.id}'")

        if verbose:
            print(f"Created job schedule {cloud_job_schedule.id}.")

        logger.debug("Job schedule creation completed successfully, returning True")
        return True
    except Exception as e:
        # Handle job schedule already exists error
        error_code = getattr(e, "error_code", None) or (
            getattr(e.error, "code", None) if hasattr(e, "error") else None
        )

        logger.debug(f"Exception caught: {type(e).__name__}, error_code={error_code}")
        logger.debug(
            f"Job schedule exists check: error_code is 'JobScheduleExists' = {error_code == 'JobScheduleExists'}, exist_ok = {exist_ok}"
        )

        if not exist_ok:
            logger.debug(
                f"Re-raising exception for job schedule '{cloud_job_schedule.id}': {error_code}"
            )
            raise e

        if verbose:
            print(f"Job schedule {cloud_job_schedule.id} exists.")

        logger.debug("Job schedule already exists scenario, returning False")
        return False
