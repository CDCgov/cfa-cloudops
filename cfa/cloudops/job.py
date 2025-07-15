"""
Utilities for working with Azure Batch jobs.
"""

from azure.batch import BatchServiceClient, models


def create_job(
    client: BatchServiceClient,
    job: models.JobAddParameter,
    verify_pool: bool = True,
    exist_ok: bool = False,
    verbose: bool = False,
    **kwargs,
) -> bool:
    """
    Create an Azure Batch job if it does
    not already exist, returning ``True``
    if the job was created successfully.

    By default, verifies that the Azure Batch pool
    specified for the job exists, erroring if the
    pool cannot be found.

    If the job itself already exists, errors by default
    but can also be configured to proceed without
    modifying or deleting the existing job.

    Parameters
    ----------
    client
        :class:`BatchServiceClient` to use when
        creating the job.

    job
        :class:`~azure.batch.models.JobAddParameter` instance defining
        the job to add.

    verify_pool
        Verify that the specified pool for the job exists before
        attempting to create the job, and error if it cannot be
        found? Default ``True``.

    exist_ok
        Proceed if the job already exists (without attempting
        to update/modify/overwrite it)? Default ``False``
        (error if the job already exists).

    verbose
        Message to stdout if on success or failure
        due to job already existing? Default ``False``.

    **kwargs
        Additional keyword arguments passed to
        :meth:`azure.batch.BatchServiceClient.job.add`.

    Returns
    -------
    bool
        ``True`` if the job is successfully created.
        ``False`` if the job already exists and
        ``exist_ok`` is set to ``True``.

    Raises
    ------
    ValueError
        If the pool for the job cannot be found and ``verify_pool``
        is ``True``.

    models.BatchErrorException
        if the job exists and ``exist_ok is not ``True``.
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
