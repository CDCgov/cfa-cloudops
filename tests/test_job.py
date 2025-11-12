import datetime
from unittest.mock import MagicMock

import pytest
from azure.batch import BatchServiceClient, models
from azure.batch.models import (
    JobConstraints,
    MetadataItem,
    OnAllTasksComplete,
    OnTaskFailure,
)

from cfa.cloudops.job import create_job, create_job_schedule


@pytest.fixture
def mock_batch_client():
    mock_client = MagicMock(spec=BatchServiceClient)
    mock_client.pool = MagicMock()
    mock_client.pool.exists = MagicMock(return_value=True)
    mock_client.job = MagicMock()
    mock_client.job.add = MagicMock(return_value=None)
    mock_client.job_schedule = MagicMock()
    mock_client.job_schedule.add = MagicMock(return_value=None)
    return mock_client


@pytest.fixture
def mock_job():
    # No task dependencies, no retries, no timeout
    return models.JobAddParameter(
        id="my-job",
        pool_info=models.PoolInformation(pool_id="my-pool"),
        uses_task_dependencies=False,
        on_all_tasks_complete=OnAllTasksComplete.terminate_job,
        on_task_failure=OnTaskFailure.perform_exit_options_job_action,
        constraints=JobConstraints(
            max_task_retry_count=0,
            max_wall_clock_time=None,
        ),
        metadata=[MetadataItem(name="mark_complete", value=False)],
    )


@pytest.fixture
def mock_job_schedule():
    schedule = models.Schedule(
        recurrence_interval=datetime.timedelta(hours=1),
        do_not_run_until=datetime.datetime.strptime(
            "2025-01-01 08:00:00", "%Y-%m-%d %H:%M:%S"
        ),
        do_not_run_after=datetime.datetime.strptime(
            "2025-01-01 17:00:00", "%Y-%m-%d %H:%M:%S"
        ),
    )
    job_manager_task = models.JobManagerTask(
        id="my-job-manager-task",
        command_line="/bin/bash -c 'printenv; echo Job manager task starting.'",
        authentication_token_settings=models.AuthenticationTokenSettings(access="job"),
    )
    job_specification = models.JobSpecification(
        pool_info=models.PoolInformation(pool_id="my-pool"),
        job_manager_task=job_manager_task,
    )
    return models.JobScheduleAddParameter(
        id="my-job-schedule",
        display_name="My Job Schedule",
        schedule=schedule,
        job_specification=job_specification,
    )


def test_create_job_success(mock_batch_client, mock_job):
    result = create_job(
        client=mock_batch_client,
        job=mock_job,
        verify_pool=True,
        exist_ok=False,
        verbose=True,
    )
    assert result
    mock_batch_client.job.add.assert_called_once_with(mock_job)


def test_create_job_success_alternate(mock_batch_client, mock_job):
    additional_kwargs = {
        "priority": 100,
        "metadata": [{"name": "custom_metadata", "value": "test_value"}],
    }
    result = create_job(
        client=mock_batch_client,
        job=mock_job,
        verify_pool=False,
        exist_ok=True,
        verbose=False,
        **additional_kwargs,
    )
    assert result
    mock_batch_client.job.add.assert_called_once_with(mock_job, **additional_kwargs)


def test_create_job_no_pool(mock_batch_client, mock_job):
    mock_batch_client.pool.exists.return_value = False
    with pytest.raises(ValueError) as excinfo:
        create_job(
            client=mock_batch_client,
            job=mock_job,
            verify_pool=True,
            exist_ok=False,
            verbose=True,
        )
    assert str(excinfo.value) == (
        "Attempt to create job my-job on pool my-pool, but could not find the requested pool. "
        "Check that this pool id is correct and that a pool with that id exists"
    )


def test_create_job_exists_error(mock_batch_client, mock_job):
    mock_deserializer = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 409  # HTTP 409 Conflict
    mock_response.code = "JobExists."
    batch_error_exception = models.BatchErrorException(
        response=mock_response,
        deserialize=mock_deserializer,
    )
    mock_batch_client.job.add.side_effect = batch_error_exception
    with pytest.raises(models.BatchErrorException) as excinfo:
        create_job(
            client=mock_batch_client,
            job=mock_job,
            verify_pool=True,
            exist_ok=False,
            verbose=True,
        )
    assert str(excinfo.value).startswith("Request encountered an exception")


def test_create_job_schedule_success(mock_batch_client, mock_job_schedule):
    create_job_schedule(
        client=mock_batch_client,
        cloud_job_schedule=mock_job_schedule,
        verify_pool=True,
        exist_ok=False,
        verbose=True,
    )
    mock_batch_client.pool.exists.assert_called_once_with("my-pool")
    mock_batch_client.job_schedule.add.assert_called_once_with(mock_job_schedule)


def test_create_job_schedule_no_pool(mock_batch_client, mock_job_schedule):
    mock_batch_client.pool.exists.return_value = False
    with pytest.raises(ValueError) as excinfo:
        create_job_schedule(
            client=mock_batch_client,
            cloud_job_schedule=mock_job_schedule,
            verify_pool=True,
            exist_ok=False,
            verbose=True,
        )
    assert str(excinfo.value) == (
        "Attempt to create job schedule my-job-schedule on pool my-pool, but could not find the requested pool. "
        "Check that this pool id is correct and that a pool with that id exists"
    )


def test_create_job_schedule_exists_error(mock_batch_client, mock_job_schedule):
    mock_deserializer = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 409  # HTTP 409 Conflict
    mock_response.code = "JobScheduleExists."
    batch_error_exception = models.BatchErrorException(
        response=mock_response,
        deserialize=mock_deserializer,
    )
    mock_batch_client.job_schedule.add.side_effect = batch_error_exception
    with pytest.raises(models.BatchErrorException) as excinfo:
        create_job_schedule(
            client=mock_batch_client,
            cloud_job_schedule=mock_job_schedule,
            verify_pool=True,
            exist_ok=False,
            verbose=True,
        )
    assert str(excinfo.value).startswith("Request encountered an exception")
