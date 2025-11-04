import datetime
from unittest.mock import MagicMock

import pytest
from azure.batch import BatchServiceClient, models

from cfa.cloudops.job import create_job_schedule


@pytest.fixture
def mock_batch_client():
    # Create a mock BatchServiceClient
    mock_client = MagicMock(spec=BatchServiceClient)

    # Mock the 'pool' attribute and its 'exists' method
    mock_client.pool = MagicMock()
    mock_client.pool.exists = MagicMock(return_value=True)

    # Mock the 'job_schedule' attribute and its 'add' method
    mock_client.job_schedule = MagicMock()
    mock_client.job_schedule.add = MagicMock(return_value=None)

    return mock_client


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


def test_create_job_schedule_success(mock_batch_client, mock_job_schedule):
    # Call the create_job_schedule function
    create_job_schedule(
        client=mock_batch_client,
        cloud_job_schedule=mock_job_schedule,
        verify_pool=True,
        exist_ok=False,
        verbose=True,
    )

    # Assertions
    mock_batch_client.pool.exists.assert_called_once_with("my-pool")
    mock_batch_client.job_schedule.add.assert_called_once_with(mock_job_schedule)
