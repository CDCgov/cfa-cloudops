import datetime

import azure.batch.models as batchmodels
import pytest

from cfa.cloudops import CloudClient


@pytest.fixture(
    params=[
        {
            "AZURE_CLIENT_ID": "mock_api_key_for_class",  # pragma: allowlist secret
            "AZURE_CLIENT_SECRET": "mock_db_host_for_class",  # pragma: allowlist secret
            "AZURE_TENANT_ID": "mock_api_key_for_class",  # pragma: allowlist secret
            "AZURE_SUBSCRIPTION_ID": "mock_db_host_for_class",  # pragma: allowlist secret
        }
    ]
)
def cloud_client(mocker, monkeypatch, request):
    """
    Fixture that sets up environment variables and returns a CloudClient instance.
    """
    for key, value in request.param.items():
        monkeypatch.setenv(key, value)
    # Mock the return call
    mocker.patch(
        "cfa.cloudops.CloudClient.get_batch_service_client()",
        return_value=True,
    )
    return CloudClient()


def test_cloud_client_reads_env(mocker, cloud_client):
    job_specification = batchmodels.JobSpecification(
        pool_info=batchmodels.PoolInformation(pool_id="at_pool_1"),
        on_all_tasks_complete=batchmodels.OnAllTasksComplete.terminate_job,
        job_manager_task=batchmodels.JobManagerTask(
            id="job-manager-task",
            command_line="cmd /c echo Hello world from the job schedule!",
        ),
    )
    assert cloud_client.create_job_schedule(
        job_schedule_name="Data Processing Job Schedule 0",
        job_specification=job_specification,
        timeout=900,
        recurrence_interval=datetime.timedelta(hours=2),
        do_not_run_after="2025-12-31 23:00:00",
        exist_ok=True,
    )
