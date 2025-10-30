import datetime
from unittest.mock import MagicMock, patch

import pytest

from cfa.cloudops._cloudclient import CloudClient


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("AZURE_TENANT_ID", "mock-tenant-id")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "mock-subscription-id")
    monkeypatch.setenv("AZURE_CLIENT_ID", "mock-client-id")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "mock-client-secret")


@pytest.fixture
def mock_get_batch_service_client():
    with patch(
        "cfa.cloudops._cloudclient.get_batch_service_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_get_batch_management_client():
    with patch(
        "cfa.cloudops._cloudclient.get_batch_management_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_get_blob_service_client():
    with patch(
        "cfa.cloudops._cloudclient.get_blob_service_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_get_compute_management_client():
    with patch(
        "cfa.cloudops._cloudclient.get_compute_management_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def cloud_client(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
):
    with patch(
        "cfa.cloudops._cloudclient.EnvCredentialHandler"
    ) as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        return CloudClient(dotenv_path=None, use_sp=False, use_federated=False)


@pytest.fixture
def cloud_client_with_service_principal(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
):
    with patch(
        "cfa.cloudops._cloudclient.SPCredentialHandler"
    ) as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        return CloudClient(dotenv_path=None, use_sp=True, use_federated=False)


def test_create_job_schedule_success(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
    cloud_client,
):
    # Mock parameters for create_job_schedule
    job_schedule_name = "MockJobSchedule"
    timeout = 600
    pool_name = "mock_pool_id"
    command_line = "cmd /c echo Mock job schedule created!"
    recurrence_interval = datetime.timedelta(hours=1)
    do_not_run_after = "2025-12-31 23:59:59"
    exist_ok = True

    # Mock the create_job_schedule method
    with patch.object(
        cloud_client, "create_job_schedule", return_value=True
    ) as mock_create_job_schedule:
        result = cloud_client.create_job_schedule(
            job_schedule_name=job_schedule_name,
            pool_name=pool_name,
            command=command_line,
            timeout=timeout,
            recurrence_interval=recurrence_interval,
            do_not_run_after=do_not_run_after,
            exist_ok=exist_ok,
        )

        # Assertions
        mock_create_job_schedule.assert_called_once_with(
            job_schedule_name=job_schedule_name,
            pool_name=pool_name,
            command=command_line,
            timeout=timeout,
            recurrence_interval=recurrence_interval,
            do_not_run_after=do_not_run_after,
            exist_ok=exist_ok,
        )
        assert result is True


def test_create_job_schedule_success_with_service_principal(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
    cloud_client_with_service_principal,
):
    # Mock parameters for create_job_schedule
    job_schedule_name = "MockJobSchedule"
    timeout = 600
    pool_name = "mock_pool_id"
    command_line = "cmd /c echo Mock job schedule created!"
    recurrence_interval = datetime.timedelta(hours=1)
    do_not_run_before = "2025-12-01 23:59:59"
    do_not_run_after = "2025-12-31 23:59:59"
    exist_ok = True

    # Mock the create_job_schedule method
    with patch.object(
        cloud_client_with_service_principal,
        "create_job_schedule",
        return_value=True,
    ) as mock_create_job_schedule:
        result = cloud_client_with_service_principal.create_job_schedule(
            job_schedule_name=job_schedule_name,
            pool_name=pool_name,
            command=command_line,
            timeout=timeout,
            recurrence_interval=recurrence_interval,
            do_not_run_before=do_not_run_before,
            do_not_run_after=do_not_run_after,
            exist_ok=exist_ok,
        )

        # Assertions
        mock_create_job_schedule.assert_called_once_with(
            job_schedule_name=job_schedule_name,
            pool_name=pool_name,
            command=command_line,
            timeout=timeout,
            recurrence_interval=recurrence_interval,
            do_not_run_before=do_not_run_before,
            do_not_run_after=do_not_run_after,
            exist_ok=exist_ok,
        )
        assert result is True


def test_delete_job_schedule_success_with_service_principal(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
    cloud_client_with_service_principal,
):
    # Mock parameters for create_job_schedule
    job_schedule_id = "MockJobSchedule"

    # Mock the create_job_schedule method
    with patch.object(
        cloud_client_with_service_principal,
        "delete_job_schedule",
        return_value=None,
    ) as mock_delete_job_schedule:
        result = cloud_client_with_service_principal.delete_job_schedule(
            job_schedule_id=job_schedule_id
        )

        # Assertions
        mock_delete_job_schedule.assert_called_once_with(
            job_schedule_id=job_schedule_id,
        )
        assert result is None


def test_suspend_job_schedule_success_with_service_principal(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
    cloud_client_with_service_principal,
):
    # Mock parameters for create_job_schedule
    job_schedule_id = "MockJobSchedule"

    # Mock the create_job_schedule method
    with patch.object(
        cloud_client_with_service_principal,
        "suspend_job_schedule",
        return_value=None,
    ) as mock_suspend_job_schedule:
        result = cloud_client_with_service_principal.suspend_job_schedule(
            job_schedule_id=job_schedule_id
        )

        # Assertions
        mock_suspend_job_schedule.assert_called_once_with(
            job_schedule_id=job_schedule_id,
        )
        assert result is None


def test_resume_job_schedule_success_with_service_principal(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
    cloud_client_with_service_principal,
):
    # Mock parameters for create_job_schedule
    job_schedule_id = "MockJobSchedule"

    # Mock the create_job_schedule method
    with patch.object(
        cloud_client_with_service_principal,
        "resume_job_schedule",
        return_value=None,
    ) as mock_resume_job_schedule:
        result = cloud_client_with_service_principal.resume_job_schedule(
            job_schedule_id=job_schedule_id
        )

        # Assertions
        mock_resume_job_schedule.assert_called_once_with(
            job_schedule_id=job_schedule_id,
        )
        assert result is None


def test_cloudclient_init_with_env_credentials(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
):
    with patch(
        "cfa.cloudops._cloudclient.EnvCredentialHandler"
    ) as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        client = CloudClient(
            dotenv_path=None, use_sp=False, use_federated=False
        )
        assert client.method == "env"
        mock_cred_handler.assert_called_once()
        mock_get_batch_service_client.assert_called_once()
        mock_get_batch_management_client.assert_called_once()
        mock_get_blob_service_client.assert_called_once()
        mock_get_compute_management_client.assert_called_once()


def test_cloudclient_init_with_default_credentials(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
):
    with patch(
        "cfa.cloudops._cloudclient.DefaultCredentialHandler"
    ) as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        client = CloudClient(
            dotenv_path=None, use_sp=False, use_federated=True
        )
        assert client.method == "default"
        mock_cred_handler.assert_called_once()
        mock_get_batch_service_client.assert_called_once()
        mock_get_batch_management_client.assert_called_once()
        mock_get_blob_service_client.assert_called_once()
        mock_get_compute_management_client.assert_called_once()


def test_cloudclient_init_with_sp_credentials(
    mock_env_vars,
    mock_get_batch_service_client,
    mock_get_batch_management_client,
    mock_get_blob_service_client,
    mock_get_compute_management_client,
):
    with patch(
        "cfa.cloudops._cloudclient.SPCredentialHandler"
    ) as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        client = CloudClient(
            dotenv_path=None, use_sp=True, use_federated=False
        )
        assert client.method == "sp"
        mock_cred_handler.assert_called_once()
        mock_get_batch_service_client.assert_called_once()
        mock_get_batch_management_client.assert_called_once()
        mock_get_blob_service_client.assert_called_once()
        mock_get_compute_management_client.assert_called_once()
