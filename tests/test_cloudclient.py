import datetime
from unittest.mock import MagicMock, patch

import pytest
from shared_fixtures import FAKE_IMAGES

from cfa.cloudops._cloudclient import CloudClient


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("AZURE_TENANT_ID", "mock-tenant-id")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "mock-subscription-id")
    monkeypatch.setenv("AZURE_CLIENT_ID", "mock-client-id")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "mock-client-secret")


@pytest.fixture
def mock_batch_service_client():
    with patch(
        "cfa.cloudops._cloudclient.get_batch_service_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_batch_management_client():
    with patch(
        "cfa.cloudops._cloudclient.get_batch_management_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_blob_service_client():
    with patch(
        "cfa.cloudops._cloudclient.get_blob_service_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def mock_compute_management_client():
    with patch(
        "cfa.cloudops._cloudclient.get_compute_management_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


@pytest.fixture
def cloud_client(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
):
    with patch("cfa.cloudops._cloudclient.EnvCredentialHandler") as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        return CloudClient(dotenv_path=None, use_sp=False, use_federated=False)


@pytest.fixture
def cloud_client_with_service_principal(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
):
    with patch("cfa.cloudops._cloudclient.SPCredentialHandler") as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        return CloudClient(dotenv_path=None, use_sp=True, use_federated=False)


def test_create_pool_success(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
    cloud_client,
):
    # Mock parameters for create_job
    pool_name = "mock_pool_id"

    with patch("cfa.cloudops.job.create_job", return_value=True):
        result = cloud_client.create_pool(
            pool_name=pool_name,
            mounts=["mock_mount_1", "mock_mount_2"],
            container_image_name="mock_container_image",
        )
        assert result is None
        result = cloud_client.create_pool(
            pool_name=pool_name,
            mounts=["mock_mount_1", "mock_mount_2"],
            container_image_name="mock_container_image",
            autoscale_formula="my-autoscale-formula",
        )
        assert result is None
        result = cloud_client.create_pool(
            pool_name=pool_name,
            mounts=[
                {"source": "input", "target": "mock_mount_1"},
                {"source": "output", "target": "mock_mount_2"},
            ],
            container_image_name="mock_container_image",
            availability_zones="zonal",
            autoscale=False,
        )
        assert result is None


def test_create_job_success(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
    cloud_client,
):
    # Mock parameters for create_job
    job_name = "my-job-1"
    pool_name = "mock_pool_id"

    with patch("cfa.cloudops.job.create_job", return_value=True):
        result = cloud_client.create_job(
            job_name=job_name,
            pool_name=pool_name,
        )
        assert result is None
        result = cloud_client.create_job(
            job_name=job_name,
            pool_name=None,
            save_logs_to_blob=True,
            logs_folder="/logs",
            timeout=300,
        )
        assert result is None
        result = cloud_client.create_job(
            job_name=job_name,
            pool_name=None,
            save_logs_to_blob=True,
            logs_folder="logs/",
            task_id_ints=True,
        )
        assert result is None
        result = cloud_client.create_job(
            job_name=job_name,
            pool_name=None,
            save_logs_to_blob=True,
            task_retries=2,
        )
        assert result is None


def test_create_job_schedule_success(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
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

        # Assertions for job schedule creation
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
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
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
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
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
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
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
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
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
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
):
    with patch("cfa.cloudops._cloudclient.EnvCredentialHandler") as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        client = CloudClient(dotenv_path=None, use_sp=False, use_federated=False)
        assert client.method == "env"
        mock_cred_handler.assert_called_once()
        mock_batch_service_client.assert_called_once()
        mock_batch_management_client.assert_called_once()
        mock_blob_service_client.assert_called_once()
        mock_compute_management_client.assert_called_once()


def test_cloudclient_init_with_default_credentials(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
):
    with patch(
        "cfa.cloudops._cloudclient.DefaultCredentialHandler"
    ) as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        client = CloudClient(dotenv_path=None, use_sp=False, use_federated=True)
        assert client.method == "default"
        mock_cred_handler.assert_called_once()
        mock_batch_service_client.assert_called_once()
        mock_batch_management_client.assert_called_once()
        mock_blob_service_client.assert_called_once()
        mock_compute_management_client.assert_called_once()


def test_cloudclient_init_with_sp_credentials(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
):
    with patch("cfa.cloudops._cloudclient.SPCredentialHandler") as mock_cred_handler:
        mock_cred_handler.return_value = MagicMock()
        client = CloudClient(dotenv_path=None, use_sp=True, use_federated=False)
        assert client.method == "sp"
        mock_cred_handler.assert_called_once()
        mock_batch_service_client.assert_called_once()
        mock_batch_management_client.assert_called_once()
        mock_blob_service_client.assert_called_once()
        mock_compute_management_client.assert_called_once()


def test_add_task(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
    cloud_client_with_service_principal,
):
    job_name = "my-job-1"
    mount_pairs = [{"source": "source", "target": "/mnt/output"}]
    with patch("cfa.cloudops.batch_helpers.add_task", return_value=True):
        with patch(
            "cfa.cloudops.batch_helpers.get_rel_mnt_path", return_value="/mnt/logs/"
        ):
            result = cloud_client_with_service_principal.add_task(
                job_name=job_name,
                command_line="echo Hello World",
                container_image_name="my-image:latest",
            )
            assert result is True
            result = cloud_client_with_service_principal.add_task(
                job_name=job_name,
                command_line="echo Hello World",
            )
            assert result is True
            cloud_client_with_service_principal.save_logs_to_blob = True
            result = cloud_client_with_service_principal.add_task(
                job_name=job_name,
                command_line="echo Hello World",
                mount_pairs=mount_pairs,
            )
            assert result is True
            cloud_client_with_service_principal.full_container_name = "my-container"
            result = cloud_client_with_service_principal.add_task(
                job_name=job_name,
                command_line="echo Hello World",
                mount_pairs=mount_pairs,
            )
            assert result is True


def test_add_task_collection(
    mock_env_vars,
    mock_batch_service_client,
    mock_batch_management_client,
    mock_blob_service_client,
    mock_compute_management_client,
    cloud_client_with_service_principal,
):
    job_name = "my-job-1"
    tasks = [
        {
            "mounts": [{"source": "source"}],
            "command_line": "echo Hello World",
            "name_suffix": "-01",
        },
        {
            "logs_folder": "/logs",
            "command_line": "echo Hello Again",
            "name_suffix": "-02",
        },
    ]
    with patch("cfa.cloudops.batch_helpers.add_task_collection", return_value=True):
        result = cloud_client_with_service_principal.add_task_collection(
            job_name=job_name, tasks=tasks
        )
        assert result is True


def test_add_tasks_from_yaml(cloud_client_with_service_principal):
    fake_task_strings = [
        "python step1.py --id 1",
        "python step2.py --id 2",
    ]
    expected_tasks = [
        {
            "command_line": "python step1.py --id 1",
            "mounts": [],
            "logs_folder": "stdout_stderr",
        },
        {
            "command_line": "python step2.py --id 2",
            "mounts": [],
            "logs_folder": "stdout_stderr",
        },
    ]
    with patch(
        "cfa.cloudops.batch_helpers.get_tasks_from_yaml", return_value=fake_task_strings
    ):
        result = cloud_client_with_service_principal.add_tasks_from_yaml(
            job_name="myjob", base_cmd="python main.py", file_path="fake.yaml"
        )
        assert result == expected_tasks


def test_list_available_images(
    mock_env_vars,
    mock_batch_service_client,
    cloud_client_with_service_principal,
):
    mock_batch_service_client.account.list_supported_images.return_value = FAKE_IMAGES
    result = cloud_client_with_service_principal.list_available_images(
        operating_system="linux"
    )
    result = cloud_client_with_service_principal.list_available_images()
    assert result
