import json
from types import SimpleNamespace
from unittest.mock import MagicMock, mock_open, patch

import pytest
from azure.batch import models
from azure.storage.blob import BlobProperties

from cfa.cloudops.batch_helpers import (
    add_task,
    check_mount_format,
    construct_vm_name,
    download_job_stats,
    get_all_vm_quotas,
    get_args_from_yaml,
    get_full_container_image_name,
    get_pool_mounts,
    get_rel_mnt_path,
    get_task_status,
    get_vm_name,
    get_vm_series_quotas,
    get_vm_size,
    monitor_tasks,
    vm_name_to_family,
)


@pytest.fixture
def mock_griddle():
    with patch(
        "azure.storage.blob.ContainerClient",
        return_value=MagicMock(),
    ) as mock_client:
        fake_blob_properties = [
            {"name": "my_test_1.txt", "size": 100},
            {"name": "my_test_2.txt", "size": 200},
            {"name": "not_my_test_1.csv", "size": 250},
            {"name": "not_my_test_2.json", "size": 50},
            {"name": "large_file_1.parquet", "size": 1e9},
            {"name": "large_file_2.parquet", "size": 2e9},
        ]
        fake_blobs = []
        for fake in fake_blob_properties:
            fake_blob = BlobProperties()
            fake_blob.name = fake["name"]
            fake_blob.size = fake["size"]
            fake_blobs.append(fake_blob)

        mock_client.list_blobs = MagicMock(return_value=fake_blobs)
        yield mock_client


def test_monitor_tasks_all_successful():
    mock_batch_client = MagicMock()
    mock_tasks = [
        MagicMock(
            state=models.BatchTaskState.COMPLETED, execution_info=MagicMock(exit_code=0)
        ),
        MagicMock(
            state=models.BatchTaskState.COMPLETED, execution_info=MagicMock(exit_code=0)
        ),
    ]
    mock_batch_client.list_tasks.return_value = mock_tasks

    with patch("time.sleep", return_value=None):
        all_successful = monitor_tasks(
            job_name="my-job", timeout=30, batch_client=mock_batch_client
        )

    assert all_successful["completed"] is True


def test_monitor_tasks_missing_job_execution_info():
    mock_batch_client = MagicMock()
    mock_batch_client.get_job.return_value = MagicMock(
        as_dict=MagicMock(return_value={"state": "completed"})
    )
    mock_batch_client.list_tasks.return_value = []

    result = monitor_tasks(
        job_name="my-job", timeout=30, batch_client=mock_batch_client
    )

    assert result["completed"] is True
    assert result["terminate_reason"] is None


def test_add_task():
    mock_batch_client = MagicMock()
    task_id_base = "task-base"
    command_line = '/bin/bash -c "echo Hello World"'

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
    )

    mock_batch_client.create_task.assert_called_once()
    added_task = mock_batch_client.create_task.call_args[0][1]
    assert added_task.id == "task-base--1"
    assert added_task.command_line == command_line

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
        depends_on=["task-base--0"],
    )
    added_task = mock_batch_client.create_task.call_args[0][1]
    assert added_task.id == "task-base--1"
    assert added_task.command_line == command_line

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
        depends_on=["task-base--0"],
        run_dependent_tasks_on_fail=True,
    )
    added_task = mock_batch_client.create_task.call_args[0][1]
    assert added_task.id == "task-base--1"
    assert added_task.command_line.startswith("/bin/bash")

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
        depends_on=["task-base--0"],
        run_dependent_tasks_on_fail=True,
        mounts=[{"source": "my-source", "target": "/mnt/data"}],
    )
    added_task = mock_batch_client.create_task.call_args[0][1]
    assert added_task.id == "task-base--1"
    assert added_task.command_line.startswith("/bin/bash")


def test_get_pool_mounts():
    batch_mgmt_client = MagicMock()
    mounts = get_pool_mounts(
        account_name="testaccount",
        resource_group_name="test-rg",
        pool_name="my-pool",
        batch_mgmt_client=batch_mgmt_client,
    )

    assert len(mounts) == 0


def test_download_job_stats():
    mock_batch_client = MagicMock()
    mock_batch_client.list_tasks.return_value = []
    file_name = "/tmp/job-stats.json"
    with patch("cfa.cloudops.batch_helpers.logger") as mock_logger:
        download_job_stats(
            batch_service_client=mock_batch_client,
            job_name="my-job",
            file_name=file_name,
        )
        mock_logger.info.assert_called_with(
            f"Job statistics download completed. File saved as: {file_name}.csv"
        )


def test_get_rel_mnt_path():
    mock_batch_client = MagicMock()
    blob_name = "data/file.txt"

    rel_path = get_rel_mnt_path(
        resource_group_name="test-rg",
        blob_name=blob_name,
        pool_name="my-pool",
        account_name="testaccount",
        batch_mgmt_client=mock_batch_client,
    )

    assert rel_path == "ERROR!"


def test_get_args_from_yaml():
    yaml_content = """
    job:
      id: test-job
      pool_info:
        pool_id: test-pool
      job_manager_task:
        id: job-manager-task
        command_line: /bin/bash -c 'echo Hello World'
    """
    with patch("builtins.open", mock_open(read_data=yaml_content)):
        with patch("griddler.parse", return_value=MagicMock()) as mock_parse:
            mock_parse.to_dict.return_value = [
                {
                    "id": "test-job",
                    "pool_info": {"pool_id": "test-pool"},
                    "job_manager_task": {
                        "id": "job-manager-task",
                        "command_line": "/bin/bash -c 'echo Hello World'",
                    },
                }
            ]
            args = get_args_from_yaml("test.yaml")

            assert type(args) is list


def test_get_full_container_image_name():
    full_image_name = get_full_container_image_name(
        container_name="my-container", registry="docker", acr_name="docker-my-acr"
    )
    assert full_image_name == "my-container"

    full_image_name = get_full_container_image_name(
        container_name="my-container", registry="azure", acr_name="azure-my-acr"
    )
    assert full_image_name.startswith("azure-my-acr")

    full_image_name = get_full_container_image_name(
        container_name="my-container", registry="github", github_org="my-github-org"
    )
    assert full_image_name.startswith("ghcr")


def test_get_full_container_image_name_fail():
    with pytest.raises(ValueError) as excinfo:
        get_full_container_image_name(container_name="my-container")
        assert str(excinfo.value) == (
            "acr_name must be provided for Azure Container Registry"
        )
    with pytest.raises(ValueError) as excinfo:
        get_full_container_image_name(
            container_name="my-container", acr_name="some-acr", registry="github"
        )
        assert str(excinfo.value) == (
            "github_org must be provided for GitHub Container Registry."
        )


def test_check_mount_format():
    # Test with no slashes
    mount = "data"
    formatted_mount = check_mount_format(mount)
    assert formatted_mount == "data"

    # Test with only leading slash
    mount = "/data"
    formatted_mount = check_mount_format(mount)
    assert formatted_mount == "data"

    # Test with only trailing slash
    mount = "data/"
    formatted_mount = check_mount_format(mount)
    assert formatted_mount == "data"

    # Test with nested path
    mount = "/mnt/data/files/"
    with pytest.raises(ValueError) as excinfo:
        formatted_mount = check_mount_format(mount)
        assert (
            str(excinfo.value)
            == "Invalid mount format: /mnt/data/files/. Mount path should not contain internal slashes."
        )


def test_get_task_status_requires_batch_client():
    with pytest.raises(ValueError) as excinfo:
        get_task_status(job_name="my-job", batch_client=None)
    assert str(excinfo.value) == "Batch client must be provided to get task status."


def test_get_task_status_job_does_not_exist():
    mock_batch_client = MagicMock()
    mock_batch_client.list_jobs.return_value = []

    with pytest.raises(ValueError) as excinfo:
        get_task_status(job_name="missing-job", batch_client=mock_batch_client)
    assert str(excinfo.value) == "Job missing-job does not exist."


def test_get_task_status_all_tasks():
    mock_batch_client = MagicMock()
    mock_batch_client.list_jobs.return_value = [MagicMock(id="my-job")]

    mock_batch_client.list_tasks.return_value = [
        MagicMock(
            id="t1",
            state=models.BatchTaskState.RUNNING,
            execution_info=MagicMock(exit_code=None),
        ),
        MagicMock(
            id="t2",
            state=models.BatchTaskState.COMPLETED,
            execution_info=MagicMock(exit_code=0),
        ),
        MagicMock(
            id="t3",
            state="active",
            execution_info=None,
        ),
    ]

    result = get_task_status(job_name="my-job", batch_client=mock_batch_client)
    payload = json.loads(result)

    assert isinstance(payload, list)
    assert {item["id"] for item in payload} == {"t1", "t2", "t3"}
    assert {item["state"] for item in payload} == {"running", "completed", "active"}
    assert next(item["exit_code"] for item in payload if item["id"] == "t3") is None


def test_get_task_status_single_task():
    mock_batch_client = MagicMock()
    mock_batch_client.list_jobs.return_value = [MagicMock(id="my-job")]

    mock_batch_client.list_tasks.return_value = [
        MagicMock(
            id="t1",
            state=models.BatchTaskState.COMPLETED,
            execution_info=MagicMock(exit_code=0),
        ),
        MagicMock(
            id="t2",
            state=models.BatchTaskState.COMPLETED,
            execution_info=MagicMock(exit_code=1),
        ),
    ]

    result = get_task_status(
        job_name="my-job", task_id="t2", batch_client=mock_batch_client
    )
    payload = json.loads(result)

    assert payload == {"id": "t2", "state": "completed", "exit_code": 1}


def test_get_task_status_unknown_task_id():
    mock_batch_client = MagicMock()
    mock_batch_client.list_jobs.return_value = [MagicMock(id="my-job")]
    mock_batch_client.list_tasks.return_value = [
        MagicMock(
            id="t1",
            state=models.BatchTaskState.COMPLETED,
            execution_info=MagicMock(exit_code=0),
        )
    ]

    with pytest.raises(ValueError) as excinfo:
        get_task_status(
            job_name="my-job", task_id="nope", batch_client=mock_batch_client
        )
    assert str(excinfo.value) == "Task nope does not exist in job my-job."


def test_get_vm_size_valid_and_invalid():
    assert get_vm_size("Small") == "Standard_D4ads_v5"

    with pytest.raises(ValueError) as excinfo:
        get_vm_size("tiny")
    assert "Invalid size descriptor" in str(excinfo.value)


def test_get_all_vm_quotas_filters_positive_quotas_only():
    mock_batch_mgmt_client = MagicMock()
    mock_batch_mgmt_client.batch_account.get.return_value = SimpleNamespace(
        dedicated_core_quota_per_vm_family=[
            SimpleNamespace(name="standardDADSv5Family", core_quota=32),
            SimpleNamespace(name="standardEASv5Family", core_quota=0),
            SimpleNamespace(name="standardFsv2Family", core_quota=-2),
        ]
    )

    result = get_all_vm_quotas(
        batch_mgmt_client=mock_batch_mgmt_client,
        resource_group="rg",
        account_name="acct",
    )

    assert result == [{"name": "standardDADSv5Family", "quota": 32}]


def test_get_vm_series_quotas_for_str_and_list():
    mock_batch_mgmt_client = MagicMock()
    mock_batch_mgmt_client.batch_account.get.return_value = SimpleNamespace(
        dedicated_core_quota_per_vm_family=[
            SimpleNamespace(name="standardDADSv5Family", core_quota=48),
            SimpleNamespace(name="standardEADSv5Family", core_quota=24),
            SimpleNamespace(name="standardFsv2Family", core_quota=16),
        ]
    )

    d_series = get_vm_series_quotas(
        series="d",
        batch_mgmt_client=mock_batch_mgmt_client,
        resource_group="rg",
        account_name="acct",
    )
    de_series = get_vm_series_quotas(
        series=["D", "e"],
        batch_mgmt_client=mock_batch_mgmt_client,
        resource_group="rg",
        account_name="acct",
    )

    assert d_series == [{"name": "standardDADSv5Family", "quota": 48}]
    assert {item["name"] for item in de_series} == {
        "standardDADSv5Family",
        "standardEADSv5Family",
    }


def test_vm_name_to_family_and_construct_vm_name():
    vm_name = "standard_D4ads_v5"
    family = vm_name_to_family(vm_name)
    assert family == "standardDADSv5Family"
    assert construct_vm_name(family, cores=4) == vm_name

    with pytest.raises(ValueError) as excinfo:
        vm_name_to_family("bad_vm")
    assert "Unexpected vm_name format" in str(excinfo.value)


def test_get_vm_name_no_verify_and_verify_errors():
    assert (
        get_vm_name(
            series="D", cores=4, amd=True, temp_disk=True, ssd=True, verify=False
        )
        == "standard_D4ads_v5"
    )

    with pytest.raises(ValueError) as excinfo:
        get_vm_name(series="D", verify=True)
    assert "must be provided when verify=True" in str(excinfo.value)


def test_get_vm_name_verify_unavailable_has_suggestions():
    mock_batch_mgmt_client = MagicMock()
    mock_batch_mgmt_client.batch_account.get.return_value = SimpleNamespace(
        dedicated_core_quota_per_vm_family=[
            SimpleNamespace(name="standardDASv5Family", core_quota=16),
            SimpleNamespace(name="standardDDSv5Family", core_quota=32),
        ]
    )

    with pytest.raises(ValueError) as excinfo:
        get_vm_name(
            series="D",
            cores=4,
            amd=True,
            temp_disk=True,
            ssd=True,
            verify=True,
            batch_mgmt_client=mock_batch_mgmt_client,
            resource_group="rg",
            account_name="acct",
        )

    assert "VM standard_D4ads_v5 is not available in the current quota." in str(
        excinfo.value
    )
    assert "Similar available VMs:" in str(excinfo.value)
