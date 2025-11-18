import builtins
from unittest.mock import MagicMock, mock_open, patch

import pytest
from azure.batch import models
from azure.storage.blob import BlobProperties

from cfa.cloudops.batch_helpers import (
    add_task,
    download_job_stats,
    get_args_from_yaml,
    get_full_container_image_name,
    get_pool_mounts,
    get_rel_mnt_path,
    monitor_tasks,
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
            state=models.TaskState.completed, execution_info=MagicMock(exit_code=0)
        ),
        MagicMock(
            state=models.TaskState.completed, execution_info=MagicMock(exit_code=0)
        ),
    ]
    mock_batch_client.task.list.return_value = mock_tasks

    with patch("time.sleep", return_value=None):
        all_successful = monitor_tasks(
            job_name="my-job", timeout=30, batch_client=mock_batch_client
        )

    assert all_successful["completed"] is True


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

    mock_batch_client.task.add.assert_called_once()
    added_task = mock_batch_client.task.add.call_args[1]["task"]
    assert added_task.id == "task-base--1"
    assert added_task.command_line == command_line

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
        depends_on=["task-base--0"],
    )
    added_task = mock_batch_client.task.add.call_args[1]["task"]
    assert added_task.id == "task-base--1"
    assert added_task.command_line == command_line

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
        depends_on=["task-base--0"],
        run_dependent_tasks_on_fail=True,
        save_logs_rel_path="/logs/task-logs/",
    )
    added_task = mock_batch_client.task.add.call_args[1]["task"]
    assert added_task.id == "task-base--1"
    assert added_task.command_line.startswith("/bin/bash")

    add_task(
        batch_client=mock_batch_client,
        job_name="my-job",
        task_id_base=task_id_base,
        command_line=command_line,
        depends_on=["task-base--0"],
        run_dependent_tasks_on_fail=True,
        save_logs_rel_path="ERROR!",
        mounts=[{"source": "my-source", "target": "/mnt/data"}],
    )
    added_task = mock_batch_client.task.add.call_args[1]["task"]
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
    file_name = "/tmp/job-stats.json"
    with patch.object(builtins, "print", return_value=True) as mock_print:
        download_job_stats(
            batch_service_client=mock_batch_client,
            job_name="my-job",
            file_name=file_name,
        )
        mock_print.assert_called_with(
            f"Downloaded job statistics report to {file_name}.csv."
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
