import builtins
from unittest.mock import MagicMock, patch

import anyio
import pytest
from azure.batch import models
from azure.storage.blob import BlobProperties

from cfa.cloudops.blob import (
    _async_download_blob_folder,
    get_node_mount_config,
)


class MockLogger:
    def __init__(self, name: str):
        self.name = name
        self.messages = []
        self.handlers = []

    def debug(self, message):
        self.messages.append(("DEBUG", message))

    def info(self, message):
        self.messages.append(("INFO", message))

    def warning(self, message):
        self.messages.append(("WARNING", message))

    def error(self, message):
        self.messages.append(("ERROR", message))

    def addHandler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def removeHandler(self, handler):
        if handler in self.handlers:
            self.handlers.remove(handler)

    def assert_logged(self, level, message):
        assert (level, message) in self.messages, (
            f"Expected log ({level}, {message}) not found."
        )


@pytest.fixture(autouse=True)
def mock_logging(monkeypatch):
    """
    Monkeypatch the logging library to use a mock logger.
    """
    mock_logger = MockLogger(name=__name__)
    monkeypatch.setattr("logging.getLogger", lambda name=None: mock_logger)
    return mock_logger


@pytest.fixture
def mock_get_container_client():
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


@pytest.fixture
def mock_compute_node():
    return models.ComputeNodeIdentityReference(resource_id="mock-resource-id")


def test_get_node_mount_config_success(mock_compute_node):
    mounts = get_node_mount_config(
        storage_containers=["mock-container-1", "mock-container-2"],
        account_names=["mock-account-1"],
        identity_references=mock_compute_node,
    )
    assert mounts
    assert len(mounts) == 2


def test_get_node_mount_config_success_alternate(mock_compute_node):
    mounts = get_node_mount_config(
        storage_containers=["mock-container-1", "mock-container-2"],
        account_names=["mock-account-1"],
        identity_references=mock_compute_node,
        mount_names=["mount1", "mount2"],
        cache_blobfuse=True,
    )
    assert mounts
    assert len(mounts) == 2


def test_get_node_mount_config_errors(mock_compute_node):
    with pytest.raises(ValueError) as excinfo:
        get_node_mount_config(
            storage_containers=["mock-container-1", "mock-container-2"],
            account_names=["mock-account-1", "mock-account-2", "mock-account-3"],
            identity_references=mock_compute_node,
        )
    assert str(excinfo.value).startswith(
        "Must either provide a single `account_names`value (as a string or a length-1 list) to cover all `storage_containers` values or provide one `account_names` value for each `storage_containers` value"
    )
    with pytest.raises(ValueError) as excinfo:
        get_node_mount_config(
            storage_containers=["mock-container-1", "mock-container-2"],
            account_names=["mock-account-1", "mock-account-2"],
            mount_names=["mount1"],
            identity_references=mock_compute_node,
        )
    assert str(excinfo.value).startswith(
        "Must provide exactly as many `mount_names` as `storage_containers` to mount"
    )
    bad_compute_nodes = [
        models.ComputeNodeIdentityReference(resource_id="mock-resource-id-1"),
        models.ComputeNodeIdentityReference(resource_id="mock-resource-id-2"),
        models.ComputeNodeIdentityReference(resource_id="mock-resource-id-23"),
    ]
    with pytest.raises(ValueError) as excinfo:
        get_node_mount_config(
            storage_containers=["mock-container-1", "mock-container-2"],
            account_names=["mock-account-1", "mock-account-2"],
            identity_references=bad_compute_nodes,
        )
    assert str(excinfo.value).startswith(
        "Must either provide a single `identity_references`value"
    )


@pytest.mark.asyncio
async def test__async_download_blob_folder_success(
    monkeypatch, mock_get_container_client, mock_logging
):
    local_folder = anyio.Path("testdata")
    monkeypatch.setattr(builtins, "input", lambda _: "Y")
    with patch.object(builtins, "print", return_value=True) as mock_print:
        await _async_download_blob_folder(
            container_client=mock_get_container_client,
            local_folder=local_folder,
            max_concurrent_downloads=10,
        )
        mock_print.assert_called_with(
            "Warning: Total size of files to download is greater than 2 GB."
        )
    await _async_download_blob_folder(
        container_client=mock_get_container_client,
        local_folder=local_folder,
        max_concurrent_downloads=10,
        name_starts_with="my_test",
        include_extensions=".txt",
    )
    await _async_download_blob_folder(
        container_client=mock_get_container_client,
        local_folder=local_folder,
        max_concurrent_downloads=10,
        exclude_extensions=".parquet",
    )
    assert mock_logging.messages == []


@pytest.mark.asyncio
async def test__async_download_blob_folder_fail(monkeypatch, mock_get_container_client):
    local_folder = anyio.Path("testdata")
    with pytest.raises(Exception) as excinfo:
        await _async_download_blob_folder(
            container_client=mock_get_container_client,
            local_folder=local_folder,
            max_concurrent_downloads=10,
            include_extensions=".txt",
            exclude_extensions=".json",
        )
        assert str(excinfo.value) == (
            "Attempt to create job schedule my-job-schedule on pool my-pool, but could not find the requested pool. "
            "Check that this pool id is correct and that a pool with that id exists"
        )
    monkeypatch.setattr(builtins, "input", lambda _: "N")
    with patch.object(builtins, "print", return_value=True) as mock_print:
        await _async_download_blob_folder(
            container_client=mock_get_container_client,
            local_folder=local_folder,
            max_concurrent_downloads=10,
            include_extensions=".parquet",
        )
        mock_print.assert_called_with("Download aborted.")
