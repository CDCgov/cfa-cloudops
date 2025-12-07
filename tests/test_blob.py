import builtins
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest
from azure.batch import models
from shared_fixtures import FAKE_BLOBS, MockLogger

from cfa.cloudops.blob import (
    _async_download_blob_folder,
    _async_upload_blob_folder,
    _async_upload_file_to_blob,
    async_download_blob_folder,
    async_upload_folder,
    create_storage_container_if_not_exists,
    download_from_storage_container,
    get_node_mount_config,
    toggle_legal_hold_on_files,
    upload_to_storage_container,
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
def mock_container_client():
    with patch(
        "azure.storage.blob.ContainerClient",
        return_value=MagicMock(),
    ) as mock_client:
        mock_client.list_blobs = MagicMock(return_value=FAKE_BLOBS)
        yield mock_client


@pytest.fixture
def mock_async_container_client():
    with patch(
        "azure.storage.blob.aio.ContainerClient",
        return_value=MagicMock(),
    ) as mock_client:

        def make_async_iter(items):
            async def gen():
                for item in items:
                    yield item

            return gen()

        mock_client.list_blobs = lambda *args, **kwargs: make_async_iter(FAKE_BLOBS)
        yield mock_client


@pytest.fixture
def mock_compute_node():
    return models.ComputeNodeIdentityReference(resource_id="mock-resource-id")


@pytest.fixture
def mock_blob_service_client():
    with patch(
        "cfa.cloudops._cloudclient.get_blob_service_client",
        return_value=MagicMock(),
    ) as mock_client:
        yield mock_client


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
    monkeypatch, mock_async_container_client, mock_logging
):
    local_folder = anyio.Path("testdata")
    monkeypatch.setattr(builtins, "input", lambda _: "Y")
    with patch.object(builtins, "print", return_value=True) as mock_print:
        await _async_download_blob_folder(
            container_client=mock_async_container_client,
            local_folder=local_folder,
            max_concurrent_downloads=10,
        )
        mock_print.assert_called_with(
            "Warning: Total size of files to download is greater than 2 GB."
        )
    await _async_download_blob_folder(
        container_client=mock_async_container_client,
        local_folder=local_folder,
        max_concurrent_downloads=10,
        name_starts_with="my_test",
        include_extensions=".txt",
    )
    await _async_download_blob_folder(
        container_client=mock_async_container_client,
        local_folder=local_folder,
        max_concurrent_downloads=10,
        exclude_extensions=".parquet",
    )
    assert mock_logging.messages == []


@pytest.mark.asyncio
async def test__async_download_blob_folder_fail(
    monkeypatch, mock_async_container_client
):
    local_folder = anyio.Path("testdata")
    monkeypatch.setattr(builtins, "input", lambda _: "Y")
    with pytest.raises(Exception) as excinfo:
        await _async_download_blob_folder(
            container_client=mock_async_container_client,
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
            container_client=mock_async_container_client,
            local_folder=local_folder,
            max_concurrent_downloads=10,
            include_extensions=".parquet",
        )
        mock_print.assert_called_with("Download aborted.")


@pytest.mark.asyncio
async def test__async_upload_blob_folder_success(
    mock_async_container_client, mock_logging
):
    local_folder = anyio.Path("testdata")
    await _async_upload_blob_folder(
        container_client=mock_async_container_client,
        folder=local_folder,
        max_concurrent_uploads=10,
        include_extensions=".txt",
    )
    await _async_upload_blob_folder(
        container_client=mock_async_container_client,
        folder=local_folder,
        max_concurrent_uploads=10,
        exclude_extensions=".parquet",
        tags={"env": "test__async_upload_blob_folder_success", "owner": "xop5"},
    )
    assert mock_logging.messages == []


@pytest.mark.asyncio
async def test__async_upload_blob_folder_fail(mock_container_client):
    with patch("anyio.Path", return_value=MagicMock()) as mock_path:
        mock_path.exists = MagicMock(return_value=True)
        mock_path.isdir = False
        with pytest.raises(Exception) as excinfo:
            await _async_upload_blob_folder(
                container_client=mock_container_client,
                folder=mock_path,
                max_concurrent_uploads=10,
                include_extensions=".txt",
                exclude_extensions=".json",
                tags={"env": "test__async_upload_blob_folder_fail", "owner": "xop5"},
            )
            assert str(excinfo.value) == (
                "Use include_extensions or exclude_extensions, not both."
            )


def test_async_upload_blob_folder():
    result = async_upload_folder(
        folder="testdata",
        container_name="my-container",
        storage_account_url="my-storage-account-url",
        tags={"env": "test_async_upload_blob_folder", "owner": "xop5"},
    )
    assert result == "testdata"


def test_async_download_blob_folder():
    result = async_download_blob_folder(
        container_name="my-container",
        local_folder="testdata",
        storage_account_url="my-storage-account-url",
    )
    assert result == "testdata"


def test_upload_to_storage_container(mocker, mock_blob_service_client):
    mocker.patch("builtins.open", mocker.mock_open(read_data="Some data"))
    with patch.object(builtins, "print", return_value=True) as mock_print:
        upload_to_storage_container(
            file_paths="/testdata/myfile.txt",
            blob_storage_container_name="my-blob-storage-container",
            blob_service_client=mock_blob_service_client,
            tags={"env": "test_upload_to_storage_container", "owner": "xop5"},
        )
        mock_print.assert_called_with("Uploaded 1 files to blob storage container")


def test_download_from_storage_container(mocker, mock_blob_service_client):
    mocker.patch("builtins.open", mocker.mock_open(read_data="Some data"))
    with patch.object(builtins, "print", return_value=True) as mock_print:
        download_from_storage_container(
            file_paths="/testdata/myfile.txt",
            blob_storage_container_name="my-blob-storage-container",
            blob_service_client=mock_blob_service_client,
        )
        mock_print.assert_called_with("Downloaded 1 files from blob storage container")


def test_create_storage_container_if_not_exists(mock_blob_service_client):
    mock_container_client = MagicMock()
    mock_container_client.exists.return_value = False
    mock_container_client.create_container = MagicMock()
    mock_blob_service_client.get_container_client.return_value = mock_container_client
    with patch.object(builtins, "print", return_value=True) as mock_print:
        create_storage_container_if_not_exists(
            blob_storage_container_name="my-container",
            blob_service_client=mock_blob_service_client,
        )
        mock_container_client.create_container.assert_called_once()
        mock_print.assert_called_with("Container [my-container] created.")
    mock_container_client.exists.return_value = True
    with patch.object(builtins, "print", return_value=True) as mock_print:
        create_storage_container_if_not_exists(
            blob_storage_container_name="my-container",
            blob_service_client=mock_blob_service_client,
        )
        mock_container_client.create_container.assert_called_once()
        mock_print.assert_called_with("Container [my-container] already exists.")


def test_toggle_legal_hold_on_files(mock_blob_service_client, mock_logging):
    mock_blob_client = MagicMock()
    mock_blob_client.set_legal_hold.return_value = True
    mock_blob_service_client.get_blob_client.return_value = mock_blob_client
    toggle_legal_hold_on_files(
        file_paths=["myfile.txt"],
        blob_storage_container_name="my-container",
        blob_service_client=mock_blob_service_client,
        legal_hold=True,
    )
    assert mock_logging.messages == []


@pytest.mark.asyncio
async def test__async_upload_file_to_blob_success(tmp_path, mocker):
    file_path = tmp_path / "testfile.txt"
    file_path.write_text("hello world")
    anyio_file_path = anyio.Path(str(file_path))
    semaphore = anyio.Semaphore(1)
    mock_blob_client = AsyncMock()
    mock_blob_client.upload_blob = AsyncMock()
    mock_container_client = MagicMock()
    mock_container_client.get_blob_client = MagicMock(return_value=mock_blob_client)

    class AsyncFileCtx:
        async def __aenter__(self):
            return MagicMock()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_open(mode):
        return AsyncFileCtx()

    mocker.patch.object(anyio.Path, "open", side_effect=fake_open)
    result = await _async_upload_file_to_blob(
        container_client=mock_container_client,
        local_file_path=anyio_file_path,
        blob_name="folder/testfile.txt",
        semaphore=semaphore,
        legal_hold=True,
        immutability_lock_days=7,
        read_only=True,
    )
    mock_container_client.get_blob_client.assert_called_once_with("folder/testfile.txt")
    mock_blob_client.upload_blob.assert_awaited_once()
    _, kwargs = mock_blob_client.upload_blob.call_args
    assert kwargs["overwrite"] is True
    assert result == anyio_file_path
