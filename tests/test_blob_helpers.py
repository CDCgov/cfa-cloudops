from unittest.mock import MagicMock, mock_open, patch

import pytest
from shared_fixtures import FAKE_BLOBS, MockLogger

from cfa.cloudops.blob_helpers import (
    download_file,
    download_folder,
    read_blob_stream,
    upload_files_in_folder,
    write_blob_stream,
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
def mock_get_blob_service_client():
    with patch(
        "cfa.cloudops._cloudclient.get_blob_service_client",
        return_value=MagicMock(),
    ) as mock_client:
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = True
        mock_container_client.list_blobs.return_value = iter(FAKE_BLOBS)
        mock_client.get_container_client.return_value = mock_container_client
        yield mock_client


@pytest.fixture
def mock_get_container_client():
    with patch(
        "azure.storage.blob.ContainerClient",
        return_value=MagicMock(),
    ) as mock_client:
        mock_client.list_blobs = MagicMock(return_value=FAKE_BLOBS)
        yield mock_client


def test_upload_files_in_folder(mocker):
    mock_blob_client = MagicMock()
    mock_container_client = MagicMock()
    mock_container_client.exists.return_value = True
    mock_blob_client.get_container_client.return_value = mock_container_client
    mocker.patch("builtins.open", mocker.mock_open(read_data="Some data"))

    with patch("cfa.cloudops.blob_helpers.walk_folder") as mock_os_walk:
        mock_os_walk.return_value = ["file1.txt", "file2.txt", "subfolder/file3.csv"]
        with patch("os.path.isdir", return_value=True):
            files = upload_files_in_folder(
                blob_service_client=mock_blob_client,
                container_name="my-container",
                folder="/folder",
                include_extensions=[".txt"],
            )
            assert type(files) is list

            files = upload_files_in_folder(
                blob_service_client=mock_blob_client,
                container_name="my-container",
                folder="/folder",
                include_extensions=[".txt"],
                force_upload=False,
                legal_hold=True,
            )
            assert type(files) is list

            files = upload_files_in_folder(
                blob_service_client=mock_blob_client,
                container_name="my-container",
                folder="/folder",
                exclude_extensions=[".txt"],
                immutability_lock_days=7,
            )
            assert type(files) is list


def test_upload_files_in_folder_fail():
    mock_blob_client = MagicMock()
    mock_container_client = MagicMock()
    mock_container_client.exists.return_value = False
    mock_blob_client.get_container_client.return_value = mock_container_client

    with (
        patch("os.walk") as mock_os_walk,
        patch("builtins.open", mock_open(read_data="data")),
    ):
        mock_os_walk.return_value = [
            ("./folder", ("subfolder",), ("file1.txt", "file2.txt")),
            ("./folder/subfolder", (), ("file3.txt",)),
        ]

        with pytest.raises(Exception) as excinfo:
            upload_files_in_folder(
                blob_service_client=mock_blob_client,
                container_name="my-container",
                folder="./folder",
                force_upload=False,
                include_extensions=[".txt"],
            )
            assert str(excinfo.value) == (
                "Blob container my-container does not exist. Please try again with an existing Blob container."
            )


def test_download_folder(mocker, mock_get_blob_service_client, mock_logging):
    with patch(
        "cfa.cloudops.blob_helpers.list_blobs_in_container", return_value=FAKE_BLOBS
    ):
        mocker.patch(
            "cfa.cloudops.blob_helpers.check_virtual_directory_existence",
            return_value=True,
        )
        mocker.patch("cfa.cloudops.blob_helpers.download_file", return_value=True)
        download_folder(
            container_name="my-container",
            src_path="my-src-path",
            dest_path="",
            blob_service_client=mock_get_blob_service_client,
            include_extensions=[".txt"],
        )
        assert mock_logging.messages == []
        download_folder(
            container_name="my-container",
            src_path="my-src-path",
            dest_path="",
            blob_service_client=mock_get_blob_service_client,
            exclude_extensions=[".csv"],
            check_size=False,
        )
        assert mock_logging.messages == []


def test_download_folder_large(mocker, mock_get_blob_service_client):
    large_files = [large for large in FAKE_BLOBS if large.size >= 1]
    mocker.patch(
        "cfa.cloudops.blob_helpers.check_virtual_directory_existence", return_value=True
    )
    mocker.patch("cfa.cloudops.blob_helpers.download_file", return_value=True)
    with patch(
        "cfa.cloudops.blob_helpers.list_blobs_in_container", return_value=large_files
    ):
        mock_container_client = MagicMock()
        mock_container_client.exists.return_value = True
        mock_container_client.list_blobs.return_value = iter(large_files)
        mock_get_blob_service_client.get_container_client.return_value = (
            mock_container_client
        )
        mocker.patch("builtins.input", return_value="N")
        result = download_folder(
            container_name="my-container",
            src_path="my-src-path",
            dest_path="",
            blob_service_client=mock_get_blob_service_client,
            include_extensions=".parquet",
            check_size=True,
        )
        assert result is None


def test_download_folder_fail():
    mock_blob_service_client = MagicMock()
    src_path = "my-src-path"
    with pytest.raises(Exception) as excinfo:
        download_folder(
            container_name="my-container",
            src_path=src_path,
            dest_path="my-dest-path",
            blob_service_client=mock_blob_service_client,
            include_extensions=[".txt"],
            exclude_extensions=[".csv"],
        )
        assert str(excinfo.value) == (
            "Use included_extensions or exclude_extensions, not both."
        )

    with patch(
        "cfa.cloudops.blob_helpers.check_virtual_directory_existence",
        return_value=False,
    ):
        with pytest.raises(ValueError) as excinfo:
            download_folder(
                container_name="my-container",
                src_path=src_path,
                dest_path="my-dest-path",
                blob_service_client=mock_blob_service_client,
            )
            assert str(excinfo.value) == (
                f"Source virtual directory: {src_path} does not exist."
            )


def test_download_file(mocker, mock_get_container_client):
    mocker.patch("builtins.input", return_value="N")
    mock_stream = MagicMock()
    mock_stream.readall.return_value = b"Some data"
    large_files = [large for large in FAKE_BLOBS if large.size >= 1]
    mock_get_container_client.list_blobs.return_value = iter(large_files)
    mocker.patch("cfa.cloudops.blob_helpers.read_blob_stream", return_value=mock_stream)
    with patch("builtins.open", return_value=MagicMock()) as mock_file:
        with patch("anyio.Path", return_value=MagicMock()) as mock_path:
            mock_file.write.return_value = None
            mock_path.open.return_value = mock_file
            result = download_file(
                c_client=mock_get_container_client,
                src_path="my-src-path/large_file_1.parquet",
                dest_path="my-dest-path",
            )
            assert result is None
    with patch("builtins.open", return_value=MagicMock()) as mock_file:
        with patch("anyio.Path", return_value=MagicMock()) as mock_path:
            mock_file.write.return_value = None
            mock_path.open.return_value = mock_file
            result = download_file(
                c_client=mock_get_container_client,
                src_path="my-src-path/large_file_2.parquet",
                dest_path="my-dest-path",
            )
            assert result is None


def test_read_blob_stream(mocker, mock_get_container_client):
    downloader = read_blob_stream(
        blob_url="my-blob-url", container_client=mock_get_container_client
    )
    assert downloader is not None
    mocker.patch(
        "cfa.cloudops.blob_helpers.get_container_client",
        return_value=mock_get_container_client,
    )
    downloader = read_blob_stream(
        blob_url="my-blob-url",
        account_name="my-account-name",
        container_name="my-container-name",
    )
    assert downloader is not None


def test_read_blob_stream_fail(mocker):
    blob_url = "my-blob-url"
    with pytest.raises(ValueError) as excinfo:
        read_blob_stream(blob_url=blob_url)
        assert str(excinfo.value) == (
            "Either container name and account name or container client must be provided."
        )
    with pytest.raises(ValueError) as excinfo:
        mocker.patch(
            "cfa.cloudops.blob_helpers.check_blob_existence", return_value=False
        )
        read_blob_stream(blob_url=blob_url, do_check=True)
        assert str(excinfo.value) == (f"Source blob: {blob_url} does not exist.")


def test_write_blob_stream(
    mocker, mock_get_container_client, mock_get_blob_service_client
):
    result = write_blob_stream(
        data=b"some data",
        blob_url="my-blob-url",
        container_client=mock_get_container_client,
    )
    assert result is True
    mocker.patch(
        "cfa.cloudops.blob_helpers.get_blob_service_client",
        return_value=mock_get_blob_service_client,
    )
    result = write_blob_stream(
        data=b"some data",
        blob_url="my-blob-url",
        account_name="my-account-name",
        container_name="my-container-name",
        append_blob=True,
    )
    assert result is True


def test_write_blob_stream_fail(
    mocker, mock_get_container_client, mock_get_blob_service_client
):
    with pytest.raises(ValueError) as excinfo:
        write_blob_stream(
            data=b"some data",
            blob_url="my-blob-url",
        )
        assert str(excinfo.value) == (
            "Either container name and account name or container client must be provided."
        )
