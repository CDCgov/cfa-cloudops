from unittest.mock import MagicMock, mock_open, patch

import pytest
from azure.storage.blob import BlobProperties

from cfa.cloudops.blob_helpers import upload_files_in_folder


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
            )
            assert type(files) is list

            files = upload_files_in_folder(
                blob_service_client=mock_blob_client,
                container_name="my-container",
                folder="/folder",
                exclude_extensions=[".txt"],
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


# def test_download_file(mock_get_container_client):
#    with patch("builtins.open", return_value=MagicMock()) as mock_file:
#        mock_file.write.return_value = None
#        download_file(
#            c_client=mock_get_container_client,
#            src_path="my-src-path",
#            dest_path ="my-dest-path"
#        )
