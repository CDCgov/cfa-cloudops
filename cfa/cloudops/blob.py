"""
Functions for interacting with Azure Blob Storage.
"""

import logging
import os

from azure.batch import models
from azure.storage.blob import BlobServiceClient

from .client import get_blob_service_client
from .util import ensure_listlike

logger = logging.getLogger(__name__)


def create_storage_container_if_not_exists(
    blob_storage_container_name: str, blob_service_client: BlobServiceClient
) -> None:
    """Create an Azure blob storage container if it does not already exist.

    Args:
        blob_storage_container_name: Name of the storage container.
        blob_service_client: The blob service client to use when looking
            for and potentially creating the storage container.

    Example:
        >>> from azure.storage.blob import BlobServiceClient
        >>> client = BlobServiceClient(account_url="...", credential="...")
        >>> create_storage_container_if_not_exists("my-container", client)
        Container [my-container] created.
    """
    container_client = blob_service_client.get_container_client(
        container=blob_storage_container_name
    )
    if not container_client.exists():
        container_client.create_container()
        print("Container [{}] created.".format(blob_storage_container_name))
    else:
        print(
            "Container [{}] already exists.".format(
                blob_storage_container_name
            )
        )


def upload_to_storage_container(
    file_paths: str | list[str],
    blob_storage_container_name: str,
    blob_service_client: BlobServiceClient,
    local_root_dir: str = ".",
    remote_root_dir: str = ".",
) -> None:
    """Upload a file or list of files to an Azure blob storage container.

    This function preserves relative directory structure among the
    uploaded files within the storage container.

    Args:
        file_paths: File or list of files to upload, as string paths relative to
            ``local_root_dir``. A single string will be coerced to a length-one list.
        blob_storage_container_name: Name of the blob storage container to which
            to upload the files. Must already exist.
        blob_service_client: BlobServiceClient to use when uploading.
        local_root_dir: Root directory for the relative file paths in local storage.
            Defaults to "." (use the local working directory).
        remote_root_dir: Root directory for the relative file paths within the blob
            storage container. Defaults to "." (start at the blob storage container root).

    Raises:
        Exception: If the blob storage container does not exist.

    Example:
        >>> from azure.storage.blob import BlobServiceClient
        >>> client = BlobServiceClient(account_url="...", credential="...")
        >>> upload_to_storage_container(
        ...     ["file1.txt", "subdir/file2.txt"],
        ...     "my-container",
        ...     client,
        ...     local_root_dir="/local/path",
        ...     remote_root_dir="uploads"
        ... )
        Uploading file 0 of 2
        Uploaded 2 files to blob storage container
    """

    file_paths = ensure_listlike(file_paths)

    n_total_files = len(file_paths)

    for i_file, file_path in enumerate(file_paths):
        if i_file % (1 + int(n_total_files / 10)) == 0:
            print("Uploading file {} of {}".format(i_file, n_total_files))

        local_file_path = os.path.join(local_root_dir, file_path)
        remote_file_path = os.path.join(remote_root_dir, file_path)

        blob_client = blob_service_client.get_blob_client(
            container=blob_storage_container_name, blob=remote_file_path
        )
        with open(local_file_path, "rb") as upload_data:
            blob_client.upload_blob(upload_data, overwrite=True)

    print("Uploaded {} files to blob storage container".format(n_total_files))


def download_from_storage_container(
    file_paths: str | list[str],
    blob_storage_container_name: str,
    blob_service_client: BlobServiceClient = None,
    local_root_dir: str = ".",
    remote_root_dir: str = ".",
    **kwargs,
) -> None:
    """Download a list of files from an Azure blob storage container.

    Preserves relative directory structure.

    Args:
        file_paths: File or list of files to download, as string paths relative to
            ``remote_root_dir``. A single string will be coerced to a length-one list.
        blob_storage_container_name: Name of the blob storage container from which
            to download the files. Must already exist.
        blob_service_client: BlobServiceClient to use when downloading.
            If None, attempt to create one via ``client.get_blob_service_client``
            using provided ``**kwargs``, if any.
        local_root_dir: Root directory for the relative file paths in local storage.
            Defaults to "." (use the local working directory).
        remote_root_dir: Root directory for the relative file paths within the blob
            storage container. Defaults to "." (start at the blob storage container root).
        **kwargs: Keyword arguments passed to ``client.get_blob_service_client``.

    Raises:
        Exception: If the blob storage container does not exist.

    Example:
        >>> from azure.storage.blob import BlobServiceClient
        >>> client = BlobServiceClient(account_url="...", credential="...")
        >>> download_from_storage_container(
        ...     ["file1.txt", "subdir/file2.txt"],
        ...     "my-container",
        ...     client,
        ...     local_root_dir="/local/path",
        ...     remote_root_dir="uploads"
        ... )
        Downloading file 0 of 2
        Downloaded 2 files from blob storage container
    """

    file_paths = ensure_listlike(file_paths)
    n_total_files = len(file_paths)

    if blob_service_client is None:
        blob_service_client = get_blob_service_client(**kwargs)

    for i_file, file_path in enumerate(file_paths):
        if i_file % (1 + int(n_total_files / 10)) == 0:
            print(f"Downloading file {i_file} of {n_total_files}")

        local_file_path = os.path.join(local_root_dir, file_path)
        remote_file_path = os.path.join(remote_root_dir, file_path)

        blob_client = blob_service_client.get_blob_client(
            container=blob_storage_container_name, blob=remote_file_path
        )
        with open(local_file_path, "wb") as target_file:
            download_stream = blob_client.download_blob()
            target_file.write(download_stream.readall())

    print(f"Downloaded {n_total_files} files from blob storage container")


def get_node_mount_config(
    storage_containers: str | list[str],
    account_names: str | list[str],
    identity_references: (
        models.ComputeNodeIdentityReference
        | list[models.ComputeNodeIdentityReference]
    ),
    shared_relative_mount_path: str = "",
    mount_names: list[str] = None,
    blobfuse_options: str | list[str] = "",
    cache_blobfuse: bool = False,
    **kwargs,
) -> list[models.MountConfiguration]:
    """Get configuration for mounting Azure Blob Storage containers to Azure Batch nodes via blobfuse.

    Args:
        storage_containers: Name(s) of the Azure Blob storage container(s) to mount.
        account_names: Name(s) of the Azure Blob storage account(s) in which to look
            for the storage container(s). If a single value, look for all storage
            containers within the same storage account. Otherwise, look for each
            container within the corresponding account. The function will raise an
            error if there is more than one ``account_names`` value but a different
            number of ``storage_containers``, as then the mapping is ambiguous.
        identity_references: Valid ComputeNodeIdentityReference objects for the node
            to use when connecting to the ``storage_containers``, or an iterable of
            such objects with one object for each of the ``storage_containers``.
        shared_relative_mount_path: Path relative to the ``fsmounts`` directory within
            the running node at which to mount the storage containers. Defaults to ""
            (mount within ``fsmounts`` itself).
        mount_names: Iterable of names (or paths) for the individual mounted storage
            containers relative to the ``shared_relative_mount_path``. If None, use
            the storage container names given in ``storage_containers`` as the
            ``mount_names``.
        blobfuse_options: Additional options passed to blobfuse. Defaults to "".
        cache_blobfuse: Whether to cache Blob storage. Defaults to False.
        **kwargs: Additional keyword arguments passed to the
            ``models.AzureBlobFileSystemConfiguration`` constructor.

    Returns:
        list[models.MountConfiguration]: A list of instantiated MountConfiguration
            objects describing the desired storage container mounts.

    Raises:
        ValueError: If the number of mount_names doesn't match storage_containers,
            or if the number of account_names or identity_references doesn't match
            storage_containers and isn't exactly 1.

    Example:
        >>> from azure.batch import models
        >>> identity_ref = models.ComputeNodeIdentityReference(
        ...     resource_id="/subscriptions/.../resourceGroups/.../providers/..."
        ... )
        >>> mount_configs = get_node_mount_config(
        ...     storage_containers=["container1", "container2"],
        ...     account_names="mystorageaccount",
        ...     identity_references=identity_ref,
        ...     shared_relative_mount_path="data",
        ...     cache_blobfuse=True
        ... )
        >>> len(mount_configs)
        2
    """
    storage_containers = ensure_listlike(storage_containers)
    account_names = ensure_listlike(account_names)
    identity_references = ensure_listlike(identity_references)

    n_containers = len(storage_containers)
    n_accounts = len(account_names)
    n_identity_refs = len(identity_references)

    if mount_names is None:
        mount_names = storage_containers
    else:
        mount_names = ensure_listlike(mount_names)
    n_mount_names = len(mount_names)

    if n_mount_names != n_containers:
        raise ValueError(
            "Must provide exactly as many "
            "`mount_names` as `storage_containers` "
            "to mount, or set `mount_names=None`, "
            "in which case the storage container "
            "names in `storage_containers` will "
            "be used as the names for mounting "
            "the containers. Got "
            f"{n_mount_names} `mount_names` and "
            f"{n_containers} `storage_containers`."
        )

    if n_containers != n_accounts:
        if n_accounts == 1:
            account_names *= n_containers
        else:
            raise ValueError(
                "Must either provide a single `account_names`"
                "value (as a string or a length-1 list) "
                "to cover all `storage_containers` values "
                "or provide one `account_names` value for "
                "each `storage_containers` value. Got "
                f"{n_accounts} `account_names` and "
                f"{n_containers} `storage_containers`."
            )

    if n_containers != n_identity_refs:
        if n_identity_refs == 1:
            identity_references *= n_containers
        else:
            raise ValueError(
                "Must either provide a single `identity_references`"
                "value (as a single ComputeNodeIdentityReference "
                "object or a length-1 list containing a "
                "ComputeNodeIdentityReference object) "
                "to cover all `storage_containers` values "
                "or provide one `identity_references` value for "
                "each `storage_containers` value. Got "
                f"{n_identity_refs} `identity_references` and "
                f"{n_containers} `storage_containers`."
            )

    relative_mount_paths = [
        os.path.join(shared_relative_mount_path, mount_name)
        for mount_name in mount_names
    ]
    if cache_blobfuse:
        blob_str = ""
    else:
        blob_str = " -o direct_io"

    return [
        models.MountConfiguration(
            azure_blob_file_system_configuration=(
                models.AzureBlobFileSystemConfiguration(
                    account_name=account_name,
                    container_name=container_name,
                    relative_mount_path=relative_mount_path,
                    blobfuse_options=blobfuse_options + blob_str,
                    identity_reference=identity_reference,
                    **kwargs,
                )
            )
        )
        for (
            account_name,
            container_name,
            relative_mount_path,
            identity_reference,
        ) in zip(
            account_names,
            storage_containers,
            relative_mount_paths,
            identity_references,
        )
    ]
