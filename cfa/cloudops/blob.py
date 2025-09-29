"""
Functions for interacting with Azure Blob Storage.
"""

import logging
import os
from pathlib import Path

import anyio
from azure.batch import models
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from .client import get_blob_service_client
from .util import ensure_listlike

logger = logging.getLogger(__name__)


def format_extensions(extension):
    """
    Formats file extensions to include leading periods.

    Ensures that file extensions have the correct format with leading periods. Accepts both single extensions and lists of extensions, with or without leading periods.

    Args:
        extension (str or list): File extension(s) to format. Can be a single extension string or a list of extension strings. Leading periods are optional (e.g., "txt" or ".txt" both work).

    Returns:
        list: List of properly formatted extensions with leading periods.

    Examples:
        Format a single extension:
            formatted = format_extensions("txt")
            # Returns: [".txt"]

        Format multiple extensions:
            formatted = format_extensions(["py", ".js", "csv"])
            # Returns: [".py", ".js", ".csv"]

        Handle mixed formats:
            formatted = format_extensions([".pdf", "docx"])
            # Returns: [".pdf", ".docx"]
    """
    if isinstance(extension, str):
        extension = [extension]
    ext = []
    for _ext in extension:
        if _ext.startswith("."):
            ext.append(_ext)
        else:
            ext.append("." + _ext)
    return ext


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


async def _async_download_blob_to_file(
    container_client: ContainerClient,
    blob_name: str,
    local_file_path: anyio.Path,
    semaphore: anyio.Semaphore,
):
    """
    Downloads a single blob to a local file asynchronously with streaming.

    Args:
        container_client (ContainerClient): Azure container client providing authentication and transport.
        blob_name (str): Name (path) of the blob within the container.
        local_file_path (anyio.Path): Local filesystem path where the blob will be written. Parent directories are created automatically.
        semaphore (anyio.Semaphore): Semaphore to limit total concurrent downloads.

    Raises:
        Exception: Network/service errors bubble up as SDK exceptions for caller handling.

    Notes:
        - Uses streaming to keep memory bounded to roughly one chunk per download.
        - Designed for use with anyio TaskGroup for higher-level concurrency control.
    """
    # The semaphore helps us limit the total number of concurrent downloads to avoid
    # overwhelming the system with too many simultaneous I/O operations.
    # The total number is limited by the number passed in when the semaphore is created.
    async with semaphore:
        try:
            blob_client = container_client.get_blob_client(blob_name)
            await local_file_path.parent.mkdir(parents=True, exist_ok=True)

            download_stream = await blob_client.download_blob()
            async with await local_file_path.open("wb") as f:
                # Streams in manageable pieces to avoid overwhelming RAM
                async for chunk in download_stream.chunks():
                    await f.write(chunk)
        except Exception as e:
            # Clean up partial file on failure
            if await local_file_path.exists():
                await local_file_path.unlink()
            logger.error(
                f"Failed to download blob {blob_name} to {local_file_path}: {e}"
            )


async def _async_upload_file_to_blob(
    container_client: ContainerClient,
    local_file_path: anyio.Path,
    blob_name: str,
    semaphore: anyio.Semaphore,
):
    """
    Uploads a single file to a blob asynchronously, respecting a concurrency limit.

    Args:
        container_client (ContainerClient): Azure container client for the destination container.
        local_file_path (anyio.Path): Local file path to upload.
        blob_name (str): Name of the blob in the container.
        semaphore (anyio.Semaphore): Semaphore to limit concurrent uploads.

    Raises:
        Exception: Logs errors if upload fails.

    Notes:
        - Uses a semaphore to control concurrency.
    """
    async with semaphore:
        try:
            blob_client = container_client.get_blob_client(blob_name)
            async with await local_file_path.open("rb") as f:
                await blob_client.upload_blob(f, overwrite=True)
        except Exception as e:
            logger.error(
                f"Failed to upload file {local_file_path} to blob {blob_name}: {e}"
            )


async def _async_download_blob_folder(
    container_client: ContainerClient,
    local_folder: anyio.Path,
    max_concurrent_downloads: int,
    name_starts_with: str | None = None,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    check_size: bool = True,
):
    """
    Downloads all matching blobs from a container to a local folder asynchronously.

    Args:
        container_client (ContainerClient): Azure container client for the source container.
        local_folder (anyio.Path): Local directory path where blobs will be downloaded.
        max_concurrent_downloads (int): Maximum number of simultaneous downloads allowed.
        name_starts_with (str, optional): Filter blobs to only those with names starting with this prefix.
        include_extensions (str | list, optional): File extensions to include (e.g., ".txt", [".json", ".csv"]).
        exclude_extensions (str | list, optional): File extensions to exclude (e.g., ".log", [".tmp", ".bak"]).
        check_size (bool, optional): If True, prompts user if total download size exceeds 2 GB. Defaults to True.

    Raises:
        Exception: If both include_extensions and exclude_extensions are provided.

    Notes:
        - include_extensions takes precedence over exclude_extensions if both are provided.
        - Uses anyio TaskGroup to manage concurrent downloads.
        - Blob folder structure is preserved in the local directory.
        - Blobs not matching the pattern are skipped with logged messages.
    """
    semaphore = anyio.Semaphore(max_concurrent_downloads)
    if include_extensions is not None:
        include_extensions = format_extensions(include_extensions)
    else:
        exclude_extensions = format_extensions(exclude_extensions)
    if include_extensions is not None and exclude_extensions is not None:
        logger.error(
            "Use included_extensions or exclude_extensions, not both."
        )
        raise Exception(
            "Use included_extensions or exclude_extensions, not both."
        ) from None

    # Gather all matching blobs and calculate total size
    matching_blobs = []
    total_size = 0
    gb = 1e9
    async for blob_obj in container_client.list_blobs(
        name_starts_with=name_starts_with
    ):
        blob_name = blob_obj.name
        ext = Path(blob_name).suffix
        if include_extensions is not None:
            if ext not in include_extensions:
                continue
        if exclude_extensions is not None:
            if ext in exclude_extensions:
                continue
        matching_blobs.append(blob_name)
        total_size += getattr(blob_obj, "size", 0)

    print(f"Total size of files to download: {total_size / gb:.2f} GB")
    if total_size > 2 * gb and check_size:
        print("Warning: Total size of files to download is greater than 2 GB.")
        cont = input("Continue? [Y/n]: ")
        if cont.lower() != "y":
            print("Download aborted.")
            return

    # A TaskGroup ensures that all spawned tasks are awaited before the block is exited.
    async with anyio.create_task_group() as tg:
        logger.info(
            f"Scheduling downloads for {len(matching_blobs)} blobs in container '{container_client.url}'..."
        )
        async for blob in matching_blobs:
            local_file_path = local_folder / blob
            tg.start_soon(
                _async_download_blob_to_file,
                container_client,
                blob,
                local_file_path,
                semaphore,
            )
    logger.info("All download tasks have been scheduled.")


async def _async_upload_blob_folder(
    container_client: ContainerClient,
    folder: anyio.Path,
    max_concurrent_uploads: int,
    location_in_blob: str = ".",
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
):
    """
    Uploads all matching files from a local folder to a blob container asynchronously.

    Args:
        container_client (ContainerClient): Azure container client for the destination container.
        folder (anyio.Path): Local directory path whose files will be uploaded.
        max_concurrent_uploads (int): Maximum number of simultaneous uploads allowed.
        location_in_blob (str, optional): Path within the blob container where files will be uploaded. Defaults to "." (container root).
        include_extensions (str | list, optional): File extensions to include (e.g., ".txt", [".json", ".csv"]).
        exclude_extensions (str | list, optional): File extensions to exclude (e.g., ".log", [".tmp", ".bak"]).

    Raises:
        Exception: If both include_extensions and exclude_extensions are provided.

    Notes:
        - Uses anyio TaskGroup to manage concurrent uploads.
        - Folder structure is preserved in the blob container.
        - Files not matching the pattern are skipped with logged messages.
    """
    semaphore = anyio.Semaphore(max_concurrent_uploads)
    if include_extensions is not None:
        include_extensions = format_extensions(include_extensions)
    else:
        exclude_extensions = format_extensions(exclude_extensions)
    if include_extensions is not None and exclude_extensions is not None:
        logger.error("Use include_extensions or exclude_extensions, not both.")
        raise Exception(
            "Use include_extensions or exclude_extensions, not both."
        ) from None

    # Check if folder exists and is a directory
    if not await folder.exists():
        logger.error(f"Upload folder does not exist: {folder}")
        raise FileNotFoundError(f"Upload folder does not exist: {folder}")
    if not await folder.is_dir():
        logger.error(f"Upload path is not a directory: {folder}")
        raise NotADirectoryError(f"Upload path is not a directory: {folder}")

    async def walk_files(base: anyio.Path):
        # Recursively yield all files under base as (relative_path, absolute_path)
        try:
            entries = []
            async for entry in base.iterdir():
                entries.append(entry)
            logger.debug(f"Found {len(entries)} entries in directory: {base}")
            for entry in entries:
                if await entry.is_dir():
                    async for sub in walk_files(entry):
                        yield sub
                elif await entry.is_file():
                    rel_path = entry.relative_to(folder)
                    yield str(rel_path), entry
        except Exception as e:
            logger.error(f"Error iterating directory {base}: {e}")
            raise

    found_files = False
    async with anyio.create_task_group() as tg:
        logger.info(f"Searching for files in local folder '{folder}'...")
        try:
            async for rel_path, abs_path in walk_files(folder):
                found_files = True
                ext = Path(rel_path).suffix
                if include_extensions is not None:
                    if ext not in include_extensions:
                        continue
                if exclude_extensions is not None:
                    if ext in exclude_extensions:
                        continue

                # Schedule the upload function to run in the background.
                tg.start_soon(
                    _async_upload_file_to_blob,
                    container_client,
                    abs_path,
                    os.path.join(location_in_blob, str(rel_path)),
                    semaphore,
                )
        except Exception as e:
            logger.error(f"Error walking files in folder {folder}: {e}")
            raise

    if not found_files:
        logger.warning(f"No files found to upload in folder: {folder}")
    else:
        logger.info("All upload tasks have been scheduled.")


def async_download_blob_folder(
    container_name: str,
    local_folder: Path,
    storage_account_url: str,
    name_starts_with: str | None = None,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    check_size: bool = True,
    max_concurrent_downloads: int = 20,
    credential: any = None,
) -> None:
    """
    Downloads blobs from an Azure container to a local folder asynchronously.

    This is the main entry point for downloading blobs. It sets up Azure credentials, creates the necessary clients, and runs the async download process.

    Args:
        container_name (str): Name of the Azure Storage container to download from.
        local_folder (Path): Local directory path where blobs will be downloaded.
        storage_account_url (str): URL of the Azure Storage account (e.g., "https://<account_name>.blob.core.windows.net").
        name_starts_with (str, optional): Filter blobs to only those with names starting with this prefix.
        include_extensions (str or list, optional): File extensions to include (e.g., ".txt", [".json", ".csv"]).
        exclude_extensions (str or list, optional): File extensions to exclude (e.g., ".log", [".tmp", ".bak"]).
        check_size (bool, optional): If True, prompts user if total download size exceeds 2 GB. Defaults to True.
        max_concurrent_downloads (int, optional): Maximum number of simultaneous downloads allowed. Defaults to 20.
        credential (any, optional): Azure credential object. If None, ManagedIdentityCredential is used.

    Raises:
        KeyboardInterrupt: If the user cancels the download operation.
        Exception: For any Azure SDK or network-related errors during download.

    Notes:
        Uses ManagedIdentityCredential for authentication.
        Preserves blob folder structure in the local directory.
        Handles cleanup of Azure credentials automatically.
    """

    async def _runner(credential) -> None:
        if credential is None:
            credential = ManagedIdentityCredential()
        try:
            with BlobServiceClient(
                account_url=storage_account_url,
                credential=credential,
            ) as blob_service_client:
                container_client = blob_service_client.get_container_client(
                    container_name
                )
                await _async_download_blob_folder(
                    container_client=container_client,
                    local_folder=anyio.Path(local_folder),
                    name_starts_with=name_starts_with,
                    include_extensions=include_extensions,
                    exclude_extensions=exclude_extensions,
                    max_concurrent_downloads=max_concurrent_downloads,
                    check_size=check_size,
                )
        except Exception as e:
            logger.error(f"Error during download: {e}")
            raise

    try:
        anyio.run(_runner, credential)

    except KeyboardInterrupt:
        logger.error("Download cancelled by user.")
    except Exception as e:
        logger.error(f"Failed to download blob folder: {e}")


def async_upload_folder(
    folder: str,
    container_name: str,
    storage_account_url: str,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    location_in_blob: str = ".",
    max_concurrent_uploads: int = 20,
    credential: any = None,
) -> None:
    """
    Upload all files from a local folder to an Azure blob container asynchronously.

    This is the main entry point for uploading files. It sets up Azure credentials,
    creates the necessary clients, and runs the async upload process.

    Args:
        folder (str): Local directory path whose files will be uploaded.
        container_name (str): Name of the Azure Storage container to upload to.
        storage_account_url (str): URL of the Azure Storage account (e.g., "https://<account_name>.blob.core.windows.net").
        include_extensions (str or list, optional): File extensions to include (e.g., ".txt", [".json", ".csv"]).
        exclude_extensions (str or list, optional): File extensions to exclude (e.g., ".log", [".tmp", ".bak"]).
        location_in_blob (str, optional): Path within the blob container where files will be uploaded. Defaults to "." (root of the container).
        max_concurrent_uploads (int, optional): Maximum number of simultaneous uploads allowed. Defaults to 20.
        credential (any, optional): Azure credential object. If None, ManagedIdentityCredential is used.

    Raises:
        KeyboardInterrupt: If the user cancels the upload operation.
        Exception: For any Azure SDK or network-related errors during upload.

    Notes:
        - Uses ManagedIdentityCredential for authentication.
        - Preserves folder structure in the blob container.
        - Handles cleanup of Azure credentials automatically.
    """

    async def _runner(credential) -> None:
        if credential is None:
            credential = ManagedIdentityCredential()
        try:
            logger.debug(f"Resolved upload folder path: {folder}")
            logger.debug(f"Target container name: {container_name}")
            blob_service_client = BlobServiceClient(
                account_url=storage_account_url,
                credential=credential,
            )
            if blob_service_client is None:
                logger.error(
                    "Failed to create BlobServiceClient. Check your storage_account_url and credentials."
                )
                raise RuntimeError("Failed to create BlobServiceClient.")
            container_client = blob_service_client.get_container_client(
                container_name
            )
            if container_client is None:
                logger.error(
                    f"Failed to get container client for container: {container_name}"
                )
                raise RuntimeError(
                    f"Failed to get container client for container: {container_name}"
                )
            await _async_upload_blob_folder(
                container_client=container_client,
                folder=anyio.Path(folder),
                location_in_blob=location_in_blob,
                include_extensions=include_extensions,
                exclude_extensions=exclude_extensions,
                max_concurrent_uploads=max_concurrent_uploads,
            )
        except Exception as e:
            logger.error(f"Error during upload: {e}")
            raise

    try:
        anyio.run(_runner, credential)
    except KeyboardInterrupt:
        logger.error("Upload cancelled by user.")
    except Exception as e:
        logger.error(f"Failed to upload blob folder: {e}")
