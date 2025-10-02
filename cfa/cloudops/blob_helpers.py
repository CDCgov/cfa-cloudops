import logging
import os
from os import path, walk
from pathlib import Path

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob import (
    BlobServiceClient,
    BlobType,
    ContainerClient,
    StorageStreamDownloader,
)
from humanize import naturalsize as ns

from .blob import upload_to_storage_container

logger = logging.getLogger(__name__)


def format_extensions(extension):
    """Format file extensions to include leading periods.

    Ensures that file extensions have the correct format with leading periods.
    Accepts both single extensions and lists of extensions, with or without
    leading periods.

    Args:
        extension (str | list): File extension(s) to format. Can be a single
            extension string or a list of extension strings. Leading periods
            are optional (e.g., "txt" or ".txt" both work).

    Returns:
        list: List of properly formatted extensions with leading periods.

    Example:
        Format single extension:

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


def walk_folder(folder: str) -> list | None:
    """Recursively walk through a folder and collect all file paths.

    Traverses the directory tree starting from the specified folder and collects
    the full paths of all files found. Subdirectories are explored recursively.

    Args:
        folder (str): Path to the folder to walk through. Can be relative or absolute.

    Returns:
        list | None: List of full file paths found in the folder and its subdirectories.
            Returns empty list if no files are found.

    Example:
        Get all files in a project directory:

            files = walk_folder("./src")
            print(f"Found {len(files)} files")
            for file_path in files:
                print(file_path)

        Process all Python files:

            all_files = walk_folder("/path/to/project")
            python_files = [f for f in all_files if f.endswith('.py')]
    """
    file_list = []
    for dirname, _, fname in walk(folder):
        for f in fname:
            _path = path.join(dirname, f)
            file_list.append(_path)
    return file_list


def upload_files_in_folder(
    folder: str,
    container_name: str,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    exclude_patterns: str | list | None = None,
    location_in_blob: str = ".",
    blob_service_client=None,
    force_upload: bool = True,
) -> list[str]:
    """Upload all files from a local folder to Azure Blob Storage with filtering options.

    Recursively uploads files from a local folder to a blob storage container while
    preserving directory structure. Supports filtering by file extensions and patterns
    to control which files are uploaded.

    Args:
        folder (str): Path to the local folder to upload. Must be a valid directory.
        container_name (str): Name of the blob storage container to upload to. The
            container must already exist.
        include_extensions (str | list, optional): File extensions to include in the
            upload. Can be a single extension string or list of extensions. Cannot be
            used together with exclude_extensions.
        exclude_extensions (str | list, optional): File extensions to exclude from
            the upload. Can be a single extension string or list. Cannot be used
            together with include_extensions.
        exclude_patterns (str | list, optional): Filename patterns to exclude using
            substring matching. Files containing any of these patterns will be skipped.
        location_in_blob (str, optional): Remote directory path within the blob container
            where files should be uploaded. Default is "." (container root).
        blob_service_client: Azure Blob service client instance for API calls.
        force_upload (bool, optional): Whether to force upload without user confirmation
            for large numbers of files (>50). Default is True.

    Returns:
        list[str]: List of local file paths that were processed for upload.

    Raises:
        Exception: If both include_extensions and exclude_extensions are specified,
            or if the container does not exist.

    Example:
        Upload Python files only:

            uploaded = upload_files_in_folder(
                folder="./src",
                container_name="code-repo",
                include_extensions=[".py", ".yaml"],
                blob_service_client=client
            )

        Upload all files except temporary ones:

            uploaded = upload_files_in_folder(
                folder="./data",
                container_name="datasets",
                exclude_extensions=[".tmp", ".log"],
                exclude_patterns=["__pycache__", ".git"],
                location_in_blob="project-data",
                blob_service_client=client
            )

    Note:
        - Container must exist before uploading
        - Directory structure is preserved in the blob container
        - Large uploads (>50 files) prompt for confirmation unless force_upload=True
        - Extensions are automatically formatted with leading periods
    """
    # check that include and exclude extensions are not both used, format if exist
    if include_extensions is not None:
        include_extensions = format_extensions(include_extensions)
    elif exclude_extensions is not None:
        exclude_extensions = format_extensions(exclude_extensions)
    if include_extensions is not None and exclude_extensions is not None:
        logger.error(
            "Use included_extensions or exclude_extensions, not both."
        )
        raise Exception(
            "Use included_extensions or exclude_extensions, not both."
        ) from None
    if exclude_patterns is not None:
        exclude_patterns = (
            [exclude_patterns]
            if not isinstance(exclude_patterns, list)
            else exclude_patterns
        )
    # check container exists
    logger.debug(f"Checking Blob container {container_name} exists.")
    # create container client
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    # check if container client exists
    if not container_client.exists():
        logger.error(
            f"Blob container {container_name} does not exist. Please try again with an existing Blob container."
        )
        raise Exception(
            f"Blob container {container_name} does not exist. Please try again with an existing Blob container."
        ) from None
    # check number of files if force_upload False
    logger.debug(f"Blob container {container_name} found. Uploading files...")
    # check if files should be force uploaded
    if not force_upload:
        fnum = []
        for _, _, file in os.walk(os.path.realpath(f"./{folder}")):
            fnum.append(len(file))
        fnum_sum = sum(fnum)
        if fnum_sum > 50:
            print(f"You are about to upload {fnum_sum} files.")
            resp = input("Continue? [Y/n]: ")
            if resp == "Y" or resp == "y":
                pass
            else:
                print("Upload aborted.")
                return None
    # get all files in folder
    file_list = []
    # check if folder is valid
    if not path.isdir(folder):
        logger.warning(
            f"{folder} is not a folder/directory. Make sure to specify a valid folder."
        )
        return None
    file_list = walk_folder(folder)
    # create sublist matching include_extensions and exclude_extensions
    flist = []
    if include_extensions is None:
        if exclude_extensions is not None:
            # find files that don't contain the specified extensions
            for _file in file_list:
                if os.path.splitext(_file)[1] not in exclude_extensions:
                    flist.append(_file)
        else:  # this is for no specified extensions to include of exclude
            flist = file_list
    else:
        # include only specified extension files
        for _file in file_list:
            if os.path.splitext(_file)[1] in include_extensions:
                flist.append(_file)
    # iteratively call the upload_blob_file function to upload individual files
    final_list = []
    for file in flist:
        # if a pattern from exclude_patterns is found, skip uploading this file
        if exclude_patterns is not None and any(
            pattern in file for pattern in exclude_patterns
        ):
            # dont upload this file if an excluded pattern is found within
            continue
        else:
            final_list.append(file)
        logger.debug(f"Calling upload_blob_file for {file}")
    upload_to_storage_container(
        file_paths=final_list,
        blob_storage_container_name=container_name,
        blob_service_client=blob_service_client,
        local_root_dir=".",
        remote_root_dir=path.join(location_in_blob),
    )
    return final_list


def download_file(
    c_client: ContainerClient,
    src_path: str,
    dest_path: str,
    do_check: bool = True,
    check_size: bool = True,
    verbose: bool = False,
) -> None:
    """Download a single file from Azure Blob Storage to the local filesystem.

    Downloads a blob file from Azure Storage to a local destination, with optional
    size checking and verification. Creates necessary parent directories automatically.

    Args:
        c_client (ContainerClient): Azure Container Client instance for the source container.
        src_path (str): Path of the blob file within the container to download.
        dest_path (str): Local filesystem path where the file should be saved.
            Parent directories will be created if they don't exist.
        do_check (bool, optional): Whether to verify that the source blob exists
            before downloading. Default is True.
        check_size (bool, optional): Whether to check file size and warn/prompt for
            large files (>1GB). Default is True.
        verbose (bool, optional): Whether to print download progress information.
            Default is False.

    Example:
        Download a data file:

            container_client = blob_service_client.get_container_client("data")
            download_file(
                c_client=container_client,
                src_path="datasets/data.csv",
                dest_path="./local_data.csv",
                verbose=True
            )

        Download without size check:

            download_file(
                c_client=container_client,
                src_path="logs/large_log.txt",
                dest_path="/tmp/log.txt",
                check_size=False
            )

    Note:
        - Files larger than 1GB trigger a confirmation prompt if check_size=True
        - Parent directories are created automatically
        - The download will overwrite existing files at the destination
        - Uses binary mode for reliable transfer of all file types
    """
    # check size
    if check_size:
        lblobs = c_client.list_blobs(name_starts_with=src_path)
        for blob in lblobs:
            if blob.name == src_path:
                t_size = blob.size
                print("Size of file to download: ", ns(t_size))
                if t_size > 1e9:
                    print("Warning: File size is greater than 1 GB.")
                    cont = input("Continue? [Y/n]: ")
                    if cont.lower() != "y":
                        print("Download aborted.")
                        return None
    download_stream = read_blob_stream(
        src_path, container_client=c_client, do_check=do_check
    )
    dest_path = Path(dest_path)
    dest_path.parents[0].mkdir(parents=True, exist_ok=True)
    with dest_path.open(mode="wb") as blob_download:
        blob_download.write(download_stream.readall())
        logger.debug("File downloaded.")
        if verbose:
            print(f"Downloaded {src_path} to {dest_path}")


def get_container_client(
    account_name: str, container_name: str
) -> ContainerClient:
    """Create a container client using managed identity authentication.

    Instantiates a container client using the specified account name and container name,
    with authentication via Azure Managed Identity.

    Args:
        account_name (str): Azure storage account name (without .blob.core.windows.net).
        container_name (str): Name of the blob storage container.

    Returns:
        ContainerClient: Azure Container Client instance for the specified container.

    Example:
        Create a container client for a specific container:

            client = get_container_client(
                account_name="mystorageaccount",
                container_name="data"
            )

    Note:
        This function uses ManagedIdentityCredential for authentication, so it
        should be used in environments where managed identity is available (e.g.,
        Azure VMs, Container Instances, App Service).
    """
    config = {
        "Storage": {
            "storage_account_url": f"https://{account_name}.blob.core.windows.net"
        }
    }
    blob_service_client = get_blob_service_client(
        config=config, credential=ManagedIdentityCredential()
    )
    container_client = blob_service_client.get_container_client(
        container=container_name
    )
    return container_client


def read_blob_stream(
    blob_url: str,
    account_name: str = None,
    container_name: str = None,
    container_client: ContainerClient = None,
    do_check: bool = True,
) -> StorageStreamDownloader[str]:
    """Read a blob as a stream from Azure Blob Storage.

    Creates a download stream for a blob from Azure Storage. Can work with either
    a provided container client or account/container names to create the client.

    Args:
        blob_url (str): Path/name of the blob within the container to stream.
        account_name (str, optional): Azure storage account name. Required if
            container_client is not provided.
        container_name (str, optional): Name of the blob container. Required if
            container_client is not provided.
        container_client (ContainerClient, optional): Azure Container Client instance.
            If provided, account_name and container_name are ignored.
        do_check (bool, optional): Whether to verify the blob exists before streaming.
            Default is True.

    Returns:
        StorageStreamDownloader[str]: Stream downloader object for reading blob data.

    Raises:
        ValueError: If neither container_client nor both account_name and container_name
            are provided, or if do_check=True and the blob doesn't exist.

    Example:
        Stream a blob using container client:

            stream = read_blob_stream(
                blob_url="data/file.txt",
                container_client=container_client
            )

        Stream a blob using account and container names:

            stream = read_blob_stream(
                blob_url="logs/app.log",
                account_name="mystorageaccount",
                container_name="logs"
            )
    """
    if container_client:
        pass
    elif container_name and account_name:
        container_client = get_container_client(account_name, container_name)
    else:
        raise ValueError(
            "Either container name and account name or container client must be provided."
        )

    if do_check and not check_blob_existence(container_client, blob_url):
        raise ValueError(f"Source blob: {blob_url} does not exist.")
    download_stream = container_client.download_blob(blob=blob_url)
    return download_stream


def check_blob_existence(c_client: ContainerClient, blob_name: str) -> bool:
    """Check if a blob exists in the container.

    Args:
        c_client (ContainerClient): Azure Container Client instance for the container.
        blob_name (str): Name/path of the blob to check within the container.

    Returns:
        bool: True if the blob exists, False otherwise.

    Example:
        Check if a file exists before downloading:

            if check_blob_existence(container_client, "data/file.csv"):
                print("File exists, proceeding with download")
            else:
                print("File not found")
    """
    logger.debug("Checking Blob existence.")
    blob = c_client.get_blob_client(blob=blob_name)
    logger.debug(f"Blob exists: {blob.exists()}")
    return blob.exists()


def check_virtual_directory_existence(
    c_client: ContainerClient, vdir_path: str
) -> bool:
    """Checks whether any blobs exist with the specified virtual directory path

    Args:
        c_client (ContainerClient): an Azure Container Client object
        vdir_path (str): path of virtual directory

    Returns:
        bool: whether the virtual directory exists

    """
    blobs = list_blobs_in_container(
        name_starts_with=vdir_path, container_client=c_client
    )
    try:
        first_blob = next(blobs)
        logger.debug(f"{first_blob.name} found.")
        return True
    except StopIteration as e:
        logger.error(repr(e))
        raise e


def list_blobs_in_container(
    container_name: str = None,
    account_name: str = None,
    name_starts_with: str = None,
    blob_service_client: BlobServiceClient = None,
    container_client: ContainerClient = None,
):
    """List blobs in a container with optional name filtering.

    Returns a list of blobs from the specified container, optionally filtering
    by name prefix. Can work with various combinations of parameters to access
    the container.

    Args:
        container_name (str, optional): Name of the blob container to list.
        account_name (str, optional): Azure storage account name.
        name_starts_with (str, optional): Prefix filter for blob names.
            Only blobs whose names start with this string will be returned.
        blob_service_client (BlobServiceClient, optional): Azure Blob service client.
        container_client (ContainerClient, optional): Azure Container client instance.

    Returns:
        ItemPaged: Iterator of blob objects matching the criteria.

    Example:
        List all blobs in a container:

            blobs = list_blobs_in_container(
                container_name="data",
                blob_service_client=client
            )

        List blobs with specific prefix:

            blobs = list_blobs_in_container(
                container_name="logs",
                name_starts_with="2024/",
                blob_service_client=client
            )
    """
    return instantiate_container_client(
        container_name=container_name,
        account_name=account_name,
        blob_service_client=blob_service_client,
        container_client=container_client,
    ).list_blobs(name_starts_with)


def list_blobs_flat(
    container_name: str, blob_service_client: BlobServiceClient, verbose=True
) -> list[str]:
    """List all blob names in a container with optional filtering.

    Args:
        container_name (str): name of Blob container
        blob_service_client (BlobServiceClient): instance of BlobServiceClient
        verbose (bool): whether to be verbose in printing files. Default True.

    Returns:
        list: list of blobs in Blob container
    """
    blob_list = list_blobs_in_container(
        container_name=container_name, blob_service_client=blob_service_client
    )
    blob_names = [blob.name for blob in blob_list]
    logger.debug("Blob names gathered.")
    if verbose:
        for blob in blob_list:
            logger.info(f"Name: {blob.name}")
    return blob_names


def get_blob_service_client(config: dict, credential: object):
    """establishes Blob Service Client using credentials

    Args:
        config (dict): contains configuration info
        credential (object): credential object from azure.identity

    Returns:
        class: an instance of BlobServiceClient
    """
    logger.debug("Initializing Blob Service Client...")
    try:
        blob_service_client = BlobServiceClient(
            account_url=config["Storage"]["storage_account_url"],
            credential=credential,
        )
        logger.debug("Blob Service Client successfully created.")
        return blob_service_client
    except KeyError as e:
        logger.error(
            f"Configuration error: '{e}' does not exist in the config file. Please add it in the Storage section.",
        )
        raise e


def instantiate_container_client(
    container_name: str = None,
    account_name: str = None,
    blob_service_client: BlobServiceClient = None,
    container_client: ContainerClient = None,
) -> ContainerClient:
    """Create and return an Azure Container Client instance.

    Creates a container client using various combinations of provided parameters.
    Can work with an existing container client, blob service client, or create
    a new one using account and container names.

    Args:
        container_name (str, optional): Name of the blob storage container.
        account_name (str, optional): Azure storage account name.
        blob_service_client (BlobServiceClient, optional): Existing blob service client.
        container_client (ContainerClient, optional): Existing container client to return as-is.

    Returns:
        ContainerClient: Azure Container Client instance for the specified container.

    Raises:
        ValueError: If insufficient parameters are provided to create a container client.

    Example:
        Create client from account and container names:

            client = instantiate_container_client(
                container_name="data",
                account_name="mystorageaccount"
            )

        Use existing blob service client:

            client = instantiate_container_client(
                container_name="data",
                blob_service_client=blob_client
            )
    """
    logger.debug("Creating container client for getting Blob info.")
    if container_client:
        pass
    elif blob_service_client and container_name:
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
    elif container_name and account_name:
        config = {
            "Storage": {
                "storage_account_url": f"https://{account_name}.blob.core.windows.net"
            }
        }
        blob_service_client = get_blob_service_client(
            config=config, credential=ManagedIdentityCredential()
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
    else:
        raise ValueError(
            "Either container name, account name, container client or blob service client must be provided."
        )
    logger.debug("Container client created. Listing Blob info.")
    return container_client


def download_folder(
    container_name: str,
    src_path: str,
    dest_path: str,
    blob_service_client,
    include_extensions: str | list | None = None,
    exclude_extensions: str | list | None = None,
    verbose=True,
    check_size=True,
) -> None:
    """Download an entire folder (virtual directory) from Azure Blob Storage.

    Recursively downloads all files from a virtual directory in a blob storage
    container, preserving the directory structure. Supports filtering by file
    extensions and includes size checking for large downloads.

    Args:
        container_name (str): Name of the blob storage container containing the folder.
        src_path (str): Path of the virtual directory within the container to download.
            Will be treated as a prefix for blob names.
        dest_path (str): Local filesystem path where the directory should be saved.
            Directory structure will be recreated under this path.
        blob_service_client: Azure Blob service client instance for API calls.
        include_extensions (str | list, optional): File extensions to include in the
            download. Can be a single extension string or list of extensions. Cannot
            be used together with exclude_extensions.
        exclude_extensions (str | list, optional): File extensions to exclude from
            the download. Can be a single extension string or list. Cannot be used
            together with include_extensions.
        verbose (bool, optional): Whether to print download progress information.
            Default is True.
        check_size (bool, optional): Whether to check total download size and warn/prompt
            for large downloads (>2GB). Default is True.

    Raises:
        ValueError: If the source virtual directory doesn't exist, or if both
            include_extensions and exclude_extensions are specified.
        Exception: If both include_extensions and exclude_extensions are specified.

    Example:
        Download entire results directory:

            download_folder(
                container_name="job-outputs",
                src_path="job-123/results",
                dest_path="./local_results",
                blob_service_client=client
            )

        Download only CSV files from a directory:

            download_folder(
                container_name="data",
                src_path="datasets/",
                dest_path="./data",
                blob_service_client=client,
                include_extensions=[".csv", ".json"]
            )

    Note:
        - The destination directory is created if it doesn't exist
        - Downloads >2GB trigger a confirmation prompt if check_size=True
        - Virtual directory paths are treated as blob name prefixes
        - Extensions are automatically formatted with leading periods
    """
    # check that include and exclude extensions are not both used, format if exist
    if include_extensions is not None:
        include_extensions = format_extensions(include_extensions)
    elif exclude_extensions is not None:
        exclude_extensions = format_extensions(exclude_extensions)
    if include_extensions is not None and exclude_extensions is not None:
        logger.error(
            "Use included_extensions or exclude_extensions, not both."
        )
        print("Use included_extensions or exclude_extensions, not both.")
        raise Exception(
            "Use included_extensions or exclude_extensions, not both."
        ) from None
    # check container exists
    logger.debug(f"Checking Blob container {container_name} exists.")
    # create container client
    c_client = blob_service_client.get_container_client(
        container=container_name
    )
    if not check_virtual_directory_existence(c_client, src_path):
        raise ValueError(
            f"Source virtual directory: {src_path} does not exist."
        )

    blob_list = []
    if not src_path.endswith("/"):
        src_path += "/"
    for blob in list_blobs_in_container(
        name_starts_with=src_path, container_client=c_client
    ):
        b = blob.name
        if b.split(src_path)[0] == "" and "." in b:
            blob_list.append(b)

    flist = []
    if include_extensions is None:
        if exclude_extensions is not None:
            # find files that don't contain the specified extensions
            for _file in blob_list:
                if os.path.splitext(_file)[1] not in exclude_extensions:
                    flist.append(_file)
        else:  # this is for no specified extensions to include or exclude
            flist = blob_list
    else:
        # include only specified extension files
        for _file in blob_list:
            if os.path.splitext(_file)[1] in include_extensions:
                flist.append(_file)
    # input check on file size here
    if check_size:
        lblobs = c_client.list_blobs(name_starts_with=src_path)
        t_size = 0
        gb = 1e9
        for blob in lblobs:
            if blob.name in flist:
                t_size += blob.size
        print("Total size of files to download: ", ns(t_size))
        if t_size > 2 * gb:
            print(
                "Warning: Total size of files to download is greater than 2 GB."
            )
            cont = input("Continue? [Y/n]: ")
            if cont.lower() != "y":
                print("Download aborted.")
                return None
    for blob in flist:
        download_file(
            c_client,
            blob,
            os.path.join(dest_path, blob),
            False,
            verbose=verbose,
            check_size=False,
        )
    logger.debug("Download complete.")


def delete_blob_snapshots(
    blob_name: str, container_name: str, blob_service_client: object
):
    """Delete a blob and all its snapshots from Azure Blob Storage.

    Permanently removes the specified blob and all of its snapshots from the given
    container. This operation cannot be undone.

    Args:
        blob_name (str): Name/path of the blob to delete within the container.
        container_name (str): Name of the blob storage container containing the blob.
        blob_service_client (object): Azure Blob service client instance for API calls.

    Example:
        Delete a single blob and its snapshots:

            delete_blob_snapshots(
                blob_name="data/file1.csv",
                container_name="input-data",
                blob_service_client=client
            )

    Warning:
        This operation is irreversible. All snapshots of the blob will be deleted.
        Ensure you have backed up any important data before deletion.
    """
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    blob_client.delete_blob(delete_snapshots="include")
    logger.info(f"Deleted {blob_name} from {container_name}.")


def delete_blob_folder(
    folder_path: str, container_name: str, blob_service_client: object
):
    """Delete all blobs within a folder (virtual directory) in Azure Blob Storage.

    Recursively deletes all blobs that have the specified folder path as a prefix
    in their blob names. This operation deletes all files within the folder and
    their snapshots.

    Args:
        folder_path (str): Path of the folder (prefix) to delete within the container.
            All blobs whose names start with this prefix will be deleted.
        container_name (str): Name of the blob storage container containing the folder.
        blob_service_client (object): Azure Blob service client instance for API calls.

    Example:
        Delete all files in a temporary folder:

            delete_blob_folder(
                folder_path="temp/job-123/",
                container_name="workspace",
                blob_service_client=client
            )

    Warning:
        This operation is irreversible. All files and snapshots within the specified
        folder will be permanently deleted. Ensure you have backed up any important
        data before deletion.
    """
    # create container client
    c_client = blob_service_client.get_container_client(
        container=container_name
    )
    # list out files in folder
    blob_names = c_client.list_blob_names(name_starts_with=folder_path)
    _files = [blob for blob in blob_names]
    # call delete_blob_snapshots for each file
    for file in _files:
        delete_blob_snapshots(
            blob_name=file,
            container_name=container_name,
            blob_service_client=blob_service_client,
        )


def walk_blobs_in_container(
    container_name: str = None,
    account_name: str = None,
    name_starts_with: str = None,
    blob_service_client: BlobServiceClient = None,
    container_client: ContainerClient = None,
):
    return instantiate_container_client(
        container_name=container_name,
        account_name=account_name,
        blob_service_client=blob_service_client,
        container_client=container_client,
    ).walk_blobs(name_starts_with)


def write_blob_stream(
    data,
    blob_url: str,
    account_name: str = None,
    container_name: str = None,
    container_client: ContainerClient = None,
    append_blob: bool = False,
    overwrite: bool = False,
) -> bool:
    """
    Write a stream into a file in Azure Blob storage

    Args:
        data (stream):
            [Required] File contents as stream
        blob_url (str):
            [Required] Path within the container to the desired file (including filename)
        account_name (str):
            [Optional] Name of Azure storage account
        container_name (str):
            [Optional] Name of Blob container within storage account
        container_client (ContainerClient):
            [Optional] Instance of ContainerClient provided with the storage account

    Raises:
        ValueError:
            When no blobs exist with the specified name (src_path)
    """
    if container_client:
        pass
    elif container_name and account_name:
        config = {
            "Storage": {
                "storage_account_url": f"https://{account_name}.blob.core.windows.net"
            }
        }
        blob_service_client = get_blob_service_client(
            config=config, credential=DefaultAzureCredential()
        )
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
    else:
        raise ValueError(
            "Either container name and account name or container client must be provided."
        )
    if append_blob:
        blob_type = BlobType.APPENDBLOB
    else:
        blob_type = BlobType.BLOCKBLOB
    container_client.upload_blob(
        name=blob_url, data=data, blob_type=blob_type, overwrite=overwrite
    )
    return True
