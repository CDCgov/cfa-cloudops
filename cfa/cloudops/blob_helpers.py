import logging
import os
from os import path, walk

from .blob import upload_to_storage_container

logger = logging.getLogger(__name__)


def format_extensions(extension):
    """
    Formats extensions to include periods.
    Args:
        extension (str | list): string or list of strings of extensions. Can include a leading period but does not need to.

    Returns:
        list: list of formatted extensions
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
    for file in flist:
        # if a pattern from exclude_patterns is found, skip uploading this file
        if exclude_patterns is not None and any(
            pattern in file for pattern in exclude_patterns
        ):
            # dont upload this file if an excluded pattern is found within
            continue
        # get the right folder location, need to drop the folder from the beginning and remove the file name, keeping only middle folders
        drop_folder = path.dirname(file).replace(folder, "", 1)
        if drop_folder.startswith("/"):
            drop_folder = drop_folder[
                1:
            ]  # removes the / so path.join doesnt mistake for root
        logger.debug(f"Calling upload_blob_file for {file}")
        upload_to_storage_container(
            file_paths=file,
            blob_storage_container_name=container_name,
            blob_service_client=blob_service_client,
            local_root_dir=".",
            remote_root_dir=path.join(location_in_blob, drop_folder),
        )
    return file_list
