import logging
import os
import shutil
import subprocess as sp
from os import path, walk
from pathlib import Path

import docker
import yaml
from docker.errors import DockerException
from griddler import parse

from cfa.cloudops.local import batch

logger = logging.getLogger(__name__)


def add_job(
    job_id: str,
    pool_id: str,
    task_retries: int = 0,
    mark_complete: bool = False,
):
    """takes in a job ID and config to create a job in the pool

    Args:
        job_id (str): name of the job to run
        pool_id (str): name of pool
        task_retries (int): number of times to retry the task if it fails. Default 3.
        mark_complete (bool): whether to mark the job complete after tasks finish running. Default False.
    """
    logger.debug(f"Attempting to create job '{job_id}'...")
    logger.debug("Attempting to add job.")
    j = batch.Job(job_id, pool_id, task_retries, mark_complete)
    # save job info
    os.makedirs("tmp/jobs", exist_ok=True)
    logger.debug("saving to tmp/jobs folder")
    save_path = Path(f"tmp/jobs/{job_id}.txt")
    save_path.write_text(f"{pool_id} {task_retries} {mark_complete}")
    return j


def create_container(container_name: str, blob_service_client: object):
    """creates a Blob container if not exists

    Args:
        container_name (str): user specified name for Blob container
        blob_service_client (object): BlobServiceClient object

    Returns:
       object: ContainerClient object
    """
    logger.debug(f"Attempting to create or access container: {container_name}")
    try:
        os.makedirs(container_name)
        logger.info(f"Container [{container_name}] created successfully.")
    except Exception:
        logger.debug(
            f"Container [{container_name}] already exists. No action needed."
        )
    return "container_client"


def upload_to_storage_container(
    filepath: str,
    location: str = "",
    container_name: object = None,
    verbose: bool = False,
):
    """Uploads a specified file to Blob storage.
    Args:
        filepath (str): the path to the file.
        location (str): the location (folder) inside the Blob container. Uploaded to root if "". Default is "".
        container_name: name of Blob container (local).
        verbose (bool): whether to be verbose in uploaded files. Defaults to False

    Example:
        upload_blob_file("sample_file.txt", container_client = cc, verbose = False)
        - uploads the "sample_file.txt" file to the root of the blob container

        upload_blob_file("sample_file.txt", "job_1/input", cc, False)
        - uploads the "sample_file.txt" file to the job_1/input folder of the blob container.
        - note that job_1/input will be created if it does not exist.
    """
    if location.startswith("/"):
        location = location[1:]
    # check container exists
    if not os.path.exists(container_name):
        logger.error("container does not exist")
        return None
    else:
        logger.debug("Container exists")

    _, _file = path.split(filepath)
    _name = path.join(location, _file)
    print(_name)
    src = Path(filepath)
    dest = Path(f"{container_name}/{_name}")
    os.makedirs(f"{container_name}/{location}", exist_ok=True)
    # copy file to container
    shutil.copy2(src, dest)
    if verbose:
        print(f"Uploaded {filepath} to {container_name} as {_name}.")
        logger.info(f"Uploaded {filepath} to {container_name} as {_name}.")


def upload_folder(
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
    if not os.path.exists(container_name):
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


def package_and_upload_dockerfile(
    registry_name: str,
    repo_name: str,
    tag: str,
    path_to_dockerfile: str = "./Dockerfile",
    use_device_code: bool = False,
):
    """
    Packages Dockerfile in root of repo and uploads to the specified registry and repo with designated tag in Azure.

    Args:
        registry_name (str): name of Azure Container Registry
        repo_name (str): name of repo
        tag (str): tag for the Docker container
        path_to_dockerfile (str): path to Dockerfile. Default is ./Dockerfile.
        use_device_code (bool): whether to use the device code when authenticating. Default False.

    Returns:
        str: full container name
    """
    # check if Dockerfile exists
    logger.debug("Trying to ping docker daemon.")
    try:
        d = docker.from_env(timeout=10).ping()
        logger.debug("Docker is running.")
    except DockerException:
        logger.error("Could not ping Docker. Make sure Docker is running.")
        logger.warning("Container not packaged/uploaded.")
        logger.warning("Try again when Docker is running.")
        raise DockerException("Make sure Docker is running.") from None

    if os.path.exists(path_to_dockerfile) and d:
        full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"
        logger.info(f"full container name: {full_container_name}")
        # Build container
        logger.debug("Building container.")
        sp.run(
            f"docker image build -f {path_to_dockerfile} -t {full_container_name} .",
            shell=True,
        )
        # Upload container to registry
        # upload with device login if desired
        if use_device_code:
            logger.debug("Device code used here for az login.")
        else:
            logger.debug("Logging in to Azure with az login.")
        logger.debug("Pushing Docker container to ACR.")
        print(f"Built {full_container_name}")
        return full_container_name
    else:
        logger.error("Dockerfile does not exist in the root of the directory.")
        raise Exception(
            "Dockerfile does not exist in the root of the directory."
        ) from None


def upload_docker_image(
    image_name: str,
    registry_name: str,
    repo_name: str,
    tag: str = "latest",
    use_device_code: bool = False,
):
    """
    Args:
        image_name (str): name of image in local Docker
        registry_name (str): name of Azure Container Registry
        repo_name (str): name of repo
        tag (str): tag for the Docker container. Default is "latest". If None, a timestamp tag will be generated.
        use_device_code (bool): whether to use the device code when authenticating. Default False.

    Returns:
        str: full container name
    """
    full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"

    # check if docker is running
    logger.debug("Trying to ping docker daemon.")
    try:
        docker_env = docker.from_env(timeout=8)
        docker_env.ping()
        logger.debug("Docker is running.")
    except DockerException:
        logger.error("Could not ping Docker. Make sure Docker is running.")
        logger.warning("Container not uploaded.")
        logger.warning("Try again when Docker is running.")
        raise DockerException("Make sure Docker is running.") from None

    # Tagging the image with the unique tag
    logger.debug(f"Tagging image {image_name} with {full_container_name}.")
    try:
        image = docker_env.images.get(image_name)
        image.tag(full_container_name)
    except docker.errors.ImageNotFound:
        # Log available images to guide the user
        available_images = [img.tags for img in docker_env.images.list()]
        logger.error(
            f"Image {image_name} does not exist. Available images are: {available_images}"
        )
        raise

    # Log in to ACR and upload container to registry
    # upload with device login if desired
    if use_device_code:
        logger.debug("Logging in with device code.")
    else:
        logger.debug("Logging in to Azure.")
    logger.debug("Pushing Docker container to ACR.")
    logger.debug("Container should have been uploaded.")
    return full_container_name


def download_file(c_client, src_path, dest_path, do_check, check_size):
    if dest_path.startswith("/"):
        dest_path = dest_path[1:]
    # check container exists
    if not os.path.exists(src_path):
        logger.error("container does not exist")
        return None
    else:
        logger.debug("Container exists")

    src = Path(src_path)
    dest = Path(dest_path)
    # copy file to container
    shutil.copy2(src, dest)


def get_tasks_from_yaml(base_cmd: str, file_path: str) -> list[str]:
    """
    combines output of get_args_from_yaml with a base command to get a complete command

    Args:
        base_cmd (str): base command to append the rest of the yaml arguments to
        file_path (str): path to yaml file

    Returns:
        list[str]: list of full commands created by joining the base command with each set of parameters
    """
    cmds = []
    arg_list = get_args_from_yaml(file_path)
    for s in arg_list:
        cmds.append(f"{base_cmd} {s}")
    return cmds


def get_args_from_yaml(file_path: str) -> list[str]:
    """
    parses yaml file and returns list of strings containing command line arguments and flags captured in the yaml.

    Args:
        file_path (str): path to yaml file

    Returns:
        list[str]: list of command line arguments
    """
    with open(file_path, "r") as f:
        raw_griddler = yaml.safe_load(f)
    griddle = parse(raw_griddler)
    parameter_sets = griddle.to_dicts()
    output = []
    for i in parameter_sets:
        full_cmd = ""
        for key, value in i.items():
            if key.endswith("(flag)"):
                if value != "":
                    full_cmd += f""" --{key.split("(flag)")[0]}"""
            else:
                full_cmd += f" --{key} {value}"
        output.append(full_cmd)
    return output


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
    """
    Args:
        folder (str): folder path

    Returns:
        list: list of file names contained in folder
    """
    file_list = []
    for dirname, _, fname in walk(folder):
        for f in fname:
            _path = path.join(dirname, f)
            file_list.append(_path)
    return file_list
