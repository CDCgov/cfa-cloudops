"""
Functions for manipulating tasks within an
Azure batch job.
"""

import logging
from pathlib import Path

import azure.batch.models as batchmodels
from azure.batch.models import (
    ComputeNodeIdentityReference,
    ContainerRegistry,
    ContainerWorkingDirectory,
    OutputFile,
    OutputFileBlobContainerDestination,
    OutputFileDestination,
    OutputFileUploadOptions,
    TaskAddParameter,
    TaskContainerSettings,
    UserIdentity,
)

from .auth import get_compute_node_identity_reference
from .defaults import default_azure_blob_storage_endpoint_subdomain
from .endpoints import construct_blob_container_endpoint
from .util import ensure_listlike

logger = logging.getLogger(__name__)


def create_bind_mount_string(
    az_mount_dir: str, source_path: str, target_path: str
) -> str:
    """Create a valid OCI bind mount string for an OCI container running in Azure batch.

    Creates a bind mount string for mounting things from Azure blob storage.

    Args:
        az_mount_dir: Directory in which to look for directories or volumes to mount.
        source_path: Path relative to ``az_mount_dir`` to use as the source.
        target_path: Absolute path within the container to bind to the source path.

    Returns:
        str: A properly formatted OCI --mount type=bind command, as a string.

    Example:
        >>> mount_str = create_bind_mount_string(
        ...     "/mnt/batch/tasks/fsmounts",
        ...     "data",
        ...     "/app/data"
        ... )
        >>> print(mount_str)
        '--mount type=bind,source=/mnt/batch/tasks/fsmounts/data,target=/app/data'
    """
    mount_template = "--mount type=bind,source={}/{},target={}"
    return mount_template.format(az_mount_dir, source_path, target_path)


def get_container_settings(
    container_image_name: str,
    az_mount_dir: str = "$AZ_BATCH_NODE_MOUNTS_DIR",
    working_directory: str | ContainerWorkingDirectory = None,
    mount_pairs: list[dict] = None,
    additional_options: str = "",
    registry: ContainerRegistry = None,
    **kwargs,
) -> TaskContainerSettings:
    """Create a valid set of container settings with bind mounts for an OCI container.

    Creates container settings with bind mounts specified in mount_pairs,
    for an OCI container run in an Azure batch task.

    Args:
        container_image_name: Name of the OCI container image to use.
        az_mount_dir: Directory in which to look for directories or volumes to mount.
        working_directory: Working directory for the task within the container, passed
            as the working_directory parameter to the TaskContainerSettings constructor.
            If None (the default), then defer to the Azure batch default (note that this
            will _not_ typically be the same as the container image's own WORKDIR).
            Otherwise specify it with a TaskWorkingDirectory instance or use the string
            "containerImageDefault" to use the container's own WORKDIR. See the
            documentation for TaskContainerSettings for more details.
        mount_pairs: Pairs of 'source' and 'target' directories to mount when the
            container is run, as a list of dictionaries with 'source' and 'target' keys.
        additional_options: Additional flags and options to pass to the container
            run command, as a string. Defaults to "".
        registry: ContainerRegistry instance specifying a private container registry
            from which to fetch task containers. Defaults to None.
        **kwargs: Additional keyword arguments passed to the TaskContainerSettings constructor.

    Returns:
        TaskContainerSettings: A TaskContainerSettings object instantiated according
            to the specified input.

    Example:
        >>> mount_pairs = [
        ...     {"source": "data", "target": "/app/data"},
        ...     {"source": "output", "target": "/app/output"}
        ... ]
        >>> settings = get_container_settings(
        ...     "myregistry.azurecr.io/myapp:latest",
        ...     mount_pairs=mount_pairs,
        ...     additional_options="--env MODE=production"
        ... )
        >>> print(settings.image_name)
        'myregistry.azurecr.io/myapp:latest'
    """

    ctr_r_opts = additional_options

    for pair in mount_pairs:
        ctr_r_opts += " " + create_bind_mount_string(
            az_mount_dir, pair["source"], pair["target"]
        )

    return TaskContainerSettings(
        image_name=container_image_name,
        working_directory=working_directory,
        container_run_options=ctr_r_opts,
        registry=registry,
        **kwargs,
    )


def output_task_files_to_blob(
    file_pattern: str,
    blob_container: str,
    blob_account: str,
    path: str = None,
    upload_condition: str = "taskCompletion",
    blob_endpoint_subdomain: str = default_azure_blob_storage_endpoint_subdomain,
    compute_node_identity_reference: ComputeNodeIdentityReference = None,
    **kwargs,
) -> OutputFile:
    """Get a properly configured OutputFile object for uploading files from a Batch task to Blob storage.

    Args:
        file_pattern: File pattern to match when uploading. Passed as the
            ``file_pattern`` argument to OutputFile.
        blob_container: Name of the Azure blob storage container to which
            to upload the files.
        blob_account: Name of the Azure blob storage account in which to look for
            the Blob storage container specified in ``blob_container``.
        path: Path within the Blob storage container to which to upload the file(s).
            Passed as the ``path`` argument to the OutputFileBlobContainerDestination
            constructor. If None, upload to the root of the container. If ``file_pattern``
            contains wildcards, ``path`` gives the subdirectory within the container to
            upload them with their original filenames and extensions. If ``file_pattern``
            contains no wildcards, ``path`` is treated as the full file path including
            filename and extension (i.e. the file is renamed). See
            OutputFileBlobContainerDestination for details.
        upload_condition: Condition under which to upload the file(s). Options are
            "taskCompletion" (always upload, the default), "taskFailure" (upload only
            for failed tasks), and "taskSuccess" (upload only for successful tasks).
            Passed as the ``upload_condition`` argument to OutputFileUploadOptions.
        blob_endpoint_subdomain: Azure Blob endpoint subdomains and domains that follow
            the account name. If None (default), use this package's
            default_azure_blob_storage_endpoint_subdomain.
        compute_node_identity_reference: ComputeNodeIdentityReference to use when
            constructing a OutputFileBlobContainerDestination object for logging.
            If None (default), attempt to obtain one via get_compute_node_identity_reference.
        **kwargs: Additional keyword arguments passed to the OutputFile constructor.

    Returns:
        OutputFile: An OutputFile object that can be used in constructing a
            batch task via get_task_config.

    Raises:
        TypeError: If ``compute_node_identity_reference`` is not of the required type.

    Example:
        >>> output_file = output_task_files_to_blob(
        ...     file_pattern="*.log",
        ...     blob_container="task-outputs",
        ...     blob_account="mystorageaccount",
        ...     path="logs/task-123",
        ...     upload_condition="taskCompletion"
        ... )
        >>> print(output_file.file_pattern)
        '*.log'
    """
    if compute_node_identity_reference is None:
        compute_node_identity_reference = get_compute_node_identity_reference()
    if not isinstance(compute_node_identity_reference, ComputeNodeIdentityReference):
        raise TypeError(
            "compute_node_identity_reference "
            "must be an instance of "
            "ComputeNodeIdentityReference. "
            f"Got {type(compute_node_identity_reference)}."
        )
    container = OutputFileBlobContainerDestination(
        container_url=construct_blob_container_endpoint(
            blob_container,
            blob_account,
            blob_endpoint_subdomain,
        ),
        path=path,
        identity_reference=compute_node_identity_reference,
    )
    destination = OutputFileDestination(container=container)
    upload_options = OutputFileUploadOptions(upload_condition=upload_condition)

    return OutputFile(
        file_pattern=file_pattern,
        destination=destination,
        upload_options=upload_options,
        **kwargs,
    )


def get_task_config(
    task_id: str,
    base_call: str,
    container_settings: TaskContainerSettings = None,
    user_identity: UserIdentity = None,
    log_blob_container: str = None,
    log_blob_account: str = None,
    log_subdir: str = None,
    log_file_pattern: str = "../std*.txt",
    log_upload_condition: str = "taskCompletion",
    log_compute_node_identity_reference: ComputeNodeIdentityReference = None,
    output_files: list[OutputFile] | OutputFile = None,
    **kwargs,
) -> TaskAddParameter:
    """Create a batch task with a given base call and set of container settings.

    If the ``user_identity`` is not set, set it up automatically with sufficient
    permissions to read and write from mounted volumes.

    Args:
        task_id: Alphanumeric identifier for the task.
        base_call: The base command line call for the task, as a string.
        container_settings: Container settings for the task. You can use the
            create_container_settings helper function to create a valid entry.
            Defaults to None.
        user_identity: User identity under which to run the task. If None, create
            one automatically with admin privileges, if permitted. Defaults to None.
        log_blob_container: If provided, save the contents of the stderr and stdout
            buffers (default) and/or other specified log files from task execution
            to files named in the specified Azure blob storage container. If None,
            do not preserve the contents of those buffers.
        log_blob_account: Azure Blob storage account in which to look for the storage
            container specified in ``log_blob_container``. Ignored if ``log_blob_container``
            is None. Defaults to None.
        log_subdir: Subdirectory of the Blob storage container given in
            ``log_blob_storage_container`` in which to save the log ``.txt`` files.
            If None, save at the root of the Blob storage container. Ignored if
            ``log_blob_container`` is None.
        log_file_pattern: File pattern for logs to persist. Defaults to "../std*.txt",
            which matches the ``.txt`` output files for the stdout and stderr buffers
            in a standard Azure Batch Linux task, which are stored one directory up
            from the task working directory. Ignored if ``log_blob_container`` is None.
        log_upload_condition: Condition under which to upload logs. Options are
            "taskCompletion" (always upload, the default), "taskFailure" (upload only
            for failed tasks), and "taskSuccess" (upload only for successful tasks).
            Passed as the ``upload_condition`` argument to OutputFileUploadOptions.
        log_compute_node_identity_reference: ComputeNodeIdentityReference to use when
            constructing a OutputFileBlobContainerDestination object for logging.
            If None (default), attempt to obtain one via get_compute_node_identity_reference.
            Ignored if ``log_blob_container`` is None.
        output_files: OutputFile object or list of such objects specifying additional
            output files for the task beyond those auto-constructed for persisting logs
            to ``log_blob_container``. Passed along with those autogenerated OutputFile
            objects as the ``output_files`` parameter to the TaskAddParameter constructor.
        **kwargs: Additional keyword arguments passed to the TaskAddParameter constructor.

    Returns:
        TaskAddParameter: The task configuration object.

    Example:
        >>> from azure.batch.models import TaskContainerSettings
        >>>
        >>> # Basic task without container
        >>> task = get_task_config(
        ...     task_id="my-task-001",
        ...     base_call="python /app/script.py --input data.txt"
        ... )
        >>>
        >>> # Task with container and logging
        >>> container_settings = TaskContainerSettings(
        ...     image_name="myregistry.azurecr.io/myapp:latest"
        ... )
        >>> task = get_task_config(
        ...     task_id="my-task-002",
        ...     base_call="python /app/script.py",
        ...     container_settings=container_settings,
        ...     log_blob_container="task-logs",
        ...     log_blob_account="mystorageaccount",
        ...     log_subdir="job-123"
        ... )
        >>> print(task.id)
        'my-task-002'
    """
    if user_identity is None:
        user_identity = UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                scope=batchmodels.AutoUserScope.pool,
                elevation_level=batchmodels.ElevationLevel.admin,
            )
        )
    if output_files is None:
        output_files = []

    if log_blob_container is not None:
        if log_subdir is None:
            log_subdir = ""
        log_output_files = output_task_files_to_blob(
            file_pattern=log_file_pattern,
            blob_container=log_blob_container,
            blob_account=log_blob_account,
            path=Path(log_subdir, task_id).as_posix(),
            upload_condition=log_upload_condition,
            compute_node_identity_reference=log_compute_node_identity_reference,
        )
    else:
        log_output_files = []

    task_config = TaskAddParameter(
        id=task_id,
        command_line=base_call,
        container_settings=container_settings,
        user_identity=user_identity,
        output_files=ensure_listlike(output_files) + ensure_listlike(log_output_files),
        **kwargs,
    )

    return task_config
