"""
Functions for manipulating tasks within an
Azure batch job.
"""

import logging
from pathlib import Path

import azure.mgmt.batch.models as batch_mgmt_models

# 15.0.0+ model names use Batch prefix
from azure.batch import models as batchmodels
from azure.batch.models import (
    AutoUserScope,
    AutoUserSpecification,
    BatchNodeIdentityReference,
    BatchTaskContainerSettings,
    BatchTaskCreateOptions,
    ElevationLevel,
    OutputFile,
    OutputFileBlobContainerDestination,
    OutputFileDestination,
    OutputFileUploadConfiguration,
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
    logger.debug(
        f"Creating bind mount string: az_mount_dir='{az_mount_dir}', source_path='{source_path}', target_path='{target_path}'"
    )

    mount_template = "--mount type=bind,source={}/{},target={}"
    logger.debug(f"Using mount template: '{mount_template}'")

    mount_string = mount_template.format(az_mount_dir, source_path, target_path)
    logger.debug(f"Generated bind mount string: '{mount_string}'")

    return mount_string


def get_container_settings(
    container_image_name: str,
    az_mount_dir: str = "/mnt/batch/tasks/fsmounts",
    working_directory: str | batchmodels.ContainerWorkingDirectory | None = None,
    mount_pairs: list[dict] = None,
    additional_options: str = "",
    registry: batchmodels.ContainerRegistryReference = None,
    **kwargs,
) -> BatchTaskContainerSettings:
    """Create a valid set of container settings with bind mounts for an OCI container.

    Creates container settings with bind mounts specified in mount_pairs,
    for an OCI container run in an Azure batch task.
    Requires azure-batch>=15.0.0.

    Args:
        container_image_name: Name of the OCI container image to use.
        az_mount_dir: Directory in which to look for directories or volumes to mount.
        working_directory: Working directory for the task within the container, passed
            as the working_directory parameter to the BatchTaskContainerSettings constructor.
            If None (the default), then defer to the Azure batch default (note that this
            will _not_ typically be the same as the container image's own WORKDIR).
            Otherwise specify it with a ContainerWorkingDirectory instance or use the string
            "containerImageDefault" to use the container's own WORKDIR. See the
            documentation for BatchTaskContainerSettings for more details.
        mount_pairs: Pairs of 'source' and 'target' directories to mount when the
            container is run, as a list of dictionaries with 'source' and 'target' keys.
        additional_options: Additional flags and options to pass to the container
            run command, as a string. Defaults to "".
        registry: ContainerRegistryReference instance specifying a private container registry
            from which to fetch task containers. Defaults to None.
        **kwargs: Additional keyword arguments passed to the BatchTaskContainerSettings constructor.

    Returns:
        BatchTaskContainerSettings: A BatchTaskContainerSettings object instantiated according
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
    logger.debug(f"Creating container settings for image: '{container_image_name}'")
    logger.debug(
        f"Parameters: az_mount_dir='{az_mount_dir}', working_directory={working_directory}"
    )
    logger.debug(
        f"Mount pairs: {mount_pairs}, additional_options='{additional_options}'"
    )

    if registry:
        logger.debug(f"Using private container registry: {registry.registry_server}")
    else:
        logger.debug("No private container registry specified, using default registry")

    ctr_r_opts = additional_options
    logger.debug(f"Starting with base container run options: '{ctr_r_opts}'")

    if mount_pairs:
        logger.debug(f"Processing {len(mount_pairs)} mount pairs")
        for i, pair in enumerate(mount_pairs):
            logger.debug(
                f"Processing mount pair {i + 1}: source='{pair['source']}', target='{pair['target']}'"
            )
            mount_string = create_bind_mount_string(
                az_mount_dir, pair["source"], pair["target"]
            )
            ctr_r_opts += " " + mount_string
            logger.debug(f"Updated container run options: '{ctr_r_opts}'")
    else:
        logger.debug("No mount pairs to process")

    logger.debug(f"Final container run options: '{ctr_r_opts}'")

    container_settings = BatchTaskContainerSettings(
        image_name=container_image_name,
        working_directory=working_directory,
        container_run_options=ctr_r_opts,
        registry=registry,
        **kwargs,
    )

    logger.debug(
        f"Created BatchTaskContainerSettings with image '{container_image_name}' and {len(ctr_r_opts.split()) if ctr_r_opts else 0} run options"
    )

    return container_settings


def output_task_files_to_blob(
    file_pattern: str,
    blob_container: str,
    blob_account: str,
    path: str = None,
    upload_condition: str = "taskCompletion",
    blob_endpoint_subdomain: str = default_azure_blob_storage_endpoint_subdomain,
    compute_node_identity_reference: BatchNodeIdentityReference = None,
    **kwargs,
) -> OutputFile:
    """Get a properly configured BatchOutputFile object for uploading files from a Batch task to Blob storage.

    Requires azure-batch>=15.0.0.

    Args:
        file_pattern: File pattern to match when uploading. Passed as the
            ``file_pattern`` argument to BatchOutputFile.
        blob_container: Name of the Azure blob storage container to which
            to upload the files.
        blob_account: Name of the Azure blob storage account in which to look for
            the Blob storage container specified in ``blob_container``.
        path: Path within the Blob storage container to which to upload the file(s).
            Passed as the ``path`` argument to the BatchOutputFileBlobContainerDestination
            constructor. If None, upload to the root of the container. If ``file_pattern``
            contains wildcards, ``path`` gives the subdirectory within the container to
            upload them with their original filenames and extensions. If ``file_pattern``
            contains no wildcards, ``path`` is treated as the full file path including
            filename and extension (i.e. the file is renamed). See
            BatchOutputFileBlobContainerDestination for details.
        upload_condition: Condition under which to upload the file(s). Options are
            "taskCompletion" (always upload, the default), "taskFailure" (upload only
            for failed tasks), and "taskSuccess" (upload only for successful tasks).
            Passed as the ``upload_condition`` argument to BatchOutputFileUploadOptions.
        blob_endpoint_subdomain: Azure Blob endpoint subdomains and domains that follow
            the account name. If None (default), use this package's
            default_azure_blob_storage_endpoint_subdomain.
        compute_node_identity_reference: BatchNodeIdentityReference to use when
            constructing a BatchOutputFileBlobContainerDestination object for logging.
            If None (default), attempt to create compute node identity reference.
        **kwargs: Additional keyword arguments passed to the BatchOutputFile constructor.

    Returns:
        OutputFile: A BatchOutputFile object that can be used in constructing a
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
    logger.debug(f"Creating output file configuration for pattern: '{file_pattern}'")
    logger.debug(
        f"Target blob container: '{blob_container}' in account: '{blob_account}'"
    )
    logger.debug(f"Upload path: '{path}', upload condition: '{upload_condition}'")
    logger.debug(f"Blob endpoint subdomain: '{blob_endpoint_subdomain}'")

    if compute_node_identity_reference is None:
        logger.debug("No compute node identity reference provided, obtaining default")
        mgmt_compute_id = get_compute_node_identity_reference()
        compute_node_identity_reference = get_batch_compute_id(mgmt_compute_id)
        logger.debug("Successfully obtained default compute node identity reference")
    else:
        logger.debug("Using provided compute node identity reference")

    logger.debug(
        f"Validating compute node identity reference type: {type(compute_node_identity_reference)}"
    )
    if not isinstance(compute_node_identity_reference, BatchNodeIdentityReference):
        error_msg = (
            "compute_node_identity_reference "
            "must be an instance of "
            "BatchNodeIdentityReference. "
            f"Got {type(compute_node_identity_reference)}."
        )
        logger.debug(f"Type validation failed: {error_msg}")
        raise TypeError(error_msg)

    logger.debug("Compute node identity reference validation successful")

    container_url = construct_blob_container_endpoint(
        blob_container,
        blob_account,
        blob_endpoint_subdomain,
    )
    logger.debug(f"Constructed container URL: '{container_url}'")

    container = OutputFileBlobContainerDestination(
        container_url=container_url,
        path=path,
        identity_reference=compute_node_identity_reference,
    )
    logger.debug(f"Created BatchOutputFileBlobContainerDestination with path: '{path}'")

    destination = OutputFileDestination(container=container)
    logger.debug("Created BatchOutputFileDestination wrapper")

    upload_options = OutputFileUploadConfiguration(upload_condition=upload_condition)
    logger.debug(f"Created upload options with condition: '{upload_condition}'")

    output_file = OutputFile(
        file_pattern=file_pattern,
        destination=destination,
        upload_options=upload_options,
        **kwargs,
    )

    logger.debug(
        f"Successfully created BatchOutputFile for pattern '{file_pattern}' -> '{blob_container}/{path or ''}'"
    )

    return output_file


def get_task_config(
    task_id: str,
    base_call: str,
    container_settings: BatchTaskContainerSettings = None,
    user_identity: UserIdentity = None,
    log_blob_container: str = None,
    log_blob_account: str = None,
    log_subdir: str = None,
    log_file_pattern: str = "../std*.txt",
    log_upload_condition: str = "taskCompletion",
    log_compute_node_identity_reference: BatchNodeIdentityReference = None,
    output_files: list[OutputFile] | OutputFile = None,
    **kwargs,
) -> BatchTaskCreateOptions:
    """Create a batch task with a given base call and set of container settings.

    If the ``user_identity`` is not set, set it up automatically with sufficient
    permissions to read and write from mounted volumes.
    Requires azure-batch>=15.0.0.

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
            Passed as the ``upload_condition`` argument to BatchOutputFileUploadOptions.
        log_compute_node_identity_reference: BatchComputeNodeIdentityReference to use when
            constructing a BatchOutputFileBlobContainerDestination object for logging.
            If None (default), attempt to create one using the user-assigned identity.
            Ignored if ``log_blob_container`` is None.
        output_files: BatchOutputFile object or list of such objects specifying additional
            output files for the task beyond those auto-constructed for persisting logs
            to ``log_blob_container``. Passed along with those autogenerated BatchOutputFile
            objects as the ``output_files`` parameter to the BatchTaskAddParameter constructor.
        **kwargs: Additional keyword arguments passed to the BatchTaskAddParameter constructor.

    Returns:
        BatchTaskAddParameter: The task configuration object.

    Example:
        >>> from azure.batch.models import BatchTaskContainerSettings
        >>>
        >>> # Basic task without container
        >>> task = get_task_config(
        ...     task_id="my-task-001",
        ...     base_call="python /app/script.py --input data.txt"
        ... )
        >>>
        >>> # Task with container and logging
        >>> container_settings = BatchTaskContainerSettings(
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
    logger.debug(f"Creating task configuration for task ID: '{task_id}'")
    logger.debug(f"Base command line: '{base_call}'")

    if container_settings:
        logger.debug(
            f"Container settings provided: image='{container_settings.image_name}'"
        )
        if hasattr(container_settings, "registry") and container_settings.registry:
            logger.debug(
                f"Using private registry: {container_settings.registry.registry_server}"
            )
    else:
        logger.debug("No container settings provided, task will run on host")

    if user_identity is None:
        logger.debug(
            "No user identity provided, creating automatic admin user identity"
        )
        user_identity = UserIdentity(
            auto_user=AutoUserSpecification(
                scope=AutoUserScope.POOL,
                elevation_level=ElevationLevel.ADMIN,
            )
        )
        logger.debug(
            "Created automatic user identity with pool scope and admin elevation"
        )
    else:
        logger.debug("Using provided user identity")

    if output_files is None:
        output_files = []
        logger.debug("No output files provided, initializing empty list")
    else:
        logger.debug(
            f"Output files provided: {len(ensure_listlike(output_files))} files"
        )

    if log_blob_container is not None:
        logger.debug(
            f"Log blob container specified: '{log_blob_container}' in account '{log_blob_account}'"
        )
        logger.debug(
            f"Log configuration: subdir='{log_subdir}', pattern='{log_file_pattern}', condition='{log_upload_condition}'"
        )

        if log_subdir is None:
            log_subdir = ""
            logger.debug("No log subdirectory specified, using container root")

        log_path = Path(log_subdir, task_id).as_posix()
        logger.debug(f"Log files will be saved to path: '{log_path}'")

        log_output_files = output_task_files_to_blob(
            file_pattern=log_file_pattern,
            blob_container=log_blob_container,
            blob_account=log_blob_account,
            path=log_path,
            upload_condition=log_upload_condition,
            compute_node_identity_reference=log_compute_node_identity_reference,
        )
        logger.debug("Successfully created log output file configuration")
    else:
        log_output_files = []
        logger.debug(
            "No log blob container specified, task logs will not be persisted to blob storage"
        )

    total_output_files = ensure_listlike(output_files) + ensure_listlike(
        log_output_files
    )
    logger.debug(
        f"Total output files configured: {len(total_output_files)} ({len(ensure_listlike(output_files))} custom + {len(ensure_listlike(log_output_files))} log files)"
    )

    # Filter kwargs to only include valid BatchTaskCreateOptions parameters
    # Remove parameters that don't exist in 15.x API but may be passed from callers
    invalid_params = {"run_dependent_tasks_on_failure", "run_dependent_tasks_on_fail"}
    valid_kwargs = {k: v for k, v in kwargs.items() if k not in invalid_params}

    if valid_kwargs:
        logger.debug(
            f"Additional BatchTaskCreateOptions kwargs: {list(valid_kwargs.keys())}"
        )
    if invalid_params & set(kwargs.keys()):
        logger.debug(
            f"Filtering out unsupported parameters: {invalid_params & set(kwargs.keys())}"
        )

    task_config = BatchTaskCreateOptions(
        id=task_id,
        command_line=base_call,
        container_settings=container_settings,
        user_identity=user_identity,
        output_files=total_output_files,
        **valid_kwargs,
    )

    logger.debug(
        f"Successfully created BatchTaskAddParameter for task '{task_id}' with {len(total_output_files)} output files"
    )

    return task_config


def get_batch_compute_id(
    compute_id: batch_mgmt_models.ComputeNodeIdentityReference,
) -> BatchNodeIdentityReference:
    resource_id = getattr(compute_id, "resource_id", None)
    if not resource_id:
        raise ValueError(
            "compute_id must be an azure.mgmt.batch.models.ComputeNodeIdentityReference with a non-empty 'resource_id'."
        )
    return BatchNodeIdentityReference(resource_id=resource_id)
