import csv
import datetime
import logging
import os
import time
import uuid
from zoneinfo import ZoneInfo as zi

import azure.batch.models as batch_models
import griddler
import yaml
from azure.batch.models import (
    DependencyAction,
    ExitCodeMapping,
    ExitConditions,
    ExitOptions,
    JobAction,
    TaskConstraints,
)

logger = logging.getLogger(__name__)

AZ_MOUNT_DIR = "$AZ_BATCH_NODE_MOUNTS_DIR"
NO_EXIT_OPTIONS = ExitOptions(
    dependency_action=DependencyAction.satisfy, job_action=JobAction.none
)
NO_EXIT_CONDITIONS = ExitConditions(
    exit_codes=[
        ExitCodeMapping(code=0, exit_options=NO_EXIT_OPTIONS),
        ExitCodeMapping(code=1, exit_options=NO_EXIT_OPTIONS),
    ],
    pre_processing_error=NO_EXIT_OPTIONS,
    file_upload_error=NO_EXIT_OPTIONS,
    default=NO_EXIT_OPTIONS,
)
TERMMINATE_EXIT_OPTIONS = ExitOptions(
    dependency_action=DependencyAction.block, job_action=JobAction.none
)
TERMMINATE_EXIT_CONDITIONS = ExitConditions(
    exit_codes=[
        ExitCodeMapping(code=0, exit_options=NO_EXIT_OPTIONS),
        ExitCodeMapping(code=1, exit_options=TERMMINATE_EXIT_OPTIONS),
    ],
    pre_processing_error=TERMMINATE_EXIT_OPTIONS,
    file_upload_error=TERMMINATE_EXIT_OPTIONS,
    default=TERMMINATE_EXIT_OPTIONS,
)


def _generate_exit_conditions(run_dependent_tasks_on_fail: bool) -> ExitConditions:
    if run_dependent_tasks_on_fail:
        logger.debug("Configured to run dependent tasks on failure")
        exit_conditions = NO_EXIT_CONDITIONS
    else:
        logger.debug("Configured to block dependent tasks on failure")
        exit_conditions = TERMMINATE_EXIT_CONDITIONS
    return exit_conditions


def _generate_command_for_saving_logs(
    command_line: str,
    job_name: str,
    task_id: str,
    save_logs_rel_path: str,
    logs_folder: str = "logs",
) -> str:
    """Generate a command line that saves stdout and stderr to log files.

    Args:
        command_line (str): Original command line to execute.
        job_name (str): Name/ID of the job.
        task_id (str): ID of the task.
        save_logs_rel_path (str): Relative path where logs should be saved.
        logs_folder (str): Subfolder name within the save_logs_rel_path to store logs. Defaults to "logs".
    """
    t = datetime.datetime.now(zi("America/New_York"))
    s_time = t.strftime("%Y%m%d_%H%M%S")
    if not save_logs_rel_path.startswith("/"):
        save_logs_rel_path = "/" + save_logs_rel_path
    _folder = f"{save_logs_rel_path}/{logs_folder}/"
    sout = f"{_folder}/stdout_{job_name}_{task_id}_{s_time}.txt"
    logger.debug(f"Logs will be saved to: '{sout}'")
    return f"""/bin/bash -c "mkdir -p {_folder}; {command_line} 2>&1 | tee {sout}" """


def _generate_mount_string(mounts):
    mount_str = ""
    if mounts is not None:
        for mount in mounts:
            logger.debug("Adding mount to mount string.")
            mount_str = (
                mount_str
                + "--mount type=bind,source="
                + AZ_MOUNT_DIR
                + f"/{mount['source']},target=/{mount['target']} "
            )
    return mount_str


def _generate_task_dependencies(depends_on, depends_on_range):
    task_deps = None
    if depends_on is not None:
        depends_on = [depends_on] if isinstance(depends_on, str) else depends_on
        task_deps = batch_models.TaskDependencies(task_ids=depends_on)

    if depends_on_range is not None:
        task_deps = batch_models.TaskDependencies(
            task_id_ranges=[
                batch_models.TaskIdRange(
                    start=int(depends_on_range[0]),
                    end=int(depends_on_range[1]),
                )
            ]
        )
    return task_deps


def monitor_tasks(
    job_name: str,
    timeout: int,
    batch_client: object,
    download_task_output: bool = False,
) -> dict:
    """Monitor tasks running in an Azure Batch job until completion or timeout.

    Continuously monitors the progress of all tasks in a job, providing real-time
    updates on task completion status, successes, and failures. Monitors until all
    tasks complete or the specified timeout is reached.

    Args:
        job_name (str): Name/ID of the job to monitor. The job must exist and be active.
        timeout (int): Maximum time in minutes to monitor before timing out. If None,
            defaults to 480 minutes (8 hours).
        batch_client (object): Azure Batch service client instance for API calls.
        download_task_output (bool): Whether to download stdout and stderr from each
            completed task. If True, saves output files to a directory named
            "{job_name}_output". Defaults to False.

    Returns:
        dict: Dictionary containing monitoring results with the following keys:
            - completed (bool): Whether all tasks completed within the timeout
            - elapsed time (timedelta): Total time spent monitoring
            - terminate_reason (str | None): Reason for job termination if applicable
            - tasks complete (int): Number of tasks that completed
            - tasks remaining (int): Number of tasks still incomplete
            - success (int): Number of tasks that completed successfully
            - fail (int): Number of tasks that failed

    Raises:
        RuntimeError: If the job does not complete within the specified timeout period.

    Example:
        Monitor a job with default timeout:

            result = monitor_tasks("data-processing-job", None, batch_client)
            if result["completed"]:
                print(f"Job completed with {result['success']} successes")

        Monitor with custom timeout:

            result = monitor_tasks("quick-job", 30, batch_client)
            print(f"Elapsed time: {result['elapsed time']}")

    Note:
        This function prints real-time progress updates and blocks until completion
        or timeout. Task status is polled every 5 seconds. Progress is displayed
        as: "X completed; Y running; Z remaining; A successes; B failures"
    """
    # start monitoring
    logger.info(
        f"Starting to monitor tasks for job '{job_name}' with a timeout of {timeout} minutes."
    )

    start_time = datetime.datetime.now().replace(microsecond=0)
    if timeout is None:
        timeout = 480

    _timeout = datetime.timedelta(minutes=timeout)
    timeout_expiration = start_time + _timeout

    logger.debug(
        f"Job '{job_name}' monitoring started at {start_time}. Timeout at {timeout_expiration}."
    )
    logger.debug("-" * 20)

    # count tasks and print to user the starting value
    # as tasks complete, print which complete
    # print remaining number of tasks
    logger.debug(f"Retrieving initial task list for job '{job_name}'")
    tasks = list(batch_client.task.list(job_name))

    # get total tasks
    total_tasks = len([task for task in tasks])
    logger.info(f"Total tasks to monitor: {total_tasks}")

    # pool setup and status
    # initialize job complete status
    completed = False
    previously_completed = []
    completions, incompletions, running, successes, failures = 0, 0, 0, 0, 0

    logger.debug(f"Getting initial job state for '{job_name}'")
    job = batch_client.job.get(job_name)
    logger.debug(f"Initial job state: {job.as_dict()['state']}")

    polling_count = 0
    while job.as_dict()["state"] != "completed" and not completed:
        if datetime.datetime.now() < timeout_expiration:
            polling_count += 1
            logger.debug(f"Polling iteration {polling_count}: sleeping 5 seconds")
            time.sleep(5)  # Polling interval

            logger.debug(f"Retrieving current task list (poll #{polling_count})")
            tasks = list(batch_client.task.list(job_name))

            incomplete_tasks = [
                task for task in tasks if task.state != batch_models.TaskState.completed
            ]
            incompletions = len(incomplete_tasks)

            completed_tasks = [
                task for task in tasks if task.state == batch_models.TaskState.completed
            ]
            completions = len(completed_tasks)

            running_tasks = [
                task for task in tasks if task.state == batch_models.TaskState.running
            ]
            running = len(running_tasks)

            # initialize the counts
            failures = 0
            successes = 0

            for task in completed_tasks:
                if task.as_dict()["execution_info"]["result"] == "failure":
                    failures += 1
                elif task.as_dict()["execution_info"]["result"] == "success":
                    successes += 1
                if download_task_output:
                    os.makedirs(f"{job_name}_output", exist_ok=True)
                    if task.id not in previously_completed:
                        stdout = batch_client.file.get_from_task(
                            job_name, task.id, "stdout.txt"
                        )
                        stderr = batch_client.file.get_from_task(
                            job_name, task.id, "stderr.txt"
                        )
                        with open(
                            os.path.join(f"{job_name}_output", f"{task.id}_stdout.txt"),
                            "w",
                        ) as f:
                            for data in stdout:
                                f.write(data.decode("utf-8"))
                        with open(
                            os.path.join(f"{job_name}_output", f"{task.id}_stderr.txt"),
                            "w",
                        ) as f:
                            for data in stderr:
                                f.write(data.decode("utf-8"))
                        print(f"\nOutput saved from task {task.id}")
                        previously_completed.append(task.id)
            _runtime = str(datetime.datetime.now() - start_time).split(".")[0]
            print(
                "monitor runtime:",
                _runtime,
                "...",
                completions,
                "completed;",
                running,
                "running;",
                incompletions,
                "remaining;",
                successes,
                "successes;",
                failures,
                "failures",
                end="\r",
            )
            logger.debug(
                f"{completions} completed; {running} running; {incompletions} remaining; {successes} successes; {failures} failures"
            )

            if not incomplete_tasks:
                logger.info("\nAll tasks completed.")
                logger.info(
                    f"Job '{job_name}' completed: {successes} successes, {failures} failures out of {completions} tasks."
                )
                logger.debug(
                    f"Monitoring completed after {polling_count} polling iterations"
                )
                completed = True
                break
            job = batch_client.job.get(job_name)
        else:
            logger.warning(f"Monitoring timeout reached after {timeout} minutes")
            logger.debug(
                f"Final status: {completions} completed, {incompletions} remaining"
            )

    end_time = datetime.datetime.now().replace(microsecond=0)

    if completed:
        logger.info(
            "All tasks have reached 'Completed' state within the timeout period."
        )
        logger.info(f"{successes} task(s) succeeded, {failures} failed.")
    else:
        logger.warning("Monitoring stopped due to timeout - not all tasks completed")
        logger.info(
            f"Job '{job_name}' monitoring timed out after {timeout} minutes. {completions} completed, {incompletions} remaining."
        )

    # get terminate reason
    logger.debug("Checking for job termination reason")
    if "terminate_reason" in job.as_dict()["execution_info"].keys():
        terminate_reason = job.as_dict()["execution_info"]["terminate_reason"]
        logger.debug(f"Job terminate reason: {terminate_reason}")
    else:
        terminate_reason = None
        logger.debug("No job termination reason found")

    runtime = end_time - start_time
    logger.info(f"Monitoring ended: {end_time}. Total elapsed time: {runtime}.")
    print("\n")
    print("-" * 50)
    return {
        "completed": completed,
        "elapsed time": runtime,
        "terminate_reason": terminate_reason,
        "tasks complete": completions,
        "tasks remaining": incompletions,
        "success": successes,
        "fail": failures,
    }


def download_job_stats(
    job_name: str, batch_service_client: object, file_name: str | None = None
) -> None:
    """Download comprehensive statistics for all tasks in a job to a CSV file.

    Retrieves detailed execution statistics for every task in the specified job
    and saves them to a pipe-delimited CSV file. Includes timing information,
    exit codes, node details, and command information.

    Args:
        job_name (str): Name/ID of the job to download statistics for. The job
            must exist and have completed tasks.
        batch_service_client (object): Azure Batch service client instance for API calls.
        file_name (str, optional): Base name for the output CSV file (without extension).
            If None, defaults to "{job_name}-stats". The .csv extension is added automatically.

    Example:
        Download stats with default filename:

            download_job_stats("data-processing-job", batch_client)
            # Creates: data-processing-job-stats.csv

        Download with custom filename:

            download_job_stats("analysis-job", batch_client, "analysis_results")
            # Creates: analysis_results.csv

    Note:
        The CSV file contains the following columns separated by pipe (|) characters:
        - task_id: Unique identifier for the task
        - command: First part of the command line (before first -)
        - creation: Task creation timestamp
        - start: Task start time (YYYY-MM-DD HH:MM:SS)
        - end: Task end time (YYYY-MM-DD HH:MM:SS)
        - runtime: Total execution duration
        - exit_code: Process exit code
        - pool: Pool ID where the task ran
        - node_id: Compute node ID where the task executed

        The file is created in the current working directory. Tasks that haven't
        completed may not have all timing information available.
    """
    logger.debug(f"Downloading job statistics for job: {job_name}")
    if file_name is None:
        file_name = f"{job_name}-stats"
        logger.debug(f"Using default filename: {file_name}")
    else:
        logger.debug(f"Using custom filename: {file_name}")

    logger.debug("Retrieving task list from batch service")
    r = batch_service_client.task.list(
        job_id=job_name,
    )
    logger.debug("Task list retrieved successfully")

    fields = [
        "task_id",
        "command",
        "creation",
        "start",
        "end",
        "runtime",
        "exit_code",
        "pool",
        "node_id",
    ]
    with open(rf"{file_name}.csv", "w") as f:
        logger.debug(f"initializing {file_name}.csv.")
        writer = csv.writer(f, delimiter="|")
        writer.writerow(fields)
    for item in r:
        st = item.execution_info.start_time
        et = item.execution_info.end_time
        rt = et - st
        id = item.id
        creation = item.creation_time
        start = item.execution_info.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end = item.execution_info.end_time.strftime("%Y-%m-%d %H:%M:%S")
        exit_code = item.execution_info.exit_code
        node_id = item.node_info.node_id
        cli = item.command_line.split(" -")[0]
        pool = item.node_info.pool_id
        fields = [id, cli, creation, start, end, rt, exit_code, pool, node_id]
        with open(rf"{file_name}.csv", "a") as f:
            writer = csv.writer(f, delimiter="|")
            writer.writerow(fields)
            logger.debug(f"Wrote task {item.id} statistics to CSV")

    logger.debug(f"Job statistics download completed. File saved as: {file_name}.csv")
    print(f"Downloaded job statistics report to {file_name}.csv.")


def check_job_exists(job_name: str, batch_client: object):
    """Check whether a job exists in the Azure Batch account.

    Verifies if a job with the specified name exists in the Batch account by
    searching through all available jobs.

    Args:
        job_name (str): Name/ID of the job to check for existence.
        batch_client (object): Azure Batch service client instance for API calls.

    Returns:
        bool: True if the job exists, False otherwise.

    Example:
        Check if a job exists before monitoring:

            if check_job_exists("my-job", batch_client):
                print("Job exists, starting monitoring...")
                monitor_tasks("my-job", 60, batch_client)
            else:
                print("Job not found")

        Verify job creation:

            create_job("new-job", pool_name, batch_client)
            if check_job_exists("new-job", batch_client):
                print("Job created successfully")

    Note:
        This function searches through all jobs in the account, so it may be
        slower for accounts with many jobs. The check is case-sensitive.
    """
    job_list = []
    for job in batch_client.job.list():
        job_list.append(job.id)

    if job_name in job_list:
        logger.debug(f"job {job_name} exists.")
        return True
    else:
        logger.debug(f"job {job_name} does not exist.")
        return False


def get_completed_tasks(job_name: str, batch_client: object):
    """Get the count of completed tasks and total tasks for a specified job.

    Retrieves all tasks for the given job and counts how many have reached the
    completed state versus the total number of tasks in the job.

    Args:
        job_name (str): Name/ID of the job to analyze. The job must exist.
        batch_client (object): Azure Batch service client instance for API calls.

    Returns:
        dict: Dictionary containing task completion statistics with keys:
            - "completed tasks" (int): Number of tasks in completed state
            - "total tasks" (int): Total number of tasks in the job

    Example:
        Check job progress:

            stats = get_completed_tasks("data-processing-job", batch_client)
            completed = stats["completed tasks"]
            total = stats["total tasks"]
            print(f"Progress: {completed}/{total} tasks completed")

        Calculate completion percentage:

            stats = get_completed_tasks("analysis-job", batch_client)
            if stats["total tasks"] > 0:
                percentage = (stats["completed tasks"] / stats["total tasks"]) * 100
                print(f"Job is {percentage:.1f}% complete")

    Note:
        A task is considered completed when it reaches the TaskState.completed state,
        regardless of whether it succeeded or failed. Use monitor_tasks() for more
        detailed success/failure information.
    """
    logger.debug("Pulling in task information.")
    tasks = [task for task in batch_client.task.list(job_name)]
    total_tasks = len(tasks)

    completed_tasks = [
        task for task in tasks if task.state == batch_models.TaskState.completed
    ]
    num_c_tasks = len(completed_tasks)

    return {"completed tasks": num_c_tasks, "total tasks": total_tasks}


def check_job_complete(job_name: str, batch_client: object) -> bool:
    """Check if a job has completed all its tasks.

    Determines whether a job has reached a completed state by checking the job's
    current state in Azure Batch.

    Args:
        job_name (str): Name/ID of the job to check. The job must exist.
        batch_client (object): Azure Batch service client instance for API calls.

    Returns:
        bool: True if the job is in completed state, False otherwise.

    Example:
        Check if a job is complete before downloading results:

            if check_job_complete("data-processing-job", batch_client):
                print("Job completed, downloading results...")
                download_job_stats("data-processing-job", batch_client)
            else:
                print("Job still running")

        Wait for job completion:

            while not check_job_complete("my-job", batch_client):
                time.sleep(30)  # Wait 30 seconds before checking again
            print("Job finished!")

    Note:
        A completed job may still have failed tasks. Use get_completed_tasks()
        or monitor_tasks() to get detailed success/failure information.
    """
    logger.debug(f"Checking completion status for job: {job_name}")
    job = batch_client.job.get(job_name)
    is_complete = job.state == batch_models.JobState.completed
    logger.debug(
        f"Job {job_name} completion status: {is_complete} (state: {job.state})"
    )
    return is_complete


def get_job_state(job_name: str, batch_client: object) -> str:
    """Get the current state of a job.

    Retrieves the current execution state of the specified job from Azure Batch.
    Job states include active, completed, completed, terminated, etc.

    Args:
        job_name (str): Name/ID of the job to check. The job must exist.
        batch_client (object): Azure Batch service client instance for API calls.

    Returns:
        str: Current state of the job (e.g., "active", "completed", "terminated").

    Example:
        Check job state and take action:

            state = get_job_state("data-processing-job", batch_client)
            print(f"Job state: {state}")

            if state == "active":
                print("Job is running")
            elif state == "completed":
                print("Job completed successfully")
            elif state == "terminated":
                print("Job was terminated")

        Monitor state changes:

            states = []
            for i in range(5):
                state = get_job_state("my-job", batch_client)
                states.append(state)
                time.sleep(60)
            print(f"State progression: {states}")

    Note:
        Possible job states include: active, completed, completed, terminating,
        terminated. The exact states depend on the Azure Batch service version.
    """
    logger.debug(f"Retrieving state for job: {job_name}")
    job = batch_client.job.get(job_name)
    state = str(job.state)
    logger.debug(f"Job {job_name} current state: {state}")
    return state


def delete_pool(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> None:
    """Delete an Azure Batch pool and all its compute nodes.

    Initiates the deletion of a pool from the Azure Batch account. This operation
    stops all running tasks on the pool's nodes and deallocates all compute resources.
    The deletion is performed asynchronously.

    Args:
        resource_group_name (str): Name of the Azure resource group containing the
            Batch account.
        account_name (str): Name of the Azure Batch account containing the pool.
        pool_name (str): Name of the pool to delete. The pool must exist.
        batch_mgmt_client (object): Azure Batch management client instance for API calls.

    Returns:
        LROPoller: Long-running operation poller object that can be used to track
            the deletion progress or wait for completion.

    Example:
        Delete a pool and wait for completion:

            poller = delete_pool(
                "my-resource-group",
                "my-batch-account",
                "old-pool",
                batch_mgmt_client
            )
            poller.wait()  # Wait for deletion to complete
            print("Pool deletion finished")

        Delete a pool without waiting:

            delete_pool(
                "my-resource-group",
                "my-batch-account",
                "temp-pool",
                batch_mgmt_client
            )
            print("Pool deletion initiated")

    Warning:
        This operation is irreversible and will terminate any running tasks on the pool.
        Ensure all important work is complete before deleting the pool. Pool deletion
        may take several minutes to complete depending on the number of nodes.

    Note:
        The function returns immediately after initiating the deletion. Use the returned
        poller object to check status or wait for completion if needed.
    """
    logger.debug(f"Attempting to delete {pool_name}...")
    poller = batch_mgmt_client.pool.begin_delete(
        resource_group_name=resource_group_name,
        account_name=account_name,
        pool_name=pool_name,
    )
    logger.info(f"Pool deletion initiated for '{pool_name}'.")
    return poller


def get_args_from_yaml(file_path: str) -> list[str]:
    """Parse a YAML file and generate command-line argument strings for each parameter set.

    Reads a YAML file describing parameter sets (e.g., for grid search or batch jobs),
    parses it using griddler, and returns a list of command-line argument strings for
    each parameter set. Handles both regular arguments and flag arguments.

    Args:
        file_path (str): Path to the YAML file describing the parameter grid or sets.

    Returns:
        list[str]: List of command-line argument strings, one for each parameter set.

    Example:
        Given a YAML file with parameter sets:

            learning_rate: [0.01, 0.1]
            batch_size: [32, 64]
            use_dropout(flag): ["", "--use-dropout"]

        The function will return a list of strings like:
            [" --learning_rate 0.01 --batch_size 32", ...]

        Usage:

            arg_list = get_args_from_yaml("params.yaml")
            for args in arg_list:
                print(f"python train.py {args}")

    Note:
        Flag arguments are handled by appending the flag only if the value is not empty.
        This function is typically used to generate task commands for batch jobs.
    """
    logger.debug(f"Parsing YAML file for arguments: {file_path}")
    with open(file_path) as f:
        raw_griddle = yaml.safe_load(f)
    logger.debug("YAML file loaded successfully")

    griddle = griddler.parse(raw_griddle)
    parameter_sets = griddle.to_dicts()
    logger.debug(f"Generated {len(parameter_sets)} parameter combinations")

    output = []
    for i, param_set in enumerate(parameter_sets):
        full_cmd = ""
        logger.debug(f"Processing parameter set {i + 1}: {param_set}")
        for key, value in param_set.items():
            if key.endswith("(flag)"):
                if value != "":
                    full_cmd += f""" --{key.split("(flag)")[0]}"""
                    logger.debug(f"Added flag: --{key.split('(flag)')[0]}")
            else:
                full_cmd += f" --{key} {value}"
                logger.debug(f"Added parameter: --{key} {value}")
        output.append(full_cmd)
        logger.debug(f"Generated command arguments: {full_cmd}")

    logger.debug(f"Total argument sets generated: {len(output)}")
    return output


def get_tasks_from_yaml(base_cmd: str, file_path: str) -> list[str]:
    """Generate full task command strings from a base command and a YAML parameter file.

    Combines a base command (e.g., "python train.py") with each set of arguments parsed
    from a YAML file to produce a list of full command strings for batch tasks.

    Args:
        base_cmd (str): The base command to prepend to each argument set (e.g., "python train.py").
        file_path (str): Path to the YAML file describing the parameter grid or sets.

    Returns:
        list[str]: List of full command strings, one for each parameter set.

    Example:
        Given a base command and YAML file:

            base_cmd = "python train.py"
            file_path = "params.yaml"

        The function will return a list like:
            ["python train.py --learning_rate 0.01 --batch_size 32", ...]

        Usage:

            cmds = get_tasks_from_yaml("python train.py", "params.yaml")
            for cmd in cmds:
                print(cmd)

    Note:
        This function is useful for generating Azure Batch task commands from a grid
        search or parameter sweep defined in YAML.
    """
    logger.debug(f"Generating task commands with base command: '{base_cmd}'")
    logger.debug(f"Using YAML file: {file_path}")

    cmds = []
    arg_list = get_args_from_yaml(file_path)
    logger.debug(f"Generated {len(arg_list)} argument combinations")

    for i, args in enumerate(arg_list):
        full_cmd = f"{base_cmd} {args}"
        cmds.append(full_cmd)
        logger.debug(f"Task {i + 1}: {full_cmd}")

    logger.debug(f"Total task commands generated: {len(cmds)}")
    return cmds


def get_full_container_image_name(
    container_name: str,
    registry: str = "azure",
    acr_name: str = None,
    github_org: str = None,
) -> str:
    """Return the full container image name for Azure Batch pool usage.

    Constructs the full image name for Azure Container Registry, Docker Hub, or GitHub Container Registry.

    Args:
        container_name (str): The name of the container image (e.g., "myimage:latest").
        registry (str): Which registry to use: "azure", "docker", or "github". Defaults to "azure".
        acr_name (str, optional): Azure Container Registry name (required if registry is "azure").
        github_org (str, optional): GitHub organization name (required if registry is "github").

    Returns:
        str: The full image name to use in Azure Batch pool.
    """
    logger.debug(f"Constructing full container image name for: {container_name}")
    logger.debug(f"Registry type: {registry}")

    if registry.lower() == "azure":
        if not acr_name:
            logger.error("ACR name not provided for Azure Container Registry")
            raise ValueError("acr_name must be provided for Azure Container Registry.")
        full_name = f"{acr_name}.azurecr.io/{container_name}"
        logger.debug(f"Azure registry image name: {full_name}")
        return full_name
    elif registry.lower().startswith("docker"):
        full_name = f"{container_name}"
        logger.debug(f"Docker Hub image name: {full_name}")
        return full_name
    elif registry.lower() == "github":
        if not github_org:
            logger.error(
                "GitHub organization not provided for GitHub Container Registry"
            )
            raise ValueError(
                "github_org must be provided for GitHub Container Registry."
            )
        full_name = f"ghcr.io/{github_org.lower()}/{container_name}"
        logger.debug(f"GitHub registry image name: {full_name}")
        return full_name
    else:
        logger.error(f"Unsupported registry type: {registry}")
        raise ValueError(
            "Unsupported registry type. Use 'azure', 'docker', or 'github'."
        )


class Task:
    def __init__(self, cmd: str, id: str | None = None, dep: str | list | None = None):
        """Initialize a Task object for Azure Batch job execution.

        Creates a task with a command, optional ID, and optional dependencies on other tasks.
        Used to build task graphs for batch processing workflows.

        Args:
            cmd (str): Command to be used with Azure Batch task execution.
            id (str, optional): Optional ID to identify tasks. If None, generates a UUID.
                Defaults to None.
            dep (str | list, optional): Task object(s) or task ID(s) this task depends on.
                Can be a single task/ID or list of tasks/IDs. Defaults to None.

        Example:
            Create a simple task:

                task1 = Task("python process_data.py")

            Create a task with dependencies:

                task2 = Task("python analyze.py", dep=[task1])

            Create a task with custom ID:

                task3 = Task("python report.py", id="generate-report")
        """
        self.cmd = cmd
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        if isinstance(dep, list):
            self.deps = dep
        elif dep is None:
            self.deps = []
        else:
            self.deps = [dep]

    def __repr__(self):
        return self.id

    def before(self, other):
        """Set that this task needs to occur before another task.

        Establishes a dependency relationship where this task must complete before
        the other task(s) can start execution.

        Args:
            other (Task | list[Task]): Task object(s) that should run after this task.

        Example:
            Set task order:

                t1 = Task("some command")
                t2 = Task("another command")
                t1.before(t2)  # t1 must occur before t2

            Set multiple downstream tasks:

                t1.before([t2, t3])  # t1 before both t2 and t3
        """
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if self not in task.deps:
                task.deps.append(self)

    def after(self, other):
        """Set that this task needs to occur after another task.

        Establishes a dependency relationship where this task waits for the
        other task(s) to complete before starting execution.

        Args:
            other (Task | list[Task]): Task object(s) that must complete before this task.

        Example:
            Set task order:

                t1 = Task("some command")
                t2 = Task("another command")
                t1.after(t2)  # t1 must occur after t2

            Set multiple upstream tasks:

                t3.after([t1, t2])  # t3 runs after both t1 and t2
        """
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if task not in self.deps:
                self.deps.append(task)

    def set_downstream(self, other):
        """Set the downstream task from the current task.

        Convenience method equivalent to calling before(). Sets the specified task(s)
        to run after this task completes, similar to using the >> operator concept.

        Args:
            other (Task | list[Task]): Task object(s) to set as downstream dependencies.

        Example:
            Set downstream task:

                t1 = Task("some command")
                t2 = Task("another command")
                t1.set_downstream(t2)  # equivalent to t1.before(t2)
        """
        self.before(other)

    def set_upstream(self, other):
        """Set the upstream task from the current task.

        Convenience method equivalent to calling after(). Sets the specified task(s)
        to run before this task starts, similar to using the << operator concept.

        Args:
            other (Task | list[Task]): Task object(s) to set as upstream dependencies.

        Example:
            Set upstream task:

                t1 = Task("some command")
                t2 = Task("another command")
                t1.set_upstream(t2)  # equivalent to t1.after(t2)
        """
        self.after(other)


def get_rel_mnt_path(
    blob_name: str,
    pool_name: str,
    resource_group_name: str,
    account_name: str,
    batch_mgmt_client: object,
):
    """Get the relative mount path for a blob container in a specified pool.

    Retrieves the relative mount path for a specific blob container that is
    mounted to an Azure Batch pool. This path is used to access the mounted
    storage from within tasks running on the pool.

    Args:
        blob_name (str): Name of the blob container to find the mount path for.
        pool_name (str): Name of the pool where the blob is mounted.
        resource_group_name (str): Name of the Azure resource group containing the pool.
        account_name (str): Name of the Azure Batch account containing the pool.
        batch_mgmt_client (object): Instance of BatchManagementClient for API calls.

    Returns:
        str: The relative mount path for the blob container, or "ERROR!" if the
            blob container is not found mounted to the pool or if pool information
            cannot be retrieved.

    Example:
        Get mount path for a data container:

            mount_path = get_rel_mnt_path(
                "data-container",
                "compute-pool",
                "my-resource-group",
                "my-batch-account",
                batch_mgmt_client
            )
            if mount_path != "ERROR!":
                print(f"Data accessible at: {mount_path}")

    Note:
        This function searches through all mount configurations in the pool to find
        the specified blob container. If the container is not mounted or the pool
        cannot be accessed, it returns "ERROR!".
    """
    logger.debug(
        f"Getting relative mount path for blob '{blob_name}' in pool '{pool_name}'"
    )
    try:
        logger.debug(
            f"Retrieving pool info for resource group '{resource_group_name}', account '{account_name}'"
        )
        pool_info = get_pool_full_info(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_name,
            batch_mgmt_client=batch_mgmt_client,
        )
    except Exception:
        logger.error("could not retrieve pool information.")
        logger.info(
            f"Could not retrieve pool info for pool '{pool_name}' in resource group '{resource_group_name}'."
        )
        return "ERROR!"

    mc = pool_info.as_dict()["mount_configuration"]
    logger.debug(f"Searching through {len(mc)} mount configurations")

    for m in mc:
        if m["azure_blob_file_system_configuration"]["container_name"] == blob_name:
            rel_mnt_path = m["azure_blob_file_system_configuration"][
                "relative_mount_path"
            ]
            logger.debug(f"Found mount path '{rel_mnt_path}' for blob '{blob_name}'")
            return rel_mnt_path
    logger.error(f"could not find blob {blob_name} mounted to pool.")
    logger.info(f"Could not find blob '{blob_name}' mounted to pool '{pool_name}'.")
    print(f"could not find blob {blob_name} mounted to pool.")
    return "ERROR!"


def get_pool_full_info(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> dict:
    """Get the full information of a specified Azure Batch pool.

    Retrieves comprehensive configuration and status information for the specified
    pool from the Azure Batch service, including node configuration, scaling settings,
    mount points, and other pool properties.

    Args:
        resource_group_name (str): Name of the Azure resource group containing the
            Batch account.
        account_name (str): Name of the Azure Batch account containing the pool.
        pool_name (str): Name of the pool to retrieve information for. The pool must exist.
        batch_mgmt_client (object): Instance of BatchManagementClient for API calls.

    Returns:
        dict: Dictionary containing complete pool configuration and status information
            including node settings, scaling configuration, mount points, and other
            pool properties.

    Example:
        Get pool information:

            pool_info = get_pool_full_info(
                "my-resource-group",
                "my-batch-account",
                "compute-pool",
                batch_mgmt_client
            )
            print(f"Pool state: {pool_info['allocation_state']}")

    Note:
        This function provides detailed pool information that can be used for
        monitoring, configuration validation, and debugging pool issues.
    """
    logger.debug("Pulling pool info.")
    result = batch_mgmt_client.pool.get(resource_group_name, account_name, pool_name)
    return result


def get_pool_mounts(
    pool_name: str,
    resource_group_name: str,
    account_name: str,
    batch_mgmt_client: object,
):
    """Get all mount configurations for a specified Azure Batch pool.

    Retrieves information about all blob containers that are mounted to the
    specified pool, including the container names and their relative mount paths.

    Args:
        pool_name (str): Name of the pool to get mount information for.
        resource_group_name (str): Name of the Azure resource group containing the pool.
        account_name (str): Name of the Azure Batch account containing the pool.
        batch_mgmt_client (object): Instance of BatchManagementClient for API calls.

    Returns:
        list[tuple[str, str]] | None: List of tuples containing (container_name, relative_mount_path)
            for each mounted blob container, or None if pool information cannot be retrieved.

    Example:
        Get all mounts for a pool:

            mounts = get_pool_mounts(
                "compute-pool",
                "my-resource-group",
                "my-batch-account",
                batch_mgmt_client
            )
            if mounts:
                for container, path in mounts:
                    print(f"Container '{container}' mounted at '{path}'")

    Note:
        If the pool information cannot be retrieved due to access issues or if the
        pool does not exist, this function returns None and logs an error.
    """
    logger.debug(
        f"Getting mount configurations for pool '{pool_name}' in resource group '{resource_group_name}'"
    )
    try:
        pool_info = get_pool_full_info(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_name,
            batch_mgmt_client=batch_mgmt_client,
        )
    except Exception:
        logger.error("could not retrieve pool information.")
        logger.info(
            f"Could not retrieve pool info for pool '{pool_name}' in resource group '{resource_group_name}'."
        )
        print(f"could not retrieve pool info for {pool_name}.")
        return None

    mounts = []
    try:
        mc = pool_info.as_dict()["mount_configuration"]
        logger.debug(f"Processing {len(mc)} mount configurations")

        for m in mc:
            mount_info = {
                "source": m["azure_blob_file_system_configuration"][
                    "relative_mount_path"
                ],
                "target": m["azure_blob_file_system_configuration"][
                    "relative_mount_path"
                ],
            }
            mounts.append(mount_info)
            logger.debug(f"Added mount: {mount_info}")

        logger.debug(f"Successfully retrieved {len(mounts)} mount configurations")
    except Exception as e:
        mounts = None
        logger.info(f"Could not find mounts for pool '{pool_name}': {e}")

    return mounts


def add_task(
    job_name: str,
    task_id_base: str,
    command_line: str,
    save_logs_rel_path: str | None = None,
    logs_folder: str = "stdout_stderr",
    name_suffix: str = "",
    mounts: list[dict] | None = None,
    depends_on: str | list[str] | None = None,
    depends_on_range: tuple | None = None,
    run_dependent_tasks_on_fail: bool = False,
    batch_client: object | None = None,
    full_container_name: str | None = None,
    task_id_max: int = 0,
    task_id_ints: bool = False,
    timeout: int | None = None,
) -> str:
    """Add a task to an Azure Batch job with comprehensive configuration options.

    Creates and adds a task to the specified job with support for dependencies,
    container execution, mount configurations, log saving, and timeout constraints.
    The task runs in a container environment with configurable execution settings.

    Args:
        job_name (str): Name/ID of the job to add the task to. The job must exist.
        task_id_base (str): Base string for generating the task ID (used with name_suffix
            and task_id_max unless task_id_ints is True).
        command_line (str | list[str]): Command to execute in the task. Can be a string
            or list of strings that will be joined.
        save_logs_rel_path (str, optional): Relative path where stdout/stderr logs should
            be saved. If None, logs are not saved to blob storage.
        logs_folder (str): Name of the folder to create for saving logs. Defaults to
            "stdout_stderr".
        name_suffix (str): Suffix to append to the task ID for uniqueness. Defaults to "".
        mounts (list[dict], optional): List of mount configurations as dicts
            of {"source": <container_name>, "target": <relative_mount_path>).
        depends_on (str | list[str], optional): Task ID(s) that this task depends on.
            Task will not start until dependencies complete successfully.
        depends_on_range (tuple[int, int], optional): Range of task IDs (start, end) that
            this task depends on. Alternative to depends_on.
        run_dependent_tasks_on_fail (bool): If True, dependent tasks will run even if
            this task fails. If False, failure blocks dependents. Defaults to False.
        batch_client (object): Azure Batch service client instance for API calls.
        full_container_name (str, optional): Full container image name to use for the task.
        task_id_max (int): Current maximum task ID number for generating unique task IDs.
            Defaults to 0.
        task_id_ints (bool): If True, use integer task IDs instead of string-based IDs.
            Defaults to False.
        timeout (int, optional): Maximum wall clock time for the task in minutes. If None,
            no timeout is set.

    Returns:
        str: The generated task ID for the newly created task.

    Example:
        Add a simple task:

            task_id = add_task(
                job_name="data-processing",
                task_id_base="process",
                command_line="python process_data.py",
                batch_client=batch_client,
                full_container_name="myregistry.azurecr.io/data-processor:latest"
            )

        Add a task with dependencies and log saving:

            task_id = add_task(
                job_name="analysis-job",
                task_id_base="analyze",
                command_line=["python", "analyze.py", "--input", "/data/input.csv"],
                depends_on=["preprocess-task-1", "preprocess-task-2"],
                save_logs_rel_path="/logs",
                timeout=120,
                batch_client=batch_client,
                full_container_name="myregistry.azurecr.io/analyzer:v1.0"
            )

    Note:
        The task runs with admin privileges in the pool and is configured with
        appropriate exit conditions based on the run_dependent_tasks_on_fail setting.
        Mount configurations allow access to blob storage from within the container.
        Log saving creates timestamped files in the specified blob storage location.
    """
    logger.debug(
        f"Adding task to job '{job_name}' with base ID '{task_id_base}', suffix '{name_suffix}'"
    )
    logger.debug(
        f"Task parameters: timeout={timeout}, mounts={len(mounts) if mounts else 0}, container={full_container_name}"
    )

    # convert command line to string if given as list
    if isinstance(command_line, list):
        cmd_str = " ".join(command_line)
        logger.debug(f"Docker command converted from list to string: '{cmd_str}'")
    else:
        cmd_str = command_line
        logger.debug(f"Using command string directly: '{cmd_str}'")

    # Add a task to the job
    logger.debug("Creating user identity with admin privileges")
    user_identity = batch_models.UserIdentity(
        auto_user=batch_models.AutoUserSpecification(
            scope=batch_models.AutoUserScope.pool,
            elevation_level=batch_models.ElevationLevel.admin,
        )
    )

    task_deps = _generate_task_dependencies(depends_on, depends_on_range)

    logger.debug("Configuring exit conditions and dependency behavior")
    exit_conditions = _generate_exit_conditions(run_dependent_tasks_on_fail)

    logger.debug("Creating mount configuration string.")
    mount_str = _generate_mount_string(mounts)

    if task_id_ints:
        task_id = str(task_id_max + 1)
        logger.debug(f"Generated integer-based task ID: '{task_id}'")
    else:
        task_id = f"{task_id_base}-{name_suffix}-{str(task_id_max + 1)}"
        logger.debug(f"Generated string-based task ID: '{task_id}'")

    if save_logs_rel_path is not None:
        if save_logs_rel_path == "ERROR!":
            logger.warning("could not find rel path")
            print(
                "could not find rel path. Stdout and stderr will not be saved to blob storage."
            )
            full_cmd = cmd_str
        else:
            logger.debug(
                f"Configuring log saving to path: '{save_logs_rel_path}' in folder: '{logs_folder}'"
            )
            full_cmd = _generate_command_for_saving_logs(
                command_line=cmd_str,
                job_name=job_name,
                task_id=task_id,
                save_logs_rel_path=save_logs_rel_path,
                logs_folder=logs_folder,
            )
            logger.debug(f"Modified command for log capture: '{full_cmd}'")
    else:
        logger.debug("No log saving configured - using command as-is")
        full_cmd = cmd_str

    # add constraints
    if timeout is None:
        _to = None
        logger.debug("No timeout constraints configured")
    else:
        _to = datetime.timedelta(minutes=timeout)
        logger.debug(f"Task timeout constraint set to {timeout} minutes")

    task_constraints = TaskConstraints(max_wall_clock_time=_to)
    command_line = full_cmd
    logger.debug(f"Final command line for task {task_id}: '{command_line}'")

    # if full container name is none, pull info from job
    container_name = f"{job_name}_{str(task_id_max + 1)}"
    container_run_options = f"--name={container_name} --rm " + mount_str
    logger.debug(
        f"Container settings: image='{full_container_name}', run_options='{container_run_options}'"
    )

    # Create the task parameter
    logger.debug("Creating task parameter object")
    task_param = batch_models.TaskAddParameter(
        id=task_id,
        command_line=command_line,
        container_settings=batch_models.TaskContainerSettings(
            image_name=full_container_name,
            container_run_options=container_run_options,
            working_directory="containerImageDefault",
        ),
        user_identity=user_identity,
        constraints=task_constraints,
        depends_on=task_deps,
        run_dependent_tasks_on_failure=run_dependent_tasks_on_fail,
        exit_conditions=exit_conditions,
    )

    # Add the task to the job
    logger.debug(f"Submitting task '{task_id}' to Azure Batch service")
    batch_client.task.add(job_id=job_name, task=task_param)
    logger.debug(f"Task '{task_id}' successfully added to job '{job_name}'")
    return task_id


def add_task_collection(
    job_name: str,
    task_id_base: str,
    tasks: list[dict],
    name_suffix: str = "",
    batch_client: object | None = None,
    task_id_max: int = 0,
    task_id_ints: bool = False,
) -> batch_models.TaskAddCollectionResult:
    """Add a list of tasks to an Azure Batch job with comprehensive configuration options.

    Creates and adds a list of tasks to the specified job with support for dependencies,
    container execution, mount configurations, log saving, and timeout constraints.
    The tasks runs in a container environment with configurable execution settings.

    Args:
        job_name (str): Name/ID of the job to add the task to. The job must exist.
        task_id_base (str): Base string for generating the task ID (used with name_suffix
            and task_id_max unless task_id_ints is True).
        tasks (list[dict]): List of task configuration dicts. Each dict can contain:
            - command_line (str | list[str]): Command to execute in the task. Can be a string
            or list of strings that will be joined.
            - save_logs_rel_path (str, optional): Relative path where stdout/stderr logs should
                be saved. If None, logs are not saved to blob storage.
            - logs_folder (str): Name of the folder to create for saving logs. Defaults to
                "stdout_stderr".
            - mounts (list[dict], optional): List of mount configurations as dicts
                of {"source": <container_name>, "target": <relative_mount_path>).
            - depends_on (str | list[str], optional): Task ID(s) that this task depends on.
                Task will not start until dependencies complete successfully.
            - depends_on_range (tuple[int, int], optional): Range of task IDs (start, end) that
                this task depends on. Alternative to depends_on.
            - run_dependent_tasks_on_fail (bool): If True, dependent tasks will run even if
                this task fails. If False, failure blocks dependents. Defaults to False.
            - timeout (int, optional): Maximum wall clock time for the task in minutes. If None,
                no timeout is set.
            - full_container_name (str, optional): Full container image name to use for the task.
        name_suffix (str): Suffix to append to the task ID for uniqueness. Defaults to "".
        batch_client (object): Azure Batch service client instance for API calls.
        task_id_max (int): Current maximum task ID number for generating unique task IDs.
            Defaults to 0.
        task_id_ints (bool): If True, use integer task IDs instead of string-based IDs.
            Defaults to False.

    Returns:
        TaskAddCollectionResult: The result of task collection operation

    Example:
        Add 2 simple tasks:

            task_id = add_task(
                job_name="data-processing",
                task_id_base="process",
                command_line="python process_data.py",
                batch_client=batch_client,
                full_container_name="myregistry.azurecr.io/data-processor:latest"
            )

        Add a task with dependencies and log saving:

            task_id = add_task_collection(
                job_name="analysis-job",
                task_id_base="analyze",
                tasks=[
                    {
                        "command_line": ["python", "analyze.py", "--input", "/data/input.csv"],
                        "depends_on": ["preprocess-task-1", "preprocess-task-2"],
                        "save_logs_rel_path": "/logs",
                        "timeout": 120,
                        "run_dependent_tasks_on_fail": False,
                        "full_container_name": "myregistry.azurecr.io/analyzer:v1.0"
                    },
                    {
                        "command_line": "python foo.py",
                        "depends_on": ["preprocess-task-2"],
                        "save_logs_rel_path": "/logs",
                        "mounts": [
                            {"source": "data-container", "target": "/data"},
                            {"source": "config-container", "target": "/config"},
                        ],
                        "timeout": 30,
                    },
                ],
                batch_client=batch_client,

            )

    Note:
        The task runs with admin privileges in the pool and is configured with
        appropriate exit conditions based on the run_dependent_tasks_on_fail setting.
        Mount configurations allow access to blob storage from within the container.
        Log saving creates timestamped files in the specified blob storage location.
    """
    logger.debug(
        f"Adding task to job '{job_name}' with base ID '{task_id_base}', suffix '{name_suffix}'"
    )

    # Add a task to the job
    logger.debug("Creating user identity with admin privileges")
    user_identity = batch_models.UserIdentity(
        auto_user=batch_models.AutoUserSpecification(
            scope=batch_models.AutoUserScope.pool,
            elevation_level=batch_models.ElevationLevel.admin,
        )
    )

    logger.debug("Creating task collection")
    tasks_to_add = []
    for n, task in enumerate(tasks):
        if task_id_ints:
            task_id = str(task_id_max + n + 1)
            logger.debug(f"Generated integer-based task ID: '{task_id}'")
        else:
            task_id = f"{task_id_base}-{name_suffix}-{str(task_id_max + n + 1)}"
            logger.debug(f"Generated string-based task ID: '{task_id}'")

        command_line = task["command_line"]

        logs_folder = task.get("logs_folder", "stdout_stderr")
        save_logs_rel_path = task.get("save_logs_rel_path")
        if save_logs_rel_path is not None:
            full_command = _generate_command_for_saving_logs(
                command_line=command_line,
                job_name=job_name,
                task_id=task_id,
                save_logs_rel_path=save_logs_rel_path,
                logs_folder=logs_folder,
            )
        else:
            full_command = command_line

        run_dependent_tasks_on_fail = task.get("run_dependent_tasks_on_fail", False)
        exit_conditions = _generate_exit_conditions(run_dependent_tasks_on_fail)

        depends_on = task.get("depends_on")
        depends_on_range = task.get("depends_on_range")
        task_deps = _generate_task_dependencies(depends_on, depends_on_range)

        timeout = task.get("timeout")
        _to = datetime.timedelta(minutes=timeout) if timeout else None
        task_constraints = TaskConstraints(max_wall_clock_time=_to)

        mounts = task.get("mounts", [])
        mount_str = _generate_mount_string(mounts)

        # if full container name is none, pull info from job
        container_name = f"{job_name}_{str(task_id_max + 1)}"
        container_run_options = f"--name={container_name} --rm " + mount_str

        new_task = batch_models.TaskAddParameter(
            id=task_id,
            command_line=full_command,
            container_settings=batch_models.TaskContainerSettings(
                image_name=task["full_container_name"],
                container_run_options=container_run_options,
                working_directory="containerImageDefault",
            ),
            user_identity=user_identity,
            constraints=task_constraints,
            depends_on=task_deps,
            run_dependent_tasks_on_failure=run_dependent_tasks_on_fail,
            exit_conditions=exit_conditions,
        )
        tasks_to_add.append(new_task)

    # Add the task list to job
    logger.debug(
        f"Adding '{len(tasks_to_add)}' to job '{job_name}' i Azure Batch service"
    )
    result = batch_client.task.add_collection(job_id=job_name, value=tasks_to_add)
    logger.debug(f"Successfully added {len(tasks_to_add)}' tasks job '{job_name}'")
    return result


def check_pool_exists(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> bool:
    """Check if a specified Azure Batch pool exists in the account.

    Verifies the existence of a pool by attempting to retrieve its information
    from the Azure Batch service. Returns True if the pool exists and is accessible,
    False if the pool does not exist or cannot be accessed.

    Args:
        resource_group_name (str): Name of the Azure resource group containing the
            Batch account.
        account_name (str): Name of the Azure Batch account to check for the pool.
        pool_name (str): Name of the pool to check for existence.
        batch_mgmt_client (object): Instance of BatchManagementClient for API calls.

    Returns:
        bool: True if the pool exists and is accessible, False otherwise.

    Example:
        Check pool before creating a job:

            if check_pool_exists("my-rg", "my-batch", "compute-pool", mgmt_client):
                print("Pool exists, proceeding with job creation")
                create_job("my-job", "compute-pool", batch_client)
            else:
                print("Pool not found, please create the pool first")

        Validate pool list:

            pools_to_check = ["pool1", "pool2", "pool3"]
            existing_pools = []
            for pool in pools_to_check:
                if check_pool_exists("my-rg", "my-batch", pool, mgmt_client):
                    existing_pools.append(pool)
            print(f"Available pools: {existing_pools}")

    Note:
        This function catches all exceptions during the pool lookup and treats
        any exception as indicating the pool does not exist. This includes
        permission errors, network issues, and actual non-existence.
    """
    logger.debug(f"Checking if pool {pool_name} exists.")
    try:
        batch_mgmt_client.pool.get(resource_group_name, account_name, pool_name)
        logger.debug("Pool exists.")
        return True
    except Exception:
        logger.debug("Pool does not exist.")
        return False
