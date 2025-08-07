import csv
import datetime
import logging
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


def monitor_tasks(job_name: str, timeout: int, batch_client: object):
    """Monitor tasks running in an Azure Batch job until completion or timeout.

    Continuously monitors the progress of all tasks in a job, providing real-time
    updates on task completion status, successes, and failures. Monitors until all
    tasks complete or the specified timeout is reached.

    Args:
        job_name (str): Name/ID of the job to monitor. The job must exist and be active.
        timeout (int): Maximum time in minutes to monitor before timing out. If None,
            defaults to 480 minutes (8 hours).
        batch_client (object): Azure Batch service client instance for API calls.

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
    tasks = list(batch_client.task.list(job_name))

    # get total tasks
    total_tasks = len([task for task in tasks])
    logger.info(f"Total tasks to monitor: {total_tasks}")

    # pool setup and status
    # initialize job complete status
    completed = False
    completions, incompletions, running, successes, failures = 0, 0, 0, 0, 0
    job = batch_client.job.get(job_name)
    while job.as_dict()["state"] != "completed" and not completed:
        if datetime.datetime.now() < timeout_expiration:
            time.sleep(5)  # Polling interval
            tasks = list(batch_client.task.list(job_name))
            incomplete_tasks = [
                task
                for task in tasks
                if task.state != batch_models.TaskState.completed
            ]
            incompletions = len(incomplete_tasks)
            completed_tasks = [
                task
                for task in tasks
                if task.state == batch_models.TaskState.completed
            ]
            completions = len(completed_tasks)
            running_tasks = [
                task
                for task in tasks
                if task.state == batch_models.TaskState.running
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
                completed = True
                break
            job = batch_client.job.get(job_name)

    end_time = datetime.datetime.now().replace(microsecond=0)

    if completed:
        logger.info(
            "All tasks have reached 'Completed' state within the timeout period."
        )
        logger.info(f"{successes} task(s) succeeded, {failures} failed.")
    # get terminate reason
    if "terminate_reason" in job.as_dict()["execution_info"].keys():
        terminate_reason = job.as_dict()["execution_info"]["terminate_reason"]
    else:
        terminate_reason = None

    runtime = end_time - start_time
    logger.info(
        f"Monitoring ended: {end_time}. Total elapsed time: {runtime}."
    )
    print("-" * 30)
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
    if file_name is None:
        file_name = f"{job_name}-stats"
    r = batch_service_client.task.list(
        job_id=job_name,
    )

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
            logger.debug("wrote task to job statistic csv.")
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
        task
        for task in tasks
        if task.state == batch_models.TaskState.completed
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
    job = batch_client.job.get(job_name)
    return job.state == batch_models.JobState.completed


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
    job = batch_client.job.get(job_name)
    return str(job.state)


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
    logger.info(f"Pool {pool_name} deleted.")
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
    with open(file_path) as f:
        raw_griddle = yaml.safe_load(f)
    griddle = griddler.parse(raw_griddle)
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
    cmds = []
    arg_list = get_args_from_yaml(file_path)
    for s in arg_list:
        cmds.append(f"{base_cmd} {s}")
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
        github_org (str, optional): .

    Returns:
        str: The full image name to use in Azure Batch pool.
    """
    if registry.lower() == "azure":
        if not acr_name:
            raise ValueError(
                "acr_name must be provided for Azure Container Registry."
            )
        return f"{acr_name}.azurecr.io/{container_name}"
    elif registry.lower().startswith("docker"):
        return f"{container_name}"
    elif registry.lower() == "github":
        if not github_org:
            raise ValueError(
                "github_org must be provided for GitHub Container Registry."
            )
        return f"ghcr.io/{github_org.lower()}/{container_name}"
    else:
        raise ValueError(
            "Unsupported registry type. Use 'azure', 'docker', or 'github'."
        )


class Task:
    def __init__(
        self, cmd: str, id: str | None = None, dep: str | list | None = None
    ):
        """
        Args:
            cmd (str): command to be used with Azure Batch task
            id (str, optional): optional id to identity tasks. Defaults to None.
            dep (str | list[str], optional): Task object(s) this task depends on. Defaults to None.
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
        """
        Set that this task needs to occur before another task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.before(t2) sets t1 must occure before t2.

        Args:
            other (Task): batch.Task object
        """
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if self not in task.deps:
                task.deps.append(self)

    def after(self, other):
        """
        Set that this task needs to occur after another task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.after(t2) sets t1 must occur after t2.

        Args:
            other (Task): batch.Task object
        """
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if task not in self.deps:
                self.deps.append(task)

    def set_downstream(self, other):
        """
        Sets the downstream task from the current task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.set_downstream(t2) sets t2 as the downstream task from t1, like t1 >> t2

        Args:
            other (Task): batch.Task object
        """
        self.before(other)

    def set_upstream(self, other):
        """
        Sets the upstream task from the current task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.set_upstream(t2) sets t2 as the upstream task from t1, like t1 << t2

        Args:
            other (Task): batch.Task object
        """
        self.after(other)


def get_rel_mnt_path(
    blob_name: str,
    pool_name: str,
    resource_group_name: str,
    account_name: str,
    batch_mgmt_client: object,
):
    """
    Args:
        blob_name (str): name of blob container
        pool_name (str): name of pool
        resource_group_name (str): name of resource group in Azure
        account_name (str): name of account in Azure
        batch_mgmt_object (object): instance of BatchManagementClient

    Returns:
        str: relative mount path for the blob and pool specified
    """
    try:
        pool_info = get_pool_full_info(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_name,
            batch_mgmt_client=batch_mgmt_client,
        )
    except Exception:
        logger.error("could not retrieve pool information.")
        return "ERROR!"
    mc = pool_info.as_dict()["mount_configuration"]
    for m in mc:
        if (
            m["azure_blob_file_system_configuration"]["container_name"]
            == blob_name
        ):
            rel_mnt_path = m["azure_blob_file_system_configuration"][
                "relative_mount_path"
            ]
            return rel_mnt_path
    logger.error(f"could not find blob {blob_name} mounted to pool.")
    print(f"could not find blob {blob_name} mounted to pool.")
    return "ERROR!"


def get_pool_full_info(
    resource_group_name: str,
    account_name: str,
    pool_name: str,
    batch_mgmt_client: object,
) -> dict:
    """Get the full information of a specified pool.

    Args:
        resource_group_name (str): name of resource group
        account_name (str): name of account
        pool_name (str): name of pool
        batch_mgmt_client (object): instance of BatchManagementClient

    Returns:
        dict: dictionary with full pool information
    """
    logger.debug("Pulling pool info.")
    result = batch_mgmt_client.pool.get(
        resource_group_name, account_name, pool_name
    )
    return result


def get_pool_mounts(
    pool_name: str,
    resource_group_name: str,
    account_name: str,
    batch_mgmt_client: object,
):
    """
    Args:
        pool_name (str): name of pool
        resource_group_name (str): name of resource group in Azure
        account_name (str): name of account in Azure
        batch_mgmt_client (object): instance of BatchManagementClient

    Returns:
        list: list of mounts in specified pool
    """
    try:
        pool_info = get_pool_full_info(
            resource_group_name=resource_group_name,
            account_name=account_name,
            pool_name=pool_name,
            batch_mgmt_client=batch_mgmt_client,
        )
    except Exception:
        logger.error("could not retrieve pool information.")
        print(f"could not retrieve pool info for {pool_name}.")
        return None
    mounts = []
    mc = pool_info.as_dict()["mount_configuration"]
    for m in mc:
        mounts.append(
            (
                m["azure_blob_file_system_configuration"]["container_name"],
                m["azure_blob_file_system_configuration"][
                    "relative_mount_path"
                ],
            )
        )
    return mounts


def add_task(
    job_name: str,
    task_id_base: str,
    command_line: str,
    save_logs_rel_path: str | None = None,
    logs_folder: str = "stdout_stderr",
    name_suffix: str = "",
    mounts: list | None = None,
    depends_on: str | list[str] | None = None,
    depends_on_range: tuple | None = None,
    run_dependent_tasks_on_fail: bool = False,
    batch_client: object | None = None,
    full_container_name: str | None = None,
    task_id_max: int = 0,
    task_id_ints: bool = False,
    timeout: int | None = None,
) -> str:
    logger.debug(f"Adding task to job {job_name}.")

    # convert command line to string if given as list
    if isinstance(command_line, list):
        cmd_str = " ".join(command_line)
        logger.debug("Docker command converted to string")
    else:
        cmd_str = command_line

    # Add a task to the job
    az_mount_dir = "$AZ_BATCH_NODE_MOUNTS_DIR"
    user_identity = batch_models.UserIdentity(
        auto_user=batch_models.AutoUserSpecification(
            scope=batch_models.AutoUserScope.pool,
            elevation_level=batch_models.ElevationLevel.admin,
        )
    )

    task_deps = None
    if depends_on is not None:
        # Create a TaskDependencies object to pass in
        if isinstance(depends_on, str):
            depends_on = [depends_on]
            logger.debug("Adding task dependency.")
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

    no_exit_options = ExitOptions(
        dependency_action=DependencyAction.satisfy, job_action=JobAction.none
    )
    if run_dependent_tasks_on_fail:
        exit_conditions = ExitConditions(
            exit_codes=[
                ExitCodeMapping(code=0, exit_options=no_exit_options),
                ExitCodeMapping(code=1, exit_options=no_exit_options),
            ],
            pre_processing_error=no_exit_options,
            file_upload_error=no_exit_options,
            default=no_exit_options,
        )
    else:
        terminate_exit_options = ExitOptions(
            dependency_action=DependencyAction.block,
            job_action=JobAction.none,
        )
        exit_conditions = ExitConditions(
            exit_codes=[
                ExitCodeMapping(code=0, exit_options=no_exit_options),
                ExitCodeMapping(code=1, exit_options=terminate_exit_options),
            ],
            pre_processing_error=terminate_exit_options,
            file_upload_error=terminate_exit_options,
            default=terminate_exit_options,
        )

    logger.debug("Creating mount configuration string.")
    mount_str = ""
    # src = env variable to fsmounts/rel_path
    # target = the directory(path) you reference in your code
    if mounts is not None:
        mount_str = ""
        for mount in mounts:
            logger.debug("Adding mount to mount string.")
            mount_str = (
                mount_str
                + "--mount type=bind,source="
                + az_mount_dir
                + f"/{mount[1]},target=/{mount[1]} "
            )

    if task_id_ints:
        task_id = str(task_id_max + 1)
    else:
        task_id = f"{task_id_base}-{name_suffix}-{str(task_id_max + 1)}"

    if save_logs_rel_path is not None:
        if save_logs_rel_path == "ERROR!":
            logger.warning("could not find rel path")
            print(
                "could not find rel path. Stdout and stderr will not be saved to blob storage."
            )
            full_cmd = cmd_str
        else:
            logger.debug("using rel path to save logs")
            t = datetime.datetime.now(zi("America/New_York"))
            s_time = t.strftime("%Y%m%d_%H%M%S")
            if not save_logs_rel_path.startswith("/"):
                save_logs_rel_path = "/" + save_logs_rel_path
            _folder = f"{save_logs_rel_path}/{logs_folder}/"
            sout = f"{_folder}/stdout_{job_name}_{task_id}_{s_time}.txt"
            full_cmd = f"""/bin/bash -c "mkdir -p {_folder}; {cmd_str} 2>&1 | tee {sout}" """
    else:
        full_cmd = cmd_str

    # add contstraints
    if timeout is None:
        _to = None
    else:
        _to = datetime.timedelta(minutes=timeout)

    task_constraints = TaskConstraints(max_wall_clock_time=_to)
    command_line = full_cmd
    logger.debug(f"Adding task {task_id}")

    # if full container name is none, pull info from job

    # Create the task parameter
    task_param = batch_models.TaskAddParameter(
        id=task_id,
        command_line=command_line,
        container_settings=batch_models.TaskContainerSettings(
            image_name=full_container_name,
            container_run_options=f"--name={job_name}_{str(task_id_max + 1)} --rm "
            + mount_str,
        ),
        user_identity=user_identity,
        constraints=task_constraints,
        depends_on=task_deps,
        run_dependent_tasks_on_failure=run_dependent_tasks_on_fail,
        exit_conditions=exit_conditions,
    )

    # Add the task to the job
    batch_client.task.add(job_id=job_name, task=task_param)
    logger.debug(f"Task '{task_id}' added to job '{job_name}'.")
    return task_id
