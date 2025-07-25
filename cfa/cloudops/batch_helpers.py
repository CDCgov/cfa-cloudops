import csv
import datetime
import logging
import time

import azure.batch.models as batch_models

logger = logging.getLogger(__name__)


def monitor_tasks(job_name: str, timeout: int, batch_client: object):
    """monitors tasks running in the job based on job ID

    Args:
        job_name (str): the name of the job to monitor
        timeout (int): number of minutes for timeout
        batch_client (object): an instance of batch client

    Raises:
        RuntimeError: this error is raised if the job does not complete in the timeout

    Returns:
        dict: dictionary with keys completed (whether the job completed) and runtime (total elapsed time)
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

            print(
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
    if file_name is None:
        file_name = f"{job_name}-stats"
    r = batch_service_client.task.list(
        job_name=job_name,
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
    """Checks whether a job exists.

    Args:
        job_id (str): the name (id) of a job
        batch_client (object): batch client object

    Returns:
        bool: whether the job exists
    """
    job_list = batch_client.job.list()
    if job_name in job_list:
        logger.debug(f"job {job_name} exists.")
        return True
    else:
        logger.debug(f"job {job_name} does not exist.")
        return False


def get_completed_tasks(job_name: str, batch_client: object):
    """Return the number of completed tasks for the specified job.

    Args:
        job_name (str): the name (id) of a job
        batch_client (object): batch client object

    Returns:
        dict: dictionary containing number of completed tasks and total tasks for the job
    """
    logger.debug("Pulling in task information.")
    tasks = batch_client.task.list(job_name)
    total_tasks = len(tasks)

    completed_tasks = [
        task
        for task in tasks
        if task.state == batch_models.TaskState.completed
    ]
    num_c_tasks = len(completed_tasks)

    return {"completed tasks": num_c_tasks, "total tasks": total_tasks}
