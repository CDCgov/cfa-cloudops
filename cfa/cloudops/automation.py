import itertools
import logging

import pandas as pd
import toml

from cfa.cloudops import CloudClient, batch_helpers

logger = logging.getLogger(__name__)


def run_experiment(exp_config: str, dotenv_path: str | None = None, **kwargs):
    """Run jobs and tasks automatically based on the provided experiment config.

    Args:
        exp_config (str): path to experiment config file (toml)
    """
    logger.info(f"Starting experiment execution with config: {exp_config}")
    logger.debug(f"Using dotenv_path: {dotenv_path}")

    # read files
    logger.debug(f"Loading experiment configuration from: {exp_config}")
    exp_toml = toml.load(exp_config)
    logger.debug(
        f"Successfully loaded experiment config with sections: {list(exp_toml.keys())}"
    )

    try:
        logger.debug("Creating CloudClient instance")
        client = CloudClient(dotenv_path=dotenv_path)
        logger.debug("CloudClient created successfully")
    except Exception as e:
        logger.error(f"Failed to create CloudClient object: {e}")
        print("could not create CloudClient object.")
        return None

    # check pool included in exp_toml and exists in azure
    logger.debug("Validating pool configuration")
    if "pool_name" in exp_toml["job"].keys():
        pool_name = exp_toml["job"]["pool_name"]
        logger.debug(f"Checking if pool '{pool_name}' exists in Azure")
        if not batch_helpers.check_pool_exists(
            resource_group_name=client.cred.azure_resource_group_name,
            account_name=client.cred.azure_batch_account,
            pool_name=pool_name,
            batch_mgmt_client=client.batch_mgmt_client,
        ):
            logger.error(f"Pool '{pool_name}' does not exist in the Azure environment")
            print(
                f"pool name {exp_toml['job']['pool_name']} does not exist in the Azure environment."
            )
            return None
        logger.debug(f"Pool '{pool_name}' validated successfully")
    else:
        logger.error(
            "Missing required 'pool_name' key in job section of experiment config"
        )
        print("could not find 'pool_name' key in 'setup' section of exp toml.")
        print("please specify a pool name to use.")
        return None

    # upload files if the section exists
    if "upload" in exp_toml.keys():
        logger.debug("Processing upload section from experiment config")
        container_name = exp_toml["upload"]["container_name"]
        logger.debug(f"Target container: {container_name}")
        if "location_in_blob" in exp_toml["upload"].keys():
            location_in_blob = exp_toml["upload"]["location_in_blob"]
        else:
            location_in_blob = ""
        logger.debug(f"Upload location in blob: '{location_in_blob}'")

        if "folders" in exp_toml["upload"].keys():
            folders = exp_toml["upload"]["folders"]
            logger.debug(f"Uploading folders: {folders}")
            client.upload_folders(
                folder_names=folders,
                location_in_blob=location_in_blob,
                container_name=container_name,
            )
            logger.info(f"Uploaded folders: {folders} to container: {container_name}")
            logger.debug("Folder upload completed")
        if "files" in exp_toml["upload"].keys():
            files = exp_toml["upload"]["files"]
            logger.debug(f"Uploading files: {files}")
            client.upload_files(
                files=files,
                location_in_blob=location_in_blob,
                container_name=container_name,
            )
            logger.info(f"Uploaded files: {files} to container: {container_name}")
            logger.debug("File upload completed")
    else:
        logger.debug("No upload section found in experiment config")

    # create the job
    job_name = exp_toml["job"]["job_name"]
    logger.debug(f"Creating job: {job_name}")

    if "save_logs_to_blob" in exp_toml["job"].keys():
        save_logs_to_blob = exp_toml["job"]["save_logs_to_blob"]
    else:
        save_logs_to_blob = None
    if "logs_folder" in exp_toml["job"].keys():
        logs_folder = exp_toml["job"]["logs_folder"]
    else:
        logs_folder = None
    if "task_retries" in exp_toml["job"].keys():
        task_retries = exp_toml["job"]["task_retries"]
    else:
        task_retries = 0

    logger.debug(
        f"Job config - save_logs_to_blob: {save_logs_to_blob}, logs_folder: {logs_folder}, task_retries: {task_retries}"
    )

    client.create_job(
        job_name=job_name,
        pool_name=pool_name,
        save_logs_to_blob=save_logs_to_blob,
        logs_folder=logs_folder,
        task_retries=task_retries,
    )
    logger.info(f"Job '{job_name}' created successfully.")

    # create the tasks for the experiment
    logger.debug("Creating tasks for experiment")
    # get the container to use if necessary
    if "container" in exp_toml["job"].keys():
        container = exp_toml["job"]["container"]
        logger.debug(f"Using container: {container}")
    else:
        container = None
        logger.debug("No container specified for tasks")

    # submit the experiment tasks
    ex = exp_toml["experiment"]
    logger.debug(f"Processing experiment section with keys: {list(ex.keys())}")
    if "exp_yaml" in ex.keys():
        logger.debug(
            f"Adding tasks from YAML file: {ex['exp_yaml']} with base command: {ex['base_cmd']}"
        )
        client.add_tasks_from_yaml(
            job_name=job_name,
            base_cmd=ex["base_cmd"],
            file_path=ex["exp_yaml"],
        )
        logger.info("Tasks added from YAML successfully.")
        logger.debug("Tasks added from YAML successfully")
    else:
        logger.debug("Processing experiment tasks with parameter combinations")
        var_list = [key for key in ex.keys() if key != "base_cmd"]
        logger.debug(f"Variable list for combinations: {var_list}")
        var_values = []
        for var in var_list:
            var_values.append(ex[var])
        logger.debug(f"Variable values: {var_values}")
        v_v = list(itertools.product(*var_values))
        logger.debug(f"Generated {len(v_v)} parameter combinations")

        for i, params in enumerate(v_v):
            j = {}
            for idx, value in enumerate(params):
                j.update({var_list[idx]: value})
            command_line = ex["base_cmd"].format(**j)
            logger.debug(f"Adding task {i + 1}/{len(v_v)} with command: {command_line}")
            client.add_task(
                job_name=job_name,
                command_line=command_line,
                container_image_name=container,
            )
        logger.info(f"Successfully added {len(v_v)} experiment tasks")
        logger.debug(f"Successfully added {len(v_v)} experiment tasks")

    if "monitor_job" in exp_toml["job"].keys():
        if exp_toml["job"]["monitor_job"] is True:
            logger.debug(f"Starting job monitoring for: {job_name}")
            client.monitor_job(job_name)
            logger.debug(f"Completed monitoring job: {job_name}")
        else:
            logger.debug("Job monitoring disabled in configuration")
    else:
        logger.debug("No monitor_job setting found in configuration")

    logger.debug(f"Experiment execution completed for job: {job_name}")


def run_tasks(task_config: str, dotenv_path: str | None = None, **kwargs) -> None:
    """Run jobs and tasks automatically based on the provided task config.
    Args:
        task_config (str): path to task config file (toml)
    """
    logger.debug(f"Starting task execution with config: {task_config}")
    logger.debug(f"Using dotenv_path: {dotenv_path}")

    # read files
    logger.debug(f"Loading task configuration from: {task_config}")
    task_toml = toml.load(task_config)
    logger.debug(
        f"Successfully loaded task config with sections: {list(task_toml.keys())}"
    )

    try:
        logger.debug("Creating CloudClient instance")
        client = CloudClient(dotenv_path=dotenv_path)
        logger.debug("CloudClient created successfully")
    except Exception as e:
        logger.error(f"Failed to create CloudClient object: {e}")
        print("could not create AzureClient object.")
        return None

    # check pool included in task_toml and exists in azure
    logger.debug("Validating pool configuration")
    if "pool_name" in task_toml["job"].keys():
        pool_name = task_toml["job"]["pool_name"]
        logger.debug(f"Checking if pool '{pool_name}' exists in Azure")
        if not batch_helpers.check_pool_exists(
            resource_group_name=client.cred.azure_resource_group_name,
            account_name=client.cred.azure_batch_account,
            pool_name=pool_name,
            batch_mgmt_client=client.batch_mgmt_client,
        ):
            logger.error(f"Pool '{pool_name}' does not exist in the Azure environment")
            print(
                f"pool name {task_toml['job']['pool_name']} does not exist in the Azure environment."
            )
            return None
        logger.debug(f"Pool '{pool_name}' validated successfully")
    else:
        logger.error("Missing required 'pool_name' key in job section of task config")
        print("could not find 'pool_name' key in 'setup' section of task config toml.")
        print("please specify a pool name to use.")
        return None

    # upload files if the section exists
    if "upload" in task_toml.keys():
        logger.debug("Processing upload section from task config")
        container_name = task_toml["upload"]["container_name"]
        logger.debug(f"Target container: {container_name}")
        if "location_in_blob" in task_toml["upload"].keys():
            location_in_blob = task_toml["upload"]["location_in_blob"]
        else:
            location_in_blob = ""
        logger.debug(f"Upload location in blob: '{location_in_blob}'")

        if "folders" in task_toml["upload"].keys():
            folders = task_toml["upload"]["folders"]
            logger.debug(f"Uploading folders: {folders}")
            client.upload_folders(
                folder_names=folders,
                location_in_blob=location_in_blob,
                container_name=container_name,
            )
            logger.info(f"Uploaded folders: {folders} to container: {container_name}")
            logger.debug("Folder upload completed")
        if "files" in task_toml["upload"].keys():
            files = task_toml["upload"]["files"]
            logger.debug(f"Uploading files: {files}")
            client.upload_files(
                files=files,
                location_in_blob=location_in_blob,
                container_name=container_name,
            )
            logger.info(f"Uploaded files: {files} to container: {container_name}")
            logger.debug("File upload completed")
    else:
        logger.debug("No upload section found in task config")

    # create the job
    job_name = task_toml["job"]["job_name"]
    logger.debug(f"Creating job: {job_name}")

    if "save_logs_to_blob" in task_toml["job"].keys():
        save_logs_to_blob = task_toml["job"]["save_logs_to_blob"]
    else:
        save_logs_to_blob = None
    if "logs_folder" in task_toml["job"].keys():
        logs_folder = task_toml["job"]["logs_folder"]
    else:
        logs_folder = None
    if "task_retries" in task_toml["job"].keys():
        task_retries = task_toml["job"]["task_retries"]
    else:
        task_retries = 0

    logger.debug(
        f"Job config - save_logs_to_blob: {save_logs_to_blob}, logs_folder: {logs_folder}, task_retries: {task_retries}"
    )

    client.create_job(
        job_name=job_name,
        pool_name=pool_name,
        save_logs_to_blob=save_logs_to_blob,
        logs_folder=logs_folder,
        task_retries=task_retries,
    )
    logger.debug(f"Job '{job_name}' created successfully")

    # create the tasks for the experiment
    logger.debug("Creating tasks for job")
    # get the container to use if necessary
    if "container" in task_toml["job"].keys():
        container = task_toml["job"]["container"]
        logger.debug(f"Using container: {container}")
    else:
        container = None
        logger.debug("No container specified for tasks")

    # submit the tasks
    tt = task_toml["task"]
    logger.debug(f"Processing {len(tt)} tasks from configuration")
    df = pd.json_normalize(tt)
    df.insert(0, "task_id", pd.Series("", index=range(df.shape[0])))
    logger.debug("Created task tracking dataframe")
    # when kicking off a task we save the taskid to the row in df
    for i, item in enumerate(tt):
        task_name = item.get("name", f"task_{i}")
        logger.debug(f"Processing task {i + 1}/{len(tt)}: {task_name}")

        if "depends_on" in item.keys():
            d_list = []
            logger.debug(f"Task has dependencies: {item['depends_on']}")
            for d in item["depends_on"]:
                d_task = df[df["name"] == d]["task_id"].values[0]
                d_list.append(d_task)
            logger.debug(f"Resolved dependency task IDs: {d_list}")
        else:
            d_list = None
            logger.debug("Task has no dependencies")

        # check for other attributes
        if "run_dependent_tasks_on_fail" in item.keys():
            run_dependent_tasks_on_fail = item["run_dependent_tasks_on_fail"]
        else:
            run_dependent_tasks_on_fail = False
        logger.debug(f"run_dependent_tasks_on_fail: {run_dependent_tasks_on_fail}")

        # submit the task
        logger.debug(f"Submitting task with command: {item['cmd']}")
        tid = client.add_task(
            job_name=job_name,
            command_line=item["cmd"],
            depends_on=d_list,
            run_dependent_tasks_on_fail=run_dependent_tasks_on_fail,
            container_image_name=container,
        )
        df.loc[i, "task_id"] = tid
        logger.debug(f"Task submitted successfully with ID: {tid}")

    if "monitor_job" in task_toml["job"].keys():
        if task_toml["job"]["monitor_job"] is True:
            logger.debug(f"Starting job monitoring for: {job_name}")
            client.monitor_job(job_name)
        else:
            logger.debug("Job monitoring disabled in configuration")
    else:
        logger.debug("No monitor_job setting found in configuration")

    logger.debug(f"Task execution completed for job: {job_name}")
    return None
