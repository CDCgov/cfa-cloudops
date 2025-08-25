import itertools
import os
from pathlib import Path

import docker
import pandas as pd
import toml

from cfa.cloudops.local._client import CloudClient


def run_experiment(exp_config: str, dotenv_path: str | None = None, **kwargs):
    """Run jobs and tasks automatically based on the provided experiment config.

    Args:
        exp_config (str): path to experiment config file (toml)
    """

    # read files
    exp_toml = toml.load(exp_config)

    try:
        client = CloudClient(dotenv_path=dotenv_path)
    except Exception:
        print("could not create CloudClient object.")
        return None

    # check pool included in exp_toml and exists in azure
    if "pool_name" in exp_toml["job"].keys():
        pool_name = exp_toml["job"]["pool_name"]
        # check pool exists (folder)
        fpath = f"tmp/pools/{pool_name}.txt"
        if not os.path.exists(fpath):
            print(f"Pool {pool_name} does not exist. Please create it.")
        else:
            ppath = Path(fpath)
            pool_info = eval(ppath.read_text())
            image_name = pool_info["image_name"]
            # check image exists in docker
            try:
                d_env = docker.from_env(timeout=8)
                d_env.ping()
            except Exception:
                print(
                    "Could not ping docker. Make sure the docker daemon is running."
                )
            try:
                d_env.images.get(image_name)
            except Exception:
                print(f"Image {image_name} from pool not found in Docker.")
            client.cont_name = image_name.replace("/", "_").replace(":", "_")
    else:
        print("could not find 'pool_name' key in 'job' section of exp toml.")
        print("please specify a pool name to use.")
        return None

    # upload files if the section exists
    if "upload" in exp_toml.keys():
        container_name = exp_toml["upload"]["container_name"]
        if "location_in_blob" in exp_toml["upload"].keys():
            location_in_blob = exp_toml["upload"]["location_in_blob"]
        else:
            location_in_blob = ""
        if "folders" in exp_toml["upload"].keys():
            client.upload_folders(
                folder_names=exp_toml["upload"]["folders"],
                location_in_blob=location_in_blob,
                container_name=container_name,
            )
        if "files" in exp_toml["upload"].keys():
            client.upload_files(
                files=exp_toml["upload"]["files"],
                location_in_blob=location_in_blob,
                container_name=container_name,
            )

    # create the job
    job_name = exp_toml["job"]["job_name"]
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

    client.create_job(
        job_name=job_name,
        pool_name=pool_name,
        save_logs_to_blob=save_logs_to_blob,
        logs_folder=logs_folder,
        task_retries=task_retries,
    )

    # create the tasks for the experiment
    # get the container to use if necessary
    if "container" in exp_toml["job"].keys():
        container = exp_toml["job"]["container"]
    else:
        p_path = Path(f"tmp/pools/{pool_name}.txt")
        pool_info = eval(p_path.read_text())
        image_name = pool_info["image_name"]
        image_name = image_name.replace("/", "_").replace(":", "_")
        container = f"{image_name}.{job_name}"

    # submit the experiment tasks
    ex = exp_toml["experiment"]
    if "exp_yaml" in ex.keys():
        client.add_tasks_from_yaml(
            job_name=job_name,
            base_cmd=ex["base_cmd"],
            file_path=ex["exp_yaml"],
        )
    else:
        var_list = [key for key in ex.keys() if key != "base_cmd"]
        var_values = []
        for var in var_list:
            var_values.append(ex[var])
        v_v = list(itertools.product(*var_values))
        for params in v_v:
            j = {}
            for i, value in enumerate(params):
                j.update({var_list[i]: value})
            client.add_task(
                job_name=job_name,
                command_line=ex["base_cmd"].format(**j),
                container_image_name=container,
            )

    if "monitor_job" in exp_toml["job"].keys():
        if exp_toml["job"]["monitor_job"] is True:
            client.monitor_job(job_name)


def run_tasks(task_config: str, dotenv_path: str | None = None, **kwargs):
    """Run jobs and tasks automatically based on the provided task config.
    Args:
        task_config (str): path to task config file (toml)
    """

    # read files
    task_toml = toml.load(task_config)

    try:
        client = CloudClient(dotenv_path=dotenv_path)
    except Exception:
        print("could not create CloudClient object.")
        return None

    # check pool included in task_toml and exists in azure
    if "pool_name" in task_toml["job"].keys():
        pool_name = task_toml["job"]["pool_name"]
    else:
        print(
            "could not find 'pool_name' key in 'job' section of task config toml."
        )
        print("please specify a pool name to use.")
        return None

    # upload files if the section exists
    if "upload" in task_toml.keys():
        container_name = task_toml["upload"]["container_name"]
        if "location_in_blob" in task_toml["upload"].keys():
            location_in_blob = task_toml["upload"]["location_in_blob"]
        else:
            location_in_blob = ""
        if "folders" in task_toml["upload"].keys():
            client.upload_folders(
                folder_names=task_toml["upload"]["folders"],
                location_in_blob=location_in_blob,
                container_name=container_name,
            )
        if "files" in task_toml["upload"].keys():
            client.upload_files(
                files=task_toml["upload"]["files"],
                location_in_blob=location_in_blob,
                container_name=container_name,
            )

    # create the job
    job_name = task_toml["job"]["job_name"]
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

    client.create_job(
        job_name=job_name,
        pool_name=pool_name,
        save_logs_to_blob=save_logs_to_blob,
        logs_folder=logs_folder,
        task_retries=task_retries,
    )

    # create the tasks for the experiment
    # get the container to use if necessary
    if "container" in task_toml["job"].keys():
        container = task_toml["job"]["container"]
    else:
        p_path = Path(f"tmp/pools/{pool_name}.txt")
        pool_info = eval(p_path.read_text())
        image_name = pool_info["image_name"]
        image_name = image_name.replace("/", "_").replace(":", "_")
        container = f"{image_name}.{job_name}"

    # submit the tasks
    tt = task_toml["task"]
    df = pd.json_normalize(tt)
    df.insert(0, "task_id", pd.Series("", index=range(df.shape[0])))
    # when kicking off a task we save the taskid to the row in df
    for i, item in enumerate(tt):
        if "depends_on" in item.keys():
            d_list = []
            for d in item["depends_on"]:
                d_task = df[df["name"] == d]["task_id"].values[0]
                d_list.append(d_task)
        else:
            d_list = None
        # check for other attributes
        if "run_dependent_tasks_on_fail" in item.keys():
            run_dependent_tasks_on_fail = item["run_dependent_tasks_on_fail"]
        else:
            run_dependent_tasks_on_fail = False
        # submit the task
        tid = client.add_task(
            job_name=job_name,
            command_line=item["cmd"],
            depends_on=d_list,
            run_dependent_tasks_on_fail=run_dependent_tasks_on_fail,
            container_image_name=container,
        )
        df.loc[i, "task_id"] = tid

    if "monitor_job" in task_toml["job"].keys():
        if task_toml["job"]["monitor_job"] is True:
            client.monitor_job(job_name)
    return None
