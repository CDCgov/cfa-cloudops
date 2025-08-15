# Running jobs with `cfa.cloudops.CloudClient`

Jobs are the main component in Azure Batch that have the capability to run large-scale parallel and high performance computing tasks. A job is composed of a collection of tasks that can be run in parallel, sequentially, or some combination of both, as well as set dependencies if one task needs to run before the other. Tasks are run on nodes within an existing pool (more info on pools [here](/docs/CloudClient/pool.md)).

The `CloudClient` class makes it extremely easy to submit jobs and tasks. When submitting tasks, the job must already exist in Azure Batch. The `monitor_job` method can also be run after submitting tasks to print the job's process in the terminal.

## Creating Jobs

The `CloudClient` class has a method called `create_job` which should be used for programmatically creating jobs in your specified Azure Batch account. The following parameters can be passed to the method for customization of the job:
- job_name: name of the job to create. Spaces will be removed.
- pool_name: name of existing pool to use for running the job.
- uses_deps: whether the job will use task dependencies. Default is True.
- save_logs_to_blob: the name of the blob container to use if you want to save logs to the blob container. Optional.
- logs_folder: the folder to save logs to if save_logs_to_blob is used. Default is 'stdout_stderr'.
- task_retries: number of retries for the tasks if they fail. Default is 0.
- mark_complete_after_tasks_run: whether to automatically mark the job as complete after all tasks finish. Default is False.
- task_id_ints: whether to use integers for task IDs. Default is False.
- timeout: maximum time in minutes that the job can run before being terminated. If omitted, jobs can run indefinitely.
- exist_ok: whether to allow the job creation if a job with the same name already exists. Default is False.
- verify_pool: whether to check if the pool exists.
- verbose: whether to be verbose as the job gets created.

### The Simplest Example
For users just looking to get started with this job creation, the following can be run to create a job called 'test-job-1' on the existing pool 'test-pool-exists'.
```python
client = CloudClient()
client.create_job(
    job_name = "test-job-1",
    pool_name = "test-pool-exists"
)
```

### A Complex Example
Suppose we want the same job name and pool name as above, but we know tasks will use dependencies, we want to save any logs to a 'logs' folder in our 'output-test' Blob Container, and tasks can be rerun once, marking the job complete after all tasks finish. In this case, the following should be run:
```python
client = CloudClient()
client.create_job(
    job_name = "test-job-1",
    pool_name = "test-pool-exists",
    uses_deps = True,
    save_logs_to_blob = "output-test",
    logs_folder = "logs",
    mark_complete_after_tasks_run = True,
    task_retries = 1
)
```

## Creating Tasks

Once jobs are created, tasks can be added to that job to run. Per the Azure Batch documentation, a task is a unit of computation that is associated with a job, runs on a node within a pool, and runs one or more programs/scripts of the work you need done. The behavior is similar to executing commands in a Docker container. Azure Batch nodes are spun up based on a container image and actions get executed from a command. Tasks can also be set to run only after one or more tasks completes if that dependency exists.

Depending on the pool setup, tasks can run in parallel or sequentially. If there are more than one task slot per node established in the pool, multiple tasks could run on a single node at one time.

The `CloudClient` class has a `add_task` method to simplify the programmatic submission of tasks to their respective job. Multiple `add_task` calls can be submitted one after the other. The following parameters can be passed to the `add_task` method:
- job_name: name of an existing job
- command_line: command for the desired task like you would run in a terminal
- name_suffix: name to append to task IDs if desired. It may help identify tasks. Optional.
- depends_on: list of task ID(s) that this task depends on. Optional.
- depends_on_range: if integers are used for the task IDs and the task depends on a range of tasks, the first task ID and the last task ID can be passed in tuple form. Form example if task 11 depends on tasks 1 through 10, the correct form would be (1, 10). Optional.
- run_dependent_tasks_on_fail: whether to run dependent tasks even if this task fails. Optional.
- container_image_name: full name of container to use if it differs from the container associated with the pool.
- timeout: time in minutes before stopping the task. Optional.


### A Simple Example

Suppose we have two simple tasks we need to run for our job 'first-job'. One task prints out 'hello, world' to the console. The other prints the python version of the container. Note that this is a dumb use case that would not require Azure Batch to complete.

```python
client = CloudClient()
client.add_task(
    job_name = "first-job",
    command_line = "echo 'hello, world'"
)
client.add_task(
    job_name = "first-job",
    command_line = "python3 --version"
)
```

### Tasks with Dependencies

Suppose we want to run two python scripts where the second script depends on the first, but can be run even if the first task fails. The following code can be used:
```python
client = CloudClient()
task_id = client.add_task(
    job_name = "first-job",
    command_line = "python3 first_task.py"
)
client.add_task(
    job_name = "first-job",
    command_line = "python3 second_task.py",
    depends_on = [task_id],
    run_dependent_tasks_on_fail = True
)
```
