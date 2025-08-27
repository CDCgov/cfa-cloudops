# Automation Module

## Overview
The `automation` module as part of `cfa-cloudops` is designed to perform certain actions in Azure based on a configuration file. This allows users to interact with Azure via this `cfa-cloudops` package even with little python experience. It also allows users to take their config file and make minor tweaks in order to upload new files, run different tasks, etc. It provides a more flexible framework than changing user-provided parameters spread throughout different functions in a python script.

Currently, the `automation` module is comprised of two functions to automate certain tasks:
1. `run_experiment`: useful when needing to create tasks based on a base command and a permutation of variables.
2. `run_tasks`: useful when needing to create specific tasks, with dependencies allowed.

## Imports
Both functions can be imported directly from `cfa.cloudops`.
```python
from cfa.cloudops import run_experiment, run_tasks
```

## The Specifics

### `run_experiment()`
The `run_experiment` function is meant for applying all permutations of a set of variables to a common base command. For example, if you have variables var1 and var2, where var1 can be 10 or 11 and var2 can be 98  or 99, and you need to apply all combinations (really permutations) of these variables to a single command, this is the function to use. It would create 4 tasks with (var1, var2) values of (10, 98), (10, 99), (11, 98), and (11, 99), passed into the command as determined in the config file.

If you have tasks you'd like to run based on a yaml file composed of the various command line arguments, this is accepted here as well. Rather than listing each parameter with the possible values, the [experiment] section will have a `base_command` and a `exp_yaml` key, which defines the command and the file path to the yaml. The yaml will be structured as described in this [example](./files/automation/params.yaml).

Here's a more concrete example of the first case. Suppose we have the following experiment section in the experiment config:
```python
[experiment]
base_cmd = "python3 /input/data/vars.py --first {var1} --another {var2} --test {var3}"
var1 = [1, 2, 4]
var2 = [10,11,12]
var3 = ['90', '99', '98']
```

The base command indicates a python script vars.py will be run with three command line arguments with flags called first, another, and test. To show the flexibility here, we use the names var1, var2, var3 for setting the list of options to cycle through. The values for var1 will be placed into {var1} of the base_cmd one at a time, var2 in {var2}, and var3 in {var3}. Any number of variables can be used and the number of elements of each list do not need to be equal. It's important that the names defining the lists of values match the placeholders (bracketed values, here var1, var2, var3). They do not need to match the actual flag names in the base_cmd (here first, another, test).
This experiment will generate 27 tasks, one for each permutation of [1, 2, 3], [10, 11, 12], ['90', '99', '98']. More specifically, the following commands will be used for the tasks:
```python
python3 /input/data/vars.py --first 1 --another 10 --test '90'
python3 /input/data/vars.py --first 1 --another 10 --test '99'
python3 /input/data/vars.py --first 1 --another 10 --test '98'
python3 /input/data/vars.py --first 2 --another 11 --test '90'
python3 /input/data/vars.py --first 2 --another 11 --test '99'
```

For the second case mentioned above in which we create tasks based on a yaml file, the [experiment] section may look like the following:
```python
[experiment]
base_cmd = "python3 main.py"
exp_yaml = "path/to/file.yaml"
```

If we have a yaml file like the one [here](./files/automation/params.yaml), the following tasks will be created for the job. Note that the schema must be v0.3 to meet pygriddler requirements and compatibility with cfa_azure, and more information on yaml files for pygriddler can be found [here](https://github.com/CDCgov/pygriddler/blob/v0.3.0/README.md).
```python
python3 main.py --method newton --start_point 0.25
python3 main.py --method newton --start_point 0.5
python3 main.py --method newton --start_point 0.75
python3 main.py --method brent --bounds [0.0, 1.0] --check
```


You can use the `run_experiment` function in two lines of code, as shown below.
```python
from cfa_azure.automation import run_experiment
run_experiment(exp_config = "path/to/exp_config.toml")
```

## `run_tasks()`
The `run_tasks` function is designed to take an arbitrary number of tasks from a configuration file to submit them as part of a job. Any folders or files included in the [upload] section of the config will be uploaded before kicking off the tasks.

Suppose we want to kick off two tasks we'll call "do_first" and "second_depends_on_first", where the R script is stored in Blob storage at the relative mount path "input/scripts/execute.R", the script can take different flags as input,  the second task depends on the first, and we will run the second task even if the first fails. We would setup the task_config.toml to have the following info in the [[task]] sections:
```python
[[task]]
cmd = 'Rscript /input/scripts/execute.R --data /example/data.csv"
name = "do_first"

[[task]]
cmd = 'Rscript /input/scripts/execute.R --model /example/model.pkl'
name = "second_depends_on_first"
depends_on = ["do_first"]
run_dependent_tasks_on_fail = true
```

You can then run the tasks in two lines of code, as shown below.
```python
from cfa_azure.automation import run_tasks
run_experiment(task_config = "path/to/task_config.toml")
```
