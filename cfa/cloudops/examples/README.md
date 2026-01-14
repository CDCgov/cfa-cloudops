# How to Invoke

## Goal
Schedule a job that runs daily at 9 PM except weekends and computes the Measles inference using daily partition stored in input-test folder of CFAAzureBatchPrd storage account. Use the measles-script:v1 container for this job.
    
## Open AI Only
`python agent_openai.py`

## Open AI with LangChain
`python agent_langchain_openai.py`

## Output
```bash
Interpreting the goal...
Generated Plan: {'actions': [{'action': 'create_pool', 'parameters': {'pool_name': 'measles_inference_pool', 'vm_size': 'Standard_D2_v2', 'container_image_name': 'measles-script:v1'}}, {'action': 'create_job', 'parameters': {'job_name': 'measles_inference_job', 'pool_name': 'measles_inference_pool'}}, {'action': 'add_task', 'parameters': {'job_name': 'measles_inference_job', 'command_line': 'python measles_inference.py --input-folder /mnt/batch/tasks/shared/LS_root/mounts/clusters/measles-inference/code/Users/input-test'}}, {'action': 'schedule_job', 'parameters': {'job_name': 'measles_inference_job', 'recurrence': {'frequency': 'daily', 'interval': 1, 'time_of_day': '21:00', 'days': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']}}}]}
Executing the plan...

Executing action: create_pool with parameters: {'pool_name': 'measles_inference_pool', 'vm_size': 'Standard_D2_v2', 'container_image_name': 'measles-script:v1'}
Executing action: create_job with parameters: {'job_name': 'measles_inference_job', 'pool_name': 'measles_inference_pool'}
Executing action: add_task with parameters: {'job_name': 'measles_inference_job', 'command_line': 'python measles_inference.py --input-folder /mnt/batch/tasks/shared/LS_root/mounts/clusters/measles-inference/code/Users/input-test'}
Executing action: schedule_job with parameters: {'job_name': 'measles_inference_job', 'recurrence': {'frequency': 'daily', 'interval': 1, 'time_of_day': '21:00', 'days': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']}}
```
