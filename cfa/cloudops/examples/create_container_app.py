import random
import string

from cfa.cloudops import ContainerAppClient

client = ContainerAppClient()
job_name = "my-job"
if not client.check_job_exists(job_name):
    client.start_job(
        job_name=job_name,
        command=["python", "main.py"],
        env=[{"name": "APP_MESSAGE", "value": "This is a container app job!"}],
    )

characters = string.ascii_letters + string.digits
random_string = "".join(random.choices(characters, k=5))

if client.check_job_exists(job_name):
    # Stop a job
    client.stop_job(job_name=job_name, job_execution_name=f"{job_name}-{random_string}")
