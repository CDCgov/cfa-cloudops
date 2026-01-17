import os

from cfa.cloudops import ContainerAppClient

client = ContainerAppClient(use_federated=True)

job_name = os.getenv("CFA_JOB_NAME", "cfa-job-test-v1")
client.start_job(job_name=job_name)
