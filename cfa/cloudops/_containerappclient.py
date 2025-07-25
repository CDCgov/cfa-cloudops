import logging
import os

from azure.identity import ManagedIdentityCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import (
    JobExecutionContainer,
    JobExecutionTemplate,
)

logger = logging.getLogger(__name__)


class ContainerAppClient:
    # allow for using env vars or config to instantiate
    def __init__(
        self,
        resource_group=None,
        subscription_id=None,
        job_name=None,
    ):
        if resource_group is None:
            resource_group = os.getenv("AZURE_RESOURCE_GROUP_NAME")
            if resource_group is None:
                raise ValueError(
                    "No resource_group provided and no RESOURCE_GROUP env var found."
                )
        self.resource_group = resource_group
        if subscription_id is None:
            subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
            if subscription_id is None:
                raise ValueError(
                    "No subscription_id provided and no AZURE_SUBSCRIPTION_ID env var found."
                )
        self.subscription_id = subscription_id
        self.job_name = job_name
        self.credential = ManagedIdentityCredential()

        self.client = ContainerAppsAPIClient(
            credential=self.credential, subscription_id=subscription_id
        )
        logger.debug("client initialized.")

    def get_job_info(self, job_name):
        for i in self.client.jobs.list_by_resource_group(self.resource_group):
            if i.name == job_name:
                job_info = i
        return job_info.as_dict()

    def get_command_info(self, job_name):
        for i in self.client.jobs.list_by_resource_group(self.resource_group):
            if i.name == job_name:
                job_info = i
        c_info = job_info.__dict__["template"].__dict__["containers"]
        container_dicts = []
        for c in c_info:
            container_dict = {
                "job_name": c.name,
                "image": c.image,
                "command": c.command,
                "args": c.args,
                "env": c.env,
            }
            container_dicts.append(container_dict)
        return container_dicts

    def list_jobs(self):
        job_list = [
            i.name
            for i in self.client.jobs.list_by_resource_group(
                self.resource_group
            )
        ]
        return job_list

    def check_job_exists(self, job_name):
        if job_name in self.list_jobs():
            return True
        else:
            logger.info(f"Container App Job {job_name} not found.")
            return False

    def start_job(
        self,
        job_name: str = None,
        command: list[str] = None,
        args: list[str] = None,
        env: list[str] = None,
    ):
        if job_name is None:
            if self.job_name is None:
                raise ValueError("Please specify a job name.")
            else:
                job_name = self.job_name
        if not command and not args and not env:
            logger.debug("submitting job start request.")
            self.client.jobs.begin_start(
                resource_group_name=self.resource_group, job_name=job_name
            )
        else:
            # raise error if command/args/env not lists
            if command is not None and not isinstance(command, list):
                raise ValueError("Command must be in list format.")
            if args is not None and not isinstance(args, list):
                raise ValueError("Args must be in list format.")
            if env is not None and not isinstance(env, list):
                raise ValueError("Env must be in list format.")
            new_containers = []
            for i in self.client.jobs.list_by_resource_group(
                self.resource_group
            ):
                if i.name == job_name:
                    job_info = i
            for c in job_info.__dict__["template"].__dict__["containers"]:
                image = c.image
                name = c.name
                resources = c.resources
                container = JobExecutionContainer(
                    image=image,
                    name=name,
                    command=command,
                    args=args,
                    env=env,
                    resources=resources,
                )
                new_containers.append(container)
            t = JobExecutionTemplate(containers=new_containers)
            logger.debug("submitting job start request.")
            self.client.jobs.begin_start(
                resource_group_name=self.resource_group,
                job_name=job_name,
                template=t,
            )
