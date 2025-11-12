import logging
import os

import dotenv
from azure.identity import ManagedIdentityCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import (
    JobExecutionContainer,
    JobExecutionTemplate,
)
from azure.mgmt.resource import SubscriptionClient

logger = logging.getLogger(__name__)


class ContainerAppClient:
    """
    Client for managing Azure Container Apps jobs via the Azure SDK.

    Provides methods to list, start, and inspect jobs in a resource group using
    managed identity authentication. Supports job info retrieval, command inspection,
    job existence checks, and flexible job start options.
    """

    def __init__(
        self,
        dotenv_path=None,
        resource_group=None,
        subscription_id=None,
        job_name=None,
    ):
        """
        Initialize a ContainerAppClient for Azure Container Apps jobs.

        Args:
            dotenv_path (str | None): Path to a .env file to load environment variables. Optional.
            resource_group (str | None): Azure resource group name. If None, uses env var AZURE_RESOURCE_GROUP_NAME. Optional.
            subscription_id (str | None): Azure subscription ID. If None, uses env var AZURE_SUBSCRIPTION_ID. Optional.
            job_name (str | None): Job name for Container App Job. Optional.

        Raises:
            ValueError: If required parameters are missing and not set in environment variables.
        """
        logger.debug("Initializing ContainerAppClient.")
        logger.debug("Setting up Managed Identity Credential.")
        self.credential = ManagedIdentityCredential()
        logger.debug("Loading environment variables from .env file if provided.")
        dotenv.load_dotenv(dotenv_path)
        logger.debug("Fetching subscription information.")
        sub_c = SubscriptionClient(self.credential)
        # pull in account info and save to environment vars
        logger.debug("Pulling account info from subscription client.")
        account_info = list(sub_c.subscriptions.list())[0]
        os.environ["AZURE_SUBSCRIPTION_ID"] = account_info.subscription_id
        os.environ["AZURE_TENANT_ID"] = account_info.tenant_id
        os.environ["AZURE_RESOURCE_GROUP_NAME"] = account_info.display_name
        if resource_group is None:
            resource_group = os.getenv("AZURE_RESOURCE_GROUP_NAME")
            logger.debug("Resource group pulled from environment variables.")
            if resource_group is None:
                logger.error("No resource group found in environment variables.")
                raise ValueError(
                    "No resource_group provided and no RESOURCE_GROUP env var found."
                )
        self.resource_group = resource_group
        logger.debug("Resource group set. ")
        if subscription_id is None:
            logger.debug("Resource group not provided, checking environment variables.")
            subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")
            logger.debug("Subscription ID pulled from environment variables.")
            if subscription_id is None:
                logger.error("No subscription ID found in environment variables.")
                raise ValueError(
                    "No subscription_id provided and no AZURE_SUBSCRIPTION_ID env var found."
                )
        self.subscription_id = subscription_id
        self.job_name = job_name

        logger.debug("Initializing ContainerAppsAPIClient.")
        self.client = ContainerAppsAPIClient(
            credential=self.credential, subscription_id=subscription_id
        )
        logger.debug("Client initialized.")
        logger.info(
            f"ContainerAppClient initialized for resource group '{self.resource_group}'."
        )

    def get_job_info(self, job_name: str | None = None):
        """
        Retrieve detailed information about a specific Container App job.

        Args:
            job_name (str | None): Name of the job to retrieve information for. If None, uses instance job_name. Optional.

        Returns:
            dict: Dictionary containing job details.
        """
        if job_name is None:
            logger.debug("Job name not provided, checking instance variable.")
            if self.job_name is None:
                logger.error("No job name provided.")
                raise ValueError("Please specify a job name.")
            job_name = self.job_name
            logger.debug(f"Job name {self.job_name} pulled from instance variable.")

        jobs = {
            i.name: i
            for i in self.client.jobs.list_by_resource_group(self.resource_group)
        }
        job_info = jobs.get(job_name)
        if job_info is None:
            logger.error(
                f"Job '{job_name}' not found in resource group '{self.resource_group}'."
            )
            raise ValueError(f"Job '{job_name}' not found.")
        logger.info(f"Retrieved info for job '{job_name}'.")
        return job_info.as_dict()

    def get_command_info(self, job_name: str | None = None):
        """
        Get command, image, and environment details for containers in a job.

        Args:
            job_name (str | None): Name of the job to inspect. If None, uses instance job_name. Optional.

        Returns:
            list[dict]: List of container info dicts (name, image, command, args, env).
        """
        if job_name is None:
            logger.debug("Job name not provided, checking instance variable.")
            if self.job_name is None:
                logger.error("No job name provided.")
                raise ValueError("Please specify a job name.")
            job_name = self.job_name
            logger.debug(f"Job name {self.job_name} pulled from instance variable.")

        jobs = {
            i.name: i
            for i in self.client.jobs.list_by_resource_group(self.resource_group)
        }
        job_info = jobs.get(job_name)
        if job_info is None:
            logger.error(
                f"Job '{job_name}' not found in resource group '{self.resource_group}'."
            )
            raise ValueError(f"Job '{job_name}' not found.")
        logger.info(f"Retrieved command info for job '{job_name}'.")
        logger.debug("Extracting container information.")
        c_info = job_info.template.containers
        container_dicts = [
            {
                "job_name": c.name,
                "image": c.image,
                "command": c.command,
                "args": c.args,
                "env": c.env,
            }
            for c in c_info
        ]
        logger.debug("Built container info list.")
        return container_dicts

    def list_jobs(self):
        """
        List all Container App job names in the resource group.

        Returns:
            list[str]: List of job names.
        """
        logger.debug("Listing all jobs in the resource group.")
        job_list = [
            i.name for i in self.client.jobs.list_by_resource_group(self.resource_group)
        ]
        logger.info(
            f"Listed {len(job_list)} jobs in resource group '{self.resource_group}'."
        )
        logger.debug(f"Found jobs: {job_list}")
        return job_list

    def check_job_exists(self, job_name: str):
        """
        Check if a Container App job exists in the resource group.

        Args:
            job_name (str): Name of the job to check.

        Returns:
            bool: True if job exists, False otherwise.
        """
        logger.debug(f"Checking existence of job {job_name}.")
        if job_name in self.list_jobs():
            logger.info(f"Job '{job_name}' exists.")
            return True
        else:
            logger.info(f"Container App Job {job_name} not found.")
            return False

    def start_job(
        self,
        job_name: str | None = None,
        command: list[str] | None = None,
        args: list[str] | None = None,
        env: list[str] | None = None,
    ):
        """
        Start a Container App job, optionally overriding command, args, or environment.

        Args:
            job_name (str | None): Name of the job to start. If None, uses instance job_name. Optional.
            command (list[str] | None): Command to run in the container. Optional.
            args (list[str] | None): Arguments for the command. Optional.
            env (list[str] | None): Environment variables for the container. Optional.

        Raises:
            ValueError: If required parameters are missing or not in correct format.
        """
        logger.debug("Checking job name.")
        if job_name is None:
            logger.debug("Job name not provided, checking instance variable.")
            if self.job_name is None:
                logger.error("No job name provided.")
                raise ValueError("Please specify a job name.")
            else:
                job_name = self.job_name
                logger.debug(f"Job name {self.job_name} pulled from instance variable.")
        if not command and not args and not env:
            logger.debug("submitting job start request.")
            self.client.jobs.begin_start(
                resource_group_name=self.resource_group, job_name=job_name
            )
            logger.info(f"Started job '{job_name}'.")
        else:
            # raise error if command/args/env not lists
            if command is not None and not isinstance(command, list):
                logger.error("Command is not in list format.")
                raise ValueError("Command must be in list format.")
            if args is not None and not isinstance(args, list):
                logger.error("Args not in list format.")
                raise ValueError("Args must be in list format.")
            if env is not None and not isinstance(env, list):
                logger.error("Env not in list format.")
                raise ValueError("Env must be in list format.")
            new_containers = []
            logger.debug("Gathering job info.")
            for i in self.client.jobs.list_by_resource_group(self.resource_group):
                if i.name == job_name:
                    logger.debug(
                        f"Job {job_name} found, preparing to start with overrides."
                    )
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
            try:
                self.client.jobs.begin_start(
                    resource_group_name=self.resource_group,
                    job_name=job_name,
                    template=t,
                )
                logger.info(f"Started job '{job_name}' with custom template.")
                logger.debug("Job start request submitted successfully.")
                print(f"Started job {job_name}.")
            except Exception as e:
                logger.error(f"Failed to start job {job_name}: {e}")
                raise

    def stop_job(self, job_name: str, job_execution_name: str):
        """
        Stop a specific execution of an Azure Container App Job.

        Args:
            job_name (str): Name of the Container App Job.
            job_execution_name (str): Name of the job execution to stop.

        Returns:
            Any: Response object from the Azure SDK if successful, or None if an error occurs.

        Raises:
            Exception: If the stop operation fails.
        """
        try:
            response = self.client.jobs.begin_stop_execution(
                resource_group_name=self.resource_group,
                job_name=job_name,
                job_execution_name=job_execution_name,
            ).result()
            print(
                f"Job execution '{job_execution_name}' for job '{job_name}' stopped successfully."
            )
            logger.info(
                f"Stopped job execution '{job_execution_name}' for job '{job_name}'."
            )
            return response
        except Exception as e:
            logger.error(f"Error stopping job execution: {e}")
            return None
