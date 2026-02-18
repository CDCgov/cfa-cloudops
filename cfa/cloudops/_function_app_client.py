import inspect
import logging
import os
import pathlib
import shutil
import subprocess
import time
from typing import List, Tuple

from azure.mgmt.web import WebSiteManagementClient

from .auth import (
    DefaultCredentialHandler,
    EnvCredentialHandler,
    SPCredentialHandler,
)

logger = logging.getLogger(__name__)

SLEEP_INTERVAL_SECONDS = 5


class FunctionAppClient:
    def __init__(
        self,
        keyvault: str = None,
        dotenv_path: str = None,
        use_sp: bool = False,
        use_federated: bool = False,
        force_keyvault: bool = False,
        **kwargs,
    ):
        logger.debug("Initializing CloudClient.")
        if keyvault is None:
            dotenv_path = dotenv_path or ".env"
        if keyvault is None and force_keyvault:
            logger.error(
                "Keyvault information not found but force_keyvault set to True."
            )
            raise ValueError("Keyvault information is required but not found.")
        # authenticate to get credentials
        if not use_sp and not use_federated:
            self.cred = EnvCredentialHandler(
                dotenv_path=dotenv_path,
                keyvault=keyvault,
                force_keyvault=force_keyvault,
                **kwargs,
            )
            self.method = "env"
            logger.info("Using managed identity credentials.")
        elif use_federated:
            self.cred = DefaultCredentialHandler(
                dotenv_path=dotenv_path,
                keyvault=keyvault,
                force_keyvault=force_keyvault,
                **kwargs,
            )
            self.method = "default"
            logger.info("Using default credentials.")
        else:
            self.cred = SPCredentialHandler(
                dotenv_path=dotenv_path,
                keyvault=keyvault,
                force_keyvault=force_keyvault,
                **kwargs,
            )
            self.method = "sp"
            logger.info("Using service principal credentials.")
        # get clients
        logger.debug("Getting Azure clients and setting other attributes.")

    def _log_into_portal(self) -> bool:
        try:
            subprocess.run(
                [
                    "az",
                    "login",
                    "--service-principal",
                    "-t",
                    self.cred.azure_tenant_id,
                    "-u",
                    self.cred.azure_client_id,
                    "-p",
                    self.cred.azure_client_secret,
                ],
                check=True,
            )

            logger.info("Logged into Azure Portal successfully ")
            time.sleep(SLEEP_INTERVAL_SECONDS)
            subprocess.run(
                ["az", "account", "set", "-s", self.cred.azure_subscription_id],
                check=True,
            )
            logger.info(
                "cfaazurefunction.log_into_portal(): Logged into Azure Portal successfully "
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"cfaazurefunction.log_into_portal(): Error logging into Azure Portal.{e}"
            )
            return False

    def _find_available_function_app(self, service_principal_credentials: str) -> str:
        # TODO: Query the table
        # TODO: If unable to access table, then list the ones from cloud
        # TODO: Check if health check is enabled for each function
        available_function_app = None
        web_mgmt_client = WebSiteManagementClient(
            self.cred.client_secret_sp_credential, self.cred.azure_subscription_id
        )
        function_app_details = web_mgmt_client.web_apps.list()
        candidate_function_apps = []
        for function_app in function_app_details:
            if function_app.tags:
                if (
                    "Purpose" in function_app.tags
                    and function_app.tags["Purpose"] == "Run Scheduled Job"
                ):
                    candidate_function_apps.append(function_app.name)
                if (
                    "purpose" in function_app.tags
                    and function_app.tags["purpose"] == "Run Scheduled Job"
                ):
                    candidate_function_apps.append(function_app.name)
        available_function_apps = []
        for candidate in candidate_function_apps:
            function_app_config = web_mgmt_client.web_apps.get_configuration(
                self.cred.azure_resource_group_name, candidate
            )
            if not function_app_config.health_check_path:
                available_function_apps.append(candidate)
        if available_function_apps:
            available_function_app = available_function_apps[0]
        return available_function_app

    def _allocate_function_app(self, function_name) -> bool:
        # TODO: Update the table
        return True

    def _enable_health_check(self, function_name):
        try:
            subprocess.run(
                [
                    "az",
                    "functionapp",
                    "config",
                    "set",
                    "--name",
                    function_name,
                    "--resource-group",
                    self.cred.azure_resource_group_name,
                    "--generic-configurations",
                    '{"healthCheckPath": "/api/HealthCheck"}',
                ],
                check=True,
            )
            logger.info(
                "cfaazurefunction.enable_health_check(): Function health check enabled."
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"cfaazurefunction.enable_health_check(): Error updating health check {e}"
            )
            return False

    def _update_app_settings(self, function_name, settings: List[Tuple[str, str]]):
        try:
            arguments = [
                "az",
                "functionapp",
                "config",
                "appsettings",
                "set",
                "--name",
                function_name,
                "--resource-group",
                self.cred.azure_resource_group_name,
                "--settings",
            ]
            for key, value in settings:
                arguments.append(f"{key}={value}")
            subprocess.run(arguments, check=True)
            logger.info(
                "cfaazurefunction.update_app_settings(): Function app settings updated successfully."
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"cfaazurefunction.update_app_settings(): Error updating app settings for Function App {e}"
            )
            return False

    def _add_user_package_to_deployment(self, user_package):
        if isinstance(user_package, str):
            source_code = user_package
        else:
            source_code = inspect.getsource(user_package)
        with open("user_package.py", "w") as f:
            f.write(source_code)
            f.write(f"\n{user_package.__name__}()")

    def _delete_deployment_folder(self, function_name: str) -> bool:
        try:
            shutil.rmtree(function_name)
            return True
        except Exception:
            return False

    def _copy_template_to_deployment(self, parent_folder: str, function_name: str):
        template_folder = f"{parent_folder}/template/"
        python_scripts = [
            "timer_blueprint",
            "function_app",
            "containers",
            "cfa_service",
        ]
        for script_file in python_scripts:
            shutil.copyfile(
                f"{template_folder}{script_file}.txt",
                f"{function_name}/{script_file}.py",
            )
        json_files = [
            "host",
            "local.settings",
        ]
        for json_file in json_files:
            shutil.copyfile(
                f"{template_folder}{json_file}.txt", f"{function_name}/{json_file}.json"
            )
        shutil.copyfile(
            f"{template_folder}requirements.txt", f"{function_name}/requirements.txt"
        )

    # Publish the function
    def _publish_function(
        self,
        function_name: str,
        schedule: str,
        user_package: any,
        dependencies: List[str] = None,
        environment_variables: List[Tuple[str, str]] = None,
    ):
        try:
            # First delete any function app folders with same name from previous runs
            fam_package_folder = pathlib.Path(__file__).parent.resolve()
            self._delete_deployment_folder(function_name)
            os.makedirs(f"{function_name}/bin")
            os.makedirs(f"{function_name}/python_packages/lib/site-packages")
            # Copy all files from template subfolder to the destination function app folder
            self._copy_template_to_deployment(
                parent_folder=fam_package_folder, function_name=function_name
            )
            # Now switch to the function app folde
            parent_path = os.getcwd()
            os.chdir(f"{parent_path}/{function_name}")
            # Append dependencies (one per line) to {function_name}/requirements.txt')
            if dependencies:
                with open("requirements.txt", "a") as f:
                    f.write("\n".join(dependencies))
            # Create a new file that contains source code of user package
            self._add_user_package_to_deployment(user_package)

            subprocess.run(
                ["func", "azure", "functionapp", "publish", function_name], check=True
            )

            # Delete the temporary folder created for function app deployment
            os.chdir(parent_path)
            self._delete_deployment_folder(function_name)
            logger.info(
                "cfaazurefunction.publish_function(): Function app published successfully."
            )

            # Now update the schedule in function app
            self._update_app_settings(
                function_name,
                [
                    ("CFANotificationV2CRON", schedule),
                    ("WEBSITE_RUN_FROM_PACKAGE", "1"),
                    ("WEBSITES_ENABLE_APP_SERVICE_STORAGE", "false"),
                ],
            )

            if environment_variables:
                self._update_app_settings(function_name, environment_variables)

            logger.info(
                "FunctionAppClient._publish_function(): Function app settings updated."
            )
            self._enable_health_check(function_name)
            time.sleep(SLEEP_INTERVAL_SECONDS)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient._publish_function(): Error publishing Function App: {e}"
            )
            if os.path.exists(f"{parent_path}/{function_name}"):
                os.chdir(parent_path)
                self._delete_deployment_folder(function_name)
            return False

    def _restart_function(self, function_name: str):
        try:
            subprocess.run(
                [
                    "az",
                    "functionapp",
                    "restart",
                    "--name",
                    function_name,
                    "--resource-group",
                    self.cred.azure_resource_group_name,
                ],
                check=True,
            )
            time.sleep(SLEEP_INTERVAL_SECONDS)
            logger.info(
                "FunctionAppClient._restart_function(): Function app restarted successfully."
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient._restart_function(): Error restarting Function App {e}"
            )
            return False

    def deploy_function(
        self,
        schedule: str,
        user_package,
        dependencies: List[str] = None,
        environment_variables: List[Tuple[str, str]] = None,
    ) -> bool:
        """Deploy a function app and configure it to run on specified schedule.

        Args:
            schedule (str): Chron schedule for running function app.
            user_package: Source code to deploy.
            dependencies (list, optional): List of dependent packages
            environment_variables (list, optional): WList of (key,value) pairs to be set
                as environment variables.

        Example:
            Define inline code to be executed in Function App

                code = '''
                    def my_function():
                        print(f"I reached here {2+2}")
                        return 2+2
                '''
            function_app_client = FunctionAppClient()
            cron_schedule = "*/5 30 * * * *"
            function_app_client.deploy_function(cron_schedule, code)
        """
        if not self._log_into_portal():
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment aborted due to login failure."
            )
            return False
        function_name = self._find_available_function_app()
        if not function_name:
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment aborted because no function apps are available."
            )
            return False
        if not self._publish_function(
            function_name, schedule, user_package, dependencies, environment_variables
        ):
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment did not complete because Function App publish operation failed."
            )
            return False
        # ToDo: Uncomment this when we have a table available for storing funcion app mappings
        # if not allocate_function_app(function_name):
        #    logger.info('cfaazurefunction.deploy_function(): Unable to assign function app to user provided applicaion.')
        if not self._restart_function(function_name):
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment was completed however Function App restart operation failed."
            )
            return False
        logger.info("Deployment complete")
        return True
