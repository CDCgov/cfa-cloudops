import inspect
import logging
import os
import pathlib
import shutil
import subprocess
import time
from typing import Callable, List, Optional, Tuple

import duckdb
from azure.identity import DefaultAzureCredential
from azure.mgmt.web import WebSiteManagementClient

from .auth import (
    DefaultCredentialHandler,
    EnvCredentialHandler,
    SPCredentialHandler,
)

logger = logging.getLogger(__name__)

SLEEP_INTERVAL_SECONDS = 5
FUNCTION_APPS_CSV_PATH = "az://input-test/data/cfa_predict_function_apps.csv"


class FunctionAppClient:
    @classmethod
    def get_configuration(
        cls,
        function_app_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> Optional[dict]:
        resource_group = resource_group or os.getenv("AZURE_RESOURCE_GROUP")
        if resource_group is None:
            raise ValueError(
                "Resource group must be provided either as an argument or through the AZURE_RESOURCE_GROUP environment variable."
            )
        subscription_id = subscription_id or os.getenv("AZURE_SUBSCRIPTION_ID")
        if subscription_id is None:
            raise ValueError(
                "Subscription ID must be provided either as an argument or through the AZURE_SUBSCRIPTION_ID environment variable."
            )
        credential = DefaultAzureCredential()
        web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
        return web_mgmt_client.web_apps.get_configuration(
            resource_group, function_app_name
        )

    @classmethod
    def get_tags(
        cls,
        function_app_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> Optional[dict]:
        return cls.get_configuration(
            function_app_name, resource_group, subscription_id
        ).additional_properties.get("tags", [])

    @classmethod
    def get_health_check_flag(
        cls,
        function_app_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> Optional[dict]:
        return (
            cls.get_configuration(
                function_app_name, resource_group, subscription_id
            ).health_check_path
            is not None
        )

    @classmethod
    def list_functions(
        cls,
        function_app_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> Optional[dict]:
        resource_group = resource_group or os.getenv("AZURE_RESOURCE_GROUP")
        if resource_group is None:
            raise ValueError(
                "Resource group must be provided either as an argument or through the AZURE_RESOURCE_GROUP environment variable."
            )
        subscription_id = subscription_id or os.getenv("AZURE_SUBSCRIPTION_ID")
        if subscription_id is None:
            raise ValueError(
                "Subscription ID must be provided either as an argument or through the AZURE_SUBSCRIPTION_ID environment variable."
            )
        credential = DefaultAzureCredential()
        web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
        function_list = []
        for function in web_mgmt_client.web_apps.list_functions(
            resource_group, function_app_name
        ):
            function_list.append(function.as_dict())
        return function_list

    @classmethod
    def list_production_deployment_status(
        cls,
        function_app_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ):
        resource_group = resource_group or os.getenv("AZURE_RESOURCE_GROUP")
        if resource_group is None:
            raise ValueError(
                "Resource group must be provided either as an argument or through the AZURE_RESOURCE_GROUP environment variable."
            )
        subscription_id = subscription_id or os.getenv("AZURE_SUBSCRIPTION_ID")
        if subscription_id is None:
            raise ValueError(
                "Subscription ID must be provided either as an argument or through the AZURE_SUBSCRIPTION_ID environment variable."
            )
        credential = DefaultAzureCredential()
        web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
        statuses = web_mgmt_client.web_apps.list_production_site_deployment_statuses(
            resource_group, function_app_name
        )
        return [
            (status.status, status.kind, status.type, status.name)
            for status in statuses
        ]

    @classmethod
    def list_slots(
        cls,
        function_app_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> list[str]:
        resource_group = resource_group or os.getenv("AZURE_RESOURCE_GROUP")
        if resource_group is None:
            raise ValueError(
                "Resource group must be provided either as an argument or through the AZURE_RESOURCE_GROUP environment variable."
            )
        subscription_id = subscription_id or os.getenv("AZURE_SUBSCRIPTION_ID")
        if subscription_id is None:
            raise ValueError(
                "Subscription ID must be provided either as an argument or through the AZURE_SUBSCRIPTION_ID environment variable."
            )
        credential = DefaultAzureCredential()
        web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
        slots = web_mgmt_client.web_apps.list_slots(resource_group, function_app_name)
        return [
            (
                slot.name,
                slot.state,
                slot.enabled,
                slot.target_swap_slot,
                slot.slot_swap_status,
            )
            for slot in slots
        ]

    @classmethod
    def get_function_details(
        cls,
        function_app_name: str,
        function_name: str,
        resource_group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> Optional[dict]:
        resource_group = resource_group or os.getenv("AZURE_RESOURCE_GROUP")
        if resource_group is None:
            raise ValueError(
                "Resource group must be provided either as an argument or through the AZURE_RESOURCE_GROUP environment variable."
            )
        subscription_id = subscription_id or os.getenv("AZURE_SUBSCRIPTION_ID")
        if subscription_id is None:
            raise ValueError(
                "Subscription ID must be provided either as an argument or through the AZURE_SUBSCRIPTION_ID environment variable."
            )
        credential = DefaultAzureCredential()
        web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
        return web_mgmt_client.web_apps.get_function(
            resource_group, function_app_name, function_name
        )

    def __init__(
        self,
        function_app_name: Optional[str] = None,
        keyvault: Optional[str] = None,
        dotenv_path: Optional[str] = None,
        use_sp: bool = False,
        use_federated: bool = False,
        force_keyvault: bool = False,
        **kwargs,
    ):
        logger.debug("Initializing FunctionAppClient.")
        self.function_app_name = function_app_name
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
        self.conn = None

    def _get_database_connection(self):
        if not self.conn:
            # Initialize DuckDB and enable the HTTPFS extension
            self.conn = duckdb.connect(database=":memory:")  # In-memory database
            self.conn.sql("SET azure_transport_option_type='curl';")
            self.conn.sql(
                f"CREATE SECRET IF NOT EXISTS azure_spn \
                        ( \
                            TYPE AZURE, \
                            PROVIDER SERVICE_PRINCIPAL, \
                            TENANT_ID '{self.cred.azure_tenant_id}', \
                            CLIENT_ID '{self.cred.azure_client_id}', \
                            CLIENT_SECRET '{self.cred.azure_client_secret}', \
                            ACCOUNT_NAME '{self.cred.azure_blob_storage_account}' \
                );"
            )
            query = f"CREATE TABLE function_apps AS SELECT * FROM '{FUNCTION_APPS_CSV_PATH}'"
            self._get_database_connection().sql(query)
        return self.conn

    def _clone_deployment_slot(self, slot_name: str, source_slot: Optional[str] = None):
        try:
            arguments = [
                "az",
                "functionapp",
                "deployment",
                "slot",
                "create",
                "--name",
                self.function_app_name,
                "--resource-group",
                self.cred.azure_resource_group_name,
                "--slot",
                slot_name,
            ]
            if source_slot and source_slot.lower() != "production":
                arguments.extend(
                    [
                        "--configuration-source",
                        f"{self.function_app_name}/{source_slot}",
                    ]
                )
                source_slot_name_caption = source_slot
            else:
                arguments.extend(["--configuration-source", self.function_app_name])
                source_slot_name_caption = "production"

            logger.info(
                f"FunctionAppClient._clone_deployment_slot(): Cloning deployment slot {source_slot_name_caption} to slot {slot_name}..."
            )

            subprocess.run(arguments, check=True)

            logger.info(
                f"FunctionAppClient._clone_deployment_slot(): Successfully cloned deployment slot {source_slot_name_caption} to slot {slot_name}."
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient._clone_deployment_slot(): Error cloning deployment slot.{e}"
            )
            return False

    def _swap_deployment_slot(self, source_slot: str, target_slot: str):
        web_mgmt_client = WebSiteManagementClient(
            self.cred.client_secret_credential, self.cred.azure_subscription_id
        )
        if target_slot.lower() == "production":
            return web_mgmt_client.web_apps.begin_swap_slot_with_production(
                resource_group_name=self.cred.azure_resource_group_name,
                name=self.function_app_name,
                slot=source_slot,
            ).result()
        return web_mgmt_client.web_apps.begin_swap_slot(
            resource_group_name=self.cred.azure_resource_group_name,
            name=self.function_app_name,
            slot=source_slot,
            slot_swap_entity=target_slot,
        ).result()

    def _delete_deployment_slot(self, deployment_slot_name: str):
        web_mgmt_client = WebSiteManagementClient(
            self.cred.client_secret_credential, self.cred.azure_subscription_id
        )
        logger.info(
            f"FunctionAppClient._delete_deployment_slot: Deleting the {deployment_slot_name} slot"
        )
        web_mgmt_client.web_apps.delete_slot(
            resource_group_name=self.cred.azure_resource_group_name,
            name=self.function_app_name,
            slot=deployment_slot_name,
            delete_metrics=True,
        )

    def _find_available_function_app(self) -> str:
        """List all function apps that are not currently in use and return the first one from this list

        Returns
        -------
        Function app name
        """
        available_function_app = None

        # Query the file directly from Azure Blob Storage
        query = (
            "SELECT FunctionAppName FROM function_apps WHERE IsDeployed = False LIMIT 1"
        )
        result = self._get_database_connection().sql(query).fetchdf()
        if not result.empty:
            available_function_app = result.iloc[0]["FunctionAppName"]
            logger.info(
                f"FunctionAppClient._find_available_function_app: Found available function app: {available_function_app}"
            )
        return available_function_app

    def _allocate_function_app(self):
        update_query = f"""
            UPDATE function_apps
            SET IsDeployed = True
            WHERE FunctionAppName = '{self.function_app_name}'
        """
        logger.info("Running update query to mark function app as deployed")
        self._get_database_connection().execute(update_query)
        self._get_database_connection().execute(f"""
            COPY function_apps
            TO '{FUNCTION_APPS_CSV_PATH}'
            (HEADER, DELIMITER ',', OVERWRITE 1)
        """)
        logger.info(
            f"FunctionAppClient._allocate_function_app: Assigned function app to user package: {self.function_app_name}"
        )
        return True

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
                "FunctionAppClient.log_into_portal(): Logged into Azure Portal successfully "
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient.log_into_portal(): Error logging into Azure Portal.{e}"
            )
            return False

    def _enable_health_check(self, slot: Optional[str] = None):
        try:
            arguments = [
                "az",
                "functionapp",
                "config",
                "set",
                "--name",
                self.function_app_name,
                "--resource-group",
                self.cred.azure_resource_group_name,
                "--generic-configurations",
                '{"healthCheckPath": "/api/HealthCheck"}',
            ]
            if slot:
                arguments.extend(["--slot", slot])

            logger.info(
                f"FunctionAppClient.enable_health_check(): Invoking the following arguments: {arguments}."
            )

            subprocess.run(arguments, check=True)

            logger.info(
                "FunctionAppClient.enable_health_check(): Function health check enabled."
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient.enable_health_check(): Error updating health check {e}"
            )
            return False

    def _update_app_settings(
        self, settings: List[Tuple[str, str]], slot: Optional[str] = None
    ):
        try:
            arguments = [
                "az",
                "functionapp",
                "config",
                "appsettings",
                "set",
                "--name",
                self.function_app_name,
                "--resource-group",
                self.cred.azure_resource_group_name,
                "--settings",
            ]
            if slot:
                arguments.extend(["--slot", slot])
            logger.info(
                f"FunctionAppClient.update_app_settings(): Updating app settings {arguments}"
            )
            for key, value in settings:
                arguments.append(f"{key}={value}")
            subprocess.run(arguments, check=True)
            logger.info(
                "FunctionAppClient.update_app_settings(): Function app settings updated successfully."
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient.update_app_settings(): Error updating app settings for Function App {e}"
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

    def _delete_deployment_folder(self) -> bool:
        try:
            shutil.rmtree(self.function_app_name)
            return True
        except Exception:
            return False

    def _copy_template_to_deployment(self, parent_folder: str):
        template_folder = f"{parent_folder}/template/"
        python_scripts = [
            "timer_blueprint",
            "function_app",
            "containers",
            "cfa_service",
            "user_package",
        ]
        for script_file in python_scripts:
            shutil.copyfile(
                f"{template_folder}{script_file}.txt",
                f"{self.function_app_name}/{script_file}.py",
            )
        json_files = [
            "host",
            "local.settings",
        ]
        for json_file in json_files:
            shutil.copyfile(
                f"{template_folder}{json_file}.txt",
                f"{self.function_app_name}/{json_file}.json",
            )
        shutil.copyfile(
            f"{template_folder}requirements.txt",
            f"{self.function_app_name}/requirements.txt",
        )

    # Publish the function
    def _publish_function(
        self,
        schedule: str,
        user_package: Callable,
        dependencies: List[str] = None,
        environment_variables: List[Tuple[str, str]] = None,
    ):
        """
        First clone production to rollback stage
        Then deploy to production slot
        Finally clone production to backup slot
        """
        try:
            # First delete any function app folders with same name from previous runs
            fam_package_folder = pathlib.Path(__file__).parent.resolve()
            self._delete_deployment_folder()

            os.makedirs(f"{self.function_app_name}/bin")
            os.makedirs(f"{self.function_app_name}/python_packages/lib/site-packages")
            # Copy all files from template subfolder to the destination function app folder
            self._copy_template_to_deployment(parent_folder=fam_package_folder)
            # Now switch to the function app folder
            parent_path = os.getcwd()
            os.chdir(f"{parent_path}/{self.function_app_name}")
            # Append dependencies (one per line) to {function_app_name}/requirements.txt')
            if dependencies:
                with open("requirements.txt", "a") as f:
                    f.write("\n".join(dependencies))
            # Create a new file that contains source code of user package
            self._add_user_package_to_deployment(user_package)

            # Check if the current production slot is healthy (i.e. something was deployed to it)
            # If yes, then first clone the rollback slot to rollback_previous
            # Then delete the rollback stage and clone production to rollback
            # Delete the rollback_previous if all went okay
            # Otherwise restore production from backup, clone rollback from rollback_previous and delete rollback_previous
            if FunctionAppClient.get_health_check_flag(
                self.function_app_name,
                self.cred.azure_resource_group_name,
                self.cred.azure_subscription_id,
            ):
                current_slots = FunctionAppClient.list_slots(
                    self.function_app_name,
                    self.cred.azure_resource_group_name,
                    self.cred.azure_subscription_id,
                )
                rollback_slot_exists = [
                    True
                    for (slot_name, _, slot_enabled, _, _) in current_slots
                    if slot_name.lower() == f"{self.function_app_name}/rollback"
                    and slot_enabled
                ]
                if rollback_slot_exists:
                    logger.info(
                        "FunctionAppClient._publish_function(): Rollback slot already exists. Cloning it to Rollback_Previous slot"
                    )
                    self._clone_deployment_slot("rollback_previous", "rollback")
                    self._delete_deployment_slot("rollback")

                self._clone_deployment_slot("rollback")

            subprocess.run(
                ["func", "azure", "functionapp", "publish", self.function_app_name],
                check=True,
            )

            # Delete the temporary folder created for function app deployment
            os.chdir(parent_path)
            self._delete_deployment_folder()
            logger.info(
                f"FunctionAppClient.publish_function(): Function app published successfully to {self.function_app_name}."
            )

            # Now update the schedule in function app
            self._update_app_settings(
                [
                    ("CFANotificationV2CRON", schedule),
                    ("WEBSITE_RUN_FROM_PACKAGE", "1"),
                    ("WEBSITES_ENABLE_APP_SERVICE_STORAGE", "false"),
                ],
            )

            if environment_variables:
                self._update_app_settings(environment_variables)

            logger.info(
                f"FunctionAppClient._publish_function(): Function app settings updated for {self.function_app_name}."
            )
            self._enable_health_check()
            time.sleep(SLEEP_INTERVAL_SECONDS)

            if FunctionAppClient.get_health_check_flag(
                self.function_app_name,
                self.cred.azure_resource_group_name,
                self.cred.azure_subscription_id,
            ):
                logger.info(
                    "FunctionAppClient._publish_function(): Production slot is healthy after deployment. Cloning production to backup slot"
                )

                backup_slot_exists = [
                    True
                    for (slot_name, _, slot_enabled, _, _) in current_slots
                    if slot_name.lower() == f"{self.function_app_name}/backup"
                    and slot_enabled
                ]
                if backup_slot_exists:
                    self._delete_deployment_slot("backup")

                self._clone_deployment_slot("backup")

                rollback_prev_slot_exists = [
                    True
                    for (slot_name, _, slot_enabled, _, _) in current_slots
                    if slot_name.lower()
                    == f"{self.function_app_name}/rollback_previous"
                    and slot_enabled
                ]
                if rollback_prev_slot_exists:
                    self._delete_deployment_slot("rollback_previous")

            else:
                current_slots = FunctionAppClient.list_slots(
                    self.function_app_name,
                    self.cred.azure_resource_group_name,
                    self.cred.azure_subscription_id,
                )
                rollback_slot_exists = [
                    True
                    for (slot_name, _, slot_enabled, _, _) in current_slots
                    if slot_name.lower() == f"{self.function_app_name}/rollback"
                    and slot_enabled
                ]
                if rollback_slot_exists:
                    self._delete_deployment_slot("rollback")

                rollback_prev_slot_exists = [
                    True
                    for (slot_name, _, slot_enabled, _, _) in current_slots
                    if slot_name.lower()
                    == f"{self.function_app_name}/rollback_previous"
                    and slot_enabled
                ]
                if rollback_prev_slot_exists:
                    self._clone_deployment_slot(
                        slot_name="rollback", source_slot="rollback_previous"
                    )
                    self._swap_deployment_slot(
                        source_slot="rollback_previous", target_slot="production"
                    )
                    self._delete_deployment_slot("rollback_previous")

            return True

        except subprocess.CalledProcessError as e:
            logger.error(
                f"FunctionAppClient._publish_function(): Error publishing Function App: {e}"
            )
            if os.path.exists(f"{parent_path}/{self.function_app_name}"):
                os.chdir(parent_path)
                self._delete_deployment_folder()
            return False

    def _restart_function(self):
        try:
            subprocess.run(
                [
                    "az",
                    "functionapp",
                    "restart",
                    "--name",
                    self.function_app_name,
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
        user_package: Callable,
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
            function_app_name='cfapredictafmprdfunc01'
            function_app_client = FunctionAppClient(function_app_name=function_app_name)
            cron_schedule = "*/5 30 * * * *"
            function_app_client.deploy_function(cron_schedule, code)
        """
        if not self._log_into_portal():
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment aborted due to login failure."
            )
            return False
        if not self.function_app_name:
            function_name = self._find_available_function_app()
            if not function_name:
                logger.error(
                    "FunctionAppClient.deploy_function(): Deployment aborted because no function apps are available. Please provision additional function apps."
                )
                return False
            self.function_app_name = function_name
        if not self._publish_function(
            schedule,
            user_package,
            dependencies,
            environment_variables,
        ):
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment did not complete because Function App publish operation failed."
            )
            return False
        if not self._allocate_function_app():
            logger.error(
                "FunctionAppClient.deploy_function(): Unable to assign function app to user provided application."
            )
        if not self._restart_function():
            logger.error(
                "FunctionAppClient.deploy_function(): Deployment was completed however Function App restart operation failed."
            )
            return False
        logger.info("Deployment complete")
        return True
