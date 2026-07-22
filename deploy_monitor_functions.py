import monitor_functions as mf
from cfa.cloudops import FunctionAppClient

EVERY_30_SEC = "*/30 * * * * *"
EVERY_10_MIN = "0 */10 * * * *"
DAILY = "45 23 * * *"


def deploy_monitor_functions(dotenv_path: str):
    func_app_client = FunctionAppClient(
        function_app_name="cfapredictafmprdfunc10",
        dotenv_path=dotenv_path,
        use_sp=True,
        update_function_database=False,
    )
    func_app_client.cred.azure_resource_group_name = "EXT-EDAV-CFA-PRD"

    func_app_client.deploy_function(
        schedule=DAILY,
        user_package=mf.main,
        dependencies=["azure-identity", "azure-mgmt-web", "requests"],
        environment_variables=[
            ("SUBSCRIPTION_ID", "replace_with_subscription_id"),
            ("AZURE_CLIENT_ID", "replace_with_azure_client_id"),
            ("AZURE_CLIENT_SECRET", "replace_with_service_principal_secret"),
            ("AZURE_TENANT_ID", "replace_with_tenant_id"),
            ("SENDGRID_API_KEY", "replace_with_sendgrid_api_key"),
            ("EMAIL_SENDER", "function_app_monitor@cdc.gov"),  # Do not change this
            (
                "RECIPIENT_LIST",
                "ODORRCFATIDAzureNotifications@cdc.gov",
            ),  # Do not change this
        ],
    )


if __name__ == "__main__":
    deploy_monitor_functions(dotenv_path="metaflow.env")
