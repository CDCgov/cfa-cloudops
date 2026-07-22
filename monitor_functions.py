def main():

    try:
        import os
        import re
        import subprocess
        import sys
        import traceback

        import requests
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.web import WebSiteManagementClient

        subprocess.check_call([sys.executable, "-m", "pip", "install", "sendgrid"])

        import sendgrid
        from sendgrid.helpers.mail import Content, Email, Mail, To

        # Nested helper functions: all code inside this one top-level function per request.
        PREFIX = "cfapredictafmprdfunc"
        TAG_KEY = "Purpose"
        TAG_VALUE = "Run Scheduled Job"

        HTTP_TIMEOUT = 90
        SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
        EMAIL_SENDER = os.getenv("EMAIL_SENDER")
        RECIPIENT_LIST = os.getenv("RECIPIENT_LIST")

        def parse_resource_group_from_id(resource_id: str):
            m = re.search(r"/resourceGroups/([^/]+)/", resource_id, re.IGNORECASE)
            return m.group(1) if m else None

        def get_slot_name_from_slot_resource(slot_resource_name: str):
            if not slot_resource_name:
                return None
            m = re.search(r"/slots/([^/]+)$", slot_resource_name)
            if m:
                return m.group(1)
            parts = slot_resource_name.split("/")
            last = parts[-1]
            if "-" in last:
                return last.split("-", 1)[1]
            return last

        def get_slot_publishing_info(token, sub_id, rg, function_name, slot_name=None):
            if slot_name:
                url = f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.Web/sites/{function_name}/slots/{slot_name}/config/publishingcredentials/list?api-version=2023-01-01"
            else:
                url = f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.Web/sites/{function_name}/config/publishingcredentials/list?api-version=2023-01-01"

            headers = {"Authorization": f"Bearer {token}"}
            response = requests.post(url, headers=headers)
            if response.status_code == 200:
                pub_profile = response.json()
                user_name = pub_profile.get("properties", {}).get("publishingUserName")
                user_pass = pub_profile.get("properties", {}).get("publishingPassword")
            else:
                print(f"Error: {response.status_code} - {response.text}")
                return (None, None, None)
            return (user_name, user_pass)

        def get_deployment_info(
            function_app_name: str, pub_user: str, pub_pass: str, slot_name=None
        ):
            """Get deployment info from Kudu."""
            if slot_name:
                scm_url = f"https://{function_app_name}-{slot_name}.scm.cfa-prd-app.appserviceenvironment.net"
            else:
                scm_url = f"https://{function_app_name}.scm.cfa-prd-app.appserviceenvironment.net"

            # Get publishing credentials first (from your REST API call)
            url = f"{scm_url}/api/deployments"
            response = requests.get(
                url, auth=(pub_user, pub_pass), timeout=HTTP_TIMEOUT
            )

            if response.status_code == 200:
                deployments = response.json()
                if deployments:
                    return deployments[0].get("id")  # Latest deployment
            return None

        def send_email(from_addr, to_addr, subject, body):
            sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
            from_email = Email(from_addr)
            to_email = To(to_addr)
            subject = "Function App Health Report"
            content = Content("text/plain", body)
            mail = Mail(from_email, to_email, subject, content)
            sg.client.mail.send.post(request_body=mail.get())

        # Begin main logic
        credential = DefaultAzureCredential()
        sub_id_env = os.environ.get("SUBSCRIPTION_ID")
        subscription_ids = [sub_id_env]

        drift_apps = []
        no_drift_apps = []
        skipped_apps = []

        for sub_id in subscription_ids:
            print(f"Processing subscription {sub_id}...")
            web_client = WebSiteManagementClient(credential, sub_id)
            try:
                apps = web_client.web_apps.list()
            except Exception as e:
                print(f"Failed to list web apps in subscription {sub_id}: {e}")
                continue

            for app in apps:
                name = getattr(app, "name", "")
                if not name or not name.startswith(PREFIX):
                    continue
                tags = getattr(app, "tags", {}) or {}
                print(
                    f"Checking tags: {TAG_KEY}. Found values {tags.get(TAG_KEY)} in {name}"
                )
                if tags.get(TAG_KEY) != TAG_VALUE:
                    continue

                rg = parse_resource_group_from_id(app.id)
                if not rg:
                    skipped_apps.append((name, "no-rg"))
                    print(f"Could not parse resource group for {name}, skipping")
                    continue

                try:
                    slots = list(web_client.web_apps.list_slots(rg, name))
                except Exception as e:
                    print(f"Failed to list slots for {name}: {e}")
                    slots = []

                found_backup_slot = None
                for s in slots:
                    slot_name = get_slot_name_from_slot_resource(
                        getattr(s, "id", None) or getattr(s, "name", None)
                    )
                    if not slot_name:
                        continue
                    if slot_name.lower() == "backup" or slot_name.lower().endswith(
                        "backup"
                    ):
                        found_backup_slot = slot_name
                        break
                if not found_backup_slot:
                    skipped_apps.append((name, "No backup slot found"))
                    continue

                # Fallback to Kudu publishing credentials
                token = credential.get_token(
                    "https://management.azure.com/.default"
                ).token
                try:
                    prod_user, prod_pass = get_slot_publishing_info(
                        token, sub_id, rg, name
                    )
                except Exception as e:
                    print(f"Failed to get publishing credentials for {name}: {e}")
                    prod_user = prod_pass = None

                try:
                    slot_user, slot_pass = get_slot_publishing_info(
                        token, sub_id, rg, name, found_backup_slot
                    )
                except Exception as e:
                    print(
                        f"Failed to get publishing credentials for slot {found_backup_slot} of {name}: {e}"
                    )
                    slot_user = slot_pass = None

                if not (prod_user and prod_pass and slot_user and slot_pass):
                    skipped_apps.append((name, "no-publishing-creds"))
                    print(
                        f"Skipping {name}: missing publishing credentials for production or slot"
                    )
                    continue

                try:
                    prod_deployment_id = get_deployment_info(name, prod_user, prod_pass)
                    found_backup_slot_deployment_id = get_deployment_info(
                        name, slot_user, slot_pass, found_backup_slot
                    )
                    if prod_deployment_id == found_backup_slot_deployment_id:
                        no_drift_apps.append((name, "Kudu deployment Ids match"))
                    else:
                        drift_apps.append(
                            (
                                name,
                                f"Kudu deployment Ids differ prod='{prod_deployment_id}'"
                                f"backup=='{found_backup_slot_deployment_id}'",
                            )
                        )
                except Exception as e:
                    skipped_apps.append((name, f"kudu-failed:{e}"))

        # Compose report
        report_lines = []
        report_lines.append("Azure Function App Code Drift Report")
        report_lines.append("")
        report_lines.append(f"Prefix: {PREFIX}, Tag: {TAG_KEY}={TAG_VALUE}")
        report_lines.append("")
        report_lines.append("Apps with NO code drift (identical deployments):")
        if no_drift_apps:
            for name, reason in no_drift_apps:
                report_lines.append(f" - {name}: {reason}")
        else:
            report_lines.append(" - None")
        report_lines.append("")
        report_lines.append("Apps WITH code drift (different deployments):")
        if drift_apps:
            for name, reason in drift_apps:
                report_lines.append(f" - {name}: {reason}")
        else:
            report_lines.append(" - None")
        report_lines.append("")
        report_lines.append("Skipped apps (not checked) and reasons:")
        if skipped_apps:
            for name, reason in skipped_apps:
                report_lines.append(f" - {name}: {reason}")
        else:
            report_lines.append(" - None")
        report_text = "\n".join(report_lines)

        print("\n" + report_text + "\n")

        if SENDGRID_API_KEY and EMAIL_SENDER and RECIPIENT_LIST:
            try:
                send_email(
                    EMAIL_SENDER,
                    RECIPIENT_LIST,
                    "Function App Code Drift Report",
                    report_text,
                )
                print(f"Email sent to {RECIPIENT_LIST}")
            except Exception as e:
                print(f"Failed to send email: {e}\n{traceback.format_exc()}")

    except Exception as outer_e:
        print(f"Failed to run main function: {outer_e}")
    return True
