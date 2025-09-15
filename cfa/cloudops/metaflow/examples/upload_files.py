from cfa.cloudops.auth import SPCredentialHandler
from cfa.cloudops.blob_helpers import upload_files_in_folder
from cfa.cloudops.client import get_blob_service_client


def upload_artifacts():
    cred = SPCredentialHandler()
    blob_client = get_blob_service_client(cred)
    upload_files_in_folder(
        folder="cliques",
        container_name="output-test",
        include_extensions=[".png", ".log", ".prof"],
        blob_service_client=blob_client,
    )


if __name__ == "__main__":
    upload_artifacts()
