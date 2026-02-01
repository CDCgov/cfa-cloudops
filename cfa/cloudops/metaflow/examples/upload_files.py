import argparse
import os
import random
import shutil
import string
from datetime import date

parser = argparse.ArgumentParser()
parser.add_argument(
    "-j",
    "--job",
    type=str,
    help="job ID",
)

# Uncomment this for testing locally within VAP
# from cfa.cloudops.auth import SPCredentialHandler
# from cfa.cloudops.blob_helpers import upload_files_in_folder
# from cfa.cloudops.client import get_blob_service_client

CURRENT_DATE_ISO = date.today().isoformat()


def generate_random_string(length):
    """Generates a random string of specified length using letters and digits."""
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.choice(characters) for _ in range(length))
    return random_string


SUFFIX = generate_random_string(10)
SOURCE_PATH = "/app/cliques/"


def move_files_to_subfolder(destination_path: str):
    for item_name in os.listdir(SOURCE_PATH):
        source_path = os.path.join(SOURCE_PATH, item_name)
        destination_item_path = os.path.join(destination_path, item_name)
        if os.path.isfile(source_path):
            try:
                shutil.move(source_path, destination_item_path)
            except Exception:
                continue


# Uncomment this for testing locally within VAP
# def upload_artifacts():
#    cred = SPCredentialHandler()
#    blob_client = get_blob_service_client(cred)
#    upload_files_in_folder(
#        folder="cliques",
#        container_name="output-test",
#        include_extensions=[".png", ".log", ".prof"],
#        blob_service_client=blob_client,
#    )


if __name__ == "__main__":
    args = parser.parse_args()
    destination_path = f"/app/cliques/{SUFFIX}/"
    remote_path = f"/output/cliques/{CURRENT_DATE_ISO}/{SUFFIX}/"
    if args.job:
        destination_path = f"/app/cliques/{args.job}/{SUFFIX}/"
        remote_path = f"/output/cliques/{args.job}/{CURRENT_DATE_ISO}/{SUFFIX}/"
    os.makedirs(destination_path, exist_ok=True)
    move_files_to_subfolder()
    # Uncomment this for testing locally within VAP
    # upload_artifacts()
    shutil.copytree(destination_path, remote_path, dirs_exist_ok=True)
