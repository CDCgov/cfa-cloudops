import os
import random
import shutil
import string

from cfa.cloudops.auth import SPCredentialHandler
from cfa.cloudops.blob_helpers import upload_files_in_folder
from cfa.cloudops.client import get_blob_service_client


def generate_random_string(length):
    """Generates a random string of specified length using letters and digits."""
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.choice(characters) for _ in range(length))
    return random_string


SUFFIX = generate_random_string(10)
SOURCE_PATH = "cliques/"
DESTINATION_PATH = f"cliques/{SUFFIX}/"
os.makedirs(DESTINATION_PATH, exist_ok=True)


def move_files_to_subfolder():
    for item_name in os.listdir(SOURCE_PATH):
        source_path = os.path.join(SOURCE_PATH, item_name)
        destination_path = os.path.join(DESTINATION_PATH, item_name)
        if os.path.isfile(source_path):
            try:
                shutil.move(source_path, destination_path)
            except Exception:
                continue


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
    move_files_to_subfolder()
    upload_artifacts()
