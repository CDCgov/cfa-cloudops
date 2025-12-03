from azure.storage.blob import BlobProperties

FAKE_BLOB_PROPERTIES = [
    {"name": "my-src-path/my_test_1.txt", "size": 100},
    {"name": "my-src-path/my_test_2.txt", "size": 200},
    {"name": "my-src-path/not_my_test_1.csv", "size": 250},
    {"name": "my-src-path/not_my_test_2.json", "size": 50},
    {"name": "my-src-path/large_file_1.parquet", "size": 1e9},
    {"name": "my-src-path/large_file_2.parquet", "size": 2e9},
]
FAKE_BLOBS = []
for fake in FAKE_BLOB_PROPERTIES:
    fake_blob = BlobProperties()
    fake_blob.name = fake["name"]
    fake_blob.size = fake["size"]
    FAKE_BLOBS.append(fake_blob)

FAKE_COMMANDLINE = [
    "script_name.py",
    "--dotenv_path",
    ".env.test",
    "--use_sp",
    "--use_federated",
]

FAKE_IMAGES = [
    "mcr.microsoft.com/azure-batch/batch-ubuntu:20.04",
    "mcr.microsoft.com/azure-batch/batch-ubuntu:18.04",
    "mcr.microsoft.com/azure-batch/batch-windows:2019",
]


class MockLogger:
    def __init__(self, name: str):
        self.name = name
        self.messages = []
        self.handlers = []

    def debug(self, message):
        self.messages.append(("DEBUG", message))

    def info(self, message):
        self.messages.append(("INFO", message))

    def warning(self, message):
        self.messages.append(("WARNING", message))

    def error(self, message):
        self.messages.append(("ERROR", message))

    def addHandler(self, handler):
        if handler not in self.handlers:
            self.handlers.append(handler)

    def removeHandler(self, handler):
        if handler in self.handlers:
            self.handlers.remove(handler)

    def assert_logged(self, level, message):
        assert (level, message) in self.messages, (
            f"Expected log ({level}, {message}) not found."
        )
