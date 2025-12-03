from shared_fixtures import FAKE_COMMANDLINE

import cfa.cloudops.scripts as scripts


def test_create_pool(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + ["--pool_name", "test-pool", "--container_image_name", "test-image"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.create_pool", return_value=None)
    scripts.create_pool()


def test_create_job(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--job_name", "test-job", "--pool_name", "test-pool"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.create_job", return_value=None)
    scripts.create_job()


def test_add_task(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + ["--job_name", "test-job", "--command_line", "echo Hello World"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.add_task", return_value=None)
    scripts.add_task()


def test_upload_file(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--container_name", "test-container", "--source_path", "."],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.upload_files", return_value=None
    )
    scripts.upload_file()


def test_upload_folder(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + ["--folder_name", "test folder", "--container_name", "test-container"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.upload_folders", return_value=None
    )
    scripts.upload_folder()


def test_download_after_job(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--job_name",
            "test-job",
            "--container_name",
            "test-container",
            "--blob_paths",
            ".",
            "--target",
            "all",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.download_after_job", return_value=None
    )
    scripts.download_after_job()
