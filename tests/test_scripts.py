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


def test_create_job_schedule(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--job_schedule_name",
            "test schedule",
            "--pool_name",
            "test-pool",
            "--command",
            "echo hi",
            "--recurrence_interval_minutes",
            "10",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.create_job_schedule", return_value=None
    )
    scripts.create_job_schedule()


def test_schedule_controls(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--job_schedule_id", "schedule-1"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.delete_job_schedule", return_value=None
    )
    scripts.delete_job_schedule()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--job_schedule_id", "schedule-1"],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.resume_job_schedule", return_value=None
    )
    scripts.resume_job_schedule()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--job_schedule_id", "schedule-1"],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.suspend_job_schedule", return_value=None
    )
    scripts.suspend_job_schedule()


def test_misc_new_scripts(mocker, monkeypatch):
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)

    monkeypatch.setattr("sys.argv", FAKE_COMMANDLINE)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.check_credentials", return_value=None
    )
    scripts.check_credentials()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--registry_name", "r1", "--repo_name", "repo1"],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.list_acr_tags", return_value=["v1"]
    )
    scripts.list_acr_tags()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--job_name", "j1", "--task_id", "t1"],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.get_task_status", return_value="ok"
    )
    scripts.get_task_status()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--series", "D", "E"],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.get_vm_series_quotas",
        return_value=[{"name": "x"}],
    )
    scripts.get_vm_series_quotas()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--source_path", "a.txt", "--container_name", "c1"],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.update_blob_protection",
        return_value=None,
    )
    scripts.update_blob_protection()


def test_add_task_collection(mocker, monkeypatch, tmp_path):
    tasks_file = tmp_path / "tasks.json"
    tasks_file.write_text('[{"command_line": "echo hi"}]')
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--job_name",
            "test-job",
            "--tasks_file",
            str(tasks_file),
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.add_task_collection", return_value=None
    )
    scripts.add_task_collection()


def test_async_folder_scripts(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--src_path",
            "src",
            "--dest_path",
            "dst",
            "--container_name",
            "cont",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.async_download_folder",
        return_value=None,
    )
    scripts.async_download_folder()

    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--folders",
            "f1",
            "f2",
            "--container_name",
            "cont",
        ],
    )
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.async_upload_folder", return_value=None
    )
    scripts.async_upload_folder()


def test_add_tasks_from_yaml(mocker, monkeypatch, tmp_path):
    yaml_file = tmp_path / "tasks.yaml"
    yaml_file.write_text("tasks:\n  - id: t1\n    cmd: echo hi\n")
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--job_name",
            "test-job",
            "--base_cmd",
            "echo",
            "-fp",
            str(yaml_file),
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.add_tasks_from_yaml", return_value=None
    )
    scripts.add_tasks_from_yaml()
