import pytest
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


def test_hello(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["script_name.py", "--name", "Tester"])
    scripts.hello()
    captured = capsys.readouterr()
    assert "Hello, Tester!" in captured.out


def test_create_blob_container(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv", FAKE_COMMANDLINE + ["--container_name", "my-container"]
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.create_blob_container",
        return_value=None,
    )
    scripts.create_blob_container()


def test_monitor_job(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv", FAKE_COMMANDLINE + ["--job_name", "job-1", "--download_job_stats"]
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.monitor_job", return_value=None)
    scripts.monitor_job()


def test_check_job_status(mocker, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", FAKE_COMMANDLINE + ["--job_name", "job-1"])
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.check_job_status",
        return_value="completed",
    )
    scripts.check_job_status()
    captured = capsys.readouterr()
    assert "completed" in captured.out


def test_delete_job(mocker, monkeypatch):
    monkeypatch.setattr("sys.argv", FAKE_COMMANDLINE + ["--job_name", "job-1"])
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.delete_job", return_value=None)
    scripts.delete_job()


def test_package_and_upload_dockerfile(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--registry_name",
            "reg",
            "--repo_name",
            "repo",
            "--tag",
            "v1",
            "--path_to_dockerfile",
            "./Dockerfile",
            "--use_device_code",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.package_and_upload_dockerfile",
        return_value=None,
    )
    scripts.package_and_upload_dockerfile()


def test_upload_docker_image(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--image_name",
            "local:latest",
            "--registry_name",
            "reg",
            "--repo_name",
            "repo",
            "--tag",
            "v2",
            "--use_device_code",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.upload_docker_image",
        return_value=None,
    )
    scripts.upload_docker_image()


def test_download_file(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--container_name",
            "my-container",
            "--blob_name",
            "path/file.txt",
            "--destination_path",
            "./file.txt",
            "--check_size",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.download_file", return_value=None
    )
    scripts.download_file()


def test_download_folder(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--src_path",
            "my-src-path",
            "--dest_path",
            "./downloads",
            "--container_name",
            "my-container",
            "--include_extensions",
            ".txt",
            "--check_size",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.download_folder", return_value=None
    )
    scripts.download_folder()


def test_delete_pool(mocker, monkeypatch):
    monkeypatch.setattr("sys.argv", FAKE_COMMANDLINE + ["--pool_name", "pool-1"])
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.delete_pool", return_value=None)
    scripts.delete_pool()


def test_list_blob_files(mocker, monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv", FAKE_COMMANDLINE + ["--container_name", "my-container"]
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.list_blob_files",
        return_value=["a.txt", "b.txt"],
    )
    scripts.list_blob_files()
    captured = capsys.readouterr()
    assert "a.txt" in captured.out
    assert "b.txt" in captured.out


def test_delete_blob_file(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + ["--container_name", "my-container", "--blob_name", "a/file.txt"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.delete_blob_file", return_value=None
    )
    scripts.delete_blob_file()


def test_delete_blob_folder(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + ["--container_name", "my-container", "--blob_folder_name", "folder/a"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.delete_blob_folder", return_value=None
    )
    scripts.delete_blob_folder()


def test_download_job_stats(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE + ["--job_name", "job-1", "--file_name", "stats.csv"],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.download_job_stats", return_value=None
    )
    scripts.download_job_stats()


def test_add_tasks_from_yaml(mocker, monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        FAKE_COMMANDLINE
        + [
            "--job_name",
            "job-1",
            "--base_cmd",
            "python main.py",
            "--file_path",
            "tasks.yaml",
        ],
    )
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch(
        "cfa.cloudops._cloudclient.CloudClient.add_tasks_from_yaml", return_value=None
    )
    scripts.add_tasks_from_yaml()


def test_generate_sample_env(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    scripts.generate_sample_env()
    generated = tmp_path / "cloudops-sample.env"
    assert generated.exists()
    assert "AZURE_BATCH_ACCOUNT" in generated.read_text()
    captured = capsys.readouterr()
    assert "created successfully" in captured.out


def test_test_entrypoint(mocker, monkeypatch):
    monkeypatch.setattr("sys.argv", ["script_name.py", "-q"])
    mocker.patch("pytest.main", return_value=0)
    with pytest.raises(SystemExit) as exc:
        scripts.test()
    assert exc.value.code == 0
