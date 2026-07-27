from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from cfa.cloudops._cloudclient import CloudClient


@pytest.fixture
def cloud_client_more(monkeypatch):
    cred = SimpleNamespace(
        azure_resource_group_name="rg",
        azure_batch_account="acct",
        azure_blob_storage_account="blobacct",
        azure_blob_storage_endpoint="https://blobacct.blob.core.windows.net/",
        user_credential="user-cred",
        client_secret_sp_credential="default-cred",
        client_secret_credential="sp-cred",
        compute_node_identity_reference=SimpleNamespace(resource_id="rid"),
    )

    with (
        patch("cfa.cloudops._cloudclient.EnvCredentialHandler", return_value=cred),
        patch("cfa.cloudops._cloudclient.get_batch_management_client", return_value=MagicMock()),
        patch("cfa.cloudops._cloudclient.get_compute_management_client", return_value=MagicMock()),
        patch("cfa.cloudops._cloudclient.get_batch_service_client", return_value=MagicMock()),
        patch("cfa.cloudops._cloudclient.get_blob_service_client", return_value=MagicMock()),
    ):
        return CloudClient(dotenv_path=None, use_sp=False, use_federated=False)


def test_check_credentials_env_default_sp(cloud_client_more, monkeypatch):
    seen_creds = []

    class FakeSub:
        def __init__(self):
            self.subscription_id = "sub-1"
            self.display_name = "sub-name"
            self.state = "Enabled"

    def fake_subscription_client(cred):
        seen_creds.append(cred)
        return SimpleNamespace(subscriptions=SimpleNamespace(list=lambda: [FakeSub()]))

    monkeypatch.setattr("cfa.cloudops._cloudclient.SubscriptionClient", fake_subscription_client)

    cloud_client_more.method = "env"
    cloud_client_more.check_credentials()

    cloud_client_more.method = "default"
    cloud_client_more.check_credentials()

    cloud_client_more.method = "sp"
    cloud_client_more.check_credentials()

    assert seen_creds == ["user-cred", "default-cred", "sp-cred"]


def test_check_credentials_handles_exception(cloud_client_more, monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.SubscriptionClient",
        lambda cred: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    cloud_client_more.check_credentials()


def test_delete_job_and_schedule_methods(cloud_client_more):
    cloud_client_more.batch_service_client.begin_delete_job.return_value.result.return_value = None
    cloud_client_more.batch_service_client.begin_delete_job_schedule.return_value.result.return_value = None

    cloud_client_more.delete_job("job-1")
    cloud_client_more.delete_job_schedule("sched-1")
    cloud_client_more.resume_job_schedule("sched-1")
    cloud_client_more.suspend_job_schedule("sched-1")

    cloud_client_more.batch_service_client.begin_delete_job.assert_called_once_with("job-1")
    cloud_client_more.batch_service_client.begin_delete_job_schedule.assert_called_once_with("sched-1")
    cloud_client_more.batch_service_client.enable_job_schedule.assert_called_once_with("sched-1")
    cloud_client_more.batch_service_client.disable_job_schedule.assert_called_once_with("sched-1")


def test_list_available_images_filtering(cloud_client_more):
    linux = SimpleNamespace(os_type="linux", name="lin")
    windows = SimpleNamespace(os_type="windows", name="win")

    cloud_client_more.batch_service_client.list_supported_images.return_value = [linux, windows]

    with patch("cfa.cloudops._cloudclient.batch_models.OSType") as os_type:
        os_type.linux = "linux"
        os_type.windows = "windows"

        result_linux = cloud_client_more.list_available_images("linux")
        result_windows = cloud_client_more.list_available_images("windows")
        result_all = cloud_client_more.list_available_images()

    assert result_linux == [linux]
    assert result_windows == [windows]
    assert result_all == [linux, windows]


def test_package_and_upload_dockerfile_delegates(cloud_client_more, monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.helpers.package_and_upload_dockerfile",
        lambda *args, **kwargs: "reg.azurecr.io/repo:v1",
    )

    out = cloud_client_more.package_and_upload_dockerfile("reg", "repo", "v1")

    assert out == "reg.azurecr.io/repo:v1"
    assert cloud_client_more.container_registry_server == "reg.azurecr.io"
    assert cloud_client_more.registry_url == "https://reg.azurecr.io"


def test_upload_docker_image_delegates(cloud_client_more, monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.helpers.upload_docker_image",
        lambda *args, **kwargs: "reg.azurecr.io/repo:v2",
    )

    out = cloud_client_more.upload_docker_image("local:latest", "reg", "repo", "v2")

    assert out == "reg.azurecr.io/repo:v2"
    assert cloud_client_more.container_image_name == "https://reg.azurecr.io/repo:v2"


def test_download_file_download_folder_delegates(cloud_client_more, monkeypatch):
    seen = {"file": None, "folder": None}

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob_helpers.download_file",
        lambda *args, **kwargs: seen.__setitem__("file", (args, kwargs)),
    )
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob_helpers.download_folder",
        lambda *args, **kwargs: seen.__setitem__("folder", (args, kwargs)),
    )

    cloud_client_more.download_file("a.txt", "./a.txt", container_name="c1")
    cloud_client_more.download_folder("src", "dest", "c2")

    assert seen["file"] is not None
    assert seen["folder"] is not None


def test_async_download_folder_uses_method_credential(cloud_client_more, monkeypatch):
    calls = []

    def fake_async_download_blob_folder(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob.async_download_blob_folder",
        fake_async_download_blob_folder,
    )

    cloud_client_more.method = "default"
    cloud_client_more.async_download_folder("src", "dest", "c")

    cloud_client_more.method = "sp"
    cloud_client_more.async_download_folder("src", "dest", "c")

    cloud_client_more.method = "env"
    cloud_client_more.async_download_folder("src", "dest", "c")

    assert calls[0]["credential"] == "default-cred"
    assert calls[1]["credential"] == "sp-cred"
    assert calls[2]["credential"] == "user-cred"


def test_async_upload_folder_handles_str_and_list(cloud_client_more, monkeypatch):
    calls = []

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob.async_upload_folder",
        lambda **kwargs: calls.append(kwargs),
    )

    cloud_client_more.method = "env"
    cloud_client_more.async_upload_folder("folder-a", "container-a")
    cloud_client_more.async_upload_folder(["folder-b", "folder-c"], "container-a")

    assert [c["folder"] for c in calls] == ["folder-a", "folder-b", "folder-c"]


def test_delete_pool_and_blob_ops(cloud_client_more, monkeypatch):
    called = {"pool": 0, "blob_file": 0, "blob_folder": 0}

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.batch_helpers.delete_pool",
        lambda **kwargs: called.__setitem__("pool", called["pool"] + 1),
    )
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob_helpers.delete_blob_snapshots",
        lambda *args, **kwargs: called.__setitem__("blob_file", called["blob_file"] + 1),
    )
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob_helpers.delete_blob_folder",
        lambda *args, **kwargs: called.__setitem__("blob_folder", called["blob_folder"] + 1),
    )

    cloud_client_more.delete_pool("pool-1")
    cloud_client_more.delete_blob_file("blob.txt", "cont")
    cloud_client_more.delete_blob_folder("folder", "cont")

    assert called == {"pool": 1, "blob_file": 1, "blob_folder": 1}


def test_list_blob_files_by_container_and_mounts(cloud_client_more, monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.blob_helpers.list_blobs_flat",
        lambda container_name, blob_service_client, verbose=False: [f"{container_name}/a.txt"],
    )

    out_container = cloud_client_more.list_blob_files("c1")

    cloud_client_more.mounts = [("m1", "m1"), ("m2", "m2")]
    out_mounts = cloud_client_more.list_blob_files()

    assert out_container == ["c1/a.txt"]
    assert out_mounts == ["m1/a.txt", "m2/a.txt"]


def test_download_job_stats_and_task_status(cloud_client_more, monkeypatch):
    seen = {"stats": None, "status": None}

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.batch_helpers.download_job_stats",
        lambda **kwargs: seen.__setitem__("stats", kwargs),
    )
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.batch_helpers.get_task_status",
        lambda **kwargs: seen.__setitem__("status", kwargs) or "{}",
    )

    cloud_client_more.download_job_stats("job-1")
    status = cloud_client_more.get_task_status("job-1", "task-1")

    assert seen["stats"]["job_name"] == "job-1"
    assert status == "{}"


def test_download_after_job_dispatches_file_and_folder(cloud_client_more, monkeypatch):
    calls = {"file": [], "folder": [], "monitor": 0, "makedirs": 0}

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.batch_helpers.monitor_tasks",
        lambda **kwargs: calls.__setitem__("monitor", calls["monitor"] + 1),
    )
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.os.makedirs",
        lambda *args, **kwargs: calls.__setitem__("makedirs", calls["makedirs"] + 1),
    )

    cloud_client_more.download_file = lambda **kwargs: calls["file"].append(kwargs)
    cloud_client_more.download_folder = lambda **kwargs: calls["folder"].append(kwargs)

    cloud_client_more.download_after_job(
        job_name="job-1",
        blob_paths=["outputs/file.txt", "outputs/folder"],
        target="./downloads",
        container_name="cont",
    )

    assert calls["monitor"] == 1
    assert calls["makedirs"] == 1
    assert len(calls["file"]) == 1
    assert len(calls["folder"]) == 1


def test_get_kv_secret_success_and_failure(cloud_client_more, monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.SecretClient",
        lambda vault_url, credential: SimpleNamespace(
            get_secret=lambda name: SimpleNamespace(value=f"v-{name}")
        ),
    )

    cloud_client_more.method = "sp"
    assert cloud_client_more.get_kv_secret("s1", "kv") == "v-s1"

    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.SecretClient",
        lambda vault_url, credential: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert cloud_client_more.get_kv_secret("s1", "kv") is None


def test_list_acr_tags_delegates(cloud_client_more, monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops._cloudclient.helpers.list_acr_tags",
        lambda registry_name, repo_name: ["latest", "v1"],
    )

    assert cloud_client_more.list_acr_tags("reg", "repo") == ["latest", "v1"]
