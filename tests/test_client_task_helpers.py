import json
import logging
from types import SimpleNamespace

import pytest
from azure.batch.models import BatchNodeIdentityReference

from cfa.cloudops import client, helpers, task


@pytest.fixture
def fake_credential_handler():
    return SimpleNamespace(
        method="sp",
        client_secret_credential="sp-cred",  # pragma: allowlist secret
        client_secret_sp_credential="default-cred",  # pragma: allowlist secret
        user_credential="user-cred",
        azure_subscription_id="sub-123",
        azure_batch_endpoint="https://batch.example",
        azure_blob_storage_endpoint="https://blob.example",
    )


def test_get_batch_management_client_methods(monkeypatch, fake_credential_handler):
    calls = []

    def fake_constructor(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(kind="batch_mgmt")

    monkeypatch.setattr("cfa.cloudops.client.BatchManagementClient", fake_constructor)

    fake_credential_handler.method = "sp"
    client.get_batch_management_client(fake_credential_handler)

    fake_credential_handler.method = "default"
    client.get_batch_management_client(fake_credential_handler)

    fake_credential_handler.method = "user"
    client.get_batch_management_client(fake_credential_handler)

    assert calls[0]["credential"] == "sp-cred"
    assert calls[1]["credential"] == "default-cred"
    assert calls[2]["credential"] == "user-cred"
    assert all(c["subscription_id"] == "sub-123" for c in calls)


def test_get_compute_management_client_methods(monkeypatch, fake_credential_handler):
    calls = []

    def fake_constructor(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(kind="compute_mgmt")

    monkeypatch.setattr("cfa.cloudops.client.ComputeManagementClient", fake_constructor)

    fake_credential_handler.method = "sp"
    client.get_compute_management_client(fake_credential_handler)

    fake_credential_handler.method = "default"
    client.get_compute_management_client(fake_credential_handler)

    fake_credential_handler.method = "user"
    client.get_compute_management_client(fake_credential_handler)

    assert calls[0]["credential"] == "sp-cred"
    assert calls[1]["credential"] == "default-cred"
    assert calls[2]["credential"] == "user-cred"


def test_get_batch_service_client_methods(monkeypatch, fake_credential_handler):
    calls = []

    def fake_constructor(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(kind="batch_service")

    monkeypatch.setattr("cfa.cloudops.client.BatchClient", fake_constructor)

    fake_credential_handler.method = "sp"
    client.get_batch_service_client(fake_credential_handler)

    fake_credential_handler.method = "default"
    client.get_batch_service_client(fake_credential_handler)

    fake_credential_handler.method = "user"
    client.get_batch_service_client(fake_credential_handler)

    assert calls[0]["credential"] == "sp-cred"
    assert calls[1]["credential"] == "default-cred"
    assert calls[2]["credential"] == "user-cred"
    assert all(c["endpoint"] == "https://batch.example" for c in calls)


def test_get_blob_service_client_methods(monkeypatch, fake_credential_handler):
    calls = []

    def fake_constructor(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(kind="blob_service")

    monkeypatch.setattr("cfa.cloudops.client.BlobServiceClient", fake_constructor)

    fake_credential_handler.method = "sp"
    client.get_blob_service_client(fake_credential_handler)

    fake_credential_handler.method = "default"
    client.get_blob_service_client(fake_credential_handler)

    fake_credential_handler.method = "user"
    client.get_blob_service_client(fake_credential_handler)

    assert calls[0]["credential"] == "sp-cred"
    assert calls[1]["credential"] == "default-cred"
    assert calls[2]["credential"] == "user-cred"
    assert all(c["account_url"] == "https://blob.example" for c in calls)


def test_get_clients_build_default_handler_when_none(
    monkeypatch, fake_credential_handler
):
    monkeypatch.setattr(
        "cfa.cloudops.client.EnvCredentialHandler", lambda: fake_credential_handler
    )
    monkeypatch.setattr(
        "cfa.cloudops.client.BatchManagementClient", lambda **kwargs: kwargs
    )

    result = client.get_batch_management_client()
    assert result["subscription_id"] == "sub-123"


def test_create_bind_mount_string():
    mount = task.create_bind_mount_string("/mnt/batch/tasks/fsmounts", "src", "/app")
    assert mount == "--mount type=bind,source=/mnt/batch/tasks/fsmounts/src,target=/app"


def test_get_container_settings_with_mounts_and_registry(monkeypatch):
    registry = SimpleNamespace(registry_server="myregistry.azurecr.io")
    settings = task.get_container_settings(
        container_image_name="myregistry.azurecr.io/app:latest",
        mount_pairs=[
            {"source": "input", "target": "/app/input"},
            {"source": "output", "target": "/app/output"},
        ],
        additional_options="--ipc=host",
        registry=registry,
    )

    assert settings.image_name == "myregistry.azurecr.io/app:latest"
    assert "--ipc=host" in settings.container_run_options
    assert "source=/mnt/batch/tasks/fsmounts/input" in settings.container_run_options
    assert "target=/app/output" in settings.container_run_options


def test_output_task_files_to_blob_uses_default_identity(monkeypatch):
    mgmt_id = SimpleNamespace(
        resource_id="/subscriptions/sub/resourceGroups/rg/providers/id"
    )
    node_id = BatchNodeIdentityReference(resource_id=mgmt_id.resource_id)

    monkeypatch.setattr(
        "cfa.cloudops.task.get_compute_node_identity_reference", lambda: mgmt_id
    )
    monkeypatch.setattr("cfa.cloudops.task.get_batch_compute_id", lambda x: node_id)

    output = task.output_task_files_to_blob(
        file_pattern="*.txt",
        blob_container="logs",
        blob_account="acct",
        path="job/task",
    )

    assert output.file_pattern == "*.txt"
    container = output.destination.container
    assert container.path == "job/task"
    assert container.identity_reference.resource_id == mgmt_id.resource_id
    assert container.container_url == "https://acct.blob.core.windows.net/logs"


def test_output_task_files_to_blob_type_error():
    with pytest.raises(TypeError):
        task.output_task_files_to_blob(
            file_pattern="*.txt",
            blob_container="logs",
            blob_account="acct",
            compute_node_identity_reference="not-a-node-id",
        )


def test_get_task_config_with_logs_and_filtered_kwargs(monkeypatch):
    node_id = BatchNodeIdentityReference(
        resource_id="/subscriptions/sub/resourceGroups/rg/providers/id"
    )
    log_output_file = task.output_task_files_to_blob(
        file_pattern="../std*.txt",
        blob_container="logs",
        blob_account="acct",
        path="preexisting",
        compute_node_identity_reference=node_id,
    )

    monkeypatch.setattr(
        "cfa.cloudops.task.output_task_files_to_blob",
        lambda **kwargs: log_output_file,
    )

    cfg = task.get_task_config(
        task_id="task-001",
        base_call="python app.py",
        output_files=[log_output_file],
        log_blob_container="logs",
        log_blob_account="acct",
        log_subdir="jobs/run-1",
        run_dependent_tasks_on_failure=True,
    )

    assert cfg.id == "task-001"
    assert cfg.command_line == "python app.py"
    assert len(cfg.output_files) == 2
    assert cfg.user_identity.auto_user.elevation_level.name.lower() == "admin"


def test_get_batch_compute_id_validation():
    valid = SimpleNamespace(
        resource_id="/subscriptions/sub/resourceGroups/rg/providers/id"
    )
    result = task.get_batch_compute_id(valid)
    assert isinstance(result, BatchNodeIdentityReference)
    assert result.resource_id == valid.resource_id

    with pytest.raises(ValueError):
        task.get_batch_compute_id(SimpleNamespace(resource_id=""))


def test_get_log_level_variants(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    assert helpers.get_log_level() == logging.CRITICAL + 1

    monkeypatch.setenv("LOG_LEVEL", "none")
    assert helpers.get_log_level() == logging.CRITICAL + 1

    monkeypatch.setenv("LOG_LEVEL", "debug")
    assert helpers.get_log_level() == logging.DEBUG

    monkeypatch.setenv("LOG_LEVEL", "warn")
    assert helpers.get_log_level() == logging.WARNING

    monkeypatch.setenv("LOG_LEVEL", "weird")
    assert helpers.get_log_level() == logging.DEBUG


def test_format_rel_path():
    assert helpers.format_rel_path("/data/input") == "data/input"
    assert helpers.format_rel_path("data/output") == "data/output"


def test_list_acr_tags_success(monkeypatch):
    tags = ["latest", "v1"]
    responses = [
        SimpleNamespace(returncode=0, stdout="", stderr=""),
        SimpleNamespace(returncode=0, stdout=json.dumps(tags), stderr=""),
    ]

    def fake_run(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    result = helpers.list_acr_tags("reg", "repo")
    assert result == tags


def test_list_acr_tags_identity_login_then_success(monkeypatch):
    tags = ["latest"]
    responses = [
        SimpleNamespace(returncode=1, stdout="", stderr="not logged in"),
        SimpleNamespace(returncode=0, stdout="", stderr=""),
        SimpleNamespace(returncode=0, stdout=json.dumps(tags), stderr=""),
    ]

    def fake_run(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    result = helpers.list_acr_tags("reg", "repo")
    assert result == tags


def test_list_acr_tags_identity_fails_but_existing_session(monkeypatch):
    tags = ["v2"]
    responses = [
        SimpleNamespace(returncode=1, stdout="", stderr="auth missing"),
        SimpleNamespace(returncode=1, stdout="", stderr="identity not available"),
        SimpleNamespace(returncode=0, stdout="", stderr=""),
        SimpleNamespace(returncode=0, stdout=json.dumps(tags), stderr=""),
    ]

    def fake_run(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    result = helpers.list_acr_tags("reg", "repo")
    assert result == tags


def test_list_acr_tags_auth_fails_hard(monkeypatch):
    responses = [
        SimpleNamespace(returncode=1, stdout="", stderr="auth missing"),
        SimpleNamespace(returncode=1, stdout="", stderr="identity not available"),
        SimpleNamespace(returncode=1, stdout="", stderr="still no login"),
    ]

    def fake_run(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    with pytest.raises(Exception):
        helpers.list_acr_tags("reg", "repo")


def test_list_acr_tags_show_tags_failure(monkeypatch):
    responses = [
        SimpleNamespace(returncode=0, stdout="", stderr=""),
        SimpleNamespace(returncode=3, stdout="", stderr="acr failed"),
    ]

    def fake_run(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    with pytest.raises(Exception):
        helpers.list_acr_tags("reg", "repo")


@pytest.mark.parametrize(
    "use_device_code,expected_login_cmd",
    [
        (False, "az login --identity"),
        (True, "az login --use-device-code"),
    ],
)
def test_package_and_upload_dockerfile_success(
    monkeypatch, use_device_code, expected_login_cmd
):
    docker_env = SimpleNamespace(ping=lambda: True)
    commands = []

    def fake_run(cmd, shell=True, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "cfa.cloudops.helpers.docker.from_env", lambda timeout=10: docker_env
    )
    monkeypatch.setattr("cfa.cloudops.helpers.os.path.exists", lambda p: True)
    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    image_name = helpers.package_and_upload_dockerfile(
        registry_name="reg",
        repo_name="repo",
        tag="v1",
        path_to_dockerfile="./Dockerfile",
        use_device_code=use_device_code,
    )

    assert image_name == "reg.azurecr.io/repo:v1"
    assert commands[0].startswith(
        "docker image build -f ./Dockerfile -t reg.azurecr.io/repo:v1"
    )
    assert expected_login_cmd in commands
    assert "az acr login --name reg" in commands
    assert "docker push reg.azurecr.io/repo:v1" in commands


def test_package_and_upload_dockerfile_docker_not_running(monkeypatch):
    def boom(timeout=10):
        raise helpers.DockerException("down")

    monkeypatch.setattr("cfa.cloudops.helpers.docker.from_env", boom)

    with pytest.raises(helpers.DockerException):
        helpers.package_and_upload_dockerfile("reg", "repo", "latest")


def test_package_and_upload_dockerfile_missing_file(monkeypatch):
    docker_env = SimpleNamespace(ping=lambda: True)

    monkeypatch.setattr(
        "cfa.cloudops.helpers.docker.from_env", lambda timeout=10: docker_env
    )
    monkeypatch.setattr("cfa.cloudops.helpers.os.path.exists", lambda p: False)

    with pytest.raises(Exception):
        helpers.package_and_upload_dockerfile("reg", "repo", "latest")


@pytest.mark.parametrize(
    "use_device_code,expected_login_cmd",
    [
        (False, "az login --identity"),
        (True, "az login --use-device-code"),
    ],
)
def test_upload_docker_image_success(monkeypatch, use_device_code, expected_login_cmd):
    tagged = []
    commands = []
    image = SimpleNamespace(tag=lambda tag_name: tagged.append(tag_name))
    images = SimpleNamespace(
        get=lambda image_name: image,
        list=lambda: [SimpleNamespace(tags=["local:latest"])],
    )
    docker_env = SimpleNamespace(ping=lambda: True, images=images)

    def fake_run(cmd, shell=True, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "cfa.cloudops.helpers.docker.from_env", lambda timeout=8: docker_env
    )
    monkeypatch.setattr("cfa.cloudops.helpers.sp.run", fake_run)

    image_name = helpers.upload_docker_image(
        image_name="local:latest",
        registry_name="reg",
        repo_name="repo",
        tag="v2",
        use_device_code=use_device_code,
    )

    assert image_name == "reg.azurecr.io/repo:v2"
    assert tagged == ["reg.azurecr.io/repo:v2"]
    assert expected_login_cmd in commands
    assert "az acr login --name reg" in commands
    assert "docker push reg.azurecr.io/repo:v2" in commands


def test_upload_docker_image_docker_not_running(monkeypatch):
    def boom(timeout=8):
        raise helpers.DockerException("down")

    monkeypatch.setattr("cfa.cloudops.helpers.docker.from_env", boom)

    with pytest.raises(helpers.DockerException):
        helpers.upload_docker_image("local:latest", "reg", "repo")


def test_upload_docker_image_not_found(monkeypatch):
    def missing_image(image_name):
        raise helpers.docker.errors.ImageNotFound("missing")

    images = SimpleNamespace(
        get=missing_image,
        list=lambda: [SimpleNamespace(tags=["other:tag"])],
    )
    docker_env = SimpleNamespace(ping=lambda: True, images=images)

    monkeypatch.setattr(
        "cfa.cloudops.helpers.docker.from_env", lambda timeout=8: docker_env
    )

    with pytest.raises(helpers.docker.errors.ImageNotFound):
        helpers.upload_docker_image("local:latest", "reg", "repo")
