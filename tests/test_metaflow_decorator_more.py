import importlib
import string
import sys
from types import ModuleType, SimpleNamespace

import pytest


@pytest.fixture
def deco_mod(monkeypatch):
    # Stub imports required by cfa.cloudops.metaflow package initialization.
    fake_examples = {
        "examples": ModuleType("examples"),
        "examples.metaflow": ModuleType("examples.metaflow"),
        "examples.metaflow.azure_batch_decorator": ModuleType("examples.metaflow.azure_batch_decorator"),
        "examples.metaflow.plugins": ModuleType("examples.metaflow.plugins"),
        "examples.metaflow.plugins.metadata_providers": ModuleType("examples.metaflow.plugins.metadata_providers"),
        "examples.metaflow.plugins.metadata_providers.local": ModuleType("examples.metaflow.plugins.metadata_providers.local"),
    }

    class FakeAzureBatchDecorator:
        pass

    class FakeLocalMetadataProvider:
        pass

    fake_examples["examples.metaflow.azure_batch_decorator"].AzureBatchDecorator = FakeAzureBatchDecorator
    fake_examples["examples.metaflow.plugins.metadata_providers.local"].LocalMetadataProvider = FakeLocalMetadataProvider

    for name, mod in fake_examples.items():
        monkeypatch.setitem(sys.modules, name, mod)

    # Stub StepDecorator base class for isolated decorator import.
    metaflow_pkg = ModuleType("metaflow")
    metaflow_decorators = ModuleType("metaflow.decorators")

    class DummyStepDecorator:
        def __init__(self, *args, **kwargs):
            pass

    metaflow_decorators.StepDecorator = DummyStepDecorator
    monkeypatch.setitem(sys.modules, "metaflow", metaflow_pkg)
    monkeypatch.setitem(sys.modules, "metaflow.decorators", metaflow_decorators)

    return importlib.import_module(
        "cfa.cloudops.metaflow.custom_metaflow.plugins.decorators.cfa_azure_batch_decorator"
    )


def _attrs():
    return {
        "AZURE_TENANT_ID": "tenant",
        "AZURE_SUBSCRIPTION_ID": "sub",
        "AZURE_SP_CLIENT_ID": "client",
        "AZURE_CLIENT_SECRET": "secret",
        "AZURE_KEYVAULT_ENDPOINT": "kv",
        "AZURE_KEYVAULT_SP_SECRET_ID": "sid",
        "AZURE_RESOURCE_GROUP": "rg",
        "AZURE_BATCH_ACCOUNT": "batch",
        "AZURE_BLOB_STORAGE_ACCOUNT": "blob",
        "AZURE_SUBNET_ID": "subnet",
        "AZURE_USER_ASSIGNED_IDENTITY": "uami",
    }


def test_generate_random_string_is_alnum(deco_mod):
    out = deco_mod.generate_random_string(12)
    assert len(out) == 12
    assert all(ch in (string.ascii_letters + string.digits) for ch in out)


def test_decorator_init_sets_clients(monkeypatch, deco_mod):
    cred = SimpleNamespace()
    monkeypatch.setattr(deco_mod, "SPCredentialHandler", lambda **kwargs: cred)
    monkeypatch.setattr(deco_mod, "get_batch_service_client", lambda c: "batch-client")
    monkeypatch.setattr(deco_mod, "get_batch_management_client", lambda c: "batch-mgmt-client")

    d = deco_mod.CFAAzureBatchDecorator(
        pool_name="pool-a",
        attributes=_attrs(),
        job_configuration={"Job": {}, "Pool": {}},
        docker_command="python main.py",
        task_parameters=[1, 2],
    )

    assert d.pool_name == "pool-a"
    assert d.batch_client == "batch-client"
    assert d.batch_mgmt_client == "batch-mgmt-client"
    assert d.task_interval == 10
    assert d.docker_command == "python main.py"
    assert d.task_parameters == [1, 2]
    assert d.cred.azure_resource_group_name == "rg"
    assert d.cred.azure_batch_account == "batch"


def test_private_create_job_builds_and_submits(monkeypatch, deco_mod):
    d = deco_mod.CFAAzureBatchDecorator.__new__(deco_mod.CFAAzureBatchDecorator)
    d.pool_name = "pool-a"
    d.batch_client = "batch-client"

    class FakeConstraints:
        def __init__(self, max_task_retry_count=None, max_wall_clock_time=None):
            self.max_task_retry_count = max_task_retry_count
            self.max_wall_clock_time = max_wall_clock_time

    class FakeJob:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    fake_models = SimpleNamespace(
        BatchAllTasksCompleteMode=SimpleNamespace(TERMINATE_JOB="term", NO_ACTION="none"),
        BatchTaskFailureMode=SimpleNamespace(PERFORM_EXIT_OPTIONS_JOB_ACTION="exit"),
        BatchJobConstraints=FakeConstraints,
        BatchPoolInfo=lambda pool_id: SimpleNamespace(pool_id=pool_id),
        BatchMetadataItem=lambda name, value: SimpleNamespace(name=name, value=value),
        BatchJobCreateOptions=lambda **kwargs: FakeJob(**kwargs),
    )
    monkeypatch.setattr(deco_mod, "batch_models", fake_models)

    submitted = {}

    def fake_create_job(client, job, exist_ok=False, verify_pool=True, verbose=False):
        submitted.update(
            {
                "client": client,
                "job": job,
                "exist_ok": exist_ok,
                "verify_pool": verify_pool,
                "verbose": verbose,
            }
        )

    monkeypatch.setattr(deco_mod, "create_job", fake_create_job)

    d._CFAAzureBatchDecorator__create_job(
        job_name="my job",
        task_retries=2,
        mark_complete_after_tasks_run=True,
        timeout=5,
        uses_deps=False,
        exist_ok=True,
        verify_pool=False,
        verbose=True,
    )

    job = submitted["job"]
    assert submitted["client"] == "batch-client"
    assert submitted["exist_ok"] is True
    assert submitted["verify_pool"] is False
    assert submitted["verbose"] is True
    assert job.id == "myjob"
    assert job.pool_info.pool_id == "pool-a"
    assert job.uses_task_dependencies is False
    assert job.all_tasks_complete_mode == "term"
    assert job.constraints.max_task_retry_count == 2
    assert job.constraints.max_wall_clock_time is not None


def test_add_task_delegates_and_overrides_logs_folder(monkeypatch, deco_mod):
    d = deco_mod.CFAAzureBatchDecorator.__new__(deco_mod.CFAAzureBatchDecorator)
    d.pool_name = "pool-a"
    d.cred = SimpleNamespace(azure_resource_group_name="rg", azure_batch_account="batch")
    d.batch_mgmt_client = "batch-mgmt"
    d.batch_client = "batch-client"

    monkeypatch.setattr(deco_mod.batch_helpers, "get_pool_mounts", lambda *a, **k: ["/mnt/input"])
    seen = {}

    def fake_add_task(**kwargs):
        seen.update(kwargs)
        return "tid-1"

    monkeypatch.setattr(deco_mod.batch_helpers, "add_task", fake_add_task)

    tid = d.add_task(
        job_name="job-1",
        command_line="echo hi",
        save_logs_to_blob="logs",
        logs_folder="custom",
        depends_on=["a"],
        run_dependent_tasks_on_fail=True,
        container_image_name="img:1",
        timeout=9,
    )

    assert tid == "tid-1"
    assert seen["job_name"] == "job-1"
    assert seen["logs_folder"] == "stdout_stderr"
    assert seen["mounts"] == ["/mnt/input"]
    assert seen["depends_on"] == ["a"]
    assert seen["batch_client"] == "batch-client"
    assert seen["full_container_name"] == "img:1"


def test_fetch_or_create_job_reuse_and_create(monkeypatch, deco_mod):
    d = deco_mod.CFAAzureBatchDecorator.__new__(deco_mod.CFAAzureBatchDecorator)
    d.job_configuration = {"Job": {"job_id": "jid", "job_id_prefix": "pref-"}}
    d.batch_client = "batch-client"

    monkeypatch.setattr(deco_mod, "generate_random_string", lambda length: "abcde")

    monkeypatch.setattr(deco_mod.batch_helpers, "check_job_exists", lambda job_id, client: True)
    assert d.fetch_or_create_job() == "pref-abcde"

    calls = []
    monkeypatch.setattr(deco_mod.batch_helpers, "check_job_exists", lambda job_id, client: False)
    monkeypatch.setattr(
        d,
        "_CFAAzureBatchDecorator__create_job",
        lambda **kwargs: calls.append(kwargs),
    )
    assert d.fetch_or_create_job() == "pref-abcde"
    assert calls[0]["job_name"] == "pref-abcde"
    assert calls[0]["mark_complete_after_tasks_run"] is True


def test_wrapper_submits_tasks_and_calls_function(monkeypatch, deco_mod):
    d = deco_mod.CFAAzureBatchDecorator.__new__(deco_mod.CFAAzureBatchDecorator)
    d.job_configuration = {
        "Job": {"parent_task": "p1,p2"},
        "Pool": {"container_image_name": "img:tag"},
    }
    d.task_interval = 0
    d.docker_command = "python run.py --input {task_input} --jid {job_id}"
    d.task_parameters = ["a", "b"]

    monkeypatch.setattr(d, "fetch_or_create_job", lambda: "jobx")
    monkeypatch.setattr(deco_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(deco_mod, "generate_random_string", lambda length: "XYZ")

    calls = []

    def fake_add_task(**kwargs):
        calls.append(kwargs)
        return f"tid-{len(calls)}"

    monkeypatch.setattr(d, "add_task", fake_add_task)

    @d
    def run(x, y):
        return x + y

    result = run(1, 2)
    assert result == 3
    assert len(calls) == 2
    assert calls[0]["job_name"] == "jobx"
    assert calls[0]["depends_on"] == ["p1", "p2"]
    assert calls[0]["container_image_name"] == "img:tag"
    assert "--input a" in calls[0]["command_line"]
    assert calls[0]["name_suffix"] == "jobx_task_XYZ_"
    assert d.task_id == "tid-2"


def test_wrapper_uses_default_container_when_not_configured(monkeypatch, deco_mod):
    d = deco_mod.CFAAzureBatchDecorator.__new__(deco_mod.CFAAzureBatchDecorator)
    d.job_configuration = {"Job": {}, "Pool": {}}
    d.task_interval = 0
    d.docker_command = "echo {task_input} {job_id}"
    d.task_parameters = [1]

    monkeypatch.setattr(d, "fetch_or_create_job", lambda: "joby")
    monkeypatch.setattr(deco_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(deco_mod, "generate_random_string", lambda length: "QWE")

    seen = {}

    def fake_add_task(**kwargs):
        seen.update(kwargs)
        return "tid-1"

    monkeypatch.setattr(d, "add_task", fake_add_task)

    @d
    def run():
        return "ok"

    assert run() == "ok"
    assert seen["depends_on"] is None
    assert seen["container_image_name"] == "python:latest"
