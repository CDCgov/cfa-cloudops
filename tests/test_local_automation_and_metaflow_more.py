import importlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from cfa.cloudops.local import automation as local_automation


@pytest.fixture
def svc_mod(monkeypatch):
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

    return importlib.import_module(
        "cfa.cloudops.metaflow.custom_metaflow.cfa_batch_pool_service"
    )


class FakeLocalClient:
    def __init__(self):
        self.calls = {
            "upload_folders": [],
            "upload_files": [],
            "create_job": [],
            "add_task": [],
            "add_tasks_from_yaml": [],
            "monitor_job": [],
        }
        self.cont_name = None

    def upload_folders(self, **kwargs):
        self.calls["upload_folders"].append(kwargs)

    def upload_files(self, **kwargs):
        self.calls["upload_files"].append(kwargs)

    def create_job(self, **kwargs):
        self.calls["create_job"].append(kwargs)

    def add_task(self, **kwargs):
        self.calls["add_task"].append(kwargs)
        return f"tid-{len(self.calls['add_task'])}"

    def add_tasks_from_yaml(self, **kwargs):
        self.calls["add_tasks_from_yaml"].append(kwargs)

    def monitor_job(self, job_name):
        self.calls["monitor_job"].append(job_name)


def test_local_run_experiment_client_creation_failure(monkeypatch):
    monkeypatch.setattr(
        local_automation,
        "CloudClient",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        local_automation.toml,
        "load",
        lambda _: {
            "job": {"pool_name": "p1", "job_name": "j1", "container": "img:1"},
            "experiment": {"base_cmd": "echo {x}", "x": [1]},
        },
    )

    assert local_automation.run_experiment("exp.toml") is None


def test_local_run_experiment_pool_missing_file_uses_job_container(monkeypatch):
    fake = FakeLocalClient()
    monkeypatch.setattr(local_automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(local_automation.os.path, "exists", lambda p: False)
    monkeypatch.setattr(
        local_automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "p1",
                "job_name": "j1",
                "container": "repo/image:tag",
                "monitor_job": True,
            },
            "experiment": {"base_cmd": "echo {x}", "x": [1]},
        },
    )

    local_automation.run_experiment("exp.toml")

    assert fake.calls["create_job"][0]["pool_name"] == "p1"
    assert fake.calls["add_task"][0]["container_image_name"] == "repo_image_tag.j1"
    assert fake.calls["monitor_job"] == ["j1"]


def test_local_run_experiment_pool_exists_docker_errors_and_yaml(monkeypatch):
    fake = FakeLocalClient()
    monkeypatch.setattr(local_automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(local_automation.os.path, "exists", lambda p: True)
    monkeypatch.setattr(
        local_automation.Path,
        "read_text",
        lambda self: "{'image_name': 'repo/image:tag'}",
    )

    class FakeDockerEnv:
        def ping(self):
            raise RuntimeError("docker down")

        class images:
            @staticmethod
            def get(name):
                raise RuntimeError("missing image")

    monkeypatch.setattr(local_automation.docker, "from_env", lambda timeout=8: FakeDockerEnv())
    monkeypatch.setattr(
        local_automation.toml,
        "load",
        lambda _: {
            "job": {"pool_name": "p1", "job_name": "j2"},
            "experiment": {"base_cmd": "python task.py", "exp_yaml": "grid.yaml"},
        },
    )

    local_automation.run_experiment("exp.toml")

    assert fake.cont_name == "repo_image_tag"
    assert fake.calls["add_tasks_from_yaml"][0]["file_path"] == "grid.yaml"


def test_local_run_tasks_client_creation_failure(monkeypatch):
    monkeypatch.setattr(
        local_automation,
        "CloudClient",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        local_automation.toml,
        "load",
        lambda _: {"job": {"pool_name": "p", "job_name": "j"}, "task": []},
    )

    assert local_automation.run_tasks("tasks.toml") is None


def test_local_run_tasks_missing_pool_name(monkeypatch):
    fake = FakeLocalClient()
    monkeypatch.setattr(local_automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(local_automation.toml, "load", lambda _: {"job": {}, "task": []})

    assert local_automation.run_tasks("tasks.toml") is None


def test_local_run_tasks_no_container_reads_pool_and_monitor_false(monkeypatch):
    fake = FakeLocalClient()
    monkeypatch.setattr(local_automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(
        local_automation.Path,
        "read_text",
        lambda self: "{'image_name': 'repo/image:tag'}",
    )
    monkeypatch.setattr(
        local_automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "p1",
                "job_name": "j3",
                "save_logs_to_blob": "logs",
                "logs_folder": "folder",
                "task_retries": 4,
                "monitor_job": False,
            },
            "upload": {"container_name": "cont", "files": ["x.txt"]},
            "task": [
                {"name": "a", "cmd": "echo a"},
                {"name": "b", "cmd": "echo b", "depends_on": ["a"]},
            ],
        },
    )

    assert local_automation.run_tasks("tasks.toml") is None

    assert fake.calls["create_job"][0]["task_retries"] == 4
    assert fake.calls["upload_files"][0]["location_in_blob"] == ""
    assert fake.calls["add_task"][0]["run_dependent_tasks_on_fail"] is False
    assert fake.calls["add_task"][1]["depends_on"] == ["tid-1"]
    assert fake.calls["add_task"][1]["container_image_name"] == "repo_image_tag.j3"
    assert fake.calls["monitor_job"] == []


def test_batch_pool_service_init(monkeypatch, svc_mod):
    attrs = {
        "AZURE_TENANT_ID": "tenant",
        "AZURE_SUBSCRIPTION_ID": "sub",
        "AZURE_SP_CLIENT_ID": "cid",
        "AZURE_CLIENT_SECRET": "secret",
        "AZURE_KEYVAULT_ENDPOINT": "kv",
        "AZURE_KEYVAULT_SP_SECRET_ID": "sid",
        "AZURE_RESOURCE_GROUP": "rg",
        "AZURE_BLOB_STORAGE_ACCOUNT": "blob",
        "AZURE_SUBNET_ID": "subnet",
        "AZURE_BATCH_ACCOUNT": "batch",
    }
    monkeypatch.setattr(svc_mod, "dotenv_values", lambda _: attrs)
    monkeypatch.setattr(svc_mod.toml, "load", lambda _: {"Pool": {"parallel_pool_limit": "2"}})

    cred = SimpleNamespace(compute_node_identity_reference="idref")
    monkeypatch.setattr(svc_mod, "SPCredentialHandler", lambda **kwargs: cred)
    monkeypatch.setattr(svc_mod, "get_batch_management_client", lambda c: "bmc")

    svc = svc_mod.CFABatchPoolService(".env", "job.toml")
    assert svc.parallel_pool_limit == 2
    assert svc.batch_mgmt_client == "bmc"


def test_batch_pool_service_setup_pools_branches(monkeypatch, svc_mod):
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.parallel_pool_limit = 2
    called = []

    monkeypatch.setattr(
        svc,
        "_CFABatchPoolService__setup_pool",
        lambda pool_name: called.append(pool_name),
    )

    svc.job_configuration = {"Pool": {"pool_name": "fixed"}}
    svc.setup_pools()
    assert called == ["fixed"]

    called.clear()
    svc.job_configuration = {"Pool": {}}
    svc.setup_pools(pools=["p1", "p2"])
    assert called == ["p1", "p2"]

    called.clear()
    svc.job_configuration = {"Pool": {"pool_name_prefix": "pref_"}}
    svc.setup_pools()
    assert called == ["pref_0", "pref_1"]


def test_batch_pool_service_setup_pool_paths(monkeypatch, svc_mod):
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.batch_pools = []
    svc.cred = SimpleNamespace(azure_resource_group_name="rg", azure_batch_account="acct")
    svc.batch_mgmt_client = "bmc"

    create_calls = []
    monkeypatch.setattr(svc_mod.bh, "check_pool_exists", lambda *a, **k: True)
    monkeypatch.setattr(svc, "_CFABatchPoolService__create_containers", lambda: "mc")
    monkeypatch.setattr(svc, "_CFABatchPoolService__create_pool_configuration", lambda n, m: "pc")
    monkeypatch.setattr(svc, "_CFABatchPoolService__create_pool", lambda n, p: create_calls.append((n, p)))

    svc._CFABatchPoolService__setup_pool("pool-a")
    assert svc.batch_pools == ["pool-a"]
    assert create_calls == []

    monkeypatch.setattr(svc_mod.bh, "check_pool_exists", lambda *a, **k: False)
    svc._CFABatchPoolService__setup_pool("pool-b")
    assert ("pool-b", "pc") in create_calls


def test_batch_pool_service_create_containers(monkeypatch, svc_mod):
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.job_configuration = {"Pool": {"input_mount": "in", "output_mount": "out"}}
    svc.cred = SimpleNamespace(
        azure_blob_storage_account="blobacct",
        compute_node_identity_reference="idref",
    )

    seen = {}

    def fake_get_node_mount_config(**kwargs):
        seen.update(kwargs)
        return {"mount": "ok"}

    monkeypatch.setattr(svc_mod, "get_node_mount_config", fake_get_node_mount_config)

    out = svc._CFABatchPoolService__create_containers()
    assert out == {"mount": "ok"}
    assert seen["storage_containers"] == ["in", "out"]


def test_batch_pool_service_create_pool_configuration(monkeypatch, svc_mod):
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.job_configuration = {
        "Pool": {
            "autoscale": "false",
            "task_slots_per_node": "3",
            "container_image_name": "repo/image:tag",
            "vm_size": "Standard_D2s_v3",
        }
    }
    svc.cred = SimpleNamespace(
        azure_subnet_id="subnet",
        azure_user_assigned_identity="uami",
        azure_container_registry="registry",
    )

    fake_pool_cfg = SimpleNamespace(
        deployment_configuration=SimpleNamespace(
            virtual_machine_configuration=SimpleNamespace(
                node_placement_configuration=None
            )
        )
    )

    monkeypatch.setattr(svc_mod, "get_default_pool_config", lambda **kwargs: fake_pool_cfg)
    monkeypatch.setattr(
        svc,
        "_CFABatchPoolService__setup_fixedscale_configuration",
        lambda pool_config: pool_config,
    )

    assigned = []
    monkeypatch.setattr(svc_mod, "assign_container_config", lambda p, c: assigned.append((p, c)))

    monkeypatch.setattr(
        svc_mod,
        "models",
        SimpleNamespace(
            ContainerConfiguration=lambda **kwargs: SimpleNamespace(**kwargs),
            NodePlacementConfiguration=lambda **kwargs: SimpleNamespace(**kwargs),
            NodePlacementPolicyType=SimpleNamespace(regional="regional"),
        ),
    )

    out = svc._CFABatchPoolService__create_pool_configuration("pool-a", {"mount": "ok"})
    assert out.task_slots_per_node == 3
    assert assigned
    assert (
        out.deployment_configuration.virtual_machine_configuration.node_placement_configuration.policy
        == "regional"
    )


def test_batch_pool_service_create_pool_and_delete(monkeypatch, svc_mod):
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.cred = SimpleNamespace(azure_resource_group_name="rg", azure_batch_account="acct")
    calls = []

    class PoolAPI:
        def create(self, **kwargs):
            calls.append(kwargs)

    svc.batch_mgmt_client = SimpleNamespace(pool=PoolAPI())
    svc._CFABatchPoolService__create_pool("pool-ok", "cfg")
    assert svc.pool_name == "pool-ok"
    assert calls[0]["pool_name"] == "pool-ok"

    class FailingPoolAPI:
        def create(self, **kwargs):
            raise RuntimeError("bad")

    svc.batch_mgmt_client = SimpleNamespace(pool=FailingPoolAPI())
    with pytest.raises(RuntimeError, match="Failed to create pool"):
        svc._CFABatchPoolService__create_pool("pool-fail", "cfg")

    svc.batch_pools = ["p1", "p2"]
    deleted = []
    monkeypatch.setattr(svc_mod.bh, "delete_pool", lambda **kwargs: deleted.append(kwargs["pool_name"]))
    assert svc.delete_all_pools() is True
    assert deleted == ["p1", "p2"]


def test_batch_pool_service_step_parameters_with_explicit_pools(svc_mod):
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.job_configuration = {"Job": {"docker_command": "python {task_input}"}}
    svc.parallel_pool_limit = 4
    svc.batch_pools = ["pool0", "pool1"]
    svc.attributes = {"k": "v"}

    params = svc.setup_step_parameters([1, 2, 3, 4], pools=["pool0", "pool1"])
    assert len(params) == 2
    assert params[0]["pool_name"] == "pool0"
    assert params[1]["pool_name"] == "pool1"
