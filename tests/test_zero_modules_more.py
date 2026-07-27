import importlib
import sys
from types import ModuleType, SimpleNamespace

import pytest

from cfa.cloudops import autoscale
from cfa.cloudops import _containerappclient as container_mod
from cfa.cloudops import _function_app_client as func_mod


def test_autoscale_formulas_exposed():
    assert "maxNumberofVMs = 10" in autoscale.dev_autoscale_formula
    assert "maxNumberofVMs = 25" in autoscale.prod_autoscale_formula
    assert "$NodeDeallocationOption = taskcompletion;" in autoscale.dev_autoscale_formula


def test_containerappclient_methods_without_constructor():
    client = container_mod.ContainerAppClient.__new__(container_mod.ContainerAppClient)
    client.resource_group = "rg"
    client.job_name = "job1"

    c1 = SimpleNamespace(name="job1")
    c2 = SimpleNamespace(name="job2")
    job_template = SimpleNamespace(
        containers=[
            SimpleNamespace(
                name="job1",
                image="img:1",
                command=["python"],
                args=["x.py"],
                env=[{"A": "B"}],
                resources={"cpu": 1},
            )
        ]
    )
    job_info = SimpleNamespace(name="job1", template=job_template)
    job_info.as_dict = lambda: {"name": "job1"}

    jobs = SimpleNamespace(
        list_by_resource_group=lambda rg: [job_info, c2],
        begin_start=lambda **kwargs: SimpleNamespace(),
        begin_stop_execution=lambda **kwargs: SimpleNamespace(result=lambda: "ok"),
    )
    client.client = SimpleNamespace(jobs=jobs)

    assert client.list_jobs() == ["job1", "job2"]
    assert client.check_job_exists("job1") is True
    assert client.check_job_exists("missing") is False
    assert client.get_job_info() == {"name": "job1"}

    info = client.get_command_info("job1")
    assert info[0]["image"] == "img:1"

    client.start_job(job_name="job1")


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"command": "echo"}, "Command must be in list format."),
        ({"args": "arg"}, "Args must be in list format."),
        ({"env": ["A=B"]}, "Env must be in dict format."),
    ],
)
def test_containerappclient_start_job_validation_errors(kwargs, expected):
    client = container_mod.ContainerAppClient.__new__(container_mod.ContainerAppClient)
    client.resource_group = "rg"
    client.job_name = "job1"
    client.client = SimpleNamespace(
        jobs=SimpleNamespace(
            list_by_resource_group=lambda rg: [], begin_start=lambda **k: None
        )
    )

    with pytest.raises(ValueError, match=expected):
        client.start_job(**kwargs)


def test_containerappclient_start_job_with_overrides(monkeypatch):
    env_objs = []

    class FakeEnv:
        def __init__(self, name=None, value=None, secret_ref=None):
            env_objs.append((name, value, secret_ref))

    monkeypatch.setattr(container_mod, "EnvironmentVar", FakeEnv)
    monkeypatch.setattr(
        container_mod,
        "JobExecutionContainer",
        lambda **k: SimpleNamespace(**k),
    )
    monkeypatch.setattr(
        container_mod,
        "JobExecutionTemplate",
        lambda **k: SimpleNamespace(**k),
    )

    client = container_mod.ContainerAppClient.__new__(container_mod.ContainerAppClient)
    client.resource_group = "rg"
    client.job_name = "job1"

    j = SimpleNamespace(
        name="job1",
        template=SimpleNamespace(
            containers=[SimpleNamespace(name="c1", image="img", resources={"cpu": 1})]
        ),
    )
    started = {}

    def begin_start(**kwargs):
        started.update(kwargs)
        return SimpleNamespace()

    client.client = SimpleNamespace(
        jobs=SimpleNamespace(
            list_by_resource_group=lambda rg: [j],
            begin_start=begin_start,
        )
    )

    client.start_job(
        command=["python"],
        args=["main.py"],
        env={"A": "B"},
        secret_ref={"S": "secret"},
    )

    assert ("A", "B", None) in env_objs
    assert ("S", None, "secret") in env_objs
    assert started["job_name"] == "job1"


def test_containerappclient_stop_job_error_path():
    client = container_mod.ContainerAppClient.__new__(container_mod.ContainerAppClient)
    client.resource_group = "rg"

    client.client = SimpleNamespace(
        jobs=SimpleNamespace(
            begin_stop_execution=lambda **k: (_ for _ in ()).throw(RuntimeError("boom"))
        )
    )

    assert client.stop_job("j", "e") is None


def test_function_app_classmethods_and_validation(monkeypatch):
    fake_cfg = SimpleNamespace(
        additional_properties={"tags": ["one"]},
        health_check_path="/health",
    )
    monkeypatch.setattr(func_mod.FunctionAppClient, "get_configuration", lambda *a, **k: fake_cfg)

    assert func_mod.FunctionAppClient.get_tags("f") == ["one"]
    assert func_mod.FunctionAppClient.get_health_check_flag("f") is True

    monkeypatch.delenv("AZURE_RESOURCE_GROUP", raising=False)
    monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
    with pytest.raises(ValueError, match="Resource group"):
        func_mod.FunctionAppClient.list_functions("f", resource_group=None, subscription_id="sub")

    monkeypatch.setenv("AZURE_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
    with pytest.raises(ValueError, match="Subscription ID"):
        func_mod.FunctionAppClient.list_slots("f", resource_group=None, subscription_id=None)


def test_function_app_init_and_database_connection(monkeypatch):
    monkeypatch.setattr(func_mod, "EnvCredentialHandler", lambda **k: "envcred")
    monkeypatch.setattr(func_mod, "DefaultCredentialHandler", lambda **k: "defaultcred")
    monkeypatch.setattr(func_mod, "SPCredentialHandler", lambda **k: "spcred")

    c_env = func_mod.FunctionAppClient(function_app_name="f")
    c_def = func_mod.FunctionAppClient(function_app_name="f", use_federated=True)
    c_sp = func_mod.FunctionAppClient(function_app_name="f", use_sp=True)

    assert c_env.method == "env"
    assert c_def.method == "default"
    assert c_sp.method == "sp"

    sql_calls = []

    class FakeConn:
        def sql(self, q):
            sql_calls.append(q)
            return self

    monkeypatch.setattr(func_mod.duckdb, "connect", lambda database=":memory:": FakeConn())

    c_env.cred = SimpleNamespace(
        azure_tenant_id="tenant",
        azure_subscription_id="sub",
        azure_client_id="cid",
        azure_client_secret="secret",
        azure_blob_storage_account="storageacct",
    )
    conn = c_env._get_database_connection()
    assert conn is not None
    assert any("CREATE SECRET" in q for q in sql_calls)


def test_metaflow_imports_and_decorator_basics(monkeypatch):
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

    metaflow_mod = importlib.reload(importlib.import_module("cfa.cloudops.metaflow"))
    assert FakeAzureBatchDecorator in metaflow_mod.STEP_DECORATORS
    assert FakeLocalMetadataProvider in metaflow_mod.METADATA_PROVIDERS

    # Stub minimal metaflow.decorators for decorator module import.
    metaflow_pkg = ModuleType("metaflow")
    metaflow_decorators = ModuleType("metaflow.decorators")

    class DummyStepDecorator:
        def __init__(self, *args, **kwargs):
            pass

    metaflow_decorators.StepDecorator = DummyStepDecorator
    monkeypatch.setitem(sys.modules, "metaflow", metaflow_pkg)
    monkeypatch.setitem(sys.modules, "metaflow.decorators", metaflow_decorators)

    deco_mod = importlib.reload(
        importlib.import_module(
            "cfa.cloudops.metaflow.custom_metaflow.plugins.decorators.cfa_azure_batch_decorator"
        )
    )

    assert len(deco_mod.generate_random_string(6)) == 6


def test_cfa_batch_pool_service_setup_step_parameters(monkeypatch):
    svc_mod = importlib.import_module(
        "cfa.cloudops.metaflow.custom_metaflow.cfa_batch_pool_service"
    )
    svc = svc_mod.CFABatchPoolService.__new__(svc_mod.CFABatchPoolService)
    svc.job_configuration = {"Job": {"docker_command": "python {task_input}"}}
    svc.parallel_pool_limit = 2
    svc.batch_pools = ["pool0", "pool1"]
    svc.attributes = {"AZURE_SUBSCRIPTION_ID": "sub"}

    params = svc.setup_step_parameters([1, 2, 3, 4])
    assert len(params) == 2
    assert params[0]["pool_name"] == "pool0"


def test_decorators_init_exports():
    mod = importlib.import_module(
        "cfa.cloudops.metaflow.custom_metaflow.plugins.decorators"
    )
    assert "cfa_azure_batch" in mod.decorators
