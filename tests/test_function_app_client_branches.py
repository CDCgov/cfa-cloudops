import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from cfa.cloudops import _function_app_client as func_mod


@pytest.fixture
def base_client():
    c = func_mod.FunctionAppClient.__new__(func_mod.FunctionAppClient)
    c.function_app_name = "fa-app"
    c.cred = SimpleNamespace(
        azure_resource_group_name="rg",
        azure_subscription_id="sub",
        azure_tenant_id="tenant",
        azure_client_id="cid",
        azure_client_secret="secret",
        azure_blob_storage_account="blob",
        client_secret_credential="cred",
    )
    c.update_function_database = True
    c.conn = None
    return c


def test_clone_deployment_slot_success_and_failure(monkeypatch, base_client):
    calls = []

    monkeypatch.setattr(func_mod.subprocess, "run", lambda args, check=True: calls.append(args))
    assert base_client._clone_deployment_slot("newslot", "rollback") is True
    assert "--configuration-source" in calls[0]
    assert "fa-app/rollback" in calls[0]

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "az")

    monkeypatch.setattr(func_mod.subprocess, "run", fail)
    assert base_client._clone_deployment_slot("newslot", "production") is False


def test_swap_and_delete_deployment_slot(monkeypatch, base_client):
    web_apps = SimpleNamespace(
        begin_swap_slot_with_production=lambda **k: SimpleNamespace(result=lambda: "prod"),
        begin_swap_slot=lambda **k: SimpleNamespace(result=lambda: "slot"),
        delete_slot=lambda **k: "deleted",
    )
    monkeypatch.setattr(
        func_mod,
        "WebSiteManagementClient",
        lambda cred, sub: SimpleNamespace(web_apps=web_apps),
    )

    assert base_client._swap_deployment_slot("staging", "production") == "prod"
    assert base_client._swap_deployment_slot("staging", "blue") == "slot"
    base_client._delete_deployment_slot("rollback")


def test_find_available_and_allocate_function_app(monkeypatch, base_client):
    class FakeSqlResult:
        def __init__(self, df):
            self._df = df

        def fetchdf(self):
            return self._df

    class FakeConn:
        def __init__(self):
            self.exec_calls = []

        def sql(self, query):
            if "SELECT FunctionAppName" in query:
                return FakeSqlResult(pd.DataFrame([{"FunctionAppName": "fa-01"}]))
            return self

        def execute(self, query):
            self.exec_calls.append(query)
            return self

    fc = FakeConn()
    monkeypatch.setattr(base_client, "_get_database_connection", lambda: fc)

    assert base_client._find_available_function_app() == "fa-01"
    assert base_client._allocate_function_app() is True
    assert any("UPDATE function_apps" in q for q in fc.exec_calls)
    assert any("COPY function_apps" in q for q in fc.exec_calls)


def test_log_into_portal_and_restart_paths(monkeypatch, base_client):
    calls = []
    monkeypatch.setattr(func_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(func_mod.subprocess, "run", lambda args, check=True: calls.append(args))

    assert base_client._log_into_portal() is True
    assert calls[0][0:3] == ["az", "login", "--service-principal"]
    assert calls[1][0:3] == ["az", "account", "set"]

    assert base_client._restart_function() is True

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "az")

    monkeypatch.setattr(func_mod.subprocess, "run", fail)
    assert base_client._log_into_portal() is False
    assert base_client._restart_function() is False


def test_enable_health_check_and_update_settings(monkeypatch, base_client):
    seen = []
    monkeypatch.setattr(func_mod.subprocess, "run", lambda args, check=True: seen.append(args))

    assert base_client._enable_health_check(slot="staging") is True
    assert "--slot" in seen[0]

    assert base_client._update_app_settings([("A", "1"), ("B", "2")], slot="staging") is True
    assert any("A=1" in x for x in seen[1])

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "az")

    monkeypatch.setattr(func_mod.subprocess, "run", fail)
    assert base_client._enable_health_check() is False
    assert base_client._update_app_settings([("A", "1")]) is False


def test_add_user_package_delete_folder_copy_template(tmp_path, monkeypatch, base_client):
    monkeypatch.chdir(tmp_path)

    # Current implementation expects a callable name even for string input.
    with pytest.raises(AttributeError):
        base_client._add_user_package_to_deployment("print('x')")

    # add_user_package with callable
    def user_package_func():
        return 1

    base_client._add_user_package_to_deployment(user_package_func)
    out = Path("user_package.py").read_text()
    assert "def user_package_func" in out
    assert "user_package_func()" in out

    # delete_deployment_folder both branches
    dep = tmp_path / base_client.function_app_name
    dep.mkdir()
    assert base_client._delete_deployment_folder() is True
    assert base_client._delete_deployment_folder() is False

    # copy_template_to_deployment
    template = tmp_path / "template"
    template.mkdir()
    for n in ["timer_blueprint", "function_app", "containers", "cfa_service", "user_package"]:
        (template / f"{n}.txt").write_text(n)
    for n in ["host", "local.settings"]:
        (template / f"{n}.txt").write_text(n)
    (template / "requirements.txt").write_text("pytest")

    (tmp_path / base_client.function_app_name).mkdir()
    base_client._copy_template_to_deployment(str(tmp_path))

    assert (tmp_path / base_client.function_app_name / "timer_blueprint.py").exists()
    assert (tmp_path / base_client.function_app_name / "host.json").exists()
    assert (tmp_path / base_client.function_app_name / "requirements.txt").exists()


def test_publish_function_success_and_failure(tmp_path, monkeypatch, base_client):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(func_mod.time, "sleep", lambda _: None)

    clone_calls = []
    delete_calls = []
    settings_calls = []

    monkeypatch.setattr(base_client, "_delete_deployment_folder", lambda: True)

    def copy_template(parent_folder):
        Path(base_client.function_app_name).mkdir(exist_ok=True)
        Path(base_client.function_app_name, "requirements.txt").write_text("base\n")

    monkeypatch.setattr(base_client, "_copy_template_to_deployment", copy_template)
    monkeypatch.setattr(base_client, "_add_user_package_to_deployment", lambda user_package: None)
    monkeypatch.setattr(base_client, "_clone_deployment_slot", lambda slot_name, source_slot=None: clone_calls.append((slot_name, source_slot)) or True)
    monkeypatch.setattr(base_client, "_delete_deployment_slot", lambda slot: delete_calls.append(slot))
    monkeypatch.setattr(base_client, "_update_app_settings", lambda settings, slot=None: settings_calls.append(settings) or True)
    monkeypatch.setattr(base_client, "_enable_health_check", lambda slot=None: True)
    monkeypatch.setattr(base_client, "_swap_deployment_slot", lambda source_slot, target_slot: True)

    monkeypatch.setattr(func_mod.subprocess, "run", lambda args, check=True: None)
    monkeypatch.setattr(func_mod.FunctionAppClient, "get_health_check_flag", lambda *a, **k: True)
    monkeypatch.setattr(
        func_mod.FunctionAppClient,
        "list_slots",
        lambda *a, **k: [
            ("fa-app/rollback", "Running", True, None, None),
            ("fa-app/backup", "Running", True, None, None),
            ("fa-app/rollbackprevious", "Running", True, None, None),
        ],
    )

    def user_package():
        return 42

    assert base_client._publish_function(
        schedule="* * * * * *",
        user_package=user_package,
        dependencies=["numpy==1.0"],
        environment_variables=[("K", "V")],
    ) is True
    assert ("rollbackprevious", "rollback") in clone_calls
    assert ("rollback", None) in clone_calls
    assert ("backup", None) in clone_calls
    assert "rollback" in delete_calls
    assert "backup" in delete_calls
    assert "rollbackprevious" in delete_calls
    assert len(settings_calls) == 2

    base_client.function_app_name = "fa-app-2"
    monkeypatch.setattr(func_mod.FunctionAppClient, "get_health_check_flag", lambda *a, **k: False)
    monkeypatch.setattr(func_mod.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "func")))
    assert base_client._publish_function("* * * * * *", user_package) is False


def test_deploy_function_branches(monkeypatch, base_client):
    # Login failure
    monkeypatch.setattr(base_client, "_log_into_portal", lambda: False)
    assert base_client.deploy_function("* * * * * *", lambda: 1) is False

    # Missing function app name and no available app
    base_client.function_app_name = None
    monkeypatch.setattr(base_client, "_log_into_portal", lambda: True)
    monkeypatch.setattr(base_client, "_find_available_function_app", lambda: None)
    assert base_client.deploy_function("* * * * * *", lambda: 1) is False

    # Publish failure
    monkeypatch.setattr(base_client, "_find_available_function_app", lambda: "fa-a")
    monkeypatch.setattr(base_client, "_publish_function", lambda *a, **k: False)
    assert base_client.deploy_function("* * * * * *", lambda: 1) is False

    # Allocate false but continue, restart fails
    monkeypatch.setattr(base_client, "_publish_function", lambda *a, **k: True)
    monkeypatch.setattr(base_client, "_allocate_function_app", lambda: False)
    monkeypatch.setattr(base_client, "_restart_function", lambda: False)
    assert base_client.deploy_function("* * * * * *", lambda: 1) is False

    # Happy path
    monkeypatch.setattr(base_client, "_allocate_function_app", lambda: True)
    monkeypatch.setattr(base_client, "_restart_function", lambda: True)
    assert base_client.deploy_function("* * * * * *", lambda: 1) is True
