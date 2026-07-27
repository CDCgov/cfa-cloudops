from types import SimpleNamespace

from cfa.cloudops import automation


class FakeClient:
    def __init__(self, dotenv_path=None):
        self.dotenv_path = dotenv_path
        self.cred = SimpleNamespace(
            azure_resource_group_name="rg",
            azure_batch_account="acct",
        )
        self.batch_mgmt_client = "bmc"
        self.calls = {
            "upload_folders": [],
            "upload_files": [],
            "create_job": [],
            "add_task": [],
            "add_tasks_from_yaml": [],
            "monitor_job": [],
        }

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


def test_run_experiment_returns_none_when_client_creation_fails(monkeypatch):
    monkeypatch.setattr(
        automation,
        "CloudClient",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {"pool_name": "pool", "job_name": "job"},
            "experiment": {"base_cmd": "echo hi", "x": [1]},
        },
    )

    assert automation.run_experiment("exp.toml") is None


def test_run_experiment_returns_none_without_pool_name(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.toml, "load", lambda _: {"job": {}, "experiment": {}})

    assert automation.run_experiment("exp.toml") is None


def test_run_experiment_returns_none_when_pool_missing(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {"pool_name": "pool-x", "job_name": "job"},
            "experiment": {"base_cmd": "echo hi", "x": [1]},
        },
    )
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: False)

    assert automation.run_experiment("exp.toml") is None


def test_run_experiment_yaml_upload_and_monitor(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: True)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "pool-x",
                "job_name": "job-1",
                "save_logs_to_blob": "logs",
                "logs_folder": "run1",
                "task_retries": 2,
                "container": "img:tag",
                "monitor_job": True,
            },
            "upload": {
                "container_name": "data-cont",
                "location_in_blob": "inputs",
                "folders": ["src"],
                "files": ["a.txt"],
            },
            "experiment": {
                "base_cmd": "python task.py",
                "exp_yaml": "grid.yaml",
            },
        },
    )

    assert automation.run_experiment("exp.toml", dotenv_path=".env") is None

    assert fake.calls["upload_folders"][0]["folder_names"] == ["src"]
    assert fake.calls["upload_folders"][0]["location_in_blob"] == "inputs"
    assert fake.calls["upload_files"][0]["files"] == ["a.txt"]
    assert fake.calls["create_job"][0]["job_name"] == "job-1"
    assert fake.calls["create_job"][0]["task_retries"] == 2
    assert fake.calls["add_tasks_from_yaml"][0]["file_path"] == "grid.yaml"
    assert fake.calls["monitor_job"] == ["job-1"]


def test_run_experiment_parameter_grid_adds_tasks(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: True)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "pool-x",
                "job_name": "job-2",
                "monitor_job": False,
            },
            "experiment": {
                "base_cmd": "python run.py --a {a} --b {b}",
                "a": [1, 2],
                "b": ["x", "y"],
            },
        },
    )

    automation.run_experiment("exp.toml")

    assert len(fake.calls["add_task"]) == 4
    commands = [x["command_line"] for x in fake.calls["add_task"]]
    assert "python run.py --a 1 --b x" in commands
    assert "python run.py --a 2 --b y" in commands
    assert all(x["container_image_name"] is None for x in fake.calls["add_task"])
    assert fake.calls["monitor_job"] == []


def test_run_tasks_returns_none_without_pool_name(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.toml, "load", lambda _: {"job": {}, "task": []})

    assert automation.run_tasks("tasks.toml") is None


def test_run_tasks_upload_dependencies_and_monitor(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: True)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "pool-x",
                "job_name": "job-3",
                "container": "img:tag",
                "monitor_job": True,
            },
            "upload": {
                "container_name": "data-cont",
                "folders": ["folder-a"],
                "files": ["in1.txt"],
            },
            "task": [
                {"name": "prep", "cmd": "echo prep"},
                {
                    "name": "train",
                    "cmd": "echo train",
                    "depends_on": ["prep"],
                    "run_dependent_tasks_on_fail": True,
                },
            ],
        },
    )

    assert automation.run_tasks("tasks.toml") is None

    assert fake.calls["upload_folders"][0]["location_in_blob"] == ""
    assert fake.calls["upload_files"][0]["location_in_blob"] == ""
    assert fake.calls["create_job"][0]["job_name"] == "job-3"

    first_task = fake.calls["add_task"][0]
    second_task = fake.calls["add_task"][1]
    assert first_task["depends_on"] is None
    assert second_task["depends_on"] == ["tid-1"]
    assert second_task["run_dependent_tasks_on_fail"] is True
    assert second_task["container_image_name"] == "img:tag"
    assert fake.calls["monitor_job"] == ["job-3"]


def test_run_experiment_upload_default_location_and_no_monitor_key(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: True)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "pool-x",
                "job_name": "job-4",
            },
            "upload": {
                "container_name": "data-cont",
                "folders": ["folder-a"],
                "files": ["in1.txt"],
            },
            "experiment": {
                "base_cmd": "echo {x}",
                "x": ["ok"],
            },
        },
    )

    automation.run_experiment("exp.toml")

    assert fake.calls["upload_folders"][0]["location_in_blob"] == ""
    assert fake.calls["upload_files"][0]["location_in_blob"] == ""
    assert fake.calls["monitor_job"] == []


def test_run_tasks_returns_none_when_client_creation_fails(monkeypatch):
    monkeypatch.setattr(
        automation,
        "CloudClient",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {"pool_name": "pool", "job_name": "job"},
            "task": [{"name": "prep", "cmd": "echo prep"}],
        },
    )

    assert automation.run_tasks("tasks.toml") is None


def test_run_tasks_returns_none_when_pool_missing(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {"pool_name": "pool-x", "job_name": "job"},
            "task": [{"name": "prep", "cmd": "echo prep"}],
        },
    )
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: False)

    assert automation.run_tasks("tasks.toml") is None


def test_run_tasks_optional_job_fields_present_and_monitor_false(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: True)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "pool-x",
                "job_name": "job-5",
                "save_logs_to_blob": "logs",
                "logs_folder": "folder",
                "task_retries": 3,
                "monitor_job": False,
            },
            "upload": {
                "container_name": "data-cont",
                "location_in_blob": "inputs",
                "files": ["in1.txt"],
            },
            "task": [{"name": "prep", "cmd": "echo prep"}],
        },
    )

    automation.run_tasks("tasks.toml")

    assert fake.calls["upload_files"][0]["location_in_blob"] == "inputs"
    assert fake.calls["create_job"][0]["save_logs_to_blob"] == "logs"
    assert fake.calls["create_job"][0]["logs_folder"] == "folder"
    assert fake.calls["create_job"][0]["task_retries"] == 3
    assert fake.calls["monitor_job"] == []


def test_run_tasks_no_upload_no_container_and_no_monitor_key(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(automation, "CloudClient", lambda **kwargs: fake)
    monkeypatch.setattr(automation.batch_helpers, "check_pool_exists", lambda **kwargs: True)
    monkeypatch.setattr(
        automation.toml,
        "load",
        lambda _: {
            "job": {
                "pool_name": "pool-x",
                "job_name": "job-6",
            },
            "task": [{"name": "prep", "cmd": "echo prep"}],
        },
    )

    automation.run_tasks("tasks.toml")

    assert fake.calls["upload_folders"] == []
    assert fake.calls["upload_files"] == []
    assert fake.calls["add_task"][0]["container_image_name"] is None
    assert fake.calls["monitor_job"] == []
