from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from cfa.cloudops.local import _client as local_client
from cfa.cloudops.local import automation as local_automation
from cfa.cloudops.local import batch as local_batch
from cfa.cloudops.local import helpers as local_helpers


def test_local_batch_task_dependencies_and_repr():
    t1 = local_batch.Task("echo 1", id="t1")
    t2 = local_batch.Task("echo 2", id="t2")

    t1.before(t2)
    assert t1 in t2.deps

    t2.after(t1)
    assert t1 in t2.deps

    t1.set_downstream(t2)
    t2.set_upstream(t1)

    assert repr(t1) == "t1"


def test_local_batch_pool_and_job_models():
    pool = local_batch.Pool("pool", "image")
    job = local_batch.Job("job", "pool", 1, True)

    assert pool.pool_id == "pool"
    assert job.job_id == "job"


def test_local_helpers_add_job_and_create_container(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    job = local_helpers.add_job("job1", "pool1")
    assert job.job_id == "job1"
    assert Path("tmp/jobs/job1.txt").exists()

    c = local_helpers.create_container("cont1")
    assert c == "container_client"
    assert Path("cont1").exists()


def test_local_helpers_upload_and_download_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    Path("cont").mkdir()
    src = Path("src.txt")
    src.write_text("hello")

    local_helpers.upload_to_storage_container(
        filepath=str(src),
        location="folder",
        container_name="cont",
        verbose=False,
    )
    assert Path("cont/folder/src.txt").read_text() == "hello"

    dest = Path("dest.txt")
    local_helpers.download_file(None, "src.txt", str(dest), True, True)
    assert dest.read_text() == "hello"


def test_local_helpers_upload_folder_filters(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    Path("cont").mkdir()
    Path("folder/sub").mkdir(parents=True)
    Path("folder/a.txt").write_text("a")
    Path("folder/sub/b.csv").write_text("b")

    uploaded = local_helpers.upload_folder(
        folder="folder",
        container_name="cont",
        include_extensions=["txt"],
        location_in_blob="blobdir",
    )

    assert any(x.endswith("a.txt") for x in uploaded)
    assert Path("cont/blobdir/a.txt").exists()


def test_local_helpers_docker_helpers(monkeypatch):
    class FakeImage:
        def tag(self, name):
            self.name = name

    fake_image = FakeImage()

    docker_env = SimpleNamespace(
        ping=lambda: True,
        images=SimpleNamespace(
            get=lambda name: fake_image,
            list=lambda: [SimpleNamespace(tags=["x:1"])],
        ),
    )

    monkeypatch.setattr("cfa.cloudops.local.helpers.docker.from_env", lambda timeout=10: docker_env)
    monkeypatch.setattr("cfa.cloudops.local.helpers.os.path.exists", lambda p: True)
    monkeypatch.setattr("cfa.cloudops.local.helpers.sp.run", lambda *a, **k: None)

    out1 = local_helpers.package_and_upload_dockerfile("reg", "repo", "v1")
    out2 = local_helpers.upload_docker_image("local:1", "reg", "repo", "v2")

    assert out1 == "reg.azurecr.io/repo:v1"
    assert out2 == "reg.azurecr.io/repo:v2"


def test_local_helpers_yaml_and_walk(tmp_path, monkeypatch):
    config = {
        "param": [1, 2],
        "flag(flag)": ["x", ""],
    }
    fpath = tmp_path / "args.yaml"
    with open(fpath, "w") as f:
        yaml.safe_dump(config, f)

    class FakeGrid:
        def to_dicts(self):
            return [{"param": 1, "flag(flag)": "x"}, {"param": 2, "flag(flag)": ""}]

    monkeypatch.setattr(local_helpers, "parse", lambda raw: FakeGrid())

    args = local_helpers.get_args_from_yaml(str(fpath))
    cmds = local_helpers.get_tasks_from_yaml("python script.py", str(fpath))

    assert len(args) > 0
    assert all(x.startswith("python script.py") for x in cmds)


def test_local_helpers_format_extensions_and_walk_folder(tmp_path):
    assert local_helpers.format_extensions("txt") == [".txt"]
    assert local_helpers.format_extensions([".txt", "csv"]) == [".txt", ".csv"]

    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "f.txt").write_text("x")
    files = local_helpers.walk_folder(str(tmp_path / "d"))
    assert any(x.endswith("f.txt") for x in files)


def test_local_cloudclient_basic_ops(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    docker_env = SimpleNamespace(
        ping=lambda: True,
        images=SimpleNamespace(
            get=lambda name: SimpleNamespace(short_id="img1"),
            pull=lambda name: SimpleNamespace(tags=[name]),
        ),
    )

    monkeypatch.setattr("cfa.cloudops.local._client.docker.from_env", lambda timeout=8: docker_env)
    monkeypatch.setattr("cfa.cloudops.local._client.sp.run", lambda *a, **k: None)

    c = local_client.CloudClient()

    with pytest.raises(ValueError):
        c.create_pool("p1", container_image_name=None)

    out = c.create_pool("pool1", container_image_name="python:3.11")
    assert out["pool_id"] == "pool1"
    assert Path("tmp/pools/pool1.txt").exists()

    c.create_job("job1", "pool1")
    assert Path("tmp/jobs/job1.txt").exists()

    tid = c.add_task("job1", "echo hello")
    assert isinstance(tid, int)

    c.create_blob_container("cont")
    Path("cont/file.txt").write_text("x")
    assert c.list_blob_files("cont") == ["file.txt"]


def test_local_cloudclient_blob_delete_and_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    c = local_client.CloudClient()
    Path("tmp/jobs").mkdir(parents=True)
    Path("tmp/pools").mkdir(parents=True)
    Path("tmp/jobs/jobx.txt").write_text("poolx 0 False")
    Path("tmp/pools/poolx.txt").write_text("{'image_name': 'img:1', 'mount_str': ''}")

    monkeypatch.setattr("cfa.cloudops.local._client.sp.run", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.local._client.docker.from_env", lambda: SimpleNamespace(images=SimpleNamespace(get=lambda name: SimpleNamespace(short_id='i'))))

    monkeypatch.setattr("cfa.cloudops.local._client.helpers.get_tasks_from_yaml", lambda **k: ["echo 1", "echo 2"])
    monkeypatch.setattr(c, "add_task", lambda **k: 1)

    tasks = c.add_tasks_from_yaml("jobx", "python x.py", "cfg.yml")
    assert tasks == [1, 1]

    Path("cont").mkdir()
    Path("cont/a.txt").write_text("x")
    c.delete_blob_file("a.txt", "cont")
    assert not Path("cont/a.txt").exists()

    Path("cont/f").mkdir(parents=True)
    Path("cont/f/b.txt").write_text("y")
    c.delete_blob_folder("f", "cont")
    assert not Path("cont/f").exists()


def test_local_automation_missing_pool_name(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.local.automation.toml.load", lambda _: {"job": {}})
    assert local_automation.run_experiment("exp.toml") is None


def test_local_automation_run_tasks_happy_path(monkeypatch):
    config = {
        "job": {"pool_name": "p1", "job_name": "j1"},
        "task": [{"name": "t1", "cmd": "echo 1"}, {"name": "t2", "cmd": "echo 2", "depends_on": ["t1"]}],
    }

    fake_client = SimpleNamespace(
        upload_folders=lambda **k: None,
        upload_files=lambda **k: None,
        create_job=lambda **k: None,
        add_task=lambda **k: "task-id",
        monitor_job=lambda *a, **k: None,
    )

    monkeypatch.setattr("cfa.cloudops.local.automation.toml.load", lambda _: config)
    monkeypatch.setattr("cfa.cloudops.local.automation.CloudClient", lambda dotenv_path=None: fake_client)
    monkeypatch.setattr("cfa.cloudops.local.automation.Path.read_text", lambda self: "{'image_name': 'img:1'}")

    assert local_automation.run_tasks("tasks.toml") is None
