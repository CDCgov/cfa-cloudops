import importlib
from types import SimpleNamespace

import pytest


def _reload_cloudops(monkeypatch, log_output=None):
    if log_output is None:
        monkeypatch.delenv("LOG_OUTPUT", raising=False)
    else:
        monkeypatch.setenv("LOG_OUTPUT", log_output)

    monkeypatch.setattr("logging.basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(
        "logging.StreamHandler", lambda stream=None: SimpleNamespace(kind="stream")
    )
    monkeypatch.setattr(
        "logging.FileHandler", lambda path: SimpleNamespace(kind="file", path=path)
    )

    import cfa.cloudops as cloudops

    return importlib.reload(cloudops)


def test_cloudops_getattr_known_symbols(monkeypatch):
    cloudops = _reload_cloudops(monkeypatch, None)

    from cfa.cloudops._cloudclient import CloudClient

    assert cloudops.__getattr__("CloudClient") is CloudClient
    assert "CloudClient" in cloudops.__all__


def test_cloudops_getattr_unknown_symbol(monkeypatch):
    cloudops = _reload_cloudops(monkeypatch, None)

    with pytest.raises(AttributeError):
        cloudops.__getattr__("DoesNotExist")


@pytest.mark.parametrize("log_output", ["both", "file", "std", "stdout"])
def test_cloudops_log_output_variants(monkeypatch, log_output):
    made_dirs = []

    monkeypatch.setattr("os.path.exists", lambda path: False)
    monkeypatch.setattr("os.mkdir", lambda path: made_dirs.append(path))

    cloudops = _reload_cloudops(monkeypatch, log_output)

    assert cloudops is not None
    if log_output.startswith("both") or log_output.startswith("file"):
        assert "logs" in made_dirs


def test_cloudops_log_output_unrecognized(monkeypatch, capsys):
    monkeypatch.setattr("os.path.exists", lambda path: True)
    cloudops = _reload_cloudops(monkeypatch, "weird-output")

    assert cloudops is not None
    captured = capsys.readouterr()
    assert "Did not recognize weird-output" in captured.out
