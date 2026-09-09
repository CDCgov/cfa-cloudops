import importlib
import logging
from unittest.mock import MagicMock

import pytest


def _reload_cloudops(monkeypatch, log_output=None):
    if log_output is None:
        monkeypatch.delenv("LOG_OUTPUT", raising=False)
    else:
        monkeypatch.setenv("LOG_OUTPUT", log_output)

    monkeypatch.delenv("LOG_LEVEL", raising=False)

    monkeypatch.setattr("logging.basicConfig", lambda **kwargs: None)
    monkeypatch.setattr("logging.StreamHandler", MagicMock())
    monkeypatch.setattr("logging.FileHandler", MagicMock())

    import cfa.cloudops as cloudops

    return importlib.reload(cloudops)


def test_cloudops_default_log_level_warning(monkeypatch):
    captured = {}

    monkeypatch.delenv("LOG_OUTPUT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.setattr("logging.StreamHandler", MagicMock())
    monkeypatch.setattr("logging.FileHandler", MagicMock())

    def fake_basic_config(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("logging.basicConfig", fake_basic_config)

    import cfa.cloudops as cloudops

    importlib.reload(cloudops)

    assert captured["level"] == logging.WARNING


def test_cloudops_log_level_none_disables_logging(monkeypatch):
    captured = {}

    monkeypatch.delenv("LOG_OUTPUT", raising=False)
    monkeypatch.setenv("LOG_LEVEL", "none")
    monkeypatch.setattr("logging.StreamHandler", MagicMock())
    monkeypatch.setattr("logging.FileHandler", MagicMock())

    def fake_basic_config(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("logging.basicConfig", fake_basic_config)

    import cfa.cloudops as cloudops

    importlib.reload(cloudops)

    assert captured["level"] == logging.CRITICAL + 1


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

    def fake_mkdir(path, mode=0o777):
        made_dirs.append(str(path))

    monkeypatch.setattr("os.mkdir", fake_mkdir)

    cloudops = _reload_cloudops(monkeypatch, log_output)

    assert cloudops is not None
    if log_output.startswith("both") or log_output.startswith("file"):
        assert "logs" in made_dirs


def test_cloudops_log_output_unrecognized(monkeypatch, caplog):
    cloudops = _reload_cloudops(monkeypatch, "weird-output")

    assert cloudops is not None
    assert "Did not recognize weird-output" in caplog.text
