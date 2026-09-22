"""Tests for CloudOps typing package resources."""

from importlib.resources import files

import cfa.cloudops as cloudops


def test_typing_resources_are_available() -> None:
    """Typing resources should be available from the CloudOps package."""
    package_files = files("cfa.cloudops")

    assert package_files.joinpath("py.typed").is_file()
    assert package_files.joinpath("__init__.pyi").is_file()


def test_public_api_matches_type_stub_exports() -> None:
    """Public CloudOps API exposed by __init__ should remain available."""
    expected_exports = {
        "CloudClient",
        "ContainerAppClient",
        "FunctionAppClient",
        "run_experiment",
        "run_tasks",
        "Task",
    }

    assert set(cloudops.__all__) == expected_exports

    for name in expected_exports:
        assert getattr(cloudops, name) is not None
