import pytest
from azure.batch import models

from cfa.cloudops.blob import get_node_mount_config


@pytest.fixture
def mock_compute_node():
    return models.ComputeNodeIdentityReference(resource_id="mock-resource-id")


def test_get_node_mount_config_success(mock_compute_node):
    mounts = get_node_mount_config(
        storage_containers=["mock-container-1", "mock-container-2"],
        account_names=["mock-account-1"],
        identity_references=mock_compute_node,
    )
    assert mounts
    assert len(mounts) == 2


def test_get_node_mount_config_success_alternate(mock_compute_node):
    mounts = get_node_mount_config(
        storage_containers=["mock-container-1", "mock-container-2"],
        account_names=["mock-account-1"],
        identity_references=mock_compute_node,
        mount_names=["mount1", "mount2"],
        cache_blobfuse=True,
    )
    assert mounts
    assert len(mounts) == 2


def test_get_node_mount_config_errors(mock_compute_node):
    with pytest.raises(ValueError) as excinfo:
        get_node_mount_config(
            storage_containers=["mock-container-1", "mock-container-2"],
            account_names=["mock-account-1", "mock-account-2", "mock-account-3"],
            identity_references=mock_compute_node,
        )
    assert str(excinfo.value).startswith(
        "Must either provide a single `account_names`value (as a string or a length-1 list) to cover all `storage_containers` values or provide one `account_names` value for each `storage_containers` value"
    )
    with pytest.raises(ValueError) as excinfo:
        get_node_mount_config(
            storage_containers=["mock-container-1", "mock-container-2"],
            account_names=["mock-account-1", "mock-account-2"],
            mount_names=["mount1"],
            identity_references=mock_compute_node,
        )
    assert str(excinfo.value).startswith(
        "Must provide exactly as many `mount_names` as `storage_containers` to mount"
    )
    bad_compute_nodes = [
        models.ComputeNodeIdentityReference(resource_id="mock-resource-id-1"),
        models.ComputeNodeIdentityReference(resource_id="mock-resource-id-2"),
        models.ComputeNodeIdentityReference(resource_id="mock-resource-id-23"),
    ]
    with pytest.raises(ValueError) as excinfo:
        get_node_mount_config(
            storage_containers=["mock-container-1", "mock-container-2"],
            account_names=["mock-account-1", "mock-account-2"],
            identity_references=bad_compute_nodes,
        )
    assert str(excinfo.value).startswith(
        "Must either provide a single `identity_references`value"
    )
