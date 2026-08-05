import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from azure.mgmt.batch import models

from cfa.cloudops import config, defaults, endpoints, util


def test_try_get_val_from_dict_success_and_missing():
    value, message = config.try_get_val_from_dict("my_key", {"my_key": "value"})
    assert value == "value"
    assert message is None

    value, message = config.try_get_val_from_dict("missing", {"my_key": "value"})
    assert value is None
    assert "missing" in message


def test_try_get_val_from_env_success_and_missing(monkeypatch):
    monkeypatch.setenv("MY_ENV", "abc")
    value, message = config.try_get_val_from_env("MY_ENV")
    assert value == "abc"
    assert message is None

    monkeypatch.delenv("MISSING_ENV", raising=False)
    value, message = config.try_get_val_from_env("MISSING_ENV")
    assert value is None
    assert "MISSING_ENV" in message


def test_get_config_val_prefers_dict_over_env(monkeypatch):
    monkeypatch.setenv("API_KEY", "env-value")
    result = config.get_config_val("api_key", config_dict={"api_key": "dict-value"})
    assert result == "dict-value"


def test_get_config_val_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("API_KEY", "env-value")
    result = config.get_config_val("api_key", config_dict={})
    assert result == "env-value"


def test_get_config_val_missing_returns_none(monkeypatch):
    monkeypatch.delenv("NOT_FOUND", raising=False)
    result = config.get_config_val(
        "not_found",
        config_dict={},
        try_env=True,
        env_variable_name="NOT_FOUND",
    )
    assert result is None


def test_construct_https_url():
    assert (
        endpoints._construct_https_url("example.com", "/v1") == "https://example.com/v1"
    )


def test_batch_blob_and_registry_endpoint_constructors():
    assert (
        endpoints.construct_batch_endpoint("acct", "eastus")
        == "https://acct.eastus.batch.azure.com/"
    )
    assert (
        endpoints.construct_batch_endpoint("acct", "westus", "custom.domain/")
        == "https://acct.westus.custom.domain/"
    )

    assert (
        endpoints.construct_azure_container_registry_endpoint("myregistry")
        == "https://myregistry.azurecr.io"
    )
    assert (
        endpoints.construct_blob_account_endpoint("storage")
        == "https://storage.blob.core.windows.net/"
    )
    assert (
        endpoints.construct_blob_container_endpoint("my folder", "storage")
        == "https://storage.blob.core.windows.net/my%20folder"
    )


@pytest.mark.parametrize(
    "endpoint,expected_valid,expected_substring",
    [
        ("https://myregistry.azurecr.io", True, None),
        ("https://myregistry.azurecr.io/", False, "trailing slash"),
        ("https://myregistry.example.com", False, "azurecr.io"),
        ("https://azurecr.io", False, "subdomain"),
    ],
)
def test_is_valid_acr_endpoint(endpoint, expected_valid, expected_substring):
    is_valid, error_message = endpoints.is_valid_acr_endpoint(endpoint)
    assert is_valid is expected_valid
    if expected_substring is None:
        assert error_message is None
    else:
        assert expected_substring in error_message


def test_remaining_task_autoscale_formula_contains_parameters():
    formula = defaults.remaining_task_autoscale_formula(
        task_sample_interval_minutes=30,
        max_number_vms=22,
    )
    assert "TimeInterval_Minute * 30" in formula
    assert "cappedPoolSize = 22" in formula


def test_set_env_vars_sets_defaults_and_derived_values(monkeypatch):
    monkeypatch.setenv("AZURE_BATCH_ACCOUNT", "acct")
    monkeypatch.setenv("AZURE_BATCH_LOCATION", "eastus")
    monkeypatch.setenv("AZURE_KEYVAULT_NAME", "myvault")
    monkeypatch.setenv("AZURE_BLOB_STORAGE_ACCOUNT", "blobacct")
    monkeypatch.setenv("AZURE_CONTAINER_REGISTRY_ACCOUNT", "regacct")

    defaults.set_env_vars()

    assert "https://acct.eastus.batch.azure.com/" == os.environ["AZURE_BATCH_ENDPOINT"]
    assert "https://myvault.vault.azure.net" == os.environ["AZURE_KEYVAULT_ENDPOINT"]
    assert (
        "https://blobacct.blob.core.windows.net/"
        == os.environ["AZURE_BLOB_STORAGE_ENDPOINT"]
    )
    assert "regacct.azurecr.io/" == os.environ["ACR_TAG_PREFIX"]


def test_get_default_pool_identity_and_pool_config():
    identity_path = "/subscriptions/sub/resourceGroups/rg/providers/id"
    identity = defaults.get_default_pool_identity(identity_path)

    assert identity.type == models.PoolIdentityType.user_assigned
    assert identity_path in identity.user_assigned_identities

    pool = defaults.get_default_pool_config(
        pool_name="pool-a",
        subnet_id="/subscriptions/sub/resourceGroups/rg/providers/net/subnets/default",
        user_assigned_identity=identity_path,
        vm_size="standard_d2s_v3",
    )

    assert pool.display_name == "pool-a"
    assert pool.vm_size == "standard_d2s_v3"
    assert pool.network_configuration.subnet_id.endswith("/subnets/default")


def test_assign_container_config_updates_pool_in_place():
    identity_path = "/subscriptions/sub/resourceGroups/rg/providers/id"
    pool = defaults.get_default_pool_config(
        pool_name="pool-b",
        subnet_id="/subscriptions/sub/resourceGroups/rg/providers/net/subnets/default",
        user_assigned_identity=identity_path,
    )

    container_config = models.ContainerConfiguration(type="dockerCompatible")
    updated = defaults.assign_container_config(pool, container_config)

    assert updated is pool
    vm_config = updated.deployment_configuration.virtual_machine_configuration
    assert vm_config.container_configuration is container_config


def test_ensure_listlike_behaviors():
    data = ["a", "b"]
    assert util.ensure_listlike(data) is data
    assert util.ensure_listlike("a") == ["a"]
    assert util.ensure_listlike(5) == [5]


def test_sku_to_dict_handles_capabilities_and_properties():
    sku = SimpleNamespace(
        name="Standard_D2s_v3",
        family_name="standardDSv3Family",
        batch_support_end_of_life="2027-01-01",
        additional_properties={"tier": "standard"},
        capabilities=[SimpleNamespace(name="vCPUs", value="2")],
    )

    as_dict = util.sku_to_dict(sku)

    assert as_dict["name"] == "Standard_D2s_v3"
    assert as_dict["family_name"] == "standardDSv3Family"
    assert as_dict["vCPUs"] == "2"
    assert as_dict["additional_properties"]["tier"] == "standard"


def test_get_subscriptions_success_and_failure(monkeypatch):
    class FakeSub:
        def __init__(self, display_name):
            self.display_name = display_name

    fake_client = MagicMock()
    fake_client.subscriptions.list.return_value = [FakeSub("sub-a"), FakeSub("sub-b")]

    monkeypatch.setattr("cfa.cloudops.util.DefaultAzureCredential", lambda: object())
    monkeypatch.setattr(
        "cfa.cloudops.util.SubscriptionClient", lambda cred: fake_client
    )

    assert util.get_subscriptions() == ["sub-a", "sub-b"]

    monkeypatch.setattr(
        "cfa.cloudops.util.DefaultAzureCredential",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert util.get_subscriptions() == []


def test_check_ext_env(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.util.get_subscriptions", lambda: ["foo"])
    assert util.check_ext_env() is False

    monkeypatch.setattr(
        "cfa.cloudops.util.get_subscriptions", lambda: ["EXT-EDAV-CFA sandbox"]
    )
    assert util.check_ext_env() is True


def test_get_user_fallbacks(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.util.getpass.getuser", lambda: "alice")
    assert util.get_user() == "alice"

    monkeypatch.setattr(
        "cfa.cloudops.util.getpass.getuser",
        lambda: (_ for _ in ()).throw(RuntimeError("no getuser")),
    )
    monkeypatch.setattr(
        "cfa.cloudops.util.sp.run",
        lambda *args, **kwargs: SimpleNamespace(stdout="bob\n"),
    )
    assert util.get_user() == "bob"

    monkeypatch.setattr(
        "cfa.cloudops.util.sp.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no whoami")),
    )
    assert util.get_user() == "unknown_user"


def test_get_date_time_success_and_failure(monkeypatch):
    timestamp = util.get_date_time()
    assert "T" in timestamp

    class BrokenDateTime:
        @staticmethod
        def now(*args, **kwargs):
            raise RuntimeError("bad clock")

    monkeypatch.setattr("cfa.cloudops.util.datetime.datetime", BrokenDateTime)
    assert util.get_date_time() == "unknown_datetime"
