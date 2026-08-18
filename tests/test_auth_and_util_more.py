import os
from types import SimpleNamespace

import pytest

from cfa.cloudops import auth, util
from cfa.cloudops import defaults as d


def test_lookup_service_principal_success(monkeypatch):
    payload = '[{"appId": "abc"}]'

    monkeypatch.setattr("cfa.cloudops.util.sp.check_output", lambda *a, **k: payload)

    result = util.lookup_service_principal("my-sp")
    assert result == [{"appId": "abc"}]


def test_lookup_service_principal_failure(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("az failed")

    monkeypatch.setattr("cfa.cloudops.util.sp.check_output", boom)

    with pytest.raises(RuntimeError):
        util.lookup_service_principal("my-sp")


def test_lookup_available_vm_skus_for_batch_to_dict(monkeypatch):
    sku1 = SimpleNamespace(
        name="Standard_D2s_v3",
        family_name="fam",
        batch_support_end_of_life=None,
        additional_properties={"tier": "standard"},
        capabilities=[SimpleNamespace(name="vCPUs", value="2")],
    )
    sku2 = SimpleNamespace(
        name="Standard_D4s_v3",
        family_name="fam",
        batch_support_end_of_life=None,
        additional_properties={},
        capabilities=[],
    )

    client = SimpleNamespace(
        location=SimpleNamespace(
            list_supported_virtual_machine_skus=lambda **kwargs: [sku1, sku2]
        )
    )

    monkeypatch.setattr("cfa.cloudops.util.get_config_val", lambda *a, **k: "eastus")

    result = util.lookup_available_vm_skus_for_batch(client=client, to_dict=True)

    assert len(result) == 2
    assert result[0]["name"] == "Standard_D2s_v3"
    assert result[0]["vCPUs"] == "2"


def test_lookup_available_vm_skus_for_batch_builds_client(monkeypatch):
    sku = SimpleNamespace(
        name="Standard_D2s_v3",
        family_name="fam",
        batch_support_end_of_life=None,
        additional_properties={},
        capabilities=[],
    )

    client = SimpleNamespace(
        location=SimpleNamespace(
            list_supported_virtual_machine_skus=lambda **kwargs: [sku]
        )
    )

    monkeypatch.setattr(
        "cfa.cloudops.client.get_batch_management_client",
        lambda **kwargs: client,
    )
    monkeypatch.setattr("cfa.cloudops.util.get_config_val", lambda *a, **k: "eastus")

    result = util.lookup_available_vm_skus_for_batch(client=None, to_dict=False)
    assert result == [sku]


def test_credential_handler_require_attr():
    ch = auth.CredentialHandler()

    with pytest.raises(AttributeError) as exc:
        ch.require_attr(["azure_tenant_id", "azure_client_id"], goal="auth")

    assert "azure_tenant_id" in str(exc.value)
    assert "azure_client_id" in str(exc.value)


def test_credential_handler_endpoint_properties():
    ch = auth.CredentialHandler(
        azure_batch_account="acct",
        azure_batch_location="eastus",
        azure_batch_endpoint_subdomain="batch.azure.com/",
        azure_blob_storage_account="blobacct",
        azure_blob_storage_endpoint_subdomain="blob.core.windows.net/",
        azure_container_registry_account="reg",
        azure_container_registry_domain="azurecr.io",
    )

    assert ch.azure_batch_endpoint == "https://acct.eastus.batch.azure.com/"
    assert ch.azure_blob_storage_endpoint == "https://blobacct.blob.core.windows.net/"
    assert ch.azure_container_registry_endpoint == "https://reg.azurecr.io"


def test_credential_handler_default_credential(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: sentinel
    )

    ch = auth.CredentialHandler()
    assert ch.default_credential is sentinel


def test_removed_service_principal_secret_property():
    assert not hasattr(auth.CredentialHandler, "service_principal_secret")


def test_removed_batch_service_principal_credentials_property():
    assert not hasattr(auth.CredentialHandler, "batch_service_principal_credentials")


def test_removed_client_secret_credential_variants():
    assert not hasattr(auth.CredentialHandler, "client_secret_sp_credential")
    assert not hasattr(auth.CredentialHandler, "client_secret_credential")


def test_compute_node_identity_reference():
    ch = auth.CredentialHandler(
        azure_user_assigned_identity="/subscriptions/sub/resourceGroups/rg/providers/id"
    )
    ref = ch.compute_node_identity_reference
    assert ref.resource_id.endswith("/providers/id")


def test_azure_container_registry_valid_and_invalid(monkeypatch):
    ch = auth.CredentialHandler(
        azure_container_registry_account="reg",
        azure_container_registry_domain="azurecr.io",
        azure_user_assigned_identity="/subscriptions/sub/resourceGroups/rg/providers/id",
    )

    monkeypatch.setattr(
        "cfa.cloudops.auth.is_valid_acr_endpoint", lambda endpoint: (True, None)
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.batch_mgmt_models.ContainerRegistry",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )

    reg = ch.azure_container_registry
    assert reg.user_name == "reg"

    ch2 = auth.CredentialHandler(
        azure_container_registry_account="reg",
        azure_container_registry_domain="azurecr.io",
        azure_user_assigned_identity="/subscriptions/sub/resourceGroups/rg/providers/id",
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.is_valid_acr_endpoint",
        lambda endpoint: (False, "bad endpoint"),
    )
    with pytest.raises(ValueError):
        _ = ch2.azure_container_registry


def test_default_credential_wrapper(monkeypatch):
    class FakeCredential:
        def get_token(self, *scopes, **kwargs):
            return "tok"

    class FakePolicy:
        def __init__(self, credential, resource_id, **kwargs):
            self.credential = credential
            self.resource_id = resource_id

        def on_request(self, request):
            request.http_request.headers["Authorization"] = "Bearer abc123"

    monkeypatch.setattr("cfa.cloudops.auth.BearerTokenCredentialPolicy", FakePolicy)

    dc = auth.DefaultCredential(credential=FakeCredential())
    assert dc.get_token("scope") == "tok"
    dc.set_token()
    assert dc.token["access_token"] == "abc123"


def test_removed_get_sp_secret_function():
    assert not hasattr(auth, "get_sp_secret")


def test_removed_get_client_secret_sp_credential_function():
    assert not hasattr(auth, "get_client_secret_sp_credential")


def test_removed_get_service_principal_credentials_function():
    assert not hasattr(auth, "get_service_principal_credentials")


def test_get_compute_node_identity_reference_helper(monkeypatch):
    identity = SimpleNamespace(resource_id="rid")

    monkeypatch.setattr(
        "cfa.cloudops.auth.DefaultCredentialHandler",
        lambda: SimpleNamespace(compute_node_identity_reference=identity),
    )

    result = auth.get_compute_node_identity_reference()
    assert result is identity


def test_get_secret_client(monkeypatch):
    captured = {}

    def fake_secret_client(vault_url, credential):
        captured["vault_url"] = vault_url
        captured["credential"] = credential
        return SimpleNamespace(vault_url=vault_url)

    monkeypatch.setattr("cfa.cloudops.auth.SecretClient", fake_secret_client)

    client = auth.get_secret_client("mykv", credential="cred")
    assert client.vault_url == "https://mykv.vault.azure.net"
    assert captured["credential"] == "cred"


def test_load_keyvault_vars_force_and_skip(monkeypatch):
    class FakeSecretClient:
        def __init__(self):
            self.calls = []

        def get_secret(self, key):
            self.calls.append(key)
            return SimpleNamespace(value=f"value-{key}")

    sc = FakeSecretClient()

    monkeypatch.setenv("AZURE_BATCH_ACCOUNT", "existing")

    auth.load_keyvault_vars(sc, force_keyvault=False)
    assert os.environ["AZURE_BATCH_ACCOUNT"] == "existing"

    auth.load_keyvault_vars(sc, force_keyvault=True)
    assert "AZURE-BATCH-ACCOUNT" in [c.upper() for c in sc.calls]


def test_get_keyvault_vars_none_and_success(monkeypatch):
    assert auth.get_keyvault_vars(None, credential="cred") is None

    seen = {}

    monkeypatch.setattr(
        "cfa.cloudops.auth.get_secret_client",
        lambda keyvault, credential: "secret-client",
    )

    def fake_load(secret_client, force_keyvault=False):
        seen["client"] = secret_client
        seen["force"] = force_keyvault

    monkeypatch.setattr("cfa.cloudops.auth.load_keyvault_vars", fake_load)

    auth.get_keyvault_vars("mykv", credential="cred", force_keyvault=True)
    assert seen["client"] == "secret-client"
    assert seen["force"] is True


def test_load_env_vars(monkeypatch):
    class FakeSub:
        subscription_id = "sub-1"
        tenant_id = "tenant-1"
        display_name = "rg-name"

    monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
    monkeypatch.delenv("AZURE_TENANT_ID", raising=False)
    monkeypatch.delenv("AZURE_RESOURCE_GROUP_NAME", raising=False)

    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dac"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )

    called = {"set_env": 0, "kv": 0}
    monkeypatch.setattr(
        "cfa.cloudops.auth.d.set_env_vars",
        lambda: called.__setitem__("set_env", called["set_env"] + 1),
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.get_keyvault_vars",
        lambda **kwargs: called.__setitem__("kv", called["kv"] + 1),
    )

    auth.load_env_vars(
        dotenv_path=".env.test", keyvault_name="mykv", force_keyvault=True
    )

    assert os.environ["AZURE_SUBSCRIPTION_ID"] == "sub-1"
    assert os.environ["AZURE_TENANT_ID"] == "tenant-1"
    assert os.environ["AZURE_RESOURCE_GROUP_NAME"] == "rg-name"
    assert called["set_env"] == 1
    assert called["kv"] == 1


def test_load_env_vars_subscription_mismatch_raises(monkeypatch):
    class FakeSub:
        subscription_id = "sub-1"
        tenant_id = "tenant-1"
        display_name = "rg-name"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "does-not-exist")
    monkeypatch.delenv("AZURE_TENANT_ID", raising=False)
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dac"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)

    with pytest.raises(ValueError, match="Subscription matching AZURE_SUBSCRIPTION_ID"):
        auth.load_env_vars(dotenv_path=".env.test")


def test_env_credential_handler_alias_exists():
    assert hasattr(auth, "EnvCredentialHandler")
    assert issubclass(auth.EnvCredentialHandler, auth.DefaultCredentialHandler)


def test_sp_credential_handler_alias_exists():
    assert hasattr(auth, "SPCredentialHandler")
    assert issubclass(auth.SPCredentialHandler, auth.DefaultCredentialHandler)


def test_default_credential_handler_success(monkeypatch):
    class FakeSub:
        subscription_id = "sub-1"
        display_name = "rg-name"
        tenant_id = "tenant-1"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-1")
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dcred"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.get_config_val",
        lambda key, config_dict=None, try_env=True: (
            config_dict.get(key)
            if config_dict and key in config_dict
            else os.getenv(key.upper())
        ),
    )

    handler = auth.DefaultCredentialHandler(dotenv_path=".env.test")
    assert handler.azure_subscription_id == "sub-1"


def test_default_credential_handler_azure_kwargs_override_env_in_memory(monkeypatch):
    class FakeSub:
        subscription_id = "sub-override"
        display_name = "rg-name"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-env")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-env")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-env")
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dcred"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.get_config_val",
        lambda key, config_dict=None, try_env=True: (
            config_dict.get(key)
            if config_dict and key in config_dict
            else os.getenv(key.upper())
        ),
    )

    handler = auth.DefaultCredentialHandler(
        dotenv_path=".env.test",
        azure_subscription_id="sub-override",
        azure_tenant_id="tenant-override",
        azure_client_id="client-override",
    )

    assert os.environ["AZURE_SUBSCRIPTION_ID"] == "sub-env"
    assert os.environ["AZURE_TENANT_ID"] == "tenant-env"
    assert os.environ["AZURE_CLIENT_ID"] == "client-env"
    assert handler.azure_subscription_id == "sub-override"
    assert handler.azure_tenant_id == "tenant-override"
    assert handler.azure_client_id == "client-override"


def test_default_credential_handler_override_env_applies_overrides(monkeypatch):
    class FakeSub:
        subscription_id = "sub-override"
        display_name = "rg-name"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-env")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-env")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-env")
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dcred"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)

    auth.DefaultCredentialHandler(
        dotenv_path=".env.test",
        override_env=True,
        azure_subscription_id="sub-override",
        azure_tenant_id="tenant-override",
        azure_client_id="client-override",
    )

    assert os.environ["AZURE_SUBSCRIPTION_ID"] == "sub-override"
    assert os.environ["AZURE_TENANT_ID"] == "tenant-override"
    assert os.environ["AZURE_CLIENT_ID"] == "client-override"


def test_default_credential_handler_uses_azure_kwargs_for_credential_bootstrap(
    monkeypatch,
):
    class FakeSub:
        subscription_id = "sub-override"
        display_name = "rg-name"
        tenant_id = "tenant-sub"

    captured = {}

    def fake_build_default_credential(**kwargs):
        captured["tenant"] = os.getenv("AZURE_TENANT_ID")
        captured["client_id"] = os.getenv("AZURE_CLIENT_ID")
        captured["client_secret"] = os.getenv("AZURE_CLIENT_SECRET")
        return "chain"

    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-env")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-env")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret-env")
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", fake_build_default_credential
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )

    handler = auth.DefaultCredentialHandler(
        dotenv_path=".env.test",
        azure_subscription_id="sub-override",
        azure_tenant_id="tenant-override",
        azure_client_id="client-override",
        azure_client_secret="secret-override",  # pragma: allowlist secret
    )

    assert captured["tenant"] == "tenant-override"
    assert captured["client_id"] == "client-override"
    assert captured["client_secret"] == "secret-override"  # pragma: allowlist secret
    assert os.environ["AZURE_TENANT_ID"] == "tenant-env"
    assert os.environ["AZURE_CLIENT_ID"] == "client-env"
    assert os.environ["AZURE_CLIENT_SECRET"] == "secret-env"  # pragma: allowlist secret
    assert handler.azure_tenant_id == "tenant-override"
    assert handler.azure_client_id == "client-override"
    assert handler.azure_client_secret == "secret-override"  # pragma: allowlist secret


def test_default_credential_handler_override_env_persists_client_secret(monkeypatch):
    class FakeSub:
        subscription_id = "sub-override"
        display_name = "rg-name"
        tenant_id = "tenant-sub"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-env")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "chain"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)

    auth.DefaultCredentialHandler(
        dotenv_path=".env.test",
        override_env=True,
        azure_subscription_id="sub-override",
        azure_client_secret="secret-override",  # pragma: allowlist secret
    )

    assert (
        os.environ["AZURE_CLIENT_SECRET"]
        == "secret-override"  # pragma: allowlist secret
    )


def test_default_credential_handler_missing_sub(monkeypatch):
    monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dcred"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(subscriptions=SimpleNamespace(list=lambda: [])),
    )

    with pytest.raises(ValueError):
        auth.DefaultCredentialHandler(dotenv_path=".env.test")


def test_default_credential_handler_preserves_default_subdomain(monkeypatch):
    class FakeSub:
        subscription_id = "sub-1"
        display_name = "rg-name"
        tenant_id = "tenant-1"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-1")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-1")
    monkeypatch.delenv("AZURE_BATCH_ENDPOINT_SUBDOMAIN", raising=False)
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", lambda **kwargs: "dcred"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )

    handler = auth.DefaultCredentialHandler(dotenv_path=".env.test")

    assert (
        handler.azure_batch_endpoint_subdomain
        == d.default_azure_batch_endpoint_subdomain
    )


def test_default_credential_handler_passes_direct_credential_chain_kwargs(monkeypatch):
    class FakeSub:
        subscription_id = "sub-1"
        display_name = "rg-name"
        tenant_id = "tenant-1"

    captured = {}

    def fake_build_default_credential(**kwargs):
        captured.setdefault("calls", []).append(kwargs)
        return "dcred"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-1")
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", fake_build_default_credential
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(
            subscriptions=SimpleNamespace(list=lambda: [FakeSub()])
        ),
    )

    auth.DefaultCredentialHandler(
        dotenv_path=".env.test",
        exclude_environment_credential=True,
        exclude_azure_cli_credential=True,
    )

    assert captured["calls"]
    assert captured["calls"][0]["exclude_environment_credential"] is True
    assert captured["calls"][0]["exclude_azure_cli_credential"] is True


def test_credential_handler_default_credential_uses_custom_kwargs(monkeypatch):
    captured = {}

    def fake_build_default_credential(**kwargs):
        captured.update(kwargs)
        return "dcred"

    monkeypatch.setattr(
        "cfa.cloudops.auth._build_default_credential", fake_build_default_credential
    )

    ch = auth.CredentialHandler()
    ch._credential_chain_kwargs = {
        "exclude_environment_credential": True,
        "exclude_azure_cli_credential": True,
    }

    assert ch.default_credential == "dcred"
    assert captured["exclude_environment_credential"] is True
    assert captured["exclude_azure_cli_credential"] is True
