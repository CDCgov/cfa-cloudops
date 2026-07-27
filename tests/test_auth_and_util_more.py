import os
from types import SimpleNamespace

import pytest

from cfa.cloudops import auth, util


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


def test_credential_handler_user_credential(monkeypatch):
    sentinel = object()
    monkeypatch.setattr("cfa.cloudops.auth.ManagedIdentityCredential", lambda: sentinel)

    ch = auth.CredentialHandler()
    assert ch.user_credential is sentinel


def test_service_principal_secret_branches(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.auth.get_sp_secret", lambda *a, **k: "kv-secret")

    ch_sp = auth.CredentialHandler(
        azure_keyvault_endpoint="https://kv",
        azure_keyvault_sp_secret_id="sp-id",
        method="sp",
    )
    ch_sp.azure_client_secret = "direct-secret"  # pragma: allowlist secret
    assert ch_sp.service_principal_secret == "direct-secret"  # pragma: allowlist secret

    ch_default = auth.CredentialHandler(
        azure_keyvault_endpoint="https://kv",
        azure_keyvault_sp_secret_id="sp-id",
        method="default",
    )
    ch_default.__dict__["default_credential"] = "default-cred"
    assert (
        ch_default.service_principal_secret == "kv-secret"  # pragma: allowlist secret
    )

    ch_env = auth.CredentialHandler(
        azure_keyvault_endpoint="https://kv",
        azure_keyvault_sp_secret_id="sp-id",
        method="env",
    )
    ch_env.__dict__["user_credential"] = "user-cred"
    assert ch_env.service_principal_secret == "kv-secret"  # pragma: allowlist secret


def test_batch_service_principal_credentials(monkeypatch):
    called = {}

    def fake_spcred(**kwargs):
        called.update(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr("cfa.cloudops.auth.ServicePrincipalCredentials", fake_spcred)

    ch = auth.CredentialHandler(
        azure_tenant_id="tenant",
        azure_client_id="client",
        azure_batch_resource_url="resource",
    )
    ch.__dict__["service_principal_secret"] = "secret"  # pragma: allowlist secret

    cred = ch.batch_service_principal_credentials
    assert cred.client_id == "client"
    assert called["secret"] == "secret"  # pragma: allowlist secret


def test_client_secret_credential_variants(monkeypatch):
    calls = []

    def fake_client_secret_cred(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        "cfa.cloudops.auth.ClientSecretCredential", fake_client_secret_cred
    )

    ch = auth.CredentialHandler(azure_tenant_id="t", azure_client_id="c")
    ch.__dict__["service_principal_secret"] = "s1"  # pragma: allowlist secret
    out1 = ch.client_secret_sp_credential
    assert out1.client_secret == "s1"  # pragma: allowlist secret

    ch2 = auth.CredentialHandler(azure_tenant_id="t", azure_client_id="c")
    ch2.azure_client_secret = "s2"  # pragma: allowlist secret
    out2 = ch2.client_secret_credential
    assert out2.client_secret == "s2"  # pragma: allowlist secret


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


def test_get_sp_secret(monkeypatch):
    monkeypatch.setattr(
        "cfa.cloudops.auth.ManagedIdentityCredential", lambda: "managed"
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.SecretClient",
        lambda vault_url, credential: SimpleNamespace(
            get_secret=lambda sid: SimpleNamespace(value=f"secret-{sid}")
        ),
    )

    result = auth.get_sp_secret("https://kv", "sp-id")
    assert result == "secret-sp-id"


def test_get_client_secret_sp_credential(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.auth.get_sp_secret", lambda *a, **k: "sp-secret")
    monkeypatch.setattr(
        "cfa.cloudops.auth.ClientSecretCredential",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )

    cred = auth.get_client_secret_sp_credential(
        vault_url="https://kv",
        vault_sp_secret_id="sp-id",
        tenant_id="tenant",
        application_id="app",
    )
    assert cred.client_secret == "sp-secret"  # pragma: allowlist secret


def test_get_service_principal_credentials(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.auth.get_sp_secret", lambda *a, **k: "sp-secret")
    monkeypatch.setattr(
        "cfa.cloudops.auth.ServicePrincipalCredentials",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )

    cred = auth.get_service_principal_credentials(
        vault_url="https://kv",
        vault_sp_secret_id="sp-id",
        tenant_id="tenant",
        application_id="app",
    )
    assert cred.secret == "sp-secret"  # pragma: allowlist secret


def test_get_compute_node_identity_reference_helper(monkeypatch):
    identity = SimpleNamespace(resource_id="rid")

    monkeypatch.setattr(
        "cfa.cloudops.auth.EnvCredentialHandler",
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

    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.ManagedIdentityCredential", lambda: "mid")
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


def test_env_credential_handler_init(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.auth.load_env_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth.get_config_val",
        lambda key, config_dict=None, try_env=True: (
            config_dict.get(key)
            if config_dict and key in config_dict
            else os.getenv(key.upper())
        ),
    )

    monkeypatch.delenv("AZURE_BATCH_LOCATION", raising=False)

    handler = auth.EnvCredentialHandler(
        dotenv_path=".env.test", azure_batch_account="acct"
    )
    assert handler.method == "env"
    assert handler.azure_batch_location == auth.d.default_azure_batch_location


def test_sp_credential_handler_init(monkeypatch):
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr(
        "cfa.cloudops.auth.ClientSecretCredential",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        "cfa.cloudops.auth.get_config_val",
        lambda key, config_dict=None, try_env=True: (
            config_dict.get(key)
            if config_dict and key in config_dict
            else os.getenv(key.upper())
        ),
    )

    handler = auth.SPCredentialHandler(
        azure_tenant_id="tenant",
        azure_subscription_id="sub",
        azure_client_id="client",
        azure_client_secret="secret",  # pragma: allowlist secret
        azure_batch_account="acct",
    )
    assert handler.method == "sp"


def test_default_credential_handler_success(monkeypatch):
    class FakeSub:
        subscription_id = "sub-1"
        display_name = "rg-name"

    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub-1")
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.d.set_env_vars", lambda: None)
    monkeypatch.setattr("cfa.cloudops.auth.get_keyvault_vars", lambda **kwargs: None)
    monkeypatch.setattr("cfa.cloudops.auth.DefaultCredential", lambda: "dcred")
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
    assert handler.method == "default"


def test_default_credential_handler_missing_sub(monkeypatch):
    monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
    monkeypatch.setattr("cfa.cloudops.auth.load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr("cfa.cloudops.auth.DefaultCredential", lambda: "dcred")
    monkeypatch.setattr(
        "cfa.cloudops.auth.SubscriptionClient",
        lambda cred: SimpleNamespace(subscriptions=SimpleNamespace(list=lambda: [])),
    )

    with pytest.raises(ValueError):
        auth.DefaultCredentialHandler(dotenv_path=".env.test")
