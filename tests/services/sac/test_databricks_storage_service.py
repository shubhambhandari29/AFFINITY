from io import BytesIO

import pytest

from services.loss_run import databricks_storage_service


def _configure(monkeypatch, *, environment: str) -> None:
    monkeypatch.setattr(databricks_storage_service.settings, "ENVIRONMENT", environment)
    monkeypatch.setattr(
        databricks_storage_service.settings,
        "DATABRICKS_HOST",
        "https://workspace.azuredatabricks.net/",
    )
    monkeypatch.setattr(
        databricks_storage_service.settings,
        "LOSS_RUN_DATABRICKS_CATALOG",
        "claims_data_pre_prod",
    )
    monkeypatch.setattr(
        databricks_storage_service.settings,
        "DATABRICKS_PROFILE",
        "claims-preprod",
    )


def test_local_storage_uses_oauth_profile_for_download_and_upload(monkeypatch):
    _configure(monkeypatch, environment="local")
    captured = {}

    class Download:
        contents = BytesIO(b"template")

    class Files:
        @staticmethod
        def download(path):
            captured["download_path"] = path
            return Download()

        @staticmethod
        def upload(path, contents, *, overwrite):
            captured["upload_path"] = path
            captured["upload_bytes"] = contents.read()
            captured["overwrite"] = overwrite

    class WorkspaceClient:
        def __init__(self, *, host, profile):
            captured["host"] = host
            captured["profile"] = profile
            self.files = Files()

    monkeypatch.setattr("databricks.sdk.WorkspaceClient", WorkspaceClient)

    storage = databricks_storage_service.DatabricksLossRunStorage()
    assert storage.download_template() == b"template"
    output_path = storage.upload_report("Customer_2026_08_06.xlsx", b"report")

    assert captured["host"] == "https://workspace.azuredatabricks.net"
    assert captured["profile"] == "claims-preprod"
    assert captured["download_path"] == (
        "/Volumes/claims_data_pre_prod/gold/statics/SACLossRunTemplate.xlsx"
    )
    assert output_path == captured["upload_path"]
    assert output_path.endswith("/Customer_2026_08_06.xlsx")
    assert captured["upload_bytes"] == b"report"
    assert captured["overwrite"] is True


def test_azure_storage_uses_managed_identity_and_files_api(monkeypatch):
    _configure(monkeypatch, environment="preprod")
    requests_made = []

    class Credential:
        @staticmethod
        def get_token(scope):
            assert scope == databricks_storage_service.DATABRICKS_TOKEN_SCOPE
            return type("AccessToken", (), {"token": "managed-identity-token"})()

    class Response:
        def __init__(self, content=b"", status_code=200):
            self.content = content
            self.status_code = status_code
            self.ok = status_code < 400
            self.text = ""

    def fake_get(url, *, headers, timeout):
        requests_made.append(("GET", url))
        assert headers == {"Authorization": "Bearer managed-identity-token"}
        assert timeout == 60
        return Response(b"template")

    def fake_put(url, *, headers, params, data, timeout):
        requests_made.append(("PUT", url))
        assert headers["Authorization"] == "Bearer managed-identity-token"
        assert headers["Content-Type"] == "application/octet-stream"
        assert params == {"overwrite": "true"}
        assert data == b"report"
        assert timeout == 60
        return Response(status_code=204)

    monkeypatch.setattr(
        databricks_storage_service,
        "ManagedIdentityCredential",
        Credential,
    )
    monkeypatch.setattr(databricks_storage_service.requests, "get", fake_get)
    monkeypatch.setattr(databricks_storage_service.requests, "put", fake_put)

    storage = databricks_storage_service.DatabricksLossRunStorage()
    assert storage.download_template() == b"template"
    storage.upload_report("Customer.xlsx", b"report")

    assert [method for method, _ in requests_made] == ["GET", "PUT"]
    assert all("/api/2.0/fs/files/Volumes/" in url for _, url in requests_made)


@pytest.mark.parametrize(
    "missing_setting", ["DATABRICKS_HOST", "LOSS_RUN_DATABRICKS_CATALOG"]
)
def test_storage_requires_databricks_configuration(monkeypatch, missing_setting):
    _configure(monkeypatch, environment="local")
    monkeypatch.setattr(databricks_storage_service.settings, missing_setting, None)

    with pytest.raises(ValueError, match=missing_setting):
        databricks_storage_service.DatabricksLossRunStorage()


def test_databricks_error_includes_response_detail():
    class Response:
        ok = False
        status_code = 403
        text = '{"error_code":"PERMISSION_DENIED"}'

        @staticmethod
        def json():
            return {"message": "Missing WRITE VOLUME"}

    with pytest.raises(RuntimeError) as error:
        databricks_storage_service.DatabricksLossRunStorage._raise_for_databricks_error(
            Response(),
            "report upload",
        )

    assert str(error.value) == (
        "Databricks report upload failed with HTTP 403: Missing WRITE VOLUME"
    )
