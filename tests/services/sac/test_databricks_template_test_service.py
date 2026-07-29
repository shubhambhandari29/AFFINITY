import asyncio
from io import BytesIO

import pytest
from fastapi import HTTPException
from openpyxl import Workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

from services.loss_run import databricks_template_test_service


def _template_bytes() -> bytes:
    workbook = Workbook()
    workbook.active.title = "Cover Page"

    claims = workbook.create_sheet("Claims Data")
    claims.append(["Claim Number"])
    claims.append(["123"])
    claims_table = Table(displayName="ClaimsData", ref="A1:A2")
    claims_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2")
    claims.add_table(claims_table)

    record_only = workbook.create_sheet("Record Only")
    record_only.append(["Claim Number"])
    record_only.append(["456"])
    record_only_table = Table(displayName="RecordOnlyData", ref="A1:A2")
    record_only_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2")
    record_only.add_table(record_only_table)

    workbook.create_sheet("Summary By Policy Year")
    workbook.create_sheet("Chart")

    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_downloads_and_validates_template(monkeypatch):
    class Credential:
        @staticmethod
        def get_token(scope):
            assert scope == databricks_template_test_service.DATABRICKS_TOKEN_SCOPE
            return type("AccessToken", (), {"token": "test-token"})()

    class Response:
        content = _template_bytes()

        @staticmethod
        def raise_for_status():
            return None

    def fake_get(url, *, headers, timeout):
        assert url == (
            f"{databricks_template_test_service.DATABRICKS_HOST}/api/2.0/fs/files"
            f"{databricks_template_test_service.DATABRICKS_TEMPLATE_PATH}"
        )
        assert headers == {"Authorization": "Bearer test-token"}
        assert timeout == 60
        return Response()

    monkeypatch.setattr(
        databricks_template_test_service,
        "ManagedIdentityCredential",
        Credential,
    )
    monkeypatch.setattr(databricks_template_test_service.requests, "get", fake_get)

    result = databricks_template_test_service._download_and_validate_template()

    assert result["status"] == "success"
    assert result["authenticationMode"] == "system-assigned-managed-identity"
    assert result["claimsDataTableFound"] is True
    assert result["recordOnlyDataTableFound"] is True
    assert result["fileSizeBytes"] > 0


def test_returns_safe_http_error_when_download_fails(monkeypatch):
    def fail():
        raise RuntimeError("test failure")

    monkeypatch.setattr(
        databricks_template_test_service,
        "_download_and_validate_template",
        fail,
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(databricks_template_test_service.test_databricks_template_access())

    assert error.value.status_code == 500
    assert error.value.detail["error"] == ("Unable to read the loss-run template from Databricks")
