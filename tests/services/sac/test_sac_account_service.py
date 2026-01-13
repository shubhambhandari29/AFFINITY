from datetime import date, datetime

import pytest
from fastapi import HTTPException

from services.sac import sac_account_service as svc


@pytest.mark.anyio
async def test_get_sac_account_without_branch(monkeypatch):
    calls = {}

    def fake_sanitize(params, allowed):
        assert allowed == svc.ALLOWED_FILTERS
        return {"CustomerNum": "1"}

    async def fake_fetch(table, filters):
        calls["filters"] = filters
        assert table == svc.TABLE_NAME
        return [
            {
                "CustomerNum": "1",
                "OnBoardDate": "2024-01-05 00:00:00",
                "DateCreated": "2023-12-31T00:00:00",
                "TermDate": datetime(2025, 6, 1, 0, 0),
            }
        ]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_sac_account({"CustomerNum": "1"})
    assert result == [
        {
            "CustomerNum": "1",
            "OnBoardDate": "05-01-2024",
            "DateCreated": "31-12-2023",
            "TermDate": "01-06-2025",
        }
    ]
    assert calls["filters"] == {"CustomerNum": "1"}


@pytest.mark.anyio
async def test_get_sac_account_with_branch(monkeypatch):
    captured = {}

    def fake_sanitize(params, allowed):
        return {"BranchName": "NY & LA", "Stage": "Active"}

    async def fake_raw(query, params):
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "CustomerNum": "1",
                "BranchName": "NY",
                "OnBoardDate": "2024-02-10T00:00:00Z",
                "RenewLetterDt": "2024-02-01",
            }
        ]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "run_raw_query_async", fake_raw)

    result = await svc.get_sac_account({})
    assert result == [
        {
            "CustomerNum": "1",
            "BranchName": "NY",
            "OnBoardDate": "10-02-2024",
            "RenewLetterDt": "01-02-2024",
        }
    ]
    assert "BranchName LIKE ?" in captured["query"]
    assert captured["params"] == ["Active", "NY%", "LA%"]


@pytest.mark.anyio
async def test_get_sac_account_handles_null_onboard_date(monkeypatch):
    def fake_sanitize(params, allowed):
        return {}

    async def fake_fetch(table, filters):
        return [{"CustomerNum": "1", "OnBoardDate": None}]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_sac_account({})
    assert result == [{"CustomerNum": "1", "OnBoardDate": None}]


@pytest.mark.anyio
async def test_get_sac_account_formats_additional_dates(monkeypatch):
    def fake_sanitize(params, allowed):
        return {}

    async def fake_fetch(table, filters):
        return [
            {
                "CustomerNum": "1",
                "DateNotif": "2024-07-04 09:00:00",
                "NCMStartDt": "2024-03-15",
                "NCMEndDt": "2024-03-20",
            }
        ]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_sac_account({})
    assert result == [
        {
            "CustomerNum": "1",
            "DateNotif": "04-07-2024",
            "NCMStartDt": "15-03-2024",
            "NCMEndDt": "20-03-2024",
        }
    ]


@pytest.mark.anyio
async def test_get_sac_account_invalid_filter(monkeypatch):
    def fake_sanitize(params, allowed):
        raise ValueError("bad field")

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)

    with pytest.raises(HTTPException) as exc:
        await svc.get_sac_account({})

    assert exc.value.status_code == 400


@pytest.mark.anyio
async def test_upsert_sac_account(monkeypatch):
    async def fake_merge(**kwargs):
        return {"message": "ok"}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    result = await svc.upsert_sac_account(
        {
            "CustomerNum": "1",
            "CustomerName": "Acme",
            "OnBoardDate": "2024-01-05",
            "BranchName": "Midwest",
        }
    )
    assert result == {"message": "ok"}


@pytest.mark.anyio
async def test_upsert_sac_account_normalizes_dates(monkeypatch):
    captured = {}

    async def fake_merge(table, data_list, key_columns, **kwargs):
        captured["data_list"] = data_list
        captured["table"] = table
        return {"message": "ok"}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "CustomerName": "Acme",
        "OnBoardDate": "05-01-2024",
        "BranchName": "Midwest",
        "DateCreated": "2024-01-01",
        "NCMEndDt": "2024/03/15",
    }
    await svc.upsert_sac_account(payload)

    row = captured["data_list"][0]
    assert row["OnBoardDate"] == date(2024, 1, 5)
    assert row["DateCreated"] == date(2024, 1, 1)
    assert row["NCMEndDt"] == date(2024, 3, 15)


@pytest.mark.anyio
async def test_upsert_sac_account_requires_customer_name(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("merge should not be called")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "Stage": "Underwriter",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
    }

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_sac_account(payload)

    assert exc.value.status_code == 422
    assert exc.value.detail["errors"][0]["field"] == "CustomerName"


@pytest.mark.anyio
async def test_upsert_sac_account_enforces_inactive_dependencies(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("merge should not be called")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "CustomerName": "Acme",
        "Stage": "Admin",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
        "AcctStatus": "Inactive",
    }

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_sac_account(payload)

    assert exc.value.status_code == 422
    error_fields = {err["field"] for err in exc.value.detail["errors"]}
    assert {"DateNotif", "TermDate", "TermCode"}.issubset(error_fields)


@pytest.mark.anyio
async def test_upsert_sac_account_inactive_dependencies_skipped_for_underwriter(monkeypatch):
    async def fake_merge(**kwargs):
        return {"message": "ok"}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "CustomerName": "Acme",
        "Stage": "Underwriter",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
        "AcctStatus": "Inactive",
    }

    result = await svc.upsert_sac_account(payload)
    assert result == {"message": "ok"}


@pytest.mark.anyio
async def test_upsert_sac_account_service_level_conflict(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("merge should not be called")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "CustomerName": "Acme",
        "Stage": "Admin",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
        "TotalPrem": 100000,
        "ServLevel": "Comprehensive",
    }

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_sac_account(payload)

    assert exc.value.status_code == 422
    assert exc.value.detail["errors"][0]["code"] == "SERVICE_LEVEL_CONFLICT"


@pytest.mark.anyio
async def test_upsert_sac_account_service_level_not_checked_for_underwriter(monkeypatch):
    async def fake_merge(**kwargs):
        return {"message": "ok"}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "CustomerName": "Acme",
        "Stage": "Underwriter",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
        "TotalPrem": 100000,
        "ServLevel": "Comprehensive",
    }

    result = await svc.upsert_sac_account(payload)
    assert result == {"message": "ok"}


@pytest.mark.anyio
async def test_upsert_sac_account_service_level_override(monkeypatch):
    async def fake_merge(**kwargs):
        return {"message": "ok"}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerNum": "1",
        "CustomerName": "Acme",
        "Stage": "Admin",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
        "TotalPrem": 100000,
        "ServLevel": "Comprehensive",
        "ServiceLevelOverride": True,
    }

    result = await svc.upsert_sac_account(payload)
    assert result == {"message": "ok"}


@pytest.mark.anyio
async def test_upsert_sac_account_requires_customer_number(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("merge should not be called")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {
        "CustomerName": "Acme",
        "OnBoardDate": "2024-01-05",
        "BranchName": "Midwest",
    }

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_sac_account(payload)

    assert exc.value.status_code == 422
    assert exc.value.detail["errors"][0]["field"] == "CustomerNum"
