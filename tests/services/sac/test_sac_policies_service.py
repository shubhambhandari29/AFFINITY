from datetime import date
from typing import Any

import pytest
from fastapi import HTTPException

from services.sac import sac_policies_service as svc


@pytest.mark.anyio
async def test_get_sac_policies(monkeypatch):
    def fake_sanitize(params, allowed):
        assert allowed == svc.ALLOWED_FILTERS
        return params

    async def fake_fetch(**kwargs):
        assert kwargs["filters"] == {"CustomerNum": "1"}
        return [
            {"PolicyNum": "P1", "EffectiveDate": "2024-01-01", "PremiumAmt": 12345.0},
            {"PolicyNum": "P2", "EffectiveDate": "2024-01-01"},
        ]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_sac_policies({"CustomerNum": "1"})
    assert result == [
        {"PolicyNum": "P1", "EffectiveDate": "01-01-2024", "PremiumAmt": "12345"},
        {"PolicyNum": "P2", "EffectiveDate": "01-01-2024"},
    ]


@pytest.mark.anyio
async def test_upsert_sac_policies_updates_when_pk_present(monkeypatch):
    calls: dict[str, Any] = {}

    async def fake_merge(**kwargs):
        calls["merge"] = kwargs

    async def fake_insert(**kwargs):
        calls["insert"] = kwargs

    async def fake_fetch(**kwargs):
        return [{"PK_Number": 1, "PolMod": "00"}]

    monkeypatch.setattr(svc, "_lookup_pk_number", lambda record: None)
    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)
    monkeypatch.setattr(svc, "insert_records_async", fake_insert)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    payload = {
        "PK_Number": 1,
        "CustomerNum": "1",
        "AccountName": "Acme",
        "LocCoded": "Y",
        "PolicyNum": "P1",
        "PolMod": "00",
        "EffectiveDate": "01-01-2024",
    }
    result = await svc.upsert_sac_policies(payload)

    assert result == {"message": "Transaction successful", "count": 1, "pk": 1}
    assert "merge" in calls and "insert" not in calls
    merged_payload = calls["merge"]["data_list"][0]
    assert merged_payload["PK_Number"] == 1
    assert merged_payload["EffectiveDate"] == date(2024, 1, 1)
    assert calls["merge"]["key_columns"] == [svc.PRIMARY_KEY]
    assert calls["merge"]["exclude_key_columns_from_insert"] is True


@pytest.mark.anyio
async def test_upsert_sac_policies_inserts_when_pk_missing(monkeypatch):
    calls: dict[str, Any] = {"merge_called": False}

    async def fake_merge(**kwargs):
        calls["merge_called"] = True

    async def fake_insert(**kwargs):
        calls["insert"] = kwargs

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)
    monkeypatch.setattr(svc, "insert_records_async", fake_insert)
    monkeypatch.setattr(svc, "fetch_records_async", lambda **kwargs: [])
    monkeypatch.setattr(svc, "_lookup_pk_number", lambda record: 99)

    payload = {
        "CustomerNum": "1",
        "AccountName": "Acme",
        "LocCoded": "Y",
        "PolicyNum": "P1",
        "PolMod": "00",
        "EffectiveDate": "01-01-2024",
    }
    result = await svc.upsert_sac_policies(payload)

    assert result == {"message": "Transaction successful", "count": 1, "pk": 99}
    assert calls["merge_called"] is False
    inserted_payload = calls["insert"]["records"][0]
    assert "PK_Number" not in inserted_payload
    assert inserted_payload["EffectiveDate"] == date(2024, 1, 1)


@pytest.mark.anyio
async def test_upsert_sac_policies_inserts_new_row_when_mod_changes(monkeypatch):
    calls: dict[str, Any] = {}

    async def fake_merge(**kwargs):
        calls["merge"] = kwargs

    async def fake_insert(**kwargs):
        calls["insert"] = kwargs

    async def fake_fetch(**kwargs):
        assert kwargs["filters"] == {svc.PRIMARY_KEY: 1}
        return [{"PK_Number": 1, "PolMod": "00"}]

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)
    monkeypatch.setattr(svc, "insert_records_async", fake_insert)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)
    monkeypatch.setattr(svc, "_lookup_pk_number", lambda record: 99)

    payload = {
        "PK_Number": 1,
        "CustomerNum": "1",
        "AccountName": "Acme",
        "LocCoded": "Y",
        "PolicyNum": "P1",
        "PolMod": "01",
        "EffectiveDate": "01-01-2024",
    }

    result = await svc.upsert_sac_policies(payload)

    assert result == {"message": "Transaction successful", "count": 1, "pk": 99}
    assert "insert" in calls and "merge" not in calls
    inserted_payload = calls["insert"]["records"][0]
    assert "PK_Number" not in inserted_payload
    assert inserted_payload["PolMod"] == "01"
    assert inserted_payload["EffectiveDate"] == date(2024, 1, 1)


@pytest.mark.anyio
async def test_upsert_sac_policies_validates_required_fields(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("should not merge")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = {"CustomerNum": "1"}

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_sac_policies(payload)

    assert exc.value.status_code == 422
    assert exc.value.detail["errors"][0]["field"] == "AccountName"


@pytest.mark.anyio
async def test_update_field_for_all_policies_runs_query(monkeypatch):
    executed = {}

    class DummyCursor:
        def execute(self, query, params):
            executed["query"] = query.strip()
            executed["params"] = params

    class DummyConn:
        def __init__(self):
            self.cursor_obj = DummyCursor()
            self.committed = False

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.committed = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_db_connection():
        return DummyConn()

    async def fake_run_in_threadpool(fn):
        return fn()

    monkeypatch.setattr(svc, "db_connection", fake_db_connection)
    monkeypatch.setattr(svc, "run_in_threadpool", fake_run_in_threadpool)

    payload = {
        "fieldName": "EffectiveDate",
        "fieldValue": "01-01-2024",
        "updateVia": "CustomerNum",
        "updateViaValue": "1",
    }
    result = await svc.update_field_for_all_policies(payload)
    assert result == {"message": "Update successful"}
    assert "UPDATE" in executed["query"]
    assert executed["params"] == (date(2024, 1, 1), "1")


@pytest.mark.anyio
async def test_update_field_for_all_policies_invalid_field():
    payload = {
        "fieldName": "Bad Field",
        "updateVia": "CustomerNum",
        "fieldValue": "x",
        "updateViaValue": "1",
    }
    with pytest.raises(HTTPException):
        await svc.update_field_for_all_policies(payload)


@pytest.mark.anyio
async def test_get_premium_builds_query(monkeypatch):
    def fake_sanitize(params, allowed):
        assert params["PolicyStatus"] == "Active"
        return params

    async def fake_run_raw(query, params):
        return [{"Premium": 50}]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "run_raw_query_async", fake_run_raw)

    result = await svc.get_premium({"CustomerNum": "1"})
    assert result == 50
