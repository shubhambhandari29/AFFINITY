from __future__ import annotations

import asyncio
from contextlib import contextmanager

import pytest
from fastapi import HTTPException

from services.sac import sac_policies_service


def test_get_sac_policies_formats_and_normalizes_premium(monkeypatch):
    def fake_sanitize_filters(query_params, allowed):
        return {"CustomerNum": "1"}

    async def fake_fetch_records_async(*, table, filters):
        return [{"PremiumAmt": 100, "Other": "x"}]

    def fake_format_records_dates(records):
        return records

    monkeypatch.setattr(sac_policies_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(sac_policies_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(sac_policies_service, "format_records_dates", fake_format_records_dates)
    monkeypatch.setattr(sac_policies_service, "normalize_money_string", lambda value: "100.00")

    result = asyncio.run(sac_policies_service.get_sac_policies({"CustomerNum": "1"}))
    assert result[0]["PremiumAmt"] == "100.00"


def test_get_sac_policies_invalid_filters():
    def fake_sanitize_filters(query_params, allowed):
        raise ValueError("bad filter")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sac_policies_service, "sanitize_filters", fake_sanitize_filters)
    try:
        with pytest.raises(HTTPException) as excinfo:
            asyncio.run(sac_policies_service.get_sac_policies({"Bad": "X"}))
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == {"error": "bad filter"}
    finally:
        monkeypatch.undo()


def test_upsert_sac_policies_inserts_without_pk(monkeypatch):
    async def fake_insert_records_async(*, table, records):
        return {"count": len(records)}

    monkeypatch.setattr(
        sac_policies_service,
        "normalize_payload_dates",
        lambda payload: {"CustomerNum": "1", "PolicyNum": "P1", "PolMod": "1"},
    )
    monkeypatch.setattr(sac_policies_service, "insert_records_async", fake_insert_records_async)
    async def fake_lookup(record):
        return 101

    monkeypatch.setattr(sac_policies_service, "_lookup_pk_number", fake_lookup)

    result = asyncio.run(sac_policies_service.upsert_sac_policies({"CustomerNum": "1"}))
    assert result == {"message": "Transaction successful", "count": 1, "pk": 101}


def test_upsert_sac_policies_inserts_when_mod_changes(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        return [{"PK_Number": 10, "PolMod": "1"}]

    async def fake_insert_records_async(*, table, records):
        return {"count": len(records)}

    monkeypatch.setattr(
        sac_policies_service,
        "normalize_payload_dates",
        lambda payload: {"PK_Number": 10, "CustomerNum": "1", "PolicyNum": "P1", "PolMod": "2"},
    )
    monkeypatch.setattr(sac_policies_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(sac_policies_service, "insert_records_async", fake_insert_records_async)
    async def fake_lookup(record):
        return 202

    monkeypatch.setattr(sac_policies_service, "_lookup_pk_number", fake_lookup)

    result = asyncio.run(sac_policies_service.upsert_sac_policies({"PK_Number": 10}))
    assert result["pk"] == 202


def test_upsert_sac_policies_updates_when_mod_same(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        return [{"PK_Number": 10, "PolMod": "1"}]

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, **kwargs):
        return {"count": len(data_list)}

    monkeypatch.setattr(
        sac_policies_service,
        "normalize_payload_dates",
        lambda payload: {"PK_Number": 10, "CustomerNum": "1", "PolicyNum": "P1", "PolMod": "1"},
    )
    monkeypatch.setattr(sac_policies_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(
        sac_policies_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    result = asyncio.run(sac_policies_service.upsert_sac_policies({"PK_Number": 10}))
    assert result == {"message": "Transaction successful", "count": 1, "pk": 10}


def test_update_field_for_all_policies_validation_errors():
    with pytest.raises(HTTPException):
        asyncio.run(sac_policies_service.update_field_for_all_policies({}))

    with pytest.raises(HTTPException):
        asyncio.run(
            sac_policies_service.update_field_for_all_policies(
                {"fieldName": "A", "updateVia": "B"}
            )
        )

    with pytest.raises(HTTPException):
        asyncio.run(
            sac_policies_service.update_field_for_all_policies(
                {"fieldName": "bad-name", "updateVia": "B", "fieldValue": 1, "updateViaValue": 2}
            )
        )


def test_update_field_for_all_policies_success(monkeypatch):
    captured = {}

    def fake_parse_date_input(value):
        return "parsed"

    class FakeCursor:
        rowcount = 2

        def execute(self, query, params):
            captured["query"] = query
            captured["params"] = params

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    @contextmanager
    def fake_db_connection():
        yield FakeConn()

    async def fake_run_in_threadpool(func):
        return func()

    monkeypatch.setattr(sac_policies_service, "parse_date_input", fake_parse_date_input)
    monkeypatch.setattr(sac_policies_service, "db_connection", fake_db_connection)
    monkeypatch.setattr(sac_policies_service, "run_in_threadpool", fake_run_in_threadpool)

    result = asyncio.run(
        sac_policies_service.update_field_for_all_policies(
            {
                "fieldName": "EffectiveDate",
                "updateVia": "CustomerNum",
                "fieldValue": "2024-01-02",
                "updateViaValue": "123",
            }
        )
    )

    assert captured["params"] == ("parsed", "123")
    assert result == {"message": "Update successful", "count": 2}


def test_get_premium_success(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        return [{"Premium": 250}]

    monkeypatch.setattr(sac_policies_service, "run_raw_query_async", fake_run_raw_query_async)
    monkeypatch.setattr(
        sac_policies_service,
        "sanitize_filters",
        lambda filters_input, allowed: filters_input,
    )

    result = asyncio.run(sac_policies_service.get_premium({"CustomerNum": "1"}))
    assert result == 250


def test_get_premium_invalid_filters(monkeypatch):
    def fake_sanitize_filters(filters_input, allowed):
        raise ValueError("bad filter")

    monkeypatch.setattr(sac_policies_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(sac_policies_service.get_premium({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filter"}
