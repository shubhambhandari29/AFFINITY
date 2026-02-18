from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import sac_account_service


def test_get_sac_account_without_branch(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        return [{"CustomerNum": "1"}]

    def fake_format_records_dates(records, fields=None):
        return records

    monkeypatch.setattr(sac_account_service, "sanitize_filters", lambda params, allowed: params)
    monkeypatch.setattr(sac_account_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(sac_account_service, "format_records_dates", fake_format_records_dates)

    result = asyncio.run(sac_account_service.get_sac_account({"CustomerNum": "1"}))
    assert result == [{"CustomerNum": "1"}]


def test_get_sac_account_with_branch_filter(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        return [{"CustomerNum": "1"}]

    def fake_format_records_dates(records, fields=None):
        return records

    monkeypatch.setattr(
        sac_account_service,
        "sanitize_filters",
        lambda params, allowed: {"BranchName": "NY, LA", "CustomerNum": "1"},
    )
    monkeypatch.setattr(sac_account_service, "run_raw_query_async", fake_run_raw_query_async)
    monkeypatch.setattr(sac_account_service, "format_records_dates", fake_format_records_dates)

    result = asyncio.run(sac_account_service.get_sac_account({"BranchName": "NY, LA"}))
    assert result == [{"CustomerNum": "1"}]


def test_get_sac_account_invalid_filters(monkeypatch):
    def fake_sanitize_filters(params, allowed):
        raise ValueError("bad filters")

    monkeypatch.setattr(sac_account_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(sac_account_service.get_sac_account({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_sac_account_inserts_without_pk(monkeypatch):
    async def fake_insert_records_async(*, table, records):
        return {"count": len(records)}

    monkeypatch.setattr(
        sac_account_service,
        "normalize_payload_dates",
        lambda payload, fields=None: {"CustomerNum": "1"},
    )
    monkeypatch.setattr(sac_account_service, "insert_records_async", fake_insert_records_async)

    result = asyncio.run(sac_account_service.upsert_sac_account({"CustomerNum": "1"}))
    assert result == {"count": 1}


def test_upsert_sac_account_updates_with_pk(monkeypatch):
    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, **kwargs):
        return {"count": len(data_list)}

    monkeypatch.setattr(
        sac_account_service,
        "normalize_payload_dates",
        lambda payload, fields=None: {"AcctSpecialKey": 10, "CustomerNum": "1"},
    )
    monkeypatch.setattr(
        sac_account_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    result = asyncio.run(sac_account_service.upsert_sac_account({"AcctSpecialKey": 10}))
    assert result == {"count": 1}
