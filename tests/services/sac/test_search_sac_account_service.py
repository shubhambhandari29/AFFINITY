from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import search_sac_account_service


def test_search_sac_account_records_invalid_search_by():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(search_sac_account_service.search_sac_account_records("Nope"))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Invalid search type"}


def test_search_sac_account_records_success_invokes_query_and_format(monkeypatch):
    captured = {"query": None, "formatted": False}
    raw_records = [{"On Board Date": "2024-01-15"}]
    formatted_records = [{"On Board Date": "01-15-2024"}]

    async def fake_run_raw_query_async(query):
        captured["query"] = query
        return list(raw_records)

    def fake_format_records_dates(records):
        assert records == raw_records
        captured["formatted"] = True
        return list(formatted_records)

    monkeypatch.setattr(
        search_sac_account_service, "run_raw_query_async", fake_run_raw_query_async
    )
    monkeypatch.setattr(
        search_sac_account_service, "format_records_dates", fake_format_records_dates
    )

    result = asyncio.run(
        search_sac_account_service.search_sac_account_records("AccountName")
    )

    assert captured["query"] == search_sac_account_service.SEARCH_QUERIES["AccountName"]
    assert captured["formatted"] is True
    assert result == formatted_records


def test_search_sac_account_records_query_error_surfaces_http_exception(monkeypatch):
    async def fake_run_raw_query_async(query):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(
        search_sac_account_service, "run_raw_query_async", fake_run_raw_query_async
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(search_sac_account_service.search_sac_account_records("CustomerNum"))

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db exploded"}
