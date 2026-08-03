from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.hcm import search_hcm_account_service


def test_search_hcm_account_records_invalid_search_by():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(search_hcm_account_service.search_hcm_account_records("Nope"))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Invalid search type"}


def test_search_hcm_account_supports_only_account_name_and_customer_number():
    assert set(search_hcm_account_service.SEARCH_QUERIES) == {
        "AccountName",
        "CustomerNum",
    }


def test_search_hcm_account_records_invokes_hcm_query_and_formats_dates(monkeypatch):
    captured = {"query": None}
    raw_records = [{"On Board Date": "2024-01-15"}]
    formatted_records = [{"On Board Date": "01-15-2024"}]

    async def fake_run_raw_query_async(query):
        captured["query"] = query
        return raw_records

    monkeypatch.setattr(
        search_hcm_account_service, "run_raw_query_async", fake_run_raw_query_async
    )
    monkeypatch.setattr(
        search_hcm_account_service,
        "format_records_dates",
        lambda records: formatted_records,
    )

    result = asyncio.run(
        search_hcm_account_service.search_hcm_account_records("AccountName")
    )

    assert captured["query"] == search_hcm_account_service.SEARCH_QUERIES["AccountName"]
    assert "tblHcmAccount" in captured["query"]
    assert "tblAcctSpecial" not in captured["query"]
    assert result == formatted_records


def test_search_hcm_account_records_query_error_surfaces_http_exception(monkeypatch):
    async def fake_run_raw_query_async(query):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(
        search_hcm_account_service, "run_raw_query_async", fake_run_raw_query_async
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(search_hcm_account_service.search_hcm_account_records("CustomerNum"))

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db exploded"}
