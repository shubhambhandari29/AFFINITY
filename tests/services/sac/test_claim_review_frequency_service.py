from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import claim_review_frequency_service


def test_remap_keys_maps_customer_num():
    payload = {"CustomerNum": "123", "MthNum": 2}
    remapped = claim_review_frequency_service._remap_keys(payload)
    assert remapped == {"CustNum": "123", "MthNum": 2}
    assert payload == {"CustomerNum": "123", "MthNum": 2}


def test_restore_customer_num_maps_custnum():
    records = [{"CustNum": "123", "MthNum": 1}, {"Other": "x"}]
    restored = claim_review_frequency_service._restore_customer_num(records)
    assert restored[0]["CustomerNum"] == "123"
    assert "CustNum" not in restored[0]
    assert restored[1] == {"Other": "x"}


def test_get_frequency_success_remaps_and_formats(monkeypatch):
    captured = {}

    def fake_sanitize_filters(normalized, allowed):
        captured["normalized"] = normalized
        captured["allowed"] = allowed
        return normalized

    async def fake_fetch_records_async(*, table, filters, order_by):
        captured["table"] = table
        captured["filters"] = filters
        captured["order_by"] = order_by
        return [{"CustNum": "123", "MthNum": 1, "CompDate": "2024-01-02"}]

    def fake_format_records_dates(records):
        captured["formatted"] = records
        return [{"CustomerNum": "123", "MthNum": 1, "CompDate": "01-02-2024"}]

    monkeypatch.setattr(claim_review_frequency_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(claim_review_frequency_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(claim_review_frequency_service, "format_records_dates", fake_format_records_dates)

    result = asyncio.run(
        claim_review_frequency_service.get_frequency({"CustomerNum": "123", "MthNum": 1})
    )

    assert captured["normalized"] == {"CustNum": "123", "MthNum": 1}
    assert captured["allowed"] == claim_review_frequency_service.ALLOWED_FILTERS_DB
    assert captured["table"] == claim_review_frequency_service.TABLE_NAME
    assert captured["order_by"] == claim_review_frequency_service.ORDER_BY_COLUMN
    assert "CustomerNum" in captured["formatted"][0]
    assert result == [{"CustomerNum": "123", "MthNum": 1, "CompDate": "01-02-2024"}]


def test_get_frequency_invalid_filters_returns_http_400(monkeypatch):
    def fake_sanitize_filters(normalized, allowed):
        raise ValueError("bad filters")

    monkeypatch.setattr(claim_review_frequency_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(claim_review_frequency_service.get_frequency({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_frequency_remaps_keys_and_nulls(monkeypatch):
    captured = {}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["table"] = table
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"count": len(data_list)}

    monkeypatch.setattr(claim_review_frequency_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        claim_review_frequency_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    data_list = [
        {
            "CustomerNum": "123",
            "MthNum": 1,
            "CompDate": "",
            "RptType": None,
            "CRNumNarr": "",
            "DelivMeth": "",
        }
    ]

    result = asyncio.run(claim_review_frequency_service.upsert_frequency(data_list))

    assert result == {"count": 1}
    row = captured["data_list"][0]
    assert "CustomerNum" not in row
    assert row["CustNum"] == "123"
    assert row["CompDate"] is None
    assert row["RptType"] is None
    assert row["CRNumNarr"] is None
    assert row["DelivMeth"] is None
    assert captured["key_columns"] == ["CustNum", "MthNum"]


def test_upsert_frequency_error_returns_http_500(monkeypatch):
    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        raise RuntimeError("db down")

    monkeypatch.setattr(claim_review_frequency_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        claim_review_frequency_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(claim_review_frequency_service.upsert_frequency([{"CustomerNum": "1"}]))

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}
