from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import loss_run_frequency_service


def test_get_frequency_success(monkeypatch):
    async def fake_fetch_records_async(*, table, filters, order_by):
        return [{"CustNum": "1", "MthNum": 1}]

    monkeypatch.setattr(
        loss_run_frequency_service, "sanitize_filters", lambda params, allowed: params
    )
    monkeypatch.setattr(
        loss_run_frequency_service, "fetch_records_async", fake_fetch_records_async
    )
    monkeypatch.setattr(
        loss_run_frequency_service, "format_records_dates", lambda records: records
    )

    result = asyncio.run(
        loss_run_frequency_service.get_frequency({"CustomerNum": "1", "MthNum": 1})
    )
    assert result == [{"CustomerNum": "1", "MthNum": 1}]


def test_get_frequency_invalid_filters(monkeypatch):
    def fake_sanitize_filters(params, allowed):
        raise ValueError("bad filters")

    monkeypatch.setattr(loss_run_frequency_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(loss_run_frequency_service.get_frequency({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_frequency_sets_compdate_none(monkeypatch):
    captured = {}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["data_list"] = data_list
        return {"count": len(data_list)}

    monkeypatch.setattr(loss_run_frequency_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        loss_run_frequency_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    result = asyncio.run(
        loss_run_frequency_service.upsert_frequency(
            [{"CustomerNum": "1", "MthNum": 1, "CompDate": ""}]
        )
    )

    assert result == {"count": 1}
    assert captured["data_list"][0]["CustNum"] == "1"
    assert captured["data_list"][0]["CompDate"] is None


def test_upsert_frequency_error(monkeypatch):
    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        loss_run_frequency_service, "normalize_payload_dates", lambda payload: dict(payload)
    )
    monkeypatch.setattr(
        loss_run_frequency_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            loss_run_frequency_service.upsert_frequency(
                [{"CustomerNum": "1", "MthNum": 1}]
            )
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}
