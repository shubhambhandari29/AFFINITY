from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import loss_run_distribution_service


def test_get_distribution_success(monkeypatch):
    captured = {}
    records = [{"CustomerNum": "123"}]
    formatted_records = [{"CustomerNum": "123"}]

    def fake_sanitize_filters(query_params, allowed):
        captured["filters"] = query_params
        captured["allowed"] = allowed
        return query_params

    async def fake_fetch_records_async(*, table, filters):
        captured["table"] = table
        captured["fetch_filters"] = filters
        return list(records)

    def fake_format_records_dates(rows):
        return list(formatted_records)

    monkeypatch.setattr(loss_run_distribution_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(loss_run_distribution_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(loss_run_distribution_service, "format_records_dates", fake_format_records_dates)

    result = asyncio.run(
        loss_run_distribution_service.get_distribution({"CustomerNum": "123"})
    )

    assert captured["allowed"] == loss_run_distribution_service.ALLOWED_FILTERS
    assert captured["table"] == loss_run_distribution_service.TABLE_NAME
    assert result == formatted_records


def test_get_distribution_invalid_filters_returns_http_400(monkeypatch):
    def fake_sanitize_filters(query_params, allowed):
        raise ValueError("bad filters")

    monkeypatch.setattr(loss_run_distribution_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(loss_run_distribution_service.get_distribution({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_distribution_strips_identity_columns(monkeypatch):
    captured = {}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["table"] = table
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"count": len(data_list)}

    monkeypatch.setattr(loss_run_distribution_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        loss_run_distribution_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    data_list = [
        {"CustomerNum": "123", "AttnTo": "A", "PK_Number": 5, "Other": "x"}
    ]

    result = asyncio.run(loss_run_distribution_service.upsert_distribution(data_list))

    assert result == {"count": 1}
    assert captured["data_list"] == [{"CustomerNum": "123", "AttnTo": "A", "Other": "x"}]
    assert captured["key_columns"] == ["CustomerNum", "AttnTo"]


def test_delete_distribution_calls_delete_records(monkeypatch):
    captured = {}

    async def fake_delete_records_async(*, table, data_list, key_columns):
        captured["table"] = table
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"count": len(data_list)}

    monkeypatch.setattr(
        loss_run_distribution_service, "delete_records_async", fake_delete_records_async
    )

    data_list = [{"CustomerNum": "123", "AttnTo": "A"}]
    result = asyncio.run(loss_run_distribution_service.delete_distribution(data_list))

    assert result == {"count": 1}
    assert captured["table"] == loss_run_distribution_service.TABLE_NAME
    assert captured["key_columns"] == ["CustomerNum", "AttnTo"]
