from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.affinity import loss_run_frequency_service


def test_get_frequency_success(monkeypatch):
    async def fake_fetch_records_async(*, table, filters, order_by):
        return [{"ProgramName": "A", "MthNum": 1}]

    monkeypatch.setattr(loss_run_frequency_service, "sanitize_filters", lambda params: params)
    monkeypatch.setattr(
        loss_run_frequency_service, "fetch_records_async", fake_fetch_records_async
    )
    monkeypatch.setattr(
        loss_run_frequency_service, "format_records_dates", lambda records: records
    )

    result = asyncio.run(
        loss_run_frequency_service.get_frequency({"ProgramName": "A", "MthNum": 1})
    )
    assert result == [{"ProgramName": "A", "MthNum": 1}]


def test_get_frequency_invalid_filters(monkeypatch):
    def fake_sanitize_filters(params):
        raise ValueError("bad filters")

    monkeypatch.setattr(loss_run_frequency_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(loss_run_frequency_service.get_frequency({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_frequency_validation_errors(monkeypatch):
    monkeypatch.setattr(
        loss_run_frequency_service,
        "validate_affinity_frequency_rows",
        lambda rows: [{"field": "CompDate"}],
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            loss_run_frequency_service.upsert_frequency(
                [{"ProgramName": "A", "MthNum": 1}]
            )
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "400: {'errors': [{'field': 'CompDate'}]}"}


def test_upsert_frequency_success(monkeypatch):
    monkeypatch.setattr(
        loss_run_frequency_service,
        "validate_affinity_frequency_rows",
        lambda rows: [],
    )
    monkeypatch.setattr(
        loss_run_frequency_service, "normalize_payload_dates", lambda payload: dict(payload)
    )

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        return {"count": len(data_list)}

    monkeypatch.setattr(
        loss_run_frequency_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    result = asyncio.run(
        loss_run_frequency_service.upsert_frequency([{"ProgramName": "A", "MthNum": 1}])
    )
    assert result == {"count": 1}


def test_upsert_frequency_error(monkeypatch):
    monkeypatch.setattr(
        loss_run_frequency_service,
        "validate_affinity_frequency_rows",
        lambda rows: [],
    )
    monkeypatch.setattr(
        loss_run_frequency_service, "normalize_payload_dates", lambda payload: dict(payload)
    )

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        loss_run_frequency_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            loss_run_frequency_service.upsert_frequency(
                [{"ProgramName": "A", "MthNum": 1}]
            )
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}
