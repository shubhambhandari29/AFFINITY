from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.affinity import policy_type_distribution_service


def test_get_distribution_success(monkeypatch):
    captured = {}
    records = [{"ProgramName": "Alpha"}]
    formatted_records = [{"ProgramName": "Alpha"}]

    def fake_sanitize_filters(query_params):
        captured["filters"] = query_params
        return query_params

    async def fake_fetch_records_async(*, table, filters):
        captured["table"] = table
        captured["fetch_filters"] = filters
        return list(records)

    def fake_format_records_dates(rows):
        return list(formatted_records)

    monkeypatch.setattr(policy_type_distribution_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(policy_type_distribution_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(policy_type_distribution_service, "format_records_dates", fake_format_records_dates)

    result = asyncio.run(
        policy_type_distribution_service.get_distribution({"ProgramName": "Alpha"})
    )

    assert captured["table"] == policy_type_distribution_service.TABLE_NAME
    assert result == formatted_records


def test_get_distribution_invalid_filters_returns_http_400(monkeypatch):
    def fake_sanitize_filters(query_params):
        raise ValueError("bad filters")

    monkeypatch.setattr(policy_type_distribution_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(policy_type_distribution_service.get_distribution({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_distribution_success(monkeypatch):
    captured = {}

    def fake_validate(rows):
        return []

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["table"] = table
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"count": len(data_list)}

    monkeypatch.setattr(
        policy_type_distribution_service,
        "validate_policy_type_distribution_rows",
        fake_validate,
    )
    monkeypatch.setattr(policy_type_distribution_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        policy_type_distribution_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    data_list = [{"ProgramName": "Alpha", "EMailAddress": "a@example.com"}]
    result = asyncio.run(policy_type_distribution_service.upsert_distribution(data_list))

    assert result == {"count": 1}
    assert captured["key_columns"] == policy_type_distribution_service.KEY_COLUMNS


def test_upsert_distribution_error_returns_http_500(monkeypatch):
    def fake_validate(rows):
        return []

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        policy_type_distribution_service,
        "validate_policy_type_distribution_rows",
        fake_validate,
    )
    monkeypatch.setattr(policy_type_distribution_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        policy_type_distribution_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            policy_type_distribution_service.upsert_distribution(
                [{"ProgramName": "Alpha", "EMailAddress": "a@example.com"}]
            )
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}


def test_delete_distribution_calls_delete_records(monkeypatch):
    captured = {}

    async def fake_delete_records_async(*, table, data_list, key_columns):
        captured["table"] = table
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"count": len(data_list)}

    monkeypatch.setattr(
        policy_type_distribution_service,
        "delete_records_async",
        fake_delete_records_async,
    )

    data_list = [{"ProgramName": "Alpha", "EMailAddress": "a@example.com"}]
    result = asyncio.run(policy_type_distribution_service.delete_distribution(data_list))

    assert result == {"count": 1}
    assert captured["table"] == policy_type_distribution_service.TABLE_NAME
    assert captured["key_columns"] == policy_type_distribution_service.KEY_COLUMNS
