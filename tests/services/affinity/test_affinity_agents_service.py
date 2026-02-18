from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.affinity import affinity_agents_service


def test_get_affinity_agents_success(monkeypatch):
    captured = {"filters": None}
    records = [{"ProgramName": "Alpha", "AgentCode": "A1"}]
    formatted_records = [{"ProgramName": "Alpha", "AgentCode": "A1"}]

    def fake_sanitize_filters(query_params):
        return {"ProgramName": "Alpha"}

    async def fake_fetch_records_async(*, table, filters):
        assert table == affinity_agents_service.TABLE_NAME
        captured["filters"] = filters
        return list(records)

    def fake_format_records_dates(rows):
        return list(formatted_records)

    monkeypatch.setattr(affinity_agents_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(affinity_agents_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(affinity_agents_service, "format_records_dates", fake_format_records_dates)

    result = asyncio.run(affinity_agents_service.get_affinity_agents({"ProgramName": "Alpha"}))

    assert captured["filters"] == {"ProgramName": "Alpha"}
    assert result == formatted_records


def test_get_affinity_agents_invalid_filter_returns_http_400(monkeypatch):
    def fake_sanitize_filters(query_params):
        raise ValueError("Invalid filter")

    monkeypatch.setattr(affinity_agents_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(affinity_agents_service.get_affinity_agents({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Invalid filter"}


def test_upsert_affinity_agents_strips_identity_columns(monkeypatch):
    captured = {"data_list": None, "key_columns": None}

    def fake_validate(payload):
        return []

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"message": "ok", "count": len(data_list)}

    monkeypatch.setattr(affinity_agents_service, "validate_affinity_agent_payload", fake_validate)
    monkeypatch.setattr(affinity_agents_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        affinity_agents_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    data_list = [
        {"ProgramName": "Alpha", "AgentCode": "A1", "PK_Number": 55, "Other": "X"}
    ]
    result = asyncio.run(affinity_agents_service.upsert_affinity_agents(data_list))

    assert result == {"message": "ok", "count": 1}
    assert captured["data_list"] == [{"ProgramName": "Alpha", "AgentCode": "A1", "Other": "X"}]
    assert captured["key_columns"] == affinity_agents_service.KEY_COLUMNS
