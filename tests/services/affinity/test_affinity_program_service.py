from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.affinity import affinity_program_service


def test_get_affinity_program_without_branch_filter_uses_fetch(monkeypatch):
    captured = {"filters": None, "formatted": False}
    records = [{"ProgramName": "Alpha", "Stage": "Retired"}, {"ProgramName": "Beta"}]
    formatted_records = [{"ProgramName": "Beta", "OnBoardDt": "01-01-2024"}]

    def fake_sanitize_filters(query_params):
        return {"ProgramName": "Alpha"}

    async def fake_fetch_records_async(*, table, filters):
        assert table == affinity_program_service.TABLE_NAME
        captured["filters"] = filters
        return list(records)

    def fake_format_records_dates(rows):
        assert rows == [{"ProgramName": "Beta"}]
        captured["formatted"] = True
        return list(formatted_records)

    monkeypatch.setattr(affinity_program_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(
        affinity_program_service, "fetch_records_async", fake_fetch_records_async
    )
    monkeypatch.setattr(
        affinity_program_service, "format_records_dates", fake_format_records_dates
    )

    result = asyncio.run(
        affinity_program_service.get_affinity_program({"ProgramName": "Alpha"})
    )

    assert captured["filters"] == {"ProgramName": "Alpha"}
    assert captured["formatted"] is True
    assert result == formatted_records


def test_get_affinity_program_with_branch_filter_builds_like_query(monkeypatch):
    captured = {"query": None, "params": None}
    records = [{"ProgramName": "Alpha", "Stage": "Retired"}, {"ProgramName": "Beta"}]

    def fake_sanitize_filters(query_params):
        return {"BranchVal": "NY, LA & SF", "ProgramName": "Alpha"}

    async def fake_run_raw_query_async(query, params):
        captured["query"] = query
        captured["params"] = params
        return list(records)

    def fake_format_records_dates(rows):
        return rows

    monkeypatch.setattr(affinity_program_service, "sanitize_filters", fake_sanitize_filters)
    monkeypatch.setattr(
        affinity_program_service, "run_raw_query_async", fake_run_raw_query_async
    )
    monkeypatch.setattr(
        affinity_program_service, "format_records_dates", fake_format_records_dates
    )

    result = asyncio.run(
        affinity_program_service.get_affinity_program({"BranchVal": "NY, LA & SF"})
    )

    assert "BranchVal LIKE ?" in captured["query"]
    assert captured["params"] == ["Alpha", "NY%", "LA%", "SF%"]
    assert result == [{"ProgramName": "Beta"}]


def test_get_affinity_program_validation_error_returns_http_400(monkeypatch):
    def fake_sanitize_filters(query_params):
        raise ValueError("Bad filter")

    monkeypatch.setattr(affinity_program_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(affinity_program_service.get_affinity_program({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Bad filter"}


def test_upsert_affinity_program_updates_when_primary_key_present(monkeypatch):
    captured = {"args": None, "kwargs": None}

    def fake_validate(payload):
        return []

    def fake_normalize(payload):
        return {"AcctAffinityProgramKey": 10, "ProgramName": "Alpha"}

    async def fake_merge_upsert_records_async(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"message": "ok", "count": 1}

    monkeypatch.setattr(affinity_program_service, "validate_affinity_program_payload", fake_validate)
    monkeypatch.setattr(affinity_program_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        affinity_program_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    result = asyncio.run(affinity_program_service.upsert_affinity_program({"ProgramName": "Alpha"}))

    assert result == {"message": "ok", "count": 1}
    assert captured["kwargs"]["table"] == affinity_program_service.TABLE_NAME
    assert captured["kwargs"]["key_columns"] == [affinity_program_service.PRIMARY_KEY]
    assert captured["kwargs"]["exclude_key_columns_from_insert"] is True
    assert captured["kwargs"]["data_list"] == [
        {"AcctAffinityProgramKey": 10, "ProgramName": "Alpha"}
    ]


def test_upsert_affinity_program_inserts_without_primary_key(monkeypatch):
    captured = {"data_list": None, "key_columns": None}

    def fake_validate(payload):
        return []

    def fake_normalize(payload):
        return {"AcctAffinityProgramKey": None, "ProgramName": "Alpha"}

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, **kwargs):
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"message": "ok", "count": 1}

    monkeypatch.setattr(affinity_program_service, "validate_affinity_program_payload", fake_validate)
    monkeypatch.setattr(affinity_program_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        affinity_program_service, "merge_upsert_records_async", fake_merge_upsert_records_async
    )

    result = asyncio.run(affinity_program_service.upsert_affinity_program({"ProgramName": "Alpha"}))

    assert result == {"message": "ok", "count": 1}
    assert captured["data_list"] == [{"ProgramName": "Alpha"}]
    assert captured["key_columns"] == affinity_program_service.KEY_COLUMNS
