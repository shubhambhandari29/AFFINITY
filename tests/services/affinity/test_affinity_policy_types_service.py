from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.affinity import affinity_policy_types_service


def test_lookup_pk_number_returns_value(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        return [{"PK_Number": 10}]

    monkeypatch.setattr(
        affinity_policy_types_service, "run_raw_query_async", fake_run_raw_query_async
    )

    result = asyncio.run(
        affinity_policy_types_service._lookup_pk_number(
            {"ProgramName": "A", "PolicyType": "B"}
        )
    )
    assert result == 10


def test_lookup_pk_number_returns_none_on_error(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        affinity_policy_types_service, "run_raw_query_async", fake_run_raw_query_async
    )

    result = asyncio.run(
        affinity_policy_types_service._lookup_pk_number(
            {"ProgramName": "A", "PolicyType": "B"}
        )
    )
    assert result is None


def test_get_affinity_policy_types_program_name_path(monkeypatch):
    captured = {}

    async def fake_run_raw_query_async(query, params):
        captured["query"] = query
        captured["params"] = params
        return [{"ProgramName": "A"}]

    monkeypatch.setattr(
        affinity_policy_types_service, "run_raw_query_async", fake_run_raw_query_async
    )
    monkeypatch.setattr(
        affinity_policy_types_service, "format_records_dates", lambda records: records
    )

    result = asyncio.run(
        affinity_policy_types_service.get_affinity_policy_types(
            {"ProgramName": "A", "PolicyType": "P"}
        )
    )

    assert "WITH primary_agents" in captured["query"]
    assert captured["params"][0] == "yes"
    assert result == [{"ProgramName": "A"}]


def test_get_affinity_policy_types_primary_agt_path(monkeypatch):
    captured = {}

    async def fake_run_raw_query_async(query, params):
        captured["query"] = query
        captured["params"] = params
        return [{"ProgramName": "A"}]

    monkeypatch.setattr(
        affinity_policy_types_service, "run_raw_query_async", fake_run_raw_query_async
    )
    monkeypatch.setattr(
        affinity_policy_types_service, "format_records_dates", lambda records: records
    )

    result = asyncio.run(
        affinity_policy_types_service.get_affinity_policy_types({"PrimaryAgt": "yes"})
    )

    assert "WHERE EXISTS" in captured["query"]
    assert captured["params"][0] == "yes"
    assert result == [{"ProgramName": "A"}]


def test_get_affinity_policy_types_invalid_identifier(monkeypatch):
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            affinity_policy_types_service.get_affinity_policy_types({"Bad-Name": "x"})
        )

    assert excinfo.value.status_code == 400


def test_upsert_affinity_policy_types_validation_error(monkeypatch):
    monkeypatch.setattr(
        affinity_policy_types_service,
        "apply_affinity_policy_type_defaults",
        lambda data: data,
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "validate_affinity_policy_type_payload",
        lambda data: [{"field": "ProgramName"}],
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(affinity_policy_types_service.upsert_affinity_policy_types({"x": 1}))

    assert excinfo.value.status_code == 400
    assert "errors" in excinfo.value.detail


def test_upsert_affinity_policy_types_pk_not_found(monkeypatch):
    monkeypatch.setattr(
        affinity_policy_types_service,
        "apply_affinity_policy_type_defaults",
        lambda data: data,
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "validate_affinity_policy_type_payload",
        lambda data: [],
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "normalize_payload_dates",
        lambda data: {"PK_Number": 99, "ProgramName": "A"},
    )

    async def fake_fetch_records_async(*, table, filters):
        return []

    monkeypatch.setattr(
        affinity_policy_types_service, "fetch_records_async", fake_fetch_records_async
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(affinity_policy_types_service.upsert_affinity_policy_types({"PK_Number": 99}))

    assert excinfo.value.status_code == 404


def test_upsert_affinity_policy_types_updates_when_pk_found(monkeypatch):
    monkeypatch.setattr(
        affinity_policy_types_service,
        "apply_affinity_policy_type_defaults",
        lambda data: data,
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "validate_affinity_policy_type_payload",
        lambda data: [],
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "normalize_payload_dates",
        lambda data: {"PK_Number": 1, "ProgramName": "A"},
    )

    async def fake_fetch_records_async(*, table, filters):
        return [{"PK_Number": 1}]

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, **kwargs):
        return {"count": len(data_list)}

    monkeypatch.setattr(
        affinity_policy_types_service, "fetch_records_async", fake_fetch_records_async
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    result = asyncio.run(affinity_policy_types_service.upsert_affinity_policy_types({"PK_Number": 1}))
    assert result == {"message": "Transaction successful", "count": 1, "pk": 1}


def test_upsert_affinity_policy_types_inserts_without_pk(monkeypatch):
    monkeypatch.setattr(
        affinity_policy_types_service,
        "apply_affinity_policy_type_defaults",
        lambda data: data,
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "validate_affinity_policy_type_payload",
        lambda data: [],
    )
    monkeypatch.setattr(
        affinity_policy_types_service,
        "normalize_payload_dates",
        lambda data: {"ProgramName": "A", "PolicyType": "P"},
    )

    async def fake_insert_records_async(*, table, records):
        return {"count": len(records)}

    async def fake_lookup(record):
        return 10

    monkeypatch.setattr(
        affinity_policy_types_service, "insert_records_async", fake_insert_records_async
    )
    monkeypatch.setattr(affinity_policy_types_service, "_lookup_pk_number", fake_lookup)

    result = asyncio.run(affinity_policy_types_service.upsert_affinity_policy_types({"x": 1}))
    assert result == {"message": "Transaction successful", "count": 1, "pk": 10}
