from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.affinity import search_affinity_programs_service


def test_search_affinity_program_records_invalid_search_by():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(search_affinity_programs_service.search_affinity_program_records("Nope"))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Invalid search type"}


def test_search_affinity_program_records_success(monkeypatch):
    captured = {"query": None, "formatted": False}
    raw_records = [{"On Board Date": "2024-01-02"}]
    formatted_records = [{"On Board Date": "01-02-2024"}]

    async def fake_run_raw_query_async(query):
        captured["query"] = query
        return list(raw_records)

    def fake_format_records_dates(records):
        captured["formatted"] = True
        assert records == raw_records
        return list(formatted_records)

    monkeypatch.setattr(
        search_affinity_programs_service, "run_raw_query_async", fake_run_raw_query_async
    )
    monkeypatch.setattr(
        search_affinity_programs_service, "format_records_dates", fake_format_records_dates
    )

    result = asyncio.run(
        search_affinity_programs_service.search_affinity_program_records("ProgramName")
    )

    assert captured["query"] == search_affinity_programs_service.SEARCH_QUERIES["ProgramName"]
    assert captured["formatted"] is True
    assert result == formatted_records


def test_search_affinity_program_records_query_error(monkeypatch):
    async def fake_run_raw_query_async(query):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(
        search_affinity_programs_service, "run_raw_query_async", fake_run_raw_query_async
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            search_affinity_programs_service.search_affinity_program_records("ProducerCode")
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db exploded"}
