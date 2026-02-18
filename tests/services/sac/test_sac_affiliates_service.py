from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import sac_affiliates_service


def test_get_affiliates_success(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        return [{"ProgramName": "A"}]

    monkeypatch.setattr(sac_affiliates_service, "sanitize_filters", lambda params: params)
    monkeypatch.setattr(sac_affiliates_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(sac_affiliates_service, "format_records_dates", lambda records: records)

    result = asyncio.run(sac_affiliates_service.get_affiliates({"ProgramName": "A"}))
    assert result == [{"ProgramName": "A"}]


def test_get_affiliates_invalid_filters(monkeypatch):
    def fake_sanitize_filters(params):
        raise ValueError("bad filter")

    monkeypatch.setattr(sac_affiliates_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(sac_affiliates_service.get_affiliates({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filter"}


def test_upsert_affiliates_splits_insert_update(monkeypatch):
    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, **kwargs):
        return {"count": len(data_list)}

    async def fake_insert_records_async(*, table, records):
        return {"count": len(records)}

    monkeypatch.setattr(
        sac_affiliates_service, "normalize_payload_dates", lambda payload: dict(payload)
    )
    monkeypatch.setattr(
        sac_affiliates_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )
    monkeypatch.setattr(
        sac_affiliates_service, "insert_records_async", fake_insert_records_async
    )

    data_list = [{"PK_Number": 1, "Name": "A"}, {"PK_Number": None, "Name": "B"}]
    result = asyncio.run(sac_affiliates_service.upsert_affiliates(data_list))
    assert result == {"message": "Transaction successful", "count": 2}
