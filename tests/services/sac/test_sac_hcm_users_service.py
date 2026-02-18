from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import hcm_users_service


def test_get_hcm_users_success(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        return [{"CustNum": "1", "UserName": "u"}]

    monkeypatch.setattr(hcm_users_service, "sanitize_filters", lambda params: params)
    monkeypatch.setattr(hcm_users_service, "fetch_records_async", fake_fetch_records_async)
    monkeypatch.setattr(hcm_users_service, "format_records_dates", lambda records: records)

    result = asyncio.run(hcm_users_service.get_hcm_users({"CustomerNum": "1"}))
    assert result == [{"CustomerNum": "1", "UserName": "u"}]


def test_get_hcm_users_invalid_filters(monkeypatch):
    def fake_sanitize_filters(params):
        raise ValueError("bad filters")

    monkeypatch.setattr(hcm_users_service, "sanitize_filters", fake_sanitize_filters)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(hcm_users_service.get_hcm_users({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_hcm_users_splits_insert_update(monkeypatch):
    captured = {"updates": None, "inserts": None}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["updates"] = data_list
        return {"count": len(data_list)}

    async def fake_insert_records_async(*, table, records):
        captured["inserts"] = records
        return {"count": len(records)}

    monkeypatch.setattr(hcm_users_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(hcm_users_service, "merge_upsert_records_async", fake_merge_upsert_records_async)
    monkeypatch.setattr(hcm_users_service, "insert_records_async", fake_insert_records_async)

    data_list = [
        {"CustomerNum": "1", "UserName": "u1", "PK_Number": 1},
        {"CustomerNum": "", "UserName": "u2"},
    ]
    result = asyncio.run(hcm_users_service.upsert_hcm_users(data_list))
    assert result == {"message": "Transaction successful", "count": 2}
    assert captured["updates"] == [{"CustNum": "1", "UserName": "u1"}]
    assert captured["inserts"] == [{"CustNum": "", "UserName": "u2"}]


def test_upsert_hcm_users_error(monkeypatch):
    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        raise RuntimeError("db down")

    monkeypatch.setattr(hcm_users_service, "normalize_payload_dates", lambda payload: dict(payload))
    monkeypatch.setattr(hcm_users_service, "merge_upsert_records_async", fake_merge_upsert_records_async)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(hcm_users_service.upsert_hcm_users([{"CustomerNum": "1", "UserName": "u"}]))

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}
