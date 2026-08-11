from __future__ import annotations

import asyncio

from fastapi import HTTPException

from services.hcm import hcm_users_service


def test_upsert_hcm_users_merges_when_customer_and_username_present(monkeypatch):
    captured = {"updates": None, "inserts": None}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, exclude_key_columns_from_insert=False):
        captured["updates"] = data_list
        return {"count": len(data_list)}

    async def fake_insert_records_async(*, table, records):
        captured["inserts"] = records
        return {"count": len(records)}

    monkeypatch.setattr(hcm_users_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(hcm_users_service, "merge_upsert_records_async", fake_merge_upsert_records_async)
    monkeypatch.setattr(hcm_users_service, "insert_records_async", fake_insert_records_async)

    data_list = [
        {"CustomerNum": "1", "UserName": "u1"},
    ]
    result = asyncio.run(hcm_users_service.upsert_hcm_users(data_list))

    assert result == {"message": "Transaction successful", "count": 1}
    assert captured["updates"] == [{"CustNum": "1", "UserName": "u1"}]
    assert captured["inserts"] == None


def test_upsert_hcm_users_discards_pk_and_uses_customer_and_username_key(monkeypatch):
    captured = {"updates": None, "inserts": None}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, exclude_key_columns_from_insert=False):
        captured["updates"] = data_list
        return {"count": len(data_list)}

    async def fake_insert_records_async(*, table, records):
        captured["inserts"] = records
        return {"count": len(records)}

    monkeypatch.setattr(hcm_users_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(hcm_users_service, "merge_upsert_records_async", fake_merge_upsert_records_async)
    monkeypatch.setattr(hcm_users_service, "insert_records_async", fake_insert_records_async)

    data_list = [
        {"PK_Number": 5, "CustomerNum": "1", "UserName": "u1"},
    ]
    result = asyncio.run(hcm_users_service.upsert_hcm_users(data_list))

    assert result == {"message": "Transaction successful", "count": 1}
    assert captured["updates"] == [{"CustNum": "1", "UserName": "u1"}]
    assert captured["inserts"] == None


def test_upsert_hcm_users_inserts_when_customer_or_username_missing(monkeypatch):
    captured = {"updates": None, "inserts": None}

    def fake_normalize(payload):
        return dict(payload)

    async def fake_merge_upsert_records_async(**kwargs):
        captured["updates"] = kwargs["data_list"]
        return {"count": len(kwargs["data_list"])}

    async def fake_insert_records_async(*, table, records):
        captured["inserts"] = records
        return {"count": len(records)}

    monkeypatch.setattr(hcm_users_service, "normalize_payload_dates", fake_normalize)
    monkeypatch.setattr(
        hcm_users_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )
    monkeypatch.setattr(
        hcm_users_service, "insert_records_async", fake_insert_records_async
    )

    data_list = [{"PK_Number": 5, "CustomerNum": "1", "UserEmail": "u@example.com"}]
    result = asyncio.run(hcm_users_service.upsert_hcm_users(data_list))

    assert result == {"message": "Transaction successful", "count": 1}
    assert captured["updates"] is None
    assert captured["inserts"] == [{"CustNum": "1", "UserEmail": "u@example.com"}]
