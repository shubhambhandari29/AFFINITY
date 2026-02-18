from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import claim_review_distribution_service


def test_get_distribution_success(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        return [{"CustomerNum": "1"}]

    monkeypatch.setattr(
        claim_review_distribution_service, "sanitize_filters", lambda params, allowed: params
    )
    monkeypatch.setattr(
        claim_review_distribution_service, "fetch_records_async", fake_fetch_records_async
    )
    monkeypatch.setattr(
        claim_review_distribution_service, "format_records_dates", lambda records: records
    )

    result = asyncio.run(
        claim_review_distribution_service.get_distribution({"CustomerNum": "1"})
    )
    assert result == [{"CustomerNum": "1"}]


def test_get_distribution_invalid_filters(monkeypatch):
    def fake_sanitize_filters(params, allowed):
        raise ValueError("bad filters")

    monkeypatch.setattr(
        claim_review_distribution_service, "sanitize_filters", fake_sanitize_filters
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(claim_review_distribution_service.get_distribution({"Bad": "X"}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "bad filters"}


def test_upsert_distribution_strips_identity_columns(monkeypatch):
    captured = {}

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        captured["data_list"] = data_list
        return {"count": len(data_list)}

    monkeypatch.setattr(
        claim_review_distribution_service, "normalize_payload_dates", lambda payload: dict(payload)
    )
    monkeypatch.setattr(
        claim_review_distribution_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    data_list = [{"CustomerNum": "1", "AttnTo": "A", "PK_Number": 1, "Other": "x"}]
    result = asyncio.run(claim_review_distribution_service.upsert_distribution(data_list))
    assert result == {"count": 1}
    assert captured["data_list"] == [{"CustomerNum": "1", "AttnTo": "A", "Other": "x"}]


def test_upsert_distribution_error(monkeypatch):
    async def fake_merge_upsert_records_async(*, table, data_list, key_columns):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        claim_review_distribution_service, "normalize_payload_dates", lambda payload: dict(payload)
    )
    monkeypatch.setattr(
        claim_review_distribution_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            claim_review_distribution_service.upsert_distribution(
                [{"CustomerNum": "1", "AttnTo": "A"}]
            )
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}


def test_delete_distribution_success(monkeypatch):
    async def fake_delete_records_async(*, table, data_list, key_columns):
        return {"count": len(data_list)}

    monkeypatch.setattr(
        claim_review_distribution_service, "delete_records_async", fake_delete_records_async
    )

    data_list = [{"CustomerNum": "1", "AttnTo": "A"}]
    result = asyncio.run(claim_review_distribution_service.delete_distribution(data_list))
    assert result == {"count": 1}
