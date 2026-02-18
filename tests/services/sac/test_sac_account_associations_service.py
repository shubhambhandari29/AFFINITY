from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.sac import sac_account_associations_service


def test_normalize_children_removes_duplicates_and_blanks():
    children = ["A", " ", None, "A", "B", "b ", "B"]
    assert sac_account_associations_service._normalize_children(children) == ["A", "B", "b"]


def test_add_associations_validates_payload():
    with pytest.raises(HTTPException):
        asyncio.run(sac_account_associations_service.add_associations({}))

    with pytest.raises(HTTPException):
        asyncio.run(
            sac_account_associations_service.add_associations(
                {"parent_account": "P", "child_account": "C"}
            )
        )


def test_add_associations_no_new_children(monkeypatch):
    payload = {"parent_account": "P", "child_account": ["P", " ", None]}

    result = asyncio.run(sac_account_associations_service.add_associations(payload))
    assert result == {"message": "No new associations to add", "count": 0}


def test_add_associations_inserts_new_pairs(monkeypatch):
    captured = {"records": None}

    async def fake_fetch_records_async(*, table, filters):
        parent = filters.get("ParentAccount")
        if parent == "P":
            return [{"AssociatedAccount": "C1"}]
        if parent == "C1":
            return [{"AssociatedAccount": "P"}]
        return []

    async def fake_insert_records_async(*, table, records):
        captured["records"] = records
        return {"count": len(records)}

    monkeypatch.setattr(
        sac_account_associations_service, "fetch_records_async", fake_fetch_records_async
    )
    monkeypatch.setattr(
        sac_account_associations_service, "insert_records_async", fake_insert_records_async
    )

    payload = {"parent_account": "P", "child_account": ["C1", "C2"]}
    result = asyncio.run(sac_account_associations_service.add_associations(payload))

    assert result == {"count": 2}
    assert captured["records"] == [
        {"ParentAccount": "P", "AssociatedAccount": "C2"},
        {"ParentAccount": "C2", "AssociatedAccount": "P"},
    ]


def test_delete_associations_validates_payload():
    with pytest.raises(HTTPException):
        asyncio.run(sac_account_associations_service.delete_associations({}))

    with pytest.raises(HTTPException):
        asyncio.run(
            sac_account_associations_service.delete_associations(
                {"parent_account": "P", "child_account": "C"}
            )
        )


def test_delete_associations_no_children():
    payload = {"parent_account": "P", "child_account": ["P", " ", None]}
    result = asyncio.run(sac_account_associations_service.delete_associations(payload))
    assert result == {"message": "No data provided for deletion", "count": 0}


def test_delete_associations_calls_delete_records(monkeypatch):
    captured = {}

    async def fake_delete_records_async(*, table, data_list, key_columns):
        captured["data_list"] = data_list
        captured["key_columns"] = key_columns
        return {"count": len(data_list)}

    monkeypatch.setattr(
        sac_account_associations_service, "delete_records_async", fake_delete_records_async
    )

    payload = {"parent_account": "P", "child_account": ["C1"]}
    result = asyncio.run(sac_account_associations_service.delete_associations(payload))

    assert result == {"count": 2}
    assert captured["key_columns"] == ["ParentAccount", "AssociatedAccount"]
    assert captured["data_list"] == [
        {"ParentAccount": "P", "AssociatedAccount": "C1"},
        {"ParentAccount": "C1", "AssociatedAccount": "P"},
    ]


def test_get_associations_requires_parent_account(monkeypatch):
    monkeypatch.setattr(
        sac_account_associations_service, "sanitize_filters", lambda params, allowed: {}
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(sac_account_associations_service.get_associations({}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "ParentAccount is required"}


def test_get_associations_success(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        return [{"ParentAccount": params[0]}]

    monkeypatch.setattr(
        sac_account_associations_service,
        "sanitize_filters",
        lambda params, allowed: {"ParentAccount": "P"},
    )
    monkeypatch.setattr(
        sac_account_associations_service, "run_raw_query_async", fake_run_raw_query_async
    )

    result = asyncio.run(sac_account_associations_service.get_associations({"ParentAccount": "P"}))
    assert result == [{"ParentAccount": "P"}]
