from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from services.hcm import hcm_account_associations_service


def test_add_associations_inserts_bidirectional_hcm_pairs(monkeypatch):
    captured = {}

    async def fake_fetch_records_async(*, table, filters):
        assert table == "tblHcmAccountAssociations"
        return []

    async def fake_insert_records_async(*, table, records):
        captured["table"] = table
        captured["records"] = records
        return {"count": len(records)}

    monkeypatch.setattr(
        hcm_account_associations_service,
        "fetch_records_async",
        fake_fetch_records_async,
    )
    monkeypatch.setattr(
        hcm_account_associations_service,
        "insert_records_async",
        fake_insert_records_async,
    )

    result = asyncio.run(
        hcm_account_associations_service.add_associations(
            {"parent_account": "P", "child_account": ["C"]}
        )
    )

    assert result == {"count": 2}
    assert captured == {
        "table": "tblHcmAccountAssociations",
        "records": [
            {"ParentAccount": "P", "AssociatedAccount": "C"},
            {"ParentAccount": "C", "AssociatedAccount": "P"},
        ],
    }


def test_add_associations_skips_existing_pairs(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        parent = filters["ParentAccount"]
        return [{"AssociatedAccount": "C" if parent == "P" else "P"}]

    monkeypatch.setattr(
        hcm_account_associations_service,
        "fetch_records_async",
        fake_fetch_records_async,
    )

    result = asyncio.run(
        hcm_account_associations_service.add_associations(
            {"parent_account": "P", "child_account": ["C"]}
        )
    )

    assert result == {"message": "No new associations to add", "count": 0}


def test_delete_associations_deletes_bidirectional_hcm_pairs(monkeypatch):
    captured = {}

    async def fake_delete_records_async(*, table, data_list, key_columns):
        captured.update(table=table, data_list=data_list, key_columns=key_columns)
        return {"count": len(data_list)}

    monkeypatch.setattr(
        hcm_account_associations_service,
        "delete_records_async",
        fake_delete_records_async,
    )

    result = asyncio.run(
        hcm_account_associations_service.delete_associations(
            {"parent_account": "P", "child_account": ["C"]}
        )
    )

    assert result == {"count": 2}
    assert captured["table"] == "tblHcmAccountAssociations"
    assert captured["key_columns"] == ["ParentAccount", "AssociatedAccount"]
    assert captured["data_list"] == [
        {"ParentAccount": "P", "AssociatedAccount": "C"},
        {"ParentAccount": "C", "AssociatedAccount": "P"},
    ]


def test_get_associations_uses_hcm_tables(monkeypatch):
    captured = {}

    async def fake_run_raw_query_async(query, params):
        captured.update(query=query, params=params)
        return [{"ParentAccount": params[0]}]

    monkeypatch.setattr(
        hcm_account_associations_service,
        "run_raw_query_async",
        fake_run_raw_query_async,
    )

    result = asyncio.run(
        hcm_account_associations_service.get_associations({"ParentAccount": "P"})
    )

    assert result == [{"ParentAccount": "P"}]
    assert captured["params"] == ["P"]
    assert "tblHcmAccountAssociations" in captured["query"]
    assert "tblHcmAccount AS child" in captured["query"]
    assert "tblSACAccountAssociations" not in captured["query"]
    assert "tblAcctSpecial" not in captured["query"]


def test_get_associations_requires_parent_account():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(hcm_account_associations_service.get_associations({}))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "ParentAccount is required"}
