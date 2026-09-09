import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from services.sac import sac_policies_service as policies


@pytest.mark.parametrize("rows,expected", [([], None), ([{"PK_Number": 42}], 42)])
def test_lookup_returns_latest_identity_or_none(monkeypatch, rows, expected):
    query = AsyncMock(return_value=rows)
    monkeypatch.setattr(policies, "run_raw_query_async", query)
    assert (
        asyncio.run(
            policies._lookup_pk_number({"CustomerNum": "001", "PolicyNum": "P", "PolMod": "2"})
        )
        == expected
    )
    assert query.await_args.args[1] == ["001", "P", "2"]
    assert "ORDER BY PK_Number DESC" in query.await_args.args[0]


def test_lookup_failure_is_nonfatal(monkeypatch):
    monkeypatch.setattr(
        policies, "run_raw_query_async", AsyncMock(side_effect=RuntimeError("offline"))
    )
    assert (
        asyncio.run(
            policies._lookup_pk_number({"CustomerNum": "001", "PolicyNum": "P", "PolMod": "2"})
        )
        is None
    )


def test_missing_identity_inserts_without_client_identity(monkeypatch):
    monkeypatch.setattr(policies, "fetch_records_async", AsyncMock(return_value=[]))
    insert = AsyncMock()
    monkeypatch.setattr(policies, "insert_records_async", insert)
    monkeypatch.setattr(policies, "_lookup_pk_number", AsyncMock(return_value=42))
    result = asyncio.run(
        policies.upsert_sac_policies(
            {"PK_Number": 99, "CustomerNum": "001", "PolicyNum": "P", "PolMod": "2"}
        )
    )
    assert result["pk"] == 42
    assert insert.await_args.kwargs["records"] == [
        {"CustomerNum": "001", "PolicyNum": "P", "PolMod": "2"}
    ]


def test_upsert_failure_returns_500(monkeypatch):
    monkeypatch.setattr(
        policies, "insert_records_async", AsyncMock(side_effect=RuntimeError("offline"))
    )
    with pytest.raises(HTTPException) as error:
        asyncio.run(policies.upsert_sac_policies({"CustomerNum": "001"}))
    assert error.value.status_code == 500
    assert error.value.detail == {"error": "offline"}


def test_underwriters_are_sorted_deduplicated_and_missing_emails_identified(monkeypatch):
    rows = [
        {
            "AcctOwnerEmail": "owner@example.com",
            "UnderwriterName": "Zoe",
            "UnderwriterEmail": None,
            "UWMgr": "Zed",
            "UWMgrEmail": None,
        },
        {
            "UnderwriterName": "Amy",
            "UnderwriterEmail": "amy@example.com",
            "UWMgr": "Ann",
            "UWMgrEmail": "ann@example.com",
        },
        {
            "UnderwriterName": "Amy",
            "UnderwriterEmail": "amy@example.com",
            "UWMgr": "Ann",
            "UWMgrEmail": "ann@example.com",
        },
        {},
    ]
    monkeypatch.setattr(policies, "run_raw_query_async", AsyncMock(return_value=rows))
    assert asyncio.run(policies.get_underwriter_details({"CustomerNum": "001"})) == {
        "AcctOwnerEmail": "owner@example.com",
        "UnderwriterNames": ["Amy", "Zoe"],
        "UnderwriterEmails": ["amy@example.com"],
        "UWMgrNames": ["Ann", "Zed"],
        "UWMgrEmails": ["ann@example.com"],
        "MissingUnderwriters": ["Zoe"],
        "MissingUWManagers": ["Zed"],
    }


@pytest.mark.parametrize("name", ["New name", "", None])
def test_sync_account_name_updates_only_requested_customer(monkeypatch, name):
    update = AsyncMock(return_value={"count": 2})
    monkeypatch.setattr(policies, "update_records_async", update)
    assert asyncio.run(policies.sync_account_name({"CustomerNum": "001", "AccountName": name})) == {
        "count": 2
    }
    update.assert_awaited_once_with(
        table="tblPolicies",
        updates=[
            {
                "fieldName": "AccountName",
                "fieldValue": name,
                "updateVia": "CustomerNum",
                "updateViaValue": "001",
            }
        ],
    )


def test_sync_requires_customer_number(monkeypatch):
    update = AsyncMock()
    monkeypatch.setattr(policies, "update_records_async", update)
    with pytest.raises(HTTPException) as error:
        asyncio.run(policies.sync_account_name({"AccountName": "Example"}))
    assert error.value.status_code == 400
    update.assert_not_awaited()


def test_sync_database_failure_returns_500(monkeypatch):
    monkeypatch.setattr(
        policies, "update_records_async", AsyncMock(side_effect=RuntimeError("offline"))
    )
    with pytest.raises(HTTPException) as error:
        asyncio.run(policies.sync_account_name({"CustomerNum": "001"}))
    assert error.value.status_code == 500
    assert error.value.detail == {"error": "offline"}


def test_empty_bulk_update_is_rejected():
    with pytest.raises(HTTPException) as error:
        asyncio.run(policies.update_field_for_all_policies([]))
    assert error.value.status_code == 400
