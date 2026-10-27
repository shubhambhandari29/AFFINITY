from __future__ import annotations

import asyncio
import importlib
from datetime import datetime

import pytest
from fastapi import HTTPException


@pytest.fixture(params=[
    ("sac.sac_account_service", "get_retired_sac_accounts", "tblAcctSpecial"),
    ("hcm.hcm_account_service", "get_retired_hcm_accounts", "tblHcmAccount"),
    ("affinity.affinity_program_service", "get_retired_affinity_programs", "tblAcctAffinityProgram"),
])
def retired_service(request):
    module_name, function_name, table = request.param
    module = importlib.import_module(f"services.{module_name}")
    return module, getattr(module, function_name), table


@pytest.mark.parametrize("empty", [False, True])
def test_retired_filter_and_date_formatting(monkeypatch, retired_service, empty):
    module, service, expected_table = retired_service
    records = [] if empty else [
        {"Stage": "Retired", "AcctStatus": status, "IsSubmitted": submitted,
         "OnBoardDate": datetime(2026, 10, 27),
         "UpdateDateTime": "2026-10-27 14:35:42.1234567 +05:30",
         "UpdatDateTime": "2026-10-27 14:35:42.1234567 +05:30"}
        for status, submitted in [("Active", 0), ("Inactive", 1), (None, None)]
    ]

    async def fake_fetch(*, table, filters):
        assert table == expected_table
        assert filters == {"Stage": "Retired"}
        return records

    monkeypatch.setattr(module, "fetch_records_async", fake_fetch)
    result = asyncio.run(service())
    assert len(result) == len(records)
    for row, original in zip(result, records, strict=True):
        assert row["Stage"] == "Retired"
        assert row["AcctStatus"] == original["AcctStatus"]
        assert row["IsSubmitted"] == original["IsSubmitted"]
        assert isinstance(row["OnBoardDate"], str)
        assert row["UpdateDateTime"] == "10-27-2026 02:35:42 PM"
        assert row["UpdatDateTime"] == "10-27-2026 02:35:42 PM"


def test_retired_database_error(monkeypatch, retired_service):
    module, service, _ = retired_service

    async def fake_fetch(**kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(module, "fetch_records_async", fake_fetch)
    with pytest.raises(HTTPException) as error:
        asyncio.run(service())
    assert error.value.status_code == 500
