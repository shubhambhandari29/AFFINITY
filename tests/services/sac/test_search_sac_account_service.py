import pytest
from fastapi import HTTPException

from services.sac import search_sac_account_service as svc


@pytest.mark.anyio
async def test_search_valid_key(monkeypatch):
    async def fake_run(query):
        assert query.strip().startswith("SELECT")
        return [{"Customer Name": "ACME", "On Board Date": "2024-01-01T00:00:00Z"}]

    monkeypatch.setattr(svc, "run_raw_query_async", fake_run)
    result = await svc.search_sac_account_records("AccountName")
    assert result == [{"Customer Name": "ACME", "On Board Date": "01-01-2024"}]


@pytest.mark.anyio
async def test_search_invalid_key():
    with pytest.raises(HTTPException):
        await svc.search_sac_account_records("Unknown")
