from datetime import date

import pytest

from services.sac import sac_affiliates_service as svc


@pytest.mark.anyio
async def test_get_affiliates(monkeypatch):
    async def fake_fetch(table, filters):
        assert table == svc.TABLE_NAME
        assert filters == {"CustomerNum": "1"}
        return [{"AffiliateName": "Alpha", "StartDate": "2024-06-01"}]

    def fake_sanitize(params, allowed=None):
        return params

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_affiliates({"CustomerNum": "1"})
    assert result == [{"AffiliateName": "Alpha", "StartDate": "01-06-2024"}]


@pytest.mark.anyio
async def test_upsert_affiliates_splits_records(monkeypatch):
    calls = {"merge": None, "insert": None}

    async def fake_merge(**kwargs):
        calls["merge"] = kwargs["data_list"]

    async def fake_insert(**kwargs):
        calls["insert"] = kwargs["records"]

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)
    monkeypatch.setattr(svc, "insert_records_async", fake_insert)

    payload = [
        {"PK_Number": 1, "CustomerNum": "1", "AffiliateName": "Alpha", "StartDate": "01-06-2024"},
        {"CustomerNum": "2", "AffiliateName": "Beta", "StartDate": "02-06-2024"},
    ]

    result = await svc.upsert_affiliates(payload)
    assert result == {"message": "Transaction successful", "count": 2}
    assert len(calls["merge"]) == 1
    assert len(calls["insert"]) == 1
    assert calls["merge"][0]["PK_Number"] == 1
    assert calls["insert"][0]["CustomerNum"] == "2"
    assert calls["merge"][0]["StartDate"] == date(2024, 6, 1)
    assert calls["insert"][0]["StartDate"] == date(2024, 6, 2)
