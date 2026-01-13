from datetime import date

import pytest

from services.sac import loss_run_frequency_service as svc


@pytest.mark.anyio
async def test_get_frequency(monkeypatch):
    def fake_sanitize(params, allowed):
        assert params == {"CustNum": "1"}
        return params

    async def fake_fetch(**kwargs):
        assert kwargs["filters"] == {"CustNum": "1"}
        return [{"CustNum": "1", "MthNum": 3, "RunDate": "2024-04-05"}]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_frequency({"CustomerNum": "1"})
    assert result == [{"CustomerNum": "1", "MthNum": 3, "RunDate": "05-04-2024"}]


@pytest.mark.anyio
async def test_upsert_frequency(monkeypatch):
    captured = {}

    async def fake_merge(**kwargs):
        captured["data"] = kwargs["data_list"]
        return {"count": 1}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [{"CustomerNum": "1", "MthNum": 3, "RunDate": "05-04-2024"}]
    result = await svc.upsert_frequency(payload)
    assert result == {"count": 1}
    assert captured["data"][0]["CustNum"] == "1"
    assert captured["data"][0]["RunDate"] == date(2024, 4, 5)


@pytest.mark.anyio
async def test_upsert_frequency_sets_null_compdate_when_missing(monkeypatch):
    captured = {}

    async def fake_merge(**kwargs):
        captured["data"] = kwargs["data_list"]
        return {"count": len(kwargs["data_list"])}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [{"CustomerNum": "1", "MthNum": 11, "RptMth": 0}]

    await svc.upsert_frequency(payload)

    assert "CompDate" in captured["data"][0]
    assert captured["data"][0]["CompDate"] is None
