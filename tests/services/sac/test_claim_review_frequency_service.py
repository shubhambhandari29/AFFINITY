from datetime import date

import pytest

from services.sac import claim_review_frequency_service as svc


@pytest.mark.anyio
async def test_get_frequency_remaps_filters(monkeypatch):
    def fake_sanitize(params, allowed):
        assert params == {"CustNum": "1"}
        assert allowed == svc.ALLOWED_FILTERS_DB
        return params

    async def fake_fetch(**kwargs):
        assert kwargs["filters"] == {"CustNum": "1"}
        return [
            {"CustNum": "1", "MthNum": 1, "NextReviewDate": "2024-02-01 00:00:00"}
        ]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_frequency({"CustomerNum": "1"})
    assert result == [
        {"CustomerNum": "1", "MthNum": 1, "NextReviewDate": "01-02-2024"}
    ]


@pytest.mark.anyio
async def test_upsert_frequency_remaps_payload(monkeypatch):
    captured = {}

    async def fake_merge(**kwargs):
        captured["data"] = kwargs["data_list"]
        return {"count": len(kwargs["data_list"])}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [
        {"CustomerNum": "1", "MthNum": 1, "Frequency": 5, "NextReviewDate": "01-02-2024"}
    ]
    result = await svc.upsert_frequency(payload)
    assert result == {"count": 1}
    assert captured["data"][0]["CustNum"] == "1"
    assert captured["data"][0]["NextReviewDate"] == date(2024, 2, 1)


@pytest.mark.anyio
async def test_upsert_frequency_sets_null_compdate_when_missing(monkeypatch):
    captured = {}

    async def fake_merge(**kwargs):
        captured["data"] = kwargs["data_list"]
        return {"count": len(kwargs["data_list"])}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [{"CustomerNum": "1", "MthNum": 12, "Frequency": 1}]

    await svc.upsert_frequency(payload)

    row = captured["data"][0]
    assert "CompDate" in row and row["CompDate"] is None
    assert "RptType" in row and row["RptType"] is None
    assert "CRNumNarr" in row and row["CRNumNarr"] is None
    assert "DelivMeth" in row and row["DelivMeth"] is None
