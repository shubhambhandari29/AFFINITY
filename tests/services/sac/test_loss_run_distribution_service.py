from datetime import date

import pytest
from fastapi import HTTPException

from services.sac import loss_run_distribution_service as svc


@pytest.mark.anyio
async def test_get_distribution(monkeypatch):
    def fake_sanitize(params, allowed=None):
        return params

    async def fake_fetch(**kwargs):
        assert kwargs["filters"] == {"CustomerNum": "1"}
        return [{"CustomerNum": "1", "ReportDate": "2024-04-01T00:00:00"}]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_distribution({"CustomerNum": "1"})
    assert result == [{"CustomerNum": "1", "ReportDate": "01-04-2024"}]


@pytest.mark.anyio
async def test_upsert_distribution(monkeypatch):
    captured: dict[str, list[dict[str, Any]] | None] = {"merge": None, "insert": None}

    async def fake_merge(**kwargs):
        captured["merge"] = kwargs["data_list"]
        return {"count": len(kwargs["data_list"])}

    async def fake_insert(**kwargs):
        captured["insert"] = kwargs["records"]
        return {"count": len(kwargs["records"])}

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)
    monkeypatch.setattr(svc, "insert_records_async", fake_insert)

    payload = [
        {
            "PK_Number": 1,
            "CustomerNum": "1",
            "RecipCat": "Loss Control",
            "DistVia": "Email",
            "AttnTo": "Name1",
            "EMailAddress": "a@example.com",
            "ReportDate": "01-04-2024",
        },
        {
            "CustomerNum": "2",
            "RecipCat": "Loss Control",
            "DistVia": "Email",
            "AttnTo": "Name2",
            "EMailAddress": "b@example.com",
            "ReportDate": "02-04-2024",
        },
    ]

    result = await svc.upsert_distribution(payload)

    assert captured["merge"] is not None
    assert captured["insert"] is not None
    assert captured["merge"][0]["ReportDate"] == date(2024, 4, 1)
    assert captured["insert"][0]["ReportDate"] == date(2024, 4, 2)
    assert "PK_Number" not in captured["insert"][0]
    assert result == {"message": "Transaction successful", "count": 2}


@pytest.mark.anyio
async def test_delete_distribution(monkeypatch):
    captured = {}

    async def fake_delete(**kwargs):
        captured["data"] = kwargs["data_list"]
        return {"count": 1}

    monkeypatch.setattr(svc, "delete_records_async", fake_delete)
    await svc.delete_distribution([{"CustomerNum": "1", "EMailAddress": "a"}])
    assert captured["data"][0]["CustomerNum"] == "1"


@pytest.mark.anyio
async def test_upsert_distribution_validates_required_fields(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("should not merge")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [
        {
            "CustomerNum": "1",
            "RecipCat": "",
            "DistVia": "Email",
            "AttnTo": "Name",
            "EMailAddress": "test@example.com",
        }
    ]

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_distribution(payload)

    assert exc.value.status_code == 422
