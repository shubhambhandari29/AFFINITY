import asyncio
from datetime import date, datetime

from services.hcm import hcm_account_service


def test_get_hcm_account_formats_date_fields(monkeypatch):
    async def fake_fetch_records_async(*, table, filters):
        assert table == "tblHcmAccount"
        assert filters == {"Stage": "Admin", "IsSubmitted": "0"}
        return [
            {
                "CustomerNum": "123",
                "OnBoardDate": datetime(2026, 8, 3, 12, 30),
                "EffectiveDate": date(2026, 8, 4),
                "CustomerName": "Example account",
            }
        ]

    monkeypatch.setattr(
        hcm_account_service, "fetch_records_async", fake_fetch_records_async
    )

    result = asyncio.run(
        hcm_account_service.get_hcm_account({"Stage": "Admin", "IsSubmitted": "0"})
    )

    assert result == [
        {
            "CustomerNum": "123",
            "OnBoardDate": "08-03-2026",
            "EffectiveDate": "08-04-2026",
            "CustomerName": "Example account",
        }
    ]


def test_upsert_hcm_account_uses_shared_date_normalization(monkeypatch):
    payload = {
        "CustomerNum": "123",
        "DateCreated": "2026-08-03",
        "OnBoardDate": "2026-08-03",
        "EffectiveDate": "2026-08-04",
        "DiscDate": "2026-08-05",
        "TermDate": "",
    }

    captured = {}

    async def fake_merge_upsert_records_async(*, table, data_list, key_columns, **kwargs):
        captured["data"] = data_list[0]
        return {"count": len(data_list)}

    monkeypatch.setattr(
        hcm_account_service,
        "merge_upsert_records_async",
        fake_merge_upsert_records_async,
    )

    result = asyncio.run(hcm_account_service.upsert_hcm_account(payload))

    assert result == {"count": 1}
    assert captured["data"]["DateCreated"] == date(2026, 8, 3)
    assert captured["data"]["OnBoardDate"] == date(2026, 8, 3)
    assert captured["data"]["EffectiveDate"] == date(2026, 8, 4)
    assert captured["data"]["DiscDate"] == date(2026, 8, 5)
    assert captured["data"]["TermDate"] is None
