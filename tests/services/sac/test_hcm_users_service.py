from datetime import date

import pytest
from fastapi import HTTPException

from services.sac import hcm_users_service as svc


@pytest.mark.anyio
async def test_get_hcm_users_remaps_filters(monkeypatch):
    def fake_sanitize(params, allowed=None):
        assert params == {"CustNum": "C1"}
        return params

    async def fake_fetch(table, filters):
        assert filters == {"CustNum": "C1"}
        return [{"CustNum": "C1", "UserID": "u1", "AccessDate": "2024-05-01"}]

    monkeypatch.setattr(svc, "sanitize_filters", fake_sanitize)
    monkeypatch.setattr(svc, "fetch_records_async", fake_fetch)

    result = await svc.get_hcm_users({"CustomerNum": "C1"})
    assert result == [{"CustomerNum": "C1", "UserID": "u1", "AccessDate": "01-05-2024"}]


@pytest.mark.anyio
async def test_upsert_hcm_users_splits_payload(monkeypatch):
    calls = {"merge": None, "insert": None}

    async def fake_merge(**kwargs):
        calls["merge"] = kwargs["data_list"]

    async def fake_insert(**kwargs):
        calls["insert"] = kwargs["records"]

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)
    monkeypatch.setattr(svc, "insert_records_async", fake_insert)

    payload = [
        {
            "PK_Number": 1,
            "CustomerNum": "1",
            "UserID": "u1",
            "AccessDate": "01-05-2024",
            "UserName": "A",
            "UserTitle": "T1",
            "UserEmail": "a@example.com",
            "UserAction": "Add",
            "LanID": "lan1",
        },
        {
            "CustomerNum": "2",
            "UserID": "u2",
            "AccessDate": "02-05-2024",
            "UserName": "B",
            "UserTitle": "T2",
            "UserEmail": "b@example.com",
            "UserAction": "Add",
            "LanID": "lan2",
        },
    ]

    result = await svc.upsert_hcm_users(payload)
    assert result == {"message": "Transaction successful", "count": 2}

    # Remapped records should convert CustomerNum -> CustNum
    assert calls["merge"][0]["CustNum"] == "1"
    assert calls["insert"][0]["CustNum"] == "2"
    assert calls["merge"][0]["AccessDate"] == date(2024, 5, 1)
    assert calls["insert"][0]["AccessDate"] == date(2024, 5, 2)


@pytest.mark.anyio
async def test_upsert_hcm_users_validates_required_fields(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("should not merge")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [
        {
            "CustomerNum": "1",
            "UserName": "",
            "UserTitle": "Title",
            "UserEmail": "test@example.com",
            "UserAction": "Add",
            "LanID": "",
        }
    ]

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_hcm_users(payload)

    assert exc.value.status_code == 422
    assert exc.value.detail["errors"][0]["field"] in {"UserName", "LanID"}


@pytest.mark.anyio
async def test_upsert_hcm_users_validates_telephone(monkeypatch):
    async def fake_merge(**kwargs):
        raise AssertionError("should not merge")

    monkeypatch.setattr(svc, "merge_upsert_records_async", fake_merge)

    payload = [
        {
            "CustomerNum": "1",
            "UserName": "Name",
            "UserTitle": "Title",
            "UserEmail": "test@example.com",
            "UserAction": "Add",
            "LanID": "lan",
            "TelNum": "12345",
        }
    ]

    with pytest.raises(HTTPException) as exc:
        await svc.upsert_hcm_users(payload)

    assert exc.value.status_code == 422
    assert exc.value.detail["errors"][0]["field"] == "TelNum"
