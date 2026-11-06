import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(params=["hcm", "sac"])
def service(request):
    return importlib.import_module(f"services.{request.param}.hcm_users_service")


def test_rename_updates_existing_row_by_primary_key(service, monkeypatch):
    rows = {9: {"PK_Number": 9, "CustNum": "0273428365", "UserName": "Test Test"}}

    async def merge(
        *,
        table,
        data_list,
        key_columns,
        exclude_key_columns_from_insert=False,
        require_match=False,
    ):
        assert table == service.TABLE_NAME
        assert key_columns == ["PK_Number"]
        assert exclude_key_columns_from_insert is True
        assert require_match is True
        for record in data_list:
            rows[record["PK_Number"]].update(record)

    insert = AsyncMock()
    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    monkeypatch.setattr(service, "insert_records_async", insert)
    payload = [
        {"PK_Number": 9, "CustomerNum": "0273428365", "UserName": "Testgg Test ggg"}
    ]

    result = asyncio.run(service.upsert_hcm_users(payload))

    assert len(rows) == 1
    assert rows[9]["UserName"] == "Testgg Test ggg"
    assert rows[9]["CustNum"] == "0273428365"
    assert "CustomerNum" not in rows[9]
    assert payload[0]["CustomerNum"] == "0273428365"
    insert.assert_not_awaited()
    assert result["count"] == 1


def test_mixed_batch_keeps_legacy_matching_for_records_without_id(service, monkeypatch):
    merge = AsyncMock()
    insert = AsyncMock()
    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    monkeypatch.setattr(service, "insert_records_async", insert)

    asyncio.run(
        service.upsert_hcm_users(
            [
                {"PK_Number": 9, "UserName": "Renamed"},
                {"PK_Number": None, "CustomerNum": "123", "UserName": "New User"},
                {"PK_Number": None, "UserEmail": "new@example.com"},
            ]
        )
    )

    assert merge.await_count == 2
    assert merge.await_args_list[0].kwargs["key_columns"] == ["PK_Number"]
    assert merge.await_args_list[0].kwargs["data_list"] == [
        {"PK_Number": 9, "UserName": "Renamed"}
    ]
    assert merge.await_args_list[1].kwargs["key_columns"] == ["CustNum", "UserName"]
    assert merge.await_args_list[1].kwargs["data_list"] == [
        {"CustNum": "123", "UserName": "New User"}
    ]
    insert.assert_awaited_once_with(
        table=service.TABLE_NAME, records=[{"UserEmail": "new@example.com"}]
    )


@pytest.mark.parametrize("pk", [0, -1, True, False, 1.5, "9", "invalid"])
def test_invalid_ids_fail_before_any_writes(service, monkeypatch, pk):
    from fastapi import HTTPException

    merge, insert = AsyncMock(), AsyncMock()
    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    monkeypatch.setattr(service, "insert_records_async", insert)
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            service.upsert_hcm_users(
                [
                    {"PK_Number": 9, "UserName": "Valid first"},
                    {"PK_Number": pk, "UserName": "Invalid"},
                ]
            )
        )
    assert error.value.status_code == 400
    merge.assert_not_awaited()
    insert.assert_not_awaited()


@pytest.mark.parametrize(
    "failure,status",
    [(LookupError("missing"), 404), (RuntimeError("database unavailable"), 500)],
)
def test_update_failure_never_falls_back_to_insert(
    service, monkeypatch, failure, status
):
    from fastapi import HTTPException

    merge, insert = AsyncMock(side_effect=failure), AsyncMock()
    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    monkeypatch.setattr(service, "insert_records_async", insert)
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            service.upsert_hcm_users(
                [
                    {"PK_Number": 999, "UserName": "Renamed"},
                    {"UserName": "New", "CustomerNum": "123"},
                    {"UserEmail": "new@example.com"},
                ]
            )
        )
    assert error.value.status_code == status
    assert merge.await_count == 1
    assert merge.await_args.kwargs["require_match"] is True
    insert.assert_not_awaited()


def test_empty_batch_does_not_write(service, monkeypatch):
    merge, insert = AsyncMock(), AsyncMock()
    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    monkeypatch.setattr(service, "insert_records_async", insert)
    assert asyncio.run(service.upsert_hcm_users([]))["count"] == 0
    merge.assert_not_awaited()
    insert.assert_not_awaited()


@pytest.mark.parametrize("pk", [None, ""])
def test_absent_id_is_not_sent_to_identity_insert(service, monkeypatch, pk):
    merge, insert = AsyncMock(), AsyncMock()
    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    monkeypatch.setattr(service, "insert_records_async", insert)
    asyncio.run(
        service.upsert_hcm_users([{"PK_Number": pk, "UserEmail": "a@example.com"}])
    )
    merge.assert_not_awaited()
    insert.assert_awaited_once_with(
        table=service.TABLE_NAME, records=[{"UserEmail": "a@example.com"}]
    )


@pytest.mark.parametrize("pk", [0, -1, True, False, 1.5, "9", "", "invalid"])
def test_api_model_rejects_invalid_id(pk):
    from pydantic import ValidationError

    from core.models.hcm_users import HCMUserUpsert

    with pytest.raises(ValidationError):
        HCMUserUpsert(PK_Number=pk)


def test_repeated_updates_and_identical_names_keep_ids_separate(service, monkeypatch):
    rows = {9: {"UserName": "Original"}, 21: {"UserName": "Other"}}

    async def merge(*, data_list, **kwargs):
        assert kwargs["key_columns"] == ["PK_Number"]
        assert kwargs["require_match"] is True
        for row in data_list:
            rows[row["PK_Number"]].update(row)

    monkeypatch.setattr(service, "merge_upsert_records_async", merge)
    insert = AsyncMock()
    monkeypatch.setattr(service, "insert_records_async", insert)
    payload = [
        {"PK_Number": 9, "UserName": "Same"},
        {"PK_Number": 21, "UserName": "Same"},
    ]
    asyncio.run(service.upsert_hcm_users(payload))
    asyncio.run(service.upsert_hcm_users(payload))
    assert len(rows) == 2
    assert rows[9]["UserName"] == rows[21]["UserName"] == "Same"
    insert.assert_not_awaited()


@pytest.mark.parametrize("namespace", ["hcm", "sac"])
def test_api_rejects_invalid_id_before_service(namespace, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    module = importlib.import_module(f"api.{namespace}.hcm_users")
    upsert = AsyncMock()
    monkeypatch.setattr(module, "upsert_hcm_users_service", upsert)
    app = FastAPI()
    app.include_router(module.router)
    app.dependency_overrides[module.get_current_user_from_token] = lambda: {
        "user": "test"
    }
    with TestClient(app) as client:
        for pk in [0, -1, True, False, 1.5, "9", "", "invalid"]:
            response = client.post(
                "/upsert", json=[{"PK_Number": pk, "UserName": "Changed"}]
            )
            assert response.status_code == 422
    upsert.assert_not_awaited()
