from __future__ import annotations

import asyncio
from contextlib import contextmanager

import pytest
from fastapi import HTTPException

from services import dropdowns_service


def test_quote_identifier_rejects_invalid():
    with pytest.raises(ValueError):
        dropdowns_service._quote_identifier("bad-name")

    with pytest.raises(ValueError):
        dropdowns_service._quote_identifier("bad]")


def test_get_dropdown_definition_default():
    table, primary_key, column_map = dropdowns_service._get_dropdown_definition("Unknown")
    assert table == "tblDropDowns"
    assert primary_key == "DD_Key"
    assert column_map == {"DD_Value": "DD_Value", "DD_SortOrder": "DD_SortOrder"}


def test_normalize_dropdown_rows_valid_and_invalid():
    rows = [{"LANID": "x", "SACName": "A"}]
    normalized = dropdowns_service._normalize_dropdown_rows(
        rows,
        primary_key="LANID",
        column_map={"SACName": "SACName"},
    )
    assert normalized == [{"LANID": "x", "SACName": "A"}]

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_dropdown_rows([], "LANID", {"SACName": "SACName"})

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_dropdown_rows([["bad"]], "LANID", {"SACName": "SACName"})

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_dropdown_rows(
            [{"LANID": "x", "Bad": "y"}], "LANID", {"SACName": "SACName"}
        )


def test_normalize_delete_rows_valid_and_invalid():
    normalized = dropdowns_service._normalize_delete_rows(
        [{"LANID": "x"}], primary_key="LANID", allowed_columns={"SACName"}
    )
    assert normalized == [{"LANID": "x"}]

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_delete_rows([], "LANID", {"SACName"})

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_delete_rows([["bad"]], "LANID", {"SACName"})

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_delete_rows([{"SACName": "x"}], "LANID", {"SACName"})

    with pytest.raises(HTTPException):
        dropdowns_service._normalize_delete_rows([{"LANID": ""}], "LANID", {"SACName"})


def test_normalize_query_definition():
    query, params = dropdowns_service._normalize_query_definition("SELECT 1")
    assert query == "SELECT 1"
    assert params == []

    query, params = dropdowns_service._normalize_query_definition(("SELECT 2", ["x"]))
    assert query == "SELECT 2"
    assert params == ["x"]


def test_get_dropdown_values_all_and_query_and_dynamic(monkeypatch):
    async def fake_get_all():
        return [{"all": True}]

    async def fake_run_raw_query_async(query, params):
        return [{"query": query, "params": params}]

    async def fake_fetch_dynamic(name):
        return [{"dynamic": name}]

    monkeypatch.setattr(dropdowns_service, "get_all_dropdowns", fake_get_all)
    monkeypatch.setattr(dropdowns_service, "run_raw_query_async", fake_run_raw_query_async)
    monkeypatch.setattr(dropdowns_service, "_fetch_dynamic_dropdown", fake_fetch_dynamic)

    result = asyncio.run(dropdowns_service.get_dropdown_values("all"))
    assert result == [{"all": True}]

    result = asyncio.run(dropdowns_service.get_dropdown_values("LossCtlRep2"))
    assert result[0]["params"] == ["Yes"]

    result = asyncio.run(dropdowns_service.get_dropdown_values("Unknown"))
    assert result == [{"dynamic": "Unknown"}]


def test_get_dropdown_values_requires_name():
    with pytest.raises(HTTPException):
        asyncio.run(dropdowns_service.get_dropdown_values("  "))


def test_fetch_dynamic_dropdown_error(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(dropdowns_service, "run_raw_query_async", fake_run_raw_query_async)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(dropdowns_service._fetch_dynamic_dropdown("Test"))

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}


def test_get_all_dropdowns_error(monkeypatch):
    async def fake_run_raw_query_async(query, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(dropdowns_service, "run_raw_query_async", fake_run_raw_query_async)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(dropdowns_service.get_all_dropdowns())

    assert excinfo.value.status_code == 500
    assert excinfo.value.detail == {"error": "db down"}


def test_upsert_dropdown_values_splits_rows(monkeypatch):
    captured = {"merge": None, "insert": None}

    monkeypatch.setattr(
        dropdowns_service,
        "_get_dropdown_definition",
        lambda name: ("tblDropDowns", "DD_Key", {"DD_Value": "DD_Value"}),
    )
    monkeypatch.setattr(
        dropdowns_service,
        "_normalize_dropdown_rows",
        lambda rows, pk, cmap: [
            {"DD_Key": 1, "DD_Value": "A"},
            {"DD_Key": None, "DD_Value": "B"},
        ],
    )

    async def fake_merge(*, table, data_list, key_columns, exclude_key_columns_from_insert):
        captured["merge"] = (table, data_list, key_columns, exclude_key_columns_from_insert)
        return {"count": len(data_list)}

    async def fake_insert(*, table, records):
        captured["insert"] = (table, records)
        return {"count": len(records)}

    monkeypatch.setattr(dropdowns_service, "_merge_upsert_dropdown_records_async", fake_merge)
    monkeypatch.setattr(dropdowns_service, "_insert_dropdown_records_async", fake_insert)

    result = asyncio.run(dropdowns_service.upsert_dropdown_values("TestType", [{"DD_Value": "A"}]))

    assert result == {"message": "Upsert successful", "count": 2}
    assert captured["merge"][1][0]["DD_Type"] == "TestType"
    assert captured["insert"][1][0]["DD_Type"] == "TestType"


def test_upsert_dropdown_values_requires_name():
    with pytest.raises(HTTPException):
        asyncio.run(dropdowns_service.upsert_dropdown_values("  ", []))

    with pytest.raises(HTTPException):
        asyncio.run(dropdowns_service.upsert_dropdown_values("all", []))


def test_delete_dropdown_values_success(monkeypatch):
    monkeypatch.setattr(
        dropdowns_service,
        "_get_dropdown_definition",
        lambda name: ("tblDropDowns", "DD_Key", {"DD_Value": "DD_Value"}),
    )
    monkeypatch.setattr(
        dropdowns_service,
        "_normalize_delete_rows",
        lambda rows, pk, allowed: [{"DD_Key": 1}],
    )

    async def fake_delete(*, table, data_list, key_column):
        return {"count": len(data_list)}

    monkeypatch.setattr(dropdowns_service, "_delete_dropdown_records_async", fake_delete)

    result = asyncio.run(dropdowns_service.delete_dropdown_values("TestType", [{"DD_Key": 1}]))
    assert result == {"count": 1}


def test_delete_dropdown_values_requires_name():
    with pytest.raises(HTTPException):
        asyncio.run(dropdowns_service.delete_dropdown_values("  ", []))

    with pytest.raises(HTTPException):
        asyncio.run(dropdowns_service.delete_dropdown_values("all", []))


def test_merge_upsert_dropdown_records_executes_queries(monkeypatch):
    executed = []

    class FakeCursor:
        def execute(self, query, values):
            executed.append((query, values))

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    @contextmanager
    def fake_db_connection():
        yield FakeConn()

    monkeypatch.setattr(dropdowns_service, "db_connection", fake_db_connection)

    result = dropdowns_service._merge_upsert_dropdown_records(
        "tblDropDowns",
        [{"DD_Key": 1, "DD_Value": "A"}],
        ["DD_Key"],
        exclude_key_columns_from_insert=True,
    )

    assert result == {"message": "Transaction successful", "count": 1}
    assert executed
    query, values = executed[0]
    assert "MERGE INTO [tblDropDowns]" in query
    assert "INSERT ([DD_Value])" in query
    assert values == [1, "A"]


def test_insert_and_delete_dropdown_records(monkeypatch):
    executed = []

    class FakeCursor:
        def execute(self, query, values):
            executed.append((query, values))

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    @contextmanager
    def fake_db_connection():
        yield FakeConn()

    monkeypatch.setattr(dropdowns_service, "db_connection", fake_db_connection)

    insert_result = dropdowns_service._insert_dropdown_records(
        "tblDropDowns", [{"DD_Value": "A"}, {}]
    )
    assert insert_result == {"message": "Insertion successful", "count": 2}
    assert "INSERT INTO [tblDropDowns]" in executed[0][0]

    delete_result = dropdowns_service._delete_dropdown_records(
        "tblDropDowns", [{"DD_Key": 1}], "DD_Key"
    )
    assert delete_result == {"message": "Deletion successful", "count": 1}
    assert "DELETE FROM [tblDropDowns]" in executed[-1][0]
