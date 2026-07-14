from __future__ import annotations

import asyncio
from contextlib import contextmanager

import pandas as pd
import pytest

from core import db_helpers


def test_ensure_safe_identifier_rejects_invalid():
    with pytest.raises(ValueError):
        db_helpers._ensure_safe_identifier("bad-name")


def test_sanitize_filters_allows_and_blocks_fields():
    result = db_helpers.sanitize_filters({"Name": "A"}, allowed_fields={"Name"})
    assert result == {"Name": "A"}

    with pytest.raises(ValueError) as excinfo:
        db_helpers.sanitize_filters({"Bad": "x"}, allowed_fields={"Name"})
    assert "Invalid filter field(s): Bad" in str(excinfo.value)


def test_sanitize_filters_rejects_invalid_identifier():
    with pytest.raises(ValueError):
        db_helpers.sanitize_filters({"Bad-Name": "x"})


def test_build_select_query_with_filters_and_order():
    query, params = db_helpers.build_select_query(
        "MyTable", {"A": 1, "B": 2}, order_by="A"
    )
    assert query == "SELECT * FROM MyTable WHERE A = ? AND B = ? ORDER BY A"
    assert params == [1, 2]


def test_build_select_query_without_filters():
    query, params = db_helpers.build_select_query("MyTable")
    assert query == "SELECT * FROM MyTable"
    assert params == []


def test_fetch_records_converts_nan_to_none(monkeypatch):
    captured = {}

    @contextmanager
    def fake_db_connection():
        yield object()

    def fake_read_sql(query, conn, params=None):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame([{"A": 1, "B": None}, {"A": 2, "B": float("nan")}])

    monkeypatch.setattr(db_helpers, "db_connection", fake_db_connection)
    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = db_helpers.fetch_records("MyTable", {"A": 1}, order_by="A")
    assert captured["query"] == "SELECT * FROM MyTable WHERE A = ? ORDER BY A"
    assert captured["params"] == [1]
    assert result == [{"A": 1, "B": None}, {"A": 2, "B": None}]


def test_run_raw_query_uses_params(monkeypatch):
    captured = {}

    @contextmanager
    def fake_db_connection():
        yield object()

    def fake_read_sql(query, conn, params=None):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame([{"A": 1}])

    monkeypatch.setattr(db_helpers, "db_connection", fake_db_connection)
    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    result = db_helpers.run_raw_query("SELECT 1", [10])
    assert captured["query"] == "SELECT 1"
    assert captured["params"] == [10]
    assert result == [{"A": 1}]


def test_merge_upsert_records_builds_merge_query(monkeypatch):
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

    import db

    monkeypatch.setattr(db, "db_connection", fake_db_connection)

    result = db_helpers.merge_upsert_records(
        "MyTable",
        data_list=[{"id": 1, "name": "Alice"}],
        key_columns=["id"],
    )

    assert result == {"message": "Transaction successful", "count": 1}
    assert executed
    query, values = executed[0]
    assert "MERGE INTO MyTable AS target" in query
    assert "ON target.id = source.id" in query
    assert "INSERT (id, name)" in query
    assert values == [1, "Alice"]


def test_merge_upsert_records_excludes_key_columns_when_requested(monkeypatch):
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

    import db

    monkeypatch.setattr(db, "db_connection", fake_db_connection)

    db_helpers.merge_upsert_records(
        "MyTable",
        data_list=[{"id": 1, "name": "Alice"}],
        key_columns=["id"],
        exclude_key_columns_from_insert=True,
    )

    query, _ = executed[0]
    assert "INSERT (name)" in query


def test_insert_records_builds_insert_query(monkeypatch):
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

    import db

    monkeypatch.setattr(db, "db_connection", fake_db_connection)

    result = db_helpers.insert_records("MyTable", [{"id": 1, "name": "Bob"}])
    assert result == {"message": "Insertion successful", "count": 1}
    query, values = executed[0]
    assert query == "INSERT INTO MyTable (id, name) VALUES (?, ?)"
    assert values == [1, "Bob"]


def test_delete_records_builds_delete_query(monkeypatch):
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

    import db

    monkeypatch.setattr(db, "db_connection", fake_db_connection)

    result = db_helpers.delete_records(
        "MyTable",
        data_list=[{"id": 1, "name": "Bob"}],
        key_columns=["id", "name"],
    )

    assert result == {"message": "Deletion successful", "count": 1}
    query, values = executed[0]
    assert query == "DELETE FROM MyTable WHERE id = ? AND name = ?"
    assert values == [1, "Bob"]


def test_fetch_records_async_uses_threadpool(monkeypatch):
    async def fake_run_in_threadpool(func):
        return func()

    monkeypatch.setattr(db_helpers, "run_in_threadpool", fake_run_in_threadpool)
    monkeypatch.setattr(db_helpers, "fetch_records", lambda **kwargs: [{"ok": True}])

    result = asyncio.run(db_helpers.fetch_records_async("MyTable"))
    assert result == [{"ok": True}]
