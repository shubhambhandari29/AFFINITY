from contextlib import contextmanager

import pandas as pd
import pytest

import db as db_module
from core import db_helpers


class DummyCursor:
    def __init__(self):
        self.queries: list[tuple[str, list]] = []

    def execute(self, query: str, params: list):
        self.queries.append((query.strip(), list(params)))


class DummyConnection:
    def __init__(self):
        self.cursor_obj = DummyCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def fake_db(monkeypatch):
    connections: list[DummyConnection] = []

    @contextmanager
    def _db_connection():
        conn = DummyConnection()
        connections.append(conn)
        yield conn

    monkeypatch.setattr(db_module, "db_connection", _db_connection)
    monkeypatch.setattr(db_helpers, "db_connection", _db_connection)
    return connections


def test_sanitize_filters_with_allowlist():
    filters = {"CustomerNum": "1", "BadField": "x"}
    with pytest.raises(ValueError) as exc:
        db_helpers.sanitize_filters(filters, {"CustomerNum"})

    assert "Invalid filter field(s): BadField" in str(exc.value)

    result = db_helpers.sanitize_filters({"CustomerNum": "1"}, {"CustomerNum"})
    assert result == {"CustomerNum": "1"}


def test_build_select_query_with_filters():
    query, params = db_helpers.build_select_query(
        "tblTest", {"CustomerNum": "1", "Stage": "Active"}, order_by="CustomerNum"
    )

    assert query == "SELECT * FROM tblTest WHERE CustomerNum = ? AND Stage = ? ORDER BY CustomerNum"
    assert params == ["1", "Active"]


def test_fetch_records(monkeypatch, fake_db):
    captured = {}

    def fake_read_sql(query, conn, params):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame([{"CustomerNum": "1", "Stage": "Active"}])

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)
    result = db_helpers.fetch_records("tblTest", {"CustomerNum": "1"})

    assert result == [{"CustomerNum": "1", "Stage": "Active"}]
    assert captured["query"] == "SELECT * FROM tblTest WHERE CustomerNum = ?"
    assert captured["params"] == ["1"]


def test_insert_records_uses_connection(fake_db):
    db_helpers.insert_records("tblTest", [{"CustomerNum": "1", "Stage": "Active"}])

    assert len(fake_db) == 1
    conn = fake_db[0]
    assert conn.committed is True
    executed_query, params = conn.cursor_obj.queries[0]
    assert "INSERT INTO tblTest" in executed_query
    assert params == ["1", "Active"]
