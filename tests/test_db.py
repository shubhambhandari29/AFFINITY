from contextlib import contextmanager

import db


def test_build_connection_string(monkeypatch):
    class DummySettings:
        DB_DRIVER = "ODBC Driver 18 for SQL Server"
        DB_SERVER = "server"
        DB_NAME = "database"
        DB_AUTH = "ActiveDirectoryInteractive"
        USE_KEY_VAULT = False

    monkeypatch.setattr(db, "settings", DummySettings)
    conn_str = db._build_connection_string()
    assert "Driver=ODBC Driver 18 for SQL Server;" in conn_str
    assert "Database=database;" in conn_str


def test_db_connection_context(monkeypatch):
    created = {}

    class DummyConnection:
        def __init__(self):
            created["conn"] = self
            self.closed = False

        def close(self):
            self.closed = True

    monkeypatch.setattr(db, "get_raw_connection", lambda: DummyConnection())

    with db.db_connection() as conn:
        assert conn is created["conn"]

    assert created["conn"].closed is True
