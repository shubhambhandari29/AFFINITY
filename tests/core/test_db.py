from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone

import pytest

import db


def test_build_connection_string_uses_settings(monkeypatch):
    class DummySettings:
        DB_DRIVER = "{Driver}"
        DB_SERVER = "server"
        DB_NAME = "db"
        DB_AUTH = "auth"

    monkeypatch.setattr(db, "settings", DummySettings)
    conn_str = db._build_connection_string()
    assert "Driver={Driver};" in conn_str
    assert "Server=server;" in conn_str
    assert "Database=db;" in conn_str
    assert "Authentication=auth;" in conn_str
    assert conn_str.endswith("Encrypt=yes;TrustServerCertificate=no;")


def test_handle_datetimeoffset_parses_20_byte_payload():
    dto_value = struct.pack("<6hI2h", 2024, 1, 2, 3, 4, 5, 1234000, 2, 30)
    result = db._handle_datetimeoffset(memoryview(dto_value))
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 2
    assert result.microsecond == 1234
    assert result.tzinfo.utcoffset(result) == timedelta(hours=2, minutes=30)


def test_handle_datetimeoffset_parses_offset_minutes_payload():
    dto_value = struct.pack("<6hIh", 2024, 2, 3, 4, 5, 6, 2000, -300)
    result = db._handle_datetimeoffset(dto_value)
    assert isinstance(result, datetime)
    assert result.tzinfo.utcoffset(result) == timedelta(minutes=-300)


def test_handle_datetimeoffset_returns_none_on_invalid_payload():
    assert db._handle_datetimeoffset(None) is None
    assert db._handle_datetimeoffset(b"bad") is None


def test_get_raw_connection_adds_output_converter(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.converter_args = None

        def add_output_converter(self, dtype, func):
            self.converter_args = (dtype, func)

    class FakePyodbc:
        SQL_SS_TIMESTAMPOFFSET = 123

        def __init__(self):
            self.connect_arg = None
            self.conn = FakeConn()

        def connect(self, conn_str):
            self.connect_arg = conn_str
            return self.conn

    fake_pyodbc = FakePyodbc()

    monkeypatch.setattr(db, "pyodbc", fake_pyodbc)
    monkeypatch.setattr(db, "_build_connection_string", lambda: "conn-str")

    conn = db.get_raw_connection()
    assert conn is fake_pyodbc.conn
    assert fake_pyodbc.connect_arg == "conn-str"
    assert fake_pyodbc.conn.converter_args == (
        123,
        db._handle_datetimeoffset,
    )


def test_get_raw_connection_ignores_converter_errors(monkeypatch):
    class FakeConn:
        def add_output_converter(self, dtype, func):
            raise RuntimeError("no converter")

    class FakePyodbc:
        SQL_SS_TIMESTAMPOFFSET = 123

        def connect(self, conn_str):
            return FakeConn()

    monkeypatch.setattr(db, "pyodbc", FakePyodbc())
    monkeypatch.setattr(db, "_build_connection_string", lambda: "conn-str")

    conn = db.get_raw_connection()
    assert isinstance(conn, FakeConn)


def test_db_connection_context_manager_closes_connection(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_conn = FakeConn()
    monkeypatch.setattr(db, "get_raw_connection", lambda: fake_conn)

    with db.db_connection() as conn:
        assert conn is fake_conn
        assert fake_conn.closed is False

    assert fake_conn.closed is True
