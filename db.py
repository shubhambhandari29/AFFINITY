# db.py

import struct
import warnings
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pyodbc

from core.config import settings

warnings.filterwarnings(
    "ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable"
)


# Build SQL connection string
def _build_connection_string() -> str:
    return (
        f"Driver={settings.DB_DRIVER};"
        f"Server={settings.DB_SERVER};"
        f"Database={settings.DB_NAME};"
        f"Authentication={settings.DB_AUTH};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )


def _handle_datetimeoffset(dto_value: bytes | bytearray | memoryview | None) -> datetime | None:
    if dto_value is None:
        return None

    if isinstance(dto_value, memoryview):
        dto_value = dto_value.tobytes()

    try:
        if len(dto_value) == 20:
            year, month, day, hour, minute, second, fraction, tz_hour, tz_minute = struct.unpack(
                "<6hI2h", dto_value
            )
            tzinfo = timezone(timedelta(hours=tz_hour, minutes=tz_minute))
        else:
            year, month, day, hour, minute, second, fraction, offset_minutes = struct.unpack(
                "<6hIh", dto_value
            )
            tzinfo = timezone(timedelta(minutes=offset_minutes))
    except struct.error:
        return None

    microseconds = fraction // 1000
    return datetime(year, month, day, hour, minute, second, microseconds, tzinfo=tzinfo)


# New Connection Getter
def get_raw_connection() -> pyodbc.Connection:
    """
    Returns a NEW pyodbc connection.
    Services or context managers will use this.
    """
    conn_str = _build_connection_string()
    conn = pyodbc.connect(conn_str)
    datetimeoffset_type = getattr(pyodbc, "SQL_SS_TIMESTAMPOFFSET", -155)
    try:
        conn.add_output_converter(datetimeoffset_type, _handle_datetimeoffset)
    except Exception:
        # If the driver doesn't support output converters, fall back to default behavior.
        pass
    return conn


# Context Manager
@contextmanager
def db_connection():
    """
    Usage:
        with db_connection() as conn:
            cursor = conn.cursor()
            ...
    """
    conn = get_raw_connection()
    try:
        yield conn
    finally:
        conn.close()
