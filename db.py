# db.py

import struct
import warnings
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import pyodbc
from azure.identity import AzureCliCredential, DefaultAzureCredential

from core.config import settings

warnings.filterwarnings(
    "ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable"
)

SQL_COPT_SS_ACCESS_TOKEN = 1256
TOKEN_AUTH_MODES = {
    "azurecli",
    "azureclicredential",
    "defaultazurecredential",
}


# Build SQL connection string
def _build_connection_string() -> str:
    authentication = str(settings.DB_AUTH or "").strip()
    auth_section = ""
    if authentication and authentication.casefold() not in TOKEN_AUTH_MODES:
        auth_section = f"Authentication={authentication};"

    return (
        f"Driver={settings.DB_DRIVER};"
        f"Server={settings.DB_SERVER};"
        f"Database={settings.DB_NAME};"
        f"{auth_section}"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )


def _normalized_auth_mode() -> str:
    return str(settings.DB_AUTH or "").strip().casefold()


def _uses_access_token_auth() -> bool:
    return _normalized_auth_mode() in TOKEN_AUTH_MODES


@lru_cache(maxsize=4)
def _get_access_token_credential(auth_mode: str):
    if auth_mode in {"azurecli", "azureclicredential"}:
        return AzureCliCredential()

    return DefaultAzureCredential(exclude_interactive_browser_credential=True)


def _get_access_token_bytes() -> bytes:
    credential = _get_access_token_credential(_normalized_auth_mode())
    access_token = credential.get_token(settings.AZURE_SQL_TOKEN_SCOPE)
    token_bytes = access_token.token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


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
    connect_kwargs = {}
    if _uses_access_token_auth():
        connect_kwargs["attrs_before"] = {
            SQL_COPT_SS_ACCESS_TOKEN: _get_access_token_bytes()
        }

    conn = pyodbc.connect(conn_str, **connect_kwargs)
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
