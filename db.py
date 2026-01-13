# db.py

import warnings
from contextlib import contextmanager

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


# New Connection Getter
def get_raw_connection() -> pyodbc.Connection:
    """
    Returns a NEW pyodbc connection.
    Services or context managers will use this.
    """
    conn_str = _build_connection_string()
    return pyodbc.connect(conn_str)


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