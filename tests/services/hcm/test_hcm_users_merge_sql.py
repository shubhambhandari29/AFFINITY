import asyncio
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from core import db_helpers


@pytest.fixture
def database(monkeypatch):
    import db

    conn = MagicMock()
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = (1,)

    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(db, "db_connection", connection)
    monkeypatch.setattr(db_helpers, "_table_has_update_datetime", lambda *args: True)
    return conn, cursor


def test_update_only_sql_cannot_insert_or_overwrite_identity_and_creation_date(
    database,
):
    conn, cursor = database
    row = {"PK_Number": 9, "UserName": "O'Brien; --", "InsertDateTime": "old"}
    db_helpers.merge_upsert_records(
        "tblHcmOnlyUsers",
        [row],
        ["PK_Number"],
        exclude_key_columns_from_insert=True,
        require_match=True,
    )
    sql, values = cursor.execute.call_args.args
    assert "target.PK_Number = source.PK_Number" in sql
    assert "WHEN NOT MATCHED" not in sql
    assert "INSERT (" not in sql
    assert "PK_Number = source.PK_Number" not in sql.split("UPDATE SET")[1]
    assert "InsertDateTime = source.InsertDateTime" not in sql
    assert "UpdateDateTime = source.UpdateDateTime" in sql
    assert "SELECT @@ROWCOUNT" in sql
    assert row["UserName"] not in sql
    assert row["UserName"] in values
    assert row["InsertDateTime"] == "old"
    conn.commit.assert_called_once()


def test_missing_id_rolls_back_entire_id_batch(database):
    conn, cursor = database
    cursor.fetchone.side_effect = [(1,), (0,)]
    with pytest.raises(LookupError):
        db_helpers.merge_upsert_records(
            "tblHcmOnlyUsers",
            [
                {"PK_Number": 9, "UserName": "Changed"},
                {"PK_Number": 999, "UserName": "Missing"},
            ],
            ["PK_Number"],
            exclude_key_columns_from_insert=True,
            require_match=True,
        )
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_default_merge_still_inserts(database):
    conn, cursor = database
    db_helpers.merge_upsert_records(
        "tblHcmOnlyUsers",
        [{"CustNum": "123", "UserName": "New"}],
        ["CustNum", "UserName"],
    )
    sql = cursor.execute.call_args.args[0]
    assert "WHEN NOT MATCHED" in sql
    assert "INSERT (" in sql
    conn.commit.assert_called_once()


def test_database_error_rolls_back(database):
    conn, cursor = database
    cursor.execute.side_effect = RuntimeError("database error")
    with pytest.raises(RuntimeError):
        db_helpers.merge_upsert_records(
            "tblHcmOnlyUsers",
            [{"PK_Number": 9, "UserName": "Changed"}],
            ["PK_Number"],
            require_match=True,
        )
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_async_wrapper_passes_update_only_option(database):
    conn, cursor = database
    asyncio.run(
        db_helpers.merge_upsert_records_async(
            "tblHcmOnlyUsers",
            [{"PK_Number": 9, "UserName": "Changed"}],
            ["PK_Number"],
            require_match=True,
        )
    )
    assert "WHEN NOT MATCHED" not in cursor.execute.call_args.args[0]
    conn.commit.assert_called_once()


def test_id_only_update_uses_server_audit_timestamp(database):
    conn, cursor = database
    db_helpers.merge_upsert_records(
        "tblHcmOnlyUsers",
        [{"PK_Number": 9}],
        ["PK_Number"],
        exclude_key_columns_from_insert=True,
        require_match=True,
    )
    sql = cursor.execute.call_args.args[0]
    assert "UPDATE SET UpdateDateTime = source.UpdateDateTime" in sql
    assert "WHEN NOT MATCHED" not in sql
    conn.commit.assert_called_once()


def test_unsafe_column_rolls_back_before_sql(database):
    conn, cursor = database
    with pytest.raises(ValueError, match="Invalid column"):
        db_helpers.merge_upsert_records(
            "tblHcmOnlyUsers",
            [{"PK_Number": 9, "UserName; DROP TABLE x": "bad"}],
            ["PK_Number"],
            require_match=True,
        )
    cursor.execute.assert_not_called()
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_odbc_rowcount_result_is_skipped_before_fetch(database):
    conn, cursor = database
    cursor.description = None

    def nextset():
        cursor.description = [("affected_rows",)]
        return True

    cursor.nextset.side_effect = nextset
    db_helpers.merge_upsert_records(
        "tblHcmOnlyUsers",
        [{"PK_Number": 9, "UserName": "Changed"}],
        ["PK_Number"],
        require_match=True,
    )
    cursor.nextset.assert_called_once()
    cursor.fetchone.assert_called_once()
    conn.commit.assert_called_once()
