import asyncio
from contextlib import nullcontext
from datetime import datetime
from unittest.mock import MagicMock

import pytest

import db
from core import db_helpers as helpers


@pytest.fixture
def database(monkeypatch):
    connection = MagicMock()
    monkeypatch.setattr(db, "db_connection", lambda: nullcontext(connection))
    monkeypatch.setattr(
        helpers, "add_update_datetime_if_supported", lambda cursor, table, rows, **kw: rows
    )
    return connection


@pytest.mark.parametrize(
    "method,args",
    [
        ("merge_upsert_records", ("Policies", [], ["id"])),
        ("insert_records", ("Policies", [])),
        ("delete_records", ("Policies", [], ["id"])),
        ("update_records", ("Policies", [])),
    ],
)
def test_empty_writes_do_not_open_database(database, method, args):
    assert getattr(helpers, method)(*args)["count"] == 0
    database.cursor.assert_not_called()
    database.commit.assert_not_called()


@pytest.mark.parametrize(
    "method,args",
    [
        ("merge_upsert_records_async", ("Policies", [{"id": 1, "Name": "A"}], ["id"])),
        ("delete_records_async", ("Policies", [{"id": 1}], ["id"])),
        (
            "update_records_async",
            (
                "Policies",
                [{"fieldName": "Name", "fieldValue": "A", "updateVia": "id", "updateViaValue": 1}],
            ),
        ),
    ],
)
@pytest.mark.parametrize("rollback_fails", [False, True])
def test_write_errors_roll_back_and_preserve_original_failure(
    database, method, args, rollback_fails
):
    failure = RuntimeError("write failed")
    database.cursor.return_value.execute.side_effect = failure
    if rollback_fails:
        database.rollback.side_effect = RuntimeError("rollback failed")
    with pytest.raises(RuntimeError, match="write failed") as error:
        asyncio.run(getattr(helpers, method)(*args))
    assert error.value is failure
    database.rollback.assert_called_once()
    database.commit.assert_not_called()


def test_insert_failure_propagates_without_commit(database):
    database.cursor.return_value.execute.side_effect = RuntimeError("insert failed")
    with pytest.raises(RuntimeError, match="insert failed"):
        asyncio.run(helpers.insert_records_async("Policies", [{"id": 1}]))
    database.commit.assert_not_called()


@pytest.mark.parametrize("rowcount,expected", [(2, 4), (0, 0), (-1, 0), (None, 0)])
def test_bulk_update_counts_only_affected_rows_and_binds_values(database, rowcount, expected):
    cursor = database.cursor.return_value
    cursor.rowcount = rowcount
    updates = [
        {"fieldName": "Name", "fieldValue": "O'Brien", "updateVia": "id", "updateViaValue": 1},
        {"fieldName": "Name", "fieldValue": None, "updateVia": "id", "updateViaValue": 2},
    ]
    assert asyncio.run(helpers.update_records_async("Policies", updates)) == {
        "message": "Update successful",
        "count": expected,
    }
    assert [call.args[1] for call in cursor.execute.call_args_list] == [("O'Brien", 1), (None, 2)]
    assert "O'Brien" not in cursor.execute.call_args_list[0].args[0]
    database.commit.assert_called_once()


def test_delete_missing_key_rolls_back_before_execution(database):
    with pytest.raises(ValueError, match="id is required"):
        helpers.delete_records("Policies", [{"Name": "A"}], ["id"])
    database.cursor.return_value.execute.assert_not_called()
    database.rollback.assert_called_once()


def test_merge_cannot_insert_only_excluded_identity(database):
    with pytest.raises(ValueError, match="No columns available"):
        helpers.merge_upsert_records(
            "Policies", [{"id": 1}], ["id"], exclude_key_columns_from_insert=True
        )
    database.rollback.assert_called_once()
    database.commit.assert_not_called()


def test_insert_skips_empty_records(database):
    helpers.insert_records("Policies", [{}, {"id": 1}])
    database.cursor.return_value.execute.assert_called_once_with(
        "INSERT INTO Policies (id) VALUES (?)", [1]
    )


def test_audit_timestamp_is_utc_with_seven_fractional_digits():
    value = helpers._utc_datetimeoffset_string()
    date, time, offset = value.split()
    assert offset == "+00:00"
    assert len(time.split(".")[1]) == 7
    parsed = datetime.fromisoformat(value)
    assert parsed.utcoffset().total_seconds() == 0
