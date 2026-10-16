import asyncio
from contextlib import nullcontext
from datetime import date
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from services.loss_run import loss_run_job_repository as repository

JOB_ID = UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture
def database(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    monkeypatch.setattr(repository, "db_connection", lambda: nullcontext(connection))
    return connection, cursor


@pytest.mark.parametrize("customers", [None, [], ["001", "002"]])
@pytest.mark.parametrize("cutoff", [None, date(2010, 1, 1)])
def test_create_job_persists_request_and_accounts(database, monkeypatch, customers, cutoff):
    connection, cursor = database
    monkeypatch.setattr(repository, "uuid4", lambda: JOB_ID)
    result = asyncio.run(repository.create_job("selected", "tester", customers, "standard", cutoff))
    assert result == (JOB_ID, True)
    assert "LossDateFrom" in cursor.execute.call_args.args[0]
    assert "PolicyEffectiveDateFrom" not in cursor.execute.call_args.args[0]
    assert cursor.execute.call_args.args[1:] == (
        str(JOB_ID),
        "selected",
        None if customers is None else len(customers),
        "tester",
        "standard",
        cutoff,
    )
    if customers:
        assert cursor.executemany.call_args.args[1] == [(str(JOB_ID), c) for c in customers]
    else:
        cursor.executemany.assert_not_called()
    connection.commit.assert_called_once()


def test_create_job_reuses_active_job_without_inserting(database):
    connection, cursor = database
    cursor.fetchone.return_value = (str(JOB_ID),)
    assert asyncio.run(repository.create_job("all", "tester", None)) == (JOB_ID, False)
    assert cursor.execute.call_count == 1
    cursor.executemany.assert_not_called()
    connection.commit.assert_called_once()


@pytest.mark.parametrize(
    "method,arguments,columns,rows,expected",
    [
        ("get_job", (JOB_ID,), ["JobId"], [(str(JOB_ID),)], {"JobId": str(JOB_ID)}),
        ("get_job", (JOB_ID,), ["JobId"], [], None),
        ("get_jobs", (), ["JobId"], [(str(JOB_ID),)], [{"JobId": str(JOB_ID)}]),
        (
            "get_failures",
            (JOB_ID,),
            ["CustomerNumber", "FailureReason"],
            [("001", "missing")],
            [{"CustomerNumber": "001", "FailureReason": "missing"}],
        ),
        ("get_all_failures", (), ["CustomerNumber"], [("001",)], [{"CustomerNumber": "001"}]),
        ("get_account_numbers", (JOB_ID,), ["CustomerNumber"], [(" 001 ",), (2,)], ["001", "2"]),
        (
            "get_completed_outputs",
            (JOB_ID,),
            ["CustomerNumber", "OutputPath"],
            [("001", "/report.xlsx")],
            [{"CustomerNumber": "001", "OutputPath": "/report.xlsx"}],
        ),
    ],
)
def test_queries_map_database_results(database, method, arguments, columns, rows, expected):
    connection, cursor = database
    cursor.description = [(column,) for column in columns]
    cursor.fetchall.return_value = rows
    assert asyncio.run(getattr(repository, method)(*arguments)) == expected
    assert cursor.execute.call_args.args[1:] == tuple(str(arg) for arg in arguments)
    connection.commit.assert_not_called()


@pytest.mark.parametrize("rows", [[], [(str(JOB_ID), "all", 2)]])
@pytest.mark.parametrize("environment", ["local", "PREPROD", "PROD"])
def test_claim_job_uses_lease_and_retry_limits(database, rows, environment, monkeypatch):
    monkeypatch.setattr(repository.settings, "ENVIRONMENT", environment)
    connection, cursor = database
    cursor.description = [("JobId",), ("JobType",), ("AttemptCount",)]
    cursor.fetchall.return_value = rows
    result = asyncio.run(repository.claim_next_job("worker"))
    assert result == ({"JobId": str(JOB_ID), "JobType": "all", "AttemptCount": 2} if rows else None)
    expire, claim = cursor.execute.call_args_list
    assert "AttemptCount >= 3" in expire.args[0]
    assert "AttemptCount < 3" in claim.args[0]
    assert "UPDLOCK, READPAST, ROWLOCK" in claim.args[0]
    allow_scheduled = repository.settings.ENVIRONMENT.strip().lower() != "local"
    assert expire.args[1:] == (allow_scheduled,)
    assert claim.args[1:] == (allow_scheduled, "worker")
    assert "TriggerSource = 'manual' OR ? = 1" in expire.args[0]
    assert "TriggerSource = 'manual' OR ? = 1" in claim.args[0]
    connection.commit.assert_called_once()


@pytest.mark.parametrize(
    "method,arguments,parameters",
    [
        ("update_heartbeat", (JOB_ID, "worker"), (str(JOB_ID), "worker")),
        ("update_phase", (JOB_ID, "worker", "generating"), ("generating", str(JOB_ID), "worker")),
        ("complete_job", (JOB_ID, "worker"), (str(JOB_ID), "worker")),
        (
            "fail_job",
            (JOB_ID, "worker", "failed to generate"),
            ("failed to generate", str(JOB_ID), "worker"),
        ),
    ],
)
def test_job_updates_require_current_worker_and_processing_state(
    database, method, arguments, parameters
):
    connection, _ = database
    asyncio.run(getattr(repository, method)(*arguments))
    query, *values = connection.execute.call_args.args
    assert tuple(values) == parameters
    assert "Status = 'processing'" in query
    assert "WorkerId = ?" in query
    connection.commit.assert_called_once()


@pytest.mark.parametrize("job_type", ["all", "selected"])
def test_upsert_accounts_normalizes_names_and_updates_all_job_count(database, job_type):
    connection, cursor = database
    asyncio.run(
        repository.upsert_accounts(
            JOB_ID,
            [{"CustomerNum": " 001 ", "CustomerName": " Example "}, {"CustomerNum": 2}],
            job_type,
        )
    )
    calls = cursor.execute.call_args_list
    assert calls[0].args[1:] == (str(JOB_ID), "001", "Example")
    assert calls[1].args[1:] == (str(JOB_ID), "2", "2")
    assert len(calls) == (3 if job_type == "all" else 2)
    if job_type == "all":
        assert calls[2].args[1:] == (2, str(JOB_ID))
    connection.commit.assert_called_once()


@pytest.mark.parametrize("succeeded", [True, False])
@pytest.mark.parametrize("rowcount", [0, 1])
def test_record_result_counts_only_new_terminal_results(database, succeeded, rowcount):
    connection, cursor = database
    cursor.rowcount = rowcount
    asyncio.run(repository.record_account_result(JOB_ID, "001", succeeded, "reason", "/output"))
    calls = cursor.execute.call_args_list
    assert calls[0].args[1:] == (
        "completed" if succeeded else "failed",
        "reason",
        "/output",
        str(JOB_ID),
        "001",
    )
    assert "Status <> 'completed'" in calls[0].args[0]
    assert "Status <> 'failed'" in calls[0].args[0]
    assert len(calls) == 1 + rowcount
    if rowcount:
        assert calls[1].args[1:] == (int(succeeded), int(not succeeded), str(JOB_ID))
    connection.commit.assert_called_once()


def test_failed_insert_does_not_commit(database):
    connection, cursor = database
    cursor.execute.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        asyncio.run(repository.create_job("all", "tester", None))
    connection.commit.assert_not_called()
