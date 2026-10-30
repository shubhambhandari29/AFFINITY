import asyncio
from contextlib import nullcontext
from threading import Event
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from api.loss_run import loss_run as routes
from services.loss_run import loss_run_cancellation as sql
from services.loss_run import loss_run_job_repository as repository
from services.loss_run import loss_run_job_service as service
from services.loss_run import loss_run_service as generation
from services.loss_run import loss_run_worker as worker_module


@pytest.mark.parametrize("state", ["queued", "processing", "cancelled", "completed"])
def test_request_is_persisted_and_queued_work_is_cancelled_atomically(
    monkeypatch, state
):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = (state, state == "cancelled")
    monkeypatch.setattr(repository, "db_connection", lambda: nullcontext(connection))
    result = asyncio.run(repository.request_cancellation(uuid4()))
    assert result["Status"] == ("cancelled" if state == "queued" else state)
    assert result["CancelRequested"] == (state != "completed")
    queries = [c.args[0] for c in cursor.execute.call_args_list]
    assert "UPDLOCK, HOLDLOCK" in queries[0]
    assert (
        len(queries)
        == {"queued": 3, "processing": 2, "cancelled": 1, "completed": 1}[state]
    )
    if state == "queued":
        assert "NOT IN ('completed', 'failed', 'cancelled')" in queries[-1]
    connection.commit.assert_called_once()


@pytest.mark.parametrize(
    "state,status", [(None, 404), ("completed", 409), ("failed", 409)]
)
def test_cancel_api_service_rejects_missing_or_finished_jobs(
    monkeypatch, state, status
):
    job = {"Status": state, "CancelRequested": False} if state else None
    monkeypatch.setattr(service, "get_job", AsyncMock(return_value=job))
    monkeypatch.setattr(service, "request_cancellation", AsyncMock(return_value=job))
    with pytest.raises(HTTPException) as error:
        asyncio.run(service.cancel_loss_run_job(uuid4()))
    assert error.value.status_code == status


def test_cancel_endpoint_returns_processing_until_worker_stops(monkeypatch):
    job_id = uuid4()
    monkeypatch.setattr(service, "get_job", AsyncMock(return_value={"BusinessType": "sac"}))
    monkeypatch.setattr(
        service,
        "request_cancellation",
        AsyncMock(
            return_value={
                "Status": "processing",
                "CancelRequested": True,
            }
        ),
    )
    result = asyncio.run(
        routes.cancel_loss_run_job_request(job_id, {"user": {"id": "tester"}})
    )
    assert result["jobId"] == job_id
    assert result["status"] == "processing"
    assert result["cancelRequested"] is True


@pytest.mark.parametrize("finish", ["complete_job", "fail_job", "cancel_job"])
def test_terminal_transition_observes_late_cancel_and_keeps_finished_accounts(
    monkeypatch, finish
):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = ("cancelled",)
    monkeypatch.setattr(repository, "db_connection", lambda: nullcontext(connection))
    args = [uuid4(), "worker"] + (["failure"] if finish == "fail_job" else [])
    asyncio.run(getattr(repository, finish)(*args))
    first, pending = cursor.execute.call_args_list
    assert first.args[0].index("OUTPUT inserted.Status") < first.args[0].index(
        "WHERE JobId"
    )
    assert "WHEN CancelRequested = 1 THEN 'cancelled'" in first.args[0]
    assert "WorkerId = ?" in first.args[0]
    assert "NOT IN ('completed', 'failed', 'cancelled')" in pending.args[0]
    assert "GeneratedCount =" not in pending.args[0]
    assert "FailedCount =" not in pending.args[0]
    connection.commit.assert_called_once()


@pytest.mark.parametrize("stage", ["execute", "fetch"])
def test_running_sql_is_interrupted_and_connection_closed(monkeypatch, stage):
    async def scenario():
        entered, release = Event(), Event()
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.description = [("Value",)]

        def block(*args):
            entered.set()
            assert release.wait(3), "test SQL was never cancelled"
            raise RuntimeError("HY008: operation cancelled")

        if stage == "execute":
            cursor.execute.side_effect = block
        else:
            cursor.fetchmany.side_effect = block
        cursor.cancel.side_effect = release.set
        monkeypatch.setattr(sql, "db_connection", lambda: nullcontext(connection))
        control = sql.LossRunCancellation()
        task = asyncio.create_task(control.run_query("SELECT Value"))
        assert await asyncio.to_thread(entered.wait, 3)
        control.request()
        with pytest.raises(sql.LossRunCancelled):
            await asyncio.wait_for(task, 3)
        cursor.cancel.assert_called()
        cursor.close.assert_called_once()

    asyncio.run(scenario())


def test_cancellation_does_not_report_stopped_while_sql_still_running(monkeypatch):
    async def scenario():
        entered, release, attempted = Event(), Event(), Event()
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.description = [("Value",)]
        cursor.fetchmany.return_value = []

        def block(*args):
            entered.set()
            assert release.wait(3)

        cursor.execute.side_effect = block
        cursor.cancel.side_effect = attempted.set  # Driver has not stopped yet.
        monkeypatch.setattr(sql, "db_connection", lambda: nullcontext(connection))
        control = sql.LossRunCancellation()
        task = asyncio.create_task(control.run_query("SELECT Value"))
        assert await asyncio.to_thread(entered.wait, 3)
        control.request()
        assert await asyncio.to_thread(attempted.wait, 3)
        assert not task.done()
        cursor.close.assert_not_called()
        release.set()
        with pytest.raises(sql.LossRunCancelled):
            await task
        cursor.close.assert_called_once()

    asyncio.run(scenario())


def test_shutdown_cancels_and_drains_sql_thread(monkeypatch):
    async def scenario():
        entered, release = Event(), Event()
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.description = [("Value",)]
        cursor.fetchmany.return_value = []

        def block(*args):
            entered.set()
            assert release.wait(3)

        cursor.execute.side_effect = block
        cursor.cancel.side_effect = release.set
        monkeypatch.setattr(sql, "db_connection", lambda: nullcontext(connection))
        task = asyncio.create_task(sql.LossRunCancellation().run_query("SELECT Value"))
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        cursor.close.assert_called_once()

    asyncio.run(scenario())


def test_sql_returns_rows_and_preserves_database_errors(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.description = [("Value",)]
    cursor.fetchmany.side_effect = [[(1,), (None,)], []]
    monkeypatch.setattr(sql, "db_connection", lambda: nullcontext(connection))
    assert asyncio.run(sql.LossRunCancellation().run_query("SELECT Value", [1])) == [
        {"Value": 1},
        {"Value": None},
    ]
    cursor.execute.side_effect = RuntimeError("permission denied")
    with pytest.raises(RuntimeError, match="permission denied"):
        asyncio.run(sql.LossRunCancellation().run_query("SELECT Value"))


def test_worker_cancelled_before_start_never_runs_query(monkeypatch):
    generate, finish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(
        worker_module, "get_cancellation_state", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(worker_module, "generate_loss_runs", generate)
    monkeypatch.setattr(worker_module, "cancel_job", finish)
    worker = worker_module.LossRunWorker()
    job_id = uuid4()
    asyncio.run(worker._process_job({"JobId": job_id, "JobType": "all"}))
    generate.assert_not_awaited()
    finish.assert_awaited_once_with(job_id, worker.worker_id)


def test_worker_handles_user_cancellation_as_cancelled_not_failed(monkeypatch):
    monkeypatch.setattr(
        worker_module, "get_cancellation_state", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(
        worker_module, "generate_loss_runs", AsyncMock(side_effect=sql.LossRunCancelled)
    )
    finish, fail, complete = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(worker_module, "cancel_job", finish)
    monkeypatch.setattr(worker_module, "fail_job", fail)
    monkeypatch.setattr(worker_module, "complete_job", complete)
    worker = worker_module.LossRunWorker()
    job_id = uuid4()
    asyncio.run(worker._process_job({"JobId": job_id, "JobType": "all"}))
    finish.assert_awaited_once_with(job_id, worker.worker_id)
    fail.assert_not_awaited()
    complete.assert_not_awaited()


def test_worker_checks_persisted_cancel_after_each_uploaded_account(monkeypatch):
    monkeypatch.setattr(worker_module, "get_cancellation_state", AsyncMock(side_effect=[False, True]))
    monkeypatch.setattr(worker_module, "record_account_result", AsyncMock())
    finish, complete = AsyncMock(), AsyncMock()
    monkeypatch.setattr(worker_module, "cancel_job", finish)
    monkeypatch.setattr(worker_module, "complete_job", complete)

    async def generate(*args, on_result, cancellation, **kwargs):
        await on_result("001", True, None, "/saved/report.xlsx")
        cancellation.check()
        pytest.fail("The next account must not start")

    monkeypatch.setattr(worker_module, "generate_loss_runs", generate)
    worker = worker_module.LossRunWorker()
    job_id = uuid4()
    asyncio.run(worker._process_job({"JobId": job_id, "JobType": "all"}))
    finish.assert_awaited_once_with(job_id, worker.worker_id)
    complete.assert_not_awaited()


def test_cancellation_between_accounts_keeps_uploaded_file(monkeypatch):
    async def scenario():
        control = sql.LossRunCancellation()
        control.run_query = AsyncMock(
            side_effect=[
                [
                    {"CustomerNum": "001", "CustomerName": "A"},
                    {"CustomerNum": "002", "CustomerName": "B"},
                ],
                [],  # Both accounts use no-claims workbooks.
            ]
        )
        storage = MagicMock()
        storage.upload_report.return_value = "/saved/first.xlsx"
        monkeypatch.setattr(generation, "DatabricksLossRunStorage", lambda: storage)
        monkeypatch.setattr(generation, "_create_workbook", lambda *args: b"report")
        results = []

        async def result(*args):
            results.append(args)
            control.request()

        with pytest.raises(sql.LossRunCancelled):
            await generation.generate_loss_runs(cancellation=control, on_result=result)
        assert results == [("001", True, None, "/saved/first.xlsx")]
        storage.upload_report.assert_called_once()

    asyncio.run(scenario())
