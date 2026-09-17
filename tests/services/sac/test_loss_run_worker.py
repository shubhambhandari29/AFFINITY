import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from services.loss_run import loss_run_worker


@pytest.fixture(autouse=True)
def mock_scheduler(monkeypatch):
    monkeypatch.setattr(loss_run_worker.settings, "LOSS_RUN_SCHEDULE_TEST_MODE", False)
    check = AsyncMock()
    monkeypatch.setattr(loss_run_worker, "enqueue_due_loss_run_job", check)
    return check


@pytest.mark.parametrize("report_type", ["standard", "claim_review"])
@pytest.mark.parametrize("first_result", ["created", "busy", "error"])
def test_local_timer_submits_once_after_five_minutes(
    monkeypatch, mock_scheduler, report_type, first_result
):
    async def scenario():
        elapsed = 0
        submitted_at = []
        worker = loss_run_worker.LossRunWorker()
        monkeypatch.setattr(loss_run_worker.settings, "ENVIRONMENT", "local")
        monkeypatch.setattr(loss_run_worker.settings, "LOSS_RUN_SCHEDULE_TEST_MODE", True)
        monkeypatch.setattr(
            loss_run_worker.settings, "LOSS_RUN_SCHEDULE_TEST_REPORT_TYPE", report_type
        )
        monkeypatch.setattr(loss_run_worker, "monotonic", lambda: elapsed)
        monkeypatch.setattr(loss_run_worker, "POLL_INTERVAL_SECONDS", 0)

        async def create(*args):
            assert args == ("all", "local-schedule-test", None, report_type)
            submitted_at.append(elapsed)
            if len(submitted_at) == 1:
                if first_result == "error":
                    raise RuntimeError("temporary database error")
                if first_result == "busy":
                    return uuid4(), False
            return uuid4(), True

        async def claim(_worker_id):
            nonlocal elapsed
            elapsed += 10
            if elapsed > 900:
                worker._stop_event.set()
            return None

        monkeypatch.setattr(loss_run_worker, "create_job", create)
        monkeypatch.setattr(loss_run_worker, "claim_next_job", claim)
        await worker._run()
        assert submitted_at == ([300] if first_result == "created" else [300, 360])
        mock_scheduler.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "environment,enabled", [("local", False), ("PREPROD", True), ("PROD", True)]
)
def test_local_timer_is_opt_in_and_ignored_in_azure(
    monkeypatch, mock_scheduler, environment, enabled
):
    async def scenario():
        worker = loss_run_worker.LossRunWorker()
        elapsed = 0
        create = AsyncMock()
        monkeypatch.setattr(loss_run_worker.settings, "ENVIRONMENT", environment)
        monkeypatch.setattr(loss_run_worker.settings, "LOSS_RUN_SCHEDULE_TEST_MODE", enabled)
        monkeypatch.setattr(loss_run_worker, "create_job", create)
        monkeypatch.setattr(loss_run_worker, "monotonic", lambda: elapsed)
        monkeypatch.setattr(loss_run_worker, "POLL_INTERVAL_SECONDS", 0)

        async def claim(_worker_id):
            nonlocal elapsed
            elapsed += 10
            if elapsed > 360:
                worker._stop_event.set()
            return None

        monkeypatch.setattr(loss_run_worker, "claim_next_job", claim)
        await worker._run()
        create.assert_not_awaited()
        assert mock_scheduler.await_count == 7

    asyncio.run(scenario())


def test_local_timer_rejects_invalid_report_type(monkeypatch):
    monkeypatch.setattr(loss_run_worker.settings, "ENVIRONMENT", "local")
    monkeypatch.setattr(loss_run_worker.settings, "LOSS_RUN_SCHEDULE_TEST_MODE", True)
    monkeypatch.setattr(loss_run_worker.settings, "LOSS_RUN_SCHEDULE_TEST_REPORT_TYPE", "invalid")
    create = AsyncMock()
    monkeypatch.setattr(loss_run_worker, "create_job", create)
    with pytest.raises(ValueError, match="must be standard or claim_review"):
        asyncio.run(loss_run_worker.LossRunWorker()._run())
    create.assert_not_awaited()


@pytest.mark.parametrize("schedule_fails", [False, True])
def test_schedule_checks_are_throttled_without_delaying_job_pickup(
    monkeypatch, mock_scheduler, schedule_fails
):
    async def scenario():
        worker = loss_run_worker.LossRunWorker()
        elapsed = 0
        checks_at = []
        claimed_at = []

        async def check():
            checks_at.append(elapsed)
            if schedule_fails:
                raise RuntimeError("schedule unavailable")

        async def claim(_worker_id):
            nonlocal elapsed
            claimed_at.append(elapsed)
            elapsed += 10
            if elapsed > 120:
                worker._stop_event.set()
            return None

        mock_scheduler.side_effect = check
        monkeypatch.setattr(loss_run_worker, "monotonic", lambda: elapsed)
        monkeypatch.setattr(loss_run_worker, "claim_next_job", claim)
        monkeypatch.setattr(loss_run_worker, "POLL_INTERVAL_SECONDS", 0)
        await worker._run()
        assert checks_at == [0, 60, 120]
        assert claimed_at == list(range(0, 121, 10))

    asyncio.run(scenario())


def test_scheduler_failure_does_not_block_manual_job(monkeypatch, mock_scheduler):
    async def scenario():
        worker = loss_run_worker.LossRunWorker()
        mock_scheduler.side_effect = RuntimeError("schedule unavailable")
        job = {"JobId": uuid4()}
        claim = AsyncMock(return_value=job)

        async def process(received):
            assert received == job
            worker._stop_event.set()

        monkeypatch.setattr(loss_run_worker, "claim_next_job", claim)
        monkeypatch.setattr(worker, "_process_job", process)
        await worker._run()
        claim.assert_awaited_once_with(worker.worker_id)

    asyncio.run(scenario())


@pytest.mark.parametrize("expected_type", ["standard", "claim_review"])
def test_worker_processes_selected_job_and_records_progress(monkeypatch, expected_type):
    job_id = uuid4()
    calls = []

    async def fake_accounts(received_job_id):
        assert received_job_id == job_id
        return ["00123"]

    async def fake_generate(
        customer_numbers, *, on_phase, on_customers, on_result, report_type="standard"
    ):
        assert customer_numbers == ["00123"]
        assert report_type == expected_type
        await on_phase("querying_loss_run_data")
        await on_customers([{"CustomerNum": "00123", "CustomerName": "Example Customer"}])
        await on_result("00123", True, None, "/Volumes/report.xlsx")

    async def fake_phase(received_job_id, worker_id, phase):
        calls.append(("phase", received_job_id, worker_id, phase))

    async def fake_upsert(received_job_id, customers, job_type):
        calls.append(("customers", received_job_id, customers, job_type))

    async def fake_result(
        received_job_id,
        customer_number,
        succeeded,
        reason,
        output_path,
    ):
        calls.append(
            (
                "result",
                received_job_id,
                customer_number,
                succeeded,
                reason,
                output_path,
            )
        )

    async def fake_complete(received_job_id, worker_id):
        calls.append(("complete", received_job_id, worker_id))

    async def fake_heartbeat(received_job_id, worker_id):
        return None

    monkeypatch.setattr(loss_run_worker, "get_account_numbers", fake_accounts)
    monkeypatch.setattr(loss_run_worker, "generate_loss_runs", fake_generate)
    monkeypatch.setattr(loss_run_worker, "update_phase", fake_phase)
    monkeypatch.setattr(loss_run_worker, "upsert_accounts", fake_upsert)
    monkeypatch.setattr(loss_run_worker, "record_account_result", fake_result)
    monkeypatch.setattr(loss_run_worker, "complete_job", fake_complete)
    monkeypatch.setattr(loss_run_worker, "update_heartbeat", fake_heartbeat)

    worker = loss_run_worker.LossRunWorker()
    asyncio.run(
        worker._process_job(
            {
                "JobId": job_id,
                "JobType": "selected",
                "ReportType": expected_type,
                "AttemptCount": 1,
            }
        )
    )

    assert [call[0] for call in calls] == [
        "phase",
        "customers",
        "result",
        "complete",
    ]
    assert calls[2][3:] == (True, None, "/Volumes/report.xlsx")


def test_worker_marks_job_failed_when_generation_fails(monkeypatch):
    job_id = uuid4()
    failed = []

    async def fake_accounts(_job_id):
        return ["00123"]

    async def fake_generate(*args, **kwargs):
        raise RuntimeError("database unavailable")

    async def fake_fail(received_job_id, worker_id, message):
        failed.append((received_job_id, worker_id, message))

    monkeypatch.setattr(loss_run_worker, "get_account_numbers", fake_accounts)
    monkeypatch.setattr(loss_run_worker, "generate_loss_runs", fake_generate)
    monkeypatch.setattr(loss_run_worker, "fail_job", fake_fail)

    worker = loss_run_worker.LossRunWorker()
    asyncio.run(
        worker._process_job(
            {
                "JobId": job_id,
                "JobType": "selected",
                "AttemptCount": 1,
            }
        )
    )

    assert failed[0][0] == job_id
    assert failed[0][2] == ("Loss-run generation failed. Check application logs for details.")


def test_worker_start_is_idempotent_and_stop_cancels_polling(monkeypatch):
    async def scenario():
        entered = asyncio.Event()

        async def claim(_worker):
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(loss_run_worker, "claim_next_job", claim)
        worker = loss_run_worker.LossRunWorker()
        worker.start()
        original = worker._task
        worker.start()
        assert worker._task is original
        await entered.wait()
        await worker.stop()
        assert original.cancelled()
        assert worker._stop_event.is_set()

    asyncio.run(scenario())


def test_polling_recovers_after_error_and_processes_next_job(monkeypatch):
    async def scenario():
        worker = loss_run_worker.LossRunWorker()
        job = {"JobId": uuid4()}
        claim = AsyncMock(side_effect=[RuntimeError("temporary outage"), None, job])

        async def process(received):
            assert received == job
            worker._stop_event.set()

        monkeypatch.setattr(loss_run_worker, "POLL_INTERVAL_SECONDS", 0)
        monkeypatch.setattr(loss_run_worker, "claim_next_job", claim)
        monkeypatch.setattr(worker, "_process_job", process)
        await worker._run()
        assert claim.await_count == 3

    asyncio.run(scenario())


def test_all_job_skips_account_lookup(monkeypatch):
    generate = AsyncMock()
    accounts = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(loss_run_worker, "generate_loss_runs", generate)
    monkeypatch.setattr(loss_run_worker, "get_account_numbers", accounts)
    monkeypatch.setattr(loss_run_worker, "complete_job", complete)
    worker = loss_run_worker.LossRunWorker()
    job_id = uuid4()
    asyncio.run(worker._process_job({"JobId": job_id, "JobType": "all"}))
    assert generate.await_args.args == (None,)
    accounts.assert_not_awaited()
    complete.assert_awaited_once_with(job_id, worker.worker_id)


def test_cancelled_job_is_not_marked_failed_or_completed(monkeypatch):
    generate = AsyncMock(side_effect=asyncio.CancelledError)
    fail = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(loss_run_worker, "generate_loss_runs", generate)
    monkeypatch.setattr(loss_run_worker, "fail_job", fail)
    monkeypatch.setattr(loss_run_worker, "complete_job", complete)
    worker = loss_run_worker.LossRunWorker()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker._process_job({"JobId": uuid4(), "JobType": "all"}))
    fail.assert_not_awaited()
    complete.assert_not_awaited()


def test_heartbeat_retries_after_transient_failure(monkeypatch):
    sleep = AsyncMock(side_effect=[None, None, asyncio.CancelledError])
    heartbeat = AsyncMock(side_effect=[RuntimeError("temporary outage"), None])
    monkeypatch.setattr(loss_run_worker.asyncio, "sleep", sleep)
    monkeypatch.setattr(loss_run_worker, "update_heartbeat", heartbeat)
    worker = loss_run_worker.LossRunWorker()
    job_id = uuid4()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker._heartbeat(job_id))
    assert heartbeat.await_count == 2
    heartbeat.assert_awaited_with(job_id, worker.worker_id)
