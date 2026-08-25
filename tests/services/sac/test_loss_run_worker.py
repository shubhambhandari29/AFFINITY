import asyncio
from uuid import uuid4

from services.loss_run import loss_run_worker


def test_worker_processes_selected_job_and_records_progress(monkeypatch):
    job_id = uuid4()
    calls = []

    async def fake_accounts(received_job_id):
        assert received_job_id == job_id
        return ["00123"]

    async def fake_generate(customer_numbers, *, on_phase, on_customers, on_result):
        assert customer_numbers == ["00123"]
        await on_phase("querying_loss_run_data")
        await on_customers(
            [{"CustomerNum": "00123", "CustomerName": "Example Customer"}]
        )
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
    assert failed[0][2] == (
        "Loss-run generation failed. Check application logs for details."
    )
