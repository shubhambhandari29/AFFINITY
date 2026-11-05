import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from services.loss_run.affinity import affinity_loss_run_service as generation
from services.loss_run.loss_run_cancellation import LossRunCancellation, LossRunCancelled
from services.loss_run import loss_run_job_service as jobs
from services.loss_run import loss_run_worker as workers


PROGRAM = {"programId": 19, "programName": "Example", "agentCodes": ["5504032"]}


def test_program_lookup_uses_cancellable_query(monkeypatch):
    control = LossRunCancellation()
    control.run_query = AsyncMock(return_value=[])
    raw = AsyncMock()
    monkeypatch.setattr(generation, "run_raw_query_async", raw)
    assert asyncio.run(generation.get_affinity_loss_run_programs(control)) == []
    control.run_query.assert_awaited_once()
    raw.assert_not_awaited()


def test_cancel_during_workbook_prevents_upload(monkeypatch):
    control = LossRunCancellation()
    control.run_query = AsyncMock(return_value=[])
    monkeypatch.setattr(generation, "get_affinity_loss_run_programs", AsyncMock(return_value=[PROGRAM]))
    storage = MagicMock()
    monkeypatch.setattr(generation, "DatabricksLossRunStorage", lambda: storage)

    async def threadpool(function, *args, **kwargs):
        if function is generation.create_affinity_workbook:
            control.request()
            return b"workbook finished while cancellation was pending"
        return function(*args, **kwargs)

    monkeypatch.setattr(generation, "run_in_threadpool", threadpool)
    result = AsyncMock()
    with pytest.raises(LossRunCancelled):
        asyncio.run(generation.generate_affinity_loss_runs(
            cancellation=control, on_phase=AsyncMock(), on_customers=AsyncMock(), on_result=result
        ))
    storage.upload_report.assert_not_called()
    result.assert_not_awaited()


@pytest.mark.parametrize("business,stored", [("affinity", "sac"), ("sac", "affinity")])
def test_wrong_business_cannot_cancel(monkeypatch, business, stored):
    monkeypatch.setattr(jobs, "get_job", AsyncMock(return_value={"BusinessType": stored}))
    mutate = AsyncMock()
    monkeypatch.setattr(jobs, "request_cancellation", mutate)
    with pytest.raises(HTTPException) as error:
        asyncio.run(jobs.cancel_loss_run_job(uuid4(), business))
    assert error.value.status_code == 404
    mutate.assert_not_awaited()


@pytest.mark.parametrize("stage", ["claims", "record_only"])
def test_cancel_during_affinity_query_is_not_program_failure(monkeypatch, stage):
    control = LossRunCancellation()
    control.run_query = AsyncMock(side_effect=(
        [LossRunCancelled()] if stage == "claims" else [[], LossRunCancelled()]
    ))
    programs = AsyncMock(return_value=[PROGRAM])
    monkeypatch.setattr(generation, "get_affinity_loss_run_programs", programs)
    storage = MagicMock()
    monkeypatch.setattr(generation, "DatabricksLossRunStorage", lambda: storage)
    result = AsyncMock()
    with pytest.raises(LossRunCancelled):
        asyncio.run(generation.generate_affinity_loss_runs(
            cancellation=control, on_phase=AsyncMock(), on_customers=AsyncMock(), on_result=result
        ))
    programs.assert_awaited_once_with(cancellation=control)
    assert control.run_query.call_args.args[0] == (
        generation.CLAIMS_QUERY if stage == "claims" else generation.RECORD_ONLY_QUERY
    )
    storage.upload_report.assert_not_called()
    result.assert_not_awaited()


def test_affinity_uploaded_file_kept_and_next_program_not_started(monkeypatch):
    control = LossRunCancellation()
    control.run_query = AsyncMock(return_value=[])
    monkeypatch.setattr(generation, "get_affinity_loss_run_programs", AsyncMock(
        return_value=[PROGRAM, dict(PROGRAM, programId=17)]
    ))
    storage = MagicMock()
    storage.upload_report.return_value = "/saved/report.xlsx"
    monkeypatch.setattr(generation, "DatabricksLossRunStorage", lambda: storage)
    monkeypatch.setattr(generation, "create_affinity_workbook", lambda *args: b"report")
    results = []

    async def on_result(*args):
        results.append(args)
        control.request()

    with pytest.raises(LossRunCancelled):
        asyncio.run(generation.generate_affinity_loss_runs(
            cancellation=control, on_phase=AsyncMock(), on_customers=AsyncMock(), on_result=on_result
        ))
    assert results == [("19", True, None, "/saved/report.xlsx")]
    assert control.run_query.await_count == 2
    storage.upload_report.assert_called_once()


def test_affinity_worker_marks_cancelled_not_failed(monkeypatch):
    monkeypatch.setattr(workers, "get_cancellation_state", AsyncMock(return_value=False))
    generator = AsyncMock(side_effect=LossRunCancelled)
    monkeypatch.setattr(workers, "generate_affinity_loss_runs", generator)
    cancelled, failed, complete = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(workers, "cancel_job", cancelled)
    monkeypatch.setattr(workers, "fail_job", failed)
    monkeypatch.setattr(workers, "complete_job", complete)
    worker = workers.LossRunWorker()
    job_id = uuid4()
    asyncio.run(worker._process_job({"JobId": job_id, "JobType": "all", "BusinessType": "affinity"}))
    assert isinstance(generator.call_args.kwargs["cancellation"], LossRunCancellation)
    cancelled.assert_awaited_once_with(job_id, worker.worker_id)
    failed.assert_not_awaited()
    complete.assert_not_awaited()
