import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import HTTPException

from services.loss_run import loss_run_job_service


def test_create_selected_job_normalizes_accounts_and_requester(monkeypatch):
    captured = {}
    job_id = uuid4()

    async def fake_create(job_type, requested_by, customer_numbers):
        captured["values"] = (job_type, requested_by, customer_numbers)
        return job_id, True

    monkeypatch.setattr(loss_run_job_service, "create_job", fake_create)

    result = asyncio.run(
        loss_run_job_service.create_loss_run_job(
            "selected",
            {"user": {"id": "sx1234", "email": "user@example.com"}},
            ["00123", " 00456 ", "00123"],
        )
    )

    assert captured["values"] == (
        "selected",
        "user@example.com",
        ["00123", "00456"],
    )
    assert result == {
        "jobId": job_id,
        "status": "queued",
        "message": "Loss-run generation has been queued",
    }


def test_create_selected_job_rejects_empty_accounts():
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            loss_run_job_service.create_loss_run_job(
                "selected",
                {"user": {"id": "sx1234"}},
                ["", "   "],
            )
        )

    assert error.value.status_code == 400


def test_get_job_returns_failures_only_when_present(monkeypatch):
    job_id = uuid4()
    now = datetime.now(UTC)

    async def fake_get(received_job_id):
        assert received_job_id == job_id
        return {
            "JobId": job_id,
            "JobType": "selected",
            "Status": "partially_completed",
            "Phase": "finished",
            "RequestedCount": 2,
            "ProcessedCount": 2,
            "GeneratedCount": 1,
            "FailedCount": 1,
            "RequestedBy": "user@example.com",
            "CreatedAt": now,
            "StartedAt": now,
            "CompletedAt": now,
            "ErrorMessage": None,
        }

    async def fake_failures(received_job_id):
        assert received_job_id == job_id
        return [
            {
                "CustomerNumber": "00456",
                "FailureReason": "No loss-run records found",
            }
        ]

    monkeypatch.setattr(loss_run_job_service, "get_job", fake_get)
    monkeypatch.setattr(loss_run_job_service, "get_failures", fake_failures)

    result = asyncio.run(loss_run_job_service.get_loss_run_job(job_id))

    assert result["generatedCount"] == 1
    assert result["failures"] == [
        {
            "customerNumber": "00456",
            "reason": "No loss-run records found",
        }
    ]
