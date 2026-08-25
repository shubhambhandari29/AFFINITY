import asyncio
from uuid import uuid4

from api.loss_run import loss_run

CURRENT_USER = {"user": {"id": "sx1234", "email": "user@example.com"}}


def test_generate_all_loss_runs_creates_job(monkeypatch):
    async def fake_create(job_type, current_user, customer_numbers=None):
        assert job_type == "all"
        assert current_user == CURRENT_USER
        assert customer_numbers is None
        return {"jobId": uuid4(), "status": "queued"}

    monkeypatch.setattr(loss_run, "create_loss_run_job", fake_create)

    result = asyncio.run(loss_run.generate_all_loss_runs(CURRENT_USER))
    assert result["status"] == "queued"


def test_generate_selected_loss_runs_creates_job_with_customer_array(monkeypatch):
    captured = {}

    async def fake_create(job_type, current_user, customer_numbers=None):
        captured["job_type"] = job_type
        captured["current_user"] = current_user
        captured["customer_numbers"] = customer_numbers
        return {"jobId": uuid4(), "status": "queued"}

    monkeypatch.setattr(loss_run, "create_loss_run_job", fake_create)
    payload = loss_run.LossRunSelection(customerNumbers=["00123"])

    result = asyncio.run(loss_run.generate_selected_loss_runs(payload, CURRENT_USER))

    assert result["status"] == "queued"
    assert captured == {
        "job_type": "selected",
        "current_user": CURRENT_USER,
        "customer_numbers": ["00123"],
    }


def test_get_loss_run_job_status_calls_service(monkeypatch):
    job_id = uuid4()

    async def fake_get(received_job_id):
        assert received_job_id == job_id
        return {"jobId": job_id, "status": "processing"}

    monkeypatch.setattr(loss_run, "get_loss_run_job", fake_get)

    result = asyncio.run(loss_run.get_loss_run_job_status(job_id, CURRENT_USER))

    assert result["status"] == "processing"
