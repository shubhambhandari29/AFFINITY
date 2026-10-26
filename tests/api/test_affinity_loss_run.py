from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api.loss_run.affinity import affinity_loss_run as api


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(api.router, prefix="/affinity_loss_run")
    app.dependency_overrides[api.get_current_user_from_token] = lambda: {
        "user": {"id": "tester"}
    }
    return TestClient(app)


def test_selected_and_all_routes(client, monkeypatch):
    create = AsyncMock(
        return_value={
            "jobId": uuid4(),
            "businessType": "affinity",
            "status": "queued",
            "message": "queued",
        }
    )
    monkeypatch.setattr(api, "create_affinity_loss_run_job", create)
    response = client.post("/affinity_loss_run/generate", json={"programIds": [3, 33]})
    assert response.status_code == 202
    assert response.json()["businessType"] == "affinity"
    assert create.call_args.args[0] == [3, 33]
    assert client.post("/affinity_loss_run/generate-all").status_code == 202
    assert create.call_args.args[0] is None
    assert (
        client.post(
            "/affinity_loss_run/generate", json={"customerNumbers": ["3"]}
        ).status_code
        == 422
    )


def test_list_programs_route(client, monkeypatch):
    monkeypatch.setattr(
        api,
        "get_affinity_loss_run_programs",
        AsyncMock(return_value=[{"programId": 3}]),
    )
    response = client.get("/affinity_loss_run/programs")
    assert response.status_code == 200
    assert response.json() == [{"programId": 3}]


def test_jobs_translate_program_failures(client, monkeypatch):
    job = {
        "jobId": str(uuid4()),
        "jobType": "selected",
        "businessType": "affinity",
        "status": "failed",
        "phase": "finished",
        "requestedCount": 1,
        "processedCount": 1,
        "generatedCount": 0,
        "failedCount": 1,
        "requestedBy": "tester",
        "createdAt": "10-26-2026 01:00:00 AM",
        "startedAt": None,
        "completedAt": None,
        "errorMessage": None,
        "failures": [{"customerNumber": "3", "reason": "query failed"}],
    }
    get = AsyncMock(return_value=job)
    monkeypatch.setattr(api, "get_loss_run_job", get)
    response = client.get(f'/affinity_loss_run/jobs/{job["jobId"]}')
    assert response.status_code == 200
    assert response.json()["failures"] == [{"programId": 3, "reason": "query failed"}]
    assert get.call_args.args[1] == "affinity"


def test_wrong_domain_download_rejected_before_storage(client, monkeypatch):
    monkeypatch.setattr(
        api, "get_loss_run_job", AsyncMock(side_effect=HTTPException(404))
    )
    download = AsyncMock()
    monkeypatch.setattr(api, "prepare_loss_run_download", download)
    assert client.get(f"/affinity_loss_run/jobs/{uuid4()}/download").status_code == 404
    download.assert_not_awaited()
