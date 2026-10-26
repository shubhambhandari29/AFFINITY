import os
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from core.models.affinity.affinity_loss_run import (
    AffinityLossRunJobResponse,
    AffinityLossRunSelection,
)
from core.models.loss_run.loss_run_job import LossRunJobCreated
from services.affinity.affinity_loss_run_service import (
    create_affinity_loss_run_job,
    get_affinity_loss_run_programs,
)
from services.auth_service import get_current_user_from_token
from services.loss_run.loss_run_download_service import prepare_loss_run_download
from services.loss_run.loss_run_job_service import get_loss_run_job, get_loss_run_jobs

router = APIRouter()
CurrentUser = Annotated[dict, Depends(get_current_user_from_token)]


@router.get("/programs", response_model=list[dict])
async def list_programs(_current_user: CurrentUser):
    return await get_affinity_loss_run_programs()


@router.post("/generate", response_model=LossRunJobCreated, status_code=202)
async def generate_selected(
    payload: AffinityLossRunSelection, current_user: CurrentUser
):
    return await create_affinity_loss_run_job(payload.programIds, current_user)


@router.post("/generate-all", response_model=LossRunJobCreated, status_code=202)
async def generate_all(current_user: CurrentUser):
    return await create_affinity_loss_run_job(None, current_user)


def _program_failures(job):
    job["failures"] = [
        {"programId": int(row["customerNumber"]), "reason": row["reason"]}
        for row in job["failures"]
    ]
    return job


@router.get("/jobs", response_model=list[AffinityLossRunJobResponse])
async def list_jobs(_current_user: CurrentUser):
    return [_program_failures(job) for job in await get_loss_run_jobs("affinity")]


@router.get("/jobs/{job_id}", response_model=AffinityLossRunJobResponse)
async def job_status(job_id: UUID, _current_user: CurrentUser):
    return _program_failures(await get_loss_run_job(job_id, "affinity"))


@router.get("/jobs/{job_id}/download", response_class=FileResponse)
async def download_job(job_id: UUID, _current_user: CurrentUser):
    await get_loss_run_job(job_id, "affinity")
    download = await prepare_loss_run_download(job_id)
    return FileResponse(
        path=download.path,
        filename=download.filename,
        media_type=download.media_type,
        background=BackgroundTask(os.remove, download.path),
    )
