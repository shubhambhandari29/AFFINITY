from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status

from core.models.loss_run.loss_run import LossRunSelection
from core.models.loss_run.loss_run_job import LossRunJobCreated, LossRunJobResponse
from services.auth_service import get_current_user_from_token
from services.loss_run.loss_run_job_service import (
    create_loss_run_job,
    get_loss_run_job,
    get_loss_run_jobs,
)

router = APIRouter()


@router.post(
    "/generate-all",
    response_model=LossRunJobCreated,
    status_code=status.HTTP_202_ACCEPTED,
)
async def generate_all_loss_runs(
    current_user: Annotated[dict, Depends(get_current_user_from_token)],
):
    return await create_loss_run_job("all", current_user)


@router.post(
    "/generate",
    response_model=LossRunJobCreated,
    status_code=status.HTTP_202_ACCEPTED,
)
async def generate_selected_loss_runs(
    payload: LossRunSelection,
    current_user: Annotated[dict, Depends(get_current_user_from_token)],
):
    return await create_loss_run_job(
        "selected",
        current_user,
        payload.customerNumbers,
    )


@router.get(
    "/jobs",
    response_model=list[LossRunJobResponse],
)
async def get_all_loss_run_jobs(
    _current_user: Annotated[dict, Depends(get_current_user_from_token)],
):
    return await get_loss_run_jobs()


@router.get(
    "/jobs/{job_id}",
    response_model=LossRunJobResponse,
)
async def get_loss_run_job_status(
    job_id: UUID,
    _current_user: Annotated[dict, Depends(get_current_user_from_token)],
):
    return await get_loss_run_job(job_id)
