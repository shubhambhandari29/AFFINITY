from fastapi import APIRouter, Depends, Request

from core.models.distribution import ComposeLinkRequest, DistributionEntry
from services.auth_service import get_current_user_from_token
from services.sac.loss_run_distribution_service import (
    build_outlook_compose_link,
    delete_distribution as delete_distribution_service,
)
from services.sac.loss_run_distribution_service import (
    get_distribution as get_distribution_service,
)

from services.sac.loss_run_distribution_service import (
    upsert_distribution as upsert_distribution_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_distribution(request: Request):
    return await get_distribution_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_distribution(payload: list[DistributionEntry]):
    entries = [entry.model_dump() for entry in payload]
    return await upsert_distribution_service(entries)


@router.post("/delete")
async def delete_distribution(payload: list[DistributionEntry]):
    entries = [entry.model_dump() for entry in payload]
    return await delete_distribution_service(entries)


@router.post("/compose_link")
async def build_compose_link(
    payload: ComposeLinkRequest,
):
    entries = [entry.model_dump() for entry in payload.entries] if payload.entries else None
    return await build_outlook_compose_link(
        entries=entries,
        subject=payload.subject,
        body=payload.body
    )
