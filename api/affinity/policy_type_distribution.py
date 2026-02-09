from fastapi import APIRouter, Depends, Request

from core.models.affinity.affinity_distribution import AffinityDistributionEntry
from services.affinity.policy_type_distribution_service import (
    get_distribution as get_distribution_service,
)
from services.affinity.policy_type_distribution_service import (
    upsert_distribution as upsert_distribution_service,
)
from services.auth_service import get_current_user_from_token

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_distribution(request: Request):
    return await get_distribution_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_distribution(payload: list[AffinityDistributionEntry]):
    entries = [entry.model_dump() for entry in payload]
    return await upsert_distribution_service(entries)
