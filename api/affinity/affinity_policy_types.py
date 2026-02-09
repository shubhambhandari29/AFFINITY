from fastapi import APIRouter, Depends, Request

from core.models.affinity.affinity_policy_types import AffinityPolicyTypeUpsert
from services.affinity.affinity_policy_types_service import (
    get_affinity_policy_types as get_affinity_policy_types_service,
)
from services.affinity.affinity_policy_types_service import (
    upsert_affinity_policy_types as upsert_affinity_policy_types_service,
)
from services.auth_service import get_current_user_from_token

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_affinity_policy_types(request: Request):
    return await get_affinity_policy_types_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_affinity_policy_types(payload: AffinityPolicyTypeUpsert):
    return await upsert_affinity_policy_types_service(payload.model_dump())
