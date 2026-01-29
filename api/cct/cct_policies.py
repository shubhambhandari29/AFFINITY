from fastapi import APIRouter, Depends, Request

from core.models.sac_policies import SacPolicyBulkFieldUpdate, SacPolicyUpsert
from services.auth_service import get_current_user_from_token
from services.cct.cct_policies_service import (
    get_cct_policies as get_cct_policies_service,
    get_premium as get_premium_service,
    update_field_for_all_policies as update_field_for_all_policies_service,
    upsert_cct_policies as upsert_cct_policies_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_cct_policies(request: Request):
    return await get_cct_policies_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_cct_policies(payload: SacPolicyUpsert):
    return await upsert_cct_policies_service(payload.model_dump(exclude_none=True))


@router.post("/update_field_for_all_policies")
async def update_field_for_all_policies(payload: SacPolicyBulkFieldUpdate):
    return await update_field_for_all_policies_service(payload.model_dump())


@router.get("/get_premium")
async def get_premium(request: Request):
    return await get_premium_service(dict(request.query_params))
