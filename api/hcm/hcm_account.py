from fastapi import APIRouter, Depends, Request

from core.models.hcm_account import HCMAccountUpsert
from services.auth_service import get_current_user_from_token
from services.hcm.hcm_account_service import get_hcm_account as get_hcm_account_service
from services.hcm.hcm_account_service import upsert_hcm_account as upsert_hcm_account_service

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_hcm_account(request: Request):
    return await get_hcm_account_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_hcm_account(payload: HCMAccountUpsert):
    return await upsert_hcm_account_service(payload.model_dump())
