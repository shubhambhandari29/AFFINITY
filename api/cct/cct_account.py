from fastapi import APIRouter, Depends, Request

from core.models.sac_account import SacAccountUpsert
from services.auth_service import get_current_user_from_token
from services.cct.cct_account_service import (
    get_cct_account as get_cct_account_service,
    upsert_cct_account as upsert_cct_account_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_cct_account(request: Request):
    return await get_cct_account_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_cct_account(payload: SacAccountUpsert):
    return await upsert_cct_account_service(payload.model_dump(exclude_none=True))
