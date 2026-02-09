from fastapi import APIRouter, Depends, Request

from core.models.sac_account import SacAccountUpsert
from services.auth_service import get_current_user_from_token
from services.sac.sac_account_service import get_sac_account as get_sac_account_service
from services.sac.sac_account_service import (
    upsert_sac_account as upsert_sac_account_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_sac_account(request: Request):
    return await get_sac_account_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_sac_account(payload: SacAccountUpsert):
    return await upsert_sac_account_service(payload.model_dump())
