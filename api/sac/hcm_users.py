from fastapi import APIRouter, Depends, Request

from core.models.hcm_users import HCMUserUpsert
from services.auth_service import get_current_user_from_token
from services.sac.hcm_users_service import get_hcm_users as get_hcm_users_service
from services.sac.hcm_users_service import upsert_hcm_users as upsert_hcm_users_service

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_hcm_users(request: Request):
    return await get_hcm_users_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_hcm_users(payload: list[HCMUserUpsert]):
    data = [item.model_dump(exclude_none=True) for item in payload]
    return await upsert_hcm_users_service(data)
