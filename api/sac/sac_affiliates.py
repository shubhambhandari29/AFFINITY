from fastapi import APIRouter, Depends, Request

from core.models.sac_affiliates import SacAffiliateUpsert
from services.auth_service import get_current_user_from_token
from services.sac.sac_affiliates_service import get_affiliates as get_affiliates_service
from services.sac.sac_affiliates_service import (
    upsert_affiliates as upsert_affiliates_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_affiliates(request: Request):
    return await get_affiliates_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_affiliates(payload: list[SacAffiliateUpsert]):
    data = [item.model_dump() for item in payload]
    return await upsert_affiliates_service(data)
