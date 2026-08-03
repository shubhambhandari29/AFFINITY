from fastapi import APIRouter, Depends, Request

from core.models.hcm_account_associations import HCMAccountAssociationUpsert
from services.auth_service import get_current_user_from_token
from services.hcm.hcm_account_associations_service import (
    get_hcm_account_associations as get_hcm_account_associations_service,
)
from services.hcm.hcm_account_associations_service import (
    upsert_hcm_account_associations as upsert_hcm_account_associations_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_hcm_account_associations(request: Request):
    return await get_hcm_account_associations_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_hcm_account_associations(payload: list[HCMAccountAssociationUpsert]):
    data = [item.model_dump() for item in payload]
    return await upsert_hcm_account_associations_service(data)
