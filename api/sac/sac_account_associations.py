from fastapi import APIRouter, Depends, Request

from core.models.sac_account_associations import SacAccountAssociationRequest
from services.auth_service import get_current_user_from_token
from services.sac.sac_account_associations_service import (
    add_associations as add_associations_service,
)
from services.sac.sac_account_associations_service import (
    delete_associations as delete_associations_service,
)
from services.sac.sac_account_associations_service import (
    get_associations as get_associations_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_associations(request: Request):
    return await get_associations_service(dict(request.query_params))


@router.post("/add")
async def add_associations(payload: SacAccountAssociationRequest):
    return await add_associations_service(payload.model_dump())


@router.post("/delete")
async def delete_associations(payload: SacAccountAssociationRequest):
    return await delete_associations_service(payload.model_dump())
