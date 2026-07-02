from fastapi import APIRouter, Depends, Request

from core.models.sac_policies import SacPolicyBulkFieldUpdate, SacPolicyUpsert
from services.auth_service import get_current_user_from_token
from services.sac.sac_policies_service import get_premium as get_premium_service
from services.sac.sac_policies_service import (
    get_underwriter_details as get_underwriter_details_service,
)
from services.sac.sac_policies_service import (
    get_sac_policies as get_sac_policies_service,
)
from services.sac.sac_policies_service import (
    update_field_for_all_policies as update_field_for_all_policies_service,
)
from services.sac.sac_policies_service import (
    upsert_sac_policies as upsert_sac_policies_service,
)

from core.models.sac_policies import (
    SacPolicyBulkFieldUpdate,
    SacPolicyUpsert,
    SacPolicySyncAccountName,
)

from services.sac.sac_policies_service import (
    sync_account_name as sync_account_name_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_sac_policies(request: Request):
    return await get_sac_policies_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_sac_policies(payload: SacPolicyUpsert):
    return await upsert_sac_policies_service(payload.model_dump())


@router.post("/update_field_for_all_policies")
async def update_field_for_all_policies(payload: list[SacPolicyBulkFieldUpdate]):
    return await update_field_for_all_policies_service(
        [item.model_dump() for item in payload])

@router.get("/get_premium")
async def get_premium(request: Request):
    return await get_premium_service(dict(request.query_params))

@router.get("/underwriter_details")
async def get_underwriter_details(request: Request):
    return await get_underwriter_details_service(dict(request.query_params))

@router.post("/sync_account_name")
async def sync_account_name(payload: SacPolicySyncAccountName):
    return await sync_account_name_service(payload.model_dump())
