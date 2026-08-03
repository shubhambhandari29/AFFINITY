from fastapi import APIRouter, Depends, Query

from services.auth_service import get_current_user_from_token
from services.hcm.search_hcm_account_service import (
    search_hcm_account_records as get_hcm_account_records_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_hcm_account_records(search_by: str = Query(..., alias="search_by")):
    return await get_hcm_account_records_service(search_by)
