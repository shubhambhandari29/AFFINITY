from fastapi import APIRouter, Depends, Query

from services.auth_service import get_current_user_from_token
from services.sac.search_sac_account_service import (
    search_sac_account_records as get_sac_account_records_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_sac_account_records(search_by: str = Query(..., alias="search_by")):
    return await get_sac_account_records_service(search_by)
