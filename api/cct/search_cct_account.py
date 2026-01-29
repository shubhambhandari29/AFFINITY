from fastapi import APIRouter, Depends, Query

from services.auth_service import get_current_user_from_token
from services.cct.search_cct_account_service import (
    search_cct_account_records as get_cct_account_records_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_cct_account_records(search_by: str = Query(..., alias="search_by")):
    return await get_cct_account_records_service(search_by)
