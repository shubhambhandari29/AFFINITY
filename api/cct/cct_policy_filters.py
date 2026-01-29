from fastapi import APIRouter, Depends, Query

from services.auth_service import get_current_user_from_token
from services.cct.cct_policy_filters_service import (
    get_cct_policy_numbers as get_cct_policy_numbers_service,
    get_cct_policy_statuses as get_cct_policy_statuses_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/policy_statuses")
async def get_cct_policy_statuses(customer_num: str = Query(..., alias="customer_num")):
    return await get_cct_policy_statuses_service(customer_num)


@router.get("/policy_numbers")
async def get_cct_policy_numbers(customer_num: str = Query(..., alias="customer_num")):
    return await get_cct_policy_numbers_service(customer_num)
