from fastapi import APIRouter, Depends, Query

from services.affinity.search_affinity_programs_service import (
    search_affinity_porgram_records as get_affinity_program_records_service,
)
from services.auth_service import get_current_user_from_token

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_affinity_program_records(search_by: str = Query(..., alias="search_by")):
    return await get_affinity_program_records_service(search_by)
