from fastapi import APIRouter, Request
from services.affinity.search_affinity_programs_service import search_affinity_porgram_records as get_affinity_program_records_service

router = APIRouter()

@router.get("/")
async def get_sac_policies_records(request: Request):
    return await get_affinity_program_records_service(dict(request.query_params)['search_by'])