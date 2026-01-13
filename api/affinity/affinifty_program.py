from fastapi import APIRouter, Request
from services.affinity.affinity_program_service import get_affinity_program as get_affinity_program_service
from services.affinity.affinity_program_service import upsert_affinity_program as upsert_affinity_program_service

router = APIRouter()

@router.get("/")
async def get_affinity_program(request: Request):
    return await get_affinity_program_service(dict(request.query_params))
    
@router.post("/upsert")
async def upsert_affinity_program(request: Request):
    data = await request.json()
    return await upsert_affinity_program_service(data)
