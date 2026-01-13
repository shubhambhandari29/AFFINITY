from fastapi import APIRouter, Request
from services.affinity.policy_type_distribution_service import get_distribution as get_distribution_service
from services.affinity.policy_type_distribution_service import upsert_distribution as upsert_distribution_service

router = APIRouter()

@router.get("/")
async def get_distribution(request: Request):
    return await get_distribution_service(dict(request.query_params))
    
@router.post("/upsert")
async def upsert_distribution(request: Request):
    data = await request.json()
    return await upsert_distribution_service(data)
