from fastapi import APIRouter, Request
from services.affinity.affinity_policy_types_service import get_affinity_policy_types as get_affinity_policy_types_service
from services.affinity.affinity_policy_types_service import upsert_affinity_policy_types as upsert_affinity_policy_types_service

router = APIRouter()

@router.get("/")
async def get_affinity_policy_types(request: Request):
    return await get_affinity_policy_types_service(dict(request.query_params))
    
@router.post("/upsert")
async def upsert_affinity_policy_types(request: Request):
    data = await request.json()
    return await upsert_affinity_policy_types_service(data)
