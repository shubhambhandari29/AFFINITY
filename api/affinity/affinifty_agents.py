from fastapi import APIRouter, Request
from services.affinity.affinity_agents_service import get_affinity_agents as get_affinity_agents_service
from services.affinity.affinity_agents_service import upsert_affinity_agents as upsert_affinity_agents_service

router = APIRouter()

@router.get("/")
async def get_affinity_agents(request: Request):
    return await get_affinity_agents_service(dict(request.query_params))
    
@router.post("/upsert")
async def upsert_affinity_agents(request: Request):
    data = await request.json()
    return await upsert_affinity_agents_service(data)
