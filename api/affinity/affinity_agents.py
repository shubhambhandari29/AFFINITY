from fastapi import APIRouter, Depends, Request

from core.models.affinity.affinity_agents import AffinityAgentUpsert
from services.auth_service import get_current_user_from_token
from services.affinity.affinity_agents_service import get_affinity_agents as get_affinity_agents_service
from services.affinity.affinity_agents_service import upsert_affinity_agents as upsert_affinity_agents_service

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])

@router.get("/")
async def get_affinity_agents(request: Request):
    return await get_affinity_agents_service(dict(request.query_params))
    
@router.post("/upsert")
async def upsert_affinity_agents(payload: list[AffinityAgentUpsert] | AffinityAgentUpsert):
    if isinstance(payload, list):
        entries = [entry.model_dump() for entry in payload]
    else:
        entries = [payload.model_dump()]
    return await upsert_affinity_agents_service(entries)
