from fastapi import APIRouter, Request
from services.affinity.claim_review_frequency_service import get_frequency as get_frequency_service
from services.affinity.claim_review_frequency_service import upsert_frequency as upsert_frequency_service

router = APIRouter()

@router.get("/")
async def get_frequency(request: Request):
    return await get_frequency_service(dict(request.query_params))
    
@router.post("/upsert")
async def upsert_frequency(request: Request):
    data = await request.json()
    return await upsert_frequency_service(data)
