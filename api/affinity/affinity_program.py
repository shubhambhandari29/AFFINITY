from fastapi import APIRouter, Depends, Request

from core.models.affinity.affinity_program import AffinityProgramUpsert
from services.affinity.affinity_program_service import (
    get_affinity_program as get_affinity_program_service,
)
from services.affinity.affinity_program_service import (
    upsert_affinity_program as upsert_affinity_program_service,
)
from services.auth_service import get_current_user_from_token

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_affinity_program(request: Request):
    return await get_affinity_program_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_affinity_program(payload: AffinityProgramUpsert):
    return await upsert_affinity_program_service(payload.model_dump())
