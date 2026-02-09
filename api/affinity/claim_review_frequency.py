from fastapi import APIRouter, Depends, Request

from core.models.affinity.affinity_claim_review_frequency import (
    AffinityClaimReviewFrequencyEntry,
)
from services.affinity.claim_review_frequency_service import (
    get_frequency as get_frequency_service,
)
from services.affinity.claim_review_frequency_service import (
    upsert_frequency as upsert_frequency_service,
)
from services.auth_service import get_current_user_from_token

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_frequency(request: Request):
    return await get_frequency_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_frequency(payload: list[AffinityClaimReviewFrequencyEntry]):
    items = [entry.model_dump() for entry in payload]
    return await upsert_frequency_service(items)
