from fastapi import APIRouter, Depends, Request

from core.models.claim_review_frequency import ClaimReviewFrequencyEntry
from services.auth_service import get_current_user_from_token
from services.sac.claim_review_frequency_service import (
    get_frequency as get_frequency_service,
)
from services.sac.claim_review_frequency_service import (
    upsert_frequency as upsert_frequency_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/")
async def get_frequency(request: Request):
    return await get_frequency_service(dict(request.query_params))


@router.post("/upsert")
async def upsert_frequency(payload: list[ClaimReviewFrequencyEntry]):
    items = [entry.model_dump() for entry in payload]
    return await upsert_frequency_service(items)
