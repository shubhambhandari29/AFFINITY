from fastapi import APIRouter, Depends

from core.models.outlook_compose import OutlookComposeRequest
from core.outlook_compose import build_compose_link
from services.auth_service import get_current_user_from_token

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.post("/compose_link")
async def build_compose_link_handler(payload: OutlookComposeRequest):
    return build_compose_link(
        recipients=payload.recipients,
        subject=payload.subject,
        body=payload.body,
    )
