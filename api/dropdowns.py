from fastapi import APIRouter, Depends, Response

from services.auth_service import get_current_user_from_token
from services.dropdowns_service import (
    DROPDOWN_CACHE_TTL_SECONDS,
)
from services.dropdowns_service import (
    get_dropdown_values as get_dropdown_values_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/{dropdown_name}")
async def get_dropdown(dropdown_name: str, response: Response):
    response.headers["Cache-Control"] = f"private, max-age={DROPDOWN_CACHE_TTL_SECONDS}"
    return await get_dropdown_values_service(dropdown_name)
