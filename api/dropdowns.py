from typing import Any

from fastapi import APIRouter, Depends, Response

from services.auth_service import get_current_user_from_token
from services.dropdowns_service import (
    delete_dropdown_values as delete_dropdown_values_service,
)
from services.dropdowns_service import (
    get_dropdown_values as get_dropdown_values_service,
    upsert_dropdown_values as upsert_dropdown_values_service,
)

router = APIRouter(dependencies=[Depends(get_current_user_from_token)])


@router.get("/{dropdown_name}")
async def get_dropdown(dropdown_name: str, response: Response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return await get_dropdown_values_service(dropdown_name)


@router.post("/{dropdown_name}/upsert")
async def upsert_dropdown(dropdown_name: str, payload: list[dict[str, Any]]):
    return await upsert_dropdown_values_service(dropdown_name, payload)


@router.post("/{dropdown_name}/delete")
async def delete_dropdown(dropdown_name: str, payload: list[dict[str, Any]]):
    return await delete_dropdown_values_service(dropdown_name, payload)
