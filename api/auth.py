from fastapi import APIRouter, Body, Depends, Request, Response
from fastapi.security import OAuth2PasswordBearer

from core.models.auth import LoginRequest
from services.auth_service import (
    get_current_user_from_token,
    login_user,
    logout_user,
    refresh_user_token,
)

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login", auto_error=False)


@router.post("/login")
async def login(
    request: Request,
    response: Response,
    payload: LoginRequest | None = Body(default=None),
):
    return await login_user(payload.model_dump() if payload else None, response, request)


@router.get("/me")
async def get_current_user(request: Request):
    return await get_current_user_from_token(request)


@router.post("/logout")
async def logout(response: Response):
    return await logout_user(response)


@router.post("/refresh_token")
async def refresh_token(
    request: Request,
    response: Response,
    token: str | None = Depends(oauth2_scheme),
):
    return await refresh_user_token(request, response, token)
