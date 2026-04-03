from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class F5LoginRequest(BaseModel):
    user: str
    groups: Any = None
    model_config = ConfigDict(extra="allow")
