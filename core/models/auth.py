from pydantic import BaseModel, ConfigDict, EmailStr


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class F5LoginRequest(BaseModel):
    user_id: str
    model_config = ConfigDict(extra="allow")
