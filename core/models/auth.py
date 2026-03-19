from pydantic import BaseModel, EmailStr


class LoginRequest(BaseModel):
    email: EmailStr
    class Config:
        extra="allow"
