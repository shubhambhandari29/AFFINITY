from pydantic import BaseModel, EmailStr


class HCMUserUpsert(BaseModel):
    UserTitle: str | None = None
    UserName: str | None = None
    UserEmail: EmailStr | None = None
    UserAction: str | None = None

    class Config:
        extra = "allow"
