from pydantic import BaseModel


class HCMAccountUpsert(BaseModel):
    CustomerNum: str | None = None
    CustomerName: str | None = None
    AccountStatus: str | None = None

    class Config:
        extra = "allow"
