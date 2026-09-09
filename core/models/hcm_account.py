from pydantic import BaseModel

class HCMAccountUpsert(BaseModel):
    CustomerNum: str | None = None
    class Config:
        extra = "allow"
    