from pydantic import BaseModel


class HCMAccountAssociationUpsert(BaseModel):
    CustomerNum: str | None = None

    class Config:
        extra = "allow"
