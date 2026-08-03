from pydantic import BaseModel


class HCMAccountAssociationUpsert(BaseModel):
    CustomerNum: str | None = None
    AccountNum: str | None = None
    AssociationType: str | None = None

    class Config:
        extra = "allow"
