from pydantic import BaseModel


class HCMAccountAssociationUpsert(BaseModel):
    PK_Number: int | None = None
    ParentAccount: str | None = None
    AssociatedAccount: str | None = None

    class Config:
        extra = "allow"
