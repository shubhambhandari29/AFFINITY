from pydantic import BaseModel, Field


class HCMAccountAssociationRequest(BaseModel):
    parent_account: str = Field(..., min_length=1)
    child_account: list[str] = Field(..., min_length=1)
