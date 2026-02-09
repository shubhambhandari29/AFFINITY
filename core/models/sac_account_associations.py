from pydantic import BaseModel, Field


class SacAccountAssociationRequest(BaseModel):
    parent_account: str = Field(..., min_length=1)
    child_account: list[str] = Field(..., min_length=1)
