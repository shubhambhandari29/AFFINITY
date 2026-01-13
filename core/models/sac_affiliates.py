from pydantic import BaseModel, Field


class SacAffiliateUpsert(BaseModel):
    PK_Number: int | None = None
    CustomerNum: str = Field(..., min_length=1)
    AffiliateName: str = Field(..., min_length=1)
