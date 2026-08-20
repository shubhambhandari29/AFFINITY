from pydantic import BaseModel, Field


class DistributionEntry(BaseModel):
    """
    Represents a single loss run distribution recipient.
    Uses the composite key (CustomerNum + AttnTo + RecipCat) and allows
    additional columns to pass through to the DB layer unchanged.
    """

    CustomerNum: str = Field(..., min_length=1)
    RecipCat: str | None = None
    DistVia: str | None = None
    AttnTo: str | None = None
    EMailAddress: str | None = None

    class Config:
        extra = "allow"
