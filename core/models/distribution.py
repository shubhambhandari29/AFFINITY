from pydantic import BaseModel, EmailStr, Field


class DistributionEntry(BaseModel):
    """
    Represents a single loss run distribution recipient.
    Requires the composite key (CustomerNum + EMailAddress) but allows
    additional columns to pass through to the DB layer unchanged.
    """

    CustomerNum: str = Field(..., min_length=1)
    RecipCat: str | None = None
    DistVia: str | None = None
    AttnTo: str | None = None
    EMailAddress: EmailStr | None = None

    class Config:
        extra = "allow"
