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


class ComposeLinkRequest(BaseModel):
    """
    Request for building an Outlook compose link from loss run distribution data.
    """

    filters: dict[str, str] | None = None
    entries: list[DistributionEntry] | None = None
    subject: str | None = None
    body: str | None = None


class ComposeLinkResponse(BaseModel):
    """
    Response containing the compose URL and recipient diagnostics.
    """

    url: str
    recipients: list[EmailStr]
    invalid_emails: list[str] = []
    filtered_out: int = 0
    total: int = 0
