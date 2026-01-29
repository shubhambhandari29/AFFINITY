from pydantic import BaseModel, Field


class OutlookComposeRequest(BaseModel):
    recipients: list[str] = Field(..., min_length=1)
    subject: str | None = None
    body: str | None = None


class OutlookComposeResponse(BaseModel):
    url: str
    recipients: list[str]
    invalid_emails: list[str] = []
    filtered_out: int = 0
    total: int = 0
