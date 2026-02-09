from typing import Any

from pydantic import BaseModel, Field


class ClaimReviewFrequencyEntry(BaseModel):
    """
    base entry for claim frequency endpoint.
    """

    CustomerNum: str = Field(..., min_length=1)
    MthNum: int = Field(..., ge=1, le=12)
    RptMth: int | None = None
    CompDate: str | None = None
    RptType: Any | None = None
    DelivMeth: Any | None = None
    CRNumNarr: int | None = None
