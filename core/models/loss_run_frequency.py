from typing import Any

from pydantic import BaseModel, Field


class LossRunFrequencyEntry(BaseModel):
    """
    base entry for loss frequency endpoints.
    """

    CustomerNum: str = Field(..., min_length=1)
    MthNum: int = Field(..., ge=1, le=12)
    RptMth: int | None = None
    CompDate: str | None = None
    RptType: Any | None = None
    DelivMeth: Any | None = None
    no_claims: bool | None = None
    AdHocReport: bool | None = None
