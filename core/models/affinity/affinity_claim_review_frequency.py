from typing import Any

from pydantic import BaseModel


class AffinityClaimReviewFrequencyEntry(BaseModel):
    ProgramName: Any | None = None
    MthNum: Any | None = None
    RptMth: Any | None = None
    CompDate: Any | None = None
    RptType: Any | None = None
    DelivMeth: Any | None = None
    CRNumNarr: Any | None = None

    class Config:
        extra = "allow"
