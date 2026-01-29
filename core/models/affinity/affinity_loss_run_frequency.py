from typing import Any

from pydantic import BaseModel


class AffinityLossRunFrequencyEntry(BaseModel):
    ProgramName: Any | None = None
    MthNum: Any | None = None
    RptMth: Any | None = None
    CompDate: Any | None = None
    RptType: Any | None = None
    DelivMeth: Any | None = None
    no_claims: bool | None = None
    AdHocReport: bool | None = None

    class Config:
        extra = "allow"
