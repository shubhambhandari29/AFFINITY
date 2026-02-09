from typing import Any

from pydantic import BaseModel


class AffinityDistributionEntry(BaseModel):
    ProgramName: Any | None = None
    RecipCat: Any | None = None
    DistVia: Any | None = None
    AttnTo: Any | None = None
    EMailAddress: Any | None = None

    class Config:
        extra = "allow"
