from typing import Any

from pydantic import BaseModel


class AffinityProgramUpsert(BaseModel):
    ProgramName: str | None = None
    BranchVal: str | None = None
    OnBoardDt: Any | None = None
    AcctStatus: str | None = None

    class Config:
        extra = "allow"
