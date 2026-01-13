from typing import Any

from pydantic import BaseModel


class AffinityProgramUpsert(BaseModel):
    ProgramName: Any | None = None

    class Config:
        extra = "allow"
