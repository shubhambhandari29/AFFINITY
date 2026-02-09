from typing import Any

from pydantic import BaseModel


class AffinityPolicyTypeUpsert(BaseModel):
    ProgramName: Any | None = None
    PolicyType: Any | None = None

    class Config:
        extra = "allow"
