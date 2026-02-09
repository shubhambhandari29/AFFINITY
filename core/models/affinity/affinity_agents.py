from typing import Any

from pydantic import BaseModel


class AffinityAgentUpsert(BaseModel):
    ProgramName: Any | None = None
    AgentCode: Any | None = None

    class Config:
        extra = "allow"
