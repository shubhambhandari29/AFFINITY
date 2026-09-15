from typing import Literal

from pydantic import BaseModel, Field


class LossRunOptions(BaseModel):
    reportType: Literal["standard", "claim_review"] = "standard"


class LossRunSelection(LossRunOptions):
    customerNumbers: list[str] = Field(..., min_length=1)
