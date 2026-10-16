from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class LossRunOptions(BaseModel):
    reportType: Literal["standard", "claim_review"] = "standard"
    lossDateFrom: date | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_old_cutoff(cls, values):
        if (
            isinstance(values, dict)
            and values.get("policyEffectiveDateFrom") is not None
        ):
            raise ValueError(
                "Use lossDateFrom (claim occurrence date); policyEffectiveDateFrom is no longer supported"
            )
        return values


class LossRunSelection(LossRunOptions):
    customerNumbers: list[str] = Field(..., min_length=1)
