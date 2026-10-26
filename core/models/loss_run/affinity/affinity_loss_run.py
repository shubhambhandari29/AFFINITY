"""Request and response models for Affinity loss-run generation."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from core.models.loss_run.loss_run_job import LossRunJobResponse


class AffinityLossRunSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    programIds: list[Annotated[int, Field(gt=0)]] = Field(min_length=1)


class AffinityLossRunFailure(BaseModel):
    programId: int
    reason: str


class AffinityLossRunJobResponse(LossRunJobResponse):
    failures: list[AffinityLossRunFailure]
