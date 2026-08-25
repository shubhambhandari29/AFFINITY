from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

LossRunJobStatus = Literal[
    "queued",
    "processing",
    "completed",
    "partially_completed",
    "failed",
]


class LossRunJobCreated(BaseModel):
    jobId: UUID
    status: LossRunJobStatus
    message: str


class LossRunFailure(BaseModel):
    customerNumber: str
    reason: str


class LossRunJobResponse(BaseModel):
    jobId: UUID
    jobType: Literal["all", "selected"]
    status: LossRunJobStatus
    phase: str | None
    requestedCount: int | None
    processedCount: int
    generatedCount: int
    failedCount: int
    requestedBy: str
    createdAt: datetime
    startedAt: datetime | None
    completedAt: datetime | None
    errorMessage: str | None
    failures: list[LossRunFailure]
