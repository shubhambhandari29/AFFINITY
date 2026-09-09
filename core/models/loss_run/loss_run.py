from pydantic import BaseModel, Field


class LossRunSelection(BaseModel):
    customerNumbers: list[str] = Field(..., min_length=1)
