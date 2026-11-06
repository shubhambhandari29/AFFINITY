from pydantic import BaseModel, Field


class HCMUserUpsert(BaseModel):
    PK_Number: int | None = Field(default=None, gt=0, strict=True)

    class Config:
        extra = "allow"
