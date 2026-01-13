from pydantic import BaseModel, Field


class DeductBillFrequencyEntry(BaseModel):
    """
    base entry for deduct frequency endpoint.
    """

    CustomerNum: str = Field(..., min_length=1)
    MthNum: int = Field(..., ge=1, le=12)
    RptMth: int | None = None