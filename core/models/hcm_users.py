
from pydantic import BaseModel

class HCMUserUpsert(BaseModel):
    PK_Number: int | None = None
    class Config:
        extra = "allow"
