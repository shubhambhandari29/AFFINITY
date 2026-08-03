from datetime import date, datetime

from pydantic import BaseModel, EmailStr


class HCMUserUpsert(BaseModel):
    PK_Number: int | None = None
    UserTitle: str | None = None
    UserID: str | None = None
    UserName: str | None = None
    UserEmail: EmailStr | None = None
    TelNum: str | None = None
    TelExt: str | None = None
    UserAction: str | None = None
    CustNum: str | None = None
    LanID: str | None = None
    PROD_Password: str | None = None
    UAT_Password: str | None = None
    DateDeleted: date | None = None
    DateAdded: date | None = None
    ChangeDate: date | None = None
    InsertDateTime: datetime | None = None
    UpdateDateTime: datetime | None = None

    class Config:
        extra = "allow"
