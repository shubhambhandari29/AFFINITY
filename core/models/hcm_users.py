from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, EmailStr


def _alias_generator(field_name: str) -> str:
    if "_" in field_name:
        parts = field_name.split("_")
        return parts[0].lower() + "".join(part.title() for part in parts[1:])
    return field_name[0].lower() + field_name[1:]


class HCMUserUpsert(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        alias_generator=_alias_generator,
    )

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
