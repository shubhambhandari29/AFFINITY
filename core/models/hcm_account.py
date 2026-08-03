from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


def _alias_generator(field_name: str) -> str:
    if "_" in field_name:
        parts = field_name.split("_")
        return parts[0].lower() + "".join(part.title() for part in parts[1:])
    return field_name[0].lower() + field_name[1:]


class HCMAccountUpsert(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        alias_generator=_alias_generator,
    )

    CustomerNum: str | None = None
    AcctSpecialKey: int | None = None
    CustomerName: str | None = None
    OnBoardDate: datetime | None = None
    AccountNotes: str | None = None
    DateCreated: datetime | None = None
    CreatedBy: str | None = None
    SAC_Contact1: str | None = None
    SAC_Contact2: str | None = None
    LossCtlRep1: str | None = None
    LossCtlRep2: str | None = None
    BranchName: str | None = None
    HCMAccess: str | None = None
    DiscDate: str | None = None
    AcctOwner: str | None = None
    OBMethod: str | None = None
    CRThresh: float | None = None
    AcctStatus: str | None = None
    TermDate: datetime | None = None
    TermCode: str | None = None
    SACApproved: str | None = None
    DateNotif: datetime | None = None
    RiskSolMgr: str | None = None
    HCM_LOC_ONLY: str | None = None
    RenewLetterDt: datetime | None = None
    RelatedEnt: str | None = None
    ChangeNotes: str | None = None
    Stage: str | None = None
    IsSubmitted: int | None = None
    MarketSegmentation: str | None = None
    EffectiveDate: date | None = None
    InsertDateTime: datetime | None = None
    UpdateDateTime: datetime | None = None
