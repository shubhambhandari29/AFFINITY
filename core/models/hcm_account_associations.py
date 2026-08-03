from pydantic import BaseModel, ConfigDict


def _alias_generator(field_name: str) -> str:
    if "_" in field_name:
        parts = field_name.split("_")
        return parts[0].lower() + "".join(part.title() for part in parts[1:])
    return field_name[0].lower() + field_name[1:]


class HCMAccountAssociationUpsert(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
        alias_generator=_alias_generator,
    )

    PK_Number: int | None = None
    ParentAccount: str | None = None
    AssociatedAccount: str | None = None
