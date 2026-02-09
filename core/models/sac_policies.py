from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SacPolicyUpsert(BaseModel):
    """
    Payload for inserting or updating a SAC policy record.
    PK_Number (identity column) is preferred when updating an existing row.
    """

    PK_Number: int | None = None
    CustomerNum: str = Field(..., min_length=1)
    PolicyNum: str = Field(..., min_length=1)
    PolMod: str = Field(..., min_length=1)
    AccountActiveYN: str | None = None
    AccountName: str | None = None
    AcctOnPolicyName: str | None = None
    PremiumAmt: str | None = None
    PolicyType: str | None = None
    AgentCode: str | None = None
    AgentName: str | None = None
    InceptDate: str | None = None

    class Config:
        extra = "allow"

    @field_validator("PremiumAmt", mode="before")
    @classmethod
    def _ensure_premium_is_string(cls, value: Any) -> str | None:
        return normalize_money_string(value)


class SacPolicyBulkFieldUpdate(BaseModel):
    """
    Request body for /sac_policies/update_field_for_all_policies.
    """

    fieldName: str
    fieldValue: Any
    updateVia: str
    updateViaValue: str = Field(..., min_length=1)

    class Config:
        extra = "allow"


def normalize_money_string(value: Any) -> str | None:
    """
    Convert numeric types returned from UI/DB (ints, floats, Decimal) into a clean
    string without unnecessary trailing zeroes. Non-numeric inputs are returned
    unchanged so free-form premium notes are preserved.
    """
    if value in (None, ""):
        return value

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return value
        candidate = stripped
    else:
        candidate = str(value)

    try:
        decimal_value = Decimal(candidate)
    except (InvalidOperation, TypeError, ValueError):
        return value if isinstance(value, str) else str(value)

    normalized = decimal_value.normalize()
    as_text = format(normalized, "f")
    if "." in as_text:
        as_text = as_text.rstrip("0").rstrip(".")
    return as_text or "0"
