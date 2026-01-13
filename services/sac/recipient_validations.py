"""Shared validation helpers for recipient distribution and HCM user payloads."""

from __future__ import annotations

from typing import Any

REQUIRED_RECIPIENT_FIELDS = (
    "CustomerNum",
    "RecipCat",
    "DistVia",
    "AttnTo",
    "EMailAddress",
)

REQUIRED_HCM_FIELDS = (
    "UserName",
    "UserTitle",
    "UserEmail",
    "UserAction",
    "LanID",
)

def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _digits(value: Any) -> str:
    if value is None:
        return ""
    return "".join(filter(str.isdigit, str(value)))


def clean_and_validate_recipient_rows(data_list: list[dict[str, Any]]):
    """Filter blank rows and validate mandatory fields."""
    cleaned: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for record in data_list:
        if not any(_has_value(record.get(field)) for field in REQUIRED_RECIPIENT_FIELDS):
            # Skip completely empty rows
            continue
        cleaned.append(record)

    for idx, record in enumerate(cleaned, start=1):
        for field in REQUIRED_RECIPIENT_FIELDS:
            if not _has_value(record.get(field)):
                errors.append(
                    {
                        "field": field,
                        "code": "REQUIRED",
                        "message": f" You've left a blank a mandatory field :{field}. Please check your entries.",
                    }
                )

    return cleaned, errors


def validate_hcm_users(records: list[dict[str, Any]]):
    """Validate HCM user rows for mandatory fields and telephone length."""
    errors: list[dict[str, str]] = []

    for idx, record in enumerate(records, start=1):
        for field in REQUIRED_HCM_FIELDS:
            if not _has_value(record.get(field)):
                errors.append(
                    {
                        "field": field,
                        "code": "REQUIRED",
                        "message": f"{field} is mandatory for HCM user row {idx}.",
                    }
                )

        digits = _digits(record.get("TelNum"))
        if digits and len(digits) != 10:
            errors.append(
                {
                    "field": "TelNum",
                    "code": "INVALID_LENGTH",
                    "message": f"Telephone number must have 10 digits for row {idx}.",
                }
            )

    return errors