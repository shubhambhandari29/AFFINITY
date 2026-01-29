"""Validation helpers for affinity payloads."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.date_utils import parse_date_input

REQUIRED_PROGRAM_FIELDS: tuple[tuple[str, str], ...] = (
    ("ProgramName", "Program Name is a mandatory field"),
    ("BranchVal", "Branch Office is a mandatory field"),
    ("OnBoardDt", "On Board Date is a mandatory field"),
)

REQUIRED_POLICY_TYPE_FIELDS: tuple[tuple[str, str], ...] = (
    ("ProgramName", "Affinity Program Name is a mandatory field"),
    ("PolicyType", "Policy Type Name is a mandatory field"),
)

REQUIRED_POLICY_TYPE_DISTRIBUTION_FIELDS: tuple[tuple[str, str], ...] = (
    ("ProgramName", "Is Not Null and Mandatory Field"),
    ("PolicyType", "Is Not Null and Mandatory Field"),
    ("RecipCat", "Is Not Null and Mandatory Field"),
    ("DistVia", "Is Not Null and Mandatory Field"),
    ("AttnTo", "Is Not Null and Mandatory Field"),
    ("EMailAddress", "Is Not Null and Mandatory Field"),
)

INACTIVE_STATUSES = {"inactive"}
DATE_FIELDS_PROGRAM: tuple[str, ...] = ("DtCreated", "OnBoardDt", "DateNotif")
DATE_FIELDS_POLICY_TYPE: tuple[str, ...] = ("DateCreated",)
DATE_FIELDS_FREQUENCY: tuple[str, ...] = ("CompDate",)
PHONE_FIELDS_AGENT: tuple[str, ...] = (
    "WorkTel1",
    "CellTel1",
    "FaxTel1",
    "WorkTel2",
    "CellTel2",
    "FaxTel2",
)

POLICY_TYPE_DEFAULTS: dict[str, Any] = {
    "AddLDocs": "No",
    "SpecHand": "Auto Assign",
}


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _error(field: str, message: str) -> dict[str, str]:
    return {"field": field, "code": "REQUIRED", "message": message}


def _number_error(field: str, message: str) -> dict[str, str]:
    return {"field": field, "code": "INVALID_VALUE", "message": message}


def _format_error(field: str, message: str) -> dict[str, str]:
    return {"field": field, "code": "INVALID_FORMAT", "message": message}


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if cleaned == "":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _is_valid_date(value: Any) -> bool:
    if value in (None, ""):
        return True
    if isinstance(value, (date, datetime)):
        return True
    if isinstance(value, str):
        parsed = parse_date_input(value)
        return isinstance(parsed, (date, datetime))
    return False


def _is_valid_phone(value: Any) -> bool:
    if value in (None, ""):
        return True
    if not isinstance(value, str):
        return False

    text = value.strip()
    if not text:
        return True

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 10:
        return False
    return True


def _apply_defaults(payload: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    for key, default_value in defaults.items():
        if not _has_value(updated.get(key)):
            updated[key] = default_value
    return updated


def validate_affinity_program_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []

    for field, message in REQUIRED_PROGRAM_FIELDS:
        if not _has_value(payload.get(field)):
            errors.append(_error(field, message))

    status = payload.get("AcctStatus")
    if isinstance(status, str) and status.strip().lower() in INACTIVE_STATUSES:
        if not _has_value(payload.get("DateNotif")):
            errors.append(
                _error(
                    "DateNotif",
                    "Notification Date is a mandatory field when account status is changed to Inactive",
                )
            )

    num_pol = payload.get("NumPol")
    if _has_value(num_pol):
        parsed = _coerce_number(num_pol)
        if parsed is None or parsed >= 99999:
            errors.append(
                _number_error("NumPol", "Invalid Value for Number of Policies")
            )

    for field in DATE_FIELDS_PROGRAM:
        if _has_value(payload.get(field)) and not _is_valid_date(payload.get(field)):
            errors.append(_format_error(field, "Invalid date format"))

    return errors


def validate_affinity_policy_type_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for field, message in REQUIRED_POLICY_TYPE_FIELDS:
        if not _has_value(payload.get(field)):
            errors.append(_error(field, message))

    for field in DATE_FIELDS_POLICY_TYPE:
        if _has_value(payload.get(field)) and not _is_valid_date(payload.get(field)):
            errors.append(_format_error(field, "Invalid date format"))
    return errors


def validate_policy_type_distribution_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []

    for record in rows:
        if not any(
            _has_value(record.get(field)) for field, _ in REQUIRED_POLICY_TYPE_DISTRIBUTION_FIELDS
        ):
            continue
        for field, message in REQUIRED_POLICY_TYPE_DISTRIBUTION_FIELDS:
            if not _has_value(record.get(field)):
                errors.append(_error(field, message))

    return errors


def apply_affinity_policy_type_defaults(payload: dict[str, Any]) -> dict[str, Any]:
    return _apply_defaults(payload, POLICY_TYPE_DEFAULTS)


def validate_affinity_agent_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []

    if not _has_value(payload.get("ProgramName")):
        errors.append(
            _error("ProgramName", "Program Name is required before Agent Details")
        )

    for field in PHONE_FIELDS_AGENT:
        if _has_value(payload.get(field)) and not _is_valid_phone(payload.get(field)):
            errors.append(_format_error(field, "Invalid phone number format"))

    return errors


def validate_affinity_frequency_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for row in rows:
        for field in DATE_FIELDS_FREQUENCY:
            if _has_value(row.get(field)) and not _is_valid_date(row.get(field)):
                errors.append(_format_error(field, "Invalid date format"))
    return errors
