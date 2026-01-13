"""Validation helpers for affinity payloads."""

from __future__ import annotations

from typing import Any

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

    return errors


def validate_affinity_policy_type_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for field, message in REQUIRED_POLICY_TYPE_FIELDS:
        if not _has_value(payload.get(field)):
            errors.append(_error(field, message))
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
