"""Validation helpers for SAC account payloads."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

DEFAULT_ROLE = "Underwriter"

UNDERWRITER_FIELDS = {
    "AcctStatus",
    "CustomerName",
    "CustomerNum",
    "OnBoardDate",
    "BranchName", 
    "DateNotif",
    "TermDate",
    "TermCode",
    "MarketSegmentation",
    "AccountNotes",
    "InsuredWebsite",
    "NCMType",
    "NCMStatus",
    "NCMStartDt",
}

DIRECTOR_ADDITIONAL_FIELDS = {
    "RelatedEnt",
    "SAC_Contact1",
    "LossCtlRep1",
    "SAC_Contact2",
    "LossCtlRep2",
    "AcctOwner",
    "RiskSolMgr",
    "OBMethod",
    "HCMAccess",
    "BusinessType",
    "LossRunDistFreq",
    "DeductDistFreq",
    "ClaimRevDistFreq",
    "CRThresh",
    "LossRunReportRecipient",
    "DeductCheckAll",
    "DeductUnCheckAll",
    "DeductReportRecipient",
    "ClaimRevCheckAll",
    "ClaimRevUnCheckAll",
    "ClaimRevReportRecipient",
}

ROLE_FIELD_PERMISSIONS = {
    "Underwriter": UNDERWRITER_FIELDS,
    "Director": UNDERWRITER_FIELDS | DIRECTOR_ADDITIONAL_FIELDS,
}

REQUIRED_FIELDS: tuple[tuple[str, str], ...] = (
    ("CustomerNum", "Customer Number is mandatory and cannot be empty."),
    ("CustomerName", "Account Name is mandatory and cannot be empty."),
    ("OnBoardDate", "On Board Date is mandatory and cannot be empty."),
    ("BranchName", "Branch Name is mandatory and cannot be empty."),
)

INACTIVE_DEPENDENCIES: tuple[tuple[str, str], ...] = (
    ("DateNotif", "Notification Date is mandatory when account is Inactive."),
    ("TermDate", "Termination Date is mandatory when account is Inactive."),
    ("TermCode", "Termination Reason is mandatory when account is Inactive.")
)

ZERO_REQUIRED_LEVELS = {
    "Deductible Billing - Special Accounts",
    "Loss Run",
    "Deductible Billing - Paragon",
    "Inactive",
}


def _normalize_role(role: str | None) -> str:
    if not role:
        return DEFAULT_ROLE
    lowered = role.strip().lower()
    if lowered == "admin":
        return "Admin"
    if lowered == "director":
        return "Director"
    return DEFAULT_ROLE


def _is_enabled(field: str, role: str) -> bool:
    if role == "Admin":
        return True
    allowed = ROLE_FIELD_PERMISSIONS.get(role)
    if not allowed:
        return True
    return field in allowed


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _error(field: str, code: str, message: str) -> dict[str, str]:
    return {"field": field, "code": code, "message": message}


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _service_level_conflict(payload: dict[str, Any]) -> bool:
    level = payload.get("ServLevel")
    if not level:
        return False

    total_value = _coerce_number(payload.get("TotalPrem"))

    if level in ZERO_REQUIRED_LEVELS:
        return total_value is None or total_value != 0

    if total_value is None:
        return False

    if level == "Comprehensive":
        return total_value < 750000
    if level == "Enhanced":
        return total_value < 500000 or total_value > 750000
    if level == "Essential":
        return total_value < 250000 or total_value > 500000
    if level == "Primary":
        return total_value < 150000 or total_value > 250000
    if level == "Exception":
        return total_value < 0 or total_value > 150000

    return False


def validate_account_payload(payload: dict[str, Any], role: str | None) -> list[dict[str, str]]:
    """
    Validate the SAC account payload and return a list of error dicts.
    """

    normalized_role = _normalize_role(role)
    errors: list[dict[str, str]] = []

    for field, message in REQUIRED_FIELDS:
        if _is_enabled(field, normalized_role) and not _has_value(payload.get(field)):
            errors.append(_error(field, "REQUIRED", message))

    if payload.get("AcctStatus") == "Inactive":
        for field, message in INACTIVE_DEPENDENCIES:
            if _is_enabled(field, normalized_role) and not _has_value(payload.get(field)):
                errors.append(_error(field, "REQUIRED", message))

    override = payload.get("ServiceLevelOverride") in (True, "true", "True", "1", 1)
    if (
        _is_enabled("ServLevel", normalized_role)
        and _is_enabled("TotalPrem", normalized_role)
        and not override
        and _service_level_conflict(payload)
    ):
        errors.append(
            _error(
                "ServLevel",
                "SERVICE_LEVEL_CONFLICT",
                "Total Active Policy Premium is in conflict with Service Level.",
            )
        )

    return errors