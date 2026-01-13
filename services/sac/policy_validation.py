"""Validation helpers for SAC policy payloads."""

from __future__ import annotations

from typing import Any

REQUIRED_FIELDS = (
    ("AccountName", "Customer Account Name is mandatory."),
    ("LocCoded", "Location Coded is mandatory."),
    ("PolicyNum", "Policy Number is mandatory."),
    ("PolMod", "Policy Mod is mandatory."),
)


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _error(field: str, message: str) -> dict[str, str]:
    return {"field": field, "code": "REQUIRED", "message": message}


def validate_policy_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for field, message in REQUIRED_FIELDS:
        if not _has_value(payload.get(field)):
            errors.append(_error(field, message))
    return errors