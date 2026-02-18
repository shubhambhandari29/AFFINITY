from __future__ import annotations

from datetime import date, datetime

import pytest

from services.validations.affinity_validations import (
    POLICY_TYPE_DEFAULTS,
    _apply_defaults,
    _coerce_number,
    _has_value,
    _is_valid_date,
    _is_valid_phone,
    apply_affinity_policy_type_defaults,
    validate_affinity_agent_payload,
    validate_affinity_frequency_rows,
    validate_affinity_policy_type_payload,
    validate_affinity_program_payload,
    validate_policy_type_distribution_rows,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, False),
        ("", False),
        ("   ", False),
        ("x", True),
        (0, True),
        (False, True),
    ],
)
def test_has_value(value, expected):
    assert _has_value(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, None),
        (10, 10.0),
        (12.5, 12.5),
        ("1,234", 1234.0),
        ("  ", None),
        ("abc", None),
    ],
)
def test_coerce_number(value, expected):
    assert _coerce_number(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, True),
        ("", True),
        (date(2024, 1, 1), True),
        (datetime(2024, 1, 1, 10, 5, 0), True),
        ("2024-01-15", True),
        ("not-a-date", False),
        (123, False),
    ],
)
def test_is_valid_date(value, expected):
    assert _is_valid_date(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, True),
        ("", True),
        ("   ", True),
        ("(312) 555-1212", True),
        ("312-555-1212", True),
        ("12345", False),
        (123, False),
    ],
)
def test_is_valid_phone(value, expected):
    assert _is_valid_phone(value) is expected


def test_apply_defaults_fills_missing_and_blank():
    payload = {"AddLDocs": "", "SpecHand": None, "Other": "Keep"}
    result = _apply_defaults(payload, {"AddLDocs": "No", "SpecHand": "Auto Assign"})
    assert result["AddLDocs"] == "No"
    assert result["SpecHand"] == "Auto Assign"
    assert result["Other"] == "Keep"
    assert payload["AddLDocs"] == ""


def test_apply_affinity_policy_type_defaults_uses_policy_defaults():
    payload = {"AddLDocs": ""}
    result = apply_affinity_policy_type_defaults(payload)
    assert result["AddLDocs"] == POLICY_TYPE_DEFAULTS["AddLDocs"]
    assert result["SpecHand"] == POLICY_TYPE_DEFAULTS["SpecHand"]


def test_validate_affinity_program_payload_required_and_inactive():
    payload = {
        "ProgramName": "Test Program",
        "BranchVal": "NY",
        "OnBoardDt": "2024-01-15",
        "AcctStatus": "Inactive",
    }
    errors = validate_affinity_program_payload(payload)
    fields = {error["field"] for error in errors}
    assert "DateNotif" in fields


def test_validate_affinity_program_payload_invalid_num_policies_and_date():
    payload = {
        "ProgramName": "Test Program",
        "BranchVal": "NY",
        "OnBoardDt": "bad-date",
        "NumPol": "100000",
    }
    errors = validate_affinity_program_payload(payload)
    fields = {error["field"] for error in errors}
    assert "OnBoardDt" in fields
    assert "NumPol" in fields


def test_validate_affinity_program_payload_valid_payload_has_no_errors():
    payload = {
        "ProgramName": "Test Program",
        "BranchVal": "NY",
        "OnBoardDt": "2024-01-15",
        "NumPol": "12,000",
    }
    assert validate_affinity_program_payload(payload) == []


def test_validate_affinity_policy_type_payload_required_and_date():
    payload = {"ProgramName": "", "PolicyType": None, "DateCreated": "bad-date"}
    errors = validate_affinity_policy_type_payload(payload)
    fields = {error["field"] for error in errors}
    assert {"ProgramName", "PolicyType", "DateCreated"} <= fields


def test_validate_policy_type_distribution_rows_skips_empty_rows():
    rows = [
        {
            "ProgramName": "",
            "PolicyType": "",
            "RecipCat": "",
            "DistVia": "",
            "AttnTo": "",
            "EMailAddress": "",
        }
    ]
    assert validate_policy_type_distribution_rows(rows) == []


def test_validate_policy_type_distribution_rows_requires_all_when_any_present():
    rows = [{"ProgramName": "Affinity One", "PolicyType": ""}]
    errors = validate_policy_type_distribution_rows(rows)
    fields = {error["field"] for error in errors}
    assert "PolicyType" in fields
    assert "RecipCat" in fields
    assert "DistVia" in fields
    assert "AttnTo" in fields
    assert "EMailAddress" in fields


def test_validate_affinity_agent_payload_requires_program_and_valid_phone():
    payload = {"WorkTel1": "123", "ProgramName": ""}
    errors = validate_affinity_agent_payload(payload)
    fields = {error["field"] for error in errors}
    assert "ProgramName" in fields
    assert "WorkTel1" in fields


def test_validate_affinity_frequency_rows_invalid_date():
    rows = [{"CompDate": "bad-date"}]
    errors = validate_affinity_frequency_rows(rows)
    assert errors
