from __future__ import annotations

from datetime import date, datetime

from core import date_utils


def test_is_sentinel_date_handles_date_and_datetime():
    assert date_utils._is_sentinel_date(date(1900, 1, 1)) is True
    assert date_utils._is_sentinel_date(datetime(1900, 1, 1, 0, 0, 0)) is True
    assert date_utils._is_sentinel_date(date(1900, 1, 2)) is False


def test_format_date_value_formats_and_handles_sentinel():
    assert date_utils.format_date_value(date(1900, 1, 1)) is None
    assert date_utils.format_date_value(datetime(1900, 1, 1, 12, 0, 0)) is None
    assert date_utils.format_date_value(date(2024, 1, 2)) == "01-02-2024"
    assert date_utils.format_date_value(datetime(2024, 1, 2, 8, 30, 0)) == "01-02-2024"
    assert date_utils.format_date_value("2024-01-02") == "01-02-2024"
    assert date_utils.format_date_value("2024-01-02T08:30:00") == "01-02-2024"
    assert date_utils.format_date_value("   ") == "   "


def test_parse_date_input_handles_formats_and_sentinel():
    assert date_utils.parse_date_input("2024-01-02") == date(2024, 1, 2)
    parsed = date_utils.parse_date_input("2024-01-02T08:30:00Z")
    assert isinstance(parsed, datetime)
    assert parsed.date() == date(2024, 1, 2)
    assert date_utils.parse_date_input("  ") is None
    assert date_utils.parse_date_input(date(1900, 1, 1)) is None
    assert date_utils.parse_date_input("not-a-date") == "not-a-date"


def test_try_parse_datetime_supports_multiple_formats():
    parsed = date_utils._try_parse_datetime("01/02/2024")
    assert isinstance(parsed, datetime)
    assert parsed.date() == date(2024, 1, 2)
    assert date_utils._try_parse_datetime("not-a-date") is None


def test_format_records_dates_auto_detects_date_like_keys():
    records = [
        {
            "StartDate": date(2024, 1, 2),
            "DtCreated": "2024/01/03",
            "Other": "keep",
        }
    ]
    result = date_utils.format_records_dates(records)
    assert result[0]["StartDate"] == "01-02-2024"
    assert result[0]["DtCreated"] == "01-03-2024"
    assert result[0]["Other"] == "keep"


def test_format_records_dates_respects_fields_allow_list():
    records = [
        {
            "StartDate": date(2024, 1, 2),
            "EndDate": date(2024, 1, 3),
        }
    ]
    result = date_utils.format_records_dates(records, fields={"EndDate"})
    assert isinstance(result[0]["StartDate"], date)
    assert result[0]["EndDate"] == "01-03-2024"


def test_normalize_payload_dates_respects_fields_allow_list():
    payload = {"StartDate": "2024-01-02", "EndDate": "2024-01-03"}
    result = date_utils.normalize_payload_dates(payload, fields={"EndDate"})
    assert result["StartDate"] == "2024-01-02"
    assert result["EndDate"] == date(2024, 1, 3)
