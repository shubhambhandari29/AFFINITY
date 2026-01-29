from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

DATE_OUTPUT_FORMAT = "%m-%d-%Y"
_DEFAULT_INPUT_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%d/%m/%Y",
    "%d-%m-%Y",
)
_SENTINEL_DATE = date(1900, 1, 1)


def _is_sentinel_date(value: date | datetime) -> bool:
    if isinstance(value, datetime):
        value = value.date()
    return value == _SENTINEL_DATE


def format_records_dates(
    records: list[dict[str, Any]],
    *,
    fields: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    if not records:
        return records

    field_set = set(fields) if fields is not None else None

    for record in records:
        for key in list(record.keys()):
            if field_set is not None:
                if key not in field_set:
                    continue
            else:
                if not key:
                    continue
                lowered = key.lower()
                if not (
                    "date" in lowered
                    or lowered.endswith("dt")
                    or lowered.endswith("_dt")
                    or lowered.endswith("date")
                ):
                    continue
            record[key] = format_date_value(record.get(key))
    return records


def normalize_payload_dates(
    payload: dict[str, Any],
    *,
    fields: Iterable[str] | None = None,
) -> dict[str, Any]:
    normalized = dict(payload)
    field_set = set(fields) if fields is not None else None

    for key in list(normalized.keys()):
        if field_set is not None:
            if key not in field_set:
                continue
        else:
            if not key:
                continue
            lowered = key.lower()
            if not (
                "date" in lowered
                or lowered.endswith("dt")
                or lowered.endswith("_dt")
                or lowered.endswith("date")
            ):
                continue
        normalized[key] = parse_date_input(normalized.get(key))
    return normalized


def format_date_value(value: Any) -> Any:
    if value in (None, ""):
        return value

    if isinstance(value, datetime):
        if _is_sentinel_date(value):
            return None
        return value.strftime(DATE_OUTPUT_FORMAT)

    if isinstance(value, date):
        if _is_sentinel_date(value):
            return None
        return value.strftime(DATE_OUTPUT_FORMAT)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return value
        parsed = _try_parse_datetime(text)
        if parsed:
            if _is_sentinel_date(parsed):
                return None
            return parsed.strftime(DATE_OUTPUT_FORMAT)

    return value


def parse_date_input(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        if _is_sentinel_date(value):
            return None
        return value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        parsed = _try_parse_datetime(text)
        if parsed:
            if _is_sentinel_date(parsed):
                return None
            if "T" in text or ":" in text:
                return parsed
            return parsed.date()

    return value


def _try_parse_datetime(text: str) -> datetime | None:
    if not text:
        return None

    clean_text = text.rstrip("Z")

    try:
        return datetime.fromisoformat(clean_text)
    except ValueError:
        pass

    for fmt in _DEFAULT_INPUT_FORMATS:
        try:
            return datetime.strptime(clean_text, fmt)
        except ValueError:
            continue

    for sep in ("T", " "):
        if sep in clean_text:
            base = clean_text.split(sep, 1)[0]
            for fmt in (
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%m/%d/%Y",
                "%m-%d-%Y",
                "%d/%m/%Y",
                "%d-%m-%Y",
            ):
                try:
                    parsed = datetime.strptime(base, fmt)
                    return parsed
                except ValueError:
                    continue
    return None

