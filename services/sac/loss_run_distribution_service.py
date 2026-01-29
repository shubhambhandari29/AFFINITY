import logging
from typing import Any
from urllib.parse import urlencode

from pydantic import EmailStr, TypeAdapter, ValidationError

from fastapi import HTTPException

from core.config import settings
from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    delete_records_async,
    fetch_records_async,
    merge_upsert_records_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)
_EMAIL_ADAPTER = TypeAdapter(EmailStr)

TABLE_NAME = "tblDistribute_LossRun"
ALLOWED_FILTERS = {"CustomerNum", "EMailAddress"}
IDENTITY_COLUMNS = {"PK_Number"}


async def get_distribution(query_params: dict[str, Any]):
    try:
        filters = sanitize_filters(query_params, ALLOWED_FILTERS)
        records = await fetch_records_async(table=TABLE_NAME, filters=filters)
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching Loss Run Distribution List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_distribution(data_list: list[dict[str, Any]]):
    try:
        normalized = [normalize_payload_dates(item) for item in data_list]
        sanitized_rows = [
            {k: v for k, v in row.items() if k not in IDENTITY_COLUMNS} for row in normalized
        ]
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=sanitized_rows,
            key_columns=["CustomerNum", "AttnTo"],
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def delete_distribution(data_list: list[dict[str, Any]]):
    try:
        return await delete_records_async(
            table=TABLE_NAME,
            data_list=data_list,
            key_columns=["CustomerNum", "AttnTo"],
        )
    except Exception as e:
        logger.warning(f"Deletion failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


def _parse_allowed_domains() -> set[str]:
    raw = settings.OUTLOOK_COMPOSE_ALLOWED_DOMAINS
    if not raw:
        return set()
    return {domain.strip().lower() for domain in raw.split(",") if domain.strip()}


def _is_email_delivery(dist_via: str | None) -> bool:
    if not dist_via:
        return True
    normalized = dist_via.lower().replace("-", "")
    return "email" in normalized


def _validate_email(value: str | None) -> str | None:
    if not value:
        return None
    try:
        normalized = _EMAIL_ADAPTER.validate_python(value)
    except ValidationError:
        return None
    return str(normalized)


def _extract_recipients(records: list[dict[str, Any]]):
    allowed_domains = _parse_allowed_domains()
    recipients: list[str] = []
    invalid_emails: list[str] = []
    seen: set[str] = set()
    filtered_out = 0

    for record in records:
        email = record.get("EMailAddress")
        if not email:
            filtered_out += 1
            continue
        if not record.get("DistVia") is "Email":
            filtered_out += 1
            continue

        normalized = _validate_email(str(email))
        if not normalized:
            invalid_emails.append(str(email))
            continue

        domain = normalized.split("@")[-1].lower()
        if allowed_domains and domain not in allowed_domains:
            filtered_out += 1
            continue

        key = normalized.lower()
        if key in seen:
            filtered_out += 1
            continue

        seen.add(key)
        recipients.append(normalized)

    return recipients, invalid_emails, filtered_out


def _build_compose_url(recipients: list[str], subject: str, body: str) -> str:
    params = urlencode(
        {
            "to": ";".join(recipients),
            "subject": subject,
            "body": body,
        }
    )
    return f"{settings.OUTLOOK_COMPOSE_BASE_URL}?{params}"


async def build_outlook_compose_link(
    entries: list[dict[str, Any]] | None,
    subject: str | None,
    body: str | None,
    requested_by: str | None,
):
    if not entries:
        raise HTTPException(
            status_code=400,
            detail={"error": "Provide entries to build the compose link"},
        )

    records = entries

    recipients, invalid_emails, filtered_out = _extract_recipients(records)
    total = len(records)

    if not recipients:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "No valid email recipients found",
                "invalid_emails": invalid_emails,
                "filtered_out": filtered_out,
                "total": total,
            },
        )

    max_recipients = settings.OUTLOOK_COMPOSE_MAX_RECIPIENTS
    if max_recipients and len(recipients) > max_recipients:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Recipient count exceeds limit",
                "max_recipients": max_recipients,
                "recipient_count": len(recipients),
            },
        )

    compose_url = _build_compose_url(
        recipients,
        subject or settings.OUTLOOK_COMPOSE_SUBJECT_TEMPLATE,
        body or settings.OUTLOOK_COMPOSE_BODY_TEMPLATE,
    )

    logger.info(
        "Outlook compose link generated by %s: recipients=%s invalid=%s filtered_out=%s total=%s",
        requested_by or "unknown",
        len(recipients),
        len(invalid_emails),
        filtered_out,
        total,
    )

    return {
        "url": compose_url,
        "recipients": recipients,
        "invalid_emails": invalid_emails,
        "filtered_out": filtered_out,
        "total": total,
    }
