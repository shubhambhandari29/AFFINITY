import logging
from typing import Any
from urllib.parse import urlencode

from fastapi import HTTPException
from core.config import Settings
from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    delete_records_async,
    fetch_records_async,
    merge_upsert_records_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblDistribute_LossRun"
ALLOWED_FILTERS = {"CustomerNum", "EMailAddress"}
IDENTITY_COLUMNS = {"PK_Number"}
OUTLOOK_COMPOSE_BASE_URL = "https://outlook.office.com/mail/deeplink/compose"


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


def extract_recipients(records: list[dict[str, Any]]):
    recipients: list[str] = []
    seen: set[str] = set()
    filtered_out = 0

    for record in records:
        email = record.get("EMailAddress")
        if not email:
            continue

        if email in seen:
            filtered_out += 1
            continue

        seen.add(email)
        recipients.append(email)

    return recipients, filtered_out



async def build_outlook_compose_link(
    entries: list[dict[str, Any]] | None,
    subject: str | None,
    body: str | None,
):
    if not entries:
        raise HTTPException(
            status_code=400,
            detail={"error": "Provide entries to build the compose link"},
        )

    recipients, filtered_out = extract_recipients(entries)
    total = len(entries)

    if not recipients:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "No valid email recipients found",
                "filtered_out": filtered_out,
                "total": total,
            },
        )

    params = urlencode(
        {
            "to": ";".join(recipients),
            "subject": subject,
            "body": body,
        }
    )
    compose_url = f"{Settings.OUTLOOK_COMPOSE_BASE_URL}?{params}"

    return {
        "url": compose_url,
        "recipients": recipients,
        "filtered_out": filtered_out,
        "total": total,
    }
