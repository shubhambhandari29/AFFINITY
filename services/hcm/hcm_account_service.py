import logging
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    fetch_records_async,
    insert_records_async,
    merge_upsert_records_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblHcmAccount"
PRIMARY_KEY = "CustomerNum"
EXCLUDE_COLUMNS = {"PK_Number", "AcctSpecialKey"}
DATETIME_FIELDS = {"OnBoardDate", "DateCreated", "TermDate", "DateNotif", "RenewLetterDt"}


def _sanitize_account_record(record: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in record.items() if k not in EXCLUDE_COLUMNS}


def _normalize_hcm_account_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_payload_dates(payload)
    for field in DATETIME_FIELDS:
        value = normalized.get(field)
        if isinstance(value, date) and not isinstance(value, datetime):
            normalized[field] = datetime.combine(value, datetime.min.time())
    return normalized


async def get_hcm_account(query_params: dict[str, Any]):
    try:
        filters = sanitize_filters(query_params)
        records = await fetch_records_async(table=TABLE_NAME, filters=filters)
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching HCM account records - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_hcm_account(data: dict[str, Any]):
    try:
        normalized_data = _normalize_hcm_account_payload(data)
        sanitized_data = _sanitize_account_record(normalized_data)
        pk_value = sanitized_data.get(PRIMARY_KEY)
        if pk_value in (None, ""):
            sanitized_insert = {k: v for k, v in sanitized_data.items() if k != PRIMARY_KEY}
            return await insert_records_async(table=TABLE_NAME, records=[sanitized_insert])

        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=[sanitized_data],
            key_columns=[PRIMARY_KEY]
        )
    except Exception as e:
        logger.warning(f"HCM account upsert failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
