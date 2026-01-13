# services/sac/deduct_bill_frequency_service.py

import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    fetch_records_async,
    merge_upsert_records_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblDeductBillFrequency"
ORDER_BY_COLUMN = "MthNum"
ALLOWED_FILTERS_DB = {"CustNum", "MthNum"}
FILTER_MAP = {"CustomerNum": "CustNum"}


def _remap_keys(payload: dict[str, Any]) -> dict[str, Any]:
    remapped = payload.copy()
    if "CustomerNum" in remapped:
        remapped["CustNum"] = remapped.pop("CustomerNum")
    return remapped


def _restore_customer_num(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for record in records:
        if "CustNum" in record:
            record["CustomerNum"] = record.pop("CustNum")
    return records


async def get_frequency(query_params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        normalized = {FILTER_MAP.get(key, key): value for key, value in query_params.items()}
        filters = sanitize_filters(normalized, ALLOWED_FILTERS_DB)
        records = await fetch_records_async(
            table=TABLE_NAME,
            filters=filters,
            order_by=ORDER_BY_COLUMN,
        )
        restored = _restore_customer_num(records)
        return format_records_dates(restored)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching Deduct Bill frequency List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_frequency(data_list: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        payload = [normalize_payload_dates(_remap_keys(item)) for item in data_list]
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=payload,
            key_columns=["CustNum", "MthNum"],
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e