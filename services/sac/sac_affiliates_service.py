import logging
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

TABLE_NAME = "tblAffiliates"
PRIMARY_KEY = "PK_Number"


async def get_affiliates(query_params: dict[str, Any]):
    try:
        filters = sanitize_filters(query_params)
        records = await fetch_records_async(table=TABLE_NAME, filters=filters)
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching SAC affiliates - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_affiliates(data_list: list[dict[str, Any]]):
    try:
        normalized_records = [normalize_payload_dates(item) for item in data_list]
        to_update: list[dict[str, Any]] = []
        to_insert: list[dict[str, Any]] = []

        for record in normalized_records:
            pk_value = record.get(PRIMARY_KEY)
            if pk_value in (None, ""):
                sanitized_record = {k: v for k, v in record.items() if k != PRIMARY_KEY}
                if sanitized_record:
                    to_insert.append(sanitized_record)
            else:
                to_update.append(record)

        if to_update:
            await merge_upsert_records_async(
                table=TABLE_NAME,
                data_list=to_update,
                key_columns=[PRIMARY_KEY],
                exclude_key_columns_from_insert=True,
            )

        if to_insert:
            await insert_records_async(table=TABLE_NAME, records=to_insert)

        return {"message": "Transaction successful", "count": len(data_list)}
    except Exception as e:
        logger.warning(f"SAC affiliates upsert failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e