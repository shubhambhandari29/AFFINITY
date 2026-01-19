import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import fetch_records_async, merge_upsert_records_async, sanitize_filters
from services.validations.affinity_validations import validate_affinity_frequency_rows

logger = logging.getLogger(__name__)

TABLE_NAME = "tblLossRunFreq_AFFIN"
ORDER_BY_COLUMN = "MthNum"
KEY_COLUMNS = ["ProgramName", "MthNum"]


async def get_frequency(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblLossRunFreq_AFFIN.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        filters = sanitize_filters(query_params)
        records = await fetch_records_async(
            table=TABLE_NAME,
            filters=filters,
            order_by=ORDER_BY_COLUMN,
        )
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching Loss Run frequency List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_frequency(data_list: list[dict[str, Any]]):
    """
    Takes an array of maps as data.
    Update row if already exists, else insert row into tblLossRunFreq_AFFIN.
    """

    try:
        errors = validate_affinity_frequency_rows(data_list)
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        payload = [normalize_payload_dates(item) for item in data_list]
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=payload,
            key_columns=KEY_COLUMNS,
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
