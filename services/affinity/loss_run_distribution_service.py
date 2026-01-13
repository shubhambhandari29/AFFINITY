import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    delete_records_async,
    fetch_records_async,
    merge_upsert_records_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblDist_LossRun_AFFIN"
KEY_COLUMNS = ["ProgramName", "EMailAddress"]


async def get_distribution(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblDist_LossRun_AFFIN.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        filters = sanitize_filters(query_params)
        records = await fetch_records_async(table=TABLE_NAME, filters=filters)
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching Loss Run Distribution List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_distribution(data_list: list[dict[str, Any]]):
    """
    Takes an array of maps as data.
    Update row if already exists, else insert row into tblDist_LossRun_AFFIN.
    """

    try:
        normalized = [normalize_payload_dates(item) for item in data_list]
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=normalized,
            key_columns=KEY_COLUMNS,
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def delete_distribution(data_list: list[dict[str, Any]]):
    """
    Takes an array of maps as data.
    Deletes matching rows from tblDist_LossRun_AFFIN.
    Matching is based on ProgramName + EMailAddress.
    """

    try:
        return await delete_records_async(
            table=TABLE_NAME,
            data_list=data_list,
            key_columns=KEY_COLUMNS,
        )
    except Exception as e:
        logger.warning(f"Deletion failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
