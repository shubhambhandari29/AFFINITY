import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import fetch_records_async, merge_upsert_records_async, sanitize_filters
from services.validations.affinity_validations import validate_affinity_program_payload

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAcctAffinityProgram"


async def get_affinity_program(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblAcctAffinityProgram.
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
        logger.warning(f"Error fetching Affinity Program List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_affinity_program(data: dict[str, Any]):
    """
    Update row if already exists, else insert row into tblAcctAffinityProgram.
    """

    try:
        errors = validate_affinity_program_payload(data)
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        normalized = normalize_payload_dates(data)
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=[normalized],
            key_columns=["ProgramName"],
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
