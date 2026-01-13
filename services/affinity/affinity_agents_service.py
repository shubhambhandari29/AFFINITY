import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import fetch_records_async, merge_upsert_records_async, sanitize_filters

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAffinityAgents"
KEY_COLUMNS = ["ProgramName", "AgentCode"]


async def get_affinity_agents(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblAffinityAgents.
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
        logger.warning(f"Error fetching Affinity Agents List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_affinity_agents(data: dict[str, Any]):
    """
    Update row if already exists, else insert row into tblAffinityAgents.
    """

    try:
        normalized = normalize_payload_dates(data)
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=[normalized],
            key_columns=KEY_COLUMNS,
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
