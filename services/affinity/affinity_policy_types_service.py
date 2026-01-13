import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import _ensure_safe_identifier, merge_upsert_records_async, run_raw_query_async
from services.validations.affinity_validations import validate_affinity_policy_type_payload

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAffinityPolicyType"
AGENTS_TABLE = "tblAffinityAgents"


async def get_affinity_policy_types(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblAffinityPolicyType joined with tblAffinityAgents.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    query_filters = dict(query_params)
    query_filters["PrimaryAgt"] = "Yes"

    try:
        filters = []
        params = []
        for key, value in query_filters.items():
            _ensure_safe_identifier(key)
            table = AGENTS_TABLE if key == "PrimaryAgt" else TABLE_NAME
            filters.append(f"{table}.{key} = ?")
            params.append(value)

        query = (
            f"SELECT * FROM {TABLE_NAME} "
            f"LEFT JOIN {AGENTS_TABLE} "
            f"ON {TABLE_NAME}.ProgramName = {AGENTS_TABLE}.ProgramName"
        )

        if filters:
            query += " WHERE " + " AND ".join(filters)

        records = await run_raw_query_async(query, params)
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching Affinity Policy Types List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_affinity_policy_types(data: dict[str, Any]):
    """
    Update row if already exists, else insert row into tblAffinityPolicyType.
    """

    try:
        errors = validate_affinity_policy_type_payload(data)
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
