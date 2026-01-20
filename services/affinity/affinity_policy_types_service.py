import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import _ensure_safe_identifier, merge_upsert_records_async, run_raw_query_async
from services.validations.affinity_validations import (
    apply_affinity_policy_type_defaults,
    validate_affinity_policy_type_payload,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAffinityPolicyType"
AGENTS_TABLE = "tblAffinityAgents"


async def get_affinity_policy_types(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblAffinityPolicyType joined with tblAffinityAgents.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        filters = []
        params = []
        for key, value in query_params.items():
            if key == "PrimaryAgt":
                continue
            _ensure_safe_identifier(key)
            filters.append(f"{TABLE_NAME}.{key} = ?")
            params.append(value)

        query = f"""
            SELECT {TABLE_NAME}.*
            FROM {TABLE_NAME}
            WHERE EXISTS (
                SELECT 1
                FROM {AGENTS_TABLE}
                WHERE {AGENTS_TABLE}.ProgramName = {TABLE_NAME}.ProgramName
                  AND {AGENTS_TABLE}.PrimaryAgt = ?
            )
        """
        params.insert(0, "Yes")

        if filters:
            query += " AND " + " AND ".join(filters)

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
        data_with_defaults = apply_affinity_policy_type_defaults(data)
        errors = validate_affinity_policy_type_payload(data_with_defaults)
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        normalized = normalize_payload_dates(data_with_defaults)
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=[normalized],
            key_columns=["ProgramName"],
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
