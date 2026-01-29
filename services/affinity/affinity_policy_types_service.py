import logging
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    _ensure_safe_identifier,
    fetch_records_async,
    insert_records_async,
    merge_upsert_records_async,
    run_raw_query_async,
)
from services.validations.affinity_validations import (
    apply_affinity_policy_type_defaults,
    validate_affinity_policy_type_payload,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAffinityPolicyType"
AGENTS_TABLE = "tblAffinityAgents"
PRIMARY_KEY = "PK_Number"

async def _lookup_pk_number(record: dict[str, Any]) -> int | None:
    """
    Fetch the latest PK_Number for a policy type identified by its natural key fields.
    Used after inserts to return the new identity value.
    """
    try:
        query = """
            SELECT TOP 1 PK_Number
            FROM tblAffinityPolicyType
            WHERE ProgramName = ? AND PolicyType = ?
            ORDER BY PK_Number DESC
        """
        params = [record.get("ProgramName"), record.get("PolicyType")]
        rows = await run_raw_query_async(query, params)
        return rows[0]["PK_Number"] if rows else None
    except Exception as exc:
        logger.warning("Failed to fetch PK_Number for new affinity policy type row: %s", exc)
        return None


async def get_affinity_policy_types(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblAffinityPolicyType joined with tblAffinityAgents.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        filters: list[str] = []
        params: list[Any] = []
        primary_agt = query_params.get("PrimaryAgt")
        for key, value in query_params.items():
            if key == "PrimaryAgt":
                continue
            _ensure_safe_identifier(key)
            filters.append(f"{TABLE_NAME}.{key} = ?")
            params.append(value)

        if primary_agt not in (None, ""):
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
            params.insert(0, primary_agt)
            if filters:
                query += " AND " + " AND ".join(filters)
        else:
            query = f"SELECT {TABLE_NAME}.* FROM {TABLE_NAME}"
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
    Update row when PK_Number is provided, else insert a new row into tblAffinityPolicyType.
    """

    try:
        data_with_defaults = apply_affinity_policy_type_defaults(data)
        errors = validate_affinity_policy_type_payload(data_with_defaults)
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        normalized = normalize_payload_dates(data_with_defaults)
        pk_value = normalized.get(PRIMARY_KEY)
        pk_response: int | None = None
        if pk_value not in (None, ""):
            existing = await fetch_records_async(
                table=TABLE_NAME,
                filters={PRIMARY_KEY: pk_value},
            )
            if not existing:
                raise HTTPException(
                    status_code=404,
                    detail={"error": f"{PRIMARY_KEY} {pk_value} not found"},
                )
            await merge_upsert_records_async(
                table=TABLE_NAME,
                data_list=[normalized],
                key_columns=[PRIMARY_KEY],
                exclude_key_columns_from_insert=True,
            )
            pk_response = pk_value
        else:
            sanitized = {k: v for k, v in normalized.items() if k != PRIMARY_KEY}
            if sanitized:
                await insert_records_async(table=TABLE_NAME, records=[sanitized])
                pk_response = await _lookup_pk_number(sanitized)

        return {"message": "Transaction successful", "count": 1, "pk": pk_response}
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
    
