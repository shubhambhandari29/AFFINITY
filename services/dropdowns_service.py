import logging
import re
from functools import partial
from typing import Any

from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool

from core.db_helpers import run_raw_query_async
from db import db_connection

logger = logging.getLogger(__name__)

DropdownQuery = str | tuple[str, list[Any]]

_DROPDOWN_QUERIES: dict[str, DropdownQuery] = {
    "SAC_Contact1": """
        SELECT LANID, SACName
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "SAC_Contact2": """
        SELECT LANID, SACName, EmpTitle, TelNum, EMailID
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "AcctOwner": """
        SELECT SACName, EMailID, EmpTitle, TelNum, TelExt, LANID
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "LossCtlRep1": """
        SELECT PK_Number, RepName, LCEmail, LCTel, LAN_ID
        FROM tblLossCtrl
        ORDER BY RepName
    """,
    "LossCtlRep2": (
        """
        SELECT PK_Number, RepName, Active, LCEmail
        FROM tblLossCtrl
        WHERE Active = ?
        ORDER BY RepName
        """,
        ["Yes"],
    ),
    "RiskSolMgr": """
        SELECT PK_Number, RepName, LCEmail, LAN_ID
        FROM tblLossCtrl
        ORDER BY RepName
    """,
    "BranchName": """
        SELECT BranchNmb, BranchName, ReportingBranch
        FROM tblBranch
        ORDER BY BranchName
    """,
    "ServLevel": """
        SELECT PK_Number, [service Level], [Dollar Threshold]
        FROM tblServiceLevel
        ORDER BY SortNum
    """,
    "Underwriters": """
        SELECT PK_Number, [UW Last], [UW Email]
        FROM tblUnderwriters
        ORDER BY [UW Last]
    """,
    "EDW_AGENT_LIST": """
        SELECT PK_Number, Agent_Code, Agent_Name
        FROM tblEDW_AGENT_LIST
        ORDER BY Agent_Code
    """,
    "tblGrpCode": """
        SELECT PK_Number, tblGrpCode.Code, tblGrpCode.[Prgram Expanded Name] AS ProgramExpandedName
        FROM tblGrpCode
        ORDER BY tblGrpCode.Code
    """,
    "LossCtl": """
SELECT tblLossCtrl.PK_Number, tblLossCtrl.RepName, tblLossCtrl.LCEmail
 FROM tblLossCtrl 
 WHERE (((tblLossCtrl.Active)='Yes')) 
 ORDER BY tblLossCtrl.RepName
""",
}

_DROPDOWN_DEFINITIONS: dict[str, dict[str, Any]] = {
    "SAC_Contact1": {
        "table": "tblMGTUsers",
        "primary_key": "LANID",
        "columns": ["SACName"],
    },
    "SAC_Contact2": {
        "table": "tblMGTUsers",
        "primary_key": "LANID",
        "columns": ["SACName", "EmpTitle", "TelNum", "EMailID"],
    },
    "AcctOwner": {
        "table": "tblMGTUsers",
        "primary_key": "LANID",
        "columns": ["SACName", "EMailID", "EmpTitle", "TelNum", "TelExt"],
    },
    "LossCtlRep1": {
        "table": "tblLossCtrl",
        "primary_key": "PK_Number",
        "columns": ["RepName", "LCEmail", "LCTel", "LAN_ID"],
    },
    "LossCtlRep2": {
        "table": "tblLossCtrl",
        "primary_key": "PK_Number",
        "columns": ["RepName", "Active", "LCEmail"],
    },
    "RiskSolMgr": {
        "table": "tblLossCtrl",
        "primary_key": "PK_Number",
        "columns": ["RepName", "LCEmail", "LAN_ID"],
    },
    "BranchName": {
        "table": "tblBranch",
        "primary_key": "BranchNmb",
        "columns": ["BranchName", "ReportingBranch"],
    },
    "ServLevel": {
        "table": "tblServiceLevel",
        "primary_key": "PK_Number",
        "columns": ["service Level", "Dollar Threshold"],
    },
    "Underwriters": {
        "table": "tblUnderwriters",
        "primary_key": "PK_Number",
        "columns": ["UW Last", "UW Email"],
    },
    "EDW_AGENT_LIST": {
        "table": "tblEDW_AGENT_LIST",
        "primary_key": "PK_Number",
        "columns": ["Agent_Code", "Agent_Name"],
    },
    "tblGrpCode": {
        "table": "tblGrpCode",
        "primary_key": "PK_Number",
        "columns": {
            "Code": "Code",
            "ProgramExpandedName": "Prgram Expanded Name",
        },
    },
    "LossCtl": {
        "table": "tblLossCtrl",
        "primary_key": "PK_Number",
        "columns": ["RepName", "LCEmail"],
    },
}

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_ ]*$")
_IDENTITY_PRIMARY_KEYS = {"DD_Key", "PK_Number"}


def _ensure_safe_identifier(identifier: str) -> None:
    if not identifier or not _IDENTIFIER_PATTERN.match(identifier) or "]" in identifier:
        raise ValueError(f"Invalid column or table name: {identifier}")


def _quote_identifier(identifier: str) -> str:
    _ensure_safe_identifier(identifier)
    return f"[{identifier}]"


def _get_dropdown_definition(name: str) -> tuple[str, str, dict[str, str]]:
    definition = _DROPDOWN_DEFINITIONS.get(name)
    if not definition:
        return "tblDropDowns", "DD_Key", {"DD_Value": "DD_Value", "DD_SortOrder": "DD_SortOrder"}

    columns = definition["columns"]
    if isinstance(columns, dict):
        column_map = dict(columns)
    else:
        column_map = {column: column for column in columns}

    return definition["table"], definition["primary_key"], column_map


def _normalize_dropdown_rows(
    rows: list[dict[str, Any]],
    primary_key: str,
    column_map: dict[str, str],
) -> list[dict[str, Any]]:
    if not rows:
        raise HTTPException(status_code=400, detail={"error": "Payload rows are required"})

    allowed_columns = set(column_map.keys()) | {primary_key}
    normalized_rows: list[dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, dict):
            raise HTTPException(status_code=400, detail={"error": "Each row must be an object"})

        extra_columns = set(row.keys()) - allowed_columns
        if extra_columns:
            raise HTTPException(
                status_code=400,
                detail={"error": f"Invalid column(s): {', '.join(sorted(extra_columns))}"},
            )

        normalized_row: dict[str, Any] = {}
        if primary_key in row:
            normalized_row[primary_key] = row.get(primary_key)

        for api_column, db_column in column_map.items():
            if api_column in row:
                normalized_row[db_column] = row.get(api_column)

        if not normalized_row:
            raise HTTPException(status_code=400, detail={"error": "Each row must include data"})

        normalized_rows.append(normalized_row)

    return normalized_rows


def _normalize_delete_rows(
    rows: list[dict[str, Any]],
    primary_key: str,
    allowed_columns: set[str],
) -> list[dict[str, Any]]:
    if not rows:
        raise HTTPException(status_code=400, detail={"error": "Payload rows are required"})

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise HTTPException(status_code=400, detail={"error": "Each row must be an object"})

        extra_columns = set(row.keys()) - allowed_columns - {primary_key}
        if extra_columns:
            raise HTTPException(
                status_code=400,
                detail={"error": f"Invalid column(s): {', '.join(sorted(extra_columns))}"},
            )

        if primary_key not in row:
            raise HTTPException(
                status_code=400,
                detail={"error": f"{primary_key} is required for deletion"},
            )

        pk_value = row.get(primary_key)
        if pk_value is None or (isinstance(pk_value, str) and not pk_value.strip()):
            raise HTTPException(
                status_code=400,
                detail={"error": f"{primary_key} is required for deletion"},
            )

        normalized_rows.append({primary_key: pk_value})

    return normalized_rows


def _merge_upsert_dropdown_records(
    table: str,
    data_list: list[dict[str, Any]],
    key_columns: list[str],
    *,
    exclude_key_columns_from_insert: bool = False,
) -> dict[str, Any]:
    if not data_list:
        return {"message": "No data provided", "count": 0}

    _ensure_safe_identifier(table)
    for key in key_columns:
        _ensure_safe_identifier(key)

    conn = None
    try:
        with db_connection() as conn:
            cursor = conn.cursor()

            for data in data_list:
                columns = list(data.keys())
                for column in columns:
                    _ensure_safe_identifier(column)

                using_cols = ", ".join([f"? AS {_quote_identifier(col)}" for col in columns])
                on_clause = " AND ".join(
                    [
                        f"target.{_quote_identifier(key)} = source.{_quote_identifier(key)}"
                        for key in key_columns
                    ]
                )

                update_cols = [col for col in columns if col not in key_columns]
                update_set = (
                    ", ".join(
                        [
                            f"{_quote_identifier(col)} = source.{_quote_identifier(col)}"
                            for col in update_cols
                        ]
                    )
                    if update_cols
                    else ""
                )
                update_section = ""
                if update_set:
                    update_section = f"""
WHEN MATCHED THEN
    UPDATE SET {update_set}
"""

                insert_columns = (
                    [col for col in columns if col not in key_columns]
                    if exclude_key_columns_from_insert
                    else columns
                )
                if not insert_columns:
                    raise ValueError("No columns available for insert operation")

                insert_columns_sql = ", ".join([_quote_identifier(col) for col in insert_columns])
                insert_values_sql = ", ".join(
                    [f"source.{_quote_identifier(col)}" for col in insert_columns]
                )

                merge_query = f"""
MERGE INTO {_quote_identifier(table)} AS target
USING (SELECT {using_cols}) AS source
ON {on_clause}
{update_section}WHEN NOT MATCHED THEN
    INSERT ({insert_columns_sql})
    VALUES ({insert_values_sql});
"""
                values = [data[col] for col in columns]
                cursor.execute(merge_query, values)

            conn.commit()
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                logger.error(
                    "Rollback failed after error in _merge_upsert_dropdown_records",
                    exc_info=True,
                )
        logger.error(
            f"Error during dropdown upsert on {table}: {exc}",
            exc_info=True,
        )
        raise

    return {"message": "Transaction successful", "count": len(data_list)}


def _insert_dropdown_records(
    table: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not records:
        return {"message": "No data provided for insertion", "count": 0}

    _ensure_safe_identifier(table)
    conn = None
    try:
        with db_connection() as conn:
            cursor = conn.cursor()

            for record in records:
                if not record:
                    continue

                columns = list(record.keys())
                for column in columns:
                    _ensure_safe_identifier(column)

                placeholders = ", ".join(["?"] * len(columns))
                column_clause = ", ".join([_quote_identifier(col) for col in columns])
                query = f"INSERT INTO {_quote_identifier(table)} ({column_clause}) VALUES ({placeholders})"
                values = [record[col] for col in columns]
                cursor.execute(query, values)

            conn.commit()
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                logger.error(
                    "Rollback failed after error in _insert_dropdown_records",
                    exc_info=True,
                )
        logger.error(f"Error inserting dropdown records into {table}: {exc}", exc_info=True)
        raise

    return {"message": "Insertion successful", "count": len(records)}


def _delete_dropdown_records(
    table: str,
    data_list: list[dict[str, Any]],
    key_column: str,
) -> dict[str, Any]:
    if not data_list:
        return {"message": "No data provided for deletion", "count": 0}

    _ensure_safe_identifier(table)
    _ensure_safe_identifier(key_column)

    conn = None
    try:
        with db_connection() as conn:
            cursor = conn.cursor()

            for data in data_list:
                if key_column not in data:
                    raise ValueError(f"{key_column} is required for deletion")

                delete_query = (
                    f"DELETE FROM {_quote_identifier(table)} WHERE {_quote_identifier(key_column)} = ?"
                )
                cursor.execute(delete_query, [data[key_column]])

            conn.commit()
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                logger.error(
                    "Rollback failed after error in _delete_dropdown_records",
                    exc_info=True,
                )
        logger.error(f"Error deleting dropdown records from {table}: {exc}", exc_info=True)
        raise

    return {"message": "Deletion successful", "count": len(data_list)}


async def _merge_upsert_dropdown_records_async(
    table: str,
    data_list: list[dict[str, Any]],
    key_columns: list[str],
    *,
    exclude_key_columns_from_insert: bool = False,
) -> dict[str, Any]:
    return await run_in_threadpool(
        partial(
            _merge_upsert_dropdown_records,
            table=table,
            data_list=data_list,
            key_columns=key_columns,
            exclude_key_columns_from_insert=exclude_key_columns_from_insert,
        )
    )


async def _insert_dropdown_records_async(
    table: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    return await run_in_threadpool(
        partial(_insert_dropdown_records, table=table, records=records)
    )


async def _delete_dropdown_records_async(
    table: str,
    data_list: list[dict[str, Any]],
    key_column: str,
) -> dict[str, Any]:
    return await run_in_threadpool(
        partial(_delete_dropdown_records, table=table, data_list=data_list, key_column=key_column)
    )


async def get_dropdown_values(name: str) -> list[dict[str, Any]]:
    normalized_name = name.strip()

    if not normalized_name:
        raise HTTPException(status_code=400, detail={"error": "Dropdown type is required"})

    if normalized_name.lower() == "all":
        return await get_all_dropdowns()

    query_def = _DROPDOWN_QUERIES.get(normalized_name)

    if query_def:
        query, params = _normalize_query_definition(query_def)
        try:
            return await run_raw_query_async(query, params)
        except Exception as exc:
            logger.warning(f"Error fetching dropdown '{name}': {exc}")
            raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc

    return await _fetch_dynamic_dropdown(normalized_name)


def _normalize_query_definition(query_def: DropdownQuery) -> tuple[str, list[Any]]:
    if isinstance(query_def, str):
        return query_def, []

    query, params = query_def
    return query, list(params)


async def _fetch_dynamic_dropdown(dd_type: str) -> list[dict[str, Any]]:
    query = """
        SELECT DD_Key, DD_Value, DD_SortOrder
        FROM tblDropDowns
        WHERE DD_Type = ?
        ORDER BY COALESCE(DD_SortOrder, 0), DD_Value
    """
    try:
        return await run_raw_query_async(query, [dd_type])
    except Exception as exc:
        logger.warning(f"Error fetching dynamic dropdown '{dd_type}': {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc


async def get_all_dropdowns() -> list[dict[str, Any]]:
    query = """
        SELECT DD_Key, DD_Type, DD_Value, DD_SortOrder
        FROM tblDropDowns
        ORDER BY DD_Type, COALESCE(DD_SortOrder, 0), DD_Value
    """
    try:
        return await run_raw_query_async(query, [])
    except Exception as exc:
        logger.warning(f"Error fetching all dropdowns: {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc


async def upsert_dropdown_values(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_name = name.strip()
    if not normalized_name:
        raise HTTPException(status_code=400, detail={"error": "Dropdown type is required"})
    if normalized_name.lower() == "all":
        raise HTTPException(status_code=400, detail={"error": "Dropdown type must be specified"})

    table, primary_key, column_map = _get_dropdown_definition(normalized_name)
    try:
        normalized_rows = _normalize_dropdown_rows(rows, primary_key, column_map)

        if table == "tblDropDowns":
            for row in normalized_rows:
                row["DD_Type"] = normalized_name

        rows_with_pk: list[dict[str, Any]] = []
        rows_without_pk: list[dict[str, Any]] = []

        for row in normalized_rows:
            pk_value = row.get(primary_key)
            if pk_value is None or (isinstance(pk_value, str) and not pk_value.strip()):
                row.pop(primary_key, None)
                rows_without_pk.append(row)
            else:
                rows_with_pk.append(row)

        total_count = 0
        if rows_with_pk:
            result = await _merge_upsert_dropdown_records_async(
                table=table,
                data_list=rows_with_pk,
                key_columns=[primary_key],
                exclude_key_columns_from_insert=primary_key in _IDENTITY_PRIMARY_KEYS,
            )
            total_count += result.get("count", len(rows_with_pk))

        if rows_without_pk:
            result = await _insert_dropdown_records_async(table=table, records=rows_without_pk)
            total_count += result.get("count", len(rows_without_pk))

        return {"message": "Upsert successful", "count": total_count}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Upsert failed for dropdown '{name}': {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc


async def delete_dropdown_values(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_name = name.strip()
    if not normalized_name:
        raise HTTPException(status_code=400, detail={"error": "Dropdown type is required"})
    if normalized_name.lower() == "all":
        raise HTTPException(status_code=400, detail={"error": "Dropdown type must be specified"})

    table, primary_key, column_map = _get_dropdown_definition(normalized_name)
    try:
        normalized_rows = _normalize_delete_rows(
            rows,
            primary_key,
            set(column_map.keys()),
        )
        result = await _delete_dropdown_records_async(
            table=table,
            data_list=normalized_rows,
            key_column=primary_key,
        )
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Deletion failed for dropdown '{name}': {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc
