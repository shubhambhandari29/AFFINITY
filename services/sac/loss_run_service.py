import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import TableColumn

from core.config import settings
from core.db_helpers import run_raw_query_async

logger = logging.getLogger(__name__)

RECORD_ONLY_EXCLUDED_COLUMNS = [
    "Claims above 50K",
    "Total Incurred",
    "Total Incurred + ALAE",
    "Total Paid Loss Net Salvage/Subro/Loss Recovery",
    "Incurred w/o ALAE",
    "ALAE Reserve",
    "ALAE Paid",
    "Salvage Recovery",
    "Subro Recovery",
    "Loss Recovery",
    "Deductible Recovery",
    "Expense Recovery",
    "Litigation Status",
    "Outstanding Loss Reserve",
]


def _write_excel_table(worksheet, table_name: str, dataframe: pd.DataFrame) -> None:
    excel_table = worksheet.tables[table_name]
    old_cells = worksheet[excel_table.ref]
    row_count = len(dataframe) + 1
    column_count = len(dataframe.columns)
    new_ref = f"A1:{get_column_letter(column_count)}{row_count}"

    for row_index, row in enumerate(worksheet[new_ref]):
        for column_index, cell in enumerate(row):
            if row_index == 0:
                cell.value = dataframe.columns[column_index]
                continue

            value = dataframe.iat[row_index - 1, column_index]
            cell.value = None if pd.isna(value) else value

    for row in old_cells:
        for cell in row:
            if cell.row > row_count or cell.column > column_count:
                cell.value = None

    excel_table.tableColumns = [
        TableColumn(id=index + 1, name=column) for index, column in enumerate(dataframe.columns)
    ]
    excel_table.autoFilter = None
    excel_table.ref = new_ref


def _create_workbook(
    records: list[dict], customer_num: str, customer_name: str, output_path: Path
) -> None:
    dataframe = pd.DataFrame(records)

    if "Exposure" in dataframe.columns:
        dataframe["Exposure"] = dataframe["Exposure"].apply(
            lambda value: f"{int(value):02d}" if pd.notna(value) and str(value).strip() else ""
        )

    dataframe["Distinct Claim Helper"] = (~dataframe["Claim Number"].duplicated()).astype(int)

    record_only = dataframe[dataframe["Record Only Indicator"] == "Y"].drop(
        columns=["Record Only Indicator", *RECORD_ONLY_EXCLUDED_COLUMNS],
        errors="ignore",
    )
    claims = dataframe[dataframe["Record Only Indicator"] != "Y"].drop(
        columns=["Record Only Indicator"]
    )

    if record_only.empty:
        record_only = pd.DataFrame(
            [["No Records Only", *([None] * (len(record_only.columns) - 1))]],
            columns=record_only.columns,
        )

    workbook = load_workbook(settings.LOSS_RUN_TEMPLATE_PATH)
    _write_excel_table(workbook["Claims Data"], "ClaimsData", claims.reset_index(drop=True))
    _write_excel_table(
        workbook["Record Only"], "RecordOnlyData", record_only.reset_index(drop=True)
    )

    cover_page = workbook["Cover Page"]
    cover_page.cell(2, 2, customer_num)
    cover_page.cell(3, 2, customer_name)
    cover_page.cell(4, 2, datetime.now().strftime("%m/%d/%Y"))

    for sheet_name in ("Summary By Policy Year", "Chart"):
        if sheet_name not in workbook.sheetnames:
            continue
        for pivot in getattr(workbook[sheet_name], "_pivots", []):
            pivot.cache.refreshOnLoad = True

    workbook.calculation.fullCalcOnLoad = True
    workbook.save(output_path)
    workbook.close()


async def generate_loss_run(customer_num: str) -> Path:
    customer_num = customer_num.strip()
    if not customer_num:
        raise HTTPException(status_code=400, detail={"error": "Customer number is required"})

    if not settings.LOSS_RUN_TEMPLATE_PATH.is_file():
        raise HTTPException(
            status_code=500,
            detail={
                "error": (
                    "Loss-run template is not configured. Place SACLossRunTemplate.xlsx "
                    "in the repository root or set LOSS_RUN_TEMPLATE_PATH."
                )
            },
        )

    try:
        customers = await run_raw_query_async(
            """
            SELECT CustomerNum, CustomerName
            FROM dbo.tblAcctSpecial
            WHERE CustomerNum = ?
            """,
            [customer_num],
        )
        if not customers:
            raise HTTPException(status_code=404, detail={"error": "Customer not found"})

        records = await run_raw_query_async(
            """
            SELECT *
            FROM dbo.SAC_Loss_Run
            WHERE [Customer Number] = ?
            """,
            [customer_num],
        )
        if not records:
            raise HTTPException(
                status_code=404,
                detail={"error": "No loss-run records found for this customer"},
            )

        customer_name = str(customers[0].get("CustomerName") or customer_num).strip()
        safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", customer_name).strip(" .")
        filename = f"{safe_name or customer_num}_{datetime.now():%Y_%m_%d}.xlsx"
        settings.LOSS_RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = settings.LOSS_RUN_OUTPUT_DIR / filename

        await run_in_threadpool(
            _create_workbook,
            records,
            customer_num,
            customer_name,
            output_path,
        )
        return output_path
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to generate loss run for customer %s", customer_num)
        raise HTTPException(
            status_code=500, detail={"error": "Failed to generate loss-run report"}
        ) from exc
