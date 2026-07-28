from io import BytesIO

from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
from openpyxl import load_workbook

DATABRICKS_HOST = "https://adb-8608532567795739.19.azuredatabricks.net"
DATABRICKS_PROFILE = "claims-preprod"
DATABRICKS_TEMPLATE_PATH = "/Volumes/claims_data_pre_prod/gold/statics/SACLossRunTemplate.xlsx"

REQUIRED_SHEETS = {
    "Cover Page",
    "Claims Data",
    "Record Only",
    "Summary By Policy Year",
    "Chart",
}


def _download_and_validate_template() -> dict:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(
        host=DATABRICKS_HOST,
        profile=DATABRICKS_PROFILE,
    )

    response = client.files.download(DATABRICKS_TEMPLATE_PATH)
    with response.contents as stream:
        template_bytes = stream.read()

    workbook = load_workbook(BytesIO(template_bytes))
    try:
        missing_sheets = sorted(REQUIRED_SHEETS.difference(workbook.sheetnames))
        claims_table_found = "ClaimsData" in workbook["Claims Data"].tables
        record_only_table_found = "RecordOnlyData" in workbook["Record Only"].tables
        cover_image_count = len(workbook["Cover Page"]._images)

        if missing_sheets or not claims_table_found or not record_only_table_found:
            raise ValueError("The downloaded file is not the expected loss-run template")

        return {
            "status": "success",
            "message": "Databricks loss-run template is accessible and valid",
            "templatePath": DATABRICKS_TEMPLATE_PATH,
            "fileSizeBytes": len(template_bytes),
            "sheetNames": workbook.sheetnames,
            "claimsDataTableFound": claims_table_found,
            "recordOnlyDataTableFound": record_only_table_found,
            "coverImageCount": cover_image_count,
        }
    finally:
        workbook.close()


async def test_databricks_template_access() -> dict:
    try:
        return await run_in_threadpool(_download_and_validate_template)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Unable to read the loss-run template from Databricks",
                "reason": str(exc),
            },
        ) from exc
