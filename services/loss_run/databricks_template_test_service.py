from io import BytesIO
from urllib.parse import quote

import requests
from azure.identity import ManagedIdentityCredential
from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
from openpyxl import load_workbook

DATABRICKS_HOST = "https://adb-8608532567795739.19.azuredatabricks.net"
DATABRICKS_TOKEN_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
DATABRICKS_TEMPLATE_PATH = "/Volumes/claims_data_pre_prod/gold/statics/SACLossRunTemplate.xlsx"

REQUIRED_SHEETS = {
    "Cover Page",
    "Claims Data",
    "Record Only",
    "Summary By Policy Year",
    "Chart",
}


def _download_and_validate_template() -> dict:
    access_token = ManagedIdentityCredential().get_token(DATABRICKS_TOKEN_SCOPE)
    file_url = f"{DATABRICKS_HOST}/api/2.0/fs/files" f"{quote(DATABRICKS_TEMPLATE_PATH, safe='/')}"

    response = requests.get(
        file_url,
        headers={"Authorization": f"Bearer {access_token.token}"},
        timeout=60,
    )
    response.raise_for_status()
    template_bytes = response.content

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
            "authenticationMode": "system-assigned-managed-identity",
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
