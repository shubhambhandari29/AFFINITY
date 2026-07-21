import asyncio

import pytest
from fastapi import HTTPException
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

from services.loss_run import loss_run_service


def _make_template(path):
    workbook = Workbook()
    cover = workbook.active
    cover.title = "Cover Page"

    for sheet_name, table_name in (
        ("Claims Data", "ClaimsData"),
        ("Record Only", "RecordOnlyData"),
    ):
        worksheet = workbook.create_sheet(sheet_name)
        worksheet.append(["Old Column", "Old Value"])
        worksheet.append(["old", "old"])
        excel_table = Table(displayName=table_name, ref="A1:B2")
        excel_table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
        worksheet.add_table(excel_table)

    workbook.create_sheet("Summary By Policy Year")
    workbook.create_sheet("Chart")
    workbook.save(path)


def test_create_workbook_populates_claims_record_only_and_cover(tmp_path, monkeypatch):
    template_path = tmp_path / "SACLossRunTemplate.xlsx"
    output_path = tmp_path / "Customer_2026_07_16.xlsx"
    _make_template(template_path)
    monkeypatch.setattr(loss_run_service, "LOSS_RUN_TEMPLATE_PATH", template_path)

    records = [
        {
            "Customer Number": "00123",
            "Claim Number": "C-1",
            "Exposure": 1,
            "Record Only Indicator": "N",
            "Total Incurred": 2500,
        },
        {
            "Customer Number": "00123",
            "Claim Number": "C-2",
            "Exposure": 2,
            "Record Only Indicator": "Y",
            "Total Incurred": 0,
        },
    ]

    loss_run_service._create_workbook(records, "00123", "Example Customer", output_path)

    workbook = load_workbook(output_path)
    assert workbook["Cover Page"]["B2"].value == "00123"
    assert workbook["Cover Page"]["B3"].value == "Example Customer"
    assert workbook["Claims Data"]["B2"].value == "C-1"
    assert workbook["Claims Data"]["C2"].value == "01"
    assert workbook["Record Only"]["B2"].value == "C-2"
    assert workbook["Record Only"]["C2"].value == "02"
    assert "Record Only Indicator" not in [cell.value for cell in workbook["Claims Data"][1]]
    assert "Total Incurred" not in [cell.value for cell in workbook["Record Only"][1]]
    workbook.close()


def test_generate_selected_loss_runs_handles_one_or_more_customers(tmp_path, monkeypatch):
    template_path = tmp_path / "SACLossRunTemplate.xlsx"
    template_path.touch()
    monkeypatch.setattr(loss_run_service, "LOSS_RUN_TEMPLATE_PATH", template_path)
    monkeypatch.setattr(loss_run_service, "LOSS_RUN_OUTPUT_DIR", tmp_path)

    calls = []

    async def fake_query(query, params):
        calls.append((query, params))
        if "tblAcctSpecial" in query:
            return [
                {"CustomerNum": "00123", "CustomerName": "Example/Customer"},
                {"CustomerNum": "00456", "CustomerName": "No Data Customer"},
            ]
        return [
            {
                "Customer Number": "00123",
                "Claim Number": "C-1",
                "Record Only Indicator": "N",
            }
        ]

    def fake_create(records, customer_num, customer_name, output_path):
        output_path.touch()

    async def fake_threadpool(func, *args):
        return func(*args)

    monkeypatch.setattr(loss_run_service, "run_raw_query_async", fake_query)
    monkeypatch.setattr(loss_run_service, "_create_workbook", fake_create)
    monkeypatch.setattr(loss_run_service, "run_in_threadpool", fake_threadpool)

    result = asyncio.run(loss_run_service.generate_loss_runs(["00123", "00456", "00123"]))

    assert result["requestedCount"] == 2
    assert result["generatedCount"] == 1
    assert result["failedCount"] == 1
    assert result["failures"] == [
        {"customerNumber": "00456", "reason": "No loss-run records found"}
    ]
    assert "files" not in result
    assert len(calls) == 2
    assert calls[0][1] == ["00123", "00456"]
    assert calls[1][1] == ["00123", "00456"]
    assert "WHERE [Customer Number] IN (?, ?)" in calls[1][0]


def test_generate_all_loss_runs_uses_current_eligibility_rules(tmp_path, monkeypatch):
    template_path = tmp_path / "SACLossRunTemplate.xlsx"
    template_path.touch()
    monkeypatch.setattr(loss_run_service, "LOSS_RUN_TEMPLATE_PATH", template_path)
    monkeypatch.setattr(loss_run_service, "LOSS_RUN_OUTPUT_DIR", tmp_path)

    calls = []

    async def fake_query(query, params=None):
        calls.append((query, params))
        if "tblAcctSpecial" in query:
            return [{"CustomerNum": "00123", "CustomerName": "Example Customer"}]
        return [
            {
                "Customer Number": "00123",
                "Claim Number": "C-1",
                "Record Only Indicator": "N",
            }
        ]

    def fake_create(records, customer_num, customer_name, output_path):
        output_path.touch()

    async def fake_threadpool(func, *args):
        return func(*args)

    monkeypatch.setattr(loss_run_service, "run_raw_query_async", fake_query)
    monkeypatch.setattr(loss_run_service, "_create_workbook", fake_create)
    monkeypatch.setattr(loss_run_service, "run_in_threadpool", fake_threadpool)

    result = asyncio.run(loss_run_service.generate_loss_runs())

    assert result["generatedCount"] == 1
    assert "AcctStatus = 'Active'" in calls[0][0]
    assert "LossRunDistFreq <> 'Not Needed'" in calls[0][0]
    assert calls[1][0].strip() == "SELECT * FROM dbo.SAC_Loss_Run"


def test_generate_loss_runs_requires_template(tmp_path, monkeypatch):
    monkeypatch.setattr(loss_run_service, "LOSS_RUN_TEMPLATE_PATH", tmp_path / "missing.xlsx")

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(loss_run_service.generate_loss_runs(["00123"]))

    assert exc_info.value.status_code == 500
    assert "LOSS_RUN_TEMPLATE_PATH" in exc_info.value.detail["error"]
