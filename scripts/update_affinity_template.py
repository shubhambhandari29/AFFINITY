"""Apply the client-reviewed layout to the blank Affinity Excel template."""

from copy import copy
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.utils import get_column_letter


TEMPLATE = Path(__file__).resolve().parents[1] / "AffinityLossRunTemplate.xlsx"
LEADING_COLUMNS = [
    "Customer Number", "Policy Insured Name", "Claim Number", "Exposure",
    "Claimant Name - Company", "Account Location Identifier", "Accident State",
    "Policy Symbol", "Policy Number", "Policy MOD", "Policy Effective Date",
    "Policy Expiration Date", "Producing Agent Code", "Loss Date",
    "Date of Notice (Report Date)", "Lag Time", "Claim Status",
    "Claim Closed Date", "Coverage",
]
FINANCIAL_COLUMNS = [
    "Current Loss Reserve", "Paid Loss Net Salvage Subro", "Total Incurred",
    "Claims above 25K", "Total Expense", "Salvage Recovery", "Subro Recovery",
    "Loss Recovery", "Deductible Recovery", "Recoveries", "Total ALAE Amount",
    "Expense Column", "Expense TXN", "Expense Recovery TXN", "Expense_Recovery",
]
LOSS_COLUMNS = ["Cause Of Loss", "Description Of Loss", "Contributing Factor"]
ALIASES = {"Feature Number": "Exposure", "Reported Date": "Date of Notice (Report Date)"}
DATE_COLUMNS = {
    "Policy Effective Date", "Policy Expiration Date", "Loss Date",
    "Date of Notice (Report Date)", "Claim Closed Date",
}


def update_template(path=TEMPLATE):
    workbook = load_workbook(path)
    for name in ("Claim Details", "Record Only"):
        sheet = workbook[name]
        # Only change a blank template, never silently discard report records.
        if any(cell.value is not None and cell.data_type != "f"
               for row in sheet.iter_rows(min_row=2) for cell in row):
            raise ValueError(f"{name} contains report data; expected a blank template")

        styles = {}
        widths = {}
        for cell in sheet[1]:
            header = ALIASES.get(cell.value, cell.value)
            if not header or header.endswith("_Source") or header in styles:
                continue
            styles[header] = (copy(cell._style), copy(sheet.cell(2, cell.column)._style))
            widths[header] = sheet.column_dimensions[cell.column_letter].width

        columns = LEADING_COLUMNS + (FINANCIAL_COLUMNS if name == "Claim Details" else []) + LOSS_COLUMNS
        columns += [header for header in styles if header not in columns]
        default_styles = styles["Customer Number"]
        sheet.delete_rows(1, sheet.max_row)
        sheet.column_dimensions.clear()
        for index, header in enumerate(columns, 1):
            header_cell = sheet.cell(1, index, header)
            body_cell = sheet.cell(2, index)
            header_style, body_style = styles.get(header, default_styles)
            header_cell._style = copy(header_style)
            body_cell._style = copy(body_style)
            alignment = copy(header_cell.alignment)
            alignment.wrap_text = True
            header_cell.alignment = alignment
            dimension = sheet.column_dimensions[get_column_letter(index)]
            dimension.width = max(16, min(widths.get(header, 20) or 20, 45))
            dimension.hidden = False
            if header in DATE_COLUMNS:
                body_cell.number_format = "mm/dd/yyyy"
            if header == "Lag Time":
                loss = get_column_letter(columns.index("Loss Date") + 1)
                notice = get_column_letter(columns.index("Date of Notice (Report Date)") + 1)
                body_cell.value = f'=IF(OR({loss}2="",{notice}2=""),"",INT({notice}2)-INT({loss}2))'
                body_cell.number_format = "0"
                header_cell.comment = Comment(
                    "Calendar days from Loss Date to Date of Notice (Report Date). "
                    "Blank when either date is missing. This is not the report generation date.",
                    "SAC Platform",
                )
        sheet.row_dimensions[1].height = 45
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = f"A1:{get_column_letter(len(columns))}2"
        sheet.sheet_state = "visible"

    workbook.save(path)


if __name__ == "__main__":
    update_template()
    print(f"Updated {TEMPLATE}")
