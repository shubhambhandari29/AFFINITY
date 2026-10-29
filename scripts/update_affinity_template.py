"""Apply the client-reviewed layout to the blank Affinity Excel template."""

from copy import copy
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.utils import get_column_letter


TEMPLATE = Path(__file__).resolve().parents[1] / "AffinityLossRunTemplate_Revised.xlsx"
REVISED_TEMPLATE = TEMPLATE
LEADING_COLUMNS = [
    "Customer Number", "Policy Insured Name", "Claim Number", "Exposure",
    "Claimant Name - Company", "Account Location Identifier", "Accident State",
    "Policy Symbol", "Policy Number", "Policy MOD", "Policy Effective Date",
    "Policy Expiration Date", "Producing Agent Code", "Loss Date",
    "Date of Notice (Report Date)", "Lag Time", "Claim Status",
    "Claim Closed Date", "Coverage",
]
FINANCIAL_COLUMNS = [
    "Outstanding Loss Reserve",
    "Total Paid Loss Net Salvage/Subro/Loss Recovery", "Incurred w/o ALAE",
    "ALAE Reserve", "ALAE Paid", "Total Incurred + ALAE",
    "Salvage Recovery", "Subro Recovery", "Loss Recovery",
    "Deductible Recovery", "Expense Recovery",
]
LOSS_COLUMNS = ["Cause Of Loss", "Description Of Loss", "Contributing Factor"]
CLAIM_COLUMNS = [
    "Product", "Occupation", "Accident Day Of Week", "Insured Driver",
    "Adjuster", "Adjuster Email", "Adjuster Phone Number",
    "Whole Claim Incurred w/o ALAE", "Claims Above $50k",
]
RECORD_ONLY_COLUMNS = [
    "Accident Day Of Week", "Adjuster", "Adjuster Email",
    "Adjuster Phone Number", "Occupation",
]
ALIASES = {"Feature Number": "Exposure", "Reported Date": "Date of Notice (Report Date)"}
DATE_COLUMNS = {
    "Policy Effective Date", "Policy Expiration Date", "Loss Date",
    "Date of Notice (Report Date)", "Claim Closed Date",
}


def update_template(path=TEMPLATE, output_path=REVISED_TEMPLATE):
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

        columns = LEADING_COLUMNS + (
            FINANCIAL_COLUMNS + ["Litigation Status"] + LOSS_COLUMNS + CLAIM_COLUMNS
            if name == "Claim Details"
            else LOSS_COLUMNS + RECORD_ONLY_COLUMNS
        )
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
            if header in FINANCIAL_COLUMNS or header == "Whole Claim Incurred w/o ALAE":
                body_cell.number_format = '#,##0.00;[Red](#,##0.00)'
            if header in {"Whole Claim Incurred w/o ALAE", "Claims Above $50k"}:
                header_cell.comment = Comment(
                    "Whole-claim incurred excluding ALAE across exposures, not an "
                    "individual exposure total. Repeated per claim row; do not sum "
                    "this column across exposures. Above $50k means strictly greater "
                    "than $50,000. The indicator displays Yes or No.",
                    "SAC Platform",
                )
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

    cover = workbook["Cover Page"]
    if cover["A3"].value != "Producing Agent Name":
        # Move the two merged disclaimer rows explicitly; insert_rows does not
        # update merged ranges automatically.
        for merged in list(cover.merged_cells.ranges):
            if merged.min_row >= 3:
                cover.unmerge_cells(str(merged))
        heights = {row: cover.row_dimensions[row].height for row in range(3, 7)}
        cover.insert_rows(3)
        for row, height in heights.items():
            cover.row_dimensions[row + 1].height = height
        for column in (1, 2):
            cover.cell(3, column)._style = copy(cover.cell(2, column)._style)
        cover["A3"] = "Producing Agent Name"
        cover.merge_cells("A6:B6")
        cover.merge_cells("A7:B7")
    for row in range(2, 6):
        cover.cell(row, 2).value = None
    cover["B5"].number_format = "mm/dd/yyyy"
    cover.print_area = "A1:B7"
    workbook.save(output_path)
    workbook.close()


if __name__ == "__main__":
    update_template()
    print(f"Created {REVISED_TEMPLATE}; runtime template unchanged")
