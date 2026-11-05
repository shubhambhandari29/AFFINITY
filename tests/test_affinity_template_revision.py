import pytest
from openpyxl import load_workbook

from scripts.update_affinity_template import (
    FINANCIAL_COLUMNS,
    ALIASES,
    REFERENCE,
    TEMPLATE,
    update_template,
)


def test_dimensions_match_client_without_copying_instruction_colours():
    reference = load_workbook(REFERENCE)
    template = load_workbook(TEMPLATE)
    for row in range(1, 8):
        assert template["Cover Page"].row_dimensions[row].height == reference["Cover Page"].row_dimensions[row].height
    for column in ("A", "B"):
        assert template["Cover Page"].column_dimensions[column].width == reference["Cover Page"].column_dimensions[column].width
    for name in ("Claim Details", "Record Only"):
        source = reference[name]
        columns = {
            ALIASES.get(str(c.value or "").strip(), str(c.value or "").strip()): c.column
            for c in source[1]
        }
        for cell in template[name][1]:
            assert cell.fill.fgColor.value not in {"FF92D050", "FFFFFF00", "FFFF0000"}
            if cell.value == "Whole Claim Incurred w/o ALAE":
                continue  # Approved additional column, absent from the example.
            index = columns[cell.value]
            width = next(d.width for d in source.column_dimensions.values() if d.min <= index <= d.max)
            assert template[name].column_dimensions[cell.column_letter].width == width
    reference.close()
    template.close()


def test_revised_layout_preserves_runtime_template_and_is_repeatable(tmp_path):
    original = TEMPLATE.read_bytes()
    output = tmp_path / "revised.xlsx"
    update_template(output_path=output)
    assert TEMPLATE.read_bytes() == original
    for attempt in range(2):
        workbook = load_workbook(output)
        cover = workbook["Cover Page"]
        assert [cover.cell(row, 1).value for row in range(2, 6)] == [
            "Producer Number", "Producing Agent Name", "Program Name", "Report Run Date"
        ]
        assert all(cover.cell(row, 2).value is None for row in range(2, 6))
        assert len(cover._images) == 1
        assert "A6:B6" in map(str, cover.merged_cells.ranges)
        assert "A7:B7" in map(str, cover.merged_cells.ranges)
        claims = [cell.value for cell in workbook["Claim Details"][1]]
        assert claims[19:30] == FINANCIAL_COLUMNS
        assert claims[19] == "Outstanding Loss Reserve"
        assert claims[29] == "Expense Recovery"
        assert claims[30] == "Litigation Status"
        assert claims[-2:] == ["Whole Claim Incurred w/o ALAE", "Claims above $50K"]
        for name in ("Claim Details", "Record Only"):
            sheet = workbook[name]
            headers = [cell.value for cell in sheet[1]]
            assert len(headers) == len(set(headers))
            assert "Producing Agent Name" not in headers
            assert "Record Only Indicator" not in headers
            assert "Claims above 25K" not in headers
            assert all(cell.value is None or cell.data_type == "f"
                       for row in sheet.iter_rows(min_row=2) for cell in row)
            assert not any(d.hidden for d in sheet.column_dimensions.values())
        record_headers = [cell.value for cell in workbook["Record Only"][1]]
        assert not set(record_headers) & {
            "Policy Holder Claim Number", "Product", "Manufacturer",
            "Policy Region Description", "Policy Holder Retailer Desc",
        }
        workbook.close()
        if attempt == 0:
            update_template(path=output, output_path=output)


def test_rejects_populated_report(tmp_path):
    source = tmp_path / "populated.xlsx"
    workbook = load_workbook(TEMPLATE)
    workbook["Claim Details"]["A2"] = "customer data"
    workbook.save(source)
    workbook.close()
    output = tmp_path / "output.xlsx"
    with pytest.raises(ValueError, match="contains report data"):
        update_template(path=source, output_path=output)
    assert not output.exists()
