"""Populate the standard template's summary sheets and Excel pivot caches."""

from copy import copy
from datetime import date, datetime
from decimal import Decimal

from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.chart.axis import ChartLines
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.shapes import GraphicalProperties
from openpyxl.drawing.line import LineProperties
from openpyxl.pivot.cache import CacheField, WorksheetSource
from openpyxl.pivot.fields import DateTimeField, Index, Missing, Number, Text
from openpyxl.pivot.record import Record, RecordList
from openpyxl.pivot.table import FieldItem, RowColItem
from openpyxl.styles import Border, PatternFill, Side
from openpyxl.utils import get_column_letter

from services.loss_run.pivot_cache import (
    cache_field_values,
    pivot_row_items,
    shared_items,
)


def populate_standard_summaries(workbook, claims: list[dict]) -> None:
    """Render real values immediately, retaining pivots for Excel filtering.

    Use the template's cache field order, including legacy helper fields, so
    removing Record Only Indicator from Claims Data cannot misalign the pivot.
    """
    targets = [
        workbook[name]
        for name in ("Summary By Policy Year", "Chart")
        if name in workbook.sheetnames and workbook[name]._pivots
    ]
    if not targets:
        return
    headers = [field.name for field in targets[0]._pivots[0].cache.cacheFields]
    if "Summary Source" in workbook.sheetnames:
        del workbook["Summary Source"]
    source = workbook.create_sheet("Summary Source")
    source.sheet_state = "hidden"
    source.append(headers)
    seen = set()
    values = []
    for row_index, claim in enumerate(claims, 2):
        claim = dict(claim)
        # Count a claim once per policy year, even with multiple exposures.
        number = (claim.get("Policy Year"), str(claim["Claim Number"]).casefold())
        claim["Distinct Claim Helper"] = int(number not in seen)
        seen.add(number)
        row = [claim.get(header) for header in headers]
        values.append(row)
        source.append(row)
        # max_row and sheet[row] scan the populated cell map. Avoid doing that
        # for every claim, which makes building a large source quadratic.
        for column in range(1, len(headers) + 1):
            cell = source.cell(row_index, column)
            if isinstance(cell.value, str):
                cell.data_type = "s"

    fields = []
    display_values = []
    indexes = [[] for _ in values]
    for column, header in enumerate(headers):
        unique, references = cache_field_values(row[column] for row in values)
        display_values.append(unique)
        items = []
        for value in unique:
            if value is None:
                items.append(Missing())
            elif isinstance(value, int | float | Decimal):
                items.append(Number(v=float(value)))
            elif isinstance(value, date | datetime):
                items.append(
                    DateTimeField(
                        v=value
                        if isinstance(value, datetime)
                        else datetime.combine(value, datetime.min.time())
                    )
                )
            else:
                items.append(Text(v=str(value)))
        fields.append(
            CacheField(
                name=header,
                sharedItems=shared_items(items),
            )
        )
        for index, reference in enumerate(references):
            indexes[index].append(reference)

    for sheet in targets:
        pivot = sheet._pivots[0]
        if [f.name for f in pivot.cache.cacheFields] != headers:
            raise ValueError(
                "Standard template pivots must use the same Claims Data fields"
            )
        cache = pivot.cache
        cache.cacheFields = fields
        cache.records = RecordList(
            r=[Record(_fields=[Index(v=v) for v in row]) for row in indexes]
        )
        cache.recordCount = len(values)
        cache.cacheSource.worksheetSource = WorksheetSource(
            sheet=source.title,
            ref=f"A1:{get_column_letter(len(headers))}{len(values) + 1}",
        )
        cache.refreshOnLoad = False
        cache.enableRefresh = True
        cache.missingItemsLimit = 0
        # Template formatting/filter references can point at old cache items.
        # Cell styles are retained below; do not retain item-specific pivot rules.
        pivot.formats = []
        pivot.conditionalFormats = None
        pivot.filters = []
        pivot.colGrandTotals = True
        pivot.rowGrandTotals = False
        for index, field in enumerate(pivot.pivotFields):
            field.items = (
                [FieldItem(x=i) for i in range(len(fields[index].sharedItems._fields))]
                if field.axis
                else []
            )
            for name in (
                "defaultSubtotal",
                "sumSubtotal",
                "countASubtotal",
                "avgSubtotal",
                "maxSubtotal",
                "minSubtotal",
                "productSubtotal",
                "countSubtotal",
                "stdDevSubtotal",
                "stdDevPSubtotal",
                "varSubtotal",
                "varPSubtotal",
            ):
                setattr(field, name, False)
        for page in pivot.pageFields:
            page.item = None
        for field in pivot.dataFields:
            field.baseField = -1
            field.baseItem = 1048832
        is_summary = sheet.title == "Summary By Policy Year"
        year_field = next(
            (i for i, f in enumerate(pivot.rowFields) if headers[f.x] == "Policy Year"),
            None,
        )
        year_subtotals = is_summary and year_field == 0
        if year_subtotals:
            year = pivot.pivotFields[pivot.rowFields[0].x]
            year.defaultSubtotal = True
            year.subtotalTop = False
            year.sortType = "descending"
            year.items = [*year.items, FieldItem(t="default")]
        pivot.compact = False
        pivot.compactData = False
        groups = {}
        for row, item_indexes in zip(values, indexes, strict=True):
            key = tuple(item_indexes[field.x] for field in pivot.rowFields)
            if key not in groups:
                groups[key] = (
                    [Decimal(0) for _ in pivot.dataFields],
                    [item_indexes[field.x] for field in pivot.rowFields],
                )
            totals, _ = groups[key]
            for index, field in enumerate(pivot.dataFields):
                if field.subtotal != "sum":
                    raise ValueError("Unexpected standard report pivot aggregation")
                totals[index] += Decimal(str(row[field.fld] or 0))

        start = 7 if sheet.title == "Summary By Policy Year" else 2
        styles = [copy(cell._style) for cell in sheet[start]]
        for row in sheet.iter_rows(min_row=start):
            for cell in row:
                cell.value = None
        rows = []
        row_paths = []
        summary_items = []
        total_rows = set()
        grand = [Decimal(0) for _ in pivot.dataFields]

        def labels(key, row_fields=pivot.rowFields):
            return [
                display_values[field.x][ref]
                for field, ref in zip(row_fields, key, strict=True)
            ]

        ordered = sorted(
            groups, key=lambda k: tuple(str(v or "").casefold() for v in labels(k))
        )
        if year_subtotals:
            # Stable secondary ordering keeps policies/claims grouped within year.
            ordered.sort(key=lambda k: str(labels(k)[0] or ""), reverse=True)
        previous_labels = []
        previous_path = []
        year_totals = [Decimal(0) for _ in pivot.dataFields]

        def finish_year(key):
            rows.append([
                f"{labels(key)[0]} Total",
                *([None] * (len(pivot.rowFields) - 1)), *year_totals,
            ])
            total_rows.add(len(rows) - 1)
            summary_items.append(RowColItem(t="default", x=[Index(v=key[0])]))

        for position, key in enumerate(ordered):
            totals, refs = groups[key]
            current_labels = labels(key)
            shown = list(current_labels)
            if is_summary:
                # The burst report retains exposure detail but blanks repeated
                # parent labels (year/policy/claim), rather than repeating claims.
                for column, value in enumerate(current_labels):
                    if previous_labels[:column + 1] != current_labels[:column + 1]:
                        break
                    shown[column] = None
                previous_labels = current_labels
            rows.append([*shown, *totals])
            row_paths.append(refs)
            # Encode repeated ancestors with r, not a fresh full path: Excel
            # otherwise displays expand/collapse buttons on blank parent cells.
            item = (
                pivot_row_items([previous_path, refs])[1]
                if previous_path else pivot_row_items([refs])[0]
            )
            summary_items.append(item)
            previous_path = refs
            grand = [a + b for a, b in zip(grand, totals, strict=True)]
            year_totals = [a + b for a, b in zip(year_totals, totals, strict=True)]
            if year_subtotals and (
                position == len(ordered) - 1 or ordered[position + 1][0] != key[0]
            ):
                finish_year(key)
                year_totals = [Decimal(0) for _ in pivot.dataFields]
                previous_labels = []
                previous_path = []
        rows.append(["Grand Total", *([None] * (len(pivot.rowFields) - 1)), *grand])
        total_rows.add(len(rows) - 1)
        for index, row in enumerate(rows, start):
            for column, value in enumerate(row, 1):
                cell = sheet.cell(index, column)
                if column <= len(styles):
                    cell._style = copy(styles[column - 1])
                if index - start in total_rows:
                    font = copy(cell.font)
                    font.bold = True
                    cell.font = font
                cell.value = value
                if isinstance(value, str):
                    cell.data_type = "s"
                if is_summary:
                    # White cells conceal worksheet gridlines inside repeated
                    # parent groups without merging pivot cells/filter targets.
                    cell.fill = PatternFill("solid", fgColor="FFFFFF")
                    edge = Side(style="thin", color="999999")
                    cell.border = Border(
                        left=edge,
                        top=edge if value is not None or index - start in total_rows else Side(),
                    )
        end = start + len(rows) - 1
        if sheet.max_row > end:
            sheet.delete_rows(end + 1, sheet.max_row - end)
        pivot.rowItems = (
            summary_items + [RowColItem(t="grand", x=[Index(v=0)])]
            if year_subtotals else pivot_row_items(row_paths)
        )
        pivot.location.ref = f"A{start - 1}:{get_column_letter(len(rows[-1]))}{end}"
        pivot.location.firstHeaderRow = 0
        pivot.location.firstDataRow = 1
        pivot.location.firstDataCol = len(pivot.rowFields)
        if sheet.title == "Chart":
            # Replace the template chart's stale cached TEST series.
            sheet._charts.clear()
            if groups:
                chart = BarChart()
                chart.width = 26
                chart.height = 14
                chart.gapWidth = 219
                # Match the burst chart: no chart/axis titles, legend at right,
                # and values only inside labels (Excel otherwise supplies names).
                chart.title = None
                chart.y_axis.title = None
                chart.x_axis.title = None
                chart.add_data(
                    Reference(sheet, min_col=2, min_row=1, max_row=end - 1),
                    titles_from_data=True,
                )
                chart.set_categories(
                    Reference(sheet, min_col=1, min_row=2, max_row=end - 1)
                )
                chart.y_axis.delete = False
                chart.y_axis.tickLblPos = "nextTo"
                chart.x_axis.delete = False
                chart.x_axis.tickLblPos = "nextTo"
                chart.y_axis.majorGridlines = ChartLines(
                    spPr=GraphicalProperties(ln=LineProperties(solidFill="D9D9D9"))
                )
                chart.dLbls = DataLabelList(
                    showVal=True, dLblPos="ctr", numFmt="0",
                    showLegendKey=False, showSerName=False, showCatName=False,
                    showPercent=False, showBubbleSize=False, showLeaderLines=False,
                    spPr=GraphicalProperties(solidFill="FFFFFF"),
                )
                chart.series[0].graphicalProperties.solidFill = "156082"
                chart.legend.position = "r"
                chart.legend.overlay = False
                incurred = LineChart()
                incurred.smooth = False
                incurred.add_data(
                    Reference(sheet, min_col=3, min_row=1, max_row=end - 1),
                    titles_from_data=True,
                )
                incurred.set_categories(
                    Reference(sheet, min_col=1, min_row=2, max_row=end - 1)
                )
                incurred.y_axis.axId = 200
                incurred.y_axis.title = None
                incurred.y_axis.numFmt = '$#,##0.00'
                incurred.y_axis.crosses = "max"
                incurred.y_axis.delete = False
                incurred.y_axis.tickLblPos = "nextTo"
                incurred.y_axis.majorGridlines = None
                incurred.series[0].smooth = False
                incurred.series[0].graphicalProperties.line.solidFill = "F46B2C"
                incurred.series[0].graphicalProperties.line.width = 28575
                incurred.dLbls = DataLabelList(
                    showVal=True, dLblPos="ctr", numFmt='$#,##0.00',
                    showLegendKey=False, showSerName=False, showCatName=False,
                    showPercent=False, showBubbleSize=False, showLeaderLines=False,
                    spPr=GraphicalProperties(solidFill="F46B2C"),
                )
                chart += incurred
                sheet.add_chart(chart, "E2")
