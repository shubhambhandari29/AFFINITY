from datetime import date


def update_report_cover(
    workbook, loss_date_from: date | None, report_date: date
) -> None:
    """Describe the actual range; policy dates remain informational columns."""
    cover = workbook["Cover Page"]
    cover.cell(4, 2).value = report_date.strftime("%m/%d/%Y")
    if loss_date_from is not None:
        start = f"{loss_date_from.month}/{loss_date_from.day}/{loss_date_from.year}"
        end = f"{report_date.month}/{report_date.day}/{report_date.year}"
        cover.cell(6, 1).value = (
            f"This report contains closed claims with loss dates from {start} through {end}, "
            "inclusive, plus all open claims and claims with positive outstanding loss "
            "reserves regardless of loss date."
        )
    else:
        cover.cell(6, 1).value = (
            "This report contains information on all open claims; as well as closed "
            "claims limited to the current and prior six policy years."
        )
