import asyncio
from contextlib import nullcontext
from datetime import date, datetime, time, timezone
from unittest.mock import MagicMock

import pytest

from services.loss_run import loss_run_scheduler as scheduler


def schedule(day=1):
    return dict(
        ScheduleId="monthly_standard",
        ReportType="standard",
        DayOfMonth=day,
        RunAtLocalTime=time(0),
        TimeZoneName="America/New_York",
        ActiveFromDate=date(2026, 1, 1),
        IsEnabled=True,
    )


@pytest.mark.parametrize("month,hour", [(1, 5), (7, 4), (11, 4), (12, 5)])
def test_midnight_follows_eastern_dst(month, hour):
    row = schedule()
    row["ActiveFromDate"] = date(2026, month, 1)
    before = datetime(2026, month, 1, hour - 1, 59, 59, tzinfo=timezone.utc)
    at = datetime(2026, month, 1, hour, tzinfo=timezone.utc)
    assert scheduler.next_due_occurrence(row, before, set()) is None
    assert scheduler.next_due_occurrence(row, at, set()).astimezone(timezone.utc) == at


def test_activation_catchup_and_existing_occurrence():
    row = schedule(20)
    row["ActiveFromDate"] = date(2025, 12, 21)
    now = datetime(2026, 3, 21, tzinfo=timezone.utc)
    existing = {(row["ScheduleId"], date(2026, 1, 20))}
    assert scheduler.next_due_occurrence(row, now, existing).date() == date(2026, 2, 20)
    row["IsEnabled"] = False
    assert scheduler.next_due_occurrence(row, now, existing) is None


def test_local_never_opens_database(monkeypatch):
    monkeypatch.setattr(scheduler.settings, "ENVIRONMENT", "local")
    db = MagicMock()
    monkeypatch.setattr(scheduler, "db_connection", db)
    assert asyncio.run(scheduler.enqueue_due_loss_run_job()) is None
    db.assert_not_called()


def test_active_job_defers_submission(monkeypatch):
    conn = MagicMock()
    conn.cursor.return_value.fetchone.return_value = ("active-job",)
    monkeypatch.setattr(scheduler, "db_connection", lambda: nullcontext(conn))
    assert scheduler._enqueue_due_job() is None
    assert conn.cursor.return_value.execute.call_count == 1
    conn.commit.assert_called_once()


@pytest.mark.parametrize("already_exists", [False, True])
def test_enqueue_claim_review_and_deduplicate(monkeypatch, already_exists):
    row = schedule(20)
    row.update(ScheduleId="monthly_claim_review", ReportType="claim_review")
    conn = MagicMock()
    cursor = conn.cursor.return_value
    cursor.fetchone.side_effect = [None, (datetime(2026, 1, 20, 5),)]
    cursor.description = [(key,) for key in row]
    cursor.fetchall.side_effect = [
        [tuple(row.values())],
        [(row["ScheduleId"], date(2026, 1, 20))] if already_exists else [],
    ]
    monkeypatch.setattr(scheduler, "db_connection", lambda: nullcontext(conn))
    result = scheduler._enqueue_due_job()
    assert (result is None) == already_exists
    conn.commit.assert_called_once()
    if not already_exists:
        sql, job_id, report_type, schedule_id, occurrence = cursor.execute.call_args.args
        assert "'scheduled'" in sql and "'all'" in sql
        assert (job_id, report_type, schedule_id, occurrence) == (
            str(result),
            "claim_review",
            "monthly_claim_review",
            date(2026, 1, 20),
        )


def test_insert_failure_does_not_commit(monkeypatch):
    conn = MagicMock()
    cursor = conn.cursor.return_value
    row = schedule()
    cursor.fetchone.side_effect = [None, (datetime(2026, 1, 1, 5),)]
    cursor.description = [(key,) for key in row]
    cursor.fetchall.side_effect = [[tuple(row.values())], []]
    cursor.execute.side_effect = [None, None, None, None, RuntimeError("insert failed")]
    monkeypatch.setattr(scheduler, "db_connection", lambda: nullcontext(conn))
    with pytest.raises(RuntimeError, match="insert failed"):
        scheduler._enqueue_due_job()
    conn.commit.assert_not_called()
