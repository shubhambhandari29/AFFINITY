"""Create due monthly jobs; the existing worker generates their reports."""

import logging
from datetime import datetime, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi.concurrency import run_in_threadpool

from core.config import settings
from db import db_connection
from services.loss_run.loss_run_job_repository import JOB_TABLE, _rows_to_dicts

logger = logging.getLogger(__name__)


def next_due_occurrence(schedule: dict, now: datetime, existing: set):
    """Return the oldest unqueued occurrence since activation, in local time."""
    if not schedule["IsEnabled"] or not schedule["ActiveFromDate"]:
        return None
    zone = ZoneInfo(schedule["TimeZoneName"])
    today = now.astimezone(zone).date()
    active_from = schedule["ActiveFromDate"]
    month = active_from.replace(day=1)
    while month <= today:
        day = month.replace(day=schedule["DayOfMonth"])
        due = datetime.combine(day, schedule["RunAtLocalTime"], tzinfo=zone)
        if day >= active_from and due <= now and (schedule["ScheduleId"], day) not in existing:
            return due
        month = (
            month.replace(year=month.year + 1, month=1)
            if month.month == 12
            else month.replace(month=month.month + 1)
        )
    return None


def _enqueue_due_job():
    with db_connection() as connection:
        cursor = connection.cursor()
        # Same transaction/range lock as manual submission: only one active job.
        cursor.execute(
            f"""
            SELECT TOP (1) JobId
            FROM {JOB_TABLE} WITH (UPDLOCK, HOLDLOCK)
            WHERE Status IN ('queued', 'processing')
            ORDER BY CreatedAt DESC
        """
        )
        if cursor.fetchone():
            connection.commit()
            return None

        cursor.execute("SELECT SYSUTCDATETIME()")
        now = cursor.fetchone()[0].replace(tzinfo=timezone.utc)
        cursor.execute("SELECT * FROM dbo.tblLossRunSchedule WHERE IsEnabled = 1")
        schedules = _rows_to_dicts(cursor)
        cursor.execute(
            f"SELECT ScheduleId, ScheduledForDate FROM {JOB_TABLE} WHERE ScheduleId IS NOT NULL"
        )
        existing = {(row[0], row[1]) for row in cursor.fetchall()}
        candidates = []
        for schedule in schedules:
            try:
                due = next_due_occurrence(schedule, now, existing)
            except (ValueError, KeyError, TypeError):
                logger.exception("Invalid loss-run schedule: %s", schedule["ScheduleId"])
                continue
            if due:
                candidates.append((due, schedule["ScheduleId"], schedule))
        if not candidates:
            connection.commit()
            return None

        due, schedule_id, schedule = min(candidates, key=lambda item: (item[0], item[1]))
        job_id = uuid4()
        cursor.execute(
            f"""
            INSERT INTO {JOB_TABLE}
                (JobId, JobType, Status, Phase, RequestedBy, ReportType,
                 TriggerSource, ScheduleId, ScheduledForDate)
            VALUES (?, 'all', 'queued', 'waiting_for_worker',
                    'system:loss-run-scheduler', ?, 'scheduled', ?, ?)
        """,
            str(job_id),
            schedule["ReportType"],
            schedule_id,
            due.date(),
        )
        connection.commit()
        logger.info("Queued scheduled loss-run %s for %s (%s)", job_id, schedule_id, due.date())
        return job_id


async def enqueue_due_loss_run_job():
    # Local development must never launch automatic batches against the shared DB.
    if settings.ENVIRONMENT.strip().lower() == "local":
        return None
    return await run_in_threadpool(_enqueue_due_job)
