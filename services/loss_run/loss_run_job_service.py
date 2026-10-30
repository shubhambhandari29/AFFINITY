from datetime import date
from uuid import UUID

from fastapi import HTTPException

from core.date_utils import format_date_value, format_loss_run_timestamp
from services.loss_run.loss_run_job_repository import (
    create_job,
    get_all_failures,
    get_failures,
    get_job,
    get_jobs,
    request_cancellation,
)


async def create_loss_run_job(
    job_type: str,
    current_user: dict,
    customer_numbers: list[str] | None = None,
    report_type: str = "standard",
    loss_date_from: date | None = None,
) -> dict:
    normalized_numbers = None
    if customer_numbers is not None:
        normalized_numbers = list(
            dict.fromkeys(str(number).strip() for number in customer_numbers if str(number).strip())
        )
        if not normalized_numbers:
            raise HTTPException(
                status_code=400,
                detail={"error": "At least one customer number is required"},
            )

    user = current_user.get("user") or {}
    requested_by = str(user.get("email") or user.get("id") or "unknown").strip()
    job_id, created = await create_job(
        job_type,
        requested_by,
        normalized_numbers,
        report_type,
        loss_date_from,
    )

    if created:
        return {
            "jobId": job_id,
            "businessType": "sac",
            "status": "queued",
            "message": "Loss-run generation has been queued",
        }

    existing = await get_job(job_id)
    return {
        "jobId": job_id,
        "businessType": existing.get("BusinessType", "sac"),
        "status": existing["Status"],
        "message": "A loss-run job is already queued or running",
    }


async def get_loss_run_job(job_id: UUID, business_type: str = "sac") -> dict:
    job = await get_job(job_id)
    if job is None or job.get("BusinessType", "sac") != business_type:
        raise HTTPException(
            status_code=404,
            detail={"error": "Loss-run job not found"},
        )

    failures = []
    if job["FailedCount"]:
        failure_rows = await get_failures(job_id)
        failures = [
            {
                "customerNumber": row["CustomerNumber"],
                "reason": row["FailureReason"] or "Report generation failed",
            }
            for row in failure_rows
        ]

    return _format_job(job, failures)


async def cancel_loss_run_job(job_id: UUID, business_type: str = "sac") -> dict:
    existing = await get_job(job_id)
    if existing is None or existing.get("BusinessType", "sac") != business_type:
        raise HTTPException(status_code=404, detail={"error": "Loss-run job not found"})
    job = await request_cancellation(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"error": "Loss-run job not found"})
    if job["Status"] not in {"queued", "processing", "cancelled"}:
        raise HTTPException(status_code=409, detail={"error": "This loss-run job has already finished"})
    return {
        "jobId": job_id,
        "status": job["Status"],
        "cancelRequested": job["CancelRequested"],
        "message": "Loss-run job cancelled" if job["Status"] == "cancelled"
        else "Cancellation requested. Waiting for the current operation to stop.",
        "businessType": business_type,
    }


async def get_loss_run_jobs(business_type: str = "sac") -> list[dict]:
    jobs = await get_jobs(business_type)
    if not jobs:
        return []

    failures_by_job: dict[str, list[dict]] = {}
    for row in await get_all_failures():
        failures_by_job.setdefault(str(row["JobId"]), []).append(
            {
                "customerNumber": row["CustomerNumber"],
                "reason": row["FailureReason"] or "Report generation failed",
            }
        )

    return [_format_job(job, failures_by_job.get(str(job["JobId"]), [])) for job in jobs]


def _format_job(job: dict, failures: list[dict]) -> dict:
    messages = {
        "waiting_for_worker": "Waiting for the report worker to start.",
        "starting": "Starting report generation.",
        "downloading_template": "Loading the Excel template.",
        "loading_template": "Loading the Excel template.",
        "loading_accounts": "Loading eligible accounts or programs.",
        "querying_loss_run_data": "Retrieving claim data. This step may take several minutes.",
        "generating_reports": "Generating and saving Excel reports.",
        "finished": "Report generation finished.",
        "failed": "Report generation failed. See the error details.",
        "cancelled": "Job cancelled. Any files already generated are available for download.",
    }
    progress = messages.get(job["Phase"], "Processing the loss-run request.")
    if job.get("CancelRequested") and job["Status"] == "processing":
        progress = "Cancellation requested. Waiting for the current operation to stop."
    return {
        "jobId": job["JobId"],
        "businessType": job.get("BusinessType", "sac"),
        "jobType": job["JobType"],
        "reportType": job.get("ReportType", "standard"),
        "triggerSource": job.get("TriggerSource", "manual"),
        "scheduleId": job.get("ScheduleId"),
        "scheduledForDate": format_date_value(job.get("ScheduledForDate")),
        "lossDateFrom": format_date_value(job.get("LossDateFrom")),
        "policyEffectiveDateFrom": format_date_value(job.get("PolicyEffectiveDateFrom")),
        "status": job["Status"],
        "phase": job["Phase"],
        "requestedCount": job["RequestedCount"],
        "processedCount": job["ProcessedCount"],
        "generatedCount": job["GeneratedCount"],
        "failedCount": job["FailedCount"],
        "requestedBy": job["RequestedBy"],
        "createdAt": format_loss_run_timestamp(job["CreatedAt"]),
        "startedAt": format_loss_run_timestamp(job["StartedAt"]),
        "completedAt": format_loss_run_timestamp(job["CompletedAt"]),
        "errorMessage": job["ErrorMessage"],
        "failures": failures,
        "cancelRequested": bool(job.get("CancelRequested", False)),
        "progressMessage": progress,
    }
