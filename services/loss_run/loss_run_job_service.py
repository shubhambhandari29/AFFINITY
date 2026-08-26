from uuid import UUID

from fastapi import HTTPException

from services.loss_run.loss_run_job_repository import (
    create_job,
    get_all_failures,
    get_failures,
    get_job,
    get_jobs,
)


def _requested_by(current_user: dict) -> str:
    user = current_user.get("user") or {}
    return str(user.get("email") or user.get("id") or "unknown").strip()


async def create_loss_run_job(
    job_type: str,
    current_user: dict,
    customer_numbers: list[str] | None = None,
) -> dict:
    normalized_numbers = None
    if customer_numbers is not None:
        normalized_numbers = list(
            dict.fromkeys(
                str(number).strip()
                for number in customer_numbers
                if str(number).strip()
            )
        )
        if not normalized_numbers:
            raise HTTPException(
                status_code=400,
                detail={"error": "At least one customer number is required"},
            )

    job_id, created = await create_job(
        job_type,
        _requested_by(current_user),
        normalized_numbers,
    )

    if created:
        return {
            "jobId": job_id,
            "status": "queued",
            "message": "Loss-run generation has been queued",
        }

    existing = await get_job(job_id)
    return {
        "jobId": job_id,
        "status": existing["Status"],
        "message": "A generate-all loss-run job is already active",
    }


async def get_loss_run_job(job_id: UUID) -> dict:
    job = await get_job(job_id)
    if job is None:
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


async def get_loss_run_jobs() -> list[dict]:
    jobs = await get_jobs()
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

    return [
        _format_job(job, failures_by_job.get(str(job["JobId"]), [])) for job in jobs
    ]


def _format_job(job: dict, failures: list[dict]) -> dict:
    return {
        "jobId": job["JobId"],
        "jobType": job["JobType"],
        "status": job["Status"],
        "phase": job["Phase"],
        "requestedCount": job["RequestedCount"],
        "processedCount": job["ProcessedCount"],
        "generatedCount": job["GeneratedCount"],
        "failedCount": job["FailedCount"],
        "requestedBy": job["RequestedBy"],
        "createdAt": job["CreatedAt"],
        "startedAt": job["StartedAt"],
        "completedAt": job["CompletedAt"],
        "errorMessage": job["ErrorMessage"],
        "failures": failures,
    }
