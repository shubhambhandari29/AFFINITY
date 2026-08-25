from uuid import UUID, uuid4

from fastapi.concurrency import run_in_threadpool

from db import db_connection

JOB_TABLE = "dbo.tblLossRunJob"
ACCOUNT_TABLE = "dbo.tblLossRunJobAccount"


def _rows_to_dicts(cursor) -> list[dict]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _create_job(
    job_type: str,
    requested_by: str,
    customer_numbers: list[str] | None,
) -> tuple[UUID, bool]:
    job_id = uuid4()

    with db_connection() as connection:
        cursor = connection.cursor()

        if job_type == "all":
            cursor.execute(
                f"""
                SELECT TOP (1) JobId
                FROM {JOB_TABLE} WITH (UPDLOCK, HOLDLOCK)
                WHERE JobType = 'all'
                  AND Status IN ('queued', 'processing')
                ORDER BY CreatedAt DESC
                """
            )
            existing = cursor.fetchone()
            if existing:
                connection.commit()
                return UUID(str(existing[0])), False

        requested_count = (
            len(customer_numbers) if customer_numbers is not None else None
        )
        cursor.execute(
            f"""
            INSERT INTO {JOB_TABLE}
            (
                JobId,
                JobType,
                Status,
                Phase,
                RequestedCount,
                RequestedBy
            )
            VALUES (?, ?, 'queued', 'waiting_for_worker', ?, ?)
            """,
            str(job_id),
            job_type,
            requested_count,
            requested_by,
        )

        if customer_numbers:
            cursor.executemany(
                f"""
                INSERT INTO {ACCOUNT_TABLE}
                    (JobId, CustomerNumber, Status)
                VALUES (?, ?, 'queued')
                """,
                [
                    (str(job_id), customer_number)
                    for customer_number in customer_numbers
                ],
            )

        connection.commit()

    return job_id, True


async def create_job(
    job_type: str,
    requested_by: str,
    customer_numbers: list[str] | None,
) -> tuple[UUID, bool]:
    return await run_in_threadpool(
        _create_job,
        job_type,
        requested_by,
        customer_numbers,
    )


def _get_job(job_id: UUID) -> dict | None:
    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT
                JobId,
                JobType,
                Status,
                Phase,
                RequestedCount,
                ProcessedCount,
                GeneratedCount,
                FailedCount,
                RequestedBy,
                CreatedAt,
                StartedAt,
                CompletedAt,
                ErrorMessage
            FROM {JOB_TABLE}
            WHERE JobId = ?
            """,
            str(job_id),
        )
        rows = _rows_to_dicts(cursor)
        return rows[0] if rows else None


async def get_job(job_id: UUID) -> dict | None:
    return await run_in_threadpool(_get_job, job_id)


def _get_failures(job_id: UUID) -> list[dict]:
    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT CustomerNumber, FailureReason
            FROM {ACCOUNT_TABLE}
            WHERE JobId = ?
              AND Status = 'failed'
            ORDER BY CustomerNumber
            """,
            str(job_id),
        )
        return _rows_to_dicts(cursor)


async def get_failures(job_id: UUID) -> list[dict]:
    return await run_in_threadpool(_get_failures, job_id)


def _get_account_numbers(job_id: UUID) -> list[str]:
    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT CustomerNumber
            FROM {ACCOUNT_TABLE}
            WHERE JobId = ?
            ORDER BY CustomerNumber
            """,
            str(job_id),
        )
        return [str(row[0]).strip() for row in cursor.fetchall()]


async def get_account_numbers(job_id: UUID) -> list[str]:
    return await run_in_threadpool(_get_account_numbers, job_id)


def _claim_next_job(worker_id: str) -> dict | None:
    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            UPDATE {JOB_TABLE}
            SET
                Status = 'failed',
                Phase = 'failed',
                ErrorMessage = 'Job stopped after the maximum retry attempts.',
                CompletedAt = SYSUTCDATETIME(),
                UpdatedAt = SYSUTCDATETIME(),
                LeaseUntil = NULL
            WHERE Status = 'processing'
              AND LeaseUntil < SYSUTCDATETIME()
              AND AttemptCount >= 3
            """
        )
        cursor.execute(
            f"""
            ;WITH NextJob AS
            (
                SELECT TOP (1) *
                FROM {JOB_TABLE} WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE
                    Status = 'queued'
                    OR
                    (
                        Status = 'processing'
                        AND LeaseUntil < SYSUTCDATETIME()
                        AND AttemptCount < 3
                    )
                ORDER BY CreatedAt
            )
            UPDATE NextJob
            SET
                Status = 'processing',
                Phase = 'starting',
                WorkerId = ?,
                StartedAt = COALESCE(StartedAt, SYSUTCDATETIME()),
                UpdatedAt = SYSUTCDATETIME(),
                LeaseUntil = DATEADD(MINUTE, 5, SYSUTCDATETIME()),
                LastHeartbeatAt = SYSUTCDATETIME(),
                AttemptCount = AttemptCount + 1
            OUTPUT
                inserted.JobId,
                inserted.JobType,
                inserted.AttemptCount;
            """,
            worker_id,
        )
        rows = _rows_to_dicts(cursor)
        connection.commit()
        return rows[0] if rows else None


async def claim_next_job(worker_id: str) -> dict | None:
    return await run_in_threadpool(_claim_next_job, worker_id)


def _update_heartbeat(job_id: UUID, worker_id: str) -> None:
    with db_connection() as connection:
        connection.execute(
            f"""
            UPDATE {JOB_TABLE}
            SET
                LeaseUntil = DATEADD(MINUTE, 5, SYSUTCDATETIME()),
                LastHeartbeatAt = SYSUTCDATETIME(),
                UpdatedAt = SYSUTCDATETIME()
            WHERE JobId = ?
              AND WorkerId = ?
              AND Status = 'processing'
            """,
            str(job_id),
            worker_id,
        )
        connection.commit()


async def update_heartbeat(job_id: UUID, worker_id: str) -> None:
    await run_in_threadpool(_update_heartbeat, job_id, worker_id)


def _update_phase(job_id: UUID, worker_id: str, phase: str) -> None:
    with db_connection() as connection:
        connection.execute(
            f"""
            UPDATE {JOB_TABLE}
            SET Phase = ?, UpdatedAt = SYSUTCDATETIME()
            WHERE JobId = ?
              AND WorkerId = ?
              AND Status = 'processing'
            """,
            phase,
            str(job_id),
            worker_id,
        )
        connection.commit()


async def update_phase(job_id: UUID, worker_id: str, phase: str) -> None:
    await run_in_threadpool(_update_phase, job_id, worker_id, phase)


def _upsert_accounts(job_id: UUID, customers: list[dict], job_type: str) -> None:
    with db_connection() as connection:
        cursor = connection.cursor()
        for customer in customers:
            customer_number = str(customer["CustomerNum"]).strip()
            customer_name = str(customer.get("CustomerName") or customer_number).strip()
            cursor.execute(
                f"""
                MERGE {ACCOUNT_TABLE} AS target
                USING
                (
                    SELECT
                        CAST(? AS UNIQUEIDENTIFIER) AS JobId,
                        CAST(? AS VARCHAR(50)) AS CustomerNumber,
                        CAST(? AS NVARCHAR(300)) AS CustomerName
                ) AS source
                ON target.JobId = source.JobId
                   AND target.CustomerNumber = source.CustomerNumber
                WHEN MATCHED THEN
                    UPDATE SET
                        CustomerName = source.CustomerName,
                        UpdatedAt = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (JobId, CustomerNumber, CustomerName, Status)
                    VALUES
                    (
                        source.JobId,
                        source.CustomerNumber,
                        source.CustomerName,
                        'queued'
                    );
                """,
                str(job_id),
                customer_number,
                customer_name,
            )

        if job_type == "all":
            cursor.execute(
                f"""
                UPDATE {JOB_TABLE}
                SET
                    RequestedCount = ?,
                    UpdatedAt = SYSUTCDATETIME()
                WHERE JobId = ?
                """,
                len(customers),
                str(job_id),
            )
        connection.commit()


async def upsert_accounts(job_id: UUID, customers: list[dict], job_type: str) -> None:
    await run_in_threadpool(_upsert_accounts, job_id, customers, job_type)


def _record_account_result(
    job_id: UUID,
    customer_number: str,
    succeeded: bool,
    reason: str | None,
    output_path: str | None,
) -> None:
    status = "completed" if succeeded else "failed"
    generated_increment = 1 if succeeded else 0
    failed_increment = 0 if succeeded else 1

    with db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            UPDATE {ACCOUNT_TABLE}
            SET
                Status = ?,
                FailureReason = ?,
                OutputPath = ?,
                CompletedAt = SYSUTCDATETIME(),
                UpdatedAt = SYSUTCDATETIME()
            WHERE JobId = ?
              AND CustomerNumber = ?
              AND Status <> 'completed'
              AND Status <> 'failed'
            """,
            status,
            reason,
            output_path,
            str(job_id),
            customer_number,
        )

        if cursor.rowcount:
            cursor.execute(
                f"""
                UPDATE {JOB_TABLE}
                SET
                    ProcessedCount = ProcessedCount + 1,
                    GeneratedCount = GeneratedCount + ?,
                    FailedCount = FailedCount + ?,
                    UpdatedAt = SYSUTCDATETIME()
                WHERE JobId = ?
                """,
                generated_increment,
                failed_increment,
                str(job_id),
            )
        connection.commit()


async def record_account_result(
    job_id: UUID,
    customer_number: str,
    succeeded: bool,
    reason: str | None = None,
    output_path: str | None = None,
) -> None:
    await run_in_threadpool(
        _record_account_result,
        job_id,
        customer_number,
        succeeded,
        reason,
        output_path,
    )


def _complete_job(job_id: UUID, worker_id: str) -> None:
    with db_connection() as connection:
        connection.execute(
            f"""
            UPDATE {JOB_TABLE}
            SET
                Status = CASE
                    WHEN FailedCount = 0 THEN 'completed'
                    ELSE 'partially_completed'
                END,
                Phase = 'finished',
                CompletedAt = SYSUTCDATETIME(),
                UpdatedAt = SYSUTCDATETIME(),
                LeaseUntil = NULL,
                LastHeartbeatAt = SYSUTCDATETIME()
            WHERE JobId = ?
              AND WorkerId = ?
              AND Status = 'processing'
            """,
            str(job_id),
            worker_id,
        )
        connection.commit()


async def complete_job(job_id: UUID, worker_id: str) -> None:
    await run_in_threadpool(_complete_job, job_id, worker_id)


def _fail_job(job_id: UUID, worker_id: str, message: str) -> None:
    with db_connection() as connection:
        connection.execute(
            f"""
            UPDATE {JOB_TABLE}
            SET
                Status = 'failed',
                Phase = 'failed',
                ErrorMessage = ?,
                CompletedAt = SYSUTCDATETIME(),
                UpdatedAt = SYSUTCDATETIME(),
                LeaseUntil = NULL,
                LastHeartbeatAt = SYSUTCDATETIME()
            WHERE JobId = ?
              AND WorkerId = ?
              AND Status = 'processing'
            """,
            message,
            str(job_id),
            worker_id,
        )
        connection.commit()


async def fail_job(job_id: UUID, worker_id: str, message: str) -> None:
    await run_in_threadpool(_fail_job, job_id, worker_id, message)
