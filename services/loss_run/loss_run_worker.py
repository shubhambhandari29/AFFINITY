import asyncio
import logging
import os
import socket
from contextlib import suppress
from time import monotonic
from uuid import UUID, uuid4

from services.loss_run.affinity.affinity_loss_run_service import (
    generate_affinity_loss_runs,
)
from services.loss_run.loss_run_cancellation import (
    LossRunCancellation,
    LossRunCancelled,
)
from services.loss_run.loss_run_job_repository import (
    cancel_job,
    claim_next_job,
    complete_job,
    fail_job,
    get_account_numbers,
    get_cancellation_state,
    record_account_result,
    update_heartbeat,
    update_phase,
    upsert_accounts,
)
from services.loss_run.loss_run_scheduler import enqueue_due_loss_run_job
from services.loss_run.loss_run_service import generate_loss_runs

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 10
SCHEDULE_CHECK_INTERVAL_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 60
CANCELLATION_CHECK_INTERVAL_SECONDS = 5


class LossRunWorker:
    def __init__(self) -> None:
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        logger.info("Loss-run worker started: %s", self.worker_id)
        next_schedule_check = 0.0
        while not self._stop_event.is_set():
            if monotonic() >= next_schedule_check:
                try:
                    await enqueue_due_loss_run_job()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A scheduling/configuration failure must not stop manual jobs.
                    logger.exception("Loss-run scheduling check failed")
                finally:
                    next_schedule_check = monotonic() + SCHEDULE_CHECK_INTERVAL_SECONDS
            try:
                job = await claim_next_job(self.worker_id)
                if job:
                    await self._process_job(job)
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Loss-run worker polling failed")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=POLL_INTERVAL_SECONDS,
                )
            except TimeoutError:
                pass

        logger.info("Loss-run worker stopped: %s", self.worker_id)

    async def _process_job(self, job: dict) -> None:
        job_id = UUID(str(job["JobId"]))
        job_type = str(job["JobType"])
        cancellation = LossRunCancellation()
        heartbeat = None

        async def on_phase(phase: str) -> None:
            if await get_cancellation_state(job_id, self.worker_id) is not False:
                cancellation.request()
            cancellation.check()
            await update_phase(job_id, self.worker_id, phase)

        async def on_customers(customers: list[dict]) -> None:
            await upsert_accounts(job_id, customers, job_type)

        async def on_result(
            customer_number: str,
            succeeded: bool,
            reason: str | None,
            output_path: str | None,
        ) -> None:
            await record_account_result(
                job_id,
                customer_number,
                succeeded,
                reason,
                output_path,
            )
            # Check at account boundaries as well as polling, so a batch of
            # quick workbooks does not start more accounts during the poll gap.
            if await get_cancellation_state(job_id, self.worker_id) is not False:
                cancellation.request()

        try:
            state = await get_cancellation_state(job_id, self.worker_id)
            if state is not False:
                cancellation.request()
            cancellation.check()
            heartbeat = asyncio.create_task(self._heartbeat(job_id, cancellation))
            if job.get("PolicyEffectiveDateFrom") and not job.get("LossDateFrom"):
                await fail_job(
                    job_id, self.worker_id,
                    "This job used the old policy-date cutoff. Submit a new job using Loss date from.",
                )
                return
            numbers = None if job_type == "all" else await get_account_numbers(job_id)
            callbacks = dict(on_phase=on_phase, on_customers=on_customers, on_result=on_result,
                             cancellation=cancellation)
            if job.get("BusinessType", "sac") == "affinity":
                await generate_affinity_loss_runs(numbers, **callbacks)
            else:
                await generate_loss_runs(
                    numbers,
                    report_type=job.get("ReportType", "standard"),
                    loss_date_from=job.get("LossDateFrom"),
                    **callbacks,
                )
            cancellation.check()
            await complete_job(job_id, self.worker_id)
        except LossRunCancelled:
            await cancel_job(job_id, self.worker_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Loss-run job %s failed", job_id)
            await fail_job(
                job_id,
                self.worker_id,
                "Loss-run generation failed. Check application logs for details.",
            )
        finally:
            if heartbeat:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat

    async def _heartbeat(self, job_id: UUID, cancellation: LossRunCancellation) -> None:
        next_heartbeat = monotonic() + HEARTBEAT_INTERVAL_SECONDS
        while True:
            await asyncio.sleep(CANCELLATION_CHECK_INTERVAL_SECONDS)
            try:
                state = await get_cancellation_state(job_id, self.worker_id)
                if state is None:
                    cancellation.request()
                    return
                if state:
                    cancellation.request()
                if monotonic() >= next_heartbeat:
                    # Keep the lease alive while the driver acknowledges cancel.
                    await update_heartbeat(job_id, self.worker_id)
                    next_heartbeat = monotonic() + HEARTBEAT_INTERVAL_SECONDS
            except Exception:
                logger.exception("Failed to renew lease for loss-run job %s", job_id)


async def _run_local_worker() -> None:
    worker = LossRunWorker()
    await worker._run()


if __name__ == "__main__":
    asyncio.run(_run_local_worker())
