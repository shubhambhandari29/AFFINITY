import asyncio
import logging
import os
import socket
from contextlib import suppress
from uuid import UUID, uuid4

from services.loss_run.loss_run_job_repository import (
    claim_next_job,
    complete_job,
    fail_job,
    get_account_numbers,
    record_account_result,
    update_heartbeat,
    update_phase,
    upsert_accounts,
)
from services.loss_run.loss_run_service import generate_loss_runs

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 10
HEARTBEAT_INTERVAL_SECONDS = 60


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
        while not self._stop_event.is_set():
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
        heartbeat = asyncio.create_task(self._heartbeat(job_id))

        async def on_phase(phase: str) -> None:
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

        try:
            await generate_loss_runs(
                None if job_type == "all" else await self._selected_accounts(job_id),
                on_phase=on_phase,
                on_customers=on_customers,
                on_result=on_result,
            )
            await complete_job(job_id, self.worker_id)
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
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _selected_accounts(self, job_id: UUID) -> list[str]:
        return await get_account_numbers(job_id)

    async def _heartbeat(self, job_id: UUID) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                await update_heartbeat(job_id, self.worker_id)
            except Exception:
                logger.exception("Failed to renew lease for loss-run job %s", job_id)


async def _run_local_worker() -> None:
    worker = LossRunWorker()
    await worker._run()


if __name__ == "__main__":
    asyncio.run(_run_local_worker())
