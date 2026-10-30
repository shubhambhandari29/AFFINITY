"""Cancel this worker's own SQL cursor, waiting for execution to actually stop."""

import asyncio
import logging
from contextlib import suppress
from threading import Event, Lock

from fastapi.concurrency import run_in_threadpool

from db import db_connection

logger = logging.getLogger(__name__)


class LossRunCancelled(Exception):
    """A persisted cancellation request was observed at a safe stopping point."""


class LossRunCancellation:
    def __init__(self) -> None:
        self._requested = Event()
        self._signal = asyncio.Event()
        self._lock = Lock()
        self._cursor = None

    def request(self) -> None:
        # Called on the event-loop thread; the SQL thread reads _requested.
        self._requested.set()
        self._signal.set()

    def check(self) -> None:
        if self._requested.is_set():
            raise LossRunCancelled()

    def _cancel_cursor(self) -> None:
        # close() and cancel() must not race on a cursor. Never hold this lock
        # around execute/fetch, since cancel must run concurrently with them.
        with self._lock:
            if self._cursor is not None:
                try:
                    self._cursor.cancel()
                except Exception:
                    logger.warning(
                        "SQL cursor cancellation has not completed", exc_info=True
                    )

    def _execute(self, query: str, params: list | None) -> list[dict]:
        self.check()
        try:
            with db_connection() as connection:
                cursor = connection.cursor()
                try:
                    with self._lock:
                        self._cursor = cursor
                    self.check()
                    cursor.execute(query, params or [])
                    columns = [column[0] for column in cursor.description]
                    records = []
                    while True:
                        self.check()
                        rows = cursor.fetchmany(1000)
                        if not rows:
                            return records
                        records.extend(
                            dict(zip(columns, row, strict=True)) for row in rows
                        )
                finally:
                    with self._lock:
                        self._cursor = None
                        cursor.close()
        except Exception as exc:
            if self._requested.is_set():
                raise LossRunCancelled() from exc
            raise

    async def _wait_for_stop(self, task: asyncio.Task) -> None:
        while not task.done():
            await run_in_threadpool(self._cancel_cursor)
            # Repeat cancellation to cover a request arriving just before execute.
            await asyncio.wait({task}, timeout=1)

    async def run_query(self, query: str, params: list | None = None) -> list[dict]:
        self.check()
        task = asyncio.create_task(run_in_threadpool(self._execute, query, params))
        signal = asyncio.create_task(self._signal.wait())
        try:
            await asyncio.wait({task, signal}, return_when=asyncio.FIRST_COMPLETED)
            if self._requested.is_set():
                await self._wait_for_stop(task)
            result = await task
            self.check()
            return result
        except asyncio.CancelledError:
            # Shutdown must not leave a SQL thread running after this job exits.
            self.request()
            await self._wait_for_stop(task)
            with suppress(Exception):
                task.result()
            raise
        finally:
            signal.cancel()
            with suppress(asyncio.CancelledError):
                await signal
