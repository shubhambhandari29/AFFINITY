"""Run with `python -m scripts.verify_loss_run_sql_cancel` on the target host.

Uses the app's existing DB settings/authentication. Only runs WAITFOR and SELECT;
no application data is changed. Check this on the company laptop and Azure SSH.
"""

import asyncio
from time import monotonic

from services.loss_run.loss_run_cancellation import (
    LossRunCancellation,
    LossRunCancelled,
)


async def main() -> None:
    # Warm up authentication/connection before measuring driver cancellation.
    print("Checking SQL connectivity...")
    await LossRunCancellation().run_query("SELECT 1 AS Connected")
    cancellation = LossRunCancellation()
    task = asyncio.create_task(
        cancellation.run_query("WAITFOR DELAY '00:00:15'; SELECT 1 AS Finished")
    )
    # Wait for the connection/cursor so a slow login cannot produce a false
    # success by cancelling before SQL execution has even begun.
    while cancellation._cursor is None and not task.done():
        await asyncio.sleep(0.05)
    if task.done():
        await task  # Report any connection or permission error directly.
        raise RuntimeError(
            "The test query finished before cancellation could be exercised"
        )
    await asyncio.sleep(2)
    started = monotonic()
    cancellation.request()
    try:
        await task
    except LossRunCancelled:
        elapsed = monotonic() - started
        if elapsed >= 10:
            raise RuntimeError(
                "Cancellation was requested, but the driver did not promptly interrupt SQL"
            ) from None
        print(f"SQL cancellation acknowledged in {elapsed:.2f}s.")
    else:
        raise RuntimeError(
            "Expected SQL cancellation, but the query completed normally"
        )
    assert await LossRunCancellation().run_query("SELECT 1 AS Connected") == [
        {"Connected": 1}
    ]
    print("PASS: cancellation stopped execution and a subsequent query succeeded.")


if __name__ == "__main__":
    asyncio.run(main())
