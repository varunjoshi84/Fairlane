"""Retry scheduler – background loop that requeues tasks whose next_retry_at has elapsed.

The scheduler runs as an asyncio task inside each worker process.  It
periodically polls Postgres for ``PENDING`` tasks whose ``next_retry_at``
timestamp is in the past, then for each one:

1. Locks the row with ``SELECT … FOR UPDATE SKIP LOCKED`` so that
   concurrent workers never pick up the same retry.
2. Clears ``next_retry_at``.
3. Pushes the task id back onto the Redis Stream (``tasks:stream``).
4. Inserts a ``REQUEUED`` audit event into ``task_events``.
"""

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import Task, TaskEvent, TaskStatus
from fairlane.redis_client import push_task_to_stream

logger = logging.getLogger("fairlane.scheduler")

POLL_INTERVAL_SECONDS = 1.0


async def _poll_and_requeue() -> int:
    """Find overdue retries, requeue them, and return the count processed."""
    now = datetime.now(UTC)
    requeued = 0

    async with async_session_factory() as db:
        # Find tasks eligible for retry – lock rows so other workers skip them.
        stmt = (
            select(Task)
            .where(
                Task.status == TaskStatus.PENDING,
                Task.next_retry_at <= now,
                Task.next_retry_at.isnot(None),
            )
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(stmt)
        tasks = result.scalars().all()

        for task in tasks:
            # Clear the retry timestamp so we don't pick it up again.
            task.next_retry_at = None

            # Record the requeue event.
            event = TaskEvent(
                task_id=task.id,
                event_type="REQUEUED",
                details={
                    "reason": "retry timer elapsed",
                    "requeued_at": now.isoformat(),
                },
            )
            db.add(event)

            # Push the task id onto the Redis stream.
            await push_task_to_stream(str(task.id))

            logger.info(
                "Requeued task %s onto stream '%s'",
                task.id,
                settings.redis_stream_name,
                extra={"task_id": str(task.id)},
            )
            requeued += 1

        if requeued:
            await db.commit()

    return requeued


async def run_scheduler(stop_event: asyncio.Event) -> None:
    """Poll Postgres every *POLL_INTERVAL_SECONDS* until *stop_event* is set.

    This coroutine is designed to be launched as an ``asyncio.Task``
    alongside the main worker loop.
    """
    logger.info("Retry scheduler started (poll interval=%.1fs)", POLL_INTERVAL_SECONDS)

    while not stop_event.is_set():
        try:
            requeued = await _poll_and_requeue()
            if requeued:
                logger.debug("Scheduler requeued %d task(s) this cycle", requeued)
        except asyncio.CancelledError:
            logger.info("Retry scheduler cancelled")
            break
        except Exception:
            logger.exception("Unexpected error in retry scheduler")

        # Wait for the next poll, but wake up early if stop_event is set.
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_INTERVAL_SECONDS)
            # If we reach here, stop_event was set – exit the loop.
            break
        except TimeoutError:
            # Normal timeout – loop around and poll again.
            pass

    logger.info("Retry scheduler stopped")
