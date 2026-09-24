"""Fairlane scheduler and queue management.

Implements priority scheduling with aging and weighted fair queuing (WFQ).
Tasks are enqueued into a per-tenant waiting room (Redis Sorted Set).
The background dispatcher pulls tasks from the waiting room based on WFQ
and pushes them to the execution stream (tasks:stream).
"""

import asyncio
import logging
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import Task, TaskEvent, TaskStatus
from fairlane.redis_client import get_redis_client

logger = logging.getLogger("fairlane.scheduler")

POLL_INTERVAL_SECONDS = 1.0

ENQUEUE_LUA = """
local ready_key = KEYS[1]
local vtime_key = KEYS[2]
local score = tonumber(ARGV[1])
local task_id = ARGV[2]
local tenant_id = ARGV[3]

-- Add task to the tenant's waiting room
redis.call('ZADD', ready_key, score, task_id)

-- Manage virtual time for the tenant to prevent hoarding
local current_vtime = redis.call('ZSCORE', vtime_key, tenant_id)
local min_vtime = 0
local min_member = redis.call('ZRANGE', vtime_key, 0, 0, 'WITHSCORES')
if #min_member > 0 then
    min_vtime = tonumber(min_member[2])
end

if not current_vtime or tonumber(current_vtime) < min_vtime then
    redis.call('ZADD', vtime_key, min_vtime, tenant_id)
end
return 1
"""


async def enqueue(task: Task, redis_client: Redis | None = None) -> None:
    """Enqueue a task into its tenant's waiting room.
    
    The score is calculated based on enqueue time and priority (aging).
    A priority-5 task waiting (4 * AGING_MS_PER_LEVEL) beats a brand-new
    priority-1 task, so starvation is impossible.
    """
    close_redis = False
    if redis_client is None:
        redis_client = get_redis_client()
        close_redis = True

    try:
        # Use original_enqueue_at if set (for retries), else fallback to created_at or now
        enqueue_time = task.original_enqueue_at or task.created_at or datetime.now(UTC)
        enqueue_time_ms = int(enqueue_time.timestamp() * 1000)
        
        # Calculate score: Lower score = served first. Priority 1 is highest.
        # Aging: Older tasks gradually accumulate "negative" score offset relative to new ones.
        # Actually, score is time + penalty. High priority (1) has penalty 0.
        # Low priority (e.g. 5) has penalty (4 * aging_ms).
        score = enqueue_time_ms + (task.priority - 1) * settings.aging_ms_per_level

        ready_key = f"ready:{task.tenant_id}"
        vtime_key = "tenants:vtime"
        
        await redis_client.eval(
            ENQUEUE_LUA,
            2,
            ready_key,
            vtime_key,
            score,
            str(task.id),
            task.tenant_id
        )
    finally:
        if close_redis:
            await redis_client.aclose()


async def rebuild_from_postgres() -> int:
    """Recover the Redis waiting room from Postgres if it was lost.
    
    Finds PENDING tasks with no next_retry_at in the future and re-adds them.
    """
    now = datetime.now(UTC)
    requeued = 0
    redis_client = get_redis_client()
    try:
        async with async_session_factory() as db:
            stmt = select(Task).where(
                Task.status == TaskStatus.PENDING,
                (Task.next_retry_at.is_(None)) | (Task.next_retry_at <= now)
            )
            result = await db.execute(stmt)
            tasks = result.scalars().all()
            
            for task in tasks:
                await enqueue(task, redis_client=redis_client)
                requeued += 1
                
        if requeued > 0:
            logger.info(f"Rebuilt queue with {requeued} pending tasks from Postgres.")
    finally:
        await redis_client.aclose()
        
    return requeued


async def _poll_and_requeue() -> int:
    """Find overdue retries, requeue them, and return the count processed."""
    now = datetime.now(UTC)
    requeued = 0
    redis_client = get_redis_client()

    try:
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

                # Keep original enqueue time so it doesn't lose its place
                if not task.original_enqueue_at:
                    task.original_enqueue_at = task.created_at

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

                # Push the task to the waiting room
                await enqueue(task, redis_client=redis_client)

                logger.info(
                    "Requeued task %s into waiting room",
                    task.id,
                    extra={"task_id": str(task.id)},
                )
                requeued += 1

            if requeued:
                await db.commit()
    finally:
        await redis_client.aclose()

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
