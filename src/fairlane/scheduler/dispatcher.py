"""Background dispatcher loop that pushes tasks from waiting room to stream.

It ensures only one dispatcher is running at a time via a Redis lock.
Applies backpressure by checking stream depth.
"""

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import insert, select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import Task, TaskEvent
from fairlane.redis_client import get_redis_client

logger = logging.getLogger("fairlane.dispatcher")

DISPATCH_LUA_SCRIPT = ""
with open("src/fairlane/scheduler/dispatch.lua") as f:
    DISPATCH_LUA_SCRIPT = f.read()


async def run_dispatcher(stop_event: asyncio.Event) -> None:
    """Run the WFQ dispatcher loop."""
    logger.info("Dispatcher started")
    redis_client = get_redis_client()
    lock_key = "dispatcher:lock"

    try:
        # Load script once
        dispatch_sha = await redis_client.script_load(DISPATCH_LUA_SCRIPT)
        
        while not stop_event.is_set():
            try:
                # Try to acquire lock. TTL is 3 seconds, we renew it if we are active.
                acquired = await redis_client.set(lock_key, "1", nx=True, px=3000)
                if acquired:
                    await _dispatch_cycle(redis_client, dispatch_sha)
                    # Renew lock if cycle took some time, though typically it's fast
                    await redis_client.expire(lock_key, 3)
                
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Unexpected error in dispatcher cycle")

            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=settings.dispatcher_interval_ms / 1000.0,
                )
                break
            except TimeoutError:
                pass

    finally:
        # Best-effort cleanup of lock if we hold it
        if await redis_client.get(lock_key) == b"1":
            await redis_client.delete(lock_key)
        await redis_client.aclose()
        logger.info("Dispatcher stopped")


async def _dispatch_cycle(redis, dispatch_sha: str) -> None:
    """One cycle of checking stream depth and dispatching tasks."""
    # Check current stream depth
    stream_len = await redis.xlen(settings.redis_stream_name)
    
    # Calculate target depth (default = active workers * 2)
    # We can count keys worker:*:alive
    worker_keys = await redis.keys("worker:*:alive")
    active_workers = len(worker_keys)
    target_depth = max(1, active_workers * 2)
    # If the user overrode stream_target_depth in config and it's large, we can use it?
    # The prompt says "(default = number of active workers * 2)".
    # Let's just use the dynamic one if active_workers > 0
    if active_workers == 0:
        target_depth = settings.stream_target_depth

    if stream_len >= target_depth:
        # Backpressure
        return

    # How many we can dispatch
    batch_size = min(settings.dispatch_batch_size, target_depth - stream_len)
    if batch_size <= 0:
        return

    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    # Call Lua script
    dispatched_task_ids = await redis.evalsha(
        dispatch_sha,
        2,
        settings.redis_stream_name,
        "tenants:vtime",
        batch_size,
        settings.max_in_stream_per_tenant,
        now_ms,
    )

    if dispatched_task_ids:
        # Log and add task_events
        await _record_dispatch_events(dispatched_task_ids)


async def _record_dispatch_events(task_ids: list[bytes]) -> None:
    """Batch write DISPATCHED events for the dispatched tasks."""
    now = datetime.now(UTC)
    task_id_strs = [t.decode("utf-8") if isinstance(t, bytes) else t for t in task_ids]
    
    events_to_insert = []
    
    async with async_session_factory() as db:
        # Fetch tasks to get priority, tenant, and enqueue time
        stmt = select(Task).where(Task.id.in_(task_id_strs))
        result = await db.execute(stmt)
        tasks = result.scalars().all()
        
        for task in tasks:
            enqueue_time = task.original_enqueue_at or task.created_at
            wait_time_ms = int((now - enqueue_time).total_seconds() * 1000)
            
            events_to_insert.append(
                {
                    "task_id": task.id,
                    "event_type": "DISPATCHED",
                    "details": {
                        "tenant_id": task.tenant_id,
                        "priority": task.priority,
                        "wait_time_ms": wait_time_ms,
                    },
                    "created_at": now,
                }
            )
            
        if events_to_insert:
            await db.execute(insert(TaskEvent).values(events_to_insert))
            await db.commit()
            
        logger.info(f"Dispatched {len(events_to_insert)} tasks.")
