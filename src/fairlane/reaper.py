"""Dead-worker reaper — detects stale workers and reclaims their tasks.

Chunk 3C: Detects workers whose Redis heartbeat key has expired and marks them DEAD.
Chunk 3D: Reclaims stuck tasks from dead workers via XAUTOCLAIM and re-delivery.

Uses a Redis distributed lock (`reaper:lock`) so that only one worker
acts as the reaper at a time.
"""

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import (
    DeadLetter,
    FailureCategory,
    Task,
    TaskEvent,
    TaskStatus,
    Worker,
    WorkerStatus,
)
from fairlane.redis_client import push_task_to_stream

logger = logging.getLogger("fairlane.reaper")

REAPER_INTERVAL_SECONDS = 5
REAPER_LOCK_KEY = "reaper:lock"
REAPER_LOCK_TTL = 10  # seconds


async def _detect_and_mark_dead_workers(redis: Redis) -> list[str]:
    """Find ACTIVE workers whose heartbeat key has expired and mark them DEAD.

    Returns the list of worker IDs that were marked DEAD.
    """
    dead_worker_ids: list[str] = []

    async with async_session_factory() as db:
        result = await db.execute(
            select(Worker).where(Worker.status == WorkerStatus.ACTIVE)
        )
        active_workers = result.scalars().all()

        for worker in active_workers:
            redis_key = f"worker:{worker.worker_id}:alive"
            exists = await redis.exists(redis_key)
            if not exists:
                worker.status = WorkerStatus.DEAD
                dead_worker_ids.append(worker.worker_id)
                logger.warning(
                    f"worker {worker.worker_id} declared dead",
                    extra={"dead_worker_id": worker.worker_id},
                )

        if dead_worker_ids:
            await db.commit()
            
            from fairlane.metrics import inc_workers_declared_dead
            inc_workers_declared_dead(len(dead_worker_ids))

    return dead_worker_ids


async def _reclaim_tasks_from_dead_worker(
    redis: Redis,
    dead_worker_id: str,
) -> int:
    """Reclaim pending Redis messages and PostgreSQL tasks from a dead worker.

    Returns the number of tasks reclaimed.
    """
    reclaimed_count = 0

    # --- 1. XAUTOCLAIM: take over pending messages idle longer than heartbeat TTL ---
    min_idle_ms = settings.heartbeat_ttl_seconds * 1000
    reclaimed_task_ids: set[str] = set()

    try:
        # XAUTOCLAIM returns (new_start_id, messages, deleted_ids)
        # Keep calling until we've consumed all idle messages from this consumer
        start_id = "0-0"
        while True:
            result = await redis.xautoclaim(
                name=settings.redis_stream_name,
                groupname=settings.redis_consumer_group,
                consumername="reaper",  # claim under temporary "reaper" consumer
                min_idle_time=min_idle_ms,
                start_id=start_id,
            )

            new_start_id = result[0]
            messages = result[1]

            if not messages:
                break

            for message_id, data in messages:
                task_id_str = data.get("task_id")
                if task_id_str:
                    reclaimed_task_ids.add(task_id_str)

                # ACK and remove from reaper's pending list
                await redis.xack(
                    settings.redis_stream_name,
                    settings.redis_consumer_group,
                    message_id,
                )

            # If new_start_id is "0-0", we've consumed everything
            if new_start_id == "0-0" or new_start_id == start_id:
                break
            start_id = new_start_id

    except Exception as e:
        logger.error(f"XAUTOCLAIM failed for dead worker {dead_worker_id}: {e}")

    # --- 2. Handle PostgreSQL-side orphan tasks ---
    # Tasks that are RUNNING + locked_by = dead_worker but have no Redis pending message
    async with async_session_factory() as db:
        result = await db.execute(
            select(Task).where(
                Task.status == TaskStatus.RUNNING,
                Task.locked_by == dead_worker_id,
            )
        )
        orphan_tasks = result.scalars().all()

        for task in orphan_tasks:
            task_id_str = str(task.id)
            if task_id_str not in reclaimed_task_ids:
                reclaimed_task_ids.add(task_id_str)

        # --- 3. Process each reclaimed task in PostgreSQL ---
        for task_id_str in reclaimed_task_ids:
            try:
                task_uuid = uuid.UUID(task_id_str)
                result = await db.execute(select(Task).where(Task.id == task_uuid))
                task = result.scalar_one_or_none()

                if not task:
                    continue

                # Skip tasks that are already terminal
                if task.status in (TaskStatus.SUCCEEDED, TaskStatus.DEAD):
                    continue

                # Clear lock
                task.locked_by = None
                task.locked_at = None
                task.attempts += 1

                if task.attempts >= task.max_attempts:
                    # --- Exceeded max attempts: move to DLQ as POISON_PILL ---
                    task.status = TaskStatus.DEAD
                    task.finished_at = datetime.now(UTC)
                    task.next_retry_at = None

                    dead_letter = DeadLetter(
                        task_id=task.id,
                        tenant_id=task.tenant_id,
                        task_type=task.task_type,
                        failure_category=FailureCategory.POISON_PILL,
                        last_error=f"Task crashed worker {dead_worker_id} and exceeded max_attempts",
                        error_history=[{
                            "attempt": task.attempts,
                            "error": f"Worker {dead_worker_id} died while running this task",
                            "timestamp": datetime.now(UTC).isoformat(),
                        }],
                        attempts_made=task.attempts,
                    )
                    db.add(dead_letter)

                    dlq_event = TaskEvent(
                        task_id=task.id,
                        event_type="DEAD_LETTERED",
                        details={
                            "dead_worker_id": dead_worker_id,
                            "attempt": task.attempts,
                            "failure_category": FailureCategory.POISON_PILL.value,
                            "reason": "exceeded max_attempts after worker death",
                        },
                    )
                    db.add(dlq_event)

                    logger.warning(
                        f"Task {task.id} moved to DLQ as POISON_PILL "
                        f"(attempt {task.attempts}/{task.max_attempts})",
                        extra={"task_id": task_id_str, "dead_worker_id": dead_worker_id},
                    )
                    
                    from fairlane.metrics import inc_tasks_completed, inc_dlq_total
                    inc_tasks_completed(task.tenant_id, task.task_type, "dead")
                    inc_dlq_total(task.tenant_id, FailureCategory.POISON_PILL.value)
                    
                else:
                    # --- Reclaim: set back to PENDING for re-delivery ---
                    task.status = TaskStatus.PENDING
                    task.next_retry_at = None

                    reclaimed_event = TaskEvent(
                        task_id=task.id,
                        event_type="RECLAIMED",
                        details={
                            "dead_worker_id": dead_worker_id,
                            "attempt": task.attempts,
                            "reason": f"worker {dead_worker_id} died",
                        },
                    )
                    db.add(reclaimed_event)

                    logger.info(
                        f"Task {task.id} reclaimed from dead worker {dead_worker_id} "
                        f"(attempt {task.attempts}/{task.max_attempts})",
                        extra={"task_id": task_id_str, "dead_worker_id": dead_worker_id},
                    )

                reclaimed_count += 1

            except Exception as e:
                logger.error(
                    f"Error reclaiming task {task_id_str} from dead worker {dead_worker_id}: {e}",
                    exc_info=True,
                )

        await db.commit()

    # --- 4. Re-deliver reclaimed tasks to the stream ---
    for task_id_str in reclaimed_task_ids:
        try:
            task_uuid = uuid.UUID(task_id_str)
            async with async_session_factory() as db:
                result = await db.execute(select(Task).where(Task.id == task_uuid))
                task = result.scalar_one_or_none()
                if task and task.status == TaskStatus.PENDING:
                    from fairlane.scheduler import enqueue
                    await enqueue(task, redis_client=redis)
                    logger.info(
                        f"Re-delivered task {task_id_str} to stream",
                        extra={"task_id": task_id_str},
                    )
        except Exception as e:
            logger.error(f"Failed to re-deliver task {task_id_str}: {e}")

    # --- 5. Remove the dead consumer from the consumer group ---
    try:
        await redis.xgroup_delconsumer(
            name=settings.redis_stream_name,
            groupname=settings.redis_consumer_group,
            consumername=dead_worker_id,
        )
        logger.info(
            f"Deleted consumer {dead_worker_id} from group {settings.redis_consumer_group}",
        )
    except Exception as e:
        logger.error(f"Failed to delete consumer {dead_worker_id}: {e}")

    # Also delete the temporary reaper consumer if it was created
    try:
        await redis.xgroup_delconsumer(
            name=settings.redis_stream_name,
            groupname=settings.redis_consumer_group,
            consumername="reaper",
        )
    except Exception:
        pass  # Ignore — may not exist

    return reclaimed_count


async def _reconcile_missing_tasks(redis: Redis) -> None:
    """Re-enqueue PENDING tasks that are not in Redis.
    
    If Redis goes down during submit, tasks are saved as PENDING in Postgres
    but fail to enqueue. This ensures they are eventually pushed.
    """
    from fairlane.scheduler import rebuild_from_postgres
    # Rebuilding from Postgres safely re-enqueues PENDING tasks
    # (ZADD is idempotent, and if it's already in the stream it's fine too)
    # Actually, rebuild_from_postgres pushes ALL PENDING tasks.
    # We could optimize it, but doing it every 5s is heavy. 
    # Let's just find tasks older than 5s that are PENDING and haven't been picked up.
    try:
        async with async_session_factory() as db:
            now = datetime.now(UTC)
            # Find tasks PENDING for more than 10 seconds (gives them time to be processed normally)
            stmt = select(Task).where(
                Task.status == TaskStatus.PENDING,
                Task.next_retry_at.is_(None)
            )
            result = await db.execute(stmt)
            tasks = result.scalars().all()
            
            reconciled = 0
            for task in tasks:
                age = (now - (task.original_enqueue_at or task.created_at)).total_seconds()
                if age > 10.0:
                    from fairlane.scheduler import enqueue
                    await enqueue(task, redis_client=redis)
                    reconciled += 1
            if reconciled > 0:
                logger.info(f"Reconciled {reconciled} missing PENDING tasks to Redis")
    except Exception as e:
        logger.error(f"Failed to reconcile missing tasks: {e}")


async def run_reaper(redis: Redis, stop_event: asyncio.Event) -> None:
    """Background loop that detects dead workers and reclaims their tasks.

    Runs every REAPER_INTERVAL_SECONDS. Uses a Redis distributed lock so
    only one worker runs the reaper at a time.
    """
    logger.info("Reaper started (interval=%ds)", REAPER_INTERVAL_SECONDS)

    while not stop_event.is_set():
        try:
            # Acquire the distributed reaper lock
            acquired = await redis.set(
                REAPER_LOCK_KEY, "1", nx=True, ex=REAPER_LOCK_TTL
            )

            if acquired:
                # --- Phase 1 (Chunk 3C): detect dead workers ---
                dead_worker_ids = await _detect_and_mark_dead_workers(redis)

                # --- Phase 2 (Chunk 3D): reclaim tasks from dead workers ---
                for dead_id in dead_worker_ids:
                    reclaimed = await _reclaim_tasks_from_dead_worker(redis, dead_id)
                    if reclaimed:
                        logger.info(
                            f"Reclaimed {reclaimed} task(s) from dead worker {dead_id}",
                        )
                        from fairlane.metrics import inc_tasks_reclaimed
                        inc_tasks_reclaimed(reclaimed)

                # --- Phase 3: Reconcile missing tasks (Redis down during submit) ---
                await _reconcile_missing_tasks(redis)

        except asyncio.CancelledError:
            logger.info("Reaper cancelled")
            break
        except Exception:
            logger.exception("Unexpected error in reaper loop")

        # Wait for the next cycle
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=REAPER_INTERVAL_SECONDS
            )
            break  # stop_event was set
        except TimeoutError:
            pass  # Normal timeout — loop again

    logger.info("Reaper stopped")
