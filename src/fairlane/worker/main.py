"""Fairlane distributed worker daemon."""

import asyncio
import contextlib
import logging
import os
import random
import signal
import socket
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from sqlalchemy import select, update

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.logging_setup import setup_logging
from fairlane.models import DeadLetter, FailureCategory, Task, TaskEvent, TaskStatus, Worker, WorkerStatus
from fairlane.retry import calculate_backoff, classify_failure, is_retryable
from fairlane.scheduler import run_scheduler
from fairlane.reaper import run_reaper

logger = logging.getLogger("fairlane.worker")


class FairlaneWorker:
    """Async worker daemon for processing tasks from Redis Streams."""

    def __init__(self) -> None:
        self.hostname = socket.gethostname()
        self.worker_id = f"worker-{self.hostname}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.stop_event = asyncio.Event()
        self.redis: Redis | None = None
        self.scheduler_task: asyncio.Task | None = None
        self.heartbeat_task: asyncio.Task | None = None
        self.reaper_task: asyncio.Task | None = None
        self.current_task_id: uuid.UUID | None = None

    async def init_redis(self) -> None:
        """Initialize Redis connection and ensure consumer group exists."""
        self.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        await self._ensure_consumer_group()

    async def _ensure_consumer_group(self) -> None:
        """Create the consumer group on the stream if it does not already exist."""
        assert self.redis is not None
        try:
            await self.redis.xgroup_create(
                name=settings.redis_stream_name,
                groupname=settings.redis_consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                f"Created consumer group '{settings.redis_consumer_group}' on stream '{settings.redis_stream_name}'",
                extra={"worker_id": self.worker_id},
            )
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(
                    f"Consumer group '{settings.redis_consumer_group}' already exists.",
                    extra={"worker_id": self.worker_id},
                )
            else:
                logger.error(
                    f"Failed to create consumer group: {e}",
                    extra={"worker_id": self.worker_id},
                )
                raise

    async def process_task(self, message_id: str, data: dict[str, Any]) -> None:
        """Execute task lifecycle: RUNNING -> Work -> SUCCEEDED / RETRY_SCHEDULED / FAILED -> XACK."""
        task_id_str = data.get("task_id")
        if not task_id_str:
            logger.warning(
                f"Malformed message without task_id: {data}. Acknowledging.",
                extra={"worker_id": self.worker_id},
            )
            if self.redis is not None:
                await self.redis.xack(settings.redis_stream_name, settings.redis_consumer_group, message_id)
            return

        try:
            task_uuid = uuid.UUID(task_id_str)
        except ValueError:
            logger.error(
                f"Invalid UUID in task message: {task_id_str}. Acknowledging.",
                extra={"worker_id": self.worker_id},
            )
            if self.redis is not None:
                await self.redis.xack(settings.redis_stream_name, settings.redis_consumer_group, message_id)
            return

        async with async_session_factory() as db:
            # 1. Fetch Task from DB
            result = await db.execute(select(Task).where(Task.id == task_uuid))
            task = result.scalar_one_or_none()

            if not task:
                logger.warning(
                    f"Task {task_id_str} not found in database. Acknowledging message.",
                    extra={"worker_id": self.worker_id, "task_id": task_id_str},
                )
                if self.redis is not None:
                    await self.redis.xack(settings.redis_stream_name, settings.redis_consumer_group, message_id)
                return

            log_context = {
                "worker_id": self.worker_id,
                "task_id": str(task.id),
                "tenant_id": task.tenant_id,
                "task_type": task.task_type,
            }

            try:
                # 2. Mark Task as RUNNING in Postgres
                now = datetime.now(UTC)
                task.status = TaskStatus.RUNNING
                task.started_at = now
                task.next_retry_at = None
                task.attempts += 1
                task.locked_by = self.worker_id
                task.locked_at = now
                self.current_task_id = task.id

                # Add task_events audit row - STARTED event
                started_event = TaskEvent(
                    task_id=task.id,
                    event_type="STARTED",
                    details={
                        "worker_id": self.worker_id,
                        "attempt": task.attempts,
                    },
                )
                db.add(started_event)
                await db.commit()

                logger.info(
                    f"Picked up task {task.id} (type: {task.task_type}, tenant: {task.tenant_id}, attempt: {task.attempts})",
                    extra={**log_context, "attempt": task.attempts},
                )

                # --- Failure-injection hooks for testing ---
                payload = task.payload or {}

                # 1. Always fail if {"fail": true}
                if payload.get("fail") is True:
                    raise RuntimeError("Injected failure: payload 'fail' is True")

                # 2. Random failure based on probability {"fail_rate": 0.5}
                if "fail_rate" in payload:
                    fail_rate = float(payload["fail_rate"])
                    if random.random() < fail_rate:
                        raise RuntimeError(f"Injected failure: triggered by fail_rate={fail_rate}")

                # 3. Fail until specific attempt threshold reached {"fail_until_attempt": 3}
                if "fail_until_attempt" in payload:
                    fail_until = int(payload["fail_until_attempt"])
                    if task.attempts < fail_until:
                        raise RuntimeError(
                            f"Injected failure: current attempt {task.attempts} < fail_until_attempt {fail_until}"
                        )

                # 4. Typed failure injection {"fail_type": "permanent"|"transient"}
                fail_type = payload.get("fail_type")
                if fail_type == "permanent":
                    raise ValueError(
                        "Injected permanent failure: payload 'fail_type' is 'permanent'"
                    )
                if fail_type == "transient":
                    raise TimeoutError(
                        "Injected transient failure: payload 'fail_type' is 'transient'"
                    )

                # Sleep support: {"sleep": N} sleeps for N seconds
                sleep_seconds = payload.get("sleep")
                if sleep_seconds:
                    sleep_seconds = float(sleep_seconds)
                    logger.info(
                        f"Task {task.id} sleeping for {sleep_seconds}s",
                        extra=log_context,
                    )
                    await asyncio.sleep(sleep_seconds)
                else:
                    # Default short sleep to simulate work
                    await asyncio.sleep(0.5)

                # 4. Mark Task as SUCCEEDED in Postgres — safe completion
                #    Only update if this worker still owns the task (locked_by check)
                result = await db.execute(
                    update(Task)
                    .where(Task.id == task.id, Task.locked_by == self.worker_id)
                    .values(
                        status=TaskStatus.SUCCEEDED,
                        finished_at=datetime.now(UTC),
                        next_retry_at=None,
                        last_error=None,
                        locked_by=None,
                        locked_at=None,
                    )
                )

                if result.rowcount == 0:
                    # This worker lost ownership — another worker reclaimed the task
                    logger.warning(
                        f"Task {task.id}: lost ownership, discarding result",
                        extra=log_context,
                    )
                    if self.redis is not None:
                        await self.redis.xack(
                            settings.redis_stream_name,
                            settings.redis_consumer_group,
                            message_id,
                        )
                    self.current_task_id = None
                    return

                succeeded_event = TaskEvent(
                    task_id=task.id,
                    event_type="SUCCEEDED",
                    details={
                        "worker_id": self.worker_id,
                        "duration_ms": int((sleep_seconds if sleep_seconds else 0.5) * 1000),
                    },
                )
                db.add(succeeded_event)
                await db.commit()

                # 5. Acknowledge message in Redis Stream
                if self.redis is not None:
                    await self.redis.xack(
                        settings.redis_stream_name,
                        settings.redis_consumer_group,
                        message_id,
                    )

                logger.info(
                    f"Task {task.id} finished SUCCEEDED",
                    extra=log_context,
                )

            except Exception as e:
                await db.rollback()
                retryable = is_retryable(e)

                try:
                    from sqlalchemy.orm import selectinload
                    result = await db.execute(
                        select(Task)
                        .where(Task.id == task_uuid)
                        .options(selectinload(Task.events))
                    )
                    failed_task = result.scalar_one_or_none()
                    if failed_task:
                        # Build cumulative error history from task_events.
                        error_entry = {
                            "attempt": failed_task.attempts,
                            "error": str(e),
                            "error_type": type(e).__name__,
                            "timestamp": datetime.now(UTC).isoformat(),
                            "worker_id": self.worker_id,
                        }

                        # Classify the failure.
                        category = classify_failure(
                            e,
                            attempts=failed_task.attempts,
                            max_attempts=failed_task.max_attempts,
                        )

                        if retryable and failed_task.attempts < failed_task.max_attempts:
                            # --- Transient error with retries remaining: schedule retry ---
                            delay_seconds = calculate_backoff(failed_task.attempts)
                            next_retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)

                            failed_task.status = TaskStatus.PENDING
                            failed_task.next_retry_at = next_retry_at
                            failed_task.last_error = str(e)
                            failed_task.locked_by = None
                            failed_task.locked_at = None

                            retry_event = TaskEvent(
                                task_id=failed_task.id,
                                event_type="RETRY_SCHEDULED",
                                details={
                                    "error": str(e),
                                    "worker_id": self.worker_id,
                                    "attempt": failed_task.attempts,
                                    "max_attempts": failed_task.max_attempts,
                                    "delay_seconds": round(delay_seconds, 2),
                                    "next_retry_at": next_retry_at.isoformat(),
                                },
                            )
                            db.add(retry_event)
                            await db.commit()

                            logger.info(
                                f"Task {failed_task.id} failed (attempt {failed_task.attempts}/{failed_task.max_attempts}). "
                                f"Retry scheduled in {delay_seconds:.2f}s at {next_retry_at.isoformat()}",
                                extra={
                                    **log_context,
                                    "attempt": failed_task.attempts,
                                    "max_attempts": failed_task.max_attempts,
                                    "delay_seconds": round(delay_seconds, 2),
                                    "next_retry_at": next_retry_at.isoformat(),
                                    "retryable": True,
                                },
                            )
                        else:
                            # --- Terminal failure: move to DLQ ---
                            # For permanent errors, category is PERMANENT.
                            # For transient errors with exhausted retries, category is TRANSIENT_EXHAUSTED.
                            if not retryable:
                                category = FailureCategory.PERMANENT
                            else:
                                category = FailureCategory.TRANSIENT_EXHAUSTED

                            # Collect error history from all previous RETRY_SCHEDULED
                            # and RUNNING events for this task.
                            error_history = []
                            for evt in (failed_task.events or []):
                                if evt.event_type in ("RETRY_SCHEDULED", "FAILED") and evt.details.get("error"):
                                    error_history.append({
                                        "attempt": evt.details.get("attempt"),
                                        "error": evt.details.get("error"),
                                        "timestamp": evt.created_at.isoformat() if evt.created_at else None,
                                    })
                            # Append the current (final) error.
                            error_history.append(error_entry)

                            now = datetime.now(UTC)

                            # 1. Set task status to DEAD
                            failed_task.status = TaskStatus.DEAD
                            failed_task.finished_at = now
                            failed_task.next_retry_at = None
                            failed_task.last_error = str(e)
                            failed_task.locked_by = None
                            failed_task.locked_at = None

                            # 2. Insert dead_letters row
                            dead_letter = DeadLetter(
                                task_id=failed_task.id,
                                tenant_id=failed_task.tenant_id,
                                task_type=failed_task.task_type,
                                failure_category=category,
                                last_error=str(e),
                                error_history=error_history,
                                attempts_made=failed_task.attempts,
                                dead_at=now,
                            )
                            db.add(dead_letter)

                            # 3. Add DEAD_LETTERED event
                            dead_event = TaskEvent(
                                task_id=failed_task.id,
                                event_type="DEAD_LETTERED",
                                details={
                                    "error": str(e),
                                    "error_type": type(e).__name__,
                                    "worker_id": self.worker_id,
                                    "attempt": failed_task.attempts,
                                    "max_attempts": failed_task.max_attempts,
                                    "failure_category": category.value,
                                    "retryable": retryable,
                                },
                            )
                            db.add(dead_event)

                            # Commit all three changes in one transaction.
                            await db.commit()

                            logger.error(
                                f"Task {failed_task.id} dead-lettered as {category.value} "
                                f"(attempt {failed_task.attempts}/{failed_task.max_attempts}): {e}",
                                extra={
                                    **log_context,
                                    "attempt": failed_task.attempts,
                                    "max_attempts": failed_task.max_attempts,
                                    "failure_category": category.value,
                                    "retryable": retryable,
                                },
                            )
                except Exception as db_err:
                    logger.error(f"Failed to record failure/retry state in DB: {db_err}", extra=log_context)

                # XACK the message from the stream
                if self.redis is not None:
                    await self.redis.xack(
                        settings.redis_stream_name,
                        settings.redis_consumer_group,
                        message_id,
                    )

            self.current_task_id = None

    async def heartbeat_loop(self) -> None:
        """Background task to report worker health to Redis and PostgreSQL."""
        assert self.redis is not None
        redis_key = f"worker:{self.worker_id}:alive"

        while not self.stop_event.is_set():
            try:
                # Set Redis key with TTL
                await self.redis.set(
                    redis_key,
                    "1",
                    ex=settings.heartbeat_ttl_seconds
                )

                # Update PostgreSQL
                async with async_session_factory() as db:
                    worker = await db.get(Worker, self.worker_id)
                    if worker:
                        worker.last_heartbeat_at = datetime.now(UTC)
                        worker.current_task_id = self.current_task_id
                        await db.commit()
            except Exception as e:
                logger.error(
                    f"Heartbeat failed: {e}",
                    extra={"worker_id": self.worker_id},
                )

            # Sleep until next heartbeat interval or stop_event is set
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=settings.heartbeat_interval_seconds
                )
            except asyncio.TimeoutError:
                pass  # Expected, time to heartbeat again

    async def run(self) -> None:
        """Main worker loop."""
        setup_logging(settings.log_level)
        logger.info(
            f"Starting Fairlane worker daemon with ID '{self.worker_id}'",
            extra={"worker_id": self.worker_id},
        )

        await self.init_redis()
        assert self.redis is not None

        # Register worker in DB as ACTIVE
        async with async_session_factory() as db:
            worker = Worker(
                worker_id=self.worker_id,
                hostname=self.hostname,
                status=WorkerStatus.ACTIVE,
                started_at=datetime.now(UTC),
                last_heartbeat_at=datetime.now(UTC),
            )
            await db.merge(worker)
            await db.commit()

        # Launch the retry scheduler as a background task.
        self.scheduler_task = asyncio.create_task(run_scheduler(self.stop_event))

        # Launch heartbeat task
        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())

        # Launch reaper task (dead worker detection + task reclamation)
        self.reaper_task = asyncio.create_task(run_reaper(self.redis, self.stop_event))

        logger.info(
            f"Worker '{self.worker_id}' listening on stream '{settings.redis_stream_name}'...",
            extra={"worker_id": self.worker_id},
        )

        while not self.stop_event.is_set():
            try:
                # Read messages using XREADGROUP (block for 2000ms)
                messages = await self.redis.xreadgroup(
                    groupname=settings.redis_consumer_group,
                    consumername=self.worker_id,
                    streams={settings.redis_stream_name: ">"},
                    count=1,
                    block=2000,
                )

                if not messages:
                    continue

                for _stream_name, stream_messages in messages:
                    for message_id, data in stream_messages:
                        if self.stop_event.is_set():
                            break
                        await self.process_task(message_id, data)

            except RedisConnectionError as e:
                logger.warning(
                    f"Redis connection error in worker loop: {e}. Retrying in 2s...",
                    extra={"worker_id": self.worker_id},
                )
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                logger.info(
                    "Worker task cancelled.",
                    extra={"worker_id": self.worker_id},
                )
                break
            except Exception as e:
                logger.error(
                    f"Unexpected error in worker loop: {e}",
                    exc_info=True,
                    extra={"worker_id": self.worker_id},
                )
                await asyncio.sleep(1.0)

        await self.cleanup()

    async def cleanup(self) -> None:
        """Close connections cleanly on shutdown."""
        logger.info(
            f"Shutting down worker '{self.worker_id}'...",
            extra={"worker_id": self.worker_id},
        )
        # Stop the retry scheduler.
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.scheduler_task

        # Stop the heartbeat task.
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.heartbeat_task

        # Stop the reaper task.
        if self.reaper_task and not self.reaper_task.done():
            self.reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reaper_task

        try:
            if self.redis:
                await self.redis.delete(f"worker:{self.worker_id}:alive")
            
            async with async_session_factory() as db:
                worker = await db.get(Worker, self.worker_id)
                if worker:
                    worker.status = WorkerStatus.STOPPED
                    await db.commit()
        except Exception as e:
            logger.error(f"Failed to cleanly stop worker in DB/Redis: {e}")

        if self.redis:
            await self.redis.aclose()
        logger.info(
            f"Worker '{self.worker_id}' stopped gracefully.",
            extra={"worker_id": self.worker_id},
        )


def handle_shutdown(worker: FairlaneWorker, _loop: asyncio.AbstractEventLoop) -> None:
    """Signal handler for graceful termination."""
    logger.info(f"Received termination signal. Stopping worker {worker.worker_id}...")
    worker.stop_event.set()


async def main() -> None:
    """Entrypoint for the worker daemon."""
    worker = FairlaneWorker()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: handle_shutdown(worker, loop))

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
