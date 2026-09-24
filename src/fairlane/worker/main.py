"""Fairlane distributed worker daemon."""

import asyncio
import contextlib
import logging
import os
import signal
import socket
import uuid
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from sqlalchemy import select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.logging_setup import setup_logging
from fairlane.models import Task, TaskEvent, TaskStatus

logger = logging.getLogger("fairlane.worker")


class FairlaneWorker:
    """Async worker daemon for processing tasks from Redis Streams."""

    def __init__(self) -> None:
        self.worker_id = f"worker-{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.stop_event = asyncio.Event()
        self.redis: Redis | None = None

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
        """Execute task lifecycle: RUNNING -> Work -> SUCCEEDED -> XACK."""
        task_id_str = data.get("task_id")
        if not task_id_str:
            logger.warning(
                f"Malformed message without task_id: {data}. Acknowledging.",
                extra={"worker_id": self.worker_id},
            )
            assert self.redis is not None
            await self.redis.xack(settings.redis_stream_name, settings.redis_consumer_group, message_id)
            return

        try:
            task_uuid = uuid.UUID(task_id_str)
        except ValueError:
            logger.error(
                f"Invalid UUID in task message: {task_id_str}. Acknowledging.",
                extra={"worker_id": self.worker_id},
            )
            assert self.redis is not None
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
                assert self.redis is not None
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
                task.status = TaskStatus.RUNNING
                task.started_at = datetime.now(UTC)
                task.attempts += 1

                # Add task_events audit row
                running_event = TaskEvent(
                    task_id=task.id,
                    event_type="RUNNING",
                    details={
                        "worker_id": self.worker_id,
                        "attempt": task.attempts,
                    },
                )
                db.add(running_event)
                await db.commit()

                logger.info(
                    f"Picked up task {task.id} (type: {task.task_type}, tenant: {task.tenant_id}, attempt: {task.attempts})",
                    extra=log_context,
                )

                # TODO: Heartbeat mechanism (send periodic heartbeats while task is running)
                # TODO: Priority-aware task execution

                # 3. Simulate work with a short sleep
                await asyncio.sleep(0.5)

                # 4. Mark Task as SUCCEEDED in Postgres
                task.status = TaskStatus.SUCCEEDED
                task.finished_at = datetime.now(UTC)
                task.last_error = None

                succeeded_event = TaskEvent(
                    task_id=task.id,
                    event_type="SUCCEEDED",
                    details={
                        "worker_id": self.worker_id,
                        "duration_ms": 500,
                    },
                )
                db.add(succeeded_event)
                await db.commit()

                # 5. Acknowledge message in Redis Stream
                assert self.redis is not None
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
                logger.error(
                    f"Error processing task {task.id}: {e}",
                    exc_info=True,
                    extra=log_context,
                )
                # TODO: Implement retry logic with exponential backoff & jitter
                # TODO: Route to Dead Letter Queue (DLQ) if task.attempts >= task.max_attempts
                # Mark as FAILED for now if unhandled
                try:
                    task.status = TaskStatus.FAILED
                    task.last_error = str(e)
                    task.finished_at = datetime.now(UTC)
                    failed_event = TaskEvent(
                        task_id=task.id,
                        event_type="FAILED",
                        details={"error": str(e), "worker_id": self.worker_id},
                    )
                    db.add(failed_event)
                    await db.commit()
                except Exception as db_err:
                    logger.error(f"Failed to record FAILED status in DB: {db_err}", extra=log_context)

    async def run(self) -> None:
        """Main worker loop."""
        setup_logging(settings.log_level)
        logger.info(
            f"Starting Fairlane worker daemon with ID '{self.worker_id}'",
            extra={"worker_id": self.worker_id},
        )

        await self.init_redis()
        assert self.redis is not None

        logger.info(
            f"Worker '{self.worker_id}' listening on stream '{settings.redis_stream_name}'...",
            extra={"worker_id": self.worker_id},
        )

        while not self.stop_event.is_set():
            try:
                # TODO: Implement XAUTOCLAIM for crash recovery of abandoned pending entries (PEL)
                # TODO: Implement worker heartbeat reporting to Redis / DB

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
