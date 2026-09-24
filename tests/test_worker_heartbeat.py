"""Tests for worker heartbeat."""

import asyncio
import pytest

from fairlane.worker.main import FairlaneWorker
from fairlane.models import Worker, WorkerStatus
from fairlane.db import async_session_factory


@pytest.mark.asyncio
async def test_worker_heartbeat() -> None:
    """Test that a running worker creates a Redis key and registers in Postgres."""
    worker = FairlaneWorker()
    
    # Initialize redis and ensure group
    await worker.init_redis()
    
    # Emulate run() startup
    async with async_session_factory() as db:
        db_worker = Worker(
            worker_id=worker.worker_id,
            hostname=worker.hostname,
            status=WorkerStatus.ACTIVE,
        )
        await db.merge(db_worker)
        await db.commit()
    
    # Launch heartbeat task
    worker.heartbeat_task = asyncio.create_task(worker.heartbeat_loop())
    
    # Wait for the first heartbeat to execute
    await asyncio.sleep(0.5)
    
    # Check Redis
    redis_key = f"worker:{worker.worker_id}:alive"
    assert worker.redis is not None
    exists = await worker.redis.exists(redis_key)
    assert exists == 1
    
    # Check Postgres
    async with async_session_factory() as db:
        w = await db.get(Worker, worker.worker_id)
        assert w is not None
        assert w.status == WorkerStatus.ACTIVE
        assert w.last_heartbeat_at is not None
        
    # Stop worker
    worker.stop_event.set()
    await worker.cleanup()
    
    # Check Redis key deleted
    exists = await worker.redis.exists(redis_key)
    assert exists == 0
    
    # Check Postgres status updated
    async with async_session_factory() as db:
        w = await db.get(Worker, worker.worker_id)
        assert w is not None
        assert w.status == WorkerStatus.STOPPED
