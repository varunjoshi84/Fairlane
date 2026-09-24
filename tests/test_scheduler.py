import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import Task, TaskStatus, TenantLimit
from fairlane.redis_client import get_redis_client
from fairlane.scheduler import enqueue, rebuild_from_postgres
from fairlane.scheduler.dispatcher import _dispatch_cycle, DISPATCH_LUA_SCRIPT


@pytest.fixture(autouse=True)
async def cleanup_redis_and_db():
    """Clean up Redis keys and DB tasks before and after each test."""
    from sqlalchemy import delete
    redis = get_redis_client()
    await redis.flushall()
    async with async_session_factory() as db:
        await db.execute(delete(Task))
        await db.commit()
    yield
    await redis.flushall()
    async with async_session_factory() as db:
        await db.execute(delete(Task))
        await db.commit()
    await redis.aclose()


@pytest.fixture
async def _dispatch_sha():
    redis = get_redis_client()
    sha = await redis.script_load(DISPATCH_LUA_SCRIPT)
    await redis.aclose()
    return sha


async def _create_task(tenant_id: str, priority: int, created_at: datetime) -> Task:
    task_id = uuid.uuid4()
    task = Task(
        id=task_id,
        tenant_id=tenant_id,
        task_type="test",
        priority=priority,
        status=TaskStatus.PENDING,
        created_at=created_at,
        original_enqueue_at=created_at,
        max_attempts=3,
    )
    async with async_session_factory() as db:
        db.add(task)
        await db.commit()
    return task


@pytest.mark.asyncio
async def test_priority_order(_dispatch_sha):
    """Priority 1 task beats older Priority 5 task if age gap is small."""
    now = datetime.now(UTC)
    redis = get_redis_client()
    try:
        task5 = await _create_task("tenant_A", 5, now - timedelta(seconds=1))
        task1 = await _create_task("tenant_A", 1, now)

        await enqueue(task5, redis)
        await enqueue(task1, redis)

        # Dispatch 1 task
        dispatched = await redis.evalsha(
            _dispatch_sha, 2, settings.redis_stream_name, "tenants:vtime", 1, 10, int(now.timestamp() * 1000)
        )
        assert len(dispatched) == 1
        assert dispatched[0] == str(task1.id)
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_aging(_dispatch_sha):
    """Old Priority 9 task beats new Priority 1 task due to aging."""
    now = datetime.now(UTC)
    redis = get_redis_client()
    
    # Temporarily override settings
    old_aging = settings.aging_ms_per_level
    settings.aging_ms_per_level = 100
    try:
        # Priority 9 has penalty of 8 * 100 = 800ms
        # If it's older than 800ms, it should beat a brand new priority 1 task.
        task9 = await _create_task("tenant_A", 9, now - timedelta(seconds=1))
        task1 = await _create_task("tenant_A", 1, now)

        await enqueue(task9, redis)
        await enqueue(task1, redis)

        dispatched = await redis.evalsha(
            _dispatch_sha, 2, settings.redis_stream_name, "tenants:vtime", 1, 10, int(now.timestamp() * 1000)
        )
        assert len(dispatched) == 1
        assert dispatched[0] == str(task9.id)
    finally:
        settings.aging_ms_per_level = old_aging
        await redis.aclose()


@pytest.mark.asyncio
async def test_weighted_share(_dispatch_sha):
    """Tenant A (weight 3) and Tenant B (weight 1). Over 200 dispatches, A gets ~150, B gets ~50."""
    now = datetime.now(UTC)
    redis = get_redis_client()
    try:
        await redis.delete("instream:A", "instream:B", "ready:A", "ready:B", "tenants:vtime", "tasks:stream")
        await redis.set("tenant_weight:A", "3")
        await redis.set("tenant_weight:B", "1")

        # Create tasks directly in Redis for speed
        for i in range(400):
            await enqueue(Task(id=uuid.uuid4(), tenant_id="A", priority=5, original_enqueue_at=now), redis)
            await enqueue(Task(id=uuid.uuid4(), tenant_id="B", priority=5, original_enqueue_at=now), redis)

        # Dispatch 200 tasks
        dispatched_A = 0
        dispatched_B = 0
        
        # We loop by batch size to avoid max_in_stream blocking if we set it too low,
        # but let's just pass a huge max_in_stream so it dispatches smoothly.
        dispatched = await redis.evalsha(
            _dispatch_sha, 2, "tasks:stream", "tenants:vtime", 200, 1000, int(now.timestamp() * 1000)
        )
        
        assert len(dispatched) == 200
        
        # We need to know which task belongs to which tenant. We didn't save that mapping.
        # But wait, we can just look at `instream` counters!
        instream_a = int(await redis.get("instream:A") or 0)
        instream_b = int(await redis.get("instream:B") or 0)
        
        assert instream_a + instream_b >= 200
        # Check ratio (~3:1)
        ratio = instream_a / max(1, instream_b)
        assert 2.0 <= ratio <= 4.0
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_no_credit_hoarding(_dispatch_sha):
    """An idle tenant returning doesn't get a burst of consecutive turns."""
    now = datetime.now(UTC)
    redis = get_redis_client()
    try:
        # Tenant A works continuously
        for i in range(10):
            await enqueue(Task(id=uuid.uuid4(), tenant_id="A", priority=5, original_enqueue_at=now), redis)
            
        # Dispatch 5 times for A. A's virtual time will advance to 5.
        await redis.evalsha(_dispatch_sha, 2, "tasks:stream", "tenants:vtime", 5, 1000, int(now.timestamp() * 1000))
        
        vtime_A = float(await redis.zscore("tenants:vtime", "A"))
        assert vtime_A == 5.0
        
        # Now Tenant B (idle) adds a task
        await enqueue(Task(id=uuid.uuid4(), tenant_id="B", priority=5, original_enqueue_at=now), redis)
        
        # B's initial vtime should be 5.0 (min_vtime), not 0.
        vtime_B = float(await redis.zscore("tenants:vtime", "B"))
        assert vtime_B == 5.0
        
        # So B does not get to monopolize the next 5 dispatches.
        # Let's dispatch 2 tasks. It should be 1 from A and 1 from B.
        await redis.evalsha(_dispatch_sha, 2, "tasks:stream", "tenants:vtime", 2, 1000, int(now.timestamp() * 1000))
        
        instream_a = int(await redis.get("instream:A") or 0)
        instream_b = int(await redis.get("instream:B") or 0)
        
        assert instream_b == 1
        assert instream_a == 6
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_atomicity(_dispatch_sha):
    """Parallel dispatchers do not double dispatch."""
    now = datetime.now(UTC)
    redis = get_redis_client()
    try:
        # 10 tasks in queue
        for i in range(10):
            await enqueue(Task(id=uuid.uuid4(), tenant_id="A", priority=5, original_enqueue_at=now), redis)
            
        async def run_dispatch():
            return await redis.evalsha(_dispatch_sha, 2, "tasks:stream", "tenants:vtime", 5, 1000, int(now.timestamp() * 1000))
            
        results = await asyncio.gather(*(run_dispatch() for _ in range(5)))
        
        all_dispatched = []
        for r in results:
            all_dispatched.extend(r)
            
        assert len(all_dispatched) == 10
        assert len(set(all_dispatched)) == 10 # No duplicates
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_rebuild_from_postgres(_dispatch_sha):
    """Rebuild from Postgres puts tasks back with correct order."""
    now = datetime.now(UTC)
    redis = get_redis_client()
    
    try:
        task_old = await _create_task("tenant_A", 5, now - timedelta(hours=1))
        task_new = await _create_task("tenant_A", 1, now)
        
        # Enqueue them directly via rebuild
        await rebuild_from_postgres()
        
        # Verify order
        dispatched = await redis.evalsha(
            _dispatch_sha, 2, settings.redis_stream_name, "tenants:vtime", 2, 10, int(now.timestamp() * 1000)
        )
        assert len(dispatched) == 2
        # Older task (1 hour old) beats new priority 1 task due to aging
        assert dispatched[0] == str(task_old.id)
        assert dispatched[1] == str(task_new.id)
    finally:
        await redis.aclose()
