import asyncio
import time
from sqlalchemy import select, func, text
from redis.asyncio import Redis

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import Task, Worker
from fairlane.metrics import (
    QUEUE_WAITING,
    QUEUE_OLDEST_AGE,
    STREAM_DEPTH,
    TASKS_RUNNING,
    DLQ_SIZE,
    WORKERS_ACTIVE,
    WORKERS_DEAD,
    TENANT_TOKENS,
    TENANT_VIRTUAL_TIME,
)

COLLECTOR_LOCK_KEY = "fairlane:metrics_collector_lock"
LOCK_EXPIRY = 5  # seconds

async def collect_metrics(redis: Redis):
    """Collect shared gauges from Redis and Postgres."""
    # 1. Acquire Redis lock
    # Use NX=True, EX=LOCK_EXPIRY to ensure only one collector runs
    lock_acquired = await redis.set(COLLECTOR_LOCK_KEY, "locked", nx=True, ex=LOCK_EXPIRY)
    if not lock_acquired:
        return  # Another worker is collecting

    try:
        # Collect Redis Stream metrics
        try:
            stream_info = await redis.xinfo_stream(settings.redis_stream_name)
            if stream_info:
                STREAM_DEPTH.set(stream_info.get("length", 0))
        except Exception:
            pass  # stream might not exist yet

        # Collect Tenant Token Bucket / Queue metrics
        # To do this efficiently, we might need to scan or use known tenants from DB
        async with async_session_factory() as db:
            # 1. DB: Tasks Running / DLQ / Waiting
            # Count by tenant & status
            result = await db.execute(
                select(Task.tenant_id, Task.status, func.count(Task.id))
                .group_by(Task.tenant_id, Task.status)
            )
            rows = result.all()
            
            tenant_running = {}
            tenant_dlq = {}
            tenant_waiting = {}
            
            for row in rows:
                t_id, status, count = row
                if status == "RUNNING":
                    tenant_running[t_id] = count
                elif status == "DEAD":
                    tenant_dlq[t_id] = count
                elif status == "PENDING":
                    tenant_waiting[t_id] = count

            # Update Gauges (we should clear old ones or just overwrite known ones)
            # For simplicity, we just set the ones we found.
            for t, c in tenant_running.items():
                TASKS_RUNNING.labels(tenant=t).set(c)
            for t, c in tenant_dlq.items():
                DLQ_SIZE.labels(tenant=t).set(c)
            for t, c in tenant_waiting.items():
                QUEUE_WAITING.labels(tenant=t).set(c)

            # 2. DB: Workers
            active = await db.execute(select(func.count(Worker.id)).where(Worker.status == "ACTIVE"))
            dead = await db.execute(select(func.count(Worker.id)).where(Worker.status == "DEAD"))
            WORKERS_ACTIVE.set(active.scalar_one_or_none() or 0)
            WORKERS_DEAD.set(dead.scalar_one_or_none() or 0)

            # 3. Redis: Oldest age, Virtual Time, Tokens
            # We can scan sorted sets ready:{tenant} to find oldest age
            # And ratelimit:{tenant} for tokens
            # We need the list of tenants. We can use the keys from DB `tenant_running` etc.
            tenants = set(tenant_running.keys()).union(tenant_dlq.keys()).union(tenant_waiting.keys())
            
            # Or just scan redis keys if there are active queues
            # But let's just use the DB tenants for now
            now = time.time()
            for t in tenants:
                # Oldest age
                first_item = await redis.zrange(f"ready:{t}", 0, 0, withscores=True)
                if first_item:
                    _, score = first_item[0]
                    QUEUE_OLDEST_AGE.labels(tenant=t).set(max(0, now - score))
                else:
                    QUEUE_OLDEST_AGE.labels(tenant=t).set(0)

                # Virtual time
                vt = await redis.get(f"vtime:{t}")
                if vt:
                    TENANT_VIRTUAL_TIME.labels(tenant=t).set(float(vt))

                # Tokens
                tokens = await redis.hget(f"ratelimit:{t}", "tokens")
                if tokens:
                    TENANT_TOKENS.labels(tenant=t).set(float(tokens))

    finally:
        # We don't necessarily need to release the lock immediately if we run every 2s
        # and expiry is 5s, but we can to be nice.
        pass

async def metrics_collection_loop(redis_url: str):
    """Background task that collects shared gauges every 2 seconds."""
    redis = Redis.from_url(redis_url)
    try:
        while True:
            await collect_metrics(redis)
            await asyncio.sleep(2.0)
    finally:
        await redis.aclose()
