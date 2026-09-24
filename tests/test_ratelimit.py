import asyncio
import uuid
import pytest
from redis.asyncio import Redis
from sqlalchemy import select
from fairlane.ratelimit import RateLimiter
from fairlane.models import TenantLimit
from fairlane.db import async_session_factory
from fairlane.config import settings

@pytest.fixture
async def rate_limiter():
    redis = Redis.from_url(settings.redis_url, max_connections=500)
    await redis.flushdb()
    limiter = RateLimiter(redis)
    yield limiter
    await redis.aclose()


@pytest.mark.asyncio
async def test_token_bucket_basics(rate_limiter: RateLimiter):
    tenant = f"t-{uuid.uuid4()}"
    async with async_session_factory() as db:
        db.add(TenantLimit(
            tenant_id=tenant,
            rate_per_second=2.0,
            burst_capacity=3,
            max_concurrent=5,
        ))
        await db.commit()
    
    # Bucket size is 3. Consume 3 tokens.
    for _ in range(3):
        res = await rate_limiter.allow(tenant)
        assert res.allowed is True
    
    # 4th should be denied
    res = await rate_limiter.allow(tenant)
    assert res.allowed is False
    assert res.retry_after_ms > 0
    assert res.remaining < 0.1

    # Wait for refill (1 token takes 0.5s at rate 2.0)
    await asyncio.sleep(0.6)
    res = await rate_limiter.allow(tenant)
    assert res.allowed is True


@pytest.mark.asyncio
async def test_token_bucket_concurrency(rate_limiter: RateLimiter):
    tenant = f"t-{uuid.uuid4()}"
    async with async_session_factory() as db:
        db.add(TenantLimit(
            tenant_id=tenant,
            rate_per_second=0.0, # no refill
            burst_capacity=10,
            max_concurrent=10,
        ))
        await db.commit()

    # Prime the cache to prevent concurrent DB connections
    await rate_limiter.get_tenant_limits(tenant)

    async def worker():
        return await rate_limiter.allow(tenant)

    # 200 parallel calls
    results = await asyncio.gather(*(worker() for _ in range(200)))
    
    allowed = sum(1 for r in results if r.allowed)
    denied = sum(1 for r in results if not r.allowed)

    assert allowed == 10
    assert denied == 190


@pytest.mark.asyncio
async def test_concurrency_slots(rate_limiter: RateLimiter):
    tenant = f"t-{uuid.uuid4()}"
    async with async_session_factory() as db:
        db.add(TenantLimit(
            tenant_id=tenant,
            rate_per_second=100.0,
            burst_capacity=100,
            max_concurrent=3,
        ))
        await db.commit()

    # Acquire 3 slots
    for i in range(3):
        assert await rate_limiter.acquire_slot(tenant, str(i)) is True

    # 4th should fail
    assert await rate_limiter.acquire_slot(tenant, "4") is False

    # Release one slot
    await rate_limiter.release_slot(tenant, "0")

    # Now 4th should succeed
    assert await rate_limiter.acquire_slot(tenant, "4") is True
