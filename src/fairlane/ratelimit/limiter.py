"""Rate limiting implementation using Redis token bucket."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import select

from fairlane.config import settings
from fairlane.db import async_session_factory
from fairlane.models import TenantLimit

logger = logging.getLogger("fairlane.ratelimit")


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    remaining: float
    retry_after_ms: int


class RateLimiter:
    """Manages tenant quotas and rate limits via Redis token bucket and concurrency slots."""

    def __init__(self, redis: Redis) -> None:
        """Initialize with a Redis connection."""
        self.redis = redis
        self.script_sha: str | None = None
        self._cache: dict[str, dict[str, Any]] = {}
        self._cache_expiry: dict[str, float] = {}

    async def _load_script(self) -> None:
        """Load the Lua token bucket script into Redis."""
        script_path = Path(__file__).parent / "token_bucket.lua"
        with open(script_path, "r") as f:
            script_content = f.read()
        self.script_sha = await self.redis.script_load(script_content)

    async def get_tenant_limits(self, tenant_id: str) -> dict[str, Any]:
        """Fetch tenant limits from DB or fallback to defaults, caching the result."""
        now = asyncio.get_event_loop().time()
        if tenant_id in self._cache and self._cache_expiry.get(tenant_id, 0) > now:
            return self._cache[tenant_id]

        async with async_session_factory() as db:
            result = await db.execute(select(TenantLimit).where(TenantLimit.tenant_id == tenant_id))
            limit = result.scalar_one_or_none()
            if limit:
                config = {
                    "rate_per_second": limit.rate_per_second,
                    "burst_capacity": limit.burst_capacity,
                    "max_concurrent": limit.max_concurrent,
                }
            else:
                config = {
                    "rate_per_second": settings.default_rate_per_second,
                    "burst_capacity": settings.default_burst,
                    "max_concurrent": settings.default_max_concurrent,
                }

            self._cache[tenant_id] = config
            self._cache_expiry[tenant_id] = now + 30.0
            return config

    async def allow(self, tenant_id: str, requested: int = 1) -> RateLimitResult:
        """Check if request is allowed by the token bucket."""
        if not self.script_sha:
            await self._load_script()
        assert self.script_sha is not None

        limits = await self.get_tenant_limits(tenant_id)
        key = f"ratelimit:{tenant_id}"

        try:
            res = await self.redis.evalsha(
                self.script_sha,
                1,
                key,
                limits["rate_per_second"],
                limits["burst_capacity"],
                requested,
                60,
            )
        except Exception as e:
            if "NOSCRIPT" in str(e):
                await self._load_script()
                assert self.script_sha is not None
                res = await self.redis.evalsha(
                    self.script_sha,
                    1,
                    key,
                    limits["rate_per_second"],
                    limits["burst_capacity"],
                    requested,
                    60,
                )
            else:
                raise

        allowed = bool(res[0])
        remaining = float(res[1])
        retry_after_ms = int(res[2])
        return RateLimitResult(allowed, remaining, retry_after_ms)

    async def acquire_slot(self, tenant_id: str, task_id: str) -> bool:
        """Acquire a concurrency slot for a task. Returns True if acquired."""
        limits = await self.get_tenant_limits(tenant_id)
        max_concurrent = limits["max_concurrent"]

        # Use a sorted set of active tasks for the tenant, scored by expiry
        zset_key = f"concurrent:{tenant_id}"
        now = datetime.now(UTC).timestamp()

        # Clean up expired slots
        await self.redis.zremrangebyscore(zset_key, "-inf", now)

        # Check current count
        count = await self.redis.zcard(zset_key)
        if count >= max_concurrent:
            return False

        # Add slot with 1 hour expiry
        await self.redis.zadd(zset_key, {task_id: now + 3600})
        # Check rank to ensure atomic acquire didn't exceed
        rank = await self.redis.zrank(zset_key, task_id)
        if rank is not None and rank >= max_concurrent:
            await self.redis.zrem(zset_key, task_id)
            return False

        return True

    async def release_slot(self, tenant_id: str, task_id: str) -> None:
        """Release a concurrency slot."""
        zset_key = f"concurrent:{tenant_id}"
        await self.redis.zrem(zset_key, task_id)
