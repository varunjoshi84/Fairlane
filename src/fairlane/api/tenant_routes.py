import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from fairlane.config import settings
from fairlane.db import get_db
from fairlane.models import Task, TaskStatus, TenantLimit
from fairlane.redis_client import get_redis
from fairlane.schemas import TenantLimitsConfig, TenantUsage
from fairlane.ratelimit import RateLimiter

logger = logging.getLogger(__name__)
tenant_router = APIRouter(prefix="/tenants", tags=["tenants"])


@tenant_router.put("/{tenant_id}/limits", response_model=TenantLimitsConfig)
async def set_tenant_limits(
    tenant_id: str,
    config: TenantLimitsConfig,
    db: AsyncSession = Depends(get_db),
):
    """Set custom limits for a tenant."""
    result = await db.execute(select(TenantLimit).where(TenantLimit.tenant_id == tenant_id))
    limit = result.scalar_one_or_none()

    if limit:
        limit.rate_per_second = config.rate_per_second
        limit.burst_capacity = config.burst_capacity
        limit.max_concurrent = config.max_concurrent
        limit.weight = config.weight
    else:
        limit = TenantLimit(
            tenant_id=tenant_id,
            rate_per_second=config.rate_per_second,
            burst_capacity=config.burst_capacity,
            max_concurrent=config.max_concurrent,
            weight=config.weight,
        )
        db.add(limit)
    
    await db.commit()
    return config


@tenant_router.get("/{tenant_id}/limits", response_model=TenantLimitsConfig)
async def get_tenant_limits(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get custom limits for a tenant, or defaults if none exist."""
    result = await db.execute(select(TenantLimit).where(TenantLimit.tenant_id == tenant_id))
    limit = result.scalar_one_or_none()
    
    if limit:
        return TenantLimitsConfig(
            rate_per_second=limit.rate_per_second,
            burst_capacity=limit.burst_capacity,
            max_concurrent=limit.max_concurrent,
            weight=limit.weight,
        )
    return TenantLimitsConfig(
        rate_per_second=settings.default_rate_per_second,
        burst_capacity=settings.default_burst,
        max_concurrent=settings.default_max_concurrent,
        weight=1,
    )


@tenant_router.get("/{tenant_id}/usage", response_model=TenantUsage)
async def get_tenant_usage(
    tenant_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get current usage for a tenant."""
    # 1. Count running tasks from DB
    result = await db.execute(
        select(func.count(Task.id))
        .where(Task.tenant_id == tenant_id, Task.status == TaskStatus.RUNNING)
    )
    running_tasks = result.scalar() or 0

    # 2. Count pending tasks from DB
    result = await db.execute(
        select(func.count(Task.id))
        .where(Task.tenant_id == tenant_id, Task.status == TaskStatus.PENDING)
    )
    pending_tasks = result.scalar() or 0

    # 3. Check token bucket for tokens remaining
    redis = await get_redis()
    limiter = RateLimiter(redis)
    res = await limiter.allow(tenant_id, requested=0)  # Check without consuming

    return TenantUsage(
        tenant_id=tenant_id,
        tokens_remaining=res.remaining,
        running_tasks=running_tasks,
        pending_tasks=pending_tasks,
    )
