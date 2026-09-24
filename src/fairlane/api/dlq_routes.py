"""API routes for the Dead-Letter Queue (DLQ)."""

import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from fairlane.db import get_db
from fairlane.models import DeadLetter, Task, TaskEvent, TaskStatus
from fairlane.redis_client import push_task_to_stream
from fairlane.schemas import (
    BulkReplayRequest,
    BulkReplayResponse,
    DeadLetterDetailResponse,
    DeadLetterListResponse,
    DeadLetterResponse,
    DeadLetterStatsResponse,
    CategoryStat,
    ReplayRequest,
    ReplayResponse,
    TenantStat,
)

logger = logging.getLogger(__name__)
dlq_router = APIRouter(prefix="/dlq", tags=["Dead-Letter Queue"])


@dlq_router.get("", response_model=DeadLetterListResponse)
async def list_dead_letters(
    tenant_id: str | None = Query(None, description="Filter by tenant ID"),
    failure_category: str | None = Query(None, description="Filter by failure category"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
) -> DeadLetterListResponse:
    """List dead-lettered tasks with optional filters and pagination."""
    # Build base query
    query = select(DeadLetter)
    count_query = select(func.count(DeadLetter.id))

    if tenant_id:
        query = query.where(DeadLetter.tenant_id == tenant_id)
        count_query = count_query.where(DeadLetter.tenant_id == tenant_id)
    if failure_category:
        query = query.where(DeadLetter.failure_category == failure_category)
        count_query = count_query.where(DeadLetter.failure_category == failure_category)

    # Total count
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()

    # Paginated items
    offset = (page - 1) * page_size
    query = query.order_by(DeadLetter.dead_at.desc()).offset(offset).limit(page_size)
    result = await db.execute(query)
    items = result.scalars().all()

    return DeadLetterListResponse(
        items=[DeadLetterResponse.model_validate(item) for item in items],
        total=total,
        page=page,
        page_size=page_size,
    )


@dlq_router.get("/stats", response_model=DeadLetterStatsResponse)
async def dead_letter_stats(
    db: AsyncSession = Depends(get_db),
) -> DeadLetterStatsResponse:
    """Return aggregated DLQ statistics: counts by category and by tenant."""
    # Total
    total_result = await db.execute(select(func.count(DeadLetter.id)))
    total = total_result.scalar_one()

    # By category
    cat_query = (
        select(DeadLetter.failure_category, func.count(DeadLetter.id))
        .group_by(DeadLetter.failure_category)
        .order_by(func.count(DeadLetter.id).desc())
    )
    cat_result = await db.execute(cat_query)
    by_category = [
        CategoryStat(category=str(row[0]), count=row[1])
        for row in cat_result.all()
    ]

    # By tenant
    tenant_query = (
        select(DeadLetter.tenant_id, func.count(DeadLetter.id))
        .group_by(DeadLetter.tenant_id)
        .order_by(func.count(DeadLetter.id).desc())
    )
    tenant_result = await db.execute(tenant_query)
    by_tenant = [
        TenantStat(tenant_id=row[0], count=row[1])
        for row in tenant_result.all()
    ]

    return DeadLetterStatsResponse(
        total=total,
        by_category=by_category,
        by_tenant=by_tenant,
    )


@dlq_router.get("/{task_id}", response_model=DeadLetterDetailResponse)
async def get_dead_letter(
    task_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> DeadLetterDetailResponse:
    """Return full details of a dead-lettered task, including error_history."""
    result = await db.execute(
        select(DeadLetter).where(DeadLetter.task_id == task_id)
    )
    dl = result.scalar_one_or_none()

    if not dl:
        raise HTTPException(status_code=404, detail="Dead letter not found for this task")

    return DeadLetterDetailResponse.model_validate(dl)


# ---------------------------------------------------------------------------
# Replay endpoints
# ---------------------------------------------------------------------------


async def _replay_task(
    task: Task,
    dl: DeadLetter,
    db: AsyncSession,
    *,
    payload_patch: dict | None = None,
    reset_attempts: bool = False,
) -> None:
    """Replay a single dead-lettered task (all DB writes in the caller's transaction).

    1. Apply payload_patch if given.
    2. Set status = PENDING, reset attempts if requested, clear next_retry_at.
    3. Update dead_letters: replayed_at = now, replay_count += 1.
    4. Add a REPLAYED task_events row.
    Then push the task id to the Redis Stream.
    """
    now = datetime.now(UTC)

    # 1. Apply payload patch
    if payload_patch:
        merged = dict(task.payload or {})
        merged.update(payload_patch)
        task.payload = merged

    # 2. Reset task state
    task.status = TaskStatus.PENDING
    task.finished_at = None
    task.next_retry_at = None
    task.last_error = None
    if reset_attempts:
        task.attempts = 0

    # 3. Update dead_letters
    dl.replayed_at = now
    dl.replay_count = (dl.replay_count or 0) + 1

    # 4. Add REPLAYED event
    replay_event = TaskEvent(
        task_id=task.id,
        event_type="REPLAYED",
        details={
            "payload_patch": payload_patch,
            "reset_attempts": reset_attempts,
            "replay_count": dl.replay_count,
            "replayed_at": now.isoformat(),
        },
    )
    db.add(replay_event)

    # Commit the transaction.
    await db.commit()

    # Push to Redis stream (outside transaction — best-effort).
    await push_task_to_stream(str(task.id))

    logger.info(
        "Replayed task %s (replay_count=%d, payload_patch=%s)",
        task.id,
        dl.replay_count,
        bool(payload_patch),
        extra={"task_id": str(task.id)},
    )


@dlq_router.post("/{task_id}/replay", response_model=ReplayResponse)
async def replay_dead_letter(
    task_id: uuid.UUID,
    body: ReplayRequest | None = None,
    db: AsyncSession = Depends(get_db),
) -> ReplayResponse:
    """Replay a single dead-lettered task, optionally patching its payload."""
    if body is None:
        body = ReplayRequest()

    # Fetch the task
    task_result = await db.execute(select(Task).where(Task.id == task_id))
    task = task_result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != TaskStatus.DEAD:
        raise HTTPException(
            status_code=409,
            detail=f"Task status is {task.status.value}, not DEAD. Only DEAD tasks can be replayed.",
        )

    # Fetch the dead_letters row
    dl_result = await db.execute(select(DeadLetter).where(DeadLetter.task_id == task_id))
    dl = dl_result.scalar_one_or_none()
    if not dl:
        raise HTTPException(status_code=404, detail="Dead letter record not found for this task")

    await _replay_task(
        task,
        dl,
        db,
        payload_patch=body.payload_patch,
        reset_attempts=body.reset_attempts,
    )

    return ReplayResponse(
        task_id=str(task.id),
        status="PENDING",
        message=f"Task replayed successfully (replay #{dl.replay_count})",
    )


@dlq_router.post("/replay-bulk", response_model=BulkReplayResponse)
async def replay_bulk(
    body: BulkReplayRequest,
    db: AsyncSession = Depends(get_db),
) -> BulkReplayResponse:
    """Replay all dead-lettered tasks matching the given filters."""
    if not body.tenant_id and not body.failure_category:
        raise HTTPException(
            status_code=400,
            detail="At least one filter (tenant_id or failure_category) is required",
        )

    # Find matching dead letters
    dl_query = select(DeadLetter)
    if body.tenant_id:
        dl_query = dl_query.where(DeadLetter.tenant_id == body.tenant_id)
    if body.failure_category:
        dl_query = dl_query.where(DeadLetter.failure_category == body.failure_category)

    dl_result = await db.execute(dl_query)
    dead_letters = dl_result.scalars().all()

    replayed_ids: list[str] = []

    for dl in dead_letters:
        # Fetch the associated task
        task_result = await db.execute(select(Task).where(Task.id == dl.task_id))
        task = task_result.scalar_one_or_none()
        if not task or task.status != TaskStatus.DEAD:
            continue

        await _replay_task(task, dl, db)
        replayed_ids.append(str(task.id))

    return BulkReplayResponse(
        replayed=len(replayed_ids),
        task_ids=replayed_ids,
    )

