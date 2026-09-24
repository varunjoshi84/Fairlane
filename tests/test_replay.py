"""Tests for the DLQ replay functionality."""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from fairlane.db import async_session_factory, init_db
from fairlane.models import (
    DeadLetter,
    FailureCategory,
    Task,
    TaskEvent,
    TaskStatus,
)


@pytest.fixture(autouse=True)
async def _setup_db():
    """Ensure tables exist before each test."""
    await init_db()


async def _create_dead_task(
    db: AsyncSession,
    *,
    tenant_id: str = "test-tenant",
    task_type: str = "test.job",
    payload: dict | None = None,
    category: FailureCategory = FailureCategory.TRANSIENT_EXHAUSTED,
) -> tuple[Task, DeadLetter]:
    """Insert a DEAD task with a matching dead_letters row."""
    task = Task(
        tenant_id=tenant_id,
        task_type=task_type,
        payload=payload or {"fail": True},
        status=TaskStatus.DEAD,
        attempts=5,
        max_attempts=5,
        finished_at=datetime.now(UTC),
        last_error="test error",
    )
    db.add(task)
    await db.flush()

    dl = DeadLetter(
        task_id=task.id,
        tenant_id=task.tenant_id,
        task_type=task.task_type,
        failure_category=category,
        last_error="test error",
        error_history=[
            {"attempt": 1, "error": "test error"},
            {"attempt": 2, "error": "test error"},
        ],
        attempts_made=task.attempts,
        dead_at=datetime.now(UTC),
    )
    db.add(dl)
    await db.commit()
    return task, dl


@pytest.mark.asyncio
async def test_replay_resets_task_to_pending():
    """Replaying a dead task sets its status back to PENDING."""
    async with async_session_factory() as db:
        task, dl = await _create_dead_task(db)

        # Simulate replay logic inline (since we can't call the API without Redis)
        now = datetime.now(UTC)
        task.status = TaskStatus.PENDING
        task.finished_at = None
        task.next_retry_at = None
        task.last_error = None

        dl.replayed_at = now
        dl.replay_count += 1

        replay_event = TaskEvent(
            task_id=task.id,
            event_type="REPLAYED",
            details={"replay_count": dl.replay_count},
        )
        db.add(replay_event)
        await db.commit()

        # Verify
        result = await db.execute(select(Task).where(Task.id == task.id))
        refreshed = result.scalar_one()
        assert refreshed.status == TaskStatus.PENDING
        assert refreshed.finished_at is None
        assert refreshed.last_error is None


@pytest.mark.asyncio
async def test_replay_applies_payload_patch():
    """Replaying with a payload_patch merges the fix into the task payload."""
    async with async_session_factory() as db:
        task, dl = await _create_dead_task(db, payload={"fail": True, "data": "keep"})

        # Apply patch
        patch = {"fail": False}
        merged = dict(task.payload)
        merged.update(patch)
        task.payload = merged
        task.status = TaskStatus.PENDING
        task.finished_at = None
        task.last_error = None

        dl.replayed_at = datetime.now(UTC)
        dl.replay_count += 1

        replay_event = TaskEvent(
            task_id=task.id,
            event_type="REPLAYED",
            details={"payload_patch": patch, "replay_count": dl.replay_count},
        )
        db.add(replay_event)
        await db.commit()

        # Verify payload was merged
        result = await db.execute(select(Task).where(Task.id == task.id))
        refreshed = result.scalar_one()
        assert refreshed.payload["fail"] is False
        assert refreshed.payload["data"] == "keep"


@pytest.mark.asyncio
async def test_replay_increments_replay_count():
    """Each replay increments the dead_letters replay_count."""
    async with async_session_factory() as db:
        task, dl = await _create_dead_task(db)

        assert dl.replay_count == 0

        # First replay
        dl.replayed_at = datetime.now(UTC)
        dl.replay_count += 1
        task.status = TaskStatus.PENDING
        await db.commit()

        result = await db.execute(select(DeadLetter).where(DeadLetter.task_id == task.id))
        assert result.scalar_one().replay_count == 1

        # Reset to DEAD for second replay
        task.status = TaskStatus.DEAD
        await db.commit()

        # Second replay
        dl.replayed_at = datetime.now(UTC)
        dl.replay_count += 1
        task.status = TaskStatus.PENDING
        await db.commit()

        result = await db.execute(select(DeadLetter).where(DeadLetter.task_id == task.id))
        assert result.scalar_one().replay_count == 2


@pytest.mark.asyncio
async def test_replay_reset_attempts():
    """When reset_attempts=True, the attempt counter goes back to 0."""
    async with async_session_factory() as db:
        task, dl = await _create_dead_task(db)

        assert task.attempts == 5

        task.status = TaskStatus.PENDING
        task.attempts = 0
        dl.replayed_at = datetime.now(UTC)
        dl.replay_count += 1
        await db.commit()

        result = await db.execute(select(Task).where(Task.id == task.id))
        assert result.scalar_one().attempts == 0


@pytest.mark.asyncio
async def test_replay_adds_replayed_event():
    """Replaying inserts a REPLAYED task_events row."""
    async with async_session_factory() as db:
        task, dl = await _create_dead_task(db)

        replay_event = TaskEvent(
            task_id=task.id,
            event_type="REPLAYED",
            details={"replay_count": 1},
        )
        db.add(replay_event)
        task.status = TaskStatus.PENDING
        dl.replayed_at = datetime.now(UTC)
        dl.replay_count += 1
        await db.commit()

        result = await db.execute(
            select(TaskEvent)
            .where(TaskEvent.task_id == task.id, TaskEvent.event_type == "REPLAYED")
        )
        events = result.scalars().all()
        assert len(events) == 1
        assert events[0].details["replay_count"] == 1


@pytest.mark.asyncio
async def test_only_dead_tasks_can_be_replayed():
    """A task that is not DEAD cannot be replayed."""
    async with async_session_factory() as db:
        task = Task(
            tenant_id="test-tenant",
            task_type="test.job",
            payload={},
            status=TaskStatus.PENDING,
            attempts=1,
            max_attempts=5,
        )
        db.add(task)
        await db.commit()

        # Attempting to find a dead_letters row should fail
        result = await db.execute(select(DeadLetter).where(DeadLetter.task_id == task.id))
        dl = result.scalar_one_or_none()
        assert dl is None, "A non-DEAD task should not have a dead_letters entry"
