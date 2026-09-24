"""Tests for the DeadLetter model and its relationship to Task."""

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
    TaskStatus,
)


@pytest.fixture(autouse=True)
async def _setup_db():
    """Ensure tables exist before each test."""
    await init_db()


async def _create_task(db: AsyncSession, **overrides) -> Task:
    """Helper to insert a minimal task row."""
    defaults = {
        "tenant_id": "test-tenant",
        "task_type": "test.job",
        "payload": {},
        "status": TaskStatus.DEAD,
        "attempts": 5,
        "max_attempts": 5,
    }
    defaults.update(overrides)
    task = Task(**defaults)
    db.add(task)
    await db.flush()
    return task


@pytest.mark.asyncio
async def test_insert_dead_letter():
    """A DeadLetter row can be inserted and read back with correct fields."""
    async with async_session_factory() as db:
        task = await _create_task(db)

        dl = DeadLetter(
            task_id=task.id,
            tenant_id=task.tenant_id,
            task_type=task.task_type,
            failure_category=FailureCategory.TRANSIENT_EXHAUSTED,
            last_error="connection refused",
            error_history=[
                {"attempt": 1, "error": "connection refused"},
                {"attempt": 2, "error": "connection refused"},
            ],
            attempts_made=task.attempts,
            dead_at=datetime.now(UTC),
        )
        db.add(dl)
        await db.commit()

        # Read it back
        result = await db.execute(
            select(DeadLetter).where(DeadLetter.task_id == task.id)
        )
        stored = result.scalar_one()

        assert stored.tenant_id == "test-tenant"
        assert stored.task_type == "test.job"
        assert stored.failure_category == FailureCategory.TRANSIENT_EXHAUSTED
        assert stored.last_error == "connection refused"
        assert len(stored.error_history) == 2
        assert stored.attempts_made == 5
        assert stored.replayed_at is None
        assert stored.replay_count == 0


@pytest.mark.asyncio
async def test_dead_letter_unique_per_task():
    """Only one DeadLetter row is allowed per task (unique constraint on task_id)."""
    async with async_session_factory() as db:
        task = await _create_task(db)

        dl1 = DeadLetter(
            task_id=task.id,
            tenant_id=task.tenant_id,
            task_type=task.task_type,
            failure_category=FailureCategory.PERMANENT,
            last_error="bad input",
            error_history=[{"attempt": 1, "error": "bad input"}],
            attempts_made=1,
            dead_at=datetime.now(UTC),
        )
        db.add(dl1)
        await db.commit()

        # Inserting a second dead letter for the same task must fail.
        dl2 = DeadLetter(
            id=uuid.uuid4(),
            task_id=task.id,
            tenant_id=task.tenant_id,
            task_type=task.task_type,
            failure_category=FailureCategory.PERMANENT,
            last_error="bad input again",
            error_history=[],
            attempts_made=1,
            dead_at=datetime.now(UTC),
        )
        db.add(dl2)
        with pytest.raises(Exception):
            await db.commit()


@pytest.mark.asyncio
async def test_task_relationship_to_dead_letter():
    """Task.dead_letter navigates to the associated DeadLetter row."""
    async with async_session_factory() as db:
        task = await _create_task(db)

        dl = DeadLetter(
            task_id=task.id,
            tenant_id=task.tenant_id,
            task_type=task.task_type,
            failure_category=FailureCategory.POISON_PILL,
            last_error="deserialization error",
            error_history=[{"attempt": 1, "error": "deserialization error"}],
            attempts_made=1,
            dead_at=datetime.now(UTC),
        )
        db.add(dl)
        await db.commit()

        # Refresh the task and access the relationship.
        await db.refresh(task, attribute_names=["dead_letter"])
        assert task.dead_letter is not None
        assert task.dead_letter.failure_category == FailureCategory.POISON_PILL


@pytest.mark.asyncio
async def test_failure_category_values():
    """All three FailureCategory variants are valid."""
    async with async_session_factory() as db:
        for category in FailureCategory:
            task = await _create_task(db)
            dl = DeadLetter(
                task_id=task.id,
                tenant_id=task.tenant_id,
                task_type=task.task_type,
                failure_category=category,
                last_error=f"error for {category.value}",
                error_history=[],
                attempts_made=1,
                dead_at=datetime.now(UTC),
            )
            db.add(dl)

        await db.commit()

        result = await db.execute(select(DeadLetter))
        all_dls = result.scalars().all()
        categories = {dl.failure_category for dl in all_dls}
        assert categories == {
            FailureCategory.TRANSIENT_EXHAUSTED,
            FailureCategory.PERMANENT,
            FailureCategory.POISON_PILL,
        }
