"""Unit and integration tests for failure injection and retry scheduling."""

import uuid

import pytest
from sqlalchemy import select

from fairlane.db import async_session_factory
from fairlane.models import Task, TaskEvent, TaskStatus
from fairlane.worker.main import FairlaneWorker


@pytest.fixture
def mock_worker():
    """Create a FairlaneWorker instance for testing."""
    worker = FairlaneWorker()
    return worker


@pytest.fixture
async def connected_worker():
    """Create a FairlaneWorker with Redis connected."""
    worker = FairlaneWorker()
    await worker.init_redis()
    return worker


@pytest.mark.asyncio
async def test_failure_injection_schedules_retry(connected_worker):
    """Verify retryable failure sets status to PENDING and populates next_retry_at."""
    task_id = uuid.uuid4()
    async with async_session_factory() as db:
        task = Task(
            id=task_id,
            tenant_id="tenant_retry_test",
            task_type="test_retry",
            payload={"fail": True},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=0,
            max_attempts=5,
        )
        db.add(task)
        await db.commit()

    # Process task with worker
    await connected_worker.process_task(
        message_id="1-0",
        data={"task_id": str(task_id)},
    )

    # Verify task state in database: should be PENDING with next_retry_at set
    async with async_session_factory() as db:
        result = await db.execute(select(Task).where(Task.id == task_id))
        updated_task = result.scalar_one()

        assert updated_task.status == TaskStatus.PENDING
        assert updated_task.attempts == 1
        assert updated_task.next_retry_at is not None
        assert updated_task.last_error is not None
        assert "Injected failure" in updated_task.last_error

        # Check events
        events_result = await db.execute(
            select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.created_at.asc())
        )
        events = events_result.scalars().all()
        event_types = [e.event_type for e in events]
        assert "STARTED" in event_types
        assert "RETRY_SCHEDULED" in event_types


@pytest.mark.asyncio
async def test_failure_injection_max_attempts_reached(connected_worker):
    """Verify failure transitions to FAILED when attempts reach max_attempts."""
    task_id = uuid.uuid4()
    async with async_session_factory() as db:
        task = Task(
            id=task_id,
            tenant_id="tenant_max_attempts_test",
            task_type="test_max_attempts",
            payload={"fail": True},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=4,  # Next attempt will be 5, reaching max_attempts=5
            max_attempts=5,
        )
        db.add(task)
        await db.commit()

    await connected_worker.process_task(
        message_id="2-0",
        data={"task_id": str(task_id)},
    )

    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.DEAD
        assert t.attempts == 5
        assert t.finished_at is not None
        assert t.next_retry_at is None

        events_result = await db.execute(
            select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.created_at.asc())
        )
        events = events_result.scalars().all()
        event_types = [e.event_type for e in events]
        assert "DEAD_LETTERED" in event_types


@pytest.mark.asyncio
async def test_failure_injection_fail_rate(connected_worker):
    """Verify payload {'fail_rate': 0.0} succeeds and {'fail_rate': 1.0} schedules retry."""
    # 1. Deterministic success with fail_rate = 0.0
    pass_task_id = uuid.uuid4()
    async with async_session_factory() as db:
        pass_task = Task(
            id=pass_task_id,
            tenant_id="tenant_rate_test",
            task_type="test_rate_pass",
            payload={"fail_rate": 0.0},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=0,
            max_attempts=3,
        )
        db.add(pass_task)
        await db.commit()

    await connected_worker.process_task(
        message_id="3-0",
        data={"task_id": str(pass_task_id)},
    )

    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == pass_task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.SUCCEEDED
        assert t.last_error is None

    # 2. Deterministic retry scheduling with fail_rate = 1.0
    fail_task_id = uuid.uuid4()
    async with async_session_factory() as db:
        fail_task = Task(
            id=fail_task_id,
            tenant_id="tenant_rate_test",
            task_type="test_rate_fail",
            payload={"fail_rate": 1.0},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=0,
            max_attempts=3,
        )
        db.add(fail_task)
        await db.commit()

    await connected_worker.process_task(
        message_id="4-0",
        data={"task_id": str(fail_task_id)},
    )

    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == fail_task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.PENDING
        assert t.next_retry_at is not None
        assert "fail_rate=1.0" in t.last_error


@pytest.mark.asyncio
async def test_failure_injection_fail_until_attempt(connected_worker):
    """Verify payload {'fail_until_attempt': 3} schedules retries until attempt 3 is reached."""
    task_id = uuid.uuid4()

    # Create task with attempt = 0
    async with async_session_factory() as db:
        task = Task(
            id=task_id,
            tenant_id="tenant_until_test",
            task_type="test_until_attempt",
            payload={"fail_until_attempt": 3},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=0,
            max_attempts=5,
        )
        db.add(task)
        await db.commit()

    # 1st attempt: should schedule retry (attempts becomes 1 < 3)
    await connected_worker.process_task(message_id="5-1", data={"task_id": str(task_id)})
    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.PENDING
        assert t.attempts == 1
        assert t.next_retry_at is not None

    # 2nd attempt: should schedule retry (attempts becomes 2 < 3)
    await connected_worker.process_task(message_id="5-2", data={"task_id": str(task_id)})
    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.PENDING
        assert t.attempts == 2
        assert t.next_retry_at is not None

    # 3rd attempt: should succeed (attempts becomes 3 >= 3)
    await connected_worker.process_task(message_id="5-3", data={"task_id": str(task_id)})
    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.SUCCEEDED
        assert t.attempts == 3
        assert t.last_error is None
