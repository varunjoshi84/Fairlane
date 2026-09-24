"""Unit and integration tests for failure injection modes."""

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


@pytest.mark.asyncio
async def test_failure_injection_fail_true(mock_worker):
    """Verify payload {'fail': True} causes task to transition to FAILED."""
    task_id = uuid.uuid4()
    async with async_session_factory() as db:
        task = Task(
            id=task_id,
            tenant_id="tenant_fail_test",
            task_type="test_fail",
            payload={"fail": True},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=0,
        )
        db.add(task)
        await db.commit()

    # Process task with worker
    await mock_worker.process_task(
        message_id="1-0",
        data={"task_id": str(task_id)},
    )

    # Verify task state in database
    async with async_session_factory() as db:
        result = await db.execute(select(Task).where(Task.id == task_id))
        updated_task = result.scalar_one()

        assert updated_task.status == TaskStatus.FAILED
        assert updated_task.attempts == 1
        assert updated_task.last_error is not None
        assert "Injected failure" in updated_task.last_error
        assert updated_task.finished_at is not None

        # Check events
        events_result = await db.execute(
            select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.created_at.asc())
        )
        events = events_result.scalars().all()
        event_types = [e.event_type for e in events]
        assert "RUNNING" in event_types
        assert "FAILED" in event_types


@pytest.mark.asyncio
async def test_failure_injection_fail_rate_deterministic(mock_worker):
    """Verify payload {'fail_rate': 1.0} fails and {'fail_rate': 0.0} succeeds."""
    # 1. Deterministic failure with fail_rate = 1.0
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
        )
        db.add(fail_task)
        await db.commit()

    await mock_worker.process_task(
        message_id="2-0",
        data={"task_id": str(fail_task_id)},
    )

    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == fail_task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert "fail_rate=1.0" in t.last_error

    # 2. Deterministic success with fail_rate = 0.0
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
        )
        db.add(pass_task)
        await db.commit()

    await mock_worker.process_task(
        message_id="3-0",
        data={"task_id": str(pass_task_id)},
    )

    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == pass_task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.SUCCEEDED
        assert t.last_error is None


@pytest.mark.asyncio
async def test_failure_injection_fail_until_attempt(mock_worker):
    """Verify payload {'fail_until_attempt': 3} fails on attempts < 3 and succeeds on attempt 3."""
    task_id = uuid.uuid4()

    # Create task with attempt = 0
    async with async_session_factory() as db:
        task = Task(
            id=task_id,
            tenant_id="tenant_retry_test",
            task_type="test_until_attempt",
            payload={"fail_until_attempt": 3},
            priority=5,
            status=TaskStatus.PENDING,
            attempts=0,
        )
        db.add(task)
        await db.commit()

    # 1st attempt: should fail (attempts becomes 1 < 3)
    await mock_worker.process_task(message_id="4-1", data={"task_id": str(task_id)})
    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempts == 1

    # 2nd attempt: should fail (attempts becomes 2 < 3)
    await mock_worker.process_task(message_id="4-2", data={"task_id": str(task_id)})
    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.FAILED
        assert t.attempts == 2

    # 3rd attempt: should succeed (attempts becomes 3 >= 3)
    await mock_worker.process_task(message_id="4-3", data={"task_id": str(task_id)})
    async with async_session_factory() as db:
        res = await db.execute(select(Task).where(Task.id == task_id))
        t = res.scalar_one()
        assert t.status == TaskStatus.SUCCEEDED
        assert t.attempts == 3
        assert t.last_error is None
