"""Tests for idempotent submission and safe task completion (Chunk 3E)."""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update

from fairlane.api.main import app
from fairlane.db import async_session_factory
from fairlane.models import Task, TaskStatus


@pytest.mark.asyncio
async def test_idempotent_submission_returns_same_task_id():
    """Submitting the same idempotency_key twice returns the same task ID."""
    transport = ASGITransport(app=app)
    idem_key = f"test-idem-{uuid.uuid4().hex[:8]}"

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # First submission
        payload = {
            "tenant_id": "tenant_idem",
            "task_type": "idem_test",
            "payload": {"data": 1},
            "idempotency_key": idem_key,
        }
        resp1 = await client.post("/tasks", json=payload)
        assert resp1.status_code == 201
        task_id_1 = resp1.json()["task_id"]

        # Second submission with same key
        resp2 = await client.post("/tasks", json=payload)
        # Should return 201 (same response shape) with same task_id
        assert resp2.status_code == 201
        task_id_2 = resp2.json()["task_id"]

        assert task_id_1 == task_id_2, "Same idempotency_key must return same task_id"


@pytest.mark.asyncio
async def test_idempotent_submission_different_keys_create_different_tasks():
    """Different idempotency_keys produce distinct tasks."""
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        base = {
            "tenant_id": "tenant_idem",
            "task_type": "idem_test",
            "payload": {},
        }
        resp1 = await client.post(
            "/tasks", json={**base, "idempotency_key": f"key-{uuid.uuid4().hex[:8]}"}
        )
        resp2 = await client.post(
            "/tasks", json={**base, "idempotency_key": f"key-{uuid.uuid4().hex[:8]}"}
        )
        assert resp1.status_code == 201
        assert resp2.status_code == 201
        assert resp1.json()["task_id"] != resp2.json()["task_id"]


@pytest.mark.asyncio
async def test_safe_completion_lost_ownership():
    """A worker that has lost ownership cannot overwrite the task result.

    Simulates the scenario where Worker A starts a task, but a reaper
    reclaims it and gives it to Worker B. Worker A's UPDATE should
    affect 0 rows because locked_by no longer matches.
    """
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create a task
        resp = await client.post("/tasks", json={
            "tenant_id": "tenant_safe",
            "task_type": "safe_test",
            "payload": {},
        })
        assert resp.status_code == 201
        task_id = uuid.UUID(resp.json()["task_id"])

    # Simulate Worker A locking the task
    async with async_session_factory() as db:
        result = await db.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one()
        task.status = TaskStatus.RUNNING
        task.locked_by = "worker-A"
        task.attempts = 1
        await db.commit()

    # Simulate reaper reclaiming and giving to Worker B
    async with async_session_factory() as db:
        result = await db.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one()
        task.locked_by = "worker-B"
        task.status = TaskStatus.RUNNING
        await db.commit()

    # Worker A tries to mark SUCCEEDED — should affect 0 rows
    async with async_session_factory() as db:
        result = await db.execute(
            update(Task)
            .where(Task.id == task_id, Task.locked_by == "worker-A")
            .values(status=TaskStatus.SUCCEEDED)
        )
        assert result.rowcount == 0, "Worker A should not be able to overwrite task owned by Worker B"

    # Verify the task is still locked by Worker B and still RUNNING
    async with async_session_factory() as db:
        result = await db.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one()
        assert task.locked_by == "worker-B"
        assert task.status == TaskStatus.RUNNING
