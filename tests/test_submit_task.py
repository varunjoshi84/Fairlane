"""Tests for task submission and retrieval endpoints."""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from fairlane.api.main import app


@pytest.mark.asyncio
async def test_submit_and_get_task():
    """Verify POST /tasks creates a task and GET /tasks/{id} retrieves it."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Submit task
        payload = {
            "tenant_id": "test_tenant",
            "task_type": "unit_test_task",
            "payload": {"key": "value", "count": 42},
            "priority": 3,
        }
        submit_resp = await client.post("/tasks", json=payload)
        assert submit_resp.status_code == 201
        data = submit_resp.json()
        assert "task_id" in data
        task_id = data["task_id"]

        # 2. Retrieve task
        get_resp = await client.get(f"/tasks/{task_id}")
        assert get_resp.status_code == 200
        task_data = get_resp.json()
        assert task_data["id"] == task_id
        assert task_data["tenant_id"] == "test_tenant"
        assert task_data["task_type"] == "unit_test_task"
        assert task_data["priority"] == 3
        assert task_data["status"] in ("PENDING", "RUNNING", "SUCCEEDED")


@pytest.mark.asyncio
async def test_get_nonexistent_task():
    """Verify GET /tasks/{nonexistent_id} returns 404."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        random_id = str(uuid.uuid4())
        resp = await client.get(f"/tasks/{random_id}")
        assert resp.status_code == 404
