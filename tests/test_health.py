"""Tests for health check endpoint."""

import pytest
from httpx import ASGITransport, AsyncClient

from fairlane.api.main import app


@pytest.mark.asyncio
async def test_health_endpoint():
    """Verify GET /health returns 200 and connectivity status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data.get("status") == "ok"
        assert "db" in data
        assert "redis" in data
