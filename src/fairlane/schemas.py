import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TaskCreate(BaseModel):
    tenant_id: str = Field(..., description="The ID of the tenant submitting the task")
    task_type: str = Field(..., description="The type/name of the task to execute")
    payload: dict[str, Any] = Field(default_factory=dict, description="Task arguments/data")
    priority: int = Field(default=5, ge=1, le=10, description="Task priority (1 is highest, 10 is lowest)")
    idempotency_key: str | None = Field(None, description="Optional idempotency key for deduplication")


class TaskEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    task_id: uuid.UUID
    event_type: str
    details: dict[str, Any]
    created_at: datetime


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: str
    task_type: str
    payload: dict[str, Any]
    priority: int
    status: str
    attempts: int
    max_attempts: int
    idempotency_key: str | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    next_retry_at: datetime | None = None
    last_error: str | None = None
    locked_by: str | None = None
    locked_at: datetime | None = None


# ---------------------------------------------------------------------------
# Dead-Letter Queue (DLQ) schemas
# ---------------------------------------------------------------------------


class DeadLetterResponse(BaseModel):
    """Summary representation of a dead-lettered task (used in list views)."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    task_id: uuid.UUID
    tenant_id: str
    task_type: str
    failure_category: str
    last_error: str | None = None
    attempts_made: int
    dead_at: datetime
    replayed_at: datetime | None = None
    replay_count: int


class DeadLetterDetailResponse(DeadLetterResponse):
    """Full representation including error history."""

    error_history: list[dict[str, Any]] = Field(default_factory=list)


class DeadLetterListResponse(BaseModel):
    """Paginated list of dead-lettered tasks."""

    items: list[DeadLetterResponse]
    total: int
    page: int
    page_size: int


class CategoryStat(BaseModel):
    """Count of dead letters for a single failure category."""

    category: str
    count: int


class TenantStat(BaseModel):
    """Count of dead letters for a single tenant."""

    tenant_id: str
    count: int


class DeadLetterStatsResponse(BaseModel):
    """Aggregated DLQ statistics."""

    total: int
    by_category: list[CategoryStat]
    by_tenant: list[TenantStat]


# ---------------------------------------------------------------------------
# Replay schemas
# ---------------------------------------------------------------------------


class ReplayRequest(BaseModel):
    """Optional body for replaying a single dead-lettered task."""

    payload_patch: dict[str, Any] | None = Field(
        None, description="Merge-patch applied to the task payload before replay"
    )
    reset_attempts: bool = Field(
        False, description="If true, reset attempts to 0"
    )


class BulkReplayRequest(BaseModel):
    """Criteria for bulk-replaying dead-lettered tasks."""

    tenant_id: str | None = Field(None, description="Filter by tenant")
    failure_category: str | None = Field(None, description="Filter by failure category")


class ReplayResponse(BaseModel):
    """Response after replaying a single task."""

    task_id: str
    status: str
    message: str


class BulkReplayResponse(BaseModel):
    """Response after bulk-replaying tasks."""

    replayed: int
    task_ids: list[str]


# ---------------------------------------------------------------------------
# Tenant schemas
# ---------------------------------------------------------------------------


class TenantLimitsConfig(BaseModel):
    rate_per_second: float = Field(..., gt=0)
    burst_capacity: int = Field(..., gt=0)
    max_concurrent: int = Field(..., gt=0)
    weight: int = Field(1, ge=1)


class TenantUsage(BaseModel):
    tenant_id: str
    tokens_remaining: float
    running_tasks: int
    pending_tasks: int


