import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TaskCreate(BaseModel):
    tenant_id: str = Field(..., description="The ID of the tenant submitting the task")
    task_type: str = Field(..., description="The type/name of the task to execute")
    payload: dict[str, Any] = Field(default_factory=dict, description="Task arguments/data")
    priority: int = Field(default=5, ge=1, le=10, description="Task priority (1 is highest, 10 is lowest)")
    # TODO: Implement idempotency key


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
    last_error: str | None = None
