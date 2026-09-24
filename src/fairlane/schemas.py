from typing import Any, Dict, Optional
import uuid
from datetime import datetime
from pydantic import BaseModel, Field

class TaskCreate(BaseModel):
    tenant_id: str = Field(..., description="The ID of the tenant submitting the task")
    task_type: str = Field(..., description="The type/name of the task to execute")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Task arguments/data")
    priority: int = Field(default=5, ge=1, le=10, description="Task priority (1 is highest, 10 is lowest)")
    # TODO: Implement idempotency key

class TaskResponse(BaseModel):
    id: uuid.UUID
    tenant_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: int
    status: str
    attempts: int
    max_attempts: int
    idempotency_key: Optional[str]
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    last_error: Optional[str]

    class Config:
        from_attributes = True
