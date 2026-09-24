import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from fairlane.config import settings
from fairlane.db import check_db_health, get_db
from fairlane.models import Task, TaskEvent, TaskStatus, Worker
from fairlane.redis_client import check_redis_health, push_task_to_stream
from fairlane.schemas import TaskCreate, TaskEventResponse, TaskResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health_check():
    """Checks Postgres and Redis connections."""
    db_ok = await check_db_health()
    redis_ok = await check_redis_health()

    if not (db_ok and redis_ok):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"db": db_ok, "redis": redis_ok},
        )
    return {"status": "ok", "db": db_ok, "redis": redis_ok}


@router.post("/tasks", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_task(task_data: TaskCreate, db: AsyncSession = Depends(get_db)):
    """Create a new task, save to DB, and push to Redis Stream."""

    try:
        # Idempotency check: if idempotency_key is provided, look for existing task
        if task_data.idempotency_key:
            result = await db.execute(
                select(Task).where(Task.idempotency_key == task_data.idempotency_key)
            )
            existing_task = result.scalar_one_or_none()
            if existing_task:
                return {"task_id": str(existing_task.id)}

        # 0. Check submission quota
        result = await db.execute(
            select(func.count(Task.id))
            .where(Task.tenant_id == task_data.tenant_id, Task.status == TaskStatus.PENDING)
        )
        pending_count = result.scalar() or 0
        if pending_count >= settings.max_pending_per_tenant:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Too many pending tasks for tenant {task_data.tenant_id}",
                headers={"Retry-After": "60"}
            )

        # 1. Save task as PENDING in Postgres
        new_task = Task(
            tenant_id=task_data.tenant_id,
            task_type=task_data.task_type,
            payload=task_data.payload,
            priority=task_data.priority,
            status=TaskStatus.PENDING,
            idempotency_key=task_data.idempotency_key,
        )
        db.add(new_task)
        await db.flush()  # to generate new_task.id

        # 2. Add a task_events row "CREATED"
        task_event = TaskEvent(
            task_id=new_task.id,
            event_type="CREATED",
            details={"message": "Task successfully created"},
        )
        db.add(task_event)

        await db.commit()
        await db.refresh(new_task)

        # 3. Push the task to the tenant's waiting room
        from fairlane.scheduler import enqueue
        try:
            await enqueue(new_task)
        except Exception as redis_err:
            logger.warning(f"Failed to enqueue task {new_task.id} to Redis (will be reconciled): {redis_err}")
        
        # Metrics
        from fairlane.metrics import inc_tasks_submitted
        inc_tasks_submitted(new_task.tenant_id, new_task.priority)

        # 4. Return the task id
        return {"task_id": str(new_task.id)}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while creating task")


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Return task status and details."""
    result = await db.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return task


@router.get("/tasks/{task_id}/events", response_model=list[TaskEventResponse])
async def get_task_events(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Return task audit events in chronological order."""
    task_exists = await db.execute(select(Task.id).where(Task.id == task_id))
    if not task_exists.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Task not found")

    result = await db.execute(
        select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.created_at.asc())
    )
    return result.scalars().all()


@router.get("/workers")
async def list_workers(db: AsyncSession = Depends(get_db)):
    """List all registered workers."""
    result = await db.execute(select(Worker))
    workers = result.scalars().all()
    return [
        {
            "worker_id": w.worker_id,
            "hostname": w.hostname,
            "status": w.status.value,
            "started_at": w.started_at.isoformat() if w.started_at else None,
            "last_heartbeat_at": w.last_heartbeat_at.isoformat() if w.last_heartbeat_at else None,
            "current_task_id": str(w.current_task_id) if w.current_task_id else None,
        }
        for w in workers
    ]

