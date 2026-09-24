"""SQLAlchemy database models for Fairlane task queue."""

import enum
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""
    pass


class TaskStatus(enum.StrEnum):
    """Lifecycle states of a task in the Fairlane engine."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD = "DEAD"


class WorkerStatus(enum.StrEnum):
    """Lifecycle states of a worker."""

    ACTIVE = "ACTIVE"
    DEAD = "DEAD"
    STOPPED = "STOPPED"


class Task(Base):
    """Represents an asynchronous task submitted to the engine."""

    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    tenant_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    task_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    # Priority: 1 (highest) to 10 (lowest), default 5
    # TODO: Implement priority-based stream partitioning and dequeuing
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status_enum", native_enum=True),
        nullable=False,
        default=TaskStatus.PENDING,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    # TODO: Implement retry backoff and Dead Letter Queue (DLQ) routing when attempts >= max_attempts
    idempotency_key: Mapped[str | None] = mapped_column(
        String(255),
        unique=True,
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    original_enqueue_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    events: Mapped[list["TaskEvent"]] = relationship(
        "TaskEvent",
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="TaskEvent.created_at.asc()",
    )
    dead_letter: Mapped["DeadLetter | None"] = relationship(
        "DeadLetter",
        back_populates="task",
        uselist=False,
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_tasks_tenant_status", "tenant_id", "status"),
        Index("ix_tasks_created_at", "created_at"),
        Index("ix_tasks_next_retry_at", "next_retry_at"),
    )


class TaskEvent(Base):
    """Audit log entry tracking state changes and lifecycle events of a task."""

    __tablename__ = "task_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    # Relationships
    task: Mapped["Task"] = relationship("Task", back_populates="events")


class FailureCategory(enum.StrEnum):
    """Classification of why a task ended up in the dead-letter queue."""

    TRANSIENT_EXHAUSTED = "TRANSIENT_EXHAUSTED"
    PERMANENT = "PERMANENT"
    POISON_PILL = "POISON_PILL"


class DeadLetter(Base):
    """Dead-letter queue entry for tasks that have permanently failed."""

    __tablename__ = "dead_letters"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    task_type: Mapped[str] = mapped_column(String(255), nullable=False)
    failure_category: Mapped[FailureCategory] = mapped_column(
        Enum(FailureCategory, name="failure_category_enum", native_enum=True),
        nullable=False,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_history: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
    )
    attempts_made: Mapped[int] = mapped_column(Integer, nullable=False)
    dead_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    replayed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    replay_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationships
    task: Mapped["Task"] = relationship("Task", back_populates="dead_letter")

    __table_args__ = (
        Index("ix_dead_letters_tenant_id", "tenant_id"),
        Index("ix_dead_letters_failure_category", "failure_category"),
    )


class Worker(Base):
    """Tracks active and stopped workers."""

    __tablename__ = "workers"

    worker_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    hostname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[WorkerStatus] = mapped_column(
        Enum(WorkerStatus, name="worker_status_enum", native_enum=True),
        nullable=False,
        default=WorkerStatus.ACTIVE,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    current_task_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )


class TenantLimit(Base):
    """Tenant configuration for rate limiting and fair scheduling."""

    __tablename__ = "tenant_limits"

    tenant_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    rate_per_second: Mapped[float] = mapped_column(Float, nullable=False, default=5.0)
    burst_capacity: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    max_concurrent: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    weight: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class SideEffect(Base):
    """Tracks deduplicated execution side effects (for exactly-once checks)."""

    __tablename__ = "side_effects"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

