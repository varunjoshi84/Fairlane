"""Add next_retry_at column to tasks table

Revision ID: 0002_add_next_retry_at
Revises: 0001_initial_schema
Create Date: 2026-09-24 14:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002_add_next_retry_at"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tasks_next_retry_at", "tasks", ["next_retry_at"])


def downgrade() -> None:
    op.drop_index("ix_tasks_next_retry_at", table_name="tasks")
    op.drop_column("tasks", "next_retry_at")
