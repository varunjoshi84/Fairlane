"""Add dead_letters table for Dead Letter Queue

Revision ID: 0003_add_dead_letters
Revises: 0002_add_next_retry_at
Create Date: 2026-09-24 14:50:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_add_dead_letters"
down_revision: str | None = "0002_add_next_retry_at"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create failure_category enum type
    failure_category_enum = postgresql.ENUM(
        "TRANSIENT_EXHAUSTED", "PERMANENT", "POISON_PILL",
        name="failure_category_enum",
        create_type=False,
    )
    failure_category_enum.create(op.get_bind(), checkfirst=True)

    # 2. Create dead_letters table
    op.create_table(
        "dead_letters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("tasks.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("tenant_id", sa.String(length=255), nullable=False),
        sa.Column("task_type", sa.String(length=255), nullable=False),
        sa.Column(
            "failure_category",
            postgresql.ENUM(
                "TRANSIENT_EXHAUSTED", "PERMANENT", "POISON_PILL",
                name="failure_category_enum",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "error_history",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("attempts_made", sa.Integer(), nullable=False),
        sa.Column(
            "dead_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "replay_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )

    # Indexes
    op.create_index("ix_dead_letters_tenant_id", "dead_letters", ["tenant_id"])
    op.create_index("ix_dead_letters_failure_category", "dead_letters", ["failure_category"])


def downgrade() -> None:
    op.drop_index("ix_dead_letters_failure_category", table_name="dead_letters")
    op.drop_index("ix_dead_letters_tenant_id", table_name="dead_letters")
    op.drop_table("dead_letters")

    failure_category_enum = postgresql.ENUM(
        "TRANSIENT_EXHAUSTED", "PERMANENT", "POISON_PILL",
        name="failure_category_enum",
    )
    failure_category_enum.drop(op.get_bind(), checkfirst=True)
