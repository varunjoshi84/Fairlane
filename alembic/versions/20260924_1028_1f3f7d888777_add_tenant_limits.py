"""add_tenant_limits

Revision ID: 1f3f7d888777
Revises: 45b4db904b7d
Create Date: 2026-09-24 10:28:09.142651+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1f3f7d888777'
down_revision: Union[str, None] = '45b4db904b7d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'tenant_limits',
        sa.Column('tenant_id', sa.String(length=255), nullable=False),
        sa.Column('rate_per_second', sa.Float(), nullable=False, server_default='5.0'),
        sa.Column('burst_capacity', sa.Integer(), nullable=False, server_default='10'),
        sa.Column('max_concurrent', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('weight', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('tenant_id')
    )


def downgrade() -> None:
    op.drop_table('tenant_limits')
