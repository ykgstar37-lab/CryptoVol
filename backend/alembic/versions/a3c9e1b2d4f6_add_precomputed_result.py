"""add precomputed_result

Revision ID: a3c9e1b2d4f6
Revises: f7fed8a3a1e3
Create Date: 2026-11-16 00:30:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a3c9e1b2d4f6'
down_revision: Union[str, Sequence[str], None] = 'f7fed8a3a1e3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'precomputed_result',
        sa.Column('key', sa.String(length=64), nullable=False),
        sa.Column('version', sa.Date(), nullable=True),
        sa.Column('payload', sa.Text(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('key'),
    )


def downgrade() -> None:
    op.drop_table('precomputed_result')
