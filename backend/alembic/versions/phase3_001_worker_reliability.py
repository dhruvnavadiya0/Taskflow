"""Phase 3: add workers table, job ownership/lease, recovery_count, and RECOVERED attempt status

Revision ID: phase3_001
Revises: phase2_001
Create Date: 2026-10-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "phase3_001"
down_revision: Union[str, None] = "phase2_001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create workers table
    op.create_table(
        "workers",
        sa.Column("id", sa.String(50), primary_key=True),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column("status", sa.Enum("ONLINE", "SUSPECTED", "DEAD", name="workerstatus"), nullable=False),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_job_id", sa.String(36), nullable=True),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    # Add ownership/lease columns to jobs table
    op.add_column("jobs", sa.Column("worker_id", sa.String(50), nullable=True))
    op.add_column("jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("recovery_count", sa.Integer(), nullable=False, server_default="0"))

    # Add RECOVERED to attempt status enum (PostgreSQL-specific)
    op.execute("ALTER TYPE attemptstatus ADD VALUE IF NOT EXISTS 'RECOVERED'")


def downgrade() -> None:
    op.drop_column("jobs", "recovery_count")
    op.drop_column("jobs", "lease_expires_at")
    op.drop_column("jobs", "worker_id")
    op.drop_table("workers")
    # Note: removing enum values from PostgreSQL is complex
