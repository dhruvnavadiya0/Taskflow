"""Phase 2: add retry fields, DEAD_LETTER status, and job_attempts table

Revision ID: phase2_001
Revises: None
Create Date: 2026-10-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "phase2_001"
down_revision: Union[str, None] = "phase1_001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add retry columns to jobs table
    op.add_column("jobs", sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("jobs", sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"))
    op.add_column("jobs", sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("last_error", sa.Text(), nullable=True))

    # Add check constraint for non-negative retry_count
    op.create_check_constraint("ck_jobs_retry_count_non_negative", "jobs", "retry_count >= 0")

    # Note: The JobStatus enum needs to include DEAD_LETTER.
    # For PostgreSQL, we need to add the new enum value.
    # This is PostgreSQL-specific syntax.
    op.execute("ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'DEAD_LETTER'")

    # Create job_attempts table
    op.create_table(
        "job_attempts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False, index=True),
        sa.Column("worker_id", sa.String(50), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Enum("RUNNING", "COMPLETED", "FAILED", name="attemptstatus"), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("job_attempts")
    op.drop_constraint("ck_jobs_retry_count_non_negative", "jobs", type_="check")
    op.drop_column("jobs", "last_error")
    op.drop_column("jobs", "next_retry_at")
    op.drop_column("jobs", "max_retries")
    op.drop_column("jobs", "retry_count")
    # Note: removing enum values from PostgreSQL is complex and typically not done
    # in downgrade. The DEAD_LETTER value will remain in the enum.
