"""Phase 1: Initial schema

Revision ID: phase1_001
Revises: None
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "phase1_001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create JobStatus and JobPriority enums
    op.execute("CREATE TYPE jobstatus AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')")
    op.execute("CREATE TYPE jobpriority AS ENUM ('HIGH', 'NORMAL', 'LOW')")
    
    # Pre-create other enums to avoid issues in later migrations
    op.execute("CREATE TYPE attemptstatus AS ENUM ('RUNNING', 'COMPLETED', 'FAILED')")
    op.execute("CREATE TYPE workerstatus AS ENUM ('ONLINE', 'SUSPECTED', 'DEAD')")
    op.execute("CREATE TYPE userrole AS ENUM ('USER', 'ADMIN')")
    
    # 2. Create jobs table (base version before phase 2)
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("job_type", sa.String(50), nullable=False),
        sa.Column("status", sa.Enum("PENDING", "RUNNING", "COMPLETED", "FAILED", name="jobstatus", create_type=False), nullable=False),
        sa.Column("priority", sa.Enum("HIGH", "NORMAL", "LOW", name="jobpriority", create_type=False), nullable=False, server_default="NORMAL"),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("jobs")
    op.execute("DROP TYPE userrole")
    op.execute("DROP TYPE workerstatus")
    op.execute("DROP TYPE attemptstatus")
    op.execute("DROP TYPE jobpriority")
    op.execute("DROP TYPE jobstatus")
