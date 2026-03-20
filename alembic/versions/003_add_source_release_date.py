"""add source release date to tracked apps

Revision ID: 003_add_source_release_date
Revises: 002_add_all_time_peak_fields
Create Date: 2026-03-19 01:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "003_add_source_release_date"
down_revision = "002_add_all_time_peak_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tracked_apps", sa.Column("source_release_date", sa.Date(), nullable=True))
    op.create_index("idx_tracked_apps_source_release_date", "tracked_apps", ["source_release_date"])


def downgrade() -> None:
    op.drop_index("idx_tracked_apps_source_release_date", table_name="tracked_apps")
    op.drop_column("tracked_apps", "source_release_date")
