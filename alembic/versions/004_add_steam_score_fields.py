"""add tracked app steam score fields

Revision ID: 004_add_steam_score_fields
Revises: 003_add_source_release_date
Create Date: 2026-05-08 11:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "004_add_steam_score_fields"
down_revision = "003_add_source_release_date"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tracked_apps", sa.Column("steam_user_score", sa.Numeric(5, 2), nullable=True))
    op.add_column("tracked_apps", sa.Column("steam_score_raw", sa.String(length=50), nullable=True))
    op.add_column("tracked_apps", sa.Column("steam_sample_size", sa.Integer(), nullable=True))
    op.add_column("tracked_apps", sa.Column("steam_positive_count", sa.Integer(), nullable=True))
    op.add_column("tracked_apps", sa.Column("steam_negative_count", sa.Integer(), nullable=True))
    op.add_column("tracked_apps", sa.Column("steam_review_score_desc", sa.String(length=100), nullable=True))
    op.add_column("tracked_apps", sa.Column("steam_score_synced_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("tracked_apps", "steam_score_synced_at")
    op.drop_column("tracked_apps", "steam_review_score_desc")
    op.drop_column("tracked_apps", "steam_negative_count")
    op.drop_column("tracked_apps", "steam_positive_count")
    op.drop_column("tracked_apps", "steam_sample_size")
    op.drop_column("tracked_apps", "steam_score_raw")
    op.drop_column("tracked_apps", "steam_user_score")
