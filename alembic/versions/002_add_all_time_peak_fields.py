"""add tracked app all-time peak fields

Revision ID: 002_add_all_time_peak_fields
Revises: 001_initial_schema
Create Date: 2026-03-19 00:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "002_add_all_time_peak_fields"
down_revision = "001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tracked_apps", sa.Column("all_time_peak_players", sa.Integer(), nullable=True))
    op.add_column("tracked_apps", sa.Column("all_time_peak_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        """
        UPDATE tracked_apps AS tracked
        SET
            all_time_peak_players = (
                SELECT sample.concurrent_players
                FROM player_samples AS sample
                WHERE sample.steam_app_id = tracked.steam_app_id
                ORDER BY sample.concurrent_players DESC, sample.sampled_at ASC
                LIMIT 1
            ),
            all_time_peak_at = (
                SELECT sample.sampled_at
                FROM player_samples AS sample
                WHERE sample.steam_app_id = tracked.steam_app_id
                ORDER BY sample.concurrent_players DESC, sample.sampled_at ASC
                LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM player_samples AS sample
            WHERE sample.steam_app_id = tracked.steam_app_id
        )
        """
    )


def downgrade() -> None:
    op.drop_column("tracked_apps", "all_time_peak_at")
    op.drop_column("tracked_apps", "all_time_peak_players")
