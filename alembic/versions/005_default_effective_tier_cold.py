"""default tracked app tier to cold

Revision ID: 005_default_effective_tier_cold
Revises: 004_add_steam_score_fields
Create Date: 2026-05-08 15:50:00
"""

from __future__ import annotations

from alembic import op

revision = "005_default_effective_tier_cold"
down_revision = "004_add_steam_score_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE tracked_apps ALTER COLUMN effective_tier SET DEFAULT 'cold'")


def downgrade() -> None:
    op.execute("ALTER TABLE tracked_apps ALTER COLUMN effective_tier SET DEFAULT 'warm'")
