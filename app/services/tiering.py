from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import PlayerSample, Tier, TrackedApp


def compute_auto_tier(max_players: int | None, settings: Settings) -> Tier:
    if max_players is None:
        return Tier.warm
    if max_players >= settings.hot_threshold:
        return Tier.hot
    if max_players >= settings.warm_threshold:
        return Tier.warm
    return Tier.cold


def resolve_effective_tier(manual_override: Tier | None, auto_tier: Tier) -> Tier:
    return manual_override or auto_tier


def poll_interval_minutes(tier: Tier, settings: Settings) -> int:
    if tier == Tier.hot:
        return settings.hot_poll_minutes
    if tier == Tier.warm:
        return settings.warm_poll_minutes
    return settings.cold_poll_minutes


async def refresh_effective_tier(
    session: AsyncSession,
    tracked_app: TrackedApp,
    settings: Settings,
    now: datetime | None = None,
) -> Tier:
    if tracked_app.manual_tier_override is not None:
        tracked_app.effective_tier = tracked_app.manual_tier_override
        return tracked_app.effective_tier

    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=7)
    result = await session.execute(
        select(func.max(PlayerSample.concurrent_players)).where(
            PlayerSample.tracked_app_id == tracked_app.id,
            PlayerSample.sampled_at >= cutoff,
        )
    )
    max_players = result.scalar_one_or_none()
    tracked_app.effective_tier = compute_auto_tier(int(max_players) if max_players is not None else None, settings)
    return tracked_app.effective_tier
