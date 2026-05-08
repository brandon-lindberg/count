from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import PlayerSample, Tier, TrackedApp


def compute_auto_tier(max_players: int | None, settings: Settings) -> Tier:
    if max_players is None:
        return Tier.cold
    if max_players >= settings.hot_threshold:
        return Tier.hot
    if max_players >= settings.warm_threshold:
        return Tier.warm
    return Tier.cold


def resolve_effective_tier(manual_override: Tier | None, auto_tier: Tier) -> Tier:
    return manual_override or auto_tier


def is_release_hot_window(
    source_release_date: date | None,
    settings: Settings,
    now: datetime | None = None,
) -> bool:
    if source_release_date is None:
        return False
    effective_date = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
    days_since_release = (effective_date - source_release_date).days
    return 0 <= days_since_release < settings.release_hot_days


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def is_recent_release_warm_floor_window(
    source_release_date: date | None,
    settings: Settings,
    now: datetime | None = None,
) -> bool:
    if source_release_date is None:
        return False

    effective_date = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
    if effective_date < source_release_date:
        return False

    return effective_date < _add_months(source_release_date, settings.warm_floor_months)


def apply_recent_release_warm_floor(
    auto_tier: Tier,
    source_release_date: date | None,
    settings: Settings,
    now: datetime | None = None,
) -> Tier:
    if auto_tier != Tier.cold:
        return auto_tier
    if is_recent_release_warm_floor_window(source_release_date, settings, now):
        return Tier.warm
    return auto_tier


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

    if is_release_hot_window(tracked_app.source_release_date, settings, now):
        tracked_app.effective_tier = Tier.hot
        return tracked_app.effective_tier

    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=7)
    result = await session.execute(
        select(func.max(PlayerSample.concurrent_players)).where(
            PlayerSample.tracked_app_id == tracked_app.id,
            PlayerSample.sampled_at >= cutoff,
        )
    )
    max_players = result.scalar_one_or_none()
    tracked_app.effective_tier = apply_recent_release_warm_floor(
        compute_auto_tier(int(max_players) if max_players is not None else None, settings),
        tracked_app.source_release_date,
        settings,
        now,
    )
    return tracked_app.effective_tier
