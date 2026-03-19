from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import PlayerSample, Tier, TrackedApp
from app.services.partitions import ensure_partition_for_timestamp
from app.services.rollups import upsert_hourly_rollup
from app.services.steam_provider import SteamAppNotFoundError, SteamCurrentPlayersProvider
from app.services.tiering import poll_interval_minutes, refresh_effective_tier

STEAM_APP_NOT_FOUND_PREFIX = "steam_app_not_found:"


@dataclass(slots=True)
class PollResult:
    steam_app_id: int
    concurrent_players: int
    sampled_at: datetime
    effective_tier: Tier


def normalize_sample_timestamp(sampled_at: datetime | None = None) -> datetime:
    effective = (sampled_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return effective.replace(second=0, microsecond=0)


def format_poll_error(exc: Exception) -> str:
    if isinstance(exc, SteamAppNotFoundError):
        return f"{STEAM_APP_NOT_FOUND_PREFIX} {exc}"
    return str(exc)


async def get_due_steam_app_ids(
    session: AsyncSession,
    tier: Tier,
    settings: Settings,
    now: datetime | None = None,
) -> list[int]:
    effective_now = now or datetime.now(timezone.utc)
    cutoff = effective_now - timedelta(minutes=poll_interval_minutes(tier, settings))
    not_found_cutoff = effective_now - timedelta(hours=settings.not_found_retry_hours)
    result = await session.execute(
        select(TrackedApp.steam_app_id)
        .where(
            TrackedApp.is_active.is_(True),
            TrackedApp.effective_tier == tier,
            or_(TrackedApp.last_polled_at.is_(None), TrackedApp.last_polled_at <= cutoff),
            or_(
                TrackedApp.last_error.is_(None),
                ~TrackedApp.last_error.like(f"{STEAM_APP_NOT_FOUND_PREFIX}%"),
                TrackedApp.last_polled_at.is_(None),
                TrackedApp.last_polled_at <= not_found_cutoff,
            ),
        )
        .order_by(TrackedApp.last_polled_at.asc().nullsfirst(), TrackedApp.steam_app_id.asc())
        .limit(settings.poll_batch_limit)
    )
    return list(result.scalars().all())


async def poll_app(
    session: AsyncSession,
    tracked_app: TrackedApp,
    provider: SteamCurrentPlayersProvider,
    settings: Settings,
    sampled_at: datetime | None = None,
) -> PollResult:
    effective_sampled_at = normalize_sample_timestamp(sampled_at)
    try:
        await ensure_partition_for_timestamp(session, effective_sampled_at)
        concurrent_players = await provider.get_current_players(tracked_app.steam_app_id)
        existing_sample = await session.scalar(
            select(PlayerSample).where(
                PlayerSample.steam_app_id == tracked_app.steam_app_id,
                PlayerSample.sampled_at == effective_sampled_at,
            )
        )
        if existing_sample is None:
            session.add(
                PlayerSample(
                    tracked_app_id=tracked_app.id,
                    steam_app_id=tracked_app.steam_app_id,
                    sampled_at=effective_sampled_at,
                    concurrent_players=concurrent_players,
                    provider="steam_current_players",
                )
            )
        else:
            existing_sample.concurrent_players = concurrent_players

        tracked_app.last_known_players = concurrent_players
        tracked_app.last_polled_at = effective_sampled_at
        tracked_app.last_success_at = effective_sampled_at
        tracked_app.last_error = None
        await upsert_hourly_rollup(session, tracked_app, effective_sampled_at)
        await refresh_effective_tier(session, tracked_app, settings, effective_sampled_at)
        await session.flush()
        return PollResult(
            steam_app_id=tracked_app.steam_app_id,
            concurrent_players=concurrent_players,
            sampled_at=effective_sampled_at,
            effective_tier=tracked_app.effective_tier,
        )
    except Exception as exc:
        tracked_app.last_polled_at = effective_sampled_at
        tracked_app.last_error = format_poll_error(exc)[:2000]
        await session.flush()
        raise
