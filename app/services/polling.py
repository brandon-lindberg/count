from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import logging

from sqlalchemy import case, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import PlayerSample, Tier, TrackedApp
from app.services.main_db_mirror import mirror_poll_to_main_db
from app.services.partitions import ensure_partition_for_timestamp
from app.services.rollups import upsert_hourly_rollup
from app.services.steam_provider import SteamAppNotFoundError, SteamCurrentPlayersProvider
from app.services.tiering import poll_interval_minutes, refresh_effective_tier

STEAM_APP_NOT_FOUND_PREFIX = "steam_app_not_found:"
logger = logging.getLogger(__name__)


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


def update_all_time_peak(tracked_app: TrackedApp, concurrent_players: int, sampled_at: datetime) -> None:
    if tracked_app.all_time_peak_players is None or concurrent_players > tracked_app.all_time_peak_players:
        tracked_app.all_time_peak_players = concurrent_players
        tracked_app.all_time_peak_at = sampled_at


def launch_priority_date_range(settings: Settings, now: datetime | None = None) -> tuple[date, date]:
    effective_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
    return (
        effective_now - timedelta(days=settings.launch_priority_past_days),
        effective_now + timedelta(days=settings.launch_priority_future_days),
    )


def is_launch_priority_release_date(
    source_release_date: date | None,
    settings: Settings,
    now: datetime | None = None,
) -> bool:
    if source_release_date is None:
        return False
    priority_start, priority_end = launch_priority_date_range(settings, now)
    return priority_start <= source_release_date <= priority_end


async def get_due_steam_app_ids(
    session: AsyncSession,
    tier: Tier,
    settings: Settings,
    now: datetime | None = None,
) -> list[int]:
    effective_now = now or datetime.now(timezone.utc)
    cutoff = effective_now - timedelta(minutes=poll_interval_minutes(tier, settings))
    not_found_cutoff = effective_now - timedelta(hours=settings.not_found_retry_hours)
    never_polled_rank = case((TrackedApp.last_polled_at.is_(None), 0), else_=1)
    first_poll_import_sort = case((TrackedApp.last_polled_at.is_(None), TrackedApp.last_imported_at), else_=None)
    result = await session.execute(
        select(TrackedApp.steam_app_id)
        .where(
            TrackedApp.is_active.is_(True),
            TrackedApp.effective_tier == tier,
            TrackedApp.last_success_at.is_not(None),
            or_(TrackedApp.last_polled_at.is_(None), TrackedApp.last_polled_at <= cutoff),
            or_(
                TrackedApp.last_error.is_(None),
                ~TrackedApp.last_error.like(f"{STEAM_APP_NOT_FOUND_PREFIX}%"),
                TrackedApp.last_polled_at.is_(None),
                TrackedApp.last_polled_at <= not_found_cutoff,
            ),
        )
        .order_by(
            never_polled_rank.asc(),
            first_poll_import_sort.desc().nullslast(),
            TrackedApp.last_polled_at.asc().nullslast(),
            TrackedApp.steam_app_id.asc(),
        )
        .limit(settings.poll_batch_limit)
    )
    return list(result.scalars().all())


async def get_due_bootstrap_steam_app_ids(
    session: AsyncSession,
    settings: Settings,
    now: datetime | None = None,
) -> list[int]:
    effective_now = now or datetime.now(timezone.utc)
    cutoff = effective_now - timedelta(minutes=settings.bootstrap_poll_minutes)
    not_found_cutoff = effective_now - timedelta(hours=settings.not_found_retry_hours)
    launch_not_found_cutoff = effective_now - timedelta(minutes=settings.launch_not_found_retry_minutes)
    priority_start, priority_end = launch_priority_date_range(settings, effective_now)
    launch_priority_rank = case(
        (
            TrackedApp.source_release_date.is_not(None)
            & (TrackedApp.source_release_date >= priority_start)
            & (TrackedApp.source_release_date <= priority_end),
            0,
        ),
        else_=1,
    )
    result = await session.execute(
        select(TrackedApp.steam_app_id)
        .where(
            TrackedApp.is_active.is_(True),
            TrackedApp.last_success_at.is_(None),
            or_(TrackedApp.last_polled_at.is_(None), TrackedApp.last_polled_at <= cutoff),
            or_(
                TrackedApp.last_error.is_(None),
                ~TrackedApp.last_error.like(f"{STEAM_APP_NOT_FOUND_PREFIX}%"),
                TrackedApp.last_polled_at.is_(None),
                (
                    TrackedApp.source_release_date.is_not(None)
                    & (TrackedApp.source_release_date >= priority_start)
                    & (TrackedApp.source_release_date <= priority_end)
                    & (TrackedApp.last_polled_at <= launch_not_found_cutoff)
                ),
                TrackedApp.last_polled_at <= not_found_cutoff,
            ),
        )
        .order_by(
            launch_priority_rank.asc(),
            TrackedApp.source_release_date.desc().nullslast(),
            TrackedApp.last_imported_at.desc().nullslast(),
            TrackedApp.source_game_id.desc().nullslast(),
            TrackedApp.steam_app_id.asc(),
        )
        .limit(settings.bootstrap_batch_limit)
    )
    return list(result.scalars().all())


async def get_due_launch_watch_steam_app_ids(
    session: AsyncSession,
    settings: Settings,
    now: datetime | None = None,
) -> list[int]:
    effective_now = now or datetime.now(timezone.utc)
    cutoff = effective_now - timedelta(minutes=settings.bootstrap_poll_minutes)
    not_found_cutoff = effective_now - timedelta(minutes=settings.launch_not_found_retry_minutes)
    priority_start, priority_end = launch_priority_date_range(settings, effective_now)
    result = await session.execute(
        select(TrackedApp.steam_app_id)
        .where(
            TrackedApp.is_active.is_(True),
            TrackedApp.source_release_date.is_not(None),
            TrackedApp.source_release_date >= priority_start,
            TrackedApp.source_release_date <= priority_end,
            or_(TrackedApp.last_polled_at.is_(None), TrackedApp.last_polled_at <= cutoff),
            or_(
                TrackedApp.last_error.is_(None),
                ~TrackedApp.last_error.like(f"{STEAM_APP_NOT_FOUND_PREFIX}%"),
                TrackedApp.last_polled_at.is_(None),
                TrackedApp.last_polled_at <= not_found_cutoff,
            ),
        )
        .order_by(
            TrackedApp.source_release_date.desc().nullslast(),
            TrackedApp.last_imported_at.desc().nullslast(),
            TrackedApp.last_polled_at.asc().nullslast(),
            TrackedApp.steam_app_id.asc(),
        )
        .limit(settings.bootstrap_batch_limit)
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
        update_all_time_peak(tracked_app, concurrent_players, effective_sampled_at)
        # Flush the newly written sample before querying sample history for tiering and rollups.
        await session.flush()
        await refresh_effective_tier(session, tracked_app, settings, effective_sampled_at)
        await upsert_hourly_rollup(session, tracked_app, effective_sampled_at)
        await session.flush()
        try:
            await mirror_poll_to_main_db(
                tracked_app,
                sampled_at=effective_sampled_at,
                concurrent_players=concurrent_players,
                latest_24h_high=tracked_app.latest_24h_high,
                latest_24h_low=tracked_app.latest_24h_low,
                all_time_peak_players=tracked_app.all_time_peak_players,
                all_time_peak_at=tracked_app.all_time_peak_at,
                settings=settings,
            )
        except Exception:
            logger.exception(
                "Main DB mirror failed for tracked app %s (Steam app %s)",
                tracked_app.id,
                tracked_app.steam_app_id,
            )
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
