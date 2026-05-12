from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import logging

from sqlalchemy import case, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import PlayerSample, Tier, TrackedApp
from app.services.main_db_mirror import mirror_poll_to_main_db
from app.services.partitions import ensure_partition_for_timestamp
from app.services.rollups import upsert_hourly_rollup
from app.services.steam_provider import (
    SteamAppNotFoundError,
    SteamCurrentPlayersProvider,
    SteamProviderError,
    SteamUserScore,
)
from app.services.tiering import poll_interval_minutes, refresh_effective_tier

STEAM_APP_NOT_FOUND_PREFIX = "steam_app_not_found:"
logger = logging.getLogger(__name__)


class PollAppMode(str, Enum):
    """What Steam + DB paths poll_app runs (mirror uses coalesce for partial updates)."""

    full = "full"
    players_only = "players_only"
    user_score_only = "user_score_only"


@dataclass(slots=True)
class PollResult:
    steam_app_id: int
    concurrent_players: int
    sampled_at: datetime
    effective_tier: Tier


def tier_poll_batch_limit(tier: Tier, settings: Settings) -> int:
    if tier == Tier.hot:
        return settings.hot_poll_batch_limit
    if tier == Tier.warm:
        return settings.warm_poll_batch_limit
    return 0


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
        .limit(tier_poll_batch_limit(tier, settings))
    )
    return list(result.scalars().all())


async def get_due_user_score_steam_app_ids(
    session: AsyncSession,
    settings: Settings,
    now: datetime | None = None,
) -> list[int]:
    """Apps that need a Steam store review / user score refresh (independent of tier player poll cadence)."""
    effective_now = now or datetime.now(timezone.utc)
    cutoff = effective_now - timedelta(minutes=settings.user_score_poll_minutes)
    tier_rank = case(
        (TrackedApp.effective_tier == Tier.hot, 0),
        (TrackedApp.effective_tier == Tier.warm, 1),
        else_=2,
    )
    result = await session.execute(
        select(TrackedApp.steam_app_id)
        .where(
            TrackedApp.is_active.is_(True),
            TrackedApp.last_success_at.is_not(None),
            or_(
                TrackedApp.steam_score_synced_at.is_(None),
                TrackedApp.steam_score_synced_at <= cutoff,
            ),
        )
        .order_by(
            tier_rank.asc(),
            TrackedApp.steam_score_synced_at.asc().nullsfirst(),
            TrackedApp.steam_app_id.asc(),
        )
        .limit(settings.user_score_poll_batch_limit)
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
        .limit(settings.launch_watch_batch_limit)
    )
    return list(result.scalars().all())


async def upsert_player_sample(
    session: AsyncSession,
    tracked_app: TrackedApp,
    sampled_at: datetime,
    concurrent_players: int,
) -> None:
    await session.execute(
        pg_insert(PlayerSample)
        .values(
            tracked_app_id=tracked_app.id,
            steam_app_id=tracked_app.steam_app_id,
            sampled_at=sampled_at,
            concurrent_players=concurrent_players,
            provider="steam_current_players",
        )
        .on_conflict_do_update(
            index_elements=[PlayerSample.steam_app_id, PlayerSample.sampled_at],
            set_={
                "tracked_app_id": tracked_app.id,
                "concurrent_players": concurrent_players,
                "provider": "steam_current_players",
            },
        )
    )


async def persist_poll_failure(
    session: AsyncSession,
    tracked_app_id: int,
    sampled_at: datetime,
    exc: Exception,
    *,
    touch_last_polled_at: bool = True,
) -> None:
    try:
        await session.rollback()
        tracked_app = await session.get(TrackedApp, tracked_app_id)
        if tracked_app is None:
            return
        if touch_last_polled_at:
            tracked_app.last_polled_at = sampled_at
        tracked_app.last_error = format_poll_error(exc)[:2000]
        await session.flush()
    except Exception:
        await session.rollback()
        logger.exception("Failed to persist poll error state for tracked app %s", tracked_app_id)


async def poll_app(
    session: AsyncSession,
    tracked_app: TrackedApp,
    provider: SteamCurrentPlayersProvider,
    settings: Settings,
    sampled_at: datetime | None = None,
    *,
    mode: PollAppMode = PollAppMode.full,
) -> PollResult:
    tracked_app_id = tracked_app.id
    effective_sampled_at = normalize_sample_timestamp(sampled_at)

    # Fetch player count and user score independently so a failure on one path
    # does not block the other. The store reviews endpoint can succeed for a
    # game whose current_players endpoint 404s (and vice versa).
    concurrent_players: int | None = None
    player_count_error: Exception | None = None
    if mode != PollAppMode.user_score_only:
        try:
            concurrent_players = await provider.get_current_players(tracked_app.steam_app_id)
        except Exception as exc:
            player_count_error = exc

    steam_score: SteamUserScore | None = None
    user_score_error: Exception | None = None
    if mode != PollAppMode.players_only:
        try:
            steam_score = await provider.get_user_score(tracked_app.steam_app_id)
        except Exception as exc:
            user_score_error = exc

    if concurrent_players is None and steam_score is None:
        primary_error: Exception
        if mode == PollAppMode.players_only:
            primary_error = (
                player_count_error
                or SteamProviderError(f"Empty Steam response for app {tracked_app.steam_app_id}")
            )
        elif mode == PollAppMode.user_score_only:
            primary_error = (
                user_score_error
                or SteamProviderError(f"Empty Steam response for app {tracked_app.steam_app_id}")
            )
        else:
            primary_error = (
                player_count_error
                or user_score_error
                or SteamProviderError(
                    f"Empty Steam response for app {tracked_app.steam_app_id}",
                )
            )
        await persist_poll_failure(
            session,
            tracked_app_id,
            effective_sampled_at,
            primary_error,
            touch_last_polled_at=mode != PollAppMode.user_score_only,
        )
        raise primary_error

    try:
        await ensure_partition_for_timestamp(session, effective_sampled_at)

        if mode != PollAppMode.user_score_only:
            tracked_app.last_polled_at = effective_sampled_at

        if concurrent_players is not None:
            await upsert_player_sample(session, tracked_app, effective_sampled_at, concurrent_players)
            tracked_app.last_known_players = concurrent_players
            tracked_app.last_success_at = effective_sampled_at
            tracked_app.last_error = None
            update_all_time_peak(tracked_app, concurrent_players, effective_sampled_at)
        elif player_count_error is not None and mode != PollAppMode.user_score_only:
            tracked_app.last_error = format_poll_error(player_count_error)[:2000]

        if steam_score is not None:
            tracked_app.steam_user_score = steam_score.score
            tracked_app.steam_score_raw = steam_score.score_raw
            tracked_app.steam_sample_size = steam_score.sample_size
            tracked_app.steam_positive_count = steam_score.positive_count
            tracked_app.steam_negative_count = steam_score.negative_count
            tracked_app.steam_review_score_desc = steam_score.review_score_desc
            tracked_app.steam_score_synced_at = steam_score.scraped_at
            # Even if the player count failed, a successful score fetch counts as
            # "we reached Steam," which lets the app graduate out of bootstrap.
            if tracked_app.last_success_at is None:
                tracked_app.last_success_at = effective_sampled_at
                tracked_app.last_error = None

        # Flush the newly written sample before querying sample history for tiering and rollups.
        await session.flush()
        if mode != PollAppMode.user_score_only:
            await refresh_effective_tier(session, tracked_app, settings, effective_sampled_at)
            if concurrent_players is not None:
                await upsert_hourly_rollup(session, tracked_app, effective_sampled_at)
        await session.flush()
    except Exception as exc:
        await persist_poll_failure(
            session,
            tracked_app_id,
            effective_sampled_at,
            exc,
            touch_last_polled_at=mode != PollAppMode.user_score_only,
        )
        raise

    # Commit the scraper-side state before attempting the mirror. A mirror failure
    # from this point on must not roll back the score/player data we just paid
    # Steam to fetch — otherwise transient main-DB issues silently zero out new
    # games' Steam scores.
    await session.commit()

    try:
        await mirror_poll_to_main_db(
            tracked_app,
            sampled_at=effective_sampled_at,
            concurrent_players=concurrent_players,
            latest_24h_high=tracked_app.latest_24h_high,
            latest_24h_low=tracked_app.latest_24h_low,
            all_time_peak_players=tracked_app.all_time_peak_players,
            all_time_peak_at=tracked_app.all_time_peak_at,
            steam_user_score=tracked_app.steam_user_score if steam_score is not None else None,
            steam_score_raw=tracked_app.steam_score_raw if steam_score is not None else None,
            steam_sample_size=tracked_app.steam_sample_size if steam_score is not None else None,
            steam_positive_count=tracked_app.steam_positive_count if steam_score is not None else None,
            steam_negative_count=tracked_app.steam_negative_count if steam_score is not None else None,
            steam_review_score_desc=tracked_app.steam_review_score_desc if steam_score is not None else None,
            steam_score_synced_at=tracked_app.steam_score_synced_at if steam_score is not None else None,
            settings=settings,
        )
    except Exception:
        logger.exception(
            "Main DB mirror failed for tracked app %s (Steam app %s); local scraper state preserved",
            tracked_app.id,
            tracked_app.steam_app_id,
        )
        # Intentionally do not re-raise: the local commit above is the source of
        # truth for the scraper; the next successful poll will re-attempt the
        # mirror via the same coalesce semantics.

    return PollResult(
        steam_app_id=tracked_app.steam_app_id,
        concurrent_players=concurrent_players if concurrent_players is not None else (tracked_app.last_known_players or 0),
        sampled_at=effective_sampled_at,
        effective_tier=tracked_app.effective_tier,
    )
