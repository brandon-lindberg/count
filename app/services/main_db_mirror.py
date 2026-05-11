from __future__ import annotations

import json
import logging
import ssl
from dataclasses import dataclass
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings, get_settings
from app.models import PlayerActivityHourly, PlayerSample, TrackedApp

logger = logging.getLogger(__name__)

_mirror_engine = None
_mirror_session_factory: async_sessionmaker[AsyncSession] | None = None
_mirror_url: str | None = None
_mirror_ssl: bool | None = None


@dataclass(slots=True)
class MirrorBackfillStats:
    apps_processed: int = 0
    apps_skipped_unmapped: int = 0
    apps_skipped_already_mirrored: int = 0
    apps_failed: int = 0
    raw_samples_seen: int = 0
    raw_samples_inserted: int = 0
    range_points_seen: int = 0
    range_points_upserted: int = 0
    summaries_updated: int = 0

    def merge(self, other: "MirrorBackfillStats") -> None:
        self.apps_processed += other.apps_processed
        self.apps_skipped_unmapped += other.apps_skipped_unmapped
        self.apps_skipped_already_mirrored += other.apps_skipped_already_mirrored
        self.apps_failed += other.apps_failed
        self.raw_samples_seen += other.raw_samples_seen
        self.raw_samples_inserted += other.raw_samples_inserted
        self.range_points_seen += other.range_points_seen
        self.range_points_upserted += other.range_points_upserted
        self.summaries_updated += other.summaries_updated


def mirror_is_configured(settings: Settings | None = None) -> bool:
    effective_settings = settings or get_settings()
    return bool(effective_settings.mirror_database_url)


def _get_mirror_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession] | None:
    global _mirror_engine, _mirror_session_factory, _mirror_url, _mirror_ssl

    if not settings.mirror_database_url:
        return None

    if (
        _mirror_session_factory is not None
        and _mirror_url == settings.mirror_database_url
        and _mirror_ssl == settings.mirror_database_use_ssl
    ):
        return _mirror_session_factory

    connect_args: dict[str, object] = {}
    if settings.mirror_database_use_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connect_args["ssl"] = ssl_context
        connect_args["timeout"] = 30

    _mirror_engine = create_async_engine(
        settings.mirror_database_url,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    _mirror_session_factory = async_sessionmaker(
        _mirror_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )
    _mirror_url = settings.mirror_database_url
    _mirror_ssl = settings.mirror_database_use_ssl
    return _mirror_session_factory


@asynccontextmanager
async def get_mirror_session(settings: Settings | None = None) -> AsyncIterator[AsyncSession | None]:
    effective_settings = settings or get_settings()
    session_factory = _get_mirror_session_factory(effective_settings)
    if session_factory is None:
        yield None
        return

    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def _resolve_game_id(session: AsyncSession, tracked_app: TrackedApp) -> int | None:
    id_from_public: int | None = None
    id_from_numeric: int | None = None

    if tracked_app.source_game_public_id:
        result = await session.execute(
            text("select id from games where public_id = :public_id"),
            {"public_id": tracked_app.source_game_public_id},
        )
        row = result.scalar_one_or_none()
        if row is not None:
            id_from_public = int(row)

    if tracked_app.source_game_id is not None:
        result = await session.execute(
            text("select id from games where id = :game_id"),
            {"game_id": tracked_app.source_game_id},
        )
        row = result.scalar_one_or_none()
        if row is not None:
            id_from_numeric = int(row)

    if id_from_public is not None and id_from_numeric is not None and id_from_public != id_from_numeric:
        logger.warning(
            "Main DB mirror skipped for Steam app %s due to mapping mismatch: public_id=%s->%s but source_game_id=%s exists as %s",
            tracked_app.steam_app_id,
            tracked_app.source_game_public_id,
            id_from_public,
            tracked_app.source_game_id,
            id_from_numeric,
        )
        return None

    if id_from_public is not None:
        return id_from_public
    if id_from_numeric is not None:
        return id_from_numeric
    return None


async def _game_has_mirrored_history(session: AsyncSession, game_id: int) -> bool:
    result = await session.execute(
        text(
            """
            select
                exists(select 1 from steam_player_snapshots where game_id = :game_id)
                or exists(select 1 from steam_player_range_snapshots where game_id = :game_id)
            """
        ),
        {"game_id": game_id},
    )
    return bool(result.scalar_one())


def _build_snapshot_rows_payload(rows: list[tuple[datetime, int]]) -> str:
    return json.dumps(
        [
            {
                "sampled_at": sampled_at.isoformat(),
                "concurrent_players": concurrent_players,
            }
            for sampled_at, concurrent_players in rows
        ]
    )


def _build_range_rows_payload(rows: list[tuple[datetime, int, int]]) -> str:
    return json.dumps(
        [
            {
                "sampled_at": sampled_at.isoformat(),
                "players_24h_high": players_24h_high,
                "players_24h_low": players_24h_low,
            }
            for sampled_at, players_24h_high, players_24h_low in rows
        ]
    )


def _build_game_summary_params(
    *,
    game_id: int,
    sampled_at: datetime | None,
    concurrent_players: int | None,
    latest_24h_high: int | None,
    latest_24h_low: int | None,
    all_time_peak_players: int | None,
    all_time_peak_at: datetime | None,
    steam_user_score: object | None = None,
    steam_sample_size: int | None = None,
) -> dict[str, object]:
    # player_sampled_at gates updates to player-count-derived timestamps so we don't
    # bump them when this poll only refreshed the user score.
    player_sampled_at = sampled_at if concurrent_players is not None else None
    return {
        "game_id": game_id,
        "sampled_at": sampled_at,
        "player_sampled_at": player_sampled_at,
        "concurrent_players": concurrent_players,
        "steam_player_24h_peak": latest_24h_high,
        "steam_player_24h_low_observed": latest_24h_low,
        "steam_player_all_time_peak": all_time_peak_players,
        "steam_player_all_time_peak_at": all_time_peak_at,
        "steam_user_score": steam_user_score,
        "steam_sample_size": steam_sample_size,
    }


async def _insert_snapshot_batch(session: AsyncSession, game_id: int, rows: list[tuple[datetime, int]]) -> int:
    if not rows:
        return 0

    result = await session.execute(
        text(
            """
            with incoming as (
                select
                    cast(:game_id as integer) as game_id,
                    payload.sampled_at,
                    payload.concurrent_players
                from jsonb_to_recordset(cast(:rows_json as jsonb)) as payload(
                    sampled_at timestamptz,
                    concurrent_players integer
                )
            ),
            inserted as (
                insert into steam_player_snapshots (game_id, sampled_at, concurrent_players)
                select incoming.game_id, incoming.sampled_at, incoming.concurrent_players
                from incoming
                where not exists (
                    select 1
                    from steam_player_snapshots existing
                    where existing.game_id = incoming.game_id
                      and existing.sampled_at = incoming.sampled_at
                )
                returning 1
            )
            select count(*) from inserted
            """
        ),
        {"game_id": game_id, "rows_json": _build_snapshot_rows_payload(rows)},
    )
    return int(result.scalar_one())


async def _upsert_range_batch(session: AsyncSession, game_id: int, rows: list[tuple[datetime, int, int]]) -> int:
    if not rows:
        return 0

    result = await session.execute(
        text(
            """
            with incoming as (
                select
                    cast(:game_id as integer) as game_id,
                    payload.sampled_at,
                    payload.players_24h_high,
                    payload.players_24h_low
                from jsonb_to_recordset(cast(:rows_json as jsonb)) as payload(
                    sampled_at timestamptz,
                    players_24h_high integer,
                    players_24h_low integer
                )
            ),
            upserted as (
                insert into steam_player_range_snapshots (game_id, sampled_at, players_24h_high, players_24h_low)
                select incoming.game_id, incoming.sampled_at, incoming.players_24h_high, incoming.players_24h_low
                from incoming
                on conflict on constraint uq_steam_player_range_snapshots_game_sampled
                do update set
                    players_24h_high = excluded.players_24h_high,
                    players_24h_low = excluded.players_24h_low
                returning 1
            )
            select count(*) from upserted
            """
        ),
        {"game_id": game_id, "rows_json": _build_range_rows_payload(rows)},
    )
    return int(result.scalar_one())


async def _update_game_summary(session: AsyncSession, params: dict[str, object]) -> None:
    # Coalesce every column so a score-only poll (player count fetch failed) does not
    # blank the player-count fields, and a player-count-only poll does not blank the
    # score fields. Each branch only writes what it actually fetched.
    await session.execute(
        text(
            """
            update games
            set
                steam_current_players = coalesce(:concurrent_players, steam_current_players),
                steam_current_players_sampled_at = coalesce(:player_sampled_at, steam_current_players_sampled_at),
                steam_player_24h_peak = coalesce(:steam_player_24h_peak, steam_player_24h_peak),
                steam_player_24h_low_observed = coalesce(:steam_player_24h_low_observed, steam_player_24h_low_observed),
                steam_player_all_time_peak = coalesce(:steam_player_all_time_peak, steam_player_all_time_peak),
                steam_player_all_time_peak_at = coalesce(:steam_player_all_time_peak_at, steam_player_all_time_peak_at),
                steam_player_stats_synced_at = coalesce(:player_sampled_at, steam_player_stats_synced_at),
                steam_user_score = coalesce(:steam_user_score, steam_user_score),
                steam_sample_size = coalesce(:steam_sample_size, steam_sample_size)
            where id = :game_id
            """
        ),
        params,
    )


async def _upsert_steam_user_score(
    session: AsyncSession,
    *,
    game_id: int,
    steam_user_score: object,
    steam_score_raw: str,
    steam_sample_size: int,
    steam_positive_count: int,
    steam_negative_count: int,
    steam_review_score_desc: str,
    scraped_at: datetime,
) -> None:
    await session.execute(
        text(
            """
            insert into user_scores (
                game_id, source, score, score_raw, sample_size, positive_count, negative_count, review_score_desc, scraped_at
            )
            values (
                :game_id, 'STEAM', :score, :score_raw, :sample_size, :positive_count, :negative_count, :review_score_desc, :scraped_at
            )
            """
        ),
        {
            "game_id": game_id,
            "score": steam_user_score,
            "score_raw": steam_score_raw,
            "sample_size": steam_sample_size,
            "positive_count": steam_positive_count,
            "negative_count": steam_negative_count,
            "review_score_desc": steam_review_score_desc,
            "scraped_at": scraped_at,
        },
    )


async def mirror_poll_to_main_db(
    tracked_app: TrackedApp,
    *,
    sampled_at: datetime,
    concurrent_players: int | None,
    latest_24h_high: int | None,
    latest_24h_low: int | None,
    all_time_peak_players: int | None,
    all_time_peak_at: datetime | None,
    steam_user_score: object | None = None,
    steam_score_raw: str | None = None,
    steam_sample_size: int | None = None,
    steam_positive_count: int | None = None,
    steam_negative_count: int | None = None,
    steam_review_score_desc: str | None = None,
    steam_score_synced_at: datetime | None = None,
    settings: Settings | None = None,
) -> bool:
    effective_settings = settings or get_settings()
    if not mirror_is_configured(effective_settings):
        return False

    if tracked_app.source_game_id is None and not tracked_app.source_game_public_id:
        return False

    async with get_mirror_session(effective_settings) as session:
        if session is None:
            return False

        game_id = await _resolve_game_id(session, tracked_app)
        if game_id is None:
            logger.warning(
                "Main DB mirror skipped for Steam app %s because no matching game row was found",
                tracked_app.steam_app_id,
            )
            return False

        if concurrent_players is not None:
            await session.execute(
                text(
                    """
                    insert into steam_player_snapshots (game_id, sampled_at, concurrent_players)
                    select :game_id, :sampled_at, :concurrent_players
                    where not exists (
                        select 1
                        from steam_player_snapshots
                        where game_id = :game_id and sampled_at = :sampled_at
                    )
                    """
                ),
                {
                    "game_id": game_id,
                    "sampled_at": sampled_at,
                    "concurrent_players": concurrent_players,
                },
            )

        if concurrent_players is not None and latest_24h_high is not None and latest_24h_low is not None:
            await session.execute(
                text(
                    """
                    insert into steam_player_range_snapshots (game_id, sampled_at, players_24h_high, players_24h_low)
                    values (:game_id, :sampled_at, :players_24h_high, :players_24h_low)
                    on conflict on constraint uq_steam_player_range_snapshots_game_sampled
                    do update set
                        players_24h_high = excluded.players_24h_high,
                        players_24h_low = excluded.players_24h_low
                    """
                ),
                {
                    "game_id": game_id,
                    "sampled_at": sampled_at,
                    "players_24h_high": latest_24h_high,
                    "players_24h_low": latest_24h_low,
                },
            )

        await _update_game_summary(
            session,
            _build_game_summary_params(
                game_id=game_id,
                sampled_at=sampled_at,
                concurrent_players=concurrent_players,
                latest_24h_high=latest_24h_high,
                latest_24h_low=latest_24h_low,
                all_time_peak_players=all_time_peak_players,
                all_time_peak_at=all_time_peak_at,
                steam_user_score=steam_user_score,
                steam_sample_size=steam_sample_size,
            ),
        )

        if (
            steam_user_score is not None
            and steam_score_raw
            and steam_sample_size is not None
            and steam_positive_count is not None
            and steam_negative_count is not None
            and steam_review_score_desc
            and steam_score_synced_at is not None
        ):
            await _upsert_steam_user_score(
                session,
                game_id=game_id,
                steam_user_score=steam_user_score,
                steam_score_raw=steam_score_raw,
                steam_sample_size=steam_sample_size,
                steam_positive_count=steam_positive_count,
                steam_negative_count=steam_negative_count,
                steam_review_score_desc=steam_review_score_desc,
                scraped_at=steam_score_synced_at,
            )

    return True


async def backfill_tracked_app_to_main_db(
    scraper_session: AsyncSession,
    tracked_app: TrackedApp,
    *,
    batch_size: int = 1000,
    skip_if_already_mirrored: bool = False,
    settings: Settings | None = None,
) -> MirrorBackfillStats:
    effective_settings = settings or get_settings()
    stats = MirrorBackfillStats()

    if not mirror_is_configured(effective_settings):
        raise RuntimeError("Main DB mirror is not configured")

    if tracked_app.source_game_id is None and not tracked_app.source_game_public_id:
        stats.apps_skipped_unmapped = 1
        return stats

    async with get_mirror_session(effective_settings) as mirror_session:
        if mirror_session is None:
            raise RuntimeError("Main DB mirror is not configured")

        game_id = await _resolve_game_id(mirror_session, tracked_app)
        if game_id is None:
            logger.warning(
                "Backfill skipped for Steam app %s because no matching game row was found in the main DB",
                tracked_app.steam_app_id,
            )
            stats.apps_skipped_unmapped = 1
            return stats

        if skip_if_already_mirrored and await _game_has_mirrored_history(mirror_session, game_id):
            stats.apps_skipped_already_mirrored = 1
            return stats

        raw_cursor: datetime | None = None
        while True:
            raw_query = (
                select(PlayerSample.sampled_at, PlayerSample.concurrent_players)
                .where(PlayerSample.tracked_app_id == tracked_app.id)
                .order_by(PlayerSample.sampled_at.asc())
                .limit(batch_size)
            )
            if raw_cursor is not None:
                raw_query = raw_query.where(PlayerSample.sampled_at > raw_cursor)
            raw_rows = list((await scraper_session.execute(raw_query)).all())
            if not raw_rows:
                break

            typed_raw_rows = [(sampled_at, concurrent_players) for sampled_at, concurrent_players in raw_rows]
            stats.raw_samples_seen += len(typed_raw_rows)
            stats.raw_samples_inserted += await _insert_snapshot_batch(mirror_session, game_id, typed_raw_rows)
            raw_cursor = typed_raw_rows[-1][0]

        range_cursor: datetime | None = None
        while True:
            range_query = (
                select(
                    PlayerActivityHourly.window_ending_at,
                    PlayerActivityHourly.observed_24h_high,
                    PlayerActivityHourly.observed_24h_low,
                )
                .where(PlayerActivityHourly.tracked_app_id == tracked_app.id)
                .order_by(PlayerActivityHourly.window_ending_at.asc())
                .limit(batch_size)
            )
            if range_cursor is not None:
                range_query = range_query.where(PlayerActivityHourly.window_ending_at > range_cursor)
            range_rows = list((await scraper_session.execute(range_query)).all())
            if not range_rows:
                break

            typed_range_rows = [
                (sampled_at, players_24h_high, players_24h_low)
                for sampled_at, players_24h_high, players_24h_low in range_rows
            ]
            stats.range_points_seen += len(typed_range_rows)
            stats.range_points_upserted += await _upsert_range_batch(mirror_session, game_id, typed_range_rows)
            range_cursor = typed_range_rows[-1][0]

        await _update_game_summary(
            mirror_session,
            _build_game_summary_params(
                game_id=game_id,
                sampled_at=tracked_app.last_success_at,
                concurrent_players=tracked_app.last_known_players,
                latest_24h_high=tracked_app.latest_24h_high,
                latest_24h_low=tracked_app.latest_24h_low,
                all_time_peak_players=tracked_app.all_time_peak_players,
                all_time_peak_at=tracked_app.all_time_peak_at,
                steam_user_score=tracked_app.steam_user_score,
                steam_sample_size=tracked_app.steam_sample_size,
            ),
        )

        if (
            tracked_app.steam_user_score is not None
            and tracked_app.steam_score_raw
            and tracked_app.steam_sample_size is not None
            and tracked_app.steam_positive_count is not None
            and tracked_app.steam_negative_count is not None
            and tracked_app.steam_review_score_desc
            and tracked_app.steam_score_synced_at is not None
        ):
            await _upsert_steam_user_score(
                mirror_session,
                game_id=game_id,
                steam_user_score=tracked_app.steam_user_score,
                steam_score_raw=tracked_app.steam_score_raw,
                steam_sample_size=tracked_app.steam_sample_size,
                steam_positive_count=tracked_app.steam_positive_count,
                steam_negative_count=tracked_app.steam_negative_count,
                steam_review_score_desc=tracked_app.steam_review_score_desc,
                scraped_at=tracked_app.steam_score_synced_at,
            )

    stats.apps_processed = 1
    stats.summaries_updated = 1
    return stats
