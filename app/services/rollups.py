from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import PlayerActivityHourly, PlayerSample, Tier, TrackedApp

SUPPORTED_WINDOWS = {"24h", "48h", "1w", "1m", "3m", "6m", "1y", "max"}


@dataclass(slots=True)
class SamplePoint:
    sampled_at: datetime
    concurrent_players: int


@dataclass(slots=True)
class RollupSnapshot:
    bucket_started_at: datetime
    window_ending_at: datetime
    observed_24h_high: int
    observed_24h_low: int
    latest_players: int
    sample_count: int
    effective_tier_at_capture: Tier


def floor_to_hour(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.replace(minute=0, second=0, microsecond=0)


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    return value.replace(year=year, month=month, day=min(value.day, max_day))


def get_history_window_start(latest_timestamp: datetime, window: str) -> datetime:
    if window not in SUPPORTED_WINDOWS:
        raise ValueError(f"Unsupported history window: {window}")
    if window == "24h":
        return latest_timestamp - timedelta(hours=24)
    if window == "48h":
        return latest_timestamp - timedelta(hours=48)
    if window == "1w":
        return latest_timestamp - timedelta(days=7)
    if window == "1m":
        return add_months(latest_timestamp, -1)
    if window == "3m":
        return add_months(latest_timestamp, -3)
    if window == "6m":
        return add_months(latest_timestamp, -6)
    return add_months(latest_timestamp, -12)


def build_rollup_snapshot(
    samples: list[SamplePoint],
    window_ending_at: datetime,
    effective_tier: Tier,
) -> RollupSnapshot | None:
    if not samples:
        return None
    latest_sample = max(samples, key=lambda sample: sample.sampled_at)
    return RollupSnapshot(
        bucket_started_at=floor_to_hour(window_ending_at),
        window_ending_at=window_ending_at,
        observed_24h_high=max(sample.concurrent_players for sample in samples),
        observed_24h_low=min(sample.concurrent_players for sample in samples),
        latest_players=latest_sample.concurrent_players,
        sample_count=len(samples),
        effective_tier_at_capture=effective_tier,
    )


async def upsert_hourly_rollup(
    session: AsyncSession,
    tracked_app: TrackedApp,
    window_ending_at: datetime | None = None,
) -> RollupSnapshot | None:
    effective_end = (window_ending_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    window_start = effective_end - timedelta(hours=24)
    result = await session.execute(
        select(PlayerSample.sampled_at, PlayerSample.concurrent_players)
        .where(
            PlayerSample.tracked_app_id == tracked_app.id,
            PlayerSample.sampled_at > window_start,
            PlayerSample.sampled_at <= effective_end,
        )
        .order_by(PlayerSample.sampled_at.asc())
    )
    samples = [SamplePoint(sampled_at=row.sampled_at, concurrent_players=row.concurrent_players) for row in result]
    snapshot = build_rollup_snapshot(samples, effective_end, tracked_app.effective_tier)
    if snapshot is None:
        return None

    await session.execute(
        pg_insert(PlayerActivityHourly)
        .values(
            tracked_app_id=tracked_app.id,
            steam_app_id=tracked_app.steam_app_id,
            bucket_started_at=snapshot.bucket_started_at,
            window_ending_at=snapshot.window_ending_at,
            observed_24h_high=snapshot.observed_24h_high,
            observed_24h_low=snapshot.observed_24h_low,
            latest_players=snapshot.latest_players,
            sample_count=snapshot.sample_count,
            effective_tier_at_capture=snapshot.effective_tier_at_capture,
        )
        .on_conflict_do_update(
            index_elements=[PlayerActivityHourly.steam_app_id, PlayerActivityHourly.bucket_started_at],
            set_={
                "tracked_app_id": tracked_app.id,
                "window_ending_at": snapshot.window_ending_at,
                "observed_24h_high": snapshot.observed_24h_high,
                "observed_24h_low": snapshot.observed_24h_low,
                "latest_players": snapshot.latest_players,
                "sample_count": snapshot.sample_count,
                "effective_tier_at_capture": snapshot.effective_tier_at_capture,
            },
        )
    )

    tracked_app.latest_24h_high = snapshot.observed_24h_high
    tracked_app.latest_24h_low = snapshot.observed_24h_low
    return snapshot
