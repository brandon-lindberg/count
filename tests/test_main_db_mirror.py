from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.services.main_db_mirror import (
    MirrorBackfillStats,
    _build_game_summary_params,
    _build_range_rows_payload,
    _build_snapshot_rows_payload,
    _update_game_summary,
)


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_build_snapshot_rows_payload_serializes_timestamps_and_counts() -> None:
    payload = _build_snapshot_rows_payload(
        [
            (_utc(2026, 3, 19, 10, 4), 12345),
            (_utc(2026, 3, 19, 10, 14), 12456),
        ]
    )

    assert '"sampled_at": "2026-03-19T10:04:00+00:00"' in payload
    assert '"concurrent_players": 12345' in payload
    assert '"concurrent_players": 12456' in payload


def test_build_range_rows_payload_serializes_hourly_ranges() -> None:
    payload = _build_range_rows_payload(
        [
            (_utc(2026, 3, 19, 10, 0), 20000, 15000),
        ]
    )

    assert '"sampled_at": "2026-03-19T10:00:00+00:00"' in payload
    assert '"players_24h_high": 20000' in payload
    assert '"players_24h_low": 15000' in payload


def test_build_game_summary_params_maps_fields_for_main_db_update() -> None:
    params = _build_game_summary_params(
        game_id=42,
        sampled_at=_utc(2026, 3, 19, 10, 4),
        concurrent_players=308,
        latest_24h_high=641,
        latest_24h_low=287,
        all_time_peak_players=88337,
        all_time_peak_at=_utc(2026, 3, 6, 9, 0),
    )

    assert params["game_id"] == 42
    assert params["concurrent_players"] == 308
    assert params["steam_player_24h_peak"] == 641
    assert params["steam_player_24h_low_observed"] == 287
    assert params["steam_player_all_time_peak"] == 88337
    assert params["steam_player_all_time_peak_at"] == _utc(2026, 3, 6, 9, 0)


@pytest.mark.asyncio
async def test_update_game_summary_preserves_score_fields_when_score_payload_is_absent() -> None:
    session = AsyncMock()
    params = _build_game_summary_params(
        game_id=42,
        sampled_at=_utc(2026, 3, 19, 10, 4),
        concurrent_players=308,
        latest_24h_high=641,
        latest_24h_low=287,
        all_time_peak_players=88337,
        all_time_peak_at=_utc(2026, 3, 6, 9, 0),
    )

    await _update_game_summary(session, params)

    statement = str(session.execute.await_args.args[0])
    assert "steam_user_score = coalesce(:steam_user_score, steam_user_score)" in statement
    assert "steam_sample_size = coalesce(:steam_sample_size, steam_sample_size)" in statement
    # Player-count columns also coalesce now so a score-only poll (no player count)
    # cannot blank them out.
    assert "steam_current_players = coalesce(:concurrent_players, steam_current_players)" in statement
    assert "steam_player_24h_peak = coalesce(:steam_player_24h_peak, steam_player_24h_peak)" in statement


def test_build_game_summary_params_drops_player_timestamp_when_player_count_missing() -> None:
    params = _build_game_summary_params(
        game_id=42,
        sampled_at=_utc(2026, 3, 19, 10, 4),
        concurrent_players=None,
        latest_24h_high=None,
        latest_24h_low=None,
        all_time_peak_players=None,
        all_time_peak_at=None,
        steam_user_score=95,
        steam_sample_size=1652,
    )

    assert params["player_sampled_at"] is None
    assert params["sampled_at"] == _utc(2026, 3, 19, 10, 4)
    assert params["steam_user_score"] == 95
    assert params["steam_sample_size"] == 1652


def test_mirror_backfill_stats_merge_accumulates_counts() -> None:
    left = MirrorBackfillStats(apps_processed=1, raw_samples_inserted=2, summaries_updated=1)
    right = MirrorBackfillStats(
        apps_processed=3,
        apps_skipped_already_mirrored=2,
        apps_failed=1,
        raw_samples_inserted=5,
        range_points_upserted=8,
    )

    left.merge(right)

    assert left.apps_processed == 4
    assert left.apps_skipped_already_mirrored == 2
    assert left.apps_failed == 1
    assert left.raw_samples_inserted == 7
    assert left.range_points_upserted == 8
    assert left.summaries_updated == 1
