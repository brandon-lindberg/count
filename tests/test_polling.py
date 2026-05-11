from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.models import Tier
from app.services.polling import (
    STEAM_APP_NOT_FOUND_PREFIX,
    format_poll_error,
    is_launch_priority_release_date,
    poll_app,
    tier_poll_batch_limit,
    update_all_time_peak,
)
from app.services.steam_provider import SteamAppNotFoundError, SteamProviderError, SteamUserScore


def test_format_poll_error_marks_not_found_errors() -> None:
    message = format_poll_error(SteamAppNotFoundError("Steam current players endpoint returned 404 for app 17933"))

    assert message.startswith(STEAM_APP_NOT_FOUND_PREFIX)


def test_format_poll_error_preserves_other_errors() -> None:
    assert format_poll_error(RuntimeError("boom")) == "boom"


def test_update_all_time_peak_sets_initial_peak() -> None:
    tracked_app = SimpleNamespace(all_time_peak_players=None, all_time_peak_at=None)
    sampled_at = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)

    update_all_time_peak(tracked_app, 1200, sampled_at)

    assert tracked_app.all_time_peak_players == 1200
    assert tracked_app.all_time_peak_at == sampled_at


def test_update_all_time_peak_only_updates_when_higher() -> None:
    original_time = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
    tracked_app = SimpleNamespace(all_time_peak_players=1200, all_time_peak_at=original_time)

    update_all_time_peak(tracked_app, 1100, datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc))

    assert tracked_app.all_time_peak_players == 1200
    assert tracked_app.all_time_peak_at == original_time


def test_is_launch_priority_release_date_handles_upcoming_and_recent_titles() -> None:
    settings = Settings()
    now = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)

    assert is_launch_priority_release_date(date(2026, 3, 19), settings, now) is True
    assert is_launch_priority_release_date(date(2026, 3, 21), settings, now) is True
    assert is_launch_priority_release_date(date(2026, 3, 16), settings, now) is True
    assert is_launch_priority_release_date(date(2026, 3, 12), settings, now) is False
    assert is_launch_priority_release_date(None, settings, now) is False


def test_tier_poll_batch_limit_uses_hot_and_warm_specific_limits() -> None:
    settings = Settings(
        hot_poll_batch_limit=250,
        warm_poll_batch_limit=2500,
        poll_batch_limit=100,
    )

    assert tier_poll_batch_limit(Tier.hot, settings) == 250
    assert tier_poll_batch_limit(Tier.warm, settings) == 2500
    assert tier_poll_batch_limit(Tier.cold, settings) == 0


@pytest.mark.asyncio
async def test_poll_app_fetches_steam_user_score_in_same_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    sampled_at = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
    score = SteamUserScore(
        score=Decimal("91.5"),
        score_raw="915/1000",
        sample_size=1000,
        positive_count=915,
        negative_count=85,
        review_score_desc="Very Positive",
        scraped_at=sampled_at,
    )
    tracked_app = SimpleNamespace(
        id=1,
        steam_app_id=730,
        last_known_players=None,
        last_polled_at=None,
        last_success_at=None,
        last_error=None,
        all_time_peak_players=None,
        all_time_peak_at=None,
        latest_24h_high=None,
        latest_24h_low=None,
        steam_user_score=None,
        steam_score_raw=None,
        steam_sample_size=None,
        steam_positive_count=None,
        steam_negative_count=None,
        steam_review_score_desc=None,
        steam_score_synced_at=None,
        effective_tier=Tier.hot,
        manual_tier_override=None,
        source_release_date=None,
    )
    provider = SimpleNamespace(
        get_current_players=AsyncMock(return_value=12345),
        get_user_score=AsyncMock(return_value=score),
    )
    session = SimpleNamespace(
        execute=AsyncMock(),
        flush=AsyncMock(),
        commit=AsyncMock(),
        rollback=AsyncMock(),
        get=AsyncMock(return_value=tracked_app),
    )

    async def fake_ensure_partition_for_timestamp(session_arg, sampled_at_arg) -> None:
        return None

    async def fake_upsert_player_sample(session_arg, tracked_app_arg, sampled_at_arg, concurrent_players_arg) -> None:
        return None

    async def fake_refresh_effective_tier(session_arg, tracked_app_arg, settings_arg, now_arg) -> Tier:
        return tracked_app_arg.effective_tier

    async def fake_upsert_hourly_rollup(session_arg, tracked_app_arg, sampled_at_arg) -> None:
        return None

    async def fake_mirror_poll_to_main_db(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr("app.services.polling.ensure_partition_for_timestamp", fake_ensure_partition_for_timestamp)
    monkeypatch.setattr("app.services.polling.upsert_player_sample", fake_upsert_player_sample)
    monkeypatch.setattr("app.services.polling.refresh_effective_tier", fake_refresh_effective_tier)
    monkeypatch.setattr("app.services.polling.upsert_hourly_rollup", fake_upsert_hourly_rollup)
    monkeypatch.setattr("app.services.polling.mirror_poll_to_main_db", fake_mirror_poll_to_main_db)

    result = await poll_app(session, tracked_app, provider, Settings(), sampled_at)

    assert result.concurrent_players == 12345
    assert tracked_app.steam_user_score == Decimal("91.5")
    assert tracked_app.steam_score_raw == "915/1000"
    assert tracked_app.steam_sample_size == 1000
    assert tracked_app.steam_positive_count == 915
    assert tracked_app.steam_negative_count == 85
    assert tracked_app.steam_review_score_desc == "Very Positive"
    assert tracked_app.steam_score_synced_at == sampled_at
    provider.get_current_players.assert_awaited_once_with(730)
    provider.get_user_score.assert_awaited_once_with(730)


@pytest.mark.asyncio
async def test_poll_app_persists_user_score_when_player_count_fetch_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    sampled_at = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
    score = SteamUserScore(
        score=Decimal("88.0"),
        score_raw="880/1000",
        sample_size=1000,
        positive_count=880,
        negative_count=120,
        review_score_desc="Very Positive",
        scraped_at=sampled_at,
    )
    tracked_app = SimpleNamespace(
        id=2,
        steam_app_id=2701720,
        last_known_players=None,
        last_polled_at=None,
        last_success_at=None,
        last_error=None,
        all_time_peak_players=None,
        all_time_peak_at=None,
        latest_24h_high=None,
        latest_24h_low=None,
        steam_user_score=None,
        steam_score_raw=None,
        steam_sample_size=None,
        steam_positive_count=None,
        steam_negative_count=None,
        steam_review_score_desc=None,
        steam_score_synced_at=None,
        effective_tier=Tier.warm,
        manual_tier_override=None,
        source_release_date=None,
    )
    provider = SimpleNamespace(
        get_current_players=AsyncMock(side_effect=SteamProviderError("missing player_count")),
        get_user_score=AsyncMock(return_value=score),
    )
    session = SimpleNamespace(
        execute=AsyncMock(),
        flush=AsyncMock(),
        commit=AsyncMock(),
        rollback=AsyncMock(),
        get=AsyncMock(return_value=tracked_app),
    )

    monkeypatch.setattr("app.services.polling.ensure_partition_for_timestamp", AsyncMock())
    monkeypatch.setattr("app.services.polling.upsert_player_sample", AsyncMock())
    monkeypatch.setattr("app.services.polling.refresh_effective_tier", AsyncMock(return_value=Tier.warm))
    monkeypatch.setattr("app.services.polling.upsert_hourly_rollup", AsyncMock())
    monkeypatch.setattr("app.services.polling.mirror_poll_to_main_db", AsyncMock())

    result = await poll_app(session, tracked_app, provider, Settings(), sampled_at)

    assert tracked_app.steam_user_score == Decimal("88.0")
    assert tracked_app.steam_sample_size == 1000
    assert tracked_app.last_success_at == sampled_at
    assert tracked_app.last_error is None
    assert result.concurrent_players == 0
    session.commit.assert_awaited()


@pytest.mark.asyncio
async def test_poll_app_does_not_rollback_local_state_when_mirror_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    sampled_at = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
    score = SteamUserScore(
        score=Decimal("92.0"),
        score_raw="920/1000",
        sample_size=1000,
        positive_count=920,
        negative_count=80,
        review_score_desc="Very Positive",
        scraped_at=sampled_at,
    )
    tracked_app = SimpleNamespace(
        id=3,
        steam_app_id=12345,
        last_known_players=None,
        last_polled_at=None,
        last_success_at=None,
        last_error=None,
        all_time_peak_players=None,
        all_time_peak_at=None,
        latest_24h_high=None,
        latest_24h_low=None,
        steam_user_score=None,
        steam_score_raw=None,
        steam_sample_size=None,
        steam_positive_count=None,
        steam_negative_count=None,
        steam_review_score_desc=None,
        steam_score_synced_at=None,
        effective_tier=Tier.hot,
        manual_tier_override=None,
        source_release_date=None,
    )
    provider = SimpleNamespace(
        get_current_players=AsyncMock(return_value=500),
        get_user_score=AsyncMock(return_value=score),
    )
    session = SimpleNamespace(
        execute=AsyncMock(),
        flush=AsyncMock(),
        commit=AsyncMock(),
        rollback=AsyncMock(),
        get=AsyncMock(return_value=tracked_app),
    )

    monkeypatch.setattr("app.services.polling.ensure_partition_for_timestamp", AsyncMock())
    monkeypatch.setattr("app.services.polling.upsert_player_sample", AsyncMock())
    monkeypatch.setattr("app.services.polling.refresh_effective_tier", AsyncMock(return_value=Tier.hot))
    monkeypatch.setattr("app.services.polling.upsert_hourly_rollup", AsyncMock())
    monkeypatch.setattr(
        "app.services.polling.mirror_poll_to_main_db",
        AsyncMock(side_effect=RuntimeError("mirror exploded")),
    )

    settings = Settings(require_mirror_success=True)
    # Should NOT raise even when mirror fails and require_mirror_success is True.
    result = await poll_app(session, tracked_app, provider, settings, sampled_at)

    assert result.concurrent_players == 500
    assert tracked_app.steam_user_score == Decimal("92.0")
    assert tracked_app.steam_sample_size == 1000
    # Local commit happened before mirror was attempted.
    session.commit.assert_awaited()
    session.rollback.assert_not_awaited()
