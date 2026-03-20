from datetime import date, datetime, timezone
from types import SimpleNamespace

from app.models import Tier
from app.services.tiering import compute_auto_tier, is_release_hot_window, poll_interval_minutes, resolve_effective_tier


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        hot_threshold=10000,
        warm_threshold=500,
        hot_poll_minutes=10,
        warm_poll_minutes=60,
        cold_poll_minutes=360,
        release_hot_days=3,
    )


def test_compute_auto_tier_defaults_to_warm_without_history() -> None:
    assert compute_auto_tier(None, _settings()) == Tier.warm


def test_compute_auto_tier_uses_thresholds() -> None:
    settings = _settings()
    assert compute_auto_tier(15000, settings) == Tier.hot
    assert compute_auto_tier(999, settings) == Tier.warm
    assert compute_auto_tier(120, settings) == Tier.cold


def test_manual_override_wins() -> None:
    assert resolve_effective_tier(Tier.hot, Tier.cold) == Tier.hot


def test_poll_interval_minutes() -> None:
    settings = _settings()
    assert poll_interval_minutes(Tier.hot, settings) == 10
    assert poll_interval_minutes(Tier.warm, settings) == 60
    assert poll_interval_minutes(Tier.cold, settings) == 360


def test_is_release_hot_window_only_for_first_three_days_after_release() -> None:
    settings = _settings()
    now = datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)

    assert is_release_hot_window(date(2026, 3, 19), settings, now) is True
    assert is_release_hot_window(date(2026, 3, 18), settings, now) is True
    assert is_release_hot_window(date(2026, 3, 17), settings, now) is True
    assert is_release_hot_window(date(2026, 3, 16), settings, now) is False
    assert is_release_hot_window(date(2026, 3, 20), settings, now) is False
