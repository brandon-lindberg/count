from types import SimpleNamespace

from app.models import Tier
from app.services.tiering import compute_auto_tier, poll_interval_minutes, resolve_effective_tier


def _settings() -> SimpleNamespace:
    return SimpleNamespace(hot_threshold=10000, warm_threshold=500, hot_poll_minutes=10, warm_poll_minutes=60, cold_poll_minutes=360)


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
