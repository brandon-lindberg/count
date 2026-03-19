from datetime import datetime, timezone

from app.models import Tier
from app.services.rollups import SamplePoint, build_rollup_snapshot, get_history_window_start


def test_build_rollup_snapshot_uses_high_low_latest() -> None:
    samples = [
        SamplePoint(sampled_at=datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc), concurrent_players=120),
        SamplePoint(sampled_at=datetime(2026, 3, 19, 9, 10, tzinfo=timezone.utc), concurrent_players=450),
        SamplePoint(sampled_at=datetime(2026, 3, 19, 9, 20, tzinfo=timezone.utc), concurrent_players=180),
    ]
    snapshot = build_rollup_snapshot(samples, datetime(2026, 3, 19, 9, 30, tzinfo=timezone.utc), Tier.warm)
    assert snapshot is not None
    assert snapshot.observed_24h_high == 450
    assert snapshot.observed_24h_low == 120
    assert snapshot.latest_players == 180
    assert snapshot.sample_count == 3


def test_history_window_start_supports_hourly_and_monthly_windows() -> None:
    latest = datetime(2026, 3, 19, 9, 30, tzinfo=timezone.utc)
    assert get_history_window_start(latest, "24h").hour == 9
    assert get_history_window_start(latest, "48h").day == 17
    assert get_history_window_start(latest, "1m").month == 2
