from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.services.svg import build_history_svg


def test_build_history_svg_includes_date_ticks_for_longer_windows() -> None:
    start = datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)
    points = [
        SimpleNamespace(
            window_ending_at=start + timedelta(hours=index * 24),
            observed_24h_high=5000 + index * 400,
            observed_24h_low=3200 + index * 300,
            latest_players=4100 + index * 250,
        )
        for index in range(8)
    ]

    svg = build_history_svg(points, window="1w")

    assert "Viewing 1 week of hourly history" in svg
    assert "Mar" in svg
    assert "UTC" not in svg
    assert 'aria-label="current player count history chart"' in svg


def test_build_history_svg_keeps_time_labels_for_48_hour_window() -> None:
    start = datetime(2026, 3, 18, 0, 0, tzinfo=timezone.utc)
    points = [
        SimpleNamespace(
            window_ending_at=start + timedelta(hours=index * 12),
            observed_24h_high=5000 + index * 400,
            observed_24h_low=3200 + index * 300,
            latest_players=4100 + index * 250,
        )
        for index in range(5)
    ]

    svg = build_history_svg(points, window="48h")

    assert "UTC" in svg


def test_build_history_svg_handles_sparse_history() -> None:
    point = SimpleNamespace(
        window_ending_at=datetime(2026, 3, 19, 0, 0, tzinfo=timezone.utc),
        observed_24h_high=100,
        observed_24h_low=50,
        latest_players=75,
    )

    svg = build_history_svg([point], window="24h")

    assert "Not enough hourly history yet." in svg
