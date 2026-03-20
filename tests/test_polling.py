from datetime import date, datetime, timezone
from types import SimpleNamespace

from app.config import Settings
from app.services.polling import (
    STEAM_APP_NOT_FOUND_PREFIX,
    format_poll_error,
    is_launch_priority_release_date,
    update_all_time_peak,
)
from app.services.steam_provider import SteamAppNotFoundError


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
