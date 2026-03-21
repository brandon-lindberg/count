from datetime import datetime, timezone

from app.worker import is_scheduled_job_due


def test_is_scheduled_job_due_when_no_previous_run() -> None:
    assert is_scheduled_job_due(None, 15, datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)) is True


def test_is_scheduled_job_due_when_interval_has_elapsed() -> None:
    now = datetime(2026, 3, 20, 5, 15, tzinfo=timezone.utc)
    last_started_at = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)

    assert is_scheduled_job_due(last_started_at, 15, now) is True


def test_is_scheduled_job_due_when_interval_has_not_elapsed() -> None:
    now = datetime(2026, 3, 20, 5, 14, tzinfo=timezone.utc)
    last_started_at = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)

    assert is_scheduled_job_due(last_started_at, 15, now) is False
