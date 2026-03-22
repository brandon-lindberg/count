from datetime import datetime, timedelta, timezone

from app.services.backfill_runtime import DashboardBackfillStatus
from app.services.main_db_mirror import MirrorBackfillStats


def test_dashboard_backfill_status_snapshot_reports_progress_fields() -> None:
    started_at = datetime.now(timezone.utc) - timedelta(seconds=30)
    status = DashboardBackfillStatus(
        state="running",
        running=True,
        started_at=started_at,
        total_apps=100,
        processed_apps=25,
        current_steam_app_id=570,
        current_title="Dota 2",
        current_message="Mirroring current app.",
        only_unmirrored=True,
    )
    status.apply_aggregate(
        MirrorBackfillStats(
            apps_processed=18,
            apps_skipped_unmapped=2,
            apps_skipped_already_mirrored=3,
            apps_failed=1,
            raw_samples_inserted=1000,
            range_points_upserted=250,
        )
    )

    snapshot = status.snapshot()

    assert snapshot["running"] is True
    assert snapshot["percent_complete"] == 25.0
    assert snapshot["apps_processed"] == 18
    assert snapshot["apps_skipped_already_mirrored"] == 3
    assert snapshot["current_title"] == "Dota 2"
    assert snapshot["elapsed_label"] != "-"
    assert snapshot["started_at_iso"].endswith("+00:00")
    assert snapshot["finished_at_iso"] == ""
