from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from app.backfill_main_db import BackfillProgressUpdate, load_backfill_targets, run_backfill_targets
from app.config import Settings, get_settings
from app.database import SessionLocal
from app.services.job_runs import complete_job_run, start_job_run
from app.services.main_db_mirror import MirrorBackfillStats, mirror_is_configured


def _normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


@dataclass(slots=True)
class DashboardBackfillStatus:
    state: str = "idle"
    running: bool = False
    started_at: datetime | None = None
    finished_at: datetime | None = None
    total_apps: int = 0
    processed_apps: int = 0
    current_steam_app_id: int | None = None
    current_title: str = ""
    current_message: str = "No backfill run started."
    last_error: str = ""
    only_unmirrored: bool = True
    batch_size: int = 1000
    apps_processed: int = 0
    apps_skipped_unmapped: int = 0
    apps_skipped_already_mirrored: int = 0
    apps_failed: int = 0
    raw_samples_seen: int = 0
    raw_samples_inserted: int = 0
    range_points_seen: int = 0
    range_points_upserted: int = 0
    summaries_updated: int = 0

    def apply_aggregate(self, aggregate: MirrorBackfillStats) -> None:
        self.apps_processed = aggregate.apps_processed
        self.apps_skipped_unmapped = aggregate.apps_skipped_unmapped
        self.apps_skipped_already_mirrored = aggregate.apps_skipped_already_mirrored
        self.apps_failed = aggregate.apps_failed
        self.raw_samples_seen = aggregate.raw_samples_seen
        self.raw_samples_inserted = aggregate.raw_samples_inserted
        self.range_points_seen = aggregate.range_points_seen
        self.range_points_upserted = aggregate.range_points_upserted
        self.summaries_updated = aggregate.summaries_updated

    def snapshot(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        elapsed_seconds: float | None = None
        if self.started_at is not None:
            ended_at = now if self.running else (self.finished_at or now)
            elapsed_seconds = max(0.0, (ended_at - self.started_at).total_seconds())

        eta_seconds: float | None = None
        if self.running and self.processed_apps > 0 and self.total_apps > self.processed_apps and elapsed_seconds is not None:
            avg_per_app = elapsed_seconds / self.processed_apps
            eta_seconds = avg_per_app * (self.total_apps - self.processed_apps)

        percent_complete = (self.processed_apps / self.total_apps * 100.0) if self.total_apps > 0 else 0.0
        started_at = _normalize_timestamp(self.started_at)
        finished_at = _normalize_timestamp(self.finished_at)
        payload = asdict(self)
        payload.update(
            {
                "percent_complete": percent_complete,
                "elapsed_seconds": elapsed_seconds,
                "eta_seconds": eta_seconds,
                "elapsed_label": _format_duration(elapsed_seconds),
                "eta_label": _format_duration(eta_seconds),
                "started_at_iso": started_at.isoformat(timespec="seconds") if started_at else "",
                "finished_at_iso": finished_at.isoformat(timespec="seconds") if finished_at else "",
                "started_at_label": started_at.strftime("%Y-%m-%d %H:%M UTC") if started_at else "-",
                "finished_at_label": finished_at.strftime("%Y-%m-%d %H:%M UTC") if finished_at else "-",
            }
        )
        return payload


_status = DashboardBackfillStatus()
_task: asyncio.Task[None] | None = None
_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


def get_dashboard_backfill_status() -> dict[str, Any]:
    return _status.snapshot()


async def _run_dashboard_backfill(*, batch_size: int, only_unmirrored: bool, settings: Settings) -> None:
    job_id: int | None = None
    try:
        async with SessionLocal() as session:
            job = await start_job_run(session, "backfill_main_db_all")
            await session.commit()
            job_id = job.id

        async with SessionLocal() as scraper_session:
            tracked_apps = await load_backfill_targets(scraper_session)
            _status.total_apps = len(tracked_apps)
            _status.current_message = (
                f"Backfill queued for {len(tracked_apps):,} tracked app{'' if len(tracked_apps) == 1 else 's'}."
                if tracked_apps
                else "No tracked apps with stored samples matched the backfill scope."
            )

            async def _on_progress(update: BackfillProgressUpdate) -> None:
                _status.processed_apps = update.index
                _status.current_steam_app_id = update.tracked_app.steam_app_id
                _status.current_title = update.tracked_app.title
                _status.apply_aggregate(update.aggregate)

                if update.error is not None:
                    _status.last_error = update.error
                    _status.current_message = (
                        f"Failed on Steam app {update.tracked_app.steam_app_id} ({update.tracked_app.title})."
                    )
                    return

                if update.app_stats is None:
                    return

                if update.app_stats.apps_skipped_unmapped:
                    _status.current_message = (
                        f"Skipped Steam app {update.tracked_app.steam_app_id} ({update.tracked_app.title})"
                        " because no matching main DB game row was found."
                    )
                    return

                if update.app_stats.apps_skipped_already_mirrored:
                    _status.current_message = (
                        f"Skipped Steam app {update.tracked_app.steam_app_id} ({update.tracked_app.title})"
                        " because mirrored history already exists."
                    )
                    return

                _status.current_message = (
                    f"Mirrored Steam app {update.tracked_app.steam_app_id} ({update.tracked_app.title}): "
                    f"{update.app_stats.raw_samples_inserted:,} raw inserted, "
                    f"{update.app_stats.range_points_upserted:,} range upserted."
                )

            aggregate = await run_backfill_targets(
                scraper_session,
                tracked_apps,
                batch_size=batch_size,
                only_unmirrored=only_unmirrored,
                progress_callback=_on_progress,
            )

        _status.running = False
        _status.state = "completed"
        _status.finished_at = datetime.now(timezone.utc)
        _status.apply_aggregate(aggregate)
        _status.current_message = (
            f"Backfill completed. Mirrored {aggregate.apps_processed:,} apps, "
            f"skipped {aggregate.apps_skipped_unmapped + aggregate.apps_skipped_already_mirrored:,}, "
            f"failed {aggregate.apps_failed:,}."
        )

        async with SessionLocal() as session:
            existing_job = await session.get(type(job), job_id) if job_id is not None else None
            if existing_job is not None:
                await complete_job_run(
                    session,
                    existing_job,
                    processed_count=aggregate.raw_samples_seen + aggregate.range_points_seen,
                    success_count=aggregate.raw_samples_inserted + aggregate.range_points_upserted,
                    failure_count=aggregate.apps_failed,
                    error_summary=_status.last_error[:2000] if _status.last_error else None,
                )
                await session.commit()
    except Exception as exc:
        _status.running = False
        _status.state = "failed"
        _status.finished_at = datetime.now(timezone.utc)
        _status.last_error = str(exc)
        _status.current_message = f"Backfill failed: {str(exc)[:240]}"

        async with SessionLocal() as session:
            existing_job = await session.get(type(job), job_id) if job_id is not None else None
            if existing_job is not None:
                await complete_job_run(
                    session,
                    existing_job,
                    processed_count=_status.raw_samples_seen + _status.range_points_seen,
                    success_count=_status.raw_samples_inserted + _status.range_points_upserted,
                    failure_count=max(1, _status.apps_failed),
                    error_summary=_status.last_error[:2000],
                )
                await session.commit()
    finally:
        global _task
        _task = None


async def start_dashboard_backfill(*, batch_size: int = 1000, only_unmirrored: bool = True, settings: Settings | None = None) -> bool:
    global _task
    effective_settings = settings or get_settings()
    if not mirror_is_configured(effective_settings):
        raise RuntimeError("Main DB mirror is not configured")

    lock = _get_lock()
    async with lock:
        if _task is not None and not _task.done():
            return False

        _status.state = "running"
        _status.running = True
        _status.started_at = datetime.now(timezone.utc)
        _status.finished_at = None
        _status.total_apps = 0
        _status.processed_apps = 0
        _status.current_steam_app_id = None
        _status.current_title = ""
        _status.current_message = "Preparing full main DB backfill..."
        _status.last_error = ""
        _status.only_unmirrored = only_unmirrored
        _status.batch_size = batch_size
        _status.apply_aggregate(MirrorBackfillStats())

        _task = asyncio.create_task(
            _run_dashboard_backfill(batch_size=batch_size, only_unmirrored=only_unmirrored, settings=effective_settings)
        )
        return True
