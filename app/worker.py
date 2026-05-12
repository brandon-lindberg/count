from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from app.config import get_settings
from app.database import SessionLocal
from app.models import JobRun, Tier, TrackedApp
from app.services.job_runs import complete_job_run, start_job_run
from app.services.partitions import ensure_future_partitions
from app.services.polling import (
    PollAppMode,
    get_due_bootstrap_steam_app_ids,
    get_due_launch_watch_steam_app_ids,
    get_due_steam_app_ids,
    get_due_user_score_steam_app_ids,
    poll_app,
)
from app.services.registry_importer import import_registry
from app.services.steam_provider import SteamAppNotFoundError, SteamCurrentPlayersProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()
RUNTIME_BUDGET_EXHAUSTED_ERROR = "runtime_budget_exhausted"

# Pipelines for `run_scheduled_jobs --pipeline` (separate GitHub Actions jobs, no shared queue).
PIPELINE_ALL = "all"
PIPELINE_MAINTENANCE = "maintenance"
PIPELINE_HOT = "hot"
PIPELINE_WARM = "warm"
PIPELINE_SCORES = "scores"
PIPELINE_CHOICES: tuple[str, ...] = (
    PIPELINE_ALL,
    PIPELINE_MAINTENANCE,
    PIPELINE_HOT,
    PIPELINE_WARM,
    PIPELINE_SCORES,
)
_tier_poll_queue: asyncio.Queue[Tier] | None = None
_tier_poll_worker_task: asyncio.Task[None] | None = None
_tier_poll_state_lock: asyncio.Lock | None = None
_queued_tier_polls: set[Tier] = set()
_worker_cycle_deadline_monotonic: float | None = None


@dataclass(slots=True)
class ScheduledJobDefinition:
    job_name: str
    interval_minutes: int
    runner: Callable[[], Awaitable[object]]


def start_worker_runtime_budget(now_monotonic: float | None = None) -> None:
    global _worker_cycle_deadline_monotonic
    if settings.worker_cycle_max_seconds is None:
        _worker_cycle_deadline_monotonic = None
        return
    _worker_cycle_deadline_monotonic = (now_monotonic if now_monotonic is not None else time.monotonic()) + settings.worker_cycle_max_seconds


def clear_worker_runtime_budget() -> None:
    global _worker_cycle_deadline_monotonic
    _worker_cycle_deadline_monotonic = None


def worker_runtime_budget_remaining_seconds(now_monotonic: float | None = None) -> float | None:
    if _worker_cycle_deadline_monotonic is None:
        return None
    return _worker_cycle_deadline_monotonic - (now_monotonic if now_monotonic is not None else time.monotonic())


def has_worker_runtime_budget(
    *,
    min_remaining_seconds: int | None = None,
    now_monotonic: float | None = None,
) -> bool:
    remaining = worker_runtime_budget_remaining_seconds(now_monotonic=now_monotonic)
    if remaining is None:
        return True
    minimum = settings.worker_cycle_shutdown_grace_seconds if min_remaining_seconds is None else min_remaining_seconds
    return remaining > minimum


def _get_tier_poll_queue() -> asyncio.Queue[Tier]:
    global _tier_poll_queue
    if _tier_poll_queue is None:
        _tier_poll_queue = asyncio.Queue()
    return _tier_poll_queue


def _get_tier_poll_state_lock() -> asyncio.Lock:
    global _tier_poll_state_lock
    if _tier_poll_state_lock is None:
        _tier_poll_state_lock = asyncio.Lock()
    return _tier_poll_state_lock


async def _run_tier_poll_queue() -> None:
    global _tier_poll_worker_task

    queue = _get_tier_poll_queue()
    current_task = asyncio.current_task()

    try:
        while True:
            try:
                tier = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            async with _get_tier_poll_state_lock():
                _queued_tier_polls.discard(tier)

            logger.info("Running queued tier poll %s", tier.value)
            try:
                await poll_tier_job(tier, mode=PollAppMode.players_only)
            except Exception:
                logger.exception("Queued tier poll crashed for %s", tier.value)
            finally:
                queue.task_done()
    finally:
        async with _get_tier_poll_state_lock():
            if _tier_poll_worker_task is current_task:
                _tier_poll_worker_task = None
            if not queue.empty() and _tier_poll_worker_task is None:
                _tier_poll_worker_task = asyncio.create_task(_run_tier_poll_queue())


async def queue_tier_poll_job(tier: Tier) -> bool:
    global _tier_poll_worker_task

    if tier == Tier.cold:
        logger.info("Cold polling is disabled; skipping queued tier poll")
        return False

    queue = _get_tier_poll_queue()
    async with _get_tier_poll_state_lock():
        if tier in _queued_tier_polls:
            logger.info("Tier poll %s already queued; skipping duplicate enqueue", tier.value)
            return False

        queue.put_nowait(tier)
        _queued_tier_polls.add(tier)
        if _tier_poll_worker_task is None or _tier_poll_worker_task.done():
            _tier_poll_worker_task = asyncio.create_task(_run_tier_poll_queue())

    logger.info("Queued tier poll %s", tier.value)
    return True


async def wait_for_tier_poll_queue_idle() -> None:
    while True:
        task = _tier_poll_worker_task
        if task is None:
            return
        await task


async def reset_tier_poll_queue_state() -> None:
    global _tier_poll_queue
    global _tier_poll_worker_task
    global _tier_poll_state_lock

    task = _tier_poll_worker_task
    if task is not None and not task.done():
        await task

    _tier_poll_queue = None
    _tier_poll_worker_task = None
    _tier_poll_state_lock = None
    _queued_tier_polls.clear()


async def record_job(job_name: str, processed: int, success: int, failure: int, error: str | None = None) -> None:
    async with SessionLocal() as session:
        job = await start_job_run(session, job_name)
        await complete_job_run(
            session,
            job,
            processed_count=processed,
            success_count=success,
            failure_count=failure,
            error_summary=error,
        )
        await session.commit()


async def commit_poll_error_state(session: AsyncSession, steam_app_id: int) -> None:
    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("Failed to persist poll error state for Steam app %s", steam_app_id)


async def import_registry_job() -> None:
    processed = 0
    try:
        async with SessionLocal() as session:
            await ensure_future_partitions(session, settings.partition_months_ahead)
            stats = await import_registry(session, settings)
            processed = stats.imported
            await session.commit()
        await record_job("import_registry", processed, processed, 0)
        logger.info("Imported %s tracked apps", processed)
    except Exception as exc:
        logger.exception("Registry import failed")
        await record_job("import_registry", processed, 0, 1, str(exc)[:2000])


async def poll_tier_job(tier: Tier, *, mode: PollAppMode = PollAppMode.full) -> None:
    if tier == Tier.cold:
        logger.info("Cold polling is disabled; skipping tier poll")
        return

    due_ids: list[int] = []
    success = 0
    failure = 0
    stopped_for_budget = False
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_steam_app_ids(session, tier, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                if not has_worker_runtime_budget():
                    logger.warning("Stopping tier %s poll early; worker runtime budget is nearly exhausted", tier.value)
                    stopped_for_budget = True
                    break
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings, mode=mode)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 from current players endpoint; backing off retries",
                            steam_app_id,
                        )
                    except Exception:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.exception("Poll failed for %s", steam_app_id)
        await record_job(
            f"poll_{tier.value}",
            len(due_ids),
            success,
            failure,
            RUNTIME_BUDGET_EXHAUSTED_ERROR if stopped_for_budget else None,
        )
        logger.info("Tier %s poll finished: %s due, %s success, %s failure", tier.value, len(due_ids), success, failure)
    except Exception as exc:
        logger.exception("Tier poll crashed for %s", tier.value)
        await record_job(f"poll_{tier.value}", len(due_ids), success, failure + 1, str(exc)[:2000])


async def sync_user_scores_job() -> None:
    due_ids: list[int] = []
    success = 0
    failure = 0
    stopped_for_budget = False
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_user_score_steam_app_ids(session, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                if not has_worker_runtime_budget():
                    logger.warning("Stopping user score sync early; worker runtime budget is nearly exhausted")
                    stopped_for_budget = True
                    break
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings, mode=PollAppMode.user_score_only)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 during user score sync; backing off retries",
                            steam_app_id,
                        )
                    except Exception:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.exception("User score sync failed for %s", steam_app_id)
        await record_job(
            "sync_user_scores",
            len(due_ids),
            success,
            failure,
            RUNTIME_BUDGET_EXHAUSTED_ERROR if stopped_for_budget else None,
        )
        logger.info("User score sync finished: %s due, %s success, %s failure", len(due_ids), success, failure)
    except Exception as exc:
        logger.exception("User score sync crashed")
        await record_job("sync_user_scores", len(due_ids), success, failure + 1, str(exc)[:2000])


async def _run_poll_hot_players_only() -> None:
    await poll_tier_job(Tier.hot, mode=PollAppMode.players_only)


async def _run_poll_warm_players_only() -> None:
    await poll_tier_job(Tier.warm, mode=PollAppMode.players_only)


async def bootstrap_poll_job() -> int:
    due_ids: list[int] = []
    success = 0
    failure = 0
    stopped_for_budget = False
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_bootstrap_steam_app_ids(session, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                if not has_worker_runtime_budget():
                    logger.warning("Stopping bootstrap poll early; worker runtime budget is nearly exhausted")
                    stopped_for_budget = True
                    break
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 from current players endpoint during bootstrap; backing off retries",
                            steam_app_id,
                        )
                    except Exception:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.exception("Bootstrap poll failed for %s", steam_app_id)
        await record_job(
            "bootstrap_poll",
            len(due_ids),
            success,
            failure,
            RUNTIME_BUDGET_EXHAUSTED_ERROR if stopped_for_budget else None,
        )
        logger.info("Bootstrap poll finished: %s due, %s success, %s failure", len(due_ids), success, failure)
        return len(due_ids)
    except Exception as exc:
        logger.exception("Bootstrap poll crashed")
        await record_job("bootstrap_poll", len(due_ids), success, failure + 1, str(exc)[:2000])
        return len(due_ids)


async def launch_watch_poll_job() -> int:
    due_ids: list[int] = []
    success = 0
    failure = 0
    stopped_for_budget = False
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_launch_watch_steam_app_ids(session, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                if not has_worker_runtime_budget():
                    logger.warning("Stopping launch watch poll early; worker runtime budget is nearly exhausted")
                    stopped_for_budget = True
                    break
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 from current players endpoint during launch watch; retrying soon",
                            steam_app_id,
                        )
                    except Exception:
                        await commit_poll_error_state(session, steam_app_id)
                        failure += 1
                        logger.exception("Launch watch poll failed for %s", steam_app_id)
        await record_job(
            "launch_watch_poll",
            len(due_ids),
            success,
            failure,
            RUNTIME_BUDGET_EXHAUSTED_ERROR if stopped_for_budget else None,
        )
        logger.info("Launch watch poll finished: %s due, %s success, %s failure", len(due_ids), success, failure)
        return len(due_ids)
    except Exception as exc:
        logger.exception("Launch watch poll crashed")
        await record_job("launch_watch_poll", len(due_ids), success, failure + 1, str(exc)[:2000])
        return len(due_ids)


async def bootstrap() -> None:
    async with SessionLocal() as session:
        await ensure_future_partitions(session, settings.partition_months_ahead)
        await session.commit()


def is_scheduled_job_due(
    last_started_at: datetime | None,
    interval_minutes: int,
    now: datetime,
) -> bool:
    if last_started_at is None:
        return True
    return last_started_at <= now - timedelta(minutes=interval_minutes)


def is_latest_job_run_due(
    latest_job: JobRun | object | None,
    interval_minutes: int,
    now: datetime,
) -> bool:
    if latest_job is None:
        return True
    if getattr(latest_job, "error_summary", None) == RUNTIME_BUDGET_EXHAUSTED_ERROR:
        return True
    return is_scheduled_job_due(getattr(latest_job, "started_at", None), interval_minutes, now)


def build_pipeline_definitions(pipeline: str = PIPELINE_ALL) -> list[ScheduledJobDefinition]:
    maintenance = [
        ScheduledJobDefinition("import_registry", settings.registry_import_minutes, import_registry_job),
        ScheduledJobDefinition("bootstrap_poll", settings.bootstrap_poll_minutes, bootstrap_poll_job),
        ScheduledJobDefinition("launch_watch_poll", settings.bootstrap_poll_minutes, launch_watch_poll_job),
    ]
    if pipeline == PIPELINE_MAINTENANCE:
        return maintenance
    if pipeline == PIPELINE_HOT:
        return [ScheduledJobDefinition("poll_hot", settings.hot_poll_minutes, _run_poll_hot_players_only)]
    if pipeline == PIPELINE_WARM:
        return [ScheduledJobDefinition("poll_warm", settings.warm_poll_minutes, _run_poll_warm_players_only)]
    if pipeline == PIPELINE_SCORES:
        return [ScheduledJobDefinition("sync_user_scores", settings.user_score_poll_minutes, sync_user_scores_job)]
    if pipeline == PIPELINE_ALL:
        return [
            *maintenance,
            ScheduledJobDefinition("poll_hot", settings.hot_poll_minutes, _run_poll_hot_players_only),
            ScheduledJobDefinition("poll_warm", settings.warm_poll_minutes, _run_poll_warm_players_only),
            ScheduledJobDefinition("sync_user_scores", settings.user_score_poll_minutes, sync_user_scores_job),
        ]
    raise ValueError(f"Unknown pipeline {pipeline!r}; expected one of {PIPELINE_CHOICES}")


def build_scheduled_job_definitions() -> list[ScheduledJobDefinition]:
    return build_pipeline_definitions(PIPELINE_ALL)


async def _latest_job_run(job_name: str) -> JobRun | None:
    async with SessionLocal() as session:
        return await session.scalar(
            select(JobRun)
            .where(JobRun.job_name == job_name)
            .order_by(JobRun.started_at.desc(), JobRun.id.desc())
            .limit(1)
        )


async def maybe_run_scheduled_job(
    definition: ScheduledJobDefinition,
    *,
    now: datetime | None = None,
    force: bool = False,
) -> bool:
    effective_now = now or datetime.now(timezone.utc)
    if not force:
        latest_job = await _latest_job_run(definition.job_name)
        if not is_latest_job_run_due(latest_job, definition.interval_minutes, effective_now):
            logger.info(
                "Skipping %s; next run not due yet",
                definition.job_name,
            )
            return False

    logger.info("Running scheduled job %s", definition.job_name)
    await definition.runner()
    return True


async def run_due_jobs_once(
    *,
    force_all: bool = False,
    now: datetime | None = None,
    pipeline: str = PIPELINE_ALL,
) -> list[str]:
    await bootstrap()
    executed: list[str] = []

    # HOT/WARM/scores workflows are on their own GitHub cron; the workflow schedule
    # is the throttle. Do not also skip based on job_runs — that causes "manual run
    # then scheduled run does nothing" and double-clocks with pipeline=all. Per-app
    # work is still limited by get_due_* queries (last_polled_at / score sync time).
    force_jobs = force_all or pipeline in (PIPELINE_HOT, PIPELINE_WARM, PIPELINE_SCORES)

    for definition in build_pipeline_definitions(pipeline):
        if not has_worker_runtime_budget():
            logger.warning("Stopping one-shot worker cycle before %s; runtime budget is nearly exhausted", definition.job_name)
            break
        if await maybe_run_scheduled_job(definition, now=now, force=force_jobs):
            executed.append(definition.job_name)

    logger.info("One-shot worker cycle finished. Jobs run: %s", ", ".join(executed) if executed else "none")
    return executed


async def startup_sync() -> None:
    logger.info("Running startup registry import")
    await import_registry_job()

    for batch in range(1, settings.startup_bootstrap_max_batches + 1):
        logger.info("Running startup bootstrap poll batch %s", batch)
        processed = await bootstrap_poll_job()
        if processed < settings.bootstrap_batch_limit:
            break

    logger.info("Running startup launch watch poll")
    await launch_watch_poll_job()

    for tier in (Tier.hot, Tier.warm):
        logger.info("Running startup poll for tier %s", tier.value)
        await poll_tier_job(tier)


async def main() -> None:
    await bootstrap()
    await startup_sync()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(import_registry_job, "interval", minutes=settings.registry_import_minutes, id="import_registry", replace_existing=True)
    scheduler.add_job(
        bootstrap_poll_job,
        "interval",
        minutes=settings.bootstrap_poll_minutes,
        id="bootstrap_poll",
        replace_existing=True,
    )
    scheduler.add_job(
        launch_watch_poll_job,
        "interval",
        minutes=settings.bootstrap_poll_minutes,
        id="launch_watch_poll",
        replace_existing=True,
    )
    scheduler.add_job(queue_tier_poll_job, "interval", minutes=settings.hot_poll_minutes, id="poll_hot", replace_existing=True, args=[Tier.hot])
    scheduler.add_job(queue_tier_poll_job, "interval", minutes=settings.warm_poll_minutes, id="poll_warm", replace_existing=True, args=[Tier.warm])
    scheduler.add_job(
        sync_user_scores_job,
        "interval",
        minutes=settings.user_score_poll_minutes,
        id="sync_user_scores",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Worker started")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
