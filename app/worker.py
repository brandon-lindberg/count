from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from app.config import get_settings
from app.database import SessionLocal
from app.models import Tier, TrackedApp
from app.services.job_runs import complete_job_run, start_job_run
from app.services.partitions import ensure_future_partitions
from app.services.polling import (
    get_due_bootstrap_steam_app_ids,
    get_due_launch_watch_steam_app_ids,
    get_due_steam_app_ids,
    poll_app,
)
from app.services.registry_importer import import_registry
from app.services.steam_provider import SteamAppNotFoundError, SteamCurrentPlayersProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


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


async def poll_tier_job(tier: Tier) -> None:
    due_ids: list[int] = []
    success = 0
    failure = 0
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_steam_app_ids(session, tier, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await session.commit()
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 from current players endpoint; backing off retries",
                            steam_app_id,
                        )
                    except Exception:
                        await session.commit()
                        failure += 1
                        logger.exception("Poll failed for %s", steam_app_id)
        await record_job(f"poll_{tier.value}", len(due_ids), success, failure)
        logger.info("Tier %s poll finished: %s due, %s success, %s failure", tier.value, len(due_ids), success, failure)
    except Exception as exc:
        logger.exception("Tier poll crashed for %s", tier.value)
        await record_job(f"poll_{tier.value}", len(due_ids), success, failure + 1, str(exc)[:2000])


async def bootstrap_poll_job() -> int:
    due_ids: list[int] = []
    success = 0
    failure = 0
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_bootstrap_steam_app_ids(session, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await session.commit()
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 from current players endpoint during bootstrap; backing off retries",
                            steam_app_id,
                        )
                    except Exception:
                        await session.commit()
                        failure += 1
                        logger.exception("Bootstrap poll failed for %s", steam_app_id)
        await record_job("bootstrap_poll", len(due_ids), success, failure)
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
    try:
        async with SessionLocal() as session:
            due_ids = await get_due_launch_watch_steam_app_ids(session, settings)
        async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
            for steam_app_id in due_ids:
                async with SessionLocal() as session:
                    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
                    if tracked_app is None:
                        continue
                    try:
                        await poll_app(session, tracked_app, provider, settings)
                        await session.commit()
                        success += 1
                    except SteamAppNotFoundError:
                        await session.commit()
                        failure += 1
                        logger.warning(
                            "Steam app %s returned 404 from current players endpoint during launch watch; retrying soon",
                            steam_app_id,
                        )
                    except Exception:
                        await session.commit()
                        failure += 1
                        logger.exception("Launch watch poll failed for %s", steam_app_id)
        await record_job("launch_watch_poll", len(due_ids), success, failure)
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

    for tier in (Tier.hot, Tier.warm, Tier.cold):
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
    scheduler.add_job(poll_tier_job, "interval", minutes=settings.hot_poll_minutes, id="poll_hot", replace_existing=True, args=[Tier.hot])
    scheduler.add_job(poll_tier_job, "interval", minutes=settings.warm_poll_minutes, id="poll_warm", replace_existing=True, args=[Tier.warm])
    scheduler.add_job(poll_tier_job, "interval", minutes=settings.cold_poll_minutes, id="poll_cold", replace_existing=True, args=[Tier.cold])
    scheduler.start()
    logger.info("Worker started")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
