from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import SessionLocal
from app.models import TrackedApp
from app.services.main_db_mirror import (
    MirrorBackfillStats,
    backfill_tracked_app_to_main_db,
    mirror_is_configured,
)


@dataclass(slots=True)
class BackfillProgressUpdate:
    index: int
    total: int
    tracked_app: TrackedApp
    aggregate: MirrorBackfillStats
    app_stats: MirrorBackfillStats | None
    error: str | None
    started_at: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-time backfill from the scraper database into the main backend database mirror.",
    )
    parser.add_argument("--steam-app-id", type=int, default=None, help="Backfill a single Steam app ID.")
    parser.add_argument("--limit-apps", type=int, default=None, help="Limit the number of tracked apps processed.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per insert/upsert batch. Default: 1000.")
    parser.add_argument(
        "--only-unmirrored",
        action="store_true",
        help=(
            "Skip re-copying raw player snapshots / range history when the main DB already has them, "
            "but still push the latest games row and Steam user_scores from the scraper (fixes missing scores)."
        ),
    )
    return parser


def _format_count(value: int) -> str:
    return f"{value:,}"


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _build_progress_prefix(index: int, total: int, started_at: float, *, width: int = 20) -> str:
    fraction = index / total if total > 0 else 0
    filled = min(width, int(fraction * width))
    bar = f"{'#' * filled}{'-' * (width - filled)}"
    elapsed = max(0.0, time.monotonic() - started_at)
    avg_per_app = elapsed / index if index > 0 else 0.0
    remaining = max(0, total - index)
    eta = avg_per_app * remaining
    return f"[{bar}] {index}/{total} {fraction * 100:5.1f}% ETA {_format_duration(eta)}"


async def load_backfill_targets(
    scraper_session: AsyncSession,
    *,
    steam_app_id: int | None = None,
    limit_apps: int | None = None,
) -> list[TrackedApp]:
    query = (
        select(TrackedApp)
        .where(TrackedApp.last_success_at.is_not(None))
        .order_by(TrackedApp.last_success_at.desc(), TrackedApp.steam_app_id.asc())
    )
    if steam_app_id is not None:
        query = query.where(TrackedApp.steam_app_id == steam_app_id)
    if limit_apps is not None:
        query = query.limit(limit_apps)
    return list((await scraper_session.execute(query)).scalars().all())


async def run_backfill_targets(
    scraper_session: AsyncSession,
    tracked_apps: list[TrackedApp],
    *,
    batch_size: int,
    only_unmirrored: bool,
    progress_callback: Callable[[BackfillProgressUpdate], Awaitable[None] | None] | None = None,
) -> MirrorBackfillStats:
    settings = get_settings()
    aggregate = MirrorBackfillStats()
    started_at = time.monotonic()

    for index, tracked_app in enumerate(tracked_apps, start=1):
        app_stats: MirrorBackfillStats | None = None
        error: str | None = None
        try:
            app_stats = await backfill_tracked_app_to_main_db(
                scraper_session,
                tracked_app,
                batch_size=batch_size,
                skip_if_already_mirrored=only_unmirrored,
                settings=settings,
            )
            aggregate.merge(app_stats)
        except Exception as exc:
            aggregate.apps_failed += 1
            error = str(exc)

        if progress_callback is not None:
            await progress_callback(
                BackfillProgressUpdate(
                    index=index,
                    total=len(tracked_apps),
                    tracked_app=tracked_app,
                    aggregate=aggregate,
                    app_stats=app_stats,
                    error=error,
                    started_at=started_at,
                )
            )

    return aggregate


async def _run(args: argparse.Namespace) -> int:
    settings = get_settings()
    if not mirror_is_configured(settings):
        print("MIRROR_DATABASE_URL is not configured in the scraper environment.", file=sys.stderr)
        return 1

    aggregate = MirrorBackfillStats()

    async with SessionLocal() as scraper_session:
        tracked_apps = await load_backfill_targets(
            scraper_session,
            steam_app_id=args.steam_app_id,
            limit_apps=args.limit_apps,
        )
        target_count = len(tracked_apps)

        if target_count == 0:
            print("No tracked apps with stored samples matched the requested backfill scope.")
            return 0

        print(
            f"Starting main DB backfill for {_format_count(target_count)} tracked app"
            f"{'' if target_count == 1 else 's'}..."
        )

        async def _print_progress(update: BackfillProgressUpdate) -> None:
            progress_prefix = _build_progress_prefix(update.index, update.total, update.started_at)

            if update.error is not None:
                print(
                    f"{progress_prefix} failed Steam app {update.tracked_app.steam_app_id}"
                    f" ({update.tracked_app.title}): {update.error}",
                    file=sys.stderr,
                )
                return

            if update.app_stats is None:
                return

            if update.app_stats.apps_skipped_unmapped:
                print(
                    f"{progress_prefix} skipped Steam app {update.tracked_app.steam_app_id}"
                    f" ({update.tracked_app.title}) because no matching main DB game row was found."
                )
                return

            if update.app_stats.apps_skipped_already_mirrored:
                print(
                    f"{progress_prefix} synced summary/Steam scores for Steam app {update.tracked_app.steam_app_id}"
                    f" ({update.tracked_app.title}); skipped re-copying snapshot/range history (already in main DB)."
                )
                return

            print(
                f"{progress_prefix} mirrored Steam app {update.tracked_app.steam_app_id}"
                f" ({update.tracked_app.title}): raw {_format_count(update.app_stats.raw_samples_inserted)}/"
                f"{_format_count(update.app_stats.raw_samples_seen)} inserted, range {_format_count(update.app_stats.range_points_upserted)}/"
                f"{_format_count(update.app_stats.range_points_seen)} upserted."
            )

        aggregate = await run_backfill_targets(
            scraper_session,
            tracked_apps,
            batch_size=args.batch_size,
            only_unmirrored=args.only_unmirrored,
            progress_callback=_print_progress,
        )

    print("")
    print("Backfill summary")
    print(f"- Apps processed (full mirror or summary/score sync): {_format_count(aggregate.apps_processed)}")
    print(f"- Apps skipped (unmapped): {_format_count(aggregate.apps_skipped_unmapped)}")
    print(
        "- Apps where bulk snapshot/range copy was skipped (summary still pushed from scraper): "
        f"{_format_count(aggregate.apps_skipped_already_mirrored)}"
    )
    print(f"- Apps failed: {_format_count(aggregate.apps_failed)}")
    print(f"- Raw samples seen: {_format_count(aggregate.raw_samples_seen)}")
    print(f"- Raw samples inserted: {_format_count(aggregate.raw_samples_inserted)}")
    print(f"- Range points seen: {_format_count(aggregate.range_points_seen)}")
    print(f"- Range points upserted: {_format_count(aggregate.range_points_upserted)}")
    print(f"- Game summaries updated: {_format_count(aggregate.summaries_updated)}")

    return 1 if aggregate.apps_failed > 0 else 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
