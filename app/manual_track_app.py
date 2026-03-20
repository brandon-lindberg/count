from __future__ import annotations

import argparse
import asyncio
from datetime import date

from app.config import get_settings
from app.database import SessionLocal
from app.services.manual_tracking import ManualTrackedAppInput, upsert_manual_tracked_app


def _parse_optional_date(value: str | None) -> date | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return date.fromisoformat(stripped)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or rebind a tracked Steam app to manual metadata that will not be overwritten by registry imports.",
    )
    parser.add_argument("--steam-app-id", type=int, required=True, help="Steam app ID to create or rebind.")
    parser.add_argument("--title", required=True, help="Display title to store for the tracked app.")
    parser.add_argument("--source-game-id", type=int, default=None, help="Main backend game ID for mirror/backfill mapping.")
    parser.add_argument(
        "--source-game-public-id",
        default=None,
        help="Main backend public_id for mirror/backfill mapping.",
    )
    parser.add_argument(
        "--release-date",
        default=None,
        help="Optional release date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--inactive",
        action="store_true",
        help="Store the tracked app as inactive instead of active.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    settings = get_settings()
    payload = ManualTrackedAppInput(
        steam_app_id=args.steam_app_id,
        title=args.title.strip(),
        source_game_id=args.source_game_id,
        source_game_public_id=(args.source_game_public_id or "").strip() or None,
        source_release_date=_parse_optional_date(args.release_date),
        is_active=not args.inactive,
    )

    async with SessionLocal() as session:
        tracked_app, created = await upsert_manual_tracked_app(session, payload, settings)
        await session.commit()

    action = "Created" if created else "Updated"
    print(
        f"{action} manual tracked app {tracked_app.steam_app_id}: "
        f"{tracked_app.title} "
        f"(game_id={tracked_app.source_game_id}, public_id={tracked_app.source_game_public_id}, "
        f"release_date={tracked_app.source_release_date}, tier={tracked_app.effective_tier.value}, "
        f"active={'yes' if tracked_app.is_active else 'no'})"
    )
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
