from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import Tier, TrackedApp


@dataclass(slots=True)
class SourceGame:
    source_game_id: int | None
    source_game_public_id: str | None
    steam_app_id: int
    title: str


@dataclass(slots=True)
class RegistryImportStats:
    imported: int = 0
    activated: int = 0
    deactivated: int = 0


def build_source_headers(settings: Settings) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if settings.source_api_token:
        headers["Authorization"] = f"Bearer {settings.source_api_token}"
    return headers


def build_source_games_url(settings: Settings) -> str:
    base_url = settings.source_api_base_url.rstrip("/")
    if base_url.endswith("/api/v1"):
        return f"{base_url}/games"
    return f"{base_url}/api/v1/games"


def extract_tracked_games(payload: dict) -> list[SourceGame]:
    items = payload.get("items") or []
    tracked: list[SourceGame] = []
    for item in items:
        steam_app_id = item.get("steam_app_id")
        title = (item.get("title") or "").strip()
        if steam_app_id is None or not title:
            continue
        tracked.append(
            SourceGame(
                source_game_id=item.get("id"),
                source_game_public_id=item.get("public_id"),
                steam_app_id=int(steam_app_id),
                title=title,
            )
        )
    return tracked


async def import_registry(session: AsyncSession, settings: Settings) -> RegistryImportStats:
    now = datetime.now(timezone.utc)
    stats = RegistryImportStats()
    seen_app_ids: set[int] = set()
    games_url = build_source_games_url(settings)

    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds, headers=build_source_headers(settings)) as client:
        page = 1
        total_pages = 1
        while page <= total_pages:
            response = await client.get(
                games_url,
                params={
                    "page": page,
                    "per_page": settings.source_api_page_size,
                    "sort_by": "release_date",
                    "sort_order": "desc",
                },
            )
            response.raise_for_status()
            payload = response.json()
            total_pages = int(payload.get("total_pages") or 1)

            for source_game in extract_tracked_games(payload):
                stats.imported += 1
                seen_app_ids.add(source_game.steam_app_id)
                tracked_app = await session.scalar(
                    select(TrackedApp).where(TrackedApp.steam_app_id == source_game.steam_app_id)
                )
                if tracked_app is None:
                    tracked_app = TrackedApp(
                        source_game_id=source_game.source_game_id,
                        source_game_public_id=source_game.source_game_public_id,
                        steam_app_id=source_game.steam_app_id,
                        title=source_game.title,
                        is_active=True,
                        import_source="backend_api",
                        effective_tier=Tier.warm,
                        last_imported_at=now,
                    )
                    session.add(tracked_app)
                    stats.activated += 1
                else:
                    if not tracked_app.is_active:
                        stats.activated += 1
                    tracked_app.source_game_id = source_game.source_game_id
                    tracked_app.source_game_public_id = source_game.source_game_public_id
                    tracked_app.title = source_game.title
                    tracked_app.is_active = True
                    tracked_app.import_source = "backend_api"
                    tracked_app.last_imported_at = now
                    if tracked_app.manual_tier_override is not None:
                        tracked_app.effective_tier = tracked_app.manual_tier_override
                await session.flush()
            page += 1

    existing_apps = (await session.execute(select(TrackedApp).where(TrackedApp.import_source == "backend_api"))).scalars().all()
    for tracked_app in existing_apps:
        if tracked_app.steam_app_id not in seen_app_ids and tracked_app.is_active:
            tracked_app.is_active = False
            stats.deactivated += 1

    return stats
