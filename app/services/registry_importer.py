from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import asyncio
import logging

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import Tier, TrackedApp
from app.services.tiering import is_recent_release_warm_floor_window, is_release_hot_window

logger = logging.getLogger(__name__)
REGISTRY_PAGE_RETRY_COUNT = 3
REGISTRY_PAGE_RETRY_DELAY_SECONDS = 60


@dataclass(slots=True)
class SourceGame:
    source_game_id: int | None
    source_game_public_id: str | None
    source_release_date: date | None
    steam_app_id: int
    title: str


@dataclass(slots=True)
class RegistryImportStats:
    imported: int = 0
    activated: int = 0
    deactivated: int = 0


def imported_at_for_source_position(base_time: datetime, position: int) -> datetime:
    # Preserve source sort order inside last_imported_at so first-pass polling can
    # process the newest imported games before older backlog rows.
    return base_time - timedelta(microseconds=position)


def build_source_headers(settings: Settings) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if settings.source_api_token:
        headers["Authorization"] = f"Bearer {settings.source_api_token}"
    return headers


def build_source_games_url(settings: Settings) -> str:
    base_url = settings.source_api_base_url.rstrip("/")
    if base_url.endswith("/api/v1"):
        return f"{base_url}/internal/tracked-games"
    return f"{base_url}/api/v1/internal/tracked-games"


def extract_tracked_games(payload: dict) -> list[SourceGame]:
    items = payload.get("items") or []
    tracked: list[SourceGame] = []
    for item in items:
        steam_app_id = item.get("steam_app_id")
        title = (item.get("title") or "").strip()
        if steam_app_id is None or not title:
            continue
        release_date_value = item.get("release_date")
        release_date = None
        if isinstance(release_date_value, str) and release_date_value:
            try:
                release_date = date.fromisoformat(release_date_value)
            except ValueError:
                release_date = None
        tracked.append(
            SourceGame(
                source_game_id=item.get("id"),
                source_game_public_id=item.get("public_id"),
                source_release_date=release_date,
                steam_app_id=int(steam_app_id),
                title=title,
            )
        )
    return tracked


def sync_existing_tracked_app_from_source(
    tracked_app: TrackedApp,
    source_game: SourceGame,
    *,
    imported_at: datetime,
    settings: Settings,
    now: datetime,
) -> bool:
    activated = not tracked_app.is_active
    tracked_app.is_active = True
    tracked_app.last_imported_at = imported_at

    if tracked_app.import_source == "manual":
        return activated

    tracked_app.source_game_id = source_game.source_game_id
    tracked_app.source_game_public_id = source_game.source_game_public_id
    tracked_app.source_release_date = source_game.source_release_date
    tracked_app.title = source_game.title
    tracked_app.import_source = "backend_api"
    if tracked_app.manual_tier_override is not None:
        tracked_app.effective_tier = tracked_app.manual_tier_override
    elif is_release_hot_window(source_game.source_release_date, settings, now):
        tracked_app.effective_tier = Tier.hot
    elif tracked_app.effective_tier == Tier.cold and is_recent_release_warm_floor_window(
        source_game.source_release_date,
        settings,
        now,
    ):
        tracked_app.effective_tier = Tier.warm

    return activated


def initial_effective_tier_for_source_game(source_release_date: date | None, settings: Settings, now: datetime) -> Tier:
    if is_release_hot_window(source_release_date, settings, now):
        return Tier.hot
    if is_recent_release_warm_floor_window(source_release_date, settings, now):
        return Tier.warm
    return Tier.cold


async def import_registry(session: AsyncSession, settings: Settings) -> RegistryImportStats:
    now = datetime.now(timezone.utc)
    stats = RegistryImportStats()
    seen_app_ids: set[int] = set()
    games_url = build_source_games_url(settings)
    import_position = 0

    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds, headers=build_source_headers(settings)) as client:
        page = 1
        total_pages = 1
        while page <= total_pages:
            if settings.source_api_max_pages is not None and page > settings.source_api_max_pages:
                logger.info(
                    "Registry import stopping at page limit SOURCE_API_MAX_PAGES=%s",
                    settings.source_api_max_pages,
                )
                break
            payload: dict | None = None
            for attempt in range(REGISTRY_PAGE_RETRY_COUNT + 1):
                try:
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
                    if not isinstance(payload, dict):
                        raise ValueError("Source API returned non-object payload")
                    break
                except (httpx.HTTPError, ValueError) as exc:
                    attempt_number = attempt + 1
                    max_attempts = REGISTRY_PAGE_RETRY_COUNT + 1
                    logger.warning(
                        "Registry import page %s failed (attempt %s/%s): %s",
                        page,
                        attempt_number,
                        max_attempts,
                        exc,
                    )
                    if attempt == REGISTRY_PAGE_RETRY_COUNT:
                        logger.warning(
                            "Registry import page %s exhausted retries; skipping this page for current cycle",
                            page,
                        )
                        break
                    logger.info(
                        "Registry import page %s retrying in %s seconds",
                        page,
                        REGISTRY_PAGE_RETRY_DELAY_SECONDS,
                    )
                    await asyncio.sleep(REGISTRY_PAGE_RETRY_DELAY_SECONDS)

            if payload is None:
                page += 1
                continue

            total_pages = int(payload.get("total_pages") or 1)

            for source_game in extract_tracked_games(payload):
                imported_at = imported_at_for_source_position(now, import_position)
                stats.imported += 1
                import_position += 1
                seen_app_ids.add(source_game.steam_app_id)
                tracked_app = await session.scalar(
                    select(TrackedApp).where(TrackedApp.steam_app_id == source_game.steam_app_id)
                )
                if tracked_app is None:
                    initial_tier = initial_effective_tier_for_source_game(source_game.source_release_date, settings, now)
                    tracked_app = TrackedApp(
                        source_game_id=source_game.source_game_id,
                        source_game_public_id=source_game.source_game_public_id,
                        source_release_date=source_game.source_release_date,
                        steam_app_id=source_game.steam_app_id,
                        title=source_game.title,
                        is_active=True,
                        import_source="backend_api",
                        effective_tier=initial_tier,
                        last_imported_at=imported_at,
                    )
                    session.add(tracked_app)
                    stats.activated += 1
                else:
                    if sync_existing_tracked_app_from_source(
                        tracked_app,
                        source_game,
                        imported_at=imported_at,
                        settings=settings,
                        now=now,
                    ):
                        stats.activated += 1
                await session.flush()
            page += 1

    existing_apps = (await session.execute(select(TrackedApp).where(TrackedApp.import_source == "backend_api"))).scalars().all()
    for tracked_app in existing_apps:
        if tracked_app.steam_app_id not in seen_app_ids and tracked_app.is_active:
            tracked_app.is_active = False
            stats.deactivated += 1

    return stats
