from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import Tier, TrackedApp
from app.services.tiering import is_release_hot_window, refresh_effective_tier


@dataclass(slots=True)
class ManualTrackedAppInput:
    steam_app_id: int
    title: str
    source_game_id: int | None = None
    source_game_public_id: str | None = None
    source_release_date: date | None = None
    is_active: bool = True


async def upsert_manual_tracked_app(
    session: AsyncSession,
    payload: ManualTrackedAppInput,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> tuple[TrackedApp, bool]:
    effective_now = now or datetime.now(timezone.utc)
    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == payload.steam_app_id))
    created = tracked_app is None

    if tracked_app is None:
        initial_tier = Tier.hot if is_release_hot_window(payload.source_release_date, settings, effective_now) else Tier.warm
        tracked_app = TrackedApp(
            steam_app_id=payload.steam_app_id,
            title=payload.title,
            source_game_id=payload.source_game_id,
            source_game_public_id=payload.source_game_public_id,
            source_release_date=payload.source_release_date,
            is_active=payload.is_active,
            import_source="manual",
            effective_tier=initial_tier,
            last_imported_at=effective_now,
        )
        session.add(tracked_app)
    else:
        tracked_app.title = payload.title
        tracked_app.source_game_id = payload.source_game_id
        tracked_app.source_game_public_id = payload.source_game_public_id
        tracked_app.source_release_date = payload.source_release_date
        tracked_app.is_active = payload.is_active
        tracked_app.import_source = "manual"
        tracked_app.last_imported_at = effective_now

    await session.flush()
    await refresh_effective_tier(session, tracked_app, settings, effective_now)
    return tracked_app, created
