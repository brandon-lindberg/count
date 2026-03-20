from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.services.manual_tracking import ManualTrackedAppInput, upsert_manual_tracked_app


@pytest.mark.asyncio
async def test_upsert_manual_tracked_app_creates_manual_row(monkeypatch: pytest.MonkeyPatch) -> None:
    refresh_mock = AsyncMock()
    monkeypatch.setattr("app.services.manual_tracking.refresh_effective_tier", refresh_mock)
    settings = Settings()

    added: list[object] = []
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=None),
        add=added.append,
        flush=AsyncMock(),
    )
    payload = ManualTrackedAppInput(
        steam_app_id=3764200,
        title="Resident Evil Requiem",
        source_game_id=36423,
        source_game_public_id="be724651782747f6b4c6f992e34be774",
        source_release_date=date(2026, 2, 27),
    )
    now = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)

    tracked_app, created = await upsert_manual_tracked_app(session, payload, settings, now=now)

    assert created is True
    assert tracked_app.steam_app_id == 3764200
    assert tracked_app.title == "Resident Evil Requiem"
    assert tracked_app.import_source == "manual"
    assert tracked_app.source_game_id == 36423
    assert tracked_app.source_game_public_id == "be724651782747f6b4c6f992e34be774"
    assert tracked_app.source_release_date == date(2026, 2, 27)
    assert tracked_app.last_imported_at == now
    assert tracked_app in added
    refresh_mock.assert_awaited_once_with(session, tracked_app, settings, now)


@pytest.mark.asyncio
async def test_upsert_manual_tracked_app_updates_existing_row(monkeypatch: pytest.MonkeyPatch) -> None:
    refresh_mock = AsyncMock()
    monkeypatch.setattr("app.services.manual_tracking.refresh_effective_tier", refresh_mock)
    settings = Settings()

    tracked_app = SimpleNamespace(
        steam_app_id=3764200,
        title="Resident Evil Re:Verse",
        source_game_id=12860,
        source_game_public_id="c4172ae2d59c41f3a50f1ef30841479e",
        source_release_date=date(2022, 10, 28),
        is_active=False,
        import_source="backend_api",
        last_imported_at=None,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=tracked_app),
        add=AsyncMock(),
        flush=AsyncMock(),
    )
    payload = ManualTrackedAppInput(
        steam_app_id=3764200,
        title="Resident Evil Requiem",
        source_game_id=36423,
        source_game_public_id="be724651782747f6b4c6f992e34be774",
        source_release_date=date(2026, 2, 27),
    )
    now = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)

    updated_row, created = await upsert_manual_tracked_app(session, payload, settings, now=now)

    assert created is False
    assert updated_row is tracked_app
    assert tracked_app.title == "Resident Evil Requiem"
    assert tracked_app.source_game_id == 36423
    assert tracked_app.source_game_public_id == "be724651782747f6b4c6f992e34be774"
    assert tracked_app.source_release_date == date(2026, 2, 27)
    assert tracked_app.is_active is True
    assert tracked_app.import_source == "manual"
    assert tracked_app.last_imported_at == now
    refresh_mock.assert_awaited_once_with(session, tracked_app, settings, now)
