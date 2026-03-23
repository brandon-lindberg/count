from datetime import date, datetime, timezone
from types import SimpleNamespace

from app.config import Settings
from app.models import Tier
from app.services.registry_importer import (
    SourceGame,
    build_source_games_url,
    extract_tracked_games,
    imported_at_for_source_position,
    sync_existing_tracked_app_from_source,
)


def test_extract_tracked_games_filters_missing_ids() -> None:
    payload = {
        "items": [
            {"id": 1, "public_id": "a", "title": "Alpha", "steam_app_id": 100, "release_date": "2026-03-19"},
            {"id": 2, "public_id": "b", "title": "Beta", "steam_app_id": None},
            {"id": 3, "public_id": "c", "title": "", "steam_app_id": 200},
        ]
    }
    tracked = extract_tracked_games(payload)
    assert len(tracked) == 1
    assert tracked[0].steam_app_id == 100
    assert tracked[0].title == "Alpha"
    assert tracked[0].source_release_date is not None


def test_build_source_games_url_defaults_to_api_v1_games() -> None:
    settings = Settings(source_api_base_url="http://localhost:8000")

    assert build_source_games_url(settings) == "http://localhost:8000/api/v1/games"


def test_build_source_games_url_preserves_existing_api_v1_base() -> None:
    settings = Settings(source_api_base_url="http://localhost:8000/api/v1")

    assert build_source_games_url(settings) == "http://localhost:8000/api/v1/games"


def test_imported_at_for_source_position_preserves_newest_first_order() -> None:
    base_time = datetime(2026, 3, 19, 2, 17, 58, 397425, tzinfo=timezone.utc)

    newest = imported_at_for_source_position(base_time, 0)
    older = imported_at_for_source_position(base_time, 1)
    oldest = imported_at_for_source_position(base_time, 2)

    assert newest > older > oldest


def test_sync_existing_tracked_app_from_source_updates_backend_rows() -> None:
    tracked_app = SimpleNamespace(
        is_active=False,
        import_source="backend_api",
        title="Old Title",
        source_game_id=10,
        source_game_public_id="old-public-id",
        source_release_date=date(2022, 10, 28),
        manual_tier_override=None,
        effective_tier=Tier.warm,
        last_imported_at=None,
    )
    source_game = SourceGame(
        source_game_id=20,
        source_game_public_id="new-public-id",
        source_release_date=date(2026, 2, 27),
        steam_app_id=3764200,
        title="Resident Evil Requiem",
    )
    now = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)
    imported_at = datetime(2026, 3, 20, 5, 1, tzinfo=timezone.utc)

    activated = sync_existing_tracked_app_from_source(
        tracked_app,
        source_game,
        imported_at=imported_at,
        settings=Settings(),
        now=now,
    )

    assert activated is True
    assert tracked_app.is_active is True
    assert tracked_app.import_source == "backend_api"
    assert tracked_app.title == "Resident Evil Requiem"
    assert tracked_app.source_game_id == 20
    assert tracked_app.source_game_public_id == "new-public-id"
    assert tracked_app.source_release_date == date(2026, 2, 27)
    assert tracked_app.last_imported_at == imported_at
    assert tracked_app.effective_tier == Tier.warm


def test_sync_existing_tracked_app_from_source_promotes_recent_cold_rows_to_warm_floor() -> None:
    tracked_app = SimpleNamespace(
        is_active=True,
        import_source="backend_api",
        title="Old Title",
        source_game_id=10,
        source_game_public_id="old-public-id",
        source_release_date=date(2024, 1, 1),
        manual_tier_override=None,
        effective_tier=Tier.cold,
        last_imported_at=None,
    )
    source_game = SourceGame(
        source_game_id=30,
        source_game_public_id="recent-public-id",
        source_release_date=date(2025, 6, 1),
        steam_app_id=123456,
        title="Recent Game",
    )

    activated = sync_existing_tracked_app_from_source(
        tracked_app,
        source_game,
        imported_at=datetime(2026, 3, 23, 3, 0, tzinfo=timezone.utc),
        settings=Settings(),
        now=datetime(2026, 3, 23, 3, 0, tzinfo=timezone.utc),
    )

    assert activated is False
    assert tracked_app.effective_tier == Tier.warm


def test_sync_existing_tracked_app_from_source_preserves_manual_rows() -> None:
    tracked_app = SimpleNamespace(
        is_active=True,
        import_source="manual",
        title="Resident Evil Requiem",
        source_game_id=36423,
        source_game_public_id="be724651782747f6b4c6f992e34be774",
        source_release_date=date(2026, 2, 27),
        manual_tier_override=None,
        effective_tier=Tier.hot,
        last_imported_at=None,
    )
    source_game = SourceGame(
        source_game_id=12860,
        source_game_public_id="c4172ae2d59c41f3a50f1ef30841479e",
        source_release_date=date(2022, 10, 28),
        steam_app_id=3764200,
        title="Resident Evil Re:Verse",
    )
    imported_at = datetime(2026, 3, 20, 5, 1, tzinfo=timezone.utc)

    activated = sync_existing_tracked_app_from_source(
        tracked_app,
        source_game,
        imported_at=imported_at,
        settings=Settings(),
        now=datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc),
    )

    assert activated is False
    assert tracked_app.is_active is True
    assert tracked_app.import_source == "manual"
    assert tracked_app.title == "Resident Evil Requiem"
    assert tracked_app.source_game_id == 36423
    assert tracked_app.source_game_public_id == "be724651782747f6b4c6f992e34be774"
    assert tracked_app.source_release_date == date(2026, 2, 27)
    assert tracked_app.last_imported_at == imported_at
