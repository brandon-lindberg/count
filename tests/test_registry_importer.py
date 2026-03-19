from app.config import Settings
from app.services.registry_importer import build_source_games_url, extract_tracked_games


def test_extract_tracked_games_filters_missing_ids() -> None:
    payload = {
        "items": [
            {"id": 1, "public_id": "a", "title": "Alpha", "steam_app_id": 100},
            {"id": 2, "public_id": "b", "title": "Beta", "steam_app_id": None},
            {"id": 3, "public_id": "c", "title": "", "steam_app_id": 200},
        ]
    }
    tracked = extract_tracked_games(payload)
    assert len(tracked) == 1
    assert tracked[0].steam_app_id == 100
    assert tracked[0].title == "Alpha"


def test_build_source_games_url_defaults_to_api_v1_games() -> None:
    settings = Settings(source_api_base_url="http://localhost:8000")

    assert build_source_games_url(settings) == "http://localhost:8000/api/v1/games"


def test_build_source_games_url_preserves_existing_api_v1_base() -> None:
    settings = Settings(source_api_base_url="http://localhost:8000/api/v1")

    assert build_source_games_url(settings) == "http://localhost:8000/api/v1/games"
