from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.main import (
    _build_page_numbers,
    _build_url_with_query,
    _format_admin_datetime,
    _format_admin_datetime_iso,
    _format_admin_number,
    _parse_app_sort,
    _parse_optional_bool,
    _parse_optional_tier,
    _resolve_admin_window,
)
from app.models import Tier


def test_parse_optional_tier_allows_blank_values() -> None:
    assert _parse_optional_tier(None) is None
    assert _parse_optional_tier("") is None
    assert _parse_optional_tier("  ") is None
    assert _parse_optional_tier("hot") == Tier.hot


def test_parse_optional_tier_rejects_invalid_values() -> None:
    with pytest.raises(HTTPException):
        _parse_optional_tier("nope")


def test_parse_optional_bool_allows_blank_values() -> None:
    assert _parse_optional_bool(None) is None
    assert _parse_optional_bool("") is None
    assert _parse_optional_bool("  ") is None
    assert _parse_optional_bool("true") is True
    assert _parse_optional_bool("false") is False


def test_parse_optional_bool_rejects_invalid_values() -> None:
    with pytest.raises(HTTPException):
        _parse_optional_bool("maybe")


def test_parse_app_sort_allows_blank_values() -> None:
    assert _parse_app_sort(None) == "title_asc"
    assert _parse_app_sort("") == "title_asc"
    assert _parse_app_sort("  ") == "title_asc"
    assert _parse_app_sort("current_players_desc") == "current_players_desc"


def test_parse_app_sort_rejects_invalid_values() -> None:
    with pytest.raises(HTTPException):
        _parse_app_sort("nope")


def test_format_admin_number_adds_commas_and_keeps_zero() -> None:
    assert _format_admin_number(None) == "-"
    assert _format_admin_number(0) == "0"
    assert _format_admin_number(1393376) == "1,393,376"


def test_format_admin_datetime_normalizes_to_utc() -> None:
    value = datetime(2026, 3, 22, 10, 6, tzinfo=timezone.utc)

    assert _format_admin_datetime(value) == "2026-03-22 10:06 UTC"


def test_format_admin_datetime_iso_converts_offset_to_utc() -> None:
    value = datetime.fromisoformat("2026-03-22T19:06:00+09:00")

    assert _format_admin_datetime_iso(value) == "2026-03-22T10:06:00+00:00"


def test_build_page_numbers_adds_ellipses_for_large_result_sets() -> None:
    assert _build_page_numbers(current_page=8, total_pages=20) == [1, None, 6, 7, 8, 9, 10, None, 20]


def test_build_url_with_query_omits_blank_values() -> None:
    url = _build_url_with_query(
        "/admin/apps",
        {"search": "", "tier": "hot", "page": 2, "per_page": 100, "sort": "title_asc"},
    )
    assert url == "/admin/apps?tier=hot&page=2&per_page=100&sort=title_asc"


def test_resolve_admin_window_disables_unavailable_ranges() -> None:
    earliest = datetime(2026, 3, 19, 2, 0, tzinfo=timezone.utc)
    latest = datetime(2026, 3, 19, 7, 0, tzinfo=timezone.utc)

    selected_window, options = _resolve_admin_window("1m", latest, earliest)

    assert selected_window == "24h"
    assert [item["value"] for item in options if item["enabled"]] == ["24h", "max"]


def test_resolve_admin_window_keeps_48h_when_history_supports_it() -> None:
    earliest = datetime(2026, 3, 17, 0, 0, tzinfo=timezone.utc)
    latest = datetime(2026, 3, 19, 7, 0, tzinfo=timezone.utc)

    selected_window, options = _resolve_admin_window("48h", latest, earliest)

    assert selected_window == "48h"
    assert [item["value"] for item in options if item["enabled"]] == ["24h", "48h", "max"]
