import pytest
from fastapi import HTTPException

from app.main import _parse_optional_bool, _parse_optional_tier
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
