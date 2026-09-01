"""The mirror engine must not open an unbounded share of the backend's pool.

Every scheduled worker in this repo mirrors into the main backend's Postgres.
On SQLAlchemy defaults each worker process can hold pool_size 5 + max_overflow
10 = 15 connections there, and several long-running workers overlap by design
(bootstrap and registry run up to 330 minutes, warm up to 240). That is enough
concurrent connections to starve the web service that serves the public site.
"""

from __future__ import annotations

import pytest

from app.config import Settings
from app.services import main_db_mirror


@pytest.fixture
def captured_engine_kwargs(monkeypatch):
    """Reset the module-level engine cache and record engine construction."""
    monkeypatch.setattr(main_db_mirror, "_mirror_engine", None)
    monkeypatch.setattr(main_db_mirror, "_mirror_session_factory", None)
    monkeypatch.setattr(main_db_mirror, "_mirror_url", None)
    monkeypatch.setattr(main_db_mirror, "_mirror_ssl", None)

    recorded: dict[str, object] = {}

    def _fake_create_async_engine(url, **kwargs):
        recorded["url"] = url
        recorded.update(kwargs)
        return object()

    monkeypatch.setattr(
        main_db_mirror, "create_async_engine", _fake_create_async_engine
    )
    monkeypatch.setattr(
        main_db_mirror, "async_sessionmaker", lambda *a, **k: object()
    )
    return recorded


def _settings(**overrides) -> Settings:
    base = {
        "mirror_database_url": "postgresql+asyncpg://u:p@backend.example/db",
        "mirror_database_use_ssl": False,
    }
    base.update(overrides)
    return Settings(**base)


def test_mirror_engine_caps_its_connection_pool(captured_engine_kwargs):
    main_db_mirror._get_mirror_session_factory(_settings())

    assert captured_engine_kwargs["pool_size"] == 2
    assert captured_engine_kwargs["max_overflow"] == 3


def test_mirror_engine_pool_total_stays_well_under_the_default(
    captured_engine_kwargs,
):
    main_db_mirror._get_mirror_session_factory(_settings())

    total = (
        captured_engine_kwargs["pool_size"]
        + captured_engine_kwargs["max_overflow"]
    )
    assert total <= 5, "a single worker must not hold 15 backend connections"


def test_mirror_engine_recycles_and_times_out_waiting_checkouts(
    captured_engine_kwargs,
):
    main_db_mirror._get_mirror_session_factory(_settings())

    # Without a timeout a saturated pool blocks the worker forever instead of
    # failing the job; without recycling, idle connections outlive the server's
    # own timeouts and come back dead.
    assert captured_engine_kwargs["pool_timeout"] == 30
    assert captured_engine_kwargs["pool_recycle"] == 1800


def test_mirror_pool_limits_are_configurable(captured_engine_kwargs):
    main_db_mirror._get_mirror_session_factory(
        _settings(mirror_pool_size=1, mirror_max_overflow=1)
    )

    assert captured_engine_kwargs["pool_size"] == 1
    assert captured_engine_kwargs["max_overflow"] == 1
