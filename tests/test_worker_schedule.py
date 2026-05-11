import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.worker as worker_module
from app.models import Tier
from app.worker import commit_poll_error_state, is_scheduled_job_due


def test_is_scheduled_job_due_when_no_previous_run() -> None:
    assert is_scheduled_job_due(None, 15, datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)) is True


def test_is_scheduled_job_due_when_interval_has_elapsed() -> None:
    now = datetime(2026, 3, 20, 5, 15, tzinfo=timezone.utc)
    last_started_at = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)

    assert is_scheduled_job_due(last_started_at, 15, now) is True


def test_is_scheduled_job_due_when_interval_has_not_elapsed() -> None:
    now = datetime(2026, 3, 20, 5, 14, tzinfo=timezone.utc)
    last_started_at = datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc)

    assert is_scheduled_job_due(last_started_at, 15, now) is False


def test_runtime_budget_exhausted_job_is_due_next_cycle() -> None:
    now = datetime(2026, 3, 20, 5, 14, tzinfo=timezone.utc)
    latest_job = SimpleNamespace(
        started_at=datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc),
        error_summary=worker_module.RUNTIME_BUDGET_EXHAUSTED_ERROR,
    )

    assert worker_module.is_latest_job_run_due(latest_job, 180, now) is True


def test_recent_complete_job_is_not_due_next_cycle() -> None:
    now = datetime(2026, 3, 20, 5, 14, tzinfo=timezone.utc)
    latest_job = SimpleNamespace(
        started_at=datetime(2026, 3, 20, 5, 0, tzinfo=timezone.utc),
        error_summary=None,
    )

    assert worker_module.is_latest_job_run_due(latest_job, 180, now) is False


def test_worker_runtime_budget_has_headroom_before_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_module.settings, "worker_cycle_max_seconds", 13_200)
    worker_module.start_worker_runtime_budget(now_monotonic=1_000)

    assert worker_module.worker_runtime_budget_remaining_seconds(now_monotonic=1_100) == 13_100
    assert worker_module.has_worker_runtime_budget(min_remaining_seconds=60, now_monotonic=14_100) is True
    assert worker_module.has_worker_runtime_budget(min_remaining_seconds=60, now_monotonic=14_141) is False

    worker_module.clear_worker_runtime_budget()


@pytest.mark.asyncio
async def test_commit_poll_error_state_rolls_back_failed_sessions() -> None:
    session = AsyncMock()
    session.commit.side_effect = RuntimeError("boom")

    await commit_poll_error_state(session, 570)

    session.commit.assert_awaited_once()
    session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_queue_tier_poll_job_runs_serially(monkeypatch: pytest.MonkeyPatch) -> None:
    await worker_module.reset_tier_poll_queue_state()
    started: list[Tier] = []
    finished: list[Tier] = []
    release_hot = asyncio.Event()

    async def fake_poll_tier_job(tier: Tier, **kwargs: object) -> None:
        started.append(tier)
        if tier == Tier.hot:
            await release_hot.wait()
        finished.append(tier)

    monkeypatch.setattr(worker_module, "poll_tier_job", fake_poll_tier_job)

    assert await worker_module.queue_tier_poll_job(Tier.hot) is True
    await asyncio.sleep(0)
    assert started == [Tier.hot]

    assert await worker_module.queue_tier_poll_job(Tier.warm) is True
    assert await worker_module.queue_tier_poll_job(Tier.cold) is False
    await asyncio.sleep(0)
    assert started == [Tier.hot]

    release_hot.set()
    await worker_module.wait_for_tier_poll_queue_idle()

    assert finished == [Tier.hot, Tier.warm]
    await worker_module.reset_tier_poll_queue_state()


@pytest.mark.asyncio
async def test_queue_tier_poll_job_skips_duplicate_pending_tiers(monkeypatch: pytest.MonkeyPatch) -> None:
    await worker_module.reset_tier_poll_queue_state()
    calls: list[Tier] = []

    async def fake_poll_tier_job(tier: Tier, **kwargs: object) -> None:
        calls.append(tier)

    monkeypatch.setattr(worker_module, "poll_tier_job", fake_poll_tier_job)

    assert await worker_module.queue_tier_poll_job(Tier.warm) is True
    assert await worker_module.queue_tier_poll_job(Tier.warm) is False

    await worker_module.wait_for_tier_poll_queue_idle()

    assert calls == [Tier.warm]
    await worker_module.reset_tier_poll_queue_state()


@pytest.mark.asyncio
async def test_queue_tier_poll_job_allows_requeue_while_running(monkeypatch: pytest.MonkeyPatch) -> None:
    await worker_module.reset_tier_poll_queue_state()
    calls: list[Tier] = []
    release_first_run = asyncio.Event()

    async def fake_poll_tier_job(tier: Tier, **kwargs: object) -> None:
        calls.append(tier)
        if len(calls) == 1:
            await release_first_run.wait()

    monkeypatch.setattr(worker_module, "poll_tier_job", fake_poll_tier_job)

    assert await worker_module.queue_tier_poll_job(Tier.hot) is True
    await asyncio.sleep(0)
    assert await worker_module.queue_tier_poll_job(Tier.hot) is True

    release_first_run.set()
    await worker_module.wait_for_tier_poll_queue_idle()

    assert calls == [Tier.hot, Tier.hot]
    await worker_module.reset_tier_poll_queue_state()


def test_scheduled_job_definitions_include_pipelines() -> None:
    job_names = [job.job_name for job in worker_module.build_scheduled_job_definitions()]

    assert job_names == [
        "import_registry",
        "bootstrap_poll",
        "launch_watch_poll",
        "poll_hot",
        "poll_warm",
        "sync_user_scores",
    ]
    assert "poll_cold" not in job_names


def test_pipeline_definitions_hot_is_players_only_job() -> None:
    hot_jobs = worker_module.build_pipeline_definitions(worker_module.PIPELINE_HOT)
    assert len(hot_jobs) == 1
    assert hot_jobs[0].job_name == "poll_hot"


def test_pipeline_definitions_maintenance_excludes_tier_polls() -> None:
    names = [j.job_name for j in worker_module.build_pipeline_definitions(worker_module.PIPELINE_MAINTENANCE)]
    assert names == ["import_registry", "bootstrap_poll", "launch_watch_poll"]


@pytest.mark.asyncio
async def test_startup_sync_polls_only_hot_and_warm(monkeypatch: pytest.MonkeyPatch) -> None:
    polled_tiers: list[Tier] = []

    async def fake_import_registry_job() -> None:
        return None

    async def fake_bootstrap_poll_job() -> int:
        return 0

    async def fake_launch_watch_poll_job() -> int:
        return 0

    async def fake_poll_tier_job(tier: Tier, **kwargs: object) -> None:
        polled_tiers.append(tier)

    monkeypatch.setattr(worker_module, "import_registry_job", fake_import_registry_job)
    monkeypatch.setattr(worker_module, "bootstrap_poll_job", fake_bootstrap_poll_job)
    monkeypatch.setattr(worker_module, "launch_watch_poll_job", fake_launch_watch_poll_job)
    monkeypatch.setattr(worker_module, "poll_tier_job", fake_poll_tier_job)

    await worker_module.startup_sync()

    assert polled_tiers == [Tier.hot, Tier.warm]


def test_scheduler_registration_is_hot_warm_only(monkeypatch: pytest.MonkeyPatch) -> None:
    registered_job_ids: list[str] = []

    class FakeScheduler:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def add_job(self, *args, **kwargs) -> None:
            registered_job_ids.append(kwargs["id"])

        def start(self) -> None:
            raise RuntimeError("stop after registration")

    async def fake_bootstrap() -> None:
        return None

    async def fake_startup_sync() -> None:
        return None

    monkeypatch.setattr(worker_module, "AsyncIOScheduler", FakeScheduler)
    monkeypatch.setattr(worker_module, "bootstrap", fake_bootstrap)
    monkeypatch.setattr(worker_module, "startup_sync", fake_startup_sync)

    with pytest.raises(RuntimeError, match="stop after registration"):
        asyncio.run(worker_module.main())

    assert "poll_hot" in registered_job_ids
    assert "poll_warm" in registered_job_ids
    assert "sync_user_scores" in registered_job_ids
    assert "poll_cold" not in registered_job_ids
