import asyncio
from datetime import datetime, timezone
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

    async def fake_poll_tier_job(tier: Tier) -> None:
        started.append(tier)
        if tier == Tier.hot:
            await release_hot.wait()
        finished.append(tier)

    monkeypatch.setattr(worker_module, "poll_tier_job", fake_poll_tier_job)

    assert await worker_module.queue_tier_poll_job(Tier.hot) is True
    await asyncio.sleep(0)
    assert started == [Tier.hot]

    assert await worker_module.queue_tier_poll_job(Tier.warm) is True
    assert await worker_module.queue_tier_poll_job(Tier.cold) is True
    await asyncio.sleep(0)
    assert started == [Tier.hot]

    release_hot.set()
    await worker_module.wait_for_tier_poll_queue_idle()

    assert finished == [Tier.hot, Tier.warm, Tier.cold]
    await worker_module.reset_tier_poll_queue_state()


@pytest.mark.asyncio
async def test_queue_tier_poll_job_skips_duplicate_pending_tiers(monkeypatch: pytest.MonkeyPatch) -> None:
    await worker_module.reset_tier_poll_queue_state()
    calls: list[Tier] = []

    async def fake_poll_tier_job(tier: Tier) -> None:
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

    async def fake_poll_tier_job(tier: Tier) -> None:
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
