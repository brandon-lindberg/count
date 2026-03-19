from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import JobRun


async def start_job_run(session: AsyncSession, job_name: str) -> JobRun:
    job = JobRun(job_name=job_name)
    session.add(job)
    await session.flush()
    return job


async def complete_job_run(
    session: AsyncSession,
    job: JobRun,
    *,
    processed_count: int,
    success_count: int,
    failure_count: int,
    error_summary: str | None = None,
) -> None:
    job.finished_at = datetime.now(timezone.utc)
    job.processed_count = processed_count
    job.success_count = success_count
    job.failure_count = failure_count
    job.error_summary = error_summary
    await session.flush()
