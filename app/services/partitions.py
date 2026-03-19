from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def month_start(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month, day=1)


async def ensure_partition_for_timestamp(session: AsyncSession, sampled_at: datetime) -> None:
    start = month_start(sampled_at)
    end = add_months(start, 1)
    table_name = f"player_samples_{start:%Y_%m}"
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    PARTITION OF player_samples
    FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');
    """
    await session.execute(text(sql))


async def ensure_future_partitions(session: AsyncSession, months_ahead: int, anchor: datetime | None = None) -> None:
    reference = month_start(anchor or datetime.now(timezone.utc))
    for offset in range(-1, months_ahead + 1):
        await ensure_partition_for_timestamp(session, add_months(reference, offset))
