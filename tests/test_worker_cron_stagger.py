"""The two long backfill lanes must not run as one continuous overlap.

`bootstrap` and `registry` each carry a 330-minute budget and both mirror into
the main backend's Postgres. Starting them five minutes apart meant that for
most of every day both were running, on top of the every-5-minute `hot` lane --
enough concurrent load to make the public site time out.

Text-level parsing on purpose: the workers install only `requirements.txt`,
which has no YAML parser.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"

# Registry walks the source catalog and creates rows; bootstrap backfills the
# rows registry produced. Registry therefore has to lead.
ORDERED_LONG_LANES = ("worker-registry.yml", "worker-bootstrap.yml")


def _cron(workflow: str) -> str:
    text = (WORKFLOWS / workflow).read_text()
    match = re.search(r'cron:\s*"([^"]+)"', text)
    assert match, f"{workflow} must declare a cron schedule"
    return match.group(1)


def _start_minutes(cron: str) -> list[int]:
    """Every start time the schedule fires at, as minutes past midnight."""
    minute_field, hour_field = cron.split()[:2]
    minute = int(minute_field)

    if hour_field.startswith("*/"):
        step = int(hour_field[2:])
        hours = list(range(0, 24, step))
    elif hour_field == "*":
        hours = list(range(24))
    else:
        hours = [int(part) for part in hour_field.split(",")]

    return sorted(hour * 60 + minute for hour in hours)


@pytest.mark.parametrize("workflow", ORDERED_LONG_LANES)
def test_long_lane_declares_a_schedule(workflow: str):
    assert _start_minutes(_cron(workflow))


def test_long_backfill_lanes_do_not_start_together():
    registry, bootstrap = (_start_minutes(_cron(w)) for w in ORDERED_LONG_LANES)

    for start in bootstrap:
        gaps = [abs(start - other) for other in registry]
        assert min(gaps) >= 120, (
            "bootstrap starts within two hours of registry, so both long lanes "
            f"hammer the shared database at once (bootstrap {start}, "
            f"registry {registry})"
        )


def test_registry_leads_bootstrap_in_each_cycle():
    registry, bootstrap = (_start_minutes(_cron(w)) for w in ORDERED_LONG_LANES)

    # Bootstrap backfills what registry imported, so each bootstrap run should
    # follow a registry run rather than race ahead of it.
    for start in bootstrap:
        preceding = [other for other in registry if other < start]
        assert preceding, (
            f"bootstrap run at {start} has no registry run before it that day"
        )
