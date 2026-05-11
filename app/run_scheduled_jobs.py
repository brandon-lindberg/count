from __future__ import annotations

import argparse
import asyncio

from app.worker import (
    PIPELINE_ALL,
    PIPELINE_CHOICES,
    clear_worker_runtime_budget,
    run_due_jobs_once,
    start_worker_runtime_budget,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one GitHub Actions-friendly worker cycle for any scheduled jobs that are currently due.",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Run every scheduled job regardless of its latest recorded run time.",
    )
    parser.add_argument(
        "--pipeline",
        choices=PIPELINE_CHOICES,
        default=PIPELINE_ALL,
        help=(
            "Which job group to run: maintenance (registry + bootstrap + launch), "
            "hot / warm (player counts only), scores (Steam reviews only), or all (legacy single-process order)."
        ),
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    start_worker_runtime_budget()
    try:
        executed = await run_due_jobs_once(force_all=args.force_all, pipeline=args.pipeline)
    finally:
        clear_worker_runtime_budget()
    print("Ran jobs:", ", ".join(executed) if executed else "none")
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
