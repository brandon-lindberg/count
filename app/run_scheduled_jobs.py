from __future__ import annotations

import argparse
import asyncio

from app.worker import run_due_jobs_once


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one GitHub Actions-friendly worker cycle for any scheduled jobs that are currently due.",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Run every scheduled job regardless of its latest recorded run time.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    executed = await run_due_jobs_once(force_all=args.force_all)
    print("Ran jobs:", ", ".join(executed) if executed else "none")
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
