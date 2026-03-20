from app.backfill_main_db import _build_progress_prefix, _format_duration, build_parser


def test_format_duration_handles_minutes_and_hours() -> None:
    assert _format_duration(9) == "00:09"
    assert _format_duration(65) == "01:05"
    assert _format_duration(3661) == "01:01:01"


def test_build_progress_prefix_includes_percent_and_eta() -> None:
    prefix = _build_progress_prefix(index=5, total=20, started_at=0.0)

    assert "5/20" in prefix
    assert "25.0%" in prefix
    assert "ETA" in prefix
    assert "[" in prefix and "]" in prefix


def test_parser_supports_only_unmirrored_flag() -> None:
    args = build_parser().parse_args(["--only-unmirrored", "--limit-apps", "25"])

    assert args.only_unmirrored is True
    assert args.limit_apps == 25
