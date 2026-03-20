from __future__ import annotations

from datetime import datetime, timezone
from html import escape


WINDOW_LABELS = {
    "24h": "24 hours",
    "48h": "48 hours",
    "1w": "1 week",
    "1m": "1 month",
    "3m": "3 months",
    "6m": "6 months",
    "1y": "1 year",
}


def _format_player_count(value: int) -> str:
    if value >= 1_000_000:
        label = f"{value / 1_000_000:.1f}M"
        return label.replace(".0M", "M")
    if value >= 1_000:
        label = f"{value / 1_000:.1f}k"
        return label.replace(".0k", "k")
    return str(value)


def _format_tick_datetime(value: datetime, span_hours: float) -> str:
    utc_value = value.astimezone(timezone.utc)
    if span_hours <= 48:
        return utc_value.strftime("%d %b %H:%M UTC")
    if span_hours <= 24 * 120:
        return utc_value.strftime("%d %b")
    if span_hours <= 24 * 365:
        return utc_value.strftime("%b %Y")
    return utc_value.strftime("%Y")


def _build_tick_indices(point_count: int, max_ticks: int = 6) -> list[int]:
    if point_count <= 1:
        return [0]
    tick_slots = min(max_ticks, point_count)
    indices = {0, point_count - 1}
    if tick_slots > 2:
        for slot in range(1, tick_slots - 1):
            index = round(slot * (point_count - 1) / (tick_slots - 1))
            indices.add(index)
    return sorted(indices)


def build_history_svg(points: list, window: str = "1m", width: int = 920, height: int = 320) -> str:
    if len(points) < 2:
        return (
            '<svg viewBox="0 0 920 320" width="100%" height="320">'
            '<rect x="0" y="0" width="920" height="320" fill="#151311" rx="16" />'
            '<text x="24" y="40" fill="#857b71" font-size="14">Not enough hourly history yet.</text>'
            '</svg>'
        )

    padding_left = 28
    padding_right = 72
    padding_top = 42
    padding_bottom = 56
    chart_width = width - padding_left - padding_right
    chart_height = height - padding_top - padding_bottom

    start_time = points[0].window_ending_at
    end_time = points[-1].window_ending_at
    span_hours = max((end_time - start_time).total_seconds() / 3600, 1)

    latest_values = [point.latest_players for point in points]
    minimum = min(latest_values)
    maximum = max(latest_values)
    span = max(maximum - minimum, 1)
    chart_min = max(0, int(minimum - span * 0.15))
    chart_max = int(maximum + span * 0.15)
    chart_span = max(chart_max - chart_min, 1)

    def x(index: int) -> float:
        return padding_left + (chart_width * index / (len(points) - 1))

    def y(value: int) -> float:
        normalized = (value - chart_min) / chart_span
        return padding_top + chart_height - normalized * chart_height

    y_tick_values = [round(chart_min + chart_span * step / 4) for step in range(5)]
    x_tick_indices = _build_tick_indices(len(points))

    current_points = " ".join(f"{x(index):.2f},{y(point.latest_players):.2f}" for index, point in enumerate(points))
    area_points = " ".join(
        [f"{x(index):.2f},{y(point.latest_players):.2f}" for index, point in enumerate(points)]
        + [f"{padding_left + chart_width:.2f},{padding_top + chart_height:.2f}", f"{padding_left:.2f},{padding_top + chart_height:.2f}"]
    )

    y_grid = []
    for value in y_tick_values:
        y_position = y(value)
        y_grid.append(
            f'<line x1="{padding_left}" y1="{y_position:.2f}" x2="{padding_left + chart_width}" y2="{y_position:.2f}" stroke="#3d3a35" stroke-dasharray="4 6" />'
        )
        y_grid.append(
            f'<text x="{padding_left + chart_width + 8}" y="{y_position + 4:.2f}" fill="#a79b8e" font-size="12">{escape(_format_player_count(value))}</text>'
        )

    x_grid = []
    x_labels = []
    for index in x_tick_indices:
        point = points[index]
        x_position = x(index)
        label = escape(_format_tick_datetime(point.window_ending_at, span_hours))
        x_grid.append(
            f'<line x1="{x_position:.2f}" y1="{padding_top}" x2="{x_position:.2f}" y2="{padding_top + chart_height}" stroke="#292520" />'
        )
        x_labels.append(
            f'<text x="{x_position:.2f}" y="{height - 18}" fill="#a79b8e" font-size="12" text-anchor="middle">{label}</text>'
        )

    range_label = escape(
        f"{points[0].window_ending_at.astimezone(timezone.utc).strftime('%d %b %Y')} to "
        f"{points[-1].window_ending_at.astimezone(timezone.utc).strftime('%d %b %Y')}"
    )
    window_label = escape(WINDOW_LABELS.get(window, window))

    return f'''<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" aria-label="current player count history chart">
  <defs>
    <linearGradient id="history-band" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#7dd3fc" stop-opacity="0.2" />
      <stop offset="100%" stop-color="#7dd3fc" stop-opacity="0.03" />
    </linearGradient>
  </defs>
  <rect x="0" y="0" width="{width}" height="{height}" fill="#151311" rx="16" />
  <text x="{padding_left}" y="20" fill="#efe9e1" font-size="14" font-weight="700">Current players</text>
  <text x="{padding_left}" y="36" fill="#857b71" font-size="12">Viewing {window_label} of hourly history • {range_label}</text>
  {''.join(x_grid)}
  {''.join(y_grid)}
  <line x1="{padding_left}" y1="{padding_top + chart_height}" x2="{padding_left + chart_width}" y2="{padding_top + chart_height}" stroke="#4a433b" />
  <polygon fill="url(#history-band)" points="{area_points}" />
  <polyline fill="none" stroke="#7dd3fc" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round" points="{current_points}" />
  <circle cx="{x(len(points) - 1):.2f}" cy="{y(points[-1].latest_players):.2f}" r="4" fill="#7dd3fc" />
  {''.join(x_labels)}
</svg>'''
