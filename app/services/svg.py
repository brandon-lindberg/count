from __future__ import annotations

from html import escape


def build_history_svg(points: list, width: int = 920, height: int = 260) -> str:
    if len(points) < 2:
        return '<svg viewBox="0 0 920 260" width="100%" height="260"><text x="20" y="40" fill="#857b71" font-size="14">Not enough hourly history yet.</text></svg>'

    padding_left = 40
    padding_top = 16
    padding_bottom = 28
    chart_width = width - padding_left - 20
    chart_height = height - padding_top - padding_bottom
    highs = [point.observed_24h_high for point in points]
    lows = [point.observed_24h_low for point in points]
    minimum = min(lows)
    maximum = max(highs)
    span = max(maximum - minimum, 1)

    def x(index: int) -> float:
        return padding_left + (chart_width * index / (len(points) - 1))

    def y(value: int) -> float:
        normalized = (value - minimum) / span
        return padding_top + chart_height - normalized * chart_height

    high_points = " ".join(f"{x(index):.2f},{y(point.observed_24h_high):.2f}" for index, point in enumerate(points))
    low_points = " ".join(f"{x(index):.2f},{y(point.observed_24h_low):.2f}" for index, point in enumerate(points))
    latest_label = escape(points[-1].window_ending_at.strftime("%Y-%m-%d %H:%M UTC"))

    return f'''<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" aria-label="24 hour high and low history chart">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#151311" rx="16" />
  <line x1="{padding_left}" y1="{padding_top}" x2="{padding_left}" y2="{padding_top + chart_height}" stroke="#3d3a35" />
  <line x1="{padding_left}" y1="{padding_top + chart_height}" x2="{padding_left + chart_width}" y2="{padding_top + chart_height}" stroke="#3d3a35" />
  <polyline fill="none" stroke="#beff0a" stroke-width="3" points="{high_points}" />
  <polyline fill="none" stroke="#ffb02e" stroke-width="2.5" points="{low_points}" />
  <text x="{padding_left}" y="{height - 8}" fill="#857b71" font-size="12">Latest window end: {latest_label}</text>
  <text x="{padding_left}" y="14" fill="#beff0a" font-size="12">24h high</text>
  <text x="{padding_left + 80}" y="14" fill="#ffb02e" font-size="12">24h low</text>
</svg>'''
