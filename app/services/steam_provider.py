from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from html import unescape
import re

import httpx


class SteamProviderError(RuntimeError):
    pass


class SteamAppNotFoundError(SteamProviderError):
    pass


@dataclass(slots=True)
class SteamUserScore:
    score: Decimal
    score_raw: str
    sample_size: int
    positive_count: int
    negative_count: int
    review_score_desc: str
    scraped_at: datetime


class SteamCurrentPlayersProvider:
    BASE_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    REVIEWS_BASE_URL = "https://store.steampowered.com/appreviews"
    APP_PAGE_URL = "https://store.steampowered.com/app"

    def __init__(self, timeout_seconds: int = 20, client: httpx.AsyncClient | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "SteamCurrentPlayersProvider":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
        if self._owns_client:
            self._client = None

    async def get_current_players(self, steam_app_id: int) -> int:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = await self._client.get(self.BASE_URL, params={"appid": steam_app_id})
                if response.status_code == 404:
                    raise SteamAppNotFoundError(f"Steam current players endpoint returned 404 for app {steam_app_id}")
                response.raise_for_status()
                payload = response.json()
                data = payload.get("response") or {}
                player_count = data.get("player_count")
                if player_count is None:
                    raise SteamProviderError(f"Missing player_count for app {steam_app_id}")
                return int(player_count)
            except SteamAppNotFoundError:
                raise
            except (ValueError, TypeError, httpx.HTTPError, SteamProviderError) as exc:
                last_error = exc
                if attempt == 2:
                    break
                await asyncio.sleep(1 + attempt)
        raise SteamProviderError(f"Failed to fetch current players for app {steam_app_id}: {last_error}")

    @staticmethod
    def _score_to_desc(score: Decimal) -> str:
        value = float(score)
        if value >= 95:
            return "Overwhelmingly Positive"
        if value >= 80:
            return "Very Positive"
        if value >= 70:
            return "Mostly Positive"
        if value >= 40:
            return "Mixed"
        if value >= 20:
            return "Mostly Negative"
        return "Overwhelmingly Negative"

    @staticmethod
    def _parse_review_summary_from_html(html: str) -> dict[str, int | str] | None:
        text = unescape(html or "")
        patterns = [
            r"(\d{1,3})% of the ([\d,]+) user reviews for this game are positive",
            r"(\d{1,3})% of the ([\d,]+) user reviews in your language are positive",
            r"(\d{1,3})% of the ([\d,]+) user reviews [^\"<]* are positive",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            pct = int(match.group(1))
            total = int(match.group(2).replace(",", ""))
            if total <= 0:
                continue
            positive = int(round((pct / 100) * total))
            negative = max(total - positive, 0)
            return {
                "total_reviews": total,
                "total_positive": positive,
                "total_negative": negative,
                "review_score_desc": SteamCurrentPlayersProvider._score_to_desc(Decimal(str(pct))),
            }
        return None

    async def _get_review_summary_from_store_page(self, steam_app_id: int) -> dict[str, int | str] | None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)
        try:
            response = await self._client.get(
                f"{self.APP_PAGE_URL}/{steam_app_id}/",
                params={"l": "english", "cc": "us"},
            )
            response.raise_for_status()
            return self._parse_review_summary_from_html(response.text)
        except Exception:
            return None

    async def get_user_score(self, steam_app_id: int) -> SteamUserScore | None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_seconds)

        request_variants = [
            {"json": "1", "language": "all", "purchase_type": "all", "num_per_page": "0"},
            {"json": "1", "filter": "summary", "language": "all", "purchase_type": "all", "num_per_page": "1"},
            {"json": "1", "language": "all", "purchase_type": "all", "num_per_page": "1"},
        ]

        summary: dict[str, int | str] | None = None
        for params in request_variants:
            try:
                response = await self._client.get(f"{self.REVIEWS_BASE_URL}/{steam_app_id}", params=params)
                response.raise_for_status()
                payload = response.json()
                if payload.get("success") and payload.get("query_summary") is not None:
                    summary = payload.get("query_summary")
                    break
            except Exception:
                continue

        if summary is None:
            summary = await self._get_review_summary_from_store_page(steam_app_id)
        if not summary:
            return None

        total = int(summary.get("total_reviews") or 0)
        if total <= 0:
            return None
        positive = int(summary.get("total_positive") or 0)
        negative = int(summary.get("total_negative") or 0)
        denominator = positive + negative
        if denominator <= 0:
            return None

        score = Decimal(str(round((positive / denominator) * 100, 2)))
        desc = str(summary.get("review_score_desc") or self._score_to_desc(score))
        return SteamUserScore(
            score=score,
            score_raw=f"{positive}/{total}",
            sample_size=total,
            positive_count=positive,
            negative_count=negative,
            review_score_desc=desc,
            scraped_at=datetime.now(timezone.utc),
        )
