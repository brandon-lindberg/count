from __future__ import annotations

import asyncio

import httpx


class SteamProviderError(RuntimeError):
    pass


class SteamAppNotFoundError(SteamProviderError):
    pass


class SteamCurrentPlayersProvider:
    BASE_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

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
