import httpx
import pytest

from app.services.steam_provider import SteamAppNotFoundError, SteamCurrentPlayersProvider


@pytest.mark.asyncio
async def test_get_current_players_parses_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["appid"] == "730"
        return httpx.Response(200, json={"response": {"player_count": 12345, "result": 1}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = SteamCurrentPlayersProvider(client=client)
        count = await provider.get_current_players(730)
    assert count == 12345


@pytest.mark.asyncio
async def test_get_current_players_raises_not_found_without_retries() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(404, json={"response": {}})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = SteamCurrentPlayersProvider(client=client)
        with pytest.raises(SteamAppNotFoundError):
            await provider.get_current_players(17933)

    assert attempts == 1
