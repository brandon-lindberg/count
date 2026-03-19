from app.services.polling import STEAM_APP_NOT_FOUND_PREFIX, format_poll_error
from app.services.steam_provider import SteamAppNotFoundError


def test_format_poll_error_marks_not_found_errors() -> None:
    message = format_poll_error(SteamAppNotFoundError("Steam current players endpoint returned 404 for app 17933"))

    assert message.startswith(STEAM_APP_NOT_FOUND_PREFIX)


def test_format_poll_error_preserves_other_errors() -> None:
    assert format_poll_error(RuntimeError("boom")) == "boom"
