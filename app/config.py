from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
    )

    app_name: str = 'Steam Player Count Scraper'
    environment: str = 'development'
    debug: bool = False
    database_url: str = 'postgresql+asyncpg://localhost:5432/player_count_scraper'
    database_echo: bool = False
    mirror_database_url: str | None = None
    mirror_database_use_ssl: bool = False

    source_api_base_url: str = 'http://localhost:8000'
    source_api_token: str | None = None
    source_api_page_size: int = 100

    service_api_token: str | None = None
    admin_username: str | None = 'admin'
    admin_password: str | None = 'admin'
    disable_auth: bool = True

    hot_poll_minutes: int = 10
    warm_poll_minutes: int = 60
    cold_poll_minutes: int = 360
    bootstrap_poll_minutes: int = 5
    registry_import_hours: int | None = None
    registry_import_minutes: int = 15
    release_hot_days: int = 3
    hot_threshold: int = 10000
    warm_threshold: int = 500
    poll_batch_limit: int = 250
    bootstrap_batch_limit: int = 500
    startup_bootstrap_max_batches: int = 25
    not_found_retry_hours: int = 24
    launch_not_found_retry_minutes: int = 15
    launch_priority_past_days: int = 3
    launch_priority_future_days: int = 2
    http_timeout_seconds: int = 20
    partition_months_ahead: int = 2


    @field_validator("debug", mode="before")
    @classmethod
    def coerce_debug_value(cls, value: object) -> object:
        if not isinstance(value, str):
            return value

        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on", "debug", "development", "dev"}:
            return True
        if normalized in {"false", "0", "no", "off", "release", "production", "prod"}:
            return False
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
