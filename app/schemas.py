from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict

from app.models import Tier


class TrackedAppRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_game_public_id: Optional[str] = None
    source_game_id: Optional[int] = None
    steam_app_id: int
    title: str
    is_active: bool
    import_source: str
    manual_tier_override: Optional[Tier] = None
    effective_tier: Tier
    last_imported_at: Optional[datetime] = None
    last_polled_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_error: Optional[str] = None
    last_known_players: Optional[int] = None
    latest_24h_high: Optional[int] = None
    latest_24h_low: Optional[int] = None


class PlayerSampleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    tracked_app_id: int
    steam_app_id: int
    sampled_at: datetime
    concurrent_players: int
    provider: str


class PlayerActivityPoint(BaseModel):
    bucket_started_at: datetime
    window_ending_at: datetime
    observed_24h_high: int
    observed_24h_low: int
    latest_players: int
    sample_count: int
    effective_tier_at_capture: Tier


class SteamActivityHistoryResponse(BaseModel):
    app: TrackedAppRead
    window: str
    points: list[PlayerActivityPoint]


class TierOverrideRequest(BaseModel):
    tier: Optional[Tier] = None


class ImportRegistryResponse(BaseModel):
    imported: int
    activated: int
    deactivated: int


class PollNowResponse(BaseModel):
    steam_app_id: int
    concurrent_players: int
    sampled_at: datetime
    effective_tier: Tier


class AppListResponse(BaseModel):
    items: list[TrackedAppRead]
    total: int
    page: int
    per_page: int
    total_pages: int


class SampleListResponse(BaseModel):
    items: list[PlayerSampleRead]
    total: int
    page: int
    per_page: int
    total_pages: int
