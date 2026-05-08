from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Enum as SqlEnum, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Tier(str, Enum):
    hot = "hot"
    warm = "warm"
    cold = "cold"


class TrackedApp(Base):
    __tablename__ = "tracked_apps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_game_public_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    source_game_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    source_release_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    steam_app_id: Mapped[int] = mapped_column(Integer, unique=True, index=True, nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    import_source: Mapped[str] = mapped_column(String(64), nullable=False, default="backend_api", server_default=text("'backend_api'"))
    manual_tier_override: Mapped[Optional[Tier]] = mapped_column(SqlEnum(Tier, name="tier_enum", native_enum=False))
    effective_tier: Mapped[Tier] = mapped_column(SqlEnum(Tier, name="tier_enum", native_enum=False), nullable=False, default=Tier.warm, server_default=text("'warm'"))
    last_imported_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_polled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    last_known_players: Mapped[Optional[int]] = mapped_column(Integer)
    all_time_peak_players: Mapped[Optional[int]] = mapped_column(Integer)
    all_time_peak_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    latest_24h_high: Mapped[Optional[int]] = mapped_column(Integer)
    latest_24h_low: Mapped[Optional[int]] = mapped_column(Integer)
    steam_user_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    steam_score_raw: Mapped[Optional[str]] = mapped_column(String(50))
    steam_sample_size: Mapped[Optional[int]] = mapped_column(Integer)
    steam_positive_count: Mapped[Optional[int]] = mapped_column(Integer)
    steam_negative_count: Mapped[Optional[int]] = mapped_column(Integer)
    steam_review_score_desc: Mapped[Optional[str]] = mapped_column(String(100))
    steam_score_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    samples: Mapped[list["PlayerSample"]] = relationship(back_populates="tracked_app")
    activity_points: Mapped[list["PlayerActivityHourly"]] = relationship(back_populates="tracked_app")


class PlayerSample(Base):
    __tablename__ = "player_samples"
    __table_args__ = (
        UniqueConstraint("steam_app_id", "sampled_at", name="uq_player_samples_app_time"),
        Index("idx_player_samples_lookup", "steam_app_id", "sampled_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tracked_app_id: Mapped[int] = mapped_column(ForeignKey("tracked_apps.id", ondelete="CASCADE"), nullable=False, index=True)
    steam_app_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sampled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False, index=True)
    concurrent_players: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False, default="steam_current_players", server_default=text("'steam_current_players'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    tracked_app: Mapped["TrackedApp"] = relationship(back_populates="samples")


class PlayerActivityHourly(Base):
    __tablename__ = "player_activity_hourly"
    __table_args__ = (
        UniqueConstraint("steam_app_id", "bucket_started_at", name="uq_player_activity_hourly_app_bucket"),
        Index("idx_player_activity_hourly_lookup", "steam_app_id", "bucket_started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tracked_app_id: Mapped[int] = mapped_column(ForeignKey("tracked_apps.id", ondelete="CASCADE"), nullable=False, index=True)
    steam_app_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    bucket_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    window_ending_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_24h_high: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_24h_low: Mapped[int] = mapped_column(Integer, nullable=False)
    latest_players: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    effective_tier_at_capture: Mapped[Tier] = mapped_column(SqlEnum(Tier, name="tier_enum", native_enum=False), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    tracked_app: Mapped["TrackedApp"] = relationship(back_populates="activity_points")


class JobRun(Base):
    __tablename__ = "job_runs"
    __table_args__ = (Index("idx_job_runs_name_started", "job_name", "started_at"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_name: Mapped[str] = mapped_column(String(128), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    processed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    error_summary: Mapped[Optional[str]] = mapped_column(Text)
