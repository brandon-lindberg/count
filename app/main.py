from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_admin_auth, require_service_auth
from app.config import get_settings
from app.database import SessionLocal, get_db
from app.models import JobRun, PlayerActivityHourly, PlayerSample, Tier, TrackedApp
from app.schemas import (
    AppListResponse,
    ImportRegistryResponse,
    PlayerActivityPoint,
    PlayerSampleRead,
    PollNowResponse,
    SampleListResponse,
    SteamActivityHistoryResponse,
    TierOverrideRequest,
    TrackedAppRead,
)
from app.services.job_runs import complete_job_run, start_job_run
from app.services.partitions import ensure_future_partitions
from app.services.polling import poll_app
from app.services.registry_importer import import_registry
from app.services.rollups import SUPPORTED_WINDOWS, get_history_window_start
from app.services.steam_provider import SteamCurrentPlayersProvider
from app.services.svg import build_history_svg
from app.services.tiering import refresh_effective_tier

settings = get_settings()
app = FastAPI(title=settings.app_name)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _total_pages(total: int, per_page: int) -> int:
    return (total + per_page - 1) // per_page if total > 0 else 0


def _parse_optional_tier(value: str | None) -> Tier | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    try:
        return Tier(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid tier filter") from exc


def _parse_optional_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise HTTPException(status_code=422, detail="Invalid active filter")


def _format_admin_datetime(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _summarize_error(value: str | None, limit: int = 140) -> str:
    if not value:
        return ""
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 1].rstrip()}…"


def _job_status(job: JobRun) -> str:
    if job.failure_count > 0 or bool(job.error_summary):
        return "failed"
    if job.processed_count == 0 and job.success_count == 0:
        return "idle"
    return "ok"


def _format_job_label(job_name: str) -> str:
    return job_name.replace("_", " ")


def _build_dashboard_job_rows(recent_jobs: list[JobRun]) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    latest_success_at: dict[str, datetime] = {}
    latest_status_by_name: dict[str, str] = {}

    for job in recent_jobs:
        status = _job_status(job)
        if job.job_name not in latest_status_by_name:
            latest_status_by_name[job.job_name] = status
        if status == "ok":
            finished_at = job.finished_at or job.started_at
            if finished_at is not None and job.job_name not in latest_success_at:
                latest_success_at[job.job_name] = finished_at

    rows: list[dict[str, object]] = []
    for job in recent_jobs:
        status = _job_status(job)
        if status == "failed":
            finished_at = job.finished_at or job.started_at
            latest_success = latest_success_at.get(job.job_name)
            if finished_at is not None and latest_success is not None and latest_success > finished_at:
                status = "resolved"

        rows.append(
            {
                "job_name": job.job_name,
                "job_label": _format_job_label(job.job_name),
                "status": status,
                "started_at": _format_admin_datetime(job.started_at),
                "finished_at": _format_admin_datetime(job.finished_at),
                "processed_count": job.processed_count,
                "success_count": job.success_count,
                "failure_count": job.failure_count,
                "error_summary": job.error_summary,
                "error_preview": _summarize_error(job.error_summary),
            }
        )

    ordered_status_names = ["import_registry", "poll_hot", "poll_warm", "poll_cold"]
    status_cards = [
        {
            "job_name": job_name,
            "job_label": _format_job_label(job_name),
            "status": latest_status_by_name.get(job_name, "idle"),
        }
        for job_name in ordered_status_names
    ]

    return rows, status_cards


async def _load_tracked_app_or_404(session: AsyncSession, steam_app_id: int) -> TrackedApp:
    tracked_app = await session.scalar(select(TrackedApp).where(TrackedApp.steam_app_id == steam_app_id))
    if tracked_app is None:
        raise HTTPException(status_code=404, detail="Tracked app not found")
    return tracked_app


@app.on_event("startup")
async def on_startup() -> None:
    async with SessionLocal() as session:
        await ensure_future_partitions(session, settings.partition_months_ahead)
        await session.commit()


@app.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse("/admin", status_code=status.HTTP_302_FOUND)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/apps", response_model=AppListResponse, dependencies=[Depends(require_service_auth)])
async def list_apps(
    tier: str | None = Query(default=None),
    active: str | None = Query(default=None),
    search: str | None = Query(default=None),
    steam_app_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
) -> AppListResponse:
    parsed_tier = _parse_optional_tier(tier)
    parsed_active = _parse_optional_bool(active)
    query = select(TrackedApp)
    count_query = select(func.count()).select_from(TrackedApp)

    if parsed_tier is not None:
        query = query.where(TrackedApp.effective_tier == parsed_tier)
        count_query = count_query.where(TrackedApp.effective_tier == parsed_tier)
    if parsed_active is not None:
        query = query.where(TrackedApp.is_active.is_(parsed_active))
        count_query = count_query.where(TrackedApp.is_active.is_(parsed_active))
    if search:
        query = query.where(TrackedApp.title.ilike(f"%{search.strip()}%"))
        count_query = count_query.where(TrackedApp.title.ilike(f"%{search.strip()}%"))
    if steam_app_id is not None:
        query = query.where(TrackedApp.steam_app_id == steam_app_id)
        count_query = count_query.where(TrackedApp.steam_app_id == steam_app_id)

    total = int((await db.execute(count_query)).scalar_one())
    items = (
        await db.execute(
            query.order_by(TrackedApp.effective_tier.asc(), TrackedApp.title.asc())
            .offset((page - 1) * per_page)
            .limit(per_page)
        )
    ).scalars().all()

    return AppListResponse(
        items=[TrackedAppRead.model_validate(item) for item in items],
        total=total,
        page=page,
        per_page=per_page,
        total_pages=_total_pages(total, per_page),
    )


@app.get("/api/v1/apps/{steam_app_id}", response_model=TrackedAppRead, dependencies=[Depends(require_service_auth)])
async def get_app(steam_app_id: int, db: AsyncSession = Depends(get_db)) -> TrackedAppRead:
    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    return TrackedAppRead.model_validate(tracked_app)


@app.get("/api/v1/apps/{steam_app_id}/history", response_model=SteamActivityHistoryResponse, dependencies=[Depends(require_service_auth)])
async def get_app_history(
    steam_app_id: int,
    window: str = Query(default="1m"),
    db: AsyncSession = Depends(get_db),
) -> SteamActivityHistoryResponse:
    if window not in SUPPORTED_WINDOWS:
        raise HTTPException(status_code=400, detail=f"Unsupported window. Expected one of: {sorted(SUPPORTED_WINDOWS)}")
    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    latest_bucket = await db.scalar(
        select(func.max(PlayerActivityHourly.window_ending_at)).where(PlayerActivityHourly.steam_app_id == steam_app_id)
    )
    if latest_bucket is None:
        return SteamActivityHistoryResponse(app=TrackedAppRead.model_validate(tracked_app), window=window, points=[])

    window_start = get_history_window_start(latest_bucket, window)
    points = (
        await db.execute(
            select(PlayerActivityHourly)
            .where(
                PlayerActivityHourly.steam_app_id == steam_app_id,
                PlayerActivityHourly.window_ending_at >= window_start,
            )
            .order_by(PlayerActivityHourly.bucket_started_at.asc())
        )
    ).scalars().all()
    return SteamActivityHistoryResponse(
        app=TrackedAppRead.model_validate(tracked_app),
        window=window,
        points=[PlayerActivityPoint.model_validate(point, from_attributes=True) for point in points],
    )


@app.get("/api/v1/apps/{steam_app_id}/samples", response_model=SampleListResponse, dependencies=[Depends(require_service_auth)])
async def get_app_samples(
    steam_app_id: int,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
) -> SampleListResponse:
    await _load_tracked_app_or_404(db, steam_app_id)
    total = int(
        (
            await db.execute(
                select(func.count()).select_from(PlayerSample).where(PlayerSample.steam_app_id == steam_app_id)
            )
        ).scalar_one()
    )
    items = (
        await db.execute(
            select(PlayerSample)
            .where(PlayerSample.steam_app_id == steam_app_id)
            .order_by(PlayerSample.sampled_at.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
        )
    ).scalars().all()
    return SampleListResponse(
        items=[PlayerSampleRead.model_validate(item) for item in items],
        total=total,
        page=page,
        per_page=per_page,
        total_pages=_total_pages(total, per_page),
    )


@app.post("/api/v1/admin/import-registry", response_model=ImportRegistryResponse, dependencies=[Depends(require_admin_auth)])
async def import_registry_endpoint(db: AsyncSession = Depends(get_db)) -> ImportRegistryResponse:
    job = await start_job_run(db, "import_registry_manual")
    stats = await import_registry(db, settings)
    await complete_job_run(
        db,
        job,
        processed_count=stats.imported,
        success_count=stats.imported,
        failure_count=0,
    )
    return ImportRegistryResponse(imported=stats.imported, activated=stats.activated, deactivated=stats.deactivated)


@app.post("/api/v1/admin/apps/{steam_app_id}/poll-now", response_model=PollNowResponse, dependencies=[Depends(require_admin_auth)])
async def poll_now_endpoint(steam_app_id: int, db: AsyncSession = Depends(get_db)) -> PollNowResponse:
    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    job = await start_job_run(db, f"poll_now:{steam_app_id}")
    async with SteamCurrentPlayersProvider(timeout_seconds=settings.http_timeout_seconds) as provider:
        result = await poll_app(db, tracked_app, provider, settings)
    await complete_job_run(db, job, processed_count=1, success_count=1, failure_count=0)
    return PollNowResponse(
        steam_app_id=result.steam_app_id,
        concurrent_players=result.concurrent_players,
        sampled_at=result.sampled_at,
        effective_tier=result.effective_tier,
    )


@app.post("/api/v1/admin/apps/{steam_app_id}/tier-override", response_model=TrackedAppRead, dependencies=[Depends(require_admin_auth)])
async def set_tier_override_endpoint(
    steam_app_id: int,
    payload: TierOverrideRequest,
    db: AsyncSession = Depends(get_db),
) -> TrackedAppRead:
    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    tracked_app.manual_tier_override = payload.tier
    await refresh_effective_tier(db, tracked_app, settings)
    return TrackedAppRead.model_validate(tracked_app)


@app.get("/admin", response_class=HTMLResponse, dependencies=[Depends(require_admin_auth)])
async def admin_dashboard(request: Request, db: AsyncSession = Depends(get_db)) -> HTMLResponse:
    total_apps = int((await db.execute(select(func.count()).select_from(TrackedApp))).scalar_one())
    hot_count = int((await db.execute(select(func.count()).select_from(TrackedApp).where(TrackedApp.effective_tier == Tier.hot))).scalar_one())
    warm_count = int((await db.execute(select(func.count()).select_from(TrackedApp).where(TrackedApp.effective_tier == Tier.warm))).scalar_one())
    cold_count = int((await db.execute(select(func.count()).select_from(TrackedApp).where(TrackedApp.effective_tier == Tier.cold))).scalar_one())
    recent_jobs = (await db.execute(select(JobRun).order_by(JobRun.started_at.desc()).limit(12))).scalars().all()
    recent_job_rows, status_cards = _build_dashboard_job_rows(recent_jobs)
    unresolved_failures = [row for row in recent_job_rows if row["status"] == "failed"]
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "total_apps": total_apps,
            "hot_count": hot_count,
            "warm_count": warm_count,
            "cold_count": cold_count,
            "recent_jobs": recent_job_rows,
            "status_cards": status_cards,
            "unresolved_failures": unresolved_failures,
        },
    )


@app.get("/admin/apps", response_class=HTMLResponse, dependencies=[Depends(require_admin_auth)])
async def admin_apps(
    request: Request,
    tier: str | None = Query(default=None),
    active: str | None = Query(default=None),
    search: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    parsed_tier = _parse_optional_tier(tier)
    parsed_active = _parse_optional_bool(active)
    query = select(TrackedApp)
    if parsed_tier is not None:
        query = query.where(TrackedApp.effective_tier == parsed_tier)
    if parsed_active is not None:
        query = query.where(TrackedApp.is_active.is_(parsed_active))
    if search:
        query = query.where(TrackedApp.title.ilike(f"%{search.strip()}%"))
    apps = (await db.execute(query.order_by(TrackedApp.effective_tier.asc(), TrackedApp.title.asc()).limit(500))).scalars().all()
    return templates.TemplateResponse(
        "apps.html",
        {
            "request": request,
            "apps": apps,
            "selected_tier": parsed_tier.value if parsed_tier else "",
            "selected_active": "" if parsed_active is None else ("true" if parsed_active else "false"),
            "search": search or "",
        },
    )


@app.get("/admin/apps/{steam_app_id}", response_class=HTMLResponse, dependencies=[Depends(require_admin_auth)])
async def admin_app_detail(steam_app_id: int, request: Request, db: AsyncSession = Depends(get_db)) -> HTMLResponse:
    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    samples = (
        await db.execute(
            select(PlayerSample)
            .where(PlayerSample.steam_app_id == steam_app_id)
            .order_by(PlayerSample.sampled_at.desc())
            .limit(100)
        )
    ).scalars().all()
    history_points = (
        await db.execute(
            select(PlayerActivityHourly)
            .where(PlayerActivityHourly.steam_app_id == steam_app_id)
            .order_by(PlayerActivityHourly.bucket_started_at.desc())
            .limit(96)
        )
    ).scalars().all()
    history_points = list(reversed(history_points))
    return templates.TemplateResponse(
        "app_detail.html",
        {
            "request": request,
            "app_item": tracked_app,
            "samples": samples,
            "history_points": history_points,
            "history_svg": build_history_svg(history_points),
            "tier_options": [tier.value for tier in Tier],
        },
    )


@app.post("/admin/actions/import-registry", dependencies=[Depends(require_admin_auth)])
async def admin_import_registry(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    await import_registry_endpoint(db)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/actions/clear-job-history", dependencies=[Depends(require_admin_auth)])
async def admin_clear_job_history(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    await db.execute(delete(JobRun))
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/apps/{steam_app_id}/poll", dependencies=[Depends(require_admin_auth)])
async def admin_poll_app(steam_app_id: int, db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    await poll_now_endpoint(steam_app_id, db)
    return RedirectResponse(f"/admin/apps/{steam_app_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/apps/{steam_app_id}/tier", dependencies=[Depends(require_admin_auth)])
async def admin_set_tier(
    steam_app_id: int,
    tier: str = Form(default=""),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    payload = TierOverrideRequest(tier=Tier(tier) if tier else None)
    await set_tier_override_endpoint(steam_app_id, payload, db)
    return RedirectResponse(f"/admin/apps/{steam_app_id}", status_code=status.HTTP_303_SEE_OTHER)
