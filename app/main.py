from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit, urlunsplit

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
from app.services.main_db_mirror import backfill_tracked_app_to_main_db, mirror_is_configured
from app.services.backfill_runtime import get_dashboard_backfill_status, start_dashboard_backfill
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
WINDOW_OPTIONS = ["24h", "48h", "1w", "1m", "3m", "6m", "1y"]
APP_SORT_OPTIONS = {
    "title_asc": "A-Z",
    "title_desc": "Z-A",
    "current_players_desc": "Highest current players",
    "current_players_asc": "Lowest current players",
    "all_time_peak_desc": "Highest all-time peak",
}
DEFAULT_APP_SORT = "title_asc"


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


def _parse_app_sort(value: str | None) -> str:
    if value is None:
        return DEFAULT_APP_SORT
    normalized = value.strip().lower()
    if not normalized:
        return DEFAULT_APP_SORT
    if normalized not in APP_SORT_OPTIONS:
        raise HTTPException(status_code=422, detail="Invalid sort filter")
    return normalized


def _tracked_app_order_by(sort_value: str) -> tuple[object, ...]:
    if sort_value == "title_desc":
        return (TrackedApp.title.desc(), TrackedApp.steam_app_id.asc())
    if sort_value == "current_players_desc":
        return (
            TrackedApp.last_known_players.desc().nulls_last(),
            TrackedApp.title.asc(),
            TrackedApp.steam_app_id.asc(),
        )
    if sort_value == "current_players_asc":
        return (
            TrackedApp.last_known_players.asc().nulls_last(),
            TrackedApp.title.asc(),
            TrackedApp.steam_app_id.asc(),
        )
    if sort_value == "all_time_peak_desc":
        return (
            TrackedApp.all_time_peak_players.desc().nulls_last(),
            TrackedApp.title.asc(),
            TrackedApp.steam_app_id.asc(),
        )
    return (TrackedApp.title.asc(), TrackedApp.steam_app_id.asc())


def _build_page_numbers(current_page: int, total_pages: int, radius: int = 2) -> list[int | None]:
    if total_pages <= 1:
        return []

    start = max(1, current_page - radius)
    end = min(total_pages, current_page + radius)
    page_numbers: list[int | None] = []

    if start > 1:
        page_numbers.append(1)
        if start > 2:
            page_numbers.append(None)

    page_numbers.extend(range(start, end + 1))

    if end < total_pages:
        if end < total_pages - 1:
            page_numbers.append(None)
        page_numbers.append(total_pages)

    return page_numbers


def _build_url_with_query(path: str, params: dict[str, object]) -> str:
    clean_params: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                continue
            clean_params[key] = stripped
            continue
        clean_params[key] = str(value)

    query = urlencode(clean_params)
    return f"{path}?{query}" if query else path


def _format_admin_datetime(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _format_admin_number(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def _window_is_available(
    window: str,
    latest_bucket: datetime | None,
    earliest_bucket: datetime | None,
) -> bool:
    if latest_bucket is None or earliest_bucket is None:
        return window == "24h"
    if window == "24h":
        return True
    return earliest_bucket <= get_history_window_start(latest_bucket, window)


def _resolve_admin_window(
    requested_window: str,
    latest_bucket: datetime | None,
    earliest_bucket: datetime | None,
) -> tuple[str, list[dict[str, object]]]:
    window_options = [
        {
            "value": window,
            "enabled": _window_is_available(window, latest_bucket, earliest_bucket),
        }
        for window in WINDOW_OPTIONS
    ]

    enabled_windows = [item["value"] for item in window_options if item["enabled"]]
    selected_window = requested_window if requested_window in enabled_windows else (enabled_windows[-1] if enabled_windows else "24h")

    for item in window_options:
        item["active"] = item["value"] == selected_window

    return selected_window, window_options


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

    ordered_status_names = ["import_registry", "bootstrap_poll", "launch_watch_poll", "poll_hot", "poll_warm", "poll_cold"]
    status_cards = [
        {
            "job_name": job_name,
            "job_label": _format_job_label(job_name),
            "status": latest_status_by_name.get(job_name, "idle"),
        }
        for job_name in ordered_status_names
    ]

    return rows, status_cards


def _redact_url_credentials(value: str | None) -> str:
    if not value:
        return "-"
    parsed = urlsplit(value)
    if not parsed.scheme:
        return value
    host = parsed.hostname or ""
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


templates.env.filters["comma_number"] = _format_admin_number


def _build_latest_job_health_cards(recent_jobs: list[JobRun]) -> list[dict[str, str]]:
    latest_jobs: dict[str, JobRun] = {}
    latest_success_at: dict[str, datetime] = {}

    for job in recent_jobs:
        if job.job_name not in latest_jobs:
            latest_jobs[job.job_name] = job
        if _job_status(job) == "ok" and job.job_name not in latest_success_at:
            latest_success_at[job.job_name] = job.finished_at or job.started_at

    ordered_status_names = ["import_registry", "bootstrap_poll", "launch_watch_poll", "poll_hot", "poll_warm", "poll_cold"]
    cards: list[dict[str, str]] = []
    for job_name in ordered_status_names:
        job = latest_jobs.get(job_name)
        if job is None:
            cards.append(
                {
                    "job_label": _format_job_label(job_name),
                    "status": "idle",
                    "last_run": "-",
                    "last_success": "-",
                    "notes": "No recorded runs yet.",
                }
            )
            continue

        status = _job_status(job)
        notes = "Healthy" if status == "ok" else ("No apps were due in the latest run." if status == "idle" else _summarize_error(job.error_summary))
        cards.append(
            {
                "job_label": _format_job_label(job_name),
                "status": status,
                "last_run": _format_admin_datetime(job.finished_at or job.started_at),
                "last_success": _format_admin_datetime(latest_success_at.get(job_name)),
                "notes": notes or "-",
            }
        )

    return cards


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
    sort: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
) -> AppListResponse:
    parsed_tier = _parse_optional_tier(tier)
    parsed_active = _parse_optional_bool(active)
    parsed_sort = _parse_app_sort(sort)
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
            query.order_by(*_tracked_app_order_by(parsed_sort))
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
            "mirror_configured": mirror_is_configured(settings),
            "backfill_status": get_dashboard_backfill_status(),
            "action_notice": request.query_params.get("notice"),
            "action_notice_level": request.query_params.get("notice_level", "ok"),
        },
    )


@app.get("/admin/health", response_class=HTMLResponse, dependencies=[Depends(require_admin_auth)])
async def admin_health(request: Request, db: AsyncSession = Depends(get_db)) -> HTMLResponse:
    await db.execute(select(1))

    total_apps = int((await db.execute(select(func.count()).select_from(TrackedApp))).scalar_one())
    active_apps = int((await db.execute(select(func.count()).select_from(TrackedApp).where(TrackedApp.is_active.is_(True)))).scalar_one())
    sampled_apps = int((await db.execute(select(func.count()).select_from(TrackedApp).where(TrackedApp.last_success_at.is_not(None)))).scalar_one())
    apps_with_errors = int((await db.execute(select(func.count()).select_from(TrackedApp).where(TrackedApp.last_error.is_not(None)))).scalar_one())

    recent_jobs = (await db.execute(select(JobRun).order_by(JobRun.started_at.desc()).limit(20))).scalars().all()
    job_health_cards = _build_latest_job_health_cards(recent_jobs)

    checks = [
        {
            "label": "API",
            "status": "ok",
            "detail": "Admin UI and service endpoints are responding.",
            "meta": "Raw endpoint: /health",
        },
        {
            "label": "Database",
            "status": "ok",
            "detail": "Postgres connection succeeded for this request.",
            "meta": _redact_url_credentials(settings.database_url),
        },
        {
            "label": "Source Backend",
            "status": "ok" if settings.source_api_base_url else "failed",
            "detail": "Registry sync pulls tracked games from the main backend API.",
            "meta": settings.source_api_base_url or "Not configured",
        },
        {
            "label": "Admin Auth",
            "status": "warn" if settings.disable_auth else "ok",
            "detail": "Dashboard authentication mode.",
            "meta": "Disabled for local development" if settings.disable_auth else "Basic auth enabled",
        },
        {
            "label": "Service Auth",
            "status": "warn" if not settings.service_api_token else "ok",
            "detail": "Token protection for scraper API consumers.",
            "meta": "No bearer token configured" if not settings.service_api_token else "Bearer token configured",
        },
        {
            "label": "Worker Telemetry",
            "status": "ok" if recent_jobs else "idle",
            "detail": "Background jobs report into the admin dashboard.",
            "meta": "Recent jobs found" if recent_jobs else "No job runs recorded yet",
        },
    ]

    return templates.TemplateResponse(
        "health.html",
        {
            "request": request,
            "total_apps": total_apps,
            "active_apps": active_apps,
            "sampled_apps": sampled_apps,
            "apps_with_errors": apps_with_errors,
            "checks": checks,
            "job_health_cards": job_health_cards,
        },
    )


@app.get("/admin/apps", response_class=HTMLResponse, dependencies=[Depends(require_admin_auth)])
async def admin_apps(
    request: Request,
    tier: str | None = Query(default=None),
    active: str | None = Query(default=None),
    search: str | None = Query(default=None),
    sort: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=250),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    parsed_tier = _parse_optional_tier(tier)
    parsed_active = _parse_optional_bool(active)
    parsed_sort = _parse_app_sort(sort)
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

    total = int((await db.execute(count_query)).scalar_one())
    total_pages = _total_pages(total, per_page)
    current_page = min(page, total_pages) if total_pages > 0 else 1
    row_offset = (current_page - 1) * per_page
    row_start = row_offset + 1 if total > 0 else 0
    row_end = min(total, row_offset + per_page)

    apps = (
        await db.execute(
            query.order_by(*_tracked_app_order_by(parsed_sort))
            .offset(row_offset)
            .limit(per_page)
        )
    ).scalars().all()

    base_params = {
        "search": search or "",
        "tier": parsed_tier.value if parsed_tier else "",
        "active": "" if parsed_active is None else ("true" if parsed_active else "false"),
        "sort": parsed_sort,
        "per_page": per_page,
    }
    page_numbers = _build_page_numbers(current_page, total_pages)

    return templates.TemplateResponse(
        "apps.html",
        {
            "request": request,
            "apps": apps,
            "selected_tier": parsed_tier.value if parsed_tier else "",
            "selected_active": "" if parsed_active is None else ("true" if parsed_active else "false"),
            "selected_sort": parsed_sort,
            "sort_options": [{"value": value, "label": label} for value, label in APP_SORT_OPTIONS.items()],
            "selected_per_page": per_page,
            "per_page_options": [50, 100, 250],
            "search": search or "",
            "total": total,
            "current_page": current_page,
            "total_pages": total_pages,
            "row_offset": row_offset,
            "row_start": row_start,
            "row_end": row_end,
            "page_numbers": [
                {
                    "value": page_number,
                    "current": page_number == current_page,
                    "url": _build_url_with_query(request.url.path, {**base_params, "page": page_number}),
                }
                if page_number is not None
                else {"value": None, "current": False, "url": ""}
                for page_number in page_numbers
            ],
            "first_page_url": _build_url_with_query(request.url.path, {**base_params, "page": 1}) if total_pages > 1 else "",
            "previous_page_url": _build_url_with_query(request.url.path, {**base_params, "page": current_page - 1}) if current_page > 1 else "",
            "next_page_url": _build_url_with_query(request.url.path, {**base_params, "page": current_page + 1}) if current_page < total_pages else "",
            "last_page_url": _build_url_with_query(request.url.path, {**base_params, "page": total_pages}) if total_pages > 1 else "",
        },
    )


@app.get("/admin/apps/{steam_app_id}", response_class=HTMLResponse, dependencies=[Depends(require_admin_auth)])
async def admin_app_detail(
    steam_app_id: int,
    request: Request,
    window: str = Query(default="1m"),
    notice: str | None = Query(default=None),
    notice_level: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    if window not in SUPPORTED_WINDOWS:
        raise HTTPException(status_code=400, detail=f"Unsupported window. Expected one of: {sorted(SUPPORTED_WINDOWS)}")
    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    samples = (
        await db.execute(
            select(PlayerSample)
            .where(PlayerSample.steam_app_id == steam_app_id)
            .order_by(PlayerSample.sampled_at.desc())
            .limit(100)
        )
    ).scalars().all()
    latest_bucket = await db.scalar(
        select(func.max(PlayerActivityHourly.window_ending_at)).where(PlayerActivityHourly.steam_app_id == steam_app_id)
    )
    earliest_bucket = await db.scalar(
        select(func.min(PlayerActivityHourly.window_ending_at)).where(PlayerActivityHourly.steam_app_id == steam_app_id)
    )
    selected_window, window_options = _resolve_admin_window(window, latest_bucket, earliest_bucket)
    history_points: list[PlayerActivityHourly] = []
    if latest_bucket is not None:
        window_start = get_history_window_start(latest_bucket, selected_window)
        history_points = (
            await db.execute(
                select(PlayerActivityHourly)
                .where(
                    PlayerActivityHourly.steam_app_id == steam_app_id,
                    PlayerActivityHourly.window_ending_at >= window_start,
                )
                .order_by(PlayerActivityHourly.bucket_started_at.asc())
            )
        ).scalars().all()

    return templates.TemplateResponse(
        "app_detail.html",
        {
            "request": request,
            "app_item": tracked_app,
            "samples": samples,
            "history_points": history_points,
            "history_svg": build_history_svg(history_points, window=selected_window),
            "tier_options": [tier.value for tier in Tier],
            "window_options": window_options,
            "selected_window": selected_window,
            "latest_window_end": _format_admin_datetime(history_points[-1].window_ending_at) if history_points else "-",
            "mirror_configured": mirror_is_configured(settings),
            "action_notice": notice,
            "action_notice_level": notice_level or "ok",
        },
    )


@app.post("/admin/actions/import-registry", dependencies=[Depends(require_admin_auth)])
async def admin_import_registry(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    await import_registry_endpoint(db)
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/actions/backfill-main-db", dependencies=[Depends(require_admin_auth)])
async def admin_backfill_main_db_all(
    only_unmirrored: str | None = Form(default=None),
) -> RedirectResponse:
    resolved_only_unmirrored = only_unmirrored is not None
    try:
        started = await start_dashboard_backfill(
            batch_size=1000,
            only_unmirrored=resolved_only_unmirrored,
            settings=settings,
        )
    except RuntimeError as exc:
        return RedirectResponse(
            _build_url_with_query("/admin", {"notice": str(exc), "notice_level": "warn"}),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    if not started:
        notice = "A full main DB backfill is already running."
        level = "warn"
    else:
        if resolved_only_unmirrored:
            notice = "Full main DB backfill started in the background for only-unmirrored games."
        else:
            notice = "Full main DB backfill started in the background for all games."
        level = "ok"

    return RedirectResponse(
        _build_url_with_query("/admin", {"notice": notice, "notice_level": level}),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.get("/admin/backfill-status", dependencies=[Depends(require_admin_auth)])
async def admin_backfill_status() -> dict[str, object]:
    return get_dashboard_backfill_status()


@app.post("/admin/actions/clear-job-history", dependencies=[Depends(require_admin_auth)])
async def admin_clear_job_history(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    await db.execute(delete(JobRun))
    return RedirectResponse("/admin", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/apps/{steam_app_id}/poll", dependencies=[Depends(require_admin_auth)])
async def admin_poll_app(steam_app_id: int, db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    await poll_now_endpoint(steam_app_id, db)
    return RedirectResponse(f"/admin/apps/{steam_app_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/apps/{steam_app_id}/backfill-main-db", dependencies=[Depends(require_admin_auth)])
async def admin_backfill_app_to_main_db(
    steam_app_id: int,
    window: str = Form(default="1m"),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    redirect_params = {"window": window}

    if not mirror_is_configured(settings):
        return RedirectResponse(
            _build_url_with_query(
                f"/admin/apps/{steam_app_id}",
                {
                    **redirect_params,
                    "notice": "Main DB mirror is not configured in the scraper environment.",
                    "notice_level": "warn",
                },
            ),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    tracked_app = await _load_tracked_app_or_404(db, steam_app_id)
    job = await start_job_run(db, f"backfill_main_db:{steam_app_id}")

    try:
        stats = await backfill_tracked_app_to_main_db(db, tracked_app, settings=settings)
        await complete_job_run(
            db,
            job,
            processed_count=stats.raw_samples_seen + stats.range_points_seen,
            success_count=stats.raw_samples_inserted + stats.range_points_upserted,
            failure_count=0,
        )

        if stats.apps_skipped_unmapped:
            notice = "Backfill skipped because no matching main DB game row was found."
            notice_level = "warn"
        else:
            notice = (
                "Main DB backfill completed. "
                f"Inserted {stats.raw_samples_inserted:,} raw samples and upserted {stats.range_points_upserted:,} range rows."
            )
            notice_level = "ok"
    except Exception as exc:
        await complete_job_run(
            db,
            job,
            processed_count=0,
            success_count=0,
            failure_count=1,
            error_summary=str(exc)[:2000],
        )
        notice = f"Main DB backfill failed: {str(exc)[:240]}"
        notice_level = "warn"

    return RedirectResponse(
        _build_url_with_query(
            f"/admin/apps/{steam_app_id}",
            {
                **redirect_params,
                "notice": notice,
                "notice_level": notice_level,
            },
        ),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.post("/admin/apps/{steam_app_id}/tier", dependencies=[Depends(require_admin_auth)])
async def admin_set_tier(
    steam_app_id: int,
    tier: str = Form(default=""),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    payload = TierOverrideRequest(tier=Tier(tier) if tier else None)
    await set_tier_override_endpoint(steam_app_id, payload, db)
    return RedirectResponse(f"/admin/apps/{steam_app_id}", status_code=status.HTTP_303_SEE_OTHER)
