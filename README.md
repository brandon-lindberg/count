# Steam Player Count Scraper

Separate FastAPI service for polling Steam concurrent player counts, storing raw history in Postgres, deriving rolling 24-hour high/low series, and exposing a small admin UI plus a read-only API for the main app.

## Stack

- FastAPI
- SQLAlchemy async
- Alembic
- APScheduler
- PostgreSQL

## Local setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
createdb player_count_scraper
alembic upgrade head
make run-api
```

In a second terminal:

```bash
source venv/bin/activate
make run-worker
```

`make run-worker` is still the original long-running local worker. It starts APScheduler and keeps running on your machine until you stop it.

If you want to locally simulate the GitHub Actions path, run one short cycle instead:

```bash
source venv/bin/activate
make run-worker-once
```

## Render deployment

This repo includes a Render Blueprint in [render.yaml](/Users/lindbergbrandon/player-count-scraper/render.yaml) that creates:

- one FastAPI web service
- one managed Postgres database

The Blueprint also:

- runs `alembic upgrade head` before each web deploy
- uses `/health` as the web health check
- disables preview environments to avoid extra spend
- normalizes Render Postgres URLs to `postgresql+asyncpg://...` at runtime

### Required values during Blueprint creation

Render will prompt you for these values when you create the Blueprint:

- `SOURCE_API_BASE_URL`
- `SERVICE_API_TOKEN`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`

### Optional production values

Add these manually in the Render dashboard if your environment needs them:

- `SOURCE_API_TOKEN`
- `MIRROR_DATABASE_URL`
- `MIRROR_DATABASE_USE_SSL=true`

### Deploy flow

1. Push this repo to GitHub.
2. In Render, create a new Blueprint and point it at this repo.
3. Fill in the prompted secrets above.
4. Deploy the Blueprint.

After the first deploy, the API will connect to the Render Postgres instance via `DATABASE_URL`.

## GitHub Actions worker

The long-running worker is set up to run from GitHub Actions instead of Render so you do not pay Render for a background worker.

This repo includes [worker.yml](/Users/lindbergbrandon/player-count-scraper/.github/workflows/worker.yml), which runs a one-shot worker cycle every 30 minutes and can also be triggered manually.
It also includes [worker-test.yml](/Users/lindbergbrandon/player-count-scraper/.github/workflows/worker-test.yml), a manual-only smoke workflow that caps registry import to 2 pages for fast validation.

This does not replace the original local worker:

- local development: `make run-worker`
- GitHub-hosted scheduled cycle: `make run-worker-once` / `.github/workflows/worker.yml`

### Why this shape

GitHub-hosted runners are free for public repositories on standard runners, but they are not a good fit for a permanently running daemon:

- scheduled workflows can run no more often than every 5 minutes
- scheduled workflows run only from the default branch
- each GitHub-hosted job has a 6 hour execution limit

Because of that, the workflow runs short scheduled cycles instead of `python -m app.worker` as a forever process.

The GitHub worker uses the same DB-backed due-job checks as the local worker. With the production defaults, hot games are eligible every 30 minutes, warm games every 60 minutes, and cold games every 180 minutes. The workflow itself wakes up every 30 minutes, so skipped jobs exit quickly when their tier is not due yet.

To reduce timeout risk on GitHub-hosted runners (especially during throttled periods), the workflow also sets conservative per-cycle limits (`POLL_BATCH_LIMIT=150`, `BOOTSTRAP_BATCH_LIMIT=300`) and a 45-minute job timeout.
The worker now also syncs Steam user score metadata (score, sample size, sentiment counts, review description) and mirrors those fields into the backend DB.
For rollout safety, the GitHub worker currently disables cold-tier polling (`ENABLE_COLD_POLLING=false`) and only runs hot/warm tier polls.

### GitHub repository settings

Set these before enabling the workflow:

- Repository secret `DATABASE_URL`
  Use your Render Postgres external URL. The workflow normalizes `postgresql://...` to `postgresql+asyncpg://...` automatically.
- Repository variable `SOURCE_API_BASE_URL`
  This should point at the upstream backend that exposes `/api/v1/games`.

Optional GitHub settings:

- Repository secret `SOURCE_API_TOKEN`
- Repository variable `MIRROR_DATABASE_USE_SSL=true`
- Repository variable `HTTP_TIMEOUT_SECONDS=45` (or `60` if timeouts persist)
- Repository variable `SOURCE_API_PAGE_SIZE=50` (smaller pages reduce registry import timeout risk)

Required for backend DB updates from GitHub worker:

- Repository secret `MIRROR_DATABASE_URL`

The workflow enforces `REQUIRE_MIRROR_SUCCESS=true` in CI so worker runs fail loudly if mirror writes fail.

### Notes

- If your default branch on GitHub is `master`, scheduled workflows will run from `master`.
- In public repositories, GitHub may automatically disable scheduled workflows after 60 days of no repository activity.

## Main DB mirror

If you want successful scraper polls mirrored into the main backend database, set these in the scraper `.env`:

```bash
MIRROR_DATABASE_URL=postgresql+asyncpg://...
MIRROR_DATABASE_USE_SSL=true
```

Then restart the scraper worker:

```bash
source venv/bin/activate
make run-worker
```

## One-time historical backfill

Future successful polls mirror automatically once the mirror DB is configured. To copy existing scraper history into the main backend DB without waiting for repolls, run:

```bash
source venv/bin/activate
make backfill-main-db
```

Optional scope controls:

```bash
python3 -m app.backfill_main_db --steam-app-id 3065800
python3 -m app.backfill_main_db --limit-apps 100
python3 -m app.backfill_main_db --batch-size 500
python3 -m app.backfill_main_db --only-unmirrored
```

If a Steam app ID has been reused or the backend registry is still pointed at the wrong game row, pin the scraper entry manually before backfilling:

```bash
python3 -m app.manual_track_app \
  --steam-app-id 3764200 \
  --title "Resident Evil Requiem" \
  --source-game-id 36423 \
  --source-game-public-id be724651782747f6b4c6f992e34be774 \
  --release-date 2026-02-27
python3 -m app.backfill_main_db --steam-app-id 3764200
```

Manual rows are preserved across registry imports, so stale backend metadata will not overwrite the manual rebind.

The command is idempotent:

- raw player snapshots are inserted only when that `game_id + sampled_at` pair does not already exist
- range snapshots are upserted on `game_id + sampled_at`
- the current summary fields on `games` are refreshed from the scraper’s latest known values
- `--only-unmirrored` skips games that already have any mirrored Steam history in the main DB

The admin dashboard also includes a `Backfill All Games` button with a live progress bar and status polling.
The admin app detail page includes a `Backfill Main DB` button for one-game backfills.

## Main endpoints

- `GET /health`
- `GET /api/v1/apps`
- `GET /api/v1/apps/{steam_app_id}`
- `GET /api/v1/apps/{steam_app_id}/history`
- `GET /api/v1/apps/{steam_app_id}/samples`
- `POST /api/v1/admin/import-registry`
- `POST /api/v1/admin/apps/{steam_app_id}/poll-now`
- `POST /api/v1/admin/apps/{steam_app_id}/tier-override`

## Admin UI

- `/admin`
- `/admin/apps`
- `/admin/apps/{steam_app_id}`

## Notes

- Raw samples are retained indefinitely.
- `player_samples` is monthly partitioned by `sampled_at`.
- Steam polling uses the official `GetNumberOfCurrentPlayers` endpoint rather than HTML scraping.
- Never-sampled apps are handled by a dedicated bootstrap queue that runs more frequently than the normal hot/warm/cold tier polls so new titles get a first sample quickly.
- On startup, the worker drains multiple bootstrap batches so an initial local run seeds broad coverage instead of waiting hours for the first sample backlog to clear.
- Registry sync now runs on a short minute-based interval so new games from the main backend are imported quickly.
- Upcoming and newly released games are placed in a launch-watch queue that polls on a short interval even before they have a stable tier.
- Steam `404` responses for launch-watch titles are retried quickly instead of waiting 24 hours, which avoids missing day-one launches.
- Newly released games are forced into the `hot` tier for the first 3 days after their backend `release_date`, then fall back to threshold-based tiering automatically.
- Games released within the last 15 months keep a `warm` minimum tier even when their player counts would otherwise place them in `cold`.
