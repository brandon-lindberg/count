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
