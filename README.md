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
