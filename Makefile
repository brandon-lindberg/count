PYTHON ?= python3

.PHONY: run-api run-worker run-worker-once migrate test backfill-main-db

run-api:
	uvicorn app.main:app --reload --port 8100

run-worker:
	python3 -m app.worker

run-worker-once:
	python3 -m app.run_scheduled_jobs

backfill-main-db:
	python3 -m app.backfill_main_db

migrate:
	alembic upgrade head

test:
	pytest
