PYTHON ?= python3

.PHONY: run-api run-worker migrate test

run-api:
	uvicorn app.main:app --reload --port 8100

run-worker:
	python3 -m app.worker

migrate:
	alembic upgrade head

test:
	pytest
