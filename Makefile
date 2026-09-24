.PHONY: up down logs migrate test lint demo-crash

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

migrate:
	alembic upgrade head

test:
	pytest -v

lint:
	ruff check .

demo-crash:
	python scripts/demo_crash.py
