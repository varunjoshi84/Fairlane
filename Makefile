.PHONY: up down logs migrate test lint

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
