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

demo-fairness:
	python scripts/demo_fairness.py

demo-load:
	python scripts/demo_load.py

grafana:
	@echo "Grafana Dashboard is available at: http://localhost:3000/d/fairlane-overview/fairlane-overview"

demo-priority:
	python scripts/demo_priority.py
