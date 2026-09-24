.PHONY: up down logs migrate test lint demo-crash demo-fairness demo-priority report demo-all ci

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

migrate:
	.venv/bin/alembic upgrade head

test:
	.venv/bin/pytest -v

lint:
	.venv/bin/ruff check .

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

report:
	python scripts/report.py

demo-all:
	python scripts/demo_all.py

ci: lint test
	python scripts/chaos/run_all.py
