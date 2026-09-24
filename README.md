# Fairlane

Fairlane is a lightweight, distributed task queue engine built with **Python 3.11+**, **FastAPI**, **PostgreSQL 16**, **Redis 7 (Streams)**, and **SQLAlchemy 2.0 (asyncio)**.

---

## Architecture Overview

- **FastAPI API**: Receives task submission requests, persists initial `PENDING` tasks and audit logs in PostgreSQL, and pushes task events to a Redis Stream (`tasks:stream`).
- **Async Workers**: Scalable background workers consuming from the Redis Stream via `XREADGROUP`, updating state to `RUNNING` and `SUCCEEDED`, and logging task lifecycle events into `task_events`.
- **PostgreSQL 16**: Durable storage for tasks, metadata, status, execution timestamps, and audit events.
- **Redis 7 (Streams)**: Real-time message transport and consumer group coordination.
- **Alembic**: Database migrations.
- **Typer CLI**: Command-line interface for submitting tasks and querying task execution state.

---

## Project Structure

```text
FairLane/
├── Dockerfile                  # Python container definition
├── Makefile                    # Target shortcuts (up, down, logs, migrate, test, lint)
├── README.md                   # Project documentation and quick start
├── alembic.ini                 # Alembic configuration
├── alembic/                    # Database migrations
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── 0001_initial_schema.py
├── docker-compose.yml          # Postgres, Redis, API, and scalable Worker services
├── pyproject.toml              # Project dependencies and tool configurations (ruff, pytest)
├── requirements.txt            # Dependency list for installation
├── src/fairlane/
│   ├── __init__.py
│   ├── config.py               # Pydantic-settings configuration
│   ├── db.py                   # Async SQLAlchemy engine, sessions, healthcheck
│   ├── models.py               # SQLAlchemy ORM models (Task, TaskEvent, TaskStatus)
│   ├── schemas.py              # Pydantic schemas for requests and responses
│   ├── redis_client.py         # Redis client and stream helpers
│   ├── logging_setup.py        # Structured JSON logger
│   ├── cli.py                  # Typer CLI (submit, status)
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py             # FastAPI entrypoint and lifespan
│   │   └── routes.py           # API endpoints (/health, /tasks, /tasks/{id})
│   └── worker/
│       ├── __init__.py
│       └── main.py             # Distributed asyncio worker daemon
└── tests/
    ├── __init__.py
    ├── test_health.py          # Health check endpoint test
    └── test_submit_task.py     # Task creation and retrieval tests
```

---

## Quick Start

### 1. Setup Environment
```bash
cp .env.example .env
```

### 2. Start Services with Docker Compose (with 3 Workers)
```bash
docker compose up --build --scale worker=3
```
Or start in the background using Make:
```bash
make up
```

Interactive API documentation will be available at: **[http://localhost:8000/docs](http://localhost:8000/docs)**

---

## Example `curl` Commands

### 1. Health Check
```bash
curl http://localhost:8000/health
```
**Response**:
```json
{
  "status": "ok",
  "postgres": "connected",
  "redis": "connected"
}
```

### 2. Submit a Task
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "tenant-alpha",
    "task_type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome to Fairlane"
    },
    "priority": 2
  }'
```
**Response**:
```json
{
  "task_id": "65631ce2-f26d-4f5d-900c-e8a875a6dacb"
}
```

### 3. Get Task Status and Details
```bash
curl http://localhost:8000/tasks/65631ce2-f26d-4f5d-900c-e8a875a6dacb
```
**Response**:
```json
{
  "id": "65631ce2-f26d-4f5d-900c-e8a875a6dacb",
  "tenant_id": "tenant-alpha",
  "task_type": "send_email",
  "payload": {
    "to": "user@example.com",
    "subject": "Welcome to Fairlane"
  },
  "priority": 2,
  "status": "SUCCEEDED",
  "attempts": 1,
  "max_attempts": 5,
  "idempotency_key": null,
  "created_at": "2026-09-24T08:15:49.720285Z",
  "updated_at": "2026-09-24T08:15:50.301140Z",
  "started_at": "2026-09-24T08:15:49.788612Z",
  "finished_at": "2026-09-24T08:15:50.294315Z",
  "last_error": null
}
```

---

## CLI Usage

Fairlane comes with a Typer-based CLI:

### Submit a Task
```bash
fairlane submit --tenant "tenant-alpha" --type "send_email" --priority 1 --payload '{"to":"user@example.com"}'
```

### Check Task Status
```bash
fairlane status <task_id>
```

---

## Development Commands

- **Run Database Migrations**:
  ```bash
  make migrate
  ```
- **Run Tests**:
  ```bash
  make test
  ```
- **Run Linter (Ruff)**:
  ```bash
  make lint
  ```
- **View Container Logs**:
  ```bash
  make logs
  ```
- **Tear Down Containers**:
  ```bash
  make down
  ```
