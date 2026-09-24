# Fairlane

**Fairlane** is a high-performance, fault-tolerant distributed task queue engineered specifically for B2B SaaS applications. It guarantees at-least-once execution semantics, strict tenant isolation via Weighted Fair Queuing (WFQ), high-performance API rate limiting, and automated crash recovery.

By combining the transactional safety of PostgreSQL with the extreme low-latency capabilities of Redis Streams, Fairlane achieves a hybrid architecture capable of absorbing massive traffic spikes without dropping a single task.

---

## High-Level Architecture

Fairlane decouples task ingestion, storage, fast-path coordination, and background processing into scalable layers.

```mermaid
flowchart TD
    %% Styling
    classDef client fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef api fill:#e1f5fe,stroke:#0288d1,stroke-width:2px;
    classDef db fill:#e8f5e9,stroke:#388e3c,stroke-width:2px;
    classDef redis fill:#ffebee,stroke:#d32f2f,stroke-width:2px;
    classDef worker fill:#fff3e0,stroke:#f57c00,stroke-width:2px;
    classDef obs fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px;

    Client([Clients / Tenants]):::client

    subgraph API_Layer [API Layer]
        FastAPI[FastAPI Service]:::api
    end

    subgraph Persistence_Layer [PostgreSQL - Source of Truth]
        PG_WaitingRoom[(Waiting Room<br>PENDING Tasks)]:::db
        PG_Audit[(Task Events<br>Audit Trail)]:::db
        PG_DLQ[(Dead Letter Queue)]:::db
    end

    subgraph Coordination_Layer [Redis - Fast Path]
        Redis_Stream[[Redis Stream<br>tasks:stream]]:::redis
        Redis_RateLimit{{Token Bucket<br>Lua Script}}:::redis
        Redis_Locks{{Locks & Heartbeats}}:::redis
    end

    subgraph Worker_Daemon [Worker Daemon]
        Dispatcher((Dispatcher Loop)):::worker
        Consumer((Consumer Loop)):::worker
        Scheduler((Scheduler Loop)):::worker
        Reaper((Reaper Loop)):::worker
    end

    subgraph Observability [Monitoring Stack]
        Prometheus([Prometheus]):::obs
        Grafana([Grafana Dashboard]):::obs
    end

    %% Data Flow - Submission
    Client -- "1. POST /tasks" --> FastAPI
    FastAPI -- "2. Persist safely" --> PG_WaitingRoom

    %% Data Flow - Dispatching
    Dispatcher -- "3. Poll PENDING" --> PG_WaitingRoom
    Dispatcher -- "4. Check Limits" --> Redis_RateLimit
    Dispatcher -- "5. Push Task" --> Redis_Stream

    %% Data Flow - Consumption
    Consumer -- "6. XREADGROUP" --> Redis_Stream
    Consumer -- "7. Execute & Update" --> PG_Audit

    %% Data Flow - Background Processes
    Scheduler -- "Retry backoff" --> PG_WaitingRoom
    Reaper -- "Monitor Heartbeats" --> Redis_Locks
    Reaper -- "XAUTOCLAIM orphaned" --> Redis_Stream
    Consumer -- "Send Heartbeats" --> Redis_Locks
    Consumer -- "Move dead tasks" --> PG_DLQ

    %% Observability Flow
    Prometheus -. "Scrape Metrics" .-> FastAPI
    Prometheus -. "Scrape Metrics" .-> Worker_Daemon
    Grafana -. "Query Data" .-> Prometheus
```

---

## Key Features

- **Multi-Tenancy & Weighted Fair Queuing (WFQ)**: Prevents the "noisy neighbor" problem. If Tenant A floods the system with 10,000 tasks, Tenant B's tasks will still process concurrently without getting stuck at the back of the queue.
- **Atomic Rate Limiting**: Uses a custom Redis Lua script to enforce a strict Token Bucket algorithm per tenant, guaranteeing fair usage limits without race conditions.
- **Persistence First**: All tasks are persisted to PostgreSQL before an HTTP response is returned. Zero data loss even in the event of a total Redis failure.
- **Crash Recovery (The Reaper)**: Worker processes emit Redis heartbeats. If a worker container crashes or is OOM-killed, the singleton Reaper process detects the dead heartbeat, reclaims the orphaned tasks via `XAUTOCLAIM`, and automatically re-queues them.
- **Exponential Backoff & Dead Letter Queue (DLQ)**: Transient failures are caught and retried automatically with exponential backoff and jitter. Tasks that fail permanently (or crash the worker repeatedly) are safely quarantined to the DLQ.

---

## Quick Start (Docker Compose)

Fairlane is fully containerized. A single command will spin up the API, Worker, PostgreSQL, Redis, Prometheus, Grafana, and Adminer.

### 1. Start the stack
```bash
docker compose up --build
```

### 2. View Interactive API Docs (Swagger UI)
Once the containers are running, navigate to:
**[http://localhost:8000/docs](http://localhost:8000/docs)**

### 3. Generate Demo Traffic
To test the queue under load, open a new terminal and run the provided load generation script. This will inject 10 tasks per second for 120 seconds with a randomized 10% failure rate (to demonstrate exponential backoff):
```bash
python scripts/demo_load.py --rate 10 --duration 120
```

### 4. View Observability Dashboards
Fairlane comes with a pre-configured Grafana dashboard that automatically visualizes queue health, per-tenant throughput, latency, and rate-limiting.
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (No login required)
- **Prometheus**: [http://localhost:9090](http://localhost:9090)

### 5. View Database (Adminer)
You can directly inspect the PostgreSQL tables (including the `tasks`, `task_events` audit trail, and `dead_letters`).
- **Adminer**: [http://localhost:8080](http://localhost:8080)
- **System**: PostgreSQL
- **Server**: `postgres`
- **Username**: `fairlane`
- **Password**: `fairlane_secret`
- **Database**: `fairlane`

---

## API Examples

### Submit a Task
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "tenant-A",
    "task_type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Hello World"
    },
    "priority": 5
  }'
```

### Check Task Status
```bash
curl http://localhost:8000/tasks/<task_id>
```

### Get Full Audit Timeline
```bash
curl http://localhost:8000/tasks/<task_id>/timeline
```

---

## Production Deployment
Fairlane includes a `render.yaml` Blueprint for zero-downtime deployments on Render. It provisions managed PostgreSQL and Redis instances alongside horizontally scalable API and Worker services.

```yaml
# deploy via render CLI or GitHub integration
```
