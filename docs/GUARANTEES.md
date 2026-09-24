# System Guarantees

## What is Guaranteed
- **At-Least-Once Delivery**: No task is ever lost. If a worker dies mid-execution, the Reaper reclaims it and it runs again.
- **No Data Loss**: Everything is persisted in PostgreSQL before hitting Redis. If Redis is wiped, the queue rebuilds from the DB.
- **Bounded Starvation (Aging)**: Low-priority tasks gradually gain score. A priority 9 task will eventually beat a flood of new priority 1 tasks.
- **Tenant Fairness**: Weighted Fair Queuing guarantees each tenant gets their proportional share of throughput during congestion, preventing Noisy Neighbors from freezing out quiet tenants.

## What is NOT Guaranteed
- **Exactly-Once Execution**: A worker might crash *after* executing side-effects but *before* committing the SUCCEEDED status. Idempotency keys must be used.
- **Strict FIFO Ordering**: Order is only best-effort. Priorities, retries, and multi-worker parallelism scramble exact order.
- **Redis High Availability**: A single Redis node is a single point of failure for *immediate* dispatch, although the DB Reconciler prevents total data loss.

## Failure Modes Handled
- **Worker Crash**: Heartbeat expires, Reaper XAUTOCLAIMs stream messages and updates DB.
- **Redis Crash**: API saves PENDING to DB. Reconciler pushes to Redis when it returns.
- **Postgres Pause**: Workers implement retry blocks (via SQLAlchemy pooling configs) and API uses connection pooling.
- **Poison Pills**: Max attempts threshold moves crashing tasks to the Dead Letter Queue.
