# Design Decisions

## 1. PostgreSQL as Source of Truth
Redis is fast but volatile. By insisting that all state transitions and audit logs (TaskEvents) hit Postgres first, we guarantee no tasks are lost. Redis is treated purely as a volatile scheduling cache and transport layer.

## 2. Redis Streams vs Lists
Streams provide Consumer Groups, enabling at-least-once delivery semantics via `XACK` and `XAUTOCLAIM`. If a worker dies, `XAUTOCLAIM` allows another worker to safely reclaim the exact task. Lists (`BLPOP`) remove the message instantly, making crash recovery brittle.

## 3. Lua Script for WFQ
Weighted Fair Queuing requires checking tenant credits, picking the lowest virtual-time tenant, grabbing their lowest-score task, updating credits, and pushing to the Stream. Doing this via round-trips in Python would cause race conditions. A Lua script executes atomically inside Redis.

## 4. Priority Aging
Static priorities lead to starvation (a flood of Priority 1 tasks means Priority 9 never runs). By scoring tasks as `Timestamp + Penalty`, old low-priority tasks eventually have lower scores than fresh high-priority tasks.

## 5. Token Bucket Rate Limiting
Allows burst capacity for tenants. Implemented in Redis Lua for atomicity. Checked independently by workers before processing a task.
