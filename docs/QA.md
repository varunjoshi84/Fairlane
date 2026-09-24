# System Q&A

**Q: Why not just use Celery?**
A: Celery is fantastic, but Fairlane offers built-in multi-tenant fairness (WFQ) and priority aging at the scheduling layer, without requiring isolated worker pools per tenant.

**Q: What happens if the dispatcher loop crashes?**
A: The dispatcher runs in a background asyncio task on the workers, guarded by a Redis lock. If the worker holding the lock crashes, the lock expires (10s TTL), and another worker immediately takes over dispatching.

**Q: Is exactly-once execution guaranteed?**
A: No. Distributed systems cannot guarantee exactly-once delivery without distributed transactions. Fairlane guarantees *at-least-once*. We provide an `idempotency_key` field to help your application deduplicate executions.

**Q: Why is Redis Streams kept shallow?**
A: If all tasks lived in the stream, they would be processed strictly FIFO. By keeping the stream shallow, tasks wait in the ZSETs (Waiting Rooms), allowing the Dispatcher to continuously re-evaluate priorities and WFQ weights right up until the moment of execution.

**Q: What if Redis goes completely down?**
A: The API will still accept tasks and store them as PENDING in PostgreSQL. A background reconciler loop detects tasks that have been PENDING for too long and safely re-enqueues them to Redis once it recovers.
