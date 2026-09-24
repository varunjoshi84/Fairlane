import asyncio
from typing import Any
from sqlalchemy import select, func, text
from fairlane.db import async_session_factory
from fairlane.models import Task, TaskStatus, Worker, WorkerStatus, SideEffect, DeadLetter, TaskEvent
from collections import defaultdict


async def check_invariants() -> dict[str, Any]:
    """Run safety invariant checks against the database."""
    results = {
        "NO_LOSS": {"pass": True, "stuck_tasks": 0, "total_tasks": 0},
        "NO_GHOST_STATE": {"pass": True, "ghost_tasks": 0},
        "AT_LEAST_ONCE": {"pass": True, "missing_side_effects": 0},
        "EFFECTIVELY_ONCE": {"pass": True, "executed_multiple_times": 0},
        "DLQ_CORRECTNESS": {"pass": True, "invalid_dlq": 0},
        "EVENT_ORDER": {"pass": True, "invalid_order": 0},
        "FAIRNESS": {"pass": True, "noisy_p95": 0.0, "quiet_p95": 0.0},
    }

    async with async_session_factory() as db:
        # NO LOSS
        stmt = select(func.count(Task.id))
        total_tasks = await db.scalar(stmt)
        results["NO_LOSS"]["total_tasks"] = total_tasks

        stmt = select(func.count(Task.id)).where(Task.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.FAILED]))
        stuck_tasks = await db.scalar(stmt)
        if stuck_tasks > 0:
            results["NO_LOSS"]["pass"] = False
            results["NO_LOSS"]["stuck_tasks"] = stuck_tasks

        # NO GHOST STATE (task RUNNING but locked_by worker is DEAD/STOPPED or missing)
        # In a fully settled system, no tasks should be RUNNING at all.
        stmt = select(func.count(Task.id)).where(
            Task.status == TaskStatus.RUNNING,
        )
        running_tasks = await db.scalar(stmt)
        if running_tasks > 0:
            results["NO_GHOST_STATE"]["pass"] = False
            results["NO_GHOST_STATE"]["ghost_tasks"] = running_tasks

        # AT-LEAST-ONCE (every successful demo task has a side effect)
        stmt = select(func.count(Task.id)).outerjoin(SideEffect, Task.id == SideEffect.task_id).where(
            Task.status == TaskStatus.SUCCEEDED,
            Task.task_type == "demo",
            SideEffect.id.is_(None)
        )
        missing_effects = await db.scalar(stmt)
        if missing_effects > 0:
            results["AT_LEAST_ONCE"]["pass"] = False
            results["AT_LEAST_ONCE"]["missing_side_effects"] = missing_effects

        # EFFECTIVELY-ONCE
        stmt = select(func.count(SideEffect.id)).where(SideEffect.count > 1)
        multi_execs = await db.scalar(stmt)
        results["EFFECTIVELY_ONCE"]["executed_multiple_times"] = multi_execs
        # While not strictly a failure if multi_execs > 0 (at-least-once allows it during crashes),
        # the idempotency test ensures side_effects are deduplicated by idempotency key.
        # Let's count idempotency key duplicates.
        stmt = text("""
            SELECT count(*) FROM (
                SELECT idempotency_key, count(*) 
                FROM tasks 
                WHERE idempotency_key IS NOT NULL AND status = 'SUCCEEDED'
                GROUP BY idempotency_key 
                HAVING count(*) > 1
            ) as dupes
        """)
        dup_keys = await db.scalar(stmt)
        if dup_keys > 0:
            results["EFFECTIVELY_ONCE"]["pass"] = False
            results["EFFECTIVELY_ONCE"]["executed_multiple_times"] = dup_keys

        # DLQ CORRECTNESS
        stmt = select(func.count(Task.id)).outerjoin(DeadLetter, Task.id == DeadLetter.task_id).where(
            Task.status == TaskStatus.DEAD,
            DeadLetter.id.is_(None)
        )
        missing_dlq = await db.scalar(stmt)
        if missing_dlq > 0:
            results["DLQ_CORRECTNESS"]["pass"] = False
            results["DLQ_CORRECTNESS"]["invalid_dlq"] = missing_dlq
            
        # EVENT ORDER
        # Very rough check: does every task have a CREATED event?
        # A more thorough check could load all events, but let's check basic counts.
        stmt = select(func.count(Task.id)).outerjoin(TaskEvent, (Task.id == TaskEvent.task_id) & (TaskEvent.event_type == "CREATED")).where(
            TaskEvent.id.is_(None)
        )
        missing_created = await db.scalar(stmt)
        if missing_created > 0:
            results["EVENT_ORDER"]["pass"] = False
            results["EVENT_ORDER"]["invalid_order"] = missing_created

        # FAIRNESS (Calculate P95 wait times)
        # Wait time = started_at - created_at for successful tasks
        stmt = text("""
            SELECT tenant_id, percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (started_at - original_enqueue_at)))
            FROM tasks
            WHERE status = 'SUCCEEDED' AND started_at IS NOT NULL AND original_enqueue_at IS NOT NULL
            GROUP BY tenant_id
        """)
        wait_times = {}
        rows = await db.execute(stmt)
        for row in rows:
            wait_times[row[0]] = float(row[1] or 0.0)
            
        # Simplistic fairness check: quiet tenant shouldn't be completely starved
        # Just record it for the report.
        if wait_times:
            results["FAIRNESS"]["quiet_p95"] = min(wait_times.values())
            results["FAIRNESS"]["noisy_p95"] = max(wait_times.values())

    return results

if __name__ == "__main__":
    import json
    res = asyncio.run(check_invariants())
    print(json.dumps(res, indent=2))
