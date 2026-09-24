from prometheus_client import Counter, Histogram, Gauge

# --- Counters ---

# fairlane_tasks_submitted_total{tenant, priority}
TASKS_SUBMITTED = Counter(
    "fairlane_tasks_submitted_total",
    "Total tasks submitted",
    ["tenant", "priority"]
)

# fairlane_tasks_completed_total{tenant, task_type, status}
TASKS_COMPLETED = Counter(
    "fairlane_tasks_completed_total",
    "Total tasks completed (succeeded, failed, dead)",
    ["tenant", "task_type", "status"]
)

# fairlane_task_retries_total{tenant, task_type}
TASK_RETRIES = Counter(
    "fairlane_task_retries_total",
    "Total task retries initiated",
    ["tenant", "task_type"]
)

# fairlane_tasks_throttled_total{tenant}
TASKS_THROTTLED = Counter(
    "fairlane_tasks_throttled_total",
    "Total tasks throttled by the rate limiter",
    ["tenant"]
)

# fairlane_dlq_total{tenant, category}
DLQ_TOTAL = Counter(
    "fairlane_dlq_total",
    "Total tasks moved to the Dead Letter Queue",
    ["tenant", "category"]
)

# fairlane_dlq_replays_total{tenant}
DLQ_REPLAYS = Counter(
    "fairlane_dlq_replays_total",
    "Total dead tasks replayed",
    ["tenant"]
)

# fairlane_tasks_reclaimed_total
TASKS_RECLAIMED = Counter(
    "fairlane_tasks_reclaimed_total",
    "Total tasks reclaimed from dead workers"
)

# fairlane_workers_declared_dead_total
WORKERS_DECLARED_DEAD = Counter(
    "fairlane_workers_declared_dead_total",
    "Total workers declared dead by the reaper"
)


# --- Histograms ---

# fairlane_task_duration_seconds{task_type}
TASK_DURATION = Histogram(
    "fairlane_task_duration_seconds",
    "Task execution time (execution start to finish)",
    ["task_type"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
)

# fairlane_task_wait_seconds{tenant, priority}
TASK_WAIT_TIME = Histogram(
    "fairlane_task_wait_seconds",
    "Task wait time (enqueue to execution start)",
    ["tenant", "priority"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
)

# fairlane_end_to_end_seconds{tenant}
END_TO_END_TIME = Histogram(
    "fairlane_end_to_end_seconds",
    "Total task lifecycle time (submit to finish)",
    ["tenant"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0]
)


# --- Gauges ---
# Gauges that read shared state will NOT have worker_id to avoid duplicate series

# fairlane_queue_waiting{tenant}
QUEUE_WAITING = Gauge(
    "fairlane_queue_waiting",
    "Number of tasks waiting in the Redis sorted set",
    ["tenant"]
)

# fairlane_queue_oldest_age_seconds{tenant}
QUEUE_OLDEST_AGE = Gauge(
    "fairlane_queue_oldest_age_seconds",
    "Age of the oldest waiting task in seconds",
    ["tenant"]
)

# fairlane_stream_depth
STREAM_DEPTH = Gauge(
    "fairlane_stream_depth",
    "Number of tasks pending/undelivered in the Redis stream"
)

# fairlane_tasks_running{tenant}
TASKS_RUNNING = Gauge(
    "fairlane_tasks_running",
    "Number of tasks currently running",
    ["tenant"]
)

# fairlane_dlq_size{tenant}
DLQ_SIZE = Gauge(
    "fairlane_dlq_size",
    "Current size of the Dead Letter Queue per tenant",
    ["tenant"]
)

# fairlane_workers_active
WORKERS_ACTIVE = Gauge(
    "fairlane_workers_active",
    "Number of actively heartbeating workers"
)

# fairlane_workers_dead
WORKERS_DEAD = Gauge(
    "fairlane_workers_dead",
    "Number of dead workers registered in the database"
)

# fairlane_tenant_tokens{tenant}
TENANT_TOKENS = Gauge(
    "fairlane_tenant_tokens",
    "Number of rate limiter tokens left",
    ["tenant"]
)

# fairlane_tenant_virtual_time{tenant}
TENANT_VIRTUAL_TIME = Gauge(
    "fairlane_tenant_virtual_time",
    "Virtual time for tenant scheduling",
    ["tenant"]
)

# Worker-specific gauges
# fairlane_worker_status{worker_id, status}
# Using labels for status and current task, value is 1
WORKER_INFO = Gauge(
    "fairlane_worker_info",
    "Info about a specific worker",
    ["worker_id", "status", "current_task"]
)

def safe_metric(func):
    """Decorator to safely execute metric updates without breaking application logic."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            pass
    return wrapper

@safe_metric
def inc_tasks_submitted(tenant: str, priority: int):
    TASKS_SUBMITTED.labels(tenant=tenant, priority=str(priority)).inc()

@safe_metric
def inc_tasks_completed(tenant: str, task_type: str, status: str):
    TASKS_COMPLETED.labels(tenant=tenant, task_type=task_type, status=status).inc()

@safe_metric
def inc_task_retries(tenant: str, task_type: str):
    TASK_RETRIES.labels(tenant=tenant, task_type=task_type).inc()

@safe_metric
def inc_tasks_throttled(tenant: str):
    TASKS_THROTTLED.labels(tenant=tenant).inc()

@safe_metric
def inc_dlq_total(tenant: str, category: str):
    DLQ_TOTAL.labels(tenant=tenant, category=category).inc()

@safe_metric
def inc_dlq_replays(tenant: str):
    DLQ_REPLAYS.labels(tenant=tenant).inc()

@safe_metric
def inc_tasks_reclaimed(count: int = 1):
    TASKS_RECLAIMED.inc(count)

@safe_metric
def inc_workers_declared_dead(count: int = 1):
    WORKERS_DECLARED_DEAD.inc(count)

@safe_metric
def observe_task_duration(task_type: str, duration: float):
    TASK_DURATION.labels(task_type=task_type).observe(duration)

@safe_metric
def observe_task_wait(tenant: str, priority: int, wait_time: float):
    TASK_WAIT_TIME.labels(tenant=tenant, priority=str(priority)).observe(wait_time)

@safe_metric
def observe_end_to_end(tenant: str, duration: float):
    END_TO_END_TIME.labels(tenant=tenant).observe(duration)

@safe_metric
def set_worker_info(worker_id: str, status: str, current_task: str):
    # Reset all labels for this worker first
    # For a gauge, we can't easily clear labels without clearing the whole metric.
    # So we just set to 1. We'll use a single dimension in practice.
    WORKER_INFO.labels(worker_id=worker_id, status=status, current_task=current_task).set(1)

