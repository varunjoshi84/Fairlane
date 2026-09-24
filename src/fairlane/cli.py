"""Fairlane Command-Line Interface (CLI) using Typer."""

import json
from datetime import datetime

import httpx
import typer

app = typer.Typer(
    name="fairlane",
    help="Fairlane Distributed Task Queue CLI",
    add_completion=False,
)

DEFAULT_API_URL = "http://localhost:8000"


@app.command("submit")
def submit(
    tenant: str = typer.Option(..., "--tenant", "-t", help="Tenant ID"),
    task_type: str = typer.Option(..., "--type", help="Task type name"),
    priority: int = typer.Option(5, "--priority", "-p", min=1, max=10, help="Task priority (1 = highest)"),
    payload: str | None = typer.Option(None, "--payload", help="JSON payload string"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Submit a new task to the Fairlane queue."""
    parsed_payload = {}
    if payload:
        try:
            parsed_payload = json.loads(payload)
        except json.JSONDecodeError as e:
            typer.secho(f"Error: Invalid JSON payload: {e}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)

    body = {
        "tenant_id": tenant,
        "task_type": task_type,
        "priority": priority,
        "payload": parsed_payload,
    }

    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.post("/tasks", json=body)
            if resp.status_code == 201:
                data = resp.json()
                task_id = data.get("task_id") or data.get("id")
                typer.secho(f"Task submitted successfully! ID: {task_id}", fg=typer.colors.GREEN, bold=True)
                typer.echo(json.dumps(data, indent=2))
            else:
                typer.secho(f"Failed to submit task (HTTP {resp.status_code}):", fg=typer.colors.RED, err=True)
                typer.echo(resp.text, err=True)
                raise typer.Exit(code=1)
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@app.command("status")
def status(
    task_id: str = typer.Argument(..., help="UUID of the task to query"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Get the current status and details of a task."""
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.get(f"/tasks/{task_id}")
            if resp.status_code == 200:
                data = resp.json()
                task_status = data.get("status", "UNKNOWN")

                # Color code status
                status_color = typer.colors.YELLOW
                if task_status == "SUCCEEDED":
                    status_color = typer.colors.GREEN
                elif task_status in ("FAILED", "DEAD"):
                    status_color = typer.colors.RED
                elif task_status == "RUNNING":
                    status_color = typer.colors.BLUE

                typer.secho(f"Task ID: {data.get('id')}", bold=True)
                typer.echo(f"Tenant:       {data.get('tenant_id')}")
                typer.echo(f"Type:         {data.get('task_type')}")
                typer.echo("Status:       ", nl=False)
                typer.secho(task_status, fg=status_color, bold=True)
                typer.echo(f"Attempts:     {data.get('attempts')}/{data.get('max_attempts')}")
                typer.echo(f"Created At:   {data.get('created_at')}")
                typer.echo(f"Started:      {data.get('started_at')}")
                typer.echo(f"Finished:     {data.get('finished_at')}")
                if data.get("next_retry_at"):
                    typer.secho(f"Next Retry:   {data.get('next_retry_at')}", fg=typer.colors.CYAN)
                if data.get("last_error"):
                    typer.secho(f"Last Error:   {data.get('last_error')}", fg=typer.colors.RED)
            elif resp.status_code == 404:
                typer.secho(f"Task '{task_id}' not found.", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
            else:
                typer.secho(f"Error fetching task (HTTP {resp.status_code}):", fg=typer.colors.RED, err=True)
                typer.echo(resp.text, err=True)
                raise typer.Exit(code=1)
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@app.command("timeline")
def timeline(
    task_id: str = typer.Argument(..., help="UUID of the task to view event history"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Print all task_events for a task in chronological order."""
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.get(f"/tasks/{task_id}/events")
            if resp.status_code == 200:
                events = resp.json()
                if not events:
                    typer.echo(f"No events found for task '{task_id}'.")
                    return

                typer.secho(f"Timeline for Task {task_id}:", bold=True)
                typer.echo("=" * 60)

                for ev in events:
                    # Format timestamp
                    raw_dt = ev.get("created_at")
                    try:
                        dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
                        time_str = dt.strftime("%H:%M:%S")
                    except Exception:
                        time_str = str(raw_dt)[:8]

                    event_type = ev.get("event_type", "UNKNOWN")
                    details = ev.get("details") or {}

                    # Color event type
                    color = typer.colors.WHITE
                    if event_type == "CREATED":
                        color = typer.colors.CYAN
                    elif event_type in ("RUNNING", "STARTED"):
                        color = typer.colors.BLUE
                    elif event_type == "RETRY_SCHEDULED":
                        color = typer.colors.YELLOW
                    elif event_type in ("REQUEUED", "RECLAIMED"):
                        color = typer.colors.MAGENTA
                    elif event_type == "SUCCEEDED":
                        color = typer.colors.GREEN
                    elif event_type in ("FAILED", "DEAD", "DEAD_LETTERED"):
                        color = typer.colors.RED

                    # Format detail description
                    detail_parts = []
                    if "attempt" in details:
                        detail_parts.append(f"attempt {details['attempt']}")
                    if "delay_seconds" in details:
                        detail_parts.append(f"delay {details['delay_seconds']}s")
                    if "error" in details:
                        detail_parts.append(f"error: {details['error']}")
                    if "reason" in details and event_type == "REQUEUED":
                        detail_parts.append(f"{details['reason']}")
                    if "dead_worker_id" in details:
                        detail_parts.append(f"dead worker: {details['dead_worker_id']}")
                    if "duration_ms" in details:
                        detail_parts.append(f"{details['duration_ms']}ms")
                    if "failure_category" in details:
                        detail_parts.append(f"category: {details['failure_category']}")

                    detail_str = f"  ({', '.join(detail_parts)})" if detail_parts else ""

                    time_formatted = typer.style(f"  {time_str}  ", fg=typer.colors.BRIGHT_BLACK)
                    event_formatted = typer.style(f"{event_type:<17}", fg=color, bold=True)
                    typer.echo(f"{time_formatted}{event_formatted}{detail_str}")

                typer.echo("=" * 60)
            elif resp.status_code == 404:
                typer.secho(f"Task '{task_id}' not found.", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
            else:
                typer.secho(f"Error fetching task events (HTTP {resp.status_code}):", fg=typer.colors.RED, err=True)
                typer.echo(resp.text, err=True)
                raise typer.Exit(code=1)
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


def _humanize_seconds(seconds: float) -> str:
    """Convert seconds into a human-readable string like '2s ago' or '3m ago'."""
    if seconds < 0:
        return "in the future"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    return f"{int(seconds // 3600)}h ago"


@app.command("workers")
def workers_cmd(
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """List all registered workers with status and heartbeat."""
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.get("/workers")
            if resp.status_code != 200:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)

            workers_list = resp.json()
            if not workers_list:
                typer.secho("No workers found.", fg=typer.colors.YELLOW)
                return

            now = datetime.now(datetime.now().astimezone().tzinfo)

            headers = ["WORKER ID", "STATUS", "HEARTBEAT", "CURRENT TASK"]
            rows = []
            for w in workers_list:
                # Compute human-readable heartbeat
                hb_str = "—"
                if w.get("last_heartbeat_at"):
                    try:
                        hb_dt = datetime.fromisoformat(w["last_heartbeat_at"])
                        diff = (now - hb_dt).total_seconds()
                        hb_str = _humanize_seconds(diff)
                    except Exception:
                        hb_str = str(w["last_heartbeat_at"])[:19]

                task_str = str(w.get("current_task_id") or "—")[:12]
                if task_str and task_str != "—":
                    task_str = task_str[:8] + "…"

                rows.append([
                    w["worker_id"],
                    w["status"],
                    hb_str,
                    task_str,
                ])

            _print_table(headers, rows)

    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# DLQ sub-commands
# ---------------------------------------------------------------------------

dlq_app = typer.Typer(
    name="dlq",
    help="Manage the Dead-Letter Queue (DLQ)",
    add_completion=False,
)
app.add_typer(dlq_app, name="dlq")


def _fmt_dt(raw: str | None) -> str:
    """Format an ISO datetime string for table display."""
    if not raw:
        return "—"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(raw)[:19]


def _category_color(cat: str) -> str:
    """Return a typer color for a failure category."""
    colors = {
        "PERMANENT": typer.colors.RED,
        "TRANSIENT_EXHAUSTED": typer.colors.YELLOW,
        "POISON_PILL": typer.colors.MAGENTA,
    }
    return colors.get(cat, typer.colors.WHITE)


def _print_table(headers: list[str], rows: list[list[str]], col_widths: list[int] | None = None) -> None:
    """Print a simple ASCII table."""
    if col_widths is None:
        col_widths = [
            max(len(h), *(len(str(row[i])) for row in rows)) if rows else len(h)
            for i, h in enumerate(headers)
        ]

    # Header
    header_line = "  ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    typer.secho(header_line, bold=True)
    typer.echo("─" * len(header_line))

    # Rows
    for row in rows:
        parts = []
        for i, (cell, width) in enumerate(zip(row, col_widths)):
            cell_str = str(cell).ljust(width)
            # Color the category column
            if headers[i].upper() in ("CATEGORY", "FAILURE_CATEGORY"):
                cell_str = typer.style(cell_str, fg=_category_color(str(cell)))
            parts.append(cell_str)
        typer.echo("  ".join(parts))


@dlq_app.command("list")
def dlq_list(
    tenant: str | None = typer.Option(None, "--tenant", "-t", help="Filter by tenant ID"),
    category: str | None = typer.Option(None, "--category", "-c", help="Filter by failure category"),
    page: int = typer.Option(1, "--page", help="Page number"),
    page_size: int = typer.Option(20, "--page-size", help="Items per page"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """List dead-lettered tasks."""
    params: dict[str, str | int] = {"page": page, "page_size": page_size}
    if tenant:
        params["tenant_id"] = tenant
    if category:
        params["failure_category"] = category

    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.get("/dlq", params=params)
            if resp.status_code != 200:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)

            data = resp.json()
            items = data.get("items", [])
            total = data.get("total", 0)

            if not items:
                typer.secho("No dead-lettered tasks found.", fg=typer.colors.YELLOW)
                return

            typer.secho(f"Dead-Letter Queue ({total} total)\n", bold=True)

            headers = ["TASK_ID", "TENANT", "TYPE", "CATEGORY", "ATTEMPTS", "DEAD_AT", "LAST_ERROR"]
            rows = []
            for item in items:
                error_preview = (item.get("last_error") or "")[:50]
                rows.append([
                    str(item["task_id"])[:8] + "…",
                    item["tenant_id"],
                    item["task_type"],
                    item["failure_category"],
                    str(item["attempts_made"]),
                    _fmt_dt(item.get("dead_at")),
                    error_preview,
                ])

            _print_table(headers, rows)

            typer.echo(f"\nPage {data.get('page')}/{max(1, -(-total // page_size))}  "
                       f"({total} total)")

    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@dlq_app.command("show")
def dlq_show(
    task_id: str = typer.Argument(..., help="UUID of the dead-lettered task"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Show full details of a dead-lettered task, including error history."""
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.get(f"/dlq/{task_id}")
            if resp.status_code == 404:
                typer.secho(f"No dead letter found for task '{task_id}'.", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
            if resp.status_code != 200:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)

            dl = resp.json()

            typer.secho("Dead Letter Details", bold=True)
            typer.echo("=" * 60)
            typer.echo(f"Task ID:        {dl['task_id']}")
            typer.echo(f"Tenant:         {dl['tenant_id']}")
            typer.echo(f"Type:           {dl['task_type']}")
            typer.echo("Category:       ", nl=False)
            typer.secho(dl["failure_category"], fg=_category_color(dl["failure_category"]), bold=True)
            typer.echo(f"Attempts Made:  {dl['attempts_made']}")
            typer.echo(f"Dead At:        {_fmt_dt(dl.get('dead_at'))}")
            typer.echo(f"Replayed At:    {_fmt_dt(dl.get('replayed_at'))}")
            typer.echo(f"Replay Count:   {dl.get('replay_count', 0)}")

            if dl.get("last_error"):
                typer.echo()
                typer.secho("Last Error:", bold=True)
                typer.secho(f"  {dl['last_error']}", fg=typer.colors.RED)

            error_history = dl.get("error_history", [])
            if error_history:
                typer.echo()
                typer.secho("Error History:", bold=True)
                typer.echo("─" * 60)
                for i, entry in enumerate(error_history, 1):
                    attempt = entry.get("attempt", "?")
                    error = entry.get("error", "—")
                    ts = entry.get("timestamp", "")
                    error_type = entry.get("error_type", "")
                    prefix = f"  [{i}] attempt {attempt}"
                    if ts:
                        prefix += f"  ({_fmt_dt(ts)})"
                    if error_type:
                        prefix += f"  [{error_type}]"
                    typer.secho(prefix, fg=typer.colors.BRIGHT_BLACK)
                    typer.secho(f"      {error}", fg=typer.colors.RED)

            typer.echo("=" * 60)

    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@dlq_app.command("stats")
def dlq_stats(
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Show DLQ statistics: counts by category and by tenant."""
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.get("/dlq/stats")
            if resp.status_code != 200:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)

            data = resp.json()
            total = data.get("total", 0)

            typer.secho(f"DLQ Statistics  (total: {total})\n", bold=True)

            # By category
            by_category = data.get("by_category", [])
            if by_category:
                typer.secho("By Category:", bold=True)
                headers = ["CATEGORY", "COUNT"]
                rows = [[c["category"], str(c["count"])] for c in by_category]
                _print_table(headers, rows)
                typer.echo()

            # By tenant
            by_tenant = data.get("by_tenant", [])
            if by_tenant:
                typer.secho("By Tenant:", bold=True)
                headers = ["TENANT", "COUNT"]
                rows = [[t["tenant_id"], str(t["count"])] for t in by_tenant]
                _print_table(headers, rows)

            if not by_category and not by_tenant:
                typer.secho("No dead-lettered tasks found.", fg=typer.colors.YELLOW)

    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@dlq_app.command("replay")
def dlq_replay(
    task_id: str = typer.Argument(..., help="UUID of the dead-lettered task to replay"),
    patch: str | None = typer.Option(None, "--patch", help="JSON payload patch to apply before replay"),
    reset_attempts: bool = typer.Option(False, "--reset-attempts", help="Reset attempt counter to 0"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Replay a dead-lettered task, optionally with a fixed payload."""
    body: dict = {}
    if patch:
        try:
            body["payload_patch"] = json.loads(patch)
        except json.JSONDecodeError as e:
            typer.secho(f"Error: Invalid JSON patch: {e}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
    if reset_attempts:
        body["reset_attempts"] = True

    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.post(f"/dlq/{task_id}/replay", json=body if body else None)
            if resp.status_code == 200:
                data = resp.json()
                typer.secho(
                    f"✓ {data.get('message', 'Task replayed')}",
                    fg=typer.colors.GREEN,
                    bold=True,
                )
                typer.echo(f"  Task ID: {data.get('task_id')}")
                typer.echo(f"  Status:  {data.get('status')}")
            elif resp.status_code == 404:
                typer.secho(f"Task '{task_id}' not found.", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
            elif resp.status_code == 409:
                detail = resp.json().get("detail", "Conflict")
                typer.secho(f"Cannot replay: {detail}", fg=typer.colors.YELLOW, err=True)
                raise typer.Exit(code=1)
            else:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@dlq_app.command("replay-bulk")
def dlq_replay_bulk(
    tenant: str | None = typer.Option(None, "--tenant", "-t", help="Filter by tenant ID"),
    category: str | None = typer.Option(None, "--category", "-c", help="Filter by failure category"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Replay all dead-lettered tasks matching the given filters."""
    if not tenant and not category:
        typer.secho("Error: At least one of --tenant or --category is required.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    body: dict = {}
    if tenant:
        body["tenant_id"] = tenant
    if category:
        body["failure_category"] = category

    try:
        with httpx.Client(base_url=api_url, timeout=30.0) as client:
            resp = client.post("/dlq/replay-bulk", json=body)
            if resp.status_code == 200:
                data = resp.json()
                count = data.get("replayed", 0)
                task_ids = data.get("task_ids", [])

                if count == 0:
                    typer.secho("No matching DEAD tasks found to replay.", fg=typer.colors.YELLOW)
                    return

                typer.secho(f"✓ Replayed {count} task(s)", fg=typer.colors.GREEN, bold=True)
                for tid in task_ids:
                    typer.echo(f"  • {tid}")
            elif resp.status_code == 400:
                detail = resp.json().get("detail", "Bad request")
                typer.secho(f"Error: {detail}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
            else:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# Tenant sub-commands
# ---------------------------------------------------------------------------

tenant_app = typer.Typer(
    name="tenant",
    help="Manage tenant limits and quotas",
    add_completion=False,
)
app.add_typer(tenant_app, name="tenant")


@tenant_app.command("set-limit")
def tenant_set_limit(
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
    rate: float = typer.Option(..., "--rate", help="Tokens per second"),
    burst: int = typer.Option(..., "--burst", help="Maximum burst capacity"),
    concurrent: int = typer.Option(..., "--concurrent", help="Maximum concurrent tasks"),
    weight: int = typer.Option(1, "--weight", help="Tenant scheduling weight"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Set custom rate limits and quotas for a tenant."""
    body = {
        "rate_per_second": rate,
        "burst_capacity": burst,
        "max_concurrent": concurrent,
        "weight": weight,
    }
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp = client.put(f"/tenants/{tenant_id}/limits", json=body)
            if resp.status_code == 200:
                typer.secho(f"✓ Limits updated for tenant '{tenant_id}'", fg=typer.colors.GREEN, bold=True)
            else:
                typer.secho(f"Error (HTTP {resp.status_code}): {resp.text}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@tenant_app.command("show")
def tenant_show(
    tenant_id: str = typer.Argument(..., help="Tenant ID"),
    api_url: str = typer.Option(DEFAULT_API_URL, "--api-url", envvar="FAIRLANE_API_URL", help="Fairlane API base URL"),
) -> None:
    """Show limits and current usage for a tenant."""
    try:
        with httpx.Client(base_url=api_url, timeout=10.0) as client:
            resp_limits = client.get(f"/tenants/{tenant_id}/limits")
            resp_usage = client.get(f"/tenants/{tenant_id}/usage")
            
            if resp_limits.status_code != 200 or resp_usage.status_code != 200:
                typer.secho(f"Error fetching tenant data", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)

            limits = resp_limits.json()
            usage = resp_usage.json()
            
            typer.secho(f"Tenant: {tenant_id}", bold=True)
            typer.echo("=" * 40)
            typer.secho("Limits:", bold=True)
            typer.echo(f"  Rate:           {limits['rate_per_second']} tokens/sec")
            typer.echo(f"  Burst Capacity: {limits['burst_capacity']} tokens")
            typer.echo(f"  Max Concurrent: {limits['max_concurrent']} tasks")
            typer.echo(f"  Weight:         {limits['weight']}")
            typer.echo()
            typer.secho("Usage:", bold=True)
            typer.echo(f"  Tokens Left:    {usage['tokens_remaining']:.2f}")
            typer.echo(f"  Running Tasks:  {usage['running_tasks']}")
            typer.echo(f"  Pending Tasks:  {usage['pending_tasks']}")
            typer.echo("=" * 40)
            
    except httpx.RequestError as e:
        typer.secho(f"Error connecting to API at {api_url}: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@app.command("scheduler")
def scheduler_cmd(
    action: str = typer.Argument(..., help="Action to perform: 'rebuild'"),
) -> None:
    """Manage the Fairlane scheduler."""
    import asyncio
    
    if action == "rebuild":
        from fairlane.scheduler import rebuild_from_postgres
        requeued = asyncio.run(rebuild_from_postgres())
        typer.secho(f"Scheduler rebuilt successfully! {requeued} tasks re-added to waiting rooms.", fg=typer.colors.GREEN, bold=True)
    else:
        typer.secho(f"Unknown action '{action}'", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


@app.command("queue")
def queue_cmd() -> None:
    """Show the current state of per-tenant waiting rooms (priority queues)."""
    import asyncio
    import time
    from fairlane.redis_client import get_redis_client
    
    async def _run():
        redis = get_redis_client()
        now_ms = int(time.time() * 1000)
        try:
            tenants = await redis.zrange("tenants:vtime", 0, -1, withscores=True)
            
            typer.echo(f"{'TENANT':<15} {'WAITING':<10} {'OLDEST AGE':<15} {'VTIME':<10} {'WEIGHT':<8} {'INSTREAM':<10}")
            typer.echo("─" * 72)
            
            if not tenants:
                typer.echo("No tenants currently have waiting tasks.")
                return
                
            for tenant_id_b, vtime in tenants:
                tenant_id = tenant_id_b if isinstance(tenant_id_b, str) else tenant_id_b.decode('utf-8')
                
                # Waiting count
                ready_key = f"ready:{tenant_id}"
                waiting_count = await redis.zcard(ready_key)
                
                # Oldest age (smallest score)
                oldest_task = await redis.zrange(ready_key, 0, 0, withscores=True)
                oldest_age = "N/A"
                if oldest_task:
                    score = oldest_task[0][1]
                    # Score is approx enqueue time + penalty. Without penalty it's hard to get exact,
                    # but we can just do (now - score) as a rough estimate or just show the score relative
                    age_ms = now_ms - score
                    oldest_age = f"{age_ms/1000.0:.1f}s" if age_ms > 0 else "0s"
                
                # Weight
                weight_key = f"tenant_weight:{tenant_id}"
                weight = await redis.get(weight_key)
                weight = weight if isinstance(weight, str) else (weight.decode('utf-8') if weight else "1")
                
                # Instream
                instream_key = f"instream:{tenant_id}"
                instream = await redis.get(instream_key)
                instream = instream if isinstance(instream, str) else (instream.decode('utf-8') if instream else "0")
                
                typer.echo(f"{tenant_id:<15} {waiting_count:<10} {oldest_age:<15} {vtime:<10.2f} {weight:<8} {instream:<10}")
                
        finally:
            await redis.aclose()

    asyncio.run(_run())


if __name__ == "__main__":
    app()
