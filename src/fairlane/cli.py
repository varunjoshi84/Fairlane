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
                    elif event_type == "REQUEUED":
                        color = typer.colors.MAGENTA
                    elif event_type == "SUCCEEDED":
                        color = typer.colors.GREEN
                    elif event_type in ("FAILED", "DEAD"):
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
                    if "duration_ms" in details:
                        detail_parts.append(f"{details['duration_ms']}ms")

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


if __name__ == "__main__":
    app()
