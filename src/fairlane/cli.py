"""Fairlane Command-Line Interface (CLI) using Typer."""

import json

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
                typer.echo(f"Tenant:     {data.get('tenant_id')}")
                typer.echo(f"Type:       {data.get('task_type')}")
                typer.echo("Status:     ", nl=False)
                typer.secho(task_status, fg=status_color, bold=True)
                typer.echo(f"Attempts:   {data.get('attempts')}/{data.get('max_attempts')}")
                typer.echo(f"Created At: {data.get('created_at')}")
                typer.echo(f"Started:    {data.get('started_at')}")
                typer.echo(f"Finished:   {data.get('finished_at')}")
                if data.get("last_error"):
                    typer.secho(f"Last Error: {data.get('last_error')}", fg=typer.colors.RED)
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


if __name__ == "__main__":
    app()
