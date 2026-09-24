#!/usr/bin/env python3
"""Crash-test demonstration for Fairlane worker failure/recovery.

Submits 5 long-running tasks, kills the worker running one of them,
then polls until all tasks complete. Prints a live progress table.

Usage:
    python scripts/demo_crash.py
    # or:  make demo-crash
"""

import json
import subprocess
import sys
import time

import httpx

API_URL = "http://localhost:8000"
NUM_TASKS = 5
SLEEP_SECONDS = 20
POLL_INTERVAL = 1  # seconds


def submit_tasks(client: httpx.Client) -> list[str]:
    """Submit NUM_TASKS tasks with a long sleep."""
    task_ids: list[str] = []
    for i in range(NUM_TASKS):
        resp = client.post(
            "/tasks",
            json={
                "tenant_id": "demo",
                "task_type": "crash_test",
                "payload": {"sleep": SLEEP_SECONDS},
                "priority": 5,
            },
        )
        resp.raise_for_status()
        task_id = resp.json()["task_id"]
        task_ids.append(task_id)
        print(f"  ✓ Submitted task {i + 1}/{NUM_TASKS}: {task_id[:12]}…")
    return task_ids


def find_worker_with_task(client: httpx.Client) -> tuple[str | None, str | None]:
    """Find a worker currently running a task. Returns (worker_id, container_hostname)."""
    resp = client.get("/workers")
    resp.raise_for_status()
    workers = resp.json()

    for w in workers:
        if w["status"] == "ACTIVE" and w.get("current_task_id"):
            return w["worker_id"], w.get("hostname")

    return None, None


def find_container_name(hostname: str | None) -> str | None:
    """Find the Docker container name by hostname."""
    if not hostname:
        return None
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.ID}} {{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        # Try to find a container with matching hostname
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) >= 2:
                container_id = parts[0]
                container_name = parts[1]
                # Check if this container's hostname matches
                try:
                    inspect = subprocess.run(
                        ["docker", "inspect", "--format", "{{.Config.Hostname}}", container_id],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    if inspect.stdout.strip() == hostname:
                        return container_name
                except subprocess.SubprocessError:
                    pass
    except subprocess.SubprocessError:
        pass
    return None


def kill_worker(container_name: str) -> bool:
    """Kill a Docker container."""
    try:
        subprocess.run(["docker", "kill", container_name], check=True, capture_output=True)
        return True
    except subprocess.SubprocessError as e:
        print(f"  ✗ Failed to kill {container_name}: {e}")
        return False


def poll_tasks(client: httpx.Client, task_ids: list[str]) -> None:
    """Poll task statuses and print a live progress table until all complete."""
    terminal_statuses = {"SUCCEEDED", "FAILED", "DEAD"}

    while True:
        # Clear screen (ANSI escape)
        print("\033[2J\033[H", end="")
        print("=" * 60)
        print("  Fairlane Crash-Test Demo — Live Progress")
        print("=" * 60)
        print(f"  {'Task ID':<14} {'Status':<12} {'Locked By'}")
        print("  " + "-" * 54)

        all_done = True
        statuses: dict[str, str] = {}

        for tid in task_ids:
            try:
                resp = client.get(f"/tasks/{tid}")
                if resp.status_code == 200:
                    data = resp.json()
                    status = data.get("status", "UNKNOWN")
                    locked_by = data.get("locked_by") or "—"
                    statuses[tid] = status

                    # Color coding
                    if status == "SUCCEEDED":
                        status_display = f"\033[32m{status}\033[0m"
                    elif status == "RUNNING":
                        status_display = f"\033[34m{status}\033[0m"
                    elif status in ("FAILED", "DEAD"):
                        status_display = f"\033[31m{status}\033[0m"
                    elif status == "PENDING":
                        status_display = f"\033[33m{status}\033[0m"
                    else:
                        status_display = status

                    locked_display = locked_by[:30] if locked_by != "—" else "—"
                    print(f"  {tid[:12]+'…':<14} {status_display:<21} {locked_display}")

                    if status not in terminal_statuses:
                        all_done = False
                else:
                    print(f"  {tid[:12]+'…':<14} {'ERROR':<12}")
                    all_done = False
            except httpx.RequestError:
                print(f"  {tid[:12]+'…':<14} {'CONN_ERR':<12}")
                all_done = False

        print("  " + "-" * 54)

        if all_done:
            break

        time.sleep(POLL_INTERVAL)

    # Final summary
    succeeded = sum(1 for s in statuses.values() if s == "SUCCEEDED")
    lost = len(task_ids) - succeeded
    print()
    print("=" * 60)
    print(f"  ALL TASKS COMPLETED, {lost} LOST")
    print("=" * 60)


def main() -> None:
    print()
    print("=" * 60)
    print("  Fairlane Crash-Test Demo")
    print("=" * 60)
    print()

    client = httpx.Client(base_url=API_URL, timeout=10.0)

    # Step 1: Submit tasks
    print(f"[1/4] Submitting {NUM_TASKS} tasks (sleep={SLEEP_SECONDS}s each)…")
    task_ids = submit_tasks(client)
    print()

    # Step 2: Wait a moment for workers to pick up tasks
    print("[2/4] Waiting 3s for workers to pick up tasks…")
    time.sleep(3)

    # Step 3: Find and kill a worker
    print("[3/4] Finding a worker with an active task…")
    worker_id, hostname = find_worker_with_task(client)

    if worker_id and hostname:
        container_name = find_container_name(hostname)
        if container_name:
            print(f"  Found worker: {worker_id}")
            print(f"  Container:    {container_name}")
            print(f"  Killing container…")
            if kill_worker(container_name):
                print(f"  ✓ Killed {container_name}")
            else:
                print("  ✗ Kill failed, continuing anyway…")
        else:
            print(f"  Worker {worker_id} found but no matching container. Skipping kill.")
    else:
        print("  No worker with active task found. Skipping kill.")

    print()
    print("[4/4] Polling task statuses…")
    time.sleep(1)

    # Step 4: Poll until all done
    poll_tasks(client, task_ids)

    client.close()


if __name__ == "__main__":
    main()
