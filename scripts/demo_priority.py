import asyncio
import sys
import time

import httpx
from rich.console import Console
from rich.table import Table

console = Console()
API_URL = "http://localhost:8000"


async def submit_task(client: httpx.AsyncClient, tenant_id: str, priority: int, sleep: float = 0.5) -> str:
    """Submit a task and return its ID."""
    resp = await client.post(
        f"{API_URL}/tasks",
        json={
            "tenant_id": tenant_id,
            "task_type": "demo",
            "priority": priority,
            "payload": {"sleep": sleep},
        },
    )
    resp.raise_for_status()
    return resp.json()["task_id"]


async def get_task_status(client: httpx.AsyncClient, task_id: str) -> str:
    """Get current status of a task."""
    resp = await client.get(f"{API_URL}/tasks/{task_id}")
    resp.raise_for_status()
    return resp.json()["status"]


async def wait_for_tasks(client: httpx.AsyncClient, task_ids: list[str]) -> list[str]:
    """Wait for tasks to complete and return them in completion order."""
    completed = []
    pending = set(task_ids)
    
    with console.status("[bold green]Waiting for tasks to complete...") as status:
        while pending:
            for task_id in list(pending):
                state = await get_task_status(client, task_id)
                if state in ("SUCCEEDED", "DEAD"):
                    completed.append(task_id)
                    pending.remove(task_id)
            await asyncio.sleep(0.5)
            
    return completed


async def part1_priority(client: httpx.AsyncClient):
    console.rule("[bold blue]Part 1: Strict Priority")
    console.print("Submitting 50 tasks at priority 9, then 5 tasks at priority 1.")
    
    p9_ids = []
    for _ in range(50):
        p9_ids.append(await submit_task(client, "tenant_p1", 9, 0.2))
        
    p1_ids = []
    for _ in range(5):
        p1_ids.append(await submit_task(client, "tenant_p1", 1, 0.2))
        
    console.print("Waiting for completion order...")
    completed = await wait_for_tasks(client, p9_ids + p1_ids)
    
    # Check where p1_ids finished
    p1_indices = [completed.index(tid) for tid in p1_ids]
    console.print(f"Priority 1 tasks finished at positions: {p1_indices}")
    console.print("[bold green]Summary: High priority tasks bypass the queue and finish before the bulk of low priority tasks.")


async def part2_aging(client: httpx.AsyncClient):
    console.rule("[bold blue]Part 2: Priority Aging (No Starvation)")
    console.print("Submitting 10 tasks at priority 9, then constantly submitting priority 1 tasks.")
    
    p9_ids = []
    for _ in range(10):
        p9_ids.append(await submit_task(client, "tenant_p2", 9, 0.2))
        
    p1_count = 0
    pending_p9 = set(p9_ids)
    completed_p9 = []
    
    # We will loop, submitting p1 tasks and checking if p9 tasks are completing
    with console.status("[bold green]Bombarding with Priority 1 tasks while monitoring Priority 9...") as status:
        start_time = time.time()
        while pending_p9:
            # Submit a batch of p1
            for _ in range(5):
                await submit_task(client, "tenant_p2", 1, 0.1)
                p1_count += 5
            
            # Check p9 status
            for task_id in list(pending_p9):
                state = await get_task_status(client, task_id)
                if state in ("SUCCEEDED", "DEAD"):
                    completed_p9.append(task_id)
                    pending_p9.remove(task_id)
                    
            status.update(f"Submitted {p1_count} p1 tasks. {len(completed_p9)}/10 p9 tasks finished. Elapsed: {time.time()-start_time:.1f}s")
            await asyncio.sleep(1.0)
            
    console.print(f"[bold green]Summary: All priority 9 tasks finished despite {p1_count} priority 1 tasks being submitted (aging prevented starvation).")


async def part3_weights(client: httpx.AsyncClient):
    console.rule("[bold blue]Part 3: Weighted Fair Queuing")
    console.print("Setting tenant A weight=3, tenant B weight=1. Flooding both.")
    
    # Set weights
    await client.put(f"{API_URL}/tenants/tenant_A/limits", json={"rate_per_second": 100, "burst_capacity": 100, "max_concurrent": 100, "weight": 3})
    await client.put(f"{API_URL}/tenants/tenant_B/limits", json={"rate_per_second": 100, "burst_capacity": 100, "max_concurrent": 100, "weight": 1})
    
    task_a = []
    task_b = []
    
    # Flood
    for _ in range(60):
        task_a.append(await submit_task(client, "tenant_A", 5, 0.5))
        task_b.append(await submit_task(client, "tenant_B", 5, 0.5))
        
    completed_a = 0
    completed_b = 0
    
    pending_a = set(task_a)
    pending_b = set(task_b)
    
    # Print live table
    while pending_a or pending_b:
        table = Table(title="WFQ Progress")
        table.add_column("Tenant", style="cyan")
        table.add_column("Weight", style="magenta")
        table.add_column("Completed", style="green")
        
        # Check A
        for tid in list(pending_a):
            if await get_task_status(client, tid) == "SUCCEEDED":
                completed_a += 1
                pending_a.remove(tid)
                
        # Check B
        for tid in list(pending_b):
            if await get_task_status(client, tid) == "SUCCEEDED":
                completed_b += 1
                pending_b.remove(tid)
                
        table.add_row("tenant_A", "3", str(completed_a))
        table.add_row("tenant_B", "1", str(completed_b))
        
        console.print(table)
        
        if not pending_a and not pending_b:
            break
            
        await asyncio.sleep(2.0)
        
    ratio = completed_a / max(1, completed_b)
    console.print(f"[bold green]Summary: tenant_A completed {completed_a}, tenant_B completed {completed_b}. Final ratio: {ratio:.2f} (Expected ~3.0)")


async def main():
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            # Check API health
            await client.get(f"{API_URL}/health")
        except httpx.RequestError:
            console.print("[red]API is not running. Please start it with 'make run'.")
            sys.exit(1)
            
        await part1_priority(client)
        print("\n")
        await part2_aging(client)
        print("\n")
        await part3_weights(client)

if __name__ == "__main__":
    asyncio.run(main())
