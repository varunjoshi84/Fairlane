#!/usr/bin/env python3
import asyncio
import time
import httpx

from collections import defaultdict
from rich.console import Console
from rich.table import Table
from rich.live import Live

console = Console()
API_URL = "http://localhost:8000"

async def demo():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        # 1. Setup tenants
        console.print("Setting up tenant A (slow, 2/s)...")
        await client.put("/tenants/A/limits", json={
            "rate_per_second": 2.0,
            "burst_capacity": 5,
            "max_concurrent": 3,
            "weight": 1,
        })
        console.print("Setting up tenant B (fast, 20/s)...")
        await client.put("/tenants/B/limits", json={
            "rate_per_second": 20.0,
            "burst_capacity": 20,
            "max_concurrent": 10,
            "weight": 1,
        })

        # 2. Submit tasks
        console.print("Submitting 100 tasks for A...")
        for _ in range(100):
            await client.post("/tasks", json={
                "tenant_id": "A",
                "task_type": "sleep",
                "payload": {"sleep": 0.1},
                "priority": 5,
            })
            
        console.print("Submitting 5 tasks for B...")
        for _ in range(5):
            await client.post("/tasks", json={
                "tenant_id": "B",
                "task_type": "sleep",
                "payload": {"sleep": 0.1},
                "priority": 5,
            })

        # 3. Monitor
        start_time = time.time()
        b_finished_time = None
        
        with Live(refresh_per_second=2) as live:
            while True:
                # Get usage
                usage_a = (await client.get("/tenants/A/usage")).json()
                usage_b = (await client.get("/tenants/B/usage")).json()
                
                # We can't directly count DONE/THROTTLED without querying DB, 
                # but we can show running/pending.
                
                table = Table(title="Tenant Task Queue Status")
                table.add_column("Tenant")
                table.add_column("Tokens")
                table.add_column("Running")
                table.add_column("Pending")
                
                table.add_row(
                    "A (2/s)",
                    f"{usage_a['tokens_remaining']:.1f}",
                    str(usage_a['running_tasks']),
                    str(usage_a['pending_tasks']),
                )
                
                table.add_row(
                    "B (20/s)",
                    f"{usage_b['tokens_remaining']:.1f}",
                    str(usage_b['running_tasks']),
                    str(usage_b['pending_tasks']),
                )
                
                live.update(table)
                
                if usage_b['pending_tasks'] == 0 and usage_b['running_tasks'] == 0 and b_finished_time is None:
                    b_finished_time = time.time()
                
                if usage_a['pending_tasks'] == 0 and usage_a['running_tasks'] == 0 and usage_b['pending_tasks'] == 0 and usage_b['running_tasks'] == 0:
                    break
                    
                await asyncio.sleep(1)

        b_elapsed = b_finished_time - start_time if b_finished_time else 0
        a_elapsed = time.time() - start_time
        
        console.print(f"\n[green]Tenant B finished in {b_elapsed:.1f} seconds while A was throttled (finished in {a_elapsed:.1f} seconds)[/green]")

if __name__ == "__main__":
    asyncio.run(demo())
