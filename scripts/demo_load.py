#!/usr/bin/env python3
import asyncio
import argparse
import httpx
import random

API_URL = "http://localhost:8000"

async def submit_batch(client: httpx.AsyncClient, num_tasks: int, tenants: list[str], priorities: list[int], fail_rate: float):
    tasks = []
    for _ in range(num_tasks):
        tenant = random.choice(tenants)
        priority = random.choice(priorities)
        payload = {"sleep": random.uniform(0.1, 0.3)}
        if random.random() < fail_rate:
            payload["fail"] = True
            
        tasks.append(client.post("/tasks", json={
            "tenant_id": tenant,
            "task_type": "demo_task",
            "payload": payload,
            "priority": priority,
        }))
        
    await asyncio.gather(*tasks)

async def demo_load(args):
    tenants = args.tenants.split(",")
    priorities = [int(p) for p in args.priority_mix.split(",")]
    
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        print(f"Starting load generation for tenants {tenants} at {args.rate} tasks/sec...")
        print(f"Duration: {args.duration}s, Fail rate: {args.fail_rate}")
        
        # Setup tenant limits so some get throttled
        for t in tenants:
            await client.put(f"/tenants/{t}/limits", json={
                "rate_per_second": random.uniform(5, 20),
                "burst_capacity": 20,
                "max_concurrent": 10,
                "weight": 1,
            })
            
        for i in range(args.duration):
            if i == args.duration // 2:
                print(">>> Bursting traffic! <<<")
                await submit_batch(client, args.rate * 5, tenants, priorities, args.fail_rate)
            
            await submit_batch(client, args.rate, tenants, priorities, args.fail_rate)
            await asyncio.sleep(1)
            
        print("Load generation complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fairlane load generator")
    parser.add_argument("--tenants", type=str, default="A,B,C", help="Comma-separated list of tenants")
    parser.add_argument("--rate", type=int, default=10, help="Tasks to submit per second")
    parser.add_argument("--priority-mix", type=str, default="1,5,10", help="Comma-separated list of priorities")
    parser.add_argument("--fail-rate", type=float, default=0.1, help="Probability of task failure")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    
    args = parser.parse_args()
    asyncio.run(demo_load(args))
