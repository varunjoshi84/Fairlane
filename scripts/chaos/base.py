import asyncio
import httpx
import json
import logging
import os
import random
import subprocess
import time
from fairlane.chaos.invariants import check_invariants

logger = logging.getLogger("chaos")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

API_URL = "http://localhost:8000"

def run_cmd(cmd: str):
    """Run a shell command safely."""
    logger.info(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def reset_env():
    """Tear down and rebuild the clean environment."""
    run_cmd("make down")
    run_cmd("docker volume rm fairlane_fairlane_db fairlane_fairlane_redis || true")
    run_cmd("docker compose up --build -d --scale worker=3")
    
    # Wait for API to be ready
    for _ in range(30):
        try:
            r = httpx.get(f"{API_URL}/health", timeout=2.0)
            if r.status_code == 200:
                logger.info("API is ready.")
                return
        except httpx.RequestError:
            pass
        time.sleep(1.0)
    raise RuntimeError("API failed to become ready.")

async def submit_tasks(count=2000, tenants=None, fail_rate=0.0):
    if not tenants:
        tenants = ["A", "B", "C"]
    
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        tasks = []
        for i in range(count):
            t = random.choice(tenants)
            p = random.randint(1, 9)
            payload = {}
            if random.random() < fail_rate:
                payload["fail_rate"] = 1.0 # Guarantee failure if we hit the rate
                
            tasks.append(
                client.post("/tasks", json={
                    "tenant_id": t,
                    "task_type": "demo",
                    "priority": p,
                    "payload": payload
                })
            )
        
        # Batch submit to avoid exhausting connections
        batch_size = 100
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size], return_exceptions=True)
            
    logger.info(f"Submitted {count} tasks.")

async def wait_for_settle(timeout_sec=120):
    """Wait until no tasks are PENDING or RUNNING."""
    start = time.time()
    from fairlane.db import async_session_factory
    from sqlalchemy import select, func
    from fairlane.models import Task, TaskStatus
    
    while time.time() - start < timeout_sec:
        async with async_session_factory() as db:
            stmt = select(func.count(Task.id)).where(Task.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING]))
            count = await db.scalar(stmt)
            if count == 0:
                logger.info("System has settled.")
                return True
        await asyncio.sleep(2.0)
    
    logger.warning("System did not settle in time.")
    return False

def save_report(scenario_name: str, results: dict):
    os.makedirs("results", exist_ok=True)
    with open(f"results/{scenario_name}.json", "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Report saved to results/{scenario_name}.json")
    
    for k, v in results.items():
        if isinstance(v, dict) and "pass" in v:
            status = "PASS" if v["pass"] else "FAIL"
            logger.info(f"{k}: {status}")

async def run_scenario(name: str, fault_func, task_count=2000, wait_time=120):
    logger.info(f"=== Starting Chaos Scenario: {name} ===")
    reset_env()
    
    # Start submit in background
    submit_task = asyncio.create_task(submit_tasks(count=task_count))
    
    # Run the fault while submitting or right after
    await fault_func()
    
    await submit_task
    
    await wait_for_settle(wait_time)
    
    # Run invariants
    results = await check_invariants()
    save_report(name, results)
    logger.info(f"=== Completed Chaos Scenario: {name} ===")
