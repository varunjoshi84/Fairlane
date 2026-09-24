import asyncio
import httpx
import json
import logging
import os
import subprocess
import time
from fairlane.db import async_session_factory
from sqlalchemy import select, text
from fairlane.models import Task, TaskStatus

logger = logging.getLogger("bench")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

API_URL = "http://localhost:8000"

def run_cmd(cmd: str):
    logger.info(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def reset_env(workers: int):
    run_cmd("make down")
    run_cmd("docker volume rm fairlane_fairlane_db fairlane_fairlane_redis || true")
    run_cmd(f"docker compose up --build -d --scale worker={workers}")
    for _ in range(30):
        try:
            if httpx.get(f"{API_URL}/health", timeout=2.0).status_code == 200:
                logger.info("API is ready.")
                return
        except httpx.RequestError:
            pass
        time.sleep(1.0)
    raise RuntimeError("API failed to become ready.")

async def submit_tasks(count, sleep_sec, tenant="A", priority=5):
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        reqs = []
        for _ in range(count):
            reqs.append(
                client.post("/tasks", json={
                    "tenant_id": tenant,
                    "task_type": "demo",
                    "priority": priority,
                    "payload": {"sleep": sleep_sec} if sleep_sec else {}
                })
            )
        # batch
        for i in range(0, len(reqs), 100):
            await asyncio.gather(*reqs[i:i+100])

async def measure_throughput(workers: int, sleep_sec: float, count=1000):
    reset_env(workers)
    # allow tenant A a high limit
    async with httpx.AsyncClient(base_url=API_URL) as client:
        await client.put("/tenants/A/limits", json={"rate_per_second": 1000, "burst_capacity": 1000, "max_concurrent": 1000, "weight": 1})
    
    start_submit = time.time()
    await submit_tasks(count, sleep_sec)
    submit_time = time.time() - start_submit
    logger.info(f"Submitted {count} tasks in {submit_time:.2f}s")
    
    # Wait for completion
    start_wait = time.time()
    while True:
        async with async_session_factory() as db:
            result = await db.execute(text("SELECT count(*) FROM tasks WHERE status IN ('PENDING', 'RUNNING')"))
            pending = result.scalar()
            if pending == 0:
                break
        await asyncio.sleep(0.5)
        
    duration = time.time() - start_wait
    tps = count / duration
    logger.info(f"Workers {workers}, Sleep {sleep_sec}s: Throughput {tps:.2f} tasks/sec")
    return {"workers": workers, "sleep": sleep_sec, "throughput_tps": tps, "duration": duration}

async def measure_latency(workers: int, count=500):
    reset_env(workers)
    async with httpx.AsyncClient(base_url=API_URL) as client:
        await client.put("/tenants/A/limits", json={"rate_per_second": 1000, "burst_capacity": 1000, "max_concurrent": 1000, "weight": 1})
        
    await submit_tasks(count, sleep_sec=0.01)
    
    while True:
        async with async_session_factory() as db:
            result = await db.execute(text("SELECT count(*) FROM tasks WHERE status IN ('PENDING', 'RUNNING')"))
            if result.scalar() == 0:
                break
        await asyncio.sleep(0.5)
        
    async with async_session_factory() as db:
        # submit to start
        q_start = await db.execute(text("SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM started_at - created_at)), percentile_cont(0.95) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM started_at - created_at)), percentile_cont(0.99) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM started_at - created_at)) FROM tasks"))
        p50_s, p95_s, p99_s = q_start.fetchone()
        
        # submit to finish
        q_fin = await db.execute(text("SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)), percentile_cont(0.95) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)), percentile_cont(0.99) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)) FROM tasks"))
        p50_f, p95_f, p99_f = q_fin.fetchone()
        
    res = {
        "start_latency": {"p50": float(p50_s), "p95": float(p95_s), "p99": float(p99_s)},
        "finish_latency": {"p50": float(p50_f), "p95": float(p95_f), "p99": float(p99_f)}
    }
    logger.info(f"Latency: {res}")
    return res

async def main():
    os.makedirs("results", exist_ok=True)
    results = {}
    
    logger.info("=== Benchmarking Throughput ===")
    results["throughput"] = []
    for w in [1, 3]:
        for s in [0, 0.05]:
            results["throughput"].append(await measure_throughput(w, s, count=500))
            
    logger.info("=== Benchmarking Latency ===")
    results["latency"] = await measure_latency(workers=3)
    
    with open("results/benchmarks.json", "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Benchmarks saved to results/benchmarks.json")

if __name__ == "__main__":
    asyncio.run(main())
