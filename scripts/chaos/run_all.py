import asyncio
import os
from scripts.chaos.base import run_cmd, run_scenario, logger, submit_tasks

async def kill_one_worker():
    await asyncio.sleep(2)
    # Find a worker container and kill it
    run_cmd("docker kill $(docker ps -q -f name=worker | head -n 1)")

async def kill_many_workers():
    await asyncio.sleep(2)
    run_cmd("docker kill $(docker ps -q -f name=worker | head -n 2)")
    await asyncio.sleep(5)
    run_cmd("docker compose up -d --scale worker=3")

async def rolling_restart():
    await asyncio.sleep(2)
    for _ in range(3):
        run_cmd("docker restart $(docker ps -q -f name=worker | head -n 1)")
        await asyncio.sleep(5)

async def redis_restart():
    await asyncio.sleep(2)
    run_cmd("docker restart fairlane_redis")

async def redis_down_during_submit():
    run_cmd("docker stop fairlane_redis")
    # Submitter is running in background. Let it hit failures.
    await asyncio.sleep(5)
    run_cmd("docker start fairlane_redis")

async def postgres_pause():
    await asyncio.sleep(2)
    run_cmd("docker pause fairlane_postgres")
    await asyncio.sleep(10)
    run_cmd("docker unpause fairlane_postgres")

async def worker_freeze():
    await asyncio.sleep(2)
    worker_id = os.popen("docker ps -q -f name=worker | head -n 1").read().strip()
    run_cmd(f"docker pause {worker_id}")
    await asyncio.sleep(15)  # Wait for reaper to reclaim
    run_cmd(f"docker unpause {worker_id}")

async def noisy_neighbor():
    # Submit a massive amount from tenant A
    asyncio.create_task(submit_tasks(count=20000, tenants=["A"]))
    await asyncio.sleep(2)

async def poison_pill():
    import httpx
    # Submit tasks that always crash
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        for _ in range(10):
            await client.post("/tasks", json={
                "tenant_id": "P",
                "task_type": "demo",
                "priority": 5,
                "payload": {"fail": True}
            })

async def duplicate_submit():
    import httpx
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        reqs = []
        for _ in range(100):
            reqs.append(client.post("/tasks", json={
                "tenant_id": "D",
                "task_type": "demo",
                "priority": 5,
                "payload": {},
                "idempotency_key": "fixed-key-123"
            }))
        await asyncio.gather(*reqs, return_exceptions=True)

async def chaos_monkey():
    import random
    faults = [kill_one_worker, redis_restart, postgres_pause]
    for _ in range(3):
        await asyncio.sleep(10)
        fault = random.choice(faults)
        logger.info(f"Chaos Monkey triggering: {fault.__name__}")
        await fault()

async def main():
    await run_scenario("1_kill_one_worker", kill_one_worker)
    await run_scenario("2_kill_many_workers", kill_many_workers)
    await run_scenario("3_rolling_restart", rolling_restart)
    await run_scenario("4_redis_restart", redis_restart)
    await run_scenario("5_redis_down_during_submit", redis_down_during_submit)
    await run_scenario("6_postgres_pause", postgres_pause)
    await run_scenario("7_worker_freeze", worker_freeze)
    await run_scenario("8_noisy_neighbor", noisy_neighbor, task_count=500)
    await run_scenario("9_poison_pill", poison_pill, task_count=100)
    await run_scenario("10_duplicate_submit", duplicate_submit, task_count=100)
    await run_scenario("11_chaos_monkey", chaos_monkey, task_count=3000, wait_time=240)

if __name__ == "__main__":
    asyncio.run(main())
