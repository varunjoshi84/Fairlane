#!/usr/bin/env python3
import time
import httpx

def check_prometheus():
    url = "http://localhost:9090/api/v1/query"
    queries = [
        "fairlane_workers_active",
        "fairlane_tasks_submitted_total",
        "fairlane_queue_waiting",
        "fairlane_dlq_size"
    ]
    
    with httpx.Client() as client:
        print("Waiting for Prometheus to be up...")
        for _ in range(10):
            try:
                resp = client.get("http://localhost:9090/-/healthy")
                if resp.status_code == 200:
                    break
            except httpx.RequestError:
                pass
            time.sleep(2)
        else:
            print("Prometheus is not healthy.")
            return False

        print("Checking metrics in Prometheus...")
        all_ok = True
        for q in queries:
            resp = client.get(url, params={"query": q})
            if resp.status_code != 200:
                print(f"Failed to query {q}: {resp.status_code}")
                all_ok = False
                continue
                
            data = resp.json()
            if data["status"] == "success" and len(data["data"]["result"]) > 0:
                print(f"✓ {q} has data")
            else:
                print(f"✗ {q} has NO data yet")
                all_ok = False
                
        return all_ok

if __name__ == "__main__":
    if check_prometheus():
        print("Observability check passed!")
        exit(0)
    else:
        print("Observability check failed.")
        exit(1)
