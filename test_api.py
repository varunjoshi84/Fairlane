import json
import time

import httpx

API_URL = "http://localhost:8000"


def test_health():
    print("Testing /health endpoint...")
    response = httpx.get(f"{API_URL}/health")
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    print("-" * 40)
    return response.status_code == 200


def test_create_task():
    print("Testing POST /tasks endpoint...")
    payload = {
        "tenant_id": "acme_corp",
        "task_type": "send_email",
        "payload": {"to": "user@example.com", "subject": "Welcome!"},
        "priority": 1,
    }
    response = httpx.post(f"{API_URL}/tasks", json=payload)
    print(f"Status: {response.status_code}")

    data = response.json()
    print(json.dumps(data, indent=2))
    print("-" * 40)
    return data.get("task_id") if response.status_code == 201 else None


def test_get_task(task_id: str):
    print(f"Testing GET /tasks/{task_id} endpoint...")
    response = httpx.get(f"{API_URL}/tasks/{task_id}")
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    print("-" * 40)


if __name__ == "__main__":
    if test_health():
        task_id = test_create_task()
        if task_id:
            # wait a tiny bit to ensure consistency
            time.sleep(0.5)
            test_get_task(task_id)
