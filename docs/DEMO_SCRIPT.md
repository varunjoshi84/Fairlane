# Fairlane Demo Script

Welcome to the Fairlane Demo!

To run the automated, guided demo, execute:
```bash
make demo-all
```

To step through it manually, run `make up` and then follow these steps:

### 1. Priority Order
```bash
make demo-priority
```
*Talking points:* Watch how priority 1 tasks are scheduled before priority 9, but notice that priority 9 tasks still eventually get scheduled due to the aging mechanism preventing starvation.

### 2. Fairness / Noisy Neighbor
```bash
python scripts/demo_fairness.py
```
*Talking points:* Tenant A is flooding the system, but Tenant B sneaks in without latency spikes. The Lua dispatcher uses Weighted Fair Queuing to balance virtual time between tenants.

### 3. Crash Recovery (Reaper)
```bash
make demo-crash
```
*Talking points:* A worker is killed mid-task. The system detects the heartbeat timeout, and the Reaper safely reclaims the task using XAUTOCLAIM and re-enqueues it for another worker.

### 4. DLQ & Replay
```bash
fairlane submit --tenant T1 --type demo --payload '{"fail_type": "permanent"}' --priority 5
sleep 2
curl http://localhost:8000/dlq
```
*Talking points:* Poison pills are routed directly to the Dead Letter Queue. They can be replayed from the CLI or API.
