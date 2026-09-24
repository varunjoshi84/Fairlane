import argparse
import subprocess
import time

def run(cmd: str, wait=True):
    print(f"\n> {cmd}")
    if wait:
        subprocess.run(cmd, shell=True)
    else:
        subprocess.Popen(cmd, shell=True)

def step(title: str, what_you_should_see: str, commands: list, auto: bool):
    print("\n" + "="*80)
    print(f"STEP: {title}")
    print("WHAT YOU SHOULD SEE: " + what_you_should_see)
    print("="*80)
    if not auto:
        input("Press ENTER to run...")
    for cmd in commands:
        run(cmd)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto", action="store_true", help="Run without pausing")
    args = parser.parse_args()

    # Make sure stack is up
    run("make up")
    time.sleep(5)

    step("1. Priority Order",
         "Priority 1 tasks should jump the queue and finish before Priority 9 tasks.",
         ["make demo-priority"],
         args.auto)
         
    step("2. Fairness / Noisy Neighbor",
         "Tenant A submits tons of tasks, Tenant B submits a few. Tenant B's tasks should still execute smoothly without extreme delay.",
         ["python scripts/demo_fairness.py"],
         args.auto)
         
    step("3. Kill a Worker",
         "A worker will be killed mid-execution. The system should reclaim its tasks and finish them on another worker.",
         ["python scripts/demo_crash.py"],
         args.auto)
         
    step("4. Retry with Backoff",
         "A task will fail repeatedly and we should see the backoff delay increasing in the logs.",
         ["fairlane submit --tenant T1 --type demo --payload '{\"fail_until_attempt\": 3}' --priority 5"],
         args.auto)
         
    step("5. DLQ + Replay with a fix",
         "A poison task fails permanently and goes to DLQ. Then we fetch it and replay it.",
         [
             "fairlane submit --tenant T2 --type demo --payload '{\"fail_type\": \"permanent\"}' --priority 5",
             "sleep 3",
             "curl -s http://localhost:8000/dlq | grep -A 10 POISON || true"
         ],
         args.auto)
         
    print("\n" + "="*80)
    print("DEMO COMPLETE.")
    print("Check out the Grafana dashboard at http://localhost:3000 to see metrics!")
    print("="*80)

if __name__ == "__main__":
    main()
