import json
import os
import glob
from datetime import datetime

def generate_report():
    os.makedirs("docs", exist_ok=True)
    report = []
    report.append("# Fairlane Task Queue - Evaluation Results\n")
    report.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # 1. Chaos Scenarios
    report.append("## Chaos Scenarios (Resilience)\n")
    report.append("| Scenario | Pass | Total Tasks | Stuck | Ghost | Missing Side Effect | Multi Exec | Invalid DLQ | Invalid Order |")
    report.append("|----------|------|-------------|-------|-------|---------------------|------------|-------------|---------------|")
    
    chaos_files = sorted(glob.glob("results/*_*.json"))
    for file in chaos_files:
        name = os.path.basename(file).replace(".json", "")
        with open(file, "r") as f:
            res = json.load(f)
            
        # Check overall pass
        all_pass = all(v.get("pass", True) for v in res.values())
        pass_str = "✅" if all_pass else "❌"
        
        t = res["NO_LOSS"]["total_tasks"]
        s = res["NO_LOSS"].get("stuck_tasks", 0)
        g = res["NO_GHOST_STATE"].get("ghost_tasks", 0)
        mse = res["AT_LEAST_ONCE"].get("missing_side_effects", 0)
        me = res["EFFECTIVELY_ONCE"].get("executed_multiple_times", 0)
        idlq = res["DLQ_CORRECTNESS"].get("invalid_dlq", 0)
        iord = res["EVENT_ORDER"].get("invalid_order", 0)
        
        report.append(f"| {name} | {pass_str} | {t} | {s} | {g} | {mse} | {me} | {idlq} | {iord} |")
        
    report.append("\n")
    
    # 2. Benchmarks
    report.append("## Benchmarks (Performance)\n")
    if os.path.exists("results/benchmarks.json"):
        with open("results/benchmarks.json", "r") as f:
            bench = json.load(f)
            
        report.append("### Throughput\n")
        report.append("| Workers | Sleep Time | TPS | Duration |")
        report.append("|---------|------------|-----|----------|")
        for t in bench.get("throughput", []):
            report.append(f"| {t['workers']} | {t['sleep']}s | {t['throughput_tps']:.1f} | {t['duration']:.1f}s |")
            
        report.append("\n### Latency (3 workers)\n")
        lat = bench.get("latency", {})
        slat = lat.get("start_latency", {})
        flat = lat.get("finish_latency", {})
        
        report.append("| Metric | P50 | P95 | P99 |")
        report.append("|--------|-----|-----|-----|")
        if slat:
            report.append(f"| Submit to Start | {slat.get('p50', 0):.3f}s | {slat.get('p95', 0):.3f}s | {slat.get('p99', 0):.3f}s |")
        if flat:
            report.append(f"| Submit to Finish | {flat.get('p50', 0):.3f}s | {flat.get('p95', 0):.3f}s | {flat.get('p99', 0):.3f}s |")
            
    with open("docs/RESULTS.md", "w") as f:
        f.write("\n".join(report))
        
    print("Report generated at docs/RESULTS.md")

if __name__ == "__main__":
    generate_report()
