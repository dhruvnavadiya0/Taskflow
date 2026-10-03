"""TaskFlow Benchmark Runner & Scaling Experiment Orchestrator — Phase 4.

Runs automated benchmark suites across different worker counts (1, 2, 4, 8) and workloads,
records environment metadata and system resource metrics, computes scaling efficiency,
saves compact JSON & CSV results, and generates publication-quality figures.
"""

import argparse
import asyncio
import csv
from datetime import datetime, timezone
import json
import logging
import os
import platform
import subprocess
import sys
import time
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from benchmarks.load_generator import run_load_test
from benchmarks.metrics import (
    ResourceMonitor,
    calculate_scaling_efficiency,
)
from benchmarks.workloads import light, cpu, failure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("taskflow.benchmark_runner")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
FIGURES_DIR = os.path.join(RESULTS_DIR, "figures")
SUMMARY_CSV_PATH = os.path.join(RESULTS_DIR, "benchmark_summary.csv")


def get_git_commit() -> str:
    """Retrieve current git commit hash if available."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip()
    except Exception:
        return "uncommitted"


def get_system_environment() -> dict[str, Any]:
    """Capture host hardware and OS specifications."""
    cpu_count = psutil.cpu_count(logical=True) if psutil else os.cpu_count() or 1
    total_mem_gb = (
        round(psutil.virtual_memory().total / (1024 ** 3), 2)
        if psutil
        else 0.0
    )
    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "cpu_count_logical": cpu_count,
        "total_memory_gb": total_mem_gb,
        "git_commit": get_git_commit(),
    }


def scale_docker_workers(worker_count: int, compose_file: str | None = None) -> bool:
    """Scale docker compose worker replicas."""
    cmd = ["docker", "compose"]
    if compose_file:
        cmd.extend(["-f", compose_file])
    cmd.extend(["up", "-d", "--no-recreate", "--scale", f"worker={worker_count}"])

    logger.info("Scaling Docker workers to %d: %s", worker_count, " ".join(cmd))
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if res.returncode != 0:
            logger.warning("Docker scale warning: %s", res.stderr)
        # Give workers a few seconds to initialize and register
        time.sleep(4)
        return res.returncode == 0
    except Exception as exc:
        logger.error("Failed to scale Docker workers: %s", exc)
        return False


def get_workload_config(workload_name: str, custom_iterations: int | None = None) -> dict[str, Any]:
    """Get workload metadata, handler type, and payload generator."""
    if workload_name == "light":
        return {
            "name": light.WORKLOAD_NAME,
            "job_type": light.JOB_TYPE,
            "priority": light.DEFAULT_PRIORITY,
            "payload_func": light.get_payload,
            "default_payload": light.DEFAULT_PAYLOAD,
        }
    elif workload_name == "cpu":
        iters = custom_iterations or cpu.DEFAULT_ITERATIONS
        return {
            "name": cpu.WORKLOAD_NAME,
            "job_type": cpu.JOB_TYPE,
            "priority": cpu.DEFAULT_PRIORITY,
            "payload_func": lambda idx: cpu.get_payload(idx, iters),
            "default_payload": {"iterations": iters},
        }
    elif workload_name == "failure":
        return {
            "name": failure.WORKLOAD_NAME,
            "job_type": failure.JOB_TYPE,
            "priority": failure.DEFAULT_PRIORITY,
            "payload_func": failure.get_payload,
            "default_payload": {"failures_before_success": 2},
        }
    else:
        raise ValueError(f"Unknown workload: {workload_name}")


async def run_single_benchmark(
    workload: dict[str, Any],
    jobs: int,
    concurrency: int,
    api_url: str,
    warmup_seconds: float = 0.0,
    redis_url: str | None = "redis://localhost:6379/0",
) -> dict[str, Any]:
    """Execute a warm-up phase (optional) followed by a measured benchmark run."""
    # Warm-up phase
    if warmup_seconds > 0:
        logger.info("Starting warm-up phase (%.1f seconds)...", warmup_seconds)
        warmup_jobs = min(15, max(5, jobs // 10))
        try:
            await run_load_test(
                jobs_count=warmup_jobs,
                concurrency=min(concurrency, 5),
                job_type=workload["job_type"],
                priority=workload["priority"],
                payload_func=workload["payload_func"],
                api_url=api_url,
                timeout=warmup_seconds + 30.0,
                poll_interval=0.3,
            )
            logger.info("Warm-up complete. Pausing 2s before measured run...")
            await asyncio.sleep(2.0)
        except Exception as exc:
            logger.warning("Warm-up encountered error: %s (continuing to benchmark)", exc)

    # Resource monitor
    monitor = ResourceMonitor(sample_interval=0.5, redis_url=redis_url)
    monitor.start()

    # Measured run
    run_result = await run_load_test(
        jobs_count=jobs,
        concurrency=concurrency,
        job_type=workload["job_type"],
        priority=workload["priority"],
        payload_func=workload["payload_func"],
        api_url=api_url,
        timeout=300.0,
        poll_interval=0.2,
    )

    resources = monitor.stop()
    run_result["resources"] = resources
    return run_result


async def run_scaling_suite(
    worker_counts: list[int],
    jobs: int,
    concurrency: int,
    workload_name: str,
    runs_per_config: int = 1,
    warmup_seconds: float = 0.0,
    api_url: str = "http://localhost:8000",
    scale_docker: bool = False,
    redis_url: str | None = "redis://localhost:6379/0",
    cpu_iterations: int | None = None,
) -> dict[str, Any]:
    """Orchestrate worker scaling benchmarks, generate summaries and charts."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(FIGURES_DIR, exist_ok=True)

    env_info = get_system_environment()
    workload = get_workload_config(workload_name, custom_iterations=cpu_iterations)

    suite_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    suite_results = []
    baseline_throughput = 0.0

    print("\n" + "=" * 70)
    print(f"STARTING SCALING BENCHMARK SUITE: Workload={workload_name.upper()}, Jobs={jobs}")
    print(f"Workers to test: {worker_counts}, Concurrency: {concurrency}, Runs: {runs_per_config}")
    print("=" * 70 + "\n")

    for w_idx, workers in enumerate(worker_counts):
        logger.info("--- Benchmark Configuration: Workers=%d ---", workers)

        if scale_docker:
            scale_docker_workers(workers)

        run_metrics_list = []
        for r in range(runs_per_config):
            logger.info("Executing run %d/%d for workers=%d...", r + 1, runs_per_config, workers)
            metric = await run_single_benchmark(
                workload=workload,
                jobs=jobs,
                concurrency=concurrency,
                api_url=api_url,
                warmup_seconds=warmup_seconds if r == 0 else 0.0,
                redis_url=redis_url,
            )
            run_metrics_list.append(metric)

        # Average across runs
        avg_throughput = sum(m["throughput"]["jobs_per_sec"] for m in run_metrics_list) / len(run_metrics_list)
        avg_p50 = sum(m["end_to_end_latency"]["p50"] for m in run_metrics_list) / len(run_metrics_list)
        avg_p95 = sum(m["end_to_end_latency"]["p95"] for m in run_metrics_list) / len(run_metrics_list)
        avg_p99 = sum(m["end_to_end_latency"]["p99"] for m in run_metrics_list) / len(run_metrics_list)
        avg_duration = sum(m["duration_seconds"] for m in run_metrics_list) / len(run_metrics_list)
        avg_cpu = sum(m["resources"]["cpu_percent_avg"] for m in run_metrics_list) / len(run_metrics_list)
        avg_mem = sum(m["resources"]["memory_used_mb_avg"] for m in run_metrics_list) / len(run_metrics_list)

        if w_idx == 0:
            baseline_throughput = avg_throughput

        efficiency = calculate_scaling_efficiency(avg_throughput, baseline_throughput, workers)

        config_summary = {
            "workers": workers,
            "jobs": jobs,
            "concurrency": concurrency,
            "duration_seconds": round(avg_duration, 2),
            "throughput_jobs_sec": round(avg_throughput, 2),
            "latency_p50": round(avg_p50, 4),
            "latency_p95": round(avg_p95, 4),
            "latency_p99": round(avg_p99, 4),
            "scaling_efficiency": efficiency,
            "cpu_avg_percent": round(avg_cpu, 2),
            "mem_avg_mb": round(avg_mem, 2),
            "runs": run_metrics_list,
        }
        suite_results.append(config_summary)

    # Print final Markdown comparison table
    _print_scaling_table(suite_results)

    # Compile final export object
    final_output = {
        "suite_timestamp": suite_timestamp,
        "environment": env_info,
        "workload": {
            "name": workload["name"],
            "job_type": workload["job_type"],
            "priority": workload["priority"],
            "default_payload": workload["default_payload"],
        },
        "configuration": {
            "worker_counts": worker_counts,
            "jobs_per_run": jobs,
            "concurrency": concurrency,
            "runs_per_config": runs_per_config,
            "warmup_seconds": warmup_seconds,
        },
        "results": suite_results,
    }

    # Save JSON summary
    json_path = os.path.join(RESULTS_DIR, f"benchmark_{suite_timestamp}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2)
    logger.info("Saved benchmark JSON results to %s", json_path)

    # Append to CSV summary
    _append_csv_summary(final_output, SUMMARY_CSV_PATH)

    # Generate charts
    if plt is not None and len(suite_results) > 1:
        _generate_charts(final_output, FIGURES_DIR)

    return final_output


def _print_scaling_table(results: list[dict[str, Any]]) -> None:
    """Print standard Markdown scaling table."""
    print("\n" + "=" * 75)
    print("WORKER SCALING EXPERIMENT RESULTS TABLE")
    print("=" * 75)
    header = f"{'Workers':<8} | {'Jobs':<6} | {'Duration (s)':<12} | {'Throughput':<12} | {'P50 (s)':<8} | {'P95 (s)':<8} | {'P99 (s)':<8} | {'Efficiency':<10}"
    print(header)
    print("-" * len(header))
    for r in results:
        eff_str = f"{r['scaling_efficiency'] * 100:.1f}%" if r["scaling_efficiency"] > 0 else "100.0%"
        print(
            f"{r['workers']:<8} | {r['jobs']:<6} | {r['duration_seconds']:<12.2f} | "
            f"{r['throughput_jobs_sec']:<12.2f} | {r['latency_p50']:<8.4f} | "
            f"{r['latency_p95']:<8.4f} | {r['latency_p99']:<8.4f} | {eff_str:<10}"
        )
    print("=" * 75 + "\n")


def _append_csv_summary(data: dict[str, Any], csv_path: str) -> None:
    """Append compact rows to benchmark_summary.csv."""
    file_exists = os.path.isfile(csv_path)
    fieldnames = [
        "timestamp",
        "workload",
        "workers",
        "jobs",
        "concurrency",
        "duration_seconds",
        "throughput_jobs_sec",
        "latency_p50",
        "latency_p95",
        "latency_p99",
        "scaling_efficiency",
        "cpu_avg_percent",
        "mem_avg_mb",
        "git_commit",
    ]

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        ts = data["suite_timestamp"]
        workload = data["workload"]["name"]
        commit = data["environment"]["git_commit"]

        for r in data["results"]:
            writer.writerow({
                "timestamp": ts,
                "workload": workload,
                "workers": r["workers"],
                "jobs": r["jobs"],
                "concurrency": r["concurrency"],
                "duration_seconds": r["duration_seconds"],
                "throughput_jobs_sec": r["throughput_jobs_sec"],
                "latency_p50": r["latency_p50"],
                "latency_p95": r["latency_p95"],
                "latency_p99": r["latency_p99"],
                "scaling_efficiency": r["scaling_efficiency"],
                "cpu_avg_percent": r["cpu_avg_percent"],
                "mem_avg_mb": r["mem_avg_mb"],
                "git_commit": commit,
            })
    logger.info("Updated CSV summary at %s", csv_path)


def _generate_charts(data: dict[str, Any], figures_dir: str) -> None:
    """Generate throughput, latency, and efficiency charts using matplotlib."""
    results = data["results"]
    workers = [r["workers"] for r in results]
    throughput = [r["throughput_jobs_sec"] for r in results]
    p50 = [r["latency_p50"] for r in results]
    p95 = [r["latency_p95"] for r in results]
    p99 = [r["latency_p99"] for r in results]
    efficiency = [r["scaling_efficiency"] * 100 for r in results]
    cpu_usage = [r["cpu_avg_percent"] for r in results]

    # Chart 1: Throughput vs Workers
    plt.figure(figsize=(8, 5))
    plt.plot(workers, throughput, marker="o", color="#2563eb", linewidth=2.5, markersize=8)
    plt.title("TaskFlow Throughput vs Worker Count", fontsize=14, fontweight="bold")
    plt.xlabel("Worker Count", fontsize=12)
    plt.ylabel("Throughput (jobs / second)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.xticks(workers)
    throughput_fig = os.path.join(figures_dir, "throughput_vs_workers.png")
    plt.tight_layout()
    plt.savefig(throughput_fig, dpi=150)
    plt.close()

    # Chart 2: Latency Percentiles vs Workers
    plt.figure(figsize=(8, 5))
    plt.plot(workers, p50, marker="o", label="P50", color="#10b981", linewidth=2)
    plt.plot(workers, p95, marker="s", label="P95", color="#f59e0b", linewidth=2)
    plt.plot(workers, p99, marker="^", label="P99", color="#ef4444", linewidth=2)
    plt.title("TaskFlow Latency vs Worker Count", fontsize=14, fontweight="bold")
    plt.xlabel("Worker Count", fontsize=12)
    plt.ylabel("End-to-End Latency (seconds)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.xticks(workers)
    plt.legend()
    latency_fig = os.path.join(figures_dir, "latency_vs_workers.png")
    plt.tight_layout()
    plt.savefig(latency_fig, dpi=150)
    plt.close()

    # Chart 3: Scaling Efficiency vs Workers
    plt.figure(figsize=(8, 5))
    plt.bar([str(w) for w in workers], efficiency, color="#6366f1", width=0.4)
    plt.axhline(100, color="gray", linestyle="--", alpha=0.7, label="Ideal 100%")
    plt.title("Scaling Efficiency by Worker Count", fontsize=14, fontweight="bold")
    plt.xlabel("Worker Count", fontsize=12)
    plt.ylabel("Scaling Efficiency (%)", fontsize=12)
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.legend()
    eff_fig = os.path.join(figures_dir, "scaling_efficiency.png")
    plt.tight_layout()
    plt.savefig(eff_fig, dpi=150)
    plt.close()

    # Chart 4: Resource Usage vs Workers
    plt.figure(figsize=(8, 5))
    plt.plot(workers, cpu_usage, marker="d", color="#dc2626", linewidth=2.5, markersize=8)
    plt.title("CPU Utilization vs Worker Count", fontsize=14, fontweight="bold")
    plt.xlabel("Worker Count", fontsize=12)
    plt.ylabel("Average Host CPU Usage (%)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.xticks(workers)
    res_fig = os.path.join(figures_dir, "resource_usage_vs_workers.png")
    plt.tight_layout()
    plt.savefig(res_fig, dpi=150)
    plt.close()

    logger.info("Saved generated benchmark figures to %s", figures_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="TaskFlow Benchmark Runner & Scaling Experiment Orchestrator")
    parser.add_argument("--workload", type=str, default="light", choices=["light", "cpu", "failure"], help="Workload type")
    parser.add_argument("--workers", type=str, default="1,2,4,8", help="Comma-separated worker counts to test (e.g. 1,2,4,8)")
    parser.add_argument("--jobs", type=int, default=100, help="Number of jobs per run")
    parser.add_argument("--concurrency", type=int, default=20, help="HTTP client submission concurrency")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs per configuration to average")
    parser.add_argument("--warmup", type=float, default=5.0, help="Warm-up duration in seconds before measurement")
    parser.add_argument("--api-url", type=str, default="http://localhost:8000", help="TaskFlow API base URL")
    parser.add_argument("--scale-docker", action="store_true", help="Automatically scale docker compose worker replicas")
    parser.add_argument("--cpu-iterations", type=int, default=None, help="Iterations for CPU_TEST handler")

    args = parser.parse_args()

    worker_counts = [int(w.strip()) for w in args.workers.split(",") if w.strip()]

    asyncio.run(
        run_scaling_suite(
            worker_counts=worker_counts,
            jobs=args.jobs,
            concurrency=args.concurrency,
            workload_name=args.workload,
            runs_per_config=args.runs,
            warmup_seconds=args.warmup,
            api_url=args.api_url,
            scale_docker=args.scale_docker,
            cpu_iterations=args.cpu_iterations,
        )
    )


if __name__ == "__main__":
    main()
