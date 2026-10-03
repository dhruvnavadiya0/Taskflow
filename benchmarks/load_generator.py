"""TaskFlow Async Load Generator — Phase 4.

Submits jobs concurrently to the TaskFlow REST API, records timestamps for every
unique job, tracks lifecycle completion, and computes throughput and latency percentiles.
"""

import argparse
import asyncio
from datetime import datetime, timezone
import json
import logging
import os
import sys
import time
from typing import Any

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import httpx

from benchmarks.metrics import (
    calculate_latency_stats,
    calculate_throughput,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("taskflow.load_generator")


def _parse_iso_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        # Normalize trailing Z to UTC
        cleaned = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


async def submit_job(
    client: httpx.AsyncClient,
    api_url: str,
    job_type: str,
    priority: str,
    payload: dict[str, Any],
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    """Submit a single job to POST /api/jobs and record submission timestamp."""
    submit_url = f"{api_url.rstrip('/')}/api/jobs"
    req_body = {
        "job_type": job_type,
        "priority": priority,
        "payload": payload,
    }

    async with semaphore:
        submission_time = time.time()
        try:
            resp = await client.post(submit_url, json=req_body, timeout=15.0)
            if resp.status_code == 201:
                data = resp.json()
                return {
                    "success": True,
                    "job_id": data["id"],
                    "submission_time": submission_time,
                    "error": None,
                }
            else:
                return {
                    "success": False,
                    "job_id": None,
                    "submission_time": submission_time,
                    "error": f"HTTP {resp.status_code}: {resp.text[:100]}",
                }
        except Exception as exc:
            return {
                "success": False,
                "job_id": None,
                "submission_time": submission_time,
                "error": str(exc),
            }


async def wait_for_job_completion(
    client: httpx.AsyncClient,
    api_url: str,
    job_record: dict[str, Any],
    poll_interval: float,
    deadline: float,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    """Poll GET /api/jobs/{job_id} until terminal status (COMPLETED, FAILED, DEAD_LETTER)."""
    job_id = job_record["job_id"]
    if not job_id:
        return {
            **job_record,
            "completed": False,
            "final_status": "SUBMIT_FAILED",
            "end_to_end_latency": None,
            "queue_wait_time": None,
            "execution_time": None,
        }

    job_url = f"{api_url.rstrip('/')}/api/jobs/{job_id}"

    while time.time() < deadline:
        async with semaphore:
            try:
                resp = await client.get(job_url, timeout=10.0)
                if resp.status_code == 200:
                    data = resp.json()
                    status = data.get("status")
                    if status in ("COMPLETED", "FAILED", "DEAD_LETTER"):
                        completed_time = time.time()
                        submission_time = job_record["submission_time"]
                        end_to_end = max(0.0, completed_time - submission_time)

                        # Server timestamps
                        created_at = _parse_iso_utc(data.get("created_at"))
                        started_at = _parse_iso_utc(data.get("started_at"))
                        completed_at = _parse_iso_utc(data.get("completed_at"))

                        queue_wait = (
                            max(0.0, (started_at - created_at).total_seconds())
                            if started_at and created_at
                            else None
                        )
                        execution_time = (
                            max(0.0, (completed_at - started_at).total_seconds())
                            if completed_at and started_at
                            else None
                        )

                        return {
                            **job_record,
                            "completed": True,
                            "completed_time": completed_time,
                            "final_status": status,
                            "end_to_end_latency": round(end_to_end, 4),
                            "queue_wait_time": round(queue_wait, 4) if queue_wait is not None else None,
                            "execution_time": round(execution_time, 4) if execution_time is not None else None,
                        }
            except Exception:
                pass

        await asyncio.sleep(poll_interval)

    # Timed out
    return {
        **job_record,
        "completed": False,
        "final_status": "TIMEOUT",
        "end_to_end_latency": None,
        "queue_wait_time": None,
        "execution_time": None,
    }


async def run_load_test(
    jobs_count: int,
    concurrency: int = 20,
    job_type: str = "SUM_NUMBERS",
    priority: str = "NORMAL",
    payload_func: Any = None,
    default_payload: dict[str, Any] | None = None,
    rate_limit: float | None = None,
    api_url: str = "http://localhost:8000",
    timeout: float = 180.0,
    poll_interval: float = 0.25,
) -> dict[str, Any]:
    """Execute complete load test run and return calculated metrics."""
    if default_payload is None:
        default_payload = {"numbers": [1, 2, 3, 4, 5]}

    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(limits=limits, timeout=30.0) as client:
        logger.info(
            "Starting load test: %d jobs, concurrency=%d, type=%s, priority=%s, api=%s",
            jobs_count, concurrency, job_type, priority, api_url,
        )

        test_start_time = time.time()

        # Phase 1: Submit all jobs
        submission_tasks = []
        for i in range(jobs_count):
            payload = payload_func(i) if payload_func else default_payload
            submission_tasks.append(
                submit_job(client, api_url, job_type, priority, payload, semaphore)
            )
            if rate_limit and rate_limit > 0:
                await asyncio.sleep(1.0 / rate_limit)

        submitted_records = await asyncio.gather(*submission_tasks)
        submission_finish_time = time.time()

        successful_submits = [r for r in submitted_records if r["success"]]
        failed_submits = [r for r in submitted_records if not r["success"]]

        logger.info(
            "Submitted %d/%d jobs in %.2fs (failed submissions: %d)",
            len(successful_submits), jobs_count,
            submission_finish_time - test_start_time, len(failed_submits),
        )

        # Phase 2: Poll for completion
        deadline = test_start_time + timeout
        polling_tasks = [
            wait_for_job_completion(
                client=client,
                api_url=api_url,
                job_record=rec,
                poll_interval=poll_interval,
                deadline=deadline,
                semaphore=semaphore,
            )
            for rec in successful_submits
        ]

        completed_records = await asyncio.gather(*polling_tasks)
        test_finish_time = time.time()
        total_duration = test_finish_time - test_start_time

        # Calculate metrics
        completed_jobs = [r for r in completed_records if r.get("completed") and r.get("final_status") == "COMPLETED"]
        failed_jobs = [
            r for r in completed_records
            if r.get("completed") and r.get("final_status") in ("FAILED", "DEAD_LETTER")
        ]
        timed_out_jobs = [r for r in completed_records if not r.get("completed")]

        e2e_latencies = [
            r["end_to_end_latency"]
            for r in completed_records
            if r.get("end_to_end_latency") is not None
        ]
        queue_waits = [
            r["queue_wait_time"]
            for r in completed_records
            if r.get("queue_wait_time") is not None
        ]
        exec_times = [
            r["execution_time"]
            for r in completed_records
            if r.get("execution_time") is not None
        ]

        throughput = calculate_throughput(len(completed_jobs), total_duration)
        e2e_stats = calculate_latency_stats(e2e_latencies)
        queue_stats = calculate_latency_stats(queue_waits)
        exec_stats = calculate_latency_stats(exec_times)

        result = {
            "job_type": job_type,
            "priority": priority,
            "concurrency": concurrency,
            "submitted_jobs": jobs_count,
            "submission_failures": len(failed_submits),
            "completed_jobs": len(completed_jobs),
            "failed_jobs": len(failed_jobs),
            "timed_out_jobs": len(timed_out_jobs),
            "duration_seconds": round(total_duration, 3),
            "submission_duration_seconds": round(submission_finish_time - test_start_time, 3),
            "throughput": throughput,
            "end_to_end_latency": e2e_stats,
            "queue_wait_time": queue_stats,
            "execution_time": exec_stats,
        }

        _print_summary(result)
        return result


def _print_summary(res: dict[str, Any]) -> None:
    """Print readable summary of the benchmark run."""
    print("\n" + "=" * 60)
    print("TASKFLOW BENCHMARK LOAD TEST RESULTS")
    print("=" * 60)
    print(f"Job Type:           {res['job_type']}")
    print(f"Priority:           {res['priority']}")
    print(f"Concurrency:        {res['concurrency']}")
    print(f"Total Submitted:    {res['submitted_jobs']}")
    print(f"Completed Jobs:     {res['completed_jobs']}")
    print(f"Failed Jobs (DLQ):  {res['failed_jobs']}")
    print(f"Timed Out / Stuck:  {res['timed_out_jobs']}")
    print(f"Total Duration:     {res['duration_seconds']:.2f} s")
    print(f"Throughput:         {res['throughput']['jobs_per_sec']:.2f} jobs/sec ({res['throughput']['jobs_per_min']:.1f} jobs/min)")
    print("-" * 60)
    print("Latency (End-to-End: Submission -> Completion)")
    e2e = res["end_to_end_latency"]
    print(f"  Min:    {e2e['min']:.4f} s")
    print(f"  Mean:   {e2e['mean']:.4f} s")
    print(f"  Median: {e2e['median']:.4f} s")
    print(f"  P50:    {e2e['p50']:.4f} s")
    print(f"  P90:    {e2e['p90']:.4f} s")
    print(f"  P95:    {e2e['p95']:.4f} s")
    print(f"  P99:    {e2e['p99']:.4f} s")
    print(f"  Max:    {e2e['max']:.4f} s")
    print("-" * 60)
    print("Queue Wait Time (Created -> Started)")
    qw = res["queue_wait_time"]
    print(f"  P50:    {qw['p50']:.4f} s | P95: {qw['p95']:.4f} s | P99: {qw['p99']:.4f} s | Max: {qw['max']:.4f} s")
    print("-" * 60)
    print("Execution Time (Started -> Finished)")
    ex = res["execution_time"]
    print(f"  P50:    {ex['p50']:.4f} s | P95: {ex['p95']:.4f} s | P99: {ex['p99']:.4f} s | Max: {ex['max']:.4f} s")
    print("=" * 60 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="TaskFlow Async Load Generator")
    parser.add_argument("--jobs", type=int, default=100, help="Number of jobs to submit")
    parser.add_argument("--concurrency", type=int, default=20, help="HTTP client concurrency")
    parser.add_argument("--rate", type=float, default=None, help="Target submission rate (jobs/sec)")
    parser.add_argument("--job-type", type=str, default="SUM_NUMBERS", help="Job type (SUM_NUMBERS, CPU_TEST, FAIL_N_TIMES)")
    parser.add_argument("--priority", type=str, default="NORMAL", help="Job priority (HIGH, NORMAL, LOW)")
    parser.add_argument("--payload", type=str, default=None, help="JSON payload string")
    parser.add_argument("--api-url", type=str, default="http://localhost:8000", help="TaskFlow API base URL")
    parser.add_argument("--timeout", type=float, default=180.0, help="Benchmark timeout in seconds")
    parser.add_argument("--poll-interval", type=float, default=0.25, help="Polling interval in seconds")

    args = parser.parse_args()

    payload = json.loads(args.payload) if args.payload else None

    asyncio.run(
        run_load_test(
            jobs_count=args.jobs,
            concurrency=args.concurrency,
            job_type=args.job_type,
            priority=args.priority,
            default_payload=payload,
            rate_limit=args.rate,
            api_url=args.api_url,
            timeout=args.timeout,
            poll_interval=args.poll_interval,
        )
    )


if __name__ == "__main__":
    main()
