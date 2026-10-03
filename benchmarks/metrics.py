"""Benchmark Metrics & Statistics Utility for TaskFlow.

Calculates throughput, latency percentiles (min, mean, median, P50, P90, P95, P99, max),
scaling efficiency, and monitors resource utilization (CPU, RAM, Redis).
"""

import math
import statistics
import threading
import time
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None

try:
    import redis
except ImportError:
    redis = None


def calculate_latency_stats(values: list[float]) -> dict[str, float]:
    """Calculate min, mean, median, p50, p90, p95, p99, max from a list of latencies (in seconds)."""
    if not values:
        return {
            "min": 0.0,
            "mean": 0.0,
            "median": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def percentile(p: float) -> float:
        if n == 1:
            return sorted_vals[0]
        idx = (p / 100.0) * (n - 1)
        lower = math.floor(idx)
        upper = math.ceil(idx)
        weight = idx - lower
        return sorted_vals[lower] * (1.0 - weight) + sorted_vals[upper] * weight

    return {
        "min": round(sorted_vals[0], 4),
        "mean": round(statistics.mean(sorted_vals), 4),
        "median": round(statistics.median(sorted_vals), 4),
        "p50": round(percentile(50), 4),
        "p90": round(percentile(90), 4),
        "p95": round(percentile(95), 4),
        "p99": round(percentile(99), 4),
        "max": round(sorted_vals[-1], 4),
    }


def calculate_throughput(completed_jobs: int, duration_seconds: float) -> dict[str, float]:
    """Calculate throughput in jobs/sec and jobs/min."""
    if duration_seconds <= 0:
        return {"jobs_per_sec": 0.0, "jobs_per_min": 0.0}
    jobs_per_sec = completed_jobs / duration_seconds
    return {
        "jobs_per_sec": round(jobs_per_sec, 2),
        "jobs_per_min": round(jobs_per_sec * 60, 2),
    }


def calculate_scaling_efficiency(throughput_n: float, throughput_1: float, workers_n: int) -> float:
    """Calculate scaling efficiency: throughput(N) / (N × throughput(1))."""
    if workers_n <= 0 or throughput_1 <= 0:
        return 0.0
    return round(throughput_n / (workers_n * throughput_1), 4)


class ResourceMonitor:
    """Background sampler for host and container resource utilization."""

    def __init__(self, sample_interval: float = 0.5, redis_url: str | None = None):
        self.sample_interval = sample_interval
        self.redis_url = redis_url
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        self.cpu_samples: list[float] = []
        self.memory_mb_samples: list[float] = []
        self.redis_memory_samples: list[int] = []
        self.redis_ops_samples: list[int] = []

    def start(self) -> None:
        """Start resource sampling in a background thread."""
        self._stop_event.clear()
        self.cpu_samples.clear()
        self.memory_mb_samples.clear()
        self.redis_memory_samples.clear()
        self.redis_ops_samples.clear()

        # Prime psutil cpu measurement
        if psutil:
            psutil.cpu_percent(interval=None)

        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def _sample_loop(self) -> None:
        redis_client = None
        if self.redis_url and redis:
            try:
                redis_client = redis.from_url(self.redis_url, decode_responses=True)
            except Exception:
                pass

        while not self._stop_event.is_set():
            if psutil:
                try:
                    cpu = psutil.cpu_percent(interval=None)
                    mem = psutil.virtual_memory()
                    self.cpu_samples.append(cpu)
                    self.memory_mb_samples.append(mem.used / (1024 * 1024))
                except Exception:
                    pass

            if redis_client:
                try:
                    info_mem = redis_client.info("memory")
                    info_stats = redis_client.info("stats")
                    self.redis_memory_samples.append(info_mem.get("used_memory", 0))
                    self.redis_ops_samples.append(info_stats.get("instantaneous_ops_per_sec", 0))
                except Exception:
                    pass

            self._stop_event.wait(self.sample_interval)

    def stop(self) -> dict[str, Any]:
        """Stop sampling and return aggregated resource statistics."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

        cpu_avg = round(statistics.mean(self.cpu_samples), 2) if self.cpu_samples else 0.0
        cpu_max = round(max(self.cpu_samples), 2) if self.cpu_samples else 0.0
        mem_avg = round(statistics.mean(self.memory_mb_samples), 2) if self.memory_mb_samples else 0.0
        mem_max = round(max(self.memory_mb_samples), 2) if self.memory_mb_samples else 0.0

        redis_mem_avg_kb = (
            round(statistics.mean(self.redis_memory_samples) / 1024, 2)
            if self.redis_memory_samples
            else 0.0
        )
        redis_ops_max = max(self.redis_ops_samples) if self.redis_ops_samples else 0

        return {
            "cpu_percent_avg": cpu_avg,
            "cpu_percent_max": cpu_max,
            "memory_used_mb_avg": mem_avg,
            "memory_used_mb_max": mem_max,
            "redis_memory_kb_avg": redis_mem_avg_kb,
            "redis_ops_sec_max": redis_ops_max,
        }
