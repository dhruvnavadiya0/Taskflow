# TaskFlow Benchmark Framework — Phase 4

The TaskFlow benchmark suite provides an automated, reproducible framework for measuring throughput, tail latencies (P50, P90, P95, P99), queue delays, worker execution durations, and resource utilization across scaling configurations.

---

## Architecture

```text
                       ┌──────────────────────┐
                       │  Load Generator /    │
                       │   Benchmark Runner   │
                       └──────────┬───────────┘
                                  │ (Async HTTP)
                                  ▼
                           ┌──────────────┐
                           │   FastAPI    │
                           └──────┬───────┘
                                  │
                       ┌──────────┴──────────┐
                       ▼                     ▼
                    Redis                PostgreSQL
                       │
            ┌──────────┼──────────┐
            ▼          ▼          ▼
         Worker 1   Worker 2   Worker N
            │          │          │
            └──────────┼──────────┘
                       │
                       ▼
                   Prometheus
                       │
                       ▼
                    Grafana
```

---

## Directory Structure

```text
benchmarks/
├── README.md               # Benchmark framework guide and documentation
├── requirements.txt        # Benchmark runner dependencies (httpx, psutil, matplotlib)
├── load_generator.py       # Asynchronous HTTP load generator & latency tracker
├── benchmark_runner.py     # Automated scaling suite orchestrator & chart generator
├── metrics.py              # Percentile calculation & system resource monitor
├── workloads/
│   ├── light.py            # Workload A: SUM_NUMBERS (queue/worker overhead)
│   ├── cpu.py              # Workload B: CPU_TEST (deterministic worker CPU scaling)
│   └── failure.py          # Workload C: FAIL_N_TIMES (retry scheduling & DLQ)
└── results/
    ├── .gitkeep
    ├── benchmark_summary.csv
    ├── benchmark_<timestamp>.json
    └── figures/            # Generated charts (throughput, latency, efficiency)
```

---

## Workload Profiles

### 1. Workload A: Light CPU (`SUM_NUMBERS`)
- **Job Type:** `SUM_NUMBERS`
- **Default Payload:** `{"numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}`
- **Objective:** Measure framework baseline overhead: HTTP parsing, database insertion, Redis priority enqueue, worker pop, and lease management.

### 2. Workload B: CPU-Heavy (`CPU_TEST`)
- **Job Type:** `CPU_TEST`
- **Default Payload:** `{"iterations": 200000}`
- **Objective:** Measure horizontal worker CPU scaling and concurrency bottlenecks under compute-bound jobs.

### 3. Workload C: Failure-Heavy (`FAIL_N_TIMES`)
- **Job Type:** `FAIL_N_TIMES`
- **Default Payload:** `{"failures_before_success": 2, "job_id": "bench-xxx"}`
- **Objective:** Exercise exponential backoff delay calculation, Redis sorted set scheduling, retry promotion, and Dead Letter Queue delivery.

---

## Usage

### 1. Direct Load Generation
Submit jobs and measure throughput and latency:

```bash
python benchmarks/load_generator.py \
    --jobs 100 \
    --concurrency 20 \
    --job-type SUM_NUMBERS \
    --api-url http://localhost:8000
```

#### Parameters:
- `--jobs`: Total number of jobs to submit (default: 100)
- `--concurrency`: Concurrent HTTP connections (default: 20)
- `--job-type`: `SUM_NUMBERS`, `CPU_TEST`, or `FAIL_N_TIMES`
- `--priority`: `HIGH`, `NORMAL`, or `LOW` (default: `NORMAL`)
- `--rate`: Target submission rate in jobs/sec (optional)
- `--api-url`: Base URL of TaskFlow API (default: `http://localhost:8000`)
- `--timeout`: Maximum test duration in seconds (default: 180s)
- `--poll-interval`: Polling frequency for job completion in seconds (default: 0.25s)

---

### 2. Automated Scaling Benchmark Suite
Run experiments across multiple worker counts (e.g. 1, 2, 4, 8 workers), with automated Docker scaling, warm-up phases, multiple runs, and chart generation:

```bash
python benchmarks/benchmark_runner.py \
    --workload light \
    --workers 1,2,4,8 \
    --jobs 100 \
    --concurrency 20 \
    --warmup 5 \
    --runs 1 \
    --scale-docker
```

---

## Output Metrics

Every benchmark run measures and reports:

1. **Throughput:**
   - Completed jobs per second (`completed_jobs / duration_seconds`)
   - Completed jobs per minute

2. **Latency Distributions:**
   - **End-to-End Latency:** Time from client HTTP submission to completion
   - **Queue Wait Time:** Time waiting in Redis priority queue before worker pickup (`started_at - created_at`)
   - **Execution Time:** Worker handler processing duration (`completed_at - started_at`)
   - Statistics reported: `Min`, `Mean`, `Median`, `P50`, `P90`, `P95`, `P99`, `Max`

3. **Scaling Efficiency:**
   $$\text{Efficiency} = \frac{\text{Throughput}(N)}{N \times \text{Throughput}(1)}$$

4. **Resource Utilization:**
   - Average and peak host CPU %
   - Average and peak host Memory (MB)
   - Redis memory and ops/sec
