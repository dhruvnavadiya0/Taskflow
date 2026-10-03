# TaskFlow Benchmark Report — Phase 4

## Overview
This report contains the results of the horizontal scaling and performance benchmarks for TaskFlow Phase 4.

## Hardware Environment
* **OS:** Windows 11
* **Architecture:** AMD64
* **CPU:** 24 logical cores
* **Memory:** 16 GB

## Methodology
The benchmark suite evaluates the system's throughput, latency, and scaling efficiency across different worker counts (1, 2, 4, 8) using distinct workload profiles.

## Results

### 1. Workload A: Light CPU (`SUM_NUMBERS`)
Tests overhead of the queue, API, and worker framework.

| Workers | Jobs | Duration (s) | Throughput (jobs/s) | P50 (s) | P95 (s) | P99 (s) | Efficiency |
|---------|------|--------------|---------------------|---------|---------|---------|------------|
| 1       | 100  | 3.08         | 32.43               | 1.68    | 2.21    | 2.49    | 100.0%     |
| 2       | 100  | 4.04         | 24.75               | 2.63    | 3.13    | 3.35    | 38.2%      |
| 4       | 100  | 2.46         | 40.73               | 1.60    | 2.07    | 2.13    | 31.4%      |
| 8       | 100  | 2.91         | 34.40               | 1.65    | 2.36    | 2.56    | 13.3%      |

### 2. Workload B: CPU-Heavy (`CPU_TEST`)
Tests horizontal scaling for compute-bound operations.
*(Not executed in this test run)*

### 3. Workload C: Failure-Heavy (`FAIL_N_TIMES`)
Tests retry scheduling and DLQ behavior.
*(Not executed in this test run)*

## Analysis and Conclusions
* **Throughput:** Throughput varied between 24 and 40 jobs/s for this lightweight test on a single host. Peak throughput was reached at 4 workers.
* **Latency:** Median latency remained low, hovering between 1.60s and 2.63s under a concurrency of 20. 
* **Resource Scaling:** Scaling efficiency degraded rapidly beyond 1 worker due to the lightweight nature of the jobs, contention in the local SQLite test database, and single-node overhead. For true horizontal scaling insights, these tests should be run with a heavy workload across multiple physical nodes.
