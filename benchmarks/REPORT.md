# TaskFlow Benchmark Report — Phase 4

## Overview
This report contains the results of the horizontal scaling and performance benchmarks for TaskFlow Phase 4.

## Hardware Environment
* **OS:** Linux
* **Architecture:** x86_64
* **CPU:** [Number of cores] cores
* **Memory:** [Amount of RAM] GB

## Methodology
The benchmark suite evaluates the system's throughput, latency, and scaling efficiency across different worker counts (1, 2, 4, 8) using distinct workload profiles.

## Results

### 1. Workload A: Light CPU (`SUM_NUMBERS`)
Tests overhead of the queue, API, and worker framework.

| Workers | Jobs | Duration (s) | Throughput (jobs/s) | P50 (s) | P95 (s) | P99 (s) | Efficiency |
|---------|------|--------------|---------------------|---------|---------|---------|------------|
| 1       |      |              |                     |         |         |         | 100.0%     |
| 2       |      |              |                     |         |         |         |            |
| 4       |      |              |                     |         |         |         |            |
| 8       |      |              |                     |         |         |         |            |

### 2. Workload B: CPU-Heavy (`CPU_TEST`)
Tests horizontal scaling for compute-bound operations.

| Workers | Jobs | Duration (s) | Throughput (jobs/s) | P50 (s) | P95 (s) | P99 (s) | Efficiency |
|---------|------|--------------|---------------------|---------|---------|---------|------------|
| 1       |      |              |                     |         |         |         | 100.0%     |
| 2       |      |              |                     |         |         |         |            |
| 4       |      |              |                     |         |         |         |            |
| 8       |      |              |                     |         |         |         |            |

### 3. Workload C: Failure-Heavy (`FAIL_N_TIMES`)
Tests retry scheduling and DLQ behavior.

| Workers | Jobs | Duration (s) | Throughput (jobs/s) | P50 (s) | P95 (s) | P99 (s) | Efficiency |
|---------|------|--------------|---------------------|---------|---------|---------|------------|
| 1       |      |              |                     |         |         |         | 100.0%     |
| 2       |      |              |                     |         |         |         |            |
| 4       |      |              |                     |         |         |         |            |
| 8       |      |              |                     |         |         |         |            |

## Analysis and Conclusions
* **Throughput:** [To be filled after running benchmarks]
* **Latency:** [To be filled after running benchmarks]
* **Resource Scaling:** [To be filled after running benchmarks]
