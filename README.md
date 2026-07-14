# Daft on Ray Worker OOM — Reproduction Case

Minimal reproduction for a Daft + Ray Worker OOM bug:
`daft.read_parquet() → to_ray_dataset()` causes Workers to exceed memory limits
due to **concurrent scan tasks sharing heap memory** inside a single Actor process.

## Quick Start

```bash
# 1. Configure (optional — defaults work out of the box)
cp .env.example .env

# 2. Start the cluster
docker compose up -d

# 3. Generate test data (pick one)
docker compose exec ray-head python /tmp/scripts/gen_data.py --50m    #  50M rows,  4 files, ~2.5 GB
docker compose exec ray-head python /tmp/scripts/gen_data.py --100m   # 100M rows,  8 files, ~5 GB
docker compose exec ray-head python /tmp/scripts/gen_data.py --200m   # 200M rows, 16 files, ~10 GB

# 4. Run the test
docker compose exec ray-head python /tmp/scripts/run_test.py --50m
docker compose exec ray-head python /tmp/scripts/run_test.py --100m
docker compose exec ray-head python /tmp/scripts/run_test.py --200m

# 5. Cleanup
docker compose down -v
```

## Step-by-step Usage

### Step 1: Start the cluster

```bash
docker compose up -d
```

Wait for all services ready (~30s first run — images are pulled and
`daft` dependencies installed):

```bash
docker compose logs ray-head    | grep "Ray head ready"
docker compose logs ray-worker01 | grep "worker01 ready"
docker compose logs ray-worker02 | grep "worker02 ready"
```

This starts: MinIO (S3) + Ray Head (CPU=0, 4GB) + 2 Workers (8 CPUs each, 4GB).

### Step 2: Generate test data

```bash
docker compose exec ray-head python /tmp/scripts/gen_data.py --50m
docker compose exec ray-head python /tmp/scripts/gen_data.py --100m
docker compose exec ray-head python /tmp/scripts/gen_data.py --200m
```

| Dataset | Rows | Files | RGs/file | RG rows | File size | Total size |
|---------|------|-------|----------|---------|-----------|------------|
| `--50m` | 50M | 4 | 4 | 3.125M | 433 MB | ~1.7 GB |
| `--100m` | 100M | 8 | 4 | 3.125M | 433 MB | ~3.4 GB |
| `--200m` | 200M | 16 | 4 | 3.125M | 433 MB | ~6.8 GB |

Each file: 12.5M rows × 10 `int64` columns (~1000 MB uncompressed), zstd compression ~2.3×.

Column distribution (designed for mixed compressibility):
| Columns | Pattern | Cardinality | Compressibility |
|---------|---------|-------------|-----------------|
| `col_0`–`col_1` | Sequential | Unique | High (delta) |
| `col_2`–`col_4` | Random 0–9,999 | ~10K | High (dict) |
| `col_5`–`col_7` | Random 0–10⁹ | Moderate | Partial |
| `col_8`–`col_9` | Random 0–2⁶³−1 | Full-range | Low (high entropy) |

All data is stored in MinIO (`s3://daft-oom-test/{dataset}_data_*.parquet`).

### Step 3: Run the test

```bash
docker compose exec ray-head python /tmp/scripts/run_test.py --50m
docker compose exec ray-head python /tmp/scripts/run_test.py --100m
docker compose exec ray-head python /tmp/scripts/run_test.py --200m
```

Each run executes 3 scenarios: A) Daft split=False, B) Daft split=True, C) ray.data.

### Step 4: Clean up

```bash
docker compose down -v
```

### Debugging

```bash
# Inspect worker logs for OOM messages
docker compose logs ray-worker01
docker compose logs ray-worker02
```

## Expected Results

Tested on 2 Workers × 4 CPU + 4 GB memory, 3 iterations per config.

### Detailed Results

| # | Dataset | Config | Status | Time | Worker Mem | Error |
|---|---------|--------|--------|------|------------|-------|
| 1 | 50m | split=False | ❌ OOM | 3.2s | 95.1% | `RayTaskError(OutOfMemoryError)` |
| 2 | 50m | split=False | ❌ OOM | 3.8s | 96.6% | `RayTaskError(OutOfMemoryError)` |
| 3 | 50m | split=False | ❌ OOM | 4.9s | 95.3% | `RayTaskError(OutOfMemoryError)` |
| 4 | 50m | split=True | ✅ OK | 5.4s | — | — |
| 5 | 50m | split=True | ✅ OK | 5.8s | — | — |
| 6 | 50m | split=True | ✅ OK | 4.6s | — | — |
| 7 | 100m | split=False | ❌ OOM | 4.6s | 98.9% | `RayTaskError(OutOfMemoryError)` |
| 8 | 100m | split=False | ❌ OOM | 3.9s | 99.99% | `RayTaskError(OutOfMemoryError)` |
| 9 | 100m | split=False | ❌ OOM | 4.1s | 99.98% | `RayTaskError(OutOfMemoryError)` |
| 10 | 100m | split=True | ✅ OK | 10.7s | — | — |
| 11 | 100m | split=True | ✅ OK | 7.7s | — | — |
| 12 | 100m | split=True | ✅ OK | 8.4s | — | — |
| 13 | 200m | split=False | ❌ OOM | 7.0s | 96.9% | `RayTaskError(OutOfMemoryError)` |
| 14 | 200m | split=False | ❌ OOM | 4.2s | 99.98% | `RayTaskError(OutOfMemoryError)` |
| 15 | 200m | split=False | ❌ OOM | 4.5s | 98.5% | `RayTaskError(OutOfMemoryError)` |
| 16 | 200m | split=True | ✅ OK | 21.4s | — | — |
| 17 | 200m | split=True | ✅ OK | 16.0s | — | — |
| 18 | 200m | split=True | ✅ OK | 16.5s | — | — |

### Summary

| Config | 50m | 100m | 200m |
|--------|-----|------|------|
| split=False | 0/3 OK | 0/3 OK | 0/3 OK |
| split=True | **3/3 OK** | **3/3 OK** | **3/3 OK** |

All OOM errors are `RayTaskError(.cause=OutOfMemoryError)` — Ray memory monitor kills
the Worker process when memory exceeds 95% threshold. `ActorDiedError` is NOT triggered
in this path (confirmed by source code and experiment).

## How It Works

**The bug**: Daft uses a Ray Actor model. Multiple scan tasks run concurrently
inside the same Actor process, sharing heap memory. With 5 tasks each consuming
~1 GB, the Worker exceeds the 6 GB limit → OOM kill.

**The workaround** (`split=True`): Each task processes one Row Group (~50 MB)
instead of a whole file (~400 MB). If OOM still occurs at the Ray level, the
smaller tasks are retried on other Workers.

**Ray Data comparison**: Ray Data uses process isolation — each read task runs
in its own process. OOM kills only one task, and three-layer retry (Actor
restart + Task retry + Iterable retry) ensures eventual success.

For full root cause analysis and fix proposals, see the companion report
(internal: `daft-ray-worker-oom-analysis.md`).

## Design

Clean separation of concerns — each step is an independent, runnable command:

```
docker compose up -d          # infrastructure (one-click)
  └── gen_data.py             # data preparation (standalone)
  └── run_test.py             # test execution (standalone, assumes data exists)
  └── docker compose down -v  # cleanup
```

| Component | Gravitino equivalent |
|-----------|---------------------|
| `docker-compose.yml` | `dev/docker/*/docker-compose.yaml` |
| `scripts/gen_data.py` | test fixture / `@BeforeEach` setup |
| `scripts/run_test.py` | `*IT.java` test class |

## Prerequisites

- Docker with `docker compose` v2
- ~16 GB free disk space (images + test data)
- ~8 GB available RAM (3 containers: 4GB head + 2×4GB workers + MinIO)
- No Python dependencies on host — everything runs inside containers

## Tuning

Edit `.env`:

```bash
# Adjust memory for your machine
RAY_HEAD_MEM_LIMIT=2g
RAY_WORKER_MEM_LIMIT=2g
```

Or use a smaller dataset:

```bash
docker compose exec ray-head python /tmp/scripts/gen_data.py --50m
```

## Architecture

```
┌─ Host ────────────────────────────────────────────────┐
│  docker compose up -d                                  │
│  docker compose exec ray-head python /tmp/scripts/...  │
└───────────────┬────────────────────────────────────────┘
                │
┌─ Docker Compose Network (oom-test) ────────────────────┐
│                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │  MinIO   │  │Ray Head  │  │ Worker 1 │            │
│  │  :9000   │  │CPU=0     │  │CPU=8     │            │
│  │          │  │mem=4g    │  │mem=4g    │            │
│  └──────────┘  └──────────┘  └──────────┘            │
│                                                        │
│                   ┌──────────┐                         │
│                   │ Worker 2 │                         │
│                   │ CPU=8    │                         │
│                   │ mem=4g   │                         │
│                   └──────────┘                         │
└────────────────────────────────────────────────────────┘
```

## Files

| File | Role | Runs on |
|------|------|---------|
| `docker-compose.yml` | Service definitions (MinIO, Ray Head, 2 Workers) | — |
| `scripts/gen_data.py` | Generate Parquet → upload to MinIO | container (ray-head) |
| `scripts/run_test.py` | Execute OOM test scenarios | container (ray-head) |
| `.env.example` | Configurable parameters | — |
