# Daft on Ray Worker OOM — Reproduction Case

Minimal reproduction for a Daft + Ray Worker OOM bug:
`daft.read_parquet()` causes Workers to exceed memory limits
due to **concurrent Row Group decoders sharing heap memory** inside a single Actor process.

## Quick Start

```bash
# 1. Start the cluster
docker compose up -d

# 2. Generate test data (pick one or more)
docker compose exec ray-head python /tmp/scripts/gen_data.py --50m     #  50M rows,  4 files, ~1.8 GB
docker compose exec ray-head python /tmp/scripts/gen_data.py --100m    # 100M rows,  8 files, ~3.5 GB
docker compose exec ray-head python /tmp/scripts/gen_data.py --200m    # 200M rows, 16 files, ~7.0 GB
docker compose exec ray-head python /tmp/scripts/gen_data.py --200m-8f # 200M rows,  8 files, ~7.0 GB (large RGs)

# If data already exists, gen_data.py prompts before overwriting.
# Add --overwrite to skip the prompt:
docker compose exec ray-head python /tmp/scripts/gen_data.py --200m --overwrite

# 3. Run the test (full suite — data must already exist, see Step 2)
docker compose exec ray-head python /tmp/scripts/run_test.py --all

# 4. Or run a single dataset
docker compose exec ray-head python /tmp/scripts/run_test.py --200m-8f

# 5. Or single-iteration mode (for scripted loops with worker restarts)
docker compose exec ray-head python /tmp/scripts/run_test.py \
    --single --dataset 200m-8f --config split --iteration 1

# 6. Cleanup
docker compose down -v
```

## Cluster Configuration

| Parameter | Value |
|-----------|-------|
| Daft | 0.7.15 |
| Ray | 2.55.0 |
| Python | 3.12 |
| Head memory / CPU | 4 GB / `--num-cpus=0` |
| Worker memory / CPU | 4 GB × 2 / **4 CPU** × 2 |
| Total resources | 8 CPU / ~8 GB memory |

Worker CPU is set to 4 (not 8) to simulate memory-constrained scenarios:
4 concurrent scan tasks × 1 RG each → peak heap can reach 2 GB+.
With 8 CPUs, more concurrent tasks trigger OOM earlier, making it harder
to observe the effect of the `split` parameter.

## Datasets

10 `int64` columns, zstd compression ~2.23×. 4 Row Groups per file.
Column distribution: `col_0-1` sequential, `col_2-4` low-cardinality (0-9999),
`col_5-7` medium range, `col_8-9` random.

| Dataset | Rows | Files | Size/File | Total | RG rows | RG decomp. est. |
|---------|------|-------|-----------|-------|---------|-----------------|
| 50m | 50M | 4 | 448 MB | ~1.8 GB | 3.125M | ~250 MB |
| 100m | 100M | 8 | 448 MB | ~3.5 GB | 3.125M | ~250 MB |
| 200m | 200M | 16 | 448 MB | ~7.0 GB | 3.125M | ~250 MB |
| 200m-8f | 200M | 8 | 896 MB | ~7.0 GB | **6.25M** | **~500 MB** |

200m and 200m-8f have the same total rows; 200m-8f doubles the rows per file,
doubling the RG size. This isolates RG size as the independent variable.

**Data generation** (`scripts/gen_data.py`): uses `pq.ParquetWriter` with
chunked RG writes to keep memory bounded (~250 MB peak). Each RG is independently
generated → written → released, preventing Head container (4 GB) OOM during generation.

## Test Results

All Ray containers (Head + 2 Workers) are restarted before each test for a clean
baseline. Data is read via `df.collect()`.

### split=False (no Row Group splitting)

| # | Dataset | Files | Status | Time | Mem usage | Error |
|---|---------|-------|--------|------|-----------|-------|
| 1 | 50m | 4 | ❌ OOM | 3.2s | 95.1% | `RayTaskError(.cause=OutOfMemoryError)` |
| 2 | 50m | 4 | ❌ OOM | 3.8s | 96.6% | `RayTaskError(.cause=OutOfMemoryError)` |
| 3 | 50m | 4 | ❌ OOM | 4.9s | 95.3% | `RayTaskError(.cause=OutOfMemoryError)` |
| 4 | 100m | 8 | ❌ OOM | 4.6s | 98.9% | `RayTaskError(.cause=OutOfMemoryError)` |
| 5 | 100m | 8 | ❌ OOM | 3.9s | 99.99% | `RayTaskError(.cause=OutOfMemoryError)` |
| 6 | 100m | 8 | ❌ OOM | 4.1s | 99.98% | `RayTaskError(.cause=OutOfMemoryError)` |
| 7 | 200m | 16 | ❌ OOM | 7.0s | 96.9% | `RayTaskError(.cause=OutOfMemoryError)` |
| 8 | 200m | 16 | ❌ OOM | 4.2s | 99.98% | `RayTaskError(.cause=OutOfMemoryError)` |
| 9 | 200m | 16 | ❌ OOM | 4.5s | 98.5% | `RayTaskError(.cause=OutOfMemoryError)` |

### split=True, max_files=25 (Row Group splitting enabled)

| # | Dataset | RGs | RG decomp. | Status | Time | Worker peak | Spill |
|---|---------|-----|------------|--------|------|-------------|-------|
| 1 | 50m | 16 | ~250 MB | ✅ OK | 5.4s | — | — |
| 2 | 50m | 16 | ~250 MB | ✅ OK | 5.8s | — | — |
| 3 | 50m | 16 | ~250 MB | ✅ OK | 4.6s | — | — |
| 4 | 100m | 32 | ~250 MB | ✅ OK | 10.7s | — | 2286 MiB |
| 5 | 100m | 32 | ~250 MB | ✅ OK | 7.7s | — | 2257 MiB |
| 6 | 100m | 32 | ~250 MB | ✅ OK | 8.4s | — | 2117 MiB |
| 7 | 200m | 64 | ~250 MB | ✅ OK | 21.0s | 4.10 GB (99.9%) | 5722 MiB |
| 8 | 200m-8f | 32 | **~500 MB** | ❌ OOM | 4.4s | 97.0% | — |
| 9 | 200m-8f | 32 | **~500 MB** | ❌ OOM | 4.4s | 97.7% | — |

### Summary

| Config | 50m | 100m | 200m | 200m-8f |
|--------|-----|------|------|---------|
| split=False | ❌❌❌ | ❌❌❌ | ❌❌❌ | — |
| split=True | ✅✅✅ | ✅✅✅ | ✅ | ❌❌ |

### Key Findings

1. **split=False: 100% OOM** — every dataset, every iteration triggers
   `RayTaskError(.cause=OutOfMemoryError)`, no exceptions.
2. **split=True saves small-RG cases** — 50m/100m/200m (RG 250 MB) all pass.
   4 concurrent RG tasks × 250 MB = 1 GB heap, well below 4 GB × 95% threshold.
3. **split=True fails for large RGs** — 200m-8f (RG 500 MB) still OOMs.
   4 concurrent × 500 MB = 2 GB heap + decoder buffers > 3.8 GB threshold.
4. **OOM exception chain confirmed** — `RayTaskError(.cause=OutOfMemoryError)`,
   not `ActorDiedError`. Ray memory monitor kill uses `OUT_OF_MEMORY` path;
   Daft does not retry.
5. **Object Store spill ≠ heap memory protection** — 200m split=True spilled
   5.7 GB, effectively clearing Object Store, but RSS still reached 4.10 GB.
   Spill cannot protect heap memory consumed by active RG decoders.

### Error Classification

`run_test.py:_classify_error()` checks the full exception chain:
`type(e).__name__` → `e.cause` → `e.__cause__` → `str(e)`,
covering the `RayTaskError(OutOfMemoryError)` nesting.

```python
def _classify_error(e: Exception) -> str:
    if "OutOfMemory" in type(e).__name__:          return "OOM"
    cause = getattr(e, "cause", None)
    if cause and "OutOfMemory" in type(cause).__name__: return "OOM"
    if cause and "ActorDied" in type(cause).__name__:   return "OOM"
    if "low on memory" in str(e).lower():           return "OOM"
    return f"ERROR({type(e).__name__})"
```

## Test Runner Usage

`run_test.py` only checks that data exists — it does **not** generate data.
If data is missing, it prints the required `gen_data.py` command and exits.
Run `gen_data.py` first (see Quick Start step 2).

### Full suite mode

```bash
# Run all datasets
docker compose exec ray-head python /tmp/scripts/run_test.py --all

# Run specific dataset(s)
docker compose exec ray-head python /tmp/scripts/run_test.py --50m
docker compose exec ray-head python /tmp/scripts/run_test.py --200m --200m-8f
```

Each dataset runs 3 iterations × 2 configs (split=False / split=True).
Results logged to `--log-dir` (default `/tmp/daft-oom-results/`).

### Single-iteration mode

For scripted loops with worker restarts between iterations:

```bash
docker compose exec ray-head python /tmp/scripts/run_test.py \
    --single --dataset 200m-8f --config split --iteration 1
```

Config options: `no_split` (split=False) or `split` (split=True, max_files=25).

### Log collection

Head-node logs (cgroup memory, raylet.out/err, ray status) are automatically
collected before and after each test iteration into `--log-dir/<run_id>/`.

Worker logs must be collected externally:

```bash
docker compose logs ray-worker01 > /tmp/worker01.log
docker compose logs ray-worker02 > /tmp/worker02.log
```

## Memory Profiling Experiment

To verify whether Daft has a memory manager controlling Parquet RG decoding,
we ran a profiling experiment with host-side `docker stats` sampling (~4s intervals)
during Daft reads. Full details in `scripts/exp_mem.py` and `scripts/mem_profile.py`.

### Results

| # | Dataset | Config | RG decomp. | Status | Time | Worker RSS peak | Spill |
|---|---------|--------|------------|--------|------|-----------------|-------|
| 1 | 200m | split=False | 250 MB | ❌ OOM | 4.4s | 2,983 MB | — |
| 2 | 200m | split=True | 250 MB | ✅ OK | 21.1s | — | 4,262 MB |
| 3 | 200m-8f | split=True | 500 MB | ✅ OK | 22.6s | 3,686 MB | 17,394 MB |
| 4 | 200m-8f | split=False | 500 MB | ❌ OOM | 5.0s | 3,639 MB | — |

### Hypothesis Verification

| # | Hypothesis | Verdict | Evidence |
|---|-----------|---------|----------|
| H1 | **No memory manager**: RSS scales linearly with RG size | ✅ Confirmed | Exp 3 (500 MB RG) peak 3,686 MB vs Exp 1 (250 MB RG) peak 2,983 MB. No memory cap observed. |
| H2 | **split=False — all RGs concurrent**: every RG in a task decodes simultaneously | ✅ Confirmed | Exp 1 & 4 both OOM in <5s. Worker RSS jumps from 420 MB to ~3 GB instantly. |
| H3 | **split=True — no RG-level throttling**: large RGs survive only by spill | ⚠️ Confirmed | Exp 3 unexpectedly OK, but RSS hit 3,686 MB (only 114 MB below the 3,800 MB kill threshold). 17 GB of spill barely saved it. |

### Conclusion

**Daft Parquet reader has no memory manager.** All RG decoders within a scan task
launch concurrently via `build_rg_stream()` in `daft-parquet`. Memory grows linearly
with `concurrent_RGs × RG_decompressed_size`. The only safety net is Ray's Object Store
spill — but spill cannot protect RSS heap memory consumed by active RG decoding.

### Running the experiment

```bash
# Start host-side memory sampler
(while true; do
  ts=$(date +%s)
  for c in daft-ray-oom-repro-ray-worker01-1 daft-ray-oom-repro-ray-worker02-1; do
    mem=$(docker stats --no-stream --format "{{.MemUsage}}" "$c" 2>/dev/null)
    echo "{\"ts\":$ts,\"container\":\"$c\",\"mem\":\"$mem\"}"
  done
  sleep 0.3
done) > /tmp/docker_stats.jsonl &
SAMPLER_PID=$!

# Run experiment
docker compose exec ray-head python /tmp/scripts/exp_mem.py --run

# Stop sampler
kill $SAMPLER_PID
```

## Why It Happens

Daft uses a Ray Actor model. Multiple scan tasks run concurrently inside the same
Actor process, sharing heap memory. Each scan task internally spawns **all Row Group
decoders concurrently** via `build_rg_stream()` in `daft-parquet` — without any
memory-aware concurrency control.

```
Worker container 4 GB
├── RSS heap (PhysicalScan process)
│   ├── ScanTask #1 → RG decoder × 4 (all concurrent)
│   ├── ScanTask #2 → RG decoder × 4 (all concurrent)
│   └── ...  ← all RGs decode simultaneously, heap memory stacks up
│
└── Ray Object Store (completed partition ObjectRefs)
    → spill can flush this, but cannot touch active RSS
```

**Two layers of concurrency, only one is controlled**:

| Layer | Control mechanism | Controlled? |
|-------|------------------|-------------|
| Scan task level | `scantask_max_parallel` (default 8) | ✅ |
| Row Group decoder level | None | ❌ **This is the gap** |

**Exact cause of 200m-8f OOM**: 4 concurrent scan tasks × 4 RGs per task all decoding
simultaneously → 16 RG decoders competing for 4 GB heap memory. Even when `split` breaks
tasks into RG-level units, RGs within each task still all run concurrently.

**Full root-cause analysis**: `~/Presentations/github/daft-ray-worker-oom-analysis.md`
**Proposed fix**: `~/Presentations/github/daft-parquet-rg-concurrency-fix.md`

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
│  │  :9000   │  │CPU=0     │  │CPU=4     │            │
│  │          │  │mem=4g    │  │mem=4g    │            │
│  └──────────┘  └──────────┘  └──────────┘            │
│                                                        │
│                   ┌──────────┐                         │
│                   │ Worker 2 │                         │
│                   │ CPU=4    │                         │
│                   │ mem=4g   │                         │
│                   └──────────┘                         │
└────────────────────────────────────────────────────────┘
```

## Design

Clean separation of concerns — each step is an independent, runnable command:

```
docker compose up -d          # infrastructure (one-click)
  └── gen_data.py             # data preparation (standalone, prompts before overwrite)
  └── run_test.py             # test execution (standalone, checks data exists, exits if missing)
  └── docker compose down -v  # cleanup
```

| Component | Role | Runs on |
|-----------|------|---------|
| `docker-compose.yml` | Service definitions (MinIO, Ray Head, 2 Workers) | — |
| `scripts/gen_data.py` | Generate Parquet → upload to MinIO. Prompts if data exists (skip with `--overwrite`). | container (ray-head) |
| `scripts/run_test.py` | Execute OOM test scenarios. Checks data, does **not** generate. | container (ray-head) |
| `scripts/mem_profile.py` | Background RSS sampler (200 ms intervals, JSONL output). | container (head/worker) |
| `scripts/exp_mem.py` | Memory profiling experiment orchestration. | container (ray-head) |
| `.env.example` | Configurable parameters | — |

## Prerequisites

- Docker with `docker compose` v2
- ~16 GB free disk space (images + test data)
- ~8 GB available RAM (3 containers: 4 GB head + 2 × 4 GB workers + MinIO)
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

## Related Documents

| Document | Path | Content |
|----------|------|---------|
| Test Report | `TEST_REPORT.md` | Full test details, OOM analysis, RSS vs spill |
| Root Cause Analysis | `~/Presentations/github/daft-ray-worker-oom-analysis.md` | Source-level analysis, exception chains, fix proposals |
| Contribution Plan | `~/Presentations/github/daft-parquet-rg-concurrency-fix.md` | Fix design, code locations, PR strategy |
