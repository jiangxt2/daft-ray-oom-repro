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

| 参数 | 值 |
|------|-----|
| Daft | 0.7.15 |
| Ray | 2.55.0 |
| Python | 3.12 |
| Head 内存 / CPU | 4 GB / `--num-cpus=0` |
| Worker 内存 / CPU | 4 GB × 2 / **4 CPU** × 2 |
| 总资源 | 8 CPU / ~8 GB memory |

Worker CPU 设为 4（非 8）是为了模拟内存受限场景——4 个并发 scan task 各解压 1 个 RG 时，堆内存峰值可达 2GB+。8 CPU 会导致更多并发 task，OOM 更早触发，反而不利于观察 split 参数的效果。

## Datasets

10 列 `int64`，zstd 压缩 ~2.3×。每文件 4 个 Row Group。列分布：`col_0-1` 顺序、`col_2-4` 低基数 0-9999、`col_5-7` 中等范围、`col_8-9` 随机。

| Dataset | 行数 | 文件数 | 单文件大小 | 总大小 | RG 行数 | RG 解压估算 |
|---------|------|--------|-----------|--------|---------|------------|
| 50m | 50M | 4 | 448 MB | ~1.8 GB | 3.125M | ~250 MB |
| 100m | 100M | 8 | 448 MB | ~3.5 GB | 3.125M | ~250 MB |
| 200m | 200M | 16 | 448 MB | ~7.0 GB | 3.125M | ~250 MB |
| 200m-8f | 200M | 8 | 896 MB | ~7.0 GB | **6.25M** | **~500 MB** |
200m / 200m-8f 总行数相同，区别在于 200m-8f 单文件行数翻倍 → RG 大小翻倍。用于测试不同 RG 大小下的内存行为。

**数据生成**（`scripts/gen_data.py`）：采用 `pq.ParquetWriter` 按 RG 分批写入，避免一次性生成全部数据导致 Head 容器（4GB）OOM。每个 RG 独立生成 → 写入 → 释放，内存峰值控制在 ~250MB。

## Test Results

每次测试前重启全部 Ray 容器（Head + 2 Worker），确保干净基线。使用 `df.collect()` 触发计算。

### split=False（无 Row Group 切分）

| # | Dataset | 文件数 | 状态 | 耗时 | 内存使用率 | 异常 |
|---|---------|--------|------|------|------------|------|
| 1 | 50m | 4 | ❌ OOM | 3.2s | 95.1% | `RayTaskError(.cause=OutOfMemoryError)` |
| 2 | 50m | 4 | ❌ OOM | 3.8s | 96.6% | `RayTaskError(.cause=OutOfMemoryError)` |
| 3 | 50m | 4 | ❌ OOM | 4.9s | 95.3% | `RayTaskError(.cause=OutOfMemoryError)` |
| 4 | 100m | 8 | ❌ OOM | 4.6s | 98.9% | `RayTaskError(.cause=OutOfMemoryError)` |
| 5 | 100m | 8 | ❌ OOM | 3.9s | 99.99% | `RayTaskError(.cause=OutOfMemoryError)` |
| 6 | 100m | 8 | ❌ OOM | 4.1s | 99.98% | `RayTaskError(.cause=OutOfMemoryError)` |
| 7 | 200m | 16 | ❌ OOM | 7.0s | 96.9% | `RayTaskError(.cause=OutOfMemoryError)` |
| 8 | 200m | 16 | ❌ OOM | 4.2s | 99.98% | `RayTaskError(.cause=OutOfMemoryError)` |
| 9 | 200m | 16 | ❌ OOM | 4.5s | 98.5% | `RayTaskError(.cause=OutOfMemoryError)` |

### split=True, max_files=25（启用 Row Group 切分）

| # | Dataset | RG 数 | RG 解压 | 状态 | 耗时 | Worker 峰值 | Spill |
|---|---------|-------|---------|------|------|------------|-------|
| 1 | 50m | 16 | ~250 MB | ✅ OK | 5.4s | — | — |
| 2 | 50m | 16 | ~250 MB | ✅ OK | 5.8s | — | — |
| 3 | 50m | 16 | ~250 MB | ✅ OK | 4.6s | — | — |
| 4 | 100m | 32 | ~250 MB | ✅ OK | 10.7s | — | 2286 MiB |
| 5 | 100m | 32 | ~250 MB | ✅ OK | 7.7s | — | 2257 MiB |
| 6 | 100m | 32 | ~250 MB | ✅ OK | 8.4s | — | 2117 MiB |
| 7 | 200m | 64 | ~250 MB | ✅ OK | 21.0s | 4.10 GB (99.9%) | 5722 MiB |
| 8 | 200m-8f | 32 | **~500 MB** | ❌ OOM | 4.4s | 97.0% | — |
| 9 | 200m-8f | 32 | **~500 MB** | ❌ OOM | 4.4s | 97.7% | — |

### 汇总

| Config | 50m | 100m | 200m | 200m-8f |
|--------|-----|------|------|---------|
| split=False | ❌❌❌ | ❌❌❌ | ❌❌❌ | — |
| split=True | ✅✅✅ | ✅✅✅ | ✅ | ❌❌ |

### 关键发现

1. **split=False 100% OOM**：所有数据集、所有迭代均触发 `RayTaskError(.cause=OutOfMemoryError)`，无一例外
2. **split=True 挽救小 RG 场景**：50m/100m/200m（RG 250MB）全部通过，4 并发 RG task × 250MB = 1GB 堆内存，低于 4GB × 95% 阈值
3. **split=True 无法挽救大 RG 场景**：200m-8f（RG 500MB）仍然 OOM——4 并发 × 500MB = 2GB 堆内存 + 解码器缓冲 > 3.8GB 阈值
4. **OOM 异常链确认**：`RayTaskError(.cause=OutOfMemoryError)` — 不是 `ActorDiedError`。Ray memory monitor kill 走 `OUT_OF_MEMORY` 分支，Daft 当前不重试
5. **Object Store spill 不等于堆内存保护**：200m split=True 时 spill 5.7GB 有效清空 Object Store，但 RSS 堆内存仍达 4.10GB——spill 管不到正在解码中的 RG task 堆内存

### 错误分类逻辑

`run_test.py:_classify_error()` 检查完整异常链：`type(e).__name__` → `e.cause` → `e.__cause__` → `str(e)`。覆盖 `RayTaskError(OutOfMemoryError)` 包装层级。

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

Each dataset runs 3 iterations × 2 configs (split=False / split=True). Results logged to `--log-dir` (default `/tmp/daft-oom-results/`).

### Single-iteration mode

For scripted loops with worker restarts between iterations:

```bash
docker compose exec ray-head python /tmp/scripts/run_test.py \
    --single --dataset 200m-8f --config split --iteration 1
```

Config options: `no_split` (split=False) or `split` (split=True, max_files=25).

### Log collection

Head-node logs (cgroup memory, raylet.out/err, ray status) are automatically collected before and after each test iteration into `--log-dir/<run_id>/`.

Worker logs must be collected externally:

```bash
docker compose logs ray-worker01 > /tmp/worker01.log
docker compose logs ray-worker02 > /tmp/worker02.log
```

## Why It Happens

Daft uses a Ray Actor model. Multiple scan tasks run concurrently inside the same Actor process, sharing heap memory. Each scan task internally spawns **all Row Group decoders concurrently** via `build_rg_stream()` in `daft-parquet` — without any memory-aware concurrency control.

```
Worker 容器 4GB
├── RSS 堆内存（PhysicalScan 进程）
│   ├── ScanTask #1 → RG decoder × 4（全部并发）
│   ├── ScanTask #2 → RG decoder × 4（全部并发）
│   └── ...  ← 所有 RG 同时解码，堆内存叠加
│
└── Ray Object Store（已完成 partition 的 ObjectRef）
    → spill 能清空这里，但管不到正在解码的 RSS
```

**两层并发，只有第一层受控**：

| 层次 | 控制机制 | 是否受控 |
|------|---------|---------|
| scan task 级 | `scantask_max_parallel`（默认 8） | ✅ |
| Row Group 解码器级 | 无 | ❌ **这是 gap** |

**200m-8f OOM 的精确原因**：4 并发 scan task × 每 task 4 个 RG 同时解码 → 16 个 RG decoder 争抢 4GB 堆内存。即使 split 将 task 拆成 RG 级，每个 task 内的多个 RG 仍然全部并发。

**完整根因分析**见 `/Users/jiangxintong/Presentations/github/daft-ray-worker-oom-analysis.md`。
**修复方案**见 `/Users/jiangxintong/Presentations/github/daft-parquet-rg-concurrency-fix.md`。

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
| `.env.example` | Configurable parameters | — |

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

## Related Documents

| Document | Path | Content |
|----------|------|---------|
| Test Report | `TEST_REPORT.md` | Full test details, OOM analysis, RSS vs spill |
| Root Cause Analysis | `~/Presentations/github/daft-ray-worker-oom-analysis.md` | Source-level analysis, exception chains, fix proposals |
| Contribution Plan | `~/Presentations/github/daft-parquet-rg-concurrency-fix.md` | Fix design, code locations, PR strategy |

