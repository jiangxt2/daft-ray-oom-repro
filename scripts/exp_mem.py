#!/usr/bin/env python3
"""Memory profiling experiment: does Daft have a memory manager for Parquet RG decoding?

Hypothesis to verify:
  H1: No memory manager — RSS scales linearly with concurrent RG count × RG size.
  H2: split=False — ALL RGs in a file decode concurrently → peak = Σ(RG per task).
  H3: split=True — single-task RGs still all concurrent → 200m-8f should OOM.

Experiment matrix:
  Dataset × Config → capture RSS(t) time series → compare peaks

Usage (inside ray-head container):
  docker compose exec ray-head python /tmp/scripts/exp_mem.py --run
  docker compose exec ray-head python /tmp/scripts/exp_mem.py --plot /tmp/daft-mem-exp/
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mem_profile import MemoryProfiler


def _classify_error(e: Exception) -> str:
    """Classify exception as OOM or other. Checks .cause chain (RayTaskError wrapping)."""
    etype = type(e).__name__
    if "OutOfMemory" in etype:
        return "OOM"
    cause = getattr(e, "cause", None)
    if cause is not None:
        cause_type = type(cause).__name__
        if "OutOfMemory" in cause_type:
            return "OOM"
        if "ActorDied" in cause_type:
            return "OOM"
    if "low on memory" in str(e).lower():
        return "OOM"
    if etype == "RayTaskError" and "OutOfMemory" in str(e):
        return "OOM"
    return f"ERROR({etype})"

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_KEY = os.environ["MINIO_USER"]
S3_SECRET = os.environ["MINIO_PASSWORD"]
BUCKET = os.environ["S3_BUCKET"]

# ── Experiment matrix ──
EXPERIMENTS = [
    # (dataset, split, max_files, description)
    ("200m",     False, 0,  "200m split=False — ALL RGs concurrent per task"),
    ("200m",     True,  25, "200m split=True  — RG-level tasks, 250MB each"),
    ("200m-8f",  True,  25, "200m-8f split=True — RG-level tasks, 500MB each"),
    ("200m-8f",  False, 0,  "200m-8f split=False — ALL 500MB RGs concurrent"),
]


def _make_io_config():
    from daft.io import IOConfig, S3Config
    return IOConfig(
        s3=S3Config(
            key_id=S3_KEY, access_key=S3_SECRET, endpoint_url=S3_ENDPOINT,
        )
    )


def _check_data(dataset: str, expected: int):
    import s3fs
    fs = s3fs.S3FileSystem(
        key=S3_KEY, secret=S3_SECRET,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
    )
    existing = fs.glob(f"{BUCKET}/{dataset}_data_*.parquet")
    if len(existing) != expected:
        print(f"ERROR: Dataset '{dataset}' has {len(existing)} files, "
              f"expected {expected}. Run gen_data.py first.")
        sys.exit(1)


def run_one(dataset: str, split: bool, max_files: int,
            log_dir: Path) -> dict:
    """Run one experiment and return the profile + result."""
    import ray
    ray.init(address="auto", ignore_reinit_error=True)

    import daft
    if split:
        daft.set_execution_config(
            enable_scan_task_split_and_merge=True,
            parquet_split_row_groups_max_files=max_files,
        )

    io_config = _make_io_config()
    glob_pattern = f"s3://{BUCKET}/{dataset}_data_*.parquet"

    label = f"{dataset}_split={split}"
    profile_path = log_dir / f"{label}.jsonl"
    profiler = MemoryProfiler(str(profile_path))

    gc.collect()
    time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

    profiler.start()
    t0 = time.time()

    try:
        df = daft.read_parquet(glob_pattern, io_config=io_config)
        result = df.collect()
        rows = len(result)
        elapsed = time.time() - t0
        status = "OK"
        error = None
    except Exception as e:
        elapsed = time.time() - t0
        status = _classify_error(e)
        error = str(e)[:500]
        rows = 0

    profiler.stop()
    profiler.flush()

    stats = profiler.stats()
    result = {
        "dataset": dataset,
        "split": split,
        "max_files": max_files,
        "status": status,
        "rows": rows,
        "elapsed_s": round(elapsed, 1),
        "num_samples": stats.get("num_samples", 0),
        "rss_peak_mb": stats.get("rss_peak_mb", 0),
        "rss_mean_mb": stats.get("rss_mean_mb", 0),
        "error": error,
    }

    print(f"  → {status}  rows={rows}  elapsed={elapsed:.1f}s  "
          f"rss_peak={result['rss_peak_mb']:.0f}MB  "
          f"samples={stats.get('num_samples', 0)}")

    ray.shutdown()
    return result


def run_all(log_dir: str):
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Verify data
    print("Checking data ...")
    _check_data("200m", 16)
    _check_data("200m-8f", 8)

    results = []
    for dataset, split, max_files, desc in EXPERIMENTS:
        print(f"\n── {desc} ──")
        r = run_one(dataset, split, max_files, log_dir)
        r["description"] = desc
        results.append(r)
        # Write incremental result
        with open(log_dir / "summary.json", "w") as f:
            json.dump(results, f, indent=2)

    # Print comparison table
    print(f"\n{'='*80}")
    print("  Comparison: RSS Peak vs Expected (if no memory manager)")
    print(f"{'='*80}")
    print(f"  {'Experiment':<35} {'Status':<8} {'Peak RSS':<12} {'Expected':<12}")
    print(f"  {'-'*67}")

    for r in results:
        ds = r["dataset"]
        sp = r["split"]
        # Expected peak if no memory manager:
        #   split=False: all RGs in task concurrent → files × RGs_per_file × RG_size
        #   split=True:  per-task still all RGs concurrent → (same file's RGs are concurrent)
        if ds == "200m":
            rg_mb = 250
            files = 16
        else:
            rg_mb = 500
            files = 8

        if sp:
            # split=True: each task = 1 file, all 4 RGs concurrent
            expected_gb = 4 * rg_mb / 1024
            expected_str = f"~{expected_gb:.1f} GB"
        else:
            # split=False: all files' RGs distributed, but single task = whole file
            expected_gb = 4 * rg_mb / 1024
            expected_str = f"~{expected_gb:.1f} GB"

        peak_str = f"{r['rss_peak_mb']:.0f} MB" if r['rss_peak_mb'] else "N/A"
        print(f"  {r['description']:<35} {r['status']:<8} {peak_str:<12} "
              f"{expected_str:<12}")

    print(f"\n  Results saved to {log_dir}/")
    print(f"  Per-run profiles: {log_dir}/*.jsonl")


def plot_results(log_dir: str):
    """Print text charts for all profile runs."""
    import subprocess
    log_dir = Path(log_dir)
    for f in sorted(log_dir.glob("*.jsonl")):
        print(f"\n{'─'*60}")
        print(f"  {f.name}")
        print(f"{'─'*60}")
        subprocess.run([sys.executable, __file__.replace("exp_mem", "mem_profile"),
                        "--plot", str(f)])


def main():
    parser = argparse.ArgumentParser(
        description="Daft Parquet Memory Profiling Experiment"
    )
    parser.add_argument("--run", action="store_true",
                        help="Run the full experiment matrix")
    parser.add_argument("--plot", metavar="DIR",
                        help="Plot all profiles from a previous run")
    parser.add_argument("--log-dir", default="/tmp/daft-mem-exp",
                        help="Output directory (default: /tmp/daft-mem-exp)")
    args = parser.parse_args()

    if args.run:
        run_all(args.log_dir)
    elif args.plot:
        plot_results(args.plot)
    else:
        print("Usage: exp_mem.py --run | --plot <dir>")


if __name__ == "__main__":
    main()
