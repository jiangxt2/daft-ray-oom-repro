#!/usr/bin/env python3
"""Daft on Ray Worker OOM — repeatable scenario runner.

For each dataset, runs split=False and split=True (max_files=25) × 3 iterations.
Auto-generates data if missing. Collects logs from Head node after each test.

Usage:
  # Run all tests (single invocation)
  docker compose exec ray-head python /tmp/scripts/run_test.py --all

  # Single iteration (for scripted loops with worker restarts)
  docker compose exec ray-head python /tmp/scripts/run_test.py \
      --single --dataset 50m --config no_split --iteration 1

  # Results + logs saved to --log-dir (default /tmp/daft-oom-results)
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path


# ── Config ──

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_KEY = os.environ["MINIO_USER"]
S3_SECRET = os.environ["MINIO_PASSWORD"]
BUCKET = os.environ["S3_BUCKET"]

ROWS_PER_FILE = 12_500_000

PRESETS = {
    "50m":  (50_000_000,  4,   "~2.5 GB"),
    "100m": (100_000_000, 8,   "~5 GB"),
    "200m":     (200_000_000, 16, "~6.8 GB"),
    "200m-8f":  (200_000_000, 8,  "~6.8 GB (25M rows/file)"),
}

ITERATIONS = 3
MAX_FILES = 25


# ── Helpers ──

def _glob_pattern(dataset: str) -> str:
    return f"s3://{BUCKET}/{dataset}_data_*.parquet"


def _make_io_config():
    from daft.io import IOConfig, S3Config
    return IOConfig(
        s3=S3Config(
            key_id=S3_KEY, access_key=S3_SECRET,
            endpoint_url=S3_ENDPOINT,
        )
    )


# ── Cluster ──

def check_cluster() -> bool:
    import ray
    try:
        ray.init(address="auto", ignore_reinit_error=True, logging_level="error")
        nodes = [n for n in ray.nodes() if n["Alive"]]
        resources = ray.cluster_resources()
        print(f"  Nodes: {len(nodes)}/3, CPU={resources.get('CPU', 0)}, "
              f"memory={resources.get('memory', 0):.0f} B")
        ray.shutdown()
        return len(nodes) >= 3
    except Exception:
        traceback.print_exc()
        return False


# ── Data ──

def _check_data(dataset: str) -> int:
    """Verify data exists in MinIO. Does NOT generate — use gen_data.py for that."""
    import s3fs
    fs = s3fs.S3FileSystem(
        key=S3_KEY, secret=S3_SECRET,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
    )
    existing = fs.glob(f"{BUCKET}/{dataset}_data_*.parquet")
    num_rows, expected_files, size_desc = PRESETS[dataset]

    if len(existing) == expected_files:
        print(f"  Data found: {len(existing)} file(s) in s3://{BUCKET}/")
        return expected_files

    print(f"ERROR: Dataset '{dataset}' not found. Expected {expected_files} "
          f"file(s), found {len(existing)}.")
    print(f"Run: docker compose exec ray-head python /tmp/scripts/gen_data.py "
          f"--{dataset}")
    sys.exit(1)


# ── Result logging ──

class ResultLogger:
    """Append results to a JSONL file inside the container."""

    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.log_dir / "results.jsonl"

    def log(self, result: dict):
        with open(self.jsonl_path, "a") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")

    def load_all(self) -> list[dict]:
        if not self.jsonl_path.exists():
            return []
        results = []
        with open(self.jsonl_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    results.append(json.loads(line))
        return results


# ── Log collection (Head node) ──

def _read_cgroup_memory() -> dict:
    """Read cgroup v1 memory stats from /sys/fs/cgroup/memory/."""
    info = {}
    for fname in ("memory.usage_in_bytes", "memory.max", "memory.limit_in_bytes",
                  "memory.stat"):
        path = f"/sys/fs/cgroup/memory/{fname}"
        try:
            with open(path) as f:
                val = f.read().strip()
            if fname == "memory.stat":
                # Parse space-separated key-value pairs (cgroup v1 format:
                # "total_cache 123\ntotal_rss 456\n..."), keep total_* lines.
                total = {}
                for line in val.split("\n"):
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    k, v = parts[0], parts[1]
                    if k.startswith("total_"):
                        total[k] = int(v)
                info[fname] = total
            else:
                info[fname] = int(val)
        except (FileNotFoundError, ValueError, PermissionError):
            info[fname] = None
    return info


def _tail_raylet_logs(max_lines: int = 200) -> dict[str, str]:
    """Tail raylet.out and raylet.err from the current Ray session."""
    logs = {"raylet_out": "", "raylet_err": ""}
    session_dir = Path("/tmp/ray/session_latest/logs")
    if not session_dir.exists():
        # Try to find the actual session dir
        ray_tmp = Path("/tmp/ray")
        if ray_tmp.exists():
            for d in sorted(ray_tmp.iterdir(), reverse=True):
                if d.is_dir() and (d / "logs" / "raylet.out").exists():
                    session_dir = d / "logs"
                    break

    for fname in ("raylet.out", "raylet.err"):
        path = session_dir / fname
        if path.exists():
            try:
                out = subprocess.run(
                    ["tail", f"-n{max_lines}", str(path)],
                    capture_output=True, text=True, timeout=5,
                )
                logs[fname.replace(".", "_")] = out.stdout[-50000:]  # last 50KB
            except Exception:
                logs[fname.replace(".", "_")] = "(read failed)"
    return logs


def _get_ray_status() -> dict:
    """Collect Ray cluster status via ray CLI and Python API."""
    info = {}
    try:
        out = subprocess.run(
            ["ray", "status"], capture_output=True, text=True, timeout=10,
        )
        info["ray_status"] = out.stdout[-10000:]  # last 10KB
    except Exception as e:
        info["ray_status"] = f"(ray status failed: {e})"

    # Also get detailed node info via Python API
    try:
        import ray
        ray.init(address="auto", ignore_reinit_error=True, logging_level="error")
        nodes = []
        for n in ray.nodes():
            nodes.append({
                "Alive": n.get("Alive"),
                "NodeName": n.get("NodeName"),
                "NodeManagerAddress": n.get("NodeManagerAddress"),
                "Resources": n.get("Resources", {}),
            })
        info["ray_nodes"] = nodes
        info["cluster_resources"] = ray.cluster_resources()
        ray.shutdown()
    except Exception as e:
        info["ray_nodes"] = f"(failed: {e})"

    return info


def _collect_head_logs(run_dir: Path, label: str):
    """Collect all available Head-node logs into run_dir."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # Cgroup memory
    cgroup = _read_cgroup_memory()
    (run_dir / f"head_cgroup_{ts}.json").write_text(
        json.dumps(cgroup, indent=2, default=str))

    # Raylet logs
    raylets = _tail_raylet_logs()
    for key, content in raylets.items():
        (run_dir / f"head_{key}_{ts}.txt").write_text(content)

    # Ray status
    status = _get_ray_status()
    (run_dir / f"head_ray_status_{ts}.json").write_text(
        json.dumps(status, indent=2, default=str))


# ── OOM detection ──
#
# Per analysis doc (daft-ray-worker-oom-analysis.md §测试结果):
#   The actual OOM exception chain is:
#     RayTaskError(.cause=OutOfMemoryError)
#   NOT ActorDiedError.
#
#   OutOfMemoryError: Ray memory monitor kills Worker when memory > 95% threshold
#   ActorDiedError:   Worker process crashes (different kill path, not triggered here)
#
# We check BOTH the top-level exception type AND the .cause chain.


from utils import classify_error as _classify_error


# ── Scenario runner ──

def _run_one(label: str, fn, logger: ResultLogger | None = None) -> dict:
    """Run one test iteration, measure memory, catch all exceptions."""
    print(f"    {label} ...", end=" ", flush=True)
    gc.collect()
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    t_start = time.time()

    try:
        rows = fn()
        elapsed = time.time() - t_start
        rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        result = {
            "status": "OK",
            "rows": rows,
            "elapsed_s": round(elapsed, 1),
            "rss_delta_mb": round(rss_after - rss_before, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        elapsed = time.time() - t_start
        rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        etype = type(e).__name__
        emsg = str(e)[:500]

        # Also capture .cause if present (RayTaskError wraps real error)
        cause = getattr(e, "cause", None)
        cause_info = None
        if cause is not None:
            cause_info = {
                "cause_type": type(cause).__name__,
                "cause_message": str(cause)[:500],
            }

        status = _classify_error(e)
        result = {
            "status": status,
            "error_type": etype,
            "error": emsg,
            "cause": cause_info,
            "elapsed_s": round(elapsed, 1),
            "rss_delta_mb": round(rss_after - rss_before, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    print(json.dumps(result, ensure_ascii=False))
    if logger:
        logger.log(result)
    return result


# ── Scenarios ──

def _do_daft_no_split(glob_pattern: str):
    """Pitfall 1 (analysis doc): must call ray.init(address='auto') BEFORE daft import.
    Otherwise daft's RayRunner.__init__ calls ray.init(address=None),
    starts a local Raylet on Head → +1.3GB RSS, Workers stay idle."""
    import ray
    ray.init(address="auto", ignore_reinit_error=True)
    import daft
    io_config = _make_io_config()
    df = daft.read_parquet(glob_pattern, io_config=io_config)
    result = df.collect()
    return len(result)


def _do_daft_with_split(glob_pattern: str):
    """Same pitfall avoidance as _do_daft_no_split."""
    import ray
    ray.init(address="auto", ignore_reinit_error=True)
    import daft
    daft.set_execution_config(
        enable_scan_task_split_and_merge=True,
        parquet_split_row_groups_max_files=MAX_FILES,
    )
    io_config = _make_io_config()
    df = daft.read_parquet(glob_pattern, io_config=io_config)
    result = df.collect()
    return len(result)


# ── Single-iteration mode ──

def run_single(dataset: str, config: str, iteration: int, log_dir: str) -> dict:
    """Run a single test iteration. Used by external loop with worker restarts.

    Collects Head-node logs after the test completes.
    Worker logs must be collected by the external wrapper (docker compose exec).
    """
    num_rows, files, size_desc = PRESETS[dataset]
    logger = ResultLogger(log_dir)

    run_id = f"{dataset}_{config}_iter{iteration}"
    run_dir = Path(log_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Dataset: {dataset}  config={config}  iteration={iteration}/{ITERATIONS}")
    print(f"{'='*60}")

    _check_data(dataset)
    glob_pattern = _glob_pattern(dataset)

    if config == "no_split":
        fn = lambda: _do_daft_no_split(glob_pattern)
        cfg_label = "split=False"
    elif config == "split":
        fn = lambda: _do_daft_with_split(glob_pattern)
        cfg_label = "split=True max_files=25"
    else:
        print(f"ERROR: unknown config '{config}'")
        sys.exit(1)

    # Collect pre-test state
    _collect_head_logs(run_dir, "pre")

    result = _run_one(f"run {iteration}/{ITERATIONS}", fn, logger=logger)
    result["dataset"] = dataset
    result["config"] = cfg_label
    result["iteration"] = iteration
    result["run_id"] = run_id

    # Collect post-test state
    _collect_head_logs(run_dir, "post")

    return result


# ── Full-suite mode ──

def run_all_for_datasets(datasets: list[str], log_dir: str):
    """Run full test suite for given datasets. Caller responsible for worker restarts."""
    logger = ResultLogger(log_dir)

    print("=" * 60)
    print("  Ray Cluster Status")
    print("=" * 60)
    if not check_cluster():
        print("ERROR: Ray cluster not healthy.")
        sys.exit(1)

    all_results = []

    for dataset in datasets:
        num_rows, files, size_desc = PRESETS[dataset]
        print(f"\n{'='*60}")
        print(f"  Dataset: {dataset}  ({num_rows:,} rows, {files} files, "
              f"{size_desc})")
        print(f"{'='*60}")
        _check_data(dataset)
        glob_pattern = _glob_pattern(dataset)

        configs = [
            ("split=False", lambda: _do_daft_no_split(glob_pattern)),
            ("split=True max_files=25", lambda: _do_daft_with_split(glob_pattern)),
        ]

        for cfg_name, fn in configs:
            print(f"\n  ── {cfg_name} ({ITERATIONS} iterations) ──")
            for i in range(1, ITERATIONS + 1):
                result = _run_one(f"run {i}/{ITERATIONS}", fn, logger=logger)
                result["dataset"] = dataset
                result["config"] = cfg_name
                result["iteration"] = i
                all_results.append(result)

    _print_summary(all_results, datasets)
    _write_summary_file(logger, all_results, datasets)


def _print_summary(all_results: list[dict], datasets: list[str]):
    print(f"\n{'='*90}")
    print(f"  Summary")
    print(f"{'='*90}")
    print(f"  {'Dataset':<8} {'Config':<28} {'Run':<6} {'Status':<16} "
          f"{'Time':<10} {'RSS Δ':<10}")
    print(f"  {'-'*78}")
    for r in all_results:
        ds = r.get("dataset", "?")
        cfg = r.get("config", "?")
        it = r.get("iteration", "?")
        status = str(r.get("status", "?"))
        elapsed = f"{r.get('elapsed_s', 'N/A')}s"
        rss = str(r.get("rss_delta_mb", "N/A"))
        print(f"  {ds:<8} {cfg:<28} {it:<6} {status:<16} {elapsed:<10} "
              f"{rss:<10}")
    print(f"{'='*90}")

    for ds in datasets:
        for cfg_name in ["split=False", "split=True max_files=25"]:
            subset = [r for r in all_results
                      if r["dataset"] == ds and r["config"] == cfg_name]
            ok = sum(1 for r in subset if r["status"] == "OK")
            oom = sum(1 for r in subset if "OOM" in str(r["status"]))
            err = len(subset) - ok - oom
            print(f"  {ds} {cfg_name:<28}: OK={ok} OOM={oom} ERR={err}")


def _write_summary_file(logger: ResultLogger, all_results: list[dict],
                        datasets: list[str]):
    """Write a human-readable summary to the log directory."""
    lines = []
    lines.append("# Daft on Ray Worker OOM — Test Results")
    lines.append(f"# Run at: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("| Dataset | Config | Run | Status | Time | RSS Δ (MB) | Error |")
    lines.append("|---------|--------|-----|--------|------|------------|-------|")
    for r in all_results:
        ds = r.get("dataset", "?")
        cfg = r.get("config", "?")
        it = str(r.get("iteration", "?"))
        status = str(r.get("status", "?"))
        elapsed = f"{r.get('elapsed_s', 'N/A')}s"
        rss = str(r.get("rss_delta_mb", "N/A"))
        err = r.get("error", "")[:80] if r.get("error") else ""
        lines.append(f"| {ds} | {cfg} | {it} | {status} | {elapsed} | {rss} | {err} |")

    lines.append("")
    lines.append("## Per-config summary")
    lines.append("")
    lines.append("| Dataset | Config | OK | OOM | ERR |")
    lines.append("|---------|--------|----|-----|-----|")
    for ds in datasets:
        for cfg_name in ["split=False", "split=True max_files=25"]:
            subset = [r for r in all_results
                      if r["dataset"] == ds and r["config"] == cfg_name]
            ok = sum(1 for r in subset if r["status"] == "OK")
            oom = sum(1 for r in subset if "OOM" in str(r["status"]))
            err = len(subset) - ok - oom
            lines.append(f"| {ds} | {cfg_name} | {ok} | {oom} | {err} |")

    summary_path = Path(logger.log_dir) / "summary.md"
    summary_path.write_text("\n".join(lines) + "\n")
    print(f"\n  Summary written to {summary_path}")


# ── Main ──

def main():
    parser = argparse.ArgumentParser(
        description="Daft on Ray Worker OOM — Test Runner"
    )
    parser.add_argument("--50m", action="store_true")
    parser.add_argument("--100m", action="store_true")
    parser.add_argument("--200m", action="store_true")
    parser.add_argument("--200m-8f", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--single", action="store_true",
                        help="Run a single iteration (use with --dataset, --config, --iteration)")
    parser.add_argument("--dataset", choices=["50m", "100m", "200m", "200m-8f"])
    parser.add_argument("--config", choices=["no_split", "split"])
    parser.add_argument("--iteration", type=int, choices=[1, 2, 3])
    parser.add_argument("--log-dir", default="/tmp/daft-oom-results",
                        help="Directory for result logs (inside container)")
    args = parser.parse_args()

    # ── Single-iteration mode ──
    if args.single:
        if not (args.dataset and args.config and args.iteration):
            print("ERROR: --single requires --dataset, --config, --iteration")
            sys.exit(1)
        run_single(args.dataset, args.config, args.iteration, args.log_dir)
        return

    # ── Full-suite mode ──
    datasets = []
    if args.all:
        datasets = ["50m", "100m", "200m", "200m-8f"]
    else:
        for flag in ("50m", "100m", "200m", "200m-8f"):
            if getattr(args, flag.replace("-", "_"), False):
                datasets.append(flag)

    if not datasets:
        print("Usage: run_test.py --50m | --100m | --200m | --200m-8f | --all")
        print("       run_test.py --single --dataset X --config Y --iteration Z")
        sys.exit(1)

    run_all_for_datasets(datasets, args.log_dir)


if __name__ == "__main__":
    main()
