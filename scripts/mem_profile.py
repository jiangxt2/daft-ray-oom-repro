#!/usr/bin/env python3
"""Memory profiler for Daft Parquet reads — runs alongside any Daft operation.

Captures a time series of RSS, jemalloc (if available), and cgroup memory
at 200ms intervals in a background thread. Results written as JSONL.

Usage:
  from mem_profile import MemoryProfiler
  with MemoryProfiler("/tmp/mem_samples.jsonl") as prof:
      df.collect()  # or any daft operation

  # Analyze later:
  python mem_profile.py --plot /tmp/mem_samples.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from pathlib import Path
from typing import Any

# ── RSS (Linux /proc, macOS resource) ──

def _get_rss_bytes() -> int:
    """Get current process RSS in bytes. Linux / macOS compatible."""
    if sys.platform == "linux":
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024  # kB → bytes
        except Exception:
            pass
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return 0


def _read_cgroup_memory() -> dict[str, Any]:
    """Read cgroup memory stats (Linux only, Docker containers)."""
    info = {}
    for fname in (
        "memory.usage_in_bytes",
        "memory.max_usage_in_bytes",
        "memory.limit_in_bytes",
    ):
        path = f"/sys/fs/cgroup/memory/{fname}"
        try:
            with open(path) as f:
                info[fname] = int(f.read().strip())
        except (FileNotFoundError, ValueError):
            pass
    # Also try memory.current (cgroup v2)
    if not info:
        try:
            with open("/sys/fs/cgroup/memory.current") as f:
                info["memory.current"] = int(f.read().strip())
            with open("/sys/fs/cgroup/memory.max") as f:
                val = f.read().strip()
                info["memory.max"] = (
                    int(val) if val != "max" else None
                )
        except (FileNotFoundError, ValueError):
            pass
    return info


# ── Profiler ──

class MemoryProfiler:
    """Background memory sampler. Use as context manager."""

    def __init__(self, output_path: str, interval_s: float = 0.2):
        self.output_path = Path(output_path)
        self.interval_s = interval_s
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._samples: list[dict] = []

    def _sample(self) -> dict:
        sample = {
            "timestamp": time.time(),
            "rss_bytes": _get_rss_bytes(),
        }
        # Platform-native memory info
        try:
            import resource
            sample["ru_maxrss"] = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            )
        except Exception:
            pass
        # cgroup (Docker)
        cg = _read_cgroup_memory()
        if cg:
            sample["cgroup"] = cg
        return sample

    def _loop(self):
        while not self._stop.is_set():
            s = self._sample()
            self._samples.append(s)
            self._stop.wait(self.interval_s)

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def flush(self):
        """Write all samples to the output file."""
        with open(self.output_path, "w") as f:
            for s in self._samples:
                f.write(json.dumps(s) + "\n")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
        self.flush()

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    def stats(self) -> dict:
        if not self._samples:
            return {}
        rss = [s["rss_bytes"] for s in self._samples if s["rss_bytes"] > 0]
        return {
            "num_samples": len(self._samples),
            "duration_s": (
                self._samples[-1]["timestamp"]
                - self._samples[0]["timestamp"]
                if len(self._samples) > 1
                else 0
            ),
            "rss_peak_bytes": max(rss) if rss else 0,
            "rss_peak_mb": max(rss) / (1024 * 1024) if rss else 0,
            "rss_mean_mb": (
                sum(rss) / len(rss) / (1024 * 1024) if rss else 0
            ),
        }


# ── CLI for analysis ──

def _plot(samples_path: str):
    """Simple text-based memory plot (no matplotlib dependency)."""
    samples = []
    with open(samples_path) as f:
        for line in f:
            line = line.strip()
            if line:
                samples.append(json.loads(line))

    if not samples:
        print("No samples found.")
        return

    t0 = samples[0]["timestamp"]
    rss_values = [s.get("rss_bytes", 0) / (1024 * 1024) for s in samples]

    print(f"Samples : {len(samples)}")
    print(f"Duration: {samples[-1]['timestamp'] - t0:.1f}s")
    print(f"RSS peak: {max(rss_values):.0f} MB")
    print(f"RSS mean: {sum(rss_values) / len(rss_values):.0f} MB")
    print()

    # ASCII chart
    if rss_values:
        peak = max(rss_values)
        width = 60
        step = max(1, len(rss_values) // width)
        print(f"RSS timeline (0 → {peak:.0f} MB):")
        bars = []
        for i in range(0, len(rss_values), step):
            v = rss_values[i]
            n = int(v / peak * width) if peak > 0 else 0
            bars.append("█" * n + "░" * (width - n))
        for bar in bars:
            print(f"  {bar}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Memory profiler for Daft Parquet reads"
    )
    parser.add_argument(
        "--plot", metavar="JSONL", help="Display text chart from a profile run"
    )
    args = parser.parse_args()

    if args.plot:
        _plot(args.plot)
    else:
        print(
            "Usage: Import MemoryProfiler in your Daft script, "
            "or run with --plot <file> to analyze."
        )
        print(
            "\nSee docstring for usage examples."
        )


if __name__ == "__main__":
    main()
