#!/usr/bin/env python3
"""Generate test Parquet data and upload to MinIO.

Run inside the Ray head container:
  docker compose exec ray-head python /tmp/scripts/gen_data.py --50m
  docker compose exec ray-head python /tmp/scripts/gen_data.py --100m
  docker compose exec ray-head python /tmp/scripts/gen_data.py --200m

All configuration from environment variables (set by docker-compose.yml).
"""
from __future__ import annotations

import argparse
import gc
import os
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# ── Config (all from environment) ──

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_KEY = os.environ["MINIO_USER"]
S3_SECRET = os.environ["MINIO_PASSWORD"]
BUCKET = os.environ["S3_BUCKET"]
TMP_DIR = Path("/tmp/daft-oom-repro-data")

# Each file: ~512 MB compressed, 4 Row Groups.
# Sequential + low-cardinality columns → zstd ~2× compression → ~500 MB/file.
ROWS_PER_RG = 3_125_000   # 3.125M rows per Row Group
RGS_PER_FILE = 4          # 4 Row Groups per file
rows_per_file = ROWS_PER_RG * RGS_PER_FILE  # 12.5M rows/file

PRESETS = {
    "50m":      (50_000_000,  12_500_000, "4 files,  ~1.7 GB compressed"),
    "100m":     (100_000_000, 12_500_000, "8 files,  ~3.4 GB compressed"),
    "200m":     (200_000_000, 12_500_000, "16 files, ~6.8 GB compressed"),
    "200m-8f":  (200_000_000, 25_000_000, "8 files,  ~6.8 GB (25M rows/file)"),
}


def generate(dataset_name: str, num_rows: int, rows_per_file: int = rows_per_file):
    """Generate Parquet files and upload to MinIO."""
    files = num_rows // rows_per_file
    rows_per_rg = rows_per_file // RGS_PER_FILE
    assert num_rows % rows_per_file == 0, \
        f"Rows ({num_rows}) must be divisible by {rows_per_file}"

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    fs = s3fs.S3FileSystem(
        key=S3_KEY, secret=S3_SECRET,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
    )
    try:
        fs.mkdir(BUCKET)
    except FileExistsError:
        pass

    print(f"Dataset:  {dataset_name}")
    print(f"Rows:     {num_rows:,}")
    print(f"Files:    {files} × ~{rows_per_file:,} rows")
    print(f"Row Grp:  {RGS_PER_FILE}/file × {rows_per_rg:,} rows/group")
    print(f"Columns:  10 × int64 (random, ~80 bytes/row uncompressed)")
    print(f"Target:   ~512 MB compressed / file (zstd)")
    print(f"Bucket:   s3://{BUCKET}/")
    print()

    rng = np.random.default_rng(42)
    total_compressed_mb = 0

    for i in range(files):
        fname = f"{dataset_name}_data_{i:03d}.parquet"
        local_path = TMP_DIR / fname

        # Write row groups one at a time to keep memory bounded (~rows_per_rg × 10 cols)
        schema = pa.schema([(f"col_{c}", pa.int64()) for c in range(10)])
        writer = pq.ParquetWriter(
            str(local_path), schema, compression="zstd",
        )

        for rg in range(RGS_PER_FILE):
            rg_start = i * rows_per_file + rg * rows_per_rg
            data = {}

            data["col_0"] = np.arange(rg_start, rg_start + rows_per_rg, dtype=np.int64)
            data["col_1"] = np.arange(rg_start + 1, rg_start + rows_per_rg + 1, dtype=np.int64)

            for c in (2, 3, 4):
                data[f"col_{c}"] = rng.integers(
                    0, 10_000, size=rows_per_rg, dtype=np.int64)

            for c in (5, 6, 7):
                data[f"col_{c}"] = rng.integers(
                    0, 10 ** 9, size=rows_per_rg, dtype=np.int64)

            for c in (8, 9):
                data[f"col_{c}"] = rng.integers(
                    0, 2 ** 63 - 1, size=rows_per_rg, dtype=np.int64)

            writer.write_table(pa.table(data))
            del data
            gc.collect()

        writer.close()
        compressed_mb = local_path.stat().st_size / (1024 * 1024)

        s3_key = f"{BUCKET}/{fname}"
        fs.put(str(local_path), s3_key)

        total_compressed_mb += compressed_mb
        print(f"  [{i+1:>3}/{files}] {fname}  {compressed_mb:7.0f} MB")

        local_path.unlink()
        gc.collect()

    avg_mb = total_compressed_mb / files
    print()
    print(f"Done. {files} file(s), {total_compressed_mb:.0f} MB total "
          f"({avg_mb:.0f} MB avg/file), "
          f"{num_rows:,} rows in s3://{BUCKET}/{dataset_name}_data_*")
    try:
        TMP_DIR.rmdir()
    except OSError:
        pass  # directory may have leftover files from interrupted runs


def check_existing(dataset: str) -> int:
    """Return number of existing files for this dataset in MinIO."""
    fs = s3fs.S3FileSystem(
        key=S3_KEY, secret=S3_SECRET,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
    )
    return len(fs.glob(f"{BUCKET}/{dataset}_data_*.parquet"))


def _prompt_overwrite(dataset: str) -> bool:
    """Ask user whether to overwrite existing data. Returns True if yes."""
    existing = check_existing(dataset)
    if existing == 0:
        return True
    print(f"Dataset '{dataset}' already exists ({existing} file(s) in "
          f"s3://{BUCKET}/).")
    try:
        answer = input("Overwrite? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("Aborted.")
        sys.exit(1)
    return answer in ("y", "yes")


def main():
    parser = argparse.ArgumentParser(
        description="Generate test Parquet data for Daft OOM repro"
    )
    parser.add_argument("--50m", action="store_true")
    parser.add_argument("--100m", action="store_true")
    parser.add_argument("--200m", action="store_true")
    parser.add_argument("--200m-8f", action="store_true",
                        help="200M rows, 8 files (25M rows/file)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Skip prompt and regenerate data")
    args = parser.parse_args()

    for flag, (rows, rpf, desc) in PRESETS.items():
        if getattr(args, flag.replace("-", "_"), False):
            if not args.overwrite and not _prompt_overwrite(flag):
                print("Skipped.")
                return
            generate(flag, rows, rows_per_file=rpf)
            return

    print("Usage: gen_data.py --50m | --100m | --200m | --200m-8f")
    for flag, (rows, rpf, desc) in PRESETS.items():
        files = rows // rpf
        rg_rows = rpf // RGS_PER_FILE
        print(f"  --{flag:<9} {desc}  ({files} files × 4 RGs, {rg_rows:,} rows/RG)")
    sys.exit(1)


if __name__ == "__main__":
    main()
