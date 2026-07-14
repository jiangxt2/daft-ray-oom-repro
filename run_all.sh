#!/usr/bin/env bash
# run_all.sh — Orchestrate full 18-test suite with worker restarts and log collection.
#
# Usage:
#   ./run_all.sh          # run all 18 tests
#   ./run_all.sh --50m    # run only 50m dataset (6 tests)
#
# Logs collected per iteration:
#   Head:     raylet.out, cgroup memory, ray status  (collected by run_test.py)
#   Worker01: raylet.out, cgroup memory               (collected by this script)
#   Worker02: raylet.out, cgroup memory               (collected by this script)
#
# All logs saved inside ray-head container at /tmp/daft-oom-results/
set -euo pipefail

LOG_DIR="/tmp/daft-oom-results"

# ── Helpers ──

collect_worker_logs() {
    local worker=$1      # ray-worker01 or ray-worker02
    local run_dir=$2     # e.g. /tmp/daft-oom-results/50m_no_split_iter1
    local label=$3       # "pre" or "post"
    local ts
    ts=$(date -u +%Y%m%dT%H%M%S)

    # Cgroup memory
    docker compose exec -T "$worker" cat /sys/fs/cgroup/memory/memory.usage_in_bytes \
        > "/tmp/_w_${worker}_usage.txt" 2>/dev/null || echo "N/A" > "/tmp/_w_${worker}_usage.txt"
    docker compose exec -T "$worker" cat /sys/fs/cgroup/memory/memory.max \
        > "/tmp/_w_${worker}_max.txt" 2>/dev/null || echo "N/A" > "/tmp/_w_${worker}_max.txt"

    # Raylet logs (last 200 lines)
    docker compose exec -T "$worker" bash -c "
        ls /tmp/ray/session_latest/logs/raylet.out 2>/dev/null &&
        tail -n200 /tmp/ray/session_latest/logs/raylet.out 2>/dev/null || echo 'N/A'
    " > "/tmp/_w_${worker}_raylet_out.txt" 2>/dev/null || true

    docker compose exec -T "$worker" bash -c "
        ls /tmp/ray/session_latest/logs/raylet.err 2>/dev/null &&
        tail -n200 /tmp/ray/session_latest/logs/raylet.err 2>/dev/null || echo 'N/A'
    " > "/tmp/_w_${worker}_raylet_err.txt" 2>/dev/null || true

    # Copy into container's log dir
    docker compose exec -T ray-head mkdir -p "$run_dir" 2>/dev/null || true
    for ftype in usage max; do
        docker compose cp "/tmp/_w_${worker}_${ftype}.txt" \
            "ray-head:${run_dir}/${worker}_cgroup_${ftype}_${label}_${ts}.txt" 2>/dev/null || true
    done
    for ftype in raylet_out raylet_err; do
        docker compose cp "/tmp/_w_${worker}_${ftype}.txt" \
            "ray-head:${run_dir}/${worker}_${ftype}_${label}_${ts}.txt" 2>/dev/null || true
    done

    for f in /tmp/_w_${worker}_*.txt; do
        [ -f "$f" ] || continue
        if command -v trash &>/dev/null; then trash "$f"; else rm -f "$f"; fi
    done
}

# ── Main ──

echo "============================================"
echo "  Daft on Ray Worker OOM — Full Test Suite"
echo "  $(date)"
echo "============================================"

# Determine datasets
if [ "${1:-}" = "--50m" ]; then
    DATASETS=("50m")
elif [ "${1:-}" = "--100m" ]; then
    DATASETS=("100m")
elif [ "${1:-}" = "--200m" ]; then
    DATASETS=("200m")
else
    DATASETS=("50m" "100m" "200m")
fi

# Clear previous results
docker compose exec ray-head rm -rf "$LOG_DIR" 2>/dev/null || true
docker compose exec ray-head mkdir -p "$LOG_DIR"

TOTAL=$(( ${#DATASETS[@]} * 2 * 3 ))  # datasets × 2 configs × 3 iterations
CURRENT=0

for ds in "${DATASETS[@]}"; do
    for cfg in no_split split; do
        for i in 1 2 3; do
            CURRENT=$((CURRENT + 1))
            RUN_ID="${ds}_${cfg}_iter${i}"
            RUN_DIR="${LOG_DIR}/${RUN_ID}"

            echo ""
            echo "=== [$CURRENT/$TOTAL] $RUN_ID ==="

            # Restart workers (fresh memory baseline, per analysis doc §执行规范)
            echo "  Restarting workers ..."
            docker compose restart ray-worker01 ray-worker02 2>&1 | tail -2
            sleep 12

            # Collect pre-test Worker logs
            echo "  Collecting pre-test Worker logs ..."
            collect_worker_logs ray-worker01 "$RUN_DIR" "pre"
            collect_worker_logs ray-worker02 "$RUN_DIR" "pre"

            # Run the test (collects Head logs internally)
            echo "  Running test ..."
            docker compose exec -T ray-head python /tmp/scripts/run_test.py \
                --single --dataset "$ds" --config "$cfg" --iteration "$i" \
                --log-dir "$LOG_DIR" 2>&1

            # Collect post-test Worker logs
            echo "  Collecting post-test Worker logs ..."
            collect_worker_logs ray-worker01 "$RUN_DIR" "post"
            collect_worker_logs ray-worker02 "$RUN_DIR" "post"

            echo "  Done: $RUN_DIR"
        done
    done
done

echo ""
echo "============================================"
echo "  All tests complete."
echo "  Results: docker compose exec ray-head cat $LOG_DIR/results.jsonl"
echo "  Logs:    docker compose exec ray-head ls $LOG_DIR/"
echo "============================================"
