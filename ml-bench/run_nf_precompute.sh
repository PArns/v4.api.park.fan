#!/usr/bin/env bash
# PAR-829: run one NeuralForecast precompute on celestrial under the shared GPU lock.
#   ml-bench/run_nf_precompute.sh <model> <cache-dir-under-/data> [nf_precompute args…]
# Waits while another agent holds /data/parkfan/ml-bench/gpu.lock, takes it
# atomically (noclobber), releases it on exit. Caps per MODEL-AGENT-RULES.
set -euo pipefail
model=$1; cache=$2; shift 2
LOCK=/data/parkfan/ml-bench/gpu.lock
MINE="PAR-829 nf_precompute $model $(date -u +%FT%TZ)"
IMAGE=${IMAGE:-ml-bench:par-829}

# ---- Provenance gate, BEFORE the lock and before any GPU time is spent.
# PAR-828 lost a 1.8 h run because its image was built from a pre-rebase tree: the
# recorded git_sha was "unknown", so every number scored from it was void. The cache
# outlives the container, so the SHA has to be right at WRITE time -- checking it
# afterwards only tells us the work was wasted. Refuse to start instead.
GATE_SHA=${GIT_SHA:-$(docker run --rm --entrypoint printenv "$IMAGE" MLBENCH_GIT_SHA 2>/dev/null || true)}
GATE_REPO=${GATE_REPO:-$HOME/ml-handoff/wt/par-829}
if [ "${SKIP_PROVENANCE_GATE:-0}" != 1 ]; then
  [ -n "$GATE_SHA" ] && [ "$GATE_SHA" != unknown ] || {
    echo "PROVENANCE GATE FAILED: $IMAGE has no MLBENCH_GIT_SHA." >&2
    echo "  Rebuild with: docker build -t ml-bench:par-829-base --build-arg GIT_SHA=\$(git rev-parse HEAD) \\" >&2
    echo "    --build-context mlsvc=ml-service ml-bench" >&2
    exit 3; }
  git -C "$GATE_REPO" cat-file -t "$GATE_SHA" >/dev/null 2>&1 || {
    echo "PROVENANCE GATE FAILED: $GATE_SHA is not a commit in $GATE_REPO (dirty or pre-rebase tree)." >&2
    exit 3; }
  # a1d20bf8 is the last of PAR-827's three coverage-parity fixes; without it the
  # plug-in arm covers a different slot set than the built-in baselines and every
  # paired diff is computed over a shifted intersection.
  git -C "$GATE_REPO" merge-base --is-ancestor a1d20bf8 "$GATE_SHA" || {
    echo "PROVENANCE GATE FAILED: $GATE_SHA is missing the coverage-parity fixes (a1d20bf8)." >&2
    exit 3; }
  echo "provenance gate OK: git_sha=$GATE_SHA (a1d20bf8 is an ancestor)"
fi
until (set -o noclobber; echo "$MINE" > "$LOCK") 2>/dev/null; do
  echo "gpu.lock held: $(cat "$LOCK" 2>/dev/null) — waiting"
  sleep 120
done
# Release only OUR lock: if a watchdog removed it and another agent took over, the
# file is theirs and deleting it would hand the GPU to two jobs at once.
release() { [ "$(cat "$LOCK" 2>/dev/null)" = "$MINE" ] && rm -f "$LOCK"; return 0; }
trap release EXIT

# torch.cuda.max_memory_allocated() (logged per block in meta.json) counts only
# torch's allocator. Sample the DEVICE to get the peak the 10 GB cap is about:
# it includes the CUDA context and pcn-service, which shares this GPU.
VLOG=${VRAM_LOG:-/data/parkfan/ml-bench/par-829/vram-$model-$(date -u +%Y%m%dT%H%M%SZ).csv}
mkdir -p "$(dirname "$VLOG")"
echo "utc,total_used_mib,this_proc_mib" > "$VLOG"
( while :; do
    u=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits)
    p=$(nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader,nounits | awk -F', ' '{s+=$2} END {print s+0}')
    echo "$(date -u +%FT%TZ),$u,$p" >> "$VLOG"
    sleep 15
  done ) & sampler=$!
trap 'kill $sampler 2>/dev/null; release' EXIT

nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader
rc=0
# Provenance into the cache's meta.json: the image bakes MLBENCH_GIT_SHA from the build
# arg, the image id can only come from the host. A cache whose git_sha is "unknown" or is
# not a real commit voids every number later scored from it.
IMAGE_ID=$(docker image inspect -f '{{.Id}}' "$IMAGE")
docker run --rm --name "par829-$model" --gpus all --cpus 6 --memory 8g --cpu-shares 256 \
  -e MLBENCH_IMAGE_ID="$IMAGE_ID" ${GIT_SHA:+-e MLBENCH_GIT_SHA="$GIT_SHA"} \
  -v /data/parkfan/ml-bench:/data --entrypoint nice "$IMAGE" -n 10 \
  python -m mlbench.models.nf_precompute --export /data/exports/20261009 --cache "$cache" \
  --model "$model" "$@" || rc=$?
echo "peak device VRAM during this run: $(awk -F, 'NR>1 && $2>m {m=$2} END {print m" MiB"}' "$VLOG") (log $VLOG)"
exit $rc
