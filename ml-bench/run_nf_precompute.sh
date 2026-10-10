#!/usr/bin/env bash
# PAR-829: run one NeuralForecast precompute on celestrial under the shared GPU lock.
#   ml-bench/run_nf_precompute.sh <model> <cache-dir-under-/data> [nf_precompute args…]
# Waits while another agent holds /data/parkfan/ml-bench/gpu.lock, takes it
# atomically (noclobber), releases it on exit. Caps per MODEL-AGENT-RULES.
set -euo pipefail
model=$1; cache=$2; shift 2
LOCK=/data/parkfan/ml-bench/gpu.lock
MINE="PAR-829 nf_precompute $model $(date -u +%FT%TZ)"
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
docker run --rm --name "par829-$model" --gpus all --cpus 6 --memory 8g --cpu-shares 256 \
  -v /data/parkfan/ml-bench:/data --entrypoint nice "${IMAGE:-ml-bench:par-829}" -n 10 \
  python -m mlbench.models.nf_precompute --export /data/exports/20261009 --cache "$cache" \
  --model "$model" "$@" || rc=$?
echo "peak device VRAM during this run: $(awk -F, 'NR>1 && $2>m {m=$2} END {print m" MiB"}' "$VLOG") (log $VLOG)"
exit $rc
