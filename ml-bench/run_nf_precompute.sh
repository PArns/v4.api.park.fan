#!/usr/bin/env bash
# PAR-829: run one NeuralForecast precompute on celestrial under the shared GPU lock.
#   ml-bench/run_nf_precompute.sh <model> <cache-dir-under-/data> [nf_precompute args…]
# Waits while another agent holds /data/parkfan/ml-bench/gpu.lock, takes it
# atomically (noclobber), releases it on exit. Caps per MODEL-AGENT-RULES.
set -euo pipefail
model=$1; cache=$2; shift 2
LOCK=/data/parkfan/ml-bench/gpu.lock
until (set -o noclobber; echo "PAR-829 nf_precompute $model $(date -u +%FT%TZ)" > "$LOCK") 2>/dev/null; do
  echo "gpu.lock held: $(cat "$LOCK" 2>/dev/null) — waiting"; sleep 120
done
trap 'rm -f "$LOCK"' EXIT
nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader
docker run --rm --name "par829-$model" --gpus all --cpus 6 --memory 8g --cpu-shares 256 \
  -v /data/parkfan/ml-bench:/data --entrypoint nice "${IMAGE:-ml-bench:par-829}" -n 10 \
  python -m mlbench.models.nf_precompute --export /data/exports/20261009 --cache "$cache" \
  --model "$model" "$@"
