#!/bin/bash
# PAR-828 ml-bench run on celestrial, GPU-locked. usage: run.sh <run-id> <mlbench run args...>
RUN=$1; shift
LOCK=/data/parkfan/ml-bench/gpu.lock
while ! (set -o noclobber; echo "PAR-828 $RUN $(date -u +%FT%TZ)" > $LOCK) 2>/dev/null; do
  echo "waiting for GPU lock: $(cat $LOCK)"; sleep 60; done
trap 'rm -f $LOCK' EXIT
nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader
cd ~/claude-worktrees/par-828
docker run --rm --name mlbench-par828-$RUN --gpus all --cpus 6 --memory 8g --cpu-shares 256 \
  -e HF_HOME=/hf -e MLBENCH_IMAGE_ID=$(docker image inspect -f "{{.Id}}" ml-bench:par-828) -v /data/parkfan/ml-bench/par-828/hf:/hf -v /data/parkfan/ml-bench:/data \
  -v $PWD/ml-bench/results:/app/results -v $PWD/ml-bench/subsets:/app/subsets \
  --entrypoint nice ml-bench:par-828 -n 10 python -m mlbench run --export /data/exports/20261009 \
  --out /app/results/$RUN --memory 5GB --threads 4 "$@"
echo "exit $? $(date -u +%FT%TZ)"
