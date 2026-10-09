#!/usr/bin/env bash
set -euo pipefail
WT=~/claude-worktrees/par-830
RUN=20261009-par830-drivers
cd "$WT"
SHA=$(cat GIT_SHA)
nice -n 10 docker build -q -t ml-bench:par830 --build-arg GIT_SHA=$SHA --build-context mlsvc=ml-service ml-bench > /tmp/par830-build.log 2>&1 || { tail -30 /tmp/par830-build.log; exit 1; }
IMG=$(docker image inspect -f '{{.Id}}' ml-bench:par830)
docker run --rm --cpus 2 --memory 2g --user 1000:1000 -e HOME=/tmp --entrypoint python ml-bench:par830 -m pytest -q -W ignore -p no:cacheprovider tests 2>&1 | tail -3
mkdir -p "$WT/ml-bench/results/$RUN"; chmod 777 "$WT/ml-bench/results/$RUN"
for i in 0 1 2; do
  docker rm -f mlbench-par830-$i >/dev/null 2>&1 || true
  docker run -d --name mlbench-par830-$i --user 1000:1000 -e HOME=/tmp -e MLBENCH_IMAGE_ID=$IMG \
    --cpus 2 --memory 4g --cpu-shares 256 --entrypoint nice \
    -v /data/parkfan/ml-bench:/data -v "$WT/ml-bench/results":/app/results \
    ml-bench:par830 -n 10 python -W ignore -m mlbench run --export /data/exports/20261009 \
    --out /app/results/$RUN --model driver_level --shard $i/3 --memory 2600MB --threads 2 --reference --resume
done
sleep 5; docker ps --filter name=mlbench --format '{{.Names}} {{.Status}}'
