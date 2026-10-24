#!/bin/bash
# PAR-828 ml-bench run on celestrial, GPU-locked.
#   usage: run.sh <run-id> <mlbench run args...>
#
# Provenance gate (PAR-828): the first subset run recorded `git_sha: "unknown"`,
# because the image was built without `--build-arg GIT_SHA` AND the container got
# only MLBENCH_IMAGE_ID, not MLBENCH_GIT_SHA — and the image has neither a git
# binary nor a repository to fall back on. A run whose SHA cannot be resolved to a
# real commit that contains the harness fixes is VOID, so this script refuses to
# start one and verifies the recorded SHA again afterwards.
set -u -o pipefail

RUN=${1:?usage: run.sh <run-id> <mlbench run args...>}; shift
REPO=${MLBENCH_REPO:-$HOME/ml-handoff/wt/par-828}
CODE=${MLBENCH_CODE:-$HOME/claude-worktrees/par-828}
IMAGE=${MLBENCH_IMAGE:-ml-bench:par-828}
LOCK=/data/parkfan/ml-bench/gpu.lock
# Every run must contain this harness fix; without it plug-ins and built-in
# baselines are scored on different slot sets (see RESULTS-PAR-828.md).
NEEDS=${MLBENCH_NEEDS_COMMIT:-a1d20bf8}

SHA=$(git -C "$REPO" rev-parse HEAD) || exit 1
case $(git -C "$REPO" status --porcelain -- ml-bench | wc -l) in
  0) ;;
  *) echo "FATAL: ml-bench/ is dirty in $REPO — commit first, or the SHA is a lie"; exit 1;;
esac
git -C "$REPO" merge-base --is-ancestor "$NEEDS" "$SHA" || {
  echo "FATAL: $SHA does not contain $NEEDS — rebase onto the PAR-827 branch first"; exit 1; }
IMG_SHA=$(docker run --rm --entrypoint cat "$IMAGE" /app/GIT_SHA 2>/dev/null)
[ "$IMG_SHA" = "$SHA" ] || {
  echo "FATAL: image $IMAGE was built from '${IMG_SHA:-<nothing>}', worktree is at $SHA."
  echo "       rebuild: docker build -t $IMAGE --build-arg GIT_SHA=$SHA \\"
  echo "                  --build-arg EXTRA_REQUIREMENTS=requirements-foundation.txt \\"
  echo "                  --build-context mlsvc=$CODE/ml-service $CODE/ml-bench"; exit 1; }

# Production's GPU jobs (TFT training 03:00, CatBoost 06:00, nf/pcn/Chronos shadow) do
# not take $LOCK, and a bench arm may reserve 0.6 x 16 GB: refuse to START inside
# 00:55-09:30 UTC. Checked again while waiting for the lock. Only the start is guarded —
# size a run (--probe) so it ends before 00:55. MLBENCH_IGNORE_WINDOW=1 overrides.
busy_window() {
  [ "${MLBENCH_IGNORE_WINDOW:-0}" = 1 ] && return 1
  local t=$((10#$(date -u +%H%M)))
  [ "$t" -ge 55 ] && [ "$t" -lt 930 ]
}
refuse_busy() { busy_window && { echo "FATAL: $(date -u +%H:%M) UTC is inside the production window 00:55-09:30 UTC — start later, or MLBENCH_IGNORE_WINDOW=1"; exit 3; }; return 0; }
refuse_busy
while ! (set -o noclobber; echo "PAR-828 $RUN $(date -u +%FT%TZ)" > $LOCK) 2>/dev/null; do
  refuse_busy; echo "waiting for GPU lock: $(cat $LOCK)"; sleep 60; done
trap 'rm -f $LOCK' EXIT
nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader

cd "$CODE" || exit 1
docker run --rm --name mlbench-par828-$RUN --gpus all --cpus 6 --memory 8g --cpu-shares 256 \
  -e HF_HOME=/hf -e MLBENCH_GIT_SHA="$SHA" \
  `# PAR-1066 (tabpfn_ts): gated weights. Bare -e passes the caller's value through, or` \
  `# nothing if unset - the tokens never appear in argv, the image or the repository.` \
  -e HF_TOKEN -e TABPFN_TOKEN -e TABPFN_MODEL_CACHE_DIR=/hf/tabpfn \
  -e MLBENCH_IMAGE_ID=$(docker image inspect -f "{{.Id}}" "$IMAGE") \
  -v /data/parkfan/ml-bench/par-828/hf:/hf -v /data/parkfan/ml-bench:/data \
  -v $PWD/ml-bench/results:/app/results -v $PWD/ml-bench/subsets:/app/subsets \
  --entrypoint nice "$IMAGE" -n 10 python -m mlbench run --export /data/exports/20261009 \
  --out /app/results/$RUN --memory 5GB --threads 4 "$@"
rc=$?
echo "exit $rc $(date -u +%FT%TZ)"

# Assert afterwards too: a run json saying "unknown" must not be reported.
for j in ml-bench/results/$RUN/run-*.json; do
  [ -e "$j" ] || continue
  got=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1])).get('git_sha','?'))" "$j")
  if [ "$got" != "$SHA" ]; then echo "VOID: $j recorded git_sha=$got, expected $SHA"; rc=2; fi
done
exit $rc
