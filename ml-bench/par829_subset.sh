#!/usr/bin/env bash
# PAR-829 bounded precompute on celestrial. Two month blocks (August + September), ALL
# parks, one model at a time under the shared GPU lock.
#
# Why August + September and not May: a block only counts towards the benchmark's headline
# target window 2026-08-15..2026-10-07, and report.py drops a cell below
# low_n_origin_days = 30. September origins target 09-01..10-07 = 30 origin days inside
# the window on their own; August adds 08-15..09-07; together they are all 54 days of it.
# May contributes zero days to that window.
#
# Why all parks and not the 20 busiest: report.py gates a cell out of usable_horizon /
# handover below MIN_COVERAGE = 0.3 paired share, and the 20 parks with the most truth
# slots are only 39.9 % of May and 31.8 % of August — an August cell would be gated out
# and the GPU time wasted. Scoring the full park set keeps the model and the baselines
# on the same rows. A park subset needs the harness's --parks (PAR-828 adds it) so the
# baselines are restricted too; see docs/ml/ml-bench.md.
#
# Why two blocks and not nine: ~3 h of GPU budget. Each extra block is another fit plus
# ~8 predict passes over every origin of the month.
#
#   PROBE=1 ml-bench/par829_subset.sh            # one short block, measures time + VRAM
#   MODELS="nf_tide nf_nhits" ml-bench/par829_subset.sh
set -uo pipefail
cd "$(dirname "$0")/.."
LOG=${LOG:-/data/parkfan/ml-bench/par-829/subset.log}
mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== start $(date -u) PROBE=${PROBE:-0} MODELS=${MODELS:-nf_tide} STEPS=${STEPS:-2000}"

# A probe first: nothing about fit time or peak VRAM is measured yet, so run ONE short
# block and read meta.json / the VRAM csv before spending the rest of the budget.
if [ "${PROBE:-0}" = "1" ]; then
  ml-bench/run_nf_precompute.sh nf_tide /data/par-829/cache/probe \
    --from 2026-09-01 --to 2026-09-03 --max-steps "${STEPS:-200}" --memory 2000MB \
    --block-estimate 1800 2>&1 | grep -vE "UserWarning|warnings.warn|LeafSpec|num_workers|kaiming"
  echo "=== probe done $(date -u)"
  exit 0
fi

for m in ${MODELS:-nf_tide}; do
  for rng in "2026-09-01 2026-09-30" "2026-08-01 2026-08-31"; do
    set -- $rng
    # --memory 2000MB: DuckDB + the panel + the predict windows must fit the 8 GB
    # container cap, and an Aug/Sep panel is ~1.7 GB (the panel always starts at the
    # first truth day, so a late block carries more history than an early one).
    ml-bench/run_nf_precompute.sh "$m" /data/par-829/cache/subset \
      --from "$1" --to "$2" --max-steps "${STEPS:-2000}" --memory 2000MB \
      2>&1 | grep --line-buffered -vE "UserWarning|warnings.warn|LeafSpec|num_workers|kaiming"
  done
done
echo "=== done $(date -u)"
