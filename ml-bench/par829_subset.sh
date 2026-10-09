#!/usr/bin/env bash
# PAR-829 subset run: 20 busiest parks, two month blocks, every model. Runs on celestrial.
set -uo pipefail
cd ~/claude-worktrees/par-829
LOG=/data/parkfan/ml-bench/par-829/subset.log
mkdir -p /data/parkfan/ml-bench/par-829
exec >>"$LOG" 2>&1
echo "=== start $(date -u)"
while docker ps --format '{{.Names}}' | grep -q mlbench-build; do echo "waiting for export rebuild"; sleep 60; done
PARKS=$(docker run --rm --cpus 2 --memory 2g -v /data/parkfan/ml-bench:/data:ro --entrypoint python ml-bench:par-829 -c "
import duckdb; con=duckdb.connect()
con.execute(\"ATTACH '/data/exports/20261009/bench.duckdb' AS b (READ_ONLY)\")
print(','.join(r[0] for r in con.execute('SELECT park_id FROM b.truth GROUP BY 1 ORDER BY count(*) DESC LIMIT 20').fetchall()))")
echo "$PARKS" > /data/parkfan/ml-bench/par-829/subset_parks.txt
for m in ${MODELS:-nf_tide nf_nhits nf_tsmixerx}; do
  for rng in "2026-05-01 2026-05-31" "2026-08-01 2026-08-31"; do
    set -- $rng
    ml-bench/run_nf_precompute.sh "$m" /data/par-829/cache/subset --parks "$PARKS" \
      --from "$1" --to "$2" --max-steps "${STEPS:-2000}" 2>&1 | grep -vE "UserWarning|warnings.warn|LeafSpec|num_workers|kaiming" 
  done
done
echo "=== done $(date -u)"
