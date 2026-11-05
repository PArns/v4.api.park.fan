# The served arm — benchmarking against what production showed (PAR-1043)

The owner's switch rule needs an offline win **over what is served**
(`offlineBetterThanServed`, `src/ml/utils/model-comparison.util.ts`). The bench's own
`prod_served` only reconstructs the planner's composed tier from d3, and PCN plus the
persistence blend are not in the bench at all. The `served` arm instead replays the curves the
[forward archive](../forward-archive/README.md) stored as they were served. Open this page to run
a verdict against `served` or to regenerate `src/ml/data/offline-model-verdicts.json`.

**Status (2026-10-27): implemented, never run.** The accumulation timer is not confirmed
installed, and no `--write` has been made — no entry in `offline-model-verdicts.json` carries
`reference: "served"` with a `diff` yet.

## Data path

`export --tables archive_curves` (surfaces 1 `park_hourly`, 2 `plan_day`, 8
`park_hourly_incumbent`; one UTC origin day per query, an index range scan on the primary key) →
`build` → `parquet/served_curves.parquet` (one row per curve, `waits` as a list) → plug-in
`served` (`mlbench/models/served.py`), which reads that file with its own DuckDB connection and
never reads truth.

| Bench origin | Surface | Archived capture used |
| --- | --- | --- |
| daily 06:00 (UC2 d0, UC3, D3) | `plan_day` — an hour served as a step over its four quarters | the 06:00 capture (d0–d7) and the 07:00 long-lead capture (d10–d90) |
| intraday 10–18 (UC1, UC2 intraday) | `park_hourly` — CatBoost + PCN + blend (+ Chronos-2 where served) | that hour's own capture (4 h ahead) |

`served_incumbent` prefers surface 8 at the same capture — what `park_hourly` would have shown
without Chronos-2, archived only while `SERVE_CHRONOS_INTRADAY` is on. It is the reference once
Chronos-2 is itself served.

## Own capture only

Per (ride, target day) the arm takes the archived row whose capture lies in
**[bench origin − 15 min, bench origin + 60 min)** (`MAX_AGE_MIN = 15`): the bench origin's own
capture, nothing older (`tests/test_served.py`).

- The bench's intraday origins are 08, 10 … 20 and the archive captures 06:00 and 10–18, so
  **08:00 and 20:00 stay unpaired**. Answering them from the 06:00 / 18:00 curve (the first
  version, `MAX_AGE_MIN = 120`) scored a curve two hours staler than what the candidate saw and
  inflated the candidates' `live_*` wins. `run-*.json` records per origin kind and hour how many
  park-origins were asked and how many had their own capture (`served_origins`).
- A slot past what was archived is not filled from an older curve, and **a slot that starts before
  the capture instant is never answered**. Every capture runs at :05 or later, so `m015` never
  pairs and the generator's `live_h0_1` reads `m030`–`m060`. The served curve still knew the live
  readings up to its capture, which favours served.

## Biases to read a close cell with

- `park_hourly` is archived **rounded** (`roundServedWait`, 5-minute steps, floor 5) while every
  bench candidate is scored raw, which favours the candidate. Read a slot-MAE margin under
  ~0.5 min against `served` on the intraday questions as a tie. (The forward archive's own live
  matrix rounds both arms.) `plan_day` is archived at 1-minute precision.
- `plan_day` is an hourly step, so on D3 its four quarters tie; the ride-day ranking breaks ties
  by slot start (`ORDER BY pred, ws` in `rideday_sql`), so served always "recommends" the first
  quarter of its best hour.
- The calendar questions (`cal_*`, TFT-served) are not covered — the arm has no daily level.

A sensitivity pass that rounds the candidate to 5 minutes is the clean answer to the first bias;
it is not built.

## The window, and the weekly accumulation

A curve is deleted 14 days past its target date, so one export holds d0–d7 rows only for the last
~14 target days. The archive started 2026-10-09; lead L has its first scored target on
2026-10-09 + L (the same floor as the live matrix's `cellFloorDate`).

Rows are never updated after capture, so `ml-bench/scripts/accumulate-archive-curves.sh` exports
each new complete UTC origin day once into one persistent directory (`MLBENCH_ARCHIVE_DIR`;
`MLBENCH_ARCHIVE_FROM` defaults to 2026-10-09), skipping days its manifest already holds
(`test_accumulating_export_reads_only_new_origin_days`). Read-only through
`scripts/prod-psql.sh`, system `python3`. **Run it at least weekly** — a gap over ~14 days loses
short-lead rows for good. Suggested install on the database host as a systemd user timer (needs
linger):

```ini
# ~/.config/systemd/user/mlbench-archive-curves.service
[Unit]
Description=ml-bench: accumulate forecast_archive_curves (PAR-1043)
[Service]
Type=oneshot
Environment=MLBENCH_ARCHIVE_DIR=/data/parkfan/ml-bench/archive-curves
ExecStart=%h/<checkout>/ml-bench/scripts/accumulate-archive-curves.sh
Nice=10

# ~/.config/systemd/user/mlbench-archive-curves.timer
[Unit]
Description=weekly ml-bench archive-curves export
[Timer]
# Mondays 11:30 UTC: outside the 00:55–09:30 UTC quiet window and away from :05
OnCalendar=Mon *-*-* 11:30:00 UTC
Persistent=true
[Install]
WantedBy=timers.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now mlbench-archive-curves.timer
systemctl --user start mlbench-archive-curves.service   # first run now
journalctl --user -u mlbench-archive-curves.service -n 50
```

**When a verdict run is possible.** The generator needs ≥ 14 paired origin days per lead
(`--min-origin-days`, default 14). With the timer installed by 2026-11-01, a run on 2026-11-09
(the `live_chronos` / `planner_chronos` `evaluateOn`) holds targets 2026-10-18 … 2026-11-07 for
`live_*`, `today_*`, `plan_d0_1` and `plan_d2_5`. `plan_besttime` (to d30) is decidable from a run
on ≥ 2026-11-23, `plan_d6_60` (to d60) from ≥ 2026-12-23. Before then those cells come out
`unclear` by construction — keep them out of the `--write`.

## Pairing and verdicts

`run --model served --pair-with served` writes paired sums of every model — baselines and
plug-ins — against `served` into the slot, intraday and ride-day tables, so `report --vs served`
writes `cells_vs_served.csv` (slot MAE per use case × lead, D3 regret per lead, park-cluster CI,
segments `all` / `busy`, regions).

**Verdicts are generated, never typed.** `python3 -m mlbench.served_verdicts --run <run> --pass
<tables dir> [--pass …] [--write]` (stdlib only) maps each matrix question to its bench cells
(`QUESTIONS` in the script; a question straddling an intraday bucket takes every bucket it
touches) and writes one entry per cell with `reference: "served"`, `metric`, `runId`, `diff`,
`ciLow`, `ciHigh` and a German note. `better` needs every lead to win (park-cluster CI below 0,
≥ 10 parks, ≥ `--min-origin-days`) in **every** pass; `worse` every CI above 0; anything else
`unclear`. The quoted figure is the least favourable lead (largest CI upper bound), never a pool.
It refuses benchmark-only models (`mlbench.licences.refuse_benchmark_only`). Dry run by default
(prints the diff); `--write` writes `src/ml/data/offline-model-verdicts.json`.

How this differs from the live matrix's rule (`poolCell`, PAR-1036): offline is **stricter on
leads** (every lead and every pass must win; the least favourable lead is quoted) but **looser on
clustering** (park-cluster bootstrap only, where the live gate is a two-way park × date
bootstrap). A cell whose park-cluster CI only just clears 0 is not yet a live `better`. Adding a
park × date bootstrap to `report --vs` would close the gap; not done.

## Running it (celestrial) — dates for 2026-11-09

Outside the quiet window; Chronos-2 plug-ins need the GPU lock. Do not `--write` before the
accumulation has run since ≤ 2026-11-01.

```bash
cd ~/claude-worktrees/<checkout>
EXP=20261109-served
# 1. export: full queue history (Chronos-2 reads ~343 days); curves from the accumulated dir
MLBENCH_ARCHIVE_DIR=/data/parkfan/ml-bench/archive-curves ml-bench/scripts/accumulate-archive-curves.sh
python3 ml-bench/mlbench/export.py --out /data/parkfan/ml-bench/exports/$EXP --repo $PWD \
  --queue-to 2026-11-08 --tables parks,attractions,schedule,holidays,weather,tft_forecasts,queue
cp /data/parkfan/ml-bench/archive-curves/raw/archive_curves_*.csv.gz /data/parkfan/ml-bench/exports/$EXP/raw/
# 2. image + build (CPU)
docker build -t ml-bench:par-1043 --build-arg GIT_SHA=$(git rev-parse HEAD) \
  --build-arg EXTRA_REQUIREMENTS=requirements-foundation.txt --build-context mlsvc=ml-service ml-bench
docker run --rm --user 1000:1000 -e HOME=/tmp --cpus 6 --memory 8g --cpu-shares 256 \
  -v /data/parkfan/ml-bench:/data ml-bench:par-1043 build --export /data/exports/$EXP --memory 5GB --threads 4
# 3. run: three shards (not a multiple of 7), one at a time under the GPU lock
RUN=20261109-par1043-served; LOCK=/data/parkfan/ml-bench/gpu.lock
for i in 0 1 2; do
  while ! (set -o noclobber; echo "PAR-1043 $RUN $i $(date -u +%FT%TZ)" > $LOCK) 2>/dev/null; do sleep 60; done
  docker run --rm --name mlbench-par1043-$i --gpus all --user 1000:1000 -e HOME=/tmp \
    -e HF_HOME=/hf -e MLBENCH_IMAGE_ID=$(docker image inspect -f '{{.Id}}' ml-bench:par-1043) \
    --cpus 6 --memory 8g --cpu-shares 256 --entrypoint nice \
    -v /data/parkfan/ml-bench/par-828/hf:/hf -v /data/parkfan/ml-bench:/data \
    -v $PWD/ml-bench/results:/app/results -v $PWD/ml-bench/subsets:/app/subsets \
    ml-bench:par-1043 -n 10 python -m mlbench run --export /data/exports/$EXP \
    --out /app/results/$RUN --from 2026-10-09 --to 2026-11-07 --shard $i/3 \
    --model served --model chronos2 --model chronos2_grid --pair-with served \
    --parks @/app/subsets/par828-30parks.txt --memory 5GB --threads 4 --reference
  rm -f $LOCK
done
# 4. report: target window and origin window
R="docker run --rm --user 1000:1000 -e HOME=/tmp -v /data/parkfan/ml-bench:/data \
   -v $PWD/ml-bench/results:/app/results ml-bench:par-1043 report --run /app/results/$RUN --vs served"
$R --target-from 2026-10-10 --target-to 2026-11-07
$R --origin-from 2026-10-09 --origin-to 2026-11-07
# 5. verdicts: dry run, then --write and open a PR
cd ml-bench
python3 -m mlbench.served_verdicts --run results/$RUN \
  --pass tables_2026-10-10_2026-11-07 --pass tables_o2026-10-09_2026-11-07 [--write]
```

Without `--parks` the run covers every park the archive holds, at a multiple of the GPU time.
