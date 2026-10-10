# Foundation models in ml-bench — Chronos-2 and TimesFM 3.0 (PAR-828)

Two pretrained time-series foundation models, plugged into the PAR-827 benchmark
(`docs/ml/ml-bench.md`) through the same restricted plug-in interface as every
other candidate: DataFrames only, truth strictly before the origin, schedules as
known at the origin, weather only as an opt-in ORACLE.

The question is not "is a foundation model impressive" but the one BENCH-SPEC
asks of every model: **up to which lead does it beat the best naive baseline at
that lead, and where does it hand over?**

## The two models

| | Chronos-2 | TimesFM 3.0 |
|---|---|---|
| Weights | `amazon/chronos-2` | `google/timesfm-3.0-pytorch` |
| Licence | **Apache-2.0** (code and weights) | code Apache-2.0, **weights TimesFM Non-Commercial License v1.0** |
| Covariates | native past + known-future, per task | native past + future (new in 3.0) |
| Multivariate | group attention over variates, no hard cap | variate attention capped at 32 variates |
| Quantiles | arbitrary levels via `predict_quantiles` | fixed head 0.1 … 0.9 — **no q95** |
| Horizon | 64 output patches × 16 = 1024 steps in one shot | autoregressive beyond the patch horizon |
| Context | up to 8192 steps | 4096 steps (adapter setting) |

Because TimesFM caps variates at 32 and a park has 40–70 rides, the two models
get different series constructions:

- **Chronos-2:** one park = ONE multivariate task. Every ride of the park is a
  target variate, the covariates are shared, so the group attention can look
  across the rides of a park (the park-day level is the dominant shared factor,
  so this is the construction that should help most).
- **TimesFM 3.0:** one ride = one univariate task, covariates as extra variates.

## Context layout

`context_mode` decides what a "step" is:

- **`compressed`** — only the 15-min slots inside the PUBLISHED operating
  windows, day after day. Nights and closed days are left out, which stretches
  8192 steps to ~170 operating days. The day boundary is communicated through the
  covariates (minutes since opening / until closing, local time of day, weekday),
  not through the spacing.
- **`grid`** — a regular 15-min UTC grid with NaN outside the windows. 8192
  steps = 85 days. Chronos-2 masks NaN natively; TimesFM interpolates NaN
  linearly, which is wrong across a night, so TimesFM is run compressed only.

Inside a window, a slot without truth (ride down, closed, wait < 5) is NaN in
both layouts.

## Covariates

Per slot: minutes since published opening, minutes until published closing,
in-window flag (grid only), local time of day. Per park-day: weekend, weekday
(sin/cos), public and school holiday of the park's own region, holiday and
school-holiday counts including neighbour regions (ml-service
`holiday_features.py` OR-semantics), any school holiday, bridge day, and the
published day length (0 = published closed, NaN beyond the operator's publishing
horizon — an absent schedule never becomes a fabricated closed day).

Bridge days are re-derived inside the adapter from the public-holiday flag (Mon
before a Tue holiday, Fri after a Thu holiday) so a past day and a future day
carry the same definition; the schedule's own bridge flag exists only where the
schedule was known at the origin.

`uses_oracle_weather = True` adds daily max temperature and precipitation. There
is no archive of weather FORECASTS for the benchmark window, so these are
**actuals**: every `*_owx` variant is an ORACLE upper bound, scored under its own
name and never reported as a serving result.

## Daily level variant

`predict_daily` runs the same model on the ride-day P90 level series (the
statistic TFT is trained on; ≥ 8 truth slots per ride-day, NaN on closed days)
over calendar days with the day covariates. The harness composes it with H5 as
`<name>_x_h5` at every slot lead, so d10–d90 and UC4 (to d365) are scored without
asking the model for 15-min slots there. `lvl_<name>` is the daily level itself
(UC4, D6, D7).

The slot path is capped at `max_lead_days = 7`: beyond d7 the composed
`<name>_x_h5` is what is scored.

## Variants registered

| Scored name | What it tests |
|---|---|
| `chronos2` | the honest candidate: compressed layout, covariates, no weather |
| `chronos2_grid` | layout: regular grid with NaN instead of compressed |
| `chronos2_nocov` | what the covariates are worth |
| `chronos2_owx` | ORACLE weather upper bound |
| `chronos2_ft` | full fine-tune on the benchmark's own pre-origin history, 400 steps, lr 1e-5, context 2048, refreshed every 28 days |
| `timesfm3` | TimesFM 3.0 zero-shot, compressed, no weather |
| `timesfm3_owx` | ORACLE weather upper bound |

`chronos2_x_h5` / `timesfm3_x_h5` and `lvl_chronos2` / `lvl_timesfm3` come out of
the daily path of the same variants.

## Harness additions this needed

- `--parks <ids|@file>` restricts a run to a subset of parks. The filter is
  applied to the `parks` table, from which origins, targets and ride sets are all
  built, so the baselines are re-scored on exactly the same subset — a subset run
  stays a valid paired comparison, it only loses N. An id that matches no park
  stops the run; the comment of an `@file` is cut per line BEFORE the commas are
  split, because a header like `# 12 EU, 10 NA, 8 Asia` otherwise contributes
  ` 10 NA` as an id (the first subset run recorded 32 parks for a 30-park file —
  the two extra entries matched nothing, so the scored rows were unaffected).
- `qn95__<model>` — a separate q95 denominator in the slot aggregate, because
  TimesFM 3.0 has no q95 and D8's q95 coverage would otherwise be divided by the
  q80 count.
- `Model.covariate_history_days` — a context model needs the covariates of the
  days BEFORE the origin too (a holiday in the context window is part of what the
  model has to explain). Past rows carry no window; the history is the source for
  that.

## Running it (celestrial)

```bash
cd ~/claude-worktrees/par-828
docker build -t ml-bench:par-828-base --build-arg GIT_SHA=$(git rev-parse HEAD) \
  --build-context mlsvc=ml-service ml-bench
docker build -t ml-bench:par-828 --build-arg GIT_SHA=$(git rev-parse HEAD) \
  --build-arg EXTRA_REQUIREMENTS=requirements-foundation.txt \
  --build-context mlsvc=ml-service ml-bench
ml-bench/scripts-par828/run.sh <run-id> --parks @/app/subsets/par828-30parks.txt \
  --shard 0/7 --model chronos2 --model chronos2_grid --model chronos2_nocov --model chronos2_owx
```

`scripts-par828/run.sh` takes the shared GPU lock `/data/parkfan/ml-bench/gpu.lock`
atomically (`set -o noclobber`), waits while another agent holds it, and removes
it on exit; `scripts-par828/quiet_guard.sh` stops every `mlbench-par828-*`
container for the 00:55–09:30 UTC quiet window. The HF cache lives at
`/data/parkfan/ml-bench/par-828/hf` (mounted as `/hf`, `HF_HOME=/hf`) so the
weights are downloaded once.

VRAM is capped with `torch.cuda.set_per_process_memory_fraction`
(`MLBENCH_FM_VRAM_FRACTION`, default 0.6 of 16 GB) to respect the 10 GB ceiling
of the shared RTX 5080; every `[fm]` log line carries the running peak.

## What the first subset run measured — and what of it is reportable

Run `20261009-par828-subset-chronos`: 30 parks (`ml-bench/subsets/par828-30parks.txt`,
12 EU / 10 NA / 8 Asia), `--shard 0/7` = every 7th origin = 33 origins over
2026-02-19…2026-10-01, models `chronos2`, `chronos2_grid`, `chronos2_nocov`,
`chronos2_owx`. 33/33 origins, exit 0, 6 545 s, VRAM peak 1.86 GB.

### Two limits to read first

**The run was executed from a pre-rebase working tree** (`code_sha256`
`d82b5a17…`, matching no commit) that is missing three PAR-827 harness fixes:
`689a7b84` (no forecast outside the opening window known at the origin),
`336b2b62` (projected windows and the plug-in grid on the quarter-hour grid) and
`a1d20bf8` (floor-divide slot offsets); the full diagnosis and the cell-by-cell
ledger are in `ml-bench/results/20261009-par828-subset-chronos/PROVENANCE.md`.
The first of those nulls
`SLOT_MODELS + ORACLES + wt_q80/95` — **built-in baseline columns only**, never
plug-in columns — so the baseline arm covered slots the plug-in arm did not.
**Every comparison of a Chronos-2 variant against `wt_med` / `h5` / `clim` /
`snaive7` / `lvlh5_*` from this run is void** and is being re-scored. What survives
is (a) the comparisons against `persistence`, which is built in `runner.ih_{h}`
from the raw `slots` table and passes through none of the changed code, and (b)
the variant-against-variant contrasts, since all four variants are plug-ins on the
same grid — their covered slot count is identical to the unit in all 16 distinct
UC2/UC3 MAE cells.

**The every-7th-origin sharding, not the park count, is what makes this a weak
run.** The harness gates a cell at ≥ 30 origin days. Over the full period the
subset reaches 33 origin days up to d21 and then falls below the gate (29 at d30,
21 at d90). In the **headline common window** 2026-08-15…2026-10-07 it reaches
only **7–8** — so every cell of the headline-window report is suppressed and every
hand-over winner falls back to the reference with margin 0.0. The subset as
sharded cannot produce a headline-window number at all. Running all 231 origins on
the same 30 parks costs ~12.7 h and fixes it.

### Layout — the regular grid wins, and costs 2–3× the GPU time

`chronos2_grid` is better than `chronos2` in **all 16** distinct UC2/UC3 slot-MAE cells
(9 leads × {all, busy}); the paired difference and its park-cluster CI are in
`ml-bench/results/20261009-par828-subset-chronos/variant-pairs.md`. The mechanism
shows up in the decision metrics, where the compressed layout fails and the grid
does not:

| metric (full period, all rides, lead d0) | `chronos2` | `chronos2_grid` |
|---|---|---|
| first-hour MAE, opening-aligned (D5) | +0.05 | **−0.55** |
| best-time regret, min (D3) | +1.09 | **+0.14** |
| best-time hit rate top-2 (D3) | −0.065 | **−0.013** |

(differences against the per-lead reference, which is why the absolute signs are
void; the *contrast between the two columns* is not.) Compressing the context to
operating-window slots only buys a ~170-day context but destroys the opening ramp,
which is exactly what D5 and D3 score. **Decision: run the grid layout.** Price:
18.6–23.4 s per daily-origin call against 6.5–11.3 s compressed.

This also sets expectations for TimesFM 3.0, which can only be run compressed —
it interpolates NaN linearly, which is wrong across nights. TimesFM 3.0 is
therefore handicapped by construction relative to `chronos2_grid`, and a
TimesFM-vs-Chronos comparison is not like-for-like on layout.

### Covariates — clearly worth having

`chronos2_nocov` is worse than `chronos2` in all 16 cells, by 0.39–0.82 min slot
MAE, and the gap is much larger on the decisions: best-time regret +2.45…+7.35 and
first-hour MAE +0.72…+2.43 against the reference, against +1.09…+3.61 and
+0.05…+1.46 for `chronos2`. **Decision: keep the covariates.** The calendar /
holiday / minutes-since-opening block is doing real work, not decoration.

### ORACLE weather — an upper bound worth almost nothing

`chronos2_owx` adds the day's weather **actuals** (there is no archived forecast,
so it is an ORACLE and **not a servable configuration**). It is worth
**0.04–0.10 min** of slot MAE over `chronos2` across d1…d7. Read the other way:
perfect weather knowledge would buy under a tenth of a minute, so weather is not
the missing ingredient for this model family and building a weather-forecast
archive is not justified by this result.

Note that `report`'s `NON_COMPETING` set is only `oracle_level` / `oracle_shape` /
`prod_ropedrop_hist`, so `chronos2_owx` and `chronos2_owx_x_h5` currently appear as
winners in the hand-over table — the table documented as the serving router's
input. Any `*_owx` scored name should be non-competing there (filed against the
harness, PAR-827).

### UC1 — the result that survives

Against `persistence`, the only reference untouched by the missing fixes, on
15-min slots from intraday origins (full period, 33 origin days, paired share
0.997):

| lead | all rides | ex-ante busy rides |
|---|---|---|
| 15 min | **+0.263** [+0.179, +0.356] | **+0.440** [+0.239, +0.654] |
| 30 min | −0.416 [−0.610, −0.212] | −0.917 [−1.281, −0.490] |
| 45 min | −0.902 [−1.168, −0.617] | −1.866 [−2.270, −1.401] |
| 60 min | −1.262 [−1.597, −0.927] | −2.617 [−3.123, −2.103] |
| 90 min | −1.882 [−2.386, −1.396] | −3.755 [−4.646, −2.896] |
| 120 min | −2.268 [−2.920, −1.714] | −4.511 [−5.726, −3.541] |

Negative = Chronos-2 better; park-cluster 95 % CI. **Persistence is unbeatable at
15 minutes and beaten from 30 minutes on**, with a margin that grows monotonically
to 2 h. The plan-block live-correction window (D2, headliners, 0–45 min) goes the
same way: −0.645 [−0.937, −0.329] against persistence.

This matters because PAR-827's baselines-v2 finds persistence **unbeaten by every
naive and profile baseline** across the whole 15–120 min range, the best of them
(`lvlh5_tft`) still +0.636 [+0.300, +0.958] behind at 2 h. Those two findings do
not conflict — they are about different model classes. Chronos-2 is the first
candidate in this benchmark to move the persistence crossover from "beyond 2 h"
down to roughly 20–30 minutes. It is the single most useful thing the foundation
models have shown so far, and it is on the surface (live / next-best-ride /
plan-block correction) where a 2–4 minute error reduction on busy rides is visible
to a user.

Only `chronos2` has `intraday = True`, so **the layout question is unmeasured on
UC1** — the grid layout, which wins everywhere it was measured, has never been run
on intraday origins. The re-score should fix that.

### Runtime and VRAM

| | measured |
|---|---|
| VRAM peak, all four variants, inference | **1.86 GB** (cap `MLBENCH_FM_VRAM_FRACTION=0.6` → 9.8 GB; ceiling 10 GB) |
| weight load | 5.0 s, once per process |
| per origin, 30 parks, 4 variants | 109 s (February) … 268 s (October), mean **198 s** |
| of which `chronos2` × 7 intraday origins | 137 s = 51 % of the largest origin |
| `chronos2` daily-origin slot call | 6.5–11.3 s · `chronos2_grid` 18.6–23.4 s · `chronos2_nocov` 5.7–22.1 s · `chronos2_owx` 7.4–27.3 s |
| subset total (33 origins) | 6 545 s = 1 h 49 min |
| report | 250 s (headline window) · 513 s (full period) |

Scaling to the export's 157 parks and 231 daily origins (GPU cost is ~linear in
the ride count, 157/30 = 5.23×): **66.6 h** for the full grid of four variants,
~52 h for `chronos2` + `chronos2_grid` only. `--shard i/n` plus `--resume` let that
be spread over nights, but **not in parallel** — one GPU job at a time across all
agents, so n nights is n × 15.4 h of serial GPU. The cheap, high-value option is
all 231 origins on the 30-park subset: **12.7 h, one night**, and it is the one
that clears the N gate and makes the headline window gradeable.

## Licences — what is allowed

- **Chronos-2 — Apache-2.0.** Benchmarking, fine-tuning and production serving
  are all fine.
- **TimesFM 3.0 — weights under the TimesFM Non-Commercial License v1.0** (the
  code package is Apache-2.0). park.fan is non-commercial, so the offline
  benchmark here is covered. **Serving these weights in production is an explicit
  owner decision** and needs the licence text re-read against the actual
  deployment before it happens — the licence restricts commercial use, and a
  public production service is the case to check. Chronos-2 has no such question.
