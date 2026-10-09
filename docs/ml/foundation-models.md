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
  stays a valid paired comparison, it only loses N.
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

## Licences — what is allowed

- **Chronos-2 — Apache-2.0.** Benchmarking, fine-tuning and production serving
  are all fine.
- **TimesFM 3.0 — weights under the TimesFM Non-Commercial License v1.0** (the
  code package is Apache-2.0). park.fan is non-commercial, so the offline
  benchmark here is covered. **Serving these weights in production is an explicit
  owner decision** and needs the licence text re-read against the actual
  deployment before it happens — the licence restricts commercial use, and a
  public production service is the case to check. Chronos-2 has no such question.
