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

## What the subset run measured

Run **`20261010-par828-subset-chronos-v2`**: 30 parks
(`ml-bench/subsets/par828-30parks.txt`, 12 EU / 10 NA / 8 Asia), `--shard 0/7` = every
7th origin = **33 origins** over 2026-02-19…2026-10-01, models `chronos2`,
`chronos2_grid`, `chronos2_nocov`, `chronos2_owx`, with
`--pair-with chronos2,chronos2_grid,h5,lvlh5_tft,persistence`. 33/33 origins, exit 0,
5 658 s, VRAM peak **1.86 GB**.

The earlier `20261009-par828-subset-chronos` is **VOID** and kept only for the
void-vs-fixed adjudication below — see its `SUPERSEDED.md`.

### Provenance: two commits, both resolvable

A published table has to identify the code that wrote *that table*, not just the code
that wrote `parts/` (BENCH-SPEC change #7). This run has two:

| | git SHA | measured `code_sha256` |
|---|---|---|
| scored `parts/` (the sums) | `cdbc95a3` | `d5acedf6…` |
| wrote `tables/` (the verdicts) | `ab8102bc` | `6de411d2…` |

Both resolve to real commits; both `code_sha256` values were recomputed from
`git archive <sha> ml-bench/mlbench` and reproduce exactly. `cdbc95a3` contains all
three coverage-parity fixes. It is no longer an ancestor of this branch — a later
rebase rewrote it to `82130382` — but it is still a real commit and is preserved on
`origin/parns123/par-828-…-mit-REBASED`. The rebase onto `f91efb43` did not change
`code_sha256`, so the identity recorded in `report-tables.json` still resolves.

**The scoring code predates PAR-827's `5c216ffa`**, so critic fixes S8, S9 and S11 were
not in it. All three are immaterial here, and the reasoning is stated so it can be
checked rather than re-derived:

* **S8** (a model could take ORACLE weather and keep its honest name, because the gate
  read the instance while `scored_name()` read the class): the foundation variants set
  `uses_oracle_weather` at **class** level, so the hole never fired and the `_owx`
  names are honest.
* **S9** (the live DuckDB connection was reachable through `HistoryView`'s closure): a
  leak the plug-in never used; it cannot move a number.
* **S11** (`sched_is_holiday` / `sched_is_bridge_day` are built from every schedule row
  of the day but were masked with the OPERATING rows' publish time) is the only one
  that *could* have moved a published cell, so it gets a decisive answer rather than a
  caveat. The day covariates `chronos2` / `chronos2_grid` actually consume are
  `is_weekend`, `dow_sin`, `dow_cos` (from the date), `is_holiday_primary`,
  `is_school_holiday_primary`, `holiday_count_total`, `school_holiday_count_total`,
  `is_school_holiday_any` (from ml-service `holiday_features.py`, i.e. the holiday
  table plus the park's region — calendar facts with no publication time),
  `is_bridge_day` (**derived in `day_covariates()`** from `is_holiday_primary`, which is
  why the schedule's own bridge flag is deliberately not read) and, for `chronos2` only,
  `day_len_h` (masked by `schedule_known_at_origin`, NaN where the window is projected).
  `chronos2_nocov` consumes none. **None of them is one of the two flags S11 unmasks.**
  Independently: the shared export `20261009` has no `sched_flags_updated_utc` column at
  all, so the fixed code takes its fallback branch and masks with `pw.sched_known` —
  bit-identical to the scoring code. On this export S11 cannot move any number for
  anyone.

### The three gates this run had to pass before anything was published

1. **Provenance** — the table above.
2. **Behavioural coverage parity** — slots a built-in baseline covers that the plug-in
   does not, over d0–d7, across **all 33 origins**: **0 in every one of the 48 cells**
   (`snaive7`, `wt_med`, `clim`, `h5`, `lvlh5_tft`, `chronos2_grid`). The void run had
   6 767 over six origins. The residual in the other direction is legitimate
   baseline-side data hunger (`wt_med` 7.4k–22.7k slots, `clim` 38k–76k, `lvlh5_tft`
   274k–334k, and `snaive7` covers nothing at d7 because a seasonal naive is undefined
   there). `chronos2` and `chronos2_grid` cover **exactly** the same slots — 0 either
   way at every lead — so the layout comparison runs on a perfectly common grid.
3. **`--pair-with`** — the intraday table carries `pn`/`psm`/`psr` for `chronos2`
   against `persistence` (1 778 746 paired slots), `h5` (3 031 943) and `lvlh5_tft`
   (1 972 308); the slot table additionally pairs it against `chronos2_grid`, `wt_med`,
   `clim` and `snaive7`.

### How to read the N, lead by lead

Per-cell `n_origin_days` in the full-period pass: **33** at d1–d6, **32** at d7 and d10,
**31** at d14, **30** at d21 — all at or above BENCH-SPEC's ≥ 30 gate, so **the
full-period pass is not LOW-N for any lead d0–d21**. LOW-N begins at d30 (29), d45 (27),
d60 (25), d90 (21): 64 of 253 distinct (use case, metric, lead) cells, all of them
there. `n_units_park` is 29–30 everywhere, so the ≥ 10-bootstrap-unit and
positive-CI-width guard (BENCH-SPEC #2) is satisfied with margin.

Do **not** quote `lead_availability.csv` for this run: it reports 231/230/…/51 origin
days, which is the *export's* theoretical availability, not this shard's 33 (critic
B4(b), still live in the full-period pass). The per-cell `n_origin_days` is the only
trustworthy count.

The **headline common target window** (2026-08-15…10-07) contains only **7–8 origin
days**, because the subset takes every 7th origin. Its `cells.csv` carries real paired
margins and is quoted below as a LOW-N cross-check, but its `usable_horizon.csv` has
`leads_tested = 0` for every model and **all 280** rows of its `handover.csv` have
`n_tested = 0`, each with an explicit `not_tested_because` of the form
`"LOW-N: 7 park-days < 30"`. Those tables say **nothing was tested**. The winner column
there is the reference falling through, not a contest anyone entered — reading
`wt_med` 170 / `snaive7` 67 as wins is exactly the error critic B4 named.

### UC1 — the live surface, and the strongest result in this review

Full period, 33 origin days, 30 park units, `paired_share` 0.998–1.000, park-cluster
95 % CI. Negative = Chronos-2 better. All three comparators are named baselines, paired
individually; nothing here rests on "beats the reference".

| lead | − `persistence` | − `h5` | − `lvlh5_tft` |
|---|---|---|---|
| 15 min | **+0.263 [+0.179, +0.356]** loses | −4.017 [−4.777, −3.222] | −3.751 [−4.549, −3.002] |
| 30 min | −0.416 [−0.610, −0.212] | −3.050 [−3.663, −2.419] | −2.887 [−3.575, −2.296] |
| 45 min | −0.902 [−1.168, −0.617] | −2.668 [−3.188, −2.114] | −2.501 [−3.056, −2.004] |
| 60 min | −1.262 [−1.597, −0.927] | −2.534 [−3.060, −1.975] | −2.341 [−2.919, −1.839] |
| 75 min | −1.557 [−1.963, −1.153] | −2.291 [−2.816, −1.751] | −2.074 [−2.646, −1.586] |
| 90 min | −1.882 [−2.386, −1.396] | −2.154 [−2.658, −1.653] | −2.016 [−2.554, −1.541] |
| 105 min | −2.062 [−2.639, −1.563] | −2.092 [−2.601, −1.580] | −1.945 [−2.456, −1.468] |
| 120 min | −2.268 [−2.920, −1.714] | −2.048 [−2.537, −1.557] | −1.868 [−2.351, −1.408] |

Ex-ante busy rides, against `persistence`: +0.440 [+0.238, +0.653] at 15 min, then
−0.917, −1.866, −2.617, −3.067, −3.755, −4.061, **−4.511 [−5.727, −3.541]** at 120 min.
Against `h5` on busy rides it is −8.226 [−9.660, −6.639] at 15 min and −4.489 at 120 min.

Every cell except the two 15-min `persistence` cells is a win under the conservative
park-cluster unit.

**The one sentence that matters: Chronos-2 beats both servable profile incumbents at
every UC1 lead from 15 to 120 minutes, and beats `persistence` from 30 minutes out.**
The 15-minute loss is to `persistence` alone — which is not a servable forecast, it is
the current wait. The `persistence` margins reproduce the void run essentially exactly,
which is what the adjudication predicted; the new information is the per-incumbent
comparison the void run never computed.

`usable_horizon.csv` gives `chronos2` `max_significant_lead = m120` on both segments but
an **empty** `usable_horizon`, because that field is a contiguous prefix from the first
lead and the model loses the first one. The horizon is a **band, m030–m120**, not a
prefix — do not read the empty cell as "no usable horizon".

**D2, plan-block live correction** (headliners, 0–45 min, the `LIVE_WINDOW_MIN=45`
surface): `chronos2` **6.769 min**, `persistence` 7.147 (−0.645 [−0.941, −0.333], win),
`h5` 12.890 (+6.146), `lvlh5_tft` 11.738 (+5.277). Roughly half the error of either
incumbent.

**D1, next-best-ride**: `chronos2` suggestion precision 0.655 at 0–120 min against
`wt_med` +0.116 [−0.014, +0.249] — **not** significant; at top-3 it is 0.708,
+0.085 [+0.017, +0.147], a win, and the only model that wins there. But **recall is far
worse**: 0.240 against `snaive7`, −0.351 [−0.382, −0.315]. So Chronos-2 makes fewer
suggestions and a larger share of them are right. For a surface that shows a handful of
suggestions that is the better trade, but it is a trade and not a clean win.

**LOW-N cross-check** (headline window, 7 origin days, marked LOW-N — not a headline):
against `persistence`, all rides, +0.204 [+0.125, +0.290] at 15 min then −0.387, −0.728,
−1.026, −1.287, −1.619, −1.889, −2.033 [−2.649, −1.454] at 120 min; busy +0.387 then
−0.821 … −4.031. Same sign pattern, magnitudes 10–20 % smaller. It corroborates the
full-period result and carries no verdict of its own.

### Heartbeat contamination makes the UC1 wins conservative

BENCH-SPEC change #6: `is_heartbeat` rows are not filtered out of the truth, and a
heartbeat row carries the previous status *and wait* forward. A frozen wait is exactly
what `persistence` predicts, so the contamination **flatters `persistence` and works
against Chronos-2**. PAR-827 has now measured it on a matched run pair
(`20261010-sens-hb` / `-sens-nohb`): dropping heartbeat-only slots costs `persistence`
**+0.275 min** at UC1 m120 against **+0.108 min** for `h5`, and the effect grows with
horizon. 4 of this run's 33 full-period origins fall after contamination starts
(≈ 12 %), against 4 of the 7 headline-window origins (57 %) — another reason the
full-period pass is the one to quote. **The 30–120 min wins above are therefore a lower
bound**, and part of the 15-min loss may be contamination rather than model quality.
The owner decision on which target to serve against is still open; this is cited from
PAR-827's measurement, not re-derived here.

### UC2 / UC3 — the planner, per named baseline

Full period, park-cluster 95 % CI, `*` = win, `!pd` = the park-day unit (BENCH-SPEC's
literal one) disagrees. Negative = Chronos-2 better.

`chronos2_grid` − `h5`, all rides: d0 −0.983 [−1.251, −0.749]\*, d1 −0.574 [−0.803,
−0.357]\*, d2 **+0.577 [+0.382, +0.809]** (loses), d3 +0.034, d4 −0.389 [−0.608,
−0.212]\*, d5 −0.377 [−0.687, −0.133]\*, d6 −0.360 [−0.601, −0.163]\*, d7 −0.165
[−0.408, +0.046] `!pd`.

`chronos2_grid` − `lvlh5_tft`, all rides: d1 −0.798\*, d2 −0.245 `!pd`, d3 −0.449\*,
d4 −0.600\*, d5 −0.534\*, d6 −0.638\*, d7 −0.568 [−0.817, −0.332]\*.

On ex-ante busy rides against `lvlh5_tft` — the incumbent the busy routing actually
uses — `chronos2_grid` wins d1 (−1.047 [−1.605, −0.568]), d6 (−0.735) and d7 (−0.673),
and is not significant at d2–d5.

**d2 is a real and reproducible hole**: every variant loses to `h5` at d2 by +0.58…+1.53
min, in both segments, in both passes. It is the one lead where a profile beats the
foundation model outright, and it has no explanation in this run.

### Layout — the regular grid wins, now on an interval

The void run could only offer 16/16 unanimity and a sign test. With
`--pair-with chronos2` the question has a paired CI on an exactly common slot set:

| cell | `chronos2_grid` − `chronos2` |
|---|---|
| UC2 d0, all | −0.157 [−0.241, −0.068]\* |
| UC2 d0, busy | −0.262 [−0.433, −0.054]\* |
| UC3 d1, all | −0.095 [−0.227, +0.046] `!pd` |
| UC3 d2, all | −0.134 [−0.266, +0.013] `!pd` |
| UC3 d3, all | −0.133 [−0.288, +0.011] `!pd` |
| UC3 d4, all | −0.225 [−0.330, −0.114]\* |
| UC3 d5, all | −0.293 [−0.463, −0.127]\* |
| UC3 d6, all | −0.215 [−0.379, −0.053]\* |
| UC3 d7, all | −0.178 [−0.304, −0.030]\* |

The grid is better at **all eight** leads; significant at **five** under the
park-cluster unit and at **all eight** under BENCH-SPEC's literal park-day unit. The
mechanism is unchanged — the compressed layout buys ~170 days of context and destroys
the opening ramp, which is what D5 and D3 score. It costs 2–3× the GPU time
(18.6–23.4 s per daily-origin call against 6.5–11.3 s). **Decision: the grid layout.**

Still unmeasured: only `chronos2` sets `intraday = True`, so **the layout question has
never been tested on UC1**, which is where the surviving signal is. The UC1 table above
is the compressed layout — i.e. the *losing* layout on every lead where the two were
compared, which makes those UC1 numbers a lower bound on what the grid would do.

### Covariates — decisive

`chronos2_nocov` − `chronos2` is worse at every lead with the park-cluster CI excluding
0: UC2 d0 +0.283 [+0.188, +0.382] all / +0.653 [+0.459, +0.834] busy; UC3 all d1…d7
+0.417, +0.815, +0.523, +0.466, +0.466, +0.391, +0.429. The calendar / holiday /
minutes-since-opening block is doing real work, and it is what makes the model beat a
weekday profile at d1 at all. **Decision: keep the covariates.**

### ORACLE weather — an upper bound worth almost nothing

`chronos2_owx` − `chronos2`, UC3 all rides: −0.086 [−0.153, −0.034] at d1, −0.095 at
d2, −0.061 at d3, −0.075 at d4, **+0.000** at d5, −0.040 at d6 and d7. Significant, and
worth **under 0.12 min** — with *perfect foreknowledge* of the day's temperature and
precipitation. There is no archived weather forecast in the export, so `_owx` is an
**ORACLE upper bound and not a servable configuration**; every `_owx` number in this
document is labelled that way. Read the useful direction: weather is not the missing
ingredient for this model family, and **building a weather-forecast archive is not
justified by this result.**

`chronos2_x_h5` (the H5 profile *shape* rescaled by Chronos-2's forecast daily *level*)
**is** servable — Apache-2.0, no oracle input. `chronos2_owx_x_h5` is **not**, because
its level comes from weather actuals. The distinction matters because in the void run
the oracle variant was outranking the servable one.

### The hand-over table, with `_owx` excluded

PAR-827 landed the suffix exclusion this workstream asked for, and it is verified here:
**0 of the 280** full-period hand-over rows are won by an `_owx` variant, against **34
of 280** in the void run. Of the 280 rows, **79 go to a servable Chronos-2
configuration** — `chronos2_grid` 28, `chronos2` 27, `chronos2_x_h5` 22,
`chronos2_nocov` 2 — against `wt_med` 112, `h5` 38, `lvlh5_tft` 14, `snaive7` 11.

`chronos2_x_h5` reaches furthest: `max_significant_lead` d14 on MAE with the schedule
known at the origin, d21 on D4 dayPeak pairwise ordering and on busy MAE, and it wins
UC3 all-rides MAE against the envelope at d1, d4, d5, d6 and d7. Its D9 level coverage
is 0.95 of operating ride-days.

### UC4 — NOT TESTED. No number, in either direction

`uc4_busy_days.csv` is **empty** and `cells.csv` contains **zero** UC4 cells. This is
not the gate tautology critic B4 found, and it must not be written as "no level source
won":

`report.uc4()` builds a (park, calendar-month) unit only when a source has **≥ 8** finite
forecast days in that park-month at that lead. The subset takes every 7th origin, so at
a fixed lead the target days are weekly too and a calendar month can hold at most
**five** of them. Computed over this shard's own 33 origins at d1, d7, d30, d90 and
d365: the maximum number of target days in any park-month is **5** at every lead, and
the number of park-months reaching 8 is **0** at every lead.

So UC4 fails at **unit construction** (max 5 < 8), and therefore also, trivially, the
≥ 30-park-month LOW-N gate of BENCH-SPEC #3 — there are no units to count. Both gates
fail for the same reason. **No report re-run can fix this**; it needs a dense-origin
run. The crowd-calendar surface is untouched by this workstream.

The daily *level* path is not wasted — it still feeds D6 (`crowd bucket exact`, where
`lvl_chronos2` has `max_significant_lead` d7) and D7, whose units are the park-day and
the park-origin rather than the park-month.

### D8 — quantile calibration, and why it decides the TimesFM-3 question

Interval coverage, full period (targets 0.80 and 0.95):

| lead | `chronos2` q80 / q95 | `chronos2_grid` q80 / q95 | `wt_med` q80 / q95 |
|---|---|---|---|
| d0 | 0.791 / 0.939 | 0.756 / 0.923 | 0.868 / 0.946 |
| d1 | 0.815 / 0.953 | 0.771 / 0.932 | 0.869 / 0.945 |
| d3 | 0.824 / 0.957 | 0.775 / 0.944 | 0.886 / 0.939 |
| d5 | 0.806 / 0.949 | 0.776 / 0.949 | 0.852 / 0.932 |
| d7 | 0.819 / 0.958 | 0.784 / 0.963 | 0.859 / 0.936 |

`chronos2`'s q95 sits within ±0.01 of its target at every lead and is **better
calibrated than `wt_med`'s**; its q80 is mildly under-covered (0.76–0.83). Field
coverage is 0.99+ at d0–d7 and **0.000 from d10**, because `max_lead_days = 7` — the
`uncertaintyMinutes` field can be served from this model for a week and no further.

### D9 — coverage is total, and that is also the limitation

`chronos2` has `P(forecast | operated) = 1.000` **and**
`P(forecast | not operated) = 1.000` at every lead: it produces a curve for every slot
of the origin-known window and **never abstains**. For comparison `snaive7` is
0.872 / 0.260 and `h5` 0.966 / 0.890. BENCH-SPEC's D9 calls `P(fc | not operated)` the
bar a model with an open/closed decision has to beat, and Chronos-2 is at the worst
possible value. **It cannot tell you a ride will not run**, so it contributes nothing to
`ridesUnavailable` / `opensAt` and must not be routed to them.

### Runtime and VRAM

| | measured |
|---|---|
| VRAM peak, four variants, inference | **1.86 GB** (cap `MLBENCH_FM_VRAM_FRACTION=0.6` → 9.8 GB of a 10 GB ceiling) |
| weight load | 3.7 s, once per process |
| per origin, 30 parks, 4 variants | 71 s (Feb) … 276 s (Oct) |
| per daily-origin slot call | `chronos2` 6.5–11.3 s · `chronos2_grid` 18.6–23.4 s |
| subset total | **5 658 s = 1 h 34 min**, exit 0, 33/33 origins |
| report (CPU, 2 cpu / 4 g) | 1 266 s full period · 769 s headline window |

VRAM is a non-issue for inference. The only real VRAM risk is `chronos2_ft` (full
fine-tune), which the runbook gates behind a one-origin probe and which was **not run**
— the GPU belonged to PAR-829.

**Full-run cost** (157 parks, 231 daily origins; GPU cost ≈ linear in ride count,
157/30 = 5.23×): **≈ 67 h** for the four-variant grid, ≈ 52 h for
`chronos2` + `chronos2_grid`. Not parallelisable — one GPU job at a time across all
agents. The cheapest change that actually fixes the evidence is **all 231 origins on the
same 30 parks: ≈ 12.7 h, one night.** That is what takes the planner leads from 33
origin days to 231, makes the headline common window gradeable for the first time, and
is the only way UC4 becomes measurable at all.

## TimesFM-3 — NOT MEASURED, ruled out on technical grounds

**No TimesFM-3 number exists in this review. It was never scored.** This is a decision,
not a result, and it must not be read as a measurement:

1. **No q95.** The quantile head is fixed at 0.1–0.9. BENCH-SPEC requires q0.8 **and
   q0.95** interval coverage for UC3, and D8 is what drives the frontend's
   `uncertaintyMinutes` / `accuracy.typicalError`. TimesFM-3 therefore **cannot serve the
   uncertainty field as specified** without an added quantile head — see the D8 table
   above for the bar Chronos-2 clears and it cannot reach at all.
2. **Licence.** The TimesFM 3.0 weights are under the TimesFM Non-Commercial License
   v1.0 (the code package is Apache-2.0). Benchmarking is covered; production serving is
   an owner decision.

Two further handicaps, which is why no GPU time was spent confirming the obvious: it
caps variate attention at 32, so each ride is a univariate task; and it can only be run
on the compressed context (it interpolates NaN linearly, which is wrong across nights),
i.e. on the layout measured above to be the losing one. A TimesFM-vs-Chronos comparison
would not have been like-for-like on layout.

The adapter is implemented, registered and tested with the stub backend, so scoring it
is a GPU booking rather than new work.

## Two harness findings from this workstream

**The `HistoryView` day cap silently bounds every long-context plug-in.** Critic S9's
fix materialises the history frame and caps it at `BenchConfig.history_days` (56). The
foundation plug-ins ask for `context_slots // 24 + 2` = 343 days of compressed context
and 400 days for the daily head, so on the merged tree every Chronos-2 origin raised
`ValueError` and the model could not be scored at all. **Raising is what made this
safe**: an implementation that truncated instead would have published numbers from a
shortened context with nothing in the output to show it. The cap is part of the plug-in
contract, not an implementation detail, which is why the fix is to let a model declare
its need — `Model.truth_history_days` (0 = the config window), with
`Runner.history_days_needed()` taking the max over the registered models and reading the
value off the **class**, exactly like S8's oracle gate, so an instance attribute cannot
widen an information budget.

**The history cache was keyed on the origin table's name, and the intraday path reuses
one name.** `Runner.intraday()` does `CREATE OR REPLACE TEMP TABLE oh` once per origin
hour, so a cache keyed on `(origin date, table name)` served 08:00's context at
10:00…20:00: every intraday origin after the first forecast from a context **frozen at
the first hour of the day**, which is the opposite of what UC1 measures, and nothing in
the output showed it. The key now includes the cut itself. The test that should have
caught it passed *because* of it — `test_information_cut_plugins` poisons everything
after the 06:00 cut and then asserted the intraday frames unchanged, but an origin at
08:00 is entitled to the day's truth up to its own cut, so once the context follows its
own cut those frames *must* move. It now runs one pass per cut: under the 06:00 poison
the intraday paths must move and the daily paths must not, and under a poison from the
last intraday cut all three must hold — which is what actually covers an intraday leak.
This regression is **not** in the code that scored this run (`cdbc95a3` predates
`5c216ffa`), so no number here is affected.

## Licences — what is allowed

- **Chronos-2 — Apache-2.0.** Benchmarking, fine-tuning and production serving are all
  fine. No decision needed, which is a real practical advantage independent of accuracy.
- **TimesFM 3.0 — weights under the TimesFM Non-Commercial License v1.0** (the code
  package is Apache-2.0). park.fan is non-commercial, so the offline benchmark here is
  covered. **Serving these weights in production is an explicit owner decision** and
  needs the licence text re-read against the actual deployment before it happens.
