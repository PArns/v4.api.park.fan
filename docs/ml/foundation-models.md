# Foundation models in ml-bench — Chronos-2, TimesFM 3.0 (PAR-828), TiRex-2 and Toto 2.0 (PAR-853), Chronos-2 LoRA and TimesFM 2.5 (PAR-1063, PAR-1064), PatchTST-FM-r2, TTM-r2 and TabPFN-TS (PAR-1067)

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

| | git SHA recorded | measured `code_sha256` |
|---|---|---|
| scored `parts/` (the sums) | `cdbc95a3` | `d5acedf6…` |
| wrote `tables/` (the verdicts) | `3107d7c9` | `4b2a41ce…` |

(An earlier revision of this table published `ab8102bc` / `6de411d2…`. That was the
**first, discarded** report pass — the one re-run in `d2dcee63` when PAR-827's `53165b5f`
moved `report.py`. The numbers were unaffected, since `4b2a41ce` equals
`code_hash(HEAD)`, but the table described a pass whose output is not what ships. That is
the second false record this mechanism has produced in two days, which is the argument for
reading it out of `report-tables.json` rather than transcribing it by hand.)

Both `code_sha256` values were recomputed from `git archive <sha> ml-bench/mlbench`
and reproduce exactly, and **the measured hash is the field to check** — not the
`git_sha` beside it. This branch is rebased onto the PAR-827 branch several times a day,
which rewrites every SHA while leaving the tree identical, so a `git_sha` recorded by a
launcher ages out within hours while the measured hash does not. That asymmetry is
exactly why BENCH-SPEC #7 requires the hash to be measured rather than asserted. The
report hash reproduces from the current branch HEAD with
`git archive HEAD ml-bench/mlbench`; `cdbc95a3`, which scored `parts/`, contains all
three coverage-parity fixes and is preserved on
`origin/parns123/par-828-…-mit-REBASED`.

When PAR-827's `53165b5f` moved `report.py` and so the report hash, **both passes were
re-run on the final code** rather than published against a hash describing code on no
remote. All six spot-checked UC1 margins reproduced to the last digit, which is the
expected result: their change only touches the `--origin-from`/`--origin-to` path, which
these passes do not use.

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

#### Read these as WEEKDAY-conditional, not lead-indexed — the two axes are aliased

**On this shard the planner lead axis is perfectly confounded with day of week, and the
confound is exact rather than approximate.** `--shard 0/7` starts at **2026-02-19, a
Thursday**, and spans **224 days = 32 × 7**, so every one of the 33 origins is a Thursday
and each lead is one fixed weekday:

| lead | d0 | d1 | d2 | d3 | d4 | d5 | d6 | d7 |
|---|---|---|---|---|---|---|---|---|
| target weekday | Thu | Fri | **Sat** | **Sun** | Mon | Tue | Wed | Thu |

So **the "d2 hole" is the Saturday cell** and the other non-win, d3, is **Sunday** —
adjacent, in order, exactly how a weekend effect would present itself. The mechanism is in
this run's own `cells.csv`, on ex-ante busy rides across d0–d7: every flat profile reaches
its **minimum** MAE at d2 (`h5` 13.679, `wt_med` 13.802, `clim` 14.323) while both
Chronos-2 variants reach their **maximum** (`chronos2` 15.141, `chronos2_grid` 14.966), and
the other level-driven model degrades the same way (`lvlh5_tft` 14.271) while the flat
profiles do not. Saturday is the most predictable crowd day there is, so a fixed profile is
at its best and a model that forecasts the day's *level* is at its worst — a weekday story,
not a horizon story.

**The lead-indexed routing rule is therefore withdrawn.** In production d2 is every
weekday, so "do not route d2" would assign to *lead* what is measured on *Saturday*. The
honest statement is: **on Thursday origins, Chronos-2 loses to `h5` on the Saturday target
and is marginal on Sunday.** Whether that is a lead effect or a weekend effect is
**irreducible on this shard** and needs the dense-origin run to separate.

One contrast *is* weekday-controlled, and it shows a genuine lead effect underneath:
**d0 and d7 are both Thursday.** Against `h5`, all rides, `chronos2` goes −0.826
[−1.116, −0.561] at d0 to +0.013 [−0.321, +0.291] at d7, and `chronos2_grid` −0.983 to
−0.165 — about **+0.82…+0.84 min of degradation over seven days at constant weekday**.
Against `lvlh5_tft` the same pair degrades by +0.39…+0.46 min and is still a win at d7. So
there is a real horizon effect; what cannot be separated is how much of the d2/d3 shape
belongs to it.

**Why this was not caught earlier: no table the harness emits carries a weekday column** —
not `cells.csv`, not `mae_by_season.csv`, not `lead_availability.csv`. The only split
offered is by season. A `mae_by_weekday` split, or simply recording the origin weekday
distribution in `run-*.json`, would have made the aliasing visible at a glance, and a
shard stride that is a multiple of 7 should arguably be refused outright.

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

**The grid claim does not extend to the live surface, and must not be read as doing so.**
Only `chronos2` sets `intraday = True`, so `chronos2_grid` has **zero** UC1, UC2-intraday,
D1 and D2 cells in either pass — it was never scored on a live surface at all. Everything
in the UC1 section above is `chronos2`, the compressed layout, and **`chronos2` is the
variant the live recommendation names.** Reading the daily-origin grid gain across to UC1
would be extrapolation, and a weak one: the largest measured grid gain is 0.157 min against
UC1 margins of 0.26–4.5 min, i.e. immaterial at that scale even if it transferred. Running
`chronos2_grid` on intraday origins is the cheapest open item in this workstream.

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
precipitation, an upper bound no forecast can reach.

**Stated plainly, because it cancels a planned piece of work: building an archive of
weather forecasts is not justified by this result.** It was on the review's follow-up
list. The ceiling on what a perfect weather feed could buy this model family is roughly a
tenth of a minute of slot MAE — two orders of magnitude below the margins that decide the
routing here (−0.4…−4.5 min on UC1). A weather archive may still be justified by
PAR-830's level work; that is a different question on different evidence and this result
does not speak to it.

There is no archived weather forecast in the export, so `_owx` is an **ORACLE upper bound
and not a servable configuration**; every `_owx` number in this document is labelled that
way.

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
**The part that deserves the emphasis is that the test meant to guard this passed
*because* of the bug.** `test_information_cut_plugins` poisons everything after the 06:00
cut and asserted the intraday frames unchanged — but an origin at 08:00 is *entitled* to
the origin day's truth up to its own cut, which the poison has already rewritten, so that
assertion could only hold while the intraday context was stale.

This is the **third instance in this review of a test that could not observe the thing it
guarded**: a two-park fixture that made `wins = True` unreachable, `--origin-from`
reviewed as working while never once executed, and now an information-cut assertion
carried by a staleness bug. Three independent instances is a pattern rather than bad luck.
The transferable lesson for anyone adding a guard to this harness: it needs a case in
which it is *capable of failing*, or it records an intention rather than a property.

This regression is **not** in the code that scored this run (`cdbc95a3` predates
`5c216ffa`), so no number here is affected; it would corrupt any UC1 re-score on the
merged tree, which is why it was routed upstream immediately.

## A third harness note: an information budget must be read off the class

`covariate_history_days` and `truth_history_days` are both per-model *information budgets*,
and both are now read off `type(m)` rather than the instance. The reason is critic S8's:
`scored_name()` is a classmethod, so a model that set the attribute in `__init__` would
receive the wider window and still be scored under its honest name. `covariate_history_days`
was read off the instance at both call sites until this was noticed.

The guard for it was **mutation-tested**, because the section above argues that a test needs
a case in which it is capable of failing — and the first version of this one was not. It
called `covariates()` with `type(m).covariate_history_days` already resolved, so it passed
with the fix reverted. It now drives `plugin_predict` / `plugin_predict_daily` and inspects
the covariate frame the model is actually handed; reverting `type(m).` to `m.` makes it fail
with the real symptom — covariates reaching back to 2026-02-01 under a class budget of 0
days, a ~78-day over-reach of pre-origin covariates.

## Licences — what is allowed

- **Chronos-2 — Apache-2.0.** Benchmarking, fine-tuning and production serving are all
  fine. No decision needed, which is a real practical advantage independent of accuracy.
- **TimesFM 3.0 — weights under the TimesFM Non-Commercial License v1.0** (the code
  package is Apache-2.0). **Benchmark-only; it can never be served on park.fan.** Re-read
  on 2026-10-24 (PAR-1064), the licence grants "non-commercial **and non-production**
  use" and states that use "in direct or indirect interactions with end users or
  production systems … is not a Non-Commercial Purpose" — so park.fan being
  non-commercial does not help, and this page's earlier "owner decision" was wrong.
  Serving it would need a commercial licence from Google. The offline benchmark is
  covered ("internal benchmarking").
- **TimesFM 2.5 — Apache-2.0** (model card of `google/timesfm-2.5-200m-pytorch`, revision
  `1d952420`, `license: apache-2.0`; the timesfm README: "model weights up to version 2.5
  remain Apache-2.0"). Production serving would be allowed.
- **Granite PatchTST-FM-r2, Granite TTM-r2 — servable; TabPFN-TS — benchmark-only**
  (PAR-1067). The quoted lines and URLs are in "Licence verdicts (PAR-1067)" below. The
  code enforces the verdict: every foundation plug-in declares `licence`
  (`servable` / `benchmark-only` / `unclear`) and `serving_candidate`, and a class that
  sets `serving_candidate = True` without `licence = "servable"` is refused when it is
  defined (`FoundationModel.__init_subclass__`). Every `run-*.json` records both per arm
  under `licences`.


---

# TiRex-2 and Toto 2.0 (PAR-853)

Two more pretrained foundation models, on the **same inputs** as `chronos2` wherever the
model takes them, and scored **inside the same run** as `chronos2` / `chronos2_nocov` —
the harness pairs models within one run, never across run directories, so a cross-run
comparison would have no paired CI.

## The two models

| | TiRex-2 | Toto 2.0 |
|---|---|---|
| Weights | `NX-AI/TiRex-2`, revision `05e5b26d` | `Datadog/Toto-2.0-313m`, revision `a7bab288` |
| Package | `tirex-2==0.2.1` | `toto-2==2.0.0` |
| Licence | **Apache-2.0**, code and weights (model card + LICENSE, checked 2026-10-11) | **Apache-2.0**, code and weights |
| Covariates | **native** past + known-future | a `known_dynamic` hook exists, but exogenous input is **not a trained feature of 2.0** — hence `toto2_nocov` as the honest variant |
| Multivariate | one park = one multivariate task | one park = one multivariate task, `series_ids` shared |
| Quantiles | fixed head 0.1 … 0.9 — **no q95** (returned as NaN, D8 reports it missing) | 9 knots on the output head, q50 / q80 read off |
| Horizon | 320-step head, rolled forward by feeding the median back | single `forecast(horizon=…)` call |
| Context | 2048 steps (`MLBENCH_TIREX2_CONTEXT`) | 4096 steps (`MLBENCH_TOTO2_CONTEXT`), left-padded to whole patches |

**Why TiRex-2 and not TiRex.** The original (`NX-AI/TiRex`, package `tirex-ts`) is under the
NXAI Community License — free below EUR 100 M revenue — and is univariate without
covariates. TiRex-2 supersedes it with native past/known-future covariates **and** an
Apache-2.0 licence, so it is TiRex-2 that is benchmarked. Both PAR-853 models are therefore
licence-clean for a production decision, unlike TimesFM 3.0, whose weights are
non-commercial.

## Variants registered

| name | context | covariates | origins |
|---|---|---|---|
| `tirex2` | compressed, 2048 | yes | daily + intraday |
| `tirex2_grid` | regular 15-min grid, 2048 steps = 21 days | yes | daily slots only |
| `toto2` | compressed, 4096 | through `known_dynamic` | daily + intraday |
| `toto2_nocov` | compressed, 4096 | **none** — the official Toto 2.0 | daily slots only |
| `toto2_grid` | regular grid, 4096 steps = 42 days | through `known_dynamic` | daily slots only |

`chronos2` and `chronos2_nocov` are re-scored in the same run as the comparators.

## Two traps this cost, both at construction time

**1. `CUDA_HOME` is required to IMPORT TiRex-2, on a box with no CUDA toolkit.**
`import tirex2` pulls in `xlstm`, whose sLSTM module resolves that kernel's include and lib
paths at import time via `torch.utils.cpp_extension.include_paths("cuda")`, which raises
`OSError: CUDA_HOME environment variable is not set`. torch here comes from the cu128 wheel
and the image is `python:3.12-slim`, so there is no toolkit. The exception leaves
`FoundationModel._engine()` and **the whole run aborts at the first origin with nothing
written** — not a degraded result, no result. `TiRex2Backend.__init__` now
`os.environ.setdefault("CUDA_HOME", "/usr/local")` before the import: TiRex-2 is a BiXLSTM
(mLSTM) model, never instantiates the sLSTM cell, and its recurrent path is the pure-PyTorch
one `device="cpu"` selects, so the value is only string arithmetic for a kernel that is never
built. `setdefault` leaves a real toolkit root alone. If a future `xlstm` actually tries to
compile, it fails loudly on the missing nvcc — a visible error, never silently different
numbers.

**2. `torch.compile` has no compiler either.** `tirex2` wraps blocks in `@torch.compile`;
inductor needs a C/C++ compiler at runtime. `torch._dynamo.config.disable = True` runs it
eagerly, which gives the same numbers.

A third, non-trap: an early CPU smoke measured `toto2` at **568 s per origin-task**
(`vram_peak=0.00GB`). That was a CPU run, not a device-placement bug — on the GPU the same
task takes 24.7 s.

## Runtime and VRAM (GPU smoke, RTX 5080, 30 parks, one origin)

| model | daily slots | daily level | intraday slot task | VRAM peak |
|---|---|---|---|---|
| `tirex2` | 16.7 s | 9.7 s | 6.0–7.3 s | 0.54 GB |
| `toto2` | 24.7 s | 5.3 s | 11.7–12.8 s | 2.29 GB |
| `toto2_nocov` | 11.5 s | — | — | 2.29 GB |
| `tirex2_grid` | 19.1 s | — | — | 2.29 GB |
| `toto2_grid` | 14.5 s | — | — | 2.39 GB |
| `chronos2` | 28.2 s | 4.7 s | 14.0–14.7 s | 3.29 GB |
| `chronos2_nocov` | 13.7 s | — | — | 3.29 GB |

All seven models in one origin: **431.4 s**, peak VRAM 3.29 GB — well inside the 10 GB cap
the resource rules set for a shared GPU. Model load: TiRex-2 3.7 s, Toto 2.0 ~8 s,
Chronos-2 10.5 s.

## Result

Run `20261011-par853-toto-tirex-shard5`: 47 origins, `--shard 0/5` over 2026-02-19 … 2026-10-07 on
the 30-park subset — the same mixed-weekday origin set as the Chronos-2 re-run (the harness' own
census in `run-0of5.json`: Mon 6, Tue 7, Wed 7, Thu 7, Fri 7, Sat 6, Sun 7, `origin_weekday_distinct`
7). Paired with `h5`, `lvlh5_tft`, `persistence` and the other six models; both report passes.
Provenance on the image: `git_sha 474e3932`, `code_sha256 8ee46789…`, `image_id sha256:f48c1197…`.
5.0 h wall clock, 3.4 GB VRAM peak. Every figure below comes out of
`ml-bench/scripts-par853/summarise.py` over the run's `cells_vs_*.csv`, never transcribed.

**Neither model beats Chronos-2, with one 15-minute exception.** Full period, all rides, paired
against `chronos2` with park-cluster CIs:

| band | `tirex2` | `toto2` |
| --- | --- | --- |
| m015 | **−0.070** [−0.124; −0.025] | **−0.176** [−0.242; −0.106] |
| m030 | +0.159 [+0.085; +0.234] | +0.014 [−0.036; +0.071] (tie) |
| m060 | +0.411 [+0.272; +0.560] | +0.154 [+0.097; +0.212] |
| m120 | +0.582 [+0.417; +0.777] | +0.300 [+0.209; +0.399] |
| h2–4 | +0.576 [+0.431; +0.738] | +0.317 [+0.222; +0.430] |
| h8+ | +0.443 [+0.265; +0.642] | +0.318 [+0.200; +0.459] |

At 15 minutes both are better than Chronos-2; from 30 minutes on Chronos-2 wins everywhere. Against
the served hourly profile `h5` both are clearly better in the live bands (`toto2` −4.19 … −1.65,
`tirex2` −4.08 … −1.36 over m015 … m120) and **worse** in the planner from d1 on (+0.32 … +0.88).

**Planner.** `tirex2_grid` is the only variant that keeps up with `chronos2` — on all rides a tie at
every lead d0–d7 (−0.013 … +0.132, every CI spans 0) and better than `chronos2_nocov` throughout
(−0.364 … −0.536). On **busy** rides it is worse at d0, d2–d5 and d7 (+0.226 … +0.447), a tie only
at d1 and d6. The level-on-H5-shape composite is again the strongest route: `tirex2_x_h5` reaches
**d21** on busy rides and wins the d10 hand-over cell (−0.453 [−0.710; −0.254]) — the one lead in the
whole run that a PAR-853 model takes off Chronos-2, whose `chronos2_x_h5` wins d1–d7, d14, d21, d30
and reaches d30. `toto2_x_h5` reaches d3. A paired test *between* the composite arms is not possible
from this run: they were not in `--pair-with`, so only their margins against a common reference are
comparable, and those overlap.

**`toto2_grid` is unusable, and it is not the harness.** +4.25 … +4.90 minutes behind `chronos2` on
all rides and +10.07 … +11.58 on busy rides (full period), with q80 coverage **0.54** against ~0.80
for every other arm. Three checks place the cause in Toto 2.0 rather than in the grid builder:
`tirex2_grid` uses the same grid path and ties `chronos2`; the penalty is present in **30 of 30**
parks (median +2.59 min at L=0, growing with the park's error level, from +0.23 to +19.0), so it is
not a per-park data gap; and the same weights in the compressed context (`toto2`) are only +0.3 …
+0.8 behind `chronos2`. What `toto2_grid` feeds the model is a regular 15-minute grid over 42 days
(4096 steps) **including closed hours**, left-padded to whole patches. Toto 2.0 does not cope with
that; Chronos-2 and TiRex-2 do. The variant stays registered and documented so nobody tries it twice.

**Covariates through Toto's `known_dynamic` hook do nothing**, as the model card implies: `toto2` and
`toto2_nocov` differ by 0.03–0.04 minutes, and both are a tie against `chronos2_nocov`. The hook is
not a substitute for trained covariate support.

**Uncertainty.** TiRex-2's q80 coverage is 0.80–0.83 against a 0.80 nominal — at least as good as
`chronos2`'s 0.79–0.81. Neither PAR-853 model has a q95 head, and D8 leaves the field **empty**
rather than inventing it (`cov_q95` is blank for all five PAR-853 variants and filled for
`chronos2` / `wt_med`).

**Recommendation: DROP both for serving; keep TiRex-2 as a plug-in.** Chronos-2 is at least as good
everywhere and better almost everywhere, already runs in the forward archive's shadow, and has a q95
head. TiRex-2 is the only PAR-853 candidate that wins anything, is Apache-2.0 in code and weights,
and is the fastest of the three (16.7 s per origin task) — the first choice if Chronos-2 ever falls
away for licence or operational reasons. Toto 2.0 adds nothing here that Chronos-2 or TiRex-2 does
not do better.

Full tables, both passes: `~/ml-review-2026-10/RESULTS-PAR-853.md`.


---

# Chronos-2 LoRA (PAR-1063) and TimesFM 2.5 (PAR-1064)

**Implemented and tested, not measured.** No GPU run has been made; the run commands
(probe, `--shard 0/5` run, both report passes) are in `docs/ml/ml-bench.md`, "Running
PAR-1063 / PAR-1064". Every arm is meant to be scored in the same run as `chronos2` and
`chronos2_ft`, because the harness pairs models inside a run only.

## Variants registered

| name | what it is | origins |
|---|---|---|
| `chronos2_lora` | Chronos-2 + a LoRA adapter, refit every 28 days on truth before the fit's origin | daily + intraday slots |
| `timesfm25` | TimesFM 2.5 zero-shot, compressed context (8192), covariates via XReg | daily + intraday slots, daily level (→ `timesfm25_x_h5`) |
| `timesfm25_grid` | the same on the regular 15-min grid (8192 steps = 85 days) | daily slots only |

## Chronos-2 LoRA

- **Mechanism:** chronos-forecasting 2.3.2's own `Chronos2Pipeline.fit(finetune_mode="lora")`
  (peft; the package default: r = 8, alpha = 16, on `self_attention.q/k/v/o` and
  `output_patch_embedding.output_layer`). A CPU smoke with the real weights trained
  **1.21 M of 120.7 M** parameters and forecast through the returned `PeftModel`.
- **Same as `chronos2_ft` in everything but the adaptation:** 400 steps, lr 1e-5 (the
  package's recommended LoRA rate), context 2048, horizon 384, batch 64, the same inputs,
  the same cadence. A test asserts the two classes agree on every one of those fields.
- **Leakage boundary:** the fit runs inside `predict()` at a **daily** origin and trains on
  the context the model is about to forecast from, which the runner has already cut at
  that origin; intraday origins and the next 27 days reuse the adapter, so every later
  forecast uses weights trained on strictly older truth. `test_lora_fit_sees_only_truth_before_its_origin`
  poisons every truth slot after the origin and asserts the fit's inputs are unchanged,
  and that it is fitted once per date, never at an intraday origin.
- **Silent fall-back closed.** Without peft, `fit(finetune_mode="lora")` only *warns* and
  runs a full fine-tune — which would have been scored as `chronos2_lora`. The backend
  imports peft first and fails instead (tested).
- **Why every 28 days and not per origin.** All 231 origins on the 30-park subset cost
  ≈ 12.7 h of inference for four zero-shot variants. A per-origin refit adds 231 × (one
  fit) on top; every 28 days adds ~9 fits. 28 days is also `chronos2_ft`'s cadence, so the
  pair isolates full vs low-rank adaptation instead of confounding it with refit
  frequency. `--probe` measures one fit; if it turns out short, a per-origin arm is a
  one-line subclass.

## TimesFM 2.5

- **Weights:** `google/timesfm-2.5-200m-pytorch`, revision `1d952420`
  (`MLBENCH_TFM25_REVISION`), **Apache-2.0** on the model card. The class
  `timesfm.TimesFM_2p5_200M_torch` ships in the same `timesfm==3.0.2` wheel the PAR-828
  image already installs.
- **Covariates — supported, but not like Chronos-2.** TimesFM 2.5 is univariate and the
  network takes no covariates; the package adds them through `forecast_with_covariates`
  (in-context regression, "XReg"). Mode `xreg + timesfm`: a linear fit (OLS through
  `pinv`, after per-series normalisation) of the target on the covariates over the
  context, TimesFM forecasts the residual. The adapter calls it once per park, so the
  regression is shared by the rides of a park. It needs jax + scikit-learn
  (`timesfm[xreg]`); the image takes CPU jax 0.6.2 (`force_on_cpu=True`; jax 0.7+ needs
  numpy 2). The covariates are `chronos2`'s, except `day_len_h` on the daily series —
  there NaN means "beyond the publishing horizon", and XReg cannot mask, so it would read
  as a 0-hour (closed) day. Other NaN covariates (the window-relative minutes off-window
  on the grid, which `in_win` flags) enter as 0.
- **NaN in the target** is cut at the front and interpolated linearly — what the package
  does to every input anyway, done up front because the regression target may not hold
  NaN. On the grid that turns every night into a ramp; `timesfm25_grid` exists to measure
  what that costs, not on the assumption that it is free.
- **Quantiles:** continuous quantile head 0.1 … 0.9, read at columns 5 (q0.5) and 8 (q0.8);
  **no q95** (NaN, D8 reports it missing — the same gap that ruled TimesFM 3.0 out of the
  uncertainty field). The head covers 1024 steps; the longest request is 768 (8 days on the
  grid) or 366 (daily path), and a longer one raises rather than truncating.
- **Runtime knobs:** `MLBENCH_TFM25_CONTEXT` (8192), `MLBENCH_TFM25_BATCH` (32);
  `torch_compile=False`, because inductor needs a C compiler the slim image lacks (the
  TiRex-2 trap).
- **CPU smoke with the real weights** (synthetic two-ride task, context 1024): plain and
  XReg paths both return finite q50/q80 for the rides that ran and NaN for the one that
  never did; a 366-step daily horizon works.

## TimesFM 3.0 is benchmark-only

See "Licences — what is allowed" above: the weights' licence excludes production and
end-user use outright, so `timesfm3` can be scored as a reference arm (`--model timesfm3`)
but is never a serving candidate, whatever it scores.

---

# Granite PatchTST-FM-r2, Granite TTM-r2 and TabPFN-TS (PAR-1067)

**Implemented and tested, not measured.** No GPU run has been made; the combined run command
(probe, `--shard 0/5` run, both report passes) is in `docs/ml/ml-bench.md`, "Running PAR-1067
together with PAR-1063 / PAR-1064". Image: `requirements-par1067.txt`, which includes
`requirements-par1063.txt`, so `chronos2`, `chronos2_ft`, `chronos2_lora` and `timesfm25` pair
with the new arms inside one run.

## Licence verdicts (PAR-1067)

Read from the primary sources on 2026-10-24.

| model | verdict | the line it rests on | source |
|---|---|---|---|
| Granite PatchTST-FM-r2 | **servable** | metadata `license: openmdw-1.0`; README: "This model is dual‑licensed under the OpenMDW License version 1.0 and the Apache License 2.0. Users may choose to use the model under either license." The repository's `LICENSE` says the same. Not gated. | <https://huggingface.co/ibm-granite/granite-timeseries-patchtst-fm-r2> (revision `b125275f`) |
| Granite TTM-r2 | **servable** | metadata `license: apache-2.0`. Not gated. | <https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2> (branch `1536-720-r2` = commit `3ca910a8`) |
| granite-tsfm (code of both) | Apache-2.0 | PyPI `license_expression: Apache-2.0` | <https://pypi.org/project/granite-tsfm/0.3.10/> |
| TabPFN-TS → TabPFN-3.5 weights | **benchmark-only** | `LICENSE`, "TABPFN-3.5 License v1.0 — Last Revised: September 9, 2026": the weights are available "for your non-commercial and non-production use"; "For clarity, use (a) for any revenue-generating activity, (b) in direct or indirect interactions with end users or production systems, or (c) to train, fine-tune, or distill other models for commercial use, in each case is not a Non-Commercial Purpose."; "Any use of Outputs in production systems, business processes, commercial research services, client deliverables, or revenue-generating activities requires a separate commercial license." Gated (`extra_gated_fields`). | <https://huggingface.co/Prior-Labs/tabpfn_3_5/blob/main/LICENSE> |
| tabpfn-time-series / tabpfn (code) | Apache-2.0 | repository licence (tabpfn-time-series); PyPI `license_expression: Apache-2.0` (tabpfn) | <https://github.com/PriorLabs/tabpfn-time-series> |

TabPFN-TS is the TimesFM 3.0 case again: the code is open, the weights it downloads by
default are not servable — park.fan being non-commercial does not help, because the licence
excludes *production and end-user* use, not only revenue. tabpfn-time-series 1.3.0 pins
`TABPFN_MODEL_VERSION = "v3.5"` (`defaults.py`). **Whatever `tabpfn_ts` scores, it is a
reference arm, never a serving candidate.**

## Covariate support — what each model can take

| model | known-future covariates | evidence |
|---|---|---|
| PatchTST-FM-r2 | **none** | `PatchTSTFMForPrediction.forward(past_values, past_observed_mask, pad_mask, prediction_length, quantile_levels, …)` — the covariate arguments are commented out: `# future_values: Optional[torch.Tensor] = None,  # future use` (granite-tsfm 0.3.10, `modeling_patchtst_fm.py`). The model card mentions no covariates. |
| TTM-r2, zero-shot | **none** (channel-independent) | model card: exogenous use appears only as "Finetuning and Forecasting with Exogenous/Control Variables"; "Decoder Channel-Mixing can be enabled during fine-tuning". |
| TTM-r2, fine-tuned | **yes** — exogenous channels via forecast channel mixing | `TinyTimeMixerConfig(exogenous_channel_indices=…, enable_forecast_channel_mixing=True)`; `ForecastChannelMixer` raises "future_values cannot be none when we have exogenous channels". Loading the r2 weights with that config reports every `head.fcm_block.*` parameter and the decoder's channel mixers as "MISSING … newly initialized": the exogenous path does not exist until it is trained. |
| TabPFN-TS | **yes**, natively | `predict_df` docstring: "Additional columns are treated as known covariates (e.g., temperature, holidays, promotional flags). These can improve forecast accuracy when their future values are known." Only columns present in both the context and the future frame are used. |

So by the rule this benchmark now works under — a model without known-future covariates
lost where it matters — **`patchtst_fm` and `ttm_r2` are covariate-free reference arms**
(the role of `chronos2_nocov`). The arms that can actually win are `ttm_r2_ft` (slots) and
`tabpfn_ts` (daily level, benchmark-only).

## Variants registered

| name | what it is | origins | quantiles |
|---|---|---|---|
| `patchtst_fm` | PatchTST-FM-r2 zero-shot, compressed context, no covariates | daily + intraday slots, daily level (→ `patchtst_fm_x_h5`) | q50 / q80 / **q95** |
| `ttm_r2` | TTM-r2 `1536-720-r2` zero-shot, compressed context, no covariates | daily + intraday slots | point only |
| `ttm_r2_ft` | TTM-r2 + the `chronos2` slot covariates as exogenous channels, fine-tuned every 28 days | daily + intraday slots | point only |
| `tabpfn_ts` | TabPFN-TS on the ride-day P90 series, day covariates + `day_len_h` | daily LEVEL only (UC4 / D6 / D7 / PEAK; `tabpfn_ts_x_h5` at the slot leads) | — |

## PatchTST-FM-r2

- **Weights:** `ibm-granite/granite-timeseries-patchtst-fm-r2`, revision `b125275f`
  (`MLBENCH_PATCHTSTFM_REVISION`), ~385 M parameters, granite-tsfm 0.3.10.
- **Window:** context **plus** forecast must fit the model's 8192 steps
  (`forecast_single_step_fast` raises otherwise), and the model always decodes at least
  d_patch × pretrain_mask_cont = 128 steps; the adapter hands over the last
  `8192 − max(H, 128)` compressed slots.
- **Missing values:** masked natively (`past_observed_mask` from NaN). A ride that never ran
  is not sent and stays NaN.
- **Quantiles:** the head holds 99 quantiles 0.01 … 0.99, so q0.5, q0.8 **and q0.95** are
  read directly — a real q95 for D8, which TimesFM, TiRex-2 and Toto do not have.
- **Knob:** `MLBENCH_PATCHTSTFM_BATCH` (16 rides per forward).
- **CPU smoke with the real weights** (three synthetic rides, period-40 sine, every 17th slot
  NaN, one ride never ran): MAE 0.35 over a 200-step horizon, quantiles monotone, the empty
  ride NaN.

## TTM-r2 (`ttm_r2`, `ttm_r2_ft`)

- **Weights:** `ibm-granite/granite-timeseries-ttm-r2`, branch `1536-720-r2` pinned to its
  commit `3ca910a8` (`MLBENCH_TTM_REVISION`; a branch can move, a commit cannot): context
  1536, horizon 720. A longer request raises instead of truncating. r2 is the "minutely to
  hourly" release ("Ex. 10 min, 15 min, 1 hour."), so there is no daily path; r2.1 adds
  daily data and is not benchmarked here.
- **Scaling:** the card asks users to "externally standard scale their data independently for
  every channel"; the adapter z-scores each ride over its context and inverts the forecast.
  Gaps are 0 with `past_observed_mask = False`; a context shorter than 1536 is left-padded as
  unobserved.
- **Point forecast only:** q80 / q95 are NaN and the arm is not in D8. The shared output step
  used to turn a missing q80 into a copy of q50 (`np.fmax(NaN, q50) = q50`); it now keeps it
  missing (tested).
- **Fine-tune (`ttm_r2_ft`):** a TTM with `1 + k` channels (`prediction_channel_indices=[0]`,
  `exogenous_channel_indices=[1..k]`, forecast channel mixing, `decoder_mode="mix_channel"`),
  backbone frozen, decoder + head + mixer trained with AdamW at lr 1e-3
  (`MLBENCH_TTM_FT_LR`; the backbone stays frozen and the mixer starts from random weights —
  Chronos-2's 1e-5 is a full-model rate), 400 steps (`MLBENCH_FT_STEPS`), batch 64. The exogenous channels are
  `chronos2`'s slot covariates (minutes since opening / until closing, time of day, the day
  covariates), z-scored with their **past** statistics on both sides of the origin.
- **Leakage boundary:** training windows are cut from the forecast context, which the runner
  has already cut at the origin; `ttm_windows` returns `(past start, cut, future end)` with the
  future end ≤ the context length, so no window can reach the origin. Tested on its own, in
  the backend (a sentinel in the future covariates never reaches a training batch) and in the
  harness (poison every truth slot after the origin: the fit's inputs are unchanged, one fit
  per date, never at an intraday origin).
- **Why every 28 days.** The cadence of `chronos2_ft` and `chronos2_lora` (~9 fits over the
  February–October origins), so the three fine-tunes differ in model and adaptation, not in
  how fresh their weights are. A TTM fit is cheap (CPU smoke: 20 steps plus both forecasts in
  ~3 s), so per-origin refitting is affordable — but it would no longer pair on cadence; it is
  a one-line subclass (`finetune_every_days = 1`) if the probe makes the case.
- **CPU smoke with the real weights:** zero-shot and fine-tuned paths both run, the empty ride
  stays NaN. On a clean synthetic sine the zero-shot model missed a period-40 cycle (z-MAE 1.0)
  but tracked a period-96 one (z-MAE 0.14, the same with and without a mask) — a property of
  the model, and the compressed operating day (~40–50 slots) is close to the period it missed.
  The run will say whether that matters on real data.

## TabPFN-TS (`tabpfn_ts`)

- **Packages:** tabpfn-time-series 1.3.0, tabpfn 9.1.0, tabpfn-client 0.6.1 (imported by the
  pipeline, never used), datasets 4.8.5 (5.x needs pyarrow ≥ 24, the base pins 23.0.1).
- **Daily level only**, against `driver_level` and `wt_med` on UC4 / D6 / D7 / PEAK. One series
  per ride: the ride-day P90 (≥ 8 slots) over the last 400 days; a closed or unknown day is left
  out of the context, never filled. Known-future columns: the `chronos2` day covariates plus the
  published day length `day_len_h` — NaN beyond the publishing horizon is TabPFN's own missing
  value, so an absent schedule never reads as a closed day.
- **Calendar from real dates:** the backend receives the context and target dates
  (`wants_days`); the pipeline's `RunningIndexFeature` and `CalendarFeature` are used, its
  `AutoSeasonalFeature` is not — closed days make the series irregular, and the package
  recommends opting out of it for irregular timestamps.
- **LOCAL mode only.** `TabPFNMode.CLIENT` would send the data to Prior Labs' cloud API.
  Telemetry (posthog) is on by default in tabpfn-common-utils; the backend sets
  `TABPFN_DISABLE_TELEMETRY=1` before importing.
- **Tokens — read from the environment, never committed, never baked into the image.** The
  weights repo is gated and tabpfn checks a Prior Labs licence acceptance before it
  downloads (`tabpfn/browser_auth.py`). A run needs `HF_TOKEN` (a Hugging Face token whose
  account accepted the gate on the model page) and `TABPFN_TOKEN` (the Prior Labs token after
  accepting the licence at ux.priorlabs.ai) passed to `docker run`, plus
  `TABPFN_MODEL_CACHE_DIR` on a mounted volume so the checkpoint downloads once. Without them
  the first `tabpfn_ts` forecast fails loudly.
- **Wiring smoke (CPU):** with the ungated TabPFN v2 checkpoint swapped in (3.5 needs the
  tokens), a weekday-driven synthetic level was forecast exactly over 14 days, a 5-day gap in
  the context was skipped, and a ride without data stayed NaN.
- **No slot path** of its own (`slot_path = False`; the information-cut test checks the level
  path instead). `tabpfn_ts_x_h5` comes from the runner's level × H5 composition.

## Dependencies — `requirements-par1067.txt`

Resolved on top of `requirements.txt` with torch 2.11.0 — the newest cu128 wheel on
2026-10-24 — in a Python 3.11 venv, the Dockerfile's two-step install order, `pip check` clean.
What changes against the base image (only in images built with this extra):

- **pandas 2.2.3 → 2.3.3**: granite-tsfm 0.3.x requires `pandas>=2.3.3`. numpy stays 1.26.4
  (pandas 2.3 supports numpy ≥ 1.23.2 on Python 3.11).
- **A latent upgrade in PAR-1063's file, fixed:** jax 0.6.2 pulled the newest `ml_dtypes`,
  whose 0.6 release needs numpy ≥ 2, so pip silently upgraded the image's numpy to 2.4.
  `requirements-par1063.txt` now pins `numpy==1.26.4` and `ml_dtypes==0.5.4`.
- granite-tsfm needs `torch<2.12,>=2.10`. The Dockerfile's `torch>=2.7.0` from the cu128 index
  is 2.11.0 today; should cu128 publish 2.12, pip would replace it with a PyPI (cu13) build —
  `python -m mlbench gpu-check` after the build is the check.
