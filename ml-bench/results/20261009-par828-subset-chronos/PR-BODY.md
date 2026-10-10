Chronos-2 and TimesFM 3.0 as ml-bench plug-ins, plus the first scored subset run of
Chronos-2 and the two design decisions it settles.

Targets the PAR-827 branch (#445), not `main`.

## What this adds

`ml-bench/mlbench/models/foundation.py` plugs both time-series foundation models into
the PAR-827 harness through its restricted plug-in interface, so they are paired
against the same baselines on the same rows as every other candidate. Chronos-2 takes
a whole park as one multivariate task (every ride a variate, covariates shared);
TimesFM 3.0 caps variate attention at 32, so each ride is a univariate task. Both run
on a compressed context of operating-window slots, with a regular-grid layout and a
covariate-free variant as controls and an ORACLE-weather variant reported separately.
A daily level path feeds the harness's `level × H5` composition, so d10–d90 and the
crowd calendar are scored without asking for 15-min slots there.

Harness additions this needed: `--parks` for subset runs that re-score the baselines
on exactly the same parks, `qn95__<model>` (TimesFM 3.0's quantile head stops at 0.9,
so D8's q95 coverage needs its own denominator), `Model.covariate_history_days`, and
**`run --pair-with` / `report --vs`** for model-against-model paired CIs.

## Read this before the numbers

The committed run `20261009-par828-subset-chronos` (33/33 origins, exit 0, 6 545 s,
VRAM peak 1.86 GB) was launched from a **pre-rebase working tree**, which
`run-0of7.json`'s `git_sha: "unknown"` hid and its `code_sha256` exposed. That tree
predates PAR-827's `689a7b84`, whose
`UPDATE tg SET <models> = NULL WHERE ko < 0 OR kc < 0 …` nulls built-in baseline
columns **only** — plug-in columns are never in that list. So the baseline arm was
credited with slots the plug-in arm never covered.

**Every comparison of a Chronos-2 variant against `wt_med` / `h5` / `clim` /
`snaive7` / `lvlh5_*` in this run is void** (946 of the 963 region-wide cells; 17
survive) and is being re-scored
on the fixed harness. `results/20261009-par828-subset-chronos/PROVENANCE.md` records
the full diagnosis, what code actually ran, and the cell-by-cell ledger. Two things
survive, and both are mechanical rather than judgement calls:

- **vs `persistence`** — built in `runner.ih_{h}` from the raw `slots` table, in no
  `SLOT_MODELS` list, never passes through `UPDATE tg`.
- **variant vs variant** — all four variants are plug-ins on the same grid, and
  `n__<variant>` is identical **to the unit** in all 16 distinct UC2/UC3 MAE cells (d0-d7 x {all, busy}).

Separately: the run used `--shard 0/7`, so the headline common window
(2026-08-15…2026-10-07) holds only 7–8 origin days against the harness's ≥ 30 gate.
**Every cell of the headline-window report is suppressed.** Both report passes are
committed; all numbers below are the full period, the only gradeable one. Nothing
here is numerically comparable with `20261009-baselines-v2`.

## UC1 — the surviving result, and the interesting one

Chronos-2 vs `persistence`, 15-min slots from intraday origins, park-cluster 95 % CI,
negative = Chronos-2 better:

| lead | all rides | ex-ante busy |
|---|---|---|
| 15 min | **+0.263 [+0.179, +0.356]** loses | **+0.440 [+0.239, +0.654]** loses |
| 30 min | −0.416 [−0.610, −0.212] | −0.917 [−1.281, −0.490] |
| 60 min | −1.262 [−1.597, −0.927] | −2.617 [−3.123, −2.103] |
| 120 min | −2.268 [−2.920, −1.714] | −4.511 [−5.726, −3.541] |

D2 (plan-block live correction, headliners, 0–45 min): −0.645 [−0.937, −0.329].

baselines-v2 finds `persistence` unbeaten by **every** naive and profile baseline from
15 to 120 min, the best of them (`lvlh5_tft`) still +0.636 [+0.300, +0.958] behind at
2 h. That is not in conflict with the above — it is about a different model class.
Chronos-2 is the first candidate here to move the persistence crossover from "beyond
2 h" to roughly 20–30 minutes.

Note `chronos2` is the only variant with `intraday = True`, so the layout question is
**unmeasured on UC1**.

## Design decision 1 — the regular grid layout wins

`chronos2_grid` beats `chronos2` in **all 16** distinct UC2/UC3 slot-MAE cells. The mechanism
is visible in the decisions, where the compressed layout destroys the opening ramp:
first-hour MAE (D5) at d0 **−0.55 vs +0.05**, best-time regret (D3) **+0.14 vs +1.09**.
Price: 2–3× the GPU time (18.6–23.4 s per daily-origin call against 6.5–11.3 s).

TimesFM 3.0 can only run compressed (it interpolates NaN linearly, wrong across
nights), so it is handicapped by construction against `chronos2_grid`.

## Design decision 2 — covariates help

`chronos2_nocov` is worse in all 16 cells by 0.39–0.82 min MAE, and much worse on the
decisions (D3 regret +2.45…+7.35 against the reference, D5 +0.72…+2.43). Without
covariates the model fails to win at d1 at all.

## ORACLE weather is worth almost nothing

`chronos2_owx` uses weather **actuals** — there is no forecast archive, so it is an
upper bound and **not a servable configuration**. It buys **0.04–0.10 min** of MAE.
Perfect weather foreknowledge is not the missing ingredient for this model family.

Two harness notes for PAR-827, not fixed here: `report.NON_COMPETING` does not include
`*_owx`, so `chronos2_owx` currently wins hand-over cells in a table documented as the
serving router's input; and `foundation.py::_grid` rebuilds its own future axis and
inner-joins to `horizon_slots`, which would silently zero the grid variant's coverage
if a horizon slot were ever off the quarter hour (it is not, after `336b2b62`).

## Licences

- **Chronos-2 — Apache-2.0.** Benchmark, fine-tune and serve, all fine.
- **TimesFM 3.0 — weights under the TimesFM Non-Commercial License v1.0** (code
  package Apache-2.0). This offline benchmark is covered. **Production serving is an
  owner decision, not a conclusion** — park.fan is non-commercial so it is probably
  acceptable, but it should be answered against the licence text before any serving
  work. Chronos-2 raises no such question.

## Provenance, fixed so it cannot recur

The Dockerfile now also writes `/app/GIT_SHA`; `git_sha()` reads that file before
falling back to a `git` binary the image does not contain; and
`scripts-par828/run.sh` refuses to start unless the worktree is clean, its SHA
contains `a1d20bf8`, and the image was built from exactly that SHA — then re-reads
every `run-*.json` afterwards and declares the run **VOID** if the recorded SHA is
wrong.

## Recommendation

**Shadow, not serve, not drop** — high caveat level, pending the re-score. Details and
the full-run costing (66.6 h for all parks × all origins; the previous ~20 h estimate
is ~3× low) are in `docs/ml/foundation-models.md`.

## Test plan

- `python -m pytest ml-bench/tests` — green, including
  `test_pair_with_enables_a_model_against_model_comparison` (asserts the paired sums
  appear in the slot aggregate and that `report --vs` picks them up) and
  `test_unknown_park_id_is_refused`.
- `ruff check ml-bench`.
- The committed run and both report passes reproduce from
  `/data/parkfan/ml-bench/exports/20261009` with the commands in
  `docs/ml/foundation-models.md`.

No production code paths are touched: ml-bench is an offline tool and is not part of
docker-compose.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
