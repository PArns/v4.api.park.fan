# Run `20261009-par830-drivers` — how to read these tables

Full analysis: `docs/ml/level-drivers.md`. Review trail: PR #447 and its critic
pass. This file exists because three of the generated tables cannot be read at
face value, and two claims first published from this run were wrong.

## Provenance

- `git_sha` `e0a4547aaa3c66bc93e89c20fb753a152dff95ac`, `code_sha256`
  `915dc7843c0b0afe910fd63f7c1ec4b23733f2325bbc30d5c5d2d2077122b0b3`, image
  `sha256:7b5e872f614f`, `--reference`, export `/data/exports/20261009`.
- 231 origins 2026-02-19 … 2026-10-07, 3 shards, 196.7 shard-minutes CPU, no GPU.
  2 078 parquet parts = 231 × 8 + 230 (the last origin has no lead ≥ 1 inside the
  truth window, so `optim` has 230).
- Two committed report passes:
  - `tables/` + `summary.md` — **all 231 origins. This is the pass that governs
    any serving decision.**
  - `tables_2026-08-15_2026-10-07/` + `summary_tables_2026-08-15_2026-10-07.md`
    — the 8-week common-target window (54 target days). Late summer + autumn
    only. Several results **reverse** between the two; see §6.7 of the doc.
- The branch head hashes to `c7f95e4a…`, not to the `915dc784…` above: commit
  `312eaf56` adds two env-var reads to `report.py` *after* this run. `report.py`
  is the only `mlbench` file that differs from the run commit at the head, and
  the two new defaults are the previous literals, so no number here moves.
  `pytest -q ml-bench/tests` at the head: 19 passed, run in this image.

## Three tables that must not be read at face value

1. **`usable_horizon.csv` and `handover.csv` are wrong for UC4.**
   `usable_horizon()` applies the ≥ 30-**origin-day** LOW-N gate to every metric,
   but UC4's unit is the park-**month** (`report.py:342`), so it tests nothing and
   prints `leads_tested = 0`; `handover.csv` then names the reference itself the
   winner with `significant_models = 0` at every UC4 lead. Both read like "no
   level source wins". They are **untested, not unbeaten** — the tautology the
   PR #445 critic raised as B4. **BENCH-SPEC recorded change #3** (binding) gates
   UC4 on **≥ 30 park-months**; this run has **607 park-months at d1** (full
   period) and **196** (window), every lead passing. Read UC4 off `cells.csv`:
   `n_park_days` is the park-month count, with `wins` and `diff_lo_park/hi_park`.
   Under the correct gate `lvl_driver_level` **wins contiguously d1–d60** and
   **loses at d120**. The code fix belongs to PAR-827.
2. **`lead_availability.csv` in the windowed pass is a verbatim copy of the
   full-period file** (231, 230, … origin days) rather than the windowed counts.
   The windowed counts are in `cells.csv`'s per-cell `n_origin_days` (54 for most
   cells). Do not quote `lead_availability.csv` out of a windowed pass. (The
   `not measurable yet` rows at d270/d365 in both summaries are legitimate — they
   are BENCH-SPEC's backtestable-limit reporting, and nothing to do with UC4.)
3. **`cells.csv`'s `wins` column is not safe for low-coverage models.**
   `evaluate()` implements neither half of **BENCH-SPEC recorded change #2** (≥ 10
   bootstrap units, strictly positive CI width). Live example in this run:
   `lvl_tft`, UC4, d60 — **1 park-month**, `paired_share = 0.003`, CI
   `[+0.4512, +0.4512]`, `wins = True`. Check `n_park_days` and the CI width
   before quoting any cell. `MIN_COVERAGE = 0.3` *is* applied in
   `usable_horizon`, but a raw `cells.csv` row does not enforce it.

## Two claims published from this run that were wrong, and are corrected

- **D4 `dayPeak` ordering is not a win for `driver_level`.** It is significant to
  d90, but `handover.csv` names `lvlh5_tft` the winner at d0–d45 and
  `lvlh5_naive` at d60–d90 (full period), and `driver_level`'s paired margin is
  **below `h5`'s at every lead d7–d90**. The d90 in `usable_horizon.csv` is a
  four-way tie (`h5`, `driver_level`, `driver_level_x_h5`, `lvlh5_naive`).
  `dayPeak` must not be routed to this model. What does survive on D4 is
  **optimiser regret**: `handover.csv` winner at all five scored leads with the
  largest margin of any model, on the same d30 horizon as `h5`.
- **UC4 is measurable** — see point 1 above. It was published as "not measurable,
  decidable ≈ 2027-04"; that read 3 and 6–9 month *labels* as sample sizes.

## Reference selection

The "vs reference" columns use the pre-PAR-827-fix unpaired argmin over
`snaive7`/`wt_med`/`clim` (critic B1). No conclusion in the doc rests on them:
the evidence is the **paired margin against each named baseline**, hand-computed
as a difference-in-differences through each lead's common reference.
`cells_all_refs.csv` (BENCH-SPEC change #5) does not exist in either pass — the
DiD grid in the doc is its stand-in, and it is exact only where the two models'
coverage coincides (92–96 % over the full period).

## Season

`mae_by_season.csv` is unpaired (each model on its own coverage): read the
direction, not the decimals. Its **d1 row is also an origin-season split**, since
at d1 the origin is the target minus one day — which is what settles
season-vs-cold-start without a re-score (doc §6.7, point 2).
