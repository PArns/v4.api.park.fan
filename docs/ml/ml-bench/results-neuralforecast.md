# NeuralForecast candidates — TiDE, NHITS, TSMixerx (PAR-829) and TiDE vs Chronos-2 (PAR-869)

Supervised global deep models with known-future covariates, the same library family as the
production TFT (`neuralforecast` pinned to nf-service's version), forecasting the planner's
hourly shape directly instead of a daily level. Open this page for what they scored and why none
is pursued; how they are built and precomputed is in
[running.md](running.md#neuralforecast-precompute-par-829).

**Verdict:** TiDE beats every incumbent (persistence, H5, TFT×H5) on several live and intraday
leads, in autumn and outside it — but loses every paired head to head against Chronos-2, which
already runs in the forward archive's shadow. Nothing from this family is in the model
comparison matrix or in production. TSMixerx is implemented and was never scored.

## Runs

| Run | Origins | Arms | Generated summaries (in the run dir) |
| --- | --- | --- | --- |
| `20261011-par829-stageA` | 61 autumn-shoulder origins 2026-08-08 … 10-07, all parks | `nf_tide`, `nf_nhits`, `nf_tide_wx` (ORACLE, scored `_owx`) | `summary_par829_handover.md`, `_verdict.md`, `_margin_tables.md`, `_coverage_census.md`, `_planner_audit.md` |
| `20261011-par829-xseason` | the other 170 origins 2026-02-19 … 08-07 (disjoint; together the 231-origin full period) | `nf_tide`, `nf_nhits` | `summary_par829_xseason_window.md` |
| `20261012-tide-vs-chronos` | 47 origins (`--shard 0/5`, all seven weekdays), 30-park subset | `chronos2`, `chronos2_grid`, `nf_tide` (read from PAR-829's cache, not recomputed) | `summary_tide_vs_chronos.md` |

Both PAR-829 runs share git `af043b17`, `code_sha256` `c29ab0ab…`, one image and export
`20261009`. The tables were produced by `par829_*.py` scripts from the runs' CSVs; nothing is
hand-transcribed.

## TiDE and NHITS against the incumbents (PAR-829)

Pre-registered rule (`par829_verdict.py`): SERVE needs a win over every incumbent with the same
sign on non-autumn origins; SHADOW a win over some; DROP on a significant regression. On autumn
origins alone SHADOW is the ceiling; `_owx` arms never get a recommendation.

- **Hand-over, autumn window:** a PAR-829 arm wins 34 of 274 cells — D1 next-best precision
  (+0.038 … +0.048 vs `snaive7`), D4 dayPeak ordering d0–d7 (+0.065 … +0.079 vs `wt_med`),
  D4 optimiser regret (`nf_tide_x_h5` d1/d3 −2.59 / −2.70, `nf_nhits_x_h5` d7), D5 first hour
  d3–d7, UC1 m090–m120 (busy m060–m120) and UC2 intraday h2–4 / h4–8. The planner slot curve
  d0–d7 is not won by any of them (`lvlh5_tft` and `h5` keep it).
- **Cross-season SERVE clause** (`summary_par829_xseason_window.md` §4): 12 cells are
  SERVE-ELIGIBLE — `nf_tide` all rides at m090, m105, m120, h2–4, h4–8 (e.g. h2–4 vs
  `lvlh5_tft` −0.641, vs `h5` −0.517, vs `persistence` −1.758, all significant, same sign as
  autumn), `nf_tide` busy at m060–m120 and h2–4, `nf_nhits` m105 (all and busy). `nf_nhits` at
  m120 flips sign outside autumn and stays SHADOW.
- `nf_nhits` regresses significantly against TFT×H5 on the intraday bands h2–h8+ (against H5
  too from h4) and against `persistence` at m015–m075.
- The models produce d0–d7 only (horizon 8 service days); at longer leads they are absent, not
  beaten. Month-block training makes weights up to 31 days stale at a block's end while profile
  baselines are recomputed per origin; `par829_staleness.py` reads MAE against
  `origin − block_start` rather than assuming a direction.

## TiDE vs Chronos-2, paired on identical rows (PAR-869)

PAR-829 and PAR-828 measured TiDE and Chronos-2 in different runs, and pairs exist only inside one
run, so `20261012-tide-vs-chronos` scores both with
`--pair-with chronos2,chronos2_grid,chronos2_x_h5,nf_tide,nf_tide_x_h5,h5,lvlh5_tft,persistence`
on the Chronos-2 mixed-weekday sample. A family is awarded a question × lead when one of its
variants wins the head to head and beats every incumbent scored there; "strict" means both report
passes agree (the headline window holds only 11 of the 47 origins, so it can confirm but never
carry a verdict).

- **Chronos-2 wins the head to head at every scored question × lead.** Awarded to Chronos-2:
  UC1 m030–m120 (`chronos2`, all and busy), UC2 rest of day h2–4 … h8+ (`chronos2`), planner
  today/tomorrow d0 (`chronos2_grid`), planner slot curve d1–d2 and d6 on all rides
  (`chronos2_grid`) and d1, d3–d4, d6–d7 on busy rides (`chronos2_x_h5`).
- **Nobody** clears the incumbents at m015 (persistence), planner d3–d5 and d7 on all rides, or
  D3 best time.
- Largest secured margins (full period, park-cluster CI): `chronos2` better than `nf_tide` by
  1.684 [1.391, 2.001] min at m015 (busy 2.985), 1.120 [0.827, 1.436] at h8+, `chronos2_grid` by
  0.844 [0.655, 1.036] on planner d0.

Helper scripts in `ml-bench/`: `tide_vs_chronos.py` (every table and the German answer from the
run's CSVs), `splice_results.py` (moves generated sections between marker pairs) and
`verify_cells_from_parts.py` (re-derives published cells from `parts/` without `report.py`, so a
wrong lead mapping or an empty arm shows up as a mismatch).

**Limit of that run:** D3 (best time), D4 and UC4 had no model-vs-model pairs — they were scored
only against the naive candidates. Since PAR-1043 the runner also writes ride-day sums against
the `--pair-with` models (D3 is pairable in new runs); the D4 ordering pairs and UC4 still pair
against the naive candidates only.
