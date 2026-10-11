# ml-bench results

Run: `20261010-par828-subset-chronos-v2` · generated 2026-10-11T12:07+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with TARGET season. The common-window pass mixes lead with ORIGIN season instead. Both are confounded; neither alone identifies the horizon curve.

- code that produced `parts/` (the scoring): git `cdbc95a36c835be3f3cb74710f724562b238321f`, sources sha256 `d5acedf61efb1a56`, image `sha256:5f68f9cc2254`; plug-ins ['chronos2', 'chronos2_grid', 'chronos2_nocov', 'chronos2_owx']
- code that produced THESE tables (the report): git `3107d7c9728aa1d54615208d6fb69638c3d34d3e` (from env), sources sha256 `4b2a41ce3dccbc88`, image `unknown` — recorded in `report-tables.json`
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 94.3 shard-minutes over 1 shard(s)
- Values: point [95 % park-day bootstrap, 1000 reps]. Paired difference vs the per-lead reference: point [95 % **park-cluster** bootstrap] — `*` = the model wins (that CI excludes 0 in its favour). Park-day CIs of the differences are in `cells.csv`.
- **The win criterion is the park-CLUSTER bootstrap, not BENCH-SPEC's literal park-day unit.** Days of one park are not independent, so the park-day CI is too narrow; the deviation is recorded in BENCH-SPEC.md. Both CIs are in `cells.csv` (`diff_lo/hi` = park-day, `diff_lo_park/hi_park` = park-cluster) and in the `… vs each naive` tables below, where `!` marks a cell on which the two units disagree about the win.
- **A win also needs ≥ 10 distinct bootstrap units and a strictly positive CI width.** With one unit the resampled ratio is weight-independent, so the CI collapses to zero width and excludes 0 for free.
- **A win means the model beats EVERY naive candidate at that lead** (the "naive envelope"), each paired on its own shared rows. Not "beats the selected reference": a single selected reference is a choice made on the data, and every rule for making it errs in one direction — an argmin over the candidates' own-coverage values picks on noise and understates models (their gaps here are an order of magnitude inside the CI, so the reference flipped along the lead axis), a ladder tie-break picks the simplest candidate and overstates them. A conjunction over a FIXED candidate set selects nothing and is strictly harder than beating any single member. `handover.csv` reports the margin against the hardest candidate (`hardest_naive`), and winners are ranked by it.
- **The `ref` column is a DISPLAY reference only**: BENCH-SPEC's ladder (persistence → seasonal-naive → weekday-median → climatology), overridden only when another candidate beats it in a paired comparison with the park-cluster CI excluding 0. `ref_choice.csv` records the candidates, their values and why each one was chosen. `cells.csv` keeps `wins_vs_ref` for that single column.
- `cells_all_refs.csv` has every (model, reference-candidate) pair, so "no model beats the best of the naives" and "against `wt_med` the model is X better" can be read separately. `low_coverage_cells.csv` lists every cell whose paired rows are < 50 % of the reference's.
- **A model scored `<name>_owx` used ORACLE weather** (the target day's actuals; no forecast archive exists). It appears in the value and difference tables as an upper bound and is marked NOT SERVABLE, but it is excluded from the hand-over table and the usable horizon — those are the serving router's input.
- MAE is in minutes on 15-min slots, each model on its own coverage — compare models through the paired difference, never through two unpaired values.
- Opening-aligned forecasts use the window KNOWN at the origin: the published one if its schedule row was last written before the origin, else a projection from the last 56 days. Weather: no forecast archive exists; baselines use none, plug-ins only by opting in (scored as `<name>_owx`).

## Horizon — the headline

### Lead availability (this pass)

| lead | origin_days | status | first_target_day | last_target_day | measurable_from |
|---|---|---|---|---|---|
| 0 | 231 | ok | 2026-02-19 | 2026-10-07 |  |
| 1 | 230 | ok | 2026-02-20 | 2026-10-07 |  |
| 2 | 229 | ok | 2026-02-21 | 2026-10-07 |  |
| 3 | 228 | ok | 2026-02-22 | 2026-10-07 |  |
| 4 | 227 | ok | 2026-02-23 | 2026-10-07 |  |
| 5 | 226 | ok | 2026-02-24 | 2026-10-07 |  |
| 6 | 225 | ok | 2026-02-25 | 2026-10-07 |  |
| 7 | 224 | ok | 2026-02-26 | 2026-10-07 |  |
| 10 | 221 | ok | 2026-03-01 | 2026-10-07 |  |
| 14 | 217 | ok | 2026-03-05 | 2026-10-07 |  |
| 21 | 210 | ok | 2026-03-12 | 2026-10-07 |  |
| 30 | 201 | ok | 2026-03-21 | 2026-10-07 |  |
| 45 | 186 | ok | 2026-04-05 | 2026-10-07 |  |
| 60 | 171 | ok | 2026-04-20 | 2026-10-07 |  |
| 90 | 141 | ok | 2026-05-20 | 2026-10-07 |  |
| 120 | 111 | ok | 2026-06-19 | 2026-10-07 |  |
| 180 | 51 | ok | 2026-08-18 | 2026-10-07 |  |
| 270 | 0 | not measurable yet |  |  | 2026-12-15 |
| 365 | 0 | not measurable yet |  |  | 2027-03-20 |

### Coverage horizon of the inputs

| input | horizon_days | note |
|---|---|---|
| TFT daily level | 60 | last run 2026-10-08 00:00:00; served only if <= 3 days stale |
| CatBoost daily level | 45 | last run 2026-10-09 00:00:00; served only if <= 3 days stale |
| weather forecast (Open-Meteo) | 16 | rows 2026-10-10 00:00:00..2026-10-24 00:00:00 only; NO archive -> only actuals exist (ORACLE, stripped unless a plug-in opts in) |
| published operator schedule (per park) | 39 | p10 -32 d, median 39 d, p90 155 d past the last truth day; a window last written after the origin is treated as unknown and projected (schedule_entries.updatedAt) |

### Usable horizon

Largest lead, contiguous from the model's first scored lead, at which the model beats EVERY naive candidate (park-cluster CI, ≥ 10 bootstrap units); cells need ≥ 30 origin days (≥ 30 park-months for UC4) and paired rows ≥ 30 % of each candidate's. Empty = never.

| metric | uc | segment | model | is_reference_candidate | unit | leads_scored | leads_tested | usable_horizon | max_significant_lead | not_tested_because |
|---|---|---|---|---|---|---|---|---|---|---|
| MAE | UC1 | all | chronos2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | all | persistence | True | park-day | 8 | 8 | m090 | m090 |  |
| MAE | UC1 | busy | chronos2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | persistence | True | park-day | 8 | 8 | m090 | m090 |  |
| MAE | UC2 | all | chronos2 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | chronos2_grid | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | chronos2_nocov | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | all | chronos2_x_h5 | False | park-day | 1 | 1 | 1 | 1 |  |
| MAE | UC2 | all | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | chronos2 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | chronos2_grid | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | chronos2_nocov | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | busy | chronos2_x_h5 | False | park-day | 1 | 1 | 1 | 1 |  |
| MAE | UC2 | busy | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2-intraday | all | chronos2 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | h5 | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | all | wt_med | True | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | chronos2 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | h5 | False | park-day | 3 | 3 |  | h8+ |  |
| MAE | UC2-intraday | busy | wt_med | True | park-day | 3 | 3 |  | h8+ |  |
| MAE | UC3 | all | chronos2 | False | park-day | 7 | 7 | 1 | 1 |  |
| MAE | UC3 | all | chronos2_grid | False | park-day | 7 | 7 | 1 | 7 |  |
| MAE | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 1 | 7 |  |
| MAE | UC3 | all | h5 | False | park-day | 14 | 10 | 21 | 21 |  |
| MAE | UC3 | all | wt_med | True | park-day | 14 | 10 | 14 | 14 |  |
| MAE | UC3 | busy | chronos2 | False | park-day | 7 | 7 | 1 | 6 |  |
| MAE | UC3 | busy | chronos2_grid | False | park-day | 7 | 7 | 1 | 7 |  |
| MAE | UC3 | busy | chronos2_x_h5 | False | park-day | 14 | 10 | 1 | 21 |  |
| MAE | UC3 | busy | h5 | False | park-day | 14 | 10 | 21 | 21 |  |
| MAE | UC3 | busy | lvlh5_naive | False | park-day | 14 | 10 |  | 10 |  |
| MAE | UC3 | busy | wt_med | True | park-day | 14 | 10 | 14 | 14 |  |
| MAE, schedule known at origin | UC3 | all | chronos2 | False | park-day | 8 | 8 | 1 | 6 |  |
| MAE, schedule known at origin | UC3 | all | chronos2_grid | False | park-day | 8 | 8 | 1 | 7 |  |
| MAE, schedule known at origin | UC3 | all | chronos2_nocov | False | park-day | 8 | 8 | 0 | 0 |  |
| MAE, schedule known at origin | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 1 | 14 |  |
| MAE, schedule known at origin | UC3 | all | h5 | False | park-day | 15 | 11 | 1 | 21 |  |
| MAE, schedule known at origin | UC3 | all | wt_med | True | park-day | 15 | 11 | 10 | 10 |  |
| MAE, schedule projected at origin | UC3 | all | chronos2 | False | park-day | 8 | 8 | 1 | 1 |  |
| MAE, schedule projected at origin | UC3 | all | chronos2_grid | False | park-day | 8 | 8 | 1 | 1 |  |
| MAE, schedule projected at origin | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 1 | 6 |  |
| MAE, schedule projected at origin | UC3 | all | h5 | False | park-day | 15 | 11 |  | 10 |  |
| MAE, schedule projected at origin | UC3 | all | wt_med | True | park-day | 15 | 11 | 14 | 14 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | chronos2_grid | False | park-day | 8 | 8 | 1 | 6 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | chronos2_x_h5 | False | park-day | 14 | 10 |  | 4 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | False | park-day | 15 | 11 |  | 21 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | False | park-day | 15 | 11 |  | 4 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | True | park-day | 15 | 11 | 7 | 14 |  |
| best-time hit rate (top-2) | D3 | all | h5 | False | park-day | 15 | 11 |  | 7 |  |
| best-time hit rate (top-2) | D3 | all | wt_med | True | park-day | 15 | 11 | 2 | 10 |  |
| best-time hit rate (±30 min) | D3 | all | h5 | False | park-day | 15 | 11 | 1 | 7 |  |
| best-time hit rate (±30 min) | D3 | all | wt_med | True | park-day | 15 | 11 |  | 4 |  |
| best-time regret (min) | D3 | all | h5 | False | park-day | 15 | 11 | 21 | 21 |  |
| best-time regret (min) | D3 | all | wt_med | True | park-day | 15 | 11 |  | 5 |  |
| crowd bucket exact | D6 | all | lvl_chronos2 | False | park-day | 16 | 10 | 1 | 7 |  |
| crowd bucket exact | D6 | all | lvl_tft | False | park-day | 12 | 10 |  | 7 |  |
| day comparison winner accuracy | D7 | all | lvl_naive4 | True | park-origin | 3 | 2 |  | d8-30 |  |
| day comparison winner accuracy | D7 | all | lvl_snaive7 | True | park-origin | 1 | 1 | d1-7 | d1-7 |  |
| dayPeak Spearman across rides | UC3 | all | wt_med | True | park-day | 15 | 11 |  | 14 |  |
| dayPeak abs error (min) | UC3 | all | chronos2 | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | chronos2_grid | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 1 | 7 |  |
| dayPeak abs error (min) | UC3 | all | wt_med | True | park-day | 15 | 11 | 1 | 10 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2 | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_grid | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_nocov | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 21 | 21 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | False | park-day | 15 | 11 | 21 | 21 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | False | park-day | 15 | 11 | 21 | 21 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | True | park-day | 15 | 11 | 2 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | chronos2_grid | False | park-day | 8 | 8 | 1 | 1 |  |
| first-hour MAE (opening-aligned) | D5 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | h5 | False | park-day | 15 | 11 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | False | park-day | 13 | 11 |  | 14 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | False | park-day | 15 | 11 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | False | park-day | 14 | 11 | 3 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | wt_med | True | park-day | 15 | 11 | 5 | 10 |  |
| first-hour MAE, schedule known at origin | D5 | all | chronos2_grid | False | park-day | 8 | 8 | 0 | 0 |  |
| first-hour MAE, schedule known at origin | D5 | all | chronos2_x_h5 | False | park-day | 14 | 10 | 21 | 21 |  |
| first-hour MAE, schedule known at origin | D5 | all | h5 | False | park-day | 15 | 11 | 21 | 21 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | False | park-day | 13 | 11 |  | 14 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 11 | 21 | 21 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | False | park-day | 14 | 11 | 3 | 21 |  |
| first-hour MAE, schedule known at origin | D5 | all | wt_med | True | park-day | 15 | 11 | 5 | 10 |  |
| first-hour MAE, schedule projected at origin | D5 | all | wt_med | True | park-day | 15 | 11 | 2 | 7 |  |
| live-window MAE (headliners) | D2 | headliners | chronos2 | False | park-day | 1 | 1 | w0-45 | w0-45 |  |
| live-window MAE (headliners) | D2 | headliners | persistence | True | park-day | 1 | 1 | w0-45 | w0-45 |  |
| next-best precision | D1 | all | h5 | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision | D1 | all | lvlh5_tft | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision | D1 | all | wt_med | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | chronos2 | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | wt_med | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best recall | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| optimiser regret (min per park-day) | D4 | all | chronos2_x_h5 | False | park-day | 5 | 4 |  | 7 |  |


**Never tested** — `leads_tested = 0` means no lead passed the gates, which is NOT "nothing won". The reason per metric × model:

| metric | uc | segment | model | unit | leads_scored | not_tested_because |
|---|---|---|---|---|---|---|
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| best-time regret (min) | D3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| best-time regret (min) | D3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| best-time regret (min) | D3 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| best-time regret (min) | D3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | park-day | 13 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served | park-day | 10 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served_lin | park-day | 10 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | snaive7 | park-day | 7 | 0: coverage 0.26 < 0.30 against one of the candidates; 1: coverage 0.26 < 0.30 against one of the candidates; 2: coverage 0.29 < 0.30 against one of the candidates; 3: coverage 0.28 < 0.30 against one of the candidates; 4: coverage 0.27 < 0.30 against one of the candidates; 5: coverage 0.29 < 0.30 against one of the candidates |
| optimiser regret (min per park-day) | D4 | all | lvlh5_cbd | park-day | 5 | 1: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 7: LOW-N: 17 park-days < 30; 14: LOW-N: 16 park-days < 30; 30: LOW-N: 14 park-days < 30 |
| optimiser regret (min per park-day) | D4 | all | lvlh5_tft | park-day | 5 | 1: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 14: LOW-N: 17 park-days < 30; 30: LOW-N: 13 park-days < 30 |
| optimiser regret (min per park-day) | D4 | all | prod_served | park-day | 4 | 3: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 14: LOW-N: 17 park-days < 30; 30: LOW-N: 14 park-days < 30 |
| optimiser regret (min per park-day) | D4 | all | prod_served_lin | park-day | 4 | 3: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 14: LOW-N: 17 park-days < 30; 30: LOW-N: 14 park-days < 30 |
| rope-drop worth agreement | D5 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 18 park-days < 30; 1: LOW-N: 18 park-days < 30; 2: LOW-N: 18 park-days < 30; 3: LOW-N: 18 park-days < 30; 4: LOW-N: 18 park-days < 30; 5: LOW-N: 18 park-days < 30 |
| rope-drop worth agreement | D5 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 19 park-days < 30; 1: LOW-N: 19 park-days < 30; 2: LOW-N: 19 park-days < 30; 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30 |
| rope-drop worth agreement | D5 | all | prod_served | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |
| rope-drop worth agreement | D5 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 19 park-days < 30; 4: LOW-N: 19 park-days < 30; 5: LOW-N: 19 park-days < 30; 6: LOW-N: 19 park-days < 30; 7: LOW-N: 18 park-days < 30; 10: LOW-N: 18 park-days < 30 |

### Reference choice per cell

Candidates (coverage ≥ 50 % of the best-covered), their own-coverage values, and why the reference is the one it is. `reason` says `ladder order` when no candidate significantly beats the ladder-first one — i.e. the choice was NOT made on the point estimates.

| metric | uc | segment | lead | candidates | selected | reason |
|---|---|---|---|---|---|---|
| MAE | UC2 | all | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2 | busy | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 7 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 7 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 10 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 10 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 14 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 14 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 21 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 21 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 30 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 30 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 45 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 45 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 60 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 60 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 90 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 90 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 7 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 10 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 14 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 21 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 30 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 45 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 60 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 90 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m015 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m015 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m030 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m030 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m045 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m045 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m060 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m060 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m075 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m075 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m090 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m090 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m105 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m105 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | all | m120 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC1 | busy | m120 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC2-intraday | all | h2-4 | persistence/snaive7/wt_med/clim | wt_med | persistence is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h2-4 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC2-intraday | all | h4-8 | persistence/snaive7/wt_med/clim | wt_med | persistence is the ladder-first candidate but is significantly beaten by snaive7/wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h4-8 | persistence/snaive7/wt_med/clim | wt_med | persistence is the ladder-first candidate but is significantly beaten by snaive7/wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | all | h8+ | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h8+ | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2 | all | 0 | chronos2 | chronos2 | only candidate |
| MAE | UC2 | busy | 0 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 1 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 1 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 2 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 2 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 3 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 3 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 4 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 4 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 5 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 5 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 6 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 6 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | all | 7 | chronos2 | chronos2 | only candidate |
| MAE | UC3 | busy | 7 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m015 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m015 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m030 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m030 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m045 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m045 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m060 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m060 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m075 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m075 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m090 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m090 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m105 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m105 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | all | m120 | chronos2 | chronos2 | only candidate |
| MAE | UC1 | busy | m120 | chronos2 | chronos2 | only candidate |
| MAE | UC2-intraday | all | h2-4 | chronos2 | chronos2 | only candidate |
| MAE | UC2-intraday | busy | h2-4 | chronos2 | chronos2 | only candidate |
| MAE | UC2-intraday | all | h4-8 | chronos2 | chronos2 | only candidate |
| MAE | UC2-intraday | busy | h4-8 | chronos2 | chronos2 | only candidate |
| MAE | UC2-intraday | all | h8+ | chronos2 | chronos2 | only candidate |
| MAE | UC2-intraday | busy | h8+ | chronos2 | chronos2 | only candidate |
| MAE | UC2 | all | 0 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC2 | busy | 0 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 1 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 1 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 2 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 2 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 3 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 3 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 4 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 4 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 5 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 5 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 6 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 6 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | all | 7 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC3 | busy | 7 | chronos2_grid | chronos2_grid | only candidate |
| MAE | UC2 | all | 0 | h5 | h5 | only candidate |
| MAE | UC2 | busy | 0 | h5 | h5 | only candidate |
| MAE | UC3 | all | 1 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 1 | h5 | h5 | only candidate |
| MAE | UC3 | all | 2 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 2 | h5 | h5 | only candidate |
| MAE | UC3 | all | 3 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 3 | h5 | h5 | only candidate |
| MAE | UC3 | all | 4 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 4 | h5 | h5 | only candidate |
| MAE | UC3 | all | 5 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 5 | h5 | h5 | only candidate |
| MAE | UC3 | all | 6 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 6 | h5 | h5 | only candidate |
| MAE | UC3 | all | 7 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 7 | h5 | h5 | only candidate |
| MAE | UC3 | all | 10 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 10 | h5 | h5 | only candidate |
| MAE | UC3 | all | 14 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 14 | h5 | h5 | only candidate |
| MAE | UC3 | all | 21 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 21 | h5 | h5 | only candidate |
| MAE | UC3 | all | 30 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 30 | h5 | h5 | only candidate |
| MAE | UC3 | all | 45 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 45 | h5 | h5 | only candidate |
| MAE | UC3 | all | 60 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 60 | h5 | h5 | only candidate |
| MAE | UC3 | all | 90 | h5 | h5 | only candidate |
| MAE | UC3 | busy | 90 | h5 | h5 | only candidate |
| MAE | UC1 | all | m015 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m015 | h5 | h5 | only candidate |
| MAE | UC1 | all | m030 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m030 | h5 | h5 | only candidate |
| MAE | UC1 | all | m045 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m045 | h5 | h5 | only candidate |
| MAE | UC1 | all | m060 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m060 | h5 | h5 | only candidate |
| MAE | UC1 | all | m075 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m075 | h5 | h5 | only candidate |
| MAE | UC1 | all | m090 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m090 | h5 | h5 | only candidate |
| MAE | UC1 | all | m105 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m105 | h5 | h5 | only candidate |
| MAE | UC1 | all | m120 | h5 | h5 | only candidate |
| MAE | UC1 | busy | m120 | h5 | h5 | only candidate |
| MAE | UC2-intraday | all | h2-4 | h5 | h5 | only candidate |
| MAE | UC2-intraday | busy | h2-4 | h5 | h5 | only candidate |
| MAE | UC2-intraday | all | h4-8 | h5 | h5 | only candidate |
| MAE | UC2-intraday | busy | h4-8 | h5 | h5 | only candidate |
| MAE | UC2-intraday | all | h8+ | h5 | h5 | only candidate |
| MAE | UC2-intraday | busy | h8+ | h5 | h5 | only candidate |
| MAE | UC2 | all | 0 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2 | busy | 0 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 1 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 1 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 2 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 2 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 3 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 3 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 4 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 4 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 5 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 5 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 6 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 6 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 7 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 7 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 10 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 10 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 14 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 14 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 21 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 21 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 30 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 30 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 45 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | busy | 45 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC3 | all | 60 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m015 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m015 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m030 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m030 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m045 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m045 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m060 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m060 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m075 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m075 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m090 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m090 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m105 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m105 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m120 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | busy | m120 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2-intraday | all | h2-4 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2-intraday | busy | h2-4 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2-intraday | all | h4-8 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2-intraday | busy | h4-8 | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2-intraday | all | h8+ | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC2-intraday | busy | h8+ | lvlh5_tft | lvlh5_tft | only candidate |
| MAE | UC1 | all | m015 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m015 | persistence | persistence | only candidate |
| MAE | UC1 | all | m030 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m030 | persistence | persistence | only candidate |
| MAE | UC1 | all | m045 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m045 | persistence | persistence | only candidate |
| MAE | UC1 | all | m060 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m060 | persistence | persistence | only candidate |
| MAE | UC1 | all | m075 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m075 | persistence | persistence | only candidate |
| MAE | UC1 | all | m090 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m090 | persistence | persistence | only candidate |
| MAE | UC1 | all | m105 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m105 | persistence | persistence | only candidate |
| MAE | UC1 | all | m120 | persistence | persistence | only candidate |
| MAE | UC1 | busy | m120 | persistence | persistence | only candidate |
| MAE | UC2-intraday | all | h2-4 | persistence | persistence | only candidate |
| MAE | UC2-intraday | busy | h2-4 | persistence | persistence | only candidate |
| MAE | UC2-intraday | all | h4-8 | persistence | persistence | only candidate |
| MAE | UC2-intraday | busy | h4-8 | persistence | persistence | only candidate |
| MAE | UC2-intraday | all | h8+ | persistence | persistence | only candidate |
| MAE | UC2-intraday | busy | h8+ | persistence | persistence | only candidate |

### Hand-over table (input for the serving router)

Per use case × lead: among the models that beat EVERY naive candidate (and pass the gates), the largest margin against the hardest candidate; otherwise the reference itself. The naive candidates themselves are excluded from the winner column even when one of them beats the others, because their margin is measured against a different and weaker pool — `usable_horizon.csv` keeps those rows, flagged `is_reference_candidate`. `park_day_unit_agrees = False` marks a row whose verdict changes under BENCH-SPEC's literal park-day bootstrap.

| uc | segment | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested | n_origin_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | wt_med | chronos2_grid | 5.995 | -1.023 | -1.294 | -0.783 | -1.200 | -0.838 | True | 1.000 | 4 | 10 | 33 |
| UC2 | busy | 0 | wt_med | chronos2_grid | 11.691 | -2.466 | -3.083 | -1.916 | -2.903 | -2.006 | True | 1.000 | 4 | 10 | 33 |
| UC3 | all | 1 | wt_med | chronos2_grid | 6.470 | -0.624 | -0.861 | -0.392 | -0.793 | -0.480 | True | 1.000 | 4 | 11 | 33 |
| UC3 | busy | 1 | wt_med | chronos2_grid | 12.720 | -1.442 | -2.093 | -0.876 | -1.900 | -1.050 | True | 1.000 | 4 | 11 | 33 |
| UC3 | all | 2 | wt_med | h5 | 7.032 | -0.033 | -0.071 | -0.004 | -0.046 | -0.019 | True | 0.995 | 1 | 11 | 33 |
| UC3 | busy | 2 | wt_med | h5 | 13.679 | -0.128 | -0.218 | -0.059 | -0.164 | -0.096 | True | 0.997 | 1 | 11 | 33 |
| UC3 | all | 3 | wt_med | h5 | 6.808 | -0.099 | -0.134 | -0.069 | -0.115 | -0.083 | True | 0.995 | 1 | 13 | 33 |
| UC3 | busy | 3 | wt_med | chronos2_x_h5 | 13.728 | -0.641 | -0.973 | -0.289 | -0.933 | -0.344 | True | 0.996 | 3 | 13 | 33 |
| UC3 | all | 4 | wt_med | chronos2_grid | 7.198 | -0.425 | -0.664 | -0.229 | -0.602 | -0.249 | True | 1.000 | 3 | 13 | 33 |
| UC3 | busy | 4 | wt_med | chronos2_x_h5 | 14.012 | -1.121 | -1.534 | -0.771 | -1.425 | -0.838 | True | 0.999 | 4 | 13 | 33 |
| UC3 | all | 5 | wt_med | chronos2_grid | 7.139 | -0.431 | -0.758 | -0.179 | -0.614 | -0.243 | True | 1.000 | 3 | 13 | 33 |
| UC3 | busy | 5 | wt_med | chronos2_grid | 13.940 | -1.040 | -1.917 | -0.422 | -1.522 | -0.571 | True | 1.000 | 3 | 13 | 33 |
| UC3 | all | 6 | wt_med | chronos2_grid | 6.831 | -0.437 | -0.694 | -0.236 | -0.630 | -0.249 | True | 1.000 | 3 | 13 | 33 |
| UC3 | busy | 6 | wt_med | chronos2_grid | 13.693 | -1.167 | -1.903 | -0.676 | -1.666 | -0.645 | True | 1.000 | 5 | 13 | 33 |
| UC3 | all | 7 | wt_med | chronos2_grid | 7.235 | -0.230 | -0.499 | -0.013 | -0.410 | -0.064 | True | 1.000 | 3 | 12 | 32 |
| UC3 | busy | 7 | wt_med | chronos2_x_h5 | 14.162 | -0.838 | -1.402 | -0.452 | -1.153 | -0.526 | True | 0.998 | 3 | 12 | 32 |
| UC3 | all | 10 | wt_med | h5 | 6.964 | -0.113 | -0.154 | -0.080 | -0.130 | -0.098 | True | 0.997 | 1 | 9 | 32 |
| UC3 | busy | 10 | wt_med | lvlh5_naive | 13.930 | -0.651 | -1.041 | -0.295 | -0.919 | -0.399 | True | 0.983 | 3 | 9 | 32 |
| UC3 | all | 14 | wt_med | h5 | 7.690 | -0.079 | -0.123 | -0.046 | -0.100 | -0.059 | True | 0.996 | 1 | 9 | 31 |
| UC3 | busy | 14 | wt_med | chronos2_x_h5 | 14.998 | -0.608 | -1.158 | -0.225 | -0.894 | -0.325 | True | 0.997 | 2 | 9 | 31 |
| UC3 | all | 21 | wt_med | h5 | 7.831 | -0.086 | -0.134 | -0.051 | -0.106 | -0.067 | True | 0.994 | 1 | 9 | 30 |
| UC3 | busy | 21 | wt_med | chronos2_x_h5 | 15.352 | -0.481 | -1.097 | -0.041 | -0.806 | -0.186 | True | 0.996 | 2 | 9 | 30 |
| UC3 | all | 30 | wt_med | wt_med | 7.762 | 0.000 |  |  |  |  | True |  | 0 | 0 | 29 |
| UC3 | busy | 30 | wt_med | wt_med | 14.743 | 0.000 |  |  |  |  | True |  | 0 | 0 | 29 |
| UC3 | all | 45 | wt_med | wt_med | 7.803 | 0.000 |  |  |  |  | True |  | 0 | 0 | 27 |
| UC3 | busy | 45 | wt_med | wt_med | 15.979 | 0.000 |  |  |  |  | True |  | 0 | 0 | 27 |
| UC3 | all | 60 | wt_med | wt_med | 8.335 | 0.000 |  |  |  |  | True |  | 0 | 0 | 25 |
| UC3 | busy | 60 | wt_med | wt_med | 16.181 | 0.000 |  |  |  |  | True |  | 0 | 0 | 25 |
| UC3 | all | 90 | wt_med | wt_med | 8.348 | 0.000 |  |  |  |  | True |  | 0 | 0 | 21 |
| UC3 | busy | 90 | wt_med | wt_med | 16.757 | 0.000 |  |  |  |  | True |  | 0 | 0 | 21 |
| UC2 | all | 1 | wt_med | chronos2_grid | 6.470 | -0.624 | -0.861 | -0.392 | -0.793 | -0.480 | True | 1.000 | 4 | 11 | 33 |
| UC2 | busy | 1 | wt_med | chronos2_grid | 12.720 | -1.442 | -2.093 | -0.876 | -1.900 | -1.050 | True | 1.000 | 4 | 11 | 33 |
| UC1 | all | m015 | persistence | persistence | 2.641 | 0.000 |  |  |  |  | False |  | 0 | 8 | 33 |
| UC1 | busy | m015 | persistence | persistence | 5.179 | 0.000 |  |  |  |  | False |  | 0 | 8 | 33 |
| UC1 | all | m030 | persistence | chronos2 | 3.860 | -0.416 | -0.610 | -0.212 | -0.475 | -0.357 | True | 0.998 | 1 | 8 | 33 |
| UC1 | busy | m030 | persistence | chronos2 | 7.609 | -0.917 | -1.282 | -0.493 | -1.044 | -0.785 | True | 0.998 | 1 | 8 | 33 |
| UC1 | all | m045 | persistence | chronos2 | 4.244 | -0.902 | -1.168 | -0.617 | -0.977 | -0.820 | True | 0.997 | 1 | 8 | 33 |
| UC1 | busy | m045 | persistence | chronos2 | 8.268 | -1.866 | -2.269 | -1.403 | -2.026 | -1.702 | True | 0.997 | 1 | 8 | 33 |
| UC1 | all | m060 | persistence | chronos2 | 4.491 | -1.262 | -1.597 | -0.927 | -1.363 | -1.162 | True | 0.996 | 1 | 8 | 33 |
| UC1 | busy | m060 | persistence | chronos2 | 8.737 | -2.617 | -3.124 | -2.100 | -2.807 | -2.422 | True | 0.997 | 1 | 8 | 33 |
| UC1 | all | m075 | persistence | chronos2 | 4.721 | -1.557 | -1.963 | -1.153 | -1.677 | -1.445 | True | 0.998 | 1 | 8 | 33 |
| UC1 | busy | m075 | persistence | chronos2 | 9.104 | -3.067 | -3.791 | -2.397 | -3.324 | -2.813 | True | 0.997 | 1 | 8 | 33 |
| UC1 | all | m090 | persistence | chronos2 | 4.841 | -1.882 | -2.386 | -1.396 | -2.023 | -1.749 | True | 0.998 | 1 | 8 | 33 |
| UC1 | busy | m090 | persistence | chronos2 | 9.370 | -3.755 | -4.655 | -2.898 | -4.070 | -3.465 | True | 0.997 | 1 | 8 | 33 |
| UC1 | all | m105 | persistence | chronos2 | 4.948 | -2.062 | -2.639 | -1.563 | -2.205 | -1.923 | True | 0.998 | 1 | 8 | 33 |
| UC1 | busy | m105 | persistence | chronos2 | 9.545 | -4.061 | -5.116 | -3.175 | -4.358 | -3.772 | True | 0.996 | 1 | 8 | 33 |
| UC1 | all | m120 | persistence | chronos2 | 5.024 | -2.268 | -2.920 | -1.714 | -2.415 | -2.121 | True | 0.997 | 1 | 8 | 33 |
| UC1 | busy | m120 | persistence | chronos2 | 9.685 | -4.511 | -5.727 | -3.541 | -4.826 | -4.200 | True | 0.996 | 1 | 8 | 33 |
| UC2-intraday | all | h2-4 | wt_med | chronos2 | 5.322 | -1.763 | -2.210 | -1.321 | -1.944 | -1.585 | True | 0.994 | 2 | 8 | 33 |
| UC2-intraday | busy | h2-4 | persistence | chronos2 | 10.220 | -5.837 | -7.213 | -4.687 | -6.184 | -5.489 | True | 0.994 | 1 | 8 | 33 |
| UC2-intraday | all | h4-8 | wt_med | chronos2 | 5.803 | -1.300 | -1.640 | -0.950 | -1.468 | -1.137 | True | 0.990 | 2 | 8 | 33 |
| UC2-intraday | busy | h4-8 | wt_med | chronos2 | 10.979 | -3.081 | -3.901 | -2.292 | -3.467 | -2.699 | True | 0.990 | 2 | 8 | 33 |
| UC2-intraday | all | h8+ | wt_med | chronos2 | 5.977 | -0.518 | -0.750 | -0.317 | -0.671 | -0.376 | True | 1.000 | 1 | 7 | 33 |
| UC2-intraday | busy | h8+ | wt_med | chronos2 | 11.105 | -1.350 | -1.812 | -0.955 | -1.682 | -1.007 | True | 1.000 | 2 | 8 | 33 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | chronos2_grid | 4.178 | -0.551 | -0.857 | -0.289 | -0.714 | -0.388 | True | 1.000 | 4 | 10 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | lvlh5_tft | 4.098 | -0.362 | -0.826 | -0.025 | -0.535 | -0.205 | True | 0.612 | 5 | 11 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | lvlh5_tft | 3.996 | -0.170 | -0.505 | -0.012 | -0.263 | -0.084 | True | 0.599 | 5 | 11 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | lvlh5_cbd | 4.250 | -0.192 | -0.401 | -0.025 | -0.336 | -0.073 | True | 0.482 | 5 | 13 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | chronos2_x_h5 | 4.756 | -0.143 | -0.344 | -0.018 | -0.227 | -0.064 | True | 0.999 | 3 | 13 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | chronos2_x_h5 | 4.859 | -0.125 | -0.289 | -0.015 | -0.200 | -0.049 | True | 0.998 | 3 | 13 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | lvlh5_tft | 4.365 | -0.182 | -0.486 | -0.001 | -0.307 | -0.080 | True | 0.598 | 5 | 13 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | lvlh5_tft | 4.360 | -0.242 | -0.572 | -0.011 | -0.381 | -0.128 | True | 0.604 | 5 | 12 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | lvlh5_cbd | 4.405 | -0.231 | -0.488 | -0.037 | -0.366 | -0.118 | True | 0.486 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | lvlh5_tft | 4.534 | -0.266 | -0.603 | -0.028 | -0.413 | -0.150 | True | 0.581 | 5 | 9 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | lvlh5_tft | 4.715 | -0.233 | -0.521 | -0.027 | -0.366 | -0.132 | True | 0.558 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | wt_med | 4.743 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | wt_med | 5.131 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE (opening-aligned) | D5 | 60 | wt_med | wt_med | 5.222 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE (opening-aligned) | D5 | 90 | wt_med | wt_med | 5.273 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | chronos2_grid | 6.007 | -1.135 | -1.423 | -0.864 | -1.335 | -0.910 | True | 1.000 | 4 | 10 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | chronos2_grid | 6.535 | -0.598 | -0.907 | -0.320 | -0.809 | -0.412 | True | 1.000 | 4 | 11 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | wt_med | 7.247 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 7.047 | -0.097 | -0.141 | -0.060 | -0.118 | -0.077 | True | 0.995 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | chronos2_grid | 7.224 | -0.496 | -0.795 | -0.244 | -0.712 | -0.271 | True | 1.000 | 4 | 13 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | chronos2_grid | 7.181 | -0.503 | -0.879 | -0.181 | -0.726 | -0.269 | True | 1.000 | 3 | 13 |
| MAE, schedule known at origin | UC3 | 6 | wt_med | chronos2_grid | 6.861 | -0.491 | -0.824 | -0.245 | -0.715 | -0.261 | True | 1.000 | 4 | 13 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | chronos2_grid | 7.310 | -0.328 | -0.642 | -0.092 | -0.549 | -0.112 | True | 1.000 | 3 | 12 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | h5 | 7.016 | -0.118 | -0.171 | -0.076 | -0.140 | -0.098 | True | 0.997 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 14 | wt_med | chronos2_x_h5 | 7.509 | -0.230 | -0.485 | -0.008 | -0.367 | -0.094 | True | 0.996 | 2 | 9 |
| MAE, schedule known at origin | UC3 | 21 | wt_med | h5 | 7.636 | -0.128 | -0.196 | -0.079 | -0.154 | -0.102 | True | 0.995 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 30 | wt_med | wt_med | 7.504 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 45 | wt_med | wt_med | 7.231 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 60 | wt_med | wt_med | 7.707 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 90 | wt_med | wt_med | 7.069 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | chronos2_grid | 4.305 | -0.639 | -0.983 | -0.321 | -0.835 | -0.440 | True | 1.000 | 4 | 10 |
| first-hour MAE, schedule known at origin | D5 | 1 | wt_med | lvlh5_tft | 4.254 | -0.449 | -1.022 | -0.031 | -0.669 | -0.251 | True | 0.624 | 4 | 11 |
| first-hour MAE, schedule known at origin | D5 | 2 | wt_med | lvlh5_tft | 4.142 | -0.210 | -0.636 | -0.011 | -0.324 | -0.103 | True | 0.596 | 5 | 11 |
| first-hour MAE, schedule known at origin | D5 | 3 | wt_med | lvlh5_tft | 4.398 | -0.218 | -0.547 | -0.013 | -0.337 | -0.114 | True | 0.597 | 5 | 13 |
| first-hour MAE, schedule known at origin | D5 | 4 | wt_med | chronos2_x_h5 | 4.880 | -0.188 | -0.471 | -0.017 | -0.297 | -0.086 | True | 0.999 | 3 | 13 |
| first-hour MAE, schedule known at origin | D5 | 5 | wt_med | chronos2_x_h5 | 5.049 | -0.150 | -0.366 | -0.008 | -0.246 | -0.058 | True | 0.998 | 3 | 13 |
| first-hour MAE, schedule known at origin | D5 | 6 | wt_med | lvlh5_cbd | 4.660 | -0.206 | -0.580 | -0.003 | -0.384 | -0.058 | True | 0.482 | 4 | 13 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | lvlh5_tft | 4.625 | -0.309 | -0.751 | -0.010 | -0.484 | -0.164 | True | 0.604 | 4 | 12 |
| first-hour MAE, schedule known at origin | D5 | 10 | wt_med | lvlh5_cbd | 4.483 | -0.299 | -0.705 | -0.024 | -0.456 | -0.178 | True | 0.472 | 5 | 9 |
| first-hour MAE, schedule known at origin | D5 | 14 | wt_med | lvlh5_tft | 4.526 | -0.359 | -0.817 | -0.034 | -0.560 | -0.200 | True | 0.572 | 5 | 9 |
| first-hour MAE, schedule known at origin | D5 | 21 | wt_med | lvlh5_tft | 4.585 | -0.327 | -0.733 | -0.034 | -0.510 | -0.187 | True | 0.548 | 4 | 9 |
| first-hour MAE, schedule known at origin | D5 | 30 | wt_med | wt_med | 4.609 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 45 | wt_med | wt_med | 4.986 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 60 | wt_med | wt_med | 4.689 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 90 | wt_med | wt_med | 4.420 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 0 | wt_med | chronos2_grid | 5.954 | -0.637 | -1.112 | -0.308 | -0.892 | -0.367 | True | 1.000 | 2 | 10 |
| MAE, schedule projected at origin | UC3 | 1 | wt_med | chronos2_grid | 6.265 | -0.702 | -1.187 | -0.442 | -0.959 | -0.472 | True | 1.000 | 3 | 11 |
| MAE, schedule projected at origin | UC3 | 2 | wt_med | wt_med | 6.561 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule projected at origin | UC3 | 3 | wt_med | h5 | 6.006 | -0.106 | -0.137 | -0.086 | -0.129 | -0.087 | True | 0.995 | 1 | 13 |
| MAE, schedule projected at origin | UC3 | 4 | wt_med | wt_med | 7.307 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| MAE, schedule projected at origin | UC3 | 5 | wt_med | wt_med | 7.246 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| MAE, schedule projected at origin | UC3 | 6 | wt_med | chronos2_x_h5 | 6.883 | -0.142 | -0.423 | -0.007 | -0.335 | 0.045 | False | 0.995 | 1 | 13 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | wt_med | 6.937 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | h5 | 6.812 | -0.100 | -0.137 | -0.070 | -0.125 | -0.076 | True | 0.995 | 1 | 9 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | wt_med | 7.881 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| MAE, schedule projected at origin | UC3 | 21 | wt_med | wt_med | 8.275 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| MAE, schedule projected at origin | UC3 | 30 | wt_med | wt_med | 8.162 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 45 | wt_med | wt_med | 8.597 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 60 | wt_med | wt_med | 8.938 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 90 | wt_med | wt_med | 9.237 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 0 | wt_med | wt_med | 3.877 | 0.000 |  |  |  |  | False |  | 0 | 10 |
| first-hour MAE, schedule projected at origin | D5 | 1 | wt_med | wt_med | 4.036 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 2 | wt_med | wt_med | 3.723 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 3 | wt_med | wt_med | 3.956 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 4 | wt_med | wt_med | 4.376 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 5 | wt_med | wt_med | 4.282 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 6 | wt_med | wt_med | 4.231 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | wt_med | 4.119 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| first-hour MAE, schedule projected at origin | D5 | 10 | wt_med | wt_med | 4.355 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 14 | wt_med | wt_med | 4.974 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 21 | wt_med | wt_med | 5.280 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 30 | wt_med | wt_med | 4.983 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 45 | wt_med | wt_med | 5.359 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 60 | wt_med | wt_med | 5.794 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 90 | wt_med | wt_med | 6.042 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | chronos2 | 6.769 | -0.645 | -0.941 | -0.333 | -0.739 | -0.550 | True | 0.997 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | wt_med | 0.827 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | wt_med | 0.821 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| best-time hit rate (top-2) | D3 | 2 | wt_med | wt_med | 0.829 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| best-time hit rate (top-2) | D3 | 3 | wt_med | wt_med | 0.821 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| best-time hit rate (top-2) | D3 | 4 | wt_med | h5 | 0.822 | 0.011 | 0.004 | 0.019 | 0.007 | 0.017 | True | 0.993 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 5 | wt_med | h5 | 0.825 | 0.010 | 0.003 | 0.019 | 0.005 | 0.016 | True | 0.992 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | h5 | 0.826 | 0.009 | 0.001 | 0.017 | 0.003 | 0.014 | True | 0.992 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 7 | wt_med | h5 | 0.831 | 0.011 | 0.002 | 0.021 | 0.005 | 0.017 | True | 0.993 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 10 | wt_med | wt_med | 0.824 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| best-time hit rate (top-2) | D3 | 14 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| best-time hit rate (top-2) | D3 | 21 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| best-time hit rate (top-2) | D3 | 30 | wt_med | wt_med | 0.828 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (top-2) | D3 | 45 | wt_med | wt_med | 0.818 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (top-2) | D3 | 60 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (top-2) | D3 | 90 | wt_med | wt_med | 0.821 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 0 | wt_med | h5 | 0.834 | 0.009 | 0.004 | 0.016 | 0.005 | 0.014 | True | 0.997 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | h5 | 0.829 | 0.007 | 0.002 | 0.013 | 0.002 | 0.011 | True | 0.996 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 2 | wt_med | wt_med | 0.829 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| best-time hit rate (±30 min) | D3 | 3 | wt_med | h5 | 0.829 | 0.011 | 0.004 | 0.020 | 0.007 | 0.016 | True | 0.989 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.826 | 0.009 | 0.002 | 0.016 | 0.004 | 0.013 | True | 0.993 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | h5 | 0.828 | 0.008 | 0.002 | 0.016 | 0.003 | 0.013 | True | 0.992 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | h5 | 0.829 | 0.010 | 0.004 | 0.017 | 0.005 | 0.014 | True | 0.992 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.834 | 0.008 | 0.003 | 0.015 | 0.004 | 0.013 | True | 0.993 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 10 | wt_med | wt_med | 0.820 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| best-time hit rate (±30 min) | D3 | 14 | wt_med | wt_med | 0.825 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| best-time hit rate (±30 min) | D3 | 21 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| best-time hit rate (±30 min) | D3 | 30 | wt_med | wt_med | 0.836 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 45 | wt_med | wt_med | 0.820 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 60 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 90 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.881 | -0.314 | -0.524 | -0.154 | -0.440 | -0.205 | True | 0.997 | 1 | 8 |
| best-time regret (min) | D3 | 1 | wt_med | h5 | 3.040 | -0.256 | -0.490 | -0.073 | -0.383 | -0.139 | True | 0.996 | 1 | 9 |
| best-time regret (min) | D3 | 2 | wt_med | h5 | 3.162 | -0.222 | -0.441 | -0.017 | -0.350 | -0.114 | True | 0.985 | 1 | 9 |
| best-time regret (min) | D3 | 3 | wt_med | h5 | 3.018 | -0.350 | -0.628 | -0.132 | -0.462 | -0.241 | True | 0.989 | 1 | 9 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 3.066 | -0.276 | -0.432 | -0.143 | -0.399 | -0.158 | True | 0.993 | 1 | 9 |
| best-time regret (min) | D3 | 5 | wt_med | h5 | 3.096 | -0.207 | -0.333 | -0.091 | -0.329 | -0.082 | True | 0.992 | 1 | 9 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 3.088 | -0.350 | -0.559 | -0.190 | -0.473 | -0.237 | True | 0.992 | 1 | 9 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 2.953 | -0.300 | -0.527 | -0.126 | -0.434 | -0.174 | True | 0.993 | 1 | 8 |
| best-time regret (min) | D3 | 10 | wt_med | h5 | 3.045 | -0.298 | -0.488 | -0.136 | -0.418 | -0.187 | True | 0.993 | 1 | 5 |
| best-time regret (min) | D3 | 14 | wt_med | h5 | 3.063 | -0.283 | -0.486 | -0.113 | -0.403 | -0.174 | True | 0.988 | 1 | 5 |
| best-time regret (min) | D3 | 21 | wt_med | h5 | 3.068 | -0.296 | -0.461 | -0.135 | -0.412 | -0.194 | True | 0.986 | 1 | 5 |
| best-time regret (min) | D3 | 30 | wt_med | wt_med | 3.226 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 45 | wt_med | wt_med | 3.218 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 60 | wt_med | wt_med | 3.367 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 90 | wt_med | wt_med | 3.432 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | chronos2_grid | 0.476 | 0.025 | 0.007 | 0.039 | 0.017 | 0.032 | True | 1.000 | 1 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | chronos2_grid | 0.447 | 0.026 | 0.005 | 0.043 | 0.018 | 0.033 | True | 1.000 | 1 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | wt_med | 0.500 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.505 | 0.016 | 0.009 | 0.023 | 0.012 | 0.020 | True | 0.965 | 1 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | chronos2_grid | 0.459 | 0.022 | 0.012 | 0.033 | 0.016 | 0.028 | True | 1.000 | 4 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | chronos2_grid | 0.461 | 0.019 | 0.008 | 0.028 | 0.012 | 0.025 | True | 1.000 | 2 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | chronos2_grid | 0.448 | 0.015 | 0.002 | 0.027 | 0.008 | 0.022 | True | 1.000 | 2 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.492 | 0.012 | 0.001 | 0.022 | 0.008 | 0.016 | True | 0.963 | 1 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.497 | 0.016 | 0.008 | 0.023 | 0.011 | 0.020 | True | 0.961 | 1 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.478 | 0.014 | 0.004 | 0.023 | 0.010 | 0.018 | True | 0.956 | 1 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | wt_med | h5 | 0.469 | 0.016 | 0.005 | 0.026 | 0.011 | 0.020 | True | 0.953 | 1 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | wt_med | wt_med | 0.474 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | wt_med | wt_med | 0.452 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | wt_med | wt_med | 0.440 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | wt_med | wt_med | 0.429 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | chronos2_grid | 7.397 | -0.841 | -1.281 | -0.458 | -1.103 | -0.566 | True | 1.000 | 2 | 8 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | chronos2_x_h5 | 7.496 | -0.640 | -0.959 | -0.365 | -0.845 | -0.454 | True | 0.995 | 1 | 9 |
| dayPeak abs error (min) | UC3 | 2 | snaive7 | snaive7 | 7.669 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| dayPeak abs error (min) | UC3 | 3 | wt_med | wt_med | 7.350 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak abs error (min) | UC3 | 4 | snaive7 | chronos2_x_h5 | 8.270 | -0.966 | -1.328 | -0.641 | -1.418 | -0.525 | True | 0.994 | 1 | 9 |
| dayPeak abs error (min) | UC3 | 5 | snaive7 | chronos2_x_h5 | 8.002 | -0.883 | -1.327 | -0.552 | -1.359 | -0.438 | True | 0.992 | 1 | 9 |
| dayPeak abs error (min) | UC3 | 6 | snaive7 | chronos2_x_h5 | 7.739 | -0.725 | -1.026 | -0.445 | -1.136 | -0.332 | True | 0.992 | 1 | 9 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | chronos2_x_h5 | 8.311 | -0.388 | -0.768 | -0.035 | -0.584 | -0.205 | True | 0.994 | 1 | 8 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 7.513 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| dayPeak abs error (min) | UC3 | 14 | wt_med | wt_med | 8.955 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| dayPeak abs error (min) | UC3 | 21 | wt_med | wt_med | 8.983 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| dayPeak abs error (min) | UC3 | 30 | wt_med | wt_med | 8.373 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 45 | wt_med | wt_med | 8.288 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 60 | wt_med | wt_med | 8.710 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 90 | wt_med | wt_med | 8.129 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 0 | wt_med | wt_med | 0.917 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.915 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 2 | snaive7 | snaive7 | 0.911 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 3 | wt_med | wt_med | 0.919 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 4 | wt_med | wt_med | 0.915 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 5 | snaive7 | snaive7 | 0.928 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 6 | snaive7 | snaive7 | 0.928 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 7 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 10 | wt_med | wt_med | 0.921 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| rope-drop worth agreement | D5 | 14 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| rope-drop worth agreement | D5 | 21 | wt_med | wt_med | 0.917 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| rope-drop worth agreement | D5 | 30 | wt_med | wt_med | 0.895 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 45 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.917 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 90 | wt_med | wt_med | 0.927 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | chronos2 | 0.840 | 0.073 | 0.053 | 0.098 | 0.064 | 0.082 | True | 1.000 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | chronos2 | 0.825 | 0.065 | 0.049 | 0.084 | 0.057 | 0.074 | True | 1.000 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | chronos2_x_h5 | 0.812 | 0.033 | 0.021 | 0.045 | 0.025 | 0.040 | True | 0.995 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | chronos2_x_h5 | 0.814 | 0.038 | 0.026 | 0.052 | 0.032 | 0.045 | True | 0.996 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | chronos2_nocov | 0.813 | 0.054 | 0.040 | 0.072 | 0.046 | 0.062 | True | 1.000 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | chronos2_nocov | 0.817 | 0.058 | 0.046 | 0.073 | 0.050 | 0.066 | True | 1.000 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | chronos2 | 0.805 | 0.054 | 0.038 | 0.074 | 0.046 | 0.062 | True | 1.000 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | chronos2 | 0.802 | 0.049 | 0.034 | 0.069 | 0.041 | 0.058 | True | 1.000 | 6 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | chronos2_x_h5 | 0.809 | 0.043 | 0.030 | 0.059 | 0.036 | 0.051 | True | 0.993 | 3 | 5 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | chronos2_x_h5 | 0.797 | 0.052 | 0.037 | 0.069 | 0.044 | 0.060 | True | 0.986 | 3 | 5 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | chronos2_x_h5 | 0.784 | 0.042 | 0.026 | 0.060 | 0.034 | 0.050 | True | 0.982 | 3 | 5 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | wt_med | wt_med | 0.759 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | wt_med | wt_med | 0.754 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | wt_med | wt_med | 0.740 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | wt_med | wt_med | 0.749 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 0 | snaive7 | snaive7 | 0.840 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 1 | snaive7 | snaive7 | 0.829 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 2 | wt_med | wt_med | 0.879 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 3 | wt_med | wt_med | 0.862 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 4 | snaive7 | snaive7 | 0.840 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 5 | snaive7 | snaive7 | 0.828 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 6 | snaive7 | snaive7 | 0.819 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.829 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.854 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| dayPeak Spearman across rides | UC3 | 21 | wt_med | wt_med | 0.813 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| dayPeak Spearman across rides | UC3 | 30 | wt_med | wt_med | 0.855 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 45 | wt_med | wt_med | 0.846 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 60 | wt_med | wt_med | 0.813 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 90 | wt_med | wt_med | 0.807 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | wt_med | 37.764 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | wt_med | 37.651 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | chronos2_x_h5 | 36.564 | -2.891 | -5.619 | -0.760 | -5.026 | -1.040 | True | 0.991 | 1 | 8 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | wt_med | 40.551 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| optimiser regret (min per park-day) | D4 | 30 | wt_med | wt_med | 39.742 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| next-best precision | D1 | 0-120 | wt_med | lvlh5_tft | 0.539 | 0.040 | 0.010 | 0.073 | 0.020 | 0.061 | True | 0.620 | 2 | 7 |
| next-best precision | D1 | 0-60 | wt_med | lvlh5_tft | 0.451 | 0.029 | 0.001 | 0.061 | 0.011 | 0.048 | True | 0.611 | 2 | 7 |
| next-best precision (top-3) | D1 | 0-120 | wt_med | chronos2 | 0.708 | 0.085 | 0.017 | 0.147 | 0.062 | 0.108 | True | 0.908 | 1 | 7 |
| next-best precision (top-3) | D1 | 0-60 | wt_med | chronos2 | 0.645 | 0.098 | 0.019 | 0.170 | 0.072 | 0.123 | True | 0.875 | 1 | 7 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.591 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.547 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_chronos2 | 0.458 | 0.066 | 0.025 | 0.103 | 0.022 | 0.107 | True | 1.000 | 1 | 7 |
| crowd bucket exact | D6 | 2 | lvl_snaive7 | lvl_snaive7 | 0.435 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| crowd bucket exact | D6 | 3 | lvl_naive4 | lvl_naive4 | 0.447 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.400 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.378 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.430 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| crowd bucket exact | D6 | 7 | lvl_naive4 | lvl_tft | 0.406 | 0.105 | 0.049 | 0.164 | 0.051 | 0.161 | True | 0.649 | 2 | 6 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_naive4 | 0.433 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_naive4 | 0.305 | 0.000 |  |  |  |  | True |  | 0 | 6 |
| crowd bucket exact | D6 | 21 | lvl_naive4 | lvl_naive4 | 0.291 | 0.000 |  |  |  |  | True |  | 0 | 6 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_naive4 | 0.401 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_naive4 | 0.381 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 60 | lvl_naive4 | lvl_naive4 | 0.285 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 90 | lvl_naive4 | lvl_naive4 | 0.325 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 120 | lvl_wt56 | lvl_wt56 | 0.277 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 180 | lvl_naive4 | lvl_naive4 | 0.214 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.441 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_naive4 | 0.310 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_naive4 | 0.287 | 0.000 |  |  |  |  | True |  | 0 | 0 |

### Paired against `chronos2` (not the per-lead reference)

MAE difference model − `chronos2` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.16 [-0.24, -0.07]* | 0.28 [0.19, 0.38] | -0.11 [-0.22, -0.04]* |  |  | 1.60 [1.17, 2.03] | 0.83 [0.56, 1.12] | 3.51 [2.66, 4.49] | 1.34 [1.04, 1.64] | 0.76 [0.46, 1.11] | -1.07 [-1.41, -0.78]* | 0.05 [-0.38, 0.54] |  |  |  | 2.16 [1.77, 2.55] | 0.87 [0.59, 1.16] | chronos2 |
| m015 |  |  |  |  |  | 4.71 [3.80, 5.60] | 4.02 [3.22, 4.78] |  | 4.47 [3.65, 5.25] | 3.75 [3.00, 4.55] |  |  | -0.26 [-0.36, -0.18]* |  |  | 5.32 [4.43, 6.23] | 3.99 [3.21, 4.74] | chronos2 |
| m030 |  |  |  |  |  | 3.77 [2.99, 4.55] | 3.05 [2.42, 3.66] |  | 3.53 [2.85, 4.20] | 2.89 [2.30, 3.58] |  |  | 0.42 [0.21, 0.61] |  |  | 4.38 [3.65, 5.12] | 3.08 [2.45, 3.70] | chronos2 |
| m045 |  |  |  |  |  | 3.39 [2.68, 4.08] | 2.67 [2.11, 3.19] |  | 3.15 [2.57, 3.72] | 2.50 [2.00, 3.06] |  |  | 0.90 [0.62, 1.17] |  |  | 4.02 [3.37, 4.67] | 2.71 [2.16, 3.23] | chronos2 |
| m060 |  |  |  |  |  | 3.24 [2.54, 3.89] | 2.53 [1.98, 3.06] |  | 2.99 [2.42, 3.57] | 2.34 [1.84, 2.92] |  |  | 1.26 [0.93, 1.60] |  |  | 3.87 [3.23, 4.53] | 2.56 [2.01, 3.08] | chronos2 |
| m075 |  |  |  |  |  | 3.04 [2.34, 3.70] | 2.29 [1.75, 2.82] |  | 2.76 [2.21, 3.32] | 2.07 [1.59, 2.65] |  |  | 1.56 [1.15, 1.96] |  |  | 3.65 [2.98, 4.29] | 2.34 [1.80, 2.89] | chronos2 |
| m090 |  |  |  |  |  | 2.96 [2.29, 3.66] | 2.15 [1.65, 2.66] |  | 2.66 [2.12, 3.17] | 2.02 [1.54, 2.55] |  |  | 1.88 [1.40, 2.39] |  |  | 3.54 [2.89, 4.16] | 2.24 [1.72, 2.76] | chronos2 |
| m105 |  |  |  |  |  | 2.90 [2.23, 3.57] | 2.09 [1.58, 2.60] |  | 2.62 [2.07, 3.18] | 1.94 [1.47, 2.46] |  |  | 2.06 [1.56, 2.64] |  |  | 3.46 [2.83, 4.10] | 2.16 [1.64, 2.68] | chronos2 |
| m120 |  |  |  |  |  | 2.87 [2.22, 3.54] | 2.05 [1.56, 2.54] |  | 2.55 [2.03, 3.06] | 1.87 [1.41, 2.35] |  |  | 2.27 [1.71, 2.92] |  |  | 3.39 [2.79, 3.99] | 2.10 [1.59, 2.59] | chronos2 |
| h2-4 |  |  |  |  |  | 2.50 [1.91, 3.10] | 1.72 [1.28, 2.16] |  | 2.25 [1.79, 2.73] | 1.59 [1.18, 2.06] |  |  | 3.02 [2.35, 3.77] |  |  | 3.08 [2.51, 3.64] | 1.76 [1.32, 2.21] | chronos2 |
| h4-8 |  |  |  |  |  | 2.02 [1.53, 2.52] | 1.24 [0.90, 1.59] |  | 1.79 [1.43, 2.17] | 1.22 [0.84, 1.64] |  |  | 4.74 [3.73, 5.91] |  |  | 2.67 [2.21, 3.15] | 1.30 [0.95, 1.64] | chronos2 |
| h8+ |  |  |  |  |  | 1.15 [0.77, 1.58] | 0.47 [0.27, 0.70] |  | 1.07 [0.79, 1.38] | 0.74 [0.40, 1.14] |  |  | 6.68 [5.20, 8.83] |  |  | 1.99 [1.63, 2.34] | 0.52 [0.32, 0.75] | chronos2 |
| 1 | -0.10 [-0.23, 0.05] | 0.42 [0.28, 0.56] | -0.09 [-0.15, -0.03]* | 0.23 [0.05, 0.43] | 0.23 [0.05, 0.42] | 1.16 [0.81, 1.65] | 0.48 [0.21, 0.77] | 3.09 [2.34, 4.13] | 0.91 [0.60, 1.28] | 0.69 [0.45, 1.00] | -1.26 [-1.69, -0.88]* | -0.59 [-1.12, -0.05]* |  |  |  | 1.83 [1.49, 2.27] | 0.54 [0.27, 0.85] | chronos2 |
| 2 | -0.13 [-0.27, 0.01] | 0.81 [0.53, 1.16] | -0.10 [-0.16, -0.05]* | -0.38 [-0.55, -0.24]* | -0.37 [-0.54, -0.23]* | -0.25 [-0.47, -0.03]* | -0.71 [-0.90, -0.54]* | 2.84 [2.17, 3.66] | -0.45 [-0.61, -0.29]* | 0.06 [-0.28, 0.36] | -2.15 [-2.62, -1.73]* | -2.30 [-2.68, -1.87]* |  |  |  | 0.50 [0.12, 0.86] | -0.68 [-0.86, -0.51]* | chronos2 |
| 3 | -0.13 [-0.29, 0.01] | 0.52 [0.34, 0.74] | -0.06 [-0.11, -0.02]* | -0.09 [-0.21, 0.02] | -0.09 [-0.21, 0.03] | 0.52 [0.26, 0.77] | -0.17 [-0.31, -0.01]* | 2.73 [2.08, 3.50] | -0.06 [-0.18, 0.06] | 0.23 [0.01, 0.45] | -1.99 [-2.49, -1.56]* | -1.55 [-1.92, -1.19]* |  | 0.98 [0.62, 1.33] | 0.96 [0.59, 1.31] | 1.18 [0.80, 1.56] | -0.07 [-0.22, 0.10] | chronos2 |
| 4 | -0.22 [-0.33, -0.11]* | 0.47 [0.28, 0.72] | -0.07 [-0.14, -0.02]* | -0.12 [-0.36, 0.12] | -0.11 [-0.35, 0.13] | 0.65 [0.38, 0.95] | 0.16 [-0.05, 0.43] | 2.82 [2.11, 3.68] | 0.46 [0.27, 0.71] | 0.29 [-0.00, 0.60] | -2.02 [-2.61, -1.50]* | -1.06 [-1.54, -0.55]* |  | 0.70 [0.23, 1.17] | 0.68 [0.21, 1.14] | 1.31 [1.02, 1.64] | 0.20 [-0.02, 0.49] | chronos2 |
| 5 | -0.29 [-0.46, -0.13]* | 0.47 [0.28, 0.67] | 0.00 [-0.06, 0.07] | -0.15 [-0.41, 0.13] | -0.14 [-0.40, 0.14] | 0.62 [0.28, 1.08] | 0.08 [-0.20, 0.46] | 2.66 [2.05, 3.41] | 0.30 [0.05, 0.61] | 0.14 [-0.18, 0.47] | -2.23 [-2.79, -1.68]* | -1.14 [-1.69, -0.52]* |  | 0.49 [-0.01, 0.93] | 0.47 [-0.02, 0.90] | 1.08 [0.82, 1.37] | 0.14 [-0.16, 0.54] | chronos2 |
| 6 | -0.22 [-0.38, -0.05]* | 0.39 [0.24, 0.55] | -0.04 [-0.09, -0.00]* | -0.08 [-0.28, 0.16] | -0.07 [-0.27, 0.16] | 0.72 [0.38, 1.11] | 0.14 [-0.11, 0.48] | 2.66 [1.96, 3.48] | 0.30 [0.09, 0.55] | 0.31 [0.09, 0.58] | -2.04 [-2.59, -1.56]* | -1.08 [-1.61, -0.54]* |  | 0.79 [0.50, 1.08] | 0.77 [0.49, 1.07] | 1.00 [0.80, 1.22] | 0.22 [-0.04, 0.57] | chronos2 |
| 7 | -0.18 [-0.30, -0.03]* | 0.43 [0.26, 0.61] | -0.04 [-0.08, -0.00]* | -0.14 [-0.36, 0.12] | -0.14 [-0.36, 0.11] | 0.54 [0.18, 0.97] | -0.01 [-0.29, 0.32] | 2.72 [2.09, 3.45] | 0.53 [0.24, 0.84] | 0.30 [0.04, 0.60] | -2.23 [-2.72, -1.76]* | -0.69 [-1.25, -0.02]* |  | 0.82 [0.48, 1.17] | 0.80 [0.47, 1.16] |  | 0.06 [-0.23, 0.40] | chronos2 |

segment `busy`:

| lead | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.26 [-0.43, -0.05]* | 0.65 [0.46, 0.83] | -0.27 [-0.51, -0.10]* |  |  | 3.71 [2.77, 4.74] | 2.08 [1.43, 2.81] | 6.95 [4.83, 9.33] | 2.73 [2.14, 3.32] | 1.26 [0.59, 2.08] | -2.53 [-3.28, -1.88]* | -0.21 [-0.86, 0.55] |  |  |  | 4.45 [3.74, 5.11] | 2.21 [1.55, 2.94] | chronos2 |
| m015 |  |  |  |  |  | 9.63 [7.74, 11.59] | 8.23 [6.64, 9.66] |  | 8.74 [7.17, 10.16] | 7.23 [5.73, 8.54] |  |  | -0.44 [-0.65, -0.24]* |  |  | 10.53 [8.90, 11.98] | 8.20 [6.66, 9.66] | chronos2 |
| m030 |  |  |  |  |  | 7.81 [6.08, 9.60] | 6.35 [4.97, 7.66] |  | 6.89 [5.51, 8.19] | 5.41 [4.12, 6.65] |  |  | 0.92 [0.49, 1.28] |  |  | 8.61 [7.26, 9.84] | 6.43 [5.02, 7.75] | chronos2 |
| m045 |  |  |  |  |  | 7.07 [5.54, 8.66] | 5.63 [4.42, 6.87] |  | 6.17 [4.98, 7.31] | 4.67 [3.65, 5.79] |  |  | 1.87 [1.40, 2.27] |  |  | 7.92 [6.77, 8.94] | 5.72 [4.48, 6.97] | chronos2 |
| m060 |  |  |  |  |  | 6.82 [5.32, 8.37] | 5.40 [4.15, 6.60] |  | 5.91 [4.70, 7.09] | 4.35 [3.26, 5.53] |  |  | 2.62 [2.10, 3.12] |  |  | 7.75 [6.61, 8.92] | 5.48 [4.25, 6.69] | chronos2 |
| m075 |  |  |  |  |  | 6.54 [4.95, 8.12] | 4.97 [3.75, 6.13] |  | 5.47 [4.32, 6.62] | 3.80 [2.75, 5.00] |  |  | 3.07 [2.40, 3.79] |  |  | 7.39 [6.20, 8.55] | 5.12 [3.87, 6.29] | chronos2 |
| m090 |  |  |  |  |  | 6.40 [4.79, 8.07] | 4.70 [3.51, 5.93] |  | 5.28 [4.13, 6.43] | 3.66 [2.65, 4.78] |  |  | 3.76 [2.90, 4.65] |  |  | 7.12 [5.94, 8.28] | 4.93 [3.70, 6.15] | chronos2 |
| m105 |  |  |  |  |  | 6.30 [4.63, 8.03] | 4.58 [3.36, 5.82] |  | 5.23 [4.03, 6.50] | 3.51 [2.48, 4.66] |  |  | 4.06 [3.17, 5.12] |  |  | 6.94 [5.71, 8.29] | 4.75 [3.51, 5.97] | chronos2 |
| m120 |  |  |  |  |  | 6.23 [4.60, 7.96] | 4.49 [3.30, 5.71] |  | 5.09 [3.95, 6.24] | 3.41 [2.42, 4.46] |  |  | 4.51 [3.54, 5.73] |  |  | 6.85 [5.65, 8.19] | 4.67 [3.47, 5.88] | chronos2 |
| h2-4 |  |  |  |  |  | 5.60 [4.15, 7.10] | 3.98 [2.92, 5.07] |  | 4.62 [3.63, 5.63] | 2.96 [2.05, 4.05] |  |  | 5.84 [4.69, 7.21] |  |  | 6.34 [5.27, 7.47] | 4.09 [3.01, 5.18] | chronos2 |
| h4-8 |  |  |  |  |  | 4.55 [3.41, 5.77] | 2.94 [2.12, 3.77] |  | 3.61 [2.86, 4.36] | 2.19 [1.34, 3.15] |  |  | 8.75 [7.00, 10.69] |  |  | 5.42 [4.56, 6.31] | 3.08 [2.29, 3.90] | chronos2 |
| h8+ |  |  |  |  |  | 2.66 [1.95, 3.52] | 1.20 [0.76, 1.72] |  | 1.88 [1.40, 2.38] | 1.05 [0.40, 1.95] |  |  | 12.27 [8.95, 16.55] |  |  | 3.92 [3.33, 4.47] | 1.35 [0.95, 1.81] | chronos2 |
| 1 | -0.18 [-0.47, 0.16] | 1.02 [0.70, 1.35] | -0.21 [-0.37, -0.07]* | 0.20 [-0.20, 0.69] | 0.19 [-0.21, 0.67] | 2.57 [1.67, 3.88] | 1.14 [0.41, 1.96] | 5.78 [3.72, 8.55] | 1.58 [0.91, 2.40] | 0.87 [0.28, 1.66] | -3.11 [-3.92, -2.31]* | -1.89 [-2.59, -1.05]* |  |  |  | 3.68 [2.99, 4.75] | 1.29 [0.59, 2.15] | chronos2 |
| 2 | -0.17 [-0.47, 0.18] | 1.94 [1.21, 2.94] | -0.23 [-0.37, -0.12]* | -1.05 [-1.47, -0.69]* | -1.02 [-1.44, -0.67]* | -0.43 [-1.06, 0.18] | -1.38 [-1.94, -0.97]* | 5.77 [4.01, 7.68] | -1.09 [-1.59, -0.72]* | -0.40 [-1.20, 0.29] | -4.72 [-5.58, -3.90]* | -5.01 [-5.83, -4.43]* |  |  |  | 1.28 [0.49, 1.93] | -1.25 [-1.82, -0.81]* | chronos2 |
| 3 | -0.29 [-0.66, 0.11] | 1.18 [0.73, 1.62] | -0.15 [-0.26, -0.05]* | -0.55 [-0.85, -0.26]* | -0.54 [-0.84, -0.24]* | 1.27 [0.70, 1.82] | -0.16 [-0.51, 0.21] | 5.03 [3.38, 6.83] | -0.40 [-0.68, -0.12]* | -0.22 [-0.67, 0.25] | -4.76 [-5.61, -3.87]* | -3.91 [-4.33, -3.48]* |  | 1.22 [0.43, 2.03] | 1.16 [0.35, 1.98] | 2.84 [2.09, 3.43] | 0.10 [-0.26, 0.48] | chronos2 |
| 4 | -0.38 [-0.56, -0.13]* | 1.10 [0.64, 1.68] | -0.17 [-0.32, -0.03]* | -0.49 [-0.97, 0.02] | -0.46 [-0.92, 0.05] | 1.52 [0.92, 2.29] | 0.54 [0.06, 1.23] | 5.28 [3.53, 7.39] | 0.76 [0.36, 1.33] | 0.05 [-0.65, 0.74] | -4.68 [-5.70, -3.65]* | -2.62 [-3.22, -1.80]* |  | 0.94 [-0.14, 2.05] | 0.88 [-0.19, 1.98] | 3.04 [2.47, 3.65] | 0.66 [0.17, 1.38] | chronos2 |
| 5 | -0.50 [-0.86, -0.13]* | 1.12 [0.66, 1.56] | 0.01 [-0.14, 0.19] | -0.53 [-1.08, 0.12] | -0.49 [-1.04, 0.17] | 1.52 [0.75, 2.59] | 0.40 [-0.28, 1.44] | 4.98 [3.28, 6.80] | 0.46 [-0.07, 1.23] | -0.09 [-0.94, 0.63] | -4.90 [-5.91, -3.85]* | -2.67 [-3.42, -1.60]* |  | 0.65 [-0.67, 1.67] | 0.60 [-0.71, 1.60] | 2.38 [1.85, 2.95] | 0.55 [-0.16, 1.61] | chronos2 |
| 6 | -0.35 [-0.65, 0.02] | 0.99 [0.63, 1.31] | -0.08 [-0.18, 0.02] | -0.31 [-0.69, 0.20] | -0.29 [-0.67, 0.22] | 1.81 [0.95, 2.90] | 0.63 [-0.00, 1.51] | 4.91 [3.02, 7.07] | 0.47 [0.04, 1.08] | 0.13 [-0.35, 0.66] | -4.67 [-5.71, -3.66]* | -2.73 [-3.41, -1.81]* |  | 1.12 [0.59, 1.71] | 1.08 [0.54, 1.66] | 2.15 [1.78, 2.55] | 0.83 [0.21, 1.75] | chronos2 |
| 7 | -0.36 [-0.63, -0.01]* | 1.05 [0.66, 1.43] | -0.10 [-0.19, -0.00]* | -0.44 [-0.82, 0.09] | -0.42 [-0.81, 0.12] | 1.40 [0.51, 2.55] | 0.25 [-0.39, 1.13] | 5.11 [3.33, 6.90] | 1.03 [0.45, 1.71] | 0.07 [-0.45, 0.58] | -5.05 [-6.07, -4.08]* | -1.82 [-2.71, -0.75]* |  | 1.19 [0.50, 1.90] | 1.14 [0.45, 1.85] |  | 0.43 [-0.22, 1.37] | chronos2 |

### Paired against `chronos2_grid` (not the per-lead reference)

MAE difference model − `chronos2_grid` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.16 [0.07, 0.24] | 0.44 [0.29, 0.60] | 0.04 [-0.12, 0.18] |  |  | 1.74 [1.35, 2.13] | 0.98 [0.75, 1.25] | 3.70 [2.84, 4.68] | 1.49 [1.20, 1.80] | 0.96 [0.62, 1.38] | -0.91 [-1.26, -0.61]* | 0.21 [-0.21, 0.69] |  |  | 2.30 [1.87, 2.74] | 1.02 [0.78, 1.29] | chronos2_grid |
| 1 | 0.10 [-0.05, 0.23] | 0.51 [0.32, 0.72] | 0.01 [-0.18, 0.17] | 0.32 [0.16, 0.49] | 0.32 [0.16, 0.48] | 1.23 [0.91, 1.63] | 0.57 [0.36, 0.80] | 3.24 [2.48, 4.27] | 1.00 [0.73, 1.32] | 0.80 [0.58, 1.06] | -1.17 [-1.60, -0.79]* | -0.51 [-0.96, -0.00]* |  |  | 1.90 [1.58, 2.26] | 0.62 [0.39, 0.86] | chronos2_grid |
| 2 | 0.13 [-0.01, 0.27] | 0.95 [0.61, 1.35] | 0.04 [-0.12, 0.17] | -0.25 [-0.44, -0.09]* | -0.24 [-0.43, -0.08]* | -0.14 [-0.40, 0.12] | -0.58 [-0.81, -0.38]* | 3.04 [2.34, 3.91] | -0.33 [-0.53, -0.16]* | 0.25 [-0.07, 0.53] | -2.01 [-2.49, -1.59]* | -2.17 [-2.53, -1.78]* |  |  | 0.65 [0.21, 1.05] | -0.55 [-0.79, -0.34]* | chronos2_grid |
| 3 | 0.13 [-0.01, 0.29] | 0.66 [0.41, 0.97] | 0.07 [-0.08, 0.23] | 0.04 [-0.07, 0.15] | 0.05 [-0.06, 0.15] | 0.63 [0.33, 0.93] | -0.03 [-0.21, 0.14] | 2.94 [2.24, 3.78] | 0.07 [-0.09, 0.23] | 0.45 [0.23, 0.69] | -1.85 [-2.30, -1.47]* | -1.42 [-1.75, -1.08]* | 1.19 [0.81, 1.55] | 1.17 [0.79, 1.53] | 1.31 [0.84, 1.78] | 0.06 [-0.13, 0.24] | chronos2_grid |
| 4 | 0.22 [0.11, 0.33] | 0.69 [0.44, 0.97] | 0.15 [0.04, 0.25] | 0.11 [-0.08, 0.28] | 0.11 [-0.06, 0.29] | 0.87 [0.62, 1.14] | 0.39 [0.21, 0.61] | 3.13 [2.44, 3.96] | 0.68 [0.51, 0.87] | 0.60 [0.29, 0.91] | -1.80 [-2.32, -1.34]* | -0.84 [-1.26, -0.39]* | 1.00 [0.48, 1.50] | 0.98 [0.46, 1.48] | 1.54 [1.23, 1.92] | 0.43 [0.23, 0.66] | chronos2_grid |
| 5 | 0.29 [0.13, 0.46] | 0.76 [0.48, 1.05] | 0.29 [0.11, 0.51] | 0.15 [-0.05, 0.35] | 0.16 [-0.04, 0.35] | 0.92 [0.58, 1.34] | 0.38 [0.13, 0.69] | 3.03 [2.38, 3.82] | 0.61 [0.40, 0.84] | 0.53 [0.20, 0.86] | -1.93 [-2.42, -1.47]* | -0.83 [-1.30, -0.30]* | 0.87 [0.31, 1.35] | 0.86 [0.30, 1.32] | 1.38 [1.05, 1.73] | 0.43 [0.18, 0.76] | chronos2_grid |
| 6 | 0.22 [0.05, 0.38] | 0.61 [0.36, 0.85] | 0.17 [0.02, 0.33] | 0.14 [0.00, 0.28] | 0.15 [0.01, 0.28] | 0.92 [0.61, 1.30] | 0.36 [0.16, 0.60] | 2.99 [2.31, 3.84] | 0.53 [0.39, 0.68] | 0.64 [0.44, 0.87] | -1.82 [-2.34, -1.38]* | -0.86 [-1.27, -0.43]* | 1.12 [0.79, 1.43] | 1.11 [0.78, 1.41] | 1.21 [1.00, 1.41] | 0.44 [0.24, 0.69] | chronos2_grid |
| 7 | 0.18 [0.03, 0.30] | 0.61 [0.36, 0.86] | 0.14 [0.00, 0.25] | 0.04 [-0.09, 0.20] | 0.04 [-0.09, 0.21] | 0.71 [0.36, 1.13] | 0.17 [-0.05, 0.41] | 3.00 [2.30, 3.78] | 0.72 [0.50, 0.95] | 0.57 [0.33, 0.82] | -2.05 [-2.51, -1.64]* | -0.50 [-1.00, 0.05] | 1.09 [0.78, 1.40] | 1.08 [0.77, 1.39] |  | 0.23 [0.01, 0.50] | chronos2_grid |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.26 [0.05, 0.43] | 0.92 [0.56, 1.22] | -0.01 [-0.43, 0.29] |  |  | 3.95 [3.10, 4.93] | 2.34 [1.78, 2.95] | 7.32 [5.14, 9.66] | 2.99 [2.41, 3.58] | 1.65 [0.87, 2.56] | -2.27 [-3.04, -1.57]* | 0.05 [-0.62, 0.79] |  |  | 4.67 [3.94, 5.38] | 2.47 [1.92, 3.08] | chronos2_grid |
| 1 | 0.18 [-0.16, 0.47] | 1.20 [0.77, 1.60] | -0.03 [-0.48, 0.36] | 0.37 [0.05, 0.71] | 0.36 [0.05, 0.70] | 2.68 [1.93, 3.75] | 1.31 [0.76, 1.95] | 6.10 [4.08, 8.85] | 1.73 [1.19, 2.33] | 1.05 [0.57, 1.61] | -2.93 [-3.83, -2.10]* | -1.74 [-2.32, -1.07]* |  |  | 3.77 [3.20, 4.57] | 1.44 [0.88, 2.09] | chronos2_grid |
| 2 | 0.17 [-0.18, 0.47] | 2.11 [1.16, 3.24] | -0.05 [-0.43, 0.27] | -0.89 [-1.35, -0.50]* | -0.86 [-1.31, -0.48]* | -0.29 [-1.01, 0.43] | -1.21 [-1.88, -0.71]* | 5.98 [4.17, 8.15] | -0.94 [-1.47, -0.54]* | -0.12 [-0.90, 0.54] | -4.56 [-5.54, -3.69]* | -4.86 [-5.69, -4.31]* |  |  | 1.47 [0.57, 2.18] | -1.09 [-1.78, -0.54]* | chronos2_grid |
| 3 | 0.29 [-0.11, 0.66] | 1.47 [0.78, 2.18] | 0.14 [-0.27, 0.53] | -0.27 [-0.56, -0.01]* | -0.26 [-0.55, -0.00]* | 1.52 [0.89, 2.18] | 0.12 [-0.30, 0.51] | 5.43 [3.61, 7.44] | -0.12 [-0.56, 0.26] | 0.23 [-0.30, 0.70] | -4.48 [-5.33, -3.64]* | -3.64 [-4.22, -3.18]* | 1.64 [0.78, 2.45] | 1.58 [0.71, 2.41] | 3.11 [2.16, 3.97] | 0.37 [-0.05, 0.76] | chronos2_grid |
| 4 | 0.38 [0.13, 0.56] | 1.47 [0.94, 2.10] | 0.21 [-0.05, 0.43] | -0.12 [-0.52, 0.29] | -0.09 [-0.48, 0.31] | 1.89 [1.39, 2.53] | 0.91 [0.50, 1.48] | 5.74 [4.07, 7.82] | 1.12 [0.80, 1.58] | 0.54 [-0.14, 1.19] | -4.31 [-5.33, -3.32]* | -2.26 [-2.80, -1.57]* | 1.43 [0.29, 2.59] | 1.37 [0.23, 2.54] | 3.42 [2.86, 3.99] | 1.02 [0.59, 1.62] | chronos2_grid |
| 5 | 0.50 [0.13, 0.86] | 1.62 [1.05, 2.15] | 0.51 [0.07, 1.00] | -0.03 [-0.54, 0.43] | 0.01 [-0.48, 0.47] | 2.02 [1.17, 3.13] | 0.90 [0.29, 1.74] | 5.58 [3.87, 7.57] | 0.99 [0.54, 1.60] | 0.57 [-0.33, 1.30] | -4.40 [-5.34, -3.47]* | -2.15 [-2.80, -1.26]* | 1.31 [-0.17, 2.44] | 1.25 [-0.20, 2.37] | 2.89 [2.28, 3.61] | 1.04 [0.42, 1.92] | chronos2_grid |
| 6 | 0.35 [-0.02, 0.65] | 1.34 [0.83, 1.79] | 0.27 [-0.09, 0.56] | 0.03 [-0.30, 0.36] | 0.06 [-0.25, 0.38] | 2.14 [1.37, 3.17] | 0.97 [0.49, 1.68] | 5.50 [3.63, 7.79] | 0.83 [0.53, 1.23] | 0.73 [0.31, 1.15] | -4.32 [-5.36, -3.32]* | -2.37 [-2.88, -1.75]* | 1.76 [1.22, 2.30] | 1.72 [1.18, 2.26] | 2.48 [2.24, 2.70] | 1.17 [0.68, 1.90] | chronos2_grid |
| 7 | 0.36 [0.01, 0.63] | 1.41 [0.92, 1.86] | 0.26 [-0.06, 0.51] | -0.08 [-0.34, 0.30] | -0.06 [-0.32, 0.34] | 1.73 [0.85, 2.88] | 0.61 [0.09, 1.27] | 5.72 [3.93, 7.67] | 1.42 [0.95, 1.90] | 0.67 [0.23, 1.06] | -4.70 [-5.65, -3.78]* | -1.45 [-2.19, -0.53]* | 1.82 [1.21, 2.37] | 1.77 [1.15, 2.33] |  | 0.77 [0.24, 1.46] | chronos2_grid |

### Paired against `h5` (not the per-lead reference)

MAE difference model − `h5` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.83 [-1.12, -0.56]* | -0.98 [-1.25, -0.75]* | -0.54 [-0.84, -0.26]* | -0.94 [-1.27, -0.62]* |  |  | 0.71 [0.51, 0.96] | 2.78 [1.99, 3.83] | 0.48 [0.31, 0.62] | 0.14 [-0.09, 0.35] | -1.89 [-2.34, -1.46]* | -0.81 [-1.15, -0.43]* |  |  |  | 1.27 [0.89, 1.61] | 0.04 [0.01, 0.07] | h5 |
| m015 | -4.02 [-4.78, -3.22]* |  |  |  |  |  | 0.67 [0.46, 0.94] |  | 0.46 [0.30, 0.61] | 0.10 [-0.14, 0.32] |  |  | -4.70 [-5.51, -3.84]* |  |  | 1.20 [0.85, 1.54] | -0.03 [-0.08, 0.02] | h5 |
| m030 | -3.05 [-3.66, -2.42]* |  |  |  |  |  | 0.70 [0.50, 0.96] |  | 0.49 [0.33, 0.63] | 0.17 [-0.06, 0.40] |  |  | -3.02 [-3.70, -2.35]* |  |  | 1.25 [0.89, 1.58] | 0.03 [0.01, 0.06] | h5 |
| m045 | -2.67 [-3.19, -2.11]* |  |  |  |  |  | 0.70 [0.50, 0.94] |  | 0.49 [0.33, 0.62] | 0.16 [-0.07, 0.38] |  |  | -2.10 [-2.73, -1.48]* |  |  | 1.27 [0.87, 1.62] | 0.04 [0.02, 0.07] | h5 |
| m060 | -2.53 [-3.06, -1.98]* |  |  |  |  |  | 0.67 [0.49, 0.90] |  | 0.45 [0.29, 0.60] | 0.12 [-0.11, 0.33] |  |  | -1.56 [-2.17, -0.94]* |  |  | 1.25 [0.87, 1.61] | 0.02 [-0.00, 0.05] | h5 |
| m075 | -2.29 [-2.82, -1.75]* |  |  |  |  |  | 0.71 [0.51, 0.95] |  | 0.47 [0.29, 0.62] | 0.11 [-0.12, 0.33] |  |  | -1.18 [-1.80, -0.57]* |  |  | 1.29 [0.89, 1.66] | 0.05 [-0.00, 0.10] | h5 |
| m090 | -2.15 [-2.66, -1.65]* |  |  |  |  |  | 0.77 [0.54, 1.03] |  | 0.50 [0.32, 0.64] | 0.16 [-0.07, 0.39] |  |  | -0.67 [-1.26, -0.09]* |  |  | 1.33 [0.92, 1.73] | 0.08 [0.03, 0.15] | h5 |
| m105 | -2.09 [-2.60, -1.58]* |  |  |  |  |  | 0.77 [0.56, 1.01] |  | 0.51 [0.33, 0.67] | 0.15 [-0.09, 0.38] |  |  | -0.42 [-1.08, 0.22] |  |  | 1.30 [0.90, 1.69] | 0.06 [0.03, 0.11] | h5 |
| m120 | -2.05 [-2.54, -1.56]* |  |  |  |  |  | 0.76 [0.56, 1.02] |  | 0.47 [0.29, 0.63] | 0.11 [-0.12, 0.32] |  |  | -0.10 [-0.73, 0.58] |  |  | 1.27 [0.88, 1.65] | 0.05 [0.01, 0.11] | h5 |
| h2-4 | -1.72 [-2.16, -1.28]* |  |  |  |  |  | 0.72 [0.52, 0.97] |  | 0.50 [0.32, 0.65] | 0.14 [-0.10, 0.36] |  |  | 0.91 [0.27, 1.62] |  |  | 1.28 [0.90, 1.63] | 0.04 [0.01, 0.07] | h5 |
| h4-8 | -1.24 [-1.59, -0.90]* |  |  |  |  |  | 0.71 [0.52, 0.95] |  | 0.52 [0.33, 0.67] | 0.18 [-0.06, 0.40] |  |  | 3.31 [2.43, 4.37] |  |  | 1.36 [1.01, 1.69] | 0.05 [0.02, 0.09] | h5 |
| h8+ | -0.47 [-0.70, -0.27]* |  |  |  |  |  | 0.62 [0.41, 0.87] |  | 0.59 [0.40, 0.76] | 0.42 [0.21, 0.64] |  |  | 6.19 [4.82, 8.15] |  |  | 1.46 [1.11, 1.77] | 0.04 [-0.00, 0.09] | h5 |
| 1 | -0.48 [-0.77, -0.21]* | -0.57 [-0.80, -0.36]* | -0.07 [-0.35, 0.24] | -0.57 [-0.90, -0.29]* | -0.25 [-0.42, -0.10]* | -0.25 [-0.43, -0.10]* | 0.62 [0.42, 0.88] | 2.60 [1.75, 3.65] | 0.40 [0.22, 0.59] | 0.26 [-0.04, 0.55] | -1.75 [-2.21, -1.36]* | -1.10 [-1.51, -0.66]* |  |  |  | 1.29 [1.00, 1.63] | 0.05 [0.02, 0.08] | h5 |
| 2 | 0.71 [0.54, 0.90] | 0.58 [0.38, 0.81] | 1.53 [1.14, 2.02] | 0.61 [0.44, 0.81] | 0.33 [0.22, 0.44] | 0.34 [0.22, 0.45] | 0.42 [0.30, 0.57] | 3.28 [2.63, 4.04] | 0.25 [0.15, 0.36] | 0.58 [0.33, 0.81] | -1.43 [-1.82, -1.09]* | -1.60 [-1.93, -1.24]* |  |  |  | 1.22 [0.87, 1.58] | 0.03 [0.00, 0.07] | h5 |
| 3 | 0.17 [0.01, 0.31] | 0.03 [-0.14, 0.21] | 0.69 [0.42, 1.00] | 0.10 [-0.06, 0.26] | 0.08 [-0.05, 0.21] | 0.08 [-0.04, 0.21] | 0.65 [0.48, 0.82] | 2.85 [2.18, 3.67] | 0.10 [-0.02, 0.22] | 0.33 [0.15, 0.53] | -1.82 [-2.31, -1.38]* | -1.39 [-1.73, -1.02]* |  | 1.08 [0.75, 1.44] | 1.06 [0.72, 1.42] | 1.34 [0.95, 1.75] | 0.10 [0.07, 0.13] | h5 |
| 4 | -0.16 [-0.43, 0.05] | -0.39 [-0.61, -0.21]* | 0.30 [-0.05, 0.63] | -0.24 [-0.48, -0.04]* | -0.28 [-0.46, -0.14]* | -0.27 [-0.45, -0.13]* | 0.46 [0.30, 0.66] | 2.68 [1.90, 3.73] | 0.28 [0.13, 0.42] | 0.20 [-0.11, 0.47] | -2.19 [-2.69, -1.71]* | -1.24 [-1.62, -0.85]* |  | 0.62 [0.11, 1.06] | 0.60 [0.09, 1.03] | 1.12 [0.73, 1.49] | 0.03 [0.00, 0.06] | h5 |
| 5 | -0.08 [-0.46, 0.20] | -0.38 [-0.69, -0.13]* | 0.39 [-0.10, 0.79] | -0.08 [-0.44, 0.20] | -0.23 [-0.43, -0.08]* | -0.22 [-0.42, -0.07]* | 0.53 [0.35, 0.75] | 2.55 [1.84, 3.41] | 0.21 [0.03, 0.35] | 0.11 [-0.26, 0.46] | -2.31 [-2.79, -1.85]* | -1.24 [-1.62, -0.81]* |  | 0.46 [-0.09, 0.96] | 0.44 [-0.11, 0.94] | 0.97 [0.52, 1.35] | 0.05 [0.02, 0.08] | h5 |
| 6 | -0.14 [-0.48, 0.11] | -0.36 [-0.60, -0.16]* | 0.25 [-0.12, 0.57] | -0.18 [-0.51, 0.07] | -0.22 [-0.42, -0.05]* | -0.21 [-0.41, -0.04]* | 0.54 [0.36, 0.77] | 2.58 [1.82, 3.60] | 0.15 [-0.00, 0.28] | 0.27 [-0.02, 0.54] | -2.18 [-2.73, -1.69]* | -1.24 [-1.62, -0.84]* |  | 0.74 [0.33, 1.07] | 0.72 [0.32, 1.05] | 0.83 [0.48, 1.14] | 0.07 [0.04, 0.12] | h5 |
| 7 | 0.01 [-0.32, 0.29] | -0.17 [-0.41, 0.05] | 0.44 [0.05, 0.79] | -0.03 [-0.34, 0.25] | -0.13 [-0.32, 0.03] | -0.12 [-0.33, 0.03] | 0.53 [0.31, 0.79] | 2.82 [2.04, 3.78] | 0.54 [0.34, 0.73] | 0.47 [0.21, 0.71] | -2.21 [-2.72, -1.72]* | -0.68 [-1.11, -0.19]* |  | 0.98 [0.64, 1.27] | 0.97 [0.63, 1.26] |  | 0.06 [0.03, 0.10] | h5 |
| 10 |  |  |  |  | 0.09 [-0.07, 0.24] | 0.11 [-0.04, 0.26] | 0.63 [0.45, 0.83] | 3.02 [2.31, 3.81] | 0.07 [-0.10, 0.22] | 0.47 [0.18, 0.73] | -1.94 [-2.42, -1.49]* | -1.39 [-1.75, -0.97]* |  | 1.29 [0.86, 1.70] | 1.27 [0.83, 1.68] |  | 0.11 [0.08, 0.15] | h5 |
| 14 |  |  |  |  | -0.06 [-0.27, 0.11] | -0.05 [-0.25, 0.12] | 0.37 [0.16, 0.61] | 2.76 [1.94, 3.77] | 0.60 [0.38, 0.83] | 0.57 [0.22, 0.99] | -2.40 [-2.96, -1.85]* | -0.55 [-1.04, 0.03] |  | 1.08 [0.62, 1.57] | 1.07 [0.61, 1.55] |  | 0.08 [0.05, 0.12] | h5 |
| 21 |  |  |  |  | 0.00 [-0.20, 0.18] | 0.01 [-0.19, 0.19] | 0.29 [0.07, 0.54] | 2.68 [1.86, 3.68] | 0.69 [0.53, 0.88] | 0.56 [0.33, 0.83] | -2.44 [-3.00, -1.90]* | -0.38 [-0.86, 0.19] |  | 1.18 [0.81, 1.54] | 1.16 [0.80, 1.52] |  | 0.09 [0.05, 0.13] | h5 |
| 30 |  |  |  |  | 0.37 [0.24, 0.48] | 0.38 [0.25, 0.50] | 0.39 [0.22, 0.57] | 3.29 [2.68, 3.99] | 0.43 [0.29, 0.56] | 0.89 [0.46, 1.31] | -1.85 [-2.29, -1.46]* | -1.30 [-1.62, -0.99]* |  | 1.97 [1.22, 2.70] | 1.93 [1.19, 2.65] |  | 0.06 [0.03, 0.10] | h5 |
| 45 |  |  |  |  | 0.01 [-0.16, 0.17] | 0.04 [-0.13, 0.20] | 0.61 [0.38, 0.89] | 1.82 [1.09, 3.00] | -0.02 [-0.24, 0.18] | 0.45 [0.16, 0.75] | -2.50 [-3.09, -1.93]* | -1.35 [-1.76, -0.93]* |  | 1.20 [0.73, 1.62] | 1.17 [0.70, 1.60] |  | 0.14 [0.10, 0.19] | h5 |
| 60 |  |  |  |  | 0.01 [-0.16, 0.15] | 0.04 [-0.14, 0.21] | 0.14 [-0.16, 0.45] |  | 0.44 [0.22, 0.65] | 0.08 [0.01, 0.89] | -2.66 [-3.22, -2.08]* | -0.90 [-1.43, -0.37]* |  | -0.07 [-0.44, 0.56] | -0.07 [-0.44, 0.59] |  | 0.07 [0.04, 0.10] | h5 |
| 90 |  |  |  |  | 0.07 [-0.11, 0.22] | 0.08 [-0.12, 0.27] | 0.36 [-0.05, 0.83] |  | 0.51 [0.28, 0.74] |  | -3.01 [-3.73, -2.26]* | -0.64 [-1.13, -0.25]* |  |  |  |  | 0.09 [0.06, 0.12] | h5 |

segment `busy`:

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -2.08 [-2.81, -1.43]* | -2.34 [-2.95, -1.78]* | -1.43 [-2.23, -0.68]* | -2.36 [-3.20, -1.62]* |  |  | 1.52 [1.06, 2.07] | 5.11 [2.86, 7.93] | 0.58 [0.13, 0.90] | -0.35 [-1.09, 0.24] | -4.61 [-5.68, -3.53]* | -2.36 [-2.91, -1.84]* |  |  |  | 2.23 [1.40, 2.87] | 0.12 [0.06, 0.19] | h5 |
| m015 | -8.23 [-9.66, -6.64]* |  |  |  |  |  | 1.42 [0.91, 2.05] |  | 0.59 [0.16, 0.91] | -0.38 [-1.18, 0.22] |  |  | -9.36 [-10.87, -7.71]* |  |  | 2.15 [1.33, 2.82] | -0.02 [-0.14, 0.11] | h5 |
| m030 | -6.35 [-7.66, -4.97]* |  |  |  |  |  | 1.46 [0.99, 2.04] |  | 0.61 [0.21, 0.91] | -0.25 [-1.00, 0.35] |  |  | -6.03 [-7.50, -4.47]* |  |  | 2.17 [1.40, 2.77] | 0.09 [0.03, 0.15] | h5 |
| m045 | -5.63 [-6.87, -4.42]* |  |  |  |  |  | 1.43 [0.96, 2.03] |  | 0.60 [0.17, 0.90] | -0.25 [-0.99, 0.34] |  |  | -4.31 [-5.81, -2.91]* |  |  | 2.16 [1.32, 2.80] | 0.09 [0.03, 0.17] | h5 |
| m060 | -5.40 [-6.60, -4.15]* |  |  |  |  |  | 1.40 [0.98, 1.92] |  | 0.55 [0.10, 0.87] | -0.34 [-1.04, 0.21] |  |  | -3.25 [-4.82, -1.84]* |  |  | 2.22 [1.37, 2.83] | 0.08 [0.02, 0.14] | h5 |
| m075 | -4.97 [-6.13, -3.75]* |  |  |  |  |  | 1.52 [1.07, 2.05] |  | 0.54 [0.08, 0.87] | -0.44 [-1.20, 0.16] |  |  | -2.75 [-4.24, -1.35]* |  |  | 2.31 [1.44, 2.97] | 0.14 [0.01, 0.28] | h5 |
| m090 | -4.70 [-5.93, -3.51]* |  |  |  |  |  | 1.65 [1.15, 2.26] |  | 0.60 [0.14, 0.92] | -0.36 [-1.11, 0.21] |  |  | -1.67 [-2.98, -0.42]* |  |  | 2.35 [1.41, 3.05] | 0.22 [0.09, 0.37] | h5 |
| m105 | -4.58 [-5.82, -3.36]* |  |  |  |  |  | 1.65 [1.19, 2.23] |  | 0.64 [0.18, 0.98] | -0.35 [-1.15, 0.27] |  |  | -1.25 [-2.93, 0.25] |  |  | 2.27 [1.41, 2.93] | 0.16 [0.07, 0.28] | h5 |
| m120 | -4.49 [-5.71, -3.30]* |  |  |  |  |  | 1.64 [1.17, 2.23] |  | 0.57 [0.11, 0.90] | -0.41 [-1.17, 0.18] |  |  | -0.53 [-2.08, 0.87] |  |  | 2.27 [1.33, 2.99] | 0.18 [0.07, 0.33] | h5 |
| h2-4 | -3.98 [-5.07, -2.92]* |  |  |  |  |  | 1.53 [1.07, 2.09] |  | 0.61 [0.14, 0.94] | -0.37 [-1.14, 0.24] |  |  | 1.27 [-0.10, 2.56] |  |  | 2.24 [1.40, 2.87] | 0.11 [0.06, 0.17] | h5 |
| h4-8 | -2.94 [-3.77, -2.12]* |  |  |  |  |  | 1.50 [1.08, 2.02] |  | 0.61 [0.16, 0.92] | -0.27 [-1.00, 0.33] |  |  | 5.60 [3.99, 7.33] |  |  | 2.37 [1.60, 2.94] | 0.13 [0.07, 0.22] | h5 |
| h8+ | -1.20 [-1.72, -0.76]* |  |  |  |  |  | 1.32 [0.92, 1.79] |  | 0.62 [0.25, 0.91] | 0.16 [-0.32, 0.68] |  |  | 11.06 [7.88, 15.12] |  |  | 2.59 [1.90, 3.11] | 0.13 [0.02, 0.25] | h5 |
| 1 | -1.14 [-1.96, -0.41]* | -1.31 [-1.95, -0.76]* | -0.12 [-0.95, 0.61] | -1.35 [-2.25, -0.59]* | -0.94 [-1.40, -0.55]* | -0.95 [-1.41, -0.55]* | 1.30 [0.87, 1.96] | 4.78 [2.56, 7.47] | 0.39 [0.05, 0.68] | -0.09 [-1.02, 0.64] | -4.24 [-5.30, -3.32]* | -3.08 [-3.54, -2.57]* |  |  |  | 2.40 [1.71, 2.99] | 0.13 [0.06, 0.21] | h5 |
| 2 | 1.38 [0.97, 1.94] | 1.21 [0.71, 1.88] | 3.31 [2.37, 4.79] | 1.14 [0.71, 1.71] | 0.33 [0.09, 0.63] | 0.35 [0.11, 0.66] | 0.85 [0.55, 1.17] | 6.51 [4.94, 8.41] | 0.27 [0.10, 0.48] | 0.57 [-0.01, 1.11] | -3.34 [-4.07, -2.60]* | -3.63 [-4.12, -3.16]* |  |  |  | 2.69 [2.10, 3.20] | 0.13 [0.06, 0.22] | h5 |
| 3 | 0.16 [-0.21, 0.51] | -0.12 [-0.51, 0.30] | 1.32 [0.67, 1.96] | 0.01 [-0.39, 0.37] | -0.40 [-0.75, -0.07]* | -0.38 [-0.74, -0.05]* | 1.32 [1.00, 1.66] | 5.15 [3.44, 6.96] | -0.25 [-0.57, 0.04] | -0.12 [-0.56, 0.34] | -4.60 [-5.45, -3.65]* | -3.76 [-4.14, -3.34]* |  | 1.32 [0.67, 2.02] | 1.25 [0.60, 1.97] | 2.95 [2.14, 3.66] | 0.25 [0.20, 0.31] | h5 |
| 4 | -0.54 [-1.23, -0.06]* | -0.91 [-1.48, -0.50]* | 0.56 [-0.40, 1.29] | -0.71 [-1.34, -0.24]* | -1.03 [-1.44, -0.66]* | -1.00 [-1.41, -0.64]* | 0.95 [0.55, 1.42] | 4.85 [2.52, 7.56] | 0.19 [-0.10, 0.47] | -0.30 [-1.07, 0.34] | -5.22 [-6.29, -4.22]* | -3.19 [-3.63, -2.75]* |  | 0.63 [-0.39, 1.64] | 0.57 [-0.44, 1.57] | 2.41 [1.58, 3.10] | 0.10 [0.04, 0.16] | h5 |
| 5 | -0.40 [-1.44, 0.28] | -0.90 [-1.74, -0.29]* | 0.72 [-0.49, 1.58] | -0.40 [-1.38, 0.26] | -0.93 [-1.45, -0.58]* | -0.89 [-1.42, -0.54]* | 1.09 [0.65, 1.62] | 4.50 [2.43, 6.74] | 0.06 [-0.35, 0.38] | -0.40 [-1.36, 0.44] | -5.30 [-6.27, -4.44]* | -3.08 [-3.61, -2.48]* |  | 0.37 [-1.02, 1.50] | 0.32 [-1.06, 1.43] | 1.92 [1.08, 2.56] | 0.13 [0.08, 0.20] | h5 |
| 6 | -0.63 [-1.51, 0.00] | -0.97 [-1.68, -0.49]* | 0.37 [-0.60, 1.14] | -0.71 [-1.55, -0.11]* | -0.94 [-1.44, -0.54]* | -0.92 [-1.40, -0.52]* | 1.11 [0.64, 1.71] | 4.52 [2.34, 7.20] | -0.18 [-0.59, 0.12] | -0.19 [-0.93, 0.49] | -5.29 [-6.49, -4.12]* | -3.37 [-3.83, -2.94]* |  | 0.80 [-0.08, 1.47] | 0.76 [-0.12, 1.42] | 1.44 [0.62, 2.01] | 0.19 [0.11, 0.28] | h5 |
| 7 | -0.25 [-1.13, 0.39] | -0.61 [-1.27, -0.09]* | 0.80 [-0.16, 1.53] | -0.35 [-1.18, 0.30] | -0.69 [-1.22, -0.32]* | -0.67 [-1.20, -0.29]* | 1.09 [0.56, 1.79] | 5.12 [2.86, 7.77] | 0.78 [0.25, 1.22] | 0.27 [-0.37, 0.81] | -5.30 [-6.52, -4.09]* | -2.09 [-2.82, -1.34]* |  | 1.38 [0.67, 1.93] | 1.34 [0.63, 1.89] |  | 0.16 [0.09, 0.24] | h5 |
| 10 |  |  |  |  | -0.28 [-0.73, 0.10] | -0.23 [-0.67, 0.14] | 1.29 [0.92, 1.71] | 5.33 [3.49, 7.17] | -0.37 [-0.76, -0.04]* | 0.25 [-0.39, 0.81] | -4.88 [-5.70, -3.91]* | -3.86 [-4.27, -3.39]* |  | 2.02 [1.14, 2.79] | 1.96 [1.07, 2.75] |  | 0.28 [0.22, 0.35] | h5 |
| 14 |  |  |  |  | -0.44 [-0.96, -0.05]* | -0.41 [-0.93, -0.04]* | 0.76 [0.21, 1.36] | 5.16 [2.74, 7.79] | 0.96 [0.41, 1.49] | 0.62 [-0.30, 1.59] | -5.75 [-7.10, -4.41]* | -1.83 [-2.64, -0.89]* |  | 1.81 [0.70, 2.83] | 1.76 [0.66, 2.77] |  | 0.19 [0.11, 0.29] | h5 |
| 21 |  |  |  |  | -0.32 [-0.87, 0.08] | -0.27 [-0.84, 0.12] | 0.61 [0.06, 1.25] | 4.90 [2.47, 7.60] | 1.18 [0.78, 1.57] | 0.43 [-0.12, 1.02] | -5.84 [-7.14, -4.61]* | -1.50 [-2.24, -0.51]* |  | 1.88 [1.16, 2.61] | 1.83 [1.11, 2.56] |  | 0.20 [0.12, 0.32] | h5 |
| 30 |  |  |  |  | 0.51 [0.23, 0.75] | 0.54 [0.27, 0.78] | 0.95 [0.58, 1.33] | 6.26 [4.63, 8.10] | 0.78 [0.52, 1.00] | 1.40 [0.67, 2.18] | -4.01 [-4.81, -3.23]* | -2.97 [-3.49, -2.52]* |  | 3.93 [2.72, 5.23] | 3.83 [2.61, 5.13] |  | 0.19 [0.12, 0.28] | h5 |
| 45 |  |  |  |  | -0.50 [-0.93, -0.12]* | -0.43 [-0.87, -0.05]* | 1.35 [0.87, 1.94] | 3.01 [1.52, 4.81] | -0.70 [-1.18, -0.25]* | 0.03 [-0.62, 0.77] | -6.17 [-7.14, -5.15]* | -3.90 [-4.40, -3.34]* |  | 1.86 [1.01, 2.76] | 1.78 [0.90, 2.70] |  | 0.34 [0.25, 0.43] | h5 |
| 60 |  |  |  |  | -0.27 [-0.71, 0.09] | -0.19 [-0.68, 0.25] | 0.28 [-0.50, 0.97] |  | 0.61 [0.08, 1.15] |  | -6.11 [-7.11, -5.11]* | -2.47 [-3.27, -1.65]* |  |  |  |  | 0.17 [0.10, 0.24] | h5 |
| 90 |  |  |  |  | -0.28 [-0.84, 0.13] | -0.23 [-0.82, 0.29] | 0.64 [-0.21, 1.71] |  | 0.61 [0.01, 1.13] |  | -7.00 [-8.50, -5.56]* | -2.25 [-3.03, -1.47]* |  |  |  |  | 0.19 [0.13, 0.25] | h5 |

### Paired against `lvlh5_tft` (not the per-lead reference)

MAE difference model − `lvlh5_tft` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.76 [-1.11, -0.46]* | -0.96 [-1.38, -0.62]* | -0.49 [-0.78, -0.23]* | -0.91 [-1.29, -0.56]* |  |  | 0.46 [0.21, 0.74] | -0.14 [-0.35, 0.09] | 2.73 [2.01, 3.74] | 0.36 [0.20, 0.50] | -1.77 [-2.25, -1.37]* | -0.87 [-1.29, -0.46]* |  |  |  | 1.02 [0.73, 1.28] | -0.09 [-0.29, 0.13] | lvlh5_tft |
| m015 | -3.75 [-4.55, -3.00]* |  |  |  |  |  | 0.46 [0.18, 0.78] | -0.10 [-0.32, 0.14] |  | 0.38 [0.20, 0.54] |  |  | -4.43 [-5.31, -3.61]* |  |  | 1.00 [0.73, 1.25] | -0.14 [-0.38, 0.10] | lvlh5_tft |
| m030 | -2.89 [-3.58, -2.30]* |  |  |  |  |  | 0.42 [0.15, 0.70] | -0.17 [-0.40, 0.06] |  | 0.33 [0.17, 0.48] |  |  | -2.92 [-3.63, -2.27]* |  |  | 0.95 [0.66, 1.18] | -0.14 [-0.37, 0.08] | lvlh5_tft |
| m045 | -2.50 [-3.06, -2.00]* |  |  |  |  |  | 0.42 [0.16, 0.70] | -0.16 [-0.38, 0.07] |  | 0.33 [0.18, 0.47] |  |  | -2.07 [-2.70, -1.49]* |  |  | 1.00 [0.69, 1.29] | -0.10 [-0.32, 0.12] | lvlh5_tft |
| m060 | -2.34 [-2.92, -1.84]* |  |  |  |  |  | 0.44 [0.19, 0.74] | -0.12 [-0.33, 0.11] |  | 0.34 [0.18, 0.49] |  |  | -1.54 [-2.19, -0.96]* |  |  | 1.03 [0.73, 1.33] | -0.08 [-0.30, 0.15] | lvlh5_tft |
| m075 | -2.07 [-2.65, -1.59]* |  |  |  |  |  | 0.49 [0.26, 0.76] | -0.11 [-0.33, 0.12] |  | 0.38 [0.22, 0.54] |  |  | -1.10 [-1.76, -0.51]* |  |  | 1.08 [0.76, 1.38] | -0.05 [-0.25, 0.18] | lvlh5_tft |
| m090 | -2.02 [-2.55, -1.54]* |  |  |  |  |  | 0.50 [0.23, 0.78] | -0.16 [-0.39, 0.07] |  | 0.36 [0.20, 0.51] |  |  | -0.71 [-1.34, -0.14]* |  |  | 1.06 [0.73, 1.39] | -0.08 [-0.28, 0.17] | lvlh5_tft |
| m105 | -1.94 [-2.46, -1.47]* |  |  |  |  |  | 0.49 [0.25, 0.75] | -0.15 [-0.38, 0.09] |  | 0.38 [0.21, 0.53] |  |  | -0.45 [-1.11, 0.18] |  |  | 1.04 [0.73, 1.32] | -0.09 [-0.30, 0.16] | lvlh5_tft |
| m120 | -1.87 [-2.35, -1.41]* |  |  |  |  |  | 0.54 [0.29, 0.82] | -0.11 [-0.32, 0.12] |  | 0.38 [0.21, 0.54] |  |  | -0.11 [-0.78, 0.53] |  |  | 1.02 [0.75, 1.26] | -0.05 [-0.25, 0.19] | lvlh5_tft |
| h2-4 | -1.59 [-2.06, -1.18]* |  |  |  |  |  | 0.46 [0.20, 0.74] | -0.14 [-0.36, 0.10] |  | 0.37 [0.21, 0.52] |  |  | 0.78 [0.11, 1.50] |  |  | 1.02 [0.73, 1.30] | -0.10 [-0.32, 0.13] | lvlh5_tft |
| h4-8 | -1.22 [-1.64, -0.84]* |  |  |  |  |  | 0.42 [0.18, 0.71] | -0.18 [-0.40, 0.06] |  | 0.35 [0.18, 0.50] |  |  | 2.72 [1.83, 3.74] |  |  | 1.05 [0.77, 1.32] | -0.12 [-0.34, 0.12] | lvlh5_tft |
| h8+ | -0.74 [-1.14, -0.40]* |  |  |  |  |  | 0.11 [-0.14, 0.42] | -0.42 [-0.64, -0.21]* |  | 0.18 [0.02, 0.31] |  |  | 5.32 [4.05, 7.09] |  |  | 0.92 [0.65, 1.21] | -0.38 [-0.57, -0.18]* | lvlh5_tft |
| 1 | -0.69 [-1.00, -0.45]* | -0.80 [-1.06, -0.58]* | -0.33 [-0.56, -0.08]* | -0.81 [-1.17, -0.52]* | -0.43 [-0.72, -0.19]* | -0.43 [-0.71, -0.20]* | 0.28 [-0.01, 0.63] | -0.26 [-0.55, 0.04] | 2.47 [1.77, 3.36] | 0.12 [-0.12, 0.38] | -1.75 [-2.32, -1.31]* | -1.25 [-1.85, -0.69]* |  |  |  | 0.78 [0.41, 1.14] | -0.19 [-0.46, 0.10] | lvlh5_tft |
| 2 | -0.06 [-0.36, 0.28] | -0.25 [-0.53, 0.07] | 0.64 [0.22, 1.13] | -0.19 [-0.47, 0.14] | -0.35 [-0.56, -0.10]* | -0.33 [-0.55, -0.09]* | -0.14 [-0.45, 0.24] | -0.58 [-0.81, -0.33]* | 2.75 [1.98, 3.67] | -0.33 [-0.58, -0.08]* | -2.01 [-2.44, -1.60]* | -2.09 [-2.48, -1.71]* |  |  |  | 0.56 [0.22, 0.95] | -0.54 [-0.77, -0.29]* | lvlh5_tft |
| 3 | -0.23 [-0.45, -0.01]* | -0.45 [-0.69, -0.23]* | 0.22 [-0.13, 0.59] | -0.30 [-0.50, -0.10]* | -0.30 [-0.44, -0.16]* | -0.29 [-0.43, -0.15]* | 0.27 [-0.05, 0.61] | -0.33 [-0.53, -0.15]* | 2.55 [1.75, 3.52] | -0.23 [-0.40, -0.04]* | -2.00 [-2.44, -1.61]* | -1.65 [-1.95, -1.32]* |  | 0.77 [0.58, 0.98] | 0.75 [0.55, 0.96] | 0.88 [0.41, 1.36] | -0.25 [-0.46, -0.04]* | lvlh5_tft |
| 4 | -0.29 [-0.60, 0.00] | -0.60 [-0.91, -0.29]* | 0.11 [-0.27, 0.49] | -0.38 [-0.67, -0.12]* | -0.48 [-0.69, -0.27]* | -0.46 [-0.67, -0.25]* | 0.16 [-0.17, 0.51] | -0.20 [-0.47, 0.11] | 2.54 [1.79, 3.44] | 0.03 [-0.17, 0.25] | -2.20 [-2.66, -1.71]* | -1.53 [-1.97, -1.09]* |  | 0.44 [0.17, 0.70] | 0.42 [0.15, 0.69] | 0.72 [0.28, 1.15] | -0.18 [-0.46, 0.14] | lvlh5_tft |
| 5 | -0.14 [-0.47, 0.18] | -0.53 [-0.86, -0.20]* | 0.21 [-0.18, 0.61] | -0.19 [-0.50, 0.09] | -0.35 [-0.58, -0.13]* | -0.34 [-0.57, -0.12]* | 0.42 [0.08, 0.85] | -0.11 [-0.46, 0.26] | 2.57 [1.90, 3.38] | 0.09 [-0.21, 0.39] | -2.25 [-2.76, -1.82]* | -1.41 [-1.88, -0.93]* |  | 0.39 [0.11, 0.63] | 0.37 [0.10, 0.61] | 0.76 [0.38, 1.10] | -0.07 [-0.42, 0.30] | lvlh5_tft |
| 6 | -0.31 [-0.58, -0.09]* | -0.64 [-0.87, -0.44]* | -0.00 [-0.31, 0.28] | -0.37 [-0.63, -0.16]* | -0.42 [-0.61, -0.23]* | -0.41 [-0.61, -0.22]* | 0.23 [-0.07, 0.56] | -0.27 [-0.54, 0.02] | 2.37 [1.65, 3.17] | -0.12 [-0.33, 0.08] | -2.20 [-2.67, -1.81]* | -1.49 [-1.94, -1.10]* |  | 0.49 [0.26, 0.70] | 0.47 [0.25, 0.69] | 0.52 [0.19, 0.83] | -0.21 [-0.48, 0.11] | lvlh5_tft |
| 7 | -0.30 [-0.60, -0.04]* | -0.57 [-0.82, -0.33]* | 0.04 [-0.32, 0.37] | -0.35 [-0.63, -0.09]* | -0.52 [-0.73, -0.34]* | -0.51 [-0.72, -0.33]* | 0.04 [-0.27, 0.36] | -0.47 [-0.71, -0.21]* | 2.48 [1.77, 3.33] | 0.10 [-0.16, 0.35] | -2.37 [-2.85, -1.91]* | -1.04 [-1.59, -0.48]* |  | 0.53 [0.34, 0.73] | 0.51 [0.33, 0.72] |  | -0.40 [-0.64, -0.13]* | lvlh5_tft |
| 10 |  |  |  |  | -0.43 [-0.63, -0.21]* | -0.41 [-0.60, -0.19]* | 0.19 [-0.23, 0.69] | -0.47 [-0.73, -0.18]* | 2.55 [1.73, 3.48] | -0.34 [-0.55, -0.10]* | -2.33 [-2.90, -1.81]* | -1.72 [-2.08, -1.34]* |  | 0.79 [0.55, 1.03] | 0.77 [0.53, 1.01] |  | -0.38 [-0.64, -0.08]* | lvlh5_tft |
| 14 |  |  |  |  | -0.56 [-0.96, -0.24]* | -0.56 [-0.96, -0.24]* | -0.25 [-0.68, 0.18] | -0.57 [-0.99, -0.22]* | 2.34 [1.65, 3.10] | 0.06 [-0.44, 0.52] | -2.69 [-3.31, -2.11]* | -1.01 [-1.77, -0.31]* |  | 0.52 [0.30, 0.73] | 0.50 [0.29, 0.72] |  | -0.50 [-0.91, -0.14]* | lvlh5_tft |
| 21 |  |  |  |  | -0.47 [-0.80, -0.20]* | -0.47 [-0.79, -0.19]* | -0.32 [-0.73, 0.02] | -0.56 [-0.83, -0.33]* | 2.22 [1.47, 3.09] | 0.12 [-0.17, 0.46] | -2.80 [-3.38, -2.25]* | -0.91 [-1.51, -0.30]* |  | 0.63 [0.39, 0.89] | 0.62 [0.37, 0.88] |  | -0.48 [-0.76, -0.25]* | lvlh5_tft |
| 30 |  |  |  |  | -0.68 [-1.06, -0.30]* | -0.67 [-1.06, -0.30]* | -0.63 [-1.05, -0.09]* | -0.89 [-1.31, -0.46]* | 2.26 [1.40, 3.15] | -0.59 [-1.04, -0.13]* | -2.69 [-3.22, -2.13]* | -2.25 [-2.86, -1.64]* |  | 0.98 [0.59, 1.39] | 0.94 [0.56, 1.34] |  | -0.84 [-1.25, -0.41]* | lvlh5_tft |
| 45 |  |  |  |  | -0.33 [-0.55, -0.14]* | -0.31 [-0.54, -0.10]* | 0.08 [-0.41, 0.67] | -0.45 [-0.75, -0.16]* | 1.27 [0.54, 2.27] | -0.16 [-0.47, 0.13] | -2.64 [-3.26, -2.08]* | -1.48 [-1.88, -1.06]* |  | 0.75 [0.41, 1.05] | 0.71 [0.38, 1.01] |  | -0.34 [-0.68, -0.03]* | lvlh5_tft |
| 60 |  |  |  |  | -0.10 [-1.03, -0.02] | -0.10 [-1.04, -0.02] | -0.22 [-4.79, 1.24] | -0.08 [-0.89, -0.01] |  | 0.01 [-1.00, 0.23] | -0.09 [-0.89, 1.50] | 0.05 [-1.07, 1.37] |  | -0.14 [-1.31, 0.43] | -0.14 [-1.31, 0.46] |  | -0.09 [-1.05, -0.01] | lvlh5_tft |

segment `busy`:

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.26 [-2.08, -0.59]* | -1.65 [-2.56, -0.87]* | -0.59 [-1.24, -0.08]* | -1.64 [-2.50, -0.91]* |  |  | 1.67 [1.08, 2.40] | 0.35 [-0.24, 1.09] | 5.60 [3.73, 8.13] | 1.05 [0.62, 1.61] | -3.74 [-4.80, -2.79]* | -1.94 [-2.63, -0.99]* |  |  |  | 2.39 [1.85, 2.92] | 0.47 [-0.08, 1.24] | lvlh5_tft |
| m015 | -7.23 [-8.54, -5.73]* |  |  |  |  |  | 1.57 [0.83, 2.44] | 0.38 [-0.22, 1.18] |  | 1.09 [0.58, 1.71] |  |  | -8.39 [-9.65, -6.83]* |  |  | 2.37 [1.82, 2.96] | 0.30 [-0.36, 1.12] | lvlh5_tft |
| m030 | -5.41 [-6.65, -4.12]* |  |  |  |  |  | 1.52 [0.90, 2.26] | 0.25 [-0.35, 1.00] |  | 0.96 [0.53, 1.51] |  |  | -5.17 [-6.45, -3.81]* |  |  | 2.23 [1.70, 2.77] | 0.32 [-0.25, 1.04] | lvlh5_tft |
| m045 | -4.67 [-5.79, -3.65]* |  |  |  |  |  | 1.47 [0.85, 2.21] | 0.25 [-0.34, 0.99] |  | 0.96 [0.56, 1.49] |  |  | -3.53 [-4.87, -2.30]* |  |  | 2.25 [1.65, 2.87] | 0.38 [-0.20, 1.12] | lvlh5_tft |
| m060 | -4.35 [-5.53, -3.26]* |  |  |  |  |  | 1.52 [0.89, 2.23] | 0.34 [-0.21, 1.04] |  | 0.99 [0.58, 1.49] |  |  | -2.44 [-3.83, -1.22]* |  |  | 2.39 [1.76, 3.02] | 0.44 [-0.12, 1.16] | lvlh5_tft |
| m075 | -3.80 [-5.00, -2.75]* |  |  |  |  |  | 1.80 [1.26, 2.52] | 0.44 [-0.16, 1.20] |  | 1.12 [0.67, 1.73] |  |  | -1.78 [-3.14, -0.53]* |  |  | 2.63 [2.00, 3.22] | 0.62 [0.03, 1.46] | lvlh5_tft |
| m090 | -3.66 [-4.78, -2.65]* |  |  |  |  |  | 1.85 [1.27, 2.56] | 0.36 [-0.21, 1.11] |  | 1.09 [0.64, 1.71] |  |  | -0.86 [-2.13, 0.32] |  |  | 2.54 [1.92, 3.11] | 0.61 [0.03, 1.37] | lvlh5_tft |
| m105 | -3.51 [-4.66, -2.48]* |  |  |  |  |  | 1.83 [1.22, 2.60] | 0.35 [-0.27, 1.15] |  | 1.12 [0.61, 1.76] |  |  | -0.38 [-2.01, 1.06] |  |  | 2.40 [1.81, 3.02] | 0.53 [-0.07, 1.36] | lvlh5_tft |
| m120 | -3.41 [-4.46, -2.42]* |  |  |  |  |  | 1.86 [1.27, 2.61] | 0.41 [-0.18, 1.17] |  | 1.12 [0.62, 1.73] |  |  | 0.28 [-1.28, 1.67] |  |  | 2.38 [1.92, 2.85] | 0.61 [0.04, 1.45] | lvlh5_tft |
| h2-4 | -2.96 [-4.05, -2.05]* |  |  |  |  |  | 1.68 [1.07, 2.43] | 0.37 [-0.24, 1.14] |  | 1.10 [0.65, 1.68] |  |  | 1.87 [0.36, 3.24] |  |  | 2.42 [1.86, 3.01] | 0.48 [-0.13, 1.28] | lvlh5_tft |
| h4-8 | -2.19 [-3.15, -1.34]* |  |  |  |  |  | 1.61 [1.01, 2.34] | 0.27 [-0.33, 1.00] |  | 1.02 [0.57, 1.59] |  |  | 5.47 [3.73, 7.23] |  |  | 2.40 [1.82, 2.96] | 0.42 [-0.15, 1.17] | lvlh5_tft |
| h8+ | -1.05 [-1.95, -0.40]* |  |  |  |  |  | 1.08 [0.52, 1.72] | -0.16 [-0.68, 0.32] |  | 0.55 [0.24, 0.92] |  |  | 10.39 [7.54, 13.93] |  |  | 2.21 [1.67, 2.73] | -0.03 [-0.46, 0.42] | lvlh5_tft |
| 1 | -0.87 [-1.66, -0.28]* | -1.05 [-1.61, -0.57]* | 0.08 [-0.48, 0.58] | -1.16 [-2.15, -0.43]* | -0.72 [-1.41, -0.12]* | -0.74 [-1.43, -0.13]* | 1.23 [0.55, 1.97] | 0.09 [-0.64, 1.02] | 5.04 [3.45, 7.26] | 0.45 [-0.09, 1.10] | -3.76 [-4.90, -2.71]* | -2.93 [-3.79, -1.84]* |  |  |  | 2.28 [1.47, 2.96] | 0.27 [-0.45, 1.18] | lvlh5_tft |
| 2 | 0.40 [-0.29, 1.20] | 0.12 [-0.54, 0.90] | 2.00 [1.04, 3.16] | 0.08 [-0.58, 0.86] | -0.48 [-0.95, 0.06] | -0.44 [-0.90, 0.10] | 0.45 [-0.32, 1.37] | -0.57 [-1.11, 0.01] | 6.03 [4.28, 8.39] | -0.31 [-0.85, 0.24] | -4.15 [-4.92, -3.36]* | -4.18 [-4.71, -3.56]* |  |  |  | 1.94 [1.19, 2.63] | -0.41 [-0.99, 0.17] | lvlh5_tft |
| 3 | 0.22 [-0.25, 0.67] | -0.23 [-0.70, 0.30] | 1.25 [0.41, 2.01] | 0.07 [-0.37, 0.49] | -0.37 [-0.75, -0.01]* | -0.34 [-0.72, 0.03] | 1.53 [0.86, 2.20] | 0.12 [-0.34, 0.56] | 5.39 [3.43, 7.43] | -0.11 [-0.54, 0.28] | -4.33 [-5.08, -3.58]* | -3.55 [-3.98, -3.12]* |  | 1.52 [1.20, 1.91] | 1.46 [1.14, 1.85] | 2.90 [1.95, 3.62] | 0.35 [-0.14, 0.80] | lvlh5_tft |
| 4 | -0.05 [-0.74, 0.65] | -0.54 [-1.19, 0.14] | 1.00 [0.12, 1.75] | -0.25 [-0.84, 0.32] | -0.77 [-1.19, -0.35]* | -0.73 [-1.14, -0.31]* | 0.99 [0.30, 1.78] | 0.30 [-0.34, 1.07] | 5.28 [3.30, 7.31] | 0.36 [-0.17, 0.95] | -4.74 [-5.62, -3.74]* | -3.21 [-3.98, -2.37]* |  | 1.01 [0.54, 1.56] | 0.95 [0.48, 1.50] | 2.42 [1.54, 3.17] | 0.37 [-0.31, 1.16] | lvlh5_tft |
| 5 | 0.09 [-0.63, 0.94] | -0.57 [-1.30, 0.33] | 1.00 [0.06, 2.00] | -0.02 [-0.65, 0.65] | -0.61 [-1.15, -0.10]* | -0.56 [-1.13, -0.05]* | 1.51 [0.76, 2.46] | 0.40 [-0.44, 1.36] | 5.25 [3.41, 7.05] | 0.51 [-0.27, 1.33] | -4.72 [-5.56, -3.80]* | -2.81 [-3.74, -1.78]* |  | 0.89 [0.35, 1.37] | 0.84 [0.31, 1.31] | 2.23 [1.62, 2.77] | 0.48 [-0.37, 1.47] | lvlh5_tft |
| 6 | -0.13 [-0.66, 0.35] | -0.73 [-1.15, -0.31]* | 0.70 [-0.04, 1.27] | -0.27 [-0.73, 0.16] | -0.59 [-1.00, -0.15]* | -0.56 [-0.99, -0.11]* | 1.22 [0.54, 2.07] | 0.19 [-0.49, 0.93] | 4.84 [3.01, 6.92] | 0.03 [-0.48, 0.55] | -4.59 [-5.48, -3.75]* | -3.21 [-3.92, -2.43]* |  | 1.06 [0.65, 1.40] | 1.01 [0.62, 1.36] | 1.70 [1.02, 2.39] | 0.35 [-0.33, 1.13] | lvlh5_tft |
| 7 | -0.07 [-0.58, 0.45] | -0.67 [-1.06, -0.23]* | 0.83 [0.15, 1.46] | -0.21 [-0.66, 0.31] | -0.81 [-1.15, -0.44]* | -0.77 [-1.12, -0.39]* | 0.86 [0.34, 1.55] | -0.27 [-0.81, 0.37] | 5.11 [3.19, 7.31] | 0.66 [-0.07, 1.44] | -4.95 [-5.85, -3.99]* | -2.23 [-3.32, -0.93]* |  | 1.16 [0.74, 1.61] | 1.11 [0.69, 1.56] |  | -0.12 [-0.69, 0.57] | lvlh5_tft |
| 10 |  |  |  |  | -0.67 [-1.18, -0.18]* | -0.61 [-1.12, -0.12]* | 1.34 [0.35, 2.45] | -0.25 [-0.81, 0.39] | 5.08 [3.15, 7.19] | -0.43 [-0.88, 0.03] | -5.13 [-6.09, -4.04]* | -3.87 [-4.37, -3.35]* |  | 1.71 [1.32, 2.15] | 1.65 [1.24, 2.10] |  | -0.01 [-0.59, 0.67] | lvlh5_tft |
| 14 |  |  |  |  | -0.86 [-1.84, -0.06]* | -0.84 [-1.80, -0.02]* | 0.11 [-0.83, 1.09] | -0.62 [-1.59, 0.30] | 4.81 [2.91, 6.94] | 0.53 [-0.82, 1.94] | -5.77 [-6.86, -4.53]* | -2.27 [-3.98, -0.48]* |  | 1.22 [0.78, 1.74] | 1.18 [0.73, 1.70] |  | -0.47 [-1.44, 0.48] | lvlh5_tft |
| 21 |  |  |  |  | -0.52 [-1.30, 0.14] | -0.50 [-1.30, 0.16] | 0.11 [-0.62, 0.83] | -0.43 [-1.02, 0.12] | 4.57 [2.38, 7.00] | 0.88 [0.02, 1.90] | -5.91 [-7.18, -4.74]* | -1.88 [-3.21, -0.27]* |  | 1.50 [1.00, 2.09] | 1.44 [0.93, 2.03] |  | -0.24 [-0.84, 0.30] | lvlh5_tft |
| 30 |  |  |  |  | -1.20 [-1.93, -0.45]* | -1.18 [-1.91, -0.44]* | -0.51 [-1.44, 0.66] | -1.40 [-2.18, -0.67]* | 4.58 [2.64, 6.57] | -0.96 [-1.96, 0.00] | -5.65 [-6.57, -4.57]* | -4.76 [-5.88, -3.63]* |  | 2.29 [1.58, 3.03] | 2.19 [1.50, 2.94] |  | -1.21 [-2.02, -0.44]* | lvlh5_tft |
| 45 |  |  |  |  | -0.41 [-0.97, 0.04] | -0.35 [-0.90, 0.10] | 1.48 [0.17, 2.74] | -0.03 [-0.77, 0.62] | 2.82 [1.38, 4.37] | -0.07 [-0.78, 0.49] | -5.87 [-6.85, -4.72]* | -3.39 [-4.10, -2.77]* |  | 1.83 [1.31, 2.31] | 1.73 [1.22, 2.23] |  | 0.28 [-0.57, 1.00] | lvlh5_tft |

### Paired against `persistence` (not the per-lead reference)

MAE difference model − `persistence` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| m015 | 0.26 [0.18, 0.36] | 5.40 [4.41, 6.34] | 4.70 [3.84, 5.51] | 5.19 [4.29, 6.06] | 4.43 [3.61, 5.31] | 5.96 [5.01, 6.89] | 4.66 [3.82, 5.46] | persistence |
| m030 | -0.42 [-0.61, -0.21]* | 3.75 [2.95, 4.55] | 3.02 [2.35, 3.70] | 3.53 [2.79, 4.27] | 2.92 [2.27, 3.63] | 4.33 [3.59, 5.06] | 3.04 [2.37, 3.73] | persistence |
| m045 | -0.90 [-1.17, -0.62]* | 2.83 [2.15, 3.56] | 2.10 [1.48, 2.73] | 2.62 [1.97, 3.28] | 2.07 [1.49, 2.70] | 3.45 [2.82, 4.06] | 2.13 [1.52, 2.77] | persistence |
| m060 | -1.26 [-1.60, -0.93]* | 2.26 [1.58, 2.94] | 1.56 [0.94, 2.17] | 2.04 [1.38, 2.76] | 1.54 [0.96, 2.19] | 2.90 [2.29, 3.55] | 1.57 [0.97, 2.19] | persistence |
| m075 | -1.56 [-1.96, -1.15]* | 1.95 [1.22, 2.66] | 1.18 [0.57, 1.80] | 1.66 [1.00, 2.35] | 1.10 [0.51, 1.76] | 2.57 [1.95, 3.20] | 1.22 [0.57, 1.86] | persistence |
| m090 | -1.88 [-2.39, -1.40]* | 1.48 [0.82, 2.12] | 0.67 [0.09, 1.26] | 1.19 [0.55, 1.85] | 0.71 [0.14, 1.34] | 2.10 [1.51, 2.71] | 0.74 [0.12, 1.35] | persistence |
| m105 | -2.06 [-2.64, -1.56]* | 1.27 [0.56, 2.00] | 0.42 [-0.22, 1.08] | 0.95 [0.22, 1.70] | 0.45 [-0.18, 1.11] | 1.85 [1.18, 2.60] | 0.50 [-0.16, 1.17] | persistence |
| m120 | -2.27 [-2.92, -1.71]* | 0.93 [0.23, 1.65] | 0.10 [-0.58, 0.73] | 0.59 [-0.13, 1.31] | 0.11 [-0.53, 0.78] | 1.52 [0.83, 2.26] | 0.16 [-0.51, 0.81] | persistence |
| h2-4 | -3.02 [-3.77, -2.35]* | -0.12 [-0.80, 0.56] | -0.91 [-1.62, -0.27]* | -0.42 [-1.20, 0.29] | -0.78 [-1.50, -0.11]* | 0.58 [-0.12, 1.28] | -0.86 [-1.57, -0.22]* | persistence |
| h4-8 | -4.74 [-5.91, -3.73]* | -2.52 [-3.48, -1.61]* | -3.31 [-4.37, -2.43]* | -2.76 [-3.94, -1.80]* | -2.72 [-3.74, -1.83]* | -1.65 [-2.83, -0.72]* | -3.26 [-4.34, -2.38]* | persistence |
| h8+ | -6.68 [-8.83, -5.20]* | -5.67 [-7.59, -4.40]* | -6.19 [-8.15, -4.82]* | -5.56 [-7.56, -4.17]* | -5.32 [-7.09, -4.05]* | -4.47 [-6.42, -3.07]* | -6.17 [-8.17, -4.82]* | persistence |

segment `busy`:

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| m015 | 0.44 [0.24, 0.65] | 10.75 [8.73, 12.75] | 9.36 [7.71, 10.87] | 9.89 [8.20, 11.40] | 8.39 [6.83, 9.65] | 11.47 [9.75, 13.02] | 9.31 [7.64, 10.81] | persistence |
| m030 | -0.92 [-1.28, -0.49]* | 7.45 [5.57, 9.36] | 6.03 [4.47, 7.50] | 6.57 [5.03, 8.04] | 5.17 [3.81, 6.45] | 8.21 [6.73, 9.49] | 6.09 [4.50, 7.58] | persistence |
| m045 | -1.87 [-2.27, -1.40]* | 5.72 [4.02, 7.55] | 4.31 [2.91, 5.81] | 4.87 [3.42, 6.37] | 3.53 [2.30, 4.87] | 6.49 [5.16, 7.72] | 4.35 [2.95, 5.86] | persistence |
| m060 | -2.62 [-3.12, -2.10]* | 4.64 [2.98, 6.37] | 3.25 [1.84, 4.82] | 3.77 [2.30, 5.31] | 2.44 [1.22, 3.83] | 5.55 [4.19, 6.94] | 3.29 [1.90, 4.90] | persistence |
| m075 | -3.07 [-3.79, -2.40]* | 4.34 [2.62, 6.09] | 2.75 [1.35, 4.24] | 3.27 [1.85, 4.73] | 1.78 [0.53, 3.14] | 5.12 [3.71, 6.48] | 2.83 [1.36, 4.36] | persistence |
| m090 | -3.76 [-4.65, -2.90]* | 3.34 [1.83, 4.94] | 1.67 [0.42, 2.98] | 2.28 [0.90, 3.66] | 0.86 [-0.32, 2.13] | 4.09 [2.78, 5.38] | 1.83 [0.51, 3.19] | persistence |
| m105 | -4.06 [-5.12, -3.17]* | 3.04 [1.32, 4.89] | 1.25 [-0.25, 2.93] | 1.87 [0.23, 3.63] | 0.38 [-1.06, 2.01] | 3.68 [2.19, 5.49] | 1.45 [-0.06, 3.15] | persistence |
| m120 | -4.51 [-5.73, -3.54]* | 2.30 [0.67, 4.10] | 0.53 [-0.87, 2.08] | 1.12 [-0.43, 2.82] | -0.28 [-1.67, 1.28] | 3.07 [1.47, 4.86] | 0.75 [-0.70, 2.28] | persistence |
| h2-4 | -5.84 [-7.21, -4.69]* | 0.37 [-1.00, 1.97] | -1.27 [-2.56, 0.10] | -0.70 [-2.24, 0.87] | -1.87 [-3.24, -0.36]* | 1.28 [-0.23, 2.88] | -1.15 [-2.45, 0.22] | persistence |
| h4-8 | -8.75 [-10.69, -7.00]* | -3.94 [-5.55, -2.25]* | -5.60 [-7.33, -3.99]* | -4.96 [-6.99, -3.10]* | -5.47 [-7.23, -3.73]* | -2.75 [-4.87, -0.87]* | -5.47 [-7.22, -3.87]* | persistence |
| h8+ | -12.27 [-16.55, -8.95]* | -9.78 [-13.61, -6.87]* | -11.06 [-15.12, -7.88]* | -10.29 [-14.52, -7.13]* | -10.39 [-13.93, -7.54]* | -8.00 [-12.30, -4.69]* | -10.95 [-15.07, -7.92]* | persistence |

### Low-coverage cells (paired rows < 50 % of the reference's)

These absolute values are not comparable with the rest of their column. `reaches_handover` = the cell still passes the 30 % gate and CAN appear as a winner.

| metric | uc | segment | lead | model | ref | value | diff_vs_ref | diff_lo_park | diff_hi_park | paired_share | n_paired | n_units_park | n_origin_days | wins | reaches_handover |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MAE | UC2 | all | 1 | lvlh5_cbd | wt_med | 9.030 | 2.570 | 1.691 | 3.654 | 0.489 | 387094.000 | 29.000 | 33 | False | True |
| MAE | UC2 | busy | 0 | lvlh5_cbd | wt_med | 18.663 | 5.020 | 2.736 | 7.855 | 0.477 | 112478.000 | 29.000 | 33 | False | True |
| MAE | UC2 | busy | 1 | lvlh5_cbd | wt_med | 17.652 | 4.716 | 2.427 | 7.507 | 0.458 | 113001.000 | 29.000 | 33 | False | True |
| MAE | UC2-intraday | all | h8+ | persistence | wt_med | 12.559 | 6.171 | 4.819 | 8.171 | 0.269 | 136130.000 | 26.000 | 33 | False | False |
| MAE | UC2-intraday | busy | h8+ | persistence | wt_med | 22.221 | 10.954 | 7.916 | 15.066 | 0.310 | 51698.000 | 26.000 | 33 | False | True |
| MAE | UC3 | all | 1 | lvlh5_cbd | wt_med | 9.030 | 2.570 | 1.691 | 3.654 | 0.489 | 387094.000 | 29.000 | 33 | False | True |
| MAE | UC3 | all | 21 | lvlh5_cbd | wt_med | 10.318 | 2.622 | 1.768 | 3.646 | 0.480 | 322435.000 | 29.000 | 30 | False | True |
| MAE | UC3 | all | 30 | lvlh5_cbd | wt_med | 10.948 | 3.272 | 2.635 | 3.969 | 0.461 | 342143.000 | 30.000 | 29 | False | True |
| MAE | UC3 | all | 45 | lvlh5_cbd | wt_med | 10.545 | 1.663 | 0.937 | 2.823 | 0.120 | 78482.000 | 9.000 | 27 | False | False |
| MAE | UC3 | all | 30 | lvlh5_tft | wt_med | 8.387 | 0.841 | 0.409 | 1.255 | 0.496 | 368149.000 | 30.000 | 29 | False | True |
| MAE | UC3 | all | 45 | lvlh5_tft | wt_med | 7.849 | 0.344 | 0.026 | 0.678 | 0.372 | 243879.000 | 30.000 | 27 | False | True |
| MAE | UC3 | all | 60 | lvlh5_tft | wt_med | 0.473 | 0.086 | 0.006 | 1.046 | 0.006 | 3343.000 | 4.000 | 25 | False | False |
| MAE | UC3 | all | 45 | prod_served | wt_med | 8.658 | 1.088 | 0.604 | 1.511 | 0.393 | 257593.000 | 30.000 | 27 | False | True |
| MAE | UC3 | all | 60 | prod_served | wt_med | 0.339 | -0.070 | -0.279 | 0.568 | 0.006 | 3160.000 | 4.000 | 25 | False | False |
| MAE | UC3 | all | 45 | prod_served_lin | wt_med | 8.630 | 1.059 | 0.567 | 1.483 | 0.393 | 257593.000 | 30.000 | 27 | False | True |
| MAE | UC3 | all | 60 | prod_served_lin | wt_med | 0.339 | -0.070 | -0.278 | 0.597 | 0.006 | 3160.000 | 4.000 | 25 | False | False |
| MAE | UC3 | busy | 1 | lvlh5_cbd | wt_med | 17.652 | 4.716 | 2.427 | 7.507 | 0.458 | 113001.000 | 29.000 | 33 | False | True |
| MAE | UC3 | busy | 2 | lvlh5_cbd | wt_med | 20.044 | 6.412 | 4.854 | 8.326 | 0.480 | 125908.000 | 29.000 | 33 | False | True |
| MAE | UC3 | busy | 3 | lvlh5_cbd | wt_med | 18.815 | 4.992 | 3.285 | 6.811 | 0.486 | 122512.000 | 29.000 | 33 | False | True |
| MAE | UC3 | busy | 4 | lvlh5_cbd | wt_med | 19.614 | 4.782 | 2.421 | 7.564 | 0.481 | 115229.000 | 29.000 | 33 | False | True |
| MAE | UC3 | busy | 5 | lvlh5_cbd | wt_med | 19.598 | 4.445 | 2.354 | 6.694 | 0.482 | 111611.000 | 29.000 | 33 | False | True |
| MAE | UC3 | busy | 6 | lvlh5_cbd | wt_med | 18.667 | 4.411 | 2.212 | 7.153 | 0.484 | 113179.000 | 29.000 | 33 | False | True |
| MAE | UC3 | busy | 7 | lvlh5_cbd | wt_med | 19.463 | 5.018 | 2.699 | 7.727 | 0.484 | 109272.000 | 29.000 | 32 | False | True |
| MAE | UC3 | busy | 10 | lvlh5_cbd | wt_med | 19.517 | 5.168 | 3.328 | 6.973 | 0.488 | 118461.000 | 29.000 | 32 | False | True |
| MAE | UC3 | busy | 14 | lvlh5_cbd | wt_med | 20.017 | 5.060 | 2.605 | 7.766 | 0.461 | 99967.000 | 29.000 | 31 | False | True |
| MAE | UC3 | busy | 21 | lvlh5_cbd | wt_med | 20.317 | 4.785 | 2.289 | 7.544 | 0.442 | 92311.000 | 29.000 | 30 | False | True |
| MAE | UC3 | busy | 30 | lvlh5_cbd | wt_med | 20.920 | 6.136 | 4.438 | 8.001 | 0.434 | 97706.000 | 30.000 | 29 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_cbd | wt_med | 17.116 | 2.744 | 1.247 | 4.570 | 0.164 | 32862.000 | 9.000 | 27 | False | False |
| MAE | UC3 | busy | 30 | lvlh5_tft | wt_med | 16.023 | 1.215 | 0.439 | 2.024 | 0.453 | 101942.000 | 30.000 | 29 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_tft | wt_med | 15.638 | -0.280 | -1.004 | 0.568 | 0.334 | 67157.000 | 30.000 | 27 | False | True |
| MAE | UC3 | busy | 30 | prod_served | wt_med | 18.646 | 3.737 | 2.512 | 5.065 | 0.467 | 105087.000 | 30.000 | 29 | False | True |
| MAE | UC3 | busy | 45 | prod_served | wt_med | 17.294 | 1.539 | 0.619 | 2.509 | 0.365 | 73405.000 | 30.000 | 27 | False | True |
| MAE | UC3 | busy | 30 | prod_served_lin | wt_med | 18.546 | 3.635 | 2.403 | 4.959 | 0.467 | 105087.000 | 30.000 | 29 | False | True |
| MAE | UC3 | busy | 45 | prod_served_lin | wt_med | 17.207 | 1.452 | 0.513 | 2.456 | 0.365 | 73405.000 | 30.000 | 27 | False | True |
| MAE, schedule known at origin | UC3 | all | 1 | lvlh5_cbd | wt_med | 9.451 | 2.915 | 1.840 | 4.189 | 0.492 | 294766.000 | 26.000 | 33 | False | True |
| MAE, schedule known at origin | UC3 | all | 14 | lvlh5_cbd | wt_med | 10.221 | 2.685 | 1.590 | 3.983 | 0.496 | 249298.000 | 25.000 | 31 | False | True |
| MAE, schedule known at origin | UC3 | all | 21 | lvlh5_cbd | wt_med | 10.318 | 2.572 | 1.477 | 3.862 | 0.482 | 224421.000 | 24.000 | 30 | False | True |
| MAE, schedule known at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 11.198 | 3.766 | 3.081 | 4.581 | 0.479 | 216051.000 | 23.000 | 29 | False | True |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 13.990 | 1.414 | 0.834 | 2.539 | 0.064 | 24240.000 | 5.000 | 27 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 7.516 | 0.467 | 0.077 | 0.864 | 0.373 | 142182.000 | 22.000 | 27 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 0.426 | 0.058 | 0.006 | 1.071 | 0.012 | 3072.000 | 2.000 | 25 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served | wt_med | 8.414 | 1.149 | 0.578 | 1.677 | 0.388 | 147806.000 | 22.000 | 27 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served | wt_med | 0.306 | -0.085 | -0.317 | -0.072 | 0.011 | 2892.000 | 2.000 | 25 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served_lin | wt_med | 8.382 | 1.116 | 0.550 | 1.649 | 0.388 | 147806.000 | 22.000 | 27 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served_lin | wt_med | 0.306 | -0.085 | -0.317 | -0.072 | 0.011 | 2892.000 | 2.000 | 25 | False | False |
| MAE, schedule projected at origin | UC3 | all | 0 | lvlh5_cbd | wt_med | 7.907 | 2.030 | 1.499 | 3.017 | 0.482 | 82136.000 | 7.000 | 33 | False | True |
| MAE, schedule projected at origin | UC3 | all | 1 | lvlh5_cbd | wt_med | 7.681 | 1.470 | 0.757 | 2.801 | 0.481 | 92328.000 | 10.000 | 33 | False | True |
| MAE, schedule projected at origin | UC3 | all | 2 | lvlh5_cbd | wt_med | 8.717 | 2.256 | 1.711 | 2.959 | 0.489 | 109528.000 | 12.000 | 33 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | lvlh5_cbd | wt_med | 8.801 | 1.874 | 1.394 | 3.032 | 0.485 | 95888.000 | 11.000 | 33 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | lvlh5_cbd | wt_med | 8.247 | 1.828 | 1.253 | 2.674 | 0.499 | 92072.000 | 11.000 | 33 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | lvlh5_cbd | wt_med | 8.082 | 1.893 | 0.846 | 3.127 | 0.497 | 92905.000 | 10.000 | 33 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | lvlh5_cbd | wt_med | 10.320 | 2.737 | 1.704 | 4.400 | 0.474 | 98014.000 | 12.000 | 30 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 10.509 | 2.427 | 1.196 | 3.738 | 0.432 | 126092.000 | 18.000 | 29 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 8.983 | 1.774 | 0.907 | 3.505 | 0.198 | 54242.000 | 8.000 | 27 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_tft | wt_med | 9.253 | 1.266 | 0.289 | 2.187 | 0.466 | 135954.000 | 20.000 | 29 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 8.322 | 0.170 | -0.448 | 0.572 | 0.370 | 101697.000 | 21.000 | 27 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 1.011 | 0.402 | 0.374 | 0.534 | 0.001 | 271.000 | 2.000 | 25 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served | wt_med | 10.809 | 2.800 | 1.000 | 4.454 | 0.473 | 138096.000 | 20.000 | 29 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served | wt_med | 8.997 | 1.005 | 0.095 | 1.647 | 0.400 | 109787.000 | 21.000 | 27 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served | wt_med | 0.698 | 0.093 | 0.000 | 0.568 | 0.001 | 268.000 | 2.000 | 25 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served_lin | wt_med | 10.768 | 2.759 | 0.958 | 4.418 | 0.473 | 138096.000 | 20.000 | 29 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served_lin | wt_med | 8.973 | 0.982 | 0.071 | 1.631 | 0.400 | 109787.000 | 21.000 | 27 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served_lin | wt_med | 0.702 | 0.098 | 0.000 | 0.597 | 0.001 | 268.000 | 2.000 | 25 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 14 | lvlh5_cbd | wt_med | 0.422 | -0.021 | -0.035 | -0.003 | 0.493 | 5038.000 | 28.000 | 16 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 21 | lvlh5_cbd | wt_med | 0.406 | -0.020 | -0.035 | -0.003 | 0.468 | 4385.000 | 28.000 | 15 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | lvlh5_cbd | wt_med | 0.415 | -0.028 | -0.040 | -0.016 | 0.476 | 4493.000 | 30.000 | 14 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_cbd | wt_med | 0.319 | -0.016 | -0.036 | 0.013 | 0.148 | 1271.000 | 9.000 | 12 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_tft | wt_med | 0.411 | 0.012 | 0.003 | 0.024 | 0.407 | 3497.000 | 30.000 | 11 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | lvlh5_tft | wt_med | 0.114 | 0.023 | -0.044 | 0.057 | 0.000 | 3.000 | 2.000 | 7 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | prod_served | wt_med | 0.429 | -0.014 | -0.030 | 0.002 | 0.497 | 4696.000 | 30.000 | 14 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served | wt_med | 0.429 | 0.008 | -0.007 | 0.024 | 0.414 | 3557.000 | 30.000 | 12 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served | wt_med | 0.289 | 0.050 | -0.059 | 0.104 | 0.000 | 3.000 | 2.000 | 7 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served_lin | wt_med | 0.440 | 0.021 | 0.005 | 0.038 | 0.418 | 3592.000 | 30.000 | 12 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served_lin | wt_med | 0.350 | 0.111 | -0.005 | 0.168 | 0.000 | 3.000 | 2.000 | 7 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 0 | snaive7 | wt_med | 0.426 | -0.080 | -0.094 | -0.064 | 0.432 | 5202.000 | 30.000 | 33 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 1 | snaive7 | wt_med | 0.415 | -0.066 | -0.086 | -0.038 | 0.434 | 5188.000 | 30.000 | 33 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 3 | snaive7 | wt_med | 0.432 | -0.071 | -0.083 | -0.058 | 0.465 | 6284.000 | 30.000 | 33 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 4 | snaive7 | wt_med | 0.420 | -0.078 | -0.089 | -0.065 | 0.447 | 5211.000 | 30.000 | 33 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 5 | snaive7 | wt_med | 0.423 | -0.075 | -0.086 | -0.062 | 0.452 | 5045.000 | 30.000 | 33 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 6 | snaive7 | wt_med | 0.417 | -0.069 | -0.087 | -0.051 | 0.458 | 5125.000 | 30.000 | 33 | False | True |
| best-time hit rate (top-2) | D3 | all | 21 | lvlh5_cbd | wt_med | 0.745 | -0.066 | -0.087 | -0.043 | 0.479 | 4949.000 | 28.000 | 15 | False | True |
| best-time hit rate (top-2) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.701 | -0.116 | -0.137 | -0.097 | 0.484 | 4998.000 | 30.000 | 14 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.705 | -0.050 | -0.087 | -0.001 | 0.143 | 1333.000 | 9.000 | 12 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_tft | wt_med | 0.782 | -0.015 | -0.034 | 0.005 | 0.417 | 3877.000 | 30.000 | 11 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | lvlh5_tft | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served | wt_med | 0.729 | -0.064 | -0.091 | -0.038 | 0.439 | 4089.000 | 30.000 | 12 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served_lin | wt_med | 0.756 | -0.032 | -0.055 | -0.012 | 0.439 | 4089.000 | 30.000 | 12 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time hit rate (top-2) | D3 | all | 0 | snaive7 | wt_med | 0.814 | -0.035 | -0.050 | -0.023 | 0.455 | 6030.000 | 30.000 | 33 | False | True |
| best-time hit rate (top-2) | D3 | all | 1 | snaive7 | wt_med | 0.803 | -0.032 | -0.043 | -0.020 | 0.460 | 6116.000 | 30.000 | 33 | False | True |
| best-time hit rate (top-2) | D3 | all | 3 | snaive7 | wt_med | 0.805 | -0.037 | -0.048 | -0.027 | 0.496 | 7210.000 | 30.000 | 33 | False | True |
| best-time hit rate (top-2) | D3 | all | 4 | snaive7 | wt_med | 0.797 | -0.034 | -0.047 | -0.022 | 0.477 | 6142.000 | 30.000 | 33 | False | True |
| best-time hit rate (top-2) | D3 | all | 5 | snaive7 | wt_med | 0.809 | -0.033 | -0.045 | -0.023 | 0.477 | 5837.000 | 30.000 | 33 | False | True |
| best-time hit rate (top-2) | D3 | all | 6 | snaive7 | wt_med | 0.821 | -0.030 | -0.044 | -0.016 | 0.495 | 6071.000 | 30.000 | 33 | False | True |
| best-time hit rate (±30 min) | D3 | all | 21 | lvlh5_cbd | wt_med | 0.776 | -0.035 | -0.055 | -0.014 | 0.479 | 4949.000 | 28.000 | 15 | False | True |
| best-time hit rate (±30 min) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.743 | -0.084 | -0.105 | -0.063 | 0.484 | 4998.000 | 30.000 | 14 | False | True |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.734 | -0.038 | -0.081 | 0.012 | 0.143 | 1333.000 | 9.000 | 12 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_tft | wt_med | 0.800 | -0.008 | -0.028 | 0.013 | 0.417 | 3877.000 | 30.000 | 11 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | lvlh5_tft | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | prod_served | wt_med | 0.763 | -0.034 | -0.058 | -0.012 | 0.439 | 4089.000 | 30.000 | 12 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | prod_served_lin | wt_med | 0.782 | -0.014 | -0.038 | 0.008 | 0.439 | 4089.000 | 30.000 | 12 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time hit rate (±30 min) | D3 | all | 0 | snaive7 | wt_med | 0.819 | -0.030 | -0.041 | -0.019 | 0.455 | 6030.000 | 30.000 | 33 | False | True |
| best-time hit rate (±30 min) | D3 | all | 1 | snaive7 | wt_med | 0.813 | -0.025 | -0.035 | -0.015 | 0.460 | 6116.000 | 30.000 | 33 | False | True |
| best-time hit rate (±30 min) | D3 | all | 3 | snaive7 | wt_med | 0.817 | -0.025 | -0.035 | -0.016 | 0.496 | 7210.000 | 30.000 | 33 | False | True |
| best-time hit rate (±30 min) | D3 | all | 4 | snaive7 | wt_med | 0.812 | -0.023 | -0.037 | -0.010 | 0.477 | 6142.000 | 30.000 | 33 | False | True |
| best-time hit rate (±30 min) | D3 | all | 5 | snaive7 | wt_med | 0.821 | -0.023 | -0.035 | -0.010 | 0.477 | 5837.000 | 30.000 | 33 | False | True |
| best-time hit rate (±30 min) | D3 | all | 6 | snaive7 | wt_med | 0.828 | -0.022 | -0.035 | -0.010 | 0.495 | 6071.000 | 30.000 | 33 | False | True |
| best-time regret (min) | D3 | all | 21 | lvlh5_cbd | wt_med | 4.498 | 0.988 | 0.547 | 1.445 | 0.479 | 4949.000 | 28.000 | 15 | False | True |
| best-time regret (min) | D3 | all | 30 | lvlh5_cbd | wt_med | 5.320 | 1.832 | 1.318 | 2.333 | 0.484 | 4998.000 | 30.000 | 14 | False | True |
| best-time regret (min) | D3 | all | 45 | lvlh5_cbd | wt_med | 5.093 | 0.803 | -0.033 | 1.472 | 0.143 | 1333.000 | 9.000 | 12 | False | False |
| best-time regret (min) | D3 | all | 45 | lvlh5_tft | wt_med | 3.734 | 0.139 | -0.260 | 0.557 | 0.417 | 3877.000 | 30.000 | 11 | False | True |
| best-time regret (min) | D3 | all | 60 | lvlh5_tft | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time regret (min) | D3 | all | 45 | prod_served | wt_med | 4.762 | 1.052 | 0.422 | 1.690 | 0.439 | 4089.000 | 30.000 | 12 | False | True |
| best-time regret (min) | D3 | all | 60 | prod_served | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time regret (min) | D3 | all | 45 | prod_served_lin | wt_med | 3.963 | 0.231 | -0.280 | 0.770 | 0.439 | 4089.000 | 30.000 | 12 | False | True |
| best-time regret (min) | D3 | all | 60 | prod_served_lin | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 9.000 | 3.000 | 7 | False | False |
| best-time regret (min) | D3 | all | 0 | snaive7 | wt_med | 3.770 | 1.073 | 0.721 | 1.447 | 0.455 | 6030.000 | 30.000 | 33 | False | True |
| best-time regret (min) | D3 | all | 1 | snaive7 | wt_med | 3.905 | 0.783 | 0.407 | 1.217 | 0.460 | 6116.000 | 30.000 | 33 | False | True |
| best-time regret (min) | D3 | all | 3 | snaive7 | wt_med | 3.593 | 0.709 | 0.450 | 0.976 | 0.496 | 7210.000 | 30.000 | 33 | False | True |
| best-time regret (min) | D3 | all | 4 | snaive7 | wt_med | 3.990 | 0.914 | 0.580 | 1.261 | 0.477 | 6142.000 | 30.000 | 33 | False | True |
| best-time regret (min) | D3 | all | 5 | snaive7 | wt_med | 3.648 | 0.818 | 0.450 | 1.136 | 0.477 | 5837.000 | 30.000 | 33 | False | True |
| best-time regret (min) | D3 | all | 6 | snaive7 | wt_med | 3.667 | 0.769 | 0.407 | 1.191 | 0.495 | 6071.000 | 30.000 | 33 | False | True |
| crowd bucket exact | D6 | all | 21 | lvl_cbd | lvl_naive4 | 0.198 | -0.084 | -0.159 | -0.010 | 0.499 | 334.000 | 29.000 | 30 | False | True |
| crowd bucket exact | D6 | all | 30 | lvl_cbd | lvl_naive4 | 0.118 | -0.300 | -0.381 | -0.225 | 0.485 | 330.000 | 30.000 | 29 | False | True |
| crowd bucket exact | D6 | all | 45 | lvl_cbd | lvl_naive4 | 0.250 | -0.167 | -0.288 | -0.030 | 0.154 | 96.000 | 9.000 | 27 | False | False |
| crowd bucket exact | D6 | all | 45 | lvl_tft | lvl_naive4 | 0.381 | -0.004 | -0.100 | 0.102 | 0.412 | 257.000 | 29.000 | 27 | False | True |
| day comparison winner accuracy | D7 | all | d8-30 | lvl_cbd | lvl_naive4 | 0.116 | -0.168 | -0.200 | -0.136 | 0.496 | 88388.000 | 30.000 | 32 | False | True |
| day comparison winner accuracy | D7 | all | d31-90 | lvl_cbd | lvl_naive4 | 0.100 | -0.157 | -0.191 | -0.125 | 0.081 | 96091.000 | 30.000 | 29 | False | False |
| day comparison winner accuracy | D7 | all | d31-90 | lvl_tft | lvl_naive4 | 0.296 | 0.030 | -0.010 | 0.073 | 0.180 | 212885.000 | 30.000 | 29 | False | False |
| dayPeak Spearman across rides | UC3 | all | 1 | lvlh5_cbd | snaive7 | 0.704 | -0.131 | -0.196 | -0.080 | 0.496 | 283.000 | 28.000 | 18 | False | True |
| dayPeak Spearman across rides | UC3 | all | 6 | lvlh5_cbd | snaive7 | 0.673 | -0.133 | -0.192 | -0.081 | 0.492 | 291.000 | 29.000 | 18 | False | True |
| dayPeak Spearman across rides | UC3 | all | 14 | lvlh5_cbd | wt_med | 0.662 | -0.156 | -0.210 | -0.112 | 0.494 | 339.000 | 28.000 | 16 | False | True |
| dayPeak Spearman across rides | UC3 | all | 21 | lvlh5_cbd | wt_med | 0.664 | -0.140 | -0.203 | -0.087 | 0.464 | 298.000 | 28.000 | 15 | False | True |
| dayPeak Spearman across rides | UC3 | all | 30 | lvlh5_cbd | wt_med | 0.651 | -0.192 | -0.248 | -0.142 | 0.473 | 282.000 | 29.000 | 14 | False | True |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_cbd | wt_med | 0.625 | -0.181 | -0.256 | -0.121 | 0.167 | 92.000 | 9.000 | 12 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_tft | wt_med | 0.794 | -0.035 | -0.058 | -0.015 | 0.415 | 229.000 | 30.000 | 11 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | lvlh5_tft | wt_med | 0.274 | -0.376 | -0.376 | -0.376 | 0.007 | 3.000 | 1.000 | 4 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | prod_served | wt_med | 0.838 | 0.000 | -0.012 | 0.015 | 0.446 | 246.000 | 30.000 | 12 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | prod_served | wt_med | 0.903 | 0.001 | 0.001 | 0.001 | 0.007 | 3.000 | 1.000 | 3 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | prod_served_lin | wt_med | 0.839 | 0.001 | -0.010 | 0.014 | 0.446 | 246.000 | 30.000 | 12 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | prod_served_lin | wt_med | 0.903 | 0.001 | 0.001 | 0.001 | 0.007 | 3.000 | 1.000 | 3 | False | False |
| dayPeak abs error (min) | UC3 | all | 2 | lvlh5_cbd | snaive7 | 14.065 | 5.675 | 4.416 | 7.232 | 0.481 | 5291.000 | 29.000 | 18 | False | True |
| dayPeak abs error (min) | UC3 | all | 3 | lvlh5_cbd | wt_med | 12.711 | 5.509 | 4.333 | 6.793 | 0.498 | 10072.000 | 29.000 | 18 | False | True |
| dayPeak abs error (min) | UC3 | all | 5 | lvlh5_cbd | snaive7 | 13.130 | 4.284 | 3.057 | 5.621 | 0.497 | 4678.000 | 29.000 | 18 | False | True |
| dayPeak abs error (min) | UC3 | all | 6 | lvlh5_cbd | snaive7 | 12.605 | 4.196 | 2.862 | 5.618 | 0.480 | 4835.000 | 29.000 | 18 | False | True |
| dayPeak abs error (min) | UC3 | all | 14 | lvlh5_cbd | wt_med | 13.485 | 4.657 | 3.001 | 6.450 | 0.490 | 7987.000 | 29.000 | 16 | False | True |
| dayPeak abs error (min) | UC3 | all | 21 | lvlh5_cbd | wt_med | 13.535 | 4.355 | 2.705 | 6.247 | 0.464 | 6994.000 | 29.000 | 15 | False | True |
| dayPeak abs error (min) | UC3 | all | 30 | lvlh5_cbd | wt_med | 15.355 | 6.138 | 4.912 | 7.338 | 0.460 | 6294.000 | 30.000 | 14 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_cbd | wt_med | 14.690 | 4.523 | 3.081 | 6.314 | 0.128 | 1642.000 | 9.000 | 12 | False | False |
| dayPeak abs error (min) | UC3 | all | 30 | lvlh5_tft | wt_med | 9.469 | 0.181 | -0.497 | 0.805 | 0.500 | 6833.000 | 30.000 | 14 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_tft | wt_med | 8.856 | 0.413 | 0.057 | 0.800 | 0.414 | 5305.000 | 30.000 | 11 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | lvlh5_tft | wt_med | 0.397 | -0.009 | -2.319 | 0.709 | 0.010 | 106.000 | 4.000 | 7 | False | False |
| dayPeak abs error (min) | UC3 | all | 45 | prod_served | wt_med | 8.937 | 0.449 | -0.019 | 0.979 | 0.433 | 5548.000 | 30.000 | 12 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served | wt_med | 0.430 | 0.000 | -0.112 | 1.940 | 0.010 | 100.000 | 4.000 | 7 | False | False |
| dayPeak abs error (min) | UC3 | all | 45 | prod_served_lin | wt_med | 8.911 | 0.432 | -0.064 | 0.975 | 0.433 | 5548.000 | 30.000 | 12 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served_lin | wt_med | 0.430 | 0.000 | -0.112 | 1.940 | 0.010 | 100.000 | 4.000 | 7 | False | False |
| dayPeak abs error (min) | UC3 | all | 0 | snaive7 | wt_med | 8.113 | 0.554 | 0.042 | 1.112 | 0.489 | 9319.000 | 30.000 | 33 | False | True |
| dayPeak abs error (min) | UC3 | all | 1 | snaive7 | wt_med | 8.228 | 0.727 | 0.319 | 1.188 | 0.499 | 9443.000 | 30.000 | 33 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 14 | lvlh5_cbd | wt_med | 0.715 | -0.034 | -0.065 | -0.004 | 0.491 | 9651.000 | 28.000 | 16 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 21 | lvlh5_cbd | wt_med | 0.714 | -0.029 | -0.069 | 0.007 | 0.468 | 8446.000 | 28.000 | 15 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 30 | lvlh5_cbd | wt_med | 0.703 | -0.045 | -0.091 | 0.002 | 0.486 | 7504.000 | 29.000 | 14 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_cbd | wt_med | 0.695 | -0.100 | -0.158 | -0.060 | 0.183 | 2752.000 | 9.000 | 12 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_tft | wt_med | 0.768 | 0.023 | 0.003 | 0.045 | 0.397 | 5976.000 | 30.000 | 9 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | prod_served | wt_med | 0.718 | -0.022 | -0.043 | 0.001 | 0.429 | 6458.000 | 30.000 | 12 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | prod_served_lin | wt_med | 0.749 | 0.007 | -0.015 | 0.031 | 0.429 | 6458.000 | 30.000 | 12 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 0 | snaive7 | wt_med | 0.766 | -0.017 | -0.045 | 0.011 | 0.259 | 5966.000 | 30.000 | 33 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 1 | snaive7 | wt_med | 0.760 | -0.016 | -0.038 | 0.002 | 0.259 | 5726.000 | 30.000 | 33 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 2 | snaive7 | wt_med | 0.771 | -0.016 | -0.030 | -0.003 | 0.291 | 6290.000 | 30.000 | 33 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 3 | snaive7 | wt_med | 0.757 | -0.025 | -0.044 | -0.008 | 0.282 | 6668.000 | 30.000 | 33 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 4 | snaive7 | wt_med | 0.754 | -0.008 | -0.032 | 0.013 | 0.280 | 6266.000 | 30.000 | 33 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 5 | snaive7 | wt_med | 0.766 | 0.002 | -0.014 | 0.016 | 0.286 | 6145.000 | 30.000 | 33 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 6 | snaive7 | wt_med | 0.761 | -0.000 | -0.024 | 0.024 | 0.276 | 5969.000 | 30.000 | 33 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 0 | lvlh5_cbd | wt_med | 4.158 | -0.195 | -0.512 | 0.017 | 0.492 | 26319.000 | 29.000 | 33 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 1 | lvlh5_cbd | wt_med | 4.044 | -0.298 | -0.768 | 0.010 | 0.470 | 25302.000 | 29.000 | 33 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 2 | lvlh5_cbd | wt_med | 3.958 | -0.137 | -0.417 | -0.007 | 0.486 | 28067.000 | 29.000 | 33 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 3 | lvlh5_cbd | wt_med | 4.250 | -0.192 | -0.401 | -0.025 | 0.483 | 27969.000 | 28.000 | 33 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 4 | lvlh5_cbd | wt_med | 4.575 | -0.160 | -0.483 | 0.004 | 0.492 | 25785.000 | 28.000 | 33 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 5 | lvlh5_cbd | wt_med | 4.820 | -0.097 | -0.320 | 0.053 | 0.492 | 25039.000 | 29.000 | 33 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 6 | lvlh5_cbd | wt_med | 4.379 | -0.159 | -0.432 | -0.004 | 0.485 | 26263.000 | 28.000 | 33 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 7 | lvlh5_cbd | wt_med | 4.514 | -0.229 | -0.548 | -0.003 | 0.496 | 25396.000 | 28.000 | 32 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 10 | lvlh5_cbd | wt_med | 4.405 | -0.231 | -0.488 | -0.037 | 0.486 | 26895.000 | 28.000 | 32 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 14 | lvlh5_cbd | wt_med | 4.649 | -0.242 | -0.575 | -0.021 | 0.471 | 23230.000 | 28.000 | 31 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 21 | lvlh5_cbd | wt_med | 4.791 | -0.207 | -0.483 | -0.016 | 0.449 | 21223.000 | 28.000 | 30 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 30 | lvlh5_cbd | wt_med | 4.267 | -0.175 | -0.516 | -0.013 | 0.443 | 21422.000 | 30.000 | 29 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_cbd | wt_med | 5.297 | -0.142 | -0.446 | -0.010 | 0.102 | 4515.000 | 9.000 | 27 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | lvlh5_tft | wt_med | 4.148 | -0.172 | -0.551 | 0.018 | 0.459 | 22198.000 | 30.000 | 29 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_tft | wt_med | 4.335 | -0.252 | -0.643 | -0.009 | 0.356 | 15772.000 | 30.000 | 27 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | lvlh5_tft | wt_med | 0.005 | 0.000 | 0.000 | 0.000 | 0.011 | 428.000 | 4.000 | 25 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | prod_served | wt_med | 7.486 | 3.084 | 2.390 | 3.880 | 0.476 | 22996.000 | 30.000 | 29 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | prod_served | wt_med | 6.594 | 1.861 | 1.251 | 2.515 | 0.376 | 16696.000 | 30.000 | 27 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served | wt_med | 0.050 | 0.050 | 0.000 | 1.281 | 0.011 | 403.000 | 4.000 | 25 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | prod_served_lin | wt_med | 7.623 | 3.227 | 2.502 | 3.963 | 0.476 | 22996.000 | 30.000 | 29 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | prod_served_lin | wt_med | 6.647 | 1.916 | 1.348 | 2.549 | 0.376 | 16696.000 | 30.000 | 27 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served_lin | wt_med | 0.050 | 0.050 | 0.000 | 1.281 | 0.011 | 403.000 | 4.000 | 25 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 0 | lvlh5_cbd | wt_med | 4.463 | -0.241 | -0.663 | 0.027 | 0.491 | 20953.000 | 29.000 | 33 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 1 | lvlh5_cbd | wt_med | 4.263 | -0.377 | -0.979 | 0.016 | 0.468 | 19965.000 | 26.000 | 33 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 2 | lvlh5_cbd | wt_med | 4.133 | -0.171 | -0.523 | -0.004 | 0.480 | 21725.000 | 24.000 | 33 | True | True |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.44 [7.17, 7.74] | 6.65 [6.31, 6.99] | 5.29 [5.11, 5.49] | 5.94 [5.67, 6.23] | 7.04 [6.77, 7.36] | 7.09 [6.82, 7.40] |
| 2 | 7.25 [7.00, 7.52] | 7.44 [7.14, 7.75] | 5.60 [5.43, 5.79] | 5.40 [5.16, 5.66] | 7.03 [6.76, 7.30] | 7.07 [6.80, 7.35] |
| 3 | 6.89 [6.60, 7.20] | 6.84 [6.48, 7.21] | 4.99 [4.83, 5.15] | 5.40 [5.11, 5.70] | 6.81 [6.51, 7.10] | 6.91 [6.61, 7.20] |
| 4 | 7.83 [7.48, 8.20] | 7.46 [7.07, 7.90] | 5.38 [5.18, 5.62] | 6.31 [5.98, 6.68] | 7.56 [7.23, 7.94] | 7.61 [7.26, 7.98] |
| 5 | 7.68 [7.32, 8.05] | 7.33 [6.92, 7.79] | 5.20 [5.01, 5.38] | 6.24 [5.89, 6.61] | 7.50 [7.16, 7.86] | 7.56 [7.22, 7.91] |
| 6 | 7.31 [7.00, 7.62] | 7.04 [6.69, 7.39] | 5.01 [4.83, 5.18] | 5.92 [5.64, 6.23] | 7.19 [6.88, 7.51] | 7.27 [6.96, 7.59] |
| 7 | 7.93 [7.62, 8.26] | 7.30 [6.95, 7.67] | 5.20 [5.02, 5.38] | 6.71 [6.38, 7.06] | 7.41 [7.10, 7.71] | 7.48 [7.16, 7.79] |
| 10 | 7.00 [6.71, 7.30] | 7.24 [6.83, 7.68] | 5.02 [4.86, 5.18] | 5.54 [5.26, 5.83] | 6.96 [6.66, 7.26] | 7.08 [6.77, 7.39] |
| 14 | 8.25 [7.91, 8.58] | 7.73 [7.35, 8.16] | 5.29 [5.10, 5.48] | 7.10 [6.71, 7.47] | 7.69 [7.37, 8.01] | 7.79 [7.47, 8.11] |
| 21 | 8.53 [8.18, 8.88] | 8.00 [7.59, 8.45] | 5.39 [5.19, 5.58] | 7.44 [7.04, 7.80] | 7.83 [7.52, 8.17] | 7.93 [7.62, 8.27] |
| 30 | 8.07 [7.79, 8.39] | 8.39 [7.98, 8.84] | 5.83 [5.64, 6.04] | 6.32 [6.05, 6.65] | 7.68 [7.39, 7.99] | 7.76 [7.48, 8.07] |
| 45 | 7.57 [7.26, 7.92] | 7.85 [7.32, 8.45] | 5.12 [4.96, 5.29] | 6.22 [5.91, 6.58] | 7.62 [7.28, 7.97] | 7.80 [7.46, 8.17] |
| 60 | 8.63 [8.26, 9.02] | 0.47 [0.14, 1.18] | 5.59 [5.34, 5.85] | 7.27 [6.88, 7.66] | 8.24 [7.86, 8.67] | 8.34 [7.96, 8.76] |
| 90 | 8.70 [8.25, 9.14] |  | 5.21 [4.98, 5.43] | 7.50 [7.06, 7.94] | 8.22 [7.79, 8.65] | 8.35 [7.92, 8.78] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| m015 | 3.00 [2.88, 3.11] | 7.75 [7.42, 8.08] | 7.03 [6.73, 7.33] | 7.51 [7.20, 7.80] | 6.59 [6.25, 6.93] | 2.64 [2.52, 2.75] | 8.35 [7.97, 8.72] | 6.99 [6.69, 7.29] |
| m030 | 3.86 [3.72, 3.99] | 7.68 [7.37, 8.01] | 6.92 [6.63, 7.21] | 7.42 [7.12, 7.72] | 6.55 [6.22, 6.88] | 4.23 [4.06, 4.39] | 8.27 [7.90, 8.63] | 6.95 [6.66, 7.24] |
| m045 | 4.24 [4.10, 4.39] | 7.68 [7.35, 8.01] | 6.92 [6.63, 7.21] | 7.43 [7.12, 7.72] | 6.56 [6.24, 6.89] | 5.06 [4.86, 5.26] | 8.28 [7.91, 8.64] | 6.96 [6.67, 7.25] |
| m060 | 4.49 [4.34, 4.65] | 7.76 [7.43, 8.09] | 7.03 [6.74, 7.31] | 7.51 [7.22, 7.80] | 6.65 [6.32, 6.99] | 5.62 [5.40, 5.84] | 8.37 [7.99, 8.73] | 7.05 [6.75, 7.34] |
| m075 | 4.72 [4.55, 4.89] | 7.78 [7.46, 8.11] | 7.01 [6.73, 7.30] | 7.51 [7.22, 7.81] | 6.59 [6.27, 6.94] | 6.12 [5.88, 6.36] | 8.39 [8.02, 8.75] | 7.06 [6.78, 7.35] |
| m090 | 4.84 [4.67, 5.01] | 7.82 [7.49, 8.15] | 6.99 [6.71, 7.28] | 7.52 [7.23, 7.81] | 6.63 [6.31, 6.97] | 6.52 [6.26, 6.77] | 8.41 [8.04, 8.77] | 7.08 [6.79, 7.37] |
| m105 | 4.95 [4.79, 5.12] | 7.86 [7.52, 8.19] | 7.04 [6.75, 7.33] | 7.59 [7.30, 7.87] | 6.68 [6.36, 7.02] | 6.74 [6.49, 7.01] | 8.43 [8.06, 8.80] | 7.11 [6.81, 7.40] |
| m120 | 5.02 [4.86, 5.19] | 7.90 [7.58, 8.23] | 7.07 [6.78, 7.36] | 7.58 [7.29, 7.86] | 6.65 [6.34, 6.99] | 7.00 [6.73, 7.28] | 8.43 [8.07, 8.79] | 7.13 [6.83, 7.42] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| m015 | 0.26 [0.18, 0.36] | 5.40 [4.41, 6.34] | 4.70 [3.84, 5.51] | 5.19 [4.29, 6.06] | 4.43 [3.61, 5.31] | 5.96 [5.01, 6.89] | 4.66 [3.82, 5.46] | persistence |
| m030 | -0.42 [-0.61, -0.21]* | 3.75 [2.95, 4.55] | 3.02 [2.35, 3.70] | 3.53 [2.79, 4.27] | 2.92 [2.27, 3.63] | 4.33 [3.59, 5.06] | 3.04 [2.37, 3.73] | persistence |
| m045 | -0.90 [-1.17, -0.62]* | 2.83 [2.15, 3.56] | 2.10 [1.48, 2.73] | 2.62 [1.97, 3.28] | 2.07 [1.49, 2.70] | 3.45 [2.82, 4.06] | 2.13 [1.52, 2.77] | persistence |
| m060 | -1.26 [-1.60, -0.93]* | 2.26 [1.58, 2.94] | 1.56 [0.94, 2.17] | 2.04 [1.38, 2.76] | 1.54 [0.96, 2.19] | 2.90 [2.29, 3.55] | 1.57 [0.97, 2.19] | persistence |
| m075 | -1.56 [-1.96, -1.15]* | 1.95 [1.22, 2.66] | 1.18 [0.57, 1.80] | 1.66 [1.00, 2.35] | 1.10 [0.51, 1.76] | 2.57 [1.95, 3.20] | 1.22 [0.57, 1.86] | persistence |
| m090 | -1.88 [-2.39, -1.40]* | 1.48 [0.82, 2.12] | 0.67 [0.09, 1.26] | 1.19 [0.55, 1.85] | 0.71 [0.14, 1.34] | 2.10 [1.51, 2.71] | 0.74 [0.12, 1.35] | persistence |
| m105 | -2.06 [-2.64, -1.56]* | 1.27 [0.56, 2.00] | 0.42 [-0.22, 1.08] | 0.95 [0.22, 1.70] | 0.45 [-0.18, 1.11] | 1.85 [1.18, 2.60] | 0.50 [-0.16, 1.17] | persistence |
| m120 | -2.27 [-2.92, -1.71]* | 0.93 [0.23, 1.65] | 0.10 [-0.58, 0.73] | 0.59 [-0.13, 1.31] | 0.11 [-0.53, 0.78] | 1.52 [0.83, 2.26] | 0.16 [-0.51, 0.81] | persistence |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 5.956 [5.005, 6.890] / [5.639, 6.280] | 4.661 [3.817, 5.462] / [4.388, 4.935] | 5.403 [4.413, 6.344] / [5.098, 5.704] | 4.699 [3.835, 5.515] / [4.431, 4.963] | 5.191 [4.291, 6.063] / [4.923, 5.472] | 4.428 [3.610, 5.306] / [4.137, 4.748] |
| m030 | 4.333 [3.591, 5.059] / [4.025, 4.634] | 3.044 [2.367, 3.731] / [2.784, 3.293] | 3.749 [2.948, 4.549] / [3.468, 4.038] | 3.019 [2.347, 3.696] / [2.764, 3.260] | 3.527 [2.786, 4.274] / [3.279, 3.790] | 2.917 [2.269, 3.635] / [2.645, 3.217] |
| m045 | 3.446 [2.815, 4.065] / [3.159, 3.737] | 2.133 [1.520, 2.770] / [1.887, 2.379] | 2.831 [2.152, 3.556] / [2.558, 3.103] | 2.103 [1.480, 2.728] / [1.858, 2.348] | 2.623 [1.966, 3.279] / [2.377, 2.887] | 2.067 [1.494, 2.697] / [1.811, 2.357] |
| m060 | 2.897 [2.287, 3.547] / [2.598, 3.205] | 1.571 [0.967, 2.194] / [1.320, 1.823] | 2.259 [1.579, 2.944] / [1.996, 2.533] | 1.560 [0.944, 2.172] / [1.316, 1.808] | 2.044 [1.380, 2.759] / [1.791, 2.312] | 1.536 [0.964, 2.187] / [1.256, 1.829] |
| m075 | 2.574 [1.947, 3.197] / [2.246, 2.897] | 1.217 [0.574, 1.862] / [0.962, 1.473] | 1.953 [1.216, 2.659] / [1.676, 2.240] | 1.184 [0.574, 1.802] / [0.940, 1.442] | 1.662 [0.995, 2.351] / [1.405, 1.942] | 1.100 [0.511, 1.764] / [0.826, 1.396] |
| m090 | 2.098 [1.510, 2.706] / [1.761, 2.414] | 0.735 [0.122, 1.352] / [0.481, 1.000] | 1.478 [0.819, 2.123] / [1.201, 1.763] | 0.673 [0.090, 1.265] / [0.426, 0.932] | 1.194 [0.553, 1.847] / [0.942, 1.473] | 0.706 [0.135, 1.339] / [0.430, 0.990] |
| m105 | 1.848 [1.180, 2.605] / [1.526, 2.173] | 0.503 [-0.164, 1.172] / [0.247, 0.754] | 1.268 [0.556, 1.995] / [0.985, 1.552] | 0.423 [-0.224, 1.075] / [0.182, 0.675] | 0.946 [0.222, 1.705] / [0.678, 1.218] | 0.451 [-0.182, 1.108] / [0.186, 0.740] |
| m120 | 1.524 [0.830, 2.263] / [1.210, 1.844] | 0.159 [-0.510, 0.813] / [-0.099, 0.417] | 0.927 [0.227, 1.652] / [0.658, 1.212] | 0.096 [-0.575, 0.726] / [-0.146, 0.333] | 0.592 [-0.133, 1.310] / [0.333, 0.869] | 0.112 [-0.531, 0.781] / [-0.155, 0.396] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.956 [-6.890, -5.005]* / [-6.280, -5.639]* | -1.249 [-1.595, -0.889]* / [-1.530, -0.983]* | -0.530 [-0.951, -0.024]* / [-0.847, -0.239]* | -1.201 [-1.544, -0.845]* / [-1.494, -0.934]* | -0.728 [-0.986, -0.444]* / [-0.983, -0.479]* | -0.998 [-1.249, -0.733]* / [-1.307, -0.704]* |
| m030 | -4.333 [-5.059, -3.591]* / [-4.634, -4.025]* | -1.229 [-1.568, -0.870]* / [-1.509, -0.976]* | -0.542 [-0.932, -0.067]* / [-0.855, -0.246]* | -1.250 [-1.581, -0.892]* / [-1.525, -0.980]* | -0.743 [-1.003, -0.443]* / [-0.981, -0.501]* | -0.949 [-1.184, -0.657]* / [-1.260, -0.642]* |
| m045 | -3.446 [-4.065, -2.815]* / [-3.737, -3.159]* | -1.241 [-1.591, -0.857]* / [-1.522, -0.971]* | -0.569 [-0.971, -0.077]* / [-0.882, -0.271]* | -1.267 [-1.621, -0.874]* / [-1.543, -1.001]* | -0.776 [-1.059, -0.450]* / [-1.019, -0.541]* | -1.003 [-1.289, -0.689]* / [-1.314, -0.703]* |
| m060 | -2.897 [-3.547, -2.287]* / [-3.205, -2.598]* | -1.249 [-1.607, -0.864]* / [-1.542, -0.977]* | -0.575 [-0.943, -0.146]* / [-0.892, -0.276]* | -1.253 [-1.608, -0.874]* / [-1.532, -0.976]* | -0.796 [-1.080, -0.461]* / [-1.057, -0.547]* | -1.027 [-1.333, -0.734]* / [-1.363, -0.709]* |
| m075 | -2.574 [-3.197, -1.947]* / [-2.897, -2.246]* | -1.238 [-1.589, -0.850]* / [-1.525, -0.976]* | -0.558 [-0.987, -0.099]* / [-0.881, -0.250]* | -1.285 [-1.656, -0.894]* / [-1.564, -1.018]* | -0.812 [-1.100, -0.484]* / [-1.060, -0.556]* | -1.082 [-1.382, -0.760]* / [-1.408, -0.764]* |
| m090 | -2.098 [-2.706, -1.510]* / [-2.414, -1.761]* | -1.245 [-1.603, -0.848]* / [-1.526, -0.986]* | -0.532 [-0.964, -0.031]* / [-0.857, -0.226]* | -1.328 [-1.726, -0.920]* / [-1.602, -1.068]* | -0.813 [-1.117, -0.462]* / [-1.079, -0.567]* | -1.065 [-1.394, -0.729]* / [-1.389, -0.767]* |
| m105 | -1.848 [-2.605, -1.180]* / [-2.173, -1.526]* | -1.238 [-1.610, -0.841]* / [-1.510, -0.984]* | -0.519 [-0.954, -0.007]* / [-0.841, -0.219]* | -1.301 [-1.689, -0.899]* / [-1.575, -1.041]* | -0.778 [-1.065, -0.431]* / [-1.026, -0.528]* | -1.039 [-1.320, -0.732]* / [-1.352, -0.734]* |
| m120 | -1.524 [-2.263, -0.830]* / [-1.844, -1.210]* | -1.232 [-1.605, -0.831]* / [-1.526, -0.966]* | -0.500 [-0.947, 0.036] / [-0.815, -0.210]*  ! | -1.273 [-1.647, -0.880]* / [-1.555, -1.016]* | -0.794 [-1.073, -0.468]* / [-1.052, -0.553]* | -1.016 [-1.261, -0.754]* / [-1.315, -0.728]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -4.661 [-5.462, -3.817]* / [-4.935, -4.388]* | 1.249 [0.889, 1.595] / [0.983, 1.530] | 0.705 [0.486, 0.975] / [0.595, 0.821] | 0.030 [-0.017, 0.076] / [-0.005, 0.067] | 0.500 [0.334, 0.647] / [0.373, 0.642] | 0.142 [-0.104, 0.384] / [-0.033, 0.334] |
| m030 | -3.044 [-3.731, -2.367]* / [-3.293, -2.784]* | 1.229 [0.870, 1.568] / [0.976, 1.509] | 0.670 [0.470, 0.917] / [0.556, 0.785] | -0.031 [-0.059, -0.010]* / [-0.058, -0.006]* | 0.463 [0.295, 0.604] / [0.346, 0.598] | 0.143 [-0.082, 0.366] / [-0.033, 0.332] |
| m045 | -2.133 [-2.770, -1.520]* / [-2.379, -1.887]* | 1.241 [0.857, 1.591] / [0.971, 1.522] | 0.652 [0.448, 0.904] / [0.541, 0.770] | -0.043 [-0.071, -0.018]* / [-0.067, -0.021]* | 0.446 [0.279, 0.588] / [0.323, 0.581] | 0.102 [-0.123, 0.316] / [-0.065, 0.288] |
| m060 | -1.571 [-2.194, -0.967]* / [-1.823, -1.320]* | 1.249 [0.864, 1.607] / [0.977, 1.542] | 0.650 [0.452, 0.888] / [0.535, 0.771] | -0.022 [-0.048, 0.003] / [-0.047, 0.004] | 0.434 [0.281, 0.574] / [0.318, 0.570] | 0.084 [-0.146, 0.296] / [-0.086, 0.271] |
| m075 | -1.217 [-1.862, -0.574]* / [-1.473, -0.962]* | 1.238 [0.850, 1.589] / [0.976, 1.525] | 0.663 [0.462, 0.905] / [0.546, 0.780] | -0.046 [-0.105, 0.005] / [-0.078, -0.016]*  ! | 0.426 [0.253, 0.564] / [0.303, 0.566] | 0.048 [-0.184, 0.246] / [-0.123, 0.230] |
| m090 | -0.735 [-1.352, -0.122]* / [-1.000, -0.481]* | 1.245 [0.848, 1.603] / [0.986, 1.526] | 0.691 [0.481, 0.950] / [0.580, 0.803] | -0.079 [-0.147, -0.027]* / [-0.110, -0.050]* | 0.423 [0.242, 0.561] / [0.304, 0.562] | 0.077 [-0.170, 0.280] / [-0.089, 0.257] |
| m105 | -0.503 [-1.172, 0.164] / [-0.754, -0.247]*  ! | 1.238 [0.841, 1.610] / [0.984, 1.510] | 0.703 [0.487, 0.953] / [0.590, 0.827] | -0.064 [-0.111, -0.026]* / [-0.093, -0.036]* | 0.452 [0.274, 0.597] / [0.329, 0.589] | 0.086 [-0.159, 0.299] / [-0.094, 0.279] |
| m120 | -0.159 [-0.813, 0.510] / [-0.417, 0.099] | 1.232 [0.831, 1.605] / [0.966, 1.526] | 0.718 [0.503, 0.978] / [0.602, 0.841] | -0.050 [-0.108, -0.010]* / [-0.084, -0.016]* | 0.426 [0.230, 0.580] / [0.310, 0.567] | 0.050 [-0.189, 0.254] / [-0.127, 0.237] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.403 [-6.344, -4.413]* / [-5.704, -5.098]* | 0.530 [0.024, 0.951] / [0.239, 0.847] | -0.705 [-0.975, -0.486]* / [-0.821, -0.595]* | -0.673 [-0.938, -0.463]* / [-0.793, -0.562]* | -0.187 [-0.559, 0.093] / [-0.373, 0.003] | -0.458 [-0.776, -0.184]* / [-0.663, -0.245]* |
| m030 | -3.749 [-4.549, -2.948]* / [-4.038, -3.468]* | 0.542 [0.067, 0.932] / [0.246, 0.855] | -0.670 [-0.917, -0.470]* / [-0.785, -0.556]* | -0.702 [-0.960, -0.498]* / [-0.822, -0.593]* | -0.193 [-0.552, 0.066] / [-0.372, -0.014]*  ! | -0.416 [-0.698, -0.147]* / [-0.615, -0.205]* |
| m045 | -2.831 [-3.556, -2.152]* / [-3.103, -2.558]* | 0.569 [0.077, 0.971] / [0.271, 0.882] | -0.652 [-0.904, -0.448]* / [-0.770, -0.541]* | -0.696 [-0.945, -0.496]* / [-0.810, -0.588]* | -0.192 [-0.567, 0.063] / [-0.367, -0.020]*  ! | -0.417 [-0.703, -0.155]* / [-0.613, -0.209]* |
| m060 | -2.259 [-2.944, -1.579]* / [-2.533, -1.996]* | 0.575 [0.146, 0.943] / [0.276, 0.892] | -0.650 [-0.888, -0.452]* / [-0.771, -0.535]* | -0.675 [-0.897, -0.488]* / [-0.791, -0.566]* | -0.207 [-0.538, 0.026] / [-0.381, -0.035]*  ! | -0.443 [-0.735, -0.188]* / [-0.642, -0.243]* |
| m075 | -1.953 [-2.659, -1.216]* / [-2.240, -1.676]* | 0.558 [0.099, 0.987] / [0.250, 0.881] | -0.663 [-0.905, -0.462]* / [-0.780, -0.546]* | -0.706 [-0.950, -0.506]* / [-0.824, -0.595]* | -0.223 [-0.591, 0.055] / [-0.403, -0.045]*  ! | -0.491 [-0.760, -0.256]* / [-0.691, -0.284]* |
| m090 | -1.478 [-2.123, -0.819]* / [-1.763, -1.201]* | 0.532 [0.031, 0.964] / [0.226, 0.857] | -0.691 [-0.950, -0.481]* / [-0.803, -0.580]* | -0.767 [-1.029, -0.540]* / [-0.884, -0.657]* | -0.247 [-0.631, 0.040] / [-0.422, -0.075]*  ! | -0.499 [-0.780, -0.232]* / [-0.699, -0.303]* |
| m105 | -1.268 [-1.995, -0.556]* / [-1.552, -0.985]* | 0.519 [0.007, 0.954] / [0.219, 0.841] | -0.703 [-0.953, -0.487]* / [-0.827, -0.590]* | -0.766 [-1.010, -0.561]* / [-0.889, -0.657]* | -0.234 [-0.603, 0.045] / [-0.421, -0.056]*  ! | -0.494 [-0.754, -0.249]* / [-0.694, -0.294]* |
| m120 | -0.927 [-1.652, -0.227]* / [-1.212, -0.658]* | 0.500 [-0.036, 0.947] / [0.210, 0.815] | -0.718 [-0.978, -0.503]* / [-0.841, -0.602]* | -0.763 [-1.015, -0.559]* / [-0.882, -0.647]* | -0.275 [-0.673, 0.024] / [-0.455, -0.099]*  ! | -0.541 [-0.819, -0.293]* / [-0.739, -0.330]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| m015 | 5.91 [5.74, 6.05] | 15.58 [14.96, 16.22] | 14.13 [13.60, 14.68] | 14.68 [14.16, 15.26] | 12.96 [12.32, 13.68] | 5.18 [4.99, 5.35] | 16.43 [15.77, 17.10] | 14.11 [13.57, 14.63] |
| m030 | 7.61 [7.42, 7.80] | 15.46 [14.86, 16.08] | 13.96 [13.44, 14.50] | 14.53 [14.01, 15.07] | 12.85 [12.23, 13.54] | 8.29 [8.05, 8.53] | 16.20 [15.57, 16.86] | 14.05 [13.52, 14.58] |
| m045 | 8.27 [8.07, 8.47] | 15.37 [14.79, 15.97] | 13.90 [13.40, 14.41] | 14.46 [13.95, 14.99] | 12.86 [12.24, 13.55] | 9.88 [9.60, 10.15] | 16.15 [15.53, 16.82] | 13.99 [13.50, 14.49] |
| m060 | 8.74 [8.52, 8.94] | 15.56 [14.96, 16.16] | 14.13 [13.62, 14.63] | 14.65 [14.12, 15.19] | 13.04 [12.40, 13.76] | 11.00 [10.69, 11.31] | 16.42 [15.76, 17.11] | 14.21 [13.70, 14.71] |
| m075 | 9.10 [8.87, 9.35] | 15.63 [15.04, 16.26] | 14.07 [13.55, 14.58] | 14.56 [14.03, 15.12] | 12.80 [12.19, 13.55] | 11.66 [11.27, 12.04] | 16.43 [15.79, 17.08] | 14.21 [13.69, 14.72] |
| m090 | 9.37 [9.11, 9.63] | 15.76 [15.15, 16.40] | 14.07 [13.54, 14.60] | 14.63 [14.10, 15.19] | 12.89 [12.31, 13.62] | 12.49 [12.07, 12.90] | 16.45 [15.80, 17.09] | 14.29 [13.76, 14.80] |
| m105 | 9.55 [9.30, 9.80] | 15.79 [15.15, 16.46] | 14.11 [13.58, 14.67] | 14.73 [14.17, 15.29] | 12.93 [12.32, 13.63] | 12.86 [12.46, 13.27] | 16.44 [15.76, 17.09] | 14.27 [13.72, 14.81] |
| m120 | 9.69 [9.45, 9.96] | 15.84 [15.22, 16.47] | 14.16 [13.64, 14.70] | 14.71 [14.17, 15.27] | 12.92 [12.31, 13.62] | 13.48 [13.07, 13.95] | 16.48 [15.85, 17.14] | 14.34 [13.80, 14.89] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| m015 | 0.44 [0.24, 0.65] | 10.75 [8.73, 12.75] | 9.36 [7.71, 10.87] | 9.89 [8.20, 11.40] | 8.39 [6.83, 9.65] | 11.47 [9.75, 13.02] | 9.31 [7.64, 10.81] | persistence |
| m030 | -0.92 [-1.28, -0.49]* | 7.45 [5.57, 9.36] | 6.03 [4.47, 7.50] | 6.57 [5.03, 8.04] | 5.17 [3.81, 6.45] | 8.21 [6.73, 9.49] | 6.09 [4.50, 7.58] | persistence |
| m045 | -1.87 [-2.27, -1.40]* | 5.72 [4.02, 7.55] | 4.31 [2.91, 5.81] | 4.87 [3.42, 6.37] | 3.53 [2.30, 4.87] | 6.49 [5.16, 7.72] | 4.35 [2.95, 5.86] | persistence |
| m060 | -2.62 [-3.12, -2.10]* | 4.64 [2.98, 6.37] | 3.25 [1.84, 4.82] | 3.77 [2.30, 5.31] | 2.44 [1.22, 3.83] | 5.55 [4.19, 6.94] | 3.29 [1.90, 4.90] | persistence |
| m075 | -3.07 [-3.79, -2.40]* | 4.34 [2.62, 6.09] | 2.75 [1.35, 4.24] | 3.27 [1.85, 4.73] | 1.78 [0.53, 3.14] | 5.12 [3.71, 6.48] | 2.83 [1.36, 4.36] | persistence |
| m090 | -3.76 [-4.65, -2.90]* | 3.34 [1.83, 4.94] | 1.67 [0.42, 2.98] | 2.28 [0.90, 3.66] | 0.86 [-0.32, 2.13] | 4.09 [2.78, 5.38] | 1.83 [0.51, 3.19] | persistence |
| m105 | -4.06 [-5.12, -3.17]* | 3.04 [1.32, 4.89] | 1.25 [-0.25, 2.93] | 1.87 [0.23, 3.63] | 0.38 [-1.06, 2.01] | 3.68 [2.19, 5.49] | 1.45 [-0.06, 3.15] | persistence |
| m120 | -4.51 [-5.73, -3.54]* | 2.30 [0.67, 4.10] | 0.53 [-0.87, 2.08] | 1.12 [-0.43, 2.82] | -0.28 [-1.67, 1.28] | 3.07 [1.47, 4.86] | 0.75 [-0.70, 2.28] | persistence |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 11.466 [9.745, 13.017] / [10.791, 12.147] | 9.307 [7.638, 10.809] / [8.678, 9.873] | 10.748 [8.726, 12.745] / [10.069, 11.403] | 9.355 [7.707, 10.866] / [8.777, 9.918] | 9.895 [8.201, 11.404] / [9.320, 10.507] | 8.388 [6.827, 9.654] / [7.748, 9.145] |
| m030 | 8.207 [6.733, 9.492] / [7.539, 8.880] | 6.086 [4.498, 7.583] / [5.504, 6.643] | 7.448 [5.574, 9.363] / [6.786, 8.083] | 6.033 [4.474, 7.496] / [5.454, 6.583] | 6.568 [5.030, 8.039] / [6.031, 7.180] | 5.166 [3.809, 6.448] / [4.498, 5.928] |
| m045 | 6.494 [5.158, 7.715] / [5.835, 7.190] | 4.351 [2.947, 5.859] / [3.783, 4.904] | 5.721 [4.016, 7.548] / [5.076, 6.337] | 4.312 [2.912, 5.811] / [3.741, 4.867] | 4.871 [3.418, 6.373] / [4.316, 5.505] | 3.529 [2.303, 4.867] / [2.891, 4.245] |
| m060 | 5.547 [4.186, 6.939] / [4.877, 6.234] | 3.289 [1.901, 4.899] / [2.689, 3.891] | 4.637 [2.977, 6.372] / [4.015, 5.282] | 3.254 [1.838, 4.819] / [2.676, 3.820] | 3.774 [2.302, 5.306] / [3.214, 4.397] | 2.438 [1.219, 3.832] / [1.736, 3.179] |
| m075 | 5.117 [3.706, 6.477] / [4.383, 5.860] | 2.833 [1.365, 4.359] / [2.204, 3.440] | 4.341 [2.616, 6.094] / [3.688, 5.018] | 2.750 [1.352, 4.241] / [2.127, 3.355] | 3.270 [1.850, 4.730] / [2.688, 3.895] | 1.777 [0.528, 3.137] / [1.093, 2.575] |
| m090 | 4.091 [2.782, 5.381] / [3.351, 4.829] | 1.830 [0.507, 3.192] / [1.208, 2.469] | 3.342 [1.830, 4.940] / [2.666, 4.001] | 1.671 [0.418, 2.984] / [1.046, 2.287] | 2.279 [0.902, 3.663] / [1.676, 2.937] | 0.860 [-0.320, 2.132] / [0.141, 1.586] |
| m105 | 3.676 [2.193, 5.493] / [2.955, 4.406] | 1.450 [-0.055, 3.151] / [0.826, 2.067] | 3.035 [1.319, 4.890] / [2.328, 3.730] | 1.248 [-0.248, 2.934] / [0.625, 1.842] | 1.866 [0.232, 3.626] / [1.240, 2.526] | 0.378 [-1.060, 2.009] / [-0.309, 1.095] |
| m120 | 3.074 [1.474, 4.864] / [2.335, 3.848] | 0.754 [-0.703, 2.280] / [0.109, 1.361] | 2.300 [0.667, 4.102] / [1.593, 2.963] | 0.535 [-0.869, 2.079] / [-0.091, 1.130] | 1.120 [-0.434, 2.817] / [0.526, 1.801] | -0.282 [-1.668, 1.276] / [-0.948, 0.416] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -11.466 [-13.017, -9.745]* / [-12.147, -10.791]* | -2.210 [-2.853, -1.436]* / [-2.849, -1.584]* | -0.720 [-1.685, 0.592] / [-1.473, -0.008]*  ! | -2.155 [-2.817, -1.330]* / [-2.806, -1.554]* | -1.540 [-2.003, -0.979]* / [-2.149, -0.985]* | -2.369 [-2.961, -1.819]* / [-3.219, -1.608]* |
| m030 | -8.207 [-9.492, -6.733]* / [-8.880, -7.539]* | -2.100 [-2.686, -1.338]* / [-2.751, -1.482]* | -0.638 [-1.528, 0.595] / [-1.381, 0.067] | -2.171 [-2.766, -1.396]* / [-2.800, -1.555]* | -1.528 [-1.942, -0.985]* / [-2.082, -0.941]* | -2.233 [-2.766, -1.700]* / [-3.041, -1.500]* |
| m045 | -6.494 [-7.715, -5.158]* / [-7.190, -5.835]* | -2.112 [-2.733, -1.285]* / [-2.734, -1.514]* | -0.672 [-1.625, 0.653] / [-1.412, 0.044] | -2.164 [-2.804, -1.317]* / [-2.796, -1.561]* | -1.560 [-2.047, -0.918]* / [-2.138, -1.012]* | -2.252 [-2.871, -1.652]* / [-3.066, -1.494]* |
| m060 | -5.547 [-6.939, -4.186]* / [-6.234, -4.877]* | -2.194 [-2.795, -1.349]* / [-2.841, -1.558]* | -0.772 [-1.613, 0.388] / [-1.524, -0.081]*  ! | -2.223 [-2.829, -1.373]* / [-2.880, -1.579]* | -1.657 [-2.099, -1.038]* / [-2.267, -1.113]* | -2.386 [-3.019, -1.764]* / [-3.275, -1.582]* |
| m075 | -5.117 [-6.477, -3.706]* / [-5.860, -4.383]* | -2.169 [-2.807, -1.280]* / [-2.828, -1.555]* | -0.709 [-1.624, 0.544] / [-1.470, -0.005]*  ! | -2.309 [-2.967, -1.439]* / [-2.955, -1.673]* | -1.755 [-2.145, -1.142]* / [-2.390, -1.168]* | -2.632 [-3.224, -1.997]* / [-3.489, -1.820]* |
| m090 | -4.091 [-5.381, -2.782]* / [-4.829, -3.351]* | -2.118 [-2.775, -1.230]* / [-2.750, -1.503]* | -0.596 [-1.601, 0.764] / [-1.352, 0.117] | -2.348 [-3.047, -1.408]* / [-2.965, -1.725]* | -1.708 [-2.183, -1.023]* / [-2.327, -1.143]* | -2.539 [-3.107, -1.918]* / [-3.333, -1.783]* |
| m105 | -3.676 [-5.493, -2.193]* / [-4.406, -2.955]* | -2.113 [-2.736, -1.231]* / [-2.731, -1.514]* | -0.562 [-1.533, 0.732] / [-1.350, 0.159] | -2.273 [-2.930, -1.412]* / [-2.888, -1.678]* | -1.600 [-2.037, -0.973]* / [-2.198, -1.048]* | -2.396 [-3.022, -1.808]* / [-3.181, -1.661]* |
| m120 | -3.074 [-4.864, -1.474]* / [-3.848, -2.335]* | -2.107 [-2.823, -1.191]* / [-2.743, -1.507]* | -0.588 [-1.647, 0.871] / [-1.329, 0.119] | -2.273 [-2.994, -1.333]* / [-2.884, -1.688]* | -1.676 [-2.150, -1.044]* / [-2.294, -1.154]* | -2.381 [-2.853, -1.923]* / [-3.173, -1.668]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -9.307 [-10.809, -7.638]* / [-9.873, -8.678]* | 2.210 [1.436, 2.853] / [1.584, 2.849] | 1.446 [0.939, 2.068] / [1.173, 1.705] | 0.022 [-0.108, 0.144] / [-0.074, 0.124] | 0.628 [0.230, 0.943] / [0.307, 0.990] | -0.303 [-1.125, 0.358] / [-0.802, 0.209] |
| m030 | -6.086 [-7.583, -4.498]* / [-6.643, -5.504]* | 2.100 [1.338, 2.686] / [1.482, 2.751] | 1.382 [0.917, 1.946] / [1.114, 1.660] | -0.086 [-0.145, -0.032]* / [-0.159, -0.014]* | 0.526 [0.116, 0.832] / [0.237, 0.858] | -0.324 [-1.041, 0.254] / [-0.816, 0.179] |
| m045 | -4.351 [-5.859, -2.947]* / [-4.904, -3.783]* | 2.112 [1.285, 2.733] / [1.514, 2.734] | 1.348 [0.863, 1.947] / [1.089, 1.623] | -0.094 [-0.167, -0.026]* / [-0.154, -0.033]* | 0.505 [0.071, 0.829] / [0.210, 0.833] | -0.383 [-1.120, 0.202] / [-0.822, 0.087] |
| m060 | -3.289 [-4.899, -1.901]* / [-3.891, -2.689]* | 2.194 [1.349, 2.795] / [1.558, 2.841] | 1.328 [0.872, 1.866] / [1.069, 1.601] | -0.077 [-0.139, -0.023]* / [-0.144, -0.009]* | 0.478 [0.063, 0.786] / [0.177, 0.801] | -0.442 [-1.165, 0.121] / [-0.909, 0.046] |
| m075 | -2.833 [-4.359, -1.365]* / [-3.440, -2.204]* | 2.169 [1.280, 2.807] / [1.555, 2.828] | 1.395 [0.934, 1.950] / [1.103, 1.666] | -0.136 [-0.284, -0.015]* / [-0.218, -0.060]* | 0.407 [-0.062, 0.744] / [0.103, 0.738] | -0.622 [-1.456, -0.030]* / [-1.099, -0.143]* |
| m090 | -1.830 [-3.192, -0.507]* / [-2.469, -1.208]* | 2.118 [1.230, 2.775] / [1.503, 2.750] | 1.440 [0.954, 2.079] / [1.164, 1.708] | -0.218 [-0.369, -0.092]* / [-0.300, -0.147]* | 0.384 [-0.066, 0.716] / [0.085, 0.712] | -0.612 [-1.366, -0.034]* / [-1.054, -0.142]* |
| m105 | -1.450 [-3.151, 0.055] / [-2.067, -0.826]*  ! | 2.113 [1.231, 2.736] / [1.514, 2.731] | 1.491 [1.003, 2.125] / [1.214, 1.771] | -0.164 [-0.277, -0.067]* / [-0.237, -0.087]* | 0.478 [0.034, 0.799] / [0.170, 0.812] | -0.527 [-1.356, 0.073] / [-1.016, -0.032]*  ! |
| m120 | -0.754 [-2.280, 0.703] / [-1.361, -0.109]*  ! | 2.107 [1.191, 2.823] / [1.507, 2.743] | 1.474 [0.985, 2.104] / [1.198, 1.756] | -0.181 [-0.334, -0.073]* / [-0.267, -0.094]* | 0.401 [-0.096, 0.756] / [0.104, 0.743] | -0.609 [-1.449, -0.036]* / [-1.098, -0.114]* |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -10.748 [-12.745, -8.726]* / [-11.403, -10.069]* | 0.720 [-0.592, 1.685] / [0.008, 1.473] | -1.446 [-2.068, -0.939]* / [-1.705, -1.173]* | -1.416 [-2.046, -0.908]* / [-1.696, -1.131]* | -0.776 [-1.742, -0.072]* / [-1.216, -0.290]* | -1.574 [-2.443, -0.828]* / [-2.106, -1.017]* |
| m030 | -7.448 [-9.363, -5.574]* / [-8.083, -6.786]* | 0.638 [-0.595, 1.528] / [-0.067, 1.381] | -1.382 [-1.946, -0.917]* / [-1.660, -1.114]* | -1.462 [-2.044, -0.987]* / [-1.743, -1.187]* | -0.829 [-1.743, -0.168]* / [-1.257, -0.370]* | -1.516 [-2.264, -0.903]* / [-2.027, -0.966]* |
| m045 | -5.721 [-7.548, -4.016]* / [-6.337, -5.076]* | 0.672 [-0.653, 1.625] / [-0.044, 1.412] | -1.348 [-1.947, -0.863]* / [-1.623, -1.089]* | -1.434 [-2.034, -0.956]* / [-1.714, -1.182]* | -0.819 [-1.808, -0.127]* / [-1.258, -0.382]* | -1.473 [-2.213, -0.846]* / [-1.959, -0.968]* |
| m060 | -4.637 [-6.372, -2.977]* / [-5.282, -4.015]* | 0.772 [-0.388, 1.613] / [0.081, 1.524] | -1.328 [-1.866, -0.872]* / [-1.601, -1.069]* | -1.403 [-1.919, -0.977]* / [-1.684, -1.151]* | -0.832 [-1.712, -0.213]* / [-1.245, -0.414]* | -1.522 [-2.231, -0.894]* / [-1.998, -1.009]* |
| m075 | -4.341 [-6.094, -2.616]* / [-5.018, -3.688]* | 0.709 [-0.544, 1.624] / [0.005, 1.470] | -1.395 [-1.950, -0.934]* / [-1.666, -1.103]* | -1.524 [-2.048, -1.073]* / [-1.791, -1.255]* | -0.961 [-1.894, -0.303]* / [-1.391, -0.521]* | -1.799 [-2.515, -1.256]* / [-2.292, -1.260]* |
| m090 | -3.342 [-4.940, -1.830]* / [-4.001, -2.666]* | 0.596 [-0.764, 1.601] / [-0.117, 1.352] | -1.440 [-2.079, -0.954]* / [-1.708, -1.164]* | -1.654 [-2.262, -1.148]* / [-1.923, -1.396]* | -1.022 [-2.016, -0.311]* / [-1.452, -0.590]* | -1.853 [-2.557, -1.270]* / [-2.320, -1.353]* |
| m105 | -3.035 [-4.890, -1.319]* / [-3.730, -2.328]* | 0.562 [-0.732, 1.533] / [-0.159, 1.350] | -1.491 [-2.125, -1.003]* / [-1.771, -1.214]* | -1.650 [-2.234, -1.192]* / [-1.935, -1.380]* | -0.985 [-1.951, -0.300]* / [-1.426, -0.518]* | -1.832 [-2.597, -1.225]* / [-2.343, -1.308]* |
| m120 | -2.300 [-4.102, -0.667]* / [-2.963, -1.593]* | 0.588 [-0.871, 1.647] / [-0.119, 1.329] | -1.474 [-2.104, -0.985]* / [-1.756, -1.198]* | -1.637 [-2.230, -1.173]* / [-1.911, -1.357]* | -1.043 [-2.072, -0.334]* / [-1.486, -0.573]* | -1.862 [-2.614, -1.268]* / [-2.369, -1.351]* |

**MAE by region (all rides)**

| lead | region | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| m015 | Asia | 3.34 | 9.95 | 8.93 | 9.35 | 8.60 | 3.03 | 9.92 | 8.81 |
| m015 | EU | 2.04 | 5.45 | 5.00 | 5.37 | 4.93 | 1.78 | 5.96 | 4.97 |
| m015 | NA | 3.90 | 8.41 | 7.76 | 8.37 | 7.03 | 3.34 | 9.80 | 7.79 |
| m060 | Asia | 5.14 | 9.92 | 8.91 | 9.34 | 8.64 | 6.60 | 10.05 | 8.90 |
| m060 | EU | 3.10 | 5.50 | 5.03 | 5.38 | 5.04 | 3.78 | 5.91 | 5.06 |
| m060 | NA | 5.70 | 8.53 | 7.86 | 8.47 | 7.14 | 6.99 | 9.94 | 7.88 |
| m120 | Asia | 5.90 | 10.31 | 9.04 | 9.43 | 8.59 | 8.57 | 10.07 | 9.07 |
| m120 | EU | 3.56 | 5.70 | 5.10 | 5.55 | 5.11 | 4.61 | 6.11 | 5.19 |
| m120 | NA | 6.05 | 8.36 | 7.73 | 8.34 | 7.07 | 8.45 | 9.77 | 7.78 |

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | chronos2 |
|---|---|---|---|---|---|---|---|---|
| m015 | -0.18 | 0.29 | -0.84 | -0.67 | -1.26 | -0.90 | -1.70 | 0.03 |
| m030 | -0.15 | 0.25 | -0.91 | -0.75 | -1.15 | -0.79 | -1.59 | -0.22 |
| m045 | 0.05 | 0.24 | -0.94 | -0.76 | -1.07 | -0.72 | -1.53 | -0.31 |
| m060 | 0.26 | 0.27 | -0.96 | -0.80 | -1.16 | -0.80 | -1.61 | -0.51 |
| m075 | 0.28 | 0.22 | -0.92 | -0.78 | -1.15 | -0.77 | -1.55 | -0.59 |
| m090 | 0.54 | 0.22 | -0.92 | -0.80 | -1.10 | -0.73 | -1.51 | -0.68 |
| m105 | 0.60 | 0.23 | -0.91 | -0.80 | -1.13 | -0.75 | -1.59 | -0.85 |
| m120 | 0.90 | 0.26 | -0.85 | -0.73 | -1.14 | -0.76 | -1.55 | -0.90 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| h2-4 | 5.32 [5.14, 5.52] | 7.83 [7.50, 8.16] | 7.04 [6.75, 7.34] | 7.57 [7.28, 7.87] | 6.65 [6.34, 6.99] | 8.07 [7.79, 8.37] | 8.42 [8.05, 8.79] | 7.08 [6.79, 7.37] |
| h4-8 | 5.80 [5.59, 6.03] | 7.84 [7.52, 8.16] | 7.04 [6.76, 7.34] | 7.59 [7.31, 7.89] | 6.76 [6.43, 7.12] | 10.42 [10.02, 10.80] | 8.50 [8.15, 8.86] | 7.10 [6.81, 7.40] |
| h8+ | 5.98 [5.72, 6.24] | 7.21 [6.90, 7.55] | 6.44 [6.17, 6.73] | 7.04 [6.78, 7.34] | 6.50 [6.16, 6.89] | 12.56 [11.99, 13.10] | 7.97 [7.61, 8.32] | 6.49 [6.21, 6.78] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|
| h2-4 | -1.76 [-2.21, -1.32]* | 0.69 [0.48, 0.94] | -0.04 [-0.07, -0.01]* | 0.47 [0.29, 0.61] | 0.10 [-0.13, 0.32] | 0.86 [0.22, 1.57] | 1.25 [0.86, 1.60] | wt_med |
| h4-8 | -1.30 [-1.64, -0.95]* | 0.67 [0.46, 0.91] | -0.05 [-0.09, -0.02]* | 0.47 [0.28, 0.62] | 0.12 [-0.12, 0.34] | 3.26 [2.38, 4.34] | 1.32 [0.97, 1.65] | wt_med |
| h8+ | -0.52 [-0.75, -0.32]* | 0.57 [0.38, 0.80] | -0.04 [-0.09, 0.00] | 0.55 [0.37, 0.72] | 0.38 [0.18, 0.57] | 6.17 [4.82, 8.17] | 1.43 [1.11, 1.71] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.576 [-0.117, 1.275] / [0.214, 0.908] | -0.864 [-1.570, -0.224]* / [-1.134, -0.613]* | -0.122 [-0.804, 0.565] / [-0.395, 0.159] | -0.912 [-1.617, -0.273]* / [-1.173, -0.661]* | -0.420 [-1.199, 0.287] / [-0.674, -0.155]*  ! | -0.784 [-1.495, -0.107]* / [-1.068, -0.483]* |
| h4-8 | -1.651 [-2.831, -0.720]* / [-2.090, -1.275]* | -3.261 [-4.336, -2.379]* / [-3.610, -2.924]* | -2.520 [-3.484, -1.610]* / [-2.854, -2.179]* | -3.309 [-4.373, -2.432]* / [-3.655, -2.983]* | -2.764 [-3.942, -1.797]* / [-3.092, -2.426]* | -2.718 [-3.738, -1.828]* / [-3.089, -2.360]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.576 [-1.275, 0.117] / [-0.908, -0.214]*  ! | -1.249 [-1.597, -0.862]* / [-1.523, -0.994]* | -0.543 [-0.957, -0.057]* / [-0.853, -0.248]* | -1.275 [-1.625, -0.898]* / [-1.548, -1.013]* | -0.769 [-1.046, -0.440]* / [-1.010, -0.525]* | -1.024 [-1.299, -0.727]* / [-1.335, -0.723]* |
| h4-8 | 1.651 [0.720, 2.831] / [1.275, 2.090] | -1.320 [-1.647, -0.972]* / [-1.604, -1.047]* | -0.641 [-1.023, -0.169]* / [-0.958, -0.345]* | -1.361 [-1.691, -1.012]* / [-1.648, -1.083]* | -0.837 [-1.094, -0.532]* / [-1.091, -0.588]* | -1.050 [-1.323, -0.769]* / [-1.364, -0.745]* |
| h8+ | 4.472 [3.074, 6.420] / [3.944, 4.993] | -1.434 [-1.715, -1.106]* / [-1.690, -1.181]* | -0.849 [-1.207, -0.365]* / [-1.143, -0.575]* | -1.462 [-1.770, -1.115]* / [-1.717, -1.212]* | -0.863 [-1.114, -0.582]* / [-1.118, -0.621]* | -0.919 [-1.209, -0.649]* / [-1.215, -0.592]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.864 [0.224, 1.570] / [0.613, 1.134] | 1.249 [0.862, 1.597] / [0.994, 1.523] | 0.687 [0.478, 0.941] / [0.574, 0.805] | -0.036 [-0.066, -0.013]* / [-0.053, -0.017]* | 0.466 [0.292, 0.607] / [0.345, 0.605] | 0.103 [-0.130, 0.322] / [-0.064, 0.288] |
| h4-8 | 3.261 [2.379, 4.336] / [2.924, 3.610] | 1.320 [0.972, 1.647] / [1.047, 1.604] | 0.665 [0.459, 0.910] / [0.558, 0.777] | -0.051 [-0.088, -0.024]* / [-0.070, -0.032]* | 0.470 [0.285, 0.619] / [0.349, 0.610] | 0.122 [-0.115, 0.337] / [-0.046, 0.307] |
| h8+ | 6.171 [4.819, 8.171] / [5.681, 6.628] | 1.434 [1.106, 1.715] / [1.181, 1.690] | 0.570 [0.381, 0.798] / [0.480, 0.666] | -0.042 [-0.094, 0.003] / [-0.072, -0.010]*  ! | 0.552 [0.367, 0.718] / [0.441, 0.675] | 0.377 [0.182, 0.569] / [0.214, 0.553] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.122 [-0.565, 0.804] / [-0.159, 0.395] | 0.543 [0.057, 0.957] / [0.248, 0.853] | -0.687 [-0.941, -0.478]* / [-0.805, -0.574]* | -0.721 [-0.966, -0.521]* / [-0.839, -0.610]* | -0.205 [-0.583, 0.062] / [-0.389, -0.029]*  ! | -0.457 [-0.743, -0.201]* / [-0.653, -0.258]* |
| h4-8 | 2.520 [1.610, 3.484] / [2.179, 2.854] | 0.641 [0.169, 1.023] / [0.345, 0.958] | -0.665 [-0.910, -0.459]* / [-0.777, -0.558]* | -0.715 [-0.946, -0.518]* / [-0.824, -0.608]* | -0.180 [-0.553, 0.084] / [-0.353, -0.001]*  ! | -0.425 [-0.706, -0.178]* / [-0.632, -0.219]* |
| h8+ | 5.666 [4.399, 7.591] / [5.186, 6.142] | 0.849 [0.365, 1.207] / [0.575, 1.143] | -0.570 [-0.798, -0.381]* / [-0.666, -0.480]* | -0.615 [-0.868, -0.412]* / [-0.719, -0.518]* | -0.014 [-0.401, 0.251] / [-0.164, 0.147] | -0.110 [-0.425, 0.143] / [-0.294, 0.104] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| h2-4 | 10.22 [9.95, 10.53] | 15.75 [15.15, 16.37] | 14.19 [13.67, 14.70] | 14.76 [14.24, 15.30] | 12.99 [12.36, 13.69] | 15.19 [14.75, 15.64] | 16.48 [15.86, 17.13] | 14.29 [13.75, 14.81] |
| h4-8 | 10.98 [10.66, 11.35] | 15.44 [14.88, 15.99] | 13.90 [13.43, 14.39] | 14.47 [13.99, 14.99] | 12.96 [12.35, 13.69] | 18.90 [18.29, 19.51] | 16.30 [15.70, 16.94] | 14.03 [13.54, 14.51] |
| h8+ | 11.10 [10.73, 11.55] | 13.70 [13.21, 14.21] | 12.29 [11.86, 12.76] | 12.89 [12.44, 13.38] | 12.09 [11.46, 12.79] | 22.22 [21.33, 23.19] | 14.86 [14.27, 15.47] | 12.40 [11.95, 12.88] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| h2-4 | -5.84 [-7.21, -4.69]* | 0.37 [-1.00, 1.97] | -1.27 [-2.56, 0.10] | -0.70 [-2.24, 0.87] | -1.87 [-3.24, -0.36] |  | 1.28 [-0.23, 2.88] | -1.15 [-2.45, 0.22] | persistence |
| h4-8 | -3.08 [-3.90, -2.29]* | 1.38 [0.93, 1.91] | -0.13 [-0.22, -0.07]* | 0.49 [0.04, 0.79] | -0.42 [-1.17, 0.15] | 5.47 [3.87, 7.22] | 2.25 [1.52, 2.80] |  | wt_med |
| h8+ | -1.35 [-1.81, -0.95]* | 1.18 [0.82, 1.67] | -0.13 [-0.25, -0.02]* | 0.52 [0.17, 0.78] | 0.03 [-0.42, 0.46] | 10.95 [7.92, 15.07] | 2.48 [1.84, 2.97] |  | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.283 [-0.231, 2.884] / [0.527, 2.042] | -1.146 [-2.454, 0.218] / [-1.784, -0.571]*  ! | 0.371 [-1.000, 1.972] / [-0.279, 1.003] | -1.267 [-2.557, 0.099] / [-1.885, -0.700]*  ! | -0.698 [-2.238, 0.868] / [-1.295, -0.097]*  ! | -1.873 [-3.241, -0.357]* / [-2.578, -1.182]* |
| h4-8 | -2.746 [-4.869, -0.873]* / [-3.649, -1.873]* | -5.469 [-7.221, -3.875]* / [-6.229, -4.816]* | -3.943 [-5.549, -2.248]* / [-4.715, -3.255]* | -5.604 [-7.329, -3.995]* / [-6.341, -4.988]* | -4.960 [-6.988, -3.100]* / [-5.697, -4.239]* | -5.472 [-7.227, -3.730]* / [-6.297, -4.643]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -1.283 [-2.884, 0.231] / [-2.042, -0.527]*  ! | -2.152 [-2.779, -1.311]* / [-2.772, -1.544]* | -0.649 [-1.586, 0.606] / [-1.394, 0.043] | -2.240 [-2.874, -1.404]* / [-2.866, -1.624]* | -1.606 [-2.043, -1.007]* / [-2.181, -1.051]* | -2.422 [-3.009, -1.855]* / [-3.241, -1.691]* |
| h4-8 | 2.746 [0.873, 4.869] / [1.873, 3.649] | -2.252 [-2.799, -1.522]* / [-2.845, -1.649]* | -0.805 [-1.685, 0.375] / [-1.507, -0.144]*  ! | -2.366 [-2.936, -1.602]* / [-2.956, -1.766]* | -1.731 [-2.102, -1.220]* / [-2.297, -1.194]* | -2.403 [-2.956, -1.817]* / [-3.211, -1.654]* |
| h8+ | 7.998 [4.687, 12.299] / [6.859, 9.137] | -2.483 [-2.972, -1.840]* / [-3.047, -1.932]* | -1.181 [-1.954, -0.087]* / [-1.813, -0.582]* | -2.592 [-3.106, -1.900]* / [-3.126, -2.063]* | -1.931 [-2.252, -1.536]* / [-2.492, -1.399]* | -2.208 [-2.725, -1.669]* / [-2.999, -1.454]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.146 [-0.218, 2.454] / [0.571, 1.784] | 2.152 [1.311, 2.779] / [1.544, 2.772] | 1.429 [0.959, 2.016] / [1.166, 1.702] | -0.109 [-0.174, -0.058]* / [-0.154, -0.063]* | 0.507 [0.046, 0.832] / [0.203, 0.850] | -0.479 [-1.283, 0.134] / [-0.952, 0.020] |
| h4-8 | 5.469 [3.875, 7.221] / [4.816, 6.229] | 2.252 [1.522, 2.799] / [1.649, 2.845] | 1.375 [0.929, 1.914] / [1.117, 1.636] | -0.135 [-0.219, -0.066]* / [-0.185, -0.084]* | 0.486 [0.038, 0.795] / [0.191, 0.822] | -0.416 [-1.175, 0.152] / [-0.875, 0.064] |
| h8+ | 10.954 [7.916, 15.066] / [10.027, 11.950] | 2.483 [1.840, 2.972] / [1.932, 3.047] | 1.184 [0.821, 1.668] / [0.987, 1.388] | -0.134 [-0.250, -0.016]* / [-0.216, -0.048]* | 0.516 [0.168, 0.783] / [0.256, 0.800] | 0.029 [-0.423, 0.458] / [-0.353, 0.445] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.371 [-1.972, 1.000] / [-1.003, 0.279] | 0.649 [-0.606, 1.586] / [-0.043, 1.394] | -1.429 [-2.016, -0.959]* / [-1.702, -1.166]* | -1.530 [-2.090, -1.067]* / [-1.802, -1.267]* | -0.891 [-1.819, -0.219]* / [-1.323, -0.427]* | -1.678 [-2.433, -1.069]* / [-2.183, -1.139]* |
| h4-8 | 3.943 [2.248, 5.549] / [3.255, 4.715] | 0.805 [-0.375, 1.685] / [0.144, 1.507] | -1.375 [-1.914, -0.929]* / [-1.636, -1.117]* | -1.501 [-2.019, -1.078]* / [-1.754, -1.247]* | -0.864 [-1.765, -0.228]* / [-1.278, -0.401]* | -1.608 [-2.344, -1.005]* / [-2.103, -1.094]* |
| h8+ | 9.776 [6.866, 13.606] / [8.875, 10.789] | 1.181 [0.087, 1.954] / [0.582, 1.813] | -1.184 [-1.668, -0.821]* / [-1.388, -0.987]* | -1.323 [-1.791, -0.920]* / [-1.541, -1.112]* | -0.668 [-1.422, -0.108]* / [-0.992, -0.292]* | -1.079 [-1.719, -0.518]* / [-1.511, -0.595]* |

**MAE by region (all rides)**

_no rows_

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | chronos2 |
|---|---|---|---|---|---|---|---|---|
| h2-4 | 1.33 | 0.24 | -0.92 | -0.77 | -1.17 | -0.79 | -1.61 | -1.18 |
| h4-8 | 2.41 | 0.21 | -1.06 | -0.88 | -1.26 | -0.86 | -1.71 | -1.49 |
| h8+ | 4.68 | 0.12 | -1.31 | -1.19 | -1.58 | -1.27 | -1.96 | -1.22 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 6.15 [5.91, 6.40] | 6.00 [5.75, 6.25] | 6.43 [6.17, 6.70] | 6.04 [5.81, 6.28] |  |  | 7.75 [7.43, 8.07] | 6.97 [6.69, 7.26] | 9.47 [8.95, 10.05] | 7.48 [7.19, 7.77] | 6.59 [6.27, 6.92] | 5.08 [4.91, 5.26] | 6.20 [5.89, 6.49] | 8.34 [7.97, 8.70] | 7.01 [6.72, 7.29] |
| 1 | 6.57 [6.31, 6.86] | 6.47 [6.22, 6.77] | 6.98 [6.70, 7.30] | 6.48 [6.23, 6.77] | 6.78 [6.55, 7.06] | 6.78 [6.55, 7.06] | 7.68 [7.37, 8.04] | 7.04 [6.77, 7.36] | 9.03 [8.50, 9.62] | 7.44 [7.17, 7.74] | 6.65 [6.31, 6.99] | 5.29 [5.11, 5.49] | 5.94 [5.67, 6.23] | 8.40 [8.08, 8.79] | 7.09 [6.82, 7.40] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.87 [-1.16, -0.59]* | -1.02 [-1.29, -0.78]* | -0.58 [-0.88, -0.30]* | -0.98 [-1.31, -0.66]* |  |  | 0.68 [0.47, 0.93] | -0.04 [-0.07, -0.01]* | 2.76 [1.95, 3.83] | 0.45 [0.28, 0.58] | 0.09 [-0.13, 0.29] | -1.94 [-2.40, -1.50]* | -0.84 [-1.18, -0.48]* | 1.24 [0.86, 1.58] | wt_med |
| 1 | -0.54 [-0.85, -0.27]* | -0.62 [-0.86, -0.39]* | -0.12 [-0.41, 0.16] | -0.63 [-0.98, -0.35]* | -0.31 [-0.49, -0.16]* | -0.31 [-0.49, -0.16]* | 0.57 [0.38, 0.82] | -0.05 [-0.08, -0.02]* | 2.57 [1.69, 3.65] | 0.35 [0.18, 0.53] | 0.19 [-0.10, 0.46] | -1.81 [-2.28, -1.42]* | -1.15 [-1.57, -0.70]* | 1.25 [0.96, 1.55] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -1.239 [-1.578, -0.857]* / [-1.511, -0.976]* | -0.543 [-0.962, -0.053]* / [-0.849, -0.246]* | -1.267 [-1.615, -0.892]* / [-1.538, -1.004]* | -0.778 [-1.046, -0.455]* / [-1.020, -0.536]* | -1.016 [-1.277, -0.725]* / [-1.325, -0.721]* | 1.811 [1.046, 2.744] / [1.339, 2.262] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.239 [0.857, 1.578] / [0.976, 1.511] | 0.678 [0.475, 0.931] / [0.562, 0.795] | -0.038 [-0.071, -0.013]* / [-0.056, -0.019]* | 0.447 [0.277, 0.583] / [0.331, 0.583] | 0.095 [-0.132, 0.294] / [-0.068, 0.273] | 2.755 [1.949, 3.834] / [2.378, 3.126] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.543 [0.053, 0.962] / [0.246, 0.849] | -0.678 [-0.931, -0.475]* / [-0.795, -0.562]* | -0.714 [-0.957, -0.513]* / [-0.832, -0.609]* | -0.215 [-0.589, 0.047] / [-0.395, -0.043]*  ! | -0.462 [-0.738, -0.212]* / [-0.652, -0.273]* | 2.199 [1.388, 3.224] / [1.813, 2.577] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 11.95 [11.52, 12.46] | 11.69 [11.25, 12.20] | 12.61 [12.18, 13.16] | 11.68 [11.29, 12.12] |  |  | 15.57 [14.98, 16.19] | 14.02 [13.50, 14.53] | 18.66 [17.52, 20.02] | 14.57 [14.07, 15.11] | 12.86 [12.24, 13.57] | 9.40 [9.18, 9.66] | 11.63 [11.05, 12.22] | 16.33 [15.72, 16.96] | 14.14 [13.60, 14.63] |
| 1 | 12.90 [12.44, 13.38] | 12.72 [12.24, 13.19] | 13.92 [13.44, 14.46] | 12.69 [12.28, 13.15] | 13.06 [12.64, 13.54] | 13.06 [12.63, 13.54] | 15.28 [14.70, 15.95] | 14.00 [13.52, 14.56] | 17.65 [16.40, 18.89] | 14.32 [13.80, 14.85] | 13.06 [12.43, 13.74] | 9.76 [9.49, 10.05] | 10.84 [10.30, 11.37] | 16.47 [15.85, 17.18] | 14.14 [13.66, 14.69] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -2.21 [-2.94, -1.55]* | -2.47 [-3.08, -1.92]* | -1.55 [-2.37, -0.83]* | -2.48 [-3.33, -1.74]* |  |  | 1.41 [0.93, 1.99] | -0.12 [-0.19, -0.06]* | 5.02 [2.74, 7.85] | 0.47 [0.05, 0.78] | -0.47 [-1.24, 0.08] | -4.73 [-5.80, -3.66]* | -2.47 [-3.00, -1.96]* | 2.14 [1.31, 2.75] | wt_med |
| 1 | -1.29 [-2.15, -0.59]* | -1.44 [-2.09, -0.88]* | -0.27 [-1.09, 0.41] | -1.50 [-2.42, -0.78]* | -1.08 [-1.56, -0.69]* | -1.09 [-1.59, -0.70]* | 1.18 [0.75, 1.81] | -0.13 [-0.21, -0.06]* | 4.72 [2.43, 7.51] | 0.26 [-0.08, 0.54] | -0.27 [-1.18, 0.45] | -4.40 [-5.46, -3.44]* | -3.21 [-3.69, -2.69]* | 2.29 [1.61, 2.85] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -2.136 [-2.755, -1.313]* / [-2.764, -1.539]* | -0.658 [-1.615, 0.638] / [-1.402, 0.044] | -2.233 [-2.865, -1.398]* / [-2.842, -1.625]* | -1.625 [-2.037, -1.040]* / [-2.207, -1.080]* | -2.387 [-2.919, -1.850]* / [-3.187, -1.642]* | 3.547 [1.502, 5.907] / [2.312, 4.727] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 2.136 [1.313, 2.755] / [1.539, 2.764] | 1.407 [0.934, 1.993] / [1.157, 1.673] | -0.116 [-0.192, -0.057]* / [-0.162, -0.069]* | 0.475 [0.050, 0.777] / [0.182, 0.806] | -0.475 [-1.235, 0.083] / [-0.924, -0.004]*  ! | 5.020 [2.736, 7.855] / [3.962, 6.031] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.658 [-0.638, 1.615] / [-0.044, 1.402] | -1.407 [-1.993, -0.934]* / [-1.673, -1.157]* | -1.516 [-2.067, -1.061]* / [-1.781, -1.263]* | -0.903 [-1.819, -0.246]* / [-1.321, -0.462]* | -1.668 [-2.397, -1.076]* / [-2.156, -1.169]* | 3.851 [1.568, 6.600] / [2.806, 4.969] |

**MAE by region (all rides)**

| lead | region | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 7.81 | 7.78 | 8.13 | 7.54 |  |  | 10.05 | 8.90 | 13.24 | 9.34 | 8.57 | 6.11 | 7.89 | 10.00 | 8.94 |
| 0 | EU | 4.26 | 4.08 | 4.48 | 4.22 |  |  | 5.53 | 5.00 | 7.38 | 5.41 | 5.02 | 3.58 | 4.69 | 5.96 | 5.05 |
| 0 | NA | 6.99 | 6.75 | 7.33 | 6.94 |  |  | 8.29 | 7.66 | 9.33 | 8.28 | 6.99 | 6.00 | 6.46 | 9.71 | 7.69 |
| 1 | Asia | 8.48 | 8.55 | 9.08 | 8.25 | 8.71 | 8.70 | 10.01 | 9.07 | 13.32 | 9.82 | 8.98 | 6.42 | 8.12 | 10.97 | 9.15 |
| 1 | EU | 4.48 | 4.42 | 4.74 | 4.46 | 4.77 | 4.78 | 5.36 | 4.96 | 6.59 | 5.31 | 4.74 | 3.74 | 4.51 | 5.79 | 5.00 |
| 1 | NA | 7.38 | 7.10 | 7.83 | 7.33 | 7.49 | 7.50 | 8.29 | 7.73 | 9.28 | 7.85 | 7.18 | 6.14 | 5.73 | 9.17 | 7.76 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 6.57 [6.31, 6.86] | 6.47 [6.22, 6.77] | 6.98 [6.70, 7.30] | 6.48 [6.23, 6.77] | 6.78 [6.55, 7.06] | 6.78 [6.55, 7.06] | 7.68 [7.37, 8.04] | 7.04 [6.77, 7.36] | 9.03 [8.50, 9.62] | 7.44 [7.17, 7.74] | 6.65 [6.31, 6.99] | 5.29 [5.11, 5.49] | 5.94 [5.67, 6.23] |  |  | 8.40 [8.08, 8.79] | 7.09 [6.82, 7.40] |
| 2 | 7.78 [7.46, 8.09] | 7.64 [7.31, 7.96] | 8.59 [8.22, 8.97] | 7.68 [7.38, 7.98] | 7.36 [7.09, 7.62] | 7.37 [7.10, 7.63] | 7.37 [7.11, 7.67] | 7.03 [6.76, 7.30] | 10.17 [9.62, 10.76] | 7.25 [7.00, 7.52] | 7.44 [7.14, 7.75] | 5.60 [5.43, 5.79] | 5.40 [5.16, 5.66] |  |  | 8.19 [7.89, 8.54] | 7.07 [6.80, 7.35] |
| 3 | 7.03 [6.71, 7.35] | 6.90 [6.58, 7.21] | 7.55 [7.22, 7.90] | 6.97 [6.65, 7.28] | 6.89 [6.60, 7.17] | 6.89 [6.60, 7.18] | 7.36 [7.06, 7.65] | 6.81 [6.51, 7.10] | 9.36 [8.82, 9.92] | 6.89 [6.60, 7.20] | 6.84 [6.48, 7.21] | 4.99 [4.83, 5.15] | 5.40 [5.11, 5.70] | 7.54 [7.18, 7.93] | 7.52 [7.16, 7.90] | 8.11 [7.72, 8.51] | 6.91 [6.61, 7.20] |
| 4 | 7.42 [7.08, 7.79] | 7.20 [6.84, 7.57] | 7.89 [7.53, 8.28] | 7.35 [7.01, 7.71] | 7.28 [6.97, 7.62] | 7.29 [6.98, 7.63] | 8.01 [7.65, 8.40] | 7.56 [7.23, 7.94] | 10.05 [9.44, 10.72] | 7.83 [7.48, 8.20] | 7.46 [7.07, 7.90] | 5.38 [5.18, 5.62] | 6.31 [5.98, 6.68] | 7.85 [7.44, 8.28] | 7.83 [7.43, 8.26] | 8.71 [8.27, 9.15] | 7.61 [7.26, 7.98] |
| 5 | 7.43 [7.04, 7.79] | 7.14 [6.74, 7.51] | 7.90 [7.47, 8.28] | 7.43 [7.04, 7.80] | 7.28 [6.95, 7.59] | 7.29 [6.96, 7.61] | 8.04 [7.67, 8.40] | 7.50 [7.16, 7.86] | 10.09 [9.40, 10.81] | 7.68 [7.32, 8.05] | 7.33 [6.92, 7.79] | 5.20 [5.01, 5.38] | 6.24 [5.89, 6.61] | 7.67 [7.23, 8.10] | 7.65 [7.21, 8.08] | 8.55 [8.08, 9.00] | 7.56 [7.22, 7.91] |
| 6 | 7.05 [6.74, 7.35] | 6.83 [6.51, 7.14] | 7.44 [7.11, 7.77] | 7.01 [6.71, 7.31] | 6.97 [6.66, 7.27] | 6.98 [6.67, 7.28] | 7.78 [7.43, 8.11] | 7.19 [6.88, 7.51] | 9.53 [8.99, 10.08] | 7.31 [7.00, 7.62] | 7.04 [6.69, 7.39] | 5.01 [4.83, 5.18] | 5.92 [5.64, 6.23] | 7.48 [7.13, 7.85] | 7.47 [7.12, 7.84] | 8.06 [7.70, 8.43] | 7.27 [6.96, 7.59] |
| 7 | 7.41 [7.11, 7.71] | 7.24 [6.91, 7.55] | 7.84 [7.51, 8.17] | 7.37 [7.08, 7.67] | 7.28 [6.99, 7.56] | 7.28 [6.99, 7.57] | 7.99 [7.67, 8.33] | 7.41 [7.10, 7.71] | 9.92 [9.39, 10.53] | 7.93 [7.62, 8.26] | 7.30 [6.95, 7.67] | 5.20 [5.02, 5.38] | 6.71 [6.38, 7.06] | 7.80 [7.42, 8.22] | 7.79 [7.41, 8.21] |  | 7.48 [7.16, 7.79] |
| 10 |  |  |  |  | 7.05 [6.75, 7.34] | 7.08 [6.78, 7.37] | 7.50 [7.20, 7.82] | 6.96 [6.66, 7.26] | 9.83 [9.26, 10.48] | 7.00 [6.71, 7.30] | 7.24 [6.83, 7.68] | 5.02 [4.86, 5.18] | 5.54 [5.26, 5.83] | 8.01 [7.58, 8.44] | 7.99 [7.56, 8.42] |  | 7.08 [6.77, 7.39] |
| 14 |  |  |  |  | 7.63 [7.33, 7.94] | 7.64 [7.34, 7.94] | 8.12 [7.79, 8.48] | 7.69 [7.37, 8.01] | 10.15 [9.59, 10.83] | 8.25 [7.91, 8.58] | 7.73 [7.35, 8.16] | 5.29 [5.10, 5.48] | 7.10 [6.71, 7.47] | 8.22 [7.81, 8.69] | 8.21 [7.80, 8.67] |  | 7.79 [7.47, 8.11] |
| 21 |  |  |  |  | 7.84 [7.55, 8.15] | 7.85 [7.56, 8.16] | 8.19 [7.85, 8.54] | 7.83 [7.52, 8.17] | 10.32 [9.74, 10.99] | 8.53 [8.18, 8.88] | 8.00 [7.59, 8.45] | 5.39 [5.19, 5.58] | 7.44 [7.04, 7.80] | 8.61 [8.17, 9.10] | 8.60 [8.15, 9.08] |  | 7.93 [7.62, 8.27] |
| 30 |  |  |  |  | 8.05 [7.76, 8.36] | 8.06 [7.77, 8.38] | 8.00 [7.69, 8.30] | 7.68 [7.39, 7.99] | 10.95 [10.39, 11.61] | 8.07 [7.79, 8.39] | 8.39 [7.98, 8.84] | 5.83 [5.64, 6.04] | 6.32 [6.05, 6.65] | 9.47 [8.98, 10.01] | 9.43 [8.95, 9.97] |  | 7.76 [7.48, 8.07] |
| 45 |  |  |  |  | 7.63 [7.30, 7.96] | 7.66 [7.33, 8.00] | 8.18 [7.82, 8.55] | 7.62 [7.28, 7.97] | 10.55 [9.50, 11.76] | 7.57 [7.26, 7.92] | 7.85 [7.32, 8.45] | 5.12 [4.96, 5.29] | 6.22 [5.91, 6.58] | 8.66 [8.13, 9.24] | 8.63 [8.10, 9.21] |  | 7.80 [7.46, 8.17] |
| 60 |  |  |  |  | 8.25 [7.89, 8.67] | 8.28 [7.92, 8.71] | 8.41 [8.02, 8.82] | 8.24 [7.86, 8.67] |  | 8.63 [8.26, 9.02] | 0.47 [0.14, 1.18] | 5.59 [5.34, 5.85] | 7.27 [6.88, 7.66] | 0.34 [0.05, 0.95] | 0.34 [0.05, 0.96] |  | 8.34 [7.96, 8.76] |
| 90 |  |  |  |  | 8.29 [7.87, 8.69] | 8.30 [7.90, 8.71] | 8.62 [8.18, 9.07] | 8.22 [7.79, 8.65] |  | 8.70 [8.25, 9.14] |  | 5.21 [4.98, 5.43] | 7.50 [7.06, 7.94] |  |  |  | 8.35 [7.92, 8.78] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.54 [-0.85, -0.27]* | -0.62 [-0.86, -0.39]* | -0.12 [-0.41, 0.16] | -0.63 [-0.98, -0.35]* | -0.31 [-0.49, -0.16]* | -0.31 [-0.49, -0.16]* | 0.57 [0.38, 0.82] | -0.05 [-0.08, -0.02]* | 2.57 [1.69, 3.65] | 0.35 [0.18, 0.53] | 0.19 [-0.10, 0.46] | -1.81 [-2.28, -1.42]* | -1.15 [-1.57, -0.70]* |  |  | 1.25 [0.96, 1.55] | wt_med |
| 2 | 0.68 [0.51, 0.86] | 0.55 [0.34, 0.79] | 1.50 [1.12, 1.99] | 0.58 [0.41, 0.77] | 0.29 [0.17, 0.41] | 0.30 [0.18, 0.42] | 0.39 [0.28, 0.53] | -0.03 [-0.07, -0.00]* | 3.26 [2.62, 4.04] | 0.21 [0.12, 0.32] | 0.54 [0.29, 0.77] | -1.48 [-1.88, -1.11]* | -1.63 [-1.99, -1.26]* |  |  | 1.19 [0.86, 1.53] | wt_med |
| 3 | 0.07 [-0.10, 0.22] | -0.06 [-0.24, 0.13] | 0.59 [0.33, 0.87] | 0.00 [-0.17, 0.17] | -0.02 [-0.16, 0.12] | -0.02 [-0.15, 0.12] | 0.55 [0.40, 0.71] | -0.10 [-0.13, -0.07]* | 2.79 [2.12, 3.62] | 0.00 [-0.14, 0.14] | 0.25 [0.04, 0.46] | -1.92 [-2.43, -1.46]* | -1.49 [-1.85, -1.10]* | 1.00 [0.66, 1.36] | 0.98 [0.63, 1.34] | 1.25 [0.88, 1.65] | wt_med |
| 4 | -0.20 [-0.49, 0.02] | -0.43 [-0.66, -0.23]* | 0.26 [-0.10, 0.59] | -0.28 [-0.54, -0.07]* | -0.32 [-0.51, -0.16]* | -0.32 [-0.50, -0.16]* | 0.43 [0.26, 0.64] | -0.03 [-0.06, -0.00]* | 2.66 [1.86, 3.75] | 0.25 [0.08, 0.41] | 0.18 [-0.14, 0.46] | -2.23 [-2.74, -1.73]* | -1.28 [-1.67, -0.88]* | 0.59 [0.08, 1.04] | 0.57 [0.07, 1.02] | 1.09 [0.70, 1.47] | wt_med |
| 5 | -0.14 [-0.54, 0.16] | -0.43 [-0.76, -0.18]* | 0.33 [-0.16, 0.73] | -0.14 [-0.52, 0.16] | -0.28 [-0.50, -0.12]* | -0.27 [-0.50, -0.11]* | 0.48 [0.30, 0.71] | -0.05 [-0.08, -0.02]* | 2.53 [1.81, 3.40] | 0.16 [-0.03, 0.30] | 0.07 [-0.30, 0.42] | -2.37 [-2.87, -1.90]* | -1.29 [-1.68, -0.86]* | 0.42 [-0.15, 0.92] | 0.40 [-0.16, 0.90] | 0.93 [0.47, 1.31] | wt_med |
| 6 | -0.22 [-0.57, 0.04] | -0.44 [-0.69, -0.24]* | 0.17 [-0.21, 0.49] | -0.26 [-0.60, -0.00]* | -0.30 [-0.51, -0.13]* | -0.29 [-0.50, -0.12]* | 0.47 [0.28, 0.70] | -0.07 [-0.12, -0.04]* | 2.53 [1.76, 3.58] | 0.07 [-0.10, 0.21] | 0.21 [-0.11, 0.48] | -2.27 [-2.81, -1.75]* | -1.31 [-1.71, -0.91]* | 0.67 [0.26, 1.01] | 0.66 [0.25, 0.99] | 0.77 [0.41, 1.09] | wt_med |
| 7 | -0.06 [-0.40, 0.23] | -0.23 [-0.50, -0.01]* | 0.38 [-0.03, 0.72] | -0.10 [-0.43, 0.18] | -0.20 [-0.41, -0.04]* | -0.19 [-0.40, -0.03]* | 0.46 [0.25, 0.74] | -0.06 [-0.10, -0.03]* | 2.77 [1.97, 3.74] | 0.48 [0.27, 0.66] | 0.40 [0.13, 0.64] | -2.28 [-2.80, -1.78]* | -0.75 [-1.17, -0.27]* | 0.91 [0.57, 1.21] | 0.90 [0.56, 1.19] |  | wt_med |
| 10 |  |  |  |  | -0.03 [-0.20, 0.14] | -0.00 [-0.17, 0.16] | 0.52 [0.36, 0.69] | -0.11 [-0.15, -0.08]* | 2.96 [2.26, 3.75] | -0.05 [-0.23, 0.13] | 0.38 [0.08, 0.64] | -2.06 [-2.56, -1.57]* | -1.50 [-1.89, -1.06]* | 1.19 [0.74, 1.60] | 1.17 [0.72, 1.58] |  | wt_med |
| 14 |  |  |  |  | -0.14 [-0.36, 0.03] | -0.13 [-0.36, 0.04] | 0.30 [0.07, 0.53] | -0.08 [-0.12, -0.05]* | 2.71 [1.86, 3.75] | 0.52 [0.29, 0.75] | 0.50 [0.14, 0.91] | -2.50 [-3.08, -1.94]* | -0.63 [-1.13, -0.06]* | 1.01 [0.54, 1.50] | 1.00 [0.53, 1.48] |  | wt_med |
| 21 |  |  |  |  | -0.09 [-0.32, 0.10] | -0.07 [-0.31, 0.11] | 0.22 [-0.01, 0.46] | -0.09 [-0.13, -0.05]* | 2.62 [1.77, 3.65] | 0.61 [0.42, 0.80] | 0.48 [0.25, 0.76] | -2.55 [-3.12, -2.00]* | -0.47 [-0.95, 0.10] | 1.10 [0.72, 1.46] | 1.08 [0.71, 1.44] |  | wt_med |
| 30 |  |  |  |  | 0.31 [0.17, 0.42] | 0.32 [0.18, 0.44] | 0.33 [0.17, 0.51] | -0.06 [-0.10, -0.03]* | 3.27 [2.64, 3.97] | 0.37 [0.23, 0.50] | 0.84 [0.41, 1.25] | -1.94 [-2.41, -1.53]* | -1.36 [-1.71, -1.04]* | 1.92 [1.17, 2.62] | 1.88 [1.14, 2.60] |  | wt_med |
| 45 |  |  |  |  | -0.14 [-0.35, 0.05] | -0.11 [-0.31, 0.07] | 0.48 [0.27, 0.74] | -0.14 [-0.19, -0.10]* | 1.66 [0.94, 2.82] | -0.17 [-0.42, 0.06] | 0.34 [0.03, 0.68] | -2.67 [-3.29, -2.06]* | -1.50 [-1.93, -1.06]* | 1.09 [0.60, 1.51] | 1.06 [0.57, 1.48] |  | wt_med |
| 60 |  |  |  |  | -0.06 [-0.25, 0.09] | -0.03 [-0.23, 0.14] | 0.07 [-0.23, 0.38] | -0.07 [-0.10, -0.04] |  | 0.38 [0.15, 0.59] | 0.09 [0.01, 1.05] | -2.76 [-3.33, -2.20]* | -0.97 [-1.50, -0.44]* | -0.07 [-0.28, 0.57] | -0.07 [-0.28, 0.60] |  | wt_med |
| 90 |  |  |  |  | -0.02 [-0.22, 0.13] | -0.01 [-0.22, 0.19] | 0.28 [-0.12, 0.73] | -0.09 [-0.12, -0.06] |  | 0.43 [0.17, 0.67] |  | -3.14 [-3.85, -2.36]* | -0.73 [-1.24, -0.32]* |  |  |  | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -1.246 [-1.549, -0.957]* / [-1.498, -1.014]* | -0.641 [-0.878, -0.404]* / [-0.915, -0.375]* | -1.287 [-1.626, -0.997]* / [-1.536, -1.044]* | -0.902 [-1.180, -0.600]* / [-1.116, -0.696]* | -0.776 [-1.137, -0.411]* / [-1.052, -0.479]* | 1.733 [1.032, 2.520] / [1.330, 2.127] |  |  |
| 2 | -1.194 [-1.530, -0.864]* / [-1.390, -0.990]* | -0.774 [-1.141, -0.406]* / [-0.995, -0.564]* | -1.218 [-1.583, -0.868]* / [-1.424, -1.011]* | -0.972 [-1.307, -0.648]* / [-1.141, -0.801]* | -0.559 [-0.951, -0.216]* / [-0.804, -0.325]* | 2.188 [1.501, 2.953] / [1.741, 2.657] |  |  |
| 3 | -1.249 [-1.654, -0.876]* / [-1.490, -1.005]* | -0.634 [-0.951, -0.285]* / [-0.896, -0.368]* | -1.339 [-1.748, -0.952]* / [-1.580, -1.094]* | -1.244 [-1.627, -0.875]* / [-1.467, -1.013]* | -0.884 [-1.360, -0.413]* / [-1.175, -0.577]* | 1.702 [1.027, 2.346] / [1.239, 2.168] | -0.145 [-0.676, 0.363] / [-0.479, 0.212] | -0.168 [-0.706, 0.341] / [-0.501, 0.189] |
| 4 | -1.091 [-1.472, -0.697]* / [-1.412, -0.808]* | -0.607 [-0.944, -0.260]* / [-0.946, -0.299]* | -1.116 [-1.493, -0.730]* / [-1.436, -0.831]* | -0.840 [-1.145, -0.521]* / [-1.141, -0.576]* | -0.721 [-1.146, -0.279]* / [-1.047, -0.384]* | 1.898 [1.247, 2.617] / [1.345, 2.454] | -0.339 [-0.903, 0.195] / [-0.717, 0.020] | -0.359 [-0.924, 0.175] / [-0.741, 0.001] |
| 5 | -0.933 [-1.311, -0.465]* / [-1.250, -0.626]* | -0.398 [-0.781, 0.055] / [-0.741, -0.063]*  ! | -0.971 [-1.346, -0.523]* / [-1.285, -0.667]* | -0.802 [-1.134, -0.442]* / [-1.083, -0.537]* | -0.758 [-1.096, -0.380]* / [-1.134, -0.431]* | 1.733 [1.192, 2.297] / [1.187, 2.303] | -0.446 [-0.908, -0.033]* / [-0.878, -0.054]* | -0.464 [-0.917, -0.045]* / [-0.898, -0.075]* |
| 6 | -0.770 [-1.086, -0.407]* / [-1.059, -0.489]* | -0.237 [-0.598, 0.155] / [-0.560, 0.077] | -0.830 [-1.144, -0.477]* / [-1.125, -0.550]* | -0.707 [-0.967, -0.451]* / [-0.955, -0.478]* | -0.518 [-0.829, -0.188]* / [-0.804, -0.231]* | 1.854 [1.130, 2.697] / [1.393, 2.377] | -0.017 [-0.374, 0.345] / [-0.319, 0.290] | -0.031 [-0.386, 0.329] / [-0.327, 0.274] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.246 [0.957, 1.549] / [1.014, 1.498] | 0.570 [0.383, 0.819] / [0.467, 0.678] | -0.048 [-0.081, -0.022]* / [-0.069, -0.026]* | 0.352 [0.178, 0.530] / [0.237, 0.466] | 0.191 [-0.101, 0.456] / [0.001, 0.385] | 2.570 [1.691, 3.654] / [2.152, 2.996] |  |  |
| 2 | 1.194 [0.864, 1.530] / [0.990, 1.390] | 0.394 [0.284, 0.527] / [0.313, 0.484] | -0.033 [-0.071, -0.004]* / [-0.046, -0.019]* | 0.213 [0.116, 0.323] / [0.129, 0.303] | 0.536 [0.292, 0.767] / [0.370, 0.725] | 3.263 [2.618, 4.035] / [2.870, 3.658] |  |  |
| 3 | 1.249 [0.876, 1.654] / [1.005, 1.490] | 0.545 [0.398, 0.713] / [0.449, 0.640] | -0.099 [-0.134, -0.069]* / [-0.115, -0.083]* | 0.001 [-0.141, 0.139] / [-0.102, 0.102] | 0.252 [0.039, 0.457] / [0.070, 0.421] | 2.790 [2.117, 3.617] / [2.397, 3.183] | 0.998 [0.656, 1.363] / [0.765, 1.227] | 0.977 [0.631, 1.343] / [0.744, 1.206] |
| 4 | 1.091 [0.697, 1.472] / [0.808, 1.412] | 0.434 [0.264, 0.642] / [0.320, 0.554] | -0.033 [-0.062, -0.005]* / [-0.052, -0.013]* | 0.246 [0.079, 0.407] / [0.120, 0.378] | 0.178 [-0.139, 0.457] / [-0.022, 0.381] | 2.662 [1.862, 3.747] / [2.190, 3.145] | 0.592 [0.084, 1.043] / [0.306, 0.871] | 0.572 [0.067, 1.025] / [0.286, 0.850] |
| 5 | 0.933 [0.465, 1.311] / [0.626, 1.250] | 0.484 [0.299, 0.706] / [0.361, 0.606] | -0.049 [-0.081, -0.024]* / [-0.067, -0.030]* | 0.160 [-0.033, 0.297] / [0.038, 0.282] | 0.072 [-0.302, 0.420] / [-0.137, 0.273] | 2.530 [1.806, 3.395] / [2.132, 2.918] | 0.416 [-0.151, 0.921] / [0.139, 0.683] | 0.400 [-0.162, 0.899] / [0.123, 0.666] |
| 6 | 0.770 [0.407, 1.086] / [0.489, 1.059] | 0.468 [0.278, 0.705] / [0.346, 0.594] | -0.074 [-0.116, -0.042]* / [-0.093, -0.055]* | 0.075 [-0.096, 0.208] / [-0.044, 0.194] | 0.205 [-0.106, 0.477] / [0.011, 0.391] | 2.531 [1.757, 3.584] / [2.148, 2.954] | 0.671 [0.256, 1.006] / [0.421, 0.921] | 0.657 [0.245, 0.990] / [0.409, 0.905] |
| 7 |  | 0.463 [0.247, 0.737] / [0.341, 0.583] | -0.063 [-0.102, -0.033]* / [-0.083, -0.043]* | 0.477 [0.274, 0.657] / [0.345, 0.617] | 0.404 [0.132, 0.644] / [0.216, 0.633] | 2.772 [1.972, 3.744] / [2.406, 3.170] | 0.914 [0.570, 1.207] / [0.648, 1.217] | 0.901 [0.555, 1.191] / [0.635, 1.204] |
| 10 |  | 0.520 [0.363, 0.694] / [0.408, 0.629] | -0.113 [-0.154, -0.080]* / [-0.130, -0.098]* | -0.045 [-0.233, 0.135] / [-0.151, 0.057] | 0.381 [0.081, 0.640] / [0.182, 0.578] | 2.958 [2.256, 3.754] / [2.569, 3.338] | 1.195 [0.742, 1.602] / [0.938, 1.481] | 1.175 [0.719, 1.582] / [0.921, 1.461] |
| 14 |  | 0.296 [0.074, 0.529] / [0.171, 0.419] | -0.079 [-0.123, -0.046]* / [-0.100, -0.059]* | 0.518 [0.285, 0.747] / [0.371, 0.659] | 0.501 [0.137, 0.913] / [0.252, 0.759] | 2.712 [1.864, 3.748] / [2.312, 3.131] | 1.011 [0.541, 1.505] / [0.676, 1.364] | 0.997 [0.533, 1.480] / [0.661, 1.349] |
| 21 |  | 0.215 [-0.012, 0.465] / [0.069, 0.352] | -0.086 [-0.134, -0.051]* / [-0.106, -0.067]* | 0.609 [0.421, 0.802] / [0.467, 0.749] | 0.479 [0.251, 0.756] / [0.246, 0.714] | 2.622 [1.768, 3.646] / [2.218, 3.053] | 1.096 [0.725, 1.462] / [0.744, 1.451] | 1.079 [0.713, 1.442] / [0.727, 1.431] |
| 30 |  | 0.329 [0.172, 0.508] / [0.215, 0.436] | -0.063 [-0.099, -0.034]* / [-0.080, -0.049]* | 0.370 [0.233, 0.499] / [0.280, 0.481] | 0.841 [0.409, 1.255] / [0.564, 1.099] | 3.272 [2.635, 3.969] / [2.900, 3.717] | 1.917 [1.171, 2.621] / [1.487, 2.359] | 1.880 [1.144, 2.597] / [1.448, 2.323] |
| 45 |  | 0.476 [0.269, 0.740] / [0.358, 0.606] | -0.143 [-0.190, -0.101]* / [-0.163, -0.125]* | -0.167 [-0.418, 0.063] / [-0.289, -0.049]*  ! | 0.344 [0.026, 0.678] / [0.077, 0.609] | 1.663 [0.937, 2.823] / [0.908, 2.345] | 1.088 [0.604, 1.511] / [0.742, 1.447] | 1.059 [0.567, 1.483] / [0.714, 1.418] |
| 60 |  | 0.074 [-0.231, 0.382] / [-0.077, 0.224] | -0.068 [-0.105, -0.038]* / [-0.087, -0.050]* | 0.376 [0.153, 0.588] / [0.238, 0.524] | 0.086 [0.006, 1.046] / [-0.001, 0.244] |  | -0.070 [-0.279, 0.568] / [-0.142, 0.016] | -0.070 [-0.278, 0.597] / [-0.143, 0.017] |
| 90 |  | 0.281 [-0.118, 0.730] / [0.122, 0.449] | -0.086 [-0.120, -0.059]* / [-0.106, -0.066]* | 0.433 [0.172, 0.673] / [0.254, 0.602] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.641 [0.404, 0.878] / [0.375, 0.915] | -0.570 [-0.819, -0.383]* / [-0.678, -0.467]* | -0.617 [-0.885, -0.424]* / [-0.726, -0.508]* | -0.225 [-0.490, 0.032] / [-0.382, -0.073]*  ! | -0.284 [-0.626, 0.012] / [-0.488, -0.090]*  ! | 2.098 [1.206, 3.136] / [1.669, 2.517] |  |  |
| 2 | 0.774 [0.406, 1.141] / [0.564, 0.995] | -0.394 [-0.527, -0.284]* / [-0.484, -0.313]* | -0.423 [-0.569, -0.304]* / [-0.518, -0.340]* | -0.187 [-0.351, -0.017]* / [-0.306, -0.076]* | 0.139 [-0.245, 0.446] / [-0.070, 0.340] | 2.892 [2.293, 3.577] / [2.461, 3.321] |  |  |
| 3 | 0.634 [0.285, 0.951] / [0.368, 0.896] | -0.545 [-0.713, -0.398]* / [-0.640, -0.449]* | -0.645 [-0.820, -0.481]* / [-0.742, -0.543]* | -0.534 [-0.767, -0.298]* / [-0.676, -0.397]* | -0.274 [-0.612, 0.046] / [-0.492, -0.069]*  ! | 2.271 [1.609, 2.965] / [1.855, 2.720] | 0.480 [0.075, 0.862] / [0.220, 0.718] | 0.459 [0.044, 0.847] / [0.201, 0.698] |
| 4 | 0.607 [0.260, 0.944] / [0.299, 0.946] | -0.434 [-0.642, -0.264]* / [-0.554, -0.320]* | -0.463 [-0.661, -0.296]* / [-0.578, -0.353]* | -0.205 [-0.430, -0.009]* / [-0.381, -0.038]* | -0.156 [-0.513, 0.165] / [-0.381, 0.084] | 2.341 [1.542, 3.310] / [1.862, 2.844] | 0.262 [-0.297, 0.766] / [-0.038, 0.544] | 0.241 [-0.317, 0.748] / [-0.058, 0.522] |
| 5 | 0.398 [-0.055, 0.781] / [0.063, 0.741] | -0.484 [-0.706, -0.299]* / [-0.606, -0.361]* | -0.529 [-0.750, -0.346]* / [-0.646, -0.411]* | -0.353 [-0.624, -0.119]* / [-0.537, -0.171]* | -0.419 [-0.847, -0.079]* / [-0.675, -0.184]* | 2.030 [1.219, 2.854] / [1.607, 2.474] | -0.081 [-0.645, 0.405] / [-0.390, 0.200] | -0.098 [-0.661, 0.385] / [-0.404, 0.184] |
| 6 | 0.237 [-0.155, 0.598] / [-0.077, 0.560] | -0.468 [-0.705, -0.278]* / [-0.594, -0.346]* | -0.539 [-0.767, -0.358]* / [-0.662, -0.421]* | -0.422 [-0.710, -0.168]* / [-0.607, -0.237]* | -0.228 [-0.559, 0.071] / [-0.443, -0.008]*  ! | 2.106 [1.290, 3.053] / [1.688, 2.531] | 0.230 [-0.194, 0.576] / [-0.034, 0.480] | 0.215 [-0.208, 0.563] / [-0.048, 0.469] |
| 7 |  | -0.463 [-0.737, -0.247]* / [-0.583, -0.341]* | -0.525 [-0.792, -0.310]* / [-0.643, -0.408]* | 0.002 [-0.419, 0.327] / [-0.192, 0.209] | -0.036 [-0.356, 0.272] / [-0.287, 0.217] | 2.350 [1.539, 3.296] / [1.963, 2.759] | 0.489 [0.128, 0.825] / [0.187, 0.813] | 0.475 [0.115, 0.811] / [0.173, 0.798] |
| 10 |  | -0.520 [-0.694, -0.363]* / [-0.629, -0.408]* | -0.632 [-0.829, -0.451]* / [-0.744, -0.517]* | -0.554 [-0.854, -0.262]* / [-0.725, -0.394]* | -0.190 [-0.694, 0.226] / [-0.418, 0.062] | 2.432 [1.697, 3.214] / [1.991, 2.885] | 0.643 [0.076, 1.135] / [0.364, 0.949] | 0.624 [0.053, 1.120] / [0.344, 0.933] |
| 14 |  | -0.296 [-0.529, -0.074]* / [-0.419, -0.171]* | -0.370 [-0.606, -0.162]* / [-0.487, -0.249]* | 0.200 [-0.198, 0.570] / [-0.008, 0.411] | 0.246 [-0.180, 0.685] / [-0.051, 0.545] | 2.479 [1.580, 3.477] / [2.055, 2.905] | 0.773 [0.298, 1.268] / [0.416, 1.147] | 0.758 [0.285, 1.255] / [0.400, 1.127] |
| 21 |  | -0.215 [-0.465, 0.012] / [-0.352, -0.069]*  ! | -0.294 [-0.545, -0.074]* / [-0.429, -0.155]* | 0.401 [0.053, 0.731] / [0.175, 0.618] | 0.322 [-0.025, 0.726] / [0.044, 0.612] | 2.486 [1.617, 3.465] / [2.066, 2.920] | 0.954 [0.535, 1.368] / [0.590, 1.337] | 0.937 [0.518, 1.348] / [0.571, 1.317] |
| 30 |  | -0.329 [-0.508, -0.172]* / [-0.436, -0.215]* | -0.388 [-0.575, -0.222]* / [-0.495, -0.272]* | 0.032 [-0.187, 0.220] / [-0.108, 0.187] | 0.626 [0.090, 1.045] / [0.369, 0.889] | 3.045 [2.366, 3.672] / [2.617, 3.524] | 1.706 [0.972, 2.368] / [1.323, 2.111] | 1.669 [0.938, 2.333] / [1.279, 2.075] |
| 45 |  | -0.476 [-0.740, -0.269]* / [-0.606, -0.358]* | -0.614 [-0.893, -0.378]* / [-0.749, -0.495]* | -0.630 [-1.044, -0.281]* / [-0.811, -0.466]* | -0.083 [-0.668, 0.405] / [-0.419, 0.242] | 0.621 [-0.517, 2.058] / [-0.281, 1.533] | 0.629 [0.118, 1.093] / [0.260, 0.984] | 0.600 [0.069, 1.069] / [0.228, 0.955] |
| 60 |  | -0.074 [-0.382, 0.231] / [-0.224, 0.077] | -0.136 [-0.448, 0.162] / [-0.285, 0.013] | 0.273 [-0.087, 0.629] / [0.058, 0.499] | 0.222 [-1.240, 4.788] / [-0.055, 0.871] |  | 0.083 [-0.263, 3.488] / [-0.172, 0.603] | 0.083 [-0.263, 3.488] / [-0.174, 0.603] |
| 90 |  | -0.281 [-0.730, 0.118] / [-0.449, -0.122]*  ! | -0.363 [-0.829, 0.049] / [-0.535, -0.208]*  ! | 0.159 [-0.439, 0.660] / [-0.110, 0.405] |  |  |  |  |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 12.90 [12.44, 13.38] | 12.72 [12.24, 13.19] | 13.92 [13.44, 14.46] | 12.69 [12.28, 13.15] | 13.06 [12.64, 13.54] | 13.06 [12.63, 13.54] | 15.28 [14.70, 15.95] | 14.00 [13.52, 14.56] | 17.65 [16.40, 18.89] | 14.32 [13.80, 14.85] | 13.06 [12.43, 13.74] | 9.76 [9.49, 10.05] | 10.84 [10.30, 11.37] |  |  | 16.47 [15.85, 17.18] | 14.14 [13.66, 14.69] |
| 2 | 15.14 [14.56, 15.70] | 14.97 [14.36, 15.54] | 17.08 [16.41, 17.73] | 14.91 [14.37, 15.43] | 14.01 [13.57, 14.43] | 14.03 [13.59, 14.45] | 14.32 [13.86, 14.76] | 13.68 [13.25, 14.12] | 20.04 [18.91, 21.24] | 13.89 [13.46, 14.35] | 14.27 [13.73, 14.83] | 10.33 [10.07, 10.60] | 9.97 [9.51, 10.46] |  |  | 16.29 [15.75, 16.87] | 13.80 [13.37, 14.25] |
| 3 | 14.35 [13.77, 14.97] | 14.06 [13.45, 14.67] | 15.53 [14.89, 16.19] | 14.21 [13.63, 14.77] | 13.71 [13.19, 14.22] | 13.73 [13.20, 14.24] | 15.20 [14.64, 15.72] | 14.11 [13.58, 14.60] | 18.81 [17.66, 20.13] | 13.83 [13.27, 14.37] | 13.68 [12.99, 14.36] | 9.51 [9.25, 9.72] | 10.31 [9.73, 10.94] | 15.10 [14.44, 15.80] | 15.03 [14.38, 15.74] | 17.07 [16.29, 17.88] | 14.36 [13.82, 14.86] |
| 4 | 14.52 [13.89, 15.28] | 14.14 [13.44, 14.90] | 15.61 [14.95, 16.41] | 14.35 [13.73, 15.10] | 13.98 [13.38, 14.65] | 14.01 [13.40, 14.69] | 15.88 [15.20, 16.68] | 15.01 [14.36, 15.75] | 19.61 [18.24, 21.11] | 15.12 [14.48, 15.83] | 14.43 [13.72, 15.25] | 9.79 [9.50, 10.12] | 11.73 [11.07, 12.48] | 15.39 [14.68, 16.19] | 15.33 [14.62, 16.13] | 17.36 [16.52, 18.22] | 15.12 [14.47, 15.83] |
| 5 | 14.44 [13.78, 15.12] | 13.94 [13.27, 14.68] | 15.56 [14.82, 16.29] | 14.45 [13.80, 15.14] | 13.88 [13.30, 14.49] | 13.92 [13.33, 14.52] | 15.90 [15.24, 16.64] | 14.81 [14.18, 15.47] | 19.60 [18.13, 21.25] | 14.80 [14.18, 15.50] | 14.22 [13.38, 15.09] | 9.50 [9.25, 9.77] | 11.66 [11.02, 12.43] | 15.01 [14.17, 15.88] | 14.96 [14.12, 15.82] | 16.80 [15.98, 17.67] | 14.96 [14.32, 15.61] |
| 6 | 14.04 [13.49, 14.62] | 13.69 [13.11, 14.28] | 15.03 [14.46, 15.61] | 13.96 [13.43, 14.51] | 13.70 [13.19, 14.26] | 13.72 [13.21, 14.27] | 15.78 [15.14, 16.44] | 14.64 [14.09, 15.21] | 18.67 [17.51, 19.84] | 14.40 [13.86, 15.00] | 13.79 [13.17, 14.48] | 9.35 [9.11, 9.59] | 11.19 [10.60, 11.81] | 14.77 [14.10, 15.43] | 14.73 [14.07, 15.40] | 16.21 [15.51, 16.91] | 14.83 [14.27, 15.39] |
| 7 | 14.59 [14.04, 15.17] | 14.23 [13.60, 14.86] | 15.64 [15.02, 16.25] | 14.49 [13.96, 15.06] | 14.14 [13.63, 14.68] | 14.16 [13.64, 14.70] | 15.98 [15.33, 16.60] | 14.83 [14.28, 15.41] | 19.46 [18.31, 20.86] | 15.52 [14.95, 16.15] | 14.19 [13.55, 14.95] | 9.52 [9.27, 9.80] | 12.64 [11.99, 13.35] | 15.36 [14.67, 16.14] | 15.31 [14.62, 16.10] |  | 15.00 [14.42, 15.57] |
| 10 |  |  |  |  | 14.09 [13.56, 14.61] | 14.14 [13.60, 14.66] | 15.40 [14.84, 15.96] | 14.37 [13.84, 14.90] | 19.52 [18.28, 20.83] | 13.93 [13.42, 14.46] | 14.59 [13.84, 15.37] | 9.48 [9.25, 9.70] | 10.43 [9.90, 10.99] | 16.34 [15.48, 17.14] | 16.28 [15.43, 17.08] |  | 14.65 [14.11, 15.18] |
| 14 |  |  |  |  | 14.97 [14.41, 15.53] | 15.00 [14.44, 15.57] | 16.20 [15.53, 16.83] | 15.41 [14.82, 15.99] | 20.02 [18.80, 21.48] | 16.24 [15.58, 16.91] | 15.16 [14.46, 15.94] | 9.66 [9.41, 9.94] | 13.44 [12.68, 14.17] | 16.42 [15.69, 17.24] | 16.38 [15.65, 17.20] |  | 15.61 [15.01, 16.22] |
| 21 |  |  |  |  | 15.31 [14.78, 15.83] | 15.35 [14.83, 15.87] | 16.26 [15.66, 16.88] | 15.62 [15.06, 16.20] | 20.32 [19.02, 21.77] | 16.72 [16.04, 17.37] | 15.57 [14.86, 16.31] | 9.78 [9.51, 10.07] | 14.02 [13.22, 14.78] | 17.11 [16.28, 17.98] | 17.06 [16.23, 17.93] |  | 15.84 [15.27, 16.42] |
| 30 |  |  |  |  | 15.06 [14.62, 15.54] | 15.09 [14.65, 15.56] | 15.32 [14.79, 15.83] | 14.55 [14.10, 15.01] | 20.92 [19.74, 22.29] | 15.29 [14.78, 15.84] | 16.02 [15.34, 16.68] | 10.53 [10.25, 10.84] | 11.54 [10.97, 12.13] | 18.65 [17.79, 19.51] | 18.55 [17.69, 19.41] |  | 14.74 [14.30, 15.22] |
| 45 |  |  |  |  | 15.12 [14.56, 15.71] | 15.20 [14.63, 15.79] | 16.81 [16.19, 17.45] | 15.62 [15.07, 16.22] | 17.12 [15.62, 18.64] | 14.92 [14.36, 15.54] | 15.64 [14.60, 16.73] | 9.45 [9.21, 9.70] | 11.70 [11.11, 12.36] | 17.29 [16.30, 18.34] | 17.21 [16.21, 18.26] |  | 15.98 [15.40, 16.58] |
| 60 |  |  |  |  | 15.73 [15.05, 16.56] | 15.81 [15.14, 16.62] | 16.23 [15.52, 17.06] | 16.01 [15.33, 16.81] |  | 16.46 [15.78, 17.23] |  | 9.89 [9.53, 10.25] | 13.35 [12.59, 14.14] |  |  |  | 16.18 [15.51, 16.97] |
| 90 |  |  |  |  | 16.29 [15.63, 16.97] | 16.34 [15.69, 17.01] | 17.17 [16.42, 17.96] | 16.57 [15.87, 17.25] |  | 17.04 [16.27, 17.82] |  | 9.58 [9.29, 9.88] | 14.14 [13.26, 15.08] |  |  |  | 16.76 [16.05, 17.45] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -1.29 [-2.15, -0.59]* | -1.44 [-2.09, -0.88]* | -0.27 [-1.09, 0.41] | -1.50 [-2.42, -0.78]* | -1.08 [-1.56, -0.69]* | -1.09 [-1.59, -0.70]* | 1.18 [0.75, 1.81] | -0.13 [-0.21, -0.06]* | 4.72 [2.43, 7.51] | 0.26 [-0.08, 0.54] | -0.27 [-1.18, 0.45] | -4.40 [-5.46, -3.44]* | -3.21 [-3.69, -2.69]* |  |  | 2.29 [1.61, 2.85] | wt_med |
| 2 | 1.25 [0.81, 1.82] | 1.09 [0.54, 1.78] | 3.18 [2.24, 4.65] | 1.01 [0.59, 1.61] | 0.19 [-0.08, 0.50] | 0.22 [-0.05, 0.53] | 0.73 [0.46, 1.04] | -0.13 [-0.22, -0.06]* | 6.41 [4.85, 8.33] | 0.14 [-0.06, 0.36] | 0.41 [-0.17, 0.99] | -3.48 [-4.21, -2.71]* | -3.75 [-4.24, -3.24]* |  |  | 2.58 [2.01, 3.07] | wt_med |
| 3 | -0.10 [-0.48, 0.26] | -0.37 [-0.76, 0.05] | 1.06 [0.43, 1.67] | -0.25 [-0.66, 0.13] | -0.65 [-0.98, -0.31]* | -0.64 [-0.97, -0.29]* | 1.06 [0.76, 1.42] | -0.25 [-0.31, -0.20]* | 4.99 [3.29, 6.81] | -0.50 [-0.83, -0.21]* | -0.35 [-0.80, 0.14] | -4.86 [-5.72, -3.90]* | -4.01 [-4.38, -3.58]* | 1.08 [0.43, 1.79] | 1.02 [0.36, 1.74] | 2.71 [1.93, 3.44] | wt_med |
| 4 | -0.66 [-1.38, -0.17]* | -1.02 [-1.62, -0.59]* | 0.44 [-0.55, 1.16] | -0.83 [-1.49, -0.35]* | -1.15 [-1.57, -0.80]* | -1.12 [-1.53, -0.77]* | 0.86 [0.43, 1.38] | -0.10 [-0.16, -0.04]* | 4.78 [2.42, 7.56] | 0.10 [-0.23, 0.43] | -0.37 [-1.16, 0.31] | -5.34 [-6.41, -4.35]* | -3.28 [-3.73, -2.82]* | 0.55 [-0.52, 1.58] | 0.49 [-0.56, 1.52] | 2.32 [1.46, 3.04] | wt_med |
| 5 | -0.55 [-1.61, 0.16] | -1.04 [-1.92, -0.42]* | 0.56 [-0.69, 1.39] | -0.55 [-1.53, 0.14] | -1.07 [-1.63, -0.70]* | -1.04 [-1.60, -0.66]* | 0.97 [0.51, 1.52] | -0.13 [-0.20, -0.08]* | 4.44 [2.35, 6.69] | -0.07 [-0.52, 0.26] | -0.48 [-1.47, 0.37] | -5.46 [-6.47, -4.58]* | -3.21 [-3.75, -2.63]* | 0.27 [-1.12, 1.41] | 0.21 [-1.16, 1.35] | 1.80 [0.93, 2.45] | wt_med |
| 6 | -0.83 [-1.75, -0.21]* | -1.17 [-1.90, -0.68]* | 0.16 [-0.84, 0.86] | -0.91 [-1.81, -0.31]* | -1.14 [-1.66, -0.73]* | -1.12 [-1.64, -0.71]* | 0.93 [0.41, 1.58] | -0.19 [-0.28, -0.11]* | 4.41 [2.21, 7.15] | -0.37 [-0.81, -0.05]* | -0.35 [-1.13, 0.33] | -5.49 [-6.74, -4.34]* | -3.56 [-4.03, -3.11]* | 0.63 [-0.27, 1.32] | 0.59 [-0.30, 1.27] | 1.28 [0.46, 1.84] | wt_med |
| 7 | -0.43 [-1.37, 0.22] | -0.77 [-1.46, -0.24]* | 0.63 [-0.40, 1.34] | -0.53 [-1.43, 0.12] | -0.86 [-1.41, -0.47]* | -0.84 [-1.40, -0.45]* | 0.94 [0.38, 1.67] | -0.16 [-0.24, -0.09]* | 5.02 [2.70, 7.73] | 0.63 [0.10, 1.05] | 0.12 [-0.57, 0.69] | -5.48 [-6.69, -4.27]* | -2.25 [-2.97, -1.51]* | 1.22 [0.49, 1.81] | 1.18 [0.46, 1.75] |  | wt_med |
| 10 |  |  |  |  | -0.57 [-1.03, -0.18]* | -0.52 [-0.97, -0.14]* | 1.01 [0.66, 1.41] | -0.28 [-0.35, -0.22]* | 5.17 [3.33, 6.97] | -0.65 [-1.04, -0.29]* | 0.01 [-0.67, 0.59] | -5.17 [-6.02, -4.21]* | -4.14 [-4.56, -3.65]* | 1.77 [0.86, 2.56] | 1.71 [0.78, 2.53] |  | wt_med |
| 14 |  |  |  |  | -0.64 [-1.20, -0.25]* | -0.61 [-1.16, -0.22]* | 0.59 [0.01, 1.21] | -0.19 [-0.29, -0.11]* | 5.06 [2.60, 7.77] | 0.78 [0.19, 1.34] | 0.47 [-0.48, 1.44] | -5.96 [-7.30, -4.63]* | -2.02 [-2.81, -1.08]* | 1.65 [0.58, 2.68] | 1.60 [0.54, 2.62] |  | wt_med |
| 21 |  |  |  |  | -0.53 [-1.15, -0.10]* | -0.48 [-1.10, -0.04]* | 0.43 [-0.17, 1.06] | -0.20 [-0.32, -0.12]* | 4.79 [2.29, 7.54] | 0.98 [0.52, 1.41] | 0.24 [-0.30, 0.84] | -6.07 [-7.39, -4.84]* | -1.71 [-2.48, -0.72]* | 1.68 [0.98, 2.40] | 1.63 [0.93, 2.35] |  | wt_med |
| 30 |  |  |  |  | 0.33 [0.03, 0.59] | 0.36 [0.07, 0.61] | 0.77 [0.41, 1.17] | -0.19 [-0.28, -0.12]* | 6.14 [4.44, 8.00] | 0.59 [0.30, 0.84] | 1.21 [0.44, 2.02] | -4.24 [-5.05, -3.43]* | -3.15 [-3.68, -2.65]* | 3.74 [2.51, 5.07] | 3.64 [2.40, 4.96] |  | wt_med |
| 45 |  |  |  |  | -0.85 [-1.30, -0.39]* | -0.77 [-1.25, -0.33]* | 1.02 [0.57, 1.59] | -0.34 [-0.43, -0.25]* | 2.74 [1.25, 4.57] | -1.03 [-1.55, -0.53]* | -0.28 [-1.00, 0.57] | -6.53 [-7.53, -5.50]* | -4.23 [-4.78, -3.62]* | 1.54 [0.62, 2.51] | 1.45 [0.51, 2.46] |  | wt_med |
| 60 |  |  |  |  | -0.45 [-0.92, -0.05]* | -0.37 [-0.88, 0.08] | 0.13 [-0.68, 0.82] | -0.17 [-0.24, -0.10] |  | 0.46 [-0.09, 0.98] |  | -6.31 [-7.34, -5.30]* | -2.63 [-3.40, -1.83]* |  |  |  | wt_med |
| 90 |  |  |  |  | -0.47 [-1.05, -0.01]* | -0.42 [-1.05, 0.12] | 0.48 [-0.37, 1.54] | -0.19 [-0.25, -0.13] |  | 0.44 [-0.21, 0.99] |  | -7.20 [-8.75, -5.71]* | -2.41 [-3.19, -1.65]* |  |  |  | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -2.289 [-2.847, -1.611]* / [-2.837, -1.768]* | -1.042 [-1.600, -0.374]* / [-1.695, -0.448]* | -2.404 [-2.995, -1.707]* / [-2.968, -1.898]* | -2.042 [-2.551, -1.474]* / [-2.502, -1.579]* | -2.284 [-2.963, -1.466]* / [-2.974, -1.568]* | 2.928 [1.049, 4.918] / [1.883, 4.025] |  |  |
| 2 | -2.575 [-3.071, -2.008]* / [-3.000, -2.128]* | -1.793 [-2.486, -1.013]* / [-2.284, -1.280]* | -2.688 [-3.196, -2.097]* / [-3.116, -2.235]* | -2.432 [-2.851, -1.937]* / [-2.807, -2.047]* | -1.937 [-2.632, -1.191]* / [-2.523, -1.384]* | 4.077 [2.353, 6.149] / [2.886, 5.312] |  |  |
| 3 | -2.712 [-3.437, -1.932]* / [-3.315, -2.122]* | -1.465 [-2.075, -0.692]* / [-2.192, -0.777]* | -2.945 [-3.655, -2.141]* / [-3.540, -2.361]* | -3.224 [-3.784, -2.559]* / [-3.802, -2.676]* | -2.896 [-3.624, -1.954]* / [-3.711, -2.102]* | 2.534 [0.826, 4.425] / [1.189, 3.850] | -1.480 [-2.442, -0.327]* / [-2.372, -0.591]* | -1.546 [-2.518, -0.384]* / [-2.436, -0.663]* |
| 4 | -2.315 [-3.036, -1.460]* / [-3.115, -1.613]* | -1.319 [-1.984, -0.632]* / [-2.221, -0.551]* | -2.413 [-3.098, -1.576]* / [-3.206, -1.712]* | -2.176 [-2.668, -1.594]* / [-2.905, -1.573]* | -2.421 [-3.171, -1.544]* / [-3.313, -1.609]* | 2.928 [1.212, 4.863] / [1.393, 4.445] | -1.556 [-2.716, -0.193]* / [-2.551, -0.667]* | -1.616 [-2.770, -0.242]* / [-2.599, -0.719]* |
| 5 | -1.800 [-2.449, -0.926]* / [-2.540, -1.105]* | -0.722 [-1.507, 0.214] / [-1.512, 0.060] | -1.918 [-2.564, -1.084]* / [-2.643, -1.240]* | -1.924 [-2.459, -1.258]* / [-2.556, -1.305]* | -2.233 [-2.774, -1.615]* / [-3.141, -1.435]* | 2.666 [1.201, 4.189] / [1.179, 4.180] | -1.582 [-2.674, -0.636]* / [-2.631, -0.668]* | -1.637 [-2.733, -0.699]* / [-2.686, -0.716]* |
| 6 | -1.278 [-1.844, -0.460]* / [-2.007, -0.583]* | -0.206 [-1.003, 0.858] / [-1.035, 0.588] | -1.439 [-2.005, -0.624]* / [-2.164, -0.752]* | -1.681 [-1.994, -1.215]* / [-2.317, -1.119]* | -1.704 [-2.392, -1.023]* / [-2.453, -0.962]* | 3.166 [1.218, 5.580] / [1.776, 4.555] | -0.640 [-1.379, 0.083] / [-1.416, 0.122] | -0.681 [-1.416, 0.038] / [-1.455, 0.072] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 2.289 [1.611, 2.847] / [1.768, 2.837] | 1.180 [0.746, 1.810] / [0.949, 1.426] | -0.131 [-0.214, -0.064]* / [-0.185, -0.081]* | 0.256 [-0.079, 0.544] / [0.022, 0.521] | -0.270 [-1.179, 0.446] / [-0.777, 0.228] | 4.716 [2.427, 7.507] / [3.525, 5.856] |  |  |
| 2 | 2.575 [2.008, 3.071] / [2.128, 3.000] | 0.735 [0.461, 1.037] / [0.555, 0.949] | -0.128 [-0.218, -0.059]* / [-0.164, -0.096]* | 0.141 [-0.057, 0.361] / [-0.034, 0.318] | 0.414 [-0.173, 0.987] / [-0.002, 0.838] | 6.412 [4.854, 8.326] / [5.323, 7.493] |  |  |
| 3 | 2.712 [1.932, 3.437] / [2.122, 3.315] | 1.060 [0.760, 1.419] / [0.831, 1.287] | -0.251 [-0.315, -0.196]* / [-0.296, -0.209]* | -0.505 [-0.828, -0.209]* / [-0.774, -0.240]* | -0.345 [-0.802, 0.142] / [-0.783, 0.078] | 4.992 [3.285, 6.811] / [3.862, 6.196] | 1.080 [0.434, 1.790] / [0.555, 1.574] | 1.018 [0.362, 1.740] / [0.492, 1.511] |
| 4 | 2.315 [1.460, 3.036] / [1.613, 3.115] | 0.863 [0.434, 1.384] / [0.609, 1.138] | -0.104 [-0.158, -0.040]* / [-0.153, -0.049]* | 0.096 [-0.228, 0.430] / [-0.207, 0.403] | -0.374 [-1.163, 0.312] / [-0.840, 0.139] | 4.782 [2.421, 7.564] / [3.468, 6.112] | 0.547 [-0.519, 1.584] / [-0.046, 1.150] | 0.488 [-0.563, 1.525] / [-0.102, 1.095] |
| 5 | 1.800 [0.926, 2.449] / [1.105, 2.540] | 0.969 [0.515, 1.523] / [0.699, 1.256] | -0.133 [-0.197, -0.078]* / [-0.179, -0.087]* | -0.072 [-0.521, 0.255] / [-0.371, 0.232] | -0.484 [-1.471, 0.366] / [-1.012, 0.009] | 4.445 [2.354, 6.694] / [3.323, 5.533] | 0.268 [-1.124, 1.415] / [-0.351, 0.869] | 0.215 [-1.162, 1.346] / [-0.408, 0.815] |
| 6 | 1.278 [0.460, 1.844] / [0.583, 2.007] | 0.933 [0.413, 1.581] / [0.645, 1.235] | -0.187 [-0.280, -0.111]* / [-0.240, -0.137]* | -0.370 [-0.810, -0.046]* / [-0.679, -0.053]* | -0.351 [-1.133, 0.332] / [-0.846, 0.153] | 4.411 [2.212, 7.153] / [3.287, 5.546] | 0.634 [-0.272, 1.318] / [0.057, 1.229] | 0.592 [-0.305, 1.271] / [0.018, 1.186] |
| 7 |  | 0.940 [0.375, 1.670] / [0.666, 1.226] | -0.159 [-0.242, -0.094]* / [-0.211, -0.109]* | 0.628 [0.105, 1.051] / [0.331, 0.950] | 0.125 [-0.569, 0.694] / [-0.363, 0.649] | 5.018 [2.699, 7.727] / [3.942, 6.137] | 1.225 [0.492, 1.813] / [0.610, 1.921] | 1.179 [0.456, 1.751] / [0.574, 1.879] |
| 10 |  | 1.015 [0.658, 1.412] / [0.759, 1.293] | -0.284 [-0.355, -0.222]* / [-0.324, -0.245]* | -0.651 [-1.041, -0.295]* / [-0.919, -0.399]* | 0.007 [-0.674, 0.589] / [-0.482, 0.497] | 5.168 [3.328, 6.973] / [4.009, 6.257] | 1.773 [0.858, 2.562] / [1.190, 2.340] | 1.712 [0.779, 2.529] / [1.132, 2.283] |
| 14 |  | 0.587 [0.010, 1.211] / [0.287, 0.884] | -0.186 [-0.290, -0.107]* / [-0.236, -0.136]* | 0.777 [0.193, 1.338] / [0.435, 1.115] | 0.465 [-0.479, 1.442] / [-0.131, 1.049] | 5.060 [2.605, 7.766] / [3.857, 6.264] | 1.649 [0.581, 2.680] / [0.907, 2.372] | 1.600 [0.541, 2.619] / [0.869, 2.329] |
| 21 |  | 0.430 [-0.165, 1.064] / [0.089, 0.763] | -0.205 [-0.321, -0.122]* / [-0.256, -0.155]* | 0.981 [0.520, 1.412] / [0.652, 1.308] | 0.243 [-0.299, 0.844] / [-0.353, 0.795] | 4.785 [2.289, 7.544] / [3.601, 5.985] | 1.680 [0.982, 2.400] / [0.925, 2.467] | 1.626 [0.931, 2.353] / [0.878, 2.411] |
| 30 |  | 0.772 [0.414, 1.169] / [0.519, 1.001] | -0.191 [-0.280, -0.122]* / [-0.231, -0.153]* | 0.586 [0.299, 0.839] / [0.377, 0.827] | 1.215 [0.439, 2.024] / [0.648, 1.767] | 6.136 [4.438, 8.001] / [5.039, 7.383] | 3.737 [2.512, 5.065] / [2.866, 4.647] | 3.635 [2.403, 4.959] / [2.756, 4.545] |
| 45 |  | 1.024 [0.571, 1.586] / [0.747, 1.332] | -0.344 [-0.430, -0.249]* / [-0.391, -0.299]* | -1.034 [-1.553, -0.531]* / [-1.309, -0.760]* | -0.280 [-1.004, 0.568] / [-0.952, 0.430] | 2.744 [1.247, 4.570] / [1.210, 4.159] | 1.539 [0.619, 2.509] / [0.689, 2.381] | 1.452 [0.513, 2.456] / [0.590, 2.300] |
| 60 |  | 0.131 [-0.684, 0.823] / [-0.209, 0.461] | -0.166 [-0.237, -0.101]* / [-0.207, -0.122]* | 0.456 [-0.085, 0.984] / [0.137, 0.803] |  |  |  |  |
| 90 |  | 0.476 [-0.370, 1.540] / [0.074, 0.878] | -0.186 [-0.254, -0.126]* / [-0.239, -0.136]* | 0.438 [-0.206, 0.994] / [-0.001, 0.858] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.042 [0.374, 1.600] / [0.448, 1.695] | -1.180 [-1.810, -0.746]* / [-1.426, -0.949]* | -1.303 [-1.957, -0.875]* / [-1.551, -1.066]* | -0.927 [-1.504, -0.496]* / [-1.294, -0.578]* | -1.229 [-1.966, -0.552]* / [-1.735, -0.739]* | 3.816 [1.546, 6.415] / [2.591, 4.904] |  |  |
| 2 | 1.793 [1.013, 2.486] / [1.280, 2.284] | -0.735 [-1.037, -0.461]* / [-0.949, -0.555]* | -0.853 [-1.175, -0.553]* / [-1.069, -0.668]* | -0.603 [-0.992, -0.226]* / [-0.880, -0.331]* | -0.449 [-1.367, 0.319] / [-0.941, 0.073] | 5.646 [4.236, 7.285] / [4.479, 6.809] |  |  |
| 3 | 1.465 [0.692, 2.075] / [0.777, 2.192] | -1.060 [-1.419, -0.760]* / [-1.287, -0.831]* | -1.315 [-1.663, -0.997]* / [-1.540, -1.082]* | -1.558 [-2.070, -1.093]* / [-1.920, -1.216]* | -1.533 [-2.198, -0.859]* / [-2.059, -1.029]* | 3.848 [2.290, 5.377] / [2.630, 5.164] | -0.053 [-0.916, 0.827] / [-0.635, 0.500] | -0.117 [-0.965, 0.768] / [-0.705, 0.435] |
| 4 | 1.319 [0.632, 1.984] / [0.551, 2.221] | -0.863 [-1.384, -0.434]* / [-1.138, -0.609]* | -0.954 [-1.424, -0.545]* / [-1.221, -0.709]* | -0.817 [-1.234, -0.451]* / [-1.207, -0.448]* | -0.992 [-1.779, -0.297]* / [-1.509, -0.465]* | 4.228 [2.075, 6.647] / [2.915, 5.542] | -0.045 [-1.156, 0.999] / [-0.696, 0.630] | -0.105 [-1.203, 0.928] / [-0.759, 0.570] |
| 5 | 0.722 [-0.214, 1.507] / [-0.060, 1.512] | -0.969 [-1.523, -0.515]* / [-1.256, -0.699]* | -1.094 [-1.620, -0.649]* / [-1.378, -0.827]* | -1.100 [-1.772, -0.575]* / [-1.543, -0.667]* | -1.511 [-2.465, -0.761]* / [-2.103, -0.945]* | 3.418 [1.224, 5.520] / [2.225, 4.563] | -0.767 [-1.918, 0.258] / [-1.496, -0.095]*  ! | -0.820 [-1.961, 0.194] / [-1.553, -0.157]*  ! |
| 6 | 0.206 [-0.858, 1.003] / [-0.588, 1.035] | -0.933 [-1.581, -0.413]* / [-1.235, -0.645]* | -1.114 [-1.710, -0.637]* / [-1.405, -0.842]* | -1.368 [-2.206, -0.746]* / [-1.842, -0.914]* | -1.220 [-2.073, -0.543]* / [-1.752, -0.708]* | 3.533 [1.359, 6.061] / [2.324, 4.649] | -0.231 [-1.168, 0.520] / [-0.894, 0.355] | -0.275 [-1.198, 0.466] / [-0.927, 0.298] |
| 7 |  | -0.940 [-1.670, -0.375]* / [-1.226, -0.666]* | -1.091 [-1.791, -0.557]* / [-1.385, -0.816]* | -0.331 [-1.458, 0.525] / [-0.771, 0.162] | -0.862 [-1.548, -0.340]* / [-1.413, -0.304]* | 4.058 [1.861, 6.500] / [2.971, 5.193] | 0.283 [-0.423, 0.864] / [-0.394, 1.008] | 0.235 [-0.474, 0.819] / [-0.444, 0.956] |
| 10 |  | -1.015 [-1.412, -0.658]* / [-1.293, -0.759]* | -1.290 [-1.709, -0.924]* / [-1.572, -1.034]* | -1.665 [-2.291, -1.057]* / [-2.102, -1.273]* | -1.339 [-2.454, -0.353]* / [-1.920, -0.717]* | 3.927 [2.109, 5.736] / [2.647, 5.217] | 0.507 [-0.824, 1.633] / [-0.149, 1.196] | 0.446 [-0.894, 1.582] / [-0.213, 1.147] |
| 14 |  | -0.587 [-1.211, -0.010]* / [-0.884, -0.287]* | -0.756 [-1.360, -0.208]* / [-1.042, -0.475]* | 0.161 [-0.865, 1.145] / [-0.321, 0.670] | -0.105 [-1.089, 0.832] / [-0.806, 0.603] | 4.561 [2.105, 7.068] / [3.397, 5.800] | 1.117 [0.113, 2.100] / [0.283, 1.901] | 1.068 [0.075, 2.034] / [0.235, 1.849] |
| 21 |  | -0.430 [-1.064, 0.165] / [-0.763, -0.089]*  ! | -0.613 [-1.248, -0.060]* / [-0.938, -0.269]* | 0.562 [-0.341, 1.409] / [0.014, 1.086] | -0.106 [-0.834, 0.623] / [-0.812, 0.565] | 4.494 [2.081, 6.892] / [3.267, 5.715] | 1.349 [0.450, 2.236] / [0.475, 2.154] | 1.293 [0.392, 2.181] / [0.422, 2.099] |
| 30 |  | -0.772 [-1.169, -0.414]* / [-1.001, -0.519]* | -0.947 [-1.331, -0.578]* / [-1.182, -0.696]* | -0.203 [-0.663, 0.221] / [-0.522, 0.136] | 0.505 [-0.661, 1.435] / [-0.116, 1.074] | 5.366 [3.624, 7.134] / [4.107, 6.746] | 3.002 [1.739, 4.342] / [2.194, 3.878] | 2.900 [1.614, 4.238] / [2.091, 3.767] |
| 45 |  | -1.024 [-1.586, -0.571]* / [-1.332, -0.747]* | -1.352 [-1.937, -0.874]* / [-1.660, -1.063]* | -2.025 [-2.892, -1.254]* / [-2.454, -1.604]* | -1.478 [-2.741, -0.167]* / [-2.362, -0.606]* | 1.180 [-1.257, 3.669] / [-0.618, 2.817] | 0.323 [-0.960, 1.614] / [-0.608, 1.214] | 0.235 [-1.084, 1.550] / [-0.705, 1.131] |
| 60 |  | -0.131 [-0.823, 0.684] / [-0.461, 0.209] | -0.283 [-0.972, 0.500] / [-0.617, 0.052] | 0.256 [-0.619, 1.080] / [-0.255, 0.785] |  |  |  |  |
| 90 |  | -0.476 [-1.540, 0.370] / [-0.878, -0.074]*  ! | -0.643 [-1.710, 0.208] / [-1.040, -0.250]*  ! | -0.031 [-1.429, 1.100] / [-0.697, 0.579] |  |  |  |  |

**MAE by region (all rides)**

| lead | region | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 8.48 | 8.55 | 9.08 | 8.25 | 8.71 | 8.70 | 10.01 | 9.07 | 13.32 | 9.82 | 8.98 | 6.42 | 8.12 |  |  | 10.97 | 9.15 |
| 1 | EU | 4.48 | 4.42 | 4.74 | 4.46 | 4.77 | 4.78 | 5.36 | 4.96 | 6.59 | 5.31 | 4.74 | 3.74 | 4.51 |  |  | 5.79 | 5.00 |
| 1 | NA | 7.38 | 7.10 | 7.83 | 7.33 | 7.49 | 7.50 | 8.29 | 7.73 | 9.28 | 7.85 | 7.18 | 6.14 | 5.73 |  |  | 9.17 | 7.76 |
| 3 | Asia | 9.24 | 8.79 | 9.85 | 9.11 | 8.84 | 8.85 | 9.64 | 9.07 | 13.11 | 9.05 | 8.81 | 6.07 | 7.41 | 9.17 | 9.13 | 10.76 | 9.17 |
| 3 | EU | 4.77 | 4.82 | 5.23 | 4.73 | 4.74 | 4.74 | 4.97 | 4.47 | 7.02 | 4.68 | 4.75 | 3.50 | 3.62 | 5.31 | 5.29 | 5.12 | 4.51 |
| 3 | NA | 8.01 | 7.93 | 8.53 | 7.98 | 7.95 | 7.95 | 8.44 | 7.85 | 9.59 | 7.84 | 8.06 | 5.98 | 5.92 | 9.32 | 9.30 | 9.71 | 8.01 |
| 7 | Asia | 9.42 | 9.12 | 10.02 | 9.34 | 9.17 | 9.18 | 10.24 | 9.41 | 13.47 | 10.05 | 9.24 | 6.26 | 8.76 | 9.63 | 9.59 |  | 9.48 |
| 7 | EU | 5.20 | 5.20 | 5.47 | 5.20 | 5.24 | 5.23 | 5.74 | 5.45 | 7.86 | 5.74 | 5.72 | 3.74 | 5.04 | 6.06 | 6.05 |  | 5.53 |
| 7 | NA | 8.36 | 8.07 | 8.83 | 8.30 | 8.09 | 8.10 | 8.63 | 8.01 | 9.82 | 8.63 | 7.80 | 6.03 | 6.83 | 8.62 | 8.61 |  | 8.07 |
| 14 | Asia |  |  |  |  | 9.71 | 9.71 | 10.27 | 9.78 | 13.97 | 10.56 | 10.24 | 6.37 | 9.41 | 10.81 | 10.76 |  | 9.87 |
| 14 | EU |  |  |  |  | 5.48 | 5.49 | 5.86 | 5.68 | 8.04 | 5.98 | 5.89 | 3.85 | 5.35 | 6.16 | 6.15 |  | 5.78 |
| 14 | NA |  |  |  |  | 8.44 | 8.46 | 8.86 | 8.31 | 9.98 | 8.86 | 8.12 | 6.11 | 7.07 | 8.86 | 8.86 |  | 8.39 |
| 30 | Asia |  |  |  |  | 10.15 | 10.14 | 9.90 | 9.79 | 14.16 | 10.44 | 10.05 | 6.98 | 8.86 | 11.20 | 11.13 |  | 9.87 |
| 30 | EU |  |  |  |  | 5.92 | 5.92 | 6.10 | 5.69 | 8.81 | 5.91 | 6.42 | 4.26 | 4.64 | 6.99 | 6.97 |  | 5.73 |
| 30 | NA |  |  |  |  | 9.03 | 9.08 | 8.77 | 8.46 | 11.44 | 8.87 | 9.84 | 6.89 | 6.39 | 11.58 | 11.55 |  | 8.57 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | chronos2 | chronos2_x_h5 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.24 | -0.91 | -0.76 | -1.14 | -0.78 | -1.57 | -5.47 |  |  | -1.63 |  | -1.70 | -2.16 | -1.55 |  | -0.44 | 0.15 |
| 1 | 0.15 | -0.86 | -0.68 | -1.11 | -0.63 | -0.48 | -4.85 |  |  | -1.57 | -0.78 | -1.68 | -2.09 | -1.48 | -0.77 | -0.31 | 0.18 |
| 2 | 0.11 | -2.04 | -1.30 | -2.24 | -0.83 | -0.57 | -6.70 |  |  | -3.57 | -1.91 | -3.70 | -4.61 | -3.45 | -1.91 | -0.58 | 0.18 |
| 3 | 0.12 | 0.75 | 1.50 | 0.49 | -0.59 | -0.46 | -5.58 | 2.01 | 2.00 | -1.44 | -0.43 | -1.73 | -2.04 | -1.34 | -0.43 | -0.27 | 0.05 |
| 4 | 0.06 | -1.51 | -1.28 | -1.76 | -1.04 | -1.58 | -5.93 | 0.95 | 0.94 | -2.52 | -1.63 | -2.53 | -2.73 | -2.44 | -1.63 | -0.49 | -0.10 |
| 5 | 0.08 | -1.03 | -0.79 | -1.28 | -0.87 | -1.75 | -5.95 | 0.65 | 0.65 | -2.10 | -1.28 | -2.09 | -2.19 | -1.93 | -1.29 | -0.52 | 0.06 |
| 6 | 0.19 | -0.28 | -0.07 | -0.54 | -0.55 | -1.26 | -5.38 | 1.04 | 1.03 | -1.50 | -0.74 | -1.54 | -1.59 | -1.40 | -0.73 | -0.38 | 0.26 |
| 7 |  | -0.76 | -0.50 | -1.01 | -0.57 | -1.36 | -5.71 | 0.96 | 0.96 | -1.90 | -0.95 | -2.12 | -2.03 | -1.80 | -0.94 | -0.42 | 0.38 |
| 10 |  | 0.97 | 1.73 | 0.71 | -0.37 | -0.14 | -5.90 | 2.23 | 2.22 |  | -0.05 |  |  |  | -0.06 | -0.24 | 0.27 |
| 14 |  | -0.63 | -0.36 | -0.89 | -0.37 | -1.18 | -5.79 | 1.05 | 1.04 |  | -0.85 |  |  |  | -0.84 | -0.36 | 0.58 |
| 21 |  | -0.57 | -0.29 | -0.84 | -0.32 | -0.65 | -5.85 | 1.66 | 1.65 |  | -0.71 |  |  |  | -0.71 | -0.32 | 0.63 |
| 30 |  | -1.43 | -0.45 | -1.64 | -0.26 | 0.08 | -7.30 | 2.74 | 2.73 |  | -1.34 |  |  |  | -1.35 | -0.48 | 0.70 |
| 45 |  | 1.85 | 2.96 | 1.57 | 0.31 | -0.42 | -4.96 | 2.01 | 2.00 |  | 0.86 |  |  |  | 0.84 | -0.15 | 0.94 |
| 60 |  | -0.41 | 0.15 | -0.69 | -0.08 | -0.14 |  | -0.04 | -0.04 |  | -0.05 |  |  |  | -0.07 | -0.31 | 0.86 |
| 90 |  | 1.47 | 2.08 | 1.19 | 1.04 |  |  |  |  |  | 1.32 |  |  |  | 1.33 | -0.25 | 1.77 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.14 [6.78, 7.49] | 7.08 [6.73, 7.44] | 7.55 [7.20, 7.89] | 6.73 [6.33, 7.15] |  |  |
| 1 | 7.12 [6.80, 7.47] | 7.06 [6.74, 7.39] | 7.64 [7.30, 7.97] | 6.77 [6.37, 7.19] |  |  |
| 2 | 7.25 [6.92, 7.60] | 7.21 [6.88, 7.56] | 7.48 [7.17, 7.83] | 7.51 [7.16, 7.89] |  |  |
| 3 | 7.15 [6.78, 7.51] | 7.05 [6.68, 7.40] | 7.14 [6.77, 7.50] | 6.99 [6.57, 7.43] | 7.51 [7.11, 7.94] | 7.48 [7.08, 7.92] |
| 4 | 7.71 [7.27, 8.15] | 7.66 [7.23, 8.11] | 7.87 [7.44, 8.29] | 7.42 [6.96, 7.97] | 7.62 [7.14, 8.15] | 7.60 [7.12, 8.13] |
| 5 | 7.67 [7.22, 8.14] | 7.59 [7.14, 8.06] | 7.76 [7.31, 8.25] | 7.49 [6.97, 8.05] | 7.68 [7.16, 8.22] | 7.66 [7.14, 8.20] |
| 6 | 7.34 [6.98, 7.72] | 7.25 [6.88, 7.62] | 7.36 [6.98, 7.74] | 7.14 [6.71, 7.59] | 7.49 [7.07, 7.95] | 7.47 [7.05, 7.93] |
| 7 | 7.65 [7.27, 8.04] | 7.55 [7.19, 7.93] | 8.06 [7.67, 8.46] | 7.44 [7.02, 7.89] | 7.87 [7.42, 8.40] | 7.86 [7.41, 8.38] |
| 10 | 7.14 [6.77, 7.50] | 7.02 [6.65, 7.36] | 7.08 [6.74, 7.41] | 7.10 [6.68, 7.54] | 7.63 [7.22, 8.11] | 7.61 [7.20, 8.09] |
| 14 | 7.75 [7.36, 8.17] | 7.62 [7.24, 8.03] | 8.06 [7.65, 8.49] | 7.73 [7.25, 8.27] | 8.15 [7.63, 8.70] | 8.13 [7.61, 8.68] |
| 21 | 7.78 [7.40, 8.20] | 7.64 [7.26, 8.05] | 8.20 [7.78, 8.63] | 7.83 [7.35, 8.36] | 8.24 [7.75, 8.79] | 8.23 [7.73, 8.77] |
| 30 | 7.50 [7.11, 7.89] | 7.41 [7.02, 7.78] | 7.83 [7.44, 8.25] | 7.89 [7.39, 8.43] | 8.73 [8.16, 9.30] | 8.69 [8.13, 9.27] |
| 45 | 7.23 [6.79, 7.67] | 7.03 [6.60, 7.47] | 7.13 [6.71, 7.57] | 7.52 [6.84, 8.30] | 8.41 [7.70, 9.27] | 8.38 [7.67, 9.23] |
| 60 | 7.71 [7.18, 8.22] | 7.55 [7.04, 8.06] | 8.13 [7.57, 8.67] | 0.43 [0.06, 1.18] | 0.31 [0.02, 0.93] | 0.31 [0.02, 0.93] |
| 90 | 7.07 [6.32, 7.80] | 6.88 [6.16, 7.58] | 7.08 [6.38, 7.79] |  |  |  |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 6.60 [6.21, 6.97] | 6.60 [6.21, 6.98] | 7.23 [6.81, 7.65] | 6.01 [5.62, 6.47] |  |  |
| 1 | 6.97 [6.48, 7.59] | 6.97 [6.47, 7.58] | 6.85 [6.40, 7.36] | 6.20 [5.77, 6.67] |  |  |
| 2 | 6.56 [6.19, 6.96] | 6.52 [6.13, 6.92] | 6.60 [6.26, 6.99] | 7.22 [6.69, 7.75] |  |  |
| 3 | 6.12 [5.76, 6.50] | 6.01 [5.66, 6.39] | 6.05 [5.70, 6.43] | 6.27 [5.72, 6.86] | 7.68 [7.03, 8.39] | 7.66 [7.02, 8.37] |
| 4 | 7.31 [6.79, 7.93] | 7.29 [6.79, 7.92] | 7.70 [7.15, 8.39] | 7.56 [6.93, 8.22] | 8.60 [7.84, 9.46] | 8.57 [7.82, 9.43] |
| 5 | 7.25 [6.78, 7.71] | 7.25 [6.79, 7.70] | 7.43 [6.98, 7.91] | 6.81 [6.34, 7.34] | 7.62 [7.05, 8.20] | 7.61 [7.04, 8.18] |
| 6 | 7.02 [6.57, 7.52] | 7.00 [6.54, 7.50] | 7.16 [6.74, 7.64] | 6.72 [6.27, 7.23] | 7.47 [6.90, 8.07] | 7.46 [6.89, 8.05] |
| 7 | 6.94 [6.54, 7.35] | 6.93 [6.53, 7.35] | 7.52 [7.12, 7.94] | 6.81 [6.38, 7.34] | 7.55 [6.97, 8.22] | 7.55 [6.96, 8.21] |
| 10 | 6.91 [6.45, 7.46] | 6.81 [6.34, 7.35] | 6.79 [6.33, 7.33] | 7.67 [6.84, 8.62] | 9.11 [8.18, 10.20] | 9.10 [8.17, 10.18] |
| 14 | 7.88 [7.39, 8.46] | 7.87 [7.38, 8.46] | 8.71 [8.11, 9.38] | 7.72 [7.13, 8.40] | 8.43 [7.76, 9.23] | 8.42 [7.75, 9.22] |
| 21 | 8.27 [7.77, 8.85] | 8.27 [7.76, 8.85] | 9.26 [8.66, 9.94] | 8.39 [7.72, 9.16] | 9.50 [8.64, 10.48] | 9.48 [8.63, 10.47] |
| 30 | 8.16 [7.71, 8.65] | 8.09 [7.64, 8.57] | 8.47 [8.02, 8.94] | 9.25 [8.61, 9.95] | 10.81 [9.98, 11.64] | 10.77 [9.94, 11.59] |
| 45 | 8.60 [8.11, 9.13] | 8.45 [7.98, 8.97] | 8.19 [7.72, 8.73] | 8.32 [7.51, 9.16] | 9.00 [8.20, 9.83] | 8.97 [8.18, 9.80] |
| 60 | 8.94 [8.41, 9.57] | 8.92 [8.38, 9.55] | 9.11 [8.58, 9.70] | 1.01 [0.22, 2.07] | 0.70 [0.00, 2.03] | 0.70 [0.00, 2.05] |
| 90 | 9.24 [8.80, 9.73] | 9.18 [8.73, 9.67] | 9.86 [9.38, 10.39] |  |  |  |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | chronos2 | chronos2_x_h5 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 106241.00 | 6.87 | 7.03 | 7.07 | 6.81 | 6.98 | 6.31 | 8.07 |  |  | 5.37 |  | 5.32 | 5.71 | 5.22 |  | 4.50 | 6.06 |
| 0 | spring | 267514.00 | 9.41 | 7.74 | 8.87 | 7.69 | 8.33 | 7.26 |  |  |  | 6.48 |  | 6.35 | 6.80 | 6.44 |  | 5.44 | 7.00 |
| 0 | summer | 365726.00 | 7.86 | 6.35 | 7.00 | 6.36 | 6.91 | 6.62 | 9.84 |  |  | 5.96 |  | 5.74 | 6.22 | 5.81 |  | 4.89 | 5.59 |
| 0 | winter | 29209.00 | 10.56 | 8.72 | 10.37 | 8.81 | 9.41 |  |  |  |  | 8.41 |  | 8.49 | 8.48 | 8.28 |  | 6.31 | 7.70 |
| 1 | autumn | 124017.00 | 7.11 | 7.01 | 7.14 | 6.82 | 6.80 | 6.74 | 8.61 |  |  | 5.96 | 6.19 | 5.94 | 6.42 | 5.89 | 6.21 | 4.75 | 5.53 |
| 1 | spring | 288635.00 | 9.94 | 8.08 | 8.94 | 8.03 | 8.64 | 7.55 |  |  |  | 7.48 | 7.70 | 7.37 | 8.01 | 7.44 | 7.70 | 5.91 | 6.93 |
| 1 | summer | 367857.00 | 7.48 | 6.25 | 6.81 | 6.22 | 6.65 | 6.55 | 9.16 |  |  | 5.94 | 6.18 | 5.80 | 6.25 | 5.82 | 6.17 | 4.91 | 5.22 |
| 1 | winter | 31024.00 | 10.63 | 8.29 | 9.55 | 8.39 | 9.17 |  |  |  |  | 7.92 | 7.89 | 8.20 | 8.50 | 7.82 | 7.91 | 6.35 | 7.72 |
| 3 | autumn | 138439.00 | 9.00 | 7.31 | 7.74 | 7.22 | 7.54 | 7.58 | 9.84 | 8.11 | 8.09 | 7.96 | 7.55 | 7.84 | 8.58 | 7.87 | 7.54 | 5.19 | 6.06 |
| 3 | spring | 317163.00 | 8.80 | 7.26 | 7.77 | 7.13 | 7.13 | 6.63 |  | 7.44 | 7.42 | 7.47 | 7.31 | 7.46 | 8.13 | 7.44 | 7.31 | 5.16 | 5.53 |
| 3 | summer | 381756.00 | 7.11 | 6.37 | 6.81 | 6.28 | 6.34 | 6.59 | 9.20 | 7.35 | 7.33 | 6.20 | 6.21 | 5.96 | 6.59 | 6.12 | 6.20 | 4.73 | 4.94 |
| 3 | winter | 16545.00 | 10.41 | 9.49 | 9.84 | 9.41 | 9.86 |  |  |  |  | 9.98 | 9.30 | 9.73 | 10.03 | 9.91 | 9.30 | 6.29 | 7.94 |
| 7 | autumn | 105678.00 |  | 7.64 | 7.71 | 7.38 | 7.74 | 7.26 | 8.56 | 7.80 | 7.79 | 6.64 | 6.58 | 6.60 | 7.10 | 6.69 | 6.56 | 4.61 | 6.99 |
| 7 | spring | 259981.00 |  | 8.42 | 9.08 | 8.33 | 8.89 |  |  |  |  | 8.03 | 8.06 | 8.01 | 8.59 | 8.00 | 8.06 | 5.63 | 7.56 |
| 7 | summer | 364561.00 |  | 6.70 | 7.25 | 6.68 | 7.29 | 7.31 | 10.39 | 7.80 | 7.79 | 7.12 | 6.85 | 6.77 | 7.43 | 7.05 | 6.84 | 5.02 | 6.03 |
| 7 | winter | 13722.00 |  | 9.46 | 10.56 | 9.52 | 9.86 |  |  |  |  | 9.59 | 9.62 | 9.72 | 10.50 | 9.49 | 9.71 | 6.22 | 8.13 |
| 14 | autumn | 105319.00 |  | 8.05 | 7.71 | 7.79 | 8.28 | 7.92 | 8.95 | 8.46 | 8.45 |  | 7.13 |  |  |  | 7.13 | 4.68 | 7.68 |
| 14 | spring | 251700.00 |  | 8.88 | 9.36 | 8.76 | 9.30 |  |  |  |  |  | 8.56 |  |  |  | 8.54 | 5.78 | 8.03 |
| 14 | summer | 363372.00 |  | 6.96 | 7.43 | 6.93 | 7.59 | 7.67 | 10.61 | 8.15 | 8.13 |  | 7.17 |  |  |  | 7.16 | 5.14 | 6.35 |
| 30 | autumn | 147265.00 |  | 8.26 | 8.13 | 8.22 | 8.44 | 8.61 | 12.00 | 9.24 | 9.20 |  | 8.27 |  |  |  | 8.28 | 6.16 | 6.56 |
| 30 | spring | 250380.00 |  | 7.96 | 8.38 | 7.89 | 8.16 |  |  |  |  |  | 8.26 |  |  |  | 8.24 | 6.11 | 6.17 |
| 30 | summer | 394950.00 |  | 7.45 | 7.72 | 7.34 | 7.88 | 8.25 | 10.29 | 9.60 | 9.57 |  | 7.86 |  |  |  | 7.84 | 5.53 | 6.32 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

**Spearman of the daily level within park-month** (paired on the same days)

_no rows_

_no rows_

**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the `≥ predicted q75` rule (ties included, which is what flat predictions cost)

## D1 — next-best ride

Candidates: every (intraday origin, ride) with a live OPERATING wait — the same set for every model. Suggest when the forecast maximum within the lookahead (60 or 120 min) is ≥ live + 10 (`next-best-ride.ts`, walking time 0); correct when the TRUE maximum there is also ≥ live + 10. False-suggestion rate = 1 − precision.

**next-best precision**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.58 [0.52, 0.64] | 0.41 [0.39, 0.43] | 0.45 [0.43, 0.47] | 0.45 [0.42, 0.47] | 0.45 [0.42, 0.49] | 0.41 [0.40, 0.43] | 0.44 [0.41, 0.45] |
| 0-120 | 0.66 [0.60, 0.72] | 0.50 [0.47, 0.52] | 0.54 [0.51, 0.56] | 0.54 [0.51, 0.56] | 0.54 [0.50, 0.57] | 0.50 [0.48, 0.51] | 0.52 [0.50, 0.54] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.12 [-0.02, 0.27] | -0.02 [-0.04, -0.00] | 0.01 [0.00, 0.02]* | 0.01 [-0.01, 0.02] | 0.03 [0.00, 0.06]* | -0.03 [-0.06, -0.01] | wt_med |
| 0-120 | 0.12 [-0.01, 0.25] | -0.02 [-0.04, -0.01] | 0.02 [0.01, 0.03]* | 0.02 [-0.00, 0.03] | 0.04 [0.01, 0.07]* | -0.03 [-0.06, -0.00] | wt_med |

**next-best precision (top-3)**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.65 [0.61, 0.68] | 0.49 [0.47, 0.51] | 0.52 [0.50, 0.54] | 0.51 [0.49, 0.53] | 0.51 [0.48, 0.54] | 0.50 [0.48, 0.51] | 0.52 [0.50, 0.54] |
| 0-120 | 0.71 [0.68, 0.74] | 0.57 [0.55, 0.59] | 0.60 [0.58, 0.62] | 0.59 [0.57, 0.61] | 0.59 [0.57, 0.62] | 0.59 [0.57, 0.60] | 0.60 [0.58, 0.62] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.10 [0.02, 0.17]* | -0.03 [-0.04, -0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.01 [-0.01, 0.03] | -0.03 [-0.05, -0.01] | wt_med |
| 0-120 | 0.09 [0.02, 0.15]* | -0.03 [-0.05, -0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.00 [-0.02, 0.03] | -0.02 [-0.04, -0.00] | wt_med |

**next-best recall**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.22 [0.21, 0.23] | 0.49 [0.47, 0.51] | 0.41 [0.39, 0.43] | 0.37 [0.35, 0.39] | 0.20 [0.18, 0.22] | 0.55 [0.52, 0.57] | 0.50 [0.48, 0.52] |
| 0-120 | 0.24 [0.23, 0.25] | 0.51 [0.49, 0.53] | 0.42 [0.40, 0.44] | 0.38 [0.37, 0.40] | 0.21 [0.19, 0.23] | 0.59 [0.57, 0.61] | 0.52 [0.50, 0.54] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|---|
| 0-60 | -0.33 [-0.35, -0.29] | -0.06 [-0.08, -0.03] | -0.14 [-0.17, -0.11] | -0.18 [-0.20, -0.15] | -0.35 [-0.39, -0.30] | -0.05 [-0.07, -0.02] | snaive7 |
| 0-120 | -0.35 [-0.38, -0.32] | -0.08 [-0.11, -0.06] | -0.17 [-0.19, -0.13] | -0.21 [-0.23, -0.18] | -0.38 [-0.42, -0.32] | -0.07 [-0.10, -0.04] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| w0-45 | 6.77 [6.60, 6.94] | 14.21 [13.67, 14.75] | 12.89 [12.41, 13.38] | 13.39 [12.87, 13.89] | 11.74 [11.20, 12.30] | 7.15 [6.93, 7.34] | 15.14 [14.54, 15.74] | 12.94 [12.44, 13.42] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| w0-45 | -0.64 [-0.94, -0.33]* | 7.42 [5.79, 9.01] | 6.15 [4.76, 7.43] | 6.65 [5.23, 8.02] | 5.28 [4.12, 6.41] | 8.32 [6.89, 9.64] | 6.17 [4.79, 7.44] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | chronos2 |
|---|---|---|---|---|---|---|---|---|
| m015 | -0.19 | 0.62 | -0.82 | -0.48 | -1.70 | -1.34 | -3.62 | 0.25 |
| m030 | 0.03 | 0.55 | -0.98 | -0.65 | -1.46 | -1.11 | -3.38 | -0.16 |
| m045 | 0.52 | 0.54 | -1.06 | -0.72 | -1.26 | -0.93 | -3.18 | -0.29 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.76 [0.74, 0.77] | 0.81 [0.79, 0.82] | 0.70 [0.69, 0.72] | 0.76 [0.75, 0.77] |  |  | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.79 [0.78, 0.80] |  |  | 0.81 [0.80, 0.83] | 0.83 [0.82, 0.84] |
| 1 | 0.72 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.64 [0.62, 0.66] | 0.72 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.75 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.80] |  |  | 0.80 [0.79, 0.82] | 0.82 [0.81, 0.83] |
| 2 | 0.68 [0.67, 0.69] | 0.78 [0.77, 0.79] | 0.57 [0.55, 0.58] | 0.69 [0.67, 0.70] | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.84 [0.82, 0.85] | 0.82 [0.81, 0.83] | 0.73 [0.71, 0.74] | 0.81 [0.79, 0.82] | 0.79 [0.78, 0.80] |  |  | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] |
| 3 | 0.69 [0.67, 0.70] | 0.79 [0.78, 0.80] | 0.58 [0.56, 0.59] | 0.69 [0.67, 0.70] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.83 [0.82, 0.84] | 0.83 [0.81, 0.84] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.79 [0.77, 0.80] | 0.72 [0.71, 0.74] | 0.76 [0.74, 0.77] | 0.80 [0.79, 0.82] | 0.82 [0.81, 0.83] |
| 4 | 0.68 [0.67, 0.69] | 0.79 [0.78, 0.80] | 0.57 [0.55, 0.59] | 0.68 [0.67, 0.70] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.80, 0.83] | 0.82 [0.81, 0.83] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.73 [0.71, 0.74] | 0.75 [0.74, 0.77] | 0.80 [0.78, 0.81] | 0.82 [0.80, 0.83] |
| 5 | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.56 [0.54, 0.58] | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.81] | 0.82 [0.81, 0.83] | 0.83 [0.81, 0.84] | 0.74 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.80] | 0.73 [0.71, 0.74] | 0.76 [0.74, 0.77] | 0.81 [0.79, 0.82] | 0.82 [0.81, 0.83] |
| 6 | 0.67 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.56 [0.54, 0.58] | 0.68 [0.66, 0.69] | 0.80 [0.78, 0.81] | 0.79 [0.78, 0.80] | 0.81 [0.80, 0.83] | 0.83 [0.82, 0.84] | 0.75 [0.73, 0.77] | 0.81 [0.79, 0.82] | 0.78 [0.76, 0.79] | 0.72 [0.70, 0.74] | 0.75 [0.74, 0.77] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] |
| 7 | 0.66 [0.64, 0.68] | 0.78 [0.76, 0.79] | 0.55 [0.53, 0.56] | 0.66 [0.65, 0.68] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.81, 0.84] | 0.83 [0.82, 0.84] | 0.76 [0.74, 0.77] | 0.81 [0.80, 0.82] | 0.79 [0.77, 0.80] | 0.72 [0.70, 0.74] | 0.75 [0.73, 0.76] |  | 0.82 [0.81, 0.83] |
| 10 |  |  |  |  | 0.79 [0.77, 0.80] | 0.79 [0.77, 0.80] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.73 [0.71, 0.74] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.72 [0.71, 0.74] | 0.76 [0.74, 0.77] |  | 0.82 [0.81, 0.83] |
| 14 |  |  |  |  | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.75 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.78 [0.76, 0.79] | 0.72 [0.70, 0.73] | 0.74 [0.73, 0.76] |  | 0.82 [0.81, 0.83] |
| 21 |  |  |  |  | 0.79 [0.78, 0.80] | 0.79 [0.77, 0.80] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.74 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.77 [0.76, 0.79] | 0.72 [0.70, 0.73] | 0.74 [0.72, 0.76] |  | 0.82 [0.81, 0.83] |
| 30 |  |  |  |  | 0.77 [0.76, 0.79] | 0.77 [0.76, 0.79] | 0.84 [0.83, 0.85] | 0.82 [0.80, 0.83] | 0.70 [0.68, 0.72] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.80] | 0.72 [0.70, 0.74] | 0.74 [0.72, 0.76] |  | 0.83 [0.82, 0.84] |
| 45 |  |  |  |  | 0.78 [0.77, 0.80] | 0.78 [0.77, 0.79] | 0.83 [0.81, 0.84] | 0.82 [0.81, 0.83] | 0.70 [0.67, 0.73] | 0.78 [0.77, 0.80] | 0.78 [0.76, 0.80] | 0.73 [0.71, 0.75] | 0.76 [0.74, 0.78] |  | 0.82 [0.81, 0.83] |
| 60 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.84] | 0.82 [0.81, 0.84] |  | 0.80 [0.79, 0.81] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.82 [0.81, 0.83] |
| 90 |  |  |  |  | 0.79 [0.78, 0.81] | 0.80 [0.78, 0.81] | 0.83 [0.81, 0.85] | 0.82 [0.81, 0.84] |  | 0.80 [0.78, 0.81] |  |  |  |  | 0.82 [0.81, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.07 [-0.09, -0.05] | -0.01 [-0.03, 0.00] | -0.12 [-0.14, -0.10] | -0.06 [-0.09, -0.04] |  |  | -0.01 [-0.01, -0.00] | 0.01 [-0.00, 0.02] | -0.06 [-0.07, -0.04] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.01] |  |  | -0.04 [-0.05, -0.02] | wt_med |
| 1 | -0.09 [-0.12, -0.06] | -0.02 [-0.04, -0.00] | -0.17 [-0.21, -0.13] | -0.09 [-0.12, -0.06] | -0.02 [-0.03, -0.01] | -0.02 [-0.04, -0.01] | -0.01 [-0.01, -0.00] | 0.01 [-0.00, 0.02] | -0.06 [-0.08, -0.04] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.01] |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 2 | -0.14 [-0.17, -0.11] | -0.03 [-0.05, -0.01] | -0.25 [-0.29, -0.21] | -0.13 [-0.16, -0.10] | -0.04 [-0.06, -0.03] | -0.04 [-0.06, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, 0.01] | -0.09 [-0.11, -0.07] | -0.02 [-0.04, -0.01] | -0.03 [-0.05, -0.01] |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 3 | -0.12 [-0.15, -0.09] | -0.02 [-0.04, -0.00] | -0.23 [-0.26, -0.19] | -0.12 [-0.15, -0.09] | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.02] | -0.07 [-0.09, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.05, -0.01] | -0.09 [-0.12, -0.06] | -0.05 [-0.08, -0.03] | -0.04 [-0.05, -0.03] | wt_med |
| 4 | -0.13 [-0.16, -0.09] | -0.02 [-0.03, -0.00] | -0.24 [-0.28, -0.19] | -0.12 [-0.15, -0.09] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.06 [-0.08, -0.04] | -0.01 [-0.02, 0.00] | -0.02 [-0.03, -0.01] | -0.08 [-0.11, -0.05] | -0.05 [-0.08, -0.03] | -0.03 [-0.05, -0.02] | wt_med |
| 5 | -0.14 [-0.17, -0.10] | -0.01 [-0.03, 0.00] | -0.25 [-0.30, -0.20] | -0.13 [-0.17, -0.09] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.06 [-0.08, -0.04] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.01] | -0.08 [-0.11, -0.05] | -0.05 [-0.07, -0.02] | -0.03 [-0.04, -0.02] | wt_med |
| 6 | -0.14 [-0.18, -0.11] | -0.02 [-0.03, -0.01] | -0.25 [-0.30, -0.20] | -0.14 [-0.17, -0.10] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.02]* | -0.06 [-0.07, -0.04] | -0.01 [-0.03, -0.00] | -0.03 [-0.04, -0.01] | -0.09 [-0.12, -0.05] | -0.05 [-0.09, -0.02] | -0.03 [-0.04, -0.02] | wt_med |
| 7 | -0.16 [-0.20, -0.12] | -0.04 [-0.06, -0.02] | -0.28 [-0.32, -0.22] | -0.16 [-0.20, -0.12] | -0.02 [-0.04, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.05 [-0.07, -0.03] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.00] | -0.09 [-0.13, -0.06] | -0.06 [-0.10, -0.03] |  | wt_med |
| 10 |  |  |  |  | -0.04 [-0.05, -0.02] | -0.04 [-0.05, -0.02] | -0.01 [-0.01, -0.00] | 0.00 [-0.01, 0.01] | -0.08 [-0.11, -0.06] | -0.03 [-0.04, -0.01] | -0.04 [-0.06, -0.02] | -0.09 [-0.11, -0.06] | -0.05 [-0.08, -0.03] |  | wt_med |
| 14 |  |  |  |  | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | 0.00 [-0.00, 0.01] | 0.01 [-0.00, 0.02] | -0.06 [-0.09, -0.04] | -0.02 [-0.03, -0.00] | -0.03 [-0.05, -0.01] | -0.09 [-0.13, -0.06] | -0.06 [-0.10, -0.03] |  | wt_med |
| 21 |  |  |  |  | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | 0.00 [-0.00, 0.01] | 0.01 [-0.00, 0.02] | -0.07 [-0.09, -0.04] | -0.02 [-0.03, -0.01] | -0.03 [-0.05, -0.01] | -0.09 [-0.13, -0.05] | -0.06 [-0.10, -0.03] |  | wt_med |
| 30 |  |  |  |  | -0.04 [-0.06, -0.02] | -0.04 [-0.06, -0.02] | -0.00 [-0.01, 0.00] | 0.00 [-0.02, 0.01] | -0.12 [-0.14, -0.10] | -0.02 [-0.04, -0.01] | -0.02 [-0.04, -0.01] | -0.09 [-0.13, -0.06] | -0.08 [-0.11, -0.05] |  | wt_med |
| 45 |  |  |  |  | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | 0.00 [-0.01, 0.01] | 0.01 [0.00, 0.02]* | -0.05 [-0.09, -0.00] | -0.02 [-0.04, -0.01] | -0.01 [-0.03, 0.00] | -0.06 [-0.09, -0.04] | -0.03 [-0.05, -0.01] |  | wt_med |
| 60 |  |  |  |  | -0.01 [-0.02, 0.00] | -0.01 [-0.02, -0.00] | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.02]* |  | -0.01 [-0.02, -0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |
| 90 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [-0.00, 0.02] | 0.00 [-0.00, 0.01] |  | -0.02 [-0.04, -0.01] |  |  |  |  | wt_med |

**best-time hit rate (±30 min)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.77 [0.76, 0.78] | 0.81 [0.80, 0.82] | 0.72 [0.71, 0.74] | 0.77 [0.76, 0.78] |  |  | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.77, 0.80] | 0.82 [0.81, 0.83] | 0.81 [0.79, 0.82] |  |  | 0.82 [0.80, 0.83] | 0.83 [0.82, 0.84] |
| 1 | 0.74 [0.73, 0.75] | 0.80 [0.79, 0.81] | 0.67 [0.65, 0.68] | 0.74 [0.73, 0.75] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.79] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] |  |  | 0.81 [0.80, 0.83] | 0.82 [0.81, 0.83] |
| 2 | 0.69 [0.68, 0.71] | 0.79 [0.77, 0.80] | 0.59 [0.57, 0.60] | 0.70 [0.68, 0.71] | 0.80 [0.80, 0.81] | 0.80 [0.80, 0.82] | 0.84 [0.83, 0.85] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.78] | 0.82 [0.81, 0.83] | 0.81 [0.80, 0.82] |  |  | 0.84 [0.83, 0.86] | 0.83 [0.82, 0.84] |
| 3 | 0.70 [0.69, 0.72] | 0.79 [0.78, 0.80] | 0.60 [0.59, 0.62] | 0.71 [0.69, 0.72] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.78] | 0.81 [0.81, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.82 [0.80, 0.83] | 0.82 [0.81, 0.83] |
| 4 | 0.70 [0.69, 0.71] | 0.80 [0.78, 0.81] | 0.60 [0.58, 0.62] | 0.70 [0.69, 0.71] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.77 [0.75, 0.78] | 0.82 [0.80, 0.83] | 0.80 [0.79, 0.81] | 0.76 [0.74, 0.77] | 0.77 [0.76, 0.79] | 0.81 [0.80, 0.83] | 0.82 [0.81, 0.83] |
| 5 | 0.70 [0.69, 0.72] | 0.80 [0.79, 0.81] | 0.59 [0.57, 0.61] | 0.70 [0.69, 0.72] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.84] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.79] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.82 [0.81, 0.84] | 0.82 [0.81, 0.84] |
| 6 | 0.70 [0.68, 0.71] | 0.80 [0.79, 0.81] | 0.59 [0.57, 0.61] | 0.70 [0.68, 0.71] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.78 [0.77, 0.80] | 0.82 [0.81, 0.83] | 0.79 [0.78, 0.81] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] |
| 7 | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.57 [0.56, 0.59] | 0.68 [0.66, 0.69] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.77, 0.80] | 0.82 [0.81, 0.83] | 0.80 [0.79, 0.82] | 0.75 [0.74, 0.77] | 0.77 [0.75, 0.78] |  | 0.83 [0.82, 0.84] |
| 10 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.82 [0.82, 0.83] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.77] | 0.78 [0.76, 0.79] |  | 0.82 [0.81, 0.83] |
| 14 |  |  |  |  | 0.81 [0.80, 0.82] | 0.81 [0.79, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.76, 0.79] | 0.82 [0.80, 0.83] | 0.80 [0.78, 0.81] | 0.75 [0.73, 0.76] | 0.77 [0.75, 0.78] |  | 0.83 [0.81, 0.84] |
| 21 |  |  |  |  | 0.81 [0.79, 0.82] | 0.81 [0.79, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.76, 0.79] | 0.81 [0.80, 0.82] | 0.80 [0.78, 0.81] | 0.75 [0.73, 0.77] | 0.76 [0.75, 0.78] |  | 0.82 [0.81, 0.83] |
| 30 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.84 [0.83, 0.85] | 0.83 [0.82, 0.84] | 0.74 [0.73, 0.76] | 0.82 [0.80, 0.83] | 0.81 [0.79, 0.82] | 0.76 [0.74, 0.77] | 0.76 [0.75, 0.78] |  | 0.84 [0.82, 0.85] |
| 45 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.84] | 0.73 [0.71, 0.76] | 0.81 [0.80, 0.82] | 0.80 [0.78, 0.82] | 0.76 [0.74, 0.78] | 0.78 [0.76, 0.80] |  | 0.82 [0.81, 0.83] |
| 60 |  |  |  |  | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.81, 0.84] |  | 0.81 [0.79, 0.82] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.82 [0.81, 0.83] |
| 90 |  |  |  |  | 0.80 [0.79, 0.82] | 0.80 [0.79, 0.82] | 0.83 [0.81, 0.85] | 0.82 [0.81, 0.84] |  | 0.80 [0.79, 0.82] |  |  |  |  | 0.82 [0.81, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.05 [-0.08, -0.03] | -0.01 [-0.03, 0.01] | -0.10 [-0.12, -0.08] | -0.05 [-0.07, -0.03] |  |  | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.06, -0.02] | -0.01 [-0.01, 0.00] | -0.01 [-0.02, -0.00] |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 1 | -0.08 [-0.11, -0.06] | -0.02 [-0.03, -0.00] | -0.15 [-0.18, -0.12] | -0.08 [-0.10, -0.06] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.00] |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 2 | -0.12 [-0.15, -0.10] | -0.03 [-0.05, -0.01] | -0.23 [-0.27, -0.20] | -0.12 [-0.15, -0.09] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.00 [-0.00, 0.01] | 0.01 [-0.00, 0.01] | -0.06 [-0.07, -0.04] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] |  |  | -0.01 [-0.02, -0.00] | wt_med |
| 3 | -0.11 [-0.13, -0.08] | -0.02 [-0.04, -0.00] | -0.21 [-0.24, -0.17] | -0.11 [-0.13, -0.08] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.06, -0.01] | -0.00 [-0.02, 0.01] | -0.01 [-0.02, 0.01] | -0.06 [-0.08, -0.04] | -0.04 [-0.06, -0.02] | -0.03 [-0.04, -0.02] | wt_med |
| 4 | -0.12 [-0.14, -0.09] | -0.02 [-0.03, -0.00] | -0.21 [-0.25, -0.17] | -0.11 [-0.14, -0.09] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.02]* | -0.03 [-0.05, -0.02] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.05 [-0.08, -0.03] | -0.04 [-0.06, -0.02] | -0.02 [-0.04, -0.01] | wt_med |
| 5 | -0.12 [-0.15, -0.08] | -0.01 [-0.03, 0.00] | -0.23 [-0.27, -0.18] | -0.11 [-0.15, -0.08] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.06, -0.02] | -0.01 [-0.02, 0.00] | -0.01 [-0.03, -0.00] | -0.06 [-0.08, -0.03] | -0.04 [-0.06, -0.01] | -0.02 [-0.04, -0.01] | wt_med |
| 6 | -0.12 [-0.15, -0.09] | -0.02 [-0.03, -0.00] | -0.23 [-0.27, -0.18] | -0.12 [-0.15, -0.08] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.03 [-0.05, -0.01] | -0.01 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.05 [-0.08, -0.03] | -0.03 [-0.06, -0.01] | -0.02 [-0.04, -0.01] | wt_med |
| 7 | -0.15 [-0.18, -0.11] | -0.03 [-0.05, -0.02] | -0.25 [-0.29, -0.20] | -0.14 [-0.18, -0.11] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.03 [-0.05, -0.01] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, -0.00] | -0.06 [-0.10, -0.03] | -0.05 [-0.08, -0.02] |  | wt_med |
| 10 |  |  |  |  | -0.01 [-0.03, -0.00] | -0.01 [-0.03, -0.00] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.02] | -0.05 [-0.07, -0.03] | -0.01 [-0.02, 0.01] | -0.01 [-0.03, 0.00] | -0.06 [-0.08, -0.04] | -0.03 [-0.06, -0.02] |  | wt_med |
| 14 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.02] | -0.04 [-0.06, -0.02] | -0.01 [-0.02, 0.00] | -0.02 [-0.03, -0.00] | -0.06 [-0.10, -0.03] | -0.04 [-0.08, -0.02] |  | wt_med |
| 21 |  |  |  |  | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.02] | -0.03 [-0.06, -0.01] | -0.01 [-0.02, 0.00] | -0.01 [-0.03, 0.00] | -0.05 [-0.09, -0.03] | -0.04 [-0.08, -0.01] |  | wt_med |
| 30 |  |  |  |  | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.01 [-0.01, -0.00] | 0.00 [-0.00, 0.01] | -0.08 [-0.10, -0.06] | -0.01 [-0.02, -0.00] | -0.01 [-0.03, -0.00] | -0.06 [-0.10, -0.04] | -0.06 [-0.09, -0.03] |  | wt_med |
| 45 |  |  |  |  | -0.01 [-0.03, 0.00] | -0.01 [-0.03, 0.00] | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.02]* | -0.04 [-0.08, 0.01] | -0.01 [-0.02, 0.01] | -0.01 [-0.03, 0.01] | -0.03 [-0.06, -0.01] | -0.01 [-0.04, 0.01] |  | wt_med |
| 60 |  |  |  |  | -0.01 [-0.01, 0.00] | -0.01 [-0.01, 0.00] | -0.00 [-0.01, 0.01] | 0.01 [0.01, 0.02]* |  | -0.01 [-0.02, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |
| 90 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.00 [-0.01, 0.02] | 0.00 [-0.00, 0.01] |  | -0.02 [-0.03, -0.00] |  |  |  |  | wt_med |

**best-time regret (min)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.35 [4.13, 4.59] | 3.37 [3.17, 3.60] | 5.68 [5.40, 6.00] | 4.29 [4.08, 4.52] |  |  | 3.17 [2.97, 3.40] | 2.88 [2.70, 3.09] | 4.04 [3.74, 4.35] | 3.26 [3.07, 3.48] | 3.39 [3.16, 3.63] |  |  | 3.77 [3.42, 4.16] | 3.14 [2.93, 3.38] |
| 1 | 5.07 [4.79, 5.34] | 3.82 [3.60, 4.08] | 7.54 [7.06, 8.02] | 5.03 [4.75, 5.30] | 3.50 [3.30, 3.69] | 3.51 [3.32, 3.70] | 3.34 [3.13, 3.58] | 3.04 [2.86, 3.23] | 3.93 [3.67, 4.20] | 3.44 [3.26, 3.63] | 3.44 [3.22, 3.66] |  |  | 3.90 [3.55, 4.29] | 3.23 [3.01, 3.44] |
| 2 | 6.45 [6.05, 6.83] | 4.56 [4.23, 4.86] | 10.44 [9.80, 11.09] | 6.37 [5.99, 6.77] | 3.76 [3.53, 3.97] | 3.77 [3.53, 3.98] | 3.00 [2.79, 3.21] | 3.16 [2.96, 3.37] | 4.54 [4.25, 4.84] | 3.35 [3.14, 3.55] | 3.48 [3.24, 3.74] |  |  | 3.56 [3.21, 3.94] | 3.25 [3.04, 3.49] |
| 3 | 6.07 [5.75, 6.42] | 4.04 [3.77, 4.32] | 9.57 [9.04, 10.16] | 6.02 [5.70, 6.37] | 3.51 [3.33, 3.71] | 3.50 [3.32, 3.69] | 3.13 [2.94, 3.36] | 3.02 [2.84, 3.20] | 4.21 [3.99, 4.47] | 3.36 [3.18, 3.53] | 3.58 [3.35, 3.82] | 4.77 [4.47, 5.13] | 3.88 [3.63, 4.16] | 3.59 [3.31, 3.92] | 3.31 [3.10, 3.54] |
| 4 | 6.48 [6.08, 6.94] | 3.87 [3.62, 4.14] | 10.30 [9.62, 11.07] | 6.42 [6.04, 6.89] | 3.52 [3.32, 3.73] | 3.53 [3.33, 3.74] | 3.43 [3.17, 3.72] | 3.07 [2.86, 3.29] | 4.19 [3.95, 4.45] | 3.31 [3.10, 3.54] | 3.64 [3.39, 3.90] | 5.17 [4.76, 5.62] | 4.28 [3.95, 4.66] | 3.99 [3.64, 4.34] | 3.27 [3.05, 3.51] |
| 5 | 6.51 [6.01, 7.01] | 3.76 [3.51, 4.04] | 10.38 [9.63, 11.14] | 6.37 [5.87, 6.87] | 3.52 [3.29, 3.74] | 3.53 [3.30, 3.76] | 3.30 [3.04, 3.59] | 3.10 [2.87, 3.34] | 4.31 [3.95, 4.73] | 3.47 [3.22, 3.73] | 3.72 [3.42, 4.06] | 4.99 [4.60, 5.44] | 4.17 [3.83, 4.56] | 3.65 [3.34, 4.00] | 3.21 [3.00, 3.44] |
| 6 | 6.44 [6.00, 6.87] | 3.91 [3.68, 4.17] | 10.03 [9.38, 10.70] | 6.32 [5.92, 6.72] | 3.54 [3.33, 3.76] | 3.55 [3.34, 3.77] | 3.35 [3.13, 3.62] | 3.09 [2.88, 3.30] | 4.22 [3.89, 4.55] | 3.45 [3.24, 3.69] | 3.88 [3.61, 4.19] | 5.18 [4.77, 5.66] | 4.29 [3.96, 4.67] | 3.67 [3.32, 4.00] | 3.31 [3.09, 3.55] |
| 7 | 6.88 [6.47, 7.33] | 4.17 [3.90, 4.48] | 10.54 [9.87, 11.23] | 6.76 [6.33, 7.20] | 3.52 [3.32, 3.76] | 3.54 [3.34, 3.78] | 3.17 [2.97, 3.41] | 2.95 [2.76, 3.16] | 4.20 [3.86, 4.58] | 3.38 [3.17, 3.61] | 3.71 [3.41, 4.07] | 5.09 [4.74, 5.49] | 4.30 [3.98, 4.65] |  | 3.19 [2.96, 3.45] |
| 10 |  |  |  |  | 3.62 [3.42, 3.84] | 3.64 [3.44, 3.85] | 3.11 [2.91, 3.33] | 3.04 [2.85, 3.25] | 4.46 [4.20, 4.74] | 3.44 [3.25, 3.63] | 3.66 [3.43, 3.89] | 4.77 [4.44, 5.14] | 3.84 [3.60, 4.11] |  | 3.28 [3.05, 3.50] |
| 14 |  |  |  |  | 3.65 [3.42, 3.89] | 3.66 [3.43, 3.90] | 3.18 [2.96, 3.44] | 3.06 [2.86, 3.28] | 4.36 [3.98, 4.76] | 3.47 [3.25, 3.72] | 3.83 [3.51, 4.19] | 5.20 [4.82, 5.60] | 4.36 [4.03, 4.74] |  | 3.25 [3.04, 3.51] |
| 21 |  |  |  |  | 3.65 [3.40, 3.90] | 3.66 [3.42, 3.91] | 3.21 [2.99, 3.47] | 3.07 [2.86, 3.30] | 4.50 [4.09, 4.93] | 3.53 [3.29, 3.77] | 3.97 [3.62, 4.35] | 5.17 [4.78, 5.56] | 4.36 [4.02, 4.74] |  | 3.24 [3.02, 3.50] |
| 30 |  |  |  |  | 3.91 [3.66, 4.13] | 3.93 [3.69, 4.15] | 3.15 [2.91, 3.41] | 3.25 [3.03, 3.49] | 5.32 [4.94, 5.73] | 3.50 [3.27, 3.74] | 3.73 [3.40, 4.11] | 5.19 [4.71, 5.72] | 4.67 [4.26, 5.11] |  | 3.23 [2.99, 3.48] |
| 45 |  |  |  |  | 3.54 [3.35, 3.74] | 3.57 [3.37, 3.76] | 3.00 [2.77, 3.23] | 3.03 [2.83, 3.22] | 5.09 [4.45, 5.82] | 3.48 [3.28, 3.68] | 3.73 [3.40, 4.11] | 4.76 [4.34, 5.29] | 3.96 [3.61, 4.37] |  | 3.22 [3.00, 3.45] |
| 60 |  |  |  |  | 3.63 [3.39, 3.88] | 3.63 [3.39, 3.89] | 3.31 [3.05, 3.56] | 3.20 [2.97, 3.44] |  | 3.65 [3.39, 3.92] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 3.37 [3.12, 3.63] |
| 90 |  |  |  |  | 3.87 [3.52, 4.24] | 3.89 [3.53, 4.24] | 3.32 [2.93, 3.73] | 3.37 [3.04, 3.72] |  | 3.88 [3.53, 4.27] |  |  |  |  | 3.43 [3.10, 3.80] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.09 [0.73, 1.48] | 0.14 [-0.21, 0.54] | 2.45 [1.86, 3.03] | 1.03 [0.67, 1.45] |  |  | 0.12 [-0.05, 0.28] | -0.31 [-0.52, -0.15]* | 0.86 [0.57, 1.14] | 0.12 [-0.09, 0.31] | 0.14 [-0.19, 0.38] |  |  | 1.07 [0.72, 1.45] | wt_med |
| 1 | 1.63 [1.17, 2.09] | 0.37 [0.04, 0.75] | 4.09 [2.99, 5.30] | 1.60 [1.16, 2.06] | 0.20 [-0.09, 0.43] | 0.22 [-0.06, 0.45] | 0.15 [0.01, 0.31] | -0.26 [-0.49, -0.07]* | 0.79 [0.42, 1.18] | 0.19 [-0.06, 0.41] | 0.19 [-0.14, 0.45] |  |  | 0.78 [0.41, 1.22] | wt_med |
| 2 | 2.83 [2.03, 3.56] | 0.93 [0.43, 1.43] | 6.84 [5.27, 8.60] | 2.75 [1.96, 3.47] | 0.37 [0.11, 0.62] | 0.37 [0.12, 0.63] | 0.04 [-0.07, 0.15] | -0.22 [-0.44, -0.02]* | 1.24 [0.86, 1.56] | 0.01 [-0.20, 0.21] | 0.10 [-0.19, 0.36] |  |  | 0.78 [0.46, 1.17] | wt_med |
| 3 | 2.46 [1.59, 3.22] | 0.41 [-0.08, 0.85] | 5.93 [4.52, 7.48] | 2.42 [1.60, 3.17] | 0.14 [-0.18, 0.42] | 0.13 [-0.20, 0.42] | 0.04 [-0.07, 0.14] | -0.35 [-0.63, -0.13]* | 0.88 [0.40, 1.25] | 0.04 [-0.23, 0.29] | 0.18 [-0.15, 0.47] | 1.42 [0.75, 2.22] | 0.48 [0.11, 0.86] | 0.71 [0.45, 0.98] | wt_med |
| 4 | 2.99 [2.12, 3.81] | 0.34 [0.03, 0.66] | 6.82 [5.13, 8.66] | 2.93 [2.09, 3.74] | 0.18 [-0.04, 0.41] | 0.18 [-0.03, 0.41] | 0.18 [0.02, 0.33] | -0.28 [-0.43, -0.14]* | 0.60 [0.24, 0.95] | 0.01 [-0.17, 0.19] | 0.16 [-0.03, 0.35] | 1.73 [0.89, 2.81] | 0.82 [0.29, 1.47] | 0.91 [0.58, 1.26] | wt_med |
| 5 | 3.17 [2.18, 4.23] | 0.26 [-0.05, 0.60] | 6.97 [5.08, 8.97] | 3.00 [2.05, 4.06] | 0.21 [-0.01, 0.42] | 0.23 [0.01, 0.44] | 0.15 [0.01, 0.31] | -0.21 [-0.33, -0.09]* | 0.84 [0.48, 1.19] | 0.20 [0.03, 0.35] | 0.30 [0.07, 0.50] | 1.50 [0.84, 2.21] | 0.65 [0.18, 1.18] | 0.82 [0.45, 1.14] | wt_med |
| 6 | 2.98 [2.01, 3.98] | 0.29 [-0.03, 0.66] | 6.61 [4.90, 8.42] | 2.85 [1.92, 3.81] | 0.08 [-0.21, 0.33] | 0.10 [-0.19, 0.35] | 0.05 [-0.04, 0.15] | -0.35 [-0.56, -0.19]* | 0.76 [0.36, 1.12] | 0.05 [-0.23, 0.31] | 0.24 [-0.12, 0.59] | 1.64 [0.92, 2.36] | 0.73 [0.30, 1.19] | 0.77 [0.41, 1.19] | wt_med |
| 7 | 3.61 [2.60, 4.68] | 0.75 [0.34, 1.20] | 7.35 [5.45, 9.21] | 3.49 [2.48, 4.51] | 0.28 [0.06, 0.48] | 0.30 [0.08, 0.51] | 0.04 [-0.11, 0.18] | -0.30 [-0.53, -0.13]* | 0.83 [0.45, 1.19] | 0.16 [-0.06, 0.37] | 0.30 [0.01, 0.58] | 1.71 [0.96, 2.50] | 0.91 [0.44, 1.41] |  | wt_med |
| 10 |  |  |  |  | 0.28 [0.00, 0.54] | 0.29 [0.01, 0.55] | 0.03 [-0.10, 0.15] | -0.30 [-0.49, -0.14]* | 1.02 [0.61, 1.42] | 0.14 [-0.11, 0.35] | 0.28 [-0.04, 0.56] | 1.36 [0.75, 2.03] | 0.41 [0.04, 0.79] |  | wt_med |
| 14 |  |  |  |  | 0.31 [0.11, 0.49] | 0.32 [0.12, 0.50] | -0.00 [-0.14, 0.11] | -0.28 [-0.49, -0.11]* | 0.94 [0.60, 1.29] | 0.19 [-0.09, 0.43] | 0.34 [0.06, 0.61] | 1.72 [0.98, 2.58] | 0.87 [0.39, 1.42] |  | wt_med |
| 21 |  |  |  |  | 0.29 [0.06, 0.52] | 0.31 [0.08, 0.55] | -0.02 [-0.15, 0.08] | -0.30 [-0.46, -0.13]* | 0.99 [0.55, 1.45] | 0.22 [-0.03, 0.44] | 0.35 [0.06, 0.65] | 1.60 [0.93, 2.31] | 0.77 [0.23, 1.33] |  | wt_med |
| 30 |  |  |  |  | 0.40 [0.08, 0.71] | 0.42 [0.11, 0.72] | 0.13 [-0.01, 0.26] | -0.20 [-0.48, 0.01] | 1.83 [1.32, 2.33] | 0.07 [-0.14, 0.26] | 0.14 [-0.17, 0.42] | 1.81 [0.98, 2.75] | 1.19 [0.46, 2.01] |  | wt_med |
| 45 |  |  |  |  | 0.22 [-0.08, 0.49] | 0.25 [-0.05, 0.52] | -0.04 [-0.18, 0.11] | -0.34 [-0.60, -0.15]* | 0.80 [-0.03, 1.47] | 0.18 [-0.13, 0.45] | 0.14 [-0.26, 0.56] | 1.05 [0.42, 1.69] | 0.23 [-0.28, 0.77] |  | wt_med |
| 60 |  |  |  |  | 0.10 [-0.06, 0.25] | 0.10 [-0.07, 0.26] | -0.07 [-0.34, 0.17] | -0.33 [-0.49, -0.19] |  | 0.12 [-0.07, 0.33] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |
| 90 |  |  |  |  | 0.28 [0.02, 0.52] | 0.30 [0.03, 0.54] | -0.08 [-0.30, 0.14] | -0.18 [-0.35, -0.05] |  | 0.29 [0.06, 0.54] |  |  |  |  | wt_med |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 40.87 [38.53, 43.45] | 36.06 [33.58, 38.51] | 46.60 [43.83, 49.27] | 40.48 [38.25, 42.95] | 36.86 [34.26, 39.74] | 37.14 [34.43, 39.99] | 39.10 [36.56, 41.95] | 36.70 [34.09, 39.40] | 31.77 [28.24, 35.29] | 36.79 [34.01, 39.49] | 34.31 [30.67, 37.95] |  |  | 43.79 [40.75, 46.85] | 37.76 [35.10, 40.54] |
| 3 | 42.47 [39.97, 44.85] | 36.86 [34.42, 39.29] | 48.55 [46.02, 51.31] | 43.78 [41.25, 46.45] | 36.53 [34.02, 39.45] | 36.73 [34.29, 39.61] | 39.85 [36.86, 43.04] | 37.06 [34.28, 40.03] | 36.16 [32.30, 40.21] | 35.67 [32.98, 38.30] | 34.07 [30.96, 37.51] | 41.72 [38.42, 45.01] | 42.10 [38.91, 45.21] | 41.49 [38.56, 44.51] | 37.65 [35.19, 40.42] |
| 7 | 44.46 [41.90, 46.96] | 36.83 [34.52, 39.27] | 51.79 [48.80, 55.12] | 45.27 [42.69, 47.89] | 36.53 [34.05, 39.14] | 36.56 [34.01, 39.28] | 39.21 [36.38, 42.15] | 37.41 [34.99, 40.11] | 33.59 [30.29, 37.18] | 37.94 [35.34, 40.69] | 33.94 [31.14, 37.40] | 39.57 [36.70, 42.80] | 42.10 [38.64, 45.94] |  | 39.33 [36.59, 42.40] |
| 14 |  |  |  |  | 37.61 [35.18, 40.41] | 37.75 [35.24, 40.54] | 39.59 [36.87, 42.46] | 38.37 [35.76, 41.31] | 37.07 [33.37, 40.97] | 39.41 [36.74, 42.28] | 35.95 [32.87, 39.55] | 41.11 [37.87, 44.30] | 40.71 [37.55, 44.07] |  | 40.55 [37.69, 43.77] |
| 30 |  |  |  |  | 38.02 [35.67, 40.70] | 38.41 [35.92, 41.03] | 41.99 [39.11, 45.14] | 37.71 [35.13, 40.51] | 39.16 [34.81, 44.03] | 39.49 [36.56, 42.71] | 37.33 [33.32, 41.52] | 49.42 [45.45, 54.28] | 49.15 [45.21, 53.95] |  | 39.74 [37.07, 42.59] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2.76 [-1.30, 6.43] | -2.31 [-5.11, 0.29] | 8.40 [4.16, 12.00] | 2.26 [-1.81, 6.05] | -1.21 [-3.80, 1.13] | -0.93 [-3.38, 1.37] | 1.31 [-1.60, 4.10] | -0.98 [-3.17, 0.98] | -2.08 [-4.59, 0.15] | -1.08 [-3.48, 1.14] | -1.59 [-4.48, 1.44] |  |  | 5.45 [2.53, 8.35] | wt_med |
| 3 | 4.83 [0.56, 9.34] | -1.01 [-5.08, 2.42] | 10.65 [7.00, 14.29] | 6.06 [1.93, 10.41] | -1.30 [-3.32, 0.87] | -1.09 [-3.09, 0.95] | 2.01 [-0.43, 5.23] | -0.71 [-2.83, 1.52] | -0.08 [-3.15, 4.02] | -2.34 [-4.98, 0.36] | -2.43 [-5.29, 0.33] | 5.01 [0.51, 9.31] | 5.38 [0.58, 10.11] | 3.91 [1.54, 6.55] | wt_med |
| 7 | 5.14 [1.63, 8.42] | -2.77 [-5.52, -0.05] | 12.51 [8.58, 16.56] | 6.06 [2.43, 9.41] | -2.92 [-5.75, -0.66]* | -2.89 [-5.62, -0.76]* | -0.41 [-2.68, 2.03] | -2.12 [-4.73, 0.04] | -3.89 [-8.03, -0.29]* | -1.43 [-3.57, 0.52] | -2.89 [-6.00, -0.23]* | 2.63 [-1.04, 5.97] | 5.17 [1.72, 8.96] |  | wt_med |
| 14 |  |  |  |  | -3.03 [-6.55, -0.25] | -2.89 [-6.41, -0.12] | -1.21 [-3.41, 0.40] | -2.24 [-5.35, 0.32] | -0.43 [-5.39, 3.77] | -0.59 [-3.42, 1.80] | -1.09 [-3.87, 1.84] | 3.63 [0.39, 7.01] | 3.24 [0.10, 6.29] |  | wt_med |
| 30 |  |  |  |  | -1.73 [-5.03, 1.21] | -1.33 [-4.73, 1.64] | 1.46 [-1.41, 3.98] | -2.03 [-4.78, 0.74] | -1.59 [-6.10, 3.75] | -0.54 [-3.18, 2.46] | -3.20 [-8.77, 1.36] | 8.50 [3.25, 13.68] | 8.24 [2.96, 13.85] |  | wt_med |

**dayPeak pairwise ordering (headliners)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.84 [0.83, 0.85] | 0.84 [0.83, 0.85] | 0.84 [0.83, 0.84] | 0.84 [0.83, 0.85] |  |  | 0.76 [0.75, 0.77] | 0.80 [0.79, 0.81] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.81 [0.79, 0.82] |  |  | 0.77 [0.75, 0.79] | 0.77 [0.76, 0.78] |
| 1 | 0.83 [0.82, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.75 [0.74, 0.77] | 0.79 [0.78, 0.80] | 0.74 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] |  |  | 0.76 [0.74, 0.78] | 0.77 [0.75, 0.78] |
| 2 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.78 [0.77, 0.79] | 0.81 [0.80, 0.81] | 0.74 [0.72, 0.75] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] |  |  | 0.77 [0.75, 0.79] | 0.78 [0.77, 0.79] |
| 3 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.81] | 0.81 [0.81, 0.82] | 0.81 [0.81, 0.82] | 0.77 [0.76, 0.79] | 0.80 [0.79, 0.81] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.76 [0.74, 0.77] | 0.78 [0.77, 0.79] |
| 4 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.81, 0.82] | 0.81 [0.80, 0.82] | 0.75 [0.74, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.77] | 0.79 [0.77, 0.80] | 0.75 [0.73, 0.78] | 0.77 [0.75, 0.78] |
| 5 | 0.82 [0.81, 0.82] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.80, 0.83] | 0.81 [0.80, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.80 [0.78, 0.81] | 0.76 [0.74, 0.77] | 0.79 [0.78, 0.80] | 0.77 [0.75, 0.78] | 0.76 [0.75, 0.77] |
| 6 | 0.80 [0.79, 0.82] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.77] | 0.78 [0.77, 0.79] | 0.73 [0.72, 0.75] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.74 [0.73, 0.76] | 0.78 [0.76, 0.79] | 0.76 [0.74, 0.78] | 0.76 [0.74, 0.77] |
| 7 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.81] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.74, 0.77] | 0.78 [0.77, 0.79] | 0.72 [0.70, 0.74] | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.80] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.80] |  | 0.76 [0.75, 0.77] |
| 10 |  |  |  |  | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.77 [0.75, 0.78] | 0.79 [0.78, 0.80] | 0.71 [0.69, 0.73] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.74 [0.73, 0.76] | 0.78 [0.77, 0.79] |  | 0.77 [0.76, 0.78] |
| 14 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.77 [0.76, 0.78] | 0.72 [0.70, 0.73] | 0.78 [0.76, 0.79] | 0.78 [0.76, 0.79] | 0.73 [0.72, 0.75] | 0.77 [0.75, 0.78] |  | 0.75 [0.74, 0.76] |
| 21 |  |  |  |  | 0.79 [0.78, 0.80] | 0.78 [0.77, 0.80] | 0.74 [0.73, 0.76] | 0.76 [0.75, 0.78] | 0.71 [0.69, 0.73] | 0.76 [0.75, 0.78] | 0.77 [0.75, 0.78] | 0.73 [0.71, 0.74] | 0.76 [0.74, 0.77] |  | 0.75 [0.73, 0.76] |
| 30 |  |  |  |  | 0.79 [0.77, 0.80] | 0.78 [0.77, 0.79] | 0.75 [0.74, 0.77] | 0.77 [0.76, 0.79] | 0.70 [0.68, 0.72] | 0.78 [0.77, 0.80] | 0.78 [0.76, 0.79] | 0.72 [0.71, 0.74] | 0.76 [0.74, 0.77] |  | 0.76 [0.75, 0.77] |
| 45 |  |  |  |  | 0.79 [0.77, 0.80] | 0.78 [0.77, 0.80] | 0.75 [0.73, 0.76] | 0.77 [0.76, 0.78] | 0.69 [0.66, 0.73] | 0.77 [0.76, 0.79] | 0.77 [0.75, 0.79] | 0.72 [0.70, 0.74] | 0.75 [0.73, 0.77] |  | 0.75 [0.74, 0.77] |
| 60 |  |  |  |  | 0.77 [0.75, 0.78] | 0.77 [0.75, 0.78] | 0.74 [0.72, 0.76] | 0.75 [0.74, 0.77] |  | 0.75 [0.74, 0.77] |  |  |  |  | 0.74 [0.72, 0.76] |
| 90 |  |  |  |  | 0.78 [0.77, 0.80] | 0.78 [0.77, 0.80] | 0.76 [0.74, 0.78] | 0.77 [0.75, 0.79] |  | 0.77 [0.76, 0.79] |  |  |  |  | 0.75 [0.73, 0.77] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.07 [0.05, 0.10]* | 0.07 [0.05, 0.10]* | 0.07 [0.05, 0.09]* | 0.07 [0.06, 0.10]* |  |  | -0.02 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.07, -0.00] | 0.02 [0.01, 0.04]* | 0.04 [0.03, 0.06]* |  |  | -0.02 [-0.04, 0.01] | wt_med |
| 1 | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.08]* | 0.07 [0.05, 0.09]* | 0.06 [0.05, 0.07]* | 0.06 [0.05, 0.07]* | -0.02 [-0.03, -0.01] | 0.03 [0.02, 0.03]* | -0.02 [-0.05, 0.01] | 0.03 [0.02, 0.05]* | 0.05 [0.03, 0.06]* |  |  | -0.02 [-0.04, 0.00] | wt_med |
| 2 | 0.03 [0.01, 0.05]* | 0.03 [0.01, 0.04]* | 0.02 [0.01, 0.04]* | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.04]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.05 [-0.09, -0.01] | 0.03 [0.02, 0.04]* | 0.02 [0.01, 0.04]* |  |  | -0.02 [-0.03, -0.00] | wt_med |
| 3 | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.00] | 0.02 [0.02, 0.03]* | -0.04 [-0.08, -0.01] | 0.03 [0.02, 0.04]* | 0.04 [0.02, 0.06]* | -0.01 [-0.02, 0.01] | 0.03 [0.01, 0.05]* | -0.03 [-0.04, -0.01] | wt_med |
| 4 | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.06 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.02 [-0.03, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, -0.00] | 0.02 [0.01, 0.04]* | 0.04 [0.03, 0.05]* | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.04]* | -0.01 [-0.03, 0.01] | wt_med |
| 5 | 0.06 [0.04, 0.07]* | 0.06 [0.05, 0.07]* | 0.06 [0.05, 0.07]* | 0.06 [0.05, 0.07]* | 0.06 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.04]* | -0.03 [-0.06, 0.00] | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.05]* | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.05]* | 0.00 [-0.01, 0.02] | wt_med |
| 6 | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.06 [0.04, 0.08]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, 0.00] | 0.04 [0.02, 0.05]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.01] | 0.03 [0.01, 0.04]* | -0.00 [-0.02, 0.02] | wt_med |
| 7 | 0.05 [0.03, 0.07]* | 0.05 [0.03, 0.06]* | 0.05 [0.03, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.03, 0.07]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.04 [-0.08, -0.01] | 0.02 [0.01, 0.04]* | 0.03 [0.03, 0.05]* | -0.00 [-0.02, 0.01] | 0.03 [0.02, 0.05]* |  | wt_med |
| 10 |  |  |  |  | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.01 [-0.02, 0.00] | 0.02 [0.02, 0.03]* | -0.05 [-0.09, -0.02] | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | -0.01 [-0.02, 0.01] | 0.03 [0.01, 0.04]* |  | wt_med |
| 14 |  |  |  |  | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.03 [-0.07, -0.00] | 0.03 [0.01, 0.04]* | 0.04 [0.03, 0.06]* | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.05]* |  | wt_med |
| 21 |  |  |  |  | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.01 [-0.02, -0.00] | 0.02 [0.02, 0.03]* | -0.03 [-0.07, 0.01] | 0.02 [0.01, 0.03]* | 0.04 [0.02, 0.05]* | 0.00 [-0.01, 0.02] | 0.03 [0.02, 0.05]* |  | wt_med |
| 30 |  |  |  |  | 0.04 [0.02, 0.05]* | 0.03 [0.02, 0.05]* | -0.01 [-0.02, 0.00] | 0.03 [0.02, 0.03]* | -0.05 [-0.09, 0.00] | 0.04 [0.02, 0.05]* | 0.03 [0.01, 0.05]* | -0.02 [-0.04, 0.00] | 0.01 [-0.01, 0.04] |  | wt_med |
| 45 |  |  |  |  | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.00 [-0.02, 0.01] | 0.02 [0.02, 0.03]* | -0.10 [-0.16, -0.06] | 0.03 [0.02, 0.04]* | 0.02 [0.00, 0.05] | -0.02 [-0.04, 0.00] | 0.01 [-0.01, 0.03] |  | wt_med |
| 60 |  |  |  |  | 0.04 [0.02, 0.06]* | 0.04 [0.02, 0.06]* | -0.01 [-0.02, 0.01] | 0.02 [0.02, 0.03]* |  | 0.02 [0.00, 0.03]* |  |  |  |  | wt_med |
| 90 |  |  |  |  | 0.04 [0.02, 0.07]* | 0.04 [0.02, 0.07]* | -0.00 [-0.02, 0.01] | 0.02 [0.01, 0.04]* |  | 0.02 [0.01, 0.04]* |  |  |  |  | wt_med |

## D5 first-hour MAE vs EACH naive candidate

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.679 [-1.098, -0.297]* / [-0.900, -0.455]* | -0.253 [-0.681, 0.231] / [-0.526, -0.010]*  ! | -0.853 [-1.418, -0.395]* / [-1.098, -0.629]* | -0.853 [-1.418, -0.395]* / [-1.098, -0.629]* | -0.780 [-1.413, -0.362]* / [-1.090, -0.498]* | -0.710 [-1.425, -0.258]* / [-1.054, -0.396]* |  |  |
| 1 | -0.879 [-1.325, -0.467]* / [-1.174, -0.557]* | -0.518 [-0.920, -0.160]* / [-0.837, -0.189]* | -1.110 [-1.774, -0.630]* / [-1.419, -0.809]* | -1.110 [-1.774, -0.630]* / [-1.419, -0.809]* | -1.061 [-1.839, -0.532]* / [-1.391, -0.737]* | -0.833 [-1.516, -0.391]* / [-1.160, -0.549]* |  |  |
| 2 | -0.833 [-1.162, -0.533]* / [-1.005, -0.653]* | -0.454 [-0.708, -0.230]* / [-0.643, -0.255]* | -0.946 [-1.385, -0.577]* / [-1.141, -0.765]* | -0.946 [-1.385, -0.577]* / [-1.141, -0.765]* | -0.927 [-1.394, -0.589]* / [-1.161, -0.727]* | -0.942 [-1.367, -0.614]* / [-1.188, -0.719]* |  |  |
| 3 | -0.722 [-0.988, -0.472]* / [-0.964, -0.497]* | -0.382 [-0.606, -0.145]* / [-0.642, -0.123]* | -0.881 [-1.220, -0.590]* / [-1.130, -0.654]* | -0.884 [-1.227, -0.588]* / [-1.132, -0.662]* | -0.790 [-1.227, -0.444]* / [-1.093, -0.545]* | -0.911 [-1.400, -0.521]* / [-1.281, -0.602]* | 1.099 [0.588, 1.714] / [0.777, 1.432] | 1.118 [0.616, 1.674] / [0.791, 1.443] |
| 4 | -0.919 [-1.363, -0.573]* / [-1.240, -0.614]* | -0.640 [-1.088, -0.275]* / [-1.000, -0.294]* | -1.062 [-1.700, -0.620]* / [-1.400, -0.741]* | -1.062 [-1.700, -0.620]* / [-1.400, -0.741]* | -0.790 [-1.621, -0.235]* / [-1.223, -0.418]* | -0.737 [-1.575, -0.172]* / [-1.246, -0.285]* | 0.886 [0.384, 1.465] / [0.467, 1.268] | 0.934 [0.428, 1.458] / [0.507, 1.320] |
| 5 | -0.553 [-0.943, -0.193]* / [-0.847, -0.261]* | -0.228 [-0.598, 0.176] / [-0.550, 0.082] | -0.668 [-1.183, -0.243]* / [-0.961, -0.389]* | -0.674 [-1.197, -0.248]* / [-0.968, -0.393]* | -0.524 [-1.146, -0.042]* / [-0.900, -0.205]* | -0.574 [-1.246, -0.012]* / [-1.046, -0.164]* | 0.854 [0.347, 1.581] / [0.483, 1.230] | 0.904 [0.425, 1.545] / [0.532, 1.278] |
| 6 | -0.494 [-0.754, -0.206]* / [-0.762, -0.258]* | -0.276 [-0.546, 0.038] / [-0.558, -0.014]*  ! | -0.627 [-1.010, -0.291]* / [-0.887, -0.385]* | -0.627 [-1.010, -0.291]* / [-0.887, -0.385]* | -0.435 [-0.780, -0.139]* / [-0.739, -0.144]* | -0.447 [-0.774, -0.139]* / [-0.788, -0.126]* | 1.120 [0.663, 1.703] / [0.828, 1.431] | 1.193 [0.773, 1.731] / [0.911, 1.501] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.679 [0.297, 1.098] / [0.455, 0.900] | 0.397 [0.197, 0.628] / [0.291, 0.504] | -0.177 [-0.393, -0.015]* / [-0.265, -0.099]* | -0.177 [-0.393, -0.015]* / [-0.265, -0.099]* | -0.230 [-0.555, -0.009]* / [-0.366, -0.115]* | -0.195 [-0.512, 0.017] / [-0.354, -0.057]*  ! |  |  |
| 1 | 0.879 [0.467, 1.325] / [0.557, 1.174] | 0.310 [0.073, 0.568] / [0.182, 0.436] | -0.240 [-0.559, -0.019]* / [-0.351, -0.134]* | -0.240 [-0.559, -0.019]* / [-0.351, -0.134]* | -0.362 [-0.826, -0.025]* / [-0.535, -0.205]* | -0.298 [-0.768, 0.010] / [-0.499, -0.121]*  ! |  |  |
| 2 | 0.833 [0.533, 1.162] / [0.653, 1.005] | 0.373 [0.191, 0.610] / [0.282, 0.462] | -0.112 [-0.287, -0.013]* / [-0.184, -0.039]* | -0.113 [-0.289, -0.014]* / [-0.184, -0.041]* | -0.170 [-0.505, -0.012]* / [-0.263, -0.084]* | -0.137 [-0.417, -0.007]* / [-0.247, -0.046]* |  |  |
| 3 | 0.722 [0.472, 0.988] / [0.497, 0.964] | 0.330 [0.156, 0.538] / [0.240, 0.423] | -0.162 [-0.350, -0.026]* / [-0.230, -0.098]* | -0.163 [-0.353, -0.027]* / [-0.231, -0.101]* | -0.168 [-0.428, -0.006]* / [-0.268, -0.073]* | -0.192 [-0.401, -0.025]* / [-0.336, -0.073]* | 1.779 [1.315, 2.332] / [1.551, 2.008] | 1.815 [1.396, 2.319] / [1.593, 2.035] |
| 4 | 0.919 [0.573, 1.363] / [0.614, 1.240] | 0.262 [0.105, 0.466] / [0.156, 0.377] | -0.143 [-0.344, -0.018]* / [-0.227, -0.064]* | -0.143 [-0.344, -0.018]* / [-0.227, -0.064]* | -0.164 [-0.473, 0.003] / [-0.290, -0.045]*  ! | -0.160 [-0.483, 0.004] / [-0.307, -0.018]*  ! | 1.642 [1.158, 2.242] / [1.387, 1.892] | 1.697 [1.222, 2.267] / [1.433, 1.965] |
| 5 | 0.553 [0.193, 0.943] / [0.261, 0.847] | 0.276 [0.107, 0.482] / [0.173, 0.386] | -0.117 [-0.279, -0.010]* / [-0.193, -0.047]* | -0.122 [-0.286, -0.015]* / [-0.196, -0.048]* | -0.115 [-0.349, 0.039] / [-0.220, -0.011]*  ! | -0.097 [-0.320, 0.053] / [-0.229, 0.024] | 1.436 [0.861, 2.093] / [1.196, 1.688] | 1.483 [0.938, 2.091] / [1.227, 1.739] |
| 6 | 0.494 [0.206, 0.754] / [0.258, 0.762] | 0.184 [-0.003, 0.394] / [0.079, 0.288] | -0.157 [-0.352, -0.018]* / [-0.240, -0.088]* | -0.157 [-0.352, -0.018]* / [-0.240, -0.088]* | -0.182 [-0.486, -0.001]* / [-0.307, -0.080]* | -0.159 [-0.432, -0.004]* / [-0.300, -0.045]* | 1.434 [0.989, 1.969] / [1.197, 1.691] | 1.511 [1.103, 2.009] / [1.268, 1.778] |
| 7 |  | 0.244 [-0.006, 0.533] / [0.130, 0.356] | -0.195 [-0.422, -0.027]* / [-0.280, -0.119]* | -0.195 [-0.422, -0.027]* / [-0.280, -0.119]* | -0.242 [-0.572, -0.011]* / [-0.381, -0.128]* | -0.229 [-0.548, -0.003]* / [-0.393, -0.101]* | 1.763 [1.267, 2.394] / [1.489, 2.021] | 1.855 [1.405, 2.427] / [1.594, 2.125] |
| 10 |  | 0.267 [0.088, 0.476] / [0.176, 0.358] | -0.177 [-0.380, -0.030]* / [-0.256, -0.104]* | -0.172 [-0.378, -0.024]* / [-0.254, -0.098]* | -0.165 [-0.440, 0.023] / [-0.287, -0.044]*  ! | -0.231 [-0.488, -0.037]* / [-0.366, -0.118]* | 1.800 [1.333, 2.322] / [1.552, 2.060] | 1.854 [1.413, 2.313] / [1.609, 2.116] |
| 14 |  | 0.170 [-0.078, 0.431] / [0.055, 0.291] | -0.208 [-0.442, -0.033]* / [-0.295, -0.131]* | -0.206 [-0.440, -0.033]* / [-0.294, -0.130]* | -0.266 [-0.603, -0.028]* / [-0.413, -0.150]* | -0.242 [-0.575, -0.021]* / [-0.421, -0.108]* | 1.616 [1.041, 2.364] / [1.308, 1.930] | 1.723 [1.194, 2.419] / [1.413, 2.045] |
| 21 |  | 0.165 [-0.095, 0.454] / [0.048, 0.287] | -0.186 [-0.387, -0.033]* / [-0.262, -0.117]* | -0.183 [-0.383, -0.032]* / [-0.261, -0.114]* | -0.233 [-0.521, -0.027]* / [-0.366, -0.132]* | -0.207 [-0.483, -0.016]* / [-0.355, -0.090]* | 1.699 [1.226, 2.314] / [1.383, 2.021] | 1.831 [1.365, 2.425] / [1.484, 2.177] |
| 30 |  | 0.333 [0.147, 0.580] / [0.238, 0.429] | -0.167 [-0.403, -0.031]* / [-0.245, -0.100]* | -0.167 [-0.403, -0.030]* / [-0.245, -0.099]* | -0.172 [-0.551, 0.018] / [-0.318, -0.065]*  ! | -0.175 [-0.516, -0.013]* / [-0.320, -0.067]* | 3.084 [2.390, 3.880] / [2.727, 3.483] | 3.227 [2.502, 3.963] / [2.865, 3.632] |
| 45 |  | 0.299 [0.075, 0.572] / [0.182, 0.414] | -0.200 [-0.456, -0.044]* / [-0.304, -0.118]* | -0.202 [-0.455, -0.048]* / [-0.310, -0.116]* | -0.252 [-0.643, -0.009]* / [-0.417, -0.111]* | -0.142 [-0.446, -0.010] / [-0.291, -0.020]*  ! | 1.861 [1.251, 2.515] / [1.515, 2.240] | 1.916 [1.348, 2.549] / [1.569, 2.282] |
| 60 |  | 0.076 [-0.209, 0.416] / [-0.046, 0.198] | -0.059 [-0.188, 0.024] / [-0.104, -0.021]*  ! | -0.059 [-0.187, 0.024] / [-0.104, -0.019]*  ! | 0.000 [0.000, 0.000] / [0.000, 0.000] |  | 0.050 [0.000, 1.281] / [0.000, 0.199] | 0.050 [0.000, 1.281] / [0.000, 0.199] |
| 90 |  | 0.066 [-0.295, 0.525] / [-0.094, 0.231] | -0.077 [-0.192, -0.001]* / [-0.127, -0.037]* | -0.069 [-0.173, -0.000]* / [-0.124, -0.024]* |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.253 [-0.231, 0.681] / [0.010, 0.526] | -0.397 [-0.628, -0.197]* / [-0.504, -0.291]* | -0.580 [-0.893, -0.348]* / [-0.702, -0.462]* | -0.580 [-0.893, -0.348]* / [-0.702, -0.462]* | -0.531 [-0.941, -0.241]* / [-0.701, -0.366]* | -0.577 [-0.994, -0.268]* / [-0.777, -0.397]* |  |  |
| 1 | 0.518 [0.160, 0.920] / [0.189, 0.837] | -0.310 [-0.568, -0.073]* / [-0.436, -0.182]* | -0.551 [-0.844, -0.321]* / [-0.681, -0.423]* | -0.551 [-0.844, -0.321]* / [-0.681, -0.423]* | -0.508 [-0.911, -0.219]* / [-0.675, -0.353]* | -0.548 [-0.959, -0.247]* / [-0.739, -0.371]* |  |  |
| 2 | 0.454 [0.230, 0.708] / [0.255, 0.643] | -0.373 [-0.610, -0.191]* / [-0.462, -0.282]* | -0.500 [-0.869, -0.248]* / [-0.608, -0.396]* | -0.500 [-0.869, -0.248]* / [-0.608, -0.397]* | -0.537 [-0.976, -0.230]* / [-0.676, -0.401]* | -0.550 [-1.031, -0.193]* / [-0.721, -0.396]* |  |  |
| 3 | 0.382 [0.145, 0.606] / [0.123, 0.642] | -0.330 [-0.538, -0.156]* / [-0.423, -0.240]* | -0.492 [-0.865, -0.223]* / [-0.605, -0.382]* | -0.494 [-0.865, -0.228]* / [-0.605, -0.385]* | -0.550 [-1.050, -0.221]* / [-0.709, -0.405]* | -0.579 [-1.000, -0.266]* / [-0.775, -0.406]* | 1.370 [0.935, 1.873] / [1.138, 1.605] | 1.406 [1.000, 1.862] / [1.177, 1.637] |
| 4 | 0.640 [0.275, 1.088] / [0.294, 1.000] | -0.262 [-0.466, -0.105]* / [-0.377, -0.156]* | -0.392 [-0.641, -0.192]* / [-0.518, -0.273]* | -0.392 [-0.641, -0.192]* / [-0.518, -0.273]* | -0.341 [-0.658, -0.102]* / [-0.501, -0.180]* | -0.395 [-0.760, -0.110]* / [-0.581, -0.210]* | 1.461 [0.977, 2.044] / [1.207, 1.734] | 1.517 [1.046, 2.085] / [1.253, 1.804] |
| 5 | 0.228 [-0.176, 0.598] / [-0.082, 0.550] | -0.276 [-0.482, -0.107]* / [-0.386, -0.173]* | -0.397 [-0.675, -0.188]* / [-0.524, -0.267]* | -0.402 [-0.681, -0.189]* / [-0.531, -0.274]* | -0.378 [-0.736, -0.109]* / [-0.553, -0.201]* | -0.432 [-0.879, -0.107]* / [-0.636, -0.233]* | 1.107 [0.663, 1.619] / [0.856, 1.355] | 1.152 [0.737, 1.624] / [0.890, 1.420] |
| 6 | 0.276 [-0.038, 0.546] / [0.014, 0.558] | -0.184 [-0.394, 0.003] / [-0.288, -0.079]*  ! | -0.348 [-0.639, -0.127]* / [-0.477, -0.218]* | -0.348 [-0.639, -0.127]* / [-0.477, -0.218]* | -0.327 [-0.704, -0.041]* / [-0.502, -0.158]* | -0.413 [-0.846, -0.086]* / [-0.637, -0.214]* | 1.227 [0.802, 1.743] / [0.958, 1.504] | 1.303 [0.888, 1.780] / [1.034, 1.583] |
| 7 |  | -0.244 [-0.533, 0.006] / [-0.356, -0.130]*  ! | -0.444 [-0.793, -0.182]* / [-0.572, -0.322]* | -0.444 [-0.793, -0.182]* / [-0.572, -0.322]* | -0.494 [-0.891, -0.192]* / [-0.665, -0.319]* | -0.529 [-1.001, -0.178]* / [-0.738, -0.332]* | 1.454 [0.980, 2.018] / [1.174, 1.721] | 1.544 [1.102, 2.056] / [1.272, 1.828] |
| 10 |  | -0.267 [-0.476, -0.088]* / [-0.358, -0.176]* | -0.448 [-0.804, -0.182]* / [-0.560, -0.335]* | -0.443 [-0.801, -0.175]* / [-0.553, -0.327]* | -0.513 [-0.984, -0.182]* / [-0.678, -0.349]* | -0.592 [-1.037, -0.250]* / [-0.788, -0.427]* | 1.439 [0.896, 1.972] / [1.181, 1.726] | 1.489 [0.946, 1.987] / [1.240, 1.779] |
| 14 |  | -0.170 [-0.431, 0.078] / [-0.291, -0.055]*  ! | -0.372 [-0.712, -0.107]* / [-0.502, -0.241]* | -0.370 [-0.707, -0.106]* / [-0.502, -0.239]* | -0.434 [-0.891, -0.089]* / [-0.621, -0.246]* | -0.429 [-0.919, -0.046]* / [-0.650, -0.215]* | 1.397 [0.805, 2.135] / [1.057, 1.727] | 1.503 [0.929, 2.222] / [1.161, 1.836] |
| 21 |  | -0.165 [-0.454, 0.095] / [-0.287, -0.048]*  ! | -0.341 [-0.701, -0.059]* / [-0.474, -0.203]* | -0.338 [-0.701, -0.057]* / [-0.471, -0.200]* | -0.357 [-0.830, -0.009]* / [-0.537, -0.183]* | -0.351 [-0.814, 0.026] / [-0.554, -0.150]*  ! | 1.551 [0.997, 2.198] / [1.220, 1.920] | 1.682 [1.148, 2.336] / [1.326, 2.070] |
| 30 |  | -0.333 [-0.580, -0.147]* / [-0.429, -0.238]* | -0.485 [-0.834, -0.237]* / [-0.603, -0.370]* | -0.485 [-0.834, -0.236]* / [-0.603, -0.370]* | -0.491 [-0.968, -0.141]* / [-0.674, -0.331]* | -0.533 [-1.024, -0.164]* / [-0.734, -0.355]* | 2.722 [2.031, 3.444] / [2.372, 3.132] | 2.853 [2.164, 3.491] / [2.487, 3.254] |
| 45 |  | -0.299 [-0.572, -0.075]* / [-0.414, -0.182]* | -0.451 [-0.831, -0.170]* / [-0.596, -0.306]* | -0.454 [-0.822, -0.169]* / [-0.602, -0.300]* | -0.692 [-1.302, -0.202]* / [-0.989, -0.432]* | -1.154 [-2.470, -0.328] / [-1.786, -0.621]*  ! | 1.427 [0.706, 2.135] / [1.021, 1.832] | 1.474 [0.788, 2.163] / [1.091, 1.864] |
| 60 |  | -0.076 [-0.416, 0.209] / [-0.198, 0.046] | -0.139 [-0.519, 0.181] / [-0.281, -0.008]*  ! | -0.139 [-0.521, 0.181] / [-0.280, -0.009]*  ! | 0.000 [0.000, 0.000] / [0.000, 0.000] |  | 0.050 [0.000, 1.281] / [0.000, 0.199] | 0.050 [0.000, 1.281] / [0.000, 0.199] |
| 90 |  | -0.066 [-0.525, 0.295] / [-0.231, 0.094] | -0.145 [-0.660, 0.243] / [-0.320, 0.021] | -0.135 [-0.644, 0.244] / [-0.317, 0.032] |  |  |  |  |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.78 [4.54, 5.04] | 4.18 [3.96, 4.43] | 5.44 [5.15, 5.73] | 4.74 [4.51, 5.00] |  |  | 5.14 [4.83, 5.46] | 4.55 [4.27, 4.82] | 4.16 [3.83, 4.52] | 4.55 [4.27, 4.82] | 4.14 [3.84, 4.47] | 4.52 [4.24, 4.78] | 4.52 [4.25, 4.80] |  |  | 5.42 [5.06, 5.77] | 4.71 [4.41, 5.00] |
| 1 | 5.42 [5.10, 5.76] | 4.62 [4.33, 4.95] | 6.14 [5.78, 6.53] | 5.38 [5.08, 5.72] | 4.60 [4.30, 4.95] | 4.60 [4.30, 4.95] | 5.15 [4.81, 5.54] | 4.60 [4.30, 4.95] | 4.04 [3.71, 4.41] | 4.60 [4.30, 4.95] | 4.10 [3.80, 4.42] | 4.60 [4.31, 4.96] | 4.39 [4.13, 4.67] |  |  | 5.72 [5.33, 6.15] | 4.83 [4.50, 5.18] |
| 2 | 5.92 [5.58, 6.25] | 5.10 [4.78, 5.43] | 6.78 [6.39, 7.18] | 5.89 [5.55, 6.22] | 4.35 [4.07, 4.62] | 4.35 [4.07, 4.62] | 4.62 [4.33, 4.91] | 4.35 [4.07, 4.62] | 3.96 [3.66, 4.31] | 4.35 [4.06, 4.62] | 4.00 [3.71, 4.30] | 4.34 [4.06, 4.62] | 3.89 [3.64, 4.17] |  |  | 5.21 [4.87, 5.55] | 4.44 [4.16, 4.72] |
| 3 | 5.89 [5.55, 6.22] | 4.96 [4.68, 5.26] | 6.72 [6.34, 7.11] | 5.86 [5.55, 6.19] | 4.57 [4.28, 4.87] | 4.57 [4.28, 4.87] | 4.93 [4.61, 5.23] | 4.57 [4.28, 4.87] | 4.25 [3.91, 4.65] | 4.57 [4.28, 4.86] | 4.26 [3.94, 4.60] | 4.54 [4.26, 4.83] | 4.05 [3.80, 4.30] | 6.22 [5.83, 6.66] | 6.25 [5.88, 6.69] | 5.47 [5.10, 5.85] | 4.72 [4.42, 5.01] |
| 4 | 6.20 [5.83, 6.60] | 4.90 [4.59, 5.25] | 7.22 [6.77, 7.70] | 6.18 [5.81, 6.58] | 4.76 [4.43, 5.11] | 4.76 [4.43, 5.11] | 5.11 [4.74, 5.47] | 4.76 [4.43, 5.11] | 4.58 [4.16, 5.08] | 4.76 [4.43, 5.11] | 4.50 [4.13, 4.93] | 4.75 [4.42, 5.11] | 4.46 [4.19, 4.79] | 6.32 [5.89, 6.80] | 6.37 [5.95, 6.83] | 5.91 [5.46, 6.42] | 4.89 [4.56, 5.24] |
| 5 | 6.38 [5.97, 6.77] | 4.98 [4.66, 5.30] | 7.37 [6.88, 7.85] | 6.40 [6.00, 6.81] | 4.86 [4.53, 5.18] | 4.86 [4.53, 5.18] | 5.23 [4.89, 5.60] | 4.87 [4.53, 5.19] | 4.82 [4.38, 5.31] | 4.86 [4.54, 5.18] | 4.68 [4.27, 5.11] | 4.85 [4.52, 5.17] | 4.63 [4.32, 4.94] | 6.31 [5.82, 6.80] | 6.36 [5.88, 6.84] | 5.61 [5.22, 6.03] | 4.98 [4.64, 5.30] |
| 6 | 6.04 [5.67, 6.43] | 4.77 [4.48, 5.09] | 7.04 [6.59, 7.49] | 6.06 [5.69, 6.45] | 4.53 [4.26, 4.81] | 4.53 [4.26, 4.81] | 4.88 [4.55, 5.20] | 4.53 [4.26, 4.81] | 4.38 [4.04, 4.76] | 4.53 [4.26, 4.81] | 4.37 [4.05, 4.72] | 4.53 [4.25, 4.81] | 4.34 [4.07, 4.61] | 5.98 [5.58, 6.43] | 6.06 [5.66, 6.51] | 5.20 [4.82, 5.59] | 4.67 [4.37, 4.96] |
| 7 | 6.52 [6.11, 6.92] | 5.13 [4.81, 5.44] | 7.48 [6.99, 7.94] | 6.53 [6.13, 6.94] | 4.84 [4.54, 5.14] | 4.84 [4.54, 5.14] | 5.28 [4.93, 5.62] | 4.84 [4.54, 5.14] | 4.51 [4.16, 4.94] | 4.84 [4.54, 5.14] | 4.36 [4.03, 4.72] | 4.82 [4.50, 5.12] | 4.93 [4.63, 5.25] | 6.38 [5.94, 6.81] | 6.48 [6.04, 6.90] |  | 5.03 [4.71, 5.35] |
| 10 |  |  |  |  | 4.67 [4.38, 4.98] | 4.67 [4.38, 4.97] | 5.01 [4.70, 5.33] | 4.66 [4.36, 4.96] | 4.40 [4.05, 4.83] | 4.67 [4.37, 4.96] | 4.45 [4.11, 4.84] | 4.64 [4.33, 4.92] | 4.17 [3.91, 4.41] | 6.42 [6.00, 6.90] | 6.48 [6.05, 6.96] |  | 4.84 [4.52, 5.15] |
| 14 |  |  |  |  | 4.99 [4.67, 5.29] | 4.99 [4.67, 5.29] | 5.34 [5.00, 5.69] | 4.99 [4.68, 5.29] | 4.65 [4.25, 5.11] | 4.99 [4.68, 5.29] | 4.53 [4.18, 4.93] | 4.95 [4.64, 5.25] | 5.23 [4.89, 5.57] | 6.43 [5.93, 6.91] | 6.53 [6.03, 7.04] |  | 5.19 [4.87, 5.52] |
| 21 |  |  |  |  | 5.06 [4.74, 5.39] | 5.06 [4.74, 5.39] | 5.39 [5.03, 5.76] | 5.06 [4.74, 5.39] | 4.79 [4.36, 5.29] | 5.05 [4.73, 5.39] | 4.72 [4.34, 5.15] | 5.02 [4.71, 5.36] | 5.50 [5.15, 5.85] | 6.70 [6.22, 7.21] | 6.83 [6.34, 7.35] |  | 5.23 [4.91, 5.58] |
| 30 |  |  |  |  | 4.61 [4.36, 4.91] | 4.61 [4.36, 4.91] | 4.95 [4.65, 5.29] | 4.61 [4.36, 4.91] | 4.27 [3.92, 4.68] | 4.61 [4.36, 4.91] | 4.15 [3.81, 4.56] | 4.61 [4.35, 4.91] | 4.59 [4.30, 4.93] | 7.49 [6.98, 8.08] | 7.62 [7.13, 8.21] |  | 4.74 [4.47, 5.08] |
| 45 |  |  |  |  | 4.94 [4.62, 5.27] | 4.94 [4.62, 5.27] | 5.28 [4.90, 5.67] | 4.92 [4.61, 5.26] | 5.30 [4.59, 6.15] | 4.92 [4.60, 5.24] | 4.34 [3.93, 4.82] | 4.88 [4.58, 5.19] | 4.74 [4.45, 5.07] | 6.59 [6.02, 7.25] | 6.65 [6.11, 7.27] |  | 5.13 [4.80, 5.49] |
| 60 |  |  |  |  | 5.16 [4.76, 5.54] | 5.16 [4.77, 5.54] | 5.32 [4.89, 5.77] | 5.17 [4.78, 5.54] |  | 5.17 [4.78, 5.55] | 0.00 [0.00, 0.02] | 5.14 [4.74, 5.52] | 5.33 [4.96, 5.69] | 0.05 [0.00, 0.20] | 0.05 [0.00, 0.20] |  | 5.22 [4.82, 5.61] |
| 90 |  |  |  |  | 5.21 [4.80, 5.63] | 5.22 [4.81, 5.64] | 5.32 [4.87, 5.82] | 5.20 [4.80, 5.63] |  | 5.24 [4.83, 5.68] |  | 5.27 [4.86, 5.70] | 5.35 [4.95, 5.77] |  |  |  | 5.27 [4.85, 5.72] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.05 [-0.31, 0.37] | -0.55 [-0.86, -0.29]* | 0.72 [0.27, 1.16] | 0.01 [-0.35, 0.34] |  |  | 0.40 [0.20, 0.63] | -0.18 [-0.39, -0.01]* | -0.20 [-0.51, 0.02] | -0.18 [-0.39, -0.01]* | -0.23 [-0.56, -0.01]* | -0.18 [-0.39, -0.01]* | -0.20 [-0.76, 0.28] |  |  | 0.68 [0.30, 1.10] | wt_med |
| 1 | 0.54 [0.12, 0.99] | -0.23 [-0.44, -0.03]* | 1.28 [0.54, 2.08] | 0.51 [0.07, 0.95] | -0.24 [-0.56, -0.02]* | -0.24 [-0.56, -0.02]* | 0.31 [0.07, 0.57] | -0.24 [-0.56, -0.02]* | -0.30 [-0.77, 0.01] | -0.24 [-0.56, -0.02]* | -0.36 [-0.83, -0.02]* | -0.24 [-0.56, -0.02]* | -0.45 [-1.07, 0.07] |  |  | 0.88 [0.47, 1.32] | wt_med |
| 2 | 1.40 [0.98, 1.87] | 0.59 [0.35, 0.91] | 2.26 [1.62, 2.98] | 1.36 [0.96, 1.82] | -0.11 [-0.29, -0.01]* | -0.11 [-0.29, -0.01]* | 0.37 [0.19, 0.61] | -0.11 [-0.29, -0.01]* | -0.14 [-0.42, -0.01]* | -0.11 [-0.29, -0.01]* | -0.17 [-0.50, -0.01]* | -0.11 [-0.29, -0.01]* | -0.52 [-1.14, -0.03]* |  |  | 0.83 [0.53, 1.16] | wt_med |
| 3 | 1.11 [0.72, 1.54] | 0.17 [-0.00, 0.35] | 1.94 [1.25, 2.70] | 1.08 [0.69, 1.52] | -0.16 [-0.35, -0.02]* | -0.16 [-0.35, -0.02]* | 0.33 [0.16, 0.54] | -0.16 [-0.35, -0.03]* | -0.19 [-0.40, -0.03]* | -0.16 [-0.35, -0.03]* | -0.17 [-0.43, -0.01]* | -0.19 [-0.37, -0.05]* | -0.65 [-1.32, -0.09]* | 1.78 [1.32, 2.33] | 1.81 [1.40, 2.32] | 0.72 [0.47, 0.99] | wt_med |
| 4 | 1.28 [0.77, 1.81] | -0.02 [-0.23, 0.15] | 2.31 [1.36, 3.32] | 1.26 [0.75, 1.79] | -0.14 [-0.34, -0.02]* | -0.14 [-0.34, -0.02]* | 0.26 [0.11, 0.47] | -0.14 [-0.34, -0.02]* | -0.16 [-0.48, 0.00] | -0.14 [-0.34, -0.02]* | -0.16 [-0.47, 0.00] | -0.14 [-0.34, -0.02]* | -0.45 [-1.12, 0.14] | 1.64 [1.16, 2.24] | 1.70 [1.22, 2.27] | 0.92 [0.57, 1.36] | wt_med |
| 5 | 1.32 [0.69, 1.90] | -0.07 [-0.37, 0.16] | 2.31 [1.35, 3.32] | 1.35 [0.73, 1.94] | -0.12 [-0.29, -0.02]* | -0.12 [-0.29, -0.02]* | 0.28 [0.11, 0.48] | -0.12 [-0.28, -0.01]* | -0.10 [-0.32, 0.05] | -0.12 [-0.29, -0.01]* | -0.12 [-0.35, 0.04] | -0.12 [-0.28, -0.01]* | -0.39 [-0.95, 0.15] | 1.44 [0.86, 2.09] | 1.48 [0.94, 2.09] | 0.55 [0.19, 0.94] | wt_med |
| 6 | 1.32 [0.72, 1.95] | 0.05 [-0.17, 0.23] | 2.33 [1.36, 3.31] | 1.34 [0.76, 1.97] | -0.16 [-0.35, -0.02]* | -0.16 [-0.35, -0.02]* | 0.18 [-0.00, 0.39] | -0.16 [-0.35, -0.02]* | -0.16 [-0.43, -0.00]* | -0.16 [-0.35, -0.02]* | -0.18 [-0.49, -0.00]* | -0.16 [-0.35, -0.02]* | -0.33 [-0.98, 0.23] | 1.43 [0.99, 1.97] | 1.51 [1.10, 2.01] | 0.49 [0.21, 0.75] | wt_med |
| 7 | 1.46 [0.77, 2.16] | 0.07 [-0.18, 0.29] | 2.42 [1.39, 3.52] | 1.48 [0.81, 2.15] | -0.20 [-0.42, -0.03]* | -0.20 [-0.42, -0.03]* | 0.24 [-0.01, 0.53] | -0.20 [-0.42, -0.03]* | -0.23 [-0.55, -0.00]* | -0.20 [-0.42, -0.03]* | -0.24 [-0.57, -0.01]* | -0.20 [-0.42, -0.03]* | -0.09 [-0.74, 0.51] | 1.76 [1.27, 2.39] | 1.86 [1.40, 2.43] |  | wt_med |
| 10 |  |  |  |  | -0.16 [-0.37, -0.02]* | -0.16 [-0.37, -0.02]* | 0.27 [0.09, 0.48] | -0.18 [-0.38, -0.03]* | -0.23 [-0.49, -0.04]* | -0.17 [-0.38, -0.02]* | -0.17 [-0.44, 0.02] | -0.20 [-0.40, -0.05]* | -0.65 [-1.36, -0.07]* | 1.80 [1.33, 2.32] | 1.85 [1.41, 2.31] |  | wt_med |
| 14 |  |  |  |  | -0.21 [-0.44, -0.04]* | -0.21 [-0.44, -0.04]* | 0.17 [-0.08, 0.43] | -0.21 [-0.44, -0.03]* | -0.24 [-0.58, -0.02]* | -0.21 [-0.44, -0.03]* | -0.27 [-0.60, -0.03]* | -0.22 [-0.45, -0.04]* | 0.06 [-0.67, 0.72] | 1.62 [1.04, 2.36] | 1.72 [1.19, 2.42] |  | wt_med |
| 21 |  |  |  |  | -0.19 [-0.39, -0.03]* | -0.19 [-0.39, -0.03]* | 0.17 [-0.09, 0.45] | -0.19 [-0.39, -0.03]* | -0.21 [-0.48, -0.02] | -0.18 [-0.38, -0.03]* | -0.23 [-0.52, -0.03]* | -0.20 [-0.40, -0.04]* | 0.26 [-0.40, 0.89] | 1.70 [1.23, 2.31] | 1.83 [1.37, 2.43] |  | wt_med |
| 30 |  |  |  |  | -0.17 [-0.40, -0.03]* | -0.17 [-0.40, -0.03]* | 0.33 [0.15, 0.58] | -0.17 [-0.40, -0.03]* | -0.17 [-0.52, -0.01]* | -0.17 [-0.40, -0.03]* | -0.17 [-0.55, 0.02] | -0.17 [-0.41, -0.03]* | -0.19 [-0.77, 0.31] | 3.08 [2.39, 3.88] | 3.23 [2.50, 3.96] |  | wt_med |
| 45 |  |  |  |  | -0.18 [-0.44, -0.03]* | -0.18 [-0.44, -0.03]* | 0.30 [0.08, 0.57] | -0.20 [-0.46, -0.04]* | -0.14 [-0.45, -0.01] | -0.20 [-0.45, -0.05]* | -0.25 [-0.64, -0.01]* | -0.24 [-0.48, -0.07]* | -0.38 [-1.07, 0.20] | 1.86 [1.25, 2.52] | 1.92 [1.35, 2.55] |  | wt_med |
| 60 |  |  |  |  | -0.07 [-0.20, 0.02] | -0.07 [-0.19, 0.02] | 0.08 [-0.21, 0.42] | -0.06 [-0.19, 0.02] |  | -0.06 [-0.19, 0.02] | 0.00 [0.00, 0.00] | -0.08 [-0.24, 0.02] | 0.03 [-0.70, 0.65] | 0.05 [0.00, 1.28] | 0.05 [0.00, 1.28] |  | wt_med |
| 90 |  |  |  |  | -0.07 [-0.19, 0.01] | -0.06 [-0.17, 0.00] | 0.07 [-0.29, 0.53] | -0.08 [-0.19, -0.00] |  | -0.07 [-0.17, -0.00] |  | -0.02 [-0.18, 0.13] | 0.07 [-0.71, 0.72] |  |  |  | wt_med |

**first-hour MAE, schedule known at origin**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.79 [4.51, 5.11] | 4.31 [4.02, 4.63] | 5.36 [5.04, 5.74] | 4.75 [4.47, 5.06] |  |  | 5.38 [4.99, 5.80] | 4.72 [4.39, 5.05] | 4.46 [4.07, 4.90] | 4.72 [4.39, 5.05] | 4.37 [4.00, 4.78] | 4.69 [4.35, 5.01] | 4.74 [4.43, 5.06] |  |  | 5.48 [5.06, 5.92] | 4.92 [4.57, 5.28] |
| 1 | 5.47 [5.10, 5.87] | 4.83 [4.48, 5.21] | 6.12 [5.70, 6.56] | 5.42 [5.06, 5.81] | 4.75 [4.41, 5.17] | 4.75 [4.41, 5.17] | 5.38 [4.96, 5.84] | 4.75 [4.41, 5.17] | 4.26 [3.85, 4.71] | 4.75 [4.41, 5.17] | 4.25 [3.91, 4.66] | 4.76 [4.41, 5.17] | 4.67 [4.36, 5.01] |  |  | 5.95 [5.46, 6.47] | 5.04 [4.64, 5.46] |
| 2 | 6.02 [5.61, 6.44] | 5.39 [4.98, 5.79] | 6.81 [6.33, 7.30] | 5.99 [5.60, 6.41] | 4.53 [4.19, 4.87] | 4.53 [4.19, 4.87] | 4.78 [4.44, 5.14] | 4.53 [4.19, 4.87] | 4.13 [3.76, 4.56] | 4.53 [4.19, 4.87] | 4.14 [3.81, 4.52] | 4.53 [4.19, 4.87] | 4.10 [3.81, 4.44] |  |  | 5.41 [5.01, 5.84] | 4.63 [4.29, 4.99] |
| 3 | 5.88 [5.48, 6.26] | 5.13 [4.79, 5.47] | 6.63 [6.17, 7.09] | 5.84 [5.43, 6.22] | 4.74 [4.38, 5.09] | 4.74 [4.38, 5.09] | 5.17 [4.78, 5.53] | 4.74 [4.38, 5.09] | 4.46 [4.06, 4.91] | 4.74 [4.38, 5.09] | 4.40 [4.00, 4.80] | 4.75 [4.39, 5.09] | 4.33 [4.04, 4.62] | 6.53 [6.07, 7.05] | 6.51 [6.05, 7.01] | 5.62 [5.18, 6.09] | 4.93 [4.56, 5.29] |
| 4 | 6.05 [5.59, 6.54] | 4.98 [4.60, 5.38] | 6.99 [6.42, 7.57] | 6.03 [5.56, 6.51] | 4.88 [4.48, 5.28] | 4.88 [4.48, 5.28] | 5.35 [4.90, 5.80] | 4.88 [4.48, 5.28] | 4.90 [4.36, 5.52] | 4.88 [4.48, 5.28] | 4.76 [4.26, 5.30] | 4.87 [4.47, 5.28] | 4.62 [4.28, 4.96] | 6.58 [6.01, 7.16] | 6.59 [6.03, 7.15] | 6.05 [5.49, 6.65] | 5.05 [4.66, 5.46] |
| 5 | 6.31 [5.80, 6.82] | 5.11 [4.71, 5.52] | 7.22 [6.64, 7.80] | 6.33 [5.83, 6.86] | 5.05 [4.63, 5.44] | 5.05 [4.63, 5.44] | 5.44 [5.00, 5.89] | 5.05 [4.63, 5.44] | 5.22 [4.65, 5.83] | 5.05 [4.63, 5.44] | 4.94 [4.44, 5.49] | 5.04 [4.63, 5.43] | 4.91 [4.54, 5.30] | 6.66 [6.05, 7.26] | 6.67 [6.08, 7.26] | 5.72 [5.23, 6.26] | 5.19 [4.78, 5.60] |
| 6 | 5.80 [5.37, 6.26] | 4.82 [4.47, 5.19] | 6.75 [6.22, 7.29] | 5.84 [5.40, 6.31] | 4.62 [4.29, 4.94] | 4.62 [4.29, 4.94] | 5.02 [4.65, 5.40] | 4.62 [4.29, 4.94] | 4.66 [4.24, 5.13] | 4.62 [4.29, 4.94] | 4.57 [4.19, 5.02] | 4.62 [4.30, 4.94] | 4.53 [4.23, 4.87] | 6.28 [5.78, 6.83] | 6.32 [5.83, 6.87] | 5.17 [4.74, 5.62] | 4.79 [4.43, 5.12] |
| 7 | 6.35 [5.89, 6.82] | 5.29 [4.91, 5.67] | 7.25 [6.70, 7.82] | 6.37 [5.92, 6.87] | 5.05 [4.68, 5.44] | 5.05 [4.68, 5.44] | 5.52 [5.12, 5.95] | 5.05 [4.68, 5.44] | 4.85 [4.41, 5.36] | 5.05 [4.68, 5.44] | 4.62 [4.22, 5.05] | 5.02 [4.64, 5.40] | 5.24 [4.86, 5.63] | 6.82 [6.28, 7.36] | 6.87 [6.34, 7.39] |  | 5.28 [4.89, 5.68] |
| 10 |  |  |  |  | 4.75 [4.39, 5.12] | 4.75 [4.39, 5.12] | 5.16 [4.79, 5.56] | 4.75 [4.39, 5.12] | 4.48 [4.04, 4.98] | 4.75 [4.39, 5.12] | 4.46 [4.04, 4.91] | 4.76 [4.39, 5.12] | 4.36 [4.07, 4.64] | 6.59 [6.06, 7.13] | 6.56 [6.05, 7.10] |  | 4.99 [4.61, 5.39] |
| 14 |  |  |  |  | 5.01 [4.62, 5.41] | 5.01 [4.62, 5.41] | 5.40 [4.99, 5.86] | 5.01 [4.62, 5.41] | 4.76 [4.28, 5.36] | 5.01 [4.62, 5.41] | 4.53 [4.10, 5.02] | 4.97 [4.58, 5.37] | 5.37 [4.96, 5.78] | 6.67 [6.06, 7.33] | 6.73 [6.12, 7.39] |  | 5.27 [4.88, 5.70] |
| 21 |  |  |  |  | 4.97 [4.59, 5.40] | 4.97 [4.59, 5.40] | 5.43 [4.98, 5.95] | 4.97 [4.59, 5.40] | 4.77 [4.27, 5.37] | 4.97 [4.59, 5.40] | 4.59 [4.14, 5.10] | 4.93 [4.54, 5.34] | 5.54 [5.15, 5.96] | 6.76 [6.19, 7.44] | 6.84 [6.26, 7.50] |  | 5.22 [4.82, 5.67] |
| 30 |  |  |  |  | 4.43 [4.08, 4.83] | 4.43 [4.08, 4.83] | 4.82 [4.43, 5.28] | 4.43 [4.08, 4.83] | 4.17 [3.74, 4.68] | 4.43 [4.08, 4.83] | 3.98 [3.56, 4.48] | 4.43 [4.08, 4.84] | 4.26 [3.89, 4.71] | 7.34 [6.66, 8.16] | 7.36 [6.68, 8.16] |  | 4.61 [4.23, 5.03] |
| 45 |  |  |  |  | 4.69 [4.28, 5.15] | 4.69 [4.28, 5.15] | 5.21 [4.67, 5.75] | 4.69 [4.28, 5.15] | 9.81 [8.59, 11.18] | 4.69 [4.28, 5.15] | 4.07 [3.52, 4.65] | 4.70 [4.28, 5.16] | 4.41 [4.03, 4.83] | 6.81 [6.01, 7.72] | 6.79 [5.98, 7.66] |  | 4.99 [4.52, 5.49] |
| 60 |  |  |  |  | 4.59 [4.11, 5.08] | 4.59 [4.11, 5.08] | 4.73 [4.25, 5.26] | 4.59 [4.11, 5.08] |  | 4.59 [4.11, 5.08] | 0.00 [0.00, 0.00] | 4.60 [4.12, 5.08] | 5.16 [4.66, 5.66] | 0.05 [0.00, 0.23] | 0.05 [0.00, 0.23] |  | 4.69 [4.17, 5.19] |
| 90 |  |  |  |  | 4.33 [3.69, 4.96] | 4.33 [3.69, 4.96] | 4.44 [3.81, 5.09] | 4.33 [3.69, 4.96] |  | 4.33 [3.69, 4.96] |  | 4.36 [3.72, 4.99] | 4.61 [3.98, 5.30] |  |  |  | 4.42 [3.75, 5.09] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.15 [-0.46, 0.15] | -0.64 [-0.98, -0.32]* | 0.42 [0.08, 0.76] | -0.20 [-0.51, 0.13] |  |  | 0.42 [0.16, 0.69] | -0.22 [-0.52, -0.02]* | -0.24 [-0.66, 0.03] | -0.22 [-0.52, -0.02]* | -0.28 [-0.69, -0.01]* | -0.22 [-0.52, -0.02]* | -0.21 [-0.93, 0.42] |  |  | 0.54 [0.13, 1.00] | wt_med |
| 1 | 0.38 [-0.01, 0.77] | -0.23 [-0.46, 0.01] | 1.04 [0.34, 1.96] | 0.32 [-0.07, 0.71] | -0.30 [-0.71, -0.02]* | -0.30 [-0.71, -0.02]* | 0.32 [0.02, 0.61] | -0.30 [-0.71, -0.02]* | -0.38 [-0.98, 0.02] | -0.30 [-0.71, -0.02]* | -0.45 [-1.02, -0.03]* | -0.30 [-0.71, -0.02]* | -0.39 [-1.14, 0.23] |  |  | 0.88 [0.44, 1.38] | wt_med |
| 2 | 1.28 [0.83, 1.82] | 0.67 [0.39, 1.04] | 2.05 [1.37, 2.83] | 1.24 [0.82, 1.77] | -0.14 [-0.35, -0.02]* | -0.14 [-0.35, -0.02]* | 0.39 [0.16, 0.68] | -0.14 [-0.35, -0.02]* | -0.17 [-0.52, -0.00]* | -0.14 [-0.35, -0.02]* | -0.21 [-0.64, -0.01]* | -0.14 [-0.35, -0.02]* | -0.50 [-1.29, 0.09] |  |  | 0.87 [0.53, 1.28] | wt_med |
| 3 | 0.87 [0.53, 1.21] | 0.12 [-0.11, 0.37] | 1.62 [1.03, 2.38] | 0.83 [0.49, 1.17] | -0.20 [-0.43, -0.03]* | -0.20 [-0.43, -0.03]* | 0.40 [0.18, 0.66] | -0.20 [-0.43, -0.03]* | -0.19 [-0.47, -0.01]* | -0.20 [-0.43, -0.03]* | -0.22 [-0.55, -0.01]* | -0.20 [-0.44, -0.03]* | -0.57 [-1.35, 0.07] | 1.92 [1.34, 2.61] | 1.89 [1.35, 2.48] | 0.68 [0.38, 1.01] | wt_med |
| 4 | 0.95 [0.54, 1.42] | -0.11 [-0.35, 0.10] | 1.89 [1.00, 2.98] | 0.92 [0.52, 1.37] | -0.19 [-0.47, -0.02]* | -0.19 [-0.47, -0.02]* | 0.28 [0.06, 0.55] | -0.19 [-0.47, -0.02]* | -0.21 [-0.66, 0.01] | -0.19 [-0.47, -0.02]* | -0.22 [-0.61, 0.00] | -0.19 [-0.47, -0.02]* | -0.47 [-1.30, 0.24] | 1.59 [0.96, 2.33] | 1.60 [1.04, 2.30] | 0.92 [0.48, 1.53] | wt_med |
| 5 | 1.01 [0.45, 1.65] | -0.17 [-0.52, 0.10] | 1.91 [0.98, 3.05] | 1.03 [0.43, 1.68] | -0.15 [-0.37, -0.01]* | -0.15 [-0.37, -0.01]* | 0.28 [0.07, 0.52] | -0.15 [-0.37, -0.01]* | -0.14 [-0.45, 0.05] | -0.15 [-0.37, -0.01]* | -0.15 [-0.46, 0.04] | -0.15 [-0.37, -0.01]* | -0.35 [-1.07, 0.33] | 1.47 [0.77, 2.29] | 1.48 [0.81, 2.27] | 0.45 [0.02, 0.95] | wt_med |
| 6 | 0.94 [0.50, 1.43] | -0.04 [-0.30, 0.17] | 1.89 [1.08, 2.90] | 0.98 [0.52, 1.51] | -0.19 [-0.47, -0.02]* | -0.19 [-0.47, -0.02]* | 0.18 [-0.04, 0.43] | -0.19 [-0.47, -0.02]* | -0.21 [-0.58, -0.00]* | -0.19 [-0.47, -0.02]* | -0.23 [-0.62, 0.00] | -0.19 [-0.47, -0.02]* | -0.27 [-1.08, 0.46] | 1.48 [0.92, 2.15] | 1.53 [1.01, 2.14] | 0.36 [0.04, 0.67] | wt_med |
| 7 | 1.02 [0.48, 1.65] | -0.02 [-0.30, 0.23] | 1.92 [1.01, 3.11] | 1.04 [0.49, 1.69] | -0.24 [-0.54, -0.02]* | -0.24 [-0.54, -0.02]* | 0.22 [-0.10, 0.57] | -0.24 [-0.54, -0.02]* | -0.29 [-0.74, 0.00] | -0.24 [-0.54, -0.02]* | -0.31 [-0.75, -0.01]* | -0.24 [-0.55, -0.02]* | -0.04 [-0.89, 0.69] | 1.87 [1.23, 2.65] | 1.92 [1.35, 2.61] |  | wt_med |
| 10 |  |  |  |  | -0.24 [-0.51, -0.04]* | -0.24 [-0.51, -0.04]* | 0.31 [0.08, 0.58] | -0.24 [-0.51, -0.04]* | -0.30 [-0.70, -0.02]* | -0.24 [-0.51, -0.04]* | -0.28 [-0.68, -0.02]* | -0.24 [-0.51, -0.04]* | -0.61 [-1.52, 0.10] | 1.84 [1.23, 2.49] | 1.82 [1.24, 2.43] |  | wt_med |
| 14 |  |  |  |  | -0.27 [-0.58, -0.04]* | -0.27 [-0.58, -0.04]* | 0.16 [-0.16, 0.47] | -0.27 [-0.58, -0.04]* | -0.33 [-0.80, -0.02]* | -0.27 [-0.58, -0.04]* | -0.36 [-0.82, -0.03]* | -0.27 [-0.58, -0.04]* | 0.12 [-0.78, 0.92] | 1.80 [1.14, 2.57] | 1.86 [1.25, 2.60] |  | wt_med |
| 21 |  |  |  |  | -0.26 [-0.53, -0.05]* | -0.26 [-0.53, -0.05]* | 0.23 [-0.09, 0.55] | -0.26 [-0.53, -0.05]* | -0.29 [-0.70, -0.02] | -0.26 [-0.53, -0.05]* | -0.33 [-0.73, -0.03]* | -0.26 [-0.54, -0.05]* | 0.31 [-0.47, 1.04] | 1.80 [1.22, 2.49] | 1.88 [1.34, 2.54] |  | wt_med |
| 30 |  |  |  |  | -0.20 [-0.53, -0.03]* | -0.20 [-0.53, -0.03]* | 0.35 [0.10, 0.65] | -0.20 [-0.53, -0.03]* | -0.21 [-0.67, -0.00]* | -0.20 [-0.53, -0.03]* | -0.23 [-0.75, 0.02] | -0.20 [-0.53, -0.03]* | -0.38 [-1.22, 0.24] | 3.07 [2.13, 4.14] | 3.09 [2.22, 4.00] |  | wt_med |
| 45 |  |  |  |  | -0.25 [-0.63, -0.04]* | -0.25 [-0.63, -0.04]* | 0.36 [0.07, 0.71] | -0.25 [-0.63, -0.04]* | -0.48 [-1.45, -0.05] | -0.25 [-0.63, -0.04]* | -0.31 [-0.81, -0.02]* | -0.25 [-0.64, -0.04]* | -0.54 [-1.45, 0.15] | 2.11 [1.19, 3.10] | 2.09 [1.25, 3.05] |  | wt_med |
| 60 |  |  |  |  | -0.08 [-0.29, 0.04] | -0.08 [-0.29, 0.04] | 0.03 [-0.36, 0.49] | -0.08 [-0.29, 0.04] |  | -0.08 [-0.29, 0.04] | 0.00 [0.00, 0.00] | -0.08 [-0.29, 0.04] | 0.42 [-0.50, 1.15] | 0.05 [0.00, 1.33] | 0.05 [0.00, 1.33] |  | wt_med |
| 90 |  |  |  |  | -0.07 [-0.29, 0.01] | -0.07 [-0.29, 0.01] | -0.09 [-0.57, 0.21] | -0.07 [-0.29, 0.01] |  | -0.07 [-0.29, 0.01] |  | -0.07 [-0.29, 0.01] | 0.20 [-0.83, 0.90] |  |  |  | wt_med |

**first-hour MAE, schedule projected at origin**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.73 [4.40, 5.06] | 3.67 [3.39, 3.96] | 5.75 [5.30, 6.17] | 4.73 [4.40, 5.06] |  |  | 4.19 [3.83, 4.59] | 3.86 [3.50, 4.26] | 2.96 [2.67, 3.28] | 3.86 [3.50, 4.26] | 3.15 [2.83, 3.49] | 3.86 [3.50, 4.25] | 3.66 [3.29, 4.05] |  |  | 5.18 [4.64, 5.72] | 3.88 [3.51, 4.26] |
| 1 | 5.22 [4.80, 5.66] | 3.81 [3.44, 4.24] | 6.24 [5.72, 6.79] | 5.26 [4.85, 5.70] | 4.02 [3.55, 4.57] | 4.02 [3.55, 4.57] | 4.31 [3.83, 4.84] | 4.02 [3.55, 4.57] | 3.22 [2.73, 3.87] | 4.02 [3.55, 4.57] | 3.45 [3.01, 4.01] | 4.01 [3.54, 4.57] | 3.33 [2.98, 3.77] |  |  | 4.90 [4.29, 5.52] | 4.04 [3.57, 4.59] |
| 2 | 5.55 [5.15, 5.94] | 4.06 [3.69, 4.48] | 6.70 [6.19, 7.25] | 5.52 [5.14, 5.91] | 3.69 [3.37, 4.01] | 3.69 [3.37, 4.01] | 4.03 [3.65, 4.44] | 3.69 [3.37, 4.01] | 3.35 [2.90, 3.88] | 3.69 [3.37, 4.01] | 3.47 [3.08, 3.92] | 3.67 [3.35, 3.99] | 3.11 [2.75, 3.56] |  |  | 4.45 [4.09, 4.88] | 3.72 [3.39, 4.06] |
| 3 | 5.93 [5.40, 6.51] | 4.33 [3.91, 4.86] | 7.07 [6.48, 7.75] | 5.96 [5.42, 6.57] | 3.94 [3.50, 4.50] | 3.94 [3.50, 4.50] | 4.08 [3.67, 4.59] | 3.92 [3.51, 4.44] | 3.49 [3.00, 4.20] | 3.91 [3.51, 4.40] | 3.72 [3.19, 4.45] | 3.78 [3.42, 4.21] | 3.02 [2.74, 3.36] | 5.00 [4.44, 5.72] | 5.28 [4.70, 6.00] | 4.89 [4.37, 5.47] | 3.96 [3.56, 4.44] |
| 4 | 6.67 [6.10, 7.32] | 4.66 [4.17, 5.31] | 7.94 [7.22, 8.76] | 6.67 [6.11, 7.32] | 4.36 [3.79, 5.13] | 4.36 [3.79, 5.13] | 4.37 [3.95, 4.88] | 4.36 [3.79, 5.13] | 3.56 [3.14, 4.16] | 4.36 [3.79, 5.13] | 3.66 [3.27, 4.20] | 4.37 [3.79, 5.14] | 3.98 [3.52, 4.59] | 5.46 [5.00, 6.05] | 5.66 [5.19, 6.21] | 5.44 [4.76, 6.36] | 4.38 [3.81, 5.12] |
| 5 | 6.61 [6.13, 7.11] | 4.55 [4.17, 4.97] | 7.86 [7.21, 8.53] | 6.61 [6.12, 7.11] | 4.23 [3.89, 4.64] | 4.23 [3.89, 4.64] | 4.58 [4.16, 5.06] | 4.27 [3.90, 4.68] | 3.58 [3.16, 4.03] | 4.24 [3.89, 4.64] | 3.79 [3.37, 4.24] | 4.23 [3.86, 4.64] | 3.72 [3.35, 4.14] | 5.16 [4.67, 5.68] | 5.31 [4.87, 5.78] | 5.24 [4.72, 5.82] | 4.28 [3.92, 4.69] |
| 6 | 6.91 [6.28, 7.71] | 4.60 [4.17, 5.12] | 8.09 [7.33, 8.93] | 6.87 [6.26, 7.63] | 4.22 [3.80, 4.73] | 4.22 [3.80, 4.73] | 4.38 [3.94, 4.89] | 4.22 [3.80, 4.73] | 3.38 [2.96, 3.96] | 4.22 [3.80, 4.73] | 3.57 [3.14, 4.12] | 4.21 [3.78, 4.73] | 3.64 [3.27, 4.06] | 4.85 [4.39, 5.42] | 5.03 [4.57, 5.60] | 5.31 [4.72, 6.03] | 4.23 [3.81, 4.74] |
| 7 | 7.14 [6.54, 7.69] | 4.53 [4.14, 4.94] | 8.30 [7.51, 9.04] | 7.12 [6.54, 7.66] | 4.09 [3.72, 4.49] | 4.09 [3.72, 4.49] | 4.42 [4.02, 4.84] | 4.09 [3.72, 4.49] | 3.33 [2.93, 3.87] | 4.09 [3.72, 4.49] | 3.38 [2.98, 3.87] | 4.09 [3.72, 4.49] | 3.82 [3.46, 4.21] | 4.79 [4.28, 5.34] | 5.03 [4.56, 5.58] |  | 4.12 [3.75, 4.53] |
| 10 |  |  |  |  | 4.42 [3.94, 4.99] | 4.42 [3.94, 4.99] | 4.54 [4.10, 5.06] | 4.37 [3.93, 4.88] | 4.18 [3.62, 4.91] | 4.39 [3.94, 4.92] | 4.41 [3.78, 5.26] | 4.24 [3.85, 4.72] | 3.54 [3.18, 3.98] | 5.93 [5.22, 6.81] | 6.22 [5.50, 7.14] |  | 4.35 [3.94, 4.82] |
| 14 |  |  |  |  | 4.93 [4.43, 5.46] | 4.93 [4.43, 5.46] | 5.19 [4.71, 5.76] | 4.94 [4.44, 5.46] | 4.36 [3.73, 5.18] | 4.93 [4.44, 5.45] | 4.56 [3.89, 5.31] | 4.91 [4.41, 5.44] | 4.82 [4.26, 5.46] | 5.75 [5.10, 6.54] | 5.98 [5.35, 6.76] |  | 4.97 [4.48, 5.51] |
| 21 |  |  |  |  | 5.28 [4.75, 5.89] | 5.28 [4.75, 5.89] | 5.29 [4.76, 5.88] | 5.29 [4.76, 5.89] | 4.84 [4.11, 5.80] | 5.27 [4.74, 5.89] | 5.04 [4.34, 5.90] | 5.25 [4.72, 5.87] | 5.38 [4.73, 6.10] | 6.55 [5.81, 7.53] | 6.82 [6.05, 7.81] |  | 5.28 [4.76, 5.86] |
| 30 |  |  |  |  | 4.95 [4.55, 5.34] | 4.95 [4.55, 5.34] | 5.18 [4.76, 5.61] | 4.95 [4.55, 5.34] | 4.45 [3.84, 5.14] | 4.95 [4.55, 5.34] | 4.49 [3.88, 5.13] | 4.93 [4.54, 5.32] | 5.19 [4.69, 5.72] | 7.77 [7.08, 8.49] | 8.13 [7.43, 8.88] |  | 4.98 [4.60, 5.38] |
| 45 |  |  |  |  | 5.33 [4.84, 5.85] | 5.33 [4.84, 5.84] | 5.38 [4.91, 5.89] | 5.29 [4.83, 5.77] | 3.59 [3.09, 4.24] | 5.28 [4.82, 5.76] | 4.74 [4.04, 5.56] | 5.19 [4.74, 5.66] | 5.27 [4.83, 5.76] | 6.28 [5.53, 7.12] | 6.44 [5.72, 7.27] |  | 5.36 [4.88, 5.88] |
| 60 |  |  |  |  | 5.78 [5.18, 6.43] | 5.78 [5.18, 6.44] | 5.90 [5.26, 6.64] | 5.80 [5.19, 6.45] |  | 5.80 [5.19, 6.45] | 0.06 [0.00, 0.22] | 5.73 [5.13, 6.37] | 5.50 [4.96, 6.13] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 5.79 [5.19, 6.43] |
| 90 |  |  |  |  | 6.02 [5.49, 6.58] | 6.03 [5.50, 6.60] | 6.07 [5.50, 6.71] | 6.01 [5.48, 6.57] |  | 6.10 [5.59, 6.72] |  | 6.10 [5.57, 6.73] | 6.03 [5.53, 6.55] |  |  |  | 6.04 [5.52, 6.63] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.86 [0.15, 1.37] | -0.21 [-0.58, 0.12] | 1.89 [0.55, 3.11] | 0.86 [0.13, 1.38] |  |  | 0.32 [0.18, 0.52] | -0.01 [-0.03, 0.01] | -0.02 [-0.06, 0.01] | -0.01 [-0.03, 0.01] | -0.01 [-0.04, 0.03] | -0.01 [-0.03, 0.01] | -0.19 [-0.57, 0.24] |  |  | 1.26 [0.98, 1.79] | wt_med |
| 1 | 1.18 [-0.19, 2.03] | -0.25 [-0.75, 0.07] | 2.20 [0.15, 3.61] | 1.21 [-0.14, 2.07] | -0.02 [-0.08, 0.02] | -0.02 [-0.08, 0.02] | 0.28 [0.11, 0.62] | -0.02 [-0.08, 0.02] | -0.01 [-0.04, 0.02] | -0.02 [-0.08, 0.02] | -0.00 [-0.02, 0.02] | -0.02 [-0.08, 0.02] | -0.70 [-1.22, -0.35]* |  |  | 0.87 [0.42, 1.71] | wt_med |
| 2 | 1.84 [0.82, 2.68] | 0.32 [0.09, 0.63] | 3.01 [1.53, 4.31] | 1.81 [0.77, 2.68] | -0.03 [-0.15, 0.01] | -0.03 [-0.16, 0.01] | 0.33 [0.19, 0.64] | -0.03 [-0.15, 0.01] | -0.02 [-0.16, 0.02] | -0.03 [-0.16, 0.01] | -0.03 [-0.13, 0.01] | -0.03 [-0.17, 0.01] | -0.59 [-0.95, -0.08]* |  |  | 0.69 [0.36, 1.11] | wt_med |
| 3 | 1.97 [0.69, 3.31] | 0.35 [0.16, 0.62] | 3.13 [1.22, 4.83] | 2.01 [0.73, 3.32] | -0.01 [-0.19, 0.11] | -0.01 [-0.19, 0.11] | 0.09 [-0.05, 0.25] | -0.03 [-0.21, 0.04] | -0.20 [-1.23, 0.06] | -0.04 [-0.25, 0.04] | 0.02 [-0.05, 0.15] | -0.15 [-0.70, 0.02] | -0.92 [-1.78, -0.37]* | 1.24 [0.83, 1.70] | 1.52 [1.03, 2.05] | 0.87 [0.53, 1.26] | wt_med |
| 4 | 2.33 [0.65, 3.86] | 0.28 [0.01, 0.66] | 3.62 [1.52, 5.64] | 2.32 [0.61, 3.86] | -0.00 [-0.07, 0.10] | -0.00 [-0.07, 0.10] | 0.21 [0.04, 0.37] | -0.00 [-0.07, 0.10] | 0.01 [-0.01, 0.06] | -0.00 [-0.07, 0.10] | 0.01 [-0.02, 0.07] | -0.00 [-0.07, 0.10] | -0.42 [-1.25, 0.23] | 1.83 [1.10, 2.69] | 2.03 [1.19, 3.01] | 0.91 [0.65, 1.76] | wt_med |
| 5 | 2.35 [0.79, 3.34] | 0.25 [-0.05, 0.69] | 3.62 [1.83, 5.03] | 2.36 [0.77, 3.38] | -0.04 [-0.13, 0.01] | -0.04 [-0.13, 0.01] | 0.26 [0.06, 0.53] | -0.01 [-0.07, 0.08] | 0.03 [-0.05, 0.25] | -0.03 [-0.09, 0.01] | 0.01 [-0.04, 0.13] | -0.01 [-0.08, 0.08] | -0.51 [-1.07, -0.03]* | 1.34 [1.00, 1.89] | 1.49 [1.04, 1.98] | 0.90 [0.58, 1.61] | wt_med |
| 6 | 2.70 [0.65, 3.97] | 0.35 [0.14, 0.60] | 3.90 [1.36, 5.53] | 2.66 [0.66, 3.97] | -0.02 [-0.08, 0.01] | -0.02 [-0.08, 0.01] | 0.19 [0.10, 0.37] | -0.02 [-0.08, 0.01] | 0.01 [-0.02, 0.08] | -0.02 [-0.08, 0.01] | 0.01 [-0.01, 0.07] | -0.02 [-0.08, 0.01] | -0.55 [-1.02, -0.20]* | 1.25 [0.89, 1.63] | 1.44 [0.91, 1.88] | 0.98 [0.60, 1.65] | wt_med |
| 7 | 3.08 [0.77, 4.66] | 0.41 [0.25, 0.70] | 4.25 [1.79, 6.34] | 3.05 [0.81, 4.66] | -0.02 [-0.09, 0.01] | -0.02 [-0.09, 0.01] | 0.32 [0.15, 0.51] | -0.02 [-0.09, 0.01] | -0.00 [-0.04, 0.07] | -0.02 [-0.09, 0.01] | 0.00 [-0.03, 0.06] | -0.02 [-0.09, 0.01] | -0.25 [-0.71, 0.14] | 1.39 [1.18, 1.99] | 1.63 [1.42, 2.13] |  | wt_med |
| 10 |  |  |  |  | 0.07 [-0.12, 0.26] | 0.07 [-0.12, 0.27] | 0.13 [-0.04, 0.35] | 0.01 [-0.15, 0.13] | -0.04 [-0.29, 0.25] | 0.03 [-0.13, 0.16] | 0.17 [0.01, 0.44] | -0.10 [-0.38, 0.07] | -0.81 [-1.52, -0.28]* | 1.68 [1.21, 2.25] | 1.97 [1.41, 2.59] |  | wt_med |
| 14 |  |  |  |  | -0.04 [-0.10, -0.00] | -0.04 [-0.10, -0.00] | 0.20 [-0.10, 0.43] | -0.03 [-0.08, 0.01] | -0.01 [-0.05, 0.02] | -0.02 [-0.07, 0.04] | -0.01 [-0.04, 0.02] | -0.05 [-0.16, -0.00] | -0.12 [-0.62, 0.68] | 1.12 [0.18, 2.42] | 1.35 [0.37, 2.60] |  | wt_med |
| 21 |  |  |  |  | -0.01 [-0.04, 0.01] | -0.01 [-0.04, 0.01] | 0.01 [-0.48, 0.37] | -0.00 [-0.03, 0.03] | -0.00 [-0.04, 0.04] | 0.01 [-0.03, 0.08] | -0.00 [-0.03, 0.03] | -0.04 [-0.13, 0.01] | 0.12 [-0.65, 1.46] | 1.46 [0.70, 2.96] | 1.72 [0.86, 3.27] |  | wt_med |
| 30 |  |  |  |  | -0.10 [-0.30, -0.01]* | -0.10 [-0.29, -0.01]* | 0.30 [0.14, 0.62] | -0.10 [-0.29, -0.01]* | -0.10 [-0.34, 0.00] | -0.10 [-0.29, -0.01]* | -0.07 [-0.28, 0.02] | -0.11 [-0.31, -0.02]* | 0.14 [-0.81, 1.04] | 3.12 [2.13, 4.19] | 3.48 [2.28, 4.60] |  | wt_med |
| 45 |  |  |  |  | -0.08 [-0.32, 0.05] | -0.08 [-0.32, 0.05] | 0.21 [0.03, 0.50] | -0.12 [-0.34, -0.02]* | -0.02 [-0.06, 0.01] | -0.13 [-0.34, -0.03]* | -0.16 [-0.64, 0.07] | -0.22 [-0.46, -0.07]* | -0.13 [-1.19, 0.85] | 1.51 [0.98, 2.07] | 1.67 [1.13, 2.23] |  | wt_med |
| 60 |  |  |  |  | -0.05 [-0.16, 0.02] | -0.05 [-0.15, 0.02] | 0.13 [-0.20, 0.50] | -0.03 [-0.13, 0.04] |  | -0.03 [-0.13, 0.04] | 0.00 [0.00, 0.00] | -0.08 [-0.26, 0.04] | -0.36 [-1.52, 0.50] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |
| 90 |  |  |  |  | -0.07 [-0.21, 0.02] | -0.06 [-0.16, 0.01] | 0.20 [-0.38, 0.98] | -0.08 [-0.23, 0.01] |  | -0.07 [-0.18, 0.01] |  | 0.02 [-0.16, 0.31] | -0.05 [-1.42, 0.99] |  |  |  | wt_med |

**rope-drop worth agreement**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] |  |  | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] |  |  | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] |
| 1 | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.92 [0.92, 0.93] | 0.92 [0.91, 0.92] |  |  | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 2 | 0.87 [0.86, 0.88] | 0.88 [0.87, 0.89] | 0.87 [0.86, 0.88] | 0.87 [0.86, 0.88] | 0.89 [0.88, 0.90] | 0.89 [0.88, 0.90] | 0.89 [0.89, 0.90] | 0.88 [0.88, 0.89] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.91] |  |  | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.90] |
| 3 | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.93] |
| 4 | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] |
| 5 | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.93 [0.92, 0.94] | 0.92 [0.91, 0.93] |
| 6 | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] | 0.91 [0.89, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.93] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.93 [0.92, 0.94] | 0.92 [0.91, 0.93] |
| 7 | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.90 [0.89, 0.91] |  | 0.92 [0.91, 0.92] |
| 10 |  |  |  |  | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] |  | 0.92 [0.91, 0.93] |
| 14 |  |  |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] |  | 0.92 [0.91, 0.92] |
| 21 |  |  |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] |  | 0.92 [0.91, 0.92] |
| 30 |  |  |  |  | 0.89 [0.88, 0.89] | 0.89 [0.88, 0.89] | 0.89 [0.89, 0.90] | 0.88 [0.88, 0.89] | 0.87 [0.86, 0.89] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.90 [0.89, 0.90] | 0.88 [0.87, 0.89] | 0.88 [0.86, 0.89] |  | 0.90 [0.89, 0.90] |
| 45 |  |  |  |  | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.85 [0.83, 0.88] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.93] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] |  | 0.91 [0.91, 0.92] |
| 60 |  |  |  |  | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.93] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.93] |  | 0.92 [0.91, 0.92] | 1.00 [1.00, 1.00] | 0.91 [0.90, 0.91] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.92 [0.91, 0.93] |
| 90 |  |  |  |  | 0.93 [0.92, 0.94] | 0.93 [0.92, 0.94] | 0.92 [0.91, 0.93] | 0.93 [0.92, 0.94] |  | 0.93 [0.92, 0.94] |  | 0.91 [0.90, 0.92] |  |  |  | 0.93 [0.92, 0.94] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] |  |  | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | 0.00 [-0.01, 0.01] |  |  | -0.01 [-0.01, -0.00] |  | wt_med |
| 1 | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.01 [-0.02, -0.00] | 0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.01] |  |  | -0.01 [-0.02, -0.00] |  | wt_med |
| 2 | -0.02 [-0.03, -0.00] | -0.01 [-0.02, 0.00] | -0.02 [-0.04, -0.00] | -0.02 [-0.03, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | 0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.01] | 0.01 [-0.00, 0.02] |  |  |  | 0.00 [-0.01, 0.01] | snaive7 |
| 3 | -0.01 [-0.02, -0.00] | -0.01 [-0.01, 0.00] | -0.02 [-0.03, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.02, -0.01] |  | wt_med |
| 4 | -0.02 [-0.03, -0.01] | -0.01 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 5 | -0.02 [-0.04, 0.00] | -0.00 [-0.01, 0.01] | -0.02 [-0.05, 0.00] | -0.01 [-0.04, 0.00] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.00, 0.01] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.00] | 0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, 0.00] | -0.02 [-0.04, 0.00] |  | 0.00 [-0.00, 0.01] | snaive7 |
| 6 | -0.01 [-0.03, 0.00] | 0.00 [-0.01, 0.01] | -0.02 [-0.04, 0.00] | -0.01 [-0.03, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.00, 0.01] | 0.00 [-0.00, 0.01] | -0.01 [-0.03, 0.01] | -0.01 [-0.03, 0.01] |  | 0.00 [-0.01, 0.01] | snaive7 |
| 7 | -0.02 [-0.03, -0.00] | -0.01 [-0.02, -0.00] | -0.02 [-0.04, -0.01] | -0.02 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.00] |  |  | wt_med |
| 10 |  |  |  |  | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  |  | wt_med |
| 14 |  |  |  |  | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.00] |  |  | wt_med |
| 21 |  |  |  |  | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  |  | wt_med |
| 30 |  |  |  |  | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.00] |  |  | wt_med |
| 45 |  |  |  |  | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.02 [-0.05, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.03, -0.01] |  |  | wt_med |
| 60 |  |  |  |  | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] |  | -0.00 [-0.01, -0.00] | 0.00 [0.00, 0.00] | -0.01 [-0.02, -0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  | wt_med |
| 90 |  |  |  |  | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] |  | -0.00 [-0.01, 0.01] |  | -0.02 [-0.03, -0.01] |  |  |  |  | wt_med |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.20 [0.16, 0.24] | 0.46 [0.42, 0.49] | 0.45 [0.42, 0.49] | 0.30 [0.27, 0.33] | 0.39 [0.36, 0.43] | 0.40 [0.36, 0.43] | 0.45 [0.41, 0.49] | 0.37 [0.34, 0.40] |
| 2 | 0.13 [0.10, 0.16] | 0.44 [0.40, 0.47] | 0.45 [0.41, 0.48] | 0.43 [0.39, 0.46] | 0.47 [0.43, 0.50] | 0.43 [0.40, 0.47] | 0.43 [0.39, 0.47] | 0.42 [0.39, 0.46] |
| 3 | 0.17 [0.13, 0.21] | 0.43 [0.40, 0.46] | 0.44 [0.40, 0.47] | 0.36 [0.33, 0.39] | 0.45 [0.41, 0.48] | 0.39 [0.36, 0.42] | 0.41 [0.37, 0.45] | 0.42 [0.38, 0.45] |
| 4 | 0.20 [0.16, 0.24] | 0.41 [0.37, 0.44] | 0.41 [0.37, 0.44] | 0.31 [0.28, 0.35] | 0.39 [0.36, 0.42] | 0.40 [0.36, 0.43] | 0.40 [0.35, 0.44] | 0.32 [0.29, 0.36] |
| 5 | 0.25 [0.21, 0.29] | 0.38 [0.35, 0.42] | 0.39 [0.35, 0.42] | 0.29 [0.26, 0.33] | 0.37 [0.34, 0.41] | 0.38 [0.35, 0.41] | 0.42 [0.37, 0.46] | 0.32 [0.29, 0.36] |
| 6 | 0.23 [0.19, 0.27] | 0.44 [0.40, 0.47] | 0.43 [0.40, 0.47] | 0.28 [0.25, 0.32] | 0.41 [0.38, 0.45] | 0.43 [0.40, 0.47] | 0.40 [0.36, 0.45] | 0.34 [0.30, 0.37] |
| 7 | 0.21 [0.17, 0.25] | 0.38 [0.35, 0.42] | 0.39 [0.36, 0.42] | 0.27 [0.24, 0.31] | 0.31 [0.28, 0.35] |  | 0.41 [0.36, 0.45] | 0.32 [0.29, 0.36] |
| 10 | 0.17 [0.13, 0.20] | 0.44 [0.40, 0.47] | 0.43 [0.40, 0.46] | 0.35 [0.31, 0.38] | 0.43 [0.40, 0.47] |  | 0.40 [0.35, 0.44] | 0.38 [0.35, 0.41] |
| 14 | 0.22 [0.18, 0.26] | 0.33 [0.30, 0.37] | 0.34 [0.31, 0.38] | 0.26 [0.22, 0.29] | 0.31 [0.27, 0.34] |  | 0.34 [0.30, 0.38] | 0.30 [0.26, 0.33] |
| 21 | 0.20 [0.15, 0.24] | 0.31 [0.28, 0.35] | 0.33 [0.29, 0.36] | 0.26 [0.23, 0.29] | 0.29 [0.26, 0.33] |  | 0.32 [0.28, 0.37] | 0.31 [0.27, 0.34] |
| 30 | 0.12 [0.09, 0.16] | 0.40 [0.37, 0.44] | 0.40 [0.37, 0.44] | 0.39 [0.35, 0.43] | 0.40 [0.36, 0.44] |  | 0.36 [0.31, 0.41] | 0.39 [0.36, 0.43] |
| 45 | 0.25 [0.17, 0.34] | 0.34 [0.31, 0.38] | 0.36 [0.32, 0.40] | 0.30 [0.27, 0.34] | 0.38 [0.34, 0.42] |  | 0.38 [0.32, 0.44] | 0.34 [0.30, 0.38] |
| 60 |  | 0.29 [0.25, 0.33] | 0.27 [0.24, 0.31] | 0.27 [0.23, 0.30] | 0.29 [0.25, 0.32] |  |  | 0.29 [0.25, 0.33] |
| 90 |  | 0.28 [0.24, 0.33] | 0.30 [0.26, 0.34] | 0.22 [0.18, 0.25] | 0.33 [0.28, 0.37] |  |  | 0.27 [0.23, 0.32] |
| 120 |  | 0.27 [0.22, 0.32] | 0.26 [0.21, 0.31] | 0.24 [0.19, 0.29] | 0.21 [0.16, 0.25] |  |  | 0.28 [0.23, 0.33] |
| 180 |  | 0.20 [0.13, 0.28] | 0.21 [0.14, 0.29] | 0.12 [0.06, 0.19] | 0.21 [0.14, 0.30] |  |  | 0.20 [0.13, 0.28] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.26 [-0.35, -0.16] | 0.07 [0.02, 0.10]* | 0.06 [0.02, 0.10]* | -0.10 [-0.16, -0.04] | 0.00 [-0.05, 0.05] |  | 0.02 [-0.05, 0.09] | -0.03 [-0.08, 0.03] | lvl_snaive7 |
| 2 | -0.28 [-0.38, -0.19] | 0.00 [-0.05, 0.06] | 0.01 [-0.04, 0.06] | -0.01 [-0.06, 0.04] | 0.03 [-0.01, 0.07] |  | 0.02 [-0.03, 0.08] | -0.01 [-0.07, 0.05] | lvl_snaive7 |
| 3 | -0.31 [-0.39, -0.23] | -0.02 [-0.06, 0.02] | -0.01 [-0.05, 0.03] | -0.08 [-0.13, -0.04] |  | -0.06 [-0.10, -0.02] | -0.04 [-0.08, 0.01] | -0.03 [-0.08, 0.01] | lvl_naive4 |
| 4 | -0.24 [-0.33, -0.15] | 0.01 [-0.01, 0.04] | 0.01 [-0.02, 0.04] | -0.09 [-0.13, -0.05] | -0.01 [-0.05, 0.02] |  | -0.02 [-0.07, 0.04] | -0.08 [-0.12, -0.04] | lvl_snaive7 |
| 5 | -0.14 [-0.21, -0.07] | 0.01 [-0.04, 0.05] | 0.01 [-0.03, 0.05] | -0.09 [-0.15, -0.03] | -0.01 [-0.05, 0.04] |  | 0.04 [-0.01, 0.08] | -0.06 [-0.13, -0.00] | lvl_snaive7 |
| 6 | -0.20 [-0.27, -0.14] | 0.01 [-0.02, 0.05] | 0.01 [-0.03, 0.05] | -0.15 [-0.21, -0.09] | -0.01 [-0.07, 0.03] |  | -0.03 [-0.07, 0.01] | -0.10 [-0.16, -0.04] | lvl_snaive7 |
| 7 | -0.09 [-0.18, -0.00] | 0.07 [0.03, 0.12]* | 0.08 [0.03, 0.13]* | -0.04 [-0.11, 0.02] |  |  | 0.11 [0.05, 0.16]* | 0.01 [-0.04, 0.06] | lvl_naive4 |
| 10 | -0.28 [-0.36, -0.19] | 0.00 [-0.04, 0.05] | -0.00 [-0.05, 0.04] | -0.09 [-0.15, -0.02] |  |  | -0.03 [-0.08, 0.03] | -0.05 [-0.11, 0.00] | lvl_naive4 |
| 14 | -0.08 [-0.16, 0.01] | 0.03 [-0.01, 0.07] | 0.04 [-0.00, 0.08] | -0.05 [-0.10, -0.00] |  |  | 0.04 [-0.00, 0.09] | -0.00 [-0.05, 0.04] | lvl_naive4 |
| 21 | -0.08 [-0.16, -0.01] | 0.02 [-0.03, 0.07] | 0.04 [-0.01, 0.08] | -0.03 [-0.08, 0.01] |  |  | 0.04 [-0.02, 0.10] | 0.02 [-0.03, 0.06] | lvl_naive4 |
| 30 | -0.30 [-0.38, -0.22] | 0.00 [-0.04, 0.04] | 0.00 [-0.04, 0.05] | -0.01 [-0.05, 0.02] |  |  | -0.06 [-0.14, 0.01] | -0.01 [-0.04, 0.03] | lvl_naive4 |
| 45 | -0.17 [-0.29, -0.03] | -0.04 [-0.08, 0.00] | -0.02 [-0.06, 0.02] | -0.08 [-0.14, -0.02] |  |  | -0.00 [-0.10, 0.10] | -0.04 [-0.09, 0.02] | lvl_naive4 |
| 60 |  | 0.01 [-0.04, 0.05] | -0.01 [-0.06, 0.04] | -0.02 [-0.08, 0.05] |  |  |  | 0.00 [-0.04, 0.05] | lvl_naive4 |
| 90 |  | -0.04 [-0.08, 0.00] | -0.02 [-0.07, 0.02] | -0.11 [-0.17, -0.05] |  |  |  | -0.05 [-0.10, -0.00] | lvl_naive4 |
| 120 |  | -0.01 [-0.09, 0.07] | -0.02 [-0.10, 0.05] | -0.04 [-0.11, 0.03] | -0.08 [-0.15, -0.02] |  |  |  | lvl_wt56 |
| 180 |  | 0.00 [-0.10, 0.09] | 0.01 [-0.07, 0.07] | -0.09 [-0.21, 0.04] |  |  |  | -0.01 [-0.12, 0.10] | lvl_naive4 |

| lead | model | park_days | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|
| 1 | lvl_naive4 | 782 | 0.83 | 0.47 | 0.60 |
| 1 | lvl_wt56 | 788 | 0.82 | 0.37 | 0.61 |
| 1 | lvl_snaive7 | 777 | 0.80 | 0.60 | 0.57 |
| 1 | lvl_clim | 788 | 0.78 | 0.20 | 0.58 |
| 1 | lvl_tft | 527 | 0.87 | 0.58 | 0.65 |
| 1 | lvl_cbd | 398 | 0.72 | 0.09 | 0.90 |
| 1 | lvl_chronos2 | 788 | 0.89 | 0.58 | 0.74 |
| 1 | lvl_chronos2_owx | 788 | 0.90 | 0.58 | 0.74 |
| 7 | lvl_naive4 | 722 | 0.78 | 0.46 | 0.52 |
| 7 | lvl_wt56 | 732 | 0.78 | 0.33 | 0.54 |
| 7 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 7 | lvl_clim | 732 | 0.74 | 0.18 | 0.57 |
| 7 | lvl_tft | 475 | 0.80 | 0.48 | 0.58 |
| 7 | lvl_cbd | 388 | 0.60 | 0.04 | 0.83 |
| 7 | lvl_chronos2 | 732 | 0.85 | 0.44 | 0.68 |
| 7 | lvl_chronos2_owx | 732 | 0.85 | 0.45 | 0.68 |
| 30 | lvl_naive4 | 681 | 0.88 | 0.75 | 0.75 |
| 30 | lvl_wt56 | 683 | 0.88 | 0.63 | 0.85 |
| 30 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_clim | 683 | 0.86 | 0.63 | 0.79 |
| 30 | lvl_tft | 356 | 0.82 | 0.74 | 0.68 |
| 30 | lvl_cbd | 330 | 0.46 | 0.05 | 1.00 |
| 30 | lvl_chronos2 | 683 | 0.87 | 0.64 | 0.83 |
| 30 | lvl_chronos2_owx | 683 | 0.87 | 0.66 | 0.83 |
| 90 | lvl_naive4 | 424 | 0.73 | 0.29 | 0.38 |
| 90 | lvl_wt56 | 434 | 0.76 | 0.23 | 0.47 |
| 90 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_clim | 434 | 0.70 | 0.14 | 0.55 |
| 90 | lvl_tft | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_cbd | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_chronos2 | 434 | 0.79 | 0.27 | 0.55 |
| 90 | lvl_chronos2_owx | 434 | 0.79 | 0.32 | 0.54 |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 3579 | 2485 | 0.69 |
| 1 | lvl_chronos2 | 7647 | 5866 | 0.77 |
| 1 | lvl_chronos2_owx | 7647 | 5868 | 0.77 |
| 1 | lvl_clim | 7647 | 4449 | 0.58 |
| 1 | lvl_naive4 | 7541 | 5196 | 0.69 |
| 1 | lvl_snaive7 | 7423 | 5186 | 0.70 |
| 1 | lvl_tft | 5603 | 3980 | 0.71 |
| 1 | lvl_wt56 | 7647 | 5023 | 0.66 |
| 7 | lvl_cbd | 3635 | 2211 | 0.61 |
| 7 | lvl_chronos2 | 6951 | 5006 | 0.72 |
| 7 | lvl_chronos2_owx | 6951 | 4989 | 0.72 |
| 7 | lvl_clim | 6951 | 3951 | 0.57 |
| 7 | lvl_naive4 | 6804 | 4335 | 0.64 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 4986 | 3366 | 0.68 |
| 7 | lvl_wt56 | 6951 | 4293 | 0.62 |
| 30 | lvl_cbd | 3335 | 2391 | 0.72 |
| 30 | lvl_chronos2 | 6873 | 5120 | 0.74 |
| 30 | lvl_chronos2_owx | 6873 | 5146 | 0.75 |
| 30 | lvl_clim | 6873 | 4903 | 0.71 |
| 30 | lvl_naive4 | 6841 | 5357 | 0.78 |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 4068 | 2918 | 0.72 |
| 30 | lvl_wt56 | 6873 | 5182 | 0.75 |
| 90 | lvl_cbd | 0 | 0 |  |
| 90 | lvl_chronos2 | 3686 | 2128 | 0.58 |
| 90 | lvl_chronos2_owx | 3686 | 2183 | 0.59 |
| 90 | lvl_clim | 3686 | 1686 | 0.46 |
| 90 | lvl_naive4 | 3548 | 2055 | 0.58 |
| 90 | lvl_snaive7 | 0 | 0 |  |
| 90 | lvl_tft | 0 | 0 |  |
| 90 | lvl_wt56 | 3686 | 2100 | 0.57 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie (counted wrong); CI over (origin, park).

**day comparison winner accuracy**

| lead | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|
| d1-7 | 0.20 [0.17, 0.22] | 0.25 [0.23, 0.27] | 0.25 [0.23, 0.27] | 0.19 [0.17, 0.21] | 0.38 [0.36, 0.40] | 0.44 [0.42, 0.46] | 0.36 [0.34, 0.39] | 0.20 [0.18, 0.21] |
| d8-30 | 0.12 [0.10, 0.13] | 0.24 [0.23, 0.26] | 0.25 [0.23, 0.26] | 0.19 [0.17, 0.20] | 0.31 [0.30, 0.32] |  | 0.31 [0.29, 0.33] | 0.17 [0.16, 0.18] |
| d31-90 | 0.10 [0.09, 0.11] | 0.26 [0.25, 0.28] | 0.27 [0.25, 0.28] | 0.20 [0.19, 0.22] | 0.29 [0.28, 0.30] |  | 0.30 [0.28, 0.31] | 0.17 [0.16, 0.18] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | lvl_clim | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|
| d1-7 | -0.22 [-0.26, -0.18] | -0.18 [-0.23, -0.14] | -0.18 [-0.22, -0.14] | -0.23 [-0.27, -0.19] | -0.04 [-0.07, -0.01] | -0.07 [-0.11, -0.04] | -0.23 [-0.26, -0.20] | lvl_snaive7 |
| d8-30 | -0.17 [-0.20, -0.14] | -0.07 [-0.11, -0.03] | -0.06 [-0.10, -0.02] | -0.12 [-0.15, -0.09] |  | 0.00 [-0.03, 0.04] | -0.14 [-0.15, -0.12] | lvl_naive4 |
| d31-90 | -0.16 [-0.19, -0.12] | -0.02 [-0.06, 0.02] | -0.02 [-0.06, 0.02] | -0.08 [-0.12, -0.05] |  | 0.03 [-0.01, 0.07] | -0.12 [-0.13, -0.10] | lvl_naive4 |

| lead | model | tie_rate |
|---|---|---|
| d1-7 | lvl_naive4 | 0.44 |
| d1-7 | lvl_wt56 | 0.70 |
| d1-7 | lvl_snaive7 | 0.32 |
| d1-7 | lvl_clim | 0.71 |
| d1-7 | lvl_tft | 0.47 |
| d1-7 | lvl_cbd | 0.70 |
| d1-7 | lvl_chronos2 | 0.66 |
| d1-7 | lvl_chronos2_owx | 0.66 |
| d8-30 | lvl_naive4 | 0.50 |
| d8-30 | lvl_wt56 | 0.71 |
| d8-30 | lvl_clim | 0.71 |
| d8-30 | lvl_tft | 0.49 |
| d8-30 | lvl_cbd | 0.80 |
| d8-30 | lvl_chronos2 | 0.65 |
| d8-30 | lvl_chronos2_owx | 0.65 |
| d31-90 | lvl_naive4 | 0.50 |
| d31-90 | lvl_wt56 | 0.71 |
| d31-90 | lvl_clim | 0.67 |
| d31-90 | lvl_tft | 0.50 |
| d31-90 | lvl_cbd | 0.81 |
| d31-90 | lvl_chronos2 | 0.60 |
| d31-90 | lvl_chronos2_owx | 0.59 |

Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the 1st and 15th):

| model | star_precision | star_recall | pred_stars | true_stars |
|---|---|---|---|---|
| lvl_cbd | 0.44 [0.00, 1.00] | 0.29 [0.00, 0.75] | 9 | 14 |
| lvl_chronos2 | 0.55 [0.17, 0.93] | 0.12 [0.02, 0.26] | 11 | 51 |
| lvl_chronos2_owx | 0.57 [0.25, 0.86] | 0.16 [0.04, 0.31] | 14 | 51 |
| lvl_clim | 0.56 [0.38, 1.00] | 0.10 [0.03, 0.19] | 9 | 51 |
| lvl_naive4 | 0.48 [0.30, 0.67] | 0.27 [0.14, 0.42] | 29 | 51 |
| lvl_snaive7 | 0.43 [0.26, 0.62] | 0.33 [0.18, 0.49] | 37 | 48 |
| lvl_tft | 0.48 [0.34, 0.67] | 0.32 [0.18, 0.49] | 33 | 50 |
| lvl_wt56 | 0.36 [0.12, 0.62] | 0.08 [0.02, 0.17] | 11 | 51 |

## D8 — uncertainty

**What production states** (`forecast-accuracy.service.ts`): MAE of TFT's predicted peak against the day's max hourly P90, by predicted band × lead bucket over the 45 days before the origin (cells with ≥ 500 comparisons). Realised = the same error of the TFT forecasts served at the origin; ratio > 1 = production understates its error.

| bucket | band | n | realised_mae | stated_mae | ratio |
|---|---|---|---|---|---|
| d1 | busy | 982.00 | 22.64 | 22.13 | 1.02 |
| d1 | mid | 3334.00 | 12.62 | 14.40 | 0.88 |
| d1 | quiet | 9786.00 | 5.58 | 6.85 | 0.81 |
| d14 | busy | 6555.00 | 24.57 | 24.56 | 1.00 |
| d14 | mid | 20658.00 | 14.29 | 15.82 | 0.90 |
| d14 | quiet | 58033.00 | 6.66 | 7.67 | 0.87 |
| d3 | busy | 2484.00 | 23.27 | 22.50 | 1.03 |
| d3 | mid | 7908.00 | 12.62 | 14.54 | 0.87 |
| d3 | quiet | 18726.00 | 5.90 | 6.97 | 0.85 |
| d30 | busy | 13519.00 | 24.39 | 25.12 | 0.97 |
| d30 | mid | 40844.00 | 14.61 | 16.24 | 0.90 |
| d30 | quiet | 108256.00 | 6.86 | 8.13 | 0.84 |
| d60 | busy | 10820.00 | 25.92 | 23.55 | 1.10 |
| d60 | mid | 30452.00 | 15.29 | 17.39 | 0.88 |
| d60 | quiet | 73593.00 | 6.57 | 10.08 | 0.65 |
| d7 | busy | 3886.00 | 24.11 | 23.60 | 1.02 |
| d7 | mid | 11455.00 | 14.42 | 14.98 | 0.96 |
| d7 | quiet | 34748.00 | 6.33 | 7.12 | 0.89 |

Empirical-quantile coverage (target 0.80 / 0.95) and the share of truth slots that HAVE an interval:

| L | wt_med cov_q80 | wt_med cov_q95 | wt_med field_coverage | chronos2 cov_q80 | chronos2 cov_q95 | chronos2 field_coverage | chronos2_grid cov_q80 | chronos2_grid cov_q95 | chronos2_grid field_coverage | chronos2_nocov cov_q80 | chronos2_nocov cov_q95 | chronos2_nocov field_coverage | chronos2_owx cov_q80 | chronos2_owx cov_q95 | chronos2_owx field_coverage |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.87 | 0.95 | 0.99 | 0.79 | 0.94 | 1.00 | 0.76 | 0.92 | 1.00 | 0.77 | 0.93 | 1.00 | 0.80 | 0.94 | 1.00 |
| 1 | 0.87 | 0.94 | 0.98 | 0.82 | 0.95 | 0.99 | 0.77 | 0.93 | 0.99 | 0.80 | 0.95 | 0.99 | 0.82 | 0.95 | 0.99 |
| 2 | 0.82 | 0.90 | 0.97 | 0.76 | 0.93 | 0.99 | 0.72 | 0.92 | 0.99 | 0.73 | 0.92 | 0.99 | 0.77 | 0.94 | 0.99 |
| 3 | 0.89 | 0.94 | 0.98 | 0.82 | 0.96 | 1.00 | 0.77 | 0.94 | 1.00 | 0.80 | 0.95 | 1.00 | 0.83 | 0.96 | 1.00 |
| 4 | 0.85 | 0.93 | 0.98 | 0.80 | 0.95 | 0.99 | 0.77 | 0.95 | 0.99 | 0.79 | 0.95 | 0.99 | 0.80 | 0.95 | 0.99 |
| 5 | 0.85 | 0.93 | 0.98 | 0.81 | 0.95 | 0.99 | 0.78 | 0.95 | 0.99 | 0.80 | 0.95 | 0.99 | 0.81 | 0.95 | 0.99 |
| 6 | 0.87 | 0.94 | 0.98 | 0.83 | 0.96 | 0.99 | 0.79 | 0.96 | 0.99 | 0.82 | 0.96 | 0.99 | 0.83 | 0.96 | 0.99 |
| 7 | 0.86 | 0.94 | 0.98 | 0.82 | 0.96 | 0.99 | 0.78 | 0.96 | 0.99 | 0.82 | 0.96 | 0.99 | 0.82 | 0.96 | 0.99 |
| 10 | 0.88 | 0.94 | 0.97 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 14 | 0.85 | 0.93 | 0.97 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 21 | 0.85 | 0.93 | 0.96 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 30 | 0.82 | 0.89 | 0.94 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 45 | 0.88 | 0.93 | 0.94 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 60 | 0.85 | 0.92 | 0.92 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 90 | 0.88 | 0.94 | 0.89 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |

Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:

| model | L | realised | stated_weighted | ratio |
|---|---|---|---|---|
| snaive7 | 0 | 8.34 | 8.58 | 0.97 |
| snaive7 | 1 | 8.37 | 8.61 | 0.97 |
| snaive7 | 3 | 8.03 | 8.24 | 0.97 |
| wt_med | 0 | 7.01 | 7.14 | 0.98 |
| wt_med | 1 | 7.07 | 7.22 | 0.98 |
| wt_med | 3 | 6.84 | 7.05 | 0.97 |
| wt_med | 7 | 7.36 | 7.58 | 0.97 |
| wt_med | 14 | 7.56 | 7.94 | 0.95 |
| wt_med | 30 | 7.52 | 7.86 | 0.96 |
| wt_med | 60 | 7.90 | 8.18 | 0.97 |
| wt_med | 90 | 7.34 | 9.38 | 0.78 |
| clim | 0 | 7.72 | 7.94 | 0.97 |
| clim | 1 | 7.65 | 7.86 | 0.97 |
| clim | 3 | 7.26 | 7.50 | 0.97 |
| clim | 7 | 7.88 | 8.13 | 0.97 |
| clim | 14 | 7.93 | 8.31 | 0.95 |
| clim | 30 | 7.72 | 8.21 | 0.94 |
| clim | 60 | 7.97 | 8.26 | 0.97 |
| clim | 90 | 8.04 | 9.23 | 0.87 |
| h5 | 0 | 6.96 | 7.11 | 0.98 |
| h5 | 1 | 7.02 | 7.18 | 0.98 |
| h5 | 3 | 6.73 | 6.94 | 0.97 |
| h5 | 7 | 7.28 | 7.52 | 0.97 |
| h5 | 14 | 7.47 | 7.84 | 0.95 |
| h5 | 30 | 7.44 | 7.74 | 0.96 |
| h5 | 60 | 7.82 | 8.00 | 0.98 |
| h5 | 90 | 7.25 | 9.09 | 0.80 |
| lvlh5_naive | 0 | 7.45 | 7.60 | 0.98 |
| lvlh5_naive | 1 | 7.41 | 7.58 | 0.98 |
| lvlh5_naive | 3 | 6.82 | 7.01 | 0.97 |
| lvlh5_naive | 7 | 7.83 | 7.97 | 0.98 |
| lvlh5_naive | 14 | 8.11 | 8.20 | 0.99 |
| lvlh5_naive | 30 | 7.88 | 8.19 | 0.96 |
| lvlh5_naive | 60 | 8.14 | 8.24 | 0.99 |
| lvlh5_naive | 90 | 7.91 | 9.01 | 0.88 |
| lvlh5_tft | 0 | 6.55 | 6.64 | 0.99 |
| lvlh5_tft | 1 | 6.59 | 6.72 | 0.98 |
| lvlh5_tft | 3 | 6.85 | 6.84 | 1.00 |
| lvlh5_tft | 7 | 7.27 | 7.35 | 0.99 |
| lvlh5_tft | 14 | 7.70 | 7.66 | 1.00 |
| lvlh5_tft | 30 | 8.31 | 8.29 | 1.00 |
| lvlh5_cbd | 0 | 9.48 | 9.56 | 0.99 |
| lvlh5_cbd | 1 | 9.01 | 9.04 | 1.00 |
| lvlh5_cbd | 3 | 9.42 | 9.33 | 1.01 |
| lvlh5_cbd | 7 | 9.90 | 10.03 | 0.99 |
| lvlh5_cbd | 14 | 10.24 | 10.13 | 1.01 |
| lvlh5_cbd | 30 | 11.37 | 10.17 | 1.12 |
| prod_served | 3 | 7.54 | 7.58 | 0.99 |
| prod_served | 7 | 7.70 | 7.89 | 0.98 |
| prod_served | 14 | 8.09 | 8.27 | 0.98 |
| prod_served | 30 | 9.14 | 10.01 | 0.91 |
| prod_served_lin | 3 | 7.52 | 7.56 | 0.99 |
| prod_served_lin | 7 | 7.68 | 7.88 | 0.98 |
| prod_served_lin | 14 | 8.08 | 8.25 | 0.98 |
| prod_served_lin | 30 | 9.10 | 9.98 | 0.91 |
| chronos2 | 0 | 6.12 | 6.26 | 0.98 |
| chronos2 | 1 | 6.53 | 6.69 | 0.98 |
| chronos2 | 3 | 6.92 | 7.14 | 0.97 |
| chronos2 | 7 | 7.28 | 7.56 | 0.96 |
| chronos2_x_h5 | 1 | 6.77 | 6.93 | 0.98 |
| chronos2_x_h5 | 3 | 6.81 | 7.03 | 0.97 |
| chronos2_x_h5 | 7 | 7.14 | 7.48 | 0.95 |
| chronos2_x_h5 | 14 | 7.40 | 7.91 | 0.94 |
| chronos2_x_h5 | 30 | 7.84 | 8.22 | 0.95 |
| chronos2_x_h5 | 60 | 7.92 | 8.07 | 0.98 |
| chronos2_x_h5 | 90 | 7.48 | 8.92 | 0.84 |
| chronos2_grid | 0 | 5.97 | 6.10 | 0.98 |
| chronos2_grid | 1 | 6.45 | 6.58 | 0.98 |
| chronos2_grid | 3 | 6.79 | 7.02 | 0.97 |
| chronos2_grid | 7 | 7.05 | 7.40 | 0.95 |
| chronos2_nocov | 0 | 6.41 | 6.53 | 0.98 |
| chronos2_nocov | 1 | 6.94 | 7.10 | 0.98 |
| chronos2_nocov | 3 | 7.45 | 7.65 | 0.97 |
| chronos2_nocov | 7 | 7.70 | 8.02 | 0.96 |
| chronos2_owx | 0 | 6.01 | 6.16 | 0.98 |
| chronos2_owx | 1 | 6.44 | 6.61 | 0.98 |
| chronos2_owx | 3 | 6.86 | 7.08 | 0.97 |
| chronos2_owx | 7 | 7.24 | 7.52 | 0.96 |
| chronos2_owx_x_h5 | 1 | 6.77 | 6.93 | 0.98 |
| chronos2_owx_x_h5 | 3 | 6.81 | 7.02 | 0.97 |
| chronos2_owx_x_h5 | 7 | 7.13 | 7.48 | 0.95 |
| chronos2_owx_x_h5 | 14 | 7.39 | 7.90 | 0.93 |
| chronos2_owx_x_h5 | 30 | 7.83 | 8.21 | 0.95 |
| chronos2_owx_x_h5 | 60 | 7.89 | 8.05 | 0.98 |
| chronos2_owx_x_h5 | 90 | 7.42 | 8.96 | 0.83 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.00 | 1.00 | 1.00 | 1.00 |  |  | 0.94 | 0.99 | 0.51 | 0.95 | 0.64 | 0.99 | 0.95 |  |  | 0.86 | 0.99 |
| 1 | 0.99 | 0.99 | 0.99 | 0.99 | 0.98 | 0.98 | 0.93 | 0.98 | 0.48 | 0.95 | 0.62 | 0.98 | 0.95 |  |  | 0.86 | 0.98 |
| 2 | 0.99 | 0.99 | 0.99 | 0.99 | 0.97 | 0.97 | 0.91 | 0.98 | 0.49 | 0.96 | 0.62 | 0.97 | 0.96 |  |  | 0.89 | 0.97 |
| 3 | 1.00 | 1.00 | 1.00 | 1.00 | 0.98 | 0.98 | 0.92 | 0.98 | 0.50 | 0.97 | 0.63 | 0.98 | 0.97 | 0.63 | 0.63 | 0.88 | 0.98 |
| 4 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.94 | 0.99 | 0.51 | 0.96 | 0.64 | 0.98 | 0.96 | 0.63 | 0.63 | 0.87 | 0.98 |
| 5 | 0.99 | 0.99 | 0.99 | 0.99 | 0.98 | 0.98 | 0.94 | 0.98 | 0.51 | 0.95 | 0.63 | 0.98 | 0.95 | 0.63 | 0.63 | 0.87 | 0.98 |
| 6 | 0.99 | 0.99 | 0.99 | 0.99 | 0.98 | 0.98 | 0.94 | 0.98 | 0.51 | 0.96 | 0.63 | 0.98 | 0.96 | 0.63 | 0.63 | 0.88 | 0.98 |
| 7 | 0.99 | 0.99 | 0.99 | 0.99 | 0.98 | 0.98 | 0.94 | 0.98 | 0.51 | 0.94 | 0.62 | 0.98 | 0.94 | 0.62 | 0.62 |  | 0.98 |
| 10 |  |  |  |  | 0.98 | 0.98 | 0.92 | 0.98 | 0.50 | 0.96 | 0.61 | 0.98 | 0.97 | 0.61 | 0.61 |  | 0.97 |
| 14 |  |  |  |  | 0.98 | 0.98 | 0.93 | 0.98 | 0.49 | 0.93 | 0.60 | 0.97 | 0.93 | 0.60 | 0.60 |  | 0.97 |
| 21 |  |  |  |  | 0.97 | 0.97 | 0.92 | 0.97 | 0.46 | 0.92 | 0.57 | 0.97 | 0.92 | 0.58 | 0.58 |  | 0.96 |
| 30 |  |  |  |  | 0.95 | 0.95 | 0.88 | 0.95 | 0.44 | 0.93 | 0.47 | 0.95 | 0.94 | 0.49 | 0.49 |  | 0.94 |
| 45 |  |  |  |  | 0.96 | 0.96 | 0.88 | 0.96 | 0.11 | 0.94 | 0.35 | 0.96 | 0.95 | 0.38 | 0.38 |  | 0.94 |
| 60 |  |  |  |  | 0.93 | 0.93 | 0.88 | 0.93 |  | 0.88 | 0.01 | 0.93 | 0.88 | 0.01 | 0.01 |  | 0.92 |
| 90 |  |  |  |  | 0.90 | 0.90 | 0.84 | 0.90 |  | 0.85 |  | 0.90 | 0.86 |  |  |  | 0.89 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | n_ride_days |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.96 | 0.99 | 0.95 | 0.97 | 0.63 | 0.48 | 1.00 | 1.00 | 7503 |
| 2 | 0.97 | 0.99 | 0.97 | 0.95 | 0.63 | 0.50 | 1.00 | 1.00 | 7699 |
| 3 | 0.98 | 0.99 | 0.97 | 0.95 | 0.63 | 0.50 | 1.00 | 1.00 | 7669 |
| 4 | 0.97 | 1.00 | 0.96 | 0.97 | 0.63 | 0.50 | 1.00 | 1.00 | 7293 |
| 5 | 0.97 | 1.00 | 0.95 | 0.97 | 0.63 | 0.51 | 1.00 | 1.00 | 7056 |
| 6 | 0.97 | 1.00 | 0.96 | 0.97 | 0.63 | 0.50 | 1.00 | 1.00 | 7158 |
| 7 | 0.95 | 0.99 | 0.00 | 0.97 | 0.62 | 0.50 | 1.00 | 1.00 | 6951 |
| 10 | 0.97 | 0.99 | 0.00 | 0.95 | 0.62 | 0.51 | 1.00 | 1.00 | 7431 |
| 14 | 0.94 | 0.99 | 0.00 | 0.96 | 0.60 | 0.48 | 1.00 | 1.00 | 6731 |
| 21 | 0.94 | 0.99 | 0.00 | 0.96 | 0.57 | 0.46 | 1.00 | 1.00 | 6548 |
| 30 | 0.96 | 0.99 | 0.00 | 0.94 | 0.47 | 0.45 | 1.00 | 1.00 | 6763 |
| 45 | 0.96 | 0.99 | 0.00 | 0.94 | 0.36 | 0.15 | 1.00 | 1.00 | 6247 |
| 60 | 0.92 | 0.98 | 0.00 | 0.95 | 0.00 | 0.00 | 1.00 | 1.00 | 5532 |
| 90 | 0.90 | 0.98 | 0.00 | 0.93 | 0.00 | 0.00 | 1.00 | 1.00 | 4409 |
| 120 | 0.88 | 0.97 | 0.00 | 0.92 | 0.00 | 0.00 | 1.00 | 1.00 | 3325 |
| 180 | 0.79 | 0.96 | 0.00 | 0.95 | 0.00 | 0.00 | 1.00 | 1.00 | 1341 |

Forecast present vs ride operated. The baselines have **no openness logic** — they forecast whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never opened still got a curve; it is the bar a model with an open/closed decision has to beat:

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | prod_served_lin P(fc|operated) | prod_served_lin P(fc|not operated) | chronos2 P(fc|operated) | chronos2 P(fc|not operated) | chronos2_x_h5 P(fc|operated) | chronos2_x_h5 P(fc|not operated) | chronos2_grid P(fc|operated) | chronos2_grid P(fc|not operated) | chronos2_nocov P(fc|operated) | chronos2_nocov P(fc|not operated) | chronos2_owx P(fc|operated) | chronos2_owx P(fc|not operated) | chronos2_owx_x_h5 P(fc|operated) | chronos2_owx_x_h5 P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.87 | 0.26 | 0.96 | 0.89 | 0.94 | 0.93 | 0.97 | 0.89 | 0.93 | 0.80 | 0.62 | 0.53 | 0.48 | 0.38 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 1.00 | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.00 | 0.00 | 23339.00 | 1734.00 |
| 1 | 0.89 | 0.27 | 0.96 | 0.88 | 0.94 | 0.92 | 0.96 | 0.89 | 0.93 | 0.79 | 0.62 | 0.52 | 0.46 | 0.36 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 1.00 | 0.95 | 0.87 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.95 | 0.87 | 24601.00 | 1683.00 |
| 2 | 0.92 | 0.28 | 0.96 | 0.89 | 0.92 | 0.83 | 0.96 | 0.89 | 0.93 | 0.83 | 0.61 | 0.54 | 0.47 | 0.41 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.95 | 0.88 | 25309.00 | 1650.00 |
| 3 | 0.91 | 0.26 | 0.96 | 0.89 | 0.91 | 0.84 | 0.96 | 0.90 | 0.93 | 0.80 | 0.61 | 0.53 | 0.47 | 0.39 | 0.60 | 0.52 | 0.60 | 0.52 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.95 | 0.88 | 25345.00 | 1598.00 |
| 4 | 0.90 | 0.28 | 0.96 | 0.90 | 0.94 | 0.92 | 0.97 | 0.91 | 0.93 | 0.80 | 0.62 | 0.55 | 0.47 | 0.38 | 0.61 | 0.54 | 0.61 | 0.54 | 1.00 | 1.00 | 0.96 | 0.89 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.96 | 0.89 | 23560.00 | 1876.00 |
| 5 | 0.89 | 0.29 | 0.96 | 0.90 | 0.94 | 0.92 | 0.96 | 0.91 | 0.93 | 0.81 | 0.62 | 0.56 | 0.48 | 0.39 | 0.61 | 0.55 | 0.61 | 0.55 | 1.00 | 1.00 | 0.96 | 0.89 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.96 | 0.89 | 22836.00 | 1870.00 |
| 6 | 0.89 | 0.29 | 0.96 | 0.91 | 0.94 | 0.92 | 0.97 | 0.91 | 0.93 | 0.82 | 0.61 | 0.55 | 0.47 | 0.40 | 0.61 | 0.54 | 0.61 | 0.54 | 1.00 | 1.00 | 0.96 | 0.89 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.96 | 0.89 | 23486.00 | 1889.00 |
| 7 | 0.00 | 0.00 | 0.96 | 0.91 | 0.94 | 0.93 | 0.96 | 0.91 | 0.93 | 0.83 | 0.61 | 0.53 | 0.48 | 0.42 | 0.60 | 0.53 | 0.60 | 0.53 | 1.00 | 1.00 | 0.95 | 0.90 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.95 | 0.90 | 22535.00 | 1692.00 |
| 10 | 0.00 | 0.00 | 0.96 | 0.90 | 0.91 | 0.83 | 0.96 | 0.91 | 0.93 | 0.82 | 0.59 | 0.50 | 0.48 | 0.39 | 0.59 | 0.50 | 0.59 | 0.50 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.89 | 24466.00 | 1604.00 |
| 14 | 0.00 | 0.00 | 0.96 | 0.91 | 0.94 | 0.92 | 0.96 | 0.91 | 0.93 | 0.82 | 0.59 | 0.50 | 0.47 | 0.40 | 0.58 | 0.50 | 0.58 | 0.50 | 0.00 | 0.00 | 0.95 | 0.90 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.90 | 21814.00 | 1711.00 |
| 21 | 0.00 | 0.00 | 0.96 | 0.91 | 0.93 | 0.90 | 0.96 | 0.92 | 0.92 | 0.82 | 0.56 | 0.47 | 0.44 | 0.38 | 0.56 | 0.46 | 0.56 | 0.46 | 0.00 | 0.00 | 0.95 | 0.90 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.90 | 21219.00 | 1707.00 |
| 30 | 0.00 | 0.00 | 0.96 | 0.90 | 0.91 | 0.78 | 0.96 | 0.91 | 0.93 | 0.85 | 0.46 | 0.40 | 0.42 | 0.36 | 0.47 | 0.41 | 0.47 | 0.41 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.89 | 21956.00 | 1605.00 |
| 45 | 0.00 | 0.00 | 0.96 | 0.90 | 0.91 | 0.78 | 0.96 | 0.91 | 0.93 | 0.82 | 0.35 | 0.30 | 0.11 | 0.03 | 0.37 | 0.28 | 0.37 | 0.28 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.89 | 20335.00 | 1528.00 |
| 60 | 0.00 | 0.00 | 0.95 | 0.90 | 0.92 | 0.87 | 0.96 | 0.91 | 0.91 | 0.83 | 0.01 | 0.00 | 0.00 | 0.00 | 0.01 | 0.00 | 0.01 | 0.00 | 0.00 | 0.00 | 0.95 | 0.88 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.88 | 17668.00 | 1666.00 |
| 90 | 0.00 | 0.00 | 0.95 | 0.91 | 0.92 | 0.86 | 0.95 | 0.92 | 0.91 | 0.84 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.88 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.88 | 14168.00 | 1457.00 |
