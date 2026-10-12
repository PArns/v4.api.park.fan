# ml-bench results

Run: `20261011-par853-toto-tirex-shard5` · generated 2026-10-12T00:51+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with TARGET season. The common-window pass mixes lead with ORIGIN season instead. Both are confounded; neither alone identifies the horizon curve.

- code that produced `parts/` (the scoring): git `474e3932d4531fc93b5716be851328a38de10b64`, sources sha256 `8ee46789a2fd61c8`, image `sha256:f48c11976ad2`; plug-ins ['chronos2', 'chronos2_nocov', 'tirex2', 'tirex2_grid', 'toto2', 'toto2_grid', 'toto2_nocov']
- code that produced THESE tables (the report): git `474e3932d4531fc93b5716be851328a38de10b64` (from env), sources sha256 `8ee46789a2fd61c8`, image `unknown` — recorded in `report-tables.json`
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 299.9 shard-minutes over 1 shard(s)
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
| MAE | UC1 | all | tirex2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | all | toto2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | chronos2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | persistence | True | park-day | 8 | 8 | m105 | m105 |  |
| MAE | UC1 | busy | tirex2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | toto2 | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC2 | all | chronos2 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | chronos2_x_h5 | False | park-day | 1 | 1 | 1 | 1 |  |
| MAE | UC2 | all | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | tirex2_grid | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | all | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | chronos2 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | chronos2_nocov | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | busy | chronos2_x_h5 | False | park-day | 1 | 1 | 1 | 1 |  |
| MAE | UC2 | busy | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | lvlh5_tft | False | park-day | 2 | 2 |  | 1 |  |
| MAE | UC2 | busy | tirex2 | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | busy | tirex2_grid | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | tirex2_x_h5 | False | park-day | 1 | 1 | 1 | 1 |  |
| MAE | UC2 | busy | toto2 | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | busy | toto2_x_h5 | False | park-day | 1 | 1 | 1 | 1 |  |
| MAE | UC2 | busy | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2-intraday | all | chronos2 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | tirex2 | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | all | toto2 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | wt_med | True | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | chronos2 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | tirex2 | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | busy | toto2 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | wt_med | True | park-day | 3 | 3 |  | h8+ |  |
| MAE | UC3 | all | chronos2 | False | park-day | 7 | 7 | 1 | 1 |  |
| MAE | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 13 | 2 | 5 |  |
| MAE | UC3 | all | h5 | False | park-day | 14 | 13 | 60 | 60 |  |
| MAE | UC3 | all | wt_med | True | park-day | 14 | 13 | 45 | 45 |  |
| MAE | UC3 | busy | chronos2 | False | park-day | 7 | 7 | 2 | 2 |  |
| MAE | UC3 | busy | chronos2_x_h5 | False | park-day | 14 | 13 | 30 | 30 |  |
| MAE | UC3 | busy | h5 | False | park-day | 14 | 13 | 60 | 60 |  |
| MAE | UC3 | busy | lvlh5_naive | False | park-day | 14 | 13 |  | 6 |  |
| MAE | UC3 | busy | lvlh5_tft | False | park-day | 12 | 12 | 1 | 1 |  |
| MAE | UC3 | busy | tirex2_grid | False | park-day | 7 | 7 | 1 | 1 |  |
| MAE | UC3 | busy | tirex2_x_h5 | False | park-day | 14 | 13 | 21 | 21 |  |
| MAE | UC3 | busy | toto2_x_h5 | False | park-day | 14 | 13 | 3 | 6 |  |
| MAE | UC3 | busy | wt_med | True | park-day | 14 | 13 | 21 | 45 |  |
| MAE, schedule known at origin | UC3 | all | chronos2 | False | park-day | 8 | 8 | 1 | 1 |  |
| MAE, schedule known at origin | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 13 |  | 2 |  |
| MAE, schedule known at origin | UC3 | all | h5 | False | park-day | 15 | 14 | 60 | 60 |  |
| MAE, schedule known at origin | UC3 | all | tirex2_grid | False | park-day | 8 | 8 | 0 | 0 |  |
| MAE, schedule known at origin | UC3 | all | tirex2_x_h5 | False | park-day | 14 | 13 |  | 2 |  |
| MAE, schedule known at origin | UC3 | all | wt_med | True | park-day | 15 | 14 | 21 | 21 |  |
| MAE, schedule projected at origin | UC3 | all | h5 | False | park-day | 15 | 14 |  | 30 |  |
| MAE, schedule projected at origin | UC3 | all | wt_med | True | park-day | 15 | 14 | 30 | 30 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | False | park-day | 15 | 14 | 60 | 60 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | False | park-day | 15 | 14 |  | 10 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | True | park-day | 15 | 14 | 14 | 14 |  |
| best-time hit rate (top-2) | D3 | all | wt_med | True | park-day | 15 | 14 | 1 | 10 |  |
| best-time hit rate (±30 min) | D3 | all | h5 | False | park-day | 15 | 14 | 5 | 30 |  |
| best-time hit rate (±30 min) | D3 | all | wt_med | True | park-day | 15 | 14 | 0 | 10 |  |
| best-time regret (min) | D3 | all | h5 | False | park-day | 15 | 14 | 10 | 30 |  |
| best-time regret (min) | D3 | all | wt_med | True | park-day | 15 | 14 | 1 | 10 |  |
| crowd bucket exact | D6 | all | lvl_chronos2 | False | park-day | 16 | 13 | 1 | 14 |  |
| crowd bucket exact | D6 | all | lvl_tft | False | park-day | 13 | 12 |  | 7 |  |
| crowd bucket exact | D6 | all | lvl_tirex2 | False | park-day | 16 | 13 |  | 14 |  |
| day comparison winner accuracy | D7 | all | lvl_naive4 | True | park-origin | 3 | 3 |  | d31-90 |  |
| day comparison winner accuracy | D7 | all | lvl_snaive7 | True | park-origin | 1 | 1 | d1-7 | d1-7 |  |
| dayPeak Spearman across rides | UC3 | all | wt_med | True | park-day | 15 | 14 |  | 14 |  |
| dayPeak abs error (min) | UC3 | all | chronos2 | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | chronos2_x_h5 | False | park-day | 14 | 13 | 2 | 7 |  |
| dayPeak abs error (min) | UC3 | all | tirex2_grid | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | tirex2_x_h5 | False | park-day | 14 | 13 | 2 | 7 |  |
| dayPeak abs error (min) | UC3 | all | toto2_x_h5 | False | park-day | 14 | 13 | 1 | 1 |  |
| dayPeak abs error (min) | UC3 | all | wt_med | True | park-day | 15 | 14 | 3 | 10 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2 | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_nocov | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_x_h5 | False | park-day | 14 | 13 | 60 | 60 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | False | park-day | 15 | 14 | 60 | 60 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | False | park-day | 15 | 14 | 60 | 60 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | tirex2 | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | tirex2_grid | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | tirex2_x_h5 | False | park-day | 14 | 13 | 60 | 60 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | toto2 | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | toto2_nocov | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | toto2_x_h5 | False | park-day | 14 | 13 | 60 | 60 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | True | park-day | 15 | 14 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | chronos2_x_h5 | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | h5 | False | park-day | 15 | 14 | 45 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 | 0 | 30 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | False | park-day | 15 | 14 | 45 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | False | park-day | 14 | 13 | 1 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | tirex2_x_h5 | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | toto2_x_h5 | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | wt_med | True | park-day | 15 | 14 | 6 | 14 |  |
| first-hour MAE, schedule known at origin | D5 | all | chronos2_x_h5 | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | h5 | False | park-day | 15 | 14 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 | 0 | 30 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 14 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | False | park-day | 14 | 13 | 1 | 30 |  |
| first-hour MAE, schedule known at origin | D5 | all | tirex2_x_h5 | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | toto2_x_h5 | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | wt_med | True | park-day | 15 | 14 | 6 | 21 |  |
| first-hour MAE, schedule projected at origin | D5 | all | h5 | False | park-day | 15 | 14 |  | 3 |  |
| first-hour MAE, schedule projected at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 14 |  | 3 |  |
| first-hour MAE, schedule projected at origin | D5 | all | wt_med | True | park-day | 15 | 14 | 14 | 14 |  |
| live-window MAE (headliners) | D2 | headliners | chronos2 | False | park-day | 1 | 1 | w0-45 | w0-45 |  |
| live-window MAE (headliners) | D2 | headliners | persistence | True | park-day | 1 | 1 | w0-45 | w0-45 |  |
| live-window MAE (headliners) | D2 | headliners | tirex2 | False | park-day | 1 | 1 | w0-45 | w0-45 |  |
| live-window MAE (headliners) | D2 | headliners | toto2 | False | park-day | 1 | 1 | w0-45 | w0-45 |  |
| next-best precision | D1 | all | lvlh5_naive | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | chronos2 | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best recall | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| optimiser regret (min per park-day) | D4 | all | h5 | False | park-day | 5 | 5 |  | 30 |  |
| optimiser regret (min per park-day) | D4 | all | lvlh5_naive | False | park-day | 5 | 5 | 1 | 1 |  |
| optimiser regret (min per park-day) | D4 | all | toto2_x_h5 | False | park-day | 5 | 5 |  | 30 |  |
| optimiser regret (min per park-day) | D4 | all | wt_med | True | park-day | 5 | 5 |  | 7 |  |
| rope-drop worth agreement | D5 | all | wt_med | True | park-day | 15 | 14 |  | 60 |  |


**Never tested** — `leads_tested = 0` means no lead passed the gates, which is NOT "nothing won". The reason per metric × model:

| metric | uc | segment | model | unit | leads_scored | not_tested_because |
|---|---|---|---|---|---|---|
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| best-time hit rate (top-2) | D3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| best-time hit rate (±30 min) | D3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| best-time regret (min) | D3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| best-time regret (min) | D3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| best-time regret (min) | D3 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| best-time regret (min) | D3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak Spearman across rides | UC3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak abs error (min) | UC3 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| dayPeak pairwise ordering (headliners) | D4 | all | snaive7 | park-day | 7 | 0: coverage 0.27 < 0.30 against one of the candidates; 1: coverage 0.29 < 0.30 against one of the candidates; 2: coverage 0.28 < 0.30 against one of the candidates; 3: coverage 0.28 < 0.30 against one of the candidates; 4: coverage 0.27 < 0.30 against one of the candidates; 5: coverage 0.27 < 0.30 against one of the candidates |
| optimiser regret (min per park-day) | D4 | all | lvlh5_cbd | park-day | 5 | 1: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 7: LOW-N: 24 park-days < 30; 14: LOW-N: 23 park-days < 30; 30: LOW-N: 20 park-days < 30 |
| optimiser regret (min per park-day) | D4 | all | lvlh5_tft | park-day | 5 | 1: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 7: LOW-N: 26 park-days < 30; 14: LOW-N: 25 park-days < 30; 30: LOW-N: 18 park-days < 30 |
| optimiser regret (min per park-day) | D4 | all | prod_served | park-day | 4 | 3: LOW-N: 27 park-days < 30; 7: LOW-N: 26 park-days < 30; 14: LOW-N: 25 park-days < 30; 30: LOW-N: 20 park-days < 30 |
| optimiser regret (min per park-day) | D4 | all | prod_served_lin | park-day | 4 | 3: LOW-N: 27 park-days < 30; 7: LOW-N: 26 park-days < 30; 14: LOW-N: 25 park-days < 30; 30: LOW-N: 20 park-days < 30 |
| rope-drop worth agreement | D5 | all | lvlh5_cbd | park-day | 13 | 0: LOW-N: 26 park-days < 30; 1: LOW-N: 25 park-days < 30; 2: LOW-N: 25 park-days < 30; 3: LOW-N: 25 park-days < 30; 4: LOW-N: 25 park-days < 30; 5: LOW-N: 25 park-days < 30 |
| rope-drop worth agreement | D5 | all | lvlh5_tft | park-day | 14 | 0: LOW-N: 28 park-days < 30; 1: LOW-N: 27 park-days < 30; 2: LOW-N: 27 park-days < 30; 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30 |
| rope-drop worth agreement | D5 | all | prod_served | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |
| rope-drop worth agreement | D5 | all | prod_served_lin | park-day | 11 | 3: LOW-N: 27 park-days < 30; 4: LOW-N: 27 park-days < 30; 5: LOW-N: 27 park-days < 30; 6: LOW-N: 26 park-days < 30; 7: LOW-N: 26 park-days < 30; 10: LOW-N: 26 park-days < 30 |

### Reference choice per cell

Candidates (coverage ≥ 50 % of the best-covered), their own-coverage values, and why the reference is the one it is. `reason` says `ladder order` when no candidate significantly beats the ladder-first one — i.e. the choice was NOT made on the point estimates.

| metric | uc | segment | lead | candidates | selected | reason |
|---|---|---|---|---|---|---|
| MAE | UC2 | all | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2 | busy | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
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
| first-hour MAE (opening-aligned) | D5 | all | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
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
| MAE | UC2 | all | 0 | tirex2 | tirex2 | only candidate |
| MAE | UC2 | busy | 0 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 1 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 1 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 2 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 2 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 3 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 3 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 4 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 4 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 5 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 5 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 6 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 6 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | all | 7 | tirex2 | tirex2 | only candidate |
| MAE | UC3 | busy | 7 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m015 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m015 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m030 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m030 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m045 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m045 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m060 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m060 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m075 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m075 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m090 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m090 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m105 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m105 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | all | m120 | tirex2 | tirex2 | only candidate |
| MAE | UC1 | busy | m120 | tirex2 | tirex2 | only candidate |
| MAE | UC2-intraday | all | h2-4 | tirex2 | tirex2 | only candidate |
| MAE | UC2-intraday | busy | h2-4 | tirex2 | tirex2 | only candidate |
| MAE | UC2-intraday | all | h4-8 | tirex2 | tirex2 | only candidate |
| MAE | UC2-intraday | busy | h4-8 | tirex2 | tirex2 | only candidate |
| MAE | UC2-intraday | all | h8+ | tirex2 | tirex2 | only candidate |
| MAE | UC2-intraday | busy | h8+ | tirex2 | tirex2 | only candidate |
| MAE | UC2 | all | 0 | toto2 | toto2 | only candidate |
| MAE | UC2 | busy | 0 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 1 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 1 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 2 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 2 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 3 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 3 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 4 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 4 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 5 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 5 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 6 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 6 | toto2 | toto2 | only candidate |
| MAE | UC3 | all | 7 | toto2 | toto2 | only candidate |
| MAE | UC3 | busy | 7 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m015 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m015 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m030 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m030 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m045 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m045 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m060 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m060 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m075 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m075 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m090 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m090 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m105 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m105 | toto2 | toto2 | only candidate |
| MAE | UC1 | all | m120 | toto2 | toto2 | only candidate |
| MAE | UC1 | busy | m120 | toto2 | toto2 | only candidate |
| MAE | UC2-intraday | all | h2-4 | toto2 | toto2 | only candidate |
| MAE | UC2-intraday | busy | h2-4 | toto2 | toto2 | only candidate |
| MAE | UC2-intraday | all | h4-8 | toto2 | toto2 | only candidate |
| MAE | UC2-intraday | busy | h4-8 | toto2 | toto2 | only candidate |
| MAE | UC2-intraday | all | h8+ | toto2 | toto2 | only candidate |
| MAE | UC2-intraday | busy | h8+ | toto2 | toto2 | only candidate |
| MAE | UC2 | all | 0 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC2 | busy | 0 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 1 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 1 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 2 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 2 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 3 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 3 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 4 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 4 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 5 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 5 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 6 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 6 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | all | 7 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC3 | busy | 7 | toto2_nocov | toto2_nocov | only candidate |
| MAE | UC2 | all | 0 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC2 | busy | 0 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 1 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 1 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 2 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 2 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 3 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 3 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 4 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 4 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 5 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 5 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 6 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 6 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | all | 7 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC3 | busy | 7 | tirex2_grid | tirex2_grid | only candidate |
| MAE | UC2 | all | 0 | toto2_grid | toto2_grid | only candidate |
| MAE | UC2 | busy | 0 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 1 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 1 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 2 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 2 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 3 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 3 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 4 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 4 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 5 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 5 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 6 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 6 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | all | 7 | toto2_grid | toto2_grid | only candidate |
| MAE | UC3 | busy | 7 | toto2_grid | toto2_grid | only candidate |
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
| MAE | UC2 | all | 0 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC2 | busy | 0 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 1 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 1 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 2 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 2 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 3 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 3 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 4 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 4 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 5 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 5 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 6 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 6 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | all | 7 | chronos2_nocov | chronos2_nocov | only candidate |
| MAE | UC3 | busy | 7 | chronos2_nocov | chronos2_nocov | only candidate |
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
| UC2 | all | 0 | wt_med | chronos2 | 6.550 | -0.579 | -0.762 | -0.395 | -0.749 | -0.422 | True | 1.000 | 3 | 14 | 47 |
| UC2 | busy | 0 | wt_med | chronos2 | 12.918 | -1.524 | -1.935 | -1.160 | -1.970 | -1.130 | True | 1.000 | 6 | 14 | 47 |
| UC3 | all | 1 | wt_med | chronos2_x_h5 | 6.757 | -0.176 | -0.325 | -0.034 | -0.294 | -0.072 | True | 0.996 | 3 | 17 | 46 |
| UC3 | busy | 1 | wt_med | chronos2_x_h5 | 13.070 | -0.928 | -1.284 | -0.626 | -1.204 | -0.661 | True | 0.998 | 7 | 17 | 46 |
| UC3 | all | 2 | wt_med | chronos2_x_h5 | 7.091 | -0.174 | -0.344 | -0.033 | -0.279 | -0.075 | True | 0.995 | 2 | 17 | 46 |
| UC3 | busy | 2 | wt_med | chronos2_x_h5 | 13.653 | -0.919 | -1.385 | -0.574 | -1.192 | -0.644 | True | 0.998 | 5 | 17 | 46 |
| UC3 | all | 3 | wt_med | h5 | 7.222 | -0.069 | -0.112 | -0.041 | -0.086 | -0.053 | True | 0.996 | 1 | 19 | 46 |
| UC3 | busy | 3 | wt_med | chronos2_x_h5 | 13.972 | -0.701 | -1.145 | -0.360 | -0.966 | -0.473 | True | 0.998 | 4 | 19 | 46 |
| UC3 | all | 4 | wt_med | h5 | 7.257 | -0.049 | -0.075 | -0.027 | -0.065 | -0.034 | True | 0.995 | 1 | 19 | 46 |
| UC3 | busy | 4 | wt_med | chronos2_x_h5 | 14.016 | -0.480 | -0.889 | -0.193 | -0.738 | -0.201 | True | 0.997 | 3 | 19 | 46 |
| UC3 | all | 5 | wt_med | chronos2_x_h5 | 7.235 | -0.100 | -0.232 | -0.001 | -0.210 | 0.010 | False | 0.993 | 2 | 19 | 46 |
| UC3 | busy | 5 | wt_med | chronos2_x_h5 | 14.057 | -0.673 | -0.983 | -0.478 | -0.953 | -0.415 | True | 0.996 | 4 | 19 | 46 |
| UC3 | all | 6 | wt_med | h5 | 7.023 | -0.057 | -0.090 | -0.033 | -0.073 | -0.043 | True | 0.990 | 1 | 19 | 45 |
| UC3 | busy | 6 | wt_med | chronos2_x_h5 | 13.483 | -0.804 | -1.252 | -0.462 | -1.058 | -0.566 | True | 0.993 | 5 | 19 | 45 |
| UC3 | all | 7 | wt_med | h5 | 7.456 | -0.070 | -0.100 | -0.046 | -0.087 | -0.054 | True | 0.994 | 1 | 18 | 45 |
| UC3 | busy | 7 | wt_med | chronos2_x_h5 | 14.352 | -0.720 | -1.172 | -0.425 | -0.985 | -0.465 | True | 0.996 | 3 | 18 | 45 |
| UC3 | all | 10 | wt_med | h5 | 7.450 | -0.075 | -0.110 | -0.047 | -0.092 | -0.059 | True | 0.994 | 1 | 11 | 45 |
| UC3 | busy | 10 | wt_med | tirex2_x_h5 | 14.644 | -0.453 | -0.710 | -0.254 | -0.677 | -0.220 | True | 0.997 | 3 | 11 | 45 |
| UC3 | all | 14 | wt_med | h5 | 7.658 | -0.071 | -0.100 | -0.049 | -0.087 | -0.057 | True | 0.993 | 1 | 11 | 44 |
| UC3 | busy | 14 | wt_med | chronos2_x_h5 | 14.824 | -0.431 | -0.850 | -0.155 | -0.672 | -0.178 | True | 0.995 | 3 | 11 | 44 |
| UC3 | all | 21 | wt_med | h5 | 7.574 | -0.079 | -0.117 | -0.052 | -0.095 | -0.064 | True | 0.992 | 1 | 11 | 42 |
| UC3 | busy | 21 | wt_med | chronos2_x_h5 | 14.938 | -0.414 | -0.878 | -0.060 | -0.672 | -0.156 | True | 0.994 | 3 | 11 | 42 |
| UC3 | all | 30 | wt_med | h5 | 7.797 | -0.087 | -0.122 | -0.060 | -0.105 | -0.071 | True | 0.990 | 1 | 11 | 41 |
| UC3 | busy | 30 | wt_med | chronos2_x_h5 | 15.285 | -0.309 | -0.770 | -0.004 | -0.531 | -0.070 | True | 0.994 | 2 | 11 | 41 |
| UC3 | all | 45 | wt_med | h5 | 7.930 | -0.084 | -0.114 | -0.058 | -0.099 | -0.069 | True | 0.989 | 1 | 10 | 38 |
| UC3 | busy | 45 | wt_med | h5 | 15.625 | -0.208 | -0.264 | -0.161 | -0.244 | -0.174 | True | 0.992 | 1 | 10 | 38 |
| UC3 | all | 60 | wt_med | h5 | 8.074 | -0.089 | -0.121 | -0.063 | -0.105 | -0.074 | True | 0.988 | 1 | 7 | 35 |
| UC3 | busy | 60 | wt_med | h5 | 15.812 | -0.210 | -0.275 | -0.159 | -0.246 | -0.174 | True | 0.991 | 1 | 7 | 35 |
| UC3 | all | 90 | wt_med | wt_med | 8.560 | 0.000 |  |  |  |  | True |  | 0 | 0 | 29 |
| UC3 | busy | 90 | wt_med | wt_med | 17.001 | 0.000 |  |  |  |  | True |  | 0 | 0 | 29 |
| UC2 | all | 1 | wt_med | chronos2_x_h5 | 6.757 | -0.176 | -0.325 | -0.034 | -0.294 | -0.072 | True | 0.996 | 3 | 17 | 46 |
| UC2 | busy | 1 | wt_med | chronos2_x_h5 | 13.070 | -0.928 | -1.284 | -0.626 | -1.204 | -0.661 | True | 0.998 | 7 | 17 | 46 |
| UC1 | all | m015 | persistence | persistence | 2.721 | 0.000 |  |  |  |  | False |  | 0 | 10 | 47 |
| UC1 | busy | m015 | persistence | persistence | 5.345 | 0.000 |  |  |  |  | False |  | 0 | 10 | 47 |
| UC1 | all | m030 | persistence | toto2 | 4.010 | -0.503 | -0.684 | -0.302 | -0.548 | -0.460 | True | 0.998 | 3 | 10 | 47 |
| UC1 | busy | m030 | persistence | toto2 | 7.986 | -1.037 | -1.384 | -0.578 | -1.135 | -0.939 | True | 0.998 | 3 | 10 | 47 |
| UC1 | all | m045 | persistence | toto2 | 4.483 | -0.931 | -1.207 | -0.639 | -0.994 | -0.869 | True | 0.997 | 3 | 10 | 47 |
| UC1 | busy | m045 | persistence | toto2 | 8.847 | -1.903 | -2.325 | -1.352 | -2.035 | -1.765 | True | 0.998 | 3 | 10 | 47 |
| UC1 | all | m060 | persistence | chronos2 | 4.630 | -1.271 | -1.619 | -0.906 | -1.350 | -1.197 | True | 0.997 | 3 | 10 | 47 |
| UC1 | busy | m060 | persistence | chronos2 | 9.055 | -2.634 | -3.163 | -2.034 | -2.801 | -2.489 | True | 0.998 | 3 | 10 | 47 |
| UC1 | all | m075 | persistence | chronos2 | 4.895 | -1.545 | -1.946 | -1.127 | -1.638 | -1.455 | True | 0.996 | 3 | 10 | 47 |
| UC1 | busy | m075 | persistence | chronos2 | 9.557 | -3.081 | -3.665 | -2.450 | -3.268 | -2.894 | True | 0.995 | 3 | 10 | 47 |
| UC1 | all | m090 | persistence | chronos2 | 5.035 | -1.876 | -2.357 | -1.392 | -1.981 | -1.770 | True | 0.996 | 3 | 10 | 47 |
| UC1 | busy | m090 | persistence | chronos2 | 9.859 | -3.749 | -4.457 | -2.968 | -3.966 | -3.547 | True | 0.995 | 3 | 10 | 47 |
| UC1 | all | m105 | persistence | chronos2 | 5.142 | -2.063 | -2.632 | -1.545 | -2.176 | -1.948 | True | 0.996 | 3 | 10 | 47 |
| UC1 | busy | m105 | persistence | chronos2 | 10.039 | -4.078 | -4.965 | -3.223 | -4.309 | -3.843 | True | 0.995 | 3 | 10 | 47 |
| UC1 | all | m120 | persistence | chronos2 | 5.241 | -2.246 | -2.835 | -1.706 | -2.369 | -2.121 | True | 0.996 | 3 | 10 | 47 |
| UC1 | busy | m120 | persistence | chronos2 | 10.176 | -4.475 | -5.459 | -3.544 | -4.733 | -4.216 | True | 0.995 | 3 | 10 | 47 |
| UC2-intraday | all | h2-4 | wt_med | chronos2 | 5.575 | -1.620 | -1.976 | -1.241 | -1.773 | -1.475 | True | 0.993 | 4 | 10 | 47 |
| UC2-intraday | busy | h2-4 | persistence | chronos2 | 10.832 | -5.807 | -6.895 | -4.699 | -6.113 | -5.536 | True | 0.993 | 4 | 10 | 47 |
| UC2-intraday | all | h4-8 | wt_med | chronos2 | 6.134 | -1.159 | -1.420 | -0.884 | -1.310 | -1.016 | True | 0.988 | 4 | 10 | 47 |
| UC2-intraday | busy | h4-8 | wt_med | chronos2 | 11.748 | -2.746 | -3.310 | -2.191 | -3.135 | -2.391 | True | 0.989 | 4 | 10 | 47 |
| UC2-intraday | all | h8+ | wt_med | chronos2 | 6.296 | -0.529 | -0.695 | -0.381 | -0.673 | -0.386 | True | 1.000 | 3 | 9 | 47 |
| UC2-intraday | busy | h8+ | wt_med | chronos2 | 11.785 | -1.273 | -1.684 | -0.962 | -1.610 | -0.950 | True | 1.000 | 3 | 10 | 47 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | lvlh5_tft | 4.257 | -0.174 | -0.418 | -0.025 | -0.272 | -0.080 | True | 0.627 | 4 | 14 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | lvlh5_tft | 4.105 | -0.215 | -0.570 | -0.011 | -0.323 | -0.128 | True | 0.624 | 6 | 17 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | lvlh5_naive | 4.558 | -0.162 | -0.393 | -0.008 | -0.236 | -0.093 | True | 0.998 | 5 | 17 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | lvlh5_cbd | 4.362 | -0.269 | -0.702 | -0.009 | -0.428 | -0.130 | True | 0.511 | 7 | 19 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | lvlh5_tft | 4.366 | -0.204 | -0.501 | -0.013 | -0.311 | -0.113 | True | 0.613 | 7 | 19 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | lvlh5_tft | 4.331 | -0.198 | -0.483 | -0.024 | -0.307 | -0.097 | True | 0.611 | 7 | 19 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | lvlh5_tft | 4.220 | -0.227 | -0.579 | -0.018 | -0.355 | -0.126 | True | 0.605 | 7 | 19 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | lvlh5_cbd | 4.423 | -0.228 | -0.558 | -0.014 | -0.366 | -0.108 | True | 0.497 | 7 | 18 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | lvlh5_cbd | 4.537 | -0.230 | -0.546 | -0.021 | -0.368 | -0.114 | True | 0.489 | 7 | 11 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | lvlh5_tft | 4.590 | -0.248 | -0.570 | -0.031 | -0.371 | -0.143 | True | 0.597 | 7 | 11 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | toto2_x_h5 | 4.840 | -0.176 | -0.360 | -0.038 | -0.244 | -0.114 | True | 0.996 | 6 | 11 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | lvlh5_tft | 4.887 | -0.190 | -0.431 | -0.034 | -0.322 | -0.081 | True | 0.451 | 7 | 11 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | lvlh5_tft | 5.124 | -0.179 | -0.407 | -0.022 | -0.301 | -0.075 | True | 0.350 | 6 | 10 |
| first-hour MAE (opening-aligned) | D5 | 60 | wt_med | wt_med | 5.279 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| first-hour MAE (opening-aligned) | D5 | 90 | wt_med | wt_med | 5.533 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | chronos2 | 6.664 | -0.680 | -0.899 | -0.475 | -0.884 | -0.490 | True | 1.000 | 3 | 14 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | chronos2 | 6.796 | -0.213 | -0.424 | -0.043 | -0.389 | -0.043 | True | 1.000 | 2 | 17 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | chronos2_x_h5 | 7.236 | -0.211 | -0.431 | -0.029 | -0.343 | -0.082 | True | 0.995 | 3 | 17 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 7.368 | -0.077 | -0.128 | -0.041 | -0.100 | -0.057 | True | 0.996 | 1 | 19 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | h5 | 7.290 | -0.062 | -0.098 | -0.034 | -0.082 | -0.045 | True | 0.995 | 1 | 19 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | h5 | 7.474 | -0.067 | -0.106 | -0.035 | -0.088 | -0.047 | True | 0.994 | 1 | 19 |
| MAE, schedule known at origin | UC3 | 6 | wt_med | h5 | 7.040 | -0.067 | -0.110 | -0.035 | -0.085 | -0.049 | True | 0.990 | 1 | 19 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | h5 | 7.606 | -0.082 | -0.121 | -0.052 | -0.104 | -0.062 | True | 0.995 | 1 | 18 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | h5 | 7.587 | -0.091 | -0.138 | -0.054 | -0.113 | -0.071 | True | 0.993 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 14 | wt_med | h5 | 7.589 | -0.093 | -0.132 | -0.062 | -0.113 | -0.075 | True | 0.992 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 21 | wt_med | h5 | 7.324 | -0.105 | -0.157 | -0.064 | -0.125 | -0.086 | True | 0.991 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 30 | wt_med | h5 | 7.638 | -0.117 | -0.172 | -0.073 | -0.143 | -0.093 | True | 0.989 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 45 | wt_med | h5 | 7.455 | -0.105 | -0.156 | -0.067 | -0.128 | -0.083 | True | 0.988 | 1 | 10 |
| MAE, schedule known at origin | UC3 | 60 | wt_med | h5 | 7.439 | -0.113 | -0.168 | -0.074 | -0.137 | -0.091 | True | 0.987 | 1 | 7 |
| MAE, schedule known at origin | UC3 | 90 | wt_med | wt_med | 7.063 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | lvlh5_tft | 4.470 | -0.217 | -0.530 | -0.030 | -0.338 | -0.103 | True | 0.637 | 4 | 14 |
| first-hour MAE, schedule known at origin | D5 | 1 | wt_med | lvlh5_tft | 4.299 | -0.275 | -0.720 | -0.018 | -0.404 | -0.166 | True | 0.632 | 6 | 17 |
| first-hour MAE, schedule known at origin | D5 | 2 | wt_med | chronos2_x_h5 | 4.752 | -0.201 | -0.497 | -0.011 | -0.291 | -0.118 | True | 0.998 | 5 | 17 |
| first-hour MAE, schedule known at origin | D5 | 3 | wt_med | lvlh5_tft | 4.537 | -0.294 | -0.782 | -0.007 | -0.444 | -0.163 | True | 0.623 | 6 | 19 |
| first-hour MAE, schedule known at origin | D5 | 4 | wt_med | lvlh5_tft | 4.592 | -0.261 | -0.642 | -0.023 | -0.397 | -0.149 | True | 0.622 | 7 | 19 |
| first-hour MAE, schedule known at origin | D5 | 5 | wt_med | lvlh5_cbd | 4.629 | -0.253 | -0.689 | -0.016 | -0.415 | -0.117 | True | 0.509 | 7 | 19 |
| first-hour MAE, schedule known at origin | D5 | 6 | wt_med | lvlh5_tft | 4.395 | -0.299 | -0.763 | -0.024 | -0.463 | -0.170 | True | 0.610 | 7 | 19 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | lvlh5_tft | 4.649 | -0.281 | -0.716 | -0.004 | -0.432 | -0.149 | True | 0.604 | 7 | 18 |
| first-hour MAE, schedule known at origin | D5 | 10 | wt_med | lvlh5_cbd | 4.789 | -0.304 | -0.769 | -0.023 | -0.480 | -0.160 | True | 0.489 | 7 | 11 |
| first-hour MAE, schedule known at origin | D5 | 14 | wt_med | lvlh5_tft | 4.648 | -0.336 | -0.794 | -0.041 | -0.506 | -0.199 | True | 0.599 | 7 | 11 |
| first-hour MAE, schedule known at origin | D5 | 21 | wt_med | chronos2_x_h5 | 4.760 | -0.235 | -0.509 | -0.047 | -0.325 | -0.153 | True | 0.995 | 6 | 11 |
| first-hour MAE, schedule known at origin | D5 | 30 | wt_med | lvlh5_tft | 4.868 | -0.289 | -0.704 | -0.046 | -0.478 | -0.129 | True | 0.464 | 7 | 11 |
| first-hour MAE, schedule known at origin | D5 | 45 | wt_med | chronos2_x_h5 | 4.702 | -0.153 | -0.364 | -0.014 | -0.232 | -0.078 | True | 0.997 | 5 | 10 |
| first-hour MAE, schedule known at origin | D5 | 60 | wt_med | wt_med | 4.811 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| first-hour MAE, schedule known at origin | D5 | 90 | wt_med | wt_med | 4.643 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 0 | wt_med | wt_med | 6.454 | 0.000 |  |  |  |  | False |  | 0 | 14 |
| MAE, schedule projected at origin | UC3 | 1 | wt_med | wt_med | 6.754 | 0.000 |  |  |  |  | False |  | 0 | 17 |
| MAE, schedule projected at origin | UC3 | 2 | wt_med | h5 | 6.642 | -0.019 | -0.038 | -0.005 | -0.035 | -0.003 | True | 0.995 | 1 | 17 |
| MAE, schedule projected at origin | UC3 | 3 | wt_med | h5 | 6.767 | -0.044 | -0.067 | -0.026 | -0.062 | -0.027 | True | 0.996 | 1 | 19 |
| MAE, schedule projected at origin | UC3 | 4 | wt_med | wt_med | 7.168 | 0.000 |  |  |  |  | False |  | 0 | 19 |
| MAE, schedule projected at origin | UC3 | 5 | wt_med | wt_med | 6.677 | 0.000 |  |  |  |  | False |  | 0 | 19 |
| MAE, schedule projected at origin | UC3 | 6 | wt_med | h5 | 6.972 | -0.028 | -0.054 | -0.002 | -0.045 | -0.014 | True | 0.992 | 1 | 19 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | h5 | 6.990 | -0.033 | -0.062 | -0.016 | -0.050 | -0.015 | True | 0.992 | 1 | 18 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | wt_med | 7.083 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | wt_med | 7.856 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule projected at origin | UC3 | 21 | wt_med | wt_med | 8.142 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule projected at origin | UC3 | 30 | wt_med | h5 | 8.050 | -0.042 | -0.071 | -0.017 | -0.062 | -0.022 | True | 0.992 | 1 | 11 |
| MAE, schedule projected at origin | UC3 | 45 | wt_med | wt_med | 8.643 | 0.000 |  |  |  |  | False |  | 0 | 10 |
| MAE, schedule projected at origin | UC3 | 60 | wt_med | wt_med | 8.772 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| MAE, schedule projected at origin | UC3 | 90 | wt_med | wt_med | 9.488 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 0 | wt_med | wt_med | 3.842 | 0.000 |  |  |  |  | False |  | 0 | 14 |
| first-hour MAE, schedule projected at origin | D5 | 1 | wt_med | wt_med | 3.891 | 0.000 |  |  |  |  | False |  | 0 | 17 |
| first-hour MAE, schedule projected at origin | D5 | 2 | wt_med | wt_med | 3.840 | 0.000 |  |  |  |  | False |  | 0 | 17 |
| first-hour MAE, schedule projected at origin | D5 | 3 | wt_med | lvlh5_naive | 3.931 | -0.067 | -0.227 | -0.014 | -0.147 | -0.005 | True | 0.997 | 2 | 19 |
| first-hour MAE, schedule projected at origin | D5 | 4 | wt_med | wt_med | 4.412 | 0.000 |  |  |  |  | False |  | 0 | 19 |
| first-hour MAE, schedule projected at origin | D5 | 5 | wt_med | wt_med | 3.948 | 0.000 |  |  |  |  | False |  | 0 | 19 |
| first-hour MAE, schedule projected at origin | D5 | 6 | wt_med | wt_med | 4.102 | 0.000 |  |  |  |  | False |  | 0 | 19 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | wt_med | 4.104 | 0.000 |  |  |  |  | False |  | 0 | 18 |
| first-hour MAE, schedule projected at origin | D5 | 10 | wt_med | wt_med | 4.174 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 14 | wt_med | wt_med | 4.917 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 21 | wt_med | wt_med | 5.046 | 0.000 |  |  |  |  | True |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 30 | wt_med | wt_med | 5.063 | 0.000 |  |  |  |  | True |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 45 | wt_med | wt_med | 5.709 | 0.000 |  |  |  |  | False |  | 0 | 10 |
| first-hour MAE, schedule projected at origin | D5 | 60 | wt_med | wt_med | 5.809 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| first-hour MAE, schedule projected at origin | D5 | 90 | wt_med | wt_med | 6.233 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | toto2 | 6.911 | -0.889 | -1.136 | -0.619 | -0.954 | -0.820 | True | 0.998 | 3 | 10 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | wt_med | 0.825 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| best-time hit rate (top-2) | D3 | 2 | wt_med | wt_med | 0.818 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| best-time hit rate (top-2) | D3 | 3 | wt_med | wt_med | 0.815 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| best-time hit rate (top-2) | D3 | 4 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| best-time hit rate (top-2) | D3 | 5 | wt_med | wt_med | 0.824 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | wt_med | 0.825 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| best-time hit rate (top-2) | D3 | 7 | wt_med | wt_med | 0.820 | 0.000 |  |  |  |  | False |  | 0 | 14 |
| best-time hit rate (top-2) | D3 | 10 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 14 | wt_med | wt_med | 0.825 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 21 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 30 | wt_med | wt_med | 0.818 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 45 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 60 | wt_med | wt_med | 0.821 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 90 | wt_med | wt_med | 0.821 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 0 | wt_med | h5 | 0.834 | 0.010 | 0.003 | 0.018 | 0.006 | 0.014 | True | 0.996 | 1 | 12 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | h5 | 0.831 | 0.008 | 0.002 | 0.014 | 0.004 | 0.011 | True | 0.991 | 1 | 15 |
| best-time hit rate (±30 min) | D3 | 2 | wt_med | h5 | 0.828 | 0.008 | 0.004 | 0.013 | 0.005 | 0.012 | True | 0.995 | 1 | 15 |
| best-time hit rate (±30 min) | D3 | 3 | wt_med | h5 | 0.825 | 0.007 | 0.002 | 0.013 | 0.003 | 0.011 | True | 0.993 | 1 | 15 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.827 | 0.008 | 0.004 | 0.013 | 0.004 | 0.012 | True | 0.993 | 1 | 15 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | h5 | 0.834 | 0.011 | 0.005 | 0.018 | 0.007 | 0.015 | True | 0.995 | 1 | 15 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | wt_med | 0.825 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.827 | 0.008 | 0.003 | 0.014 | 0.004 | 0.012 | True | 0.992 | 1 | 14 |
| best-time hit rate (±30 min) | D3 | 10 | wt_med | h5 | 0.831 | 0.009 | 0.003 | 0.018 | 0.005 | 0.013 | True | 0.992 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 14 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| best-time hit rate (±30 min) | D3 | 21 | wt_med | h5 | 0.829 | 0.008 | 0.002 | 0.014 | 0.003 | 0.012 | True | 0.986 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 30 | wt_med | h5 | 0.827 | 0.007 | 0.002 | 0.014 | 0.002 | 0.011 | True | 0.985 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 45 | wt_med | wt_med | 0.822 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time hit rate (±30 min) | D3 | 60 | wt_med | wt_med | 0.824 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| best-time hit rate (±30 min) | D3 | 90 | wt_med | wt_med | 0.823 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.986 | -0.331 | -0.520 | -0.175 | -0.437 | -0.219 | True | 0.996 | 1 | 12 |
| best-time regret (min) | D3 | 1 | wt_med | h5 | 3.000 | -0.265 | -0.466 | -0.100 | -0.369 | -0.166 | True | 0.991 | 1 | 15 |
| best-time regret (min) | D3 | 2 | wt_med | h5 | 2.978 | -0.285 | -0.493 | -0.138 | -0.381 | -0.188 | True | 0.995 | 1 | 15 |
| best-time regret (min) | D3 | 3 | wt_med | h5 | 3.195 | -0.255 | -0.474 | -0.079 | -0.366 | -0.155 | True | 0.993 | 1 | 15 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 3.122 | -0.256 | -0.411 | -0.116 | -0.373 | -0.142 | True | 0.993 | 1 | 15 |
| best-time regret (min) | D3 | 5 | wt_med | h5 | 3.004 | -0.319 | -0.513 | -0.155 | -0.426 | -0.204 | True | 0.995 | 1 | 15 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 3.015 | -0.263 | -0.434 | -0.101 | -0.362 | -0.165 | True | 0.981 | 1 | 15 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 2.994 | -0.304 | -0.538 | -0.130 | -0.393 | -0.211 | True | 0.992 | 1 | 14 |
| best-time regret (min) | D3 | 10 | wt_med | h5 | 3.082 | -0.283 | -0.448 | -0.143 | -0.383 | -0.174 | True | 0.992 | 1 | 7 |
| best-time regret (min) | D3 | 14 | wt_med | wt_med | 3.376 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| best-time regret (min) | D3 | 21 | wt_med | h5 | 3.082 | -0.275 | -0.426 | -0.130 | -0.381 | -0.177 | True | 0.986 | 1 | 7 |
| best-time regret (min) | D3 | 30 | wt_med | h5 | 3.104 | -0.352 | -0.546 | -0.191 | -0.481 | -0.237 | True | 0.985 | 1 | 7 |
| best-time regret (min) | D3 | 45 | wt_med | wt_med | 3.197 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time regret (min) | D3 | 60 | wt_med | wt_med | 3.233 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time regret (min) | D3 | 90 | wt_med | wt_med | 3.280 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | h5 | 0.506 | 0.012 | 0.005 | 0.020 | 0.009 | 0.015 | True | 0.963 | 1 | 12 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | h5 | 0.502 | 0.011 | 0.002 | 0.019 | 0.008 | 0.014 | True | 0.920 | 1 | 15 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | h5 | 0.500 | 0.012 | 0.004 | 0.020 | 0.010 | 0.015 | True | 0.964 | 1 | 15 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.493 | 0.012 | 0.004 | 0.019 | 0.009 | 0.015 | True | 0.963 | 1 | 15 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | h5 | 0.497 | 0.011 | 0.001 | 0.021 | 0.008 | 0.015 | True | 0.961 | 1 | 15 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | h5 | 0.501 | 0.014 | 0.007 | 0.022 | 0.011 | 0.017 | True | 0.962 | 1 | 15 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | h5 | 0.499 | 0.013 | 0.005 | 0.020 | 0.009 | 0.016 | True | 0.907 | 1 | 15 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.493 | 0.014 | 0.005 | 0.022 | 0.011 | 0.017 | True | 0.960 | 1 | 14 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.490 | 0.015 | 0.008 | 0.023 | 0.012 | 0.018 | True | 0.958 | 2 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.482 | 0.017 | 0.008 | 0.026 | 0.013 | 0.020 | True | 0.955 | 1 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | wt_med | h5 | 0.477 | 0.015 | 0.005 | 0.024 | 0.010 | 0.019 | True | 0.951 | 1 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | wt_med | h5 | 0.467 | 0.016 | 0.006 | 0.025 | 0.011 | 0.020 | True | 0.950 | 1 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | wt_med | h5 | 0.458 | 0.015 | 0.005 | 0.025 | 0.010 | 0.020 | True | 0.946 | 1 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | wt_med | h5 | 0.457 | 0.018 | 0.011 | 0.028 | 0.014 | 0.022 | True | 0.952 | 1 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | wt_med | wt_med | 0.432 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | tirex2_grid | 7.931 | -0.386 | -0.669 | -0.129 | -0.621 | -0.163 | True | 0.999 | 2 | 12 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | chronos2_x_h5 | 7.538 | -0.411 | -0.632 | -0.195 | -0.587 | -0.244 | True | 0.992 | 3 | 15 |
| dayPeak abs error (min) | UC3 | 2 | wt_med | chronos2_x_h5 | 7.938 | -0.331 | -0.637 | -0.049 | -0.507 | -0.172 | True | 0.994 | 2 | 15 |
| dayPeak abs error (min) | UC3 | 3 | wt_med | wt_med | 8.058 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| dayPeak abs error (min) | UC3 | 4 | snaive7 | snaive7 | 8.154 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| dayPeak abs error (min) | UC3 | 5 | snaive7 | tirex2_x_h5 | 8.113 | -0.608 | -1.016 | -0.208 | -0.972 | -0.259 | True | 0.990 | 2 | 15 |
| dayPeak abs error (min) | UC3 | 6 | wt_med | chronos2_x_h5 | 7.811 | -0.340 | -0.592 | -0.090 | -0.505 | -0.191 | True | 0.982 | 2 | 15 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | chronos2_x_h5 | 8.235 | -0.293 | -0.595 | -0.024 | -0.464 | -0.143 | True | 0.992 | 2 | 14 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 8.514 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 14 | wt_med | wt_med | 8.716 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 21 | wt_med | wt_med | 8.447 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 30 | wt_med | wt_med | 8.686 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 45 | wt_med | wt_med | 8.509 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 60 | wt_med | wt_med | 8.527 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 90 | wt_med | wt_med | 8.544 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 0 | snaive7 | snaive7 | 0.920 | 0.000 |  |  |  |  | True |  | 0 | 12 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| rope-drop worth agreement | D5 | 2 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| rope-drop worth agreement | D5 | 3 | wt_med | wt_med | 0.917 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| rope-drop worth agreement | D5 | 4 | wt_med | wt_med | 0.912 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| rope-drop worth agreement | D5 | 5 | snaive7 | snaive7 | 0.921 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| rope-drop worth agreement | D5 | 6 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| rope-drop worth agreement | D5 | 7 | wt_med | wt_med | 0.915 | 0.000 |  |  |  |  | True |  | 0 | 14 |
| rope-drop worth agreement | D5 | 10 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| rope-drop worth agreement | D5 | 14 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| rope-drop worth agreement | D5 | 21 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| rope-drop worth agreement | D5 | 30 | wt_med | wt_med | 0.915 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| rope-drop worth agreement | D5 | 45 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| rope-drop worth agreement | D5 | 90 | wt_med | wt_med | 0.919 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | chronos2 | 0.831 | 0.061 | 0.047 | 0.078 | 0.054 | 0.068 | True | 1.000 | 8 | 11 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | chronos2 | 0.823 | 0.053 | 0.040 | 0.070 | 0.046 | 0.060 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | chronos2 | 0.812 | 0.054 | 0.038 | 0.073 | 0.047 | 0.061 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | tirex2 | 0.810 | 0.050 | 0.038 | 0.065 | 0.043 | 0.057 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | chronos2_nocov | 0.809 | 0.047 | 0.035 | 0.064 | 0.040 | 0.054 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | chronos2_nocov | 0.811 | 0.049 | 0.036 | 0.064 | 0.042 | 0.056 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | tirex2 | 0.810 | 0.046 | 0.035 | 0.060 | 0.040 | 0.053 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | tirex2 | 0.800 | 0.046 | 0.033 | 0.062 | 0.039 | 0.053 | True | 1.000 | 11 | 14 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | chronos2_x_h5 | 0.802 | 0.046 | 0.034 | 0.060 | 0.039 | 0.053 | True | 0.989 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | chronos2_x_h5 | 0.800 | 0.048 | 0.034 | 0.065 | 0.041 | 0.054 | True | 0.988 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | chronos2_x_h5 | 0.793 | 0.044 | 0.028 | 0.063 | 0.037 | 0.052 | True | 0.983 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | wt_med | chronos2_x_h5 | 0.776 | 0.035 | 0.023 | 0.049 | 0.028 | 0.042 | True | 0.985 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | wt_med | chronos2_x_h5 | 0.768 | 0.038 | 0.026 | 0.053 | 0.030 | 0.046 | True | 0.981 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | wt_med | chronos2_x_h5 | 0.766 | 0.039 | 0.021 | 0.058 | 0.030 | 0.047 | True | 0.991 | 5 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | wt_med | wt_med | 0.743 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 0 | snaive7 | snaive7 | 0.846 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| dayPeak Spearman across rides | UC3 | 1 | wt_med | wt_med | 0.848 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| dayPeak Spearman across rides | UC3 | 2 | wt_med | wt_med | 0.843 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| dayPeak Spearman across rides | UC3 | 3 | wt_med | wt_med | 0.851 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| dayPeak Spearman across rides | UC3 | 4 | snaive7 | snaive7 | 0.840 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| dayPeak Spearman across rides | UC3 | 5 | snaive7 | snaive7 | 0.846 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| dayPeak Spearman across rides | UC3 | 6 | snaive7 | snaive7 | 0.834 | 0.000 |  |  |  |  | True |  | 0 | 15 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.835 | 0.000 |  |  |  |  | False |  | 0 | 14 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.842 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.831 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 21 | wt_med | wt_med | 0.830 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 30 | wt_med | wt_med | 0.827 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 45 | wt_med | wt_med | 0.824 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 60 | wt_med | wt_med | 0.818 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 90 | wt_med | wt_med | 0.800 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | lvlh5_naive | 37.039 | -1.668 | -3.754 | -0.007 | -3.287 | 0.019 | False | 0.956 | 1 | 15 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | wt_med | 38.164 | 0.000 |  |  |  |  | False |  | 0 | 15 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | wt_med | 36.440 | 0.000 |  |  |  |  | True |  | 0 | 14 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | wt_med | 39.460 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| optimiser regret (min per park-day) | D4 | 30 | wt_med | h5 | 37.278 | -2.268 | -4.319 | -0.148 | -3.930 | -0.565 | True | 0.983 | 2 | 7 |
| next-best precision | D1 | 0-120 | snaive7 | lvlh5_naive | 0.561 | 0.046 | 0.014 | 0.073 | 0.032 | 0.060 | True | 0.976 | 1 | 9 |
| next-best precision | D1 | 0-60 | snaive7 | lvlh5_naive | 0.469 | 0.039 | 0.008 | 0.067 | 0.026 | 0.053 | True | 0.974 | 1 | 9 |
| next-best precision (top-3) | D1 | 0-120 | snaive7 | chronos2 | 0.717 | 0.105 | 0.024 | 0.175 | 0.084 | 0.125 | True | 0.920 | 1 | 9 |
| next-best precision (top-3) | D1 | 0-60 | snaive7 | chronos2 | 0.640 | 0.121 | 0.031 | 0.198 | 0.097 | 0.143 | True | 0.887 | 1 | 9 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.580 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.534 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_chronos2 | 0.472 | 0.061 | 0.026 | 0.099 | 0.025 | 0.098 | False | 1.000 | 1 | 9 |
| crowd bucket exact | D6 | 2 | lvl_snaive7 | lvl_snaive7 | 0.405 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| crowd bucket exact | D6 | 3 | lvl_snaive7 | lvl_snaive7 | 0.414 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.397 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.419 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.408 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| crowd bucket exact | D6 | 7 | lvl_naive4 | lvl_tirex2 | 0.401 | 0.055 | 0.018 | 0.093 | 0.022 | 0.085 | True | 1.000 | 2 | 8 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_naive4 | 0.380 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_tirex2 | 0.379 | 0.043 | 0.021 | 0.067 | 0.012 | 0.077 | True | 1.000 | 2 | 8 |
| crowd bucket exact | D6 | 21 | lvl_naive4 | lvl_naive4 | 0.337 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_naive4 | 0.324 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_naive4 | 0.315 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| crowd bucket exact | D6 | 60 | lvl_naive4 | lvl_naive4 | 0.334 | 0.000 |  |  |  |  | True |  | 0 | 6 |
| crowd bucket exact | D6 | 90 | lvl_naive4 | lvl_naive4 | 0.337 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 120 | lvl_naive4 | lvl_naive4 | 0.321 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 180 | lvl_naive4 | lvl_naive4 | 0.284 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.438 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_naive4 | 0.305 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_naive4 | 0.288 | 0.000 |  |  |  |  | False |  | 0 | 6 |

### Paired against `tirex2` (not the per-lead reference)

MAE difference model − `tirex2` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.50 [-0.62, -0.37]* | -0.07 [-0.13, -0.01]* |  | 0.70 [0.48, 0.93] | 0.04 [-0.13, 0.21] | 2.87 [2.14, 3.80] | 0.40 [0.23, 0.60] | 0.13 [-0.09, 0.32] | -1.85 [-2.38, -1.43]* | -1.07 [-1.47, -0.66]* |  |  |  | 1.36 [0.98, 1.73] | -0.44 [-0.57, -0.32]* |  | -0.01 [-0.11, 0.11] | 4.40 [2.99, 5.90] | 0.02 [-0.07, 0.15] |  | 0.08 [-0.09, 0.26] | tirex2 |
| m015 | 0.07 [0.03, 0.12] |  |  | 4.63 [3.77, 5.54] | 4.08 [3.32, 4.89] |  | 4.37 [3.52, 5.23] | 3.99 [3.24, 4.82] |  |  | -0.15 [-0.21, -0.10]* |  |  | 5.36 [4.38, 6.42] |  |  | -0.11 [-0.16, -0.04]* |  |  |  | 4.07 [3.34, 4.87] | tirex2 |
| m030 | -0.16 [-0.23, -0.09]* |  |  | 3.41 [2.76, 4.11] | 2.86 [2.28, 3.46] |  | 3.17 [2.51, 3.85] | 2.84 [2.29, 3.47] |  |  | 0.29 [0.15, 0.43] |  |  | 4.18 [3.36, 5.06] |  |  | -0.14 [-0.22, -0.07]* |  |  |  | 2.90 [2.31, 3.51] | tirex2 |
| m045 | -0.30 [-0.42, -0.19]* |  |  | 2.91 [2.36, 3.51] | 2.33 [1.88, 2.83] |  | 2.65 [2.11, 3.24] | 2.35 [1.90, 2.86] |  |  | 0.65 [0.43, 0.86] |  |  | 3.61 [2.91, 4.37] |  |  | -0.21 [-0.32, -0.10]* |  |  |  | 2.38 [1.93, 2.90] | tirex2 |
| m060 | -0.41 [-0.56, -0.27]* |  |  | 2.64 [2.13, 3.20] | 2.07 [1.63, 2.54] |  | 2.37 [1.86, 2.93] | 2.06 [1.65, 2.54] |  |  | 0.92 [0.64, 1.19] |  |  | 3.36 [2.69, 4.09] |  |  | -0.26 [-0.38, -0.14]* |  |  |  | 2.10 [1.67, 2.54] | tirex2 |
| m075 | -0.48 [-0.63, -0.35]* |  |  | 2.34 [1.85, 2.87] | 1.72 [1.33, 2.11] |  | 2.04 [1.60, 2.49] | 1.74 [1.36, 2.17] |  |  | 1.16 [0.83, 1.48] |  |  | 3.06 [2.42, 3.74] |  |  | -0.27 [-0.41, -0.15]* |  |  |  | 1.77 [1.37, 2.18] | tirex2 |
| m090 | -0.52 [-0.66, -0.37]* |  |  | 2.21 [1.72, 2.71] | 1.53 [1.17, 1.92] |  | 1.88 [1.46, 2.34] | 1.59 [1.23, 1.99] |  |  | 1.44 [1.05, 1.83] |  |  | 2.89 [2.28, 3.53] |  |  | -0.25 [-0.36, -0.14]* |  |  |  | 1.62 [1.24, 2.01] | tirex2 |
| m105 | -0.53 [-0.70, -0.39]* |  |  | 2.13 [1.63, 2.63] | 1.47 [1.08, 1.86] |  | 1.83 [1.38, 2.30] | 1.47 [1.10, 1.85] |  |  | 1.61 [1.17, 2.07] |  |  | 2.83 [2.18, 3.54] |  |  | -0.26 [-0.36, -0.15]* |  |  |  | 1.54 [1.15, 1.94] | tirex2 |
| m120 | -0.58 [-0.78, -0.42]* |  |  | 2.03 [1.55, 2.53] | 1.36 [1.00, 1.74] |  | 1.68 [1.26, 2.13] | 1.33 [0.98, 1.71] |  |  | 1.75 [1.29, 2.23] |  |  | 2.70 [2.09, 3.33] |  |  | -0.28 [-0.40, -0.18]* |  |  |  | 1.42 [1.04, 1.81] | tirex2 |
| h2-4 | -0.58 [-0.74, -0.43]* |  |  | 1.65 [1.23, 2.06] | 1.00 [0.70, 1.30] |  | 1.37 [0.98, 1.75] | 1.05 [0.74, 1.34] |  |  | 2.46 [1.86, 3.08] |  |  | 2.33 [1.74, 2.97] |  |  | -0.26 [-0.38, -0.14]* |  |  |  | 1.05 [0.75, 1.34] | tirex2 |
| h4-8 | -0.53 [-0.69, -0.38]* |  |  | 1.25 [0.94, 1.56] | 0.57 [0.36, 0.78] |  | 0.95 [0.68, 1.24] | 0.69 [0.42, 0.93] |  |  | 3.95 [3.12, 4.84] |  |  | 1.94 [1.43, 2.49] |  |  | -0.22 [-0.34, -0.10]* |  |  |  | 0.64 [0.43, 0.85] | tirex2 |
| h8+ | -0.44 [-0.64, -0.27]* |  |  | 0.62 [0.42, 0.88] | 0.01 [-0.21, 0.25] |  | 0.43 [0.22, 0.67] | 0.40 [0.19, 0.61] |  |  | 5.80 [4.48, 7.70] |  |  | 1.44 [1.02, 1.86] |  |  | -0.12 [-0.24, -0.02]* |  |  |  | 0.10 [-0.08, 0.30] | tirex2 |
| 1 | -0.44 [-0.56, -0.33]* | 0.02 [-0.05, 0.09] | -0.44 [-0.63, -0.26]* | 0.28 [0.05, 0.52] | -0.32 [-0.51, -0.15]* | 2.54 [1.95, 3.23] | -0.05 [-0.21, 0.12] | 0.05 [-0.15, 0.27] | -2.05 [-2.52, -1.64]* | -1.55 [-1.97, -1.14]* |  |  |  | 1.09 [0.85, 1.34] | -0.37 [-0.53, -0.21]* | -0.36 [-0.53, -0.20]* | 0.05 [-0.05, 0.16] | 4.39 [3.15, 5.73] | 0.09 [-0.01, 0.21] | -0.32 [-0.51, -0.13]* | -0.27 [-0.43, -0.09]* | tirex2 |
| 2 | -0.46 [-0.64, -0.30]* | 0.08 [-0.01, 0.15] | -0.49 [-0.71, -0.29]* | 0.22 [-0.04, 0.53] | -0.38 [-0.61, -0.15]* | 2.74 [2.05, 3.66] | -0.16 [-0.38, 0.08] | -0.10 [-0.35, 0.12] | -2.39 [-2.91, -1.93]* | -1.65 [-2.18, -1.14]* |  |  |  | 0.83 [0.58, 1.14] | -0.38 [-0.53, -0.25]* | -0.44 [-0.65, -0.24]* | 0.14 [0.04, 0.24] | 4.32 [3.09, 5.65] | 0.17 [0.06, 0.29] | -0.35 [-0.55, -0.15]* | -0.32 [-0.54, -0.08]* | tirex2 |
| 3 | -0.42 [-0.58, -0.27]* | 0.08 [-0.01, 0.16] | -0.50 [-0.73, -0.27]* | 0.15 [-0.09, 0.44] | -0.48 [-0.70, -0.23]* | 2.38 [1.82, 3.01] | -0.21 [-0.40, 0.00] | -0.03 [-0.26, 0.22] | -2.47 [-3.04, -1.97]* | -1.73 [-2.19, -1.29]* |  | 0.57 [0.18, 0.92] | 0.55 [0.17, 0.90] | 0.53 [0.34, 0.72] | -0.37 [-0.50, -0.24]* | -0.47 [-0.69, -0.25]* | 0.20 [0.10, 0.33] | 4.13 [3.00, 5.33] | 0.24 [0.13, 0.38] | -0.39 [-0.61, -0.16]* | -0.40 [-0.60, -0.16]* | tirex2 |
| 4 | -0.47 [-0.67, -0.29]* | 0.12 [0.04, 0.20] | -0.65 [-0.99, -0.33]* | -0.09 [-0.39, 0.24] | -0.69 [-1.01, -0.35]* | 2.37 [1.86, 2.91] | -0.40 [-0.74, -0.06]* | -0.18 [-0.48, 0.12] | -2.65 [-3.30, -2.04]* | -1.89 [-2.51, -1.25]* |  | 0.42 [0.02, 0.82] | 0.39 [-0.01, 0.80] | 0.41 [0.17, 0.64] | -0.39 [-0.57, -0.21]* | -0.62 [-0.93, -0.32]* | 0.20 [0.08, 0.35] | 4.18 [3.00, 5.39] | 0.24 [0.12, 0.39] | -0.50 [-0.79, -0.20]* | -0.63 [-0.95, -0.30]* | tirex2 |
| 5 | -0.49 [-0.70, -0.30]* | 0.06 [-0.02, 0.13] | -0.65 [-0.94, -0.37]* | -0.05 [-0.31, 0.23] | -0.61 [-0.93, -0.30]* | 2.46 [1.85, 3.27] | -0.41 [-0.68, -0.14]* | -0.13 [-0.44, 0.19] | -2.65 [-3.35, -2.03]* | -1.95 [-2.54, -1.39]* |  | 0.54 [0.15, 0.90] | 0.51 [0.12, 0.88] | 0.49 [0.24, 0.75] | -0.39 [-0.58, -0.20]* | -0.64 [-0.93, -0.36]* | 0.11 [0.01, 0.23] | 3.96 [2.80, 5.20] | 0.15 [0.03, 0.27] | -0.53 [-0.80, -0.26]* | -0.55 [-0.88, -0.24]* | tirex2 |
| 6 | -0.45 [-0.61, -0.29]* | 0.07 [-0.00, 0.15] | -0.66 [-0.96, -0.36]* | 0.05 [-0.22, 0.35] | -0.57 [-0.83, -0.29]* | 2.36 [1.87, 2.90] | -0.42 [-0.70, -0.13]* | 0.00 [-0.29, 0.31] | -2.41 [-2.95, -1.90]* | -1.97 [-2.54, -1.40]* |  | 0.73 [0.33, 1.12] | 0.71 [0.30, 1.10] | 0.66 [0.48, 0.89] | -0.46 [-0.69, -0.25]* | -0.63 [-0.91, -0.34]* | 0.14 [0.03, 0.27] | 3.80 [2.67, 5.02] | 0.17 [0.06, 0.32] | -0.52 [-0.81, -0.21]* | -0.51 [-0.77, -0.22]* | tirex2 |
| 7 | -0.46 [-0.64, -0.28]* | 0.05 [-0.04, 0.14] | -0.71 [-1.00, -0.42]* | -0.18 [-0.46, 0.16] | -0.66 [-0.97, -0.33]* | 2.51 [1.92, 3.30] | -0.36 [-0.66, -0.06]* | -0.21 [-0.51, 0.09] | -2.85 [-3.46, -2.27]* | -1.78 [-2.38, -1.21]* |  | 0.38 [-0.02, 0.77] | 0.36 [-0.03, 0.75] |  | -0.33 [-0.51, -0.14]* | -0.67 [-0.96, -0.39]* | 0.18 [0.05, 0.35] | 3.79 [2.68, 4.96] | 0.22 [0.08, 0.38] | -0.49 [-0.76, -0.21]* | -0.59 [-0.89, -0.24]* | tirex2 |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.09 [-1.38, -0.81]* | -0.10 [-0.24, 0.04] |  | 1.69 [1.24, 2.17] | 0.30 [-0.08, 0.73] | 5.58 [3.65, 7.77] | 0.73 [0.38, 1.18] | -0.13 [-0.67, 0.28] | -4.35 [-5.43, -3.36]* | -2.56 [-3.05, -2.02]* |  |  |  | 3.05 [2.26, 3.72] | -0.86 [-1.12, -0.63]* |  | -0.04 [-0.25, 0.25] | 10.49 [7.26, 13.55] | 0.05 [-0.17, 0.35] |  | 0.44 [0.05, 0.87] | tirex2 |
| m015 | 0.12 [0.01, 0.26] |  |  | 9.38 [7.71, 11.14] | 8.26 [6.72, 9.81] |  | 8.54 [6.85, 10.30] | 7.75 [6.24, 9.13] |  |  | -0.26 [-0.39, -0.10]* |  |  | 10.93 [8.98, 12.79] |  |  | -0.21 [-0.32, -0.07]* |  |  |  | 8.27 [6.76, 9.79] | tirex2 |
| m030 | -0.33 [-0.47, -0.17]* |  |  | 7.04 [5.64, 8.59] | 5.94 [4.68, 7.28] |  | 6.25 [4.84, 7.81] | 5.43 [4.23, 6.58] |  |  | 0.67 [0.35, 0.94] |  |  | 8.73 [7.02, 10.45] |  |  | -0.30 [-0.46, -0.13]* |  |  |  | 6.06 [4.77, 7.43] | tirex2 |
| m045 | -0.67 [-0.87, -0.42]* |  |  | 6.04 [4.93, 7.38] | 4.88 [3.91, 6.02] |  | 5.21 [4.06, 6.58] | 4.44 [3.47, 5.48] |  |  | 1.37 [0.97, 1.69] |  |  | 7.53 [6.09, 9.00] |  |  | -0.46 [-0.68, -0.20]* |  |  |  | 5.02 [4.05, 6.13] | tirex2 |
| m060 | -0.90 [-1.18, -0.60]* |  |  | 5.52 [4.46, 6.80] | 4.35 [3.42, 5.43] |  | 4.65 [3.53, 5.91] | 3.83 [2.92, 4.85] |  |  | 1.87 [1.42, 2.27] |  |  | 7.04 [5.66, 8.45] |  |  | -0.55 [-0.81, -0.26]* |  |  |  | 4.47 [3.58, 5.48] | tirex2 |
| m075 | -1.03 [-1.29, -0.73]* |  |  | 5.00 [3.96, 6.09] | 3.69 [2.86, 4.62] |  | 4.00 [3.04, 5.02] | 3.18 [2.39, 4.03] |  |  | 2.28 [1.77, 2.77] |  |  | 6.54 [5.21, 7.81] |  |  | -0.62 [-0.89, -0.30]* |  |  |  | 3.84 [3.03, 4.73] | tirex2 |
| m090 | -1.09 [-1.36, -0.80]* |  |  | 4.78 [3.75, 5.92] | 3.33 [2.51, 4.28] |  | 3.68 [2.71, 4.78] | 2.86 [2.10, 3.74] |  |  | 2.83 [2.25, 3.42] |  |  | 6.18 [4.88, 7.48] |  |  | -0.53 [-0.75, -0.26]* |  |  |  | 3.57 [2.74, 4.53] | tirex2 |
| m105 | -1.14 [-1.50, -0.81]* |  |  | 4.62 [3.49, 5.81] | 3.23 [2.33, 4.23] |  | 3.61 [2.58, 4.74] | 2.55 [1.71, 3.42] |  |  | 3.16 [2.44, 3.90] |  |  | 6.07 [4.64, 7.49] |  |  | -0.54 [-0.77, -0.27]* |  |  |  | 3.41 [2.50, 4.41] | tirex2 |
| m120 | -1.27 [-1.71, -0.89]* |  |  | 4.42 [3.34, 5.55] | 2.98 [2.15, 3.90] |  | 3.31 [2.39, 4.33] | 2.28 [1.47, 3.14] |  |  | 3.42 [2.67, 4.20] |  |  | 5.75 [4.45, 7.03] |  |  | -0.60 [-0.86, -0.33]* |  |  |  | 3.16 [2.30, 4.09] | tirex2 |
| h2-4 | -1.25 [-1.59, -0.92]* |  |  | 3.76 [2.89, 4.71] | 2.38 [1.70, 3.12] |  | 2.80 [1.94, 3.72] | 1.86 [1.14, 2.52] |  |  | 4.72 [3.80, 5.72] |  |  | 5.15 [3.92, 6.45] |  |  | -0.56 [-0.83, -0.26]* |  |  |  | 2.53 [1.84, 3.24] | tirex2 |
| h4-8 | -1.14 [-1.51, -0.79]* |  |  | 2.85 [2.17, 3.53] | 1.44 [0.93, 2.01] |  | 1.89 [1.28, 2.60] | 1.09 [0.50, 1.65] |  |  | 7.11 [5.89, 8.51] |  |  | 4.26 [3.18, 5.34] |  |  | -0.47 [-0.75, -0.16]* |  |  |  | 1.63 [1.12, 2.17] | tirex2 |
| h8+ | -0.98 [-1.46, -0.53]* |  |  | 1.36 [0.91, 1.95] | 0.08 [-0.49, 0.74] |  | 0.54 [0.00, 1.11] | 0.20 [-0.27, 0.74] |  |  | 10.60 [7.63, 14.63] |  |  | 2.95 [2.19, 3.68] |  |  | -0.23 [-0.48, 0.06] |  |  |  | 0.33 [-0.13, 0.91] | tirex2 |
| 1 | -0.99 [-1.24, -0.73]* | 0.07 [-0.11, 0.22] | -1.29 [-1.66, -0.93]* | 0.77 [0.15, 1.44] | -0.52 [-0.96, -0.04]* | 4.63 [3.11, 6.35] | -0.41 [-0.79, -0.03]* | -0.68 [-1.07, -0.31]* | -4.77 [-5.55, -3.88]* | -3.75 [-4.20, -3.23]* |  |  |  | 2.19 [1.85, 2.53] | -0.74 [-1.08, -0.37]* | -1.13 [-1.44, -0.82]* | 0.12 [-0.10, 0.36] | 10.38 [7.59, 13.07] | 0.20 [-0.02, 0.45] | -1.11 [-1.42, -0.75]* | -0.36 [-0.77, 0.12] | tirex2 |
| 2 | -1.10 [-1.51, -0.71]* | 0.17 [-0.06, 0.35] | -1.46 [-1.89, -1.01]* | 0.54 [-0.15, 1.33] | -0.70 [-1.28, -0.06]* | 5.09 [3.26, 7.52] | -0.71 [-1.23, -0.18]* | -0.96 [-1.47, -0.40]* | -5.53 [-6.50, -4.54]* | -4.11 [-4.85, -3.31]* |  |  |  | 1.67 [1.19, 2.19] | -0.77 [-1.10, -0.45]* | -1.35 [-1.78, -0.91]* | 0.27 [0.06, 0.50] | 10.09 [7.32, 12.82] | 0.35 [0.13, 0.60] | -1.15 [-1.53, -0.73]* | -0.54 [-1.09, 0.10] | tirex2 |
| 3 | -1.02 [-1.37, -0.68]* | 0.14 [-0.09, 0.36] | -1.50 [-1.89, -1.03]* | 0.31 [-0.31, 1.11] | -0.99 [-1.45, -0.37]* | 4.15 [2.73, 5.66] | -0.77 [-1.17, -0.34]* | -0.79 [-1.27, -0.26]* | -5.72 [-6.73, -4.68]* | -4.17 [-4.64, -3.58]* |  | 0.46 [-0.34, 1.23] | 0.40 [-0.39, 1.18] | 1.12 [0.71, 1.51] | -0.69 [-0.98, -0.39]* | -1.41 [-1.80, -0.96]* | 0.42 [0.20, 0.71] | 9.65 [7.15, 12.16] | 0.51 [0.26, 0.82] | -1.27 [-1.68, -0.79]* | -0.78 [-1.21, -0.20]* | tirex2 |
| 4 | -1.15 [-1.60, -0.74]* | 0.25 [0.04, 0.43] | -1.86 [-2.47, -1.18]* | -0.23 [-0.89, 0.62] | -1.52 [-2.19, -0.69]* | 4.18 [2.83, 5.51] | -1.28 [-1.94, -0.47]* | -1.11 [-1.73, -0.44]* | -6.14 [-7.37, -4.86]* | -4.65 [-5.47, -3.55]* |  | 0.11 [-0.78, 1.07] | 0.04 [-0.85, 0.99] | 0.81 [0.26, 1.41] | -0.79 [-1.21, -0.38]* | -1.79 [-2.35, -1.16]* | 0.41 [0.12, 0.72] | 9.68 [7.06, 12.11] | 0.50 [0.22, 0.82] | -1.53 [-2.04, -0.93]* | -1.37 [-2.00, -0.53]* | tirex2 |
| 5 | -1.19 [-1.65, -0.74]* | 0.10 [-0.09, 0.27] | -1.75 [-2.36, -1.13]* | -0.11 [-0.68, 0.56] | -1.26 [-1.94, -0.47]* | 4.54 [2.90, 6.65] | -1.20 [-1.73, -0.57]* | -0.93 [-1.54, -0.29]* | -6.17 [-7.60, -4.80]* | -4.57 [-5.30, -3.72]* |  | 0.48 [-0.30, 1.31] | 0.41 [-0.38, 1.25] | 1.06 [0.53, 1.62] | -0.80 [-1.20, -0.37]* | -1.72 [-2.30, -1.10]* | 0.25 [0.01, 0.54] | 9.34 [6.69, 11.82] | 0.33 [0.08, 0.61] | -1.48 [-2.00, -0.94]* | -1.07 [-1.76, -0.27]* | tirex2 |
| 6 | -1.04 [-1.44, -0.66]* | 0.17 [-0.04, 0.36] | -1.74 [-2.28, -1.14]* | 0.20 [-0.48, 1.06] | -1.10 [-1.65, -0.39]* | 4.08 [2.79, 5.51] | -1.29 [-1.79, -0.68]* | -0.85 [-1.41, -0.23]* | -5.60 [-6.49, -4.60]* | -4.74 [-5.36, -3.97]* |  | 0.52 [-0.29, 1.36] | 0.47 [-0.38, 1.32] | 1.26 [0.85, 1.73] | -0.96 [-1.40, -0.47]* | -1.64 [-2.14, -1.05]* | 0.38 [0.13, 0.68] | 9.07 [6.52, 11.62] | 0.45 [0.19, 0.79] | -1.47 [-2.00, -0.85]* | -0.93 [-1.47, -0.20]* | tirex2 |
| 7 | -1.11 [-1.49, -0.65]* | 0.10 [-0.12, 0.31] | -1.84 [-2.41, -1.23]* | -0.31 [-0.98, 0.47] | -1.31 [-1.93, -0.51]* | 4.49 [2.88, 6.69] | -1.09 [-1.66, -0.44]* | -1.16 [-1.77, -0.49]* | -6.50 [-7.68, -5.34]* | -4.35 [-5.16, -3.47]* |  | 0.03 [-0.82, 0.97] | -0.03 [-0.88, 0.92] |  | -0.67 [-1.00, -0.24]* | -1.75 [-2.31, -1.12]* | 0.45 [0.12, 0.85] | 8.95 [6.33, 11.46] | 0.53 [0.18, 0.94] | -1.37 [-1.86, -0.81]* | -1.12 [-1.74, -0.31]* | tirex2 |

### Paired against `toto2` (not the per-lead reference)

MAE difference model − `toto2` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.49 [-0.63, -0.36]* | -0.07 [-0.20, 0.05] |  | 0.71 [0.45, 0.96] | 0.05 [-0.12, 0.22] | 2.86 [2.06, 3.88] | 0.41 [0.23, 0.62] | 0.14 [-0.11, 0.35] | -1.84 [-2.34, -1.43]* | -1.06 [-1.42, -0.68]* |  |  |  | 1.39 [0.93, 1.80] | 0.01 [-0.11, 0.11] | -0.43 [-0.57, -0.31]* |  | 4.41 [2.98, 5.97] | 0.03 [0.01, 0.05] |  | 0.09 [-0.09, 0.27] | toto2 |
| m015 | 0.18 [0.11, 0.24] |  |  | 4.74 [3.86, 5.68] | 4.19 [3.43, 5.01] |  | 4.49 [3.63, 5.37] | 4.10 [3.34, 4.95] |  |  | -0.00 [-0.07, 0.06] |  |  | 5.50 [4.50, 6.60] | 0.11 [0.04, 0.16] |  |  |  |  |  | 4.18 [3.43, 4.99] | toto2 |
| m030 | -0.01 [-0.07, 0.04] |  |  | 3.58 [2.88, 4.29] | 3.01 [2.41, 3.62] |  | 3.34 [2.65, 4.03] | 2.99 [2.41, 3.63] |  |  | 0.50 [0.30, 0.68] |  |  | 4.36 [3.52, 5.27] | 0.14 [0.07, 0.22] |  |  |  |  |  | 3.06 [2.45, 3.69] | toto2 |
| m045 | -0.09 [-0.14, -0.04]* |  |  | 3.14 [2.56, 3.77] | 2.55 [2.05, 3.06] |  | 2.88 [2.30, 3.50] | 2.56 [2.06, 3.12] |  |  | 0.93 [0.64, 1.21] |  |  | 3.87 [3.11, 4.73] | 0.21 [0.10, 0.32] |  |  |  |  |  | 2.61 [2.11, 3.15] | toto2 |
| m060 | -0.15 [-0.21, -0.10]* |  |  | 2.92 [2.35, 3.50] | 2.34 [1.85, 2.83] |  | 2.65 [2.09, 3.23] | 2.33 [1.84, 2.87] |  |  | 1.24 [0.89, 1.57] |  |  | 3.66 [2.92, 4.47] | 0.26 [0.14, 0.38] |  |  |  |  |  | 2.37 [1.90, 2.87] | toto2 |
| m075 | -0.21 [-0.28, -0.15]* |  |  | 2.62 [2.07, 3.19] | 2.00 [1.59, 2.44] |  | 2.33 [1.84, 2.83] | 2.01 [1.56, 2.49] |  |  | 1.50 [1.10, 1.90] |  |  | 3.36 [2.65, 4.13] | 0.27 [0.15, 0.41] |  |  |  |  |  | 2.05 [1.61, 2.52] | toto2 |
| m090 | -0.26 [-0.34, -0.19]* |  |  | 2.47 [1.93, 3.01] | 1.79 [1.40, 2.19] |  | 2.15 [1.69, 2.63] | 1.85 [1.46, 2.29] |  |  | 1.80 [1.33, 2.24] |  |  | 3.17 [2.50, 3.88] | 0.25 [0.14, 0.36] |  |  |  |  |  | 1.88 [1.46, 2.32] | toto2 |
| m105 | -0.27 [-0.36, -0.19]* |  |  | 2.39 [1.87, 2.91] | 1.73 [1.33, 2.14] |  | 2.10 [1.64, 2.59] | 1.75 [1.36, 2.17] |  |  | 1.99 [1.49, 2.53] |  |  | 3.12 [2.43, 3.86] | 0.26 [0.15, 0.36] |  |  |  |  |  | 1.80 [1.39, 2.22] | toto2 |
| m120 | -0.30 [-0.40, -0.21]* |  |  | 2.32 [1.81, 2.85] | 1.65 [1.28, 2.04] |  | 1.98 [1.55, 2.44] | 1.63 [1.26, 2.03] |  |  | 2.14 [1.62, 2.72] |  |  | 3.01 [2.38, 3.70] | 0.28 [0.18, 0.40] |  |  |  |  |  | 1.71 [1.32, 2.10] | toto2 |
| h2-4 | -0.32 [-0.43, -0.22]* |  |  | 1.92 [1.46, 2.42] | 1.27 [0.91, 1.63] |  | 1.64 [1.21, 2.09] | 1.31 [0.94, 1.69] |  |  | 2.89 [2.23, 3.58] |  |  | 2.62 [1.96, 3.35] | 0.26 [0.14, 0.38] |  |  |  |  |  | 1.32 [0.96, 1.67] | toto2 |
| h4-8 | -0.31 [-0.42, -0.20]* |  |  | 1.47 [1.10, 1.85] | 0.79 [0.53, 1.07] |  | 1.18 [0.84, 1.54] | 0.88 [0.55, 1.21] |  |  | 4.47 [3.53, 5.45] |  |  | 2.18 [1.57, 2.83] | 0.22 [0.10, 0.34] |  |  |  |  |  | 0.86 [0.60, 1.13] | toto2 |
| h8+ | -0.32 [-0.46, -0.20]* |  |  | 0.75 [0.53, 0.98] | 0.13 [-0.03, 0.32] |  | 0.56 [0.39, 0.76] | 0.50 [0.24, 0.73] |  |  | 6.19 [4.75, 8.22] |  |  | 1.57 [1.12, 2.03] | 0.12 [0.02, 0.24] |  |  |  |  |  | 0.22 [0.07, 0.39] | toto2 |
| 1 | -0.49 [-0.65, -0.35]* | -0.03 [-0.16, 0.07] | -0.50 [-0.72, -0.31]* | 0.23 [-0.01, 0.47] | -0.38 [-0.59, -0.19]* | 2.50 [1.89, 3.25] | -0.10 [-0.30, 0.10] | 0.01 [-0.22, 0.25] | -2.11 [-2.59, -1.67]* | -1.60 [-1.99, -1.16]* |  |  |  | 1.04 [0.79, 1.28] | -0.05 [-0.16, 0.05] | -0.42 [-0.59, -0.26]* | -0.42 [-0.62, -0.26]* | 4.34 [3.08, 5.71] | 0.04 [0.03, 0.05] | -0.38 [-0.59, -0.18]* | -0.32 [-0.52, -0.13]* | toto2 |
| 2 | -0.59 [-0.83, -0.39]* | -0.06 [-0.22, 0.07] | -0.63 [-0.90, -0.39]* | 0.09 [-0.16, 0.36] | -0.51 [-0.77, -0.26]* | 2.61 [1.96, 3.47] | -0.30 [-0.55, -0.05]* | -0.23 [-0.51, 0.02] | -2.52 [-3.06, -2.05]* | -1.79 [-2.32, -1.29]* |  |  |  | 0.73 [0.45, 1.01] | -0.14 [-0.24, -0.04]* | -0.51 [-0.68, -0.36]* | -0.58 [-0.82, -0.35]* | 4.19 [2.98, 5.50] | 0.04 [0.02, 0.05] | -0.48 [-0.73, -0.26]* | -0.45 [-0.68, -0.20]* | toto2 |
| 3 | -0.62 [-0.85, -0.42]* | -0.13 [-0.29, 0.01] | -0.70 [-0.97, -0.47]* | -0.05 [-0.31, 0.23] | -0.68 [-0.92, -0.42]* | 2.20 [1.63, 2.82] | -0.41 [-0.63, -0.20]* | -0.21 [-0.46, 0.01] | -2.67 [-3.25, -2.13]* | -1.93 [-2.36, -1.48]* |  | 0.39 [0.04, 0.71] | 0.37 [0.03, 0.69] | 0.36 [0.11, 0.58] | -0.20 [-0.33, -0.10]* | -0.57 [-0.76, -0.41]* | -0.67 [-0.92, -0.44]* | 3.92 [2.82, 5.10] | 0.04 [0.02, 0.05] | -0.60 [-0.85, -0.36]* | -0.60 [-0.82, -0.35]* | toto2 |
| 4 | -0.67 [-0.92, -0.43]* | -0.08 [-0.26, 0.07] | -0.84 [-1.19, -0.53]* | -0.29 [-0.57, 0.03] | -0.88 [-1.23, -0.55]* | 2.17 [1.60, 2.77] | -0.59 [-0.95, -0.25]* | -0.37 [-0.66, -0.10]* | -2.84 [-3.51, -2.23]* | -2.08 [-2.70, -1.44]* |  | 0.23 [-0.18, 0.61] | 0.21 [-0.21, 0.59] | 0.25 [-0.02, 0.53] | -0.20 [-0.35, -0.08]* | -0.58 [-0.81, -0.38]* | -0.82 [-1.13, -0.53]* | 3.98 [2.82, 5.23] | 0.04 [0.03, 0.06] | -0.70 [-1.01, -0.40]* | -0.82 [-1.16, -0.49]* | toto2 |
| 5 | -0.61 [-0.84, -0.40]* | -0.06 [-0.20, 0.07] | -0.76 [-1.06, -0.48]* | -0.17 [-0.39, 0.11] | -0.72 [-1.03, -0.41]* | 2.32 [1.72, 3.11] | -0.52 [-0.79, -0.26]* | -0.26 [-0.57, 0.05] | -2.74 [-3.44, -2.12]* | -2.03 [-2.60, -1.48]* |  | 0.41 [0.02, 0.77] | 0.39 [-0.01, 0.75] | 0.45 [0.18, 0.69] | -0.11 [-0.23, -0.01]* | -0.50 [-0.70, -0.31]* | -0.75 [-1.03, -0.48]* | 3.85 [2.70, 5.09] | 0.03 [0.02, 0.05] | -0.64 [-0.90, -0.39]* | -0.66 [-0.96, -0.34]* | toto2 |
| 6 | -0.59 [-0.84, -0.39]* | -0.07 [-0.23, 0.07] | -0.79 [-1.13, -0.51]* | -0.09 [-0.32, 0.19] | -0.71 [-0.99, -0.43]* | 2.23 [1.72, 2.77] | -0.55 [-0.86, -0.27]* | -0.12 [-0.45, 0.19] | -2.53 [-3.14, -2.00]* | -2.09 [-2.67, -1.53]* |  | 0.62 [0.16, 1.01] | 0.60 [0.13, 1.00] | 0.60 [0.41, 0.80] | -0.14 [-0.27, -0.03]* | -0.60 [-0.86, -0.37]* | -0.77 [-1.08, -0.50]* | 3.66 [2.55, 4.87] | 0.03 [0.02, 0.05] | -0.66 [-0.97, -0.37]* | -0.64 [-0.91, -0.36]* | toto2 |
| 7 | -0.65 [-0.89, -0.43]* | -0.14 [-0.29, -0.01]* | -0.89 [-1.25, -0.58]* | -0.36 [-0.64, -0.04]* | -0.84 [-1.18, -0.50]* | 2.26 [1.70, 2.93] | -0.54 [-0.88, -0.23]* | -0.46 [-0.85, -0.11]* | -3.01 [-3.67, -2.43]* | -1.93 [-2.57, -1.34]* |  | 0.14 [-0.37, 0.58] | 0.12 [-0.40, 0.56] |  | -0.18 [-0.35, -0.05]* | -0.51 [-0.74, -0.31]* | -0.85 [-1.20, -0.54]* | 3.61 [2.52, 4.72] | 0.03 [0.02, 0.05] | -0.67 [-0.99, -0.38]* | -0.77 [-1.10, -0.42]* | toto2 |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.05 [-1.32, -0.82]* | -0.06 [-0.36, 0.17] |  | 1.72 [1.14, 2.24] | 0.33 [-0.07, 0.75] | 5.59 [3.46, 8.02] | 0.77 [0.36, 1.22] | -0.11 [-0.72, 0.38] | -4.31 [-5.40, -3.35]* | -2.52 [-2.98, -2.01]* |  |  |  | 3.12 [2.17, 3.89] | 0.04 [-0.25, 0.25] | -0.82 [-1.05, -0.62]* |  | 10.53 [7.08, 13.68] | 0.09 [0.07, 0.13] |  | 0.47 [0.07, 0.89] | toto2 |
| m015 | 0.32 [0.15, 0.49] |  |  | 9.59 [7.82, 11.41] | 8.48 [6.88, 10.09] |  | 8.77 [7.04, 10.60] | 7.99 [6.43, 9.42] |  |  | -0.01 [-0.19, 0.15] |  |  | 11.21 [9.20, 13.14] | 0.21 [0.07, 0.32] |  |  |  |  |  | 8.49 [6.93, 10.04] | toto2 |
| m030 | -0.03 [-0.18, 0.10] |  |  | 7.37 [5.89, 8.93] | 6.26 [4.95, 7.64] |  | 6.59 [5.12, 8.14] | 5.75 [4.44, 6.94] |  |  | 1.04 [0.58, 1.38] |  |  | 9.12 [7.31, 10.88] | 0.30 [0.13, 0.46] |  |  |  |  |  | 6.38 [5.06, 7.79] | toto2 |
| m045 | -0.21 [-0.32, -0.11]* |  |  | 6.53 [5.33, 7.91] | 5.35 [4.32, 6.49] |  | 5.70 [4.48, 7.06] | 4.91 [3.85, 5.93] |  |  | 1.90 [1.35, 2.33] |  |  | 8.08 [6.54, 9.64] | 0.46 [0.20, 0.68] |  |  |  |  |  | 5.50 [4.45, 6.61] | toto2 |
| m060 | -0.35 [-0.47, -0.25]* |  |  | 6.10 [4.91, 7.35] | 4.92 [3.91, 6.05] |  | 5.24 [4.06, 6.55] | 4.41 [3.35, 5.39] |  |  | 2.51 [1.90, 3.02] |  |  | 7.68 [6.14, 9.21] | 0.55 [0.26, 0.81] |  |  |  |  |  | 5.04 [4.07, 6.08] | toto2 |
| m075 | -0.41 [-0.54, -0.30]* |  |  | 5.63 [4.39, 6.85] | 4.33 [3.39, 5.33] |  | 4.65 [3.60, 5.73] | 3.80 [2.80, 4.69] |  |  | 2.97 [2.31, 3.58] |  |  | 7.22 [5.68, 8.68] | 0.62 [0.30, 0.89] |  |  |  |  |  | 4.48 [3.51, 5.48] | toto2 |
| m090 | -0.56 [-0.73, -0.41]* |  |  | 5.31 [4.14, 6.51] | 3.88 [2.98, 4.88] |  | 4.24 [3.20, 5.36] | 3.41 [2.51, 4.28] |  |  | 3.54 [2.78, 4.26] |  |  | 6.75 [5.30, 8.18] | 0.53 [0.26, 0.75] |  |  |  |  |  | 4.12 [3.23, 5.10] | toto2 |
| m105 | -0.60 [-0.78, -0.43]* |  |  | 5.14 [3.97, 6.32] | 3.78 [2.86, 4.73] |  | 4.18 [3.10, 5.26] | 3.14 [2.27, 3.95] |  |  | 3.89 [3.01, 4.77] |  |  | 6.63 [5.13, 8.10] | 0.54 [0.27, 0.77] |  |  |  |  |  | 3.96 [3.02, 4.90] | toto2 |
| m120 | -0.67 [-0.88, -0.48]* |  |  | 5.01 [3.90, 6.19] | 3.60 [2.73, 4.50] |  | 3.95 [2.96, 4.97] | 2.94 [2.13, 3.73] |  |  | 4.20 [3.29, 5.12] |  |  | 6.40 [5.00, 7.74] | 0.60 [0.33, 0.86] |  |  |  |  |  | 3.78 [2.91, 4.68] | toto2 |
| h2-4 | -0.68 [-0.93, -0.48]* |  |  | 4.32 [3.29, 5.35] | 2.96 [2.16, 3.78] |  | 3.40 [2.45, 4.41] | 2.44 [1.56, 3.23] |  |  | 5.56 [4.45, 6.70] |  |  | 5.79 [4.40, 7.20] | 0.56 [0.26, 0.83] |  |  |  |  |  | 3.10 [2.33, 3.91] | toto2 |
| h4-8 | -0.67 [-0.95, -0.44]* |  |  | 3.28 [2.43, 4.11] | 1.91 [1.30, 2.52] |  | 2.37 [1.64, 3.14] | 1.50 [0.70, 2.24] |  |  | 8.14 [6.64, 9.71] |  |  | 4.76 [3.47, 6.02] | 0.47 [0.16, 0.75] |  |  |  |  |  | 2.10 [1.50, 2.70] | toto2 |
| h8+ | -0.75 [-1.08, -0.48]* |  |  | 1.56 [1.08, 2.09] | 0.30 [-0.11, 0.77] |  | 0.77 [0.39, 1.20] | 0.38 [-0.10, 0.89] |  |  | 11.35 [8.06, 15.56] |  |  | 3.17 [2.28, 3.98] | 0.23 [-0.06, 0.48] |  |  |  |  |  | 0.53 [0.19, 0.97] | toto2 |
| 1 | -1.10 [-1.46, -0.81]* | -0.04 [-0.30, 0.15] | -1.42 [-1.85, -1.03]* | 0.66 [0.00, 1.30] | -0.65 [-1.18, -0.13]* | 4.53 [2.87, 6.34] | -0.51 [-0.97, -0.06]* | -0.79 [-1.27, -0.36]* | -4.90 [-5.70, -4.00]* | -3.85 [-4.43, -3.30]* |  |  |  | 2.08 [1.61, 2.47] | -0.12 [-0.36, 0.10] | -0.85 [-1.23, -0.48]* | -1.26 [-1.62, -0.94]* | 10.27 [7.32, 13.05] | 0.08 [0.05, 0.12] | -1.24 [-1.60, -0.89]* | -0.49 [-0.96, -0.01]* | toto2 |
| 2 | -1.36 [-1.90, -0.89]* | -0.10 [-0.42, 0.15] | -1.73 [-2.24, -1.26]* | 0.27 [-0.33, 0.97] | -0.97 [-1.52, -0.36]* | 4.78 [2.98, 7.06] | -0.98 [-1.50, -0.44]* | -1.28 [-1.87, -0.68]* | -5.79 [-6.74, -4.84]* | -4.38 [-5.10, -3.65]* |  |  |  | 1.47 [0.98, 1.97] | -0.27 [-0.50, -0.06]* | -1.02 [-1.36, -0.72]* | -1.61 [-2.10, -1.17]* | 9.83 [7.02, 12.57] | 0.08 [0.05, 0.12] | -1.42 [-1.88, -0.99]* | -0.81 [-1.32, -0.18]* | toto2 |
| 3 | -1.44 [-1.95, -0.98]* | -0.28 [-0.68, 0.02] | -1.91 [-2.44, -1.40]* | -0.10 [-0.70, 0.64] | -1.40 [-1.93, -0.80]* | 3.76 [2.33, 5.15] | -1.18 [-1.70, -0.72]* | -1.18 [-1.63, -0.72]* | -6.13 [-7.16, -5.09]* | -4.59 [-5.13, -4.00]* |  | 0.09 [-0.59, 0.77] | 0.03 [-0.64, 0.70] | 0.77 [0.23, 1.25] | -0.42 [-0.71, -0.20]* | -1.10 [-1.47, -0.70]* | -1.82 [-2.33, -1.34]* | 9.23 [6.64, 11.68] | 0.09 [0.05, 0.14] | -1.69 [-2.18, -1.18]* | -1.19 [-1.65, -0.62]* | toto2 |
| 4 | -1.56 [-2.13, -1.04]* | -0.16 [-0.54, 0.15] | -2.26 [-2.89, -1.65]* | -0.63 [-1.26, 0.11] | -1.93 [-2.55, -1.16]* | 3.70 [2.17, 5.17] | -1.67 [-2.36, -0.94]* | -1.53 [-2.07, -0.96]* | -6.52 [-7.66, -5.30]* | -5.02 [-5.88, -4.00]* |  | -0.30 [-1.07, 0.55] | -0.36 [-1.15, 0.50] | 0.51 [-0.13, 1.13] | -0.41 [-0.72, -0.12]* | -1.19 [-1.66, -0.70]* | -2.20 [-2.75, -1.64]* | 9.28 [6.47, 11.76] | 0.10 [0.07, 0.13] | -1.93 [-2.46, -1.40]* | -1.76 [-2.36, -0.99]* | toto2 |
| 5 | -1.44 [-1.90, -0.98]* | -0.15 [-0.50, 0.15] | -2.00 [-2.64, -1.43]* | -0.36 [-0.89, 0.24] | -1.51 [-2.17, -0.79]* | 4.13 [2.43, 6.19] | -1.43 [-1.96, -0.89]* | -1.31 [-1.91, -0.65]* | -6.39 [-7.75, -5.09]* | -4.76 [-5.47, -4.01]* |  | 0.12 [-0.71, 0.96] | 0.05 [-0.79, 0.88] | 0.98 [0.46, 1.47] | -0.25 [-0.54, -0.01]* | -1.03 [-1.45, -0.61]* | -1.97 [-2.58, -1.41]* | 9.08 [6.39, 11.65] | 0.07 [0.04, 0.11] | -1.73 [-2.28, -1.23]* | -1.32 [-1.99, -0.61]* | toto2 |
| 6 | -1.42 [-1.97, -0.96]* | -0.21 [-0.64, 0.11] | -2.12 [-2.78, -1.57]* | -0.17 [-0.77, 0.52] | -1.48 [-2.00, -0.85]* | 3.61 [2.15, 5.03] | -1.66 [-2.23, -1.11]* | -1.28 [-1.87, -0.70]* | -5.93 [-6.85, -4.94]* | -5.06 [-5.76, -4.34]* |  | 0.10 [-0.81, 0.94] | 0.04 [-0.87, 0.90] | 1.03 [0.69, 1.38] | -0.38 [-0.68, -0.13]* | -1.32 [-1.86, -0.86]* | -2.02 [-2.62, -1.50]* | 8.69 [6.06, 11.15] | 0.07 [0.03, 0.13] | -1.85 [-2.43, -1.30]* | -1.30 [-1.81, -0.67]* | toto2 |
| 7 | -1.57 [-2.11, -1.06]* | -0.36 [-0.76, -0.06]* | -2.30 [-3.02, -1.68]* | -0.77 [-1.39, -0.08]* | -1.77 [-2.44, -0.99]* | 3.76 [2.10, 5.68] | -1.52 [-2.21, -0.85]* | -1.92 [-2.74, -1.11]* | -6.87 [-8.04, -5.69]* | -4.71 [-5.56, -3.81]* |  | -0.70 [-1.77, 0.33] | -0.76 [-1.85, 0.29] |  | -0.45 [-0.85, -0.12]* | -1.10 [-1.58, -0.65]* | -2.20 [-2.91, -1.58]* | 8.50 [5.91, 10.99] | 0.08 [0.05, 0.11] | -1.83 [-2.45, -1.27]* | -1.57 [-2.24, -0.78]* | toto2 |

### Paired against `toto2_nocov` (not the per-lead reference)

MAE difference model − `toto2_nocov` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.52 [-0.67, -0.38]* | -0.10 [-0.24, 0.02] |  | 0.67 [0.42, 0.93] | 0.01 [-0.15, 0.18] | 2.84 [2.05, 3.87] | 0.38 [0.19, 0.58] | 0.11 [-0.14, 0.33] | -1.88 [-2.39, -1.46]* | -1.09 [-1.46, -0.71]* |  |  | 1.36 [0.90, 1.76] | -0.02 [-0.15, 0.07] | -0.46 [-0.61, -0.33]* |  | -0.03 [-0.05, -0.01]* | 4.37 [2.94, 5.94] |  | 0.06 [-0.12, 0.24] | toto2_nocov |
| 1 | -0.53 [-0.69, -0.38]* | -0.07 [-0.20, 0.04] | -0.54 [-0.76, -0.34]* | 0.19 [-0.04, 0.42] | -0.42 [-0.63, -0.21]* | 2.47 [1.86, 3.21] | -0.13 [-0.34, 0.07] | -0.03 [-0.26, 0.22] | -2.14 [-2.63, -1.70]* | -1.63 [-2.03, -1.19]* |  |  | 1.01 [0.76, 1.23] | -0.09 [-0.21, 0.01] | -0.45 [-0.63, -0.29]* | -0.46 [-0.66, -0.29]* | -0.04 [-0.05, -0.03]* | 4.30 [3.04, 5.66] | -0.42 [-0.63, -0.21]* | -0.36 [-0.56, -0.16]* | toto2_nocov |
| 2 | -0.63 [-0.87, -0.42]* | -0.09 [-0.26, 0.04] | -0.66 [-0.93, -0.42]* | 0.05 [-0.18, 0.32] | -0.55 [-0.80, -0.30]* | 2.58 [1.93, 3.43] | -0.33 [-0.59, -0.08]* | -0.26 [-0.55, -0.01]* | -2.56 [-3.09, -2.08]* | -1.82 [-2.36, -1.32]* |  |  | 0.70 [0.41, 0.97] | -0.17 [-0.29, -0.06]* | -0.55 [-0.72, -0.40]* | -0.61 [-0.86, -0.38]* | -0.04 [-0.05, -0.02]* | 4.15 [2.95, 5.47] | -0.51 [-0.76, -0.28]* | -0.49 [-0.71, -0.23]* | toto2_nocov |
| 3 | -0.66 [-0.90, -0.45]* | -0.16 [-0.34, -0.02]* | -0.74 [-1.02, -0.50]* | -0.09 [-0.34, 0.19] | -0.72 [-0.96, -0.45]* | 2.16 [1.59, 2.77] | -0.45 [-0.67, -0.24]* | -0.24 [-0.49, -0.02]* | -2.70 [-3.30, -2.16]* | -1.97 [-2.41, -1.51]* | 0.36 [0.00, 0.68] | 0.34 [-0.02, 0.66] | 0.32 [0.07, 0.54] | -0.24 [-0.38, -0.13]* | -0.61 [-0.80, -0.44]* | -0.71 [-0.97, -0.47]* | -0.04 [-0.05, -0.02]* | 3.88 [2.79, 5.06] | -0.63 [-0.90, -0.39]* | -0.64 [-0.86, -0.39]* | toto2_nocov |
| 4 | -0.71 [-0.97, -0.47]* | -0.12 [-0.30, 0.03] | -0.89 [-1.25, -0.57]* | -0.33 [-0.61, -0.01]* | -0.92 [-1.28, -0.59]* | 2.13 [1.58, 2.72] | -0.63 [-1.00, -0.29]* | -0.41 [-0.70, -0.14]* | -2.88 [-3.56, -2.27]* | -2.12 [-2.74, -1.48]* | 0.20 [-0.22, 0.58] | 0.18 [-0.25, 0.57] | 0.21 [-0.07, 0.49] | -0.24 [-0.39, -0.12]* | -0.63 [-0.85, -0.41]* | -0.86 [-1.18, -0.57]* | -0.04 [-0.06, -0.03]* | 3.94 [2.79, 5.19] | -0.74 [-1.06, -0.44]* | -0.86 [-1.21, -0.53]* | toto2_nocov |
| 5 | -0.64 [-0.88, -0.42]* | -0.09 [-0.24, 0.05] | -0.79 [-1.11, -0.51]* | -0.20 [-0.43, 0.08] | -0.75 [-1.07, -0.43]* | 2.29 [1.70, 3.07] | -0.55 [-0.83, -0.28]* | -0.29 [-0.60, 0.02] | -2.78 [-3.48, -2.14]* | -2.07 [-2.63, -1.51]* | 0.39 [-0.01, 0.75] | 0.36 [-0.04, 0.73] | 0.41 [0.14, 0.65] | -0.15 [-0.27, -0.03]* | -0.53 [-0.74, -0.33]* | -0.78 [-1.07, -0.51]* | -0.03 [-0.05, -0.02]* | 3.82 [2.67, 5.05] | -0.67 [-0.95, -0.41]* | -0.69 [-1.00, -0.37]* | toto2_nocov |
| 6 | -0.62 [-0.89, -0.41]* | -0.10 [-0.28, 0.04] | -0.83 [-1.18, -0.53]* | -0.12 [-0.35, 0.15] | -0.74 [-1.03, -0.45]* | 2.20 [1.70, 2.73] | -0.58 [-0.90, -0.29]* | -0.14 [-0.48, 0.16] | -2.56 [-3.18, -2.02]* | -2.12 [-2.70, -1.55]* | 0.60 [0.13, 1.00] | 0.57 [0.11, 0.98] | 0.57 [0.36, 0.78] | -0.17 [-0.32, -0.06]* | -0.63 [-0.90, -0.39]* | -0.80 [-1.14, -0.52]* | -0.03 [-0.05, -0.02]* | 3.63 [2.53, 4.83] | -0.69 [-1.02, -0.39]* | -0.67 [-0.96, -0.38]* | toto2_nocov |
| 7 | -0.68 [-0.93, -0.45]* | -0.17 [-0.33, -0.04]* | -0.92 [-1.29, -0.61]* | -0.39 [-0.68, -0.07]* | -0.88 [-1.22, -0.52]* | 2.23 [1.66, 2.90] | -0.57 [-0.93, -0.25]* | -0.49 [-0.89, -0.14]* | -3.04 [-3.72, -2.45]* | -1.97 [-2.61, -1.38]* | 0.11 [-0.40, 0.55] | 0.09 [-0.43, 0.54] |  | -0.22 [-0.38, -0.08]* | -0.54 [-0.78, -0.34]* | -0.88 [-1.24, -0.56]* | -0.03 [-0.05, -0.02]* | 3.57 [2.49, 4.68] | -0.70 [-1.04, -0.40]* | -0.80 [-1.14, -0.44]* | toto2_nocov |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.15 [-1.43, -0.90]* | -0.16 [-0.48, 0.09] |  | 1.63 [1.04, 2.15] | 0.24 [-0.17, 0.66] | 5.50 [3.37, 7.94] | 0.68 [0.26, 1.13] | -0.20 [-0.82, 0.30] | -4.41 [-5.52, -3.44]* | -2.61 [-3.08, -2.11]* |  |  | 3.03 [2.08, 3.81] | -0.05 [-0.35, 0.17] | -0.91 [-1.15, -0.71]* |  | -0.09 [-0.13, -0.07]* | 10.43 [7.00, 13.58] |  | 0.38 [-0.03, 0.81] | toto2_nocov |
| 1 | -1.18 [-1.55, -0.88]* | -0.13 [-0.41, 0.08] | -1.50 [-1.94, -1.10]* | 0.57 [-0.07, 1.19] | -0.73 [-1.25, -0.21]* | 4.45 [2.80, 6.26] | -0.59 [-1.08, -0.14]* | -0.87 [-1.35, -0.43]* | -4.98 [-5.82, -4.07]* | -3.93 [-4.50, -3.37]* |  |  | 1.99 [1.52, 2.39] | -0.20 [-0.45, 0.02] | -0.94 [-1.30, -0.58]* | -1.34 [-1.70, -1.02]* | -0.08 [-0.12, -0.05]* | 10.18 [7.26, 12.95] | -1.32 [-1.68, -0.97]* | -0.57 [-1.04, -0.08]* | toto2_nocov |
| 2 | -1.45 [-1.97, -0.97]* | -0.18 [-0.53, 0.08] | -1.80 [-2.32, -1.33]* | 0.19 [-0.39, 0.88] | -1.05 [-1.59, -0.44]* | 4.69 [2.92, 6.95] | -1.06 [-1.57, -0.50]* | -1.36 [-1.96, -0.74]* | -5.87 [-6.81, -4.91]* | -4.46 [-5.16, -3.72]* |  |  | 1.39 [0.90, 1.88] | -0.35 [-0.60, -0.13]* | -1.11 [-1.43, -0.81]* | -1.69 [-2.19, -1.23]* | -0.08 [-0.12, -0.05]* | 9.75 [6.93, 12.49] | -1.50 [-1.97, -1.06]* | -0.88 [-1.40, -0.26]* | toto2_nocov |
| 3 | -1.53 [-2.07, -1.05]* | -0.37 [-0.79, -0.04]* | -1.99 [-2.55, -1.48]* | -0.19 [-0.78, 0.53] | -1.49 [-2.00, -0.87]* | 3.67 [2.25, 5.06] | -1.27 [-1.79, -0.81]* | -1.25 [-1.70, -0.79]* | -6.22 [-7.28, -5.15]* | -4.67 [-5.25, -4.08]* | 0.01 [-0.67, 0.68] | -0.04 [-0.72, 0.63] | 0.68 [0.11, 1.16] | -0.51 [-0.82, -0.26]* | -1.18 [-1.58, -0.78]* | -1.91 [-2.44, -1.42]* | -0.09 [-0.14, -0.05]* | 9.14 [6.59, 11.60] | -1.77 [-2.30, -1.25]* | -1.28 [-1.76, -0.71]* | toto2_nocov |
| 4 | -1.66 [-2.22, -1.12]* | -0.26 [-0.64, 0.07] | -2.35 [-2.99, -1.74]* | -0.72 [-1.34, 0.01] | -2.02 [-2.63, -1.25]* | 3.62 [2.09, 5.07] | -1.77 [-2.46, -1.04]* | -1.61 [-2.15, -1.04]* | -6.62 [-7.77, -5.38]* | -5.12 [-5.96, -4.10]* | -0.37 [-1.16, 0.47] | -0.44 [-1.23, 0.42] | 0.41 [-0.23, 1.03] | -0.50 [-0.82, -0.22]* | -1.28 [-1.77, -0.78]* | -2.29 [-2.86, -1.72]* | -0.10 [-0.13, -0.07]* | 9.18 [6.37, 11.67] | -2.02 [-2.56, -1.49]* | -1.85 [-2.45, -1.08]* | toto2_nocov |
| 5 | -1.51 [-1.99, -1.05]* | -0.22 [-0.57, 0.09] | -2.07 [-2.72, -1.50]* | -0.43 [-0.97, 0.18] | -1.58 [-2.26, -0.86]* | 4.06 [2.37, 6.07] | -1.50 [-2.04, -0.94]* | -1.38 [-2.00, -0.72]* | -6.46 [-7.85, -5.14]* | -4.83 [-5.56, -4.07]* | 0.05 [-0.79, 0.90] | -0.02 [-0.86, 0.82] | 0.91 [0.38, 1.41] | -0.33 [-0.61, -0.08]* | -1.11 [-1.53, -0.67]* | -2.04 [-2.67, -1.48]* | -0.07 [-0.11, -0.04]* | 9.01 [6.34, 11.56] | -1.80 [-2.37, -1.28]* | -1.39 [-2.08, -0.67]* | toto2_nocov |
| 6 | -1.49 [-2.08, -1.02]* | -0.28 [-0.75, 0.06] | -2.19 [-2.92, -1.62]* | -0.24 [-0.84, 0.42] | -1.55 [-2.09, -0.91]* | 3.54 [2.10, 4.96] | -1.74 [-2.35, -1.17]* | -1.33 [-1.96, -0.75]* | -6.01 [-6.95, -4.99]* | -5.13 [-5.88, -4.41]* | 0.05 [-0.88, 0.90] | -0.01 [-0.95, 0.86] | 0.96 [0.58, 1.33] | -0.45 [-0.79, -0.19]* | -1.39 [-1.97, -0.90]* | -2.10 [-2.74, -1.55]* | -0.07 [-0.13, -0.03]* | 8.62 [6.00, 11.08] | -1.93 [-2.54, -1.34]* | -1.38 [-1.91, -0.73]* | toto2_nocov |
| 7 | -1.64 [-2.20, -1.12]* | -0.43 [-0.85, -0.12]* | -2.37 [-3.12, -1.74]* | -0.84 [-1.47, -0.16]* | -1.84 [-2.53, -1.05]* | 3.68 [2.03, 5.58] | -1.59 [-2.28, -0.91]* | -1.99 [-2.83, -1.17]* | -6.95 [-8.13, -5.75]* | -4.78 [-5.66, -3.88]* | -0.77 [-1.86, 0.28] | -0.83 [-1.94, 0.24] |  | -0.53 [-0.94, -0.18]* | -1.17 [-1.67, -0.72]* | -2.28 [-3.00, -1.65]* | -0.08 [-0.11, -0.05]* | 8.42 [5.85, 10.89] | -1.90 [-2.55, -1.33]* | -1.65 [-2.33, -0.84]* | toto2_nocov |

### Paired against `tirex2_grid` (not the per-lead reference)

MAE difference model − `tirex2_grid` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.06 [-0.14, 0.03] | 0.36 [0.24, 0.51] |  | 1.13 [0.85, 1.42] | 0.49 [0.32, 0.67] | 3.30 [2.48, 4.31] | 0.85 [0.65, 1.05] | 0.57 [0.31, 0.83] | -1.40 [-1.87, -1.04]* | -0.63 [-0.94, -0.28]* |  |  | 1.81 [1.36, 2.25] | 0.44 [0.32, 0.57] |  | 0.43 [0.31, 0.57] | 4.84 [3.34, 6.43] | 0.46 [0.33, 0.61] |  | 0.53 [0.34, 0.72] | tirex2_grid |
| 1 | -0.08 [-0.19, 0.04] | 0.39 [0.19, 0.60] | -0.07 [-0.22, 0.04] | 0.65 [0.43, 0.88] | 0.05 [-0.10, 0.20] | 2.94 [2.32, 3.70] | 0.34 [0.18, 0.49] | 0.48 [0.29, 0.66] | -1.68 [-2.10, -1.32]* | -1.16 [-1.47, -0.82]* |  |  | 1.45 [1.14, 1.77] | 0.37 [0.21, 0.53] | 0.00 [-0.10, 0.10] | 0.42 [0.26, 0.59] | 4.76 [3.45, 6.23] | 0.45 [0.29, 0.63] | 0.05 [-0.08, 0.16] | 0.10 [-0.05, 0.26] | tirex2_grid |
| 2 | -0.08 [-0.21, 0.03] | 0.46 [0.27, 0.66] | -0.11 [-0.24, 0.01] | 0.60 [0.38, 0.86] | 0.01 [-0.15, 0.19] | 3.17 [2.42, 4.13] | 0.23 [0.09, 0.41] | 0.34 [0.18, 0.51] | -2.00 [-2.46, -1.60]* | -1.26 [-1.66, -0.85]* |  |  | 1.21 [0.91, 1.52] | 0.38 [0.25, 0.53] | -0.06 [-0.17, 0.05] | 0.51 [0.36, 0.68] | 4.70 [3.41, 6.09] | 0.55 [0.40, 0.72] | 0.04 [-0.08, 0.17] | 0.06 [-0.09, 0.26] | tirex2_grid |
| 3 | -0.04 [-0.13, 0.04] | 0.45 [0.27, 0.62] | -0.11 [-0.26, 0.04] | 0.53 [0.25, 0.88] | -0.09 [-0.29, 0.12] | 2.80 [2.18, 3.54] | 0.18 [0.02, 0.37] | 0.40 [0.18, 0.61] | -2.08 [-2.60, -1.61]* | -1.34 [-1.72, -0.96]* | 0.99 [0.59, 1.33] | 0.97 [0.57, 1.31] | 0.91 [0.69, 1.16] | 0.37 [0.24, 0.50] | -0.08 [-0.22, 0.06] | 0.57 [0.41, 0.76] | 4.50 [3.31, 5.80] | 0.61 [0.44, 0.80] | -0.01 [-0.15, 0.14] | -0.02 [-0.20, 0.19] | tirex2_grid |
| 4 | -0.08 [-0.21, 0.04] | 0.51 [0.28, 0.73] | -0.25 [-0.46, -0.04]* | 0.31 [0.02, 0.63] | -0.29 [-0.53, -0.04]* | 2.80 [2.20, 3.48] | 0.01 [-0.22, 0.24] | 0.22 [-0.01, 0.49] | -2.24 [-2.86, -1.73]* | -1.49 [-1.97, -0.97]* | 0.83 [0.43, 1.22] | 0.81 [0.41, 1.19] | 0.80 [0.58, 1.01] | 0.39 [0.21, 0.57] | -0.22 [-0.42, -0.03]* | 0.58 [0.38, 0.81] | 4.57 [3.26, 5.90] | 0.63 [0.41, 0.85] | -0.10 [-0.29, 0.07] | -0.23 [-0.46, 0.02] | tirex2_grid |
| 5 | -0.10 [-0.23, 0.02] | 0.45 [0.24, 0.68] | -0.24 [-0.45, -0.06]* | 0.35 [0.15, 0.60] | -0.21 [-0.41, 0.01] | 2.92 [2.22, 3.85] | -0.00 [-0.15, 0.15] | 0.32 [0.05, 0.61] | -2.24 [-2.82, -1.69]* | -1.53 [-1.95, -1.10]* | 1.01 [0.63, 1.36] | 0.99 [0.61, 1.33] | 0.89 [0.59, 1.14] | 0.39 [0.20, 0.58] | -0.24 [-0.43, -0.07]* | 0.50 [0.31, 0.70] | 4.36 [3.04, 5.74] | 0.53 [0.33, 0.74] | -0.13 [-0.28, 0.02] | -0.15 [-0.36, 0.08] | tirex2_grid |
| 6 | 0.01 [-0.14, 0.15] | 0.54 [0.29, 0.78] | -0.18 [-0.28, -0.08]* | 0.53 [0.27, 0.84] | -0.10 [-0.25, 0.07] | 2.90 [2.32, 3.57] | 0.06 [-0.02, 0.18] | 0.54 [0.35, 0.75] | -1.93 [-2.33, -1.56]* | -1.49 [-1.85, -1.11]* | 1.29 [0.92, 1.61] | 1.27 [0.90, 1.60] | 1.13 [0.90, 1.38] | 0.46 [0.25, 0.69] | -0.15 [-0.24, -0.06]* | 0.60 [0.37, 0.86] | 4.27 [2.98, 5.64] | 0.63 [0.39, 0.90] | -0.05 [-0.15, 0.07] | -0.04 [-0.18, 0.13] | tirex2_grid |
| 7 | -0.13 [-0.29, 0.02] | 0.38 [0.16, 0.60] | -0.36 [-0.55, -0.21]* | 0.17 [-0.03, 0.44] | -0.32 [-0.51, -0.09]* | 2.94 [2.30, 3.75] | 0.00 [-0.18, 0.19] | 0.27 [0.05, 0.48] | -2.50 [-3.03, -2.04]* | -1.42 [-1.88, -0.97]* | 0.87 [0.55, 1.18] | 0.85 [0.53, 1.17] |  | 0.33 [0.14, 0.51] | -0.33 [-0.51, -0.16]* | 0.51 [0.31, 0.74] | 4.12 [2.90, 5.42] | 0.54 [0.34, 0.78] | -0.15 [-0.30, 0.01] | -0.24 [-0.43, -0.01]* | tirex2_grid |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.23 [-0.42, -0.05]* | 0.76 [0.49, 1.06] |  | 2.53 [2.00, 3.04] | 1.18 [0.84, 1.60] | 6.42 [4.27, 8.80] | 1.60 [1.22, 2.00] | 0.74 [0.13, 1.25] | -3.46 [-4.51, -2.55]* | -1.69 [-2.13, -1.20]* |  |  | 3.93 [3.05, 4.65] | 0.86 [0.63, 1.12] |  | 0.82 [0.62, 1.05] | 11.35 [7.91, 14.58] | 0.91 [0.71, 1.15] |  | 1.32 [0.95, 1.74] | tirex2_grid |
| 1 | -0.24 [-0.57, 0.01] | 0.82 [0.32, 1.27] | -0.55 [-0.95, -0.25]* | 1.52 [1.06, 2.03] | 0.23 [-0.15, 0.61] | 5.46 [3.83, 7.14] | 0.36 [-0.08, 0.75] | 0.24 [-0.23, 0.61] | -4.02 [-4.86, -3.20]* | -2.97 [-3.45, -2.53]* |  |  | 2.92 [2.40, 3.33] | 0.74 [0.37, 1.08] | -0.38 [-0.66, -0.16]* | 0.85 [0.48, 1.23] | 11.13 [8.08, 13.92] | 0.94 [0.58, 1.30] | -0.36 [-0.67, -0.10]* | 0.38 [0.00, 0.77] | tirex2_grid |
| 2 | -0.33 [-0.68, -0.05]* | 0.94 [0.45, 1.40] | -0.67 [-0.98, -0.39]* | 1.30 [0.83, 1.93] | 0.08 [-0.30, 0.56] | 5.99 [3.99, 8.41] | 0.08 [-0.22, 0.46] | 0.01 [-0.39, 0.41] | -4.74 [-5.69, -3.83]* | -3.32 [-3.81, -2.76]* |  |  | 2.43 [1.96, 2.88] | 0.77 [0.45, 1.10] | -0.56 [-0.82, -0.31]* | 1.02 [0.72, 1.36] | 10.87 [7.84, 13.69] | 1.11 [0.81, 1.43] | -0.37 [-0.62, -0.12]* | 0.24 [-0.13, 0.71] | tirex2_grid |
| 3 | -0.33 [-0.55, -0.14]* | 0.83 [0.41, 1.22] | -0.78 [-1.08, -0.45]* | 1.00 [0.31, 1.90] | -0.28 [-0.72, 0.29] | 4.86 [3.27, 6.52] | -0.06 [-0.38, 0.34] | 0.03 [-0.44, 0.50] | -5.00 [-6.02, -4.00]* | -3.47 [-3.84, -3.00]* | 1.27 [0.37, 2.02] | 1.22 [0.33, 1.96] | 1.82 [1.43, 2.24] | 0.69 [0.39, 0.98] | -0.70 [-0.98, -0.37]* | 1.10 [0.70, 1.47] | 10.34 [7.66, 12.92] | 1.18 [0.78, 1.58] | -0.57 [-0.83, -0.26]* | -0.08 [-0.50, 0.49] | tirex2_grid |
| 4 | -0.35 [-0.67, -0.08]* | 1.05 [0.51, 1.50] | -1.05 [-1.50, -0.59]* | 0.56 [-0.19, 1.49] | -0.72 [-1.29, -0.05]* | 5.02 [3.39, 6.74] | -0.46 [-0.99, 0.14] | -0.24 [-0.79, 0.28] | -5.32 [-6.48, -4.17]* | -3.83 [-4.51, -2.97]* | 1.00 [0.12, 1.84] | 0.94 [0.06, 1.77] | 1.59 [1.19, 2.02] | 0.79 [0.38, 1.21] | -0.98 [-1.39, -0.57]* | 1.19 [0.70, 1.66] | 10.48 [7.61, 13.17] | 1.28 [0.78, 1.77] | -0.72 [-1.08, -0.38]* | -0.57 [-1.13, 0.13] | tirex2_grid |
| 5 | -0.38 [-0.66, -0.11]* | 0.91 [0.45, 1.36] | -0.94 [-1.37, -0.51]* | 0.71 [0.17, 1.23] | -0.45 [-0.96, 0.15] | 5.46 [3.51, 7.88] | -0.37 [-0.73, 0.00] | 0.01 [-0.65, 0.68] | -5.35 [-6.67, -4.09]* | -3.74 [-4.23, -3.18]* | 1.49 [0.65, 2.28] | 1.42 [0.57, 2.20] | 1.85 [1.35, 2.28] | 0.80 [0.37, 1.20] | -0.90 [-1.32, -0.50]* | 1.03 [0.61, 1.45] | 10.14 [7.27, 12.81] | 1.11 [0.67, 1.53] | -0.68 [-0.99, -0.39]* | -0.27 [-0.78, 0.33] | tirex2_grid |
| 6 | -0.08 [-0.43, 0.21] | 1.13 [0.59, 1.56] | -0.77 [-0.98, -0.53]* | 1.18 [0.51, 2.09] | -0.13 [-0.52, 0.35] | 5.24 [3.71, 6.86] | -0.31 [-0.50, -0.07]* | 0.31 [-0.02, 0.65] | -4.63 [-5.37, -3.83]* | -3.74 [-4.02, -3.37]* | 1.74 [1.14, 2.36] | 1.68 [1.06, 2.30] | 2.19 [1.83, 2.53] | 0.96 [0.47, 1.40] | -0.67 [-0.86, -0.46]* | 1.32 [0.86, 1.86] | 10.04 [7.09, 12.91] | 1.39 [0.90, 1.97] | -0.51 [-0.74, -0.25]* | 0.04 [-0.36, 0.50] | tirex2_grid |
| 7 | -0.45 [-0.84, -0.10]* | 0.77 [0.27, 1.19] | -1.16 [-1.56, -0.77]* | 0.36 [-0.20, 1.00] | -0.63 [-1.07, -0.07]* | 5.32 [3.58, 7.43] | -0.35 [-0.78, 0.10] | -0.15 [-0.63, 0.34] | -5.80 [-6.85, -4.75]* | -3.62 [-4.27, -2.99]* | 1.07 [0.43, 1.78] | 1.02 [0.35, 1.72] |  | 0.67 [0.24, 1.00] | -1.07 [-1.43, -0.68]* | 1.10 [0.65, 1.58] | 9.62 [6.76, 12.35] | 1.17 [0.72, 1.67] | -0.71 [-1.02, -0.37]* | -0.44 [-0.88, 0.14] | tirex2_grid |

### Paired against `toto2_grid` (not the per-lead reference)

MAE difference model − `toto2_grid` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -4.89 [-6.50, -3.39]* | -4.47 [-5.97, -3.06]* |  | -3.84 [-5.25, -2.47]* | -4.41 [-5.94, -2.91]* | -1.35 [-2.30, -0.47]* | -4.12 [-5.64, -2.66]* | -3.97 [-5.60, -2.57]* | -6.31 [-8.19, -4.61]* | -5.60 [-7.42, -3.93]* |  |  | -3.31 [-4.64, -2.06]* | -4.40 [-5.90, -2.99]* | -4.84 [-6.43, -3.34]* |  | -4.41 [-5.97, -2.98]* | -4.37 [-5.94, -2.94]* |  | -4.38 [-5.91, -2.89]* | toto2_grid |
| 1 | -4.83 [-6.22, -3.55]* | -4.37 [-5.69, -3.14]* | -4.88 [-6.37, -3.49]* | -4.28 [-5.64, -3.04]* | -4.76 [-6.20, -3.43]* | -1.64 [-2.57, -0.79]* | -4.56 [-6.01, -3.18]* | -4.05 [-5.43, -2.83]* | -6.50 [-8.19, -4.95]* | -6.07 [-7.76, -4.51]* |  |  | -3.56 [-4.89, -2.31]* | -4.39 [-5.73, -3.15]* | -4.76 [-6.23, -3.45]* | -4.80 [-6.29, -3.45]* | -4.34 [-5.71, -3.08]* | -4.30 [-5.66, -3.04]* | -4.76 [-6.24, -3.38]* | -4.73 [-6.16, -3.40]* | toto2_grid |
| 2 | -4.78 [-6.19, -3.47]* | -4.25 [-5.53, -3.03]* | -4.87 [-6.35, -3.47]* | -4.24 [-5.63, -2.91]* | -4.75 [-6.22, -3.36]* | -1.54 [-2.49, -0.73]* | -4.58 [-6.04, -3.18]* | -4.34 [-5.93, -2.94]* | -6.77 [-8.52, -5.21]* | -6.08 [-7.87, -4.41]* |  |  | -3.73 [-4.99, -2.52]* | -4.32 [-5.65, -3.09]* | -4.70 [-6.09, -3.41]* | -4.81 [-6.30, -3.45]* | -4.19 [-5.50, -2.98]* | -4.15 [-5.47, -2.95]* | -4.72 [-6.20, -3.33]* | -4.71 [-6.19, -3.31]* | toto2_grid |
| 3 | -4.54 [-5.84, -3.37]* | -4.05 [-5.25, -2.93]* | -4.66 [-6.04, -3.37]* | -4.10 [-5.34, -2.95]* | -4.64 [-5.98, -3.39]* | -1.54 [-2.38, -0.71]* | -4.41 [-5.69, -3.22]* | -3.82 [-5.12, -2.69]* | -6.64 [-8.35, -5.10]* | -5.94 [-7.48, -4.48]* | -3.24 [-4.48, -2.09]* | -3.26 [-4.50, -2.11]* | -3.82 [-5.01, -2.67]* | -4.13 [-5.33, -3.00]* | -4.50 [-5.80, -3.31]* | -4.63 [-6.00, -3.35]* | -3.92 [-5.10, -2.82]* | -3.88 [-5.06, -2.79]* | -4.56 [-5.93, -3.27]* | -4.59 [-5.91, -3.35]* | toto2_grid |
| 4 | -4.64 [-5.99, -3.39]* | -4.06 [-5.22, -2.92]* | -4.87 [-6.40, -3.46]* | -4.34 [-5.71, -3.07]* | -4.90 [-6.45, -3.52]* | -1.70 [-2.69, -0.85]* | -4.67 [-6.22, -3.23]* | -4.15 [-5.61, -2.85]* | -6.87 [-8.73, -5.26]* | -6.17 [-8.00, -4.50]* | -3.57 [-5.04, -2.24]* | -3.59 [-5.08, -2.25]* | -3.96 [-5.24, -2.70]* | -4.18 [-5.39, -3.00]* | -4.57 [-5.90, -3.26]* | -4.84 [-6.36, -3.45]* | -3.98 [-5.23, -2.82]* | -3.94 [-5.19, -2.79]* | -4.72 [-6.20, -3.31]* | -4.87 [-6.42, -3.49]* | toto2_grid |
| 5 | -4.45 [-5.83, -3.16]* | -3.91 [-5.14, -2.74]* | -4.66 [-6.17, -3.25]* | -4.16 [-5.60, -2.81]* | -4.62 [-6.14, -3.18]* | -1.52 [-2.41, -0.67]* | -4.47 [-5.98, -3.06]* | -4.01 [-5.49, -2.69]* | -6.66 [-8.53, -5.02]* | -5.99 [-7.80, -4.33]* | -3.37 [-4.87, -2.02]* | -3.39 [-4.91, -2.03]* | -3.67 [-4.97, -2.44]* | -3.96 [-5.20, -2.80]* | -4.36 [-5.74, -3.04]* | -4.65 [-6.16, -3.22]* | -3.85 [-5.09, -2.70]* | -3.82 [-5.05, -2.67]* | -4.54 [-6.03, -3.10]* | -4.59 [-6.10, -3.15]* | toto2_grid |
| 6 | -4.25 [-5.58, -3.00]* | -3.73 [-4.93, -2.60]* | -4.51 [-6.01, -3.15]* | -3.91 [-5.32, -2.67]* | -4.42 [-5.86, -3.10]* | -1.23 [-2.25, -0.30]* | -4.31 [-5.77, -2.93]* | -3.49 [-4.90, -2.26]* | -6.27 [-7.96, -4.72]* | -5.85 [-7.60, -4.25]* | -2.78 [-4.21, -1.50]* | -2.80 [-4.24, -1.51]* | -3.28 [-4.58, -2.02]* | -3.80 [-5.02, -2.67]* | -4.27 [-5.64, -2.98]* | -4.48 [-5.96, -3.12]* | -3.66 [-4.87, -2.55]* | -3.63 [-4.83, -2.53]* | -4.37 [-5.88, -2.99]* | -4.39 [-5.83, -3.06]* | toto2_grid |
| 7 | -4.26 [-5.55, -3.05]* | -3.74 [-4.90, -2.66]* | -4.56 [-5.99, -3.19]* | -4.09 [-5.44, -2.79]* | -4.51 [-5.93, -3.15]* | -1.31 [-2.37, -0.34]* | -4.25 [-5.68, -2.93]* | -3.97 [-5.53, -2.59]* | -6.68 [-8.39, -5.08]* | -5.67 [-7.42, -4.00]* | -3.40 [-4.93, -1.98]* | -3.42 [-4.96, -1.99]* |  | -3.79 [-4.96, -2.68]* | -4.12 [-5.42, -2.90]* | -4.52 [-5.96, -3.15]* | -3.61 [-4.72, -2.52]* | -3.57 [-4.68, -2.49]* | -4.34 [-5.74, -3.00]* | -4.46 [-5.88, -3.10]* | toto2_grid |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -11.58 [-14.80, -8.10]* | -10.59 [-13.66, -7.31]* |  | -9.08 [-12.13, -5.83]* | -10.27 [-13.56, -6.78]* | -4.64 [-6.19, -2.78]* | -10.00 [-13.21, -6.68]* | -10.10 [-13.43, -6.66]* | -14.92 [-18.64, -11.11]* | -13.30 [-16.61, -9.77]* |  |  | -7.99 [-10.79, -4.92]* | -10.49 [-13.55, -7.26]* | -11.35 [-14.58, -7.91]* |  | -10.53 [-13.68, -7.08]* | -10.43 [-13.58, -7.00]* |  | -10.17 [-13.45, -6.66]* | toto2_grid |
| 1 | -11.37 [-14.21, -8.41]* | -10.31 [-13.02, -7.49]* | -11.76 [-14.76, -8.62]* | -9.99 [-12.65, -7.14]* | -10.98 [-13.98, -7.89]* | -5.44 [-6.93, -3.64]* | -11.03 [-14.03, -7.90]* | -10.54 [-13.37, -7.62]* | -15.24 [-18.60, -11.88]* | -14.37 [-17.41, -11.09]* |  |  | -8.73 [-11.53, -5.79]* | -10.38 [-13.07, -7.59]* | -11.13 [-13.92, -8.08]* | -11.59 [-14.51, -8.52]* | -10.27 [-13.05, -7.32]* | -10.18 [-12.95, -7.26]* | -11.57 [-14.53, -8.46]* | -10.87 [-13.85, -7.76]* | toto2_grid |
| 2 | -11.19 [-14.11, -8.15]* | -9.93 [-12.62, -7.17]* | -11.64 [-14.73, -8.38]* | -9.89 [-12.88, -6.69]* | -10.89 [-13.99, -7.59]* | -5.16 [-6.57, -3.48]* | -10.99 [-14.03, -7.75]* | -11.15 [-14.38, -7.68]* | -15.72 [-19.01, -12.25]* | -14.40 [-17.68, -10.97]* |  |  | -8.92 [-11.60, -5.96]* | -10.09 [-12.82, -7.32]* | -10.87 [-13.69, -7.84]* | -11.53 [-14.56, -8.33]* | -9.83 [-12.57, -7.02]* | -9.75 [-12.49, -6.93]* | -11.33 [-14.38, -8.16]* | -10.77 [-13.87, -7.45]* | toto2_grid |
| 3 | -10.67 [-13.25, -7.94]* | -9.51 [-12.00, -7.02]* | -11.21 [-13.96, -8.35]* | -9.57 [-11.95, -6.91]* | -10.70 [-13.36, -7.87]* | -5.25 [-6.55, -3.65]* | -10.53 [-13.01, -7.85]* | -9.87 [-12.32, -7.14]* | -15.43 [-18.62, -12.13]* | -13.93 [-16.66, -11.02]* | -8.66 [-11.17, -5.79]* | -8.71 [-11.23, -5.83]* | -9.05 [-11.52, -6.35]* | -9.65 [-12.16, -7.15]* | -10.34 [-12.92, -7.66]* | -11.12 [-13.87, -8.26]* | -9.23 [-11.68, -6.64]* | -9.14 [-11.60, -6.59]* | -10.99 [-13.67, -8.17]* | -10.54 [-13.14, -7.72]* | toto2_grid |
| 4 | -10.83 [-13.52, -7.92]* | -9.43 [-11.90, -6.86]* | -11.62 [-14.55, -8.46]* | -10.11 [-12.73, -7.23]* | -11.29 [-14.24, -8.00]* | -5.39 [-6.84, -3.63]* | -11.14 [-14.14, -7.77]* | -10.41 [-13.19, -7.25]* | -15.90 [-19.30, -12.17]* | -14.50 [-17.65, -10.93]* | -9.30 [-12.32, -5.90]* | -9.37 [-12.41, -5.94]* | -9.36 [-12.09, -6.37]* | -9.68 [-12.11, -7.06]* | -10.48 [-13.17, -7.61]* | -11.55 [-14.47, -8.42]* | -9.28 [-11.76, -6.47]* | -9.18 [-11.67, -6.37]* | -11.29 [-14.21, -8.18]* | -11.19 [-14.18, -7.88]* | toto2_grid |
| 5 | -10.52 [-13.41, -7.60]* | -9.23 [-11.73, -6.68]* | -11.17 [-14.24, -7.95]* | -9.80 [-12.76, -6.55]* | -10.68 [-13.80, -7.28]* | -5.12 [-6.54, -3.40]* | -10.72 [-13.74, -7.49]* | -10.37 [-13.55, -7.08]* | -15.60 [-19.29, -11.86]* | -14.08 [-17.30, -10.68]* | -9.09 [-12.35, -5.62]* | -9.16 [-12.45, -5.67]* | -8.75 [-11.37, -5.89]* | -9.34 [-11.82, -6.69]* | -10.14 [-12.81, -7.27]* | -11.13 [-14.15, -7.92]* | -9.08 [-11.65, -6.39]* | -9.01 [-11.56, -6.34]* | -10.90 [-13.80, -7.79]* | -10.55 [-13.70, -7.13]* | toto2_grid |
| 6 | -10.11 [-12.97, -7.29]* | -8.90 [-11.45, -6.36]* | -10.89 [-13.91, -7.76]* | -9.24 [-11.94, -6.28]* | -10.25 [-13.14, -7.07]* | -4.75 [-6.42, -2.76]* | -10.54 [-13.54, -7.38]* | -9.41 [-12.36, -6.33]* | -14.76 [-18.07, -11.39]* | -13.97 [-17.04, -10.70]* | -8.17 [-11.27, -4.92]* | -8.22 [-11.35, -4.96]* | -8.23 [-11.00, -5.17]* | -9.07 [-11.62, -6.52]* | -10.04 [-12.91, -7.09]* | -10.80 [-13.81, -7.63]* | -8.69 [-11.15, -6.06]* | -8.62 [-11.08, -6.00]* | -10.63 [-13.64, -7.46]* | -10.14 [-13.05, -6.92]* | toto2_grid |
| 7 | -10.07 [-12.84, -7.20]* | -8.85 [-11.30, -6.30]* | -10.90 [-13.93, -7.75]* | -9.62 [-12.51, -6.53]* | -10.36 [-13.40, -7.10]* | -4.85 [-6.99, -2.78]* | -10.27 [-13.21, -7.12]* | -10.44 [-13.76, -6.97]* | -15.56 [-18.89, -12.22]* | -13.54 [-16.67, -10.20]* | -9.35 [-12.80, -5.65]* | -9.40 [-12.88, -5.69]* |  | -8.95 [-11.46, -6.33]* | -9.62 [-12.35, -6.76]* | -10.80 [-13.85, -7.61]* | -8.50 [-10.99, -5.91]* | -8.42 [-10.89, -5.85]* | -10.43 [-13.39, -7.32]* | -10.23 [-13.32, -7.00]* | toto2_grid |

### Paired against `chronos2` (not the per-lead reference)

MAE difference model − `chronos2` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.42 [0.31, 0.56] |  | 1.20 [0.89, 1.48] | 0.53 [0.36, 0.71] | 3.36 [2.53, 4.36] | 0.91 [0.69, 1.13] | 0.63 [0.37, 0.89] | -1.36 [-1.81, -1.01]* | -0.56 [-0.91, -0.21]* |  |  |  | 1.87 [1.42, 2.31] | 0.50 [0.37, 0.62] | 0.06 [-0.03, 0.14] |  | 0.49 [0.36, 0.63] | 4.89 [3.39, 6.50] | 0.52 [0.38, 0.67] |  | 0.58 [0.39, 0.76] | chronos2 |
| m015 |  |  | 4.56 [3.70, 5.46] | 4.01 [3.26, 4.81] |  | 4.30 [3.47, 5.14] | 3.90 [3.17, 4.73] |  |  | -0.27 [-0.37, -0.19]* |  |  | 5.29 [4.31, 6.32] | -0.07 [-0.12, -0.03]* |  |  | -0.18 [-0.24, -0.11]* |  |  |  | 4.00 [3.28, 4.80] | chronos2 |
| m030 |  |  | 3.58 [2.90, 4.31] | 3.02 [2.42, 3.65] |  | 3.34 [2.67, 4.01] | 2.96 [2.40, 3.58] |  |  | 0.41 [0.20, 0.60] |  |  | 4.35 [3.49, 5.25] | 0.16 [0.09, 0.23] |  |  | 0.01 [-0.04, 0.07] |  |  |  | 3.06 [2.46, 3.70] | chronos2 |
| m045 |  |  | 3.22 [2.62, 3.86] | 2.63 [2.11, 3.16] |  | 2.96 [2.36, 3.57] | 2.60 [2.09, 3.16] |  |  | 0.90 [0.61, 1.17] |  |  | 3.92 [3.16, 4.77] | 0.30 [0.19, 0.42] |  |  | 0.09 [0.04, 0.14] |  |  |  | 2.69 [2.16, 3.24] | chronos2 |
| m060 |  |  | 3.05 [2.46, 3.68] | 2.48 [1.97, 3.00] |  | 2.78 [2.20, 3.37] | 2.44 [1.93, 2.98] |  |  | 1.27 [0.91, 1.62] |  |  | 3.78 [3.03, 4.59] | 0.41 [0.27, 0.56] |  |  | 0.15 [0.10, 0.21] |  |  |  | 2.51 [2.00, 3.03] | chronos2 |
| m075 |  |  | 2.83 [2.24, 3.43] | 2.21 [1.76, 2.68] |  | 2.54 [2.02, 3.06] | 2.19 [1.73, 2.69] |  |  | 1.54 [1.13, 1.95] |  |  | 3.55 [2.82, 4.32] | 0.48 [0.35, 0.63] |  |  | 0.21 [0.15, 0.28] |  |  |  | 2.25 [1.78, 2.76] | chronos2 |
| m090 |  |  | 2.72 [2.15, 3.32] | 2.05 [1.61, 2.49] |  | 2.40 [1.92, 2.90] | 2.08 [1.65, 2.53] |  |  | 1.88 [1.39, 2.36] |  |  | 3.40 [2.71, 4.12] | 0.52 [0.37, 0.66] |  |  | 0.26 [0.19, 0.34] |  |  |  | 2.13 [1.67, 2.58] | chronos2 |
| m105 |  |  | 2.65 [2.10, 3.23] | 2.00 [1.55, 2.45] |  | 2.37 [1.86, 2.89] | 1.99 [1.56, 2.44] |  |  | 2.06 [1.54, 2.63] |  |  | 3.36 [2.64, 4.11] | 0.53 [0.39, 0.70] |  |  | 0.27 [0.19, 0.36] |  |  |  | 2.07 [1.61, 2.52] | chronos2 |
| m120 |  |  | 2.60 [2.07, 3.19] | 1.95 [1.51, 2.38] |  | 2.27 [1.79, 2.75] | 1.91 [1.49, 2.35] |  |  | 2.25 [1.71, 2.83] |  |  | 3.26 [2.59, 3.97] | 0.58 [0.42, 0.78] |  |  | 0.30 [0.21, 0.40] |  |  |  | 2.00 [1.56, 2.44] | chronos2 |
| h2-4 |  |  | 2.22 [1.74, 2.71] | 1.58 [1.20, 1.93] |  | 1.94 [1.50, 2.41] | 1.59 [1.22, 1.99] |  |  | 2.99 [2.33, 3.70] |  |  | 2.90 [2.25, 3.62] | 0.58 [0.43, 0.74] |  |  | 0.32 [0.22, 0.43] |  |  |  | 1.62 [1.24, 1.98] | chronos2 |
| h4-8 |  |  | 1.76 [1.37, 2.14] | 1.09 [0.82, 1.35] |  | 1.48 [1.13, 1.83] | 1.18 [0.85, 1.52] |  |  | 4.59 [3.65, 5.56] |  |  | 2.46 [1.89, 3.08] | 0.53 [0.38, 0.69] |  |  | 0.31 [0.20, 0.42] |  |  |  | 1.16 [0.88, 1.42] | chronos2 |
| h8+ |  |  | 1.05 [0.80, 1.31] | 0.45 [0.32, 0.59] |  | 0.89 [0.71, 1.07] | 0.82 [0.56, 1.07] |  |  | 6.50 [4.97, 8.65] |  |  | 1.87 [1.41, 2.35] | 0.44 [0.27, 0.64] |  |  | 0.32 [0.20, 0.46] |  |  |  | 0.53 [0.38, 0.69] | chronos2 |
| 1 | 0.47 [0.32, 0.62] | -0.00 [-0.14, 0.14] | 0.73 [0.47, 1.00] | 0.12 [-0.03, 0.29] | 2.98 [2.34, 3.77] | 0.41 [0.27, 0.57] | 0.47 [0.27, 0.68] | -1.61 [-2.02, -1.25]* | -1.09 [-1.45, -0.71]* |  |  |  | 1.53 [1.26, 1.82] | 0.44 [0.33, 0.56] | 0.08 [-0.04, 0.19] | 0.08 [-0.04, 0.20] | 0.49 [0.35, 0.65] | 4.83 [3.55, 6.22] | 0.53 [0.38, 0.69] | 0.12 [-0.02, 0.27] | 0.17 [0.02, 0.35] | chronos2 |
| 2 | 0.54 [0.35, 0.75] | -0.03 [-0.15, 0.10] | 0.69 [0.44, 1.01] | 0.08 [-0.10, 0.32] | 3.11 [2.32, 4.11] | 0.32 [0.15, 0.50] | 0.30 [0.16, 0.45] | -1.92 [-2.40, -1.50]* | -1.18 [-1.61, -0.77]* |  |  |  | 1.29 [0.98, 1.65] | 0.46 [0.30, 0.64] | 0.08 [-0.03, 0.21] | 0.02 [-0.11, 0.17] | 0.59 [0.39, 0.83] | 4.78 [3.47, 6.19] | 0.63 [0.42, 0.87] | 0.12 [-0.01, 0.29] | 0.14 [-0.04, 0.39] | chronos2 |
| 3 | 0.49 [0.32, 0.70] | -0.07 [-0.20, 0.07] | 0.58 [0.30, 0.92] | -0.05 [-0.24, 0.18] | 2.71 [2.05, 3.47] | 0.22 [0.06, 0.43] | 0.31 [0.10, 0.55] | -2.04 [-2.55, -1.60]* | -1.30 [-1.66, -0.92]* |  | 0.91 [0.51, 1.27] | 0.89 [0.49, 1.25] | 0.94 [0.71, 1.22] | 0.42 [0.27, 0.58] | 0.04 [-0.04, 0.13] | -0.04 [-0.16, 0.11] | 0.62 [0.42, 0.85] | 4.54 [3.37, 5.84] | 0.66 [0.45, 0.90] | 0.03 [-0.10, 0.19] | 0.02 [-0.16, 0.25] | chronos2 |
| 4 | 0.59 [0.36, 0.84] | -0.17 [-0.41, 0.05] | 0.38 [0.08, 0.74] | -0.21 [-0.45, 0.07] | 2.72 [2.13, 3.35] | 0.08 [-0.17, 0.34] | 0.19 [-0.06, 0.48] | -2.17 [-2.75, -1.67]* | -1.42 [-1.91, -0.86]* |  | 0.80 [0.43, 1.18] | 0.78 [0.41, 1.16] | 0.87 [0.64, 1.12] | 0.47 [0.29, 0.67] | 0.08 [-0.04, 0.21] | -0.15 [-0.37, 0.07] | 0.67 [0.43, 0.92] | 4.64 [3.39, 5.99] | 0.71 [0.47, 0.97] | -0.03 [-0.25, 0.20] | -0.16 [-0.40, 0.13] | chronos2 |
| 5 | 0.55 [0.35, 0.79] | -0.15 [-0.35, 0.06] | 0.43 [0.20, 0.69] | -0.11 [-0.32, 0.12] | 2.87 [2.14, 3.77] | 0.09 [-0.08, 0.27] | 0.28 [0.04, 0.57] | -2.15 [-2.72, -1.64]* | -1.44 [-1.88, -0.98]* |  | 0.95 [0.63, 1.28] | 0.93 [0.61, 1.25] | 0.98 [0.67, 1.28] | 0.49 [0.30, 0.70] | 0.10 [-0.02, 0.23] | -0.14 [-0.33, 0.05] | 0.61 [0.40, 0.84] | 4.45 [3.16, 5.83] | 0.64 [0.42, 0.88] | -0.03 [-0.19, 0.14] | -0.05 [-0.27, 0.19] | chronos2 |
| 6 | 0.52 [0.35, 0.70] | -0.20 [-0.41, 0.01] | 0.51 [0.21, 0.86] | -0.12 [-0.34, 0.12] | 2.77 [2.16, 3.44] | 0.03 [-0.16, 0.25] | 0.36 [0.15, 0.60] | -1.95 [-2.43, -1.53]* | -1.52 [-2.00, -1.04]* |  | 1.11 [0.75, 1.43] | 1.09 [0.72, 1.41] | 1.12 [0.90, 1.38] | 0.45 [0.29, 0.61] | -0.01 [-0.15, 0.14] | -0.18 [-0.37, 0.03] | 0.59 [0.39, 0.84] | 4.25 [3.00, 5.58] | 0.62 [0.41, 0.89] | -0.06 [-0.27, 0.15] | -0.06 [-0.27, 0.19] | chronos2 |
| 7 | 0.51 [0.31, 0.72] | -0.24 [-0.42, -0.04]* | 0.30 [0.05, 0.61] | -0.19 [-0.40, 0.08] | 2.86 [2.16, 3.72] | 0.12 [-0.08, 0.35] | 0.15 [-0.06, 0.38] | -2.37 [-2.92, -1.90]* | -1.29 [-1.79, -0.80]* |  | 0.76 [0.43, 1.08] | 0.75 [0.41, 1.07] |  | 0.46 [0.28, 0.64] | 0.13 [-0.02, 0.29] | -0.20 [-0.39, 0.01] | 0.65 [0.43, 0.89] | 4.26 [3.05, 5.55] | 0.68 [0.45, 0.93] | -0.02 [-0.19, 0.19] | -0.11 [-0.34, 0.16] | chronos2 |

segment `busy`:

| lead | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.99 [0.71, 1.27] |  | 2.77 [2.20, 3.33] | 1.39 [1.04, 1.80] | 6.61 [4.42, 9.09] | 1.84 [1.43, 2.26] | 0.96 [0.32, 1.49] | -3.26 [-4.22, -2.43]* | -1.44 [-1.93, -0.92]* |  |  |  | 4.17 [3.29, 4.91] | 1.09 [0.81, 1.38] | 0.23 [0.05, 0.42] |  | 1.05 [0.82, 1.32] | 11.58 [8.10, 14.80] | 1.15 [0.90, 1.43] |  | 1.52 [1.16, 1.94] | chronos2 |
| m015 |  |  | 9.26 [7.61, 10.99] | 8.14 [6.66, 9.64] |  | 8.43 [6.79, 10.15] | 7.58 [6.09, 8.91] |  |  | -0.49 [-0.71, -0.29]* |  |  | 10.80 [8.84, 12.70] | -0.12 [-0.26, -0.01]* |  |  | -0.32 [-0.49, -0.15]* |  |  |  | 8.15 [6.70, 9.63] | chronos2 |
| m030 |  |  | 7.38 [5.95, 8.91] | 6.28 [5.03, 7.62] |  | 6.60 [5.19, 8.13] | 5.67 [4.43, 6.80] |  |  | 0.88 [0.45, 1.24] |  |  | 9.07 [7.29, 10.80] | 0.33 [0.17, 0.47] |  |  | 0.03 [-0.10, 0.18] |  |  |  | 6.40 [5.13, 7.72] | chronos2 |
| m045 |  |  | 6.71 [5.51, 8.10] | 5.54 [4.49, 6.68] |  | 5.88 [4.67, 7.23] | 5.01 [3.97, 6.01] |  |  | 1.88 [1.34, 2.32] |  |  | 8.20 [6.66, 9.74] | 0.67 [0.42, 0.87] |  |  | 0.21 [0.11, 0.32] |  |  |  | 5.68 [4.62, 6.79] | chronos2 |
| m060 |  |  | 6.42 [5.23, 7.66] | 5.25 [4.21, 6.36] |  | 5.56 [4.39, 6.85] | 4.66 [3.60, 5.61] |  |  | 2.63 [2.03, 3.16] |  |  | 7.93 [6.43, 9.45] | 0.90 [0.60, 1.18] |  |  | 0.35 [0.25, 0.47] |  |  |  | 5.36 [4.37, 6.44] | chronos2 |
| m075 |  |  | 6.02 [4.84, 7.26] | 4.72 [3.75, 5.69] |  | 5.03 [3.99, 6.11] | 4.14 [3.14, 4.97] |  |  | 3.08 [2.45, 3.66] |  |  | 7.56 [6.08, 8.97] | 1.03 [0.73, 1.29] |  |  | 0.41 [0.30, 0.54] |  |  |  | 4.87 [3.89, 5.85] | chronos2 |
| m090 |  |  | 5.86 [4.69, 7.13] | 4.42 [3.51, 5.42] |  | 4.77 [3.75, 5.88] | 3.91 [3.02, 4.77] |  |  | 3.75 [2.97, 4.46] |  |  | 7.24 [5.82, 8.66] | 1.09 [0.80, 1.36] |  |  | 0.56 [0.41, 0.73] |  |  |  | 4.66 [3.72, 5.66] | chronos2 |
| m105 |  |  | 5.73 [4.51, 6.96] | 4.37 [3.42, 5.37] |  | 4.75 [3.66, 5.85] | 3.68 [2.77, 4.51] |  |  | 4.08 [3.22, 4.96] |  |  | 7.16 [5.64, 8.67] | 1.14 [0.81, 1.50] |  |  | 0.60 [0.43, 0.78] |  |  |  | 4.54 [3.59, 5.55] | chronos2 |
| m120 |  |  | 5.64 [4.49, 6.84] | 4.25 [3.37, 5.21] |  | 4.59 [3.57, 5.61] | 3.57 [2.70, 4.36] |  |  | 4.48 [3.54, 5.46] |  |  | 6.96 [5.55, 8.32] | 1.27 [0.89, 1.71] |  |  | 0.67 [0.48, 0.88] |  |  |  | 4.42 [3.48, 5.36] | chronos2 |
| h2-4 |  |  | 4.95 [3.95, 5.96] | 3.62 [2.83, 4.42] |  | 4.04 [3.09, 5.01] | 3.05 [2.17, 3.81] |  |  | 5.81 [4.70, 6.90] |  |  | 6.37 [5.03, 7.74] | 1.25 [0.92, 1.59] |  |  | 0.68 [0.48, 0.93] |  |  |  | 3.76 [2.98, 4.54] | chronos2 |
| h4-8 |  |  | 3.91 [3.12, 4.67] | 2.57 [2.01, 3.15] |  | 3.02 [2.32, 3.72] | 2.14 [1.35, 2.86] |  |  | 8.42 [6.86, 9.96] |  |  | 5.36 [4.16, 6.54] | 1.14 [0.79, 1.51] |  |  | 0.67 [0.44, 0.95] |  |  |  | 2.75 [2.19, 3.31] | chronos2 |
| h8+ |  |  | 2.28 [1.80, 2.85] | 1.06 [0.75, 1.47] |  | 1.54 [1.19, 2.02] | 1.15 [0.57, 1.68] |  |  | 12.07 [8.60, 16.38] |  |  | 3.87 [2.96, 4.75] | 0.98 [0.53, 1.46] |  |  | 0.75 [0.48, 1.08] |  |  |  | 1.27 [0.96, 1.68] | chronos2 |
| 1 | 1.06 [0.73, 1.38] | -0.31 [-0.57, -0.03]* | 1.76 [1.18, 2.45] | 0.47 [0.09, 0.90] | 5.60 [3.85, 7.41] | 0.60 [0.30, 0.93] | 0.29 [-0.16, 0.62] | -3.79 [-4.52, -2.99]* | -2.73 [-3.15, -2.27]* |  |  |  | 3.18 [2.82, 3.55] | 0.99 [0.73, 1.24] | 0.24 [-0.01, 0.57] | -0.15 [-0.34, 0.08] | 1.10 [0.81, 1.46] | 11.37 [8.41, 14.21] | 1.18 [0.88, 1.55] | -0.13 [-0.35, 0.16] | 0.62 [0.24, 1.06] | chronos2 |
| 2 | 1.27 [0.77, 1.75] | -0.34 [-0.60, -0.05]* | 1.68 [1.08, 2.46] | 0.41 [-0.08, 1.05] | 6.05 [3.94, 8.59] | 0.43 [0.08, 0.87] | 0.07 [-0.32, 0.40] | -4.41 [-5.42, -3.51]* | -2.97 [-3.47, -2.36]* |  |  |  | 2.77 [2.24, 3.34] | 1.10 [0.71, 1.51] | 0.33 [0.05, 0.68] | -0.23 [-0.51, 0.09] | 1.36 [0.89, 1.90] | 11.19 [8.15, 14.11] | 1.45 [0.97, 1.97] | -0.03 [-0.28, 0.33] | 0.58 [0.09, 1.21] | chronos2 |
| 3 | 1.16 [0.71, 1.63] | -0.45 [-0.72, -0.13]* | 1.35 [0.67, 2.29] | 0.05 [-0.38, 0.65] | 4.95 [3.39, 6.62] | 0.28 [-0.06, 0.71] | 0.06 [-0.40, 0.54] | -4.68 [-5.75, -3.72]* | -3.13 [-3.54, -2.61]* |  | 1.35 [0.51, 2.06] | 1.29 [0.47, 2.00] | 2.12 [1.75, 2.50] | 1.02 [0.68, 1.37] | 0.33 [0.14, 0.55] | -0.37 [-0.62, -0.03]* | 1.44 [0.98, 1.95] | 10.67 [7.94, 13.25] | 1.53 [1.05, 2.07] | -0.23 [-0.49, 0.11] | 0.25 [-0.18, 0.87] | chronos2 |
| 4 | 1.40 [0.88, 1.91] | -0.69 [-1.17, -0.17]* | 0.91 [0.17, 1.86] | -0.35 [-0.90, 0.40] | 5.05 [3.46, 6.55] | -0.11 [-0.64, 0.56] | -0.14 [-0.72, 0.45] | -4.97 [-6.15, -3.83]* | -3.48 [-4.16, -2.60]* |  | 1.10 [0.25, 1.93] | 1.03 [0.19, 1.86] | 1.94 [1.55, 2.42] | 1.15 [0.74, 1.60] | 0.35 [0.08, 0.67] | -0.62 [-1.06, -0.13]* | 1.56 [1.04, 2.13] | 10.83 [7.92, 13.52] | 1.66 [1.12, 2.22] | -0.35 [-0.80, 0.09] | -0.20 [-0.78, 0.60] | chronos2 |
| 5 | 1.29 [0.79, 1.81] | -0.56 [-0.97, -0.11]* | 1.03 [0.47, 1.65] | -0.06 [-0.55, 0.50] | 5.50 [3.46, 7.87] | 0.02 [-0.33, 0.45] | 0.09 [-0.45, 0.64] | -4.98 [-6.20, -3.85]* | -3.35 [-3.84, -2.72]* |  | 1.52 [0.81, 2.21] | 1.45 [0.75, 2.14] | 2.22 [1.64, 2.73] | 1.19 [0.74, 1.65] | 0.38 [0.11, 0.66] | -0.52 [-0.90, -0.11]* | 1.44 [0.98, 1.90] | 10.52 [7.60, 13.41] | 1.51 [1.05, 1.99] | -0.29 [-0.61, 0.06] | 0.12 [-0.40, 0.71] | chronos2 |
| 6 | 1.21 [0.80, 1.59] | -0.69 [-1.05, -0.24]* | 1.27 [0.46, 2.30] | -0.05 [-0.62, 0.61] | 5.00 [3.31, 6.76] | -0.24 [-0.64, 0.23] | 0.01 [-0.36, 0.41] | -4.56 [-5.36, -3.71]* | -3.69 [-4.20, -3.03]* |  | 1.43 [0.83, 2.06] | 1.37 [0.77, 2.00] | 2.32 [1.89, 2.77] | 1.04 [0.66, 1.44] | 0.08 [-0.21, 0.43] | -0.60 [-0.96, -0.18]* | 1.42 [0.96, 1.97] | 10.11 [7.29, 12.97] | 1.49 [1.02, 2.08] | -0.43 [-0.80, 0.05] | 0.11 [-0.46, 0.79] | chronos2 |
| 7 | 1.21 [0.72, 1.66] | -0.71 [-1.10, -0.27]* | 0.83 [0.19, 1.55] | -0.18 [-0.68, 0.51] | 5.34 [3.45, 7.51] | 0.09 [-0.31, 0.59] | -0.29 [-0.73, 0.21] | -5.36 [-6.42, -4.38]* | -3.18 [-3.78, -2.47]* |  | 0.97 [0.31, 1.70] | 0.92 [0.26, 1.64] |  | 1.11 [0.65, 1.49] | 0.45 [0.10, 0.84] | -0.61 [-0.99, -0.17]* | 1.57 [1.06, 2.11] | 10.07 [7.20, 12.84] | 1.64 [1.12, 2.20] | -0.24 [-0.60, 0.20] | 0.01 [-0.50, 0.71] | chronos2 |

### Paired against `chronos2_nocov` (not the per-lead reference)

MAE difference model − `chronos2_nocov` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.42 [-0.56, -0.31]* |  | 0.78 [0.53, 1.02] | 0.11 [-0.06, 0.30] | 3.00 [2.24, 3.95] | 0.48 [0.29, 0.71] | 0.23 [0.00, 0.44] | -1.78 [-2.31, -1.36]* | -0.99 [-1.41, -0.58]* |  |  | 1.44 [1.07, 1.81] | 0.07 [0.01, 0.13] | -0.36 [-0.51, -0.24]* |  | 0.07 [-0.05, 0.20] | 4.47 [3.06, 5.97] | 0.10 [-0.02, 0.24] |  | 0.15 [-0.02, 0.35] | chronos2_nocov |
| 1 | -0.47 [-0.62, -0.32]* | -0.47 [-0.71, -0.25]* | 0.25 [0.00, 0.51] | -0.35 [-0.58, -0.15]* | 2.58 [1.99, 3.30] | -0.08 [-0.29, 0.13] | 0.04 [-0.19, 0.31] | -2.08 [-2.59, -1.65]* | -1.58 [-2.03, -1.12]* |  |  | 1.05 [0.82, 1.27] | -0.02 [-0.09, 0.05] | -0.39 [-0.60, -0.19]* | -0.39 [-0.62, -0.19]* | 0.03 [-0.07, 0.16] | 4.37 [3.14, 5.69] | 0.07 [-0.04, 0.20] | -0.35 [-0.58, -0.11]* | -0.30 [-0.50, -0.10]* | chronos2_nocov |
| 2 | -0.54 [-0.75, -0.35]* | -0.57 [-0.84, -0.32]* | 0.14 [-0.16, 0.49] | -0.45 [-0.73, -0.17]* | 2.72 [2.04, 3.62] | -0.23 [-0.51, 0.05] | -0.16 [-0.45, 0.09] | -2.46 [-3.03, -1.97]* | -1.73 [-2.30, -1.18]* |  |  | 0.75 [0.50, 1.05] | -0.08 [-0.15, 0.01] | -0.46 [-0.66, -0.27]* | -0.52 [-0.79, -0.27]* | 0.06 [-0.07, 0.22] | 4.25 [3.03, 5.53] | 0.09 [-0.04, 0.26] | -0.42 [-0.68, -0.16]* | -0.40 [-0.66, -0.11]* | chronos2_nocov |
| 3 | -0.49 [-0.70, -0.32]* | -0.57 [-0.86, -0.29]* | 0.08 [-0.23, 0.45] | -0.55 [-0.82, -0.25]* | 2.36 [1.80, 3.00] | -0.28 [-0.54, -0.02]* | -0.10 [-0.37, 0.18] | -2.54 [-3.15, -2.01]* | -1.80 [-2.30, -1.30]* | 0.50 [0.11, 0.87] | 0.48 [0.09, 0.85] | 0.45 [0.26, 0.65] | -0.08 [-0.16, 0.01] | -0.45 [-0.62, -0.27]* | -0.54 [-0.82, -0.28]* | 0.13 [-0.01, 0.29] | 4.05 [2.93, 5.25] | 0.16 [0.02, 0.34] | -0.47 [-0.74, -0.19]* | -0.48 [-0.72, -0.18]* | chronos2_nocov |
| 4 | -0.59 [-0.84, -0.36]* | -0.77 [-1.15, -0.38]* | -0.21 [-0.56, 0.16] | -0.80 [-1.17, -0.42]* | 2.27 [1.81, 2.79] | -0.52 [-0.93, -0.11]* | -0.30 [-0.64, 0.05] | -2.77 [-3.48, -2.14]* | -2.02 [-2.68, -1.31]* | 0.31 [-0.15, 0.74] | 0.29 [-0.18, 0.72] | 0.28 [0.03, 0.55] | -0.12 [-0.20, -0.04]* | -0.51 [-0.73, -0.28]* | -0.74 [-1.09, -0.38]* | 0.08 [-0.07, 0.26] | 4.06 [2.92, 5.22] | 0.12 [-0.03, 0.30] | -0.62 [-0.96, -0.27]* | -0.75 [-1.11, -0.36]* | chronos2_nocov |
| 5 | -0.55 [-0.79, -0.35]* | -0.70 [-1.03, -0.38]* | -0.11 [-0.37, 0.18] | -0.67 [-1.01, -0.31]* | 2.42 [1.82, 3.21] | -0.47 [-0.78, -0.16]* | -0.22 [-0.54, 0.15] | -2.70 [-3.41, -2.08]* | -2.00 [-2.62, -1.41]* | 0.46 [0.07, 0.84] | 0.44 [0.04, 0.82] | 0.42 [0.18, 0.69] | -0.06 [-0.13, 0.02] | -0.45 [-0.68, -0.24]* | -0.70 [-1.01, -0.38]* | 0.06 [-0.07, 0.20] | 3.91 [2.74, 5.14] | 0.09 [-0.05, 0.24] | -0.58 [-0.88, -0.29]* | -0.61 [-0.94, -0.26]* | chronos2_nocov |
| 6 | -0.52 [-0.70, -0.35]* | -0.73 [-1.05, -0.41]* | -0.03 [-0.32, 0.32] | -0.65 [-0.95, -0.33]* | 2.34 [1.80, 2.92] | -0.50 [-0.79, -0.17]* | -0.07 [-0.38, 0.24] | -2.48 [-3.06, -1.96]* | -2.05 [-2.65, -1.45]* | 0.67 [0.26, 1.05] | 0.65 [0.23, 1.03] | 0.58 [0.40, 0.81] | -0.07 [-0.15, 0.00] | -0.54 [-0.78, -0.29]* | -0.70 [-1.02, -0.39]* | 0.07 [-0.07, 0.23] | 3.73 [2.60, 4.93] | 0.10 [-0.04, 0.28] | -0.59 [-0.91, -0.27]* | -0.58 [-0.87, -0.25]* | chronos2_nocov |
| 7 | -0.51 [-0.72, -0.31]* | -0.75 [-1.07, -0.43]* | -0.22 [-0.52, 0.14] | -0.70 [-1.05, -0.34]* | 2.47 [1.88, 3.24] | -0.40 [-0.74, -0.07]* | -0.28 [-0.64, 0.05] | -2.89 [-3.54, -2.30]* | -1.82 [-2.45, -1.21]* | 0.32 [-0.11, 0.72] | 0.30 [-0.14, 0.70] |  | -0.05 [-0.14, 0.04] | -0.38 [-0.60, -0.16]* | -0.71 [-1.04, -0.39]* | 0.14 [0.01, 0.29] | 3.74 [2.66, 4.90] | 0.17 [0.04, 0.33] | -0.53 [-0.83, -0.22]* | -0.63 [-0.97, -0.26]* | chronos2_nocov |

segment `busy`:

| lead | chronos2 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.99 [-1.27, -0.71]* |  | 1.80 [1.29, 2.35] | 0.39 [-0.03, 0.88] | 5.80 [3.76, 8.09] | 0.84 [0.44, 1.34] | 0.02 [-0.54, 0.49] | -4.26 [-5.32, -3.29]* | -2.45 [-2.97, -1.87]* |  |  | 3.15 [2.34, 3.90] | 0.10 [-0.04, 0.24] | -0.76 [-1.06, -0.49]* |  | 0.06 [-0.17, 0.36] | 10.59 [7.31, 13.66] | 0.16 [-0.09, 0.48] |  | 0.53 [0.13, 1.01] | chronos2_nocov |
| 1 | -1.06 [-1.38, -0.73]* | -1.37 [-1.81, -0.95]* | 0.68 [0.01, 1.42] | -0.60 [-1.13, -0.07]* | 4.67 [3.10, 6.51] | -0.50 [-0.97, -0.06]* | -0.71 [-1.16, -0.26]* | -4.85 [-5.66, -3.94]* | -3.84 [-4.39, -3.25]* |  |  | 2.10 [1.74, 2.47] | -0.07 [-0.22, 0.11] | -0.82 [-1.27, -0.32]* | -1.21 [-1.63, -0.83]* | 0.04 [-0.15, 0.30] | 10.31 [7.49, 13.02] | 0.13 [-0.08, 0.41] | -1.19 [-1.60, -0.78]* | -0.44 [-0.91, 0.08] | chronos2_nocov |
| 2 | -1.27 [-1.75, -0.77]* | -1.61 [-2.16, -1.05]* | 0.39 [-0.39, 1.28] | -0.85 [-1.52, -0.13]* | 5.09 [3.24, 7.45] | -0.86 [-1.50, -0.23]* | -1.07 [-1.70, -0.44]* | -5.68 [-6.70, -4.60]* | -4.26 [-5.13, -3.39]* |  |  | 1.53 [1.03, 2.09] | -0.17 [-0.35, 0.06] | -0.94 [-1.40, -0.45]* | -1.50 [-2.05, -0.93]* | 0.10 [-0.15, 0.42] | 9.93 [7.17, 12.62] | 0.18 [-0.08, 0.53] | -1.30 [-1.83, -0.73]* | -0.69 [-1.33, 0.04] | chronos2_nocov |
| 3 | -1.16 [-1.63, -0.71]* | -1.62 [-2.15, -1.00]* | 0.20 [-0.63, 1.20] | -1.11 [-1.74, -0.35]* | 4.15 [2.82, 5.61] | -0.89 [-1.46, -0.28]* | -0.91 [-1.46, -0.24]* | -5.84 [-6.87, -4.74]* | -4.29 [-4.91, -3.52]* | 0.35 [-0.52, 1.19] | 0.30 [-0.57, 1.13] | 0.99 [0.49, 1.45] | -0.14 [-0.36, 0.09] | -0.83 [-1.22, -0.41]* | -1.53 [-2.05, -0.92]* | 0.28 [-0.02, 0.68] | 9.51 [7.02, 12.00] | 0.37 [0.04, 0.79] | -1.40 [-1.94, -0.77]* | -0.90 [-1.48, -0.17]* | chronos2_nocov |
| 4 | -1.40 [-1.91, -0.88]* | -2.09 [-2.76, -1.28]* | -0.48 [-1.30, 0.52] | -1.76 [-2.53, -0.77]* | 3.99 [2.72, 5.29] | -1.53 [-2.31, -0.55]* | -1.34 [-2.08, -0.52]* | -6.37 [-7.69, -5.02]* | -4.90 [-5.85, -3.61]* | -0.12 [-1.03, 0.95] | -0.18 [-1.10, 0.90] | 0.56 [0.00, 1.22] | -0.25 [-0.43, -0.04]* | -1.05 [-1.50, -0.51]* | -2.03 [-2.66, -1.26]* | 0.16 [-0.15, 0.54] | 9.43 [6.86, 11.90] | 0.26 [-0.07, 0.64] | -1.76 [-2.38, -1.00]* | -1.60 [-2.36, -0.62]* | chronos2_nocov |
| 5 | -1.29 [-1.81, -0.79]* | -1.85 [-2.45, -1.15]* | -0.21 [-0.85, 0.49] | -1.36 [-2.06, -0.50]* | 4.43 [2.85, 6.58] | -1.30 [-1.93, -0.58]* | -1.14 [-1.75, -0.42]* | -6.28 [-7.61, -4.86]* | -4.67 [-5.46, -3.74]* | 0.31 [-0.50, 1.17] | 0.24 [-0.57, 1.11] | 0.93 [0.40, 1.51] | -0.10 [-0.27, 0.09] | -0.91 [-1.36, -0.45]* | -1.82 [-2.41, -1.14]* | 0.15 [-0.15, 0.50] | 9.23 [6.68, 11.73] | 0.22 [-0.09, 0.57] | -1.58 [-2.11, -0.95]* | -1.18 [-1.86, -0.30]* | chronos2_nocov |
| 6 | -1.21 [-1.59, -0.80]* | -1.91 [-2.44, -1.29]* | 0.03 [-0.76, 1.00] | -1.27 [-1.89, -0.47]* | 4.01 [2.56, 5.58] | -1.47 [-1.97, -0.79]* | -1.05 [-1.59, -0.41]* | -5.78 [-6.67, -4.74]* | -4.91 [-5.57, -4.08]* | 0.36 [-0.36, 1.20] | 0.30 [-0.43, 1.14] | 1.08 [0.71, 1.52] | -0.17 [-0.36, 0.04] | -1.13 [-1.56, -0.59]* | -1.82 [-2.34, -1.19]* | 0.21 [-0.11, 0.64] | 8.90 [6.36, 11.45] | 0.28 [-0.06, 0.75] | -1.65 [-2.19, -0.96]* | -1.10 [-1.73, -0.28]* | chronos2_nocov |
| 7 | -1.21 [-1.66, -0.72]* | -1.92 [-2.54, -1.23]* | -0.39 [-1.09, 0.45] | -1.39 [-2.07, -0.51]* | 4.39 [2.83, 6.47] | -1.15 [-1.83, -0.44]* | -1.38 [-2.09, -0.62]* | -6.57 [-7.73, -5.35]* | -4.42 [-5.34, -3.47]* | -0.15 [-1.01, 0.85] | -0.20 [-1.10, 0.81] |  | -0.10 [-0.31, 0.12] | -0.77 [-1.19, -0.27]* | -1.83 [-2.44, -1.12]* | 0.36 [0.06, 0.76] | 8.85 [6.30, 11.30] | 0.43 [0.12, 0.85] | -1.45 [-1.99, -0.83]* | -1.20 [-1.85, -0.32]* | chronos2_nocov |

### Paired against `h5` (not the per-lead reference)

MAE difference model − `h5` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.53 [-0.71, -0.36]* | -0.11 [-0.30, 0.06] |  | 0.62 [0.48, 0.78] | 2.88 [2.07, 4.04] | 0.36 [0.25, 0.48] | 0.22 [0.05, 0.36] | -1.89 [-2.34, -1.47]* | -1.11 [-1.45, -0.75]* |  |  |  | 1.30 [0.91, 1.67] | -0.04 [-0.21, 0.13] | -0.49 [-0.67, -0.32]* |  | -0.05 [-0.22, 0.12] | 4.41 [2.91, 5.94] | -0.01 [-0.18, 0.15] |  | 0.04 [0.02, 0.07] | h5 |
| m015 | -4.01 [-4.81, -3.26]* |  |  | 0.59 [0.45, 0.74] |  | 0.34 [0.22, 0.47] | 0.19 [0.01, 0.34] |  |  | -4.71 [-5.54, -3.93]* |  |  | 1.24 [0.84, 1.59] | -4.08 [-4.89, -3.32]* |  |  | -4.19 [-5.01, -3.43]* |  |  |  | -0.02 [-0.07, 0.04] | h5 |
| m030 | -3.02 [-3.65, -2.42]* |  |  | 0.60 [0.46, 0.74] |  | 0.37 [0.25, 0.50] | 0.24 [0.06, 0.40] |  |  | -3.00 [-3.64, -2.40]* |  |  | 1.32 [0.93, 1.70] | -2.86 [-3.46, -2.28]* |  |  | -3.01 [-3.62, -2.41]* |  |  |  | 0.04 [0.02, 0.07] | h5 |
| m045 | -2.63 [-3.16, -2.11]* |  |  | 0.61 [0.47, 0.77] |  | 0.37 [0.24, 0.50] | 0.23 [0.05, 0.38] |  |  | -2.09 [-2.70, -1.56]* |  |  | 1.28 [0.90, 1.66] | -2.33 [-2.83, -1.88]* |  |  | -2.55 [-3.06, -2.05]* |  |  |  | 0.05 [0.01, 0.09] | h5 |
| m060 | -2.48 [-3.00, -1.97]* |  |  | 0.59 [0.46, 0.74] |  | 0.34 [0.23, 0.47] | 0.19 [0.01, 0.33] |  |  | -1.53 [-2.12, -1.01]* |  |  | 1.29 [0.88, 1.66] | -2.07 [-2.54, -1.63]* |  |  | -2.34 [-2.83, -1.85]* |  |  |  | 0.02 [-0.02, 0.07] | h5 |
| m075 | -2.21 [-2.68, -1.76]* |  |  | 0.63 [0.47, 0.79] |  | 0.36 [0.25, 0.47] | 0.20 [0.02, 0.34] |  |  | -1.15 [-1.73, -0.63]* |  |  | 1.32 [0.90, 1.71] | -1.72 [-2.11, -1.33]* |  |  | -2.00 [-2.44, -1.59]* |  |  |  | 0.04 [-0.00, 0.09] | h5 |
| m090 | -2.05 [-2.49, -1.61]* |  |  | 0.67 [0.50, 0.86] |  | 0.38 [0.27, 0.50] | 0.25 [0.08, 0.39] |  |  | -0.63 [-1.21, -0.13]* |  |  | 1.34 [0.94, 1.73] | -1.53 [-1.92, -1.17]* |  |  | -1.79 [-2.19, -1.40]* |  |  |  | 0.08 [0.04, 0.13] | h5 |
| m105 | -2.00 [-2.45, -1.55]* |  |  | 0.66 [0.51, 0.83] |  | 0.39 [0.28, 0.51] | 0.23 [0.07, 0.37] |  |  | -0.37 [-1.02, 0.22] |  |  | 1.35 [0.96, 1.75] | -1.47 [-1.86, -1.08]* |  |  | -1.73 [-2.14, -1.33]* |  |  |  | 0.06 [0.04, 0.09] | h5 |
| m120 | -1.95 [-2.38, -1.51]* |  |  | 0.66 [0.51, 0.83] |  | 0.34 [0.24, 0.44] | 0.19 [0.03, 0.34] |  |  | -0.08 [-0.68, 0.49] |  |  | 1.31 [0.94, 1.66] | -1.36 [-1.74, -1.00]* |  |  | -1.65 [-2.04, -1.28]* |  |  |  | 0.05 [0.01, 0.09] | h5 |
| h2-4 | -1.58 [-1.93, -1.20]* |  |  | 0.63 [0.48, 0.78] |  | 0.38 [0.26, 0.50] | 0.22 [0.04, 0.37] |  |  | 0.94 [0.32, 1.54] |  |  | 1.30 [0.90, 1.68] | -1.00 [-1.30, -0.70]* |  |  | -1.27 [-1.63, -0.91]* |  |  |  | 0.04 [0.02, 0.06] | h5 |
| h4-8 | -1.09 [-1.35, -0.82]* |  |  | 0.64 [0.50, 0.78] |  | 0.38 [0.26, 0.50] | 0.23 [0.05, 0.39] |  |  | 3.16 [2.33, 4.04] |  |  | 1.35 [0.93, 1.74] | -0.57 [-0.78, -0.36]* |  |  | -0.79 [-1.07, -0.53]* |  |  |  | 0.06 [0.04, 0.08] | h5 |
| h8+ | -0.45 [-0.59, -0.32]* |  |  | 0.55 [0.42, 0.72] |  | 0.42 [0.33, 0.53] | 0.42 [0.25, 0.57] |  |  | 6.00 [4.50, 8.10] |  |  | 1.39 [0.95, 1.81] | -0.01 [-0.25, 0.21] |  |  | -0.13 [-0.32, 0.03] |  |  |  | 0.07 [0.02, 0.12] | h5 |
| 1 | -0.12 [-0.29, 0.03] | 0.35 [0.15, 0.58] | -0.12 [-0.27, 0.03] | 0.57 [0.40, 0.78] | 2.73 [2.10, 3.57] | 0.28 [0.17, 0.40] | 0.29 [0.09, 0.50] | -1.72 [-2.11, -1.36]* | -1.22 [-1.56, -0.86]* |  |  |  | 1.38 [1.08, 1.68] | 0.32 [0.15, 0.51] | -0.05 [-0.20, 0.10] | -0.04 [-0.17, 0.09] | 0.38 [0.19, 0.59] | 4.76 [3.43, 6.20] | 0.42 [0.21, 0.63] | 0.00 [-0.14, 0.15] | 0.05 [0.03, 0.08] | h5 |
| 2 | -0.08 [-0.32, 0.10] | 0.45 [0.17, 0.73] | -0.11 [-0.27, 0.02] | 0.56 [0.43, 0.73] | 2.97 [2.15, 4.06] | 0.22 [0.12, 0.32] | 0.22 [0.02, 0.37] | -2.01 [-2.44, -1.61]* | -1.28 [-1.64, -0.91]* |  |  |  | 1.18 [0.83, 1.54] | 0.38 [0.15, 0.61] | -0.01 [-0.19, 0.15] | -0.06 [-0.20, 0.06] | 0.51 [0.26, 0.77] | 4.75 [3.36, 6.22] | 0.55 [0.30, 0.80] | 0.03 [-0.10, 0.16] | 0.06 [0.03, 0.09] | h5 |
| 3 | 0.05 [-0.18, 0.24] | 0.55 [0.25, 0.82] | -0.02 [-0.19, 0.12] | 0.61 [0.45, 0.80] | 2.70 [2.03, 3.52] | 0.27 [0.15, 0.39] | 0.34 [0.16, 0.49] | -1.98 [-2.47, -1.56]* | -1.25 [-1.55, -0.93]* |  | 0.93 [0.59, 1.25] | 0.91 [0.58, 1.23] | 0.98 [0.67, 1.29] | 0.48 [0.23, 0.70] | 0.09 [-0.12, 0.29] | 0.01 [-0.15, 0.15] | 0.68 [0.42, 0.92] | 4.64 [3.39, 5.98] | 0.72 [0.45, 0.96] | 0.08 [-0.07, 0.23] | 0.07 [0.04, 0.11] | h5 |
| 4 | 0.21 [-0.07, 0.45] | 0.80 [0.42, 1.17] | 0.04 [-0.12, 0.17] | 0.56 [0.38, 0.80] | 2.89 [2.12, 3.70] | 0.29 [0.17, 0.42] | 0.42 [0.20, 0.64] | -1.96 [-2.42, -1.55]* | -1.21 [-1.54, -0.84]* |  | 1.02 [0.66, 1.36] | 1.00 [0.64, 1.33] | 1.06 [0.69, 1.41] | 0.69 [0.35, 1.01] | 0.29 [0.04, 0.53] | 0.06 [-0.08, 0.18] | 0.88 [0.55, 1.23] | 4.90 [3.52, 6.45] | 0.92 [0.59, 1.28] | 0.19 [0.02, 0.35] | 0.05 [0.03, 0.08] | h5 |
| 5 | 0.11 [-0.12, 0.32] | 0.67 [0.31, 1.01] | -0.04 [-0.16, 0.06] | 0.51 [0.37, 0.68] | 2.91 [2.07, 4.04] | 0.20 [0.09, 0.32] | 0.41 [0.16, 0.69] | -2.03 [-2.51, -1.60]* | -1.33 [-1.67, -0.96]* |  | 1.07 [0.70, 1.40] | 1.04 [0.68, 1.37] | 1.09 [0.70, 1.47] | 0.61 [0.30, 0.93] | 0.21 [-0.01, 0.41] | -0.03 [-0.14, 0.06] | 0.72 [0.41, 1.03] | 4.62 [3.18, 6.14] | 0.75 [0.43, 1.07] | 0.08 [-0.05, 0.19] | 0.06 [0.03, 0.09] | h5 |
| 6 | 0.12 [-0.12, 0.34] | 0.65 [0.33, 0.95] | -0.08 [-0.25, 0.06] | 0.59 [0.42, 0.81] | 2.80 [2.17, 3.57] | 0.15 [0.02, 0.28] | 0.45 [0.22, 0.67] | -1.83 [-2.23, -1.45]* | -1.40 [-1.77, -1.02]* |  | 1.19 [0.77, 1.52] | 1.17 [0.76, 1.49] | 1.20 [0.89, 1.50] | 0.57 [0.29, 0.83] | 0.10 [-0.07, 0.25] | -0.06 [-0.20, 0.07] | 0.71 [0.43, 0.99] | 4.42 [3.10, 5.86] | 0.74 [0.45, 1.03] | 0.05 [-0.09, 0.21] | 0.06 [0.03, 0.09] | h5 |
| 7 | 0.19 [-0.08, 0.40] | 0.70 [0.34, 1.05] | -0.05 [-0.20, 0.07] | 0.46 [0.33, 0.62] | 3.02 [2.27, 4.08] | 0.32 [0.21, 0.42] | 0.36 [0.16, 0.52] | -2.18 [-2.65, -1.76]* | -1.10 [-1.47, -0.75]* |  | 0.96 [0.62, 1.27] | 0.94 [0.60, 1.25] |  | 0.66 [0.33, 0.97] | 0.32 [0.09, 0.51] | -0.01 [-0.13, 0.10] | 0.84 [0.50, 1.18] | 4.51 [3.15, 5.93] | 0.88 [0.52, 1.22] | 0.17 [0.04, 0.31] | 0.07 [0.05, 0.10] | h5 |
| 10 |  |  | 0.07 [-0.06, 0.19] | 0.44 [0.30, 0.59] | 3.05 [2.25, 4.08] | 0.36 [0.21, 0.51] | 0.58 [0.35, 0.82] | -2.15 [-2.65, -1.70]* | -1.08 [-1.44, -0.73]* |  | 1.22 [0.80, 1.56] | 1.20 [0.78, 1.54] |  |  |  | 0.07 [-0.04, 0.17] |  |  |  | 0.31 [0.15, 0.48] | 0.08 [0.05, 0.11] | h5 |
| 14 |  |  | 0.04 [-0.11, 0.16] | 0.40 [0.24, 0.60] | 2.90 [2.10, 3.78] | 0.45 [0.33, 0.57] | 0.64 [0.36, 0.93] | -2.24 [-2.76, -1.77]* | -0.93 [-1.31, -0.54]* |  | 1.27 [0.77, 1.71] | 1.25 [0.75, 1.68] |  |  |  | 0.05 [-0.06, 0.16] |  |  |  | 0.26 [0.12, 0.41] | 0.07 [0.05, 0.10] | h5 |
| 21 |  |  | 0.07 [-0.10, 0.22] | 0.36 [0.20, 0.56] | 2.77 [2.05, 3.58] | 0.43 [0.35, 0.51] | 0.65 [0.42, 0.89] | -2.20 [-2.69, -1.73]* | -0.95 [-1.30, -0.57]* |  | 1.41 [0.93, 1.87] | 1.39 [0.90, 1.86] |  |  |  | 0.09 [-0.04, 0.21] |  |  |  | 0.34 [0.16, 0.56] | 0.08 [0.05, 0.12] | h5 |
| 30 |  |  | 0.10 [-0.07, 0.22] | 0.32 [0.13, 0.52] | 2.88 [2.05, 3.84] | 0.52 [0.39, 0.66] | 0.52 [0.27, 0.74] | -2.37 [-2.88, -1.89]* | -0.77 [-1.14, -0.40]* |  | 1.28 [0.83, 1.74] | 1.25 [0.80, 1.72] |  |  |  | 0.17 [0.04, 0.26] |  |  |  | 0.46 [0.29, 0.64] | 0.09 [0.06, 0.12] | h5 |
| 45 |  |  | 0.12 [-0.01, 0.22] | 0.30 [0.08, 0.55] | 1.80 [1.08, 2.68] | 0.45 [0.30, 0.63] | 0.49 [0.17, 0.82] | -2.47 [-3.01, -1.97]* | -0.86 [-1.27, -0.42]* |  | 1.48 [0.89, 2.07] | 1.45 [0.85, 2.04] |  |  |  | 0.23 [0.12, 0.34] |  |  |  | 0.50 [0.32, 0.72] | 0.08 [0.06, 0.11] | h5 |
| 60 |  |  | 0.13 [-0.01, 0.26] | 0.37 [0.08, 0.72] |  | 0.48 [0.34, 0.64] | 0.10 [-0.12, 0.23] | -2.59 [-3.14, -2.07]* | -0.80 [-1.25, -0.32]* |  | 0.02 [0.00, 1.32] | 0.03 [0.00, 1.42] |  |  |  | 0.23 [0.11, 0.36] |  |  |  | 0.47 [0.27, 0.71] | 0.09 [0.06, 0.12] | h5 |
| 90 |  |  | 0.14 [-0.03, 0.30] | 0.37 [0.04, 0.80] |  | 0.59 [0.42, 0.83] |  | -2.97 [-3.65, -2.28]* | -0.70 [-1.10, -0.21]* |  |  |  |  |  |  | 0.31 [0.20, 0.47] |  |  |  | 0.55 [0.28, 0.88] | 0.09 [0.06, 0.12] | h5 |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.39 [-1.80, -1.04]* | -0.39 [-0.88, 0.03] |  | 1.30 [0.99, 1.63] | 5.33 [2.98, 8.27] | 0.42 [0.17, 0.66] | -0.13 [-0.62, 0.24] | -4.64 [-5.68, -3.70]* | -2.87 [-3.22, -2.49]* |  |  |  | 2.68 [1.91, 3.33] | -0.30 [-0.73, 0.08] | -1.18 [-1.60, -0.84]* |  | -0.33 [-0.75, 0.07] | 10.27 [6.78, 13.56] | -0.24 [-0.66, 0.17] |  | 0.13 [0.07, 0.19] | h5 |
| m015 | -8.14 [-9.64, -6.66]* |  |  | 1.23 [0.94, 1.54] |  | 0.41 [0.16, 0.68] | -0.12 [-0.64, 0.29] |  |  | -9.35 [-10.79, -7.81]* |  |  | 2.56 [1.79, 3.21] | -8.26 [-9.81, -6.72]* |  |  | -8.48 [-10.09, -6.88]* |  |  |  | -0.00 [-0.15, 0.13] | h5 |
| m030 | -6.28 [-7.62, -5.03]* |  |  | 1.21 [0.93, 1.50] |  | 0.45 [0.19, 0.72] | -0.07 [-0.57, 0.33] |  |  | -5.99 [-7.38, -4.67]* |  |  | 2.76 [1.98, 3.43] | -5.94 [-7.28, -4.68]* |  |  | -6.26 [-7.64, -4.95]* |  |  |  | 0.11 [0.06, 0.17] | h5 |
| m045 | -5.54 [-6.68, -4.49]* |  |  | 1.25 [0.92, 1.59] |  | 0.45 [0.18, 0.73] | -0.08 [-0.58, 0.33] |  |  | -4.24 [-5.64, -3.07]* |  |  | 2.63 [1.82, 3.29] | -4.88 [-6.02, -3.91]* |  |  | -5.35 [-6.49, -4.32]* |  |  |  | 0.12 [0.03, 0.24] | h5 |
| m060 | -5.25 [-6.36, -4.21]* |  |  | 1.22 [0.93, 1.52] |  | 0.40 [0.15, 0.66] | -0.14 [-0.63, 0.24] |  |  | -3.16 [-4.57, -1.97]* |  |  | 2.63 [1.85, 3.28] | -4.35 [-5.43, -3.42]* |  |  | -4.92 [-6.05, -3.91]* |  |  |  | 0.11 [0.01, 0.22] | h5 |
| m075 | -4.72 [-5.69, -3.75]* |  |  | 1.34 [0.98, 1.72] |  | 0.40 [0.16, 0.64] | -0.18 [-0.67, 0.19] |  |  | -2.54 [-3.84, -1.40]* |  |  | 2.78 [1.96, 3.44] | -3.69 [-4.62, -2.86]* |  |  | -4.33 [-5.33, -3.39]* |  |  |  | 0.14 [0.05, 0.26] | h5 |
| m090 | -4.42 [-5.42, -3.51]* |  |  | 1.43 [1.05, 1.85] |  | 0.43 [0.19, 0.66] | -0.09 [-0.56, 0.25] |  |  | -1.56 [-2.92, -0.46]* |  |  | 2.77 [1.97, 3.45] | -3.33 [-4.28, -2.51]* |  |  | -3.88 [-4.88, -2.98]* |  |  |  | 0.22 [0.13, 0.33] | h5 |
| m105 | -4.37 [-5.37, -3.42]* |  |  | 1.38 [1.06, 1.74] |  | 0.45 [0.21, 0.69] | -0.16 [-0.64, 0.21] |  |  | -1.12 [-2.58, 0.17] |  |  | 2.76 [1.91, 3.47] | -3.23 [-4.23, -2.33]* |  |  | -3.78 [-4.73, -2.86]* |  |  |  | 0.17 [0.09, 0.24] | h5 |
| m120 | -4.25 [-5.21, -3.37]* |  |  | 1.39 [1.07, 1.75] |  | 0.38 [0.17, 0.57] | -0.21 [-0.72, 0.15] |  |  | -0.49 [-1.92, 0.79] |  |  | 2.67 [1.92, 3.28] | -2.98 [-3.90, -2.15]* |  |  | -3.60 [-4.50, -2.73]* |  |  |  | 0.16 [0.08, 0.26] | h5 |
| h2-4 | -3.62 [-4.42, -2.83]* |  |  | 1.30 [1.01, 1.62] |  | 0.44 [0.18, 0.70] | -0.15 [-0.65, 0.25] |  |  | 1.36 [0.07, 2.47] |  |  | 2.68 [1.88, 3.36] | -2.38 [-3.12, -1.70]* |  |  | -2.96 [-3.78, -2.16]* |  |  |  | 0.12 [0.07, 0.17] | h5 |
| h4-8 | -2.57 [-3.15, -2.01]* |  |  | 1.28 [0.99, 1.56] |  | 0.43 [0.19, 0.67] | -0.13 [-0.65, 0.26] |  |  | 5.31 [3.87, 6.63] |  |  | 2.74 [1.94, 3.44] | -1.44 [-2.01, -0.93]* |  |  | -1.91 [-2.52, -1.30]* |  |  |  | 0.16 [0.12, 0.21] | h5 |
| h8+ | -1.06 [-1.47, -0.75]* |  |  | 1.13 [0.86, 1.43] |  | 0.44 [0.25, 0.63] | 0.15 [-0.24, 0.48] |  |  | 10.95 [7.40, 15.16] |  |  | 2.74 [1.90, 3.54] | -0.08 [-0.74, 0.49] |  |  | -0.30 [-0.77, 0.11] |  |  |  | 0.20 [0.08, 0.32] | h5 |
| 1 | -0.47 [-0.90, -0.09]* | 0.60 [0.07, 1.13] | -0.78 [-1.14, -0.45]* | 1.20 [0.79, 1.75] | 4.85 [2.97, 6.91] | 0.10 [-0.19, 0.35] | -0.27 [-0.79, 0.14] | -4.25 [-4.98, -3.51]* | -3.24 [-3.61, -2.84]* |  |  |  | 2.63 [2.05, 3.15] | 0.52 [0.04, 0.96] | -0.23 [-0.61, 0.15] | -0.61 [-0.93, -0.33]* | 0.65 [0.13, 1.18] | 10.98 [7.89, 13.98] | 0.73 [0.21, 1.25] | -0.59 [-0.91, -0.28]* | 0.14 [0.08, 0.22] | h5 |
| 2 | -0.41 [-1.05, 0.08] | 0.85 [0.13, 1.52] | -0.76 [-1.20, -0.41]* | 1.16 [0.89, 1.52] | 5.60 [3.22, 8.48] | -0.02 [-0.24, 0.18] | -0.26 [-0.84, 0.15] | -4.82 [-5.77, -3.88]* | -3.42 [-3.79, -3.06]* |  |  |  | 2.29 [1.56, 2.87] | 0.70 [0.06, 1.28] | -0.08 [-0.56, 0.30] | -0.64 [-1.02, -0.34]* | 0.97 [0.36, 1.52] | 10.89 [7.59, 13.99] | 1.05 [0.44, 1.59] | -0.45 [-0.79, -0.12]* | 0.15 [0.10, 0.22] | h5 |
| 3 | -0.05 [-0.65, 0.38] | 1.11 [0.35, 1.74] | -0.51 [-0.93, -0.18]* | 1.24 [0.88, 1.73] | 4.88 [3.03, 6.79] | 0.21 [-0.08, 0.45] | 0.06 [-0.38, 0.41] | -4.73 [-5.71, -3.81]* | -3.20 [-3.57, -2.77]* |  | 1.33 [0.68, 1.87] | 1.27 [0.63, 1.80] | 2.01 [1.34, 2.52] | 0.99 [0.37, 1.45] | 0.28 [-0.29, 0.72] | -0.42 [-0.82, -0.13]* | 1.40 [0.80, 1.93] | 10.70 [7.87, 13.36] | 1.49 [0.87, 2.00] | -0.29 [-0.68, 0.03] | 0.19 [0.12, 0.28] | h5 |
| 4 | 0.35 [-0.40, 0.90] | 1.76 [0.77, 2.53] | -0.34 [-0.73, -0.06]* | 1.21 [0.77, 1.80] | 5.42 [3.24, 7.46] | 0.24 [-0.04, 0.49] | 0.35 [-0.22, 0.83] | -4.61 [-5.62, -3.68]* | -3.13 [-3.51, -2.67]* |  | 1.58 [0.91, 2.14] | 1.51 [0.86, 2.07] | 2.23 [1.43, 2.87] | 1.52 [0.69, 2.19] | 0.72 [0.05, 1.29] | -0.27 [-0.65, 0.01] | 1.93 [1.16, 2.55] | 11.29 [8.00, 14.24] | 2.02 [1.25, 2.63] | -0.00 [-0.46, 0.37] | 0.13 [0.09, 0.19] | h5 |
| 5 | 0.06 [-0.50, 0.55] | 1.36 [0.50, 2.06] | -0.49 [-0.78, -0.30]* | 1.04 [0.72, 1.40] | 5.42 [3.05, 8.28] | 0.08 [-0.18, 0.33] | 0.21 [-0.38, 0.85] | -4.91 [-5.95, -3.92]* | -3.28 [-3.65, -2.90]* |  | 1.58 [0.80, 2.31] | 1.51 [0.74, 2.22] | 2.27 [1.46, 2.96] | 1.26 [0.47, 1.94] | 0.45 [-0.15, 0.96] | -0.46 [-0.73, -0.27]* | 1.51 [0.79, 2.17] | 10.68 [7.28, 13.80] | 1.58 [0.86, 2.26] | -0.22 [-0.57, 0.06] | 0.17 [0.11, 0.23] | h5 |
| 6 | 0.05 [-0.61, 0.62] | 1.27 [0.47, 1.89] | -0.64 [-1.09, -0.29]* | 1.21 [0.78, 1.81] | 4.90 [3.14, 6.91] | -0.19 [-0.53, 0.09] | 0.03 [-0.55, 0.49] | -4.50 [-5.31, -3.70]* | -3.63 [-4.05, -3.19]* |  | 1.41 [0.66, 2.04] | 1.35 [0.59, 1.98] | 2.25 [1.60, 2.79] | 1.10 [0.39, 1.65] | 0.13 [-0.35, 0.52] | -0.55 [-0.94, -0.24]* | 1.48 [0.85, 2.00] | 10.25 [7.07, 13.14] | 1.55 [0.91, 2.09] | -0.37 [-0.77, -0.02]* | 0.15 [0.09, 0.23] | h5 |
| 7 | 0.18 [-0.51, 0.68] | 1.39 [0.51, 2.07] | -0.53 [-0.96, -0.23]* | 0.93 [0.60, 1.33] | 5.52 [3.35, 8.19] | 0.27 [0.01, 0.51] | 0.02 [-0.50, 0.44] | -5.18 [-6.16, -4.19]* | -2.99 [-3.40, -2.59]* |  | 1.26 [0.50, 1.92] | 1.20 [0.45, 1.85] |  | 1.31 [0.51, 1.93] | 0.63 [0.07, 1.07] | -0.44 [-0.78, -0.18]* | 1.77 [0.99, 2.44] | 10.36 [7.10, 13.40] | 1.84 [1.05, 2.53] | -0.06 [-0.46, 0.27] | 0.18 [0.13, 0.24] | h5 |
| 10 |  |  | -0.23 [-0.51, -0.01]* | 0.90 [0.59, 1.21] | 5.66 [3.31, 8.28] | 0.50 [0.12, 0.88] | 0.64 [0.07, 1.10] | -5.18 [-6.18, -4.21]* | -2.69 [-3.18, -2.14]* |  | 2.06 [1.35, 2.64] | 2.00 [1.29, 2.57] |  |  |  | -0.23 [-0.47, -0.06]* |  |  |  | 0.33 [-0.07, 0.71] | 0.21 [0.15, 0.28] | h5 |
| 14 |  |  | -0.25 [-0.63, 0.01] | 0.89 [0.46, 1.37] | 5.49 [3.15, 7.65] | 0.57 [0.31, 0.81] | 0.78 [0.28, 1.24] | -5.24 [-6.43, -4.16]* | -2.71 [-3.12, -2.24]* |  | 2.25 [1.47, 3.07] | 2.19 [1.41, 2.99] |  |  |  | -0.24 [-0.54, -0.02]* |  |  |  | 0.27 [-0.09, 0.63] | 0.17 [0.12, 0.24] | h5 |
| 21 |  |  | -0.20 [-0.61, 0.15] | 0.79 [0.35, 1.28] | 5.07 [2.95, 7.12] | 0.51 [0.30, 0.67] | 0.65 [0.16, 1.14] | -5.29 [-6.29, -4.31]* | -2.70 [-2.99, -2.37]* |  | 2.35 [1.50, 3.36] | 2.28 [1.44, 3.31] |  |  |  | -0.19 [-0.53, 0.11] |  |  |  | 0.41 [-0.09, 1.00] | 0.20 [0.14, 0.28] | h5 |
| 30 |  |  | -0.10 [-0.53, 0.19] | 0.65 [0.17, 1.12] | 5.31 [2.92, 7.76] | 0.83 [0.49, 1.19] | 0.54 [-0.03, 1.04] | -5.57 [-6.58, -4.60]* | -2.22 [-2.79, -1.67]* |  | 2.27 [1.32, 3.21] | 2.19 [1.24, 3.14] |  |  |  | 0.05 [-0.26, 0.30] |  |  |  | 0.73 [0.31, 1.16] | 0.21 [0.15, 0.28] | h5 |
| 45 |  |  | -0.01 [-0.33, 0.23] | 0.72 [0.22, 1.27] | 2.68 [1.16, 4.07] | 0.70 [0.32, 1.12] | 0.48 [-0.33, 1.35] | -5.78 [-6.73, -4.87]* | -2.39 [-3.00, -1.67]* |  | 2.87 [1.43, 4.15] | 2.79 [1.33, 4.07] |  |  |  | 0.24 [-0.06, 0.51] |  |  |  | 0.81 [0.36, 1.31] | 0.21 [0.16, 0.26] | h5 |
| 60 |  |  | -0.04 [-0.41, 0.27] | 0.87 [0.15, 1.61] |  | 0.63 [0.27, 1.06] |  | -5.96 [-6.89, -5.04]* | -2.40 [-3.06, -1.58]* |  |  |  |  |  |  | 0.14 [-0.18, 0.44] |  |  |  | 0.71 [0.22, 1.32] | 0.21 [0.16, 0.27] | h5 |
| 90 |  |  | -0.16 [-0.60, 0.23] | 0.71 [-0.08, 1.76] |  | 0.85 [0.58, 1.20] |  | -6.89 [-8.01, -5.71]* | -2.16 [-2.67, -1.52]* |  |  |  |  |  |  | 0.26 [-0.03, 0.62] |  |  |  | 0.74 [0.13, 1.56] | 0.20 [0.15, 0.25] | h5 |

### Paired against `lvlh5_tft` (not the per-lead reference)

MAE difference model − `lvlh5_tft` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.63 [-0.89, -0.37]* | -0.23 [-0.44, -0.00]* |  | 0.34 [0.18, 0.55] | -0.22 [-0.36, -0.05]* | 2.70 [1.94, 3.75] | 0.15 [-0.02, 0.33] | -1.86 [-2.34, -1.44]* | -1.29 [-1.72, -0.88]* |  |  |  | 1.01 [0.72, 1.30] | -0.13 [-0.32, 0.09] | -0.57 [-0.83, -0.31]* |  | -0.14 [-0.35, 0.11] | 3.97 [2.57, 5.60] | -0.11 [-0.33, 0.14] |  | -0.17 [-0.32, 0.00] | lvlh5_tft |
| m015 | -3.90 [-4.73, -3.17]* |  |  | 0.35 [0.18, 0.57] | -0.19 [-0.34, -0.01]* |  | 0.16 [-0.01, 0.34] |  |  | -4.60 [-5.44, -3.85]* |  |  | 1.00 [0.72, 1.27] | -3.99 [-4.82, -3.24]* |  |  | -4.10 [-4.95, -3.34]* |  |  |  | -0.20 [-0.40, -0.00]* | lvlh5_tft |
| m030 | -2.96 [-3.58, -2.40]* |  |  | 0.30 [0.14, 0.51] | -0.24 [-0.40, -0.06]* |  | 0.14 [-0.02, 0.30] |  |  | -3.00 [-3.67, -2.42]* |  |  | 1.04 [0.76, 1.32] | -2.84 [-3.47, -2.29]* |  |  | -2.99 [-3.63, -2.41]* |  |  |  | -0.19 [-0.35, -0.01]* | lvlh5_tft |
| m045 | -2.60 [-3.16, -2.09]* |  |  | 0.32 [0.15, 0.54] | -0.23 [-0.38, -0.05]* |  | 0.14 [-0.03, 0.32] |  |  | -2.18 [-2.80, -1.66]* |  |  | 1.00 [0.70, 1.30] | -2.35 [-2.86, -1.90]* |  |  | -2.56 [-3.12, -2.06]* |  |  |  | -0.16 [-0.32, 0.02] | lvlh5_tft |
| m060 | -2.44 [-2.98, -1.93]* |  |  | 0.33 [0.16, 0.56] | -0.19 [-0.33, -0.01]* |  | 0.15 [-0.01, 0.32] |  |  | -1.59 [-2.19, -1.08]* |  |  | 1.04 [0.73, 1.33] | -2.06 [-2.54, -1.65]* |  |  | -2.33 [-2.87, -1.84]* |  |  |  | -0.15 [-0.29, 0.02] | lvlh5_tft |
| m075 | -2.19 [-2.69, -1.73]* |  |  | 0.35 [0.17, 0.57] | -0.20 [-0.34, -0.02]* |  | 0.18 [0.01, 0.36] |  |  | -1.18 [-1.76, -0.68]* |  |  | 1.04 [0.71, 1.35] | -1.74 [-2.17, -1.36]* |  |  | -2.01 [-2.49, -1.56]* |  |  |  | -0.16 [-0.30, 0.03] | lvlh5_tft |
| m090 | -2.08 [-2.53, -1.65]* |  |  | 0.36 [0.17, 0.60] | -0.25 [-0.39, -0.08]* |  | 0.15 [-0.02, 0.33] |  |  | -0.75 [-1.38, -0.24]* |  |  | 0.99 [0.67, 1.30] | -1.59 [-1.99, -1.23]* |  |  | -1.85 [-2.29, -1.46]* |  |  |  | -0.17 [-0.30, 0.01] | lvlh5_tft |
| m105 | -1.99 [-2.44, -1.56]* |  |  | 0.36 [0.19, 0.56] | -0.23 [-0.37, -0.07]* |  | 0.17 [-0.00, 0.34] |  |  | -0.46 [-1.09, 0.10] |  |  | 1.01 [0.72, 1.28] | -1.47 [-1.85, -1.10]* |  |  | -1.75 [-2.17, -1.36]* |  |  |  | -0.17 [-0.30, 0.00] | lvlh5_tft |
| m120 | -1.91 [-2.35, -1.49]* |  |  | 0.38 [0.22, 0.61] | -0.19 [-0.34, -0.03]* |  | 0.15 [-0.02, 0.33] |  |  | -0.13 [-0.75, 0.44] |  |  | 1.06 [0.77, 1.35] | -1.33 [-1.71, -0.98]* |  |  | -1.63 [-2.03, -1.26]* |  |  |  | -0.14 [-0.29, 0.03] | lvlh5_tft |
| h2-4 | -1.59 [-1.99, -1.22]* |  |  | 0.33 [0.17, 0.55] | -0.22 [-0.37, -0.04]* |  | 0.16 [-0.01, 0.34] |  |  | 0.74 [0.12, 1.38] |  |  | 1.02 [0.72, 1.31] | -1.05 [-1.34, -0.74]* |  |  | -1.31 [-1.69, -0.94]* |  |  |  | -0.18 [-0.33, 0.00] | lvlh5_tft |
| h4-8 | -1.18 [-1.52, -0.85]* |  |  | 0.34 [0.17, 0.55] | -0.23 [-0.39, -0.05]* |  | 0.17 [-0.01, 0.36] |  |  | 2.55 [1.73, 3.44] |  |  | 1.07 [0.75, 1.38] | -0.69 [-0.93, -0.42]* |  |  | -0.88 [-1.21, -0.55]* |  |  |  | -0.17 [-0.33, 0.02] | lvlh5_tft |
| h8+ | -0.82 [-1.07, -0.56]* |  |  | 0.08 [-0.10, 0.33] | -0.42 [-0.57, -0.25]* |  | 0.04 [-0.13, 0.23] |  |  | 5.07 [3.44, 7.18] |  |  | 0.92 [0.55, 1.27] | -0.40 [-0.61, -0.19]* |  |  | -0.50 [-0.73, -0.24]* |  |  |  | -0.35 [-0.50, -0.17]* | lvlh5_tft |
| 1 | -0.47 [-0.68, -0.27]* | -0.04 [-0.31, 0.19] | -0.45 [-0.63, -0.28]* | 0.22 [-0.03, 0.45] | -0.29 [-0.50, -0.09]* | 2.48 [1.81, 3.24] | -0.03 [-0.15, 0.11] | -1.83 [-2.24, -1.48]* | -1.50 [-1.86, -1.16]* |  |  |  | 0.93 [0.64, 1.23] | -0.05 [-0.27, 0.15] | -0.48 [-0.66, -0.29]* | -0.38 [-0.53, -0.23]* | -0.01 [-0.25, 0.22] | 4.05 [2.83, 5.43] | 0.03 [-0.22, 0.26] | -0.39 [-0.52, -0.24]* | -0.24 [-0.45, -0.03]* | lvlh5_tft |
| 2 | -0.30 [-0.45, -0.16]* | 0.16 [-0.09, 0.45] | -0.32 [-0.43, -0.21]* | 0.32 [0.09, 0.58] | -0.22 [-0.37, -0.02]* | 2.86 [2.01, 3.94] | 0.03 [-0.11, 0.16] | -2.03 [-2.42, -1.67]* | -1.43 [-1.86, -1.05]* |  |  |  | 0.85 [0.50, 1.22] | 0.10 [-0.12, 0.35] | -0.34 [-0.51, -0.18]* | -0.29 [-0.39, -0.19]* | 0.23 [-0.02, 0.51] | 4.34 [2.94, 5.93] | 0.26 [0.01, 0.55] | -0.24 [-0.36, -0.12]* | -0.17 [-0.32, 0.04] | lvlh5_tft |
| 3 | -0.31 [-0.55, -0.10]* | 0.10 [-0.18, 0.37] | -0.40 [-0.56, -0.26]* | 0.26 [0.04, 0.50] | -0.34 [-0.49, -0.16]* | 2.54 [1.86, 3.34] | -0.09 [-0.23, 0.06] | -2.20 [-2.68, -1.78]* | -1.57 [-1.85, -1.27]* |  | 0.63 [0.39, 0.87] | 0.61 [0.38, 0.84] | 0.42 [0.11, 0.74] | 0.03 [-0.22, 0.26] | -0.40 [-0.61, -0.18]* | -0.37 [-0.52, -0.24]* | 0.21 [-0.01, 0.46] | 3.82 [2.69, 5.12] | 0.24 [0.02, 0.49] | -0.33 [-0.48, -0.19]* | -0.27 [-0.42, -0.09]* | lvlh5_tft |
| 4 | -0.19 [-0.48, 0.06] | 0.30 [-0.05, 0.64] | -0.39 [-0.58, -0.23]* | 0.11 [-0.17, 0.43] | -0.42 [-0.64, -0.20]* | 2.60 [1.92, 3.36] | -0.13 [-0.33, 0.05] | -2.17 [-2.64, -1.74]* | -1.59 [-2.04, -1.18]* |  | 0.62 [0.42, 0.81] | 0.59 [0.39, 0.79] | 0.64 [0.25, 1.01] | 0.18 [-0.12, 0.48] | -0.22 [-0.49, 0.01] | -0.37 [-0.56, -0.21]* | 0.37 [0.10, 0.66] | 4.15 [2.85, 5.61] | 0.41 [0.14, 0.70] | -0.32 [-0.51, -0.16]* | -0.38 [-0.58, -0.15]* | lvlh5_tft |
| 5 | -0.28 [-0.57, -0.04]* | 0.22 [-0.15, 0.54] | -0.42 [-0.65, -0.24]* | 0.16 [-0.13, 0.44] | -0.41 [-0.69, -0.16]* | 2.58 [1.83, 3.48] | -0.18 [-0.37, 0.02] | -2.15 [-2.72, -1.68]* | -1.69 [-2.15, -1.27]* |  | 0.68 [0.47, 0.89] | 0.66 [0.45, 0.87] | 0.62 [0.26, 0.94] | 0.13 [-0.19, 0.44] | -0.32 [-0.61, -0.05]* | -0.44 [-0.68, -0.24]* | 0.26 [-0.05, 0.57] | 4.01 [2.69, 5.49] | 0.29 [-0.02, 0.60] | -0.37 [-0.61, -0.16]* | -0.35 [-0.65, -0.10]* | lvlh5_tft |
| 6 | -0.36 [-0.60, -0.15]* | 0.07 [-0.24, 0.38] | -0.52 [-0.69, -0.36]* | 0.12 [-0.14, 0.38] | -0.45 [-0.67, -0.22]* | 2.36 [1.69, 3.10] | -0.30 [-0.45, -0.14]* | -2.09 [-2.49, -1.73]* | -1.84 [-2.18, -1.51]* |  | 0.73 [0.50, 0.95] | 0.71 [0.49, 0.93] | 0.61 [0.27, 0.94] | -0.00 [-0.31, 0.29] | -0.54 [-0.75, -0.35]* | -0.53 [-0.69, -0.36]* | 0.12 [-0.19, 0.45] | 3.49 [2.26, 4.90] | 0.14 [-0.16, 0.48] | -0.48 [-0.63, -0.33]* | -0.39 [-0.61, -0.14]* | lvlh5_tft |
| 7 | -0.15 [-0.38, 0.06] | 0.28 [-0.05, 0.64] | -0.37 [-0.52, -0.22]* | 0.13 [-0.12, 0.42] | -0.36 [-0.52, -0.16]* | 2.70 [1.97, 3.64] | 0.01 [-0.16, 0.22] | -2.36 [-2.80, -1.95]* | -1.36 [-1.81, -0.98]* |  | 0.61 [0.38, 0.83] | 0.59 [0.36, 0.80] |  | 0.21 [-0.09, 0.51] | -0.27 [-0.48, -0.05]* | -0.37 [-0.50, -0.24]* | 0.46 [0.11, 0.85] | 3.97 [2.59, 5.53] | 0.49 [0.14, 0.89] | -0.29 [-0.43, -0.14]* | -0.29 [-0.46, -0.10]* | lvlh5_tft |
| 10 |  |  | -0.54 [-0.70, -0.38]* | -0.09 [-0.34, 0.18] | -0.58 [-0.82, -0.35]* | 2.56 [1.83, 3.47] | -0.24 [-0.39, -0.07]* | -2.50 [-3.08, -2.00]* | -1.63 [-2.03, -1.23]* |  | 0.65 [0.42, 0.85] | 0.63 [0.40, 0.84] |  |  |  | -0.55 [-0.72, -0.38]* |  |  |  | -0.41 [-0.60, -0.23]* | -0.51 [-0.74, -0.28]* | lvlh5_tft |
| 14 |  |  | -0.58 [-0.83, -0.34]* | -0.26 [-0.57, 0.10] | -0.64 [-0.93, -0.36]* | 2.37 [1.57, 3.24] | -0.18 [-0.46, 0.08] | -2.65 [-3.16, -2.14]* | -1.50 [-2.00, -1.03]* |  | 0.63 [0.38, 0.87] | 0.60 [0.35, 0.85] |  |  |  | -0.60 [-0.85, -0.36]* |  |  |  | -0.50 [-0.78, -0.25]* | -0.57 [-0.86, -0.30]* | lvlh5_tft |
| 21 |  |  | -0.51 [-0.75, -0.27]* | -0.31 [-0.60, 0.02] | -0.65 [-0.89, -0.42]* | 2.20 [1.43, 3.08] | -0.18 [-0.43, 0.07] | -2.64 [-3.14, -2.13]* | -1.50 [-1.93, -1.08]* |  | 0.76 [0.46, 1.07] | 0.74 [0.44, 1.05] |  |  |  | -0.49 [-0.71, -0.26]* |  |  |  | -0.34 [-0.60, -0.05]* | -0.58 [-0.82, -0.33]* | lvlh5_tft |
| 30 |  |  | -0.55 [-0.77, -0.35]* | -0.24 [-0.52, 0.09] | -0.52 [-0.74, -0.27]* | 2.35 [1.54, 3.29] | -0.05 [-0.29, 0.20] | -2.81 [-3.41, -2.26]* | -1.33 [-1.85, -0.81]* |  | 0.71 [0.40, 1.03] | 0.69 [0.38, 1.00] |  |  |  | -0.48 [-0.67, -0.31]* |  |  |  | -0.30 [-0.53, -0.08]* | -0.44 [-0.67, -0.19]* | lvlh5_tft |
| 45 |  |  | -0.55 [-0.91, -0.19]* | -0.17 [-0.67, 0.36] | -0.49 [-0.82, -0.17]* | 0.58 [-0.30, 1.35] | -0.00 [-0.35, 0.39] | -3.07 [-3.77, -2.42]* | -1.23 [-1.81, -0.59]* |  | 0.88 [0.56, 1.22] | 0.85 [0.52, 1.19] |  |  |  | -0.41 [-0.72, -0.12]* |  |  |  | -0.18 [-0.55, 0.23] | -0.39 [-0.72, -0.06]* | lvlh5_tft |
| 60 |  |  | -0.06 [-0.21, 0.19] | -0.10 [-0.23, -0.09] | -0.10 [-0.23, 0.12] |  | 0.07 [-0.24, 0.10] | -0.13 [-0.21, 0.35] | 0.22 [-0.21, 0.72] |  | -0.05 [-0.21, 1.50] | -0.05 [-0.21, 1.59] |  |  |  | -0.07 [-0.21, 0.03] |  |  |  | -0.06 [-0.21, 0.63] | -0.10 [-0.26, 0.20] | lvlh5_tft |

segment `busy`:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | oracle_level | oracle_shape | persistence | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.96 [-1.49, -0.32]* | -0.02 [-0.49, 0.54] |  | 1.30 [0.94, 1.73] | 0.13 [-0.24, 0.62] | 5.58 [3.58, 8.23] | 0.56 [0.14, 1.07] | -4.07 [-4.99, -3.14]* | -2.82 [-3.47, -2.04]* |  |  |  | 2.68 [2.19, 3.19] | 0.13 [-0.28, 0.67] | -0.74 [-1.25, -0.13]* |  | 0.11 [-0.38, 0.72] | 10.10 [6.66, 13.43] | 0.20 [-0.30, 0.82] |  | 0.27 [-0.11, 0.76] | lvlh5_tft |
| m015 | -7.58 [-8.91, -6.09]* |  |  | 1.26 [0.87, 1.78] | 0.12 [-0.29, 0.64] |  | 0.55 [0.11, 1.07] |  |  | -8.82 [-10.12, -7.35]* |  |  | 2.63 [2.12, 3.22] | -7.75 [-9.13, -6.24]* |  |  | -7.99 [-9.42, -6.43]* |  |  |  | 0.11 [-0.39, 0.68] | lvlh5_tft |
| m030 | -5.67 [-6.80, -4.43]* |  |  | 1.17 [0.80, 1.63] | 0.07 [-0.33, 0.57] |  | 0.51 [0.09, 1.00] |  |  | -5.46 [-6.74, -4.27]* |  |  | 2.78 [2.29, 3.33] | -5.43 [-6.58, -4.23]* |  |  | -5.75 [-6.94, -4.44]* |  |  |  | 0.20 [-0.23, 0.72] | lvlh5_tft |
| m045 | -5.01 [-6.01, -3.97]* |  |  | 1.21 [0.79, 1.66] | 0.08 [-0.33, 0.58] |  | 0.51 [0.07, 1.02] |  |  | -3.84 [-5.13, -2.73]* |  |  | 2.61 [2.07, 3.16] | -4.44 [-5.48, -3.47]* |  |  | -4.91 [-5.93, -3.85]* |  |  |  | 0.25 [-0.14, 0.72] | lvlh5_tft |
| m060 | -4.66 [-5.61, -3.60]* |  |  | 1.24 [0.81, 1.70] | 0.14 [-0.24, 0.63] |  | 0.53 [0.10, 1.06] |  |  | -2.67 [-3.95, -1.55]* |  |  | 2.66 [2.11, 3.21] | -3.83 [-4.85, -2.92]* |  |  | -4.41 [-5.39, -3.35]* |  |  |  | 0.28 [-0.08, 0.74] | lvlh5_tft |
| m075 | -4.14 [-4.97, -3.14]* |  |  | 1.35 [0.98, 1.76] | 0.18 [-0.19, 0.67] |  | 0.63 [0.20, 1.15] |  |  | -2.00 [-3.24, -0.94]* |  |  | 2.75 [2.19, 3.28] | -3.18 [-4.03, -2.39]* |  |  | -3.80 [-4.69, -2.80]* |  |  |  | 0.31 [-0.09, 0.80] | lvlh5_tft |
| m090 | -3.91 [-4.77, -3.02]* |  |  | 1.39 [0.96, 1.86] | 0.09 [-0.25, 0.56] |  | 0.57 [0.15, 1.07] |  |  | -1.11 [-2.50, -0.01]* |  |  | 2.63 [2.11, 3.15] | -2.86 [-3.74, -2.10]* |  |  | -3.41 [-4.28, -2.51]* |  |  |  | 0.31 [-0.06, 0.79] | lvlh5_tft |
| m105 | -3.68 [-4.51, -2.77]* |  |  | 1.41 [1.05, 1.81] | 0.16 [-0.21, 0.64] |  | 0.62 [0.20, 1.14] |  |  | -0.54 [-2.02, 0.70] |  |  | 2.69 [2.19, 3.18] | -2.55 [-3.42, -1.71]* |  |  | -3.14 [-3.95, -2.27]* |  |  |  | 0.32 [-0.05, 0.83] | lvlh5_tft |
| m120 | -3.57 [-4.36, -2.70]* |  |  | 1.46 [1.08, 1.91] | 0.21 [-0.15, 0.72] |  | 0.61 [0.17, 1.14] |  |  | 0.12 [-1.36, 1.36] |  |  | 2.80 [2.25, 3.38] | -2.28 [-3.14, -1.47]* |  |  | -2.94 [-3.73, -2.13]* |  |  |  | 0.40 [0.01, 0.92] | lvlh5_tft |
| h2-4 | -3.05 [-3.81, -2.17]* |  |  | 1.30 [0.93, 1.74] | 0.15 [-0.25, 0.65] |  | 0.61 [0.17, 1.11] |  |  | 1.64 [0.25, 2.83] |  |  | 2.72 [2.21, 3.24] | -1.86 [-2.52, -1.14]* |  |  | -2.44 [-3.23, -1.56]* |  |  |  | 0.27 [-0.14, 0.78] | lvlh5_tft |
| h4-8 | -2.14 [-2.86, -1.35]* |  |  | 1.30 [0.92, 1.73] | 0.13 [-0.26, 0.65] |  | 0.61 [0.17, 1.12] |  |  | 5.02 [3.53, 6.43] |  |  | 2.80 [2.23, 3.35] | -1.09 [-1.65, -0.50]* |  |  | -1.50 [-2.24, -0.70]* |  |  |  | 0.29 [-0.10, 0.81] | lvlh5_tft |
| h8+ | -1.15 [-1.68, -0.57]* |  |  | 0.86 [0.47, 1.31] | -0.15 [-0.48, 0.24] |  | 0.34 [-0.03, 0.81] |  |  | 10.22 [6.62, 14.12] |  |  | 2.52 [1.87, 3.13] | -0.20 [-0.74, 0.27] |  |  | -0.38 [-0.89, 0.10] |  |  |  | 0.02 [-0.31, 0.41] | lvlh5_tft |
| 1 | -0.29 [-0.62, 0.16] | 0.71 [0.26, 1.16] | -0.60 [-0.96, -0.22]* | 1.39 [1.00, 1.91] | 0.27 [-0.14, 0.79] | 5.26 [3.64, 7.01] | 0.33 [0.03, 0.70] | -3.79 [-4.41, -3.08]* | -3.01 [-3.44, -2.48]* |  |  |  | 2.60 [2.05, 3.16] | 0.68 [0.31, 1.07] | -0.24 [-0.61, 0.23] | -0.46 [-0.75, -0.12]* | 0.79 [0.36, 1.27] | 10.54 [7.62, 13.37] | 0.87 [0.43, 1.35] | -0.50 [-0.80, -0.14]* | 0.42 [0.00, 0.93] | lvlh5_tft |
| 2 | -0.07 [-0.40, 0.32] | 1.07 [0.44, 1.70] | -0.47 [-0.68, -0.24]* | 1.39 [0.91, 2.01] | 0.26 [-0.15, 0.84] | 6.13 [3.87, 8.81] | 0.36 [-0.02, 0.74] | -4.23 [-4.97, -3.49]* | -3.19 [-3.80, -2.55]* |  |  |  | 2.39 [1.73, 3.10] | 0.96 [0.40, 1.47] | -0.01 [-0.41, 0.39] | -0.38 [-0.59, -0.14]* | 1.28 [0.68, 1.87] | 11.15 [7.68, 14.38] | 1.36 [0.74, 1.96] | -0.23 [-0.51, 0.07] | 0.39 [-0.01, 0.98] | lvlh5_tft |
| 3 | -0.06 [-0.54, 0.40] | 0.91 [0.24, 1.46] | -0.66 [-1.00, -0.36]* | 1.23 [0.79, 1.76] | -0.06 [-0.41, 0.38] | 5.23 [3.54, 6.93] | 0.12 [-0.21, 0.44] | -4.63 [-5.51, -3.70]* | -3.27 [-3.73, -2.81]* |  | 1.37 [0.99, 1.77] | 1.32 [0.94, 1.72] | 1.68 [1.08, 2.21] | 0.79 [0.26, 1.27] | -0.03 [-0.50, 0.44] | -0.55 [-0.89, -0.26]* | 1.18 [0.72, 1.63] | 9.87 [7.14, 12.32] | 1.25 [0.79, 1.70] | -0.45 [-0.80, -0.11]* | 0.13 [-0.23, 0.58] | lvlh5_tft |
| 4 | 0.14 [-0.45, 0.72] | 1.34 [0.52, 2.08] | -0.69 [-1.03, -0.37]* | 0.84 [0.16, 1.68] | -0.35 [-0.83, 0.22] | 5.32 [3.54, 7.07] | -0.08 [-0.58, 0.41] | -4.65 [-5.39, -3.76]* | -3.47 [-4.14, -2.81]* |  | 1.28 [0.91, 1.67] | 1.21 [0.85, 1.59] | 2.08 [1.32, 2.81] | 1.11 [0.44, 1.73] | 0.24 [-0.28, 0.79] | -0.62 [-0.99, -0.28]* | 1.53 [0.96, 2.07] | 10.41 [7.25, 13.19] | 1.61 [1.04, 2.15] | -0.51 [-0.86, -0.14]* | -0.23 [-0.69, 0.37] | lvlh5_tft |
| 5 | -0.09 [-0.64, 0.45] | 1.14 [0.42, 1.75] | -0.66 [-1.17, -0.22]* | 0.95 [0.47, 1.49] | -0.21 [-0.85, 0.38] | 5.38 [3.38, 7.71] | -0.09 [-0.57, 0.40] | -4.60 [-5.63, -3.64]* | -3.53 [-4.19, -2.87]* |  | 1.42 [1.02, 1.82] | 1.35 [0.95, 1.77] | 1.99 [1.38, 2.56] | 0.93 [0.29, 1.54] | -0.01 [-0.68, 0.65] | -0.68 [-1.22, -0.21]* | 1.31 [0.65, 1.91] | 10.37 [7.08, 13.55] | 1.38 [0.72, 2.00] | -0.50 [-1.07, 0.02] | -0.04 [-0.72, 0.56] | lvlh5_tft |
| 6 | -0.01 [-0.41, 0.36] | 1.05 [0.41, 1.59] | -0.64 [-1.00, -0.30]* | 1.20 [0.76, 1.78] | -0.03 [-0.49, 0.55] | 4.90 [3.37, 6.67] | -0.21 [-0.50, 0.13] | -4.31 [-4.92, -3.57]* | -3.69 [-4.03, -3.27]* |  | 1.40 [1.00, 1.82] | 1.34 [0.93, 1.76] | 2.02 [1.45, 2.56] | 0.85 [0.23, 1.41] | -0.31 [-0.65, 0.02] | -0.64 [-0.93, -0.31]* | 1.28 [0.70, 1.87] | 9.41 [6.33, 12.36] | 1.33 [0.75, 1.96] | -0.57 [-0.87, -0.25]* | 0.14 [-0.31, 0.70] | lvlh5_tft |
| 7 | 0.29 [-0.21, 0.73] | 1.38 [0.62, 2.09] | -0.47 [-0.78, -0.19]* | 1.03 [0.57, 1.62] | -0.02 [-0.44, 0.50] | 5.51 [3.57, 7.81] | 0.39 [-0.10, 0.99] | -4.92 [-5.73, -4.07]* | -3.03 [-3.69, -2.25]* |  | 1.26 [0.88, 1.69] | 1.20 [0.81, 1.62] |  | 1.16 [0.49, 1.77] | 0.15 [-0.34, 0.63] | -0.48 [-0.73, -0.25]* | 1.92 [1.11, 2.74] | 10.44 [6.97, 13.76] | 1.99 [1.17, 2.83] | -0.24 [-0.55, 0.07] | 0.13 [-0.29, 0.67] | lvlh5_tft |
| 10 |  |  | -0.94 [-1.25, -0.56]* | 0.43 [-0.04, 1.01] | -0.64 [-1.10, -0.07]* | 5.13 [3.15, 7.31] | -0.19 [-0.52, 0.21] | -5.44 [-6.49, -4.39]* | -3.41 [-3.90, -2.80]* |  | 1.45 [1.05, 1.85] | 1.38 [0.98, 1.81] |  |  |  | -0.95 [-1.24, -0.58]* |  |  |  | -0.60 [-0.97, -0.15]* | -0.45 [-0.95, 0.18] | lvlh5_tft |
| 14 |  |  | -0.94 [-1.40, -0.49]* | 0.10 [-0.46, 0.83] | -0.78 [-1.24, -0.28]* | 4.90 [2.78, 6.96] | -0.15 [-0.82, 0.51] | -5.59 [-6.42, -4.66]* | -3.48 [-4.45, -2.55]* |  | 1.45 [1.04, 1.93] | 1.38 [0.97, 1.88] |  |  |  | -1.00 [-1.43, -0.57]* |  |  |  | -0.71 [-1.23, -0.16]* | -0.63 [-1.10, -0.12]* | lvlh5_tft |
| 21 |  |  | -0.67 [-1.33, -0.08]* | 0.12 [-0.61, 0.92] | -0.65 [-1.14, -0.16]* | 4.50 [2.25, 6.71] | -0.02 [-0.62, 0.63] | -5.72 [-6.48, -4.86]* | -3.34 [-3.87, -2.73]* |  | 1.69 [1.16, 2.29] | 1.62 [1.09, 2.24] |  |  |  | -0.67 [-1.25, -0.09]* |  |  |  | -0.24 [-0.89, 0.53] | -0.46 [-0.94, 0.01] | lvlh5_tft |
| 30 |  |  | -0.95 [-1.42, -0.51]* | 0.22 [-0.50, 0.97] | -0.54 [-1.04, 0.03] | 4.79 [2.41, 7.07] | 0.20 [-0.31, 0.75] | -6.21 [-7.30, -5.13]* | -3.22 [-3.97, -2.32]* |  | 1.68 [1.06, 2.36] | 1.60 [0.95, 2.29] |  |  |  | -0.76 [-1.16, -0.38]* |  |  |  | -0.37 [-0.88, 0.16] | -0.35 [-0.88, 0.26] | lvlh5_tft |
| 45 |  |  | -0.89 [-1.95, 0.03] | 0.52 [-0.75, 1.69] | -0.48 [-1.35, 0.33] | 0.79 [-0.96, 2.55] | 0.23 [-0.80, 1.30] | -6.99 [-8.26, -5.68]* | -3.16 [-4.31, -1.73]* |  | 2.13 [1.35, 2.91] | 2.03 [1.25, 2.86] |  |  |  | -0.57 [-1.49, 0.22] |  |  |  | -0.13 [-1.21, 0.93] | -0.23 [-1.11, 0.61] | lvlh5_tft |

### Paired against `persistence` (not the per-lead reference)

MAE difference model − `persistence` on the slots BOTH cover, park-cluster CI, `*` = the model wins:

segment `all`:

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 0.27 [0.19, 0.37] | 5.25 [4.35, 6.17] | 4.71 [3.93, 5.54] | 5.03 [4.17, 5.91] | 4.60 [3.85, 5.44] | 5.97 [4.98, 7.01] | 0.15 [0.10, 0.21] | 0.00 [-0.06, 0.07] | 4.68 [3.93, 5.51] | persistence |
| m030 | -0.41 [-0.60, -0.20]* | 3.56 [2.87, 4.27] | 3.00 [2.40, 3.64] | 3.35 [2.69, 4.03] | 3.00 [2.42, 3.67] | 4.33 [3.52, 5.17] | -0.29 [-0.43, -0.15]* | -0.50 [-0.68, -0.30]* | 3.04 [2.43, 3.70] | persistence |
| m045 | -0.90 [-1.17, -0.61]* | 2.67 [2.09, 3.35] | 2.09 [1.56, 2.70] | 2.44 [1.86, 3.10] | 2.18 [1.66, 2.80] | 3.39 [2.68, 4.12] | -0.65 [-0.86, -0.43]* | -0.93 [-1.21, -0.64]* | 2.12 [1.60, 2.74] | persistence |
| m060 | -1.27 [-1.62, -0.91]* | 2.10 [1.55, 2.71] | 1.53 [1.01, 2.12] | 1.86 [1.28, 2.51] | 1.59 [1.08, 2.19] | 2.82 [2.15, 3.52] | -0.92 [-1.19, -0.64]* | -1.24 [-1.57, -0.89]* | 1.54 [1.03, 2.12] | persistence |
| m075 | -1.54 [-1.95, -1.13]* | 1.79 [1.20, 2.41] | 1.15 [0.63, 1.73] | 1.47 [0.90, 2.10] | 1.18 [0.68, 1.76] | 2.52 [1.86, 3.20] | -1.16 [-1.48, -0.83]* | -1.50 [-1.90, -1.10]* | 1.18 [0.66, 1.78] | persistence |
| m090 | -1.88 [-2.36, -1.39]* | 1.33 [0.76, 1.92] | 0.63 [0.13, 1.21] | 1.00 [0.48, 1.62] | 0.75 [0.24, 1.38] | 2.03 [1.45, 2.66] | -1.44 [-1.83, -1.05]* | -1.80 [-2.24, -1.33]* | 0.71 [0.19, 1.29] | persistence |
| m105 | -2.06 [-2.63, -1.54]* | 1.08 [0.47, 1.76] | 0.37 [-0.22, 1.02] | 0.75 [0.14, 1.42] | 0.46 [-0.10, 1.09] | 1.81 [1.16, 2.52] | -1.61 [-2.07, -1.17]* | -1.99 [-2.53, -1.49]* | 0.46 [-0.15, 1.11] | persistence |
| m120 | -2.25 [-2.83, -1.71]* | 0.79 [0.22, 1.44] | 0.08 [-0.49, 0.68] | 0.41 [-0.16, 1.06] | 0.13 [-0.44, 0.75] | 1.49 [0.88, 2.18] | -1.75 [-2.23, -1.29]* | -2.14 [-2.72, -1.62]* | 0.16 [-0.44, 0.79] | persistence |
| h2-4 | -2.99 [-3.70, -2.33]* | -0.25 [-0.82, 0.35] | -0.94 [-1.54, -0.32]* | -0.59 [-1.21, 0.05] | -0.74 [-1.38, -0.12]* | 0.54 [-0.02, 1.19] | -2.46 [-3.08, -1.86]* | -2.89 [-3.58, -2.23]* | -0.87 [-1.47, -0.26]* | persistence |
| h4-8 | -4.59 [-5.56, -3.65]* | -2.43 [-3.23, -1.63]* | -3.16 [-4.04, -2.33]* | -2.76 [-3.60, -1.94]* | -2.55 [-3.44, -1.73]* | -1.47 [-2.25, -0.72]* | -3.95 [-4.84, -3.12]* | -4.47 [-5.45, -3.53]* | -3.07 [-3.95, -2.26]* | persistence |
| h8+ | -6.50 [-8.65, -4.97]* | -5.37 [-7.38, -3.99]* | -6.00 [-8.10, -4.50]* | -5.55 [-7.51, -4.12]* | -5.07 [-7.18, -3.44]* | -4.14 [-5.78, -3.07]* | -5.80 [-7.70, -4.48]* | -6.19 [-8.22, -4.75]* | -5.90 [-7.96, -4.44]* | persistence |

segment `busy`:

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 0.49 [0.29, 0.71] | 10.44 [8.76, 12.13] | 9.35 [7.81, 10.79] | 9.67 [7.94, 11.28] | 8.82 [7.35, 10.12] | 11.94 [9.98, 13.78] | 0.26 [0.10, 0.39] | 0.01 [-0.15, 0.19] | 9.32 [7.83, 10.76] | persistence |
| m030 | -0.88 [-1.24, -0.45]* | 7.06 [5.57, 8.61] | 5.99 [4.67, 7.38] | 6.32 [4.88, 7.84] | 5.46 [4.27, 6.74] | 8.75 [6.98, 10.55] | -0.67 [-0.94, -0.35]* | -1.04 [-1.38, -0.58]* | 6.09 [4.73, 7.48] | persistence |
| m045 | -1.88 [-2.32, -1.34]* | 5.36 [4.01, 6.82] | 4.24 [3.07, 5.64] | 4.59 [3.24, 6.09] | 3.84 [2.73, 5.13] | 6.89 [5.29, 8.57] | -1.37 [-1.69, -0.97]* | -1.90 [-2.33, -1.35]* | 4.32 [3.16, 5.72] | persistence |
| m060 | -2.63 [-3.16, -2.03]* | 4.29 [2.99, 5.71] | 3.16 [1.97, 4.57] | 3.48 [2.18, 5.01] | 2.67 [1.55, 3.95] | 5.80 [4.24, 7.43] | -1.87 [-2.27, -1.42]* | -2.51 [-3.02, -1.90]* | 3.22 [2.08, 4.53] | persistence |
| m075 | -3.08 [-3.66, -2.45]* | 3.87 [2.57, 5.35] | 2.54 [1.40, 3.84] | 2.88 [1.65, 4.27] | 2.00 [0.94, 3.24] | 5.40 [3.94, 6.93] | -2.28 [-2.77, -1.77]* | -2.97 [-3.58, -2.31]* | 2.63 [1.50, 3.94] | persistence |
| m090 | -3.75 [-4.46, -2.97]* | 3.00 [1.81, 4.39] | 1.56 [0.46, 2.92] | 1.96 [0.82, 3.36] | 1.11 [0.01, 2.50] | 4.46 [3.13, 5.90] | -2.83 [-3.42, -2.25]* | -3.54 [-4.26, -2.78]* | 1.76 [0.66, 3.13] | persistence |
| m105 | -4.08 [-4.96, -3.22]* | 2.59 [1.21, 4.10] | 1.12 [-0.17, 2.58] | 1.55 [0.22, 3.05] | 0.54 [-0.70, 2.02] | 4.10 [2.55, 5.81] | -3.16 [-3.90, -2.44]* | -3.89 [-4.77, -3.01]* | 1.33 [0.01, 2.81] | persistence |
| m120 | -4.48 [-5.46, -3.54]* | 2.00 [0.67, 3.41] | 0.49 [-0.79, 1.92] | 0.87 [-0.45, 2.33] | -0.12 [-1.36, 1.36] | 3.43 [2.03, 5.06] | -3.42 [-4.20, -2.67]* | -4.20 [-5.12, -3.29]* | 0.72 [-0.55, 2.14] | persistence |
| h2-4 | -5.81 [-6.90, -4.70]* | 0.05 [-1.01, 1.32] | -1.36 [-2.47, -0.07]* | -0.95 [-2.07, 0.35] | -1.64 [-2.83, -0.25]* | 1.63 [0.40, 3.07] | -4.72 [-5.72, -3.80]* | -5.56 [-6.70, -4.45]* | -1.19 [-2.33, 0.10] | persistence |
| h4-8 | -8.42 [-9.96, -6.86]* | -3.78 [-4.96, -2.45]* | -5.31 [-6.63, -3.87]* | -4.81 [-6.11, -3.37]* | -5.02 [-6.43, -3.53]* | -1.97 [-3.32, -0.56]* | -7.11 [-8.51, -5.89]* | -8.14 [-9.71, -6.64]* | -5.07 [-6.42, -3.61]* | persistence |
| h8+ | -12.07 [-16.38, -8.60]* | -9.55 [-13.75, -6.28]* | -10.95 [-15.16, -7.40]* | -10.46 [-14.49, -7.12]* | -10.22 [-14.12, -6.62]* | -7.40 [-11.10, -4.80]* | -10.60 [-14.63, -7.63]* | -11.35 [-15.56, -8.06]* | -10.66 [-14.93, -7.15]* | persistence |

### Low-coverage cells (paired rows < 50 % of the reference's)

These absolute values are not comparable with the rest of their column. `reaches_handover` = the cell still passes the 30 % gate and CAN appear as a winner.

| metric | uc | segment | lead | model | ref | value | diff_vs_ref | diff_lo_park | diff_hi_park | paired_share | n_paired | n_units_park | n_origin_days | wins | reaches_handover |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MAE | UC2 | busy | 1 | lvlh5_cbd | wt_med | 18.372 | 4.750 | 2.879 | 6.819 | 0.492 | 167635.000 | 29.000 | 46 | False | True |
| MAE | UC2-intraday | all | h8+ | persistence | wt_med | 12.535 | 5.904 | 4.439 | 7.964 | 0.267 | 201628.000 | 27.000 | 47 | False | False |
| MAE | UC2-intraday | busy | h8+ | persistence | wt_med | 22.332 | 10.663 | 7.153 | 14.929 | 0.309 | 77495.000 | 27.000 | 47 | False | True |
| MAE | UC3 | all | 21 | lvlh5_cbd | wt_med | 10.091 | 2.719 | 2.003 | 3.536 | 0.484 | 475449.000 | 29.000 | 42 | False | True |
| MAE | UC3 | all | 30 | lvlh5_cbd | wt_med | 10.652 | 2.830 | 1.959 | 3.788 | 0.466 | 445861.000 | 30.000 | 41 | False | True |
| MAE | UC3 | all | 45 | lvlh5_cbd | wt_med | 10.348 | 1.700 | 0.971 | 2.557 | 0.123 | 107184.000 | 9.000 | 38 | False | False |
| MAE | UC3 | all | 30 | lvlh5_tft | wt_med | 8.172 | 0.442 | 0.187 | 0.669 | 0.491 | 469297.000 | 30.000 | 41 | False | True |
| MAE | UC3 | all | 45 | lvlh5_tft | wt_med | 8.456 | 0.393 | 0.064 | 0.723 | 0.366 | 320203.000 | 30.000 | 38 | False | True |
| MAE | UC3 | all | 60 | lvlh5_tft | wt_med | 0.299 | 0.099 | -0.202 | 0.258 | 0.007 | 5106.000 | 4.000 | 35 | False | False |
| MAE | UC3 | all | 45 | prod_served | wt_med | 9.497 | 1.394 | 0.814 | 1.967 | 0.384 | 335762.000 | 30.000 | 38 | False | True |
| MAE | UC3 | all | 60 | prod_served | wt_med | 0.234 | 0.023 | 0.000 | 1.228 | 0.006 | 4790.000 | 4.000 | 35 | False | False |
| MAE | UC3 | all | 45 | prod_served_lin | wt_med | 9.469 | 1.366 | 0.781 | 1.939 | 0.384 | 335762.000 | 30.000 | 38 | False | True |
| MAE | UC3 | all | 60 | prod_served_lin | wt_med | 0.239 | 0.028 | 0.000 | 1.324 | 0.006 | 4790.000 | 4.000 | 35 | False | False |
| MAE | UC3 | busy | 1 | lvlh5_cbd | wt_med | 18.372 | 4.750 | 2.879 | 6.819 | 0.492 | 167635.000 | 29.000 | 46 | False | True |
| MAE | UC3 | busy | 2 | lvlh5_cbd | wt_med | 19.866 | 5.512 | 3.097 | 8.489 | 0.494 | 166842.000 | 29.000 | 46 | False | True |
| MAE | UC3 | busy | 3 | lvlh5_cbd | wt_med | 19.083 | 4.740 | 2.905 | 6.659 | 0.495 | 167773.000 | 29.000 | 46 | False | True |
| MAE | UC3 | busy | 4 | lvlh5_cbd | wt_med | 19.407 | 5.349 | 3.136 | 7.415 | 0.495 | 168875.000 | 29.000 | 46 | False | True |
| MAE | UC3 | busy | 5 | lvlh5_cbd | wt_med | 19.556 | 5.311 | 2.934 | 8.236 | 0.497 | 169291.000 | 29.000 | 46 | False | True |
| MAE | UC3 | busy | 6 | lvlh5_cbd | wt_med | 18.801 | 4.781 | 3.000 | 6.814 | 0.487 | 160918.000 | 29.000 | 45 | False | True |
| MAE | UC3 | busy | 7 | lvlh5_cbd | wt_med | 20.207 | 5.412 | 3.193 | 8.176 | 0.486 | 159037.000 | 29.000 | 45 | False | True |
| MAE | UC3 | busy | 10 | lvlh5_cbd | wt_med | 20.291 | 5.514 | 3.110 | 8.187 | 0.484 | 160330.000 | 29.000 | 45 | False | True |
| MAE | UC3 | busy | 14 | lvlh5_cbd | wt_med | 20.178 | 5.375 | 3.003 | 7.587 | 0.469 | 150767.000 | 29.000 | 44 | False | True |
| MAE | UC3 | busy | 21 | lvlh5_cbd | wt_med | 20.066 | 4.941 | 2.790 | 7.004 | 0.446 | 134895.000 | 29.000 | 42 | False | True |
| MAE | UC3 | busy | 30 | lvlh5_cbd | wt_med | 21.037 | 5.183 | 2.747 | 7.678 | 0.433 | 128498.000 | 30.000 | 41 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_cbd | wt_med | 16.692 | 2.507 | 1.015 | 4.029 | 0.162 | 44227.000 | 9.000 | 38 | False | False |
| MAE | UC3 | busy | 30 | lvlh5_tft | wt_med | 16.180 | 0.346 | -0.263 | 0.880 | 0.450 | 133725.000 | 30.000 | 41 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_tft | wt_med | 16.998 | 0.228 | -0.614 | 1.111 | 0.334 | 91171.000 | 30.000 | 38 | False | True |
| MAE | UC3 | busy | 30 | prod_served | wt_med | 18.003 | 2.058 | 1.084 | 3.025 | 0.465 | 138044.000 | 30.000 | 41 | False | True |
| MAE | UC3 | busy | 45 | prod_served | wt_med | 19.217 | 2.638 | 1.172 | 3.923 | 0.361 | 98610.000 | 30.000 | 38 | False | True |
| MAE | UC3 | busy | 30 | prod_served_lin | wt_med | 17.928 | 1.982 | 1.000 | 2.965 | 0.465 | 138044.000 | 30.000 | 41 | False | True |
| MAE | UC3 | busy | 45 | prod_served_lin | wt_med | 19.134 | 2.554 | 1.069 | 3.850 | 0.361 | 98610.000 | 30.000 | 38 | False | True |
| MAE, schedule known at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 10.877 | 2.993 | 2.022 | 4.120 | 0.483 | 282287.000 | 24.000 | 41 | False | True |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 14.090 | 2.152 | 0.673 | 3.524 | 0.053 | 26795.000 | 5.000 | 38 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 7.985 | 0.129 | -0.161 | 0.444 | 0.365 | 183544.000 | 23.000 | 38 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 0.299 | 0.092 | -0.542 | 0.326 | 0.012 | 4752.000 | 3.000 | 35 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served | wt_med | 8.979 | 0.995 | 0.401 | 1.722 | 0.378 | 189801.000 | 23.000 | 38 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served | wt_med | 0.244 | 0.025 | 0.000 | 2.105 | 0.012 | 4436.000 | 3.000 | 35 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served_lin | wt_med | 8.949 | 0.964 | 0.356 | 1.695 | 0.378 | 189801.000 | 23.000 | 38 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served_lin | wt_med | 0.249 | 0.030 | 0.000 | 2.270 | 0.012 | 4436.000 | 3.000 | 35 | False | False |
| MAE, schedule projected at origin | UC3 | all | 0 | lvlh5_cbd | wt_med | 7.718 | 1.811 | 1.344 | 2.435 | 0.483 | 128519.000 | 12.000 | 47 | False | True |
| MAE, schedule projected at origin | UC3 | all | 1 | lvlh5_cbd | wt_med | 8.075 | 1.829 | 1.214 | 2.932 | 0.489 | 129638.000 | 12.000 | 46 | False | True |
| MAE, schedule projected at origin | UC3 | all | 2 | lvlh5_cbd | wt_med | 7.850 | 1.905 | 1.340 | 3.068 | 0.484 | 123891.000 | 10.000 | 46 | False | True |
| MAE, schedule projected at origin | UC3 | all | 3 | lvlh5_cbd | wt_med | 7.801 | 1.529 | 0.942 | 2.282 | 0.487 | 129757.000 | 16.000 | 46 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | lvlh5_cbd | wt_med | 9.044 | 2.586 | 1.944 | 3.601 | 0.479 | 128864.000 | 12.000 | 46 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | lvlh5_cbd | wt_med | 8.311 | 2.087 | 1.639 | 2.945 | 0.497 | 134301.000 | 13.000 | 46 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | lvlh5_cbd | wt_med | 8.408 | 2.002 | 1.341 | 3.097 | 0.483 | 130701.000 | 14.000 | 45 | False | True |
| MAE, schedule projected at origin | UC3 | all | 7 | lvlh5_cbd | wt_med | 8.711 | 2.343 | 1.516 | 4.301 | 0.488 | 126418.000 | 13.000 | 45 | False | True |
| MAE, schedule projected at origin | UC3 | all | 14 | lvlh5_cbd | wt_med | 10.388 | 3.012 | 1.657 | 5.334 | 0.476 | 140521.000 | 14.000 | 44 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | lvlh5_cbd | wt_med | 10.445 | 2.687 | 1.303 | 4.718 | 0.449 | 141074.000 | 16.000 | 42 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 10.254 | 2.549 | 1.256 | 4.384 | 0.439 | 163574.000 | 16.000 | 41 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 9.082 | 1.550 | 0.670 | 2.368 | 0.216 | 80389.000 | 9.000 | 38 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_tft | wt_med | 8.516 | 0.783 | 0.345 | 1.233 | 0.455 | 169704.000 | 18.000 | 41 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 9.101 | 0.748 | 0.077 | 1.343 | 0.367 | 136659.000 | 20.000 | 38 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 0.304 | 0.191 | 0.001 | 0.211 | 0.001 | 354.000 | 2.000 | 35 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served | wt_med | 9.526 | 1.780 | 0.754 | 2.862 | 0.461 | 171840.000 | 18.000 | 41 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served | wt_med | 10.191 | 1.913 | 0.946 | 2.799 | 0.392 | 145961.000 | 20.000 | 38 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served | wt_med | 0.113 | 0.000 | 0.000 | 0.000 | 0.001 | 354.000 | 2.000 | 35 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served_lin | wt_med | 9.501 | 1.755 | 0.731 | 2.810 | 0.461 | 171840.000 | 18.000 | 41 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served_lin | wt_med | 10.166 | 1.888 | 0.901 | 2.775 | 0.392 | 145961.000 | 20.000 | 38 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served_lin | wt_med | 0.113 | 0.000 | 0.000 | 0.000 | 0.001 | 354.000 | 2.000 | 35 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 21 | lvlh5_cbd | wt_med | 0.414 | -0.022 | -0.035 | -0.009 | 0.489 | 6609.000 | 29.000 | 21 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | lvlh5_cbd | wt_med | 0.397 | -0.017 | -0.029 | -0.004 | 0.460 | 5797.000 | 30.000 | 20 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_cbd | wt_med | 0.346 | -0.018 | -0.037 | 0.007 | 0.145 | 1558.000 | 9.000 | 17 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | lvlh5_tft | wt_med | 0.406 | 0.008 | 0.001 | 0.017 | 0.478 | 6016.000 | 30.000 | 21 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_tft | wt_med | 0.401 | 0.007 | -0.000 | 0.016 | 0.396 | 4258.000 | 30.000 | 15 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | lvlh5_tft | wt_med | 0.249 | -0.118 | -0.134 | 0.006 | 0.001 | 9.000 | 2.000 | 10 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | prod_served | wt_med | 0.424 | 0.002 | -0.009 | 0.014 | 0.477 | 6002.000 | 30.000 | 21 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served | wt_med | 0.418 | 0.002 | -0.009 | 0.011 | 0.398 | 4279.000 | 30.000 | 17 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served | wt_med | 0.386 | -0.089 | -0.128 | 0.145 | 0.001 | 7.000 | 2.000 | 10 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | prod_served_lin | wt_med | 0.434 | 0.012 | -0.000 | 0.027 | 0.482 | 6072.000 | 30.000 | 21 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served_lin | wt_med | 0.428 | 0.013 | 0.002 | 0.023 | 0.402 | 4323.000 | 30.000 | 17 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served_lin | wt_med | 0.388 | -0.087 | -0.116 | 0.085 | 0.001 | 7.000 | 2.000 | 10 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 0 | snaive7 | wt_med | 0.431 | -0.079 | -0.096 | -0.058 | 0.445 | 7855.000 | 30.000 | 47 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 1 | snaive7 | wt_med | 0.429 | -0.075 | -0.089 | -0.058 | 0.470 | 8033.000 | 30.000 | 46 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 2 | snaive7 | wt_med | 0.430 | -0.069 | -0.081 | -0.055 | 0.459 | 7886.000 | 30.000 | 46 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 3 | snaive7 | wt_med | 0.415 | -0.073 | -0.085 | -0.060 | 0.457 | 7620.000 | 30.000 | 46 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 4 | snaive7 | wt_med | 0.431 | -0.064 | -0.079 | -0.044 | 0.450 | 7497.000 | 30.000 | 46 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 5 | snaive7 | wt_med | 0.430 | -0.072 | -0.088 | -0.049 | 0.456 | 7605.000 | 30.000 | 46 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 6 | snaive7 | wt_med | 0.427 | -0.068 | -0.083 | -0.053 | 0.483 | 7805.000 | 30.000 | 45 | False | True |
| best-time hit rate (top-2) | D3 | all | 21 | lvlh5_cbd | wt_med | 0.736 | -0.080 | -0.098 | -0.062 | 0.496 | 7331.000 | 29.000 | 21 | False | True |
| best-time hit rate (top-2) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.726 | -0.070 | -0.091 | -0.047 | 0.472 | 6525.000 | 30.000 | 20 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.714 | -0.060 | -0.092 | -0.020 | 0.138 | 1630.000 | 9.000 | 17 | False | False |
| best-time hit rate (top-2) | D3 | all | 30 | lvlh5_tft | wt_med | 0.773 | -0.020 | -0.035 | -0.005 | 0.487 | 6728.000 | 30.000 | 21 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_tft | wt_med | 0.779 | -0.017 | -0.032 | -0.003 | 0.404 | 4760.000 | 30.000 | 15 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | lvlh5_tft | wt_med | 0.909 | -0.100 | -0.125 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served | wt_med | 0.727 | -0.071 | -0.099 | -0.047 | 0.419 | 4940.000 | 30.000 | 17 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served_lin | wt_med | 0.749 | -0.044 | -0.069 | -0.021 | 0.419 | 4940.000 | 30.000 | 17 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time hit rate (top-2) | D3 | all | 0 | snaive7 | wt_med | 0.814 | -0.036 | -0.046 | -0.028 | 0.468 | 9021.000 | 30.000 | 47 | False | True |
| best-time hit rate (top-2) | D3 | all | 1 | snaive7 | wt_med | 0.808 | -0.036 | -0.046 | -0.027 | 0.498 | 9297.000 | 30.000 | 46 | False | True |
| best-time hit rate (top-2) | D3 | all | 2 | snaive7 | wt_med | 0.814 | -0.026 | -0.036 | -0.015 | 0.487 | 9176.000 | 30.000 | 46 | False | True |
| best-time hit rate (top-2) | D3 | all | 3 | snaive7 | wt_med | 0.811 | -0.027 | -0.040 | -0.015 | 0.484 | 8811.000 | 30.000 | 46 | False | True |
| best-time hit rate (top-2) | D3 | all | 4 | snaive7 | wt_med | 0.813 | -0.035 | -0.047 | -0.026 | 0.483 | 8835.000 | 30.000 | 46 | False | True |
| best-time hit rate (top-2) | D3 | all | 5 | snaive7 | wt_med | 0.813 | -0.034 | -0.043 | -0.025 | 0.480 | 8759.000 | 30.000 | 46 | False | True |
| best-time hit rate (±30 min) | D3 | all | 21 | lvlh5_cbd | wt_med | 0.768 | -0.049 | -0.066 | -0.031 | 0.496 | 7331.000 | 29.000 | 21 | False | True |
| best-time hit rate (±30 min) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.760 | -0.044 | -0.063 | -0.024 | 0.472 | 6525.000 | 30.000 | 20 | False | True |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.758 | -0.019 | -0.050 | 0.018 | 0.138 | 1630.000 | 9.000 | 17 | False | False |
| best-time hit rate (±30 min) | D3 | all | 30 | lvlh5_tft | wt_med | 0.795 | -0.008 | -0.021 | 0.007 | 0.487 | 6728.000 | 30.000 | 21 | False | True |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_tft | wt_med | 0.796 | -0.002 | -0.018 | 0.016 | 0.404 | 4760.000 | 30.000 | 15 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | lvlh5_tft | wt_med | 0.818 | -0.100 | -0.125 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | prod_served | wt_med | 0.754 | -0.041 | -0.068 | -0.021 | 0.419 | 4940.000 | 30.000 | 17 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | prod_served_lin | wt_med | 0.773 | -0.018 | -0.045 | 0.007 | 0.419 | 4940.000 | 30.000 | 17 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time hit rate (±30 min) | D3 | all | 0 | snaive7 | wt_med | 0.825 | -0.027 | -0.036 | -0.017 | 0.468 | 9021.000 | 30.000 | 47 | False | True |
| best-time hit rate (±30 min) | D3 | all | 1 | snaive7 | wt_med | 0.822 | -0.022 | -0.033 | -0.013 | 0.498 | 9297.000 | 30.000 | 46 | False | True |
| best-time hit rate (±30 min) | D3 | all | 2 | snaive7 | wt_med | 0.824 | -0.019 | -0.031 | -0.008 | 0.487 | 9176.000 | 30.000 | 46 | False | True |
| best-time hit rate (±30 min) | D3 | all | 3 | snaive7 | wt_med | 0.818 | -0.024 | -0.035 | -0.014 | 0.484 | 8811.000 | 30.000 | 46 | False | True |
| best-time hit rate (±30 min) | D3 | all | 4 | snaive7 | wt_med | 0.822 | -0.024 | -0.035 | -0.015 | 0.483 | 8835.000 | 30.000 | 46 | False | True |
| best-time hit rate (±30 min) | D3 | all | 5 | snaive7 | wt_med | 0.826 | -0.023 | -0.031 | -0.014 | 0.480 | 8759.000 | 30.000 | 46 | False | True |
| best-time regret (min) | D3 | all | 21 | lvlh5_cbd | wt_med | 4.422 | 1.115 | 0.755 | 1.521 | 0.496 | 7331.000 | 29.000 | 21 | False | True |
| best-time regret (min) | D3 | all | 30 | lvlh5_cbd | wt_med | 4.696 | 0.889 | 0.474 | 1.304 | 0.472 | 6525.000 | 30.000 | 20 | False | True |
| best-time regret (min) | D3 | all | 45 | lvlh5_cbd | wt_med | 4.675 | 0.751 | 0.162 | 1.357 | 0.138 | 1630.000 | 9.000 | 17 | False | False |
| best-time regret (min) | D3 | all | 30 | lvlh5_tft | wt_med | 3.986 | 0.065 | -0.301 | 0.385 | 0.487 | 6728.000 | 30.000 | 21 | False | True |
| best-time regret (min) | D3 | all | 45 | lvlh5_tft | wt_med | 3.842 | 0.135 | -0.226 | 0.437 | 0.404 | 4760.000 | 30.000 | 15 | False | True |
| best-time regret (min) | D3 | all | 60 | lvlh5_tft | wt_med | 0.727 | 0.500 | 0.000 | 0.625 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time regret (min) | D3 | all | 45 | prod_served | wt_med | 4.906 | 1.044 | 0.463 | 1.792 | 0.419 | 4940.000 | 30.000 | 17 | False | True |
| best-time regret (min) | D3 | all | 60 | prod_served | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time regret (min) | D3 | all | 45 | prod_served_lin | wt_med | 4.240 | 0.318 | -0.262 | 1.021 | 0.419 | 4940.000 | 30.000 | 17 | False | True |
| best-time regret (min) | D3 | all | 60 | prod_served_lin | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 10.000 | 3.000 | 10 | False | False |
| best-time regret (min) | D3 | all | 0 | snaive7 | wt_med | 3.755 | 0.911 | 0.573 | 1.344 | 0.468 | 9021.000 | 30.000 | 47 | False | True |
| best-time regret (min) | D3 | all | 1 | snaive7 | wt_med | 3.701 | 0.887 | 0.532 | 1.337 | 0.498 | 9297.000 | 30.000 | 46 | False | True |
| best-time regret (min) | D3 | all | 2 | snaive7 | wt_med | 3.647 | 0.813 | 0.493 | 1.149 | 0.487 | 9176.000 | 30.000 | 46 | False | True |
| best-time regret (min) | D3 | all | 3 | snaive7 | wt_med | 3.780 | 0.805 | 0.553 | 1.078 | 0.484 | 8811.000 | 30.000 | 46 | False | True |
| best-time regret (min) | D3 | all | 4 | snaive7 | wt_med | 3.786 | 0.905 | 0.597 | 1.239 | 0.483 | 8835.000 | 30.000 | 46 | False | True |
| best-time regret (min) | D3 | all | 5 | snaive7 | wt_med | 3.715 | 0.782 | 0.488 | 1.133 | 0.480 | 8759.000 | 30.000 | 46 | False | True |
| crowd bucket exact | D6 | all | 30 | lvl_cbd | lvl_naive4 | 0.162 | -0.178 | -0.247 | -0.114 | 0.495 | 461.000 | 30.000 | 41 | False | True |
| crowd bucket exact | D6 | all | 45 | lvl_cbd | lvl_naive4 | 0.294 | -0.119 | -0.283 | 0.059 | 0.149 | 126.000 | 9.000 | 38 | False | False |
| crowd bucket exact | D6 | all | 45 | lvl_tft | lvl_naive4 | 0.336 | 0.040 | -0.009 | 0.086 | 0.419 | 354.000 | 29.000 | 38 | False | True |
| crowd bucket exact | D6 | all | 60 | lvl_tft | lvl_naive4 | 1.000 | 1.000 | 1.000 | 1.000 | 0.001 | 1.000 | 1.000 | 35 | False | False |
| day comparison winner accuracy | D7 | all | d31-90 | lvl_cbd | lvl_naive4 | 0.105 | -0.150 | -0.182 | -0.120 | 0.082 | 134465.000 | 30.000 | 40 | False | False |
| day comparison winner accuracy | D7 | all | d31-90 | lvl_tft | lvl_naive4 | 0.290 | 0.025 | -0.009 | 0.060 | 0.179 | 294502.000 | 30.000 | 40 | False | False |
| dayPeak Spearman across rides | UC3 | all | 21 | lvlh5_cbd | wt_med | 0.653 | -0.164 | -0.226 | -0.115 | 0.480 | 433.000 | 28.000 | 21 | False | True |
| dayPeak Spearman across rides | UC3 | all | 30 | lvlh5_cbd | wt_med | 0.624 | -0.177 | -0.226 | -0.133 | 0.465 | 400.000 | 30.000 | 20 | False | True |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_cbd | wt_med | 0.600 | -0.217 | -0.277 | -0.149 | 0.157 | 116.000 | 9.000 | 17 | False | False |
| dayPeak Spearman across rides | UC3 | all | 30 | lvlh5_tft | wt_med | 0.787 | -0.035 | -0.055 | -0.018 | 0.490 | 421.000 | 30.000 | 20 | False | True |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_tft | wt_med | 0.769 | -0.038 | -0.055 | -0.023 | 0.414 | 306.000 | 30.000 | 14 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | lvlh5_tft | wt_med | 0.432 | -0.485 | -0.485 | -0.485 | 0.006 | 4.000 | 1.000 | 5 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | prod_served | wt_med | 0.805 | -0.012 | -0.021 | -0.002 | 0.432 | 320.000 | 30.000 | 17 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | prod_served | wt_med | 0.899 | 0.009 | 0.009 | 0.009 | 0.006 | 4.000 | 1.000 | 5 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | prod_served_lin | wt_med | 0.807 | -0.012 | -0.021 | -0.003 | 0.432 | 320.000 | 30.000 | 17 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | prod_served_lin | wt_med | 0.899 | 0.009 | 0.009 | 0.009 | 0.006 | 4.000 | 1.000 | 5 | False | False |
| dayPeak abs error (min) | UC3 | all | 21 | lvlh5_cbd | wt_med | 13.491 | 4.860 | 3.520 | 6.378 | 0.475 | 10098.000 | 29.000 | 21 | False | True |
| dayPeak abs error (min) | UC3 | all | 30 | lvlh5_cbd | wt_med | 13.766 | 4.809 | 3.298 | 6.446 | 0.453 | 8920.000 | 30.000 | 20 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_cbd | wt_med | 14.035 | 5.011 | 2.997 | 6.897 | 0.118 | 1988.000 | 9.000 | 17 | False | False |
| dayPeak abs error (min) | UC3 | all | 30 | lvlh5_tft | wt_med | 9.188 | 0.321 | -0.161 | 0.748 | 0.484 | 9513.000 | 30.000 | 21 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_tft | wt_med | 9.122 | 0.361 | -0.072 | 0.773 | 0.404 | 6802.000 | 30.000 | 15 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | lvlh5_tft | wt_med | 0.376 | -0.032 | -2.627 | 0.193 | 0.010 | 150.000 | 4.000 | 10 | False | False |
| dayPeak abs error (min) | UC3 | all | 30 | prod_served | wt_med | 9.257 | 0.471 | -0.076 | 1.011 | 0.498 | 9796.000 | 30.000 | 21 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | prod_served | wt_med | 9.613 | 0.896 | 0.308 | 1.452 | 0.415 | 6996.000 | 30.000 | 17 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served | wt_med | 0.327 | -0.093 | -0.105 | 0.000 | 0.010 | 140.000 | 4.000 | 10 | False | False |
| dayPeak abs error (min) | UC3 | all | 30 | prod_served_lin | wt_med | 9.220 | 0.427 | -0.117 | 0.974 | 0.498 | 9796.000 | 30.000 | 21 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | prod_served_lin | wt_med | 9.556 | 0.841 | 0.246 | 1.408 | 0.415 | 6996.000 | 30.000 | 17 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served_lin | wt_med | 0.334 | -0.085 | -0.096 | 0.000 | 0.010 | 140.000 | 4.000 | 10 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 21 | lvlh5_cbd | wt_med | 0.714 | -0.035 | -0.076 | 0.004 | 0.487 | 12065.000 | 28.000 | 21 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 30 | lvlh5_cbd | wt_med | 0.708 | -0.027 | -0.062 | 0.011 | 0.463 | 10607.000 | 29.000 | 20 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_cbd | wt_med | 0.727 | -0.052 | -0.094 | -0.014 | 0.170 | 3328.000 | 9.000 | 17 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 30 | lvlh5_tft | wt_med | 0.765 | 0.035 | 0.023 | 0.050 | 0.478 | 10954.000 | 30.000 | 18 | True | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_tft | wt_med | 0.763 | 0.034 | 0.020 | 0.048 | 0.397 | 7761.000 | 30.000 | 13 | True | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 60 | lvlh5_tft | wt_med | 0.727 | -0.091 | -0.091 | -0.091 | 0.001 | 11.000 | 1.000 | 1 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 30 | prod_served | wt_med | 0.722 | -0.005 | -0.019 | 0.011 | 0.496 | 11366.000 | 30.000 | 20 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | prod_served | wt_med | 0.717 | -0.009 | -0.030 | 0.010 | 0.416 | 8138.000 | 30.000 | 17 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 60 | prod_served | wt_med | 0.636 | -0.182 | -0.182 | -0.182 | 0.001 | 11.000 | 1.000 | 1 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 30 | prod_served_lin | wt_med | 0.756 | 0.029 | 0.014 | 0.047 | 0.496 | 11366.000 | 30.000 | 20 | True | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | prod_served_lin | wt_med | 0.750 | 0.023 | 0.004 | 0.043 | 0.416 | 8138.000 | 30.000 | 17 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 60 | prod_served_lin | wt_med | 0.636 | -0.182 | -0.182 | -0.182 | 0.001 | 11.000 | 1.000 | 1 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 0 | snaive7 | wt_med | 0.770 | -0.012 | -0.030 | 0.004 | 0.268 | 8707.000 | 30.000 | 47 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 1 | snaive7 | wt_med | 0.768 | -0.014 | -0.028 | 0.000 | 0.285 | 8961.000 | 30.000 | 46 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 2 | snaive7 | wt_med | 0.756 | -0.014 | -0.032 | 0.001 | 0.279 | 8782.000 | 30.000 | 46 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 3 | snaive7 | wt_med | 0.748 | -0.015 | -0.033 | 0.002 | 0.277 | 8551.000 | 30.000 | 46 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 4 | snaive7 | wt_med | 0.766 | -0.022 | -0.037 | -0.009 | 0.267 | 8243.000 | 30.000 | 46 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 5 | snaive7 | wt_med | 0.769 | -0.005 | -0.019 | 0.008 | 0.275 | 8441.000 | 30.000 | 46 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 6 | snaive7 | wt_med | 0.768 | -0.008 | -0.023 | 0.008 | 0.294 | 8730.000 | 30.000 | 45 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 7 | lvlh5_cbd | wt_med | 4.423 | -0.228 | -0.558 | -0.014 | 0.497 | 36208.000 | 29.000 | 45 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 10 | lvlh5_cbd | wt_med | 4.537 | -0.230 | -0.546 | -0.021 | 0.489 | 36147.000 | 29.000 | 45 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 14 | lvlh5_cbd | wt_med | 4.701 | -0.234 | -0.567 | -0.013 | 0.483 | 34837.000 | 28.000 | 44 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 21 | lvlh5_cbd | wt_med | 4.625 | -0.143 | -0.320 | -0.023 | 0.461 | 30993.000 | 28.000 | 42 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 30 | lvlh5_cbd | wt_med | 4.941 | -0.190 | -0.432 | -0.022 | 0.437 | 28552.000 | 30.000 | 41 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_cbd | wt_med | 5.556 | -0.089 | -0.337 | 0.035 | 0.101 | 5972.000 | 9.000 | 38 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | lvlh5_tft | wt_med | 4.887 | -0.190 | -0.431 | -0.034 | 0.451 | 29451.000 | 30.000 | 41 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_tft | wt_med | 5.124 | -0.179 | -0.407 | -0.022 | 0.350 | 20765.000 | 30.000 | 38 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | lvlh5_tft | wt_med | 0.081 | 0.000 | 0.000 | 0.000 | 0.012 | 632.000 | 4.000 | 35 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | prod_served | wt_med | 6.867 | 1.720 | 1.222 | 2.285 | 0.469 | 30642.000 | 30.000 | 41 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | prod_served | wt_med | 7.352 | 1.994 | 1.454 | 2.634 | 0.367 | 21778.000 | 30.000 | 38 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served | wt_med | 0.076 | -0.004 | -0.005 | 0.000 | 0.011 | 591.000 | 4.000 | 35 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | prod_served_lin | wt_med | 6.927 | 1.782 | 1.335 | 2.293 | 0.469 | 30642.000 | 30.000 | 41 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | prod_served_lin | wt_med | 7.433 | 2.076 | 1.507 | 2.808 | 0.367 | 21778.000 | 30.000 | 38 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served_lin | wt_med | 0.104 | 0.023 | 0.000 | 2.448 | 0.011 | 591.000 | 4.000 | 35 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 7 | lvlh5_cbd | wt_med | 4.756 | -0.280 | -0.744 | -0.005 | 0.496 | 28214.000 | 25.000 | 45 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 10 | lvlh5_cbd | wt_med | 4.789 | -0.304 | -0.769 | -0.023 | 0.489 | 27828.000 | 25.000 | 45 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 14 | lvlh5_cbd | wt_med | 4.863 | -0.325 | -0.825 | -0.027 | 0.485 | 25912.000 | 24.000 | 44 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 21 | lvlh5_cbd | wt_med | 4.576 | -0.203 | -0.476 | -0.031 | 0.465 | 22320.000 | 23.000 | 42 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 30 | lvlh5_cbd | wt_med | 4.910 | -0.288 | -0.715 | -0.029 | 0.448 | 18712.000 | 24.000 | 41 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 45 | lvlh5_cbd | wt_med | 9.827 | -0.200 | -0.919 | 0.085 | 0.037 | 1349.000 | 5.000 | 38 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 30 | lvlh5_tft | wt_med | 4.868 | -0.289 | -0.704 | -0.046 | 0.464 | 19389.000 | 24.000 | 41 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 45 | lvlh5_tft | wt_med | 4.847 | -0.254 | -0.621 | -0.033 | 0.347 | 12593.000 | 23.000 | 38 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 60 | lvlh5_tft | wt_med | 0.086 | 0.000 | 0.000 | 0.000 | 0.021 | 592.000 | 3.000 | 35 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 30 | prod_served | wt_med | 6.933 | 1.735 | 1.040 | 2.509 | 0.487 | 20357.000 | 23.000 | 41 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 45 | prod_served | wt_med | 7.259 | 1.992 | 1.352 | 2.773 | 0.358 | 12995.000 | 23.000 | 38 | False | True |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.13 [6.90, 7.36] | 6.76 [6.48, 7.03] | 5.16 [5.01, 5.30] | 5.63 [5.40, 5.86] | 6.88 [6.64, 7.11] | 6.93 [6.69, 7.17] |
| 2 | 7.41 [7.17, 7.65] | 7.08 [6.79, 7.41] | 5.20 [5.06, 5.36] | 5.91 [5.67, 6.14] | 7.20 [6.96, 7.46] | 7.26 [7.02, 7.53] |
| 3 | 7.48 [7.21, 7.76] | 7.24 [6.94, 7.57] | 5.24 [5.10, 5.41] | 5.96 [5.70, 6.24] | 7.22 [6.95, 7.49] | 7.30 [7.03, 7.57] |
| 4 | 7.53 [7.28, 7.80] | 7.32 [7.02, 7.64] | 5.30 [5.12, 5.48] | 6.04 [5.79, 6.30] | 7.26 [7.00, 7.53] | 7.32 [7.06, 7.59] |
| 5 | 7.44 [7.17, 7.69] | 7.20 [6.89, 7.52] | 5.24 [5.08, 5.40] | 5.91 [5.63, 6.17] | 7.27 [6.99, 7.52] | 7.34 [7.06, 7.59] |
| 6 | 7.17 [6.93, 7.40] | 7.07 [6.78, 7.36] | 5.20 [5.04, 5.35] | 5.61 [5.38, 5.84] | 7.02 [6.78, 7.27] | 7.09 [6.84, 7.34] |
| 7 | 7.73 [7.48, 8.00] | 7.49 [7.16, 7.84] | 5.28 [5.13, 5.45] | 6.31 [6.07, 6.57] | 7.46 [7.20, 7.74] | 7.53 [7.28, 7.81] |
| 10 | 7.78 [7.49, 8.05] | 7.61 [7.25, 7.97] | 5.30 [5.14, 5.45] | 6.34 [6.06, 6.61] | 7.45 [7.17, 7.71] | 7.54 [7.26, 7.80] |
| 14 | 8.10 [7.83, 8.37] | 7.95 [7.61, 8.28] | 5.42 [5.24, 5.61] | 6.71 [6.45, 6.99] | 7.66 [7.38, 7.93] | 7.74 [7.47, 8.01] |
| 21 | 7.99 [7.70, 8.28] | 7.85 [7.49, 8.21] | 5.38 [5.22, 5.54] | 6.60 [6.31, 6.91] | 7.57 [7.31, 7.85] | 7.67 [7.40, 7.95] |
| 30 | 8.34 [8.05, 8.64] | 8.17 [7.77, 8.58] | 5.43 [5.26, 5.60] | 7.05 [6.74, 7.36] | 7.80 [7.50, 8.08] | 7.92 [7.62, 8.20] |
| 45 | 8.38 [8.05, 8.69] | 8.46 [7.90, 9.08] | 5.46 [5.28, 5.64] | 7.04 [6.72, 7.36] | 7.93 [7.63, 8.24] | 8.06 [7.75, 8.37] |
| 60 | 8.55 [8.22, 8.88] | 0.30 [0.12, 0.50] | 5.48 [5.29, 5.66] | 7.24 [6.90, 7.57] | 8.07 [7.76, 8.39] | 8.20 [7.89, 8.52] |
| 90 | 9.00 [8.60, 9.39] |  | 5.45 [5.25, 5.66] | 7.67 [7.24, 8.08] | 8.42 [8.06, 8.80] | 8.56 [8.18, 8.94] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 3.11 [3.01, 3.21] | 7.68 [7.40, 7.95] | 7.12 [6.84, 7.39] | 7.43 [7.16, 7.69] | 6.88 [6.59, 7.22] | 2.72 [2.63, 2.82] | 8.39 [8.02, 8.76] | 3.04 [2.93, 3.14] | 2.93 [2.82, 3.04] | 7.10 [6.83, 7.37] |
| m030 | 4.00 [3.88, 4.13] | 7.59 [7.31, 7.87] | 7.02 [6.75, 7.28] | 7.36 [7.08, 7.62] | 6.84 [6.55, 7.16] | 4.36 [4.21, 4.51] | 8.34 [8.00, 8.72] | 4.15 [4.02, 4.30] | 4.01 [3.88, 4.14] | 7.06 [6.79, 7.33] |
| m045 | 4.39 [4.25, 4.53] | 7.61 [7.34, 7.89] | 7.03 [6.76, 7.29] | 7.36 [7.09, 7.62] | 6.86 [6.58, 7.18] | 5.22 [5.05, 5.40] | 8.31 [7.98, 8.69] | 4.69 [4.54, 4.85] | 4.48 [4.34, 4.63] | 7.08 [6.81, 7.35] |
| m060 | 4.63 [4.48, 4.78] | 7.67 [7.39, 7.94] | 7.11 [6.83, 7.37] | 7.42 [7.13, 7.68] | 6.90 [6.61, 7.21] | 5.78 [5.59, 5.97] | 8.38 [8.04, 8.76] | 5.04 [4.87, 5.21] | 4.78 [4.63, 4.94] | 7.14 [6.87, 7.40] |
| m075 | 4.90 [4.74, 5.05] | 7.70 [7.42, 7.98] | 7.11 [6.83, 7.37] | 7.43 [7.16, 7.69] | 6.91 [6.62, 7.21] | 6.27 [6.06, 6.49] | 8.44 [8.08, 8.81] | 5.38 [5.20, 5.56] | 5.11 [4.95, 5.27] | 7.15 [6.88, 7.41] |
| m090 | 5.03 [4.88, 5.19] | 7.72 [7.44, 8.00] | 7.08 [6.81, 7.34] | 7.43 [7.15, 7.69] | 6.92 [6.64, 7.23] | 6.71 [6.48, 6.94] | 8.42 [8.06, 8.78] | 5.55 [5.37, 5.74] | 5.30 [5.12, 5.47] | 7.16 [6.90, 7.42] |
| m105 | 5.14 [4.98, 5.31] | 7.76 [7.48, 8.05] | 7.14 [6.87, 7.40] | 7.50 [7.22, 7.76] | 6.96 [6.67, 7.28] | 6.96 [6.73, 7.20] | 8.49 [8.14, 8.86] | 5.67 [5.49, 5.86] | 5.42 [5.24, 5.59] | 7.21 [6.93, 7.47] |
| m120 | 5.24 [5.08, 5.40] | 7.80 [7.53, 8.09] | 7.18 [6.91, 7.45] | 7.50 [7.23, 7.76] | 6.96 [6.68, 7.28] | 7.20 [6.97, 7.44] | 8.47 [8.12, 8.83] | 5.82 [5.62, 6.01] | 5.54 [5.36, 5.71] | 7.24 [6.97, 7.50] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 0.27 [0.19, 0.37] | 5.25 [4.35, 6.17] | 4.71 [3.93, 5.54] | 5.03 [4.17, 5.91] | 4.60 [3.85, 5.44] | 5.97 [4.98, 7.01] | 0.15 [0.10, 0.21] | 0.00 [-0.06, 0.07] | 4.68 [3.93, 5.51] | persistence |
| m030 | -0.41 [-0.60, -0.20]* | 3.56 [2.87, 4.27] | 3.00 [2.40, 3.64] | 3.35 [2.69, 4.03] | 3.00 [2.42, 3.67] | 4.33 [3.52, 5.17] | -0.29 [-0.43, -0.15]* | -0.50 [-0.68, -0.30]* | 3.04 [2.43, 3.70] | persistence |
| m045 | -0.90 [-1.17, -0.61]* | 2.67 [2.09, 3.35] | 2.09 [1.56, 2.70] | 2.44 [1.86, 3.10] | 2.18 [1.66, 2.80] | 3.39 [2.68, 4.12] | -0.65 [-0.86, -0.43]* | -0.93 [-1.21, -0.64]* | 2.12 [1.60, 2.74] | persistence |
| m060 | -1.27 [-1.62, -0.91]* | 2.10 [1.55, 2.71] | 1.53 [1.01, 2.12] | 1.86 [1.28, 2.51] | 1.59 [1.08, 2.19] | 2.82 [2.15, 3.52] | -0.92 [-1.19, -0.64]* | -1.24 [-1.57, -0.89]* | 1.54 [1.03, 2.12] | persistence |
| m075 | -1.54 [-1.95, -1.13]* | 1.79 [1.20, 2.41] | 1.15 [0.63, 1.73] | 1.47 [0.90, 2.10] | 1.18 [0.68, 1.76] | 2.52 [1.86, 3.20] | -1.16 [-1.48, -0.83]* | -1.50 [-1.90, -1.10]* | 1.18 [0.66, 1.78] | persistence |
| m090 | -1.88 [-2.36, -1.39]* | 1.33 [0.76, 1.92] | 0.63 [0.13, 1.21] | 1.00 [0.48, 1.62] | 0.75 [0.24, 1.38] | 2.03 [1.45, 2.66] | -1.44 [-1.83, -1.05]* | -1.80 [-2.24, -1.33]* | 0.71 [0.19, 1.29] | persistence |
| m105 | -2.06 [-2.63, -1.54]* | 1.08 [0.47, 1.76] | 0.37 [-0.22, 1.02] | 0.75 [0.14, 1.42] | 0.46 [-0.10, 1.09] | 1.81 [1.16, 2.52] | -1.61 [-2.07, -1.17]* | -1.99 [-2.53, -1.49]* | 0.46 [-0.15, 1.11] | persistence |
| m120 | -2.25 [-2.83, -1.71]* | 0.79 [0.22, 1.44] | 0.08 [-0.49, 0.68] | 0.41 [-0.16, 1.06] | 0.13 [-0.44, 0.75] | 1.49 [0.88, 2.18] | -1.75 [-2.23, -1.29]* | -2.14 [-2.72, -1.62]* | 0.16 [-0.44, 0.79] | persistence |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 5.966 [4.982, 7.008] / [5.635, 6.313] | 4.682 [3.926, 5.506] / [4.437, 4.943] | 5.254 [4.353, 6.175] / [5.003, 5.520] | 4.708 [3.934, 5.535] / [4.463, 4.970] | 5.029 [4.168, 5.906] / [4.785, 5.285] | 4.603 [3.846, 5.437] / [4.347, 4.893] |
| m030 | 4.333 [3.521, 5.171] / [4.034, 4.680] | 3.037 [2.428, 3.700] / [2.816, 3.290] | 3.557 [2.868, 4.274] / [3.323, 3.811] | 3.000 [2.399, 3.643] / [2.778, 3.253] | 3.346 [2.687, 4.034] / [3.114, 3.597] | 3.003 [2.424, 3.668] / [2.756, 3.273] |
| m045 | 3.389 [2.679, 4.120] / [3.107, 3.719] | 2.124 [1.597, 2.738] / [1.909, 2.376] | 2.667 [2.089, 3.346] / [2.440, 2.922] | 2.089 [1.559, 2.703] / [1.867, 2.335] | 2.444 [1.858, 3.101] / [2.214, 2.683] | 2.176 [1.662, 2.803] / [1.939, 2.444] |
| m060 | 2.820 [2.146, 3.517] / [2.525, 3.147] | 1.536 [1.028, 2.124] / [1.319, 1.779] | 2.095 [1.550, 2.712] / [1.884, 2.326] | 1.525 [1.015, 2.118] / [1.309, 1.762] | 1.855 [1.278, 2.505] / [1.623, 2.092] | 1.588 [1.079, 2.187] / [1.363, 1.851] |
| m075 | 2.524 [1.856, 3.203] / [2.231, 2.849] | 1.181 [0.658, 1.775] / [0.962, 1.436] | 1.787 [1.200, 2.406] / [1.556, 2.041] | 1.146 [0.633, 1.731] / [0.924, 1.395] | 1.470 [0.900, 2.098] / [1.235, 1.706] | 1.184 [0.683, 1.763] / [0.956, 1.461] |
| m090 | 2.028 [1.446, 2.659] / [1.740, 2.359] | 0.713 [0.185, 1.292] / [0.481, 0.962] | 1.326 [0.761, 1.925] / [1.092, 1.572] | 0.634 [0.129, 1.211] / [0.406, 0.876] | 1.002 [0.478, 1.618] / [0.771, 1.238] | 0.753 [0.240, 1.376] / [0.517, 1.008] |
| m105 | 1.810 [1.162, 2.516] / [1.506, 2.154] | 0.458 [-0.151, 1.112] / [0.226, 0.707] | 1.082 [0.472, 1.756] / [0.840, 1.337] | 0.371 [-0.219, 1.023] / [0.144, 0.618] | 0.750 [0.139, 1.422] / [0.509, 0.993] | 0.457 [-0.097, 1.087] / [0.219, 0.723] |
| m120 | 1.492 [0.884, 2.177] / [1.198, 1.835] | 0.155 [-0.441, 0.787] / [-0.066, 0.404] | 0.791 [0.216, 1.443] / [0.559, 1.032] | 0.078 [-0.491, 0.684] / [-0.141, 0.320] | 0.414 [-0.164, 1.056] / [0.176, 0.654] | 0.132 [-0.436, 0.750] / [-0.098, 0.391] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.966 [-7.008, -4.982]* / [-6.313, -5.635]* | -1.271 [-1.651, -0.874]* / [-1.526, -1.041]* | -0.626 [-0.896, -0.310]* / [-0.904, -0.387]* | -1.237 [-1.593, -0.844]* / [-1.496, -1.000]* | -0.897 [-1.181, -0.572]* / [-1.116, -0.689]* | -0.999 [-1.275, -0.720]* / [-1.276, -0.736]* |
| m030 | -4.333 [-5.171, -3.521]* / [-4.680, -4.034]* | -1.288 [-1.672, -0.909]* / [-1.535, -1.058]* | -0.696 [-0.987, -0.377]* / [-0.966, -0.443]* | -1.318 [-1.702, -0.932]* / [-1.565, -1.085]* | -0.943 [-1.251, -0.606]* / [-1.165, -0.735]* | -1.041 [-1.323, -0.756]* / [-1.315, -0.778]* |
| m045 | -3.389 [-4.120, -2.679]* / [-3.719, -3.107]* | -1.243 [-1.628, -0.867]* / [-1.501, -1.010]* | -0.645 [-0.935, -0.320]* / [-0.919, -0.393]* | -1.283 [-1.658, -0.904]* / [-1.532, -1.049]* | -0.918 [-1.215, -0.568]* / [-1.142, -0.712]* | -1.003 [-1.301, -0.700]* / [-1.280, -0.743]* |
| m060 | -2.820 [-3.517, -2.146]* / [-3.147, -2.525]* | -1.272 [-1.672, -0.857]* / [-1.533, -1.036]* | -0.659 [-0.953, -0.322]* / [-0.938, -0.401]* | -1.287 [-1.662, -0.881]* / [-1.537, -1.052]* | -0.947 [-1.264, -0.589]* / [-1.179, -0.732]* | -1.039 [-1.330, -0.728]* / [-1.325, -0.774]* |
| m075 | -2.524 [-3.203, -1.856]* / [-2.849, -2.231]* | -1.283 [-1.676, -0.882]* / [-1.549, -1.038]* | -0.636 [-0.893, -0.311]* / [-0.920, -0.372]* | -1.319 [-1.714, -0.904]* / [-1.577, -1.076]* | -0.966 [-1.306, -0.581]* / [-1.197, -0.752]* | -1.036 [-1.347, -0.714]* / [-1.313, -0.755]* |
| m090 | -2.028 [-2.659, -1.446]* / [-2.359, -1.740]* | -1.263 [-1.643, -0.874]* / [-1.522, -1.027]* | -0.608 [-0.854, -0.303]* / [-0.891, -0.351]* | -1.337 [-1.732, -0.937]* / [-1.596, -1.105]* | -0.955 [-1.289, -0.585]* / [-1.180, -0.754]* | -0.989 [-1.300, -0.674]* / [-1.267, -0.724]* |
| m105 | -1.810 [-2.516, -1.162]* / [-2.154, -1.506]* | -1.295 [-1.689, -0.905]* / [-1.563, -1.060]* | -0.654 [-0.913, -0.349]* / [-0.937, -0.409]* | -1.351 [-1.752, -0.955]* / [-1.611, -1.113]* | -0.965 [-1.281, -0.613]* / [-1.187, -0.758]* | -1.006 [-1.281, -0.724]* / [-1.293, -0.743]* |
| m120 | -1.492 [-2.177, -0.884]* / [-1.835, -1.198]* | -1.267 [-1.615, -0.911]* / [-1.520, -1.021]* | -0.609 [-0.851, -0.319]* / [-0.886, -0.366]* | -1.307 [-1.656, -0.939]* / [-1.559, -1.071]* | -0.976 [-1.267, -0.649]* / [-1.187, -0.767]* | -1.065 [-1.350, -0.771]* / [-1.344, -0.790]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -4.682 [-5.506, -3.926]* / [-4.943, -4.437]* | 1.271 [0.874, 1.651] / [1.041, 1.526] | 0.608 [0.450, 0.773] / [0.517, 0.697] | 0.016 [-0.038, 0.070] / [-0.015, 0.045] | 0.362 [0.219, 0.529] / [0.261, 0.460] | 0.204 [0.002, 0.399] / [0.052, 0.371] |
| m030 | -3.037 [-3.700, -2.428]* / [-3.290, -2.816]* | 1.288 [0.909, 1.672] / [1.058, 1.535] | 0.552 [0.424, 0.691] / [0.462, 0.633] | -0.042 [-0.066, -0.022]* / [-0.065, -0.021]* | 0.331 [0.216, 0.462] / [0.233, 0.425] | 0.191 [0.014, 0.354] / [0.037, 0.355] |
| m045 | -2.124 [-2.738, -1.597]* / [-2.376, -1.909]* | 1.243 [0.867, 1.628] / [1.010, 1.501] | 0.560 [0.422, 0.713] / [0.462, 0.645] | -0.048 [-0.094, -0.011]* / [-0.069, -0.030]* | 0.319 [0.187, 0.468] / [0.223, 0.414] | 0.161 [-0.015, 0.318] / [0.011, 0.318] |
| m060 | -1.536 [-2.124, -1.028]* / [-1.779, -1.319]* | 1.272 [0.857, 1.672] / [1.036, 1.533] | 0.562 [0.426, 0.711] / [0.468, 0.647] | -0.023 [-0.073, 0.021] / [-0.048, -0.001]*  ! | 0.316 [0.181, 0.476] / [0.218, 0.413] | 0.147 [-0.024, 0.294] / [-0.005, 0.307] |
| m075 | -1.181 [-1.775, -0.658]* / [-1.436, -0.962]* | 1.283 [0.882, 1.676] / [1.038, 1.549] | 0.595 [0.449, 0.749] / [0.502, 0.681] | -0.037 [-0.086, 0.002] / [-0.070, -0.009]*  ! | 0.319 [0.193, 0.444] / [0.223, 0.419] | 0.157 [-0.034, 0.304] / [0.009, 0.318] |
| m090 | -0.713 [-1.292, -0.185]* / [-0.962, -0.481]* | 1.263 [0.874, 1.643] / [1.027, 1.522] | 0.599 [0.451, 0.756] / [0.507, 0.685] | -0.076 [-0.129, -0.037]* / [-0.104, -0.049]* | 0.305 [0.185, 0.421] / [0.211, 0.405] | 0.166 [-0.009, 0.298] / [0.024, 0.329] |
| m105 | -0.458 [-1.112, 0.151] / [-0.707, -0.226]*  ! | 1.295 [0.905, 1.689] / [1.060, 1.563] | 0.598 [0.451, 0.749] / [0.503, 0.690] | -0.063 [-0.095, -0.037]* / [-0.089, -0.040]* | 0.329 [0.210, 0.458] / [0.233, 0.426] | 0.166 [-0.002, 0.303] / [0.020, 0.331] |
| m120 | -0.155 [-0.787, 0.441] / [-0.404, 0.066] | 1.267 [0.911, 1.615] / [1.021, 1.520] | 0.615 [0.471, 0.772] / [0.523, 0.707] | -0.048 [-0.089, -0.011]* / [-0.074, -0.021]* | 0.293 [0.185, 0.401] / [0.192, 0.389] | 0.143 [-0.030, 0.290] / [-0.013, 0.314] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.254 [-6.175, -4.353]* / [-5.520, -5.003]* | 0.626 [0.310, 0.896] / [0.387, 0.904] | -0.608 [-0.773, -0.450]* / [-0.697, -0.517]* | -0.589 [-0.739, -0.452]* / [-0.677, -0.497]* | -0.231 [-0.372, -0.102]* / [-0.361, -0.102]* | -0.346 [-0.574, -0.180]* / [-0.523, -0.159]* |
| m030 | -3.557 [-4.274, -2.868]* / [-3.811, -3.323]* | 0.696 [0.377, 0.987] / [0.443, 0.966] | -0.552 [-0.691, -0.424]* / [-0.633, -0.462]* | -0.596 [-0.740, -0.462]* / [-0.680, -0.507]* | -0.215 [-0.342, -0.100]* / [-0.339, -0.084]* | -0.296 [-0.508, -0.141]* / [-0.474, -0.116]* |
| m045 | -2.667 [-3.346, -2.089]* / [-2.922, -2.440]* | 0.645 [0.320, 0.935] / [0.393, 0.919] | -0.560 [-0.713, -0.422]* / [-0.645, -0.462]* | -0.612 [-0.772, -0.469]* / [-0.701, -0.519]* | -0.235 [-0.391, -0.100]* / [-0.361, -0.112]* | -0.324 [-0.543, -0.149]* / [-0.495, -0.146]* |
| m060 | -2.095 [-2.712, -1.550]* / [-2.326, -1.884]* | 0.659 [0.322, 0.953] / [0.401, 0.938] | -0.562 [-0.711, -0.426]* / [-0.647, -0.468]* | -0.591 [-0.745, -0.457]* / [-0.675, -0.501]* | -0.239 [-0.399, -0.097]* / [-0.365, -0.116]* | -0.334 [-0.559, -0.157]* / [-0.502, -0.151]* |
| m075 | -1.787 [-2.406, -1.200]* / [-2.041, -1.556]* | 0.636 [0.311, 0.893] / [0.372, 0.920] | -0.595 [-0.749, -0.449]* / [-0.681, -0.502]* | -0.627 [-0.791, -0.468]* / [-0.717, -0.532]* | -0.256 [-0.404, -0.106]* / [-0.382, -0.123]* | -0.347 [-0.565, -0.169]* / [-0.521, -0.164]* |
| m090 | -1.326 [-1.925, -0.761]* / [-1.572, -1.092]* | 0.608 [0.303, 0.854] / [0.351, 0.891] | -0.599 [-0.756, -0.451]* / [-0.685, -0.507]* | -0.672 [-0.862, -0.504]* / [-0.760, -0.578]* | -0.279 [-0.442, -0.122]* / [-0.400, -0.150]* | -0.359 [-0.596, -0.168]* / [-0.527, -0.173]* |
| m105 | -1.082 [-1.756, -0.472]* / [-1.337, -0.840]* | 0.654 [0.349, 0.913] / [0.409, 0.937] | -0.598 [-0.749, -0.451]* / [-0.690, -0.503]* | -0.663 [-0.829, -0.510]* / [-0.754, -0.564]* | -0.258 [-0.389, -0.118]* / [-0.381, -0.131]* | -0.358 [-0.562, -0.194]* / [-0.528, -0.174]* |
| m120 | -0.791 [-1.443, -0.216]* / [-1.032, -0.559]* | 0.609 [0.319, 0.851] / [0.366, 0.886] | -0.615 [-0.772, -0.471]* / [-0.707, -0.523]* | -0.659 [-0.831, -0.507]* / [-0.750, -0.564]* | -0.308 [-0.447, -0.159]* / [-0.437, -0.177]* | -0.385 [-0.606, -0.218]* / [-0.556, -0.203]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 6.18 [6.05, 6.32] | 15.44 [14.91, 15.98] | 14.32 [13.82, 14.86] | 14.64 [14.07, 15.19] | 13.62 [13.03, 14.28] | 5.34 [5.19, 5.49] | 16.92 [16.17, 17.69] | 6.06 [5.92, 6.21] | 5.86 [5.72, 6.01] | 14.31 [13.80, 14.86] |
| m030 | 7.95 [7.78, 8.12] | 15.32 [14.77, 15.86] | 14.23 [13.72, 14.76] | 14.57 [14.01, 15.12] | 13.57 [12.99, 14.22] | 8.59 [8.38, 8.81] | 16.97 [16.24, 17.75] | 8.29 [8.10, 8.47] | 7.99 [7.81, 8.16] | 14.34 [13.84, 14.87] |
| m045 | 8.64 [8.44, 8.84] | 15.31 [14.79, 15.85] | 14.17 [13.69, 14.72] | 14.51 [13.98, 15.04] | 13.58 [13.01, 14.23] | 10.26 [10.01, 10.51] | 16.78 [16.07, 17.54] | 9.30 [9.08, 9.53] | 8.85 [8.64, 9.07] | 14.30 [13.81, 14.86] |
| m060 | 9.05 [8.85, 9.26] | 15.39 [14.87, 15.92] | 14.29 [13.79, 14.83] | 14.58 [14.06, 15.13] | 13.56 [12.99, 14.21] | 11.36 [11.09, 11.62] | 16.90 [16.18, 17.68] | 9.95 [9.70, 10.19] | 9.40 [9.18, 9.63] | 14.39 [13.90, 14.93] |
| m075 | 9.56 [9.32, 9.79] | 15.48 [14.92, 16.05] | 14.26 [13.78, 14.81] | 14.54 [13.95, 15.08] | 13.51 [12.92, 14.16] | 12.11 [11.80, 12.41] | 17.03 [16.26, 17.80] | 10.59 [10.31, 10.86] | 9.97 [9.72, 10.20] | 14.40 [13.91, 14.95] |
| m090 | 9.86 [9.62, 10.10] | 15.59 [15.03, 16.17] | 14.26 [13.76, 14.79] | 14.57 [14.00, 15.10] | 13.58 [12.98, 14.21] | 12.91 [12.58, 13.26] | 17.01 [16.25, 17.81] | 10.95 [10.64, 11.26] | 10.42 [10.15, 10.69] | 14.48 [13.96, 15.02] |
| m105 | 10.04 [9.79, 10.30] | 15.63 [15.07, 16.19] | 14.39 [13.86, 14.92] | 14.72 [14.16, 15.28] | 13.60 [12.99, 14.26] | 13.38 [13.01, 13.74] | 17.12 [16.38, 17.87] | 11.18 [10.86, 11.49] | 10.64 [10.36, 10.91] | 14.54 [14.04, 15.10] |
| m120 | 10.18 [9.92, 10.42] | 15.67 [15.12, 16.23] | 14.40 [13.91, 14.96] | 14.68 [14.14, 15.24] | 13.58 [12.99, 14.22] | 13.90 [13.53, 14.26] | 17.02 [16.30, 17.75] | 11.44 [11.10, 11.77] | 10.84 [10.54, 11.13] | 14.56 [14.07, 15.12] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 0.49 [0.29, 0.71] | 10.44 [8.76, 12.13] | 9.35 [7.81, 10.79] | 9.67 [7.94, 11.28] | 8.82 [7.35, 10.12] | 11.94 [9.98, 13.78] | 0.26 [0.10, 0.39] | 0.01 [-0.15, 0.19] | 9.32 [7.83, 10.76] | persistence |
| m030 | -0.88 [-1.24, -0.45]* | 7.06 [5.57, 8.61] | 5.99 [4.67, 7.38] | 6.32 [4.88, 7.84] | 5.46 [4.27, 6.74] | 8.75 [6.98, 10.55] | -0.67 [-0.94, -0.35]* | -1.04 [-1.38, -0.58]* | 6.09 [4.73, 7.48] | persistence |
| m045 | -1.88 [-2.32, -1.34]* | 5.36 [4.01, 6.82] | 4.24 [3.07, 5.64] | 4.59 [3.24, 6.09] | 3.84 [2.73, 5.13] | 6.89 [5.29, 8.57] | -1.37 [-1.69, -0.97]* | -1.90 [-2.33, -1.35]* | 4.32 [3.16, 5.72] | persistence |
| m060 | -2.63 [-3.16, -2.03]* | 4.29 [2.99, 5.71] | 3.16 [1.97, 4.57] | 3.48 [2.18, 5.01] | 2.67 [1.55, 3.95] | 5.80 [4.24, 7.43] | -1.87 [-2.27, -1.42]* | -2.51 [-3.02, -1.90]* | 3.22 [2.08, 4.53] | persistence |
| m075 | -3.08 [-3.66, -2.45]* | 3.87 [2.57, 5.35] | 2.54 [1.40, 3.84] | 2.88 [1.65, 4.27] | 2.00 [0.94, 3.24] | 5.40 [3.94, 6.93] | -2.28 [-2.77, -1.77]* | -2.97 [-3.58, -2.31]* | 2.63 [1.50, 3.94] | persistence |
| m090 | -3.75 [-4.46, -2.97]* | 3.00 [1.81, 4.39] | 1.56 [0.46, 2.92] | 1.96 [0.82, 3.36] | 1.11 [0.01, 2.50] | 4.46 [3.13, 5.90] | -2.83 [-3.42, -2.25]* | -3.54 [-4.26, -2.78]* | 1.76 [0.66, 3.13] | persistence |
| m105 | -4.08 [-4.96, -3.22]* | 2.59 [1.21, 4.10] | 1.12 [-0.17, 2.58] | 1.55 [0.22, 3.05] | 0.54 [-0.70, 2.02] | 4.10 [2.55, 5.81] | -3.16 [-3.90, -2.44]* | -3.89 [-4.77, -3.01]* | 1.33 [0.01, 2.81] | persistence |
| m120 | -4.48 [-5.46, -3.54]* | 2.00 [0.67, 3.41] | 0.49 [-0.79, 1.92] | 0.87 [-0.45, 2.33] | -0.12 [-1.36, 1.36] | 3.43 [2.03, 5.06] | -3.42 [-4.20, -2.67]* | -4.20 [-5.12, -3.29]* | 0.72 [-0.55, 2.14] | persistence |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 11.935 [9.976, 13.780] / [11.143, 12.743] | 9.324 [7.831, 10.764] / [8.825, 9.922] | 10.438 [8.756, 12.128] / [9.888, 11.014] | 9.350 [7.813, 10.794] / [8.827, 9.945] | 9.665 [7.941, 11.284] / [9.098, 10.262] | 8.818 [7.351, 10.123] / [8.212, 9.438] |
| m030 | 8.750 [6.978, 10.547] / [8.008, 9.545] | 6.086 [4.728, 7.480] / [5.575, 6.673] | 7.061 [5.572, 8.607] / [6.514, 7.640] | 5.988 [4.669, 7.375] / [5.478, 6.591] | 6.319 [4.879, 7.842] / [5.751, 6.920] | 5.461 [4.269, 6.741] / [4.881, 6.080] |
| m045 | 6.888 [5.291, 8.567] / [6.181, 7.691] | 4.316 [3.162, 5.723] / [3.800, 4.907] | 5.357 [4.014, 6.823] / [4.813, 5.927] | 4.238 [3.065, 5.642] / [3.726, 4.827] | 4.594 [3.239, 6.089] / [4.040, 5.191] | 3.844 [2.731, 5.130] / [3.261, 4.476] |
| m060 | 5.795 [4.244, 7.428] / [5.031, 6.581] | 3.224 [2.079, 4.534] / [2.701, 3.775] | 4.290 [2.992, 5.710] / [3.771, 4.847] | 3.161 [1.973, 4.570] / [2.643, 3.719] | 3.481 [2.180, 5.013] / [2.938, 4.047] | 2.667 [1.554, 3.945] / [2.084, 3.294] |
| m075 | 5.400 [3.942, 6.929] / [4.632, 6.200] | 2.635 [1.503, 3.939] / [2.110, 3.261] | 3.872 [2.566, 5.354] / [3.316, 4.473] | 2.535 [1.397, 3.842] / [2.005, 3.107] | 2.878 [1.650, 4.271] / [2.319, 3.459] | 1.996 [0.938, 3.238] / [1.423, 2.608] |
| m090 | 4.462 [3.126, 5.902] / [3.710, 5.255] | 1.763 [0.656, 3.129] / [1.229, 2.375] | 3.003 [1.808, 4.388] / [2.442, 3.584] | 1.559 [0.462, 2.923] / [1.038, 2.171] | 1.958 [0.823, 3.363] / [1.406, 2.540] | 1.115 [0.014, 2.495] / [0.567, 1.723] |
| m105 | 4.099 [2.550, 5.810] / [3.309, 4.905] | 1.334 [0.012, 2.805] / [0.792, 1.953] | 2.587 [1.214, 4.100] / [2.018, 3.196] | 1.118 [-0.170, 2.584] / [0.594, 1.714] | 1.551 [0.217, 3.046] / [0.983, 2.156] | 0.541 [-0.699, 2.017] / [-0.042, 1.204] |
| m120 | 3.430 [2.026, 5.062] / [2.679, 4.192] | 0.717 [-0.554, 2.137] / [0.178, 1.286] | 1.996 [0.666, 3.407] / [1.453, 2.595] | 0.490 [-0.786, 1.921] / [-0.045, 1.058] | 0.869 [-0.446, 2.333] / [0.287, 1.469] | -0.120 [-1.363, 1.360] / [-0.702, 0.530] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -11.935 [-13.780, -9.976]* / [-12.743, -11.143]* | -2.601 [-3.304, -1.784]* / [-3.195, -2.014]* | -1.257 [-1.839, -0.544]* / [-1.901, -0.641]* | -2.562 [-3.210, -1.788]* / [-3.174, -1.974]* | -2.138 [-2.632, -1.484]* / [-2.660, -1.606]* | -2.635 [-3.216, -2.125]* / [-3.404, -1.929]* |
| m030 | -8.750 [-10.547, -6.978]* / [-9.545, -8.008]* | -2.662 [-3.336, -1.900]* / [-3.275, -2.079]* | -1.454 [-2.049, -0.703]* / [-2.090, -0.836]* | -2.758 [-3.433, -1.977]* / [-3.372, -2.180]* | -2.283 [-2.774, -1.640]* / [-2.826, -1.759]* | -2.781 [-3.329, -2.288]* / [-3.523, -2.079]* |
| m045 | -6.888 [-8.567, -5.291]* / [-7.691, -6.181]* | -2.520 [-3.222, -1.739]* / [-3.139, -1.939]* | -1.278 [-1.906, -0.519]* / [-1.934, -0.648]* | -2.632 [-3.294, -1.816]* / [-3.238, -2.049]* | -2.177 [-2.649, -1.518]* / [-2.722, -1.651]* | -2.609 [-3.157, -2.069]* / [-3.351, -1.927]* |
| m060 | -5.795 [-7.428, -4.244]* / [-6.581, -5.031]* | -2.539 [-3.235, -1.754]* / [-3.176, -1.962]* | -1.276 [-1.862, -0.516]* / [-1.940, -0.658]* | -2.633 [-3.277, -1.847]* / [-3.257, -2.070]* | -2.222 [-2.706, -1.535]* / [-2.779, -1.679]* | -2.660 [-3.211, -2.106]* / [-3.404, -1.989]* |
| m075 | -5.400 [-6.929, -3.942]* / [-6.200, -4.632]* | -2.634 [-3.318, -1.857]* / [-3.278, -2.043]* | -1.270 [-1.743, -0.560]* / [-1.947, -0.629]* | -2.778 [-3.444, -1.959]* / [-3.406, -2.184]* | -2.369 [-2.885, -1.711]* / [-2.940, -1.825]* | -2.753 [-3.279, -2.194]* / [-3.499, -2.046]* |
| m090 | -4.462 [-5.902, -3.126]* / [-5.255, -3.710]* | -2.552 [-3.225, -1.775]* / [-3.182, -1.961]* | -1.180 [-1.635, -0.491]* / [-1.850, -0.562]* | -2.770 [-3.448, -1.965]* / [-3.384, -2.184]* | -2.333 [-2.849, -1.673]* / [-2.869, -1.805]* | -2.633 [-3.145, -2.107]* / [-3.323, -1.957]* |
| m105 | -4.099 [-5.810, -2.550]* / [-4.905, -3.309]* | -2.602 [-3.339, -1.763]* / [-3.245, -2.026]* | -1.271 [-1.820, -0.532]* / [-1.917, -0.653]* | -2.760 [-3.471, -1.915]* / [-3.397, -2.181]* | -2.298 [-2.848, -1.634]* / [-2.851, -1.779]* | -2.685 [-3.183, -2.194]* / [-3.411, -2.029]* |
| m120 | -3.430 [-5.062, -2.026]* / [-4.192, -2.679]* | -2.513 [-3.138, -1.763]* / [-3.101, -1.934]* | -1.155 [-1.667, -0.472]* / [-1.788, -0.542]* | -2.668 [-3.277, -1.917]* / [-3.262, -2.092]* | -2.284 [-2.768, -1.632]* / [-2.807, -1.769]* | -2.795 [-3.379, -2.252]* / [-3.541, -2.116]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -9.324 [-10.764, -7.831]* / [-9.922, -8.825]* | 2.601 [1.784, 3.304] / [2.014, 3.195] | 1.239 [0.913, 1.622] / [1.023, 1.441] | 0.004 [-0.130, 0.149] / [-0.076, 0.080] | 0.425 [0.116, 0.783] / [0.188, 0.651] | -0.114 [-0.676, 0.386] / [-0.536, 0.303] |
| m030 | -6.086 [-7.480, -4.728]* / [-6.673, -5.575]* | 2.662 [1.900, 3.336] / [2.079, 3.275] | 1.092 [0.810, 1.389] / [0.880, 1.290] | -0.115 [-0.172, -0.063]* / [-0.173, -0.060]* | 0.333 [0.087, 0.598] / [0.111, 0.556] | -0.205 [-0.719, 0.230] / [-0.595, 0.210] |
| m045 | -4.316 [-5.723, -3.162]* / [-4.907, -3.800]* | 2.520 [1.739, 3.222] / [1.939, 3.139] | 1.118 [0.816, 1.469] / [0.892, 1.328] | -0.123 [-0.235, -0.035]* / [-0.178, -0.072]* | 0.315 [0.023, 0.637] / [0.098, 0.528] | -0.249 [-0.716, 0.139] / [-0.601, 0.136] |
| m060 | -3.224 [-4.534, -2.079]* / [-3.775, -2.701]* | 2.539 [1.754, 3.235] / [1.962, 3.176] | 1.099 [0.799, 1.447] / [0.875, 1.304] | -0.109 [-0.220, -0.011]* / [-0.179, -0.050]* | 0.283 [-0.026, 0.640] / [0.065, 0.486] | -0.284 [-0.741, 0.081] / [-0.639, 0.100] |
| m075 | -2.635 [-3.939, -1.503]* / [-3.261, -2.110]* | 2.634 [1.857, 3.318] / [2.043, 3.278] | 1.216 [0.860, 1.591] / [0.986, 1.431] | -0.143 [-0.258, -0.048]* / [-0.231, -0.073]* | 0.252 [-0.033, 0.533] / [0.018, 0.469] | -0.309 [-0.801, 0.086] / [-0.678, 0.080] |
| m090 | -1.763 [-3.129, -0.656]* / [-2.375, -1.229]* | 2.552 [1.775, 3.225] / [1.961, 3.182] | 1.220 [0.873, 1.602] / [0.995, 1.435] | -0.221 [-0.326, -0.129]* / [-0.290, -0.153]* | 0.202 [-0.073, 0.456] / [-0.020, 0.427] | -0.315 [-0.787, 0.057] / [-0.680, 0.068] |
| m105 | -1.334 [-2.805, -0.012]* / [-1.953, -0.792]* | 2.602 [1.763, 3.339] / [2.026, 3.245] | 1.210 [0.880, 1.584] / [0.987, 1.430] | -0.167 [-0.244, -0.093]* / [-0.233, -0.109]* | 0.286 [0.031, 0.528] / [0.067, 0.503] | -0.325 [-0.827, 0.055] / [-0.711, 0.068] |
| m120 | -0.717 [-2.137, 0.554] / [-1.286, -0.178]*  ! | 2.513 [1.763, 3.138] / [1.934, 3.101] | 1.240 [0.919, 1.602] / [1.008, 1.458] | -0.165 [-0.263, -0.084]* / [-0.234, -0.093]* | 0.218 [-0.028, 0.443] / [-0.003, 0.435] | -0.402 [-0.925, -0.013]* / [-0.810, 0.018]  ! |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -10.438 [-12.128, -8.756]* / [-11.014, -9.888]* | 1.257 [0.544, 1.839] / [0.641, 1.901] | -1.239 [-1.622, -0.913]* / [-1.441, -1.023]* | -1.233 [-1.542, -0.938]* / [-1.462, -1.011]* | -0.785 [-1.058, -0.535]* / [-1.075, -0.478]* | -1.262 [-1.783, -0.874]* / [-1.716, -0.758]* |
| m030 | -7.061 [-8.607, -5.572]* / [-7.640, -6.514]* | 1.454 [0.703, 2.049] / [0.836, 2.090] | -1.092 [-1.389, -0.810]* / [-1.290, -0.880]* | -1.212 [-1.504, -0.930]* / [-1.414, -0.992]* | -0.744 [-0.999, -0.510]* / [-1.024, -0.451]* | -1.174 [-1.626, -0.803]* / [-1.630, -0.714]* |
| m045 | -5.357 [-6.823, -4.014]* / [-5.927, -4.813]* | 1.278 [0.519, 1.906] / [0.648, 1.934] | -1.118 [-1.469, -0.816]* / [-1.328, -0.892]* | -1.248 [-1.589, -0.925]* / [-1.460, -1.011]* | -0.788 [-1.107, -0.457]* / [-1.058, -0.480]* | -1.212 [-1.661, -0.793]* / [-1.657, -0.739]* |
| m060 | -4.290 [-5.710, -2.992]* / [-4.847, -3.771]* | 1.276 [0.516, 1.862] / [0.658, 1.940] | -1.099 [-1.447, -0.799]* / [-1.304, -0.875]* | -1.218 [-1.521, -0.929]* / [-1.419, -0.997]* | -0.796 [-1.103, -0.482]* / [-1.079, -0.483]* | -1.238 [-1.696, -0.810]* / [-1.669, -0.764]* |
| m075 | -3.872 [-5.354, -2.566]* / [-4.473, -3.316]* | 1.270 [0.560, 1.743] / [0.629, 1.947] | -1.216 [-1.591, -0.860]* / [-1.431, -0.986]* | -1.344 [-1.722, -0.981]* / [-1.561, -1.126]* | -0.920 [-1.214, -0.606]* / [-1.202, -0.626]* | -1.353 [-1.759, -0.981]* / [-1.792, -0.869]* |
| m090 | -3.003 [-4.388, -1.808]* / [-3.584, -2.442]* | 1.180 [0.491, 1.635] / [0.562, 1.850] | -1.220 [-1.602, -0.873]* / [-1.435, -0.995]* | -1.431 [-1.850, -1.050]* / [-1.643, -1.203]* | -0.986 [-1.312, -0.639]* / [-1.259, -0.685]* | -1.394 [-1.857, -0.963]* / [-1.824, -0.914]* |
| m105 | -2.587 [-4.100, -1.214]* / [-3.196, -2.018]* | 1.271 [0.532, 1.820] / [0.653, 1.917] | -1.210 [-1.584, -0.880]* / [-1.430, -0.987]* | -1.378 [-1.739, -1.058]* / [-1.594, -1.159]* | -0.900 [-1.166, -0.645]* / [-1.182, -0.609]* | -1.406 [-1.813, -1.049]* / [-1.843, -0.914]* |
| m120 | -1.996 [-3.407, -0.666]* / [-2.595, -1.453]* | 1.155 [0.472, 1.667] / [0.542, 1.788] | -1.240 [-1.602, -0.919]* / [-1.458, -1.008]* | -1.393 [-1.749, -1.070]* / [-1.605, -1.166]* | -0.994 [-1.280, -0.704]* / [-1.290, -0.695]* | -1.457 [-1.908, -1.078]* / [-1.894, -0.964]* |

**MAE by region (all rides)**

| lead | region | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| m015 | Asia | 3.48 | 9.83 | 9.15 | 9.67 | 8.98 | 3.16 | 10.78 | 3.43 | 3.30 | 9.03 |
| m015 | EU | 2.15 | 5.33 | 4.95 | 5.11 | 4.89 | 1.89 | 5.56 | 2.10 | 2.00 | 4.95 |
| m015 | NA | 3.95 | 8.50 | 7.88 | 8.16 | 7.67 | 3.32 | 9.56 | 3.84 | 3.76 | 7.95 |
| m060 | Asia | 5.28 | 9.77 | 9.11 | 9.61 | 8.96 | 6.86 | 10.83 | 5.95 | 5.53 | 9.08 |
| m060 | EU | 3.27 | 5.36 | 4.98 | 5.15 | 4.95 | 3.95 | 5.60 | 3.54 | 3.38 | 4.99 |
| m060 | NA | 5.80 | 8.60 | 7.99 | 8.26 | 7.79 | 7.05 | 9.66 | 6.15 | 5.92 | 8.07 |
| m120 | Asia | 6.19 | 10.14 | 9.26 | 9.69 | 8.95 | 8.84 | 10.95 | 7.11 | 6.66 | 9.30 |
| m120 | EU | 3.77 | 5.50 | 5.07 | 5.28 | 5.07 | 4.85 | 5.77 | 4.21 | 4.02 | 5.11 |
| m120 | NA | 6.22 | 8.49 | 7.92 | 8.21 | 7.75 | 8.57 | 9.51 | 6.67 | 6.43 | 7.98 |

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | tirex2 | toto2 | chronos2 |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | -0.18 | -0.16 | -1.00 | -0.60 | -1.41 | -1.10 | -1.21 | -0.21 | 0.02 | 0.07 |
| m030 | -0.15 | -0.16 | -1.03 | -0.67 | -1.29 | -0.98 | -1.13 | -0.44 | -0.24 | -0.17 |
| m045 | 0.01 | -0.20 | -1.10 | -0.70 | -1.23 | -0.91 | -1.08 | -0.59 | -0.39 | -0.28 |
| m060 | 0.18 | -0.19 | -1.13 | -0.75 | -1.33 | -1.01 | -1.14 | -0.76 | -0.64 | -0.46 |
| m075 | 0.21 | -0.18 | -1.10 | -0.73 | -1.35 | -1.02 | -1.11 | -0.90 | -0.75 | -0.53 |
| m090 | 0.48 | -0.18 | -1.07 | -0.73 | -1.27 | -0.95 | -1.06 | -1.07 | -0.90 | -0.60 |
| m105 | 0.62 | -0.17 | -1.06 | -0.71 | -1.28 | -0.96 | -1.06 | -1.25 | -1.09 | -0.74 |
| m120 | 0.86 | -0.17 | -1.05 | -0.70 | -1.32 | -1.00 | -1.07 | -1.34 | -1.20 | -0.86 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 5.57 [5.38, 5.76] | 7.74 [7.47, 8.02] | 7.15 [6.87, 7.41] | 7.49 [7.21, 7.75] | 6.95 [6.67, 7.27] | 8.27 [8.01, 8.53] | 8.44 [8.09, 8.82] | 6.15 [5.92, 6.36] | 5.89 [5.69, 6.09] | 7.19 [6.92, 7.45] |
| h4-8 | 6.13 [5.90, 6.36] | 7.83 [7.56, 8.11] | 7.22 [6.94, 7.48] | 7.57 [7.29, 7.82] | 7.06 [6.77, 7.39] | 10.48 [10.13, 10.82] | 8.54 [8.20, 8.91] | 6.66 [6.39, 6.92] | 6.44 [6.18, 6.68] | 7.28 [7.01, 7.54] |
| h8+ | 6.30 [6.03, 6.56] | 7.34 [7.08, 7.60] | 6.73 [6.48, 6.99] | 7.12 [6.85, 7.36] | 6.86 [6.55, 7.19] | 12.54 [12.04, 13.03] | 8.09 [7.77, 8.43] | 6.74 [6.45, 7.02] | 6.61 [6.33, 6.91] | 6.81 [6.55, 7.07] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | -1.62 [-1.98, -1.24]* | 0.59 [0.44, 0.74] | -0.04 [-0.06, -0.02]* | 0.34 [0.22, 0.47] | 0.18 [-0.00, 0.33] | 0.87 [0.26, 1.47] | 1.27 [0.87, 1.66] | -1.05 [-1.34, -0.75]* | -1.32 [-1.67, -0.96]* | wt_med |
| h4-8 | -1.16 [-1.42, -0.88]* | 0.58 [0.44, 0.72] | -0.06 [-0.08, -0.04]* | 0.32 [0.20, 0.45] | 0.17 [-0.02, 0.33] | 3.07 [2.26, 3.95] | 1.29 [0.89, 1.68] | -0.64 [-0.85, -0.43]* | -0.86 [-1.13, -0.60]* | wt_med |
| h8+ | -0.53 [-0.69, -0.38]* | 0.48 [0.37, 0.61] | -0.07 [-0.12, -0.02]* | 0.35 [0.25, 0.47] | 0.35 [0.17, 0.50] | 5.90 [4.44, 7.96] | 1.33 [0.93, 1.72] | -0.10 [-0.30, 0.08] | -0.22 [-0.39, -0.07]* | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.538 [-0.020, 1.188] / [0.237, 0.876] | -0.869 [-1.475, -0.257]* / [-1.098, -0.610]* | -0.247 [-0.820, 0.346] / [-0.489, 0.007] | -0.936 [-1.542, -0.320]* / [-1.158, -0.686]* | -0.590 [-1.213, 0.053] / [-0.821, -0.347]*  ! | -0.741 [-1.384, -0.118]* / [-0.966, -0.476]* |
| h4-8 | -1.469 [-2.251, -0.723]* / [-1.822, -1.106]* | -3.071 [-3.948, -2.260]* / [-3.365, -2.779]* | -2.429 [-3.231, -1.634]* / [-2.707, -2.164]* | -3.158 [-4.040, -2.332]* / [-3.442, -2.868]* | -2.760 [-3.600, -1.943]* / [-3.042, -2.471]* | -2.555 [-3.441, -1.730]* / [-2.844, -2.248]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.538 [-1.188, 0.020] / [-0.876, -0.237]*  ! | -1.267 [-1.658, -0.872]* / [-1.521, -1.034]* | -0.636 [-0.910, -0.314]* / [-0.906, -0.391]* | -1.296 [-1.682, -0.899]* / [-1.548, -1.063]* | -0.924 [-1.232, -0.576]* / [-1.145, -0.723]* | -1.020 [-1.308, -0.721]* / [-1.301, -0.763]* |
| h4-8 | 1.469 [0.723, 2.251] / [1.106, 1.822] | -1.295 [-1.676, -0.889]* / [-1.554, -1.061]* | -0.677 [-0.968, -0.339]* / [-0.950, -0.418]* | -1.346 [-1.744, -0.930]* / [-1.602, -1.113]* | -0.971 [-1.293, -0.615]* / [-1.194, -0.758]* | -1.074 [-1.378, -0.754]* / [-1.361, -0.806]* |
| h8+ | 4.143 [3.071, 5.780] / [3.728, 4.578] | -1.333 [-1.717, -0.933]* / [-1.580, -1.097]* | -0.817 [-1.156, -0.416]* / [-1.074, -0.576]* | -1.390 [-1.811, -0.951]* / [-1.645, -1.157]* | -0.982 [-1.348, -0.564]* / [-1.196, -0.770]* | -0.919 [-1.275, -0.549]* / [-1.225, -0.636]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.869 [0.257, 1.475] / [0.610, 1.098] | 1.267 [0.872, 1.658] / [1.034, 1.521] | 0.588 [0.445, 0.743] / [0.493, 0.676] | -0.038 [-0.062, -0.016]* / [-0.054, -0.024]* | 0.339 [0.218, 0.475] / [0.241, 0.438] | 0.178 [-0.004, 0.332] / [0.029, 0.345] |
| h4-8 | 3.071 [2.260, 3.948] / [2.779, 3.365] | 1.295 [0.889, 1.676] / [1.061, 1.554] | 0.575 [0.442, 0.715] / [0.482, 0.660] | -0.060 [-0.084, -0.040]* / [-0.077, -0.044]* | 0.318 [0.203, 0.449] / [0.214, 0.420] | 0.166 [-0.020, 0.326] / [0.011, 0.333] |
| h8+ | 5.904 [4.439, 7.964] / [5.517, 6.327] | 1.333 [0.933, 1.717] / [1.097, 1.580] | 0.479 [0.369, 0.606] / [0.402, 0.552] | -0.071 [-0.124, -0.024]* / [-0.098, -0.043]* | 0.351 [0.251, 0.470] / [0.258, 0.443] | 0.349 [0.170, 0.504] / [0.213, 0.505] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.247 [-0.346, 0.820] / [-0.007, 0.489] | 0.636 [0.314, 0.910] / [0.391, 0.906] | -0.588 [-0.743, -0.445]* / [-0.676, -0.493]* | -0.626 [-0.784, -0.482]* / [-0.713, -0.532]* | -0.238 [-0.377, -0.113]* / [-0.366, -0.112]* | -0.332 [-0.547, -0.165]* / [-0.503, -0.148]* |
| h4-8 | 2.429 [1.634, 3.231] / [2.164, 2.707] | 0.677 [0.339, 0.968] / [0.418, 0.950] | -0.575 [-0.715, -0.442]* / [-0.660, -0.482]* | -0.635 [-0.780, -0.498]* / [-0.721, -0.541]* | -0.244 [-0.373, -0.117]* / [-0.367, -0.115]* | -0.339 [-0.554, -0.173]* / [-0.514, -0.149]* |
| h8+ | 5.372 [3.993, 7.376] / [4.985, 5.793] | 0.817 [0.416, 1.156] / [0.576, 1.074] | -0.479 [-0.606, -0.369]* / [-0.552, -0.402]* | -0.555 [-0.721, -0.418]* / [-0.633, -0.471]* | -0.129 [-0.288, -0.001]* / [-0.248, -0.009]* | -0.080 [-0.325, 0.101] / [-0.239, 0.095] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 10.83 [10.54, 11.14] | 15.61 [15.07, 16.16] | 14.42 [13.92, 14.97] | 14.75 [14.21, 15.30] | 13.67 [13.08, 14.29] | 15.64 [15.25, 16.02] | 17.08 [16.35, 17.83] | 12.08 [11.71, 12.44] | 11.52 [11.19, 11.84] | 14.54 [14.02, 15.08] |
| h4-8 | 11.75 [11.35, 12.16] | 15.44 [14.93, 15.97] | 14.27 [13.80, 14.78] | 14.59 [14.07, 15.11] | 13.65 [13.09, 14.25] | 19.04 [18.52, 19.57] | 16.94 [16.22, 17.70] | 12.89 [12.41, 13.35] | 12.42 [12.00, 12.88] | 14.42 [13.94, 14.94] |
| h8+ | 11.78 [11.35, 12.23] | 13.87 [13.43, 14.31] | 12.76 [12.35, 13.19] | 13.12 [12.68, 13.57] | 12.75 [12.22, 13.35] | 22.33 [21.46, 23.17] | 15.44 [14.83, 16.07] | 12.76 [12.26, 13.23] | 12.53 [12.06, 13.03] | 12.95 [12.54, 13.39] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | -5.81 [-6.90, -4.70]* | 0.05 [-1.01, 1.32] | -1.36 [-2.47, -0.07]* | -0.95 [-2.07, 0.35] | -1.64 [-2.83, -0.25] |  | 1.63 [0.40, 3.07] | -4.72 [-5.72, -3.80]* | -5.56 [-6.70, -4.45]* | -1.19 [-2.33, 0.10] | persistence |
| h4-8 | -2.75 [-3.31, -2.19]* | 1.12 [0.83, 1.42] | -0.16 [-0.21, -0.12]* | 0.27 [0.02, 0.52] | -0.29 [-0.81, 0.10] | 5.07 [3.61, 6.42] | 2.59 [1.79, 3.28] | -1.63 [-2.17, -1.12]* | -2.10 [-2.70, -1.50]* |  | wt_med |
| h8+ | -1.27 [-1.68, -0.96]* | 0.92 [0.70, 1.17] | -0.20 [-0.32, -0.08]* | 0.24 [0.02, 0.47] | -0.02 [-0.41, 0.31] | 10.66 [7.15, 14.93] | 2.57 [1.78, 3.31] | -0.33 [-0.91, 0.13] | -0.53 [-0.97, -0.19]* |  | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.629 [0.403, 3.071] / [0.882, 2.414] | -1.193 [-2.325, 0.096] / [-1.700, -0.630]*  ! | 0.050 [-1.009, 1.317] / [-0.475, 0.617] | -1.361 [-2.475, -0.073]* / [-1.856, -0.802]* | -0.953 [-2.071, 0.354] / [-1.508, -0.362]*  ! | -1.643 [-2.832, -0.250]* / [-2.180, -1.040]* |
| h4-8 | -1.966 [-3.319, -0.559]* / [-2.794, -1.111]* | -5.071 [-6.418, -3.607]* / [-5.648, -4.466]* | -3.784 [-4.956, -2.450]* / [-4.341, -3.214]* | -5.308 [-6.634, -3.875]* / [-5.894, -4.715]* | -4.811 [-6.108, -3.374]* / [-5.388, -4.168]* | -5.020 [-6.430, -3.525]* / [-5.695, -4.348]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -1.629 [-3.071, -0.403]* / [-2.414, -0.882]* | -2.572 [-3.273, -1.773]* / [-3.177, -2.000]* | -1.258 [-1.812, -0.523]* / [-1.894, -0.640]* | -2.678 [-3.363, -1.884]* / [-3.283, -2.100]* | -2.227 [-2.717, -1.556]* / [-2.762, -1.706]* | -2.719 [-3.236, -2.215]* / [-3.449, -2.028]* |
| h4-8 | 1.966 [0.559, 3.319] / [1.111, 2.794] | -2.595 [-3.282, -1.791]* / [-3.183, -2.032]* | -1.345 [-1.925, -0.570]* / [-1.979, -0.726]* | -2.741 [-3.437, -1.944]* / [-3.341, -2.172]* | -2.298 [-2.828, -1.599]* / [-2.826, -1.776]* | -2.798 [-3.351, -2.235]* / [-3.522, -2.122]* |
| h8+ | 7.402 [4.798, 11.100] / [6.526, 8.307] | -2.570 [-3.312, -1.781]* / [-3.118, -2.045]* | -1.508 [-2.176, -0.687]* / [-2.071, -0.945]* | -2.743 [-3.541, -1.896]* / [-3.310, -2.219]* | -2.316 [-2.990, -1.517]* / [-2.806, -1.825]* | -2.515 [-3.126, -1.867]* / [-3.208, -1.875]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.193 [-0.096, 2.325] / [0.630, 1.700] | 2.572 [1.773, 3.273] / [2.000, 3.177] | 1.182 [0.868, 1.527] / [0.954, 1.395] | -0.122 [-0.173, -0.070]* / [-0.163, -0.087]* | 0.320 [0.051, 0.609] / [0.103, 0.542] | -0.270 [-0.781, 0.140] / [-0.658, 0.132] |
| h4-8 | 5.071 [3.607, 6.418] / [4.466, 5.648] | 2.595 [1.791, 3.282] / [2.032, 3.183] | 1.121 [0.831, 1.420] / [0.911, 1.321] | -0.163 [-0.215, -0.116]* / [-0.207, -0.126]* | 0.266 [0.019, 0.521] / [0.053, 0.482] | -0.293 [-0.814, 0.105] / [-0.672, 0.107] |
| h8+ | 10.663 [7.153, 14.929] / [9.834, 11.558] | 2.570 [1.781, 3.312] / [2.045, 3.118] | 0.922 [0.699, 1.168] / [0.754, 1.086] | -0.202 [-0.319, -0.083]* / [-0.269, -0.133]* | 0.240 [0.017, 0.470] / [0.036, 0.433] | -0.019 [-0.412, 0.314] / [-0.338, 0.313] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.050 [-1.317, 1.009] / [-0.617, 0.475] | 1.258 [0.523, 1.812] / [0.640, 1.894] | -1.182 [-1.527, -0.868]* / [-1.395, -0.954]* | -1.305 [-1.622, -1.008]* / [-1.516, -1.084]* | -0.838 [-1.098, -0.576]* / [-1.119, -0.540]* | -1.304 [-1.739, -0.932]* / [-1.752, -0.819]* |
| h4-8 | 3.784 [2.450, 4.956] / [3.214, 4.341] | 1.345 [0.570, 1.925] / [0.726, 1.979] | -1.121 [-1.420, -0.831]* / [-1.321, -0.911]* | -1.285 [-1.562, -0.993]* / [-1.479, -1.073]* | -0.826 [-1.084, -0.569]* / [-1.100, -0.543]* | -1.296 [-1.734, -0.923]* / [-1.749, -0.818]* |
| h8+ | 9.546 [6.281, 13.747] / [8.744, 10.397] | 1.508 [0.687, 2.176] / [0.945, 2.071] | -0.922 [-1.168, -0.699]* / [-1.086, -0.754]* | -1.133 [-1.434, -0.861]* / [-1.302, -0.954]* | -0.687 [-0.972, -0.421]* / [-0.933, -0.427]* | -0.864 [-1.314, -0.473]* / [-1.226, -0.472]* |

**MAE by region (all rides)**

_no rows_

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | tirex2 | toto2 | chronos2 |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 1.23 | -0.18 | -1.09 | -0.71 | -1.33 | -1.00 | -1.12 | -1.78 | -1.52 | -1.17 |
| h4-8 | 2.26 | -0.17 | -1.19 | -0.81 | -1.40 | -1.05 | -1.17 | -2.06 | -1.84 | -1.58 |
| h8+ | 4.69 | -0.11 | -1.27 | -1.06 | -1.56 | -1.26 | -1.27 | -1.50 | -1.46 | -1.37 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 6.55 [6.28, 6.83] | 6.97 [6.67, 7.26] |  | 7.67 [7.40, 7.95] | 7.07 [6.81, 7.33] | 9.60 [9.12, 10.17] | 7.40 [7.13, 7.66] | 6.88 [6.60, 7.19] | 5.19 [5.04, 5.34] | 5.93 [5.66, 6.19] | 8.38 [8.03, 8.75] | 7.05 [6.75, 7.33] | 6.60 [6.33, 6.88] |  | 7.04 [6.75, 7.34] | 11.45 [10.94, 11.96] | 7.07 [6.77, 7.37] |  | 7.12 [6.85, 7.37] |
| 1 | 6.77 [6.53, 7.01] | 7.24 [6.96, 7.50] | 6.76 [6.54, 6.98] | 7.44 [7.19, 7.71] | 6.88 [6.64, 7.11] | 9.25 [8.81, 9.72] | 7.13 [6.90, 7.36] | 6.76 [6.48, 7.03] | 5.16 [5.01, 5.30] | 5.63 [5.40, 5.86] | 8.28 [7.96, 8.60] | 7.22 [6.96, 7.47] | 6.84 [6.59, 7.10] | 6.84 [6.60, 7.07] | 7.27 [6.99, 7.52] | 11.60 [11.14, 12.09] | 7.30 [7.03, 7.56] | 6.88 [6.65, 7.11] | 6.93 [6.69, 7.17] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.58 [-0.76, -0.39]* | -0.15 [-0.35, 0.02] |  | 0.58 [0.44, 0.74] | -0.04 [-0.07, -0.02]* | 2.85 [2.05, 4.03] | 0.32 [0.21, 0.45] | 0.17 [-0.00, 0.32] | -1.94 [-2.41, -1.51]* | -1.15 [-1.50, -0.79]* | 1.27 [0.88, 1.64] | -0.08 [-0.26, 0.09] | -0.53 [-0.72, -0.34]* |  | -0.09 [-0.27, 0.09] | 4.38 [2.89, 5.91] | -0.06 [-0.24, 0.12] |  | wt_med |
| 1 | -0.17 [-0.35, -0.02]* | 0.30 [0.10, 0.50] | -0.18 [-0.32, -0.03]* | 0.52 [0.35, 0.74] | -0.05 [-0.08, -0.03]* | 2.69 [2.06, 3.53] | 0.23 [0.12, 0.34] | 0.24 [0.03, 0.45] | -1.78 [-2.17, -1.41]* | -1.27 [-1.61, -0.91]* | 1.34 [1.04, 1.63] | 0.27 [0.09, 0.43] | -0.10 [-0.26, 0.05] | -0.10 [-0.24, 0.03] | 0.32 [0.13, 0.52] | 4.73 [3.40, 6.16] | 0.36 [0.16, 0.56] | -0.05 [-0.20, 0.09] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -1.268 [-1.644, -0.885]* / [-1.524, -1.039]* | -0.643 [-0.904, -0.327]* / [-0.919, -0.402]* | -1.299 [-1.671, -0.909]* / [-1.549, -1.067]* | -0.941 [-1.244, -0.594]* / [-1.159, -0.737]* | -1.013 [-1.296, -0.720]* / [-1.289, -0.757]* | 1.824 [1.133, 2.636] / [1.432, 2.224] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.268 [0.885, 1.644] / [1.039, 1.524] | 0.582 [0.442, 0.737] / [0.490, 0.667] | -0.040 [-0.068, -0.016]* / [-0.056, -0.026]* | 0.323 [0.207, 0.453] / [0.227, 0.417] | 0.171 [-0.005, 0.319] / [0.030, 0.328] | 2.854 [2.046, 4.028] / [2.515, 3.201] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.643 [0.327, 0.904] / [0.402, 0.919] | -0.582 [-0.737, -0.442]* / [-0.667, -0.490]* | -0.622 [-0.781, -0.479]* / [-0.703, -0.531]* | -0.248 [-0.386, -0.124]* / [-0.371, -0.125]* | -0.335 [-0.547, -0.175]* / [-0.503, -0.156]* | 2.356 [1.524, 3.437] / [2.004, 2.704] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 12.92 [12.40, 13.48] | 13.91 [13.32, 14.52] |  | 15.43 [14.90, 15.97] | 14.24 [13.76, 14.77] | 19.11 [18.00, 20.43] | 14.55 [14.00, 15.09] | 13.53 [12.95, 14.15] | 9.60 [9.37, 9.82] | 11.26 [10.71, 11.82] | 16.91 [16.18, 17.66] | 14.01 [13.46, 14.57] | 13.13 [12.64, 13.70] |  | 13.97 [13.41, 14.59] | 24.50 [23.52, 25.40] | 14.07 [13.49, 14.68] |  | 14.37 [13.88, 14.91] |
| 1 | 13.44 [12.99, 13.90] | 14.50 [13.98, 14.99] | 13.07 [12.68, 13.46] | 14.99 [14.55, 15.50] | 13.85 [13.44, 14.28] | 18.37 [17.43, 19.36] | 13.85 [13.43, 14.26] | 13.22 [12.69, 13.68] | 9.59 [9.37, 9.81] | 10.51 [10.06, 10.96] | 16.51 [15.95, 17.08] | 14.42 [13.95, 14.89] | 13.67 [13.15, 14.16] | 13.23 [12.80, 13.62] | 14.54 [14.00, 15.06] | 24.80 [23.93, 25.69] | 14.62 [14.08, 15.14] | 13.25 [12.83, 13.67] | 13.99 [13.58, 14.43] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.52 [-1.94, -1.16]* | -0.53 [-1.01, -0.13]* |  | 1.17 [0.86, 1.52] | -0.13 [-0.19, -0.07]* | 5.23 [2.87, 8.21] | 0.29 [0.04, 0.57] | -0.27 [-0.76, 0.11] | -4.78 [-5.78, -3.83]* | -2.99 [-3.36, -2.60]* | 2.57 [1.79, 3.23] | -0.44 [-0.87, -0.05]* | -1.32 [-1.74, -0.95]* |  | -0.47 [-0.89, -0.07]* | 10.17 [6.66, 13.45] | -0.38 [-0.81, 0.03] |  | wt_med |
| 1 | -0.62 [-1.06, -0.24]* | 0.44 [-0.08, 0.91] | -0.93 [-1.28, -0.63]* | 1.07 [0.66, 1.66] | -0.14 [-0.22, -0.08]* | 4.75 [2.88, 6.82] | -0.04 [-0.31, 0.18] | -0.42 [-0.93, -0.00]* | -4.41 [-5.13, -3.67]* | -3.38 [-3.73, -3.00]* | 2.50 [1.91, 2.99] | 0.36 [-0.12, 0.77] | -0.38 [-0.77, -0.00]* | -0.76 [-1.09, -0.50]* | 0.49 [0.01, 0.96] | 10.87 [7.76, 13.85] | 0.57 [0.08, 1.04] | -0.74 [-1.04, -0.44]* | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -2.567 [-3.231, -1.794]* / [-3.167, -1.991]* | -1.266 [-1.799, -0.562]* / [-1.908, -0.652]* | -2.681 [-3.333, -1.909]* / [-3.286, -2.120]* | -2.253 [-2.737, -1.603]* / [-2.788, -1.743]* | -2.679 [-3.191, -2.187]* / [-3.408, -1.998]* | 3.204 [1.307, 5.478] / [2.143, 4.264] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 2.567 [1.794, 3.231] / [1.991, 3.167] | 1.173 [0.864, 1.521] / [0.947, 1.385] | -0.128 [-0.190, -0.067]* / [-0.168, -0.092]* | 0.289 [0.038, 0.569] / [0.084, 0.494] | -0.271 [-0.762, 0.114] / [-0.637, 0.106] | 5.233 [2.866, 8.211] / [4.300, 6.182] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.266 [0.562, 1.799] / [0.652, 1.908] | -1.173 [-1.521, -0.864]* / [-1.385, -0.947]* | -1.300 [-1.632, -0.993]* / [-1.509, -1.084]* | -0.858 [-1.122, -0.596]* / [-1.129, -0.570]* | -1.301 [-1.729, -0.937]* / [-1.737, -0.825]* | 4.198 [1.919, 6.979] / [3.284, 5.173] |

**MAE by region (all rides)**

| lead | region | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 8.44 | 9.00 |  | 9.92 | 9.12 | 13.98 | 9.64 | 8.96 | 6.16 | 8.07 | 10.88 | 9.15 | 8.58 |  | 9.14 | 16.55 | 9.21 |  | 9.16 |
| 0 | EU | 4.54 | 4.88 |  | 5.38 | 4.97 | 7.15 | 5.17 | 4.97 | 3.72 | 4.24 | 5.63 | 4.96 | 4.62 |  | 4.96 | 7.27 | 4.96 |  | 5.00 |
| 0 | NA | 7.35 | 7.74 |  | 8.37 | 7.81 | 9.39 | 8.09 | 7.63 | 6.11 | 6.04 | 9.43 | 7.73 | 7.28 |  | 7.72 | 12.02 | 7.76 |  | 7.87 |
| 1 | Asia | 8.53 | 9.02 | 8.39 | 9.36 | 8.51 | 12.60 | 8.83 | 8.41 | 6.04 | 7.33 | 10.14 | 9.08 | 8.66 | 8.54 | 9.13 | 16.00 | 9.19 | 8.51 | 8.55 |
| 1 | EU | 4.68 | 5.08 | 4.82 | 5.27 | 4.90 | 7.03 | 5.10 | 5.03 | 3.71 | 4.10 | 5.74 | 5.09 | 4.81 | 4.86 | 5.07 | 7.62 | 5.10 | 4.99 | 4.94 |
| 1 | NA | 7.81 | 8.35 | 7.69 | 8.35 | 7.87 | 9.74 | 8.09 | 7.73 | 6.15 | 5.97 | 9.79 | 8.21 | 7.77 | 7.76 | 8.35 | 12.66 | 8.38 | 7.75 | 7.95 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 6.77 [6.53, 7.01] | 7.24 [6.96, 7.50] | 6.76 [6.54, 6.98] | 7.44 [7.19, 7.71] | 6.88 [6.64, 7.11] | 9.25 [8.81, 9.72] | 7.13 [6.90, 7.36] | 6.76 [6.48, 7.03] | 5.16 [5.01, 5.30] | 5.63 [5.40, 5.86] |  |  | 8.28 [7.96, 8.60] | 7.22 [6.96, 7.47] | 6.84 [6.59, 7.10] | 6.84 [6.60, 7.07] | 7.27 [6.99, 7.52] | 11.60 [11.14, 12.09] | 7.30 [7.03, 7.56] | 6.88 [6.65, 7.11] | 6.93 [6.69, 7.17] |
| 2 | 7.14 [6.89, 7.40] | 7.68 [7.41, 7.96] | 7.09 [6.86, 7.33] | 7.76 [7.50, 8.02] | 7.20 [6.96, 7.46] | 9.99 [9.50, 10.56] | 7.41 [7.17, 7.65] | 7.08 [6.79, 7.41] | 5.20 [5.06, 5.36] | 5.91 [5.67, 6.14] |  |  | 8.41 [8.11, 8.72] | 7.60 [7.34, 7.87] | 7.21 [6.95, 7.49] | 7.14 [6.91, 7.38] | 7.73 [7.46, 8.01] | 11.92 [11.46, 12.40] | 7.77 [7.49, 8.05] | 7.24 [7.00, 7.49] | 7.26 [7.02, 7.53] |
| 3 | 7.29 [7.03, 7.58] | 7.79 [7.50, 8.08] | 7.20 [6.95, 7.46] | 7.82 [7.54, 8.12] | 7.22 [6.95, 7.49] | 9.76 [9.29, 10.26] | 7.48 [7.21, 7.76] | 7.24 [6.94, 7.57] | 5.24 [5.10, 5.41] | 5.96 [5.70, 6.24] | 7.80 [7.49, 8.14] | 7.78 [7.47, 8.13] | 8.21 [7.90, 8.55] | 7.71 [7.43, 7.99] | 7.33 [7.05, 7.62] | 7.23 [6.99, 7.48] | 7.91 [7.63, 8.20] | 11.83 [11.35, 12.32] | 7.95 [7.67, 8.24] | 7.31 [7.06, 7.57] | 7.30 [7.03, 7.57] |
| 4 | 7.49 [7.21, 7.78] | 8.08 [7.75, 8.40] | 7.30 [7.02, 7.56] | 7.81 [7.53, 8.09] | 7.26 [7.00, 7.53] | 9.90 [9.43, 10.37] | 7.53 [7.28, 7.80] | 7.32 [7.02, 7.64] | 5.30 [5.12, 5.48] | 6.04 [5.79, 6.30] | 7.88 [7.58, 8.19] | 7.86 [7.56, 8.17] | 8.37 [8.05, 8.71] | 7.96 [7.66, 8.27] | 7.56 [7.26, 7.86] | 7.32 [7.06, 7.59] | 8.16 [7.84, 8.49] | 12.14 [11.59, 12.70] | 8.20 [7.89, 8.53] | 7.44 [7.16, 7.72] | 7.32 [7.06, 7.59] |
| 5 | 7.40 [7.10, 7.69] | 7.95 [7.64, 8.25] | 7.23 [6.97, 7.48] | 7.74 [7.48, 8.02] | 7.27 [6.99, 7.52] | 9.86 [9.35, 10.42] | 7.44 [7.17, 7.69] | 7.20 [6.89, 7.52] | 5.24 [5.08, 5.40] | 5.91 [5.63, 6.17] | 7.82 [7.51, 8.16] | 7.79 [7.48, 8.13] | 8.36 [8.00, 8.73] | 7.89 [7.58, 8.19] | 7.49 [7.18, 7.78] | 7.24 [6.98, 7.50] | 8.01 [7.69, 8.31] | 11.86 [11.38, 12.36] | 8.04 [7.72, 8.34] | 7.35 [7.07, 7.63] | 7.34 [7.06, 7.59] |
| 6 | 7.16 [6.92, 7.41] | 7.68 [7.40, 7.96] | 6.94 [6.72, 7.17] | 7.62 [7.34, 7.89] | 7.02 [6.78, 7.27] | 9.53 [9.09, 10.00] | 7.17 [6.93, 7.40] | 7.07 [6.78, 7.36] | 5.20 [5.04, 5.35] | 5.61 [5.38, 5.84] | 7.75 [7.44, 8.06] | 7.73 [7.41, 8.04] | 8.25 [7.93, 8.56] | 7.61 [7.35, 7.88] | 7.13 [6.88, 7.40] | 6.97 [6.74, 7.19] | 7.75 [7.47, 8.03] | 11.41 [10.93, 11.87] | 7.78 [7.49, 8.05] | 7.08 [6.84, 7.31] | 7.09 [6.84, 7.34] |
| 7 | 7.66 [7.40, 7.94] | 8.17 [7.89, 8.49] | 7.41 [7.17, 7.67] | 7.89 [7.64, 8.17] | 7.46 [7.20, 7.74] | 10.28 [9.76, 10.84] | 7.73 [7.48, 8.00] | 7.49 [7.16, 7.84] | 5.28 [5.13, 5.45] | 6.31 [6.07, 6.57] | 8.04 [7.70, 8.40] | 8.02 [7.68, 8.38] |  | 8.13 [7.86, 8.43] | 7.79 [7.51, 8.08] | 7.45 [7.21, 7.70] | 8.31 [8.03, 8.62] | 11.92 [11.44, 12.45] | 8.34 [8.06, 8.66] | 7.63 [7.38, 7.90] | 7.53 [7.28, 7.81] |
| 10 |  |  | 7.53 [7.26, 7.79] | 7.87 [7.60, 8.14] | 7.45 [7.17, 7.71] | 10.25 [9.74, 10.82] | 7.78 [7.49, 8.05] | 7.61 [7.25, 7.97] | 5.30 [5.14, 5.45] | 6.34 [6.06, 6.61] | 8.21 [7.82, 8.58] | 8.20 [7.81, 8.56] |  |  |  | 7.52 [7.25, 7.79] |  |  |  | 7.76 [7.47, 8.05] | 7.54 [7.26, 7.80] |
| 14 |  |  | 7.70 [7.43, 7.98] | 8.07 [7.78, 8.36] | 7.66 [7.38, 7.93] | 10.35 [9.85, 10.83] | 8.10 [7.83, 8.37] | 7.95 [7.61, 8.28] | 5.42 [5.24, 5.61] | 6.71 [6.45, 6.99] | 8.53 [8.14, 8.90] | 8.51 [8.12, 8.88] |  |  |  | 7.71 [7.44, 7.99] |  |  |  | 7.92 [7.61, 8.22] | 7.74 [7.47, 8.01] |
| 21 |  |  | 7.65 [7.39, 7.92] | 7.96 [7.66, 8.25] | 7.57 [7.31, 7.85] | 10.09 [9.59, 10.62] | 7.99 [7.70, 8.28] | 7.85 [7.49, 8.21] | 5.38 [5.22, 5.54] | 6.60 [6.31, 6.91] | 8.58 [8.18, 8.99] | 8.56 [8.16, 8.97] |  |  |  | 7.66 [7.40, 7.93] |  |  |  | 7.92 [7.63, 8.21] | 7.67 [7.40, 7.95] |
| 30 |  |  | 7.90 [7.62, 8.17] | 8.14 [7.85, 8.44] | 7.80 [7.50, 8.08] | 10.65 [10.00, 11.28] | 8.34 [8.05, 8.64] | 8.17 [7.77, 8.58] | 5.43 [5.26, 5.60] | 7.05 [6.74, 7.36] | 8.92 [8.51, 9.35] | 8.89 [8.49, 9.32] |  |  |  | 7.97 [7.69, 8.24] |  |  |  | 8.26 [7.94, 8.57] | 7.92 [7.62, 8.20] |
| 45 |  |  | 8.05 [7.75, 8.34] | 8.28 [7.98, 8.62] | 7.93 [7.63, 8.24] | 10.35 [9.52, 11.20] | 8.38 [8.05, 8.69] | 8.46 [7.90, 9.08] | 5.46 [5.28, 5.64] | 7.04 [6.72, 7.36] | 9.50 [8.94, 10.15] | 9.47 [8.91, 10.12] |  |  |  | 8.16 [7.87, 8.45] |  |  |  | 8.43 [8.13, 8.75] | 8.06 [7.75, 8.37] |
| 60 |  |  | 8.20 [7.90, 8.50] | 8.47 [8.16, 8.81] | 8.07 [7.76, 8.39] |  | 8.55 [8.22, 8.88] | 0.30 [0.12, 0.50] | 5.48 [5.29, 5.66] | 7.24 [6.90, 7.57] | 0.23 [0.02, 0.52] | 0.24 [0.02, 0.53] |  |  |  | 8.30 [7.99, 8.59] |  |  |  | 8.54 [8.21, 8.88] | 8.20 [7.89, 8.52] |
| 90 |  |  | 8.56 [8.21, 8.91] | 8.79 [8.41, 9.17] | 8.42 [8.06, 8.80] |  | 9.00 [8.60, 9.39] |  | 5.45 [5.25, 5.66] | 7.67 [7.24, 8.08] |  |  |  |  |  | 8.73 [8.36, 9.10] |  |  |  | 8.96 [8.58, 9.36] | 8.56 [8.18, 8.94] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.17 [-0.35, -0.02]* | 0.30 [0.10, 0.50] | -0.18 [-0.32, -0.03]* | 0.52 [0.35, 0.74] | -0.05 [-0.08, -0.03]* | 2.69 [2.06, 3.53] | 0.23 [0.12, 0.34] | 0.24 [0.03, 0.45] | -1.78 [-2.17, -1.41]* | -1.27 [-1.61, -0.91]* |  |  | 1.34 [1.04, 1.63] | 0.27 [0.09, 0.43] | -0.10 [-0.26, 0.05] | -0.10 [-0.24, 0.03] | 0.32 [0.13, 0.52] | 4.73 [3.40, 6.16] | 0.36 [0.16, 0.56] | -0.05 [-0.20, 0.09] | wt_med |
| 2 | -0.14 [-0.39, 0.04] | 0.40 [0.11, 0.66] | -0.17 [-0.34, -0.03]* | 0.51 [0.38, 0.67] | -0.06 [-0.09, -0.03]* | 2.94 [2.12, 4.05] | 0.16 [0.05, 0.26] | 0.17 [-0.04, 0.32] | -2.07 [-2.51, -1.66]* | -1.34 [-1.72, -0.96]* |  |  | 1.14 [0.80, 1.47] | 0.32 [0.08, 0.54] | -0.06 [-0.26, 0.09] | -0.12 [-0.27, 0.00] | 0.45 [0.20, 0.68] | 4.71 [3.31, 6.19] | 0.49 [0.23, 0.71] | -0.02 [-0.16, 0.10] | wt_med |
| 3 | -0.02 [-0.25, 0.16] | 0.48 [0.18, 0.72] | -0.09 [-0.27, 0.07] | 0.54 [0.38, 0.73] | -0.07 [-0.11, -0.04]* | 2.66 [1.99, 3.45] | 0.20 [0.08, 0.31] | 0.27 [0.09, 0.42] | -2.06 [-2.56, -1.61]* | -1.32 [-1.64, -0.99]* | 0.86 [0.52, 1.16] | 0.84 [0.50, 1.13] | 0.93 [0.62, 1.21] | 0.40 [0.16, 0.60] | 0.02 [-0.19, 0.20] | -0.06 [-0.24, 0.08] | 0.60 [0.35, 0.82] | 4.59 [3.35, 5.91] | 0.64 [0.39, 0.86] | 0.01 [-0.14, 0.16] | wt_med |
| 4 | 0.16 [-0.13, 0.40] | 0.75 [0.36, 1.11] | -0.02 [-0.18, 0.12] | 0.51 [0.34, 0.75] | -0.05 [-0.08, -0.03]* | 2.87 [2.09, 3.68] | 0.24 [0.10, 0.36] | 0.38 [0.15, 0.58] | -2.02 [-2.49, -1.60]* | -1.26 [-1.60, -0.89]* | 0.97 [0.61, 1.31] | 0.95 [0.59, 1.28] | 1.02 [0.65, 1.36] | 0.63 [0.30, 0.95] | 0.23 [-0.02, 0.46] | 0.01 [-0.14, 0.13] | 0.82 [0.49, 1.16] | 4.87 [3.49, 6.42] | 0.86 [0.53, 1.21] | 0.13 [-0.05, 0.30] | wt_med |
| 5 | 0.05 [-0.19, 0.27] | 0.61 [0.26, 0.94] | -0.10 [-0.23, -0.00]* | 0.46 [0.32, 0.63] | -0.06 [-0.09, -0.03]* | 2.88 [2.03, 4.03] | 0.15 [0.03, 0.28] | 0.35 [0.10, 0.65] | -2.10 [-2.59, -1.66]* | -1.38 [-1.74, -1.01]* | 1.01 [0.64, 1.35] | 0.99 [0.62, 1.32] | 1.05 [0.67, 1.42] | 0.55 [0.24, 0.88] | 0.15 [-0.08, 0.36] | -0.09 [-0.21, 0.00] | 0.66 [0.34, 0.96] | 4.59 [3.15, 6.10] | 0.69 [0.37, 1.00] | 0.02 [-0.11, 0.13] | wt_med |
| 6 | 0.06 [-0.19, 0.27] | 0.58 [0.25, 0.87] | -0.15 [-0.32, 0.00] | 0.54 [0.36, 0.77] | -0.06 [-0.09, -0.03]* | 2.76 [2.12, 3.52] | 0.09 [-0.04, 0.22] | 0.39 [0.14, 0.61] | -1.90 [-2.31, -1.50]* | -1.46 [-1.83, -1.07]* | 1.12 [0.70, 1.45] | 1.10 [0.69, 1.43] | 1.15 [0.83, 1.44] | 0.51 [0.22, 0.77] | 0.04 [-0.13, 0.18] | -0.12 [-0.26, 0.01] | 0.64 [0.36, 0.91] | 4.39 [3.06, 5.83] | 0.67 [0.38, 0.96] | -0.01 [-0.16, 0.15] | wt_med |
| 7 | 0.11 [-0.16, 0.34] | 0.63 [0.26, 0.97] | -0.12 [-0.28, 0.00] | 0.39 [0.26, 0.56] | -0.07 [-0.10, -0.05]* | 2.98 [2.21, 4.07] | 0.25 [0.13, 0.36] | 0.29 [0.10, 0.46] | -2.27 [-2.74, -1.82]* | -1.17 [-1.55, -0.82]* | 0.90 [0.55, 1.21] | 0.88 [0.54, 1.19] |  | 0.59 [0.24, 0.89] | 0.24 [0.01, 0.43] | -0.08 [-0.22, 0.02] | 0.77 [0.42, 1.10] | 4.46 [3.10, 5.88] | 0.80 [0.44, 1.14] | 0.10 [-0.05, 0.22] | wt_med |
| 10 |  |  | -0.01 [-0.15, 0.11] | 0.37 [0.24, 0.51] | -0.08 [-0.11, -0.05]* | 3.00 [2.19, 4.05] | 0.28 [0.12, 0.45] | 0.51 [0.28, 0.74] | -2.24 [-2.74, -1.78]* | -1.16 [-1.53, -0.80]* | 1.15 [0.72, 1.50] | 1.13 [0.71, 1.46] |  |  |  | -0.01 [-0.14, 0.10] |  |  |  | 0.23 [0.07, 0.39] | wt_med |
| 14 |  |  | -0.04 [-0.20, 0.08] | 0.33 [0.17, 0.53] | -0.07 [-0.10, -0.05]* | 2.86 [2.04, 3.75] | 0.38 [0.25, 0.50] | 0.57 [0.30, 0.86] | -2.33 [-2.87, -1.84]* | -1.01 [-1.39, -0.60]* | 1.21 [0.71, 1.65] | 1.18 [0.69, 1.62] |  |  |  | -0.02 [-0.15, 0.08] |  |  |  | 0.18 [0.04, 0.32] | wt_med |
| 21 |  |  | -0.01 [-0.20, 0.13] | 0.29 [0.13, 0.48] | -0.08 [-0.12, -0.05]* | 2.72 [2.00, 3.54] | 0.35 [0.26, 0.43] | 0.58 [0.33, 0.82] | -2.31 [-2.80, -1.82]* | -1.03 [-1.39, -0.65]* | 1.33 [0.85, 1.82] | 1.31 [0.83, 1.80] |  |  |  | -0.00 [-0.14, 0.13] |  |  |  | 0.25 [0.07, 0.46] | wt_med |
| 30 |  |  | 0.01 [-0.18, 0.14] | 0.24 [0.05, 0.42] | -0.09 [-0.12, -0.06]* | 2.83 [1.96, 3.79] | 0.44 [0.29, 0.59] | 0.44 [0.19, 0.67] | -2.48 [-3.02, -1.99]* | -0.86 [-1.24, -0.48]* | 1.20 [0.74, 1.65] | 1.17 [0.72, 1.63] |  |  |  | 0.08 [-0.06, 0.18] |  |  |  | 0.37 [0.21, 0.54] | wt_med |
| 45 |  |  | 0.03 [-0.11, 0.15] | 0.22 [0.01, 0.46] | -0.08 [-0.11, -0.06]* | 1.70 [0.97, 2.56] | 0.37 [0.22, 0.55] | 0.39 [0.06, 0.72] | -2.60 [-3.16, -2.09]* | -0.95 [-1.36, -0.50]* | 1.39 [0.81, 1.97] | 1.37 [0.78, 1.94] |  |  |  | 0.14 [0.01, 0.26] |  |  |  | 0.41 [0.23, 0.63] | wt_med |
| 60 |  |  | 0.04 [-0.11, 0.17] | 0.29 [-0.01, 0.62] | -0.09 [-0.12, -0.06]* |  | 0.40 [0.25, 0.56] | 0.10 [-0.20, 0.26] | -2.72 [-3.29, -2.18]* | -0.89 [-1.34, -0.41]* | 0.02 [0.00, 1.23] | 0.03 [0.00, 1.32] |  |  |  | 0.13 [0.01, 0.26] |  |  |  | 0.37 [0.18, 0.61] | wt_med |
| 90 |  |  | 0.05 [-0.12, 0.20] | 0.28 [-0.03, 0.69] | -0.09 [-0.12, -0.06]* |  | 0.51 [0.32, 0.74] |  | -3.10 [-3.79, -2.41]* | -0.79 [-1.20, -0.29]* |  |  |  |  |  | 0.22 [0.10, 0.37] |  |  |  | 0.45 [0.19, 0.80] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -1.338 [-1.627, -1.037]* / [-1.548, -1.103]* | -0.797 [-1.083, -0.490]* / [-1.016, -0.539]* | -1.381 [-1.678, -1.076]* / [-1.590, -1.146]* | -1.128 [-1.415, -0.861]* / [-1.328, -0.929]* | -0.929 [-1.229, -0.636]* / [-1.202, -0.677]* | 1.706 [0.976, 2.409] / [1.327, 2.093] |  |  |
| 2 | -1.138 [-1.475, -0.796]* / [-1.377, -0.910]* | -0.564 [-0.814, -0.272]* / [-0.817, -0.313]* | -1.182 [-1.536, -0.826]* / [-1.424, -0.953]* | -0.962 [-1.301, -0.659]* / [-1.178, -0.760]* | -0.852 [-1.221, -0.504]* / [-1.117, -0.595]* | 2.114 [1.375, 2.876] / [1.692, 2.547] |  |  |
| 3 | -0.925 [-1.213, -0.619]* / [-1.113, -0.733]* | -0.366 [-0.670, -0.026]* / [-0.601, -0.126]* | -0.984 [-1.289, -0.669]* / [-1.173, -0.791]* | -0.717 [-0.971, -0.457]* / [-0.877, -0.549]* | -0.419 [-0.742, -0.111]* / [-0.649, -0.197]* | 2.086 [1.452, 2.770] / [1.722, 2.471] | 0.156 [-0.284, 0.537] / [-0.124, 0.429] | 0.136 [-0.305, 0.519] / [-0.143, 0.405] |
| 4 | -1.023 [-1.359, -0.650]* / [-1.257, -0.811]* | -0.447 [-0.820, -0.041]* / [-0.718, -0.210]* | -1.061 [-1.406, -0.691]* / [-1.295, -0.859]* | -0.800 [-1.153, -0.439]* / [-0.994, -0.615]* | -0.642 [-1.014, -0.254]* / [-0.875, -0.398]* | 1.986 [1.423, 2.574] / [1.610, 2.362] | -0.042 [-0.476, 0.370] / [-0.285, 0.224] | -0.066 [-0.500, 0.348] / [-0.310, 0.200] |
| 5 | -1.045 [-1.424, -0.667]* / [-1.312, -0.813]* | -0.512 [-0.792, -0.199]* / [-0.789, -0.247]* | -1.091 [-1.466, -0.702]* / [-1.357, -0.855]* | -0.894 [-1.201, -0.547]* / [-1.116, -0.684]* | -0.624 [-0.943, -0.257]* / [-0.910, -0.378]* | 2.036 [1.337, 2.826] / [1.651, 2.457] | 0.089 [-0.270, 0.450] / [-0.208, 0.376] | 0.063 [-0.294, 0.423] / [-0.233, 0.347] |
| 6 | -1.151 [-1.440, -0.833]* / [-1.371, -0.907]* | -0.565 [-0.881, -0.246]* / [-0.803, -0.295]* | -1.196 [-1.497, -0.886]* / [-1.419, -0.953]* | -1.070 [-1.349, -0.801]* / [-1.273, -0.870]* | -0.606 [-0.939, -0.267]* / [-0.891, -0.352]* | 1.910 [1.193, 2.594] / [1.506, 2.305] | 0.166 [-0.254, 0.542] / [-0.134, 0.439] | 0.143 [-0.279, 0.524] / [-0.153, 0.419] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.338 [1.037, 1.627] / [1.103, 1.548] | 0.523 [0.354, 0.740] / [0.437, 0.614] | -0.051 [-0.083, -0.027]* / [-0.064, -0.037]* | 0.229 [0.119, 0.339] / [0.127, 0.323] | 0.238 [0.031, 0.448] / [0.078, 0.383] | 2.692 [2.058, 3.531] / [2.412, 2.999] |  |  |
| 2 | 1.138 [0.796, 1.475] / [0.910, 1.377] | 0.509 [0.378, 0.671] / [0.421, 0.601] | -0.056 [-0.087, -0.033]* / [-0.072, -0.041]* | 0.161 [0.052, 0.262] / [0.062, 0.254] | 0.167 [-0.037, 0.325] / [0.002, 0.332] | 2.938 [2.119, 4.047] / [2.611, 3.313] |  |  |
| 3 | 0.925 [0.619, 1.213] / [0.733, 1.113] | 0.541 [0.385, 0.734] / [0.444, 0.637] | -0.069 [-0.112, -0.041]* / [-0.086, -0.053]* | 0.204 [0.078, 0.311] / [0.103, 0.291] | 0.267 [0.089, 0.423] / [0.100, 0.417] | 2.660 [1.993, 3.455] / [2.341, 3.013] | 0.856 [0.519, 1.158] / [0.628, 1.068] | 0.836 [0.504, 1.133] / [0.606, 1.049] |
| 4 | 1.023 [0.650, 1.359] / [0.811, 1.257] | 0.514 [0.336, 0.754] / [0.416, 0.612] | -0.049 [-0.075, -0.027]* / [-0.065, -0.034]* | 0.242 [0.105, 0.363] / [0.146, 0.344] | 0.377 [0.150, 0.585] / [0.221, 0.537] | 2.865 [2.088, 3.679] / [2.542, 3.166] | 0.973 [0.615, 1.312] / [0.771, 1.196] | 0.950 [0.590, 1.282] / [0.750, 1.171] |
| 5 | 1.045 [0.667, 1.424] / [0.813, 1.312] | 0.459 [0.317, 0.625] / [0.366, 0.550] | -0.056 [-0.085, -0.032]* / [-0.074, -0.041]* | 0.148 [0.026, 0.281] / [0.047, 0.247] | 0.352 [0.102, 0.645] / [0.186, 0.522] | 2.882 [2.026, 4.030] / [2.538, 3.239] | 1.011 [0.642, 1.348] / [0.780, 1.229] | 0.987 [0.624, 1.316] / [0.758, 1.207] |
| 6 | 1.151 [0.833, 1.440] / [0.907, 1.371] | 0.535 [0.362, 0.769] / [0.443, 0.627] | -0.057 [-0.090, -0.033]* / [-0.073, -0.043]* | 0.093 [-0.039, 0.221] / [-0.015, 0.192] | 0.392 [0.142, 0.614] / [0.229, 0.551] | 2.755 [2.119, 3.523] / [2.458, 3.069] | 1.123 [0.704, 1.452] / [0.906, 1.342] | 1.102 [0.688, 1.432] / [0.888, 1.320] |
| 7 |  | 0.392 [0.257, 0.557] / [0.295, 0.493] | -0.070 [-0.100, -0.046]* / [-0.087, -0.054]* | 0.247 [0.129, 0.358] / [0.154, 0.345] | 0.293 [0.103, 0.456] / [0.124, 0.477] | 2.976 [2.215, 4.067] / [2.646, 3.345] | 0.902 [0.552, 1.207] / [0.660, 1.159] | 0.882 [0.537, 1.188] / [0.641, 1.137] |
| 10 |  | 0.368 [0.238, 0.506] / [0.272, 0.461] | -0.075 [-0.110, -0.047]* / [-0.092, -0.059]* | 0.281 [0.122, 0.448] / [0.181, 0.388] | 0.514 [0.276, 0.744] / [0.342, 0.687] | 3.001 [2.190, 4.048] / [2.620, 3.366] | 1.151 [0.724, 1.496] / [0.901, 1.409] | 1.132 [0.707, 1.464] / [0.886, 1.388] |
| 14 |  | 0.334 [0.169, 0.534] / [0.230, 0.440] | -0.071 [-0.100, -0.049]* / [-0.087, -0.057]* | 0.380 [0.254, 0.499] / [0.284, 0.490] | 0.575 [0.300, 0.860] / [0.388, 0.752] | 2.860 [2.042, 3.753] / [2.504, 3.200] | 1.208 [0.708, 1.650] / [0.948, 1.480] | 1.185 [0.693, 1.623] / [0.926, 1.458] |
| 21 |  | 0.290 [0.126, 0.478] / [0.180, 0.394] | -0.079 [-0.117, -0.052]* / [-0.095, -0.064]* | 0.346 [0.263, 0.431] / [0.248, 0.450] | 0.576 [0.331, 0.823] / [0.411, 0.743] | 2.719 [2.003, 3.536] / [2.351, 3.079] | 1.335 [0.848, 1.818] / [1.086, 1.612] | 1.312 [0.832, 1.796] / [1.065, 1.591] |
| 30 |  | 0.237 [0.053, 0.423] / [0.126, 0.350] | -0.087 [-0.122, -0.060]* / [-0.105, -0.071]* | 0.437 [0.292, 0.585] / [0.341, 0.547] | 0.442 [0.187, 0.669] / [0.226, 0.656] | 2.830 [1.959, 3.788] / [2.418, 3.260] | 1.200 [0.742, 1.655] / [0.889, 1.530] | 1.173 [0.722, 1.633] / [0.863, 1.505] |
| 45 |  | 0.221 [0.009, 0.462] / [0.113, 0.336] | -0.084 [-0.114, -0.058]* / [-0.099, -0.069]* | 0.375 [0.216, 0.549] / [0.271, 0.482] | 0.393 [0.064, 0.723] / [0.101, 0.677] | 1.700 [0.971, 2.557] / [1.005, 2.311] | 1.394 [0.814, 1.967] / [0.989, 1.834] | 1.366 [0.781, 1.939] / [0.959, 1.806] |
| 60 |  | 0.286 [-0.008, 0.625] / [0.167, 0.407] | -0.089 [-0.121, -0.063]* / [-0.105, -0.074]* | 0.397 [0.252, 0.559] / [0.282, 0.517] | 0.099 [-0.202, 0.258] / [-0.002, 0.207] |  | 0.023 [0.000, 1.228] / [0.003, 0.048] | 0.028 [0.000, 1.324] / [0.003, 0.055] |
| 90 |  | 0.284 [-0.034, 0.691] / [0.132, 0.436] | -0.086 [-0.117, -0.060]* / [-0.105, -0.070]* | 0.507 [0.318, 0.742] / [0.350, 0.664] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.797 [0.490, 1.083] / [0.539, 1.016] | -0.523 [-0.740, -0.354]* / [-0.614, -0.437]* | -0.569 [-0.782, -0.401]* / [-0.664, -0.484]* | -0.308 [-0.578, -0.085]* / [-0.459, -0.178]* | -0.220 [-0.448, 0.029] / [-0.407, -0.052]*  ! | 2.280 [1.659, 2.986] / [1.976, 2.606] |  |  |
| 2 | 0.564 [0.272, 0.814] / [0.313, 0.817] | -0.509 [-0.671, -0.378]* / [-0.601, -0.421]* | -0.562 [-0.727, -0.427]* / [-0.654, -0.471]* | -0.364 [-0.561, -0.194]* / [-0.495, -0.234]* | -0.322 [-0.584, -0.094]* / [-0.507, -0.125]* | 2.485 [1.645, 3.535] / [2.135, 2.867] |  |  |
| 3 | 0.366 [0.026, 0.670] / [0.126, 0.601] | -0.541 [-0.734, -0.385]* / [-0.637, -0.444]* | -0.607 [-0.798, -0.454]* / [-0.703, -0.511]* | -0.331 [-0.504, -0.147]* / [-0.472, -0.188]* | -0.262 [-0.501, -0.040]* / [-0.453, -0.081]* | 2.187 [1.464, 2.971] / [1.838, 2.551] | 0.336 [0.003, 0.634] / [0.084, 0.563] | 0.317 [-0.024, 0.613] / [0.066, 0.544] |
| 4 | 0.447 [0.041, 0.820] / [0.210, 0.718] | -0.514 [-0.754, -0.336]* / [-0.612, -0.416]* | -0.563 [-0.804, -0.380]* / [-0.663, -0.461]* | -0.290 [-0.581, -0.057]* / [-0.436, -0.152]* | -0.114 [-0.431, 0.165] / [-0.310, 0.085] | 2.428 [1.669, 3.216] / [2.069, 2.775] | 0.483 [0.084, 0.851] / [0.260, 0.727] | 0.460 [0.059, 0.824] / [0.237, 0.705] |
| 5 | 0.512 [0.199, 0.792] / [0.247, 0.789] | -0.459 [-0.625, -0.317]* / [-0.550, -0.366]* | -0.513 [-0.685, -0.369]* / [-0.603, -0.425]* | -0.318 [-0.504, -0.159]* / [-0.450, -0.182]* | -0.155 [-0.435, 0.128] / [-0.352, 0.051] | 2.429 [1.556, 3.479] / [2.061, 2.814] | 0.518 [0.210, 0.783] / [0.276, 0.756] | 0.494 [0.183, 0.750] / [0.254, 0.730] |
| 6 | 0.565 [0.246, 0.881] / [0.295, 0.803] | -0.535 [-0.769, -0.362]* / [-0.627, -0.443]* | -0.590 [-0.813, -0.416]* / [-0.681, -0.501]* | -0.471 [-0.782, -0.216]* / [-0.631, -0.313]* | -0.118 [-0.379, 0.145] / [-0.306, 0.066] | 2.309 [1.611, 3.036] / [1.985, 2.646] | 0.619 [0.235, 0.933] / [0.387, 0.850] | 0.598 [0.213, 0.914] / [0.367, 0.828] |
| 7 |  | -0.392 [-0.557, -0.257]* / [-0.493, -0.295]* | -0.460 [-0.625, -0.326]* / [-0.561, -0.362]* | -0.163 [-0.359, 0.014] / [-0.297, -0.021]*  ! | -0.132 [-0.415, 0.116] / [-0.324, 0.078] | 2.571 [1.799, 3.557] / [2.218, 2.947] | 0.484 [0.095, 0.806] / [0.221, 0.758] | 0.464 [0.077, 0.789] / [0.199, 0.740] |
| 10 |  | -0.368 [-0.506, -0.238]* / [-0.461, -0.272]* | -0.438 [-0.588, -0.301]* / [-0.532, -0.340]* | -0.087 [-0.287, 0.095] / [-0.230, 0.058] | 0.091 [-0.178, 0.343] / [-0.108, 0.300] | 2.655 [1.795, 3.683] / [2.279, 3.053] | 0.732 [0.382, 1.040] / [0.485, 0.992] | 0.712 [0.364, 1.024] / [0.464, 0.974] |
| 14 |  | -0.334 [-0.534, -0.169]* / [-0.440, -0.230]* | -0.402 [-0.599, -0.238]* / [-0.511, -0.301]* | 0.038 [-0.215, 0.247] / [-0.112, 0.194] | 0.262 [-0.097, 0.572] / [0.048, 0.472] | 2.578 [1.737, 3.457] / [2.177, 2.947] | 0.891 [0.396, 1.319] / [0.622, 1.175] | 0.868 [0.374, 1.296] / [0.599, 1.152] |
| 21 |  | -0.290 [-0.478, -0.126]* / [-0.394, -0.180]* | -0.362 [-0.558, -0.197]* / [-0.469, -0.252]* | 0.061 [-0.162, 0.251] / [-0.093, 0.211] | 0.314 [-0.016, 0.604] / [0.103, 0.508] | 2.489 [1.711, 3.286] / [2.102, 2.895] | 1.068 [0.581, 1.525] / [0.803, 1.339] | 1.045 [0.545, 1.504] / [0.779, 1.318] |
| 30 |  | -0.237 [-0.423, -0.053]* / [-0.350, -0.126]* | -0.319 [-0.517, -0.129]* / [-0.431, -0.210]* | 0.205 [-0.043, 0.425] / [0.057, 0.367] | 0.238 [-0.091, 0.524] / [-0.013, 0.498] | 2.624 [1.675, 3.606] / [2.182, 3.079] | 1.009 [0.650, 1.372] / [0.700, 1.335] | 0.983 [0.618, 1.346] / [0.676, 1.307] |
| 45 |  | -0.221 [-0.462, -0.009]* / [-0.336, -0.113]* | -0.300 [-0.554, -0.077]* / [-0.416, -0.193]* | 0.141 [-0.190, 0.425] / [-0.020, 0.295] | 0.174 [-0.361, 0.667] / [-0.166, 0.492] | 0.665 [-0.227, 1.818] / [-0.137, 1.444] | 1.188 [0.621, 1.735] / [0.762, 1.601] | 1.160 [0.584, 1.715] / [0.730, 1.574] |
| 60 |  | -0.286 [-0.625, 0.008] / [-0.407, -0.167]*  ! | -0.370 [-0.725, -0.077]* / [-0.493, -0.248]* | 0.085 [-0.318, 0.478] / [-0.104, 0.272] | 0.097 [0.088, 0.229] / [-0.027, 0.187] |  | 0.044 [0.000, 1.689] / [-0.084, 0.169] | 0.049 [0.000, 1.785] / [-0.083, 0.181] |
| 90 |  | -0.284 [-0.691, 0.034] / [-0.436, -0.132]*  ! | -0.369 [-0.796, -0.045]* / [-0.519, -0.224]* | 0.203 [-0.300, 0.666] / [-0.033, 0.439] |  |  |  |  |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 13.44 [12.99, 13.90] | 14.50 [13.98, 14.99] | 13.07 [12.68, 13.46] | 14.99 [14.55, 15.50] | 13.85 [13.44, 14.28] | 18.37 [17.43, 19.36] | 13.85 [13.43, 14.26] | 13.22 [12.69, 13.68] | 9.59 [9.37, 9.81] | 10.51 [10.06, 10.96] |  |  | 16.51 [15.95, 17.08] | 14.42 [13.95, 14.89] | 13.67 [13.15, 14.16] | 13.23 [12.80, 13.62] | 14.54 [14.00, 15.06] | 24.80 [23.93, 25.69] | 14.62 [14.08, 15.14] | 13.25 [12.83, 13.67] | 13.99 [13.58, 14.43] |
| 2 | 14.06 [13.59, 14.55] | 15.33 [14.79, 15.85] | 13.65 [13.22, 14.07] | 15.53 [15.06, 16.02] | 14.41 [13.96, 14.86] | 19.87 [18.78, 21.03] | 14.34 [13.87, 14.78] | 13.76 [13.23, 14.30] | 9.58 [9.36, 9.80] | 10.94 [10.45, 11.41] |  |  | 16.70 [16.07, 17.28] | 15.16 [14.67, 15.65] | 14.38 [13.87, 14.90] | 13.77 [13.33, 14.18] | 15.43 [14.94, 15.95] | 25.26 [24.42, 26.22] | 15.51 [15.02, 16.04] | 13.96 [13.50, 14.41] | 14.56 [14.11, 15.02] |
| 3 | 14.49 [13.98, 15.04] | 15.65 [15.12, 16.21] | 13.97 [13.53, 14.47] | 15.67 [15.10, 16.24] | 14.48 [13.99, 15.02] | 19.08 [18.07, 20.32] | 14.64 [14.14, 15.19] | 14.10 [13.54, 14.77] | 9.75 [9.51, 9.99] | 11.22 [10.69, 11.81] | 15.33 [14.74, 15.99] | 15.28 [14.69, 15.93] | 16.50 [15.88, 17.14] | 15.51 [15.00, 16.06] | 14.79 [14.24, 15.39] | 14.06 [13.62, 14.53] | 15.93 [15.41, 16.48] | 25.16 [24.29, 26.13] | 16.02 [15.48, 16.57] | 14.19 [13.73, 14.69] | 14.68 [14.18, 15.23] |
| 4 | 14.78 [14.26, 15.32] | 16.18 [15.59, 16.75] | 14.02 [13.54, 14.49] | 15.49 [14.95, 16.07] | 14.35 [13.88, 14.85] | 19.41 [18.35, 20.44] | 14.52 [14.06, 14.99] | 14.23 [13.67, 14.82] | 9.73 [9.50, 9.98] | 11.14 [10.67, 11.64] | 15.42 [14.86, 15.98] | 15.36 [14.79, 15.91] | 16.66 [16.09, 17.25] | 15.93 [15.38, 16.48] | 15.12 [14.51, 15.72] | 14.08 [13.62, 14.56] | 16.34 [15.76, 16.93] | 25.62 [24.63, 26.60] | 16.44 [15.86, 17.04] | 14.35 [13.83, 14.84] | 14.50 [14.02, 15.00] |
| 5 | 14.71 [14.12, 15.24] | 16.00 [15.39, 16.58] | 14.06 [13.54, 14.55] | 15.48 [14.98, 16.01] | 14.55 [14.04, 15.07] | 19.56 [18.38, 20.94] | 14.55 [14.02, 15.07] | 14.07 [13.40, 14.73] | 9.63 [9.40, 9.86] | 11.18 [10.64, 11.72] | 15.44 [14.78, 16.11] | 15.37 [14.71, 16.03] | 16.81 [16.08, 17.58] | 15.89 [15.30, 16.45] | 15.08 [14.47, 15.72] | 14.09 [13.58, 14.58] | 16.15 [15.52, 16.75] | 25.23 [24.25, 26.20] | 16.22 [15.59, 16.84] | 14.33 [13.78, 14.83] | 14.72 [14.20, 15.24] |
| 6 | 14.24 [13.81, 14.66] | 15.45 [14.97, 15.89] | 13.48 [13.09, 13.87] | 15.32 [14.80, 15.86] | 14.12 [13.70, 14.58] | 18.80 [17.87, 19.78] | 13.88 [13.45, 14.31] | 13.80 [13.29, 14.29] | 9.62 [9.40, 9.82] | 10.43 [9.97, 10.88] | 15.12 [14.57, 15.60] | 15.06 [14.52, 15.55] | 16.45 [15.89, 17.03] | 15.28 [14.80, 15.71] | 14.30 [13.81, 14.76] | 13.58 [13.20, 13.96] | 15.66 [15.14, 16.18] | 24.35 [23.45, 25.20] | 15.74 [15.20, 16.25] | 13.75 [13.32, 14.17] | 14.29 [13.86, 14.74] |
| 7 | 15.13 [14.62, 15.63] | 16.34 [15.82, 16.87] | 14.35 [13.90, 14.80] | 15.75 [15.26, 16.29] | 14.88 [14.42, 15.34] | 20.21 [19.05, 21.45] | 15.04 [14.55, 15.52] | 14.56 [13.98, 15.19] | 9.70 [9.47, 9.94] | 11.77 [11.24, 12.27] | 15.76 [15.18, 16.37] | 15.70 [15.12, 16.32] |  | 16.24 [15.74, 16.75] | 15.56 [15.01, 16.10] | 14.45 [13.99, 14.90] | 16.70 [16.16, 17.26] | 25.19 [24.17, 26.29] | 16.77 [16.23, 17.34] | 14.82 [14.33, 15.32] | 15.06 [14.58, 15.54] |
| 10 |  |  | 14.65 [14.17, 15.13] | 15.70 [15.19, 16.21] | 14.88 [14.38, 15.38] | 20.29 [19.09, 21.61] | 15.27 [14.74, 15.81] | 14.97 [14.22, 15.67] | 9.69 [9.46, 9.93] | 12.06 [11.50, 12.65] | 16.43 [15.69, 17.16] | 16.37 [15.63, 17.10] |  |  |  | 14.64 [14.16, 15.15] |  |  |  | 15.20 [14.62, 15.81] | 15.09 [14.59, 15.62] |
| 14 |  |  | 14.82 [14.33, 15.34] | 15.94 [15.38, 16.54] | 15.07 [14.55, 15.60] | 20.18 [19.08, 21.21] | 15.56 [15.04, 16.09] | 15.32 [14.73, 15.94] | 9.83 [9.60, 10.08] | 12.26 [11.71, 12.83] | 16.74 [16.11, 17.40] | 16.68 [16.05, 17.33] |  |  |  | 14.83 [14.35, 15.33] |  |  |  | 15.34 [14.80, 15.89] | 15.26 [14.75, 15.78] |
| 21 |  |  | 14.94 [14.48, 15.40] | 15.92 [15.35, 16.49] | 15.14 [14.66, 15.68] | 20.07 [19.03, 21.14] | 15.59 [15.11, 16.14] | 15.57 [15.01, 16.09] | 9.84 [9.61, 10.06] | 12.36 [11.81, 12.95] | 17.32 [16.64, 17.96] | 17.26 [16.57, 17.89] |  |  |  | 14.95 [14.48, 15.41] |  |  |  | 15.55 [15.02, 16.09] | 15.35 [14.87, 15.90] |
| 30 |  |  | 15.29 [14.80, 15.78] | 16.04 [15.53, 16.59] | 15.39 [14.86, 15.91] | 21.04 [19.61, 22.66] | 16.15 [15.58, 16.71] | 16.18 [15.39, 16.98] | 9.81 [9.58, 10.05] | 13.09 [12.47, 13.72] | 18.00 [17.19, 18.83] | 17.93 [17.12, 18.75] |  |  |  | 15.44 [14.95, 15.94] |  |  |  | 16.12 [15.52, 16.72] | 15.61 [15.08, 16.13] |
| 45 |  |  | 15.62 [15.11, 16.20] | 16.34 [15.80, 16.95] | 15.63 [15.05, 16.19] | 16.69 [15.37, 17.92] | 16.27 [15.67, 16.88] | 17.00 [15.92, 18.33] | 9.84 [9.59, 10.10] | 13.16 [12.51, 13.83] | 19.22 [18.05, 20.49] | 19.13 [17.99, 20.42] |  |  |  | 15.86 [15.36, 16.41] |  |  |  | 16.43 [15.84, 17.07] | 15.85 [15.28, 16.43] |
| 60 |  |  | 15.77 [15.28, 16.31] | 16.69 [16.12, 17.30] | 15.81 [15.27, 16.41] |  | 16.42 [15.79, 17.03] |  | 9.85 [9.58, 10.14] | 13.35 [12.67, 14.01] |  |  |  |  |  | 15.96 [15.43, 16.51] |  |  |  | 16.53 [15.90, 17.19] | 16.03 [15.51, 16.62] |
| 90 |  |  | 16.63 [16.01, 17.24] | 17.40 [16.73, 18.14] | 16.79 [16.15, 17.45] |  | 17.53 [16.75, 18.30] |  | 9.90 [9.62, 10.21] | 14.49 [13.63, 15.30] |  |  |  |  |  | 17.05 [16.37, 17.71] |  |  |  | 17.53 [16.78, 18.32] | 17.00 [16.34, 17.67] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.62 [-1.06, -0.24]* | 0.44 [-0.08, 0.91] | -0.93 [-1.28, -0.63]* | 1.07 [0.66, 1.66] | -0.14 [-0.22, -0.08]* | 4.75 [2.88, 6.82] | -0.04 [-0.31, 0.18] | -0.42 [-0.93, -0.00]* | -4.41 [-5.13, -3.67]* | -3.38 [-3.73, -3.00]* |  |  | 2.50 [1.91, 2.99] | 0.36 [-0.12, 0.77] | -0.38 [-0.77, -0.00]* | -0.76 [-1.09, -0.50]* | 0.49 [0.01, 0.96] | 10.87 [7.76, 13.85] | 0.57 [0.08, 1.04] | -0.74 [-1.04, -0.44]* | wt_med |
| 2 | -0.58 [-1.21, -0.09]* | 0.69 [-0.04, 1.33] | -0.92 [-1.39, -0.57]* | 1.02 [0.72, 1.42] | -0.15 [-0.22, -0.10]* | 5.51 [3.10, 8.49] | -0.17 [-0.40, 0.04] | -0.39 [-0.98, 0.01] | -4.99 [-5.93, -4.06]* | -3.56 [-3.95, -3.21]* |  |  | 2.16 [1.44, 2.73] | 0.54 [-0.10, 1.09] | -0.24 [-0.71, 0.13] | -0.80 [-1.21, -0.50]* | 0.81 [0.18, 1.32] | 10.77 [7.45, 13.87] | 0.88 [0.26, 1.40] | -0.60 [-0.96, -0.28]* | wt_med |
| 3 | -0.25 [-0.87, 0.18] | 0.90 [0.17, 1.48] | -0.70 [-1.14, -0.36]* | 1.06 [0.67, 1.57] | -0.19 [-0.28, -0.12]* | 4.74 [2.91, 6.66] | 0.02 [-0.27, 0.27] | -0.13 [-0.58, 0.23] | -4.93 [-5.90, -3.96]* | -3.38 [-3.75, -2.96]* | 1.11 [0.50, 1.61] | 1.05 [0.45, 1.56] | 1.84 [1.20, 2.34] | 0.78 [0.20, 1.21] | 0.08 [-0.49, 0.50] | -0.62 [-1.03, -0.31]* | 1.19 [0.62, 1.65] | 10.54 [7.72, 13.14] | 1.28 [0.71, 1.76] | -0.48 [-0.87, -0.15]* | wt_med |
| 4 | 0.20 [-0.60, 0.78] | 1.60 [0.62, 2.36] | -0.48 [-0.89, -0.19]* | 1.08 [0.63, 1.70] | -0.13 [-0.19, -0.09]* | 5.35 [3.14, 7.42] | 0.10 [-0.18, 0.34] | 0.23 [-0.37, 0.69] | -4.77 [-5.79, -3.82]* | -3.26 [-3.64, -2.79]* | 1.45 [0.80, 2.01] | 1.38 [0.73, 1.94] | 2.12 [1.32, 2.75] | 1.37 [0.53, 2.00] | 0.57 [-0.13, 1.13] | -0.41 [-0.81, -0.12]* | 1.76 [0.99, 2.36] | 11.19 [7.88, 14.18] | 1.85 [1.08, 2.45] | -0.14 [-0.59, 0.23] | wt_med |
| 5 | -0.12 [-0.71, 0.40] | 1.18 [0.30, 1.86] | -0.67 [-0.98, -0.48]* | 0.88 [0.54, 1.26] | -0.17 [-0.23, -0.11]* | 5.31 [2.93, 8.24] | -0.09 [-0.37, 0.19] | 0.04 [-0.56, 0.72] | -5.10 [-6.13, -4.10]* | -3.44 [-3.82, -3.05]* | 1.41 [0.62, 2.17] | 1.34 [0.56, 2.07] | 2.12 [1.30, 2.83] | 1.07 [0.27, 1.76] | 0.27 [-0.33, 0.78] | -0.64 [-0.93, -0.44]* | 1.32 [0.61, 1.99] | 10.55 [7.13, 13.70] | 1.39 [0.67, 2.08] | -0.40 [-0.76, -0.12]* | wt_med |
| 6 | -0.11 [-0.79, 0.46] | 1.10 [0.28, 1.73] | -0.80 [-1.25, -0.46]* | 1.07 [0.60, 1.71] | -0.15 [-0.23, -0.09]* | 4.78 [3.00, 6.81] | -0.35 [-0.67, -0.08]* | -0.14 [-0.70, 0.31] | -4.68 [-5.47, -3.88]* | -3.78 [-4.18, -3.38]* | 1.24 [0.47, 1.86] | 1.18 [0.40, 1.80] | 2.12 [1.48, 2.65] | 0.93 [0.20, 1.47] | -0.04 [-0.50, 0.36] | -0.71 [-1.11, -0.40]* | 1.30 [0.67, 1.81] | 10.14 [6.92, 13.05] | 1.38 [0.73, 1.91] | -0.53 [-0.93, -0.19]* | wt_med |
| 7 | -0.01 [-0.71, 0.50] | 1.20 [0.32, 1.85] | -0.72 [-1.17, -0.43]* | 0.76 [0.41, 1.19] | -0.18 [-0.24, -0.13]* | 5.41 [3.19, 8.18] | 0.09 [-0.19, 0.35] | -0.13 [-0.67, 0.29] | -5.37 [-6.35, -4.37]* | -3.16 [-3.61, -2.74]* | 1.10 [0.34, 1.76] | 1.04 [0.29, 1.70] |  | 1.12 [0.31, 1.74] | 0.44 [-0.14, 0.88] | -0.62 [-0.98, -0.38]* | 1.57 [0.78, 2.24] | 10.23 [7.00, 13.32] | 1.65 [0.84, 2.33] | -0.25 [-0.65, 0.05] | wt_med |
| 10 |  |  | -0.44 [-0.75, -0.21]* | 0.71 [0.39, 1.01] | -0.21 [-0.28, -0.15]* | 5.51 [3.11, 8.19] | 0.28 [-0.12, 0.70] | 0.45 [-0.18, 0.95] | -5.40 [-6.43, -4.40]* | -2.90 [-3.40, -2.35]* | 1.86 [1.12, 2.46] | 1.80 [1.06, 2.40] |  |  |  | -0.45 [-0.71, -0.25]* |  |  |  | 0.11 [-0.28, 0.48] | wt_med |
| 14 |  |  | -0.43 [-0.85, -0.15]* | 0.73 [0.28, 1.23] | -0.17 [-0.24, -0.12]* | 5.37 [3.00, 7.59] | 0.40 [0.13, 0.65] | 0.63 [0.12, 1.10] | -5.44 [-6.59, -4.34]* | -2.88 [-3.27, -2.41]* | 2.09 [1.28, 2.91] | 2.03 [1.25, 2.85] |  |  |  | -0.42 [-0.73, -0.19]* |  |  |  | 0.08 [-0.28, 0.42] | wt_med |
| 21 |  |  | -0.41 [-0.88, -0.06]* | 0.61 [0.16, 1.09] | -0.20 [-0.28, -0.14]* | 4.94 [2.79, 7.00] | 0.31 [0.09, 0.49] | 0.46 [-0.01, 0.94] | -5.52 [-6.56, -4.54]* | -2.90 [-3.19, -2.55]* | 2.15 [1.29, 3.19] | 2.09 [1.22, 3.15] |  |  |  | -0.41 [-0.78, -0.09]* |  |  |  | 0.18 [-0.29, 0.75] | wt_med |
| 30 |  |  | -0.31 [-0.77, -0.00]* | 0.46 [-0.02, 0.91] | -0.21 [-0.28, -0.15]* | 5.18 [2.75, 7.68] | 0.63 [0.24, 1.02] | 0.35 [-0.26, 0.88] | -5.80 [-6.88, -4.84]* | -2.42 [-3.01, -1.83]* | 2.06 [1.08, 3.02] | 1.98 [1.00, 2.97] |  |  |  | -0.17 [-0.53, 0.10] |  |  |  | 0.52 [0.10, 0.93] | wt_med |
| 45 |  |  | -0.22 [-0.59, 0.04] | 0.52 [0.02, 1.05] | -0.21 [-0.26, -0.16]* | 2.51 [1.02, 4.03] | 0.50 [0.11, 0.93] | 0.23 [-0.61, 1.11] | -6.04 [-7.00, -5.11]* | -2.58 [-3.18, -1.87]* | 2.64 [1.17, 3.92] | 2.55 [1.07, 3.85] |  |  |  | 0.01 [-0.33, 0.31] |  |  |  | 0.59 [0.14, 1.09] | wt_med |
| 60 |  |  | -0.26 [-0.63, 0.06] | 0.67 [-0.05, 1.42] | -0.21 [-0.27, -0.16]* |  | 0.43 [0.08, 0.85] |  | -6.20 [-7.16, -5.25]* | -2.60 [-3.25, -1.80]* |  |  |  |  |  | -0.07 [-0.41, 0.24] |  |  |  | 0.49 [-0.00, 1.09] | wt_med |
| 90 |  |  | -0.36 [-0.79, 0.03] | 0.53 [-0.25, 1.51] | -0.20 [-0.25, -0.15] |  | 0.66 [0.35, 1.00] |  | -7.12 [-8.25, -5.96]* | -2.34 [-2.85, -1.72]* |  |  |  |  |  | 0.06 [-0.25, 0.41] |  |  |  | 0.54 [-0.08, 1.37] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -2.504 [-2.988, -1.909]* / [-3.037, -1.958]* | -1.384 [-2.004, -0.589]* / [-1.973, -0.740]* | -2.633 [-3.147, -2.048]* / [-3.164, -2.086]* | -2.590 [-3.010, -2.123]* / [-3.089, -2.129]* | -2.603 [-3.161, -2.054]* / [-3.322, -1.961]* | 2.906 [1.165, 4.658] / [1.833, 3.932] |  |  |
| 2 | -2.159 [-2.733, -1.442]* / [-2.708, -1.631]* | -1.002 [-1.500, -0.363]* / [-1.589, -0.434]* | -2.292 [-2.866, -1.563]* / [-2.839, -1.758]* | -2.296 [-2.803, -1.740]* / [-2.801, -1.836]* | -2.393 [-3.095, -1.733]* / [-3.026, -1.791]* | 3.839 [1.814, 5.923] / [2.718, 5.010] |  |  |
| 3 | -1.836 [-2.336, -1.195]* / [-2.267, -1.367]* | -0.746 [-1.453, 0.112] / [-1.298, -0.140]*  ! | -2.011 [-2.519, -1.345]* / [-2.460, -1.539]* | -1.795 [-2.139, -1.353]* / [-2.160, -1.404]* | -1.679 [-2.207, -1.080]* / [-2.261, -1.121]* | 3.406 [1.724, 5.236] / [2.403, 4.499] | -0.499 [-1.387, 0.427] / [-1.181, 0.171] | -0.554 [-1.437, 0.370] / [-1.230, 0.115] |
| 4 | -2.116 [-2.747, -1.319]* / [-2.636, -1.635]* | -0.875 [-1.706, 0.153] / [-1.495, -0.301]*  ! | -2.229 [-2.866, -1.426]* / [-2.748, -1.758]* | -2.063 [-2.603, -1.394]* / [-2.498, -1.671]* | -2.083 [-2.814, -1.318]* / [-2.608, -1.492]* | 3.266 [1.669, 4.778] / [2.271, 4.269] | -0.861 [-1.776, 0.092] / [-1.492, -0.249]*  ! | -0.926 [-1.861, 0.037] / [-1.563, -0.310]*  ! |
| 5 | -2.122 [-2.827, -1.298]* / [-2.740, -1.544]* | -1.058 [-1.687, -0.348]* / [-1.724, -0.420]* | -2.271 [-2.957, -1.464]* / [-2.888, -1.710]* | -2.185 [-2.684, -1.513]* / [-2.725, -1.638]* | -1.988 [-2.557, -1.375]* / [-2.709, -1.350]* | 3.645 [1.700, 5.778] / [2.530, 4.772] | -0.491 [-1.158, 0.221] / [-1.260, 0.194] | -0.562 [-1.231, 0.160] / [-1.326, 0.117] |
| 6 | -2.116 [-2.647, -1.478]* / [-2.666, -1.552]* | -0.920 [-1.609, 0.006] / [-1.543, -0.255]*  ! | -2.247 [-2.785, -1.602]* / [-2.793, -1.686]* | -2.481 [-2.888, -2.027]* / [-2.980, -2.010]* | -2.017 [-2.559, -1.453]* / [-2.706, -1.397]* | 3.230 [1.487, 4.960] / [2.118, 4.305] | -0.546 [-1.318, 0.190] / [-1.265, 0.103] | -0.606 [-1.410, 0.145] / [-1.321, 0.040] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 2.504 [1.909, 2.988] / [1.958, 3.037] | 1.071 [0.659, 1.658] / [0.853, 1.290] | -0.143 [-0.220, -0.077]* / [-0.176, -0.110]* | -0.041 [-0.311, 0.181] / [-0.276, 0.189] | -0.425 [-0.935, -0.005]* / [-0.805, -0.058]* | 4.750 [2.879, 6.819] / [3.927, 5.601] |  |  |
| 2 | 2.159 [1.442, 2.733] / [1.631, 2.708] | 1.019 [0.715, 1.420] / [0.803, 1.244] | -0.154 [-0.217, -0.101]* / [-0.192, -0.116]* | -0.170 [-0.402, 0.037] / [-0.413, 0.054] | -0.394 [-0.984, 0.012] / [-0.799, 0.004] | 5.512 [3.097, 8.489] / [4.596, 6.536] |  |  |
| 3 | 1.836 [1.195, 2.336] / [1.367, 2.267] | 1.060 [0.673, 1.566] / [0.839, 1.287] | -0.187 [-0.281, -0.118]* / [-0.231, -0.143]* | 0.024 [-0.268, 0.270] / [-0.219, 0.247] | -0.132 [-0.582, 0.226] / [-0.543, 0.251] | 4.740 [2.905, 6.659] / [3.858, 5.704] | 1.106 [0.505, 1.614] / [0.605, 1.598] | 1.052 [0.450, 1.555] / [0.549, 1.545] |
| 4 | 2.116 [1.319, 2.747] / [1.635, 2.636] | 1.084 [0.632, 1.697] / [0.849, 1.311] | -0.133 [-0.189, -0.087]* / [-0.172, -0.096]* | 0.105 [-0.184, 0.345] / [-0.133, 0.350] | 0.234 [-0.368, 0.693] / [-0.155, 0.630] | 5.349 [3.136, 7.415] / [4.425, 6.216] | 1.448 [0.801, 2.012] / [1.025, 1.943] | 1.385 [0.726, 1.939] / [0.960, 1.880] |
| 5 | 2.122 [1.298, 2.827] / [1.544, 2.740] | 0.878 [0.543, 1.258] / [0.669, 1.082] | -0.168 [-0.228, -0.114]* / [-0.211, -0.132]* | -0.089 [-0.370, 0.191] / [-0.328, 0.137] | 0.042 [-0.562, 0.719] / [-0.380, 0.467] | 5.311 [2.934, 8.236] / [4.299, 6.270] | 1.408 [0.621, 2.166] / [0.886, 1.927] | 1.339 [0.559, 2.074] / [0.817, 1.856] |
| 6 | 2.116 [1.478, 2.647] / [1.552, 2.666] | 1.070 [0.605, 1.709] / [0.831, 1.297] | -0.154 [-0.229, -0.093]* / [-0.190, -0.118]* | -0.348 [-0.674, -0.077]* / [-0.603, -0.107]* | -0.141 [-0.704, 0.307] / [-0.515, 0.239] | 4.781 [3.000, 6.814] / [3.981, 5.668] | 1.235 [0.467, 1.864] / [0.767, 1.688] | 1.178 [0.405, 1.803] / [0.709, 1.628] |
| 7 |  | 0.765 [0.407, 1.190] / [0.524, 0.995] | -0.178 [-0.239, -0.128]* / [-0.218, -0.139]* | 0.089 [-0.194, 0.353] / [-0.134, 0.311] | -0.135 [-0.668, 0.294] / [-0.524, 0.307] | 5.412 [3.193, 8.176] / [4.476, 6.493] | 1.099 [0.339, 1.758] / [0.603, 1.650] | 1.042 [0.290, 1.703] / [0.541, 1.591] |
| 10 |  | 0.711 [0.391, 1.009] / [0.479, 0.931] | -0.211 [-0.280, -0.151]* / [-0.256, -0.171]* | 0.285 [-0.122, 0.695] / [0.041, 0.542] | 0.449 [-0.176, 0.954] / [0.010, 0.865] | 5.514 [3.110, 8.187] / [4.483, 6.493] | 1.864 [1.121, 2.459] / [1.306, 2.404] | 1.801 [1.059, 2.404] / [1.245, 2.340] |
| 14 |  | 0.727 [0.276, 1.226] / [0.462, 0.978] | -0.175 [-0.239, -0.125]* / [-0.211, -0.138]* | 0.400 [0.129, 0.649] / [0.192, 0.631] | 0.626 [0.119, 1.105] / [0.192, 1.046] | 5.375 [3.003, 7.587] / [4.350, 6.335] | 2.095 [1.284, 2.911] / [1.555, 2.665] | 2.031 [1.247, 2.846] / [1.483, 2.605] |
| 21 |  | 0.611 [0.157, 1.091] / [0.350, 0.859] | -0.203 [-0.284, -0.138]* / [-0.241, -0.168]* | 0.306 [0.094, 0.489] / [0.089, 0.547] | 0.458 [-0.012, 0.940] / [0.047, 0.869] | 4.941 [2.790, 7.004] / [3.956, 5.969] | 2.152 [1.292, 3.193] / [1.568, 2.761] | 2.087 [1.221, 3.148] / [1.499, 2.694] |
| 30 |  | 0.457 [-0.020, 0.911] / [0.175, 0.710] | -0.207 [-0.282, -0.148]* / [-0.249, -0.166]* | 0.626 [0.240, 1.018] / [0.386, 0.878] | 0.346 [-0.263, 0.880] / [-0.235, 0.877] | 5.183 [2.747, 7.678] / [3.992, 6.386] | 2.058 [1.084, 3.025] / [1.323, 2.814] | 1.982 [1.000, 2.965] / [1.244, 2.742] |
| 45 |  | 0.525 [0.017, 1.048] / [0.282, 0.807] | -0.208 [-0.264, -0.161]* / [-0.244, -0.174]* | 0.500 [0.109, 0.927] / [0.265, 0.759] | 0.228 [-0.614, 1.111] / [-0.466, 0.912] | 2.507 [1.015, 4.029] / [1.237, 3.676] | 2.638 [1.172, 3.923] / [1.742, 3.589] | 2.554 [1.069, 3.850] / [1.645, 3.501] |
| 60 |  | 0.674 [-0.053, 1.419] / [0.402, 0.954] | -0.210 [-0.275, -0.159]* / [-0.246, -0.174]* | 0.429 [0.082, 0.850] / [0.165, 0.726] |  |  |  |  |
| 90 |  | 0.529 [-0.247, 1.513] / [0.195, 0.880] | -0.196 [-0.255, -0.145]* / [-0.238, -0.153]* | 0.659 [0.354, 0.997] / [0.304, 1.004] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.384 [0.589, 2.004] / [0.740, 1.973] | -1.071 [-1.658, -0.659]* / [-1.290, -0.853]* | -1.198 [-1.748, -0.794]* / [-1.415, -0.991]* | -1.133 [-1.861, -0.619]* / [-1.480, -0.816]* | -1.392 [-1.909, -1.001]* / [-1.850, -0.973]* | 3.869 [2.126, 5.572] / [3.015, 4.787] |  |  |
| 2 | 1.002 [0.363, 1.500] / [0.434, 1.589] | -1.019 [-1.420, -0.715]* / [-1.244, -0.803]* | -1.165 [-1.520, -0.885]* / [-1.388, -0.952]* | -1.221 [-1.711, -0.869]* / [-1.535, -0.902]* | -1.395 [-2.014, -0.905]* / [-1.842, -0.920]* | 4.605 [2.237, 7.340] / [3.633, 5.667] |  |  |
| 3 | 0.746 [-0.112, 1.453] / [0.140, 1.298] | -1.060 [-1.566, -0.673]* / [-1.287, -0.839]* | -1.242 [-1.730, -0.883]* / [-1.463, -1.024]* | -1.022 [-1.559, -0.570]* / [-1.374, -0.698]* | -1.227 [-1.764, -0.792]* / [-1.723, -0.792]* | 3.815 [1.840, 5.666] / [2.849, 4.815] | 0.054 [-0.551, 0.536] / [-0.471, 0.565] | 0.000 [-0.598, 0.490] / [-0.526, 0.508] |
| 4 | 0.875 [-0.153, 1.706] / [0.301, 1.495] | -1.084 [-1.697, -0.632]* / [-1.311, -0.849]* | -1.215 [-1.795, -0.768]* / [-1.437, -0.984]* | -1.019 [-1.735, -0.465]* / [-1.372, -0.687]* | -0.839 [-1.682, -0.161]* / [-1.319, -0.340]* | 4.376 [2.197, 6.276] / [3.440, 5.362] | 0.377 [-0.516, 1.082] / [-0.140, 0.911] | 0.314 [-0.614, 1.041] / [-0.202, 0.851] |
| 5 | 1.058 [0.348, 1.687] / [0.420, 1.724] | -0.878 [-1.258, -0.543]* / [-1.082, -0.669]* | -1.038 [-1.398, -0.720]* / [-1.246, -0.832]* | -0.974 [-1.329, -0.629]* / [-1.276, -0.658]* | -0.953 [-1.490, -0.467]* / [-1.414, -0.471]* | 4.417 [2.020, 7.125] / [3.417, 5.425] | 0.447 [-0.133, 0.995] / [-0.102, 0.999] | 0.377 [-0.182, 0.911] / [-0.176, 0.929] |
| 6 | 0.920 [-0.006, 1.609] / [0.255, 1.543] | -1.070 [-1.709, -0.605]* / [-1.297, -0.831]* | -1.213 [-1.812, -0.775]* / [-1.444, -0.976]* | -1.473 [-2.339, -0.866]* / [-1.865, -1.097]* | -1.202 [-1.784, -0.764]* / [-1.615, -0.775]* | 3.873 [2.116, 5.625] / [3.008, 4.866] | 0.224 [-0.544, 0.865] / [-0.274, 0.719] | 0.165 [-0.617, 0.807] / [-0.334, 0.663] |
| 7 |  | -0.765 [-1.190, -0.407]* / [-0.995, -0.524]* | -0.935 [-1.327, -0.605]* / [-1.166, -0.690]* | -0.710 [-1.192, -0.257]* / [-1.049, -0.378]* | -1.029 [-1.621, -0.566]* / [-1.469, -0.525]* | 4.554 [2.465, 6.985] / [3.591, 5.635] | 0.242 [-0.583, 0.928] / [-0.345, 0.844] | 0.184 [-0.634, 0.877] / [-0.398, 0.794] |
| 10 |  | -0.711 [-1.009, -0.391]* / [-0.931, -0.479]* | -0.903 [-1.212, -0.591]* / [-1.122, -0.686]* | -0.411 [-0.837, 0.002] / [-0.741, -0.069]*  ! | -0.431 [-1.010, 0.041] / [-0.911, 0.058] | 4.788 [2.412, 7.416] / [3.733, 5.848] | 1.005 [0.352, 1.538] / [0.436, 1.606] | 0.941 [0.298, 1.465] / [0.375, 1.543] |
| 14 |  | -0.727 [-1.226, -0.276]* / [-0.978, -0.462]* | -0.889 [-1.369, -0.458]* / [-1.136, -0.638]* | -0.350 [-0.986, 0.157] / [-0.688, 0.024] | -0.103 [-0.828, 0.464] / [-0.603, 0.373] | 4.641 [2.419, 6.899] / [3.565, 5.710] | 1.338 [0.415, 2.249] / [0.740, 1.935] | 1.274 [0.345, 2.184] / [0.676, 1.875] |
| 21 |  | -0.611 [-1.091, -0.157]* / [-0.859, -0.350]* | -0.793 [-1.276, -0.349]* / [-1.038, -0.537]* | -0.301 [-0.917, 0.236] / [-0.667, 0.061] | -0.123 [-0.924, 0.605] / [-0.637, 0.377] | 4.380 [2.283, 6.287] / [3.292, 5.486] | 1.561 [0.456, 2.625] / [0.943, 2.209] | 1.495 [0.372, 2.580] / [0.866, 2.147] |
| 30 |  | -0.457 [-0.911, 0.020] / [-0.710, -0.175]*  ! | -0.647 [-1.117, -0.168]* / [-0.904, -0.364]* | 0.160 [-0.471, 0.764] / [-0.198, 0.558] | -0.221 [-0.973, 0.497] / [-0.831, 0.402] | 4.629 [2.201, 7.046] / [3.380, 5.903] | 1.504 [0.588, 2.441] / [0.791, 2.301] | 1.427 [0.497, 2.381] / [0.710, 2.233] |
| 45 |  | -0.525 [-1.048, -0.017]* / [-0.807, -0.282]* | -0.720 [-1.266, -0.218]* / [-1.006, -0.479]* | -0.051 [-0.831, 0.639] / [-0.437, 0.311] | -0.516 [-1.686, 0.746] / [-1.320, 0.251] | 0.826 [-1.304, 3.056] / [-0.603, 2.258] | 1.926 [0.489, 3.348] / [0.926, 2.852] | 1.842 [0.369, 3.293] / [0.831, 2.771] |
| 60 |  | -0.674 [-1.419, 0.053] / [-0.954, -0.402]*  ! | -0.870 [-1.614, -0.152]* / [-1.159, -0.596]* | -0.301 [-1.218, 0.708] / [-0.757, 0.141] |  |  |  |  |
| 90 |  | -0.529 [-1.513, 0.247] / [-0.880, -0.195]*  ! | -0.714 [-1.757, 0.080] / [-1.062, -0.368]*  ! | 0.067 [-1.047, 0.992] / [-0.494, 0.615] |  |  |  |  |

**MAE by region (all rides)**

| lead | region | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 8.53 | 9.02 | 8.39 | 9.36 | 8.51 | 12.60 | 8.83 | 8.41 | 6.04 | 7.33 |  |  | 10.14 | 9.08 | 8.66 | 8.54 | 9.13 | 16.00 | 9.19 | 8.51 | 8.55 |
| 1 | EU | 4.68 | 5.08 | 4.82 | 5.27 | 4.90 | 7.03 | 5.10 | 5.03 | 3.71 | 4.10 |  |  | 5.74 | 5.09 | 4.81 | 4.86 | 5.07 | 7.62 | 5.10 | 4.99 | 4.94 |
| 1 | NA | 7.81 | 8.35 | 7.69 | 8.35 | 7.87 | 9.74 | 8.09 | 7.73 | 6.15 | 5.97 |  |  | 9.79 | 8.21 | 7.77 | 7.76 | 8.35 | 12.66 | 8.38 | 7.75 | 7.95 |
| 3 | Asia | 9.35 | 9.87 | 9.09 | 9.88 | 9.18 | 13.43 | 9.65 | 9.28 | 6.29 | 8.03 | 9.73 | 9.69 | 10.29 | 9.85 | 9.48 | 9.15 | 10.15 | 16.33 | 10.20 | 9.30 | 9.27 |
| 3 | EU | 5.01 | 5.36 | 5.13 | 5.67 | 5.19 | 7.67 | 5.29 | 5.36 | 3.73 | 4.21 | 5.59 | 5.58 | 5.59 | 5.35 | 4.96 | 5.14 | 5.43 | 7.74 | 5.45 | 5.20 | 5.24 |
| 3 | NA | 8.24 | 8.89 | 8.03 | 8.53 | 7.94 | 9.64 | 8.16 | 7.95 | 6.13 | 6.20 | 9.07 | 9.05 | 9.55 | 8.68 | 8.30 | 8.05 | 8.95 | 12.83 | 8.99 | 8.08 | 8.02 |
| 7 | Asia | 9.77 | 10.49 | 9.22 | 9.87 | 9.30 | 14.10 | 9.72 | 9.58 | 6.26 | 8.20 | 9.88 | 9.84 |  | 10.53 | 10.04 | 9.29 | 10.73 | 16.41 | 10.79 | 9.65 | 9.37 |
| 7 | EU | 5.45 | 5.78 | 5.41 | 5.77 | 5.50 | 8.17 | 5.64 | 5.72 | 3.89 | 4.70 | 6.01 | 6.00 |  | 5.74 | 5.54 | 5.44 | 5.94 | 8.21 | 5.95 | 5.60 | 5.57 |
| 7 | NA | 8.61 | 9.14 | 8.30 | 8.74 | 8.26 | 10.23 | 8.52 | 8.18 | 6.13 | 6.61 | 9.34 | 9.32 |  | 9.02 | 8.64 | 8.31 | 9.17 | 12.62 | 9.20 | 8.36 | 8.33 |
| 14 | Asia |  |  | 9.63 | 10.12 | 9.57 | 13.72 | 10.06 | 9.78 | 6.26 | 8.54 | 10.35 | 10.30 |  |  |  | 9.58 |  |  |  | 10.04 | 9.65 |
| 14 | EU |  |  | 5.59 | 5.96 | 5.69 | 8.17 | 5.97 | 6.00 | 4.04 | 5.15 | 6.21 | 6.20 |  |  |  | 5.68 |  |  |  | 5.77 | 5.77 |
| 14 | NA |  |  | 8.63 | 8.81 | 8.43 | 10.82 | 8.93 | 9.10 | 6.40 | 6.96 | 10.23 | 10.22 |  |  |  | 8.60 |  |  |  | 8.71 | 8.50 |
| 30 | Asia |  |  | 9.96 | 10.10 | 9.84 | 15.48 | 10.63 | 10.62 | 6.49 | 9.37 | 11.13 | 11.07 |  |  |  | 10.09 |  |  |  | 10.68 | 9.94 |
| 30 | EU |  |  | 5.73 | 5.89 | 5.73 | 8.20 | 6.16 | 6.23 | 4.07 | 5.34 | 6.49 | 6.47 |  |  |  | 5.81 |  |  |  | 6.00 | 5.83 |
| 30 | NA |  |  | 8.81 | 9.09 | 8.57 | 10.08 | 8.92 | 8.68 | 6.20 | 7.03 | 10.33 | 10.31 |  |  |  | 8.80 |  |  |  | 8.95 | 8.69 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | tirex2 | tirex2_x_h5 | toto2 | toto2_x_h5 | toto2_nocov | tirex2_grid | toto2_grid | chronos2 | chronos2_x_h5 | chronos2_nocov | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.17 | -1.07 | -0.71 | -1.31 | -0.99 | -1.10 | -5.70 |  |  | -2.29 |  | -2.11 |  | -2.21 | -1.41 | -7.50 | -1.75 |  | -2.37 | -0.44 | -0.15 |
| 1 | 0.52 | -0.72 | -0.29 | -0.96 | -0.52 | -0.65 | -5.33 |  |  | -2.00 | -0.83 | -1.84 | -1.00 | -1.90 | -1.33 | -7.29 | -1.72 | -0.87 | -2.18 | -0.44 | 0.36 |
| 2 | 0.34 | -0.81 | -0.39 | -1.06 | -0.72 | -1.30 | -6.04 |  |  | -2.15 | -1.09 | -2.05 | -1.27 | -2.13 | -1.50 | -7.47 | -1.99 | -1.18 | -2.33 | -0.46 | 0.17 |
| 3 | 0.03 | -0.73 | -0.29 | -0.97 | -0.69 | -0.90 | -5.82 | 1.56 | 1.55 | -1.96 | -0.91 | -1.93 | -1.13 | -2.01 | -1.39 | -7.29 | -1.93 | -1.02 | -2.23 | -0.44 | 0.17 |
| 4 | -0.02 | -0.93 | -0.44 | -1.17 | -0.85 | -0.95 | -5.93 | 1.55 | 1.54 | -2.17 | -1.12 | -2.22 | -1.34 | -2.29 | -1.54 | -7.50 | -2.21 | -1.18 | -2.44 | -0.43 | 0.02 |
| 5 | -0.16 | -0.93 | -0.47 | -1.17 | -0.91 | -0.90 | -5.92 | 1.62 | 1.62 | -2.17 | -1.05 | -2.24 | -1.35 | -2.29 | -1.62 | -7.45 | -2.17 | -1.15 | -2.37 | -0.43 | -0.08 |
| 6 | 0.55 | -0.57 | -0.14 | -0.80 | -0.42 | -0.60 | -5.59 | 1.85 | 1.84 | -1.81 | -0.76 | -1.90 | -0.95 | -1.96 | -1.25 | -7.09 | -1.73 | -0.74 | -2.00 | -0.41 | 0.43 |
| 7 |  | -0.72 | -0.29 | -0.97 | -0.64 | -1.18 | -6.10 | 1.26 | 1.25 | -1.98 | -0.92 | -2.15 | -1.24 | -2.22 | -1.42 | -7.14 | -1.98 | -0.95 | -2.24 | -0.45 | 0.25 |
| 10 |  | -0.80 | -0.27 | -1.05 | -0.73 | -0.91 | -6.18 | 1.55 | 1.55 |  | -0.97 |  | -1.35 |  |  |  |  | -1.04 |  | -0.43 | 0.13 |
| 14 |  | -0.72 | -0.19 | -0.97 | -0.65 | -0.75 | -6.14 | 1.73 | 1.72 |  | -0.90 |  | -1.25 |  |  |  |  | -1.00 |  | -0.38 | 0.24 |
| 21 |  | -0.39 | 0.13 | -0.64 | -0.19 | 0.06 | -5.79 | 2.53 | 2.52 |  | -0.45 |  | -0.94 |  |  |  |  | -0.58 |  | -0.35 | 0.69 |
| 30 |  | -0.41 | 0.23 | -0.67 | -0.30 | -0.49 | -6.29 | 1.84 | 1.83 |  | -0.46 |  | -1.03 |  |  |  |  | -0.63 |  | -0.33 | 0.57 |
| 45 |  | -0.02 | 0.71 | -0.29 | 0.03 | 0.15 | -5.29 | 2.20 | 2.19 |  | 0.04 |  | -0.57 |  |  |  |  | -0.05 |  | -0.26 | 0.87 |
| 60 |  | 0.29 | 1.16 | 0.02 | 0.31 | -0.00 |  | 0.04 | 0.04 |  | 0.43 |  | -0.43 |  |  |  |  | 0.37 |  | -0.27 | 1.14 |
| 90 |  | 1.08 | 1.85 | 0.81 | 1.17 |  |  |  |  |  | 1.12 |  | 0.01 |  |  |  |  | 1.03 |  | -0.24 | 1.97 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.32 [6.98, 7.65] | 7.27 [6.93, 7.60] | 7.63 [7.29, 7.96] | 7.01 [6.67, 7.39] |  |  |
| 1 | 6.99 [6.70, 7.27] | 6.92 [6.64, 7.21] | 7.19 [6.92, 7.46] | 6.82 [6.49, 7.13] |  |  |
| 2 | 7.45 [7.17, 7.76] | 7.37 [7.09, 7.68] | 7.60 [7.31, 7.91] | 7.23 [6.89, 7.62] |  |  |
| 3 | 7.45 [7.11, 7.79] | 7.37 [7.03, 7.70] | 7.67 [7.33, 8.00] | 7.37 [7.00, 7.77] | 7.77 [7.38, 8.19] | 7.75 [7.36, 8.16] |
| 4 | 7.36 [7.06, 7.67] | 7.29 [6.99, 7.59] | 7.55 [7.25, 7.85] | 7.34 [6.99, 7.71] | 7.80 [7.44, 8.17] | 7.77 [7.42, 8.14] |
| 5 | 7.55 [7.20, 7.90] | 7.47 [7.12, 7.82] | 7.67 [7.32, 7.99] | 7.34 [6.95, 7.72] | 7.81 [7.43, 8.22] | 7.78 [7.40, 8.19] |
| 6 | 7.12 [6.82, 7.42] | 7.04 [6.74, 7.33] | 7.21 [6.92, 7.49] | 7.09 [6.73, 7.44] | 7.62 [7.24, 7.98] | 7.60 [7.22, 7.95] |
| 7 | 7.69 [7.40, 8.05] | 7.61 [7.31, 7.96] | 7.91 [7.58, 8.26] | 7.62 [7.21, 8.06] | 8.01 [7.60, 8.42] | 7.98 [7.58, 8.40] |
| 10 | 7.70 [7.33, 8.05] | 7.59 [7.23, 7.94] | 7.97 [7.61, 8.33] | 7.67 [7.25, 8.13] | 8.11 [7.69, 8.55] | 8.09 [7.67, 8.53] |
| 14 | 7.70 [7.36, 8.03] | 7.59 [7.25, 7.92] | 7.98 [7.66, 8.32] | 7.85 [7.42, 8.27] | 8.25 [7.80, 8.69] | 8.22 [7.78, 8.66] |
| 21 | 7.45 [7.11, 7.82] | 7.32 [6.98, 7.68] | 7.71 [7.35, 8.06] | 7.53 [7.09, 7.95] | 7.91 [7.46, 8.35] | 7.89 [7.44, 8.33] |
| 30 | 7.80 [7.39, 8.24] | 7.64 [7.24, 8.06] | 8.21 [7.81, 8.66] | 7.98 [7.43, 8.56] | 8.59 [8.04, 9.13] | 8.57 [8.01, 9.11] |
| 45 | 7.62 [7.18, 8.06] | 7.45 [7.02, 7.89] | 7.96 [7.51, 8.41] | 7.98 [7.22, 8.86] | 8.98 [8.19, 9.81] | 8.95 [8.16, 9.77] |
| 60 | 7.62 [7.10, 8.10] | 7.44 [6.92, 7.92] | 8.06 [7.55, 8.60] | 0.30 [0.10, 0.52] | 0.24 [0.01, 0.55] | 0.25 [0.01, 0.56] |
| 90 | 7.06 [6.52, 7.63] | 6.87 [6.34, 7.45] | 7.41 [6.80, 8.07] |  |  |  |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 6.45 [6.12, 6.82] | 6.43 [6.10, 6.80] | 6.68 [6.32, 7.05] | 6.40 [5.96, 6.90] |  |  |
| 1 | 6.75 [6.34, 7.22] | 6.73 [6.32, 7.19] | 6.95 [6.56, 7.39] | 6.56 [6.09, 7.08] |  |  |
| 2 | 6.67 [6.34, 7.00] | 6.64 [6.32, 6.98] | 6.77 [6.44, 7.09] | 6.54 [6.16, 6.98] |  |  |
| 3 | 6.82 [6.47, 7.20] | 6.77 [6.42, 7.15] | 6.90 [6.57, 7.24] | 6.76 [6.33, 7.25] | 7.90 [7.34, 8.48] | 7.88 [7.33, 8.45] |
| 4 | 7.17 [6.72, 7.66] | 7.15 [6.71, 7.65] | 7.49 [7.02, 8.03] | 7.26 [6.76, 7.78] | 8.20 [7.65, 8.84] | 8.18 [7.64, 8.82] |
| 5 | 6.68 [6.33, 7.04] | 6.64 [6.30, 7.02] | 6.73 [6.41, 7.09] | 6.73 [6.25, 7.23] | 7.84 [7.33, 8.40] | 7.82 [7.31, 8.38] |
| 6 | 7.01 [6.59, 7.53] | 6.97 [6.55, 7.49] | 7.04 [6.64, 7.48] | 7.01 [6.55, 7.54] | 8.16 [7.58, 8.75] | 8.14 [7.56, 8.72] |
| 7 | 7.03 [6.64, 7.43] | 6.99 [6.60, 7.39] | 7.19 [6.79, 7.57] | 7.05 [6.59, 7.60] | 8.15 [7.54, 8.87] | 8.13 [7.53, 8.85] |
| 10 | 7.08 [6.71, 7.48] | 7.04 [6.67, 7.44] | 7.23 [6.86, 7.62] | 7.43 [6.88, 8.01] | 8.53 [7.83, 9.33] | 8.52 [7.82, 9.31] |
| 14 | 7.86 [7.42, 8.30] | 7.83 [7.40, 8.28] | 8.38 [7.92, 8.88] | 8.24 [7.65, 8.78] | 9.30 [8.65, 9.93] | 9.28 [8.64, 9.90] |
| 21 | 8.14 [7.72, 8.67] | 8.11 [7.69, 8.63] | 8.57 [8.12, 9.11] | 8.57 [8.02, 9.18] | 10.13 [9.40, 10.93] | 10.11 [9.38, 10.91] |
| 30 | 8.10 [7.73, 8.47] | 8.05 [7.69, 8.42] | 8.55 [8.16, 8.96] | 8.52 [7.97, 9.10] | 9.53 [8.88, 10.21] | 9.50 [8.86, 10.18] |
| 45 | 8.64 [8.28, 9.06] | 8.58 [8.22, 9.00] | 8.94 [8.57, 9.36] | 9.10 [8.42, 9.82] | 10.19 [9.43, 11.05] | 10.17 [9.41, 11.03] |
| 60 | 8.77 [8.39, 9.21] | 8.70 [8.32, 9.14] | 9.04 [8.64, 9.48] | 0.30 [0.13, 0.54] | 0.11 [0.00, 0.37] | 0.11 [0.00, 0.37] |
| 90 | 9.49 [9.01, 9.98] | 9.40 [8.92, 9.89] | 10.01 [9.52, 10.53] |  |  |  |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | tirex2 | tirex2_x_h5 | toto2 | toto2_x_h5 | toto2_nocov | tirex2_grid | toto2_grid | chronos2 | chronos2_x_h5 | chronos2_nocov | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 194149.00 | 8.33 | 7.52 | 7.72 | 7.37 | 7.64 | 7.29 | 9.31 |  |  | 7.18 |  | 7.26 |  | 7.30 | 6.84 | 10.74 | 6.62 |  | 7.06 | 4.99 | 6.36 |
| 0 | spring | 412842.00 | 9.26 | 7.87 | 8.42 | 7.82 | 8.16 | 8.03 |  |  |  | 7.66 |  | 7.63 |  | 7.67 | 7.18 | 12.39 | 7.16 |  | 7.67 | 5.49 | 6.64 |
| 0 | summer | 520128.00 | 7.62 | 6.33 | 6.97 | 6.32 | 6.66 | 6.62 | 9.71 |  |  | 6.46 |  | 6.45 |  | 6.47 | 6.04 | 10.72 | 6.01 |  | 6.36 | 4.97 | 5.17 |
| 0 | winter | 29283.00 | 9.90 | 8.03 | 9.96 | 8.17 | 9.04 |  |  |  |  | 7.80 |  | 7.68 |  | 7.73 | 6.89 | 15.76 | 7.11 |  | 7.39 | 6.18 | 7.36 |
| 1 | autumn | 168352.00 | 8.19 | 7.12 | 7.30 | 6.94 | 7.11 | 6.82 | 8.91 |  |  | 7.14 | 6.50 | 7.40 | 6.53 | 7.45 | 6.67 | 11.14 | 6.53 | 6.36 | 7.22 | 4.95 | 5.79 |
| 1 | spring | 410450.00 | 9.24 | 7.48 | 8.15 | 7.42 | 7.71 | 6.89 |  |  |  | 7.91 | 7.48 | 7.99 | 7.60 | 8.03 | 7.67 | 12.81 | 7.41 | 7.40 | 7.98 | 5.48 | 6.19 |
| 1 | summer | 520161.00 | 7.42 | 6.35 | 6.83 | 6.33 | 6.61 | 6.73 | 9.37 |  |  | 6.60 | 6.36 | 6.57 | 6.34 | 6.61 | 6.18 | 10.61 | 6.24 | 6.30 | 6.55 | 4.91 | 5.06 |
| 1 | winter | 30761.00 | 10.88 | 8.54 | 9.89 | 8.60 | 8.98 |  |  |  |  | 8.85 | 8.27 | 8.69 | 8.47 | 8.71 | 8.03 | 14.90 | 8.59 | 8.13 | 9.16 | 6.22 | 7.41 |
| 3 | autumn | 165115.00 | 7.40 | 7.55 | 7.69 | 7.39 | 7.44 | 7.48 | 9.81 | 7.80 | 7.79 | 8.06 | 7.17 | 8.56 | 7.32 | 8.60 | 7.74 | 11.65 | 7.51 | 7.07 | 8.09 | 5.06 | 5.89 |
| 3 | spring | 383722.00 | 9.06 | 7.74 | 8.40 | 7.66 | 7.97 | 7.31 |  | 6.97 | 6.97 | 8.39 | 7.79 | 8.63 | 7.91 | 8.69 | 8.10 | 13.13 | 7.84 | 7.76 | 8.52 | 5.57 | 6.34 |
| 3 | summer | 541951.00 | 7.73 | 6.82 | 7.39 | 6.77 | 7.05 | 7.16 | 9.74 | 7.83 | 7.81 | 7.03 | 6.79 | 7.12 | 6.79 | 7.15 | 6.59 | 10.79 | 6.75 | 6.78 | 7.10 | 5.01 | 5.61 |
| 3 | winter | 31648.00 | 10.45 | 8.87 | 9.56 | 8.91 | 9.54 |  |  |  |  | 9.20 | 8.57 | 9.39 | 8.92 | 9.41 | 8.35 | 15.12 | 8.83 | 8.68 | 9.20 | 6.24 | 7.94 |
| 7 | autumn | 167608.00 |  | 7.75 | 7.85 | 7.57 | 7.85 | 7.46 | 9.54 | 8.07 | 8.05 | 8.12 | 7.24 | 8.58 | 7.36 | 8.62 | 7.93 | 10.80 | 7.52 | 7.09 | 8.15 | 4.88 | 6.82 |
| 7 | spring | 374310.00 |  | 8.13 | 8.49 | 8.04 | 8.21 |  |  |  |  | 8.83 | 8.05 | 8.90 | 8.43 | 8.94 | 8.78 | 12.70 | 8.22 | 7.94 | 8.84 | 5.54 | 6.74 |
| 7 | summer | 537814.00 |  | 7.01 | 7.45 | 6.97 | 7.33 | 7.50 | 10.56 | 8.03 | 8.01 | 7.57 | 7.05 | 7.75 | 7.12 | 7.77 | 7.00 | 11.64 | 7.27 | 7.09 | 7.65 | 5.20 | 5.84 |
| 7 | winter | 13722.00 |  | 9.46 | 10.56 | 9.52 | 9.86 |  |  |  |  | 10.91 | 9.66 | 11.07 | 9.61 | 11.12 | 9.90 | 15.24 | 9.59 | 9.62 | 10.50 | 6.22 | 8.13 |
| 14 | autumn | 190613.00 |  | 8.50 | 8.34 | 8.35 | 8.81 | 8.88 | 10.39 | 9.42 | 9.40 |  | 8.12 |  | 8.30 |  |  |  |  | 8.04 |  | 5.27 | 7.84 |
| 14 | spring | 372107.00 |  | 8.46 | 8.81 | 8.36 | 8.88 |  |  |  |  |  | 8.45 |  | 8.84 |  |  |  |  | 8.36 |  | 5.71 | 7.34 |
| 14 | summer | 519436.00 |  | 6.96 | 7.46 | 6.91 | 7.31 | 7.59 | 10.33 | 8.18 | 8.15 |  | 7.04 |  | 7.13 |  |  |  |  | 7.11 |  | 5.27 | 5.86 |
| 30 | autumn | 190908.00 |  | 8.27 | 8.23 | 8.09 | 8.75 | 8.55 | 10.20 | 9.60 | 9.57 |  | 8.04 |  | 8.31 |  |  |  |  | 7.90 |  | 5.13 | 7.83 |
| 30 | spring | 310896.00 |  | 8.56 | 8.77 | 8.44 | 9.02 |  |  |  |  |  | 8.66 |  | 9.14 |  |  |  |  | 8.43 |  | 5.73 | 7.61 |
| 30 | summer | 512181.00 |  | 7.40 | 7.73 | 7.31 | 7.81 | 7.93 | 10.94 | 8.50 | 8.48 |  | 7.54 |  | 7.72 |  |  |  |  | 7.59 |  | 5.38 | 6.43 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

**Spearman of the daily level within park-month** (paired on the same days)

_no rows_

_no rows_

**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the `≥ predicted q75` rule (ties included, which is what flat predictions cost)

## D1 — next-best ride

Candidates: every (intraday origin, ride) with a live OPERATING wait — the same set for every model. Suggest when the forecast maximum within the lookahead (60 or 120 min) is ≥ live + 10 (`next-best-ride.ts`, walking time 0); correct when the TRUE maximum there is also ≥ live + 10. False-suggestion rate = 1 − precision.

**next-best precision**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.57 [0.52, 0.61] | 0.43 [0.41, 0.44] | 0.46 [0.44, 0.47] | 0.47 [0.45, 0.49] | 0.46 [0.44, 0.48] | 0.43 [0.42, 0.45] | 0.46 [0.42, 0.51] | 0.50 [0.46, 0.54] | 0.45 [0.43, 0.46] |
| 0-120 | 0.65 [0.61, 0.70] | 0.51 [0.50, 0.53] | 0.55 [0.53, 0.57] | 0.56 [0.54, 0.58] | 0.55 [0.53, 0.58] | 0.52 [0.51, 0.53] | 0.56 [0.52, 0.60] | 0.58 [0.54, 0.62] | 0.53 [0.51, 0.55] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.15 [-0.00, 0.30] | -0.00 [-0.03, 0.02] | 0.03 [-0.01, 0.06] | 0.04 [0.01, 0.07]* | 0.04 [-0.00, 0.08] | 0.05 [-0.09, 0.22] | 0.08 [-0.05, 0.23] | 0.02 [-0.01, 0.04] | snaive7 |
| 0-120 | 0.15 [0.01, 0.29] | -0.00 [-0.03, 0.03] | 0.03 [-0.00, 0.07] | 0.05 [0.01, 0.07]* | 0.04 [-0.00, 0.09] | 0.06 [-0.08, 0.21] | 0.08 [-0.05, 0.21] | 0.02 [-0.02, 0.05] | snaive7 |

**next-best precision (top-3)**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.64 [0.61, 0.67] | 0.51 [0.50, 0.53] | 0.54 [0.52, 0.56] | 0.53 [0.51, 0.55] | 0.52 [0.50, 0.54] | 0.52 [0.50, 0.53] | 0.56 [0.53, 0.58] | 0.58 [0.55, 0.60] | 0.54 [0.52, 0.55] |
| 0-120 | 0.72 [0.69, 0.74] | 0.59 [0.58, 0.61] | 0.62 [0.60, 0.64] | 0.62 [0.60, 0.63] | 0.60 [0.58, 0.62] | 0.61 [0.60, 0.62] | 0.64 [0.61, 0.67] | 0.65 [0.62, 0.67] | 0.62 [0.60, 0.63] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.12 [0.03, 0.20]* | 0.00 [-0.02, 0.02] | 0.03 [0.00, 0.05] | 0.01 [-0.01, 0.04] | 0.01 [-0.03, 0.04] | 0.04 [-0.05, 0.13] | 0.06 [-0.03, 0.15] | 0.02 [-0.00, 0.04] | snaive7 |
| 0-120 | 0.10 [0.02, 0.17]* | -0.01 [-0.03, 0.01] | 0.01 [-0.01, 0.04] | 0.01 [-0.02, 0.03] | -0.00 [-0.03, 0.04] | 0.03 [-0.05, 0.11] | 0.04 [-0.05, 0.12] | 0.01 [-0.01, 0.03] | snaive7 |

**next-best recall**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.22 [0.21, 0.23] | 0.49 [0.47, 0.51] | 0.39 [0.38, 0.41] | 0.36 [0.34, 0.38] | 0.22 [0.21, 0.24] | 0.53 [0.52, 0.55] | 0.17 [0.16, 0.18] | 0.21 [0.20, 0.21] | 0.49 [0.48, 0.51] |
| 0-120 | 0.24 [0.23, 0.25] | 0.51 [0.50, 0.53] | 0.41 [0.40, 0.43] | 0.38 [0.36, 0.39] | 0.24 [0.22, 0.26] | 0.58 [0.56, 0.60] | 0.19 [0.18, 0.20] | 0.23 [0.22, 0.23] | 0.51 [0.50, 0.53] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.32 [-0.34, -0.29] | -0.04 [-0.07, -0.02] | -0.14 [-0.16, -0.11] | -0.17 [-0.19, -0.15] | -0.31 [-0.34, -0.27] | -0.37 [-0.39, -0.34] | -0.33 [-0.35, -0.30] | -0.04 [-0.06, -0.02] | snaive7 |
| 0-120 | -0.34 [-0.37, -0.31] | -0.07 [-0.09, -0.04] | -0.17 [-0.19, -0.14] | -0.20 [-0.23, -0.18] | -0.34 [-0.38, -0.29] | -0.38 [-0.41, -0.35] | -0.35 [-0.38, -0.32] | -0.07 [-0.09, -0.04] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | tirex2 | toto2 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| w0-45 | 7.00 [6.84, 7.15] | 14.03 [13.57, 14.54] | 13.03 [12.57, 13.50] | 13.31 [12.81, 13.77] | 12.22 [11.71, 12.76] | 7.35 [7.17, 7.55] | 15.43 [14.79, 16.10] | 7.25 [7.07, 7.42] | 6.91 [6.74, 7.06] | 13.10 [12.63, 13.57] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | tirex2 | toto2 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| w0-45 | -0.62 [-0.91, -0.31]* | 7.06 [5.64, 8.51] | 6.09 [4.80, 7.41] | 6.39 [4.98, 7.89] | 5.50 [4.38, 6.63] | 8.50 [6.83, 10.20] | -0.49 [-0.70, -0.26]* | -0.89 [-1.14, -0.62]* | 6.14 [4.84, 7.48] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | tirex2 | toto2 | chronos2 |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | -0.18 | -0.15 | -0.98 | -0.23 | -1.83 | -1.66 | -2.36 | -0.16 | 0.15 | 0.34 |
| m030 | 0.05 | -0.16 | -1.07 | -0.38 | -1.58 | -1.41 | -2.19 | -0.48 | -0.29 | -0.05 |
| m045 | 0.48 | -0.26 | -1.23 | -0.50 | -1.43 | -1.26 | -2.05 | -0.71 | -0.56 | -0.21 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.75 [0.73, 0.76] | 0.68 [0.67, 0.70] |  | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.80 [0.78, 0.81] |  |  | 0.81 [0.80, 0.83] | 0.66 [0.65, 0.67] | 0.75 [0.74, 0.76] |  | 0.68 [0.67, 0.69] | 0.42 [0.41, 0.43] | 0.68 [0.67, 0.69] |  | 0.83 [0.82, 0.83] |
| 1 | 0.72 [0.71, 0.73] | 0.62 [0.61, 0.64] | 0.79 [0.78, 0.80] | 0.83 [0.82, 0.84] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.76] | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] |  |  | 0.81 [0.80, 0.82] | 0.60 [0.59, 0.62] | 0.75 [0.74, 0.76] | 0.79 [0.78, 0.80] | 0.64 [0.63, 0.66] | 0.46 [0.45, 0.47] | 0.64 [0.63, 0.65] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 2 | 0.70 [0.69, 0.72] | 0.60 [0.58, 0.61] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] |  |  | 0.81 [0.80, 0.83] | 0.58 [0.56, 0.59] | 0.75 [0.74, 0.76] | 0.79 [0.79, 0.80] | 0.61 [0.60, 0.63] | 0.46 [0.45, 0.47] | 0.61 [0.60, 0.63] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 3 | 0.70 [0.68, 0.71] | 0.58 [0.57, 0.60] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.72 [0.71, 0.74] | 0.75 [0.73, 0.76] | 0.81 [0.80, 0.82] | 0.57 [0.55, 0.58] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.59 [0.58, 0.61] | 0.47 [0.46, 0.48] | 0.59 [0.58, 0.61] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 4 | 0.68 [0.67, 0.69] | 0.56 [0.54, 0.57] | 0.79 [0.78, 0.80] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.74 [0.73, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.72 [0.71, 0.73] | 0.74 [0.73, 0.76] | 0.81 [0.80, 0.83] | 0.55 [0.53, 0.56] | 0.75 [0.74, 0.76] | 0.79 [0.78, 0.80] | 0.57 [0.56, 0.59] | 0.47 [0.46, 0.48] | 0.57 [0.56, 0.59] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 5 | 0.68 [0.67, 0.69] | 0.55 [0.53, 0.57] | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.75 [0.74, 0.76] | 0.81 [0.80, 0.82] | 0.79 [0.78, 0.80] | 0.73 [0.71, 0.74] | 0.75 [0.74, 0.77] | 0.81 [0.80, 0.83] | 0.53 [0.52, 0.55] | 0.75 [0.74, 0.76] | 0.80 [0.79, 0.81] | 0.56 [0.54, 0.57] | 0.46 [0.46, 0.47] | 0.56 [0.54, 0.57] | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] |
| 6 | 0.67 [0.66, 0.68] | 0.54 [0.53, 0.56] | 0.79 [0.78, 0.80] | 0.83 [0.82, 0.84] | 0.82 [0.82, 0.83] | 0.74 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.73 [0.71, 0.74] | 0.76 [0.74, 0.77] | 0.81 [0.80, 0.82] | 0.53 [0.52, 0.55] | 0.75 [0.74, 0.76] | 0.79 [0.78, 0.80] | 0.55 [0.54, 0.57] | 0.47 [0.46, 0.48] | 0.55 [0.54, 0.57] | 0.79 [0.78, 0.80] | 0.82 [0.82, 0.83] |
| 7 | 0.66 [0.64, 0.67] | 0.54 [0.52, 0.55] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.73 [0.71, 0.74] | 0.80 [0.79, 0.81] | 0.77 [0.76, 0.79] | 0.72 [0.70, 0.73] | 0.75 [0.73, 0.76] |  | 0.52 [0.51, 0.53] | 0.73 [0.72, 0.74] | 0.79 [0.78, 0.80] | 0.54 [0.53, 0.56] | 0.47 [0.46, 0.48] | 0.54 [0.53, 0.56] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 10 |  |  | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.82, 0.83] | 0.74 [0.73, 0.75] | 0.80 [0.80, 0.81] | 0.78 [0.77, 0.79] | 0.72 [0.71, 0.74] | 0.75 [0.74, 0.76] |  |  |  | 0.79 [0.78, 0.80] |  |  |  | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 14 |  |  | 0.79 [0.78, 0.79] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.72 [0.70, 0.73] | 0.75 [0.73, 0.76] |  |  |  | 0.79 [0.78, 0.80] |  |  |  | 0.79 [0.78, 0.80] | 0.83 [0.82, 0.83] |
| 21 |  |  | 0.78 [0.77, 0.79] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.79] | 0.72 [0.71, 0.74] | 0.75 [0.73, 0.76] |  |  |  | 0.79 [0.78, 0.80] |  |  |  | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 30 |  |  | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.73 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.77 [0.76, 0.79] | 0.72 [0.70, 0.74] | 0.74 [0.73, 0.76] |  |  |  | 0.78 [0.77, 0.80] |  |  |  | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.83] |
| 45 |  |  | 0.79 [0.77, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.71 [0.69, 0.74] | 0.79 [0.78, 0.81] | 0.78 [0.76, 0.80] | 0.73 [0.71, 0.75] | 0.75 [0.73, 0.77] |  |  |  | 0.78 [0.77, 0.80] |  |  |  | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.83] |
| 60 |  |  | 0.80 [0.79, 0.81] | 0.83 [0.81, 0.84] | 0.83 [0.82, 0.84] |  | 0.80 [0.79, 0.81] | 0.91 [0.87, 1.00] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  |  |  | 0.79 [0.78, 0.80] |  |  |  | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] |
| 90 |  |  | 0.79 [0.77, 0.80] | 0.83 [0.81, 0.84] | 0.82 [0.80, 0.83] |  | 0.79 [0.78, 0.81] |  |  |  |  |  |  | 0.79 [0.77, 0.80] |  |  |  | 0.78 [0.77, 0.80] | 0.82 [0.81, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.07 [-0.09, -0.05] | -0.13 [-0.16, -0.11] |  | -0.01 [-0.01, -0.00] | 0.01 [-0.00, 0.02] | -0.06 [-0.08, -0.04] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.01] |  |  | -0.04 [-0.05, -0.03] | -0.16 [-0.18, -0.14] | -0.06 [-0.08, -0.04] |  | -0.14 [-0.16, -0.12] | -0.41 [-0.47, -0.36] | -0.14 [-0.16, -0.12] |  | wt_med |
| 1 | -0.09 [-0.12, -0.07] | -0.19 [-0.22, -0.16] | -0.03 [-0.04, -0.02] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.01] | -0.07 [-0.08, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.01] |  |  | -0.04 [-0.05, -0.03] | -0.21 [-0.24, -0.18] | -0.06 [-0.08, -0.04] | -0.03 [-0.04, -0.02] | -0.17 [-0.20, -0.14] | -0.37 [-0.41, -0.32] | -0.17 [-0.20, -0.14] | -0.03 [-0.04, -0.02] | wt_med |
| 2 | -0.10 [-0.13, -0.08] | -0.21 [-0.25, -0.17] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.02] | -0.07 [-0.09, -0.05] | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.01] |  |  | -0.03 [-0.04, -0.02] | -0.23 [-0.26, -0.20] | -0.05 [-0.07, -0.03] | -0.02 [-0.03, -0.01] | -0.20 [-0.23, -0.16] | -0.36 [-0.40, -0.31] | -0.20 [-0.23, -0.16] | -0.02 [-0.03, -0.01] | wt_med |
| 3 | -0.11 [-0.14, -0.08] | -0.22 [-0.26, -0.18] | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.02] | -0.07 [-0.09, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.01] | -0.08 [-0.12, -0.05] | -0.06 [-0.09, -0.03] | -0.03 [-0.04, -0.02] | -0.24 [-0.28, -0.21] | -0.05 [-0.06, -0.03] | -0.02 [-0.04, -0.01] | -0.21 [-0.24, -0.18] | -0.35 [-0.39, -0.30] | -0.21 [-0.25, -0.18] | -0.03 [-0.04, -0.01] | wt_med |
| 4 | -0.13 [-0.16, -0.10] | -0.25 [-0.30, -0.21] | -0.03 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [-0.01, 0.01] | -0.07 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.04, -0.01] | -0.09 [-0.13, -0.05] | -0.06 [-0.10, -0.03] | -0.04 [-0.05, -0.03] | -0.26 [-0.31, -0.22] | -0.06 [-0.08, -0.04] | -0.02 [-0.04, -0.01] | -0.24 [-0.28, -0.20] | -0.36 [-0.40, -0.31] | -0.24 [-0.28, -0.20] | -0.03 [-0.04, -0.01] | wt_med |
| 5 | -0.14 [-0.17, -0.11] | -0.27 [-0.31, -0.22] | -0.02 [-0.03, -0.01] | -0.01 [-0.01, -0.00] | 0.01 [-0.00, 0.02] | -0.06 [-0.08, -0.04] | -0.01 [-0.02, 0.00] | -0.02 [-0.03, -0.00] | -0.09 [-0.12, -0.06] | -0.06 [-0.09, -0.03] | -0.03 [-0.04, -0.03] | -0.28 [-0.32, -0.24] | -0.06 [-0.08, -0.04] | -0.02 [-0.03, -0.00] | -0.26 [-0.30, -0.22] | -0.36 [-0.40, -0.32] | -0.26 [-0.30, -0.22] | -0.02 [-0.04, -0.01] | wt_med |
| 6 | -0.15 [-0.18, -0.11] | -0.28 [-0.32, -0.23] | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.01] | -0.08 [-0.09, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.05, -0.02] | -0.09 [-0.12, -0.06] | -0.06 [-0.09, -0.03] | -0.03 [-0.05, -0.03] | -0.28 [-0.33, -0.24] | -0.06 [-0.08, -0.05] | -0.03 [-0.04, -0.02] | -0.27 [-0.31, -0.22] | -0.36 [-0.39, -0.32] | -0.27 [-0.31, -0.23] | -0.03 [-0.04, -0.02] | wt_med |
| 7 | -0.15 [-0.19, -0.12] | -0.27 [-0.32, -0.23] | -0.03 [-0.04, -0.01] | -0.00 [-0.01, -0.00] | 0.01 [-0.01, 0.02] | -0.07 [-0.09, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.05, -0.02] | -0.09 [-0.13, -0.05] | -0.06 [-0.10, -0.03] |  | -0.30 [-0.34, -0.25] | -0.08 [-0.09, -0.06] | -0.03 [-0.04, -0.01] | -0.27 [-0.31, -0.22] | -0.35 [-0.39, -0.31] | -0.27 [-0.31, -0.22] | -0.03 [-0.04, -0.01] | wt_med |
| 10 |  |  | -0.03 [-0.04, -0.01] | -0.01 [-0.01, -0.00] | 0.01 [-0.00, 0.01] | -0.07 [-0.09, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.05, -0.01] | -0.09 [-0.12, -0.06] | -0.06 [-0.09, -0.03] |  |  |  | -0.03 [-0.04, -0.01] |  |  |  | -0.03 [-0.04, -0.01] | wt_med |
| 14 |  |  | -0.03 [-0.05, -0.02] | 0.00 [-0.00, 0.01] | 0.00 [-0.01, 0.01] | -0.08 [-0.09, -0.06] | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.02] | -0.09 [-0.13, -0.05] | -0.06 [-0.09, -0.03] |  |  |  | -0.03 [-0.04, -0.02] |  |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 21 |  |  | -0.03 [-0.05, -0.02] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.01] | -0.08 [-0.10, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.02] | -0.08 [-0.12, -0.05] | -0.06 [-0.10, -0.03] |  |  |  | -0.03 [-0.04, -0.02] |  |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 30 |  |  | -0.03 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.02] | -0.07 [-0.09, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.00] | -0.07 [-0.10, -0.04] | -0.04 [-0.07, -0.02] |  |  |  | -0.03 [-0.04, -0.01] |  |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 45 |  |  | -0.03 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.02] | -0.06 [-0.09, -0.02] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.00] | -0.07 [-0.10, -0.05] | -0.04 [-0.07, -0.02] |  |  |  | -0.03 [-0.04, -0.01] |  |  |  | -0.03 [-0.05, -0.02] | wt_med |
| 60 |  |  | -0.02 [-0.03, -0.00] | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.02] |  | -0.02 [-0.03, -0.00] | -0.10 [-0.12, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | -0.02 [-0.03, -0.01] |  |  |  | -0.02 [-0.04, -0.01] | wt_med |
| 90 |  |  | -0.03 [-0.04, -0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] |  | -0.02 [-0.04, -0.01] |  |  |  |  |  |  | -0.03 [-0.04, -0.02] |  |  |  | -0.03 [-0.05, -0.01] | wt_med |

**best-time hit rate (±30 min)**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.76 [0.75, 0.77] | 0.70 [0.69, 0.72] |  | 0.82 [0.81, 0.83] | 0.83 [0.83, 0.84] | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.83] | 0.81 [0.80, 0.82] |  |  | 0.83 [0.81, 0.84] | 0.69 [0.68, 0.70] | 0.78 [0.77, 0.79] |  | 0.70 [0.69, 0.71] | 0.45 [0.44, 0.46] | 0.70 [0.69, 0.71] |  | 0.83 [0.82, 0.84] |
| 1 | 0.74 [0.73, 0.75] | 0.65 [0.64, 0.66] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.81, 0.83] | 0.64 [0.63, 0.65] | 0.77 [0.76, 0.78] | 0.81 [0.80, 0.82] | 0.67 [0.65, 0.68] | 0.47 [0.46, 0.48] | 0.66 [0.65, 0.68] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.83] |
| 2 | 0.72 [0.71, 0.73] | 0.63 [0.61, 0.64] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.78] | 0.82 [0.81, 0.82] | 0.80 [0.79, 0.81] |  |  | 0.82 [0.81, 0.83] | 0.62 [0.60, 0.63] | 0.78 [0.77, 0.78] | 0.81 [0.80, 0.82] | 0.64 [0.63, 0.65] | 0.48 [0.47, 0.49] | 0.64 [0.63, 0.65] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] |
| 3 | 0.71 [0.70, 0.73] | 0.61 [0.60, 0.62] | 0.80 [0.79, 0.81] | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.77 [0.75, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.77] | 0.77 [0.76, 0.78] | 0.82 [0.81, 0.83] | 0.61 [0.59, 0.62] | 0.79 [0.78, 0.79] | 0.80 [0.79, 0.81] | 0.62 [0.60, 0.63] | 0.48 [0.47, 0.49] | 0.62 [0.60, 0.63] | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] |
| 4 | 0.70 [0.68, 0.71] | 0.58 [0.57, 0.60] | 0.81 [0.80, 0.81] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.83] | 0.78 [0.77, 0.79] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.77 [0.75, 0.78] | 0.82 [0.81, 0.83] | 0.59 [0.58, 0.60] | 0.78 [0.77, 0.79] | 0.81 [0.80, 0.81] | 0.60 [0.58, 0.61] | 0.49 [0.48, 0.50] | 0.60 [0.58, 0.61] | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] |
| 5 | 0.70 [0.69, 0.71] | 0.58 [0.56, 0.59] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.83] | 0.83 [0.83, 0.84] | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.83] | 0.81 [0.80, 0.82] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.83 [0.81, 0.84] | 0.57 [0.56, 0.59] | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.82] | 0.58 [0.57, 0.60] | 0.48 [0.47, 0.49] | 0.58 [0.57, 0.60] | 0.81 [0.81, 0.82] | 0.83 [0.82, 0.84] |
| 6 | 0.69 [0.68, 0.70] | 0.57 [0.56, 0.59] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.79] | 0.81 [0.81, 0.82] | 0.80 [0.79, 0.81] | 0.76 [0.75, 0.78] | 0.78 [0.76, 0.79] | 0.82 [0.81, 0.83] | 0.58 [0.56, 0.59] | 0.78 [0.77, 0.78] | 0.81 [0.80, 0.82] | 0.58 [0.56, 0.59] | 0.48 [0.47, 0.48] | 0.57 [0.56, 0.59] | 0.81 [0.80, 0.82] | 0.82 [0.82, 0.83] |
| 7 | 0.68 [0.66, 0.69] | 0.56 [0.55, 0.58] | 0.81 [0.80, 0.81] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.77] | 0.77 [0.75, 0.78] |  | 0.56 [0.54, 0.57] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.57 [0.55, 0.58] | 0.49 [0.47, 0.50] | 0.57 [0.55, 0.58] | 0.80 [0.80, 0.81] | 0.82 [0.81, 0.83] |
| 10 |  |  | 0.81 [0.80, 0.82] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.78] | 0.82 [0.81, 0.83] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.77] | 0.77 [0.76, 0.79] |  |  |  | 0.81 [0.80, 0.82] |  |  |  | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] |
| 14 |  |  | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.82 [0.82, 0.83] | 0.76 [0.75, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.77 [0.75, 0.78] |  |  |  | 0.80 [0.80, 0.81] |  |  |  | 0.80 [0.80, 0.81] | 0.82 [0.81, 0.83] |
| 21 |  |  | 0.80 [0.80, 0.81] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.76 [0.74, 0.77] | 0.77 [0.76, 0.78] |  |  |  | 0.81 [0.80, 0.82] |  |  |  | 0.80 [0.80, 0.81] | 0.83 [0.82, 0.83] |
| 30 |  |  | 0.80 [0.80, 0.81] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.80 [0.78, 0.81] | 0.75 [0.73, 0.77] | 0.77 [0.75, 0.78] |  |  |  | 0.80 [0.80, 0.81] |  |  |  | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] |
| 45 |  |  | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.76 [0.73, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.78, 0.81] | 0.75 [0.73, 0.77] | 0.77 [0.76, 0.79] |  |  |  | 0.80 [0.79, 0.81] |  |  |  | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] |
| 60 |  |  | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] |  | 0.81 [0.80, 0.82] | 0.82 [0.33, 1.00] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  |  |  | 0.81 [0.80, 0.82] |  |  |  | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] |
| 90 |  |  | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.85] | 0.83 [0.82, 0.84] |  | 0.81 [0.80, 0.82] |  |  |  |  |  |  | 0.81 [0.80, 0.82] |  |  |  | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.06 [-0.08, -0.04] | -0.12 [-0.14, -0.10] |  | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.05, -0.02] | -0.01 [-0.01, 0.01] | -0.01 [-0.02, 0.00] |  |  | -0.03 [-0.04, -0.02] | -0.13 [-0.15, -0.11] | -0.04 [-0.06, -0.02] |  | -0.12 [-0.14, -0.10] | -0.38 [-0.44, -0.32] | -0.12 [-0.14, -0.10] |  | wt_med |
| 1 | -0.08 [-0.10, -0.06] | -0.17 [-0.20, -0.14] | -0.02 [-0.02, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  |  | -0.02 [-0.03, -0.01] | -0.18 [-0.21, -0.15] | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.01] | -0.15 [-0.17, -0.12] | -0.36 [-0.41, -0.31] | -0.15 [-0.17, -0.12] | -0.02 [-0.02, -0.01] | wt_med |
| 2 | -0.09 [-0.12, -0.07] | -0.19 [-0.22, -0.15] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.01 [-0.01, 0.00] | -0.01 [-0.02, 0.00] |  |  | -0.02 [-0.03, -0.01] | -0.20 [-0.23, -0.16] | -0.04 [-0.05, -0.02] | -0.01 [-0.02, -0.00] | -0.17 [-0.20, -0.14] | -0.34 [-0.39, -0.29] | -0.17 [-0.20, -0.14] | -0.01 [-0.02, -0.00] | wt_med |
| 3 | -0.10 [-0.13, -0.08] | -0.20 [-0.23, -0.17] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] | -0.06 [-0.09, -0.04] | -0.04 [-0.07, -0.02] | -0.02 [-0.03, -0.01] | -0.21 [-0.24, -0.17] | -0.03 [-0.04, -0.01] | -0.01 [-0.02, -0.01] | -0.19 [-0.22, -0.16] | -0.34 [-0.38, -0.29] | -0.19 [-0.22, -0.16] | -0.01 [-0.02, -0.01] | wt_med |
| 4 | -0.12 [-0.14, -0.09] | -0.23 [-0.27, -0.19] | -0.01 [-0.02, -0.00] | 0.00 [-0.00, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.02] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.06 [-0.09, -0.03] | -0.04 [-0.08, -0.02] | -0.02 [-0.04, -0.01] | -0.22 [-0.26, -0.18] | -0.04 [-0.05, -0.02] | -0.01 [-0.02, -0.00] | -0.21 [-0.25, -0.18] | -0.33 [-0.38, -0.29] | -0.22 [-0.25, -0.18] | -0.02 [-0.02, -0.01] | wt_med |
| 5 | -0.12 [-0.15, -0.09] | -0.24 [-0.28, -0.20] | -0.01 [-0.02, 0.00] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.05, -0.02] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.00] | -0.06 [-0.10, -0.04] | -0.04 [-0.07, -0.02] | -0.02 [-0.03, -0.01] | -0.25 [-0.28, -0.21] | -0.04 [-0.05, -0.02] | -0.01 [-0.02, 0.00] | -0.23 [-0.27, -0.20] | -0.35 [-0.38, -0.30] | -0.24 [-0.27, -0.20] | -0.01 [-0.02, 0.00] | wt_med |
| 6 | -0.12 [-0.15, -0.09] | -0.25 [-0.29, -0.20] | -0.02 [-0.02, -0.01] | 0.00 [-0.00, 0.00] | 0.01 [0.00, 0.02] | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.00] | -0.05 [-0.09, -0.03] | -0.04 [-0.07, -0.02] | -0.02 [-0.03, -0.01] | -0.24 [-0.28, -0.20] | -0.04 [-0.06, -0.02] | -0.01 [-0.02, -0.00] | -0.24 [-0.28, -0.20] | -0.35 [-0.39, -0.31] | -0.24 [-0.28, -0.20] | -0.01 [-0.02, -0.00] | wt_med |
| 7 | -0.14 [-0.17, -0.10] | -0.25 [-0.30, -0.20] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.07, -0.03] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.06 [-0.09, -0.03] | -0.04 [-0.07, -0.02] |  | -0.26 [-0.30, -0.21] | -0.05 [-0.07, -0.03] | -0.01 [-0.02, -0.00] | -0.24 [-0.28, -0.20] | -0.34 [-0.37, -0.29] | -0.25 [-0.29, -0.20] | -0.01 [-0.02, -0.01] | wt_med |
| 10 |  |  | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.06, -0.02] | -0.01 [-0.02, 0.01] | -0.02 [-0.03, 0.00] | -0.06 [-0.09, -0.04] | -0.04 [-0.07, -0.02] |  |  |  | -0.01 [-0.02, 0.00] |  |  |  | -0.01 [-0.02, 0.00] | wt_med |
| 14 |  |  | -0.02 [-0.02, -0.01] | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.01] | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.06 [-0.09, -0.03] | -0.04 [-0.07, -0.01] |  |  |  | -0.01 [-0.02, -0.00] |  |  |  | -0.01 [-0.02, -0.00] | wt_med |
| 21 |  |  | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.07, -0.03] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] | -0.05 [-0.09, -0.03] | -0.04 [-0.07, -0.01] |  |  |  | -0.02 [-0.03, -0.01] |  |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 30 |  |  | -0.02 [-0.03, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, 0.01] | -0.05 [-0.07, -0.03] | -0.03 [-0.05, -0.01] |  |  |  | -0.02 [-0.03, -0.01] |  |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 45 |  |  | -0.01 [-0.02, -0.00] | 0.00 [-0.01, 0.01] | 0.01 [0.00, 0.02] | -0.02 [-0.05, 0.02] | -0.01 [-0.02, 0.00] | -0.00 [-0.02, 0.02] | -0.04 [-0.07, -0.02] | -0.02 [-0.04, 0.01] |  |  |  | -0.01 [-0.02, 0.00] |  |  |  | -0.01 [-0.02, -0.00] | wt_med |
| 60 |  |  | -0.01 [-0.02, 0.00] | 0.00 [-0.01, 0.01] | 0.01 [0.01, 0.02] |  | -0.01 [-0.02, 0.00] | -0.10 [-0.12, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | -0.01 [-0.02, 0.00] |  |  |  | -0.02 [-0.03, -0.00] | wt_med |
| 90 |  |  | -0.01 [-0.02, -0.00] | 0.01 [-0.00, 0.01] | 0.01 [0.00, 0.01] |  | -0.01 [-0.02, 0.00] |  |  |  |  |  |  | -0.01 [-0.02, -0.00] |  |  |  | -0.01 [-0.03, -0.00] | wt_med |

**best-time regret (min)**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.69 [4.43, 4.95] | 6.38 [6.06, 6.73] |  | 3.28 [3.08, 3.50] | 2.99 [2.83, 3.17] | 4.02 [3.79, 4.27] | 3.31 [3.15, 3.50] | 3.51 [3.31, 3.73] |  |  | 3.76 [3.46, 4.04] | 6.76 [6.44, 7.09] | 4.24 [4.03, 4.47] |  | 6.58 [6.27, 6.90] | 15.26 [14.65, 15.80] | 6.59 [6.28, 6.91] |  | 3.25 [3.06, 3.44] |
| 1 | 5.17 [4.90, 5.44] | 8.28 [7.82, 8.73] | 3.52 [3.35, 3.68] | 3.09 [2.91, 3.26] | 3.00 [2.84, 3.16] | 3.98 [3.76, 4.23] | 3.30 [3.14, 3.46] | 3.51 [3.30, 3.72] |  |  | 3.70 [3.43, 4.01] | 8.37 [7.93, 8.80] | 4.28 [4.07, 4.50] | 3.49 [3.32, 3.67] | 7.63 [7.22, 8.01] | 13.81 [13.36, 14.30] | 7.72 [7.31, 8.11] | 3.50 [3.33, 3.67] | 3.18 [3.00, 3.36] |
| 2 | 5.66 [5.37, 5.99] | 8.92 [8.43, 9.39] | 3.45 [3.28, 3.64] | 3.15 [2.97, 3.35] | 2.98 [2.81, 3.15] | 4.28 [3.98, 4.61] | 3.30 [3.12, 3.48] | 3.53 [3.30, 3.76] |  |  | 3.65 [3.37, 3.97] | 9.01 [8.57, 9.45] | 4.34 [4.11, 4.57] | 3.44 [3.26, 3.62] | 8.59 [8.15, 9.06] | 13.86 [13.33, 14.39] | 8.60 [8.15, 9.07] | 3.47 [3.30, 3.64] | 3.19 [3.00, 3.38] |
| 3 | 5.94 [5.63, 6.27] | 9.48 [8.99, 10.01] | 3.74 [3.55, 3.94] | 3.35 [3.15, 3.55] | 3.20 [3.02, 3.38] | 4.53 [4.27, 4.84] | 3.51 [3.33, 3.70] | 3.79 [3.56, 4.04] | 5.10 [4.77, 5.43] | 4.29 [4.01, 4.58] | 3.78 [3.48, 4.08] | 9.55 [9.10, 10.02] | 4.36 [4.12, 4.59] | 3.67 [3.48, 3.87] | 9.25 [8.76, 9.76] | 14.06 [13.60, 14.58] | 9.27 [8.78, 9.78] | 3.70 [3.50, 3.91] | 3.38 [3.17, 3.59] |
| 4 | 6.31 [5.97, 6.66] | 10.22 [9.66, 10.79] | 3.61 [3.43, 3.80] | 3.28 [3.06, 3.51] | 3.12 [2.94, 3.31] | 4.26 [4.03, 4.51] | 3.51 [3.33, 3.70] | 3.66 [3.44, 3.89] | 5.16 [4.80, 5.53] | 4.34 [4.05, 4.65] | 3.79 [3.47, 4.09] | 10.08 [9.56, 10.61] | 4.49 [4.23, 4.76] | 3.61 [3.43, 3.79] | 9.90 [9.35, 10.44] | 13.94 [13.45, 14.47] | 9.95 [9.42, 10.48] | 3.67 [3.49, 3.86] | 3.29 [3.11, 3.49] |
| 5 | 6.38 [6.03, 6.74] | 10.50 [9.90, 11.07] | 3.45 [3.29, 3.63] | 3.26 [3.06, 3.48] | 3.00 [2.84, 3.19] | 4.08 [3.86, 4.34] | 3.30 [3.12, 3.48] | 3.57 [3.36, 3.79] | 4.99 [4.68, 5.32] | 4.18 [3.92, 4.44] | 3.71 [3.43, 4.00] | 10.43 [9.90, 10.94] | 4.43 [4.21, 4.67] | 3.45 [3.29, 3.63] | 10.18 [9.60, 10.73] | 13.99 [13.50, 14.50] | 10.24 [9.65, 10.80] | 3.50 [3.33, 3.68] | 3.23 [3.03, 3.42] |
| 6 | 6.61 [6.26, 6.97] | 10.67 [10.10, 11.24] | 3.57 [3.39, 3.75] | 3.10 [2.91, 3.27] | 3.01 [2.85, 3.17] | 4.14 [3.92, 4.37] | 3.35 [3.18, 3.53] | 3.66 [3.44, 3.87] | 4.74 [4.46, 5.06] | 4.00 [3.74, 4.27] | 3.69 [3.43, 4.00] | 10.41 [9.87, 10.92] | 4.34 [4.13, 4.57] | 3.50 [3.33, 3.68] | 10.41 [9.86, 10.95] | 13.71 [13.23, 14.14] | 10.43 [9.88, 10.99] | 3.54 [3.36, 3.72] | 3.18 [2.99, 3.35] |
| 7 | 7.00 [6.60, 7.41] | 10.95 [10.36, 11.54] | 3.50 [3.31, 3.69] | 3.20 [3.02, 3.39] | 2.99 [2.82, 3.17] | 4.51 [4.18, 4.87] | 3.34 [3.15, 3.54] | 3.75 [3.48, 4.02] | 5.20 [4.84, 5.58] | 4.39 [4.08, 4.72] |  | 10.93 [10.39, 11.50] | 4.77 [4.54, 5.02] | 3.47 [3.28, 3.66] | 10.63 [10.09, 11.18] | 14.06 [13.53, 14.63] | 10.64 [10.09, 11.21] | 3.51 [3.32, 3.70] | 3.20 [3.02, 3.38] |
| 10 |  |  | 3.62 [3.45, 3.81] | 3.27 [3.07, 3.50] | 3.08 [2.89, 3.28] | 4.31 [4.07, 4.57] | 3.36 [3.17, 3.54] | 3.76 [3.51, 4.04] | 5.07 [4.74, 5.42] | 4.20 [3.94, 4.47] |  |  |  | 3.58 [3.40, 3.77] |  |  |  | 3.62 [3.44, 3.82] | 3.26 [3.04, 3.47] |
| 14 |  |  | 3.77 [3.56, 3.99] | 3.25 [3.03, 3.48] | 3.24 [3.02, 3.45] | 4.57 [4.30, 4.85] | 3.64 [3.42, 3.85] | 3.88 [3.60, 4.17] | 5.22 [4.85, 5.62] | 4.41 [4.09, 4.73] |  |  |  | 3.68 [3.48, 3.88] |  |  |  | 3.71 [3.51, 3.89] | 3.38 [3.17, 3.61] |
| 21 |  |  | 3.69 [3.50, 3.88] | 3.10 [2.90, 3.29] | 3.08 [2.90, 3.26] | 4.42 [4.15, 4.69] | 3.50 [3.31, 3.69] | 3.76 [3.51, 4.02] | 4.84 [4.53, 5.18] | 4.19 [3.91, 4.51] |  |  |  | 3.62 [3.43, 3.82] |  |  |  | 3.67 [3.48, 3.86] | 3.21 [3.02, 3.39] |
| 30 |  |  | 3.64 [3.45, 3.84] | 3.19 [2.98, 3.40] | 3.10 [2.91, 3.31] | 4.70 [4.36, 5.04] | 3.55 [3.34, 3.77] | 3.99 [3.64, 4.34] | 5.03 [4.62, 5.45] | 4.34 [4.01, 4.68] |  |  |  | 3.66 [3.46, 3.86] |  |  |  | 3.71 [3.51, 3.91] | 3.34 [3.12, 3.56] |
| 45 |  |  | 3.71 [3.48, 3.96] | 3.07 [2.86, 3.27] | 3.17 [2.96, 3.41] | 4.67 [4.03, 5.36] | 3.61 [3.38, 3.89] | 3.84 [3.45, 4.34] | 4.91 [4.46, 5.40] | 4.24 [3.85, 4.65] |  |  |  | 3.70 [3.48, 3.94] |  |  |  | 3.79 [3.55, 4.05] | 3.20 [2.97, 3.45] |
| 60 |  |  | 3.69 [3.48, 3.89] | 3.11 [2.90, 3.34] | 3.16 [2.96, 3.36] |  | 3.62 [3.40, 3.84] | 0.73 [0.00, 2.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | 3.70 [3.49, 3.92] |  |  |  | 3.87 [3.64, 4.11] | 3.23 [3.01, 3.44] |
| 90 |  |  | 3.77 [3.51, 4.05] | 3.14 [2.83, 3.44] | 3.25 [2.98, 3.52] |  | 3.68 [3.41, 3.94] |  |  |  |  |  |  | 3.80 [3.53, 4.06] |  |  |  | 3.89 [3.61, 4.16] | 3.28 [3.02, 3.56] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.25 [0.86, 1.73] | 2.92 [2.31, 3.52] |  | 0.16 [0.03, 0.30] | -0.33 [-0.52, -0.17]* | 0.76 [0.42, 1.10] | 0.04 [-0.19, 0.22] | 0.13 [-0.11, 0.33] |  |  | 0.91 [0.57, 1.34] | 3.31 [2.71, 3.84] | 0.73 [0.36, 1.08] |  | 3.12 [2.68, 3.58] | 12.17 [9.43, 14.91] | 3.12 [2.67, 3.56] |  | wt_med |
| 1 | 1.87 [1.38, 2.40] | 4.90 [3.85, 5.99] | 0.29 [0.09, 0.46] | 0.11 [0.01, 0.21] | -0.26 [-0.47, -0.10]* | 0.85 [0.53, 1.14] | 0.10 [-0.08, 0.26] | 0.25 [0.05, 0.44] |  |  | 0.89 [0.53, 1.34] | 5.04 [4.09, 6.11] | 0.86 [0.52, 1.20] | 0.26 [0.06, 0.43] | 4.23 [3.35, 5.27] | 10.77 [8.51, 13.07] | 4.29 [3.39, 5.31] | 0.26 [0.07, 0.43] | wt_med |
| 2 | 2.21 [1.62, 2.87] | 5.47 [4.20, 6.99] | 0.20 [-0.02, 0.38] | 0.03 [-0.10, 0.15] | -0.28 [-0.49, -0.14]* | 0.91 [0.52, 1.28] | 0.08 [-0.14, 0.26] | 0.16 [-0.08, 0.38] |  |  | 0.81 [0.49, 1.15] | 5.52 [4.43, 6.68] | 0.84 [0.50, 1.15] | 0.18 [-0.03, 0.35] | 5.06 [4.02, 6.26] | 10.78 [8.51, 13.12] | 5.07 [4.00, 6.28] | 0.21 [-0.03, 0.39] | wt_med |
| 3 | 2.37 [1.73, 3.04] | 5.83 [4.48, 7.26] | 0.28 [0.01, 0.51] | 0.11 [-0.01, 0.22] | -0.25 [-0.47, -0.08]* | 1.05 [0.62, 1.44] | 0.12 [-0.11, 0.30] | 0.34 [0.06, 0.56] | 1.58 [0.88, 2.28] | 0.75 [0.32, 1.18] | 0.80 [0.55, 1.08] | 5.90 [4.71, 7.16] | 0.68 [0.31, 1.01] | 0.22 [-0.01, 0.43] | 5.57 [4.38, 6.86] | 10.70 [8.48, 12.83] | 5.57 [4.36, 6.88] | 0.25 [0.01, 0.46] | wt_med |
| 4 | 2.78 [2.06, 3.57] | 6.76 [5.09, 8.48] | 0.23 [-0.02, 0.45] | 0.10 [-0.02, 0.22] | -0.26 [-0.41, -0.12]* | 0.79 [0.48, 1.12] | 0.16 [-0.04, 0.36] | 0.18 [-0.08, 0.42] | 1.73 [0.93, 2.78] | 0.88 [0.36, 1.50] | 0.90 [0.60, 1.24] | 6.53 [5.05, 8.07] | 0.91 [0.56, 1.28] | 0.23 [-0.01, 0.47] | 6.31 [4.86, 7.83] | 10.63 [8.49, 12.69] | 6.36 [4.95, 7.86] | 0.29 [0.04, 0.53] | wt_med |
| 5 | 2.98 [2.13, 3.79] | 7.07 [5.27, 8.90] | 0.13 [-0.15, 0.37] | 0.14 [0.02, 0.28] | -0.32 [-0.51, -0.15]* | 0.73 [0.37, 1.05] | 0.01 [-0.20, 0.21] | 0.20 [-0.09, 0.45] | 1.67 [0.93, 2.44] | 0.86 [0.40, 1.34] | 0.78 [0.49, 1.13] | 6.98 [5.27, 8.74] | 0.87 [0.52, 1.22] | 0.12 [-0.14, 0.35] | 6.63 [5.11, 8.31] | 10.85 [8.60, 13.03] | 6.72 [5.21, 8.36] | 0.17 [-0.08, 0.40] | wt_med |
| 6 | 3.29 [2.29, 4.27] | 7.43 [5.58, 9.27] | 0.28 [0.09, 0.48] | 0.04 [-0.06, 0.15] | -0.26 [-0.43, -0.10]* | 0.95 [0.63, 1.24] | 0.12 [-0.07, 0.29] | 0.34 [0.10, 0.57] | 1.45 [0.81, 2.17] | 0.67 [0.23, 1.10] | 0.82 [0.46, 1.28] | 7.15 [5.36, 8.95] | 0.87 [0.58, 1.17] | 0.22 [0.02, 0.41] | 7.14 [5.42, 8.99] | 10.67 [8.56, 12.67] | 7.16 [5.49, 8.98] | 0.26 [0.07, 0.45] | wt_med |
| 7 | 3.61 [2.50, 4.74] | 7.62 [5.65, 9.64] | 0.20 [-0.06, 0.43] | 0.04 [-0.07, 0.16] | -0.30 [-0.54, -0.13]* | 1.06 [0.61, 1.54] | 0.10 [-0.15, 0.31] | 0.30 [0.06, 0.54] | 1.80 [0.94, 2.82] | 0.98 [0.38, 1.68] |  | 7.54 [5.63, 9.53] | 1.25 [0.88, 1.59] | 0.17 [-0.09, 0.40] | 7.21 [5.58, 8.95] | 10.93 [8.61, 13.32] | 7.20 [5.59, 8.91] | 0.19 [-0.04, 0.40] | wt_med |
| 10 |  |  | 0.24 [-0.03, 0.48] | 0.09 [0.01, 0.17] | -0.28 [-0.45, -0.14]* | 0.88 [0.48, 1.26] | 0.05 [-0.18, 0.24] | 0.30 [0.03, 0.54] | 1.67 [0.96, 2.36] | 0.76 [0.36, 1.21] |  |  |  | 0.21 [-0.05, 0.46] |  |  |  | 0.23 [-0.03, 0.47] | wt_med |
| 14 |  |  | 0.24 [-0.06, 0.54] | -0.06 [-0.18, 0.04] | -0.27 [-0.53, -0.07] | 0.87 [0.36, 1.30] | 0.17 [-0.11, 0.45] | 0.20 [-0.13, 0.53] | 1.58 [0.81, 2.50] | 0.71 [0.18, 1.27] |  |  |  | 0.16 [-0.14, 0.46] |  |  |  | 0.18 [-0.14, 0.47] | wt_med |
| 21 |  |  | 0.33 [0.11, 0.54] | -0.04 [-0.14, 0.07] | -0.27 [-0.43, -0.13]* | 1.11 [0.75, 1.52] | 0.18 [-0.01, 0.37] | 0.32 [0.08, 0.55] | 1.47 [0.79, 2.26] | 0.78 [0.27, 1.37] |  |  |  | 0.27 [0.05, 0.49] |  |  |  | 0.28 [0.05, 0.51] | wt_med |
| 30 |  |  | 0.17 [-0.11, 0.41] | -0.05 [-0.18, 0.07] | -0.35 [-0.55, -0.19]* | 0.89 [0.47, 1.30] | 0.13 [-0.12, 0.34] | 0.06 [-0.30, 0.38] | 1.19 [0.51, 1.94] | 0.46 [-0.07, 1.07] |  |  |  | 0.18 [-0.08, 0.43] |  |  |  | 0.20 [-0.07, 0.44] | wt_med |
| 45 |  |  | 0.31 [0.09, 0.53] | -0.01 [-0.16, 0.18] | -0.21 [-0.38, -0.06] | 0.75 [0.16, 1.36] | 0.26 [0.02, 0.46] | 0.14 [-0.23, 0.44] | 1.04 [0.46, 1.79] | 0.32 [-0.26, 1.02] |  |  |  | 0.32 [0.08, 0.57] |  |  |  | 0.41 [0.17, 0.65] | wt_med |
| 60 |  |  | 0.27 [0.04, 0.47] | -0.13 [-0.38, 0.12] | -0.28 [-0.44, -0.14] |  | 0.26 [0.07, 0.44] | 0.50 [0.00, 0.62] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | 0.31 [0.08, 0.52] |  |  |  | 0.43 [0.18, 0.66] | wt_med |
| 90 |  |  | 0.31 [0.12, 0.49] | -0.09 [-0.33, 0.13] | -0.18 [-0.33, -0.06] |  | 0.25 [0.04, 0.48] |  |  |  |  |  |  | 0.34 [0.14, 0.54] |  |  |  | 0.40 [0.19, 0.63] | wt_med |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 41.94 [39.81, 44.22] | 46.86 [44.35, 49.31] | 36.82 [34.54, 39.08] | 39.37 [37.19, 41.80] | 36.83 [34.71, 39.00] | 36.25 [32.87, 39.88] | 37.04 [34.82, 39.44] | 34.62 [31.70, 37.51] |  |  | 44.45 [41.80, 47.06] | 47.25 [44.97, 49.80] | 37.73 [35.56, 40.09] | 36.80 [34.48, 39.13] | 47.00 [44.79, 49.47] | 53.81 [51.22, 56.30] | 47.04 [44.79, 49.53] | 36.96 [34.59, 39.31] | 38.76 [36.31, 41.04] |
| 3 | 42.57 [40.35, 44.90] | 50.45 [47.67, 53.37] | 36.66 [34.31, 39.19] | 39.36 [36.83, 42.04] | 37.26 [34.89, 39.64] | 36.04 [32.83, 39.19] | 37.14 [34.64, 39.70] | 35.30 [32.47, 38.08] | 43.69 [40.62, 46.79] | 42.71 [39.63, 46.11] | 44.39 [41.52, 47.16] | 48.76 [46.13, 51.59] | 38.72 [36.43, 41.21] | 37.25 [34.78, 39.81] | 50.44 [47.76, 53.23] | 54.19 [51.61, 56.88] | 51.17 [48.52, 54.13] | 36.59 [34.28, 39.06] | 38.16 [35.80, 40.72] |
| 7 | 44.55 [42.52, 46.75] | 53.43 [50.83, 56.04] | 36.37 [34.26, 38.38] | 38.18 [35.84, 40.65] | 36.66 [34.57, 38.96] | 34.89 [31.97, 38.06] | 37.02 [34.92, 39.26] | 33.67 [30.97, 36.56] | 41.68 [38.84, 44.69] | 40.90 [37.98, 43.98] |  | 51.75 [49.41, 54.35] | 39.11 [37.07, 41.06] | 36.86 [34.77, 39.07] | 53.55 [51.01, 56.26] | 54.88 [52.44, 57.31] | 54.47 [51.87, 57.28] | 36.64 [34.48, 39.14] | 36.44 [34.47, 38.68] |
| 14 |  |  | 39.54 [37.03, 42.07] | 40.33 [37.89, 42.85] | 39.78 [37.12, 42.31] | 38.18 [34.85, 41.57] | 38.93 [36.52, 41.32] | 38.34 [35.04, 41.29] | 42.92 [40.34, 45.70] | 43.09 [40.33, 45.91] |  |  |  | 39.22 [36.58, 41.79] |  |  |  | 39.32 [36.78, 41.88] | 39.46 [37.07, 41.90] |
| 30 |  |  | 38.67 [36.43, 40.94] | 40.61 [38.13, 43.19] | 37.28 [35.11, 39.55] | 35.09 [31.85, 38.63] | 38.32 [35.91, 40.68] | 36.84 [33.52, 40.25] | 43.08 [39.80, 46.45] | 42.09 [38.77, 45.46] |  |  |  | 37.44 [35.31, 39.79] |  |  |  | 37.38 [35.33, 39.84] | 39.63 [37.22, 42.07] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2.83 [-0.36, 5.92] | 7.73 [4.41, 11.21] | -2.05 [-4.45, 0.08] | 0.87 [-1.65, 3.23] | -1.80 [-4.54, 0.22] | 0.44 [-2.40, 3.49] | -1.67 [-3.75, -0.01]* | -1.92 [-4.28, 0.24] |  |  | 5.45 [2.10, 8.79] | 8.23 [5.57, 10.64] | -1.28 [-3.67, 1.22] | -1.94 [-4.59, 0.19] | 7.88 [4.43, 11.41] | 14.99 [10.57, 20.18] | 7.94 [4.41, 11.57] | -1.81 [-4.43, 0.27] | wt_med |
| 3 | 4.35 [0.51, 7.72] | 12.17 [8.06, 16.21] | -1.49 [-3.67, 0.58] | 0.94 [-1.34, 3.43] | -0.91 [-2.88, 1.10] | -1.79 [-3.92, 0.52] | -1.25 [-3.53, 0.77] | -1.55 [-4.77, 1.16] | 6.62 [3.46, 10.05] | 5.65 [2.61, 8.75] | 6.12 [3.11, 8.93] | 10.44 [6.81, 13.97] | 0.30 [-2.42, 3.20] | -0.92 [-3.17, 1.18] | 12.27 [8.76, 15.96] | 16.20 [11.94, 20.82] | 13.00 [9.55, 16.66] | -1.60 [-3.58, 0.25] | wt_med |
| 7 | 8.00 [5.06, 10.79] | 16.99 [12.74, 20.79] | -0.13 [-1.93, 1.62] | 1.70 [0.10, 3.65] | 0.06 [-1.55, 1.62] | 0.20 [-2.63, 2.93] | 0.61 [-1.27, 2.41] | -0.37 [-2.41, 1.58] | 7.41 [4.73, 10.08] | 6.62 [3.97, 8.74] |  | 15.47 [12.24, 18.59] | 2.50 [0.03, 4.80] | 0.30 [-1.46, 2.29] | 17.28 [12.43, 21.63] | 18.57 [15.23, 22.25] | 18.20 [13.25, 22.67] | 0.12 [-1.57, 2.11] | wt_med |
| 14 |  |  | 0.05 [-1.95, 1.98] | 0.77 [-0.61, 2.18] | 0.28 [-2.10, 2.39] | -0.74 [-4.22, 2.57] | -0.76 [-2.98, 1.47] | -0.05 [-3.30, 3.35] | 4.41 [0.40, 8.53] | 4.58 [1.15, 8.05] |  |  |  | -0.15 [-2.28, 1.95] |  |  |  | -0.14 [-2.32, 2.14] | wt_med |
| 30 |  |  | -0.95 [-2.64, 1.05] | 0.79 [-1.07, 2.75] | -2.27 [-4.32, -0.15]* | -2.43 [-6.31, 2.13] | -1.78 [-3.43, 0.04] | -1.26 [-3.58, 1.29] | 4.80 [0.41, 9.28] | 3.86 [-0.34, 8.31] |  |  |  | -2.24 [-4.45, 0.18] |  |  |  | -2.23 [-4.07, -0.44]* | wt_med |

**dayPeak pairwise ordering (headliners)**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] |  | 0.76 [0.75, 0.77] | 0.80 [0.79, 0.81] | 0.74 [0.72, 0.75] | 0.79 [0.79, 0.80] | 0.80 [0.79, 0.81] |  |  | 0.77 [0.75, 0.78] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.83] |  | 0.83 [0.82, 0.83] | 0.69 [0.68, 0.70] | 0.83 [0.82, 0.83] |  | 0.77 [0.77, 0.78] |
| 1 | 0.82 [0.82, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.77 [0.76, 0.78] | 0.80 [0.79, 0.81] | 0.74 [0.73, 0.75] | 0.80 [0.79, 0.81] | 0.80 [0.80, 0.81] |  |  | 0.77 [0.75, 0.78] | 0.82 [0.81, 0.82] | 0.82 [0.81, 0.82] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.67 [0.66, 0.68] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] | 0.78 [0.77, 0.79] |
| 2 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.72, 0.74] | 0.79 [0.79, 0.80] | 0.80 [0.79, 0.81] |  |  | 0.76 [0.74, 0.77] | 0.81 [0.80, 0.82] | 0.80 [0.80, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.80, 0.81] | 0.66 [0.65, 0.67] | 0.80 [0.80, 0.81] | 0.81 [0.80, 0.82] | 0.77 [0.76, 0.77] |
| 3 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.79 [0.78, 0.80] | 0.75 [0.73, 0.76] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.80, 0.81] | 0.66 [0.65, 0.67] | 0.80 [0.80, 0.81] | 0.81 [0.80, 0.82] | 0.77 [0.76, 0.78] |
| 4 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] | 0.76 [0.74, 0.77] | 0.79 [0.78, 0.80] | 0.77 [0.75, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.66 [0.65, 0.67] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.81] | 0.77 [0.76, 0.78] |
| 5 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.79] | 0.77 [0.75, 0.78] | 0.81 [0.80, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.67 [0.66, 0.68] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.77 [0.76, 0.78] |
| 6 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.71, 0.74] | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.79] | 0.77 [0.75, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.67 [0.66, 0.68] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.81] | 0.77 [0.76, 0.78] |
| 7 | 0.79 [0.79, 0.80] | 0.80 [0.79, 0.81] | 0.80 [0.80, 0.81] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.79] | 0.72 [0.70, 0.73] | 0.79 [0.78, 0.79] | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.80] |  | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] | 0.66 [0.65, 0.67] | 0.79 [0.78, 0.80] | 0.80 [0.79, 0.81] | 0.76 [0.75, 0.77] |
| 10 |  |  | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.79] | 0.72 [0.71, 0.74] | 0.78 [0.78, 0.79] | 0.78 [0.77, 0.80] | 0.73 [0.72, 0.75] | 0.77 [0.76, 0.78] |  |  |  | 0.80 [0.79, 0.81] |  |  |  | 0.80 [0.79, 0.81] | 0.76 [0.75, 0.77] |
| 14 |  |  | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.79] | 0.72 [0.70, 0.73] | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.73 [0.72, 0.74] | 0.77 [0.75, 0.78] |  |  |  | 0.80 [0.79, 0.81] |  |  |  | 0.79 [0.79, 0.80] | 0.76 [0.75, 0.77] |
| 21 |  |  | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.76] | 0.77 [0.76, 0.78] | 0.71 [0.70, 0.73] | 0.78 [0.77, 0.79] | 0.77 [0.76, 0.78] | 0.72 [0.71, 0.74] | 0.76 [0.75, 0.77] |  |  |  | 0.79 [0.78, 0.80] |  |  |  | 0.79 [0.78, 0.80] | 0.76 [0.74, 0.77] |
| 30 |  |  | 0.78 [0.77, 0.79] | 0.75 [0.74, 0.76] | 0.76 [0.75, 0.77] | 0.71 [0.69, 0.72] | 0.76 [0.75, 0.77] | 0.77 [0.75, 0.78] | 0.72 [0.71, 0.74] | 0.76 [0.74, 0.77] |  |  |  | 0.77 [0.76, 0.78] |  |  |  | 0.77 [0.76, 0.78] | 0.75 [0.74, 0.76] |
| 45 |  |  | 0.77 [0.76, 0.78] | 0.75 [0.73, 0.76] | 0.75 [0.74, 0.77] | 0.73 [0.70, 0.76] | 0.76 [0.75, 0.77] | 0.76 [0.75, 0.78] | 0.72 [0.70, 0.73] | 0.75 [0.73, 0.77] |  |  |  | 0.76 [0.75, 0.77] |  |  |  | 0.76 [0.74, 0.77] | 0.74 [0.73, 0.75] |
| 60 |  |  | 0.77 [0.75, 0.78] | 0.74 [0.73, 0.76] | 0.75 [0.74, 0.76] |  | 0.76 [0.74, 0.77] | 0.73 [0.73, 0.73] | 0.64 [0.64, 0.64] | 0.64 [0.64, 0.64] |  |  |  | 0.76 [0.74, 0.77] |  |  |  | 0.76 [0.74, 0.77] | 0.74 [0.72, 0.75] |
| 90 |  |  | 0.78 [0.77, 0.80] | 0.75 [0.73, 0.77] | 0.76 [0.75, 0.78] |  | 0.77 [0.75, 0.78] |  |  |  |  |  |  | 0.77 [0.76, 0.79] |  |  |  | 0.77 [0.76, 0.79] | 0.74 [0.73, 0.76] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.06 [0.05, 0.08]* | 0.06 [0.04, 0.07]* |  | -0.02 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.05, -0.01] | 0.02 [0.01, 0.03]* | 0.04 [0.03, 0.05]* |  |  | -0.01 [-0.03, 0.00] | 0.05 [0.04, 0.06]* | 0.06 [0.04, 0.07]* |  | 0.06 [0.04, 0.08]* | -0.08 [-0.11, -0.05] | 0.06 [0.04, 0.07]* |  | wt_med |
| 1 | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, -0.01] | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* |  |  | -0.01 [-0.03, 0.00] | 0.05 [0.03, 0.06]* | 0.05 [0.03, 0.06]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.10 [-0.13, -0.07] | 0.05 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | wt_med |
| 2 | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.04]* | -0.03 [-0.06, -0.01] | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.06]* |  |  | -0.01 [-0.03, 0.00] | 0.05 [0.04, 0.07]* | 0.05 [0.03, 0.06]* | 0.05 [0.04, 0.07]* | 0.05 [0.03, 0.06]* | -0.10 [-0.13, -0.06] | 0.05 [0.03, 0.06]* | 0.05 [0.04, 0.06]* | wt_med |
| 3 | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, -0.00] | 0.03 [0.02, 0.05]* | 0.04 [0.03, 0.06]* | -0.00 [-0.02, 0.01] | 0.03 [0.01, 0.05]* | -0.01 [-0.03, 0.00] | 0.05 [0.04, 0.06]* | 0.04 [0.03, 0.05]* | 0.05 [0.04, 0.06]* | 0.04 [0.03, 0.06]* | -0.10 [-0.13, -0.07] | 0.04 [0.03, 0.06]* | 0.05 [0.03, 0.06]* | wt_med |
| 4 | 0.05 [0.03, 0.06]* | 0.05 [0.04, 0.06]* | 0.05 [0.03, 0.06]* | -0.02 [-0.03, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, 0.00] | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.01] | 0.03 [0.02, 0.04]* | -0.02 [-0.04, -0.01] | 0.05 [0.03, 0.06]* | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.10 [-0.13, -0.07] | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.06]* | wt_med |
| 5 | 0.05 [0.03, 0.06]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.02 [-0.05, 0.00] | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.00] | 0.03 [0.02, 0.04]* | -0.00 [-0.02, 0.01] | 0.04 [0.03, 0.06]* | 0.03 [0.02, 0.05]* | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.10 [-0.13, -0.06] | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.05]* | wt_med |
| 6 | 0.05 [0.03, 0.06]* | 0.05 [0.03, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.04 [-0.07, -0.01] | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.05]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.03]* | -0.01 [-0.02, 0.01] | 0.05 [0.04, 0.06]* | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.05]* | -0.10 [-0.13, -0.07] | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.05]* | wt_med |
| 7 | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | 0.05 [0.03, 0.06]* | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.04 [-0.07, -0.01] | 0.02 [0.02, 0.04]* | 0.04 [0.02, 0.05]* | -0.00 [-0.02, 0.01] | 0.03 [0.02, 0.05]* |  | 0.05 [0.03, 0.06]* | 0.03 [0.02, 0.05]* | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.05]* | -0.10 [-0.13, -0.06] | 0.04 [0.02, 0.05]* | 0.04 [0.03, 0.06]* | wt_med |
| 10 |  |  | 0.05 [0.03, 0.06]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, -0.00] | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.04]* |  |  |  | 0.04 [0.03, 0.05]* |  |  |  | 0.04 [0.03, 0.05]* | wt_med |
| 14 |  |  | 0.05 [0.03, 0.07]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.07, 0.00] | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.04]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.04]* |  |  |  | 0.05 [0.03, 0.06]* |  |  |  | 0.04 [0.03, 0.06]* | wt_med |
| 21 |  |  | 0.04 [0.03, 0.06]* | -0.01 [-0.02, -0.00] | 0.02 [0.02, 0.03]* | -0.04 [-0.08, 0.00] | 0.02 [0.01, 0.04]* | 0.03 [0.02, 0.04]* | -0.01 [-0.03, 0.01] | 0.02 [0.01, 0.04]* |  |  |  | 0.04 [0.03, 0.06]* |  |  |  | 0.04 [0.02, 0.05]* | wt_med |
| 30 |  |  | 0.03 [0.02, 0.05]* | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, 0.01] | 0.02 [0.01, 0.03]* | 0.03 [0.02, 0.05]* | -0.00 [-0.02, 0.01] | 0.03 [0.01, 0.05]* |  |  |  | 0.03 [0.02, 0.04]* |  |  |  | 0.03 [0.02, 0.04]* | wt_med |
| 45 |  |  | 0.04 [0.03, 0.05]* | 0.00 [-0.01, 0.02] | 0.03 [0.02, 0.03]* | -0.05 [-0.09, -0.01] | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | -0.01 [-0.03, 0.01] | 0.02 [0.00, 0.04] |  |  |  | 0.03 [0.02, 0.05]* |  |  |  | 0.03 [0.02, 0.04]* | wt_med |
| 60 |  |  | 0.04 [0.02, 0.06]* | -0.00 [-0.02, 0.01] | 0.02 [0.02, 0.03]* |  | 0.02 [0.01, 0.04]* | -0.09 [-0.09, -0.09] | -0.18 [-0.18, -0.18] | -0.18 [-0.18, -0.18] |  |  |  | 0.03 [0.01, 0.05]* |  |  |  | 0.03 [0.02, 0.04]* | wt_med |
| 90 |  |  | 0.05 [0.02, 0.07]* | -0.00 [-0.02, 0.01] | 0.02 [0.02, 0.04]* |  | 0.02 [0.01, 0.04]* |  |  |  |  |  |  | 0.04 [0.02, 0.06]* |  |  |  | 0.04 [0.02, 0.06]* | wt_med |

## D5 first-hour MAE vs EACH naive candidate

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.913 [-1.289, -0.556]* / [-1.158, -0.672]* | -0.522 [-0.802, -0.276]* / [-0.795, -0.274]* | -1.048 [-1.551, -0.632]* / [-1.298, -0.793]* | -1.046 [-1.551, -0.630]* / [-1.296, -0.792]* | -0.932 [-1.617, -0.529]* / [-1.214, -0.680]* | -0.879 [-1.560, -0.468]* / [-1.185, -0.586]* |  |  |
| 1 | -0.827 [-1.235, -0.546]* / [-1.056, -0.608]* | -0.465 [-0.891, -0.110]* / [-0.720, -0.214]* | -1.000 [-1.649, -0.602]* / [-1.254, -0.764]* | -1.002 [-1.649, -0.602]* / [-1.256, -0.768]* | -0.898 [-1.702, -0.440]* / [-1.212, -0.641]* | -0.896 [-1.807, -0.392]* / [-1.280, -0.593]* |  |  |
| 2 | -0.729 [-1.043, -0.427]* / [-0.940, -0.507]* | -0.414 [-0.700, -0.126]* / [-0.641, -0.167]* | -0.908 [-1.393, -0.509]* / [-1.129, -0.687]* | -0.908 [-1.393, -0.509]* / [-1.129, -0.688]* | -0.843 [-1.333, -0.465]* / [-1.089, -0.593]* | -0.842 [-1.383, -0.431]* / [-1.116, -0.557]* |  |  |
| 3 | -0.724 [-1.037, -0.396]* / [-0.913, -0.524]* | -0.424 [-0.735, -0.136]* / [-0.633, -0.216]* | -0.901 [-1.424, -0.491]* / [-1.107, -0.682]* | -0.905 [-1.434, -0.491]* / [-1.112, -0.687]* | -0.675 [-1.343, -0.219]* / [-0.926, -0.433]* | -0.623 [-1.285, -0.102]* / [-0.907, -0.353]* | 1.178 [0.611, 1.925] / [0.890, 1.448] | 1.260 [0.715, 1.957] / [0.962, 1.529] |
| 4 | -0.571 [-0.844, -0.300]* / [-0.763, -0.376]* | -0.202 [-0.488, 0.106] / [-0.434, 0.002] | -0.699 [-1.050, -0.401]* / [-0.895, -0.509]* | -0.698 [-1.042, -0.397]* / [-0.895, -0.507]* | -0.651 [-1.049, -0.335]* / [-0.903, -0.410]* | -0.473 [-0.809, -0.145]* / [-0.720, -0.239]* | 1.149 [0.686, 1.700] / [0.882, 1.395] | 1.276 [0.819, 1.822] / [1.014, 1.533] |
| 5 | -0.754 [-1.105, -0.413]* / [-1.014, -0.503]* | -0.466 [-0.780, -0.173]* / [-0.760, -0.194]* | -0.898 [-1.397, -0.485]* / [-1.168, -0.642]* | -0.898 [-1.396, -0.485]* / [-1.168, -0.642]* | -0.827 [-1.471, -0.431]* / [-1.116, -0.560]* | -0.794 [-1.486, -0.372]* / [-1.134, -0.479]* | 1.259 [0.724, 1.936] / [0.950, 1.571] | 1.315 [0.807, 1.930] / [1.001, 1.631] |
| 6 | -0.670 [-1.078, -0.382]* / [-0.905, -0.450]* | -0.340 [-0.723, 0.036] / [-0.608, -0.084]*  ! | -0.840 [-1.470, -0.443]* / [-1.088, -0.610]* | -0.844 [-1.474, -0.443]* / [-1.099, -0.615]* | -0.790 [-1.559, -0.339]* / [-1.125, -0.527]* | -0.748 [-1.617, -0.247]* / [-1.146, -0.448]* | 1.278 [0.809, 1.837] / [0.957, 1.551] | 1.380 [0.908, 1.905] / [1.052, 1.663] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.913 [0.556, 1.289] / [0.672, 1.158] | 0.385 [0.200, 0.607] / [0.301, 0.467] | -0.152 [-0.339, -0.030]* / [-0.216, -0.092]* | -0.150 [-0.338, -0.028]* / [-0.213, -0.089]* | -0.174 [-0.418, -0.025]* / [-0.272, -0.080]* | -0.159 [-0.415, -0.008]* / [-0.274, -0.054]* |  |  |
| 1 | 0.827 [0.546, 1.235] / [0.608, 1.056] | 0.333 [0.147, 0.561] / [0.247, 0.420] | -0.164 [-0.389, -0.019]* / [-0.235, -0.107]* | -0.166 [-0.390, -0.020]* / [-0.239, -0.108]* | -0.215 [-0.570, -0.011]* / [-0.323, -0.128]* | -0.185 [-0.512, 0.002] / [-0.304, -0.082]*  ! |  |  |
| 2 | 0.729 [0.427, 1.043] / [0.507, 0.940] | 0.270 [0.103, 0.475] / [0.175, 0.360] | -0.162 [-0.392, -0.008]* / [-0.236, -0.093]* | -0.162 [-0.393, -0.008]* / [-0.236, -0.093]* | -0.198 [-0.526, 0.026] / [-0.307, -0.098]*  ! | -0.204 [-0.547, 0.027] / [-0.336, -0.090]*  ! |  |  |
| 3 | 0.724 [0.396, 1.037] / [0.524, 0.913] | 0.296 [0.132, 0.493] / [0.203, 0.385] | -0.197 [-0.461, -0.026]* / [-0.277, -0.126]* | -0.200 [-0.462, -0.029]* / [-0.280, -0.128]* | -0.242 [-0.646, -0.005]* / [-0.358, -0.137]* | -0.269 [-0.702, -0.009]* / [-0.428, -0.130]* | 1.685 [1.215, 2.254] / [1.458, 1.890] | 1.778 [1.323, 2.322] / [1.542, 1.990] |
| 4 | 0.571 [0.300, 0.844] / [0.376, 0.763] | 0.326 [0.138, 0.548] / [0.242, 0.412] | -0.161 [-0.341, -0.027]* / [-0.237, -0.093]* | -0.160 [-0.339, -0.027]* / [-0.236, -0.092]* | -0.204 [-0.501, -0.013]* / [-0.311, -0.113]* | -0.185 [-0.461, -0.002]* / [-0.310, -0.080]* | 1.738 [1.268, 2.314] / [1.530, 1.950] | 1.859 [1.421, 2.423] / [1.649, 2.081] |
| 5 | 0.754 [0.413, 1.105] / [0.503, 1.014] | 0.301 [0.122, 0.531] / [0.220, 0.388] | -0.163 [-0.351, -0.030]* / [-0.234, -0.094]* | -0.162 [-0.350, -0.029]* / [-0.234, -0.093]* | -0.198 [-0.483, -0.024]* / [-0.307, -0.097]* | -0.197 [-0.510, -0.014]* / [-0.324, -0.084]* | 1.943 [1.395, 2.612] / [1.724, 2.202] | 2.013 [1.505, 2.605] / [1.790, 2.256] |
| 6 | 0.670 [0.382, 1.078] / [0.450, 0.905] | 0.282 [0.075, 0.532] / [0.184, 0.373] | -0.176 [-0.386, -0.031]* / [-0.259, -0.113]* | -0.181 [-0.389, -0.032]* / [-0.259, -0.115]* | -0.227 [-0.579, -0.018]* / [-0.355, -0.126]* | -0.195 [-0.509, -0.012]* / [-0.335, -0.087]* | 1.842 [1.383, 2.403] / [1.605, 2.073] | 1.952 [1.502, 2.479] / [1.707, 2.185] |
| 7 |  | 0.161 [-0.032, 0.379] / [0.069, 0.253] | -0.177 [-0.390, -0.031]* / [-0.255, -0.110]* | -0.178 [-0.391, -0.031]* / [-0.256, -0.111]* | -0.222 [-0.538, -0.010]* / [-0.337, -0.121]* | -0.228 [-0.558, -0.014]* / [-0.366, -0.108]* | 1.880 [1.394, 2.499] / [1.645, 2.128] | 1.936 [1.491, 2.466] / [1.705, 2.191] |
| 10 |  | 0.208 [0.041, 0.416] / [0.112, 0.296] | -0.177 [-0.367, -0.038]* / [-0.250, -0.110]* | -0.177 [-0.367, -0.037]* / [-0.250, -0.110]* | -0.200 [-0.459, -0.027]* / [-0.315, -0.102]* | -0.230 [-0.546, -0.021]* / [-0.368, -0.114]* | 1.915 [1.380, 2.563] / [1.695, 2.167] | 1.987 [1.478, 2.587] / [1.753, 2.241] |
| 14 |  | 0.181 [0.005, 0.370] / [0.089, 0.277] | -0.200 [-0.426, -0.039]* / [-0.284, -0.134]* | -0.200 [-0.427, -0.039]* / [-0.285, -0.134]* | -0.248 [-0.570, -0.031]* / [-0.371, -0.143]* | -0.234 [-0.567, -0.013]* / [-0.374, -0.112]* | 1.707 [1.277, 2.222] / [1.496, 1.949] | 1.829 [1.405, 2.329] / [1.606, 2.080] |
| 21 |  | 0.180 [-0.019, 0.416] / [0.081, 0.279] | -0.169 [-0.347, -0.036]* / [-0.238, -0.110]* | -0.170 [-0.349, -0.036]* / [-0.238, -0.110]* | -0.158 [-0.334, -0.030]* / [-0.254, -0.083]* | -0.143 [-0.320, -0.023]* / [-0.256, -0.056]* | 1.916 [1.475, 2.446] / [1.662, 2.163] | 2.071 [1.629, 2.598] / [1.807, 2.332] |
| 30 |  | 0.179 [-0.015, 0.426] / [0.078, 0.277] | -0.165 [-0.353, -0.033]* / [-0.234, -0.097]* | -0.163 [-0.349, -0.032]* / [-0.233, -0.097]* | -0.190 [-0.431, -0.034]* / [-0.322, -0.081]* | -0.190 [-0.432, -0.022]* / [-0.321, -0.074]* | 1.720 [1.222, 2.285] / [1.452, 2.029] | 1.782 [1.335, 2.293] / [1.495, 2.090] |
| 45 |  | 0.131 [-0.062, 0.409] / [0.027, 0.240] | -0.124 [-0.268, -0.021]* / [-0.177, -0.073]* | -0.117 [-0.250, -0.021]* / [-0.168, -0.067]* | -0.179 [-0.407, -0.022]* / [-0.301, -0.075]* | -0.089 [-0.337, 0.035] / [-0.195, -0.009]*  ! | 1.994 [1.454, 2.634] / [1.646, 2.378] | 2.076 [1.507, 2.808] / [1.705, 2.477] |
| 60 |  | 0.116 [-0.168, 0.471] / [-0.002, 0.226] | -0.093 [-0.219, -0.013]* / [-0.140, -0.050]* | -0.071 [-0.189, 0.018] / [-0.126, -0.015]*  ! | 0.000 [0.000, 0.000] / [0.000, 0.000] |  | -0.004 [-0.005, 0.000] / [-0.011, 0.000] | 0.023 [0.000, 2.448] / [0.000, 0.054] |
| 90 |  | 0.051 [-0.296, 0.493] / [-0.115, 0.199] | -0.121 [-0.265, -0.020]* / [-0.183, -0.063]* | -0.104 [-0.247, 0.007] / [-0.178, -0.038]*  ! |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.522 [0.276, 0.802] / [0.274, 0.795] | -0.385 [-0.607, -0.200]* / [-0.467, -0.301]* | -0.532 [-0.858, -0.292]* / [-0.637, -0.437]* | -0.530 [-0.855, -0.289]* / [-0.634, -0.435]* | -0.509 [-0.939, -0.203]* / [-0.643, -0.377]* | -0.553 [-1.040, -0.204]* / [-0.709, -0.401]* |  |  |
| 1 | 0.465 [0.110, 0.891] / [0.214, 0.720] | -0.333 [-0.561, -0.147]* / [-0.420, -0.247]* | -0.501 [-0.824, -0.262]* / [-0.602, -0.400]* | -0.504 [-0.830, -0.262]* / [-0.609, -0.400]* | -0.485 [-0.896, -0.189]* / [-0.620, -0.350]* | -0.483 [-0.924, -0.174]* / [-0.630, -0.333]* |  |  |
| 2 | 0.414 [0.126, 0.700] / [0.167, 0.641] | -0.270 [-0.475, -0.103]* / [-0.360, -0.175]* | -0.436 [-0.739, -0.215]* / [-0.540, -0.329]* | -0.436 [-0.740, -0.215]* / [-0.539, -0.328]* | -0.447 [-0.862, -0.151]* / [-0.589, -0.309]* | -0.493 [-0.956, -0.171]* / [-0.647, -0.346]* |  |  |
| 3 | 0.424 [0.136, 0.735] / [0.216, 0.633] | -0.296 [-0.493, -0.132]* / [-0.385, -0.203]* | -0.495 [-0.823, -0.255]* / [-0.608, -0.381]* | -0.499 [-0.823, -0.260]* / [-0.610, -0.385]* | -0.516 [-0.981, -0.220]* / [-0.670, -0.362]* | -0.585 [-1.138, -0.224]* / [-0.774, -0.402]* | 1.378 [0.942, 1.908] / [1.146, 1.596] | 1.468 [1.056, 1.975] / [1.227, 1.692] |
| 4 | 0.202 [-0.106, 0.488] / [-0.002, 0.434] | -0.326 [-0.548, -0.138]* / [-0.412, -0.242]* | -0.499 [-0.824, -0.254]* / [-0.602, -0.394]* | -0.498 [-0.821, -0.254]* / [-0.600, -0.393]* | -0.518 [-0.945, -0.220]* / [-0.659, -0.382]* | -0.488 [-0.964, -0.172]* / [-0.654, -0.328]* | 1.398 [1.027, 1.875] / [1.184, 1.619] | 1.520 [1.157, 1.979] / [1.308, 1.747] |
| 5 | 0.466 [0.173, 0.780] / [0.194, 0.760] | -0.301 [-0.531, -0.122]* / [-0.388, -0.220]* | -0.458 [-0.797, -0.217]* / [-0.562, -0.357]* | -0.457 [-0.795, -0.216]* / [-0.562, -0.356]* | -0.520 [-0.964, -0.196]* / [-0.670, -0.385]* | -0.551 [-1.084, -0.168]* / [-0.725, -0.387]* | 1.578 [1.053, 2.150] / [1.365, 1.848] | 1.649 [1.185, 2.166] / [1.431, 1.908] |
| 6 | 0.340 [-0.036, 0.723] / [0.084, 0.608] | -0.282 [-0.532, -0.075]* / [-0.373, -0.184]* | -0.467 [-0.815, -0.218]* / [-0.576, -0.364]* | -0.472 [-0.828, -0.219]* / [-0.580, -0.370]* | -0.505 [-0.931, -0.196]* / [-0.649, -0.363]* | -0.480 [-0.929, -0.148]* / [-0.637, -0.327]* | 1.506 [1.075, 1.968] / [1.272, 1.726] | 1.614 [1.195, 2.059] / [1.381, 1.842] |
| 7 |  | -0.161 [-0.379, 0.032] / [-0.253, -0.069]*  ! | -0.343 [-0.622, -0.128]* / [-0.443, -0.238]* | -0.344 [-0.627, -0.128]* / [-0.444, -0.239]* | -0.393 [-0.796, -0.100]* / [-0.528, -0.260]* | -0.428 [-0.862, -0.116]* / [-0.589, -0.278]* | 1.660 [1.180, 2.189] / [1.419, 1.924] | 1.714 [1.251, 2.202] / [1.473, 1.983] |
| 10 |  | -0.208 [-0.416, -0.041]* / [-0.296, -0.112]* | -0.375 [-0.692, -0.155]* / [-0.482, -0.269]* | -0.374 [-0.691, -0.154]* / [-0.481, -0.268]* | -0.479 [-0.930, -0.161]* / [-0.627, -0.336]* | -0.521 [-1.073, -0.137]* / [-0.690, -0.348]* | 1.567 [1.067, 2.092] / [1.338, 1.799] | 1.636 [1.206, 2.135] / [1.401, 1.870] |
| 14 |  | -0.181 [-0.370, -0.005]* / [-0.277, -0.089]* | -0.381 [-0.705, -0.159]* / [-0.495, -0.271]* | -0.381 [-0.706, -0.159]* / [-0.495, -0.272]* | -0.441 [-0.881, -0.136]* / [-0.602, -0.299]* | -0.457 [-0.963, -0.114]* / [-0.645, -0.294]* | 1.468 [1.069, 1.929] / [1.228, 1.731] | 1.589 [1.175, 2.045] / [1.346, 1.853] |
| 21 |  | -0.180 [-0.416, 0.019] / [-0.279, -0.081]*  ! | -0.331 [-0.673, -0.079]* / [-0.441, -0.221]* | -0.331 [-0.673, -0.079]* / [-0.442, -0.222]* | -0.325 [-0.740, -0.027]* / [-0.477, -0.193]* | -0.301 [-0.721, 0.014] / [-0.478, -0.141]*  ! | 1.732 [1.281, 2.215] / [1.464, 1.973] | 1.880 [1.426, 2.383] / [1.604, 2.146] |
| 30 |  | -0.179 [-0.426, 0.015] / [-0.277, -0.078]*  ! | -0.327 [-0.697, -0.073]* / [-0.441, -0.207]* | -0.326 [-0.693, -0.071]* / [-0.439, -0.206]* | -0.383 [-0.832, -0.047]* / [-0.554, -0.207]* | -0.424 [-0.965, -0.037]* / [-0.618, -0.244]* | 1.517 [0.965, 2.007] / [1.213, 1.827] | 1.577 [1.074, 2.038] / [1.275, 1.896] |
| 45 |  | -0.131 [-0.409, 0.062] / [-0.240, -0.027]*  ! | -0.251 [-0.617, -0.011]* / [-0.372, -0.135]* | -0.244 [-0.600, -0.005]* / [-0.361, -0.129]* | -0.371 [-0.870, -0.028]* / [-0.581, -0.163]* | -1.277 [-2.616, -0.409] / [-1.684, -0.906]*  ! | 1.780 [1.206, 2.417] / [1.403, 2.197] | 1.861 [1.255, 2.524] / [1.446, 2.294] |
| 60 |  | -0.116 [-0.471, 0.168] / [-0.226, 0.002] | -0.215 [-0.663, 0.122] / [-0.339, -0.085]*  ! | -0.191 [-0.618, 0.142] / [-0.321, -0.055]*  ! | 0.012 [0.000, 0.013] / [0.000, 0.033] |  | 0.008 [0.000, 0.009] / [0.000, 0.022] | 0.036 [0.000, 2.448] / [0.000, 0.086] |
| 90 |  | -0.051 [-0.493, 0.296] / [-0.199, 0.115] | -0.140 [-0.639, 0.240] / [-0.293, 0.039] | -0.126 [-0.630, 0.271] / [-0.281, 0.052] |  |  |  |  |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 5.13 [4.87, 5.39] | 5.96 [5.65, 6.26] |  | 5.00 [4.72, 5.28] | 4.51 [4.26, 4.76] | 4.22 [3.93, 4.56] | 4.51 [4.26, 4.76] | 4.26 [3.97, 4.57] | 4.50 [4.25, 4.76] | 4.34 [4.10, 4.57] |  |  | 5.59 [5.23, 5.97] | 6.09 [5.77, 6.41] | 4.98 [4.71, 5.27] |  | 6.22 [5.90, 6.53] | 8.86 [8.39, 9.32] | 6.24 [5.92, 6.55] |  | 4.64 [4.37, 4.89] |
| 1 | 5.38 [5.13, 5.63] | 6.30 [5.98, 6.62] | 4.42 [4.18, 4.66] | 4.85 [4.58, 5.12] | 4.42 [4.18, 4.66] | 4.10 [3.81, 4.39] | 4.42 [4.18, 4.66] | 4.11 [3.85, 4.36] | 4.42 [4.18, 4.65] | 4.09 [3.88, 4.31] |  |  | 5.45 [5.10, 5.82] | 6.42 [6.11, 6.73] | 5.11 [4.84, 5.40] | 4.42 [4.18, 4.66] | 6.33 [6.03, 6.66] | 9.02 [8.57, 9.48] | 6.35 [6.05, 6.68] | 4.42 [4.18, 4.66] | 4.56 [4.30, 4.82] |
| 2 | 5.73 [5.46, 6.02] | 6.73 [6.38, 7.10] | 4.56 [4.33, 4.81] | 4.94 [4.68, 5.23] | 4.56 [4.33, 4.81] | 4.28 [3.98, 4.61] | 4.56 [4.33, 4.81] | 4.24 [3.96, 4.55] | 4.55 [4.32, 4.81] | 4.34 [4.11, 4.57] |  |  | 5.46 [5.15, 5.78] | 6.79 [6.45, 7.14] | 5.34 [5.09, 5.64] | 4.56 [4.33, 4.81] | 6.93 [6.57, 7.28] | 9.19 [8.75, 9.65] | 6.95 [6.59, 7.31] | 4.56 [4.33, 4.81] | 4.71 [4.46, 4.99] |
| 3 | 6.08 [5.78, 6.38] | 7.08 [6.70, 7.46] | 4.70 [4.46, 4.94] | 5.10 [4.82, 5.37] | 4.69 [4.45, 4.94] | 4.36 [4.06, 4.68] | 4.69 [4.45, 4.93] | 4.36 [4.09, 4.67] | 4.66 [4.42, 4.89] | 4.37 [4.14, 4.61] | 6.35 [5.99, 6.73] | 6.45 [6.08, 6.82] | 5.55 [5.22, 5.92] | 7.15 [6.78, 7.53] | 5.66 [5.37, 5.98] | 4.69 [4.46, 4.94] | 7.36 [6.97, 7.76] | 9.33 [8.86, 9.80] | 7.37 [6.99, 7.78] | 4.69 [4.46, 4.94] | 4.87 [4.62, 5.14] |
| 4 | 6.12 [5.82, 6.45] | 7.18 [6.81, 7.57] | 4.68 [4.42, 4.95] | 5.10 [4.83, 5.39] | 4.68 [4.42, 4.95] | 4.46 [4.12, 4.77] | 4.68 [4.42, 4.95] | 4.37 [4.09, 4.66] | 4.68 [4.41, 4.94] | 4.43 [4.20, 4.68] | 6.33 [5.98, 6.68] | 6.45 [6.10, 6.80] | 5.43 [5.12, 5.77] | 7.25 [6.90, 7.65] | 5.60 [5.33, 5.90] | 4.68 [4.42, 4.95] | 7.56 [7.19, 7.97] | 9.41 [8.96, 9.89] | 7.57 [7.21, 7.98] | 4.68 [4.42, 4.95] | 4.82 [4.56, 5.09] |
| 5 | 6.20 [5.89, 6.50] | 7.37 [6.96, 7.74] | 4.65 [4.39, 4.90] | 5.03 [4.75, 5.32] | 4.65 [4.39, 4.90] | 4.37 [4.06, 4.71] | 4.65 [4.39, 4.90] | 4.33 [4.04, 4.66] | 4.64 [4.39, 4.90] | 4.33 [4.08, 4.55] | 6.47 [6.09, 6.92] | 6.54 [6.17, 6.98] | 5.58 [5.21, 5.95] | 7.48 [7.10, 7.86] | 5.68 [5.38, 6.00] | 4.65 [4.39, 4.90] | 7.66 [7.26, 8.05] | 9.29 [8.83, 9.77] | 7.68 [7.27, 8.07] | 4.65 [4.39, 4.90] | 4.80 [4.51, 5.06] |
| 6 | 6.24 [5.95, 6.57] | 7.37 [6.97, 7.76] | 4.54 [4.29, 4.78] | 4.93 [4.64, 5.22] | 4.54 [4.30, 4.79] | 4.27 [3.96, 4.59] | 4.54 [4.29, 4.79] | 4.22 [3.95, 4.50] | 4.54 [4.29, 4.79] | 4.08 [3.87, 4.30] | 6.28 [5.89, 6.66] | 6.39 [6.00, 6.77] | 5.43 [5.09, 5.82] | 7.44 [7.06, 7.82] | 5.62 [5.31, 5.95] | 4.54 [4.30, 4.79] | 7.71 [7.31, 8.11] | 9.12 [8.63, 9.56] | 7.73 [7.32, 8.13] | 4.54 [4.29, 4.78] | 4.70 [4.43, 4.97] |
| 7 | 6.56 [6.24, 6.91] | 7.61 [7.21, 8.03] | 4.72 [4.48, 5.01] | 4.99 [4.72, 5.29] | 4.71 [4.48, 5.01] | 4.42 [4.11, 4.80] | 4.71 [4.47, 5.00] | 4.39 [4.10, 4.72] | 4.71 [4.46, 5.00] | 4.67 [4.44, 4.94] | 6.45 [6.09, 6.87] | 6.51 [6.16, 6.92] |  | 7.78 [7.38, 8.18] | 5.97 [5.67, 6.29] | 4.72 [4.48, 5.01] | 8.10 [7.60, 8.69] | 9.13 [8.70, 9.57] | 8.12 [7.62, 8.70] | 4.72 [4.48, 5.01] | 4.88 [4.62, 5.16] |
| 10 |  |  | 4.78 [4.50, 5.05] | 5.08 [4.80, 5.37] | 4.78 [4.50, 5.05] | 4.54 [4.21, 4.91] | 4.78 [4.50, 5.05] | 4.49 [4.18, 4.85] | 4.77 [4.50, 5.05] | 4.66 [4.41, 4.91] | 6.60 [6.21, 7.01] | 6.67 [6.28, 7.07] |  |  |  | 4.78 [4.50, 5.05] |  |  |  | 4.78 [4.50, 5.05] | 4.96 [4.65, 5.26] |
| 14 |  |  | 4.92 [4.64, 5.21] | 5.22 [4.94, 5.51] | 4.92 [4.65, 5.21] | 4.70 [4.33, 5.07] | 4.92 [4.65, 5.21] | 4.59 [4.26, 4.90] | 4.91 [4.64, 5.20] | 4.94 [4.69, 5.22] | 6.56 [6.17, 6.95] | 6.69 [6.29, 7.07] |  |  |  | 4.92 [4.64, 5.21] |  |  |  | 4.92 [4.64, 5.21] | 5.11 [4.82, 5.40] |
| 21 |  |  | 4.84 [4.57, 5.14] | 5.11 [4.81, 5.41] | 4.85 [4.57, 5.15] | 4.62 [4.25, 5.01] | 4.84 [4.57, 5.14] | 4.50 [4.18, 4.83] | 4.84 [4.57, 5.13] | 4.79 [4.54, 5.07] | 6.63 [6.20, 7.06] | 6.78 [6.35, 7.22] |  |  |  | 4.84 [4.57, 5.14] |  |  |  | 4.84 [4.57, 5.13] | 5.00 [4.71, 5.30] |
| 30 |  |  | 4.94 [4.65, 5.23] | 5.22 [4.91, 5.53] | 4.94 [4.65, 5.23] | 4.94 [4.52, 5.40] | 4.94 [4.65, 5.23] | 4.89 [4.46, 5.38] | 4.93 [4.64, 5.22] | 5.20 [4.92, 5.46] | 6.87 [6.39, 7.35] | 6.93 [6.46, 7.38] |  |  |  | 4.94 [4.65, 5.23] |  |  |  | 4.94 [4.65, 5.23] | 5.12 [4.82, 5.41] |
| 45 |  |  | 5.07 [4.76, 5.38] | 5.29 [4.98, 5.63] | 5.07 [4.77, 5.38] | 5.56 [4.88, 6.38] | 5.07 [4.77, 5.39] | 5.12 [4.59, 5.76] | 5.05 [4.75, 5.35] | 5.25 [4.93, 5.55] | 7.35 [6.70, 7.99] | 7.43 [6.82, 8.06] |  |  |  | 5.07 [4.77, 5.38] |  |  |  | 5.07 [4.77, 5.38] | 5.20 [4.89, 5.50] |
| 60 |  |  | 5.21 [4.89, 5.56] | 5.39 [5.05, 5.77] | 5.22 [4.89, 5.56] |  | 5.24 [4.91, 5.59] | 0.08 [0.00, 0.21] | 5.18 [4.86, 5.54] | 5.39 [5.04, 5.73] | 0.08 [0.00, 0.20] | 0.10 [0.00, 0.26] |  |  |  | 5.22 [4.89, 5.56] |  |  |  | 5.21 [4.88, 5.55] | 5.28 [4.95, 5.65] |
| 90 |  |  | 5.43 [5.05, 5.83] | 5.53 [5.13, 5.98] | 5.43 [5.06, 5.83] |  | 5.46 [5.09, 5.86] |  | 5.41 [5.04, 5.82] | 5.66 [5.26, 6.04] |  |  |  |  |  | 5.43 [5.06, 5.83] |  |  |  | 5.43 [5.07, 5.84] | 5.53 [5.14, 5.97] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.45 [0.22, 0.69] | 1.29 [0.86, 1.76] |  | 0.39 [0.20, 0.61] | -0.15 [-0.34, -0.03]* | -0.16 [-0.42, -0.01]* | -0.15 [-0.34, -0.03]* | -0.17 [-0.42, -0.03]* | -0.15 [-0.34, -0.03]* | -0.32 [-0.91, 0.17] |  |  | 0.91 [0.56, 1.29] | 1.42 [0.94, 1.88] | 0.30 [0.02, 0.59] |  | 1.55 [1.08, 2.05] | 4.24 [2.88, 5.67] | 1.57 [1.10, 2.08] |  | wt_med |
| 1 | 0.78 [0.48, 1.10] | 1.70 [1.08, 2.37] | -0.17 [-0.39, -0.02]* | 0.33 [0.15, 0.56] | -0.16 [-0.39, -0.02]* | -0.18 [-0.51, 0.00] | -0.17 [-0.39, -0.02]* | -0.21 [-0.57, -0.01]* | -0.16 [-0.39, -0.02]* | -0.46 [-1.08, 0.06] |  |  | 0.83 [0.55, 1.24] | 1.81 [1.24, 2.40] | 0.50 [0.24, 0.78] | -0.17 [-0.39, -0.02]* | 1.71 [1.19, 2.29] | 4.45 [3.18, 5.80] | 1.74 [1.21, 2.31] | -0.17 [-0.39, -0.02]* | wt_med |
| 2 | 0.98 [0.60, 1.36] | 1.99 [1.21, 2.88] | -0.16 [-0.39, -0.01]* | 0.27 [0.10, 0.47] | -0.16 [-0.39, -0.01]* | -0.20 [-0.55, 0.03] | -0.16 [-0.39, -0.01]* | -0.20 [-0.53, 0.03] | -0.17 [-0.40, -0.01]* | -0.36 [-1.04, 0.17] |  |  | 0.73 [0.43, 1.04] | 2.05 [1.34, 2.81] | 0.61 [0.29, 0.93] | -0.16 [-0.39, -0.01]* | 2.19 [1.48, 3.00] | 4.49 [3.18, 5.88] | 2.21 [1.50, 3.03] | -0.16 [-0.39, -0.01]* | wt_med |
| 3 | 1.15 [0.69, 1.61] | 2.13 [1.33, 2.98] | -0.19 [-0.46, -0.02]* | 0.30 [0.13, 0.49] | -0.20 [-0.46, -0.03]* | -0.27 [-0.70, -0.01]* | -0.20 [-0.46, -0.03]* | -0.24 [-0.65, -0.01]* | -0.21 [-0.47, -0.04]* | -0.48 [-1.01, -0.03]* | 1.69 [1.21, 2.25] | 1.78 [1.32, 2.32] | 0.72 [0.40, 1.04] | 2.23 [1.46, 3.04] | 0.74 [0.41, 1.05] | -0.19 [-0.46, -0.02]* | 2.41 [1.62, 3.24] | 4.45 [3.22, 5.80] | 2.43 [1.65, 3.26] | -0.19 [-0.46, -0.02]* | wt_med |
| 4 | 1.23 [0.73, 1.74] | 2.28 [1.38, 3.22] | -0.16 [-0.34, -0.03]* | 0.33 [0.14, 0.55] | -0.16 [-0.34, -0.03]* | -0.19 [-0.46, -0.00]* | -0.16 [-0.34, -0.03]* | -0.20 [-0.50, -0.01]* | -0.16 [-0.34, -0.03]* | -0.41 [-0.99, 0.12] | 1.74 [1.27, 2.31] | 1.86 [1.42, 2.42] | 0.57 [0.30, 0.84] | 2.37 [1.53, 3.25] | 0.72 [0.40, 1.01] | -0.16 [-0.34, -0.03]* | 2.63 [1.76, 3.55] | 4.58 [3.31, 5.96] | 2.65 [1.78, 3.56] | -0.16 [-0.34, -0.03]* | wt_med |
| 5 | 1.36 [0.83, 1.88] | 2.53 [1.61, 3.55] | -0.16 [-0.35, -0.03]* | 0.30 [0.12, 0.53] | -0.16 [-0.35, -0.03]* | -0.20 [-0.51, -0.01]* | -0.16 [-0.35, -0.03]* | -0.20 [-0.48, -0.02]* | -0.16 [-0.35, -0.03]* | -0.49 [-1.14, 0.04] | 1.94 [1.39, 2.61] | 2.01 [1.50, 2.61] | 0.75 [0.41, 1.10] | 2.65 [1.69, 3.65] | 0.84 [0.47, 1.19] | -0.16 [-0.35, -0.03]* | 2.83 [1.90, 3.81] | 4.50 [3.12, 5.91] | 2.84 [1.91, 3.82] | -0.16 [-0.35, -0.03]* | wt_med |
| 6 | 1.49 [0.95, 2.06] | 2.63 [1.65, 3.68] | -0.18 [-0.39, -0.03]* | 0.28 [0.07, 0.53] | -0.18 [-0.39, -0.03]* | -0.19 [-0.51, -0.01]* | -0.18 [-0.39, -0.03]* | -0.23 [-0.58, -0.02]* | -0.18 [-0.39, -0.03]* | -0.63 [-1.27, -0.07]* | 1.84 [1.38, 2.40] | 1.95 [1.50, 2.48] | 0.67 [0.38, 1.08] | 2.70 [1.76, 3.68] | 0.87 [0.53, 1.21] | -0.18 [-0.39, -0.03]* | 2.95 [1.94, 4.01] | 4.41 [3.13, 5.72] | 2.96 [1.96, 4.01] | -0.18 [-0.39, -0.03]* | wt_med |
| 7 | 1.65 [1.02, 2.28] | 2.71 [1.68, 3.82] | -0.18 [-0.39, -0.03]* | 0.16 [-0.03, 0.38] | -0.18 [-0.39, -0.03]* | -0.23 [-0.56, -0.01]* | -0.18 [-0.39, -0.03]* | -0.22 [-0.54, -0.01]* | -0.18 [-0.40, -0.03]* | -0.19 [-0.86, 0.36] | 1.88 [1.39, 2.50] | 1.94 [1.49, 2.47] |  | 2.88 [1.86, 3.96] | 1.05 [0.62, 1.49] | -0.17 [-0.38, -0.03]* | 3.20 [2.01, 4.39] | 4.26 [2.99, 5.61] | 3.22 [2.03, 4.40] | -0.18 [-0.38, -0.03]* | wt_med |
| 10 |  |  | -0.18 [-0.37, -0.04]* | 0.21 [0.04, 0.42] | -0.18 [-0.37, -0.04]* | -0.23 [-0.55, -0.02]* | -0.18 [-0.37, -0.04]* | -0.20 [-0.46, -0.03]* | -0.18 [-0.37, -0.04]* | -0.28 [-0.89, 0.23] | 1.91 [1.38, 2.56] | 1.99 [1.48, 2.59] |  |  |  | -0.18 [-0.37, -0.04]* |  |  |  | -0.18 [-0.37, -0.04]* | wt_med |
| 14 |  |  | -0.20 [-0.43, -0.04]* | 0.18 [0.01, 0.37] | -0.20 [-0.43, -0.04]* | -0.23 [-0.57, -0.01]* | -0.20 [-0.43, -0.04]* | -0.25 [-0.57, -0.03]* | -0.21 [-0.43, -0.04]* | -0.15 [-0.81, 0.44] | 1.71 [1.28, 2.22] | 1.83 [1.41, 2.33] |  |  |  | -0.20 [-0.43, -0.04]* |  |  |  | -0.20 [-0.43, -0.04]* | wt_med |
| 21 |  |  | -0.17 [-0.35, -0.04]* | 0.18 [-0.02, 0.42] | -0.17 [-0.35, -0.04]* | -0.14 [-0.32, -0.02] | -0.17 [-0.35, -0.04]* | -0.16 [-0.33, -0.03]* | -0.17 [-0.35, -0.04]* | -0.21 [-0.88, 0.37] | 1.92 [1.48, 2.45] | 2.07 [1.63, 2.60] |  |  |  | -0.17 [-0.36, -0.04]* |  |  |  | -0.18 [-0.36, -0.04]* | wt_med |
| 30 |  |  | -0.17 [-0.36, -0.03]* | 0.18 [-0.02, 0.43] | -0.16 [-0.35, -0.03]* | -0.19 [-0.43, -0.02]* | -0.16 [-0.35, -0.03]* | -0.19 [-0.43, -0.03]* | -0.17 [-0.36, -0.03]* | 0.04 [-0.56, 0.56] | 1.72 [1.22, 2.29] | 1.78 [1.34, 2.29] |  |  |  | -0.16 [-0.35, -0.03]* |  |  |  | -0.16 [-0.35, -0.03]* | wt_med |
| 45 |  |  | -0.13 [-0.27, -0.02]* | 0.13 [-0.06, 0.41] | -0.12 [-0.27, -0.02]* | -0.09 [-0.34, 0.03] | -0.12 [-0.25, -0.02]* | -0.18 [-0.41, -0.02]* | -0.14 [-0.31, -0.03]* | 0.02 [-0.50, 0.50] | 1.99 [1.45, 2.63] | 2.08 [1.51, 2.81] |  |  |  | -0.13 [-0.27, -0.02]* |  |  |  | -0.12 [-0.26, -0.01]* | wt_med |
| 60 |  |  | -0.10 [-0.23, -0.01] | 0.12 [-0.17, 0.47] | -0.09 [-0.22, -0.01] |  | -0.07 [-0.19, 0.02] | 0.00 [0.00, 0.00] | -0.13 [-0.31, -0.01] | 0.06 [-0.56, 0.63] | -0.00 [-0.00, 0.00] | 0.02 [0.00, 2.45] |  |  |  | -0.10 [-0.23, -0.01] |  |  |  | -0.10 [-0.24, -0.01] | wt_med |
| 90 |  |  | -0.12 [-0.27, -0.01] | 0.05 [-0.30, 0.49] | -0.12 [-0.27, -0.02] |  | -0.10 [-0.25, 0.01] |  | -0.14 [-0.36, 0.04] | 0.08 [-0.65, 0.77] |  |  |  |  |  | -0.12 [-0.27, -0.01] |  |  |  | -0.12 [-0.26, -0.01] | wt_med |

**first-hour MAE, schedule known at origin**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 5.17 [4.84, 5.49] | 5.89 [5.51, 6.27] |  | 5.22 [4.87, 5.58] | 4.69 [4.39, 5.01] | 4.45 [4.10, 4.86] | 4.69 [4.39, 5.01] | 4.47 [4.14, 4.87] | 4.69 [4.38, 5.00] | 4.65 [4.35, 4.95] |  |  | 5.81 [5.37, 6.30] | 6.03 [5.65, 6.41] | 5.09 [4.76, 5.42] |  | 6.18 [5.80, 6.56] | 8.94 [8.39, 9.48] | 6.20 [5.83, 6.58] |  | 4.86 [4.53, 5.16] |
| 1 | 5.40 [5.09, 5.72] | 6.26 [5.87, 6.64] | 4.57 [4.28, 4.86] | 5.05 [4.72, 5.38] | 4.57 [4.28, 4.86] | 4.32 [3.97, 4.68] | 4.57 [4.28, 4.86] | 4.30 [3.99, 4.61] | 4.57 [4.27, 4.86] | 4.30 [4.05, 4.55] |  |  | 5.58 [5.16, 6.03] | 6.45 [6.07, 6.83] | 5.23 [4.90, 5.58] | 4.57 [4.28, 4.86] | 6.31 [5.96, 6.70] | 9.08 [8.55, 9.63] | 6.34 [5.98, 6.74] | 4.57 [4.28, 4.86] | 4.74 [4.42, 5.06] |
| 2 | 5.78 [5.44, 6.15] | 6.71 [6.28, 7.16] | 4.75 [4.49, 5.06] | 5.16 [4.85, 5.52] | 4.75 [4.49, 5.06] | 4.61 [4.25, 5.02] | 4.75 [4.49, 5.06] | 4.50 [4.16, 4.90] | 4.75 [4.49, 5.07] | 4.55 [4.29, 4.82] |  |  | 5.63 [5.27, 6.02] | 6.87 [6.45, 7.31] | 5.47 [5.15, 5.84] | 4.75 [4.49, 5.06] | 6.94 [6.50, 7.38] | 9.35 [8.81, 9.89] | 6.96 [6.52, 7.40] | 4.75 [4.49, 5.06] | 4.94 [4.64, 5.27] |
| 3 | 6.11 [5.73, 6.46] | 7.02 [6.56, 7.48] | 4.89 [4.60, 5.20] | 5.34 [5.02, 5.69] | 4.89 [4.60, 5.20] | 4.63 [4.26, 5.01] | 4.89 [4.60, 5.20] | 4.54 [4.21, 4.87] | 4.87 [4.59, 5.18] | 4.61 [4.34, 4.92] | 6.70 [6.25, 7.15] | 6.75 [6.31, 7.21] | 5.77 [5.37, 6.20] | 7.20 [6.74, 7.65] | 5.80 [5.43, 6.19] | 4.89 [4.60, 5.20] | 7.32 [6.85, 7.78] | 9.53 [8.98, 10.09] | 7.34 [6.87, 7.81] | 4.89 [4.60, 5.20] | 5.12 [4.80, 5.45] |
| 4 | 5.97 [5.62, 6.32] | 6.92 [6.51, 7.37] | 4.76 [4.48, 5.07] | 5.28 [4.94, 5.62] | 4.76 [4.48, 5.07] | 4.75 [4.35, 5.16] | 4.76 [4.48, 5.07] | 4.59 [4.25, 4.95] | 4.77 [4.48, 5.07] | 4.58 [4.31, 4.86] | 6.63 [6.22, 7.06] | 6.71 [6.30, 7.16] | 5.40 [5.04, 5.79] | 7.16 [6.75, 7.63] | 5.59 [5.27, 5.95] | 4.76 [4.48, 5.07] | 7.33 [6.91, 7.81] | 9.37 [8.85, 9.96] | 7.35 [6.93, 7.83] | 4.76 [4.48, 5.07] | 4.93 [4.65, 5.24] |
| 5 | 6.18 [5.80, 6.59] | 7.26 [6.79, 7.71] | 4.85 [4.54, 5.19] | 5.23 [4.88, 5.59] | 4.85 [4.54, 5.19] | 4.63 [4.26, 5.06] | 4.85 [4.54, 5.19] | 4.55 [4.21, 4.94] | 4.85 [4.54, 5.18] | 4.65 [4.35, 4.93] | 6.81 [6.35, 7.32] | 6.83 [6.38, 7.34] | 5.79 [5.35, 6.30] | 7.49 [7.03, 7.97] | 5.81 [5.45, 6.21] | 4.85 [4.54, 5.19] | 7.54 [7.04, 8.00] | 9.42 [8.88, 10.00] | 7.55 [7.06, 8.02] | 4.85 [4.54, 5.19] | 5.04 [4.70, 5.36] |
| 6 | 6.07 [5.69, 6.45] | 7.12 [6.63, 7.57] | 4.67 [4.37, 4.98] | 5.10 [4.74, 5.44] | 4.67 [4.37, 4.98] | 4.51 [4.13, 4.89] | 4.67 [4.37, 4.98] | 4.40 [4.06, 4.73] | 4.67 [4.37, 4.98] | 4.27 [4.02, 4.51] | 6.54 [6.08, 6.99] | 6.60 [6.14, 7.05] | 5.51 [5.06, 5.95] | 7.34 [6.84, 7.80] | 5.67 [5.29, 6.05] | 4.67 [4.37, 4.98] | 7.46 [6.96, 7.93] | 9.13 [8.55, 9.65] | 7.48 [6.97, 7.95] | 4.67 [4.37, 4.98] | 4.87 [4.55, 5.20] |
| 7 | 6.48 [6.08, 6.91] | 7.46 [6.98, 7.99] | 4.89 [4.60, 5.24] | 5.19 [4.86, 5.57] | 4.89 [4.60, 5.24] | 4.76 [4.37, 5.21] | 4.89 [4.60, 5.24] | 4.65 [4.28, 5.07] | 4.89 [4.61, 5.24] | 4.91 [4.63, 5.22] | 6.78 [6.33, 7.32] | 6.78 [6.32, 7.29] |  | 7.76 [7.27, 8.28] | 6.05 [5.70, 6.45] | 4.89 [4.60, 5.24] | 7.74 [7.25, 8.26] | 9.22 [8.69, 9.78] | 7.77 [7.27, 8.29] | 4.89 [4.60, 5.24] | 5.09 [4.79, 5.45] |
| 10 |  |  | 4.97 [4.63, 5.32] | 5.24 [4.89, 5.59] | 4.97 [4.63, 5.32] | 4.79 [4.38, 5.27] | 4.97 [4.63, 5.32] | 4.69 [4.32, 5.15] | 4.97 [4.62, 5.32] | 4.98 [4.68, 5.30] | 6.88 [6.41, 7.38] | 6.90 [6.44, 7.41] |  |  |  | 4.97 [4.63, 5.32] |  |  |  | 4.97 [4.63, 5.32] | 5.20 [4.83, 5.58] |
| 14 |  |  | 4.92 [4.61, 5.28] | 5.27 [4.91, 5.62] | 4.92 [4.61, 5.28] | 4.86 [4.42, 5.33] | 4.92 [4.61, 5.28] | 4.65 [4.25, 5.06] | 4.92 [4.61, 5.28] | 5.08 [4.79, 5.39] | 6.72 [6.24, 7.22] | 6.80 [6.30, 7.31] |  |  |  | 4.92 [4.61, 5.28] |  |  |  | 4.92 [4.61, 5.28] | 5.18 [4.84, 5.53] |
| 21 |  |  | 4.76 [4.43, 5.13] | 5.13 [4.74, 5.52] | 4.76 [4.43, 5.13] | 4.58 [4.15, 5.01] | 4.76 [4.43, 5.13] | 4.40 [4.00, 4.79] | 4.76 [4.43, 5.14] | 4.79 [4.47, 5.12] | 6.57 [6.06, 7.08] | 6.65 [6.11, 7.17] |  |  |  | 4.76 [4.43, 5.13] |  |  |  | 4.76 [4.43, 5.13] | 4.98 [4.62, 5.35] |
| 30 |  |  | 4.88 [4.51, 5.27] | 5.26 [4.86, 5.69] | 4.88 [4.51, 5.27] | 4.91 [4.39, 5.54] | 4.88 [4.51, 5.27] | 4.87 [4.31, 5.51] | 4.87 [4.51, 5.26] | 5.22 [4.85, 5.63] | 6.93 [6.31, 7.61] | 6.91 [6.31, 7.57] |  |  |  | 4.88 [4.51, 5.27] |  |  |  | 4.88 [4.51, 5.27] | 5.16 [4.77, 5.57] |
| 45 |  |  | 4.70 [4.34, 5.10] | 4.97 [4.53, 5.41] | 4.70 [4.34, 5.10] | 9.83 [8.72, 11.11] | 4.70 [4.34, 5.10] | 4.85 [4.07, 5.71] | 4.70 [4.34, 5.10] | 4.96 [4.57, 5.35] | 7.26 [6.38, 8.19] | 7.28 [6.41, 8.13] |  |  |  | 4.70 [4.34, 5.10] |  |  |  | 4.70 [4.34, 5.10] | 4.87 [4.50, 5.26] |
| 60 |  |  | 4.72 [4.31, 5.17] | 4.84 [4.39, 5.29] | 4.72 [4.31, 5.17] |  | 4.72 [4.31, 5.17] | 0.09 [0.00, 0.23] | 4.73 [4.31, 5.18] | 4.97 [4.51, 5.45] | 0.08 [0.00, 0.22] | 0.11 [0.00, 0.28] |  |  |  | 4.72 [4.31, 5.17] |  |  |  | 4.72 [4.31, 5.17] | 4.81 [4.38, 5.28] |
| 90 |  |  | 4.55 [4.06, 5.10] | 4.56 [4.07, 5.10] | 4.55 [4.06, 5.10] |  | 4.55 [4.06, 5.10] |  | 4.55 [4.07, 5.11] | 4.63 [4.10, 5.18] |  |  |  |  |  | 4.55 [4.06, 5.10] |  |  |  | 4.55 [4.06, 5.10] | 4.64 [4.14, 5.22] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.26 [0.07, 0.44] | 1.00 [0.68, 1.38] |  | 0.41 [0.19, 0.67] | -0.19 [-0.43, -0.03]* | -0.20 [-0.55, -0.01]* | -0.19 [-0.43, -0.03]* | -0.22 [-0.53, -0.03]* | -0.19 [-0.43, -0.03]* | -0.23 [-0.99, 0.37] |  |  | 0.93 [0.49, 1.41] | 1.13 [0.74, 1.61] | 0.19 [-0.10, 0.51] |  | 1.29 [0.91, 1.73] | 4.09 [2.48, 5.81] | 1.30 [0.92, 1.76] |  | wt_med |
| 1 | 0.60 [0.40, 0.81] | 1.45 [0.91, 2.18] | -0.20 [-0.50, -0.02]* | 0.35 [0.13, 0.62] | -0.20 [-0.50, -0.02]* | -0.24 [-0.67, 0.00] | -0.20 [-0.50, -0.02]* | -0.27 [-0.72, -0.02]* | -0.20 [-0.51, -0.02]* | -0.44 [-1.19, 0.24] |  |  | 0.80 [0.46, 1.34] | 1.63 [1.06, 2.35] | 0.42 [0.13, 0.77] | -0.20 [-0.50, -0.02]* | 1.49 [0.99, 2.07] | 4.32 [2.76, 5.90] | 1.52 [1.01, 2.10] | -0.20 [-0.50, -0.02]* | wt_med |
| 2 | 0.78 [0.42, 1.19] | 1.72 [0.95, 2.79] | -0.20 [-0.50, -0.01]* | 0.27 [0.05, 0.51] | -0.20 [-0.50, -0.01]* | -0.24 [-0.70, 0.05] | -0.20 [-0.50, -0.01]* | -0.25 [-0.67, 0.03] | -0.20 [-0.50, -0.01]* | -0.38 [-1.20, 0.29] |  |  | 0.68 [0.34, 1.04] | 1.89 [1.10, 2.83] | 0.50 [0.16, 0.89] | -0.20 [-0.50, -0.01]* | 1.95 [1.22, 2.83] | 4.40 [2.81, 6.05] | 1.98 [1.23, 2.85] | -0.20 [-0.50, -0.01]* | wt_med |
| 3 | 0.91 [0.51, 1.36] | 1.80 [1.04, 2.80] | -0.24 [-0.56, -0.03]* | 0.31 [0.10, 0.56] | -0.24 [-0.56, -0.03]* | -0.29 [-0.82, 0.00] | -0.24 [-0.56, -0.03]* | -0.29 [-0.78, -0.01]* | -0.24 [-0.56, -0.03]* | -0.47 [-1.13, 0.07] | 1.79 [1.19, 2.51] | 1.84 [1.26, 2.52] | 0.71 [0.33, 1.08] | 2.01 [1.17, 2.99] | 0.62 [0.23, 1.02] | -0.24 [-0.56, -0.03]* | 2.11 [1.33, 3.07] | 4.40 [2.90, 6.01] | 2.13 [1.36, 3.08] | -0.24 [-0.56, -0.03]* | wt_med |
| 4 | 0.93 [0.53, 1.38] | 1.88 [1.07, 2.88] | -0.20 [-0.44, -0.03]* | 0.35 [0.11, 0.62] | -0.20 [-0.44, -0.03]* | -0.24 [-0.62, -0.01]* | -0.20 [-0.44, -0.03]* | -0.26 [-0.64, -0.02]* | -0.20 [-0.44, -0.03]* | -0.38 [-1.13, 0.26] | 1.76 [1.21, 2.41] | 1.84 [1.32, 2.48] | 0.45 [0.18, 0.76] | 2.12 [1.26, 3.19] | 0.58 [0.26, 0.93] | -0.20 [-0.44, -0.03]* | 2.25 [1.43, 3.27] | 4.41 [2.89, 6.04] | 2.27 [1.44, 3.30] | -0.20 [-0.44, -0.03]* | wt_med |
| 5 | 1.08 [0.62, 1.60] | 2.16 [1.26, 3.27] | -0.20 [-0.46, -0.03]* | 0.29 [0.08, 0.54] | -0.20 [-0.46, -0.03]* | -0.25 [-0.69, -0.02]* | -0.20 [-0.46, -0.03]* | -0.25 [-0.63, -0.03]* | -0.20 [-0.46, -0.03]* | -0.42 [-1.27, 0.24] | 2.02 [1.35, 2.86] | 2.04 [1.43, 2.81] | 0.74 [0.33, 1.14] | 2.40 [1.37, 3.56] | 0.73 [0.33, 1.17] | -0.20 [-0.46, -0.03]* | 2.44 [1.49, 3.55] | 4.38 [2.72, 6.12] | 2.46 [1.50, 3.57] | -0.20 [-0.46, -0.03]* | wt_med |
| 6 | 1.13 [0.70, 1.60] | 2.18 [1.29, 3.29] | -0.23 [-0.52, -0.04]* | 0.28 [0.02, 0.57] | -0.23 [-0.52, -0.04]* | -0.26 [-0.70, -0.02]* | -0.23 [-0.52, -0.04]* | -0.30 [-0.76, -0.02]* | -0.23 [-0.52, -0.04]* | -0.62 [-1.45, 0.11] | 1.87 [1.30, 2.55] | 1.93 [1.40, 2.60] | 0.59 [0.27, 1.06] | 2.40 [1.41, 3.55] | 0.74 [0.38, 1.13] | -0.23 [-0.52, -0.04]* | 2.50 [1.52, 3.65] | 4.22 [2.73, 5.80] | 2.51 [1.53, 3.67] | -0.23 [-0.52, -0.04]* | wt_med |
| 7 | 1.33 [0.71, 1.98] | 2.32 [1.31, 3.55] | -0.22 [-0.51, -0.03]* | 0.14 [-0.10, 0.42] | -0.22 [-0.51, -0.03]* | -0.28 [-0.74, -0.01]* | -0.22 [-0.51, -0.03]* | -0.28 [-0.72, -0.00]* | -0.22 [-0.52, -0.03]* | -0.18 [-1.02, 0.51] | 1.90 [1.29, 2.66] | 1.89 [1.33, 2.57] |  | 2.62 [1.44, 3.90] | 0.90 [0.43, 1.44] | -0.22 [-0.51, -0.03]* | 2.60 [1.46, 3.93] | 4.12 [2.60, 5.65] | 2.62 [1.47, 3.95] | -0.22 [-0.51, -0.03]* | wt_med |
| 10 |  |  | -0.22 [-0.48, -0.04]* | 0.17 [-0.02, 0.40] | -0.22 [-0.48, -0.04]* | -0.30 [-0.77, -0.02]* | -0.22 [-0.48, -0.04]* | -0.26 [-0.62, -0.03]* | -0.22 [-0.48, -0.04]* | -0.19 [-0.97, 0.43] | 1.95 [1.28, 2.75] | 1.97 [1.33, 2.72] |  |  |  | -0.22 [-0.48, -0.04]* |  |  |  | -0.22 [-0.48, -0.04]* | wt_med |
| 14 |  |  | -0.27 [-0.58, -0.05]* | 0.18 [-0.05, 0.41] | -0.27 [-0.58, -0.05]* | -0.33 [-0.82, -0.03]* | -0.27 [-0.58, -0.05]* | -0.34 [-0.79, -0.04]* | -0.27 [-0.58, -0.05]* | -0.08 [-0.99, 0.68] | 1.71 [1.20, 2.24] | 1.78 [1.28, 2.30] |  |  |  | -0.27 [-0.58, -0.05]* |  |  |  | -0.27 [-0.58, -0.05]* | wt_med |
| 21 |  |  | -0.24 [-0.51, -0.05]* | 0.23 [0.00, 0.49] | -0.24 [-0.51, -0.05]* | -0.20 [-0.48, -0.03] | -0.24 [-0.51, -0.05]* | -0.22 [-0.51, -0.03]* | -0.23 [-0.51, -0.05]* | -0.18 [-1.10, 0.55] | 1.90 [1.31, 2.57] | 1.98 [1.42, 2.62] |  |  |  | -0.24 [-0.51, -0.05]* |  |  |  | -0.24 [-0.51, -0.05]* | wt_med |
| 30 |  |  | -0.25 [-0.54, -0.04]* | 0.20 [-0.04, 0.45] | -0.25 [-0.54, -0.04]* | -0.29 [-0.72, -0.03]* | -0.25 [-0.54, -0.04]* | -0.29 [-0.70, -0.05]* | -0.25 [-0.54, -0.04]* | 0.01 [-0.89, 0.74] | 1.73 [1.04, 2.51] | 1.71 [1.12, 2.36] |  |  |  | -0.25 [-0.54, -0.04]* |  |  |  | -0.25 [-0.54, -0.04]* | wt_med |
| 45 |  |  | -0.15 [-0.36, -0.01]* | 0.12 [-0.07, 0.38] | -0.15 [-0.36, -0.01]* | -0.20 [-0.92, 0.09] | -0.15 [-0.36, -0.01]* | -0.25 [-0.62, -0.03] | -0.15 [-0.36, -0.01]* | 0.07 [-0.60, 0.65] | 1.99 [1.35, 2.77] | 2.01 [1.30, 2.84] |  |  |  | -0.15 [-0.36, -0.01]* |  |  |  | -0.15 [-0.36, -0.01]* | wt_med |
| 60 |  |  | -0.13 [-0.32, -0.01] | 0.04 [-0.26, 0.46] | -0.13 [-0.32, -0.01] |  | -0.13 [-0.32, -0.01] | 0.00 [0.00, 0.00] | -0.13 [-0.32, -0.01] | 0.13 [-0.75, 0.78] | -0.00 [-0.00, 0.00] | 0.02 [0.00, 3.75] |  |  |  | -0.13 [-0.32, -0.01] |  |  |  | -0.13 [-0.32, -0.01] | wt_med |
| 90 |  |  | -0.09 [-0.27, -0.00] | -0.14 [-0.66, 0.22] | -0.09 [-0.27, -0.00] |  | -0.09 [-0.27, -0.00] |  | -0.09 [-0.27, -0.00] | -0.09 [-1.11, 0.69] |  |  |  |  |  | -0.09 [-0.27, -0.00] |  |  |  | -0.09 [-0.27, -0.00] | wt_med |

**first-hour MAE, schedule projected at origin**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.98 [4.67, 5.31] | 6.18 [5.79, 6.62] |  | 4.18 [3.82, 4.53] | 3.83 [3.53, 4.14] | 3.24 [2.89, 3.67] | 3.84 [3.53, 4.16] | 3.40 [3.05, 3.77] | 3.81 [3.50, 4.13] | 3.20 [2.92, 3.50] |  |  | 4.78 [4.36, 5.25] | 6.31 [5.92, 6.72] | 4.59 [4.32, 4.90] |  | 6.36 [5.91, 6.84] | 8.57 [7.93, 9.25] | 6.40 [5.95, 6.88] |  | 3.84 [3.53, 4.14] |
| 1 | 5.30 [5.00, 5.62] | 6.47 [6.04, 6.92] | 3.87 [3.56, 4.24] | 4.12 [3.82, 4.47] | 3.88 [3.57, 4.24] | 3.28 [2.96, 3.67] | 3.86 [3.56, 4.24] | 3.35 [3.09, 3.67] | 3.87 [3.56, 4.24] | 3.33 [3.00, 3.71] |  |  | 4.94 [4.57, 5.35] | 6.31 [5.90, 6.73] | 4.68 [4.37, 5.00] | 3.87 [3.56, 4.23] | 6.39 [5.95, 6.83] | 8.79 [8.17, 9.45] | 6.40 [5.96, 6.85] | 3.87 [3.56, 4.23] | 3.89 [3.59, 4.26] |
| 2 | 5.53 [5.19, 5.88] | 6.84 [6.36, 7.33] | 3.82 [3.50, 4.15] | 4.08 [3.74, 4.43] | 3.82 [3.50, 4.15] | 3.00 [2.73, 3.30] | 3.81 [3.50, 4.14] | 3.25 [2.98, 3.56] | 3.79 [3.47, 4.11] | 3.52 [3.23, 3.83] |  |  | 4.80 [4.41, 5.21] | 6.48 [6.08, 6.93] | 4.85 [4.52, 5.19] | 3.82 [3.50, 4.15] | 6.89 [6.38, 7.38] | 8.60 [7.94, 9.29] | 6.90 [6.39, 7.39] | 3.82 [3.50, 4.15] | 3.84 [3.52, 4.17] |
| 3 | 5.99 [5.58, 6.42] | 7.29 [6.81, 7.85] | 3.97 [3.61, 4.34] | 4.24 [3.87, 4.63] | 3.95 [3.60, 4.28] | 3.36 [3.01, 3.80] | 3.93 [3.59, 4.26] | 3.66 [3.26, 4.15] | 3.87 [3.56, 4.19] | 3.46 [3.17, 3.75] | 5.00 [4.53, 5.60] | 5.26 [4.79, 5.85] | 4.77 [4.36, 5.26] | 6.98 [6.54, 7.49] | 5.16 [4.79, 5.58] | 3.97 [3.61, 4.34] | 7.50 [6.99, 8.07] | 8.59 [7.95, 9.25] | 7.51 [6.99, 8.07] | 3.97 [3.61, 4.34] | 4.00 [3.64, 4.36] |
| 4 | 6.66 [6.18, 7.18] | 8.07 [7.47, 8.71] | 4.39 [3.87, 4.98] | 4.49 [4.07, 4.95] | 4.40 [3.88, 4.99] | 3.35 [3.04, 3.70] | 4.41 [3.88, 5.00] | 3.50 [3.21, 3.82] | 4.37 [3.84, 4.96] | 3.90 [3.48, 4.44] | 5.17 [4.73, 5.65] | 5.43 [5.00, 5.92] | 5.53 [4.91, 6.25] | 7.58 [7.02, 8.19] | 5.61 [5.12, 6.12] | 4.39 [3.87, 4.98] | 8.36 [7.71, 9.06] | 9.52 [8.68, 10.39] | 8.37 [7.72, 9.09] | 4.39 [3.87, 4.98] | 4.41 [3.89, 5.00] |
| 5 | 6.25 [5.84, 6.64] | 7.73 [7.20, 8.27] | 3.93 [3.62, 4.23] | 4.31 [3.96, 4.68] | 3.92 [3.62, 4.22] | 3.38 [3.00, 3.86] | 3.93 [3.62, 4.23] | 3.52 [3.18, 3.95] | 3.91 [3.61, 4.21] | 3.19 [2.91, 3.49] | 5.17 [4.71, 5.77] | 5.42 [4.94, 5.99] | 4.81 [4.35, 5.29] | 7.43 [6.93, 7.93] | 5.20 [4.87, 5.55] | 3.93 [3.62, 4.23] | 8.12 [7.55, 8.70] | 8.83 [8.18, 9.51] | 8.13 [7.55, 8.71] | 3.93 [3.62, 4.23] | 3.95 [3.64, 4.24] |
| 6 | 6.81 [6.34, 7.45] | 8.21 [7.63, 8.88] | 4.09 [3.77, 4.47] | 4.35 [4.02, 4.70] | 4.11 [3.78, 4.50] | 3.44 [3.08, 3.85] | 4.09 [3.77, 4.48] | 3.60 [3.27, 3.98] | 4.10 [3.77, 4.49] | 3.46 [3.12, 3.86] | 5.34 [4.91, 5.82] | 5.61 [5.19, 6.09] | 5.16 [4.73, 5.71] | 7.78 [7.29, 8.35] | 5.45 [5.09, 5.86] | 4.09 [3.78, 4.48] | 8.57 [7.95, 9.23] | 9.09 [8.46, 9.81] | 8.56 [7.95, 9.22] | 4.09 [3.77, 4.48] | 4.10 [3.79, 4.50] |
| 7 | 6.87 [6.42, 7.35] | 8.14 [7.57, 8.77] | 4.08 [3.73, 4.44] | 4.28 [3.91, 4.65] | 4.07 [3.73, 4.43] | 3.23 [2.84, 3.62] | 4.07 [3.72, 4.43] | 3.47 [3.13, 3.84] | 4.05 [3.71, 4.40] | 3.85 [3.46, 4.23] | 5.29 [4.83, 5.81] | 5.56 [5.10, 6.08] |  | 7.85 [7.30, 8.45] | 5.68 [5.32, 6.11] | 4.08 [3.74, 4.45] | 9.39 [8.13, 11.35] | 8.79 [8.13, 9.46] | 9.39 [8.13, 11.32] | 4.08 [3.73, 4.44] | 4.10 [3.76, 4.45] |
| 10 |  |  | 4.13 [3.81, 4.48] | 4.55 [4.16, 4.93] | 4.14 [3.82, 4.48] | 3.68 [3.24, 4.18] | 4.14 [3.82, 4.48] | 3.80 [3.41, 4.27] | 4.12 [3.80, 4.47] | 3.57 [3.26, 3.91] | 5.63 [5.08, 6.23] | 5.87 [5.32, 6.47] |  |  |  | 4.14 [3.82, 4.48] |  |  |  | 4.14 [3.83, 4.49] | 4.17 [3.86, 4.52] |
| 14 |  |  | 4.92 [4.48, 5.47] | 5.09 [4.66, 5.55] | 4.93 [4.48, 5.48] | 4.22 [3.72, 4.75] | 4.93 [4.48, 5.48] | 4.42 [3.95, 4.92] | 4.89 [4.44, 5.43] | 4.52 [4.05, 5.08] | 6.10 [5.53, 6.73] | 6.36 [5.80, 6.98] |  |  |  | 4.93 [4.48, 5.47] |  |  |  | 4.93 [4.48, 5.47] | 4.92 [4.47, 5.46] |
| 21 |  |  | 5.06 [4.66, 5.51] | 5.06 [4.66, 5.49] | 5.07 [4.66, 5.52] | 4.75 [4.13, 5.54] | 5.04 [4.64, 5.51] | 4.74 [4.24, 5.36] | 5.03 [4.64, 5.49] | 4.77 [4.30, 5.36] | 6.76 [6.18, 7.52] | 7.09 [6.51, 7.83] |  |  |  | 5.06 [4.65, 5.51] |  |  |  | 5.04 [4.65, 5.50] | 5.05 [4.65, 5.50] |
| 30 |  |  | 5.05 [4.66, 5.43] | 5.14 [4.74, 5.56] | 5.05 [4.66, 5.43] | 5.00 [4.44, 5.64] | 5.05 [4.67, 5.44] | 4.93 [4.39, 5.57] | 5.03 [4.64, 5.43] | 5.16 [4.71, 5.61] | 6.73 [6.13, 7.42] | 6.96 [6.37, 7.66] |  |  |  | 5.05 [4.66, 5.44] |  |  |  | 5.05 [4.66, 5.44] | 5.06 [4.68, 5.46] |
| 45 |  |  | 5.66 [5.22, 6.13] | 5.78 [5.26, 6.27] | 5.67 [5.22, 6.13] | 4.30 [3.72, 5.04] | 5.68 [5.24, 6.15] | 5.56 [4.85, 6.34] | 5.60 [5.15, 6.07] | 5.70 [5.24, 6.20] | 7.49 [6.64, 8.43] | 7.67 [6.80, 8.60] |  |  |  | 5.66 [5.22, 6.13] |  |  |  | 5.68 [5.23, 6.14] | 5.71 [5.25, 6.17] |
| 60 |  |  | 5.78 [5.32, 6.29] | 5.96 [5.46, 6.50] | 5.79 [5.32, 6.30] |  | 5.84 [5.37, 6.34] | 0.00 [0.00, 0.00] | 5.71 [5.25, 6.21] | 5.85 [5.35, 6.39] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | 5.78 [5.32, 6.29] |  |  |  | 5.78 [5.31, 6.29] | 5.81 [5.34, 6.32] |
| 90 |  |  | 6.14 [5.66, 6.68] | 6.27 [5.72, 6.90] | 6.14 [5.66, 6.68] |  | 6.20 [5.73, 6.73] |  | 6.10 [5.62, 6.65] | 6.45 [5.97, 6.96] |  |  |  |  |  | 6.14 [5.67, 6.69] |  |  |  | 6.14 [5.66, 6.68] | 6.23 [5.73, 6.82] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.16 [0.56, 1.61] | 2.38 [1.02, 3.81] |  | 0.31 [0.16, 0.57] | -0.02 [-0.07, 0.03] | 0.02 [-0.06, 0.13] | -0.01 [-0.05, 0.04] | 0.00 [-0.07, 0.08] | -0.02 [-0.06, 0.03] | -0.64 [-0.94, -0.35]* |  |  | 0.86 [0.52, 1.25] | 2.48 [1.18, 3.40] | 0.72 [0.00, 1.10] |  | 2.52 [1.15, 4.11] | 4.79 [3.02, 8.40] | 2.56 [1.19, 4.18] |  | wt_med |
| 1 | 1.42 [0.29, 2.24] | 2.60 [0.93, 4.08] | -0.02 [-0.15, 0.02] | 0.26 [0.14, 0.53] | -0.01 [-0.10, 0.02] | 0.01 [-0.02, 0.09] | -0.03 [-0.17, 0.02] | 0.02 [-0.00, 0.05] | -0.01 [-0.08, 0.02] | -0.56 [-1.06, -0.15]* |  |  | 0.92 [0.53, 1.24] | 2.44 [0.97, 3.43] | 0.79 [0.18, 1.18] | -0.02 [-0.16, 0.02] | 2.52 [1.01, 4.29] | 4.95 [3.34, 8.50] | 2.53 [1.05, 4.28] | -0.02 [-0.16, 0.02] | wt_med |
| 2 | 1.72 [0.64, 2.62] | 3.05 [1.45, 4.59] | -0.01 [-0.08, 0.07] | 0.27 [0.06, 0.52] | -0.02 [-0.09, 0.06] | -0.07 [-0.24, 0.00] | -0.02 [-0.09, 0.06] | -0.02 [-0.10, 0.09] | -0.04 [-0.15, 0.03] | -0.30 [-0.72, 0.08] |  |  | 0.92 [0.66, 1.61] | 2.69 [1.38, 3.64] | 1.01 [0.51, 1.35] | -0.01 [-0.08, 0.08] | 3.08 [1.41, 5.14] | 4.83 [3.12, 8.38] | 3.09 [1.42, 5.17] | -0.01 [-0.08, 0.08] | wt_med |
| 3 | 2.02 [0.68, 3.09] | 3.34 [1.54, 4.89] | -0.03 [-0.16, 0.05] | 0.24 [0.11, 0.36] | -0.05 [-0.20, -0.01]* | -0.19 [-0.99, 0.02] | -0.07 [-0.23, -0.01]* | -0.04 [-0.24, 0.02] | -0.12 [-0.50, -0.01]* | -0.51 [-1.14, -0.02]* | 1.28 [1.04, 1.63] | 1.54 [1.26, 1.93] | 0.76 [0.47, 1.39] | 3.01 [1.51, 4.11] | 1.15 [0.77, 1.49] | -0.03 [-0.17, 0.04] | 3.52 [1.52, 5.58] | 4.65 [2.91, 7.75] | 3.52 [1.53, 5.58] | -0.03 [-0.17, 0.04] | wt_med |
| 4 | 2.29 [0.65, 3.40] | 3.72 [1.68, 5.35] | -0.02 [-0.09, 0.01] | 0.23 [0.12, 0.39] | -0.01 [-0.05, 0.01] | 0.03 [-0.00, 0.07] | -0.01 [-0.05, 0.01] | 0.01 [-0.00, 0.04] | -0.02 [-0.08, 0.01] | -0.49 [-0.99, -0.13]* | 1.66 [1.29, 2.59] | 1.93 [1.57, 2.78] | 1.01 [0.68, 1.83] | 3.21 [1.60, 4.31] | 1.20 [0.68, 1.60] | -0.02 [-0.08, 0.01] | 3.98 [1.74, 6.15] | 5.18 [3.51, 8.48] | 4.00 [1.79, 6.17] | -0.02 [-0.09, 0.01] | wt_med |
| 5 | 2.34 [0.67, 3.46] | 3.85 [1.72, 5.48] | -0.02 [-0.09, 0.04] | 0.34 [0.17, 0.80] | -0.02 [-0.09, 0.03] | 0.01 [-0.08, 0.12] | -0.02 [-0.09, 0.03] | 0.00 [-0.07, 0.10] | -0.02 [-0.09, 0.03] | -0.72 [-1.07, -0.34]* | 1.66 [1.27, 2.56] | 1.90 [1.47, 2.77] | 0.82 [0.44, 1.35] | 3.51 [1.59, 4.83] | 1.23 [0.65, 1.69] | -0.02 [-0.09, 0.04] | 4.18 [1.64, 6.51] | 4.91 [3.15, 8.31] | 4.18 [1.66, 6.52] | -0.02 [-0.09, 0.04] | wt_med |
| 6 | 2.73 [0.89, 3.91] | 4.15 [1.77, 5.96] | -0.03 [-0.16, 0.02] | 0.30 [0.19, 0.52] | -0.01 [-0.05, 0.04] | 0.04 [-0.01, 0.20] | -0.03 [-0.13, 0.02] | 0.02 [-0.02, 0.11] | -0.01 [-0.06, 0.04] | -0.64 [-1.15, -0.22]* | 1.75 [1.40, 2.27] | 2.03 [1.62, 2.54] | 0.94 [0.56, 1.38] | 3.73 [1.65, 5.12] | 1.33 [0.63, 1.77] | -0.02 [-0.11, 0.03] | 4.50 [1.99, 6.97] | 5.03 [3.35, 8.18] | 4.49 [2.00, 6.96] | -0.03 [-0.14, 0.02] | wt_med |
| 7 | 2.81 [0.76, 4.07] | 4.10 [1.68, 5.82] | -0.02 [-0.09, 0.05] | 0.22 [0.01, 0.45] | -0.03 [-0.10, 0.03] | -0.05 [-0.18, 0.02] | -0.03 [-0.11, 0.02] | -0.02 [-0.09, 0.05] | -0.05 [-0.18, 0.01] | -0.23 [-0.75, 0.20] | 1.83 [1.50, 2.41] | 2.10 [1.75, 2.63] |  | 3.80 [1.77, 5.25] | 1.57 [0.82, 2.09] | -0.02 [-0.09, 0.07] | 5.35 [2.27, 7.96] | 4.75 [2.96, 8.14] | 5.35 [2.27, 7.96] | -0.02 [-0.09, 0.06] | wt_med |
| 10 |  |  | -0.04 [-0.15, 0.05] | 0.34 [0.18, 0.77] | -0.04 [-0.14, 0.05] | 0.01 [-0.09, 0.16] | -0.04 [-0.14, 0.05] | 0.01 [-0.07, 0.13] | -0.04 [-0.14, 0.05] | -0.57 [-0.94, -0.17]* | 1.80 [1.35, 2.84] | 2.05 [1.50, 3.07] |  |  |  | -0.04 [-0.14, 0.05] |  |  |  | -0.03 [-0.13, 0.05] | wt_med |
| 14 |  |  | -0.01 [-0.05, 0.04] | 0.19 [0.04, 0.38] | 0.00 [-0.04, 0.05] | 0.03 [0.00, 0.11] | 0.00 [-0.04, 0.05] | 0.01 [-0.02, 0.07] | -0.02 [-0.09, 0.03] | -0.36 [-0.93, 0.19] | 1.71 [1.12, 2.73] | 1.97 [1.27, 2.95] |  |  |  | -0.00 [-0.05, 0.04] |  |  |  | -0.00 [-0.05, 0.05] | wt_med |
| 21 |  |  | -0.01 [-0.06, 0.02] | 0.06 [-0.26, 0.40] | -0.00 [-0.03, 0.03] | 0.01 [-0.01, 0.05] | -0.01 [-0.04, 0.03] | -0.01 [-0.04, 0.02] | -0.02 [-0.09, 0.02] | -0.27 [-0.81, 0.49] | 1.97 [1.41, 2.73] | 2.30 [1.56, 3.18] |  |  |  | -0.01 [-0.08, 0.03] |  |  |  | -0.03 [-0.12, 0.02] | wt_med |
| 30 |  |  | -0.02 [-0.08, 0.02] | 0.14 [-0.13, 0.58] | -0.02 [-0.08, 0.02] | -0.00 [-0.10, 0.08] | -0.01 [-0.08, 0.04] | 0.00 [-0.07, 0.06] | -0.03 [-0.09, 0.02] | 0.08 [-0.52, 0.77] | 1.69 [1.19, 2.30] | 1.92 [1.32, 2.60] |  |  |  | -0.02 [-0.08, 0.02] |  |  |  | -0.01 [-0.07, 0.03] | wt_med |
| 45 |  |  | -0.08 [-0.21, -0.01] | 0.15 [-0.17, 0.64] | -0.08 [-0.21, -0.01] | -0.06 [-0.32, 0.03] | -0.06 [-0.16, -0.00] | -0.06 [-0.25, 0.07] | -0.13 [-0.33, -0.02] | -0.05 [-0.75, 0.64] | 2.00 [1.16, 3.07] | 2.17 [1.22, 3.28] |  |  |  | -0.08 [-0.20, -0.02] |  |  |  | -0.06 [-0.20, 0.01] | wt_med |
| 60 |  |  | -0.07 [-0.17, -0.01] | 0.20 [-0.18, 0.73] | -0.06 [-0.16, 0.01] |  | -0.01 [-0.11, 0.12] | 0.00 [0.00, 0.00] | -0.13 [-0.36, -0.01] | -0.02 [-0.88, 0.84] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | -0.07 [-0.17, -0.00] |  |  |  | -0.07 [-0.20, 0.01] | wt_med |
| 90 |  |  | -0.15 [-0.38, -0.00] | 0.19 [-0.28, 0.88] | -0.15 [-0.36, -0.01] |  | -0.12 [-0.33, 0.04] |  | -0.18 [-0.57, 0.10] | 0.22 [-0.92, 1.23] |  |  |  |  |  | -0.14 [-0.37, 0.00] |  |  |  | -0.14 [-0.37, 0.00] | wt_med |

**rope-drop worth agreement**

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |  |  | 0.92 [0.91, 0.93] | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.91 [0.90, 0.91] | 0.88 [0.87, 0.89] | 0.91 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 1 | 0.90 [0.90, 0.91] | 0.90 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |  |  | 0.92 [0.91, 0.93] | 0.90 [0.89, 0.90] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.90] | 0.88 [0.87, 0.89] | 0.90 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 2 | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] |  |  | 0.92 [0.91, 0.93] | 0.90 [0.89, 0.90] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.90] | 0.88 [0.87, 0.89] | 0.90 [0.89, 0.90] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] |
| 3 | 0.90 [0.89, 0.91] | 0.89 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.92 [0.92, 0.93] | 0.89 [0.89, 0.90] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.89 [0.89, 0.90] | 0.88 [0.87, 0.89] | 0.89 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 4 | 0.90 [0.89, 0.90] | 0.89 [0.88, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.92 [0.91, 0.92] | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] |
| 5 | 0.90 [0.89, 0.91] | 0.89 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.92 [0.91, 0.93] | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.91, 0.92] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] |
| 6 | 0.90 [0.89, 0.90] | 0.89 [0.89, 0.90] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.90, 0.91] | 0.92 [0.91, 0.93] | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] |
| 7 | 0.90 [0.89, 0.90] | 0.89 [0.88, 0.90] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] |  | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.90] | 0.91 [0.90, 0.92] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] |
| 10 |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] |  |  |  | 0.91 [0.90, 0.92] |  |  |  | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] |
| 14 |  |  | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.90, 0.91] | 0.90 [0.89, 0.91] |  |  |  | 0.91 [0.90, 0.91] |  |  |  | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] |
| 21 |  |  | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] |  |  |  | 0.91 [0.90, 0.92] |  |  |  | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] |
| 30 |  |  | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] |  |  |  | 0.91 [0.91, 0.92] |  |  |  | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] |
| 45 |  |  | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.87 [0.84, 0.89] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.93] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.89, 0.92] |  |  |  | 0.91 [0.91, 0.92] |  |  |  | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 60 |  |  | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] |  | 0.91 [0.91, 0.92] | 1.00 [1.00, 1.00] | 0.90 [0.89, 0.91] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  |  |  | 0.91 [0.91, 0.92] |  |  |  | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 90 |  |  | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.93] |  | 0.92 [0.91, 0.93] |  | 0.90 [0.89, 0.91] |  |  |  |  |  | 0.92 [0.91, 0.93] |  |  |  | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.93] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  | 0.01 [-0.00, 0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.01] | 0.00 [-0.00, 0.01] | 0.01 [-0.00, 0.02] | 0.01 [0.00, 0.02] |  |  |  | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  | -0.00 [-0.01, 0.01] | -0.03 [-0.06, -0.00] | -0.00 [-0.01, 0.01] |  | 0.01 [-0.00, 0.02] | snaive7 |
| 1 | -0.01 [-0.02, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.01] |  |  | -0.01 [-0.02, -0.00] | -0.02 [-0.04, -0.01] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.02 [-0.03, -0.01] | -0.04 [-0.07, -0.01] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, -0.00] |  | wt_med |
| 2 | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.03, -0.01] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] |  |  | -0.01 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.04 [-0.06, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, -0.00] |  | wt_med |
| 3 | -0.02 [-0.03, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] | -0.01 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.04 [-0.06, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, -0.00] |  | wt_med |
| 4 | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, -0.00] | -0.01 [-0.03, -0.00] | -0.01 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.02 [-0.04, -0.01] | -0.04 [-0.06, -0.01] | -0.02 [-0.04, -0.01] | -0.01 [-0.01, -0.00] |  | wt_med |
| 5 | -0.01 [-0.03, 0.00] | -0.02 [-0.04, 0.00] | 0.00 [-0.00, 0.01] | 0.01 [-0.00, 0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.01 [-0.00, 0.02] | -0.01 [-0.03, 0.01] | -0.01 [-0.03, 0.00] |  | -0.02 [-0.04, 0.00] | -0.01 [-0.03, 0.00] | 0.00 [-0.00, 0.01] | -0.02 [-0.04, 0.00] | -0.03 [-0.06, -0.00] | -0.02 [-0.04, 0.00] | 0.00 [-0.01, 0.01] | 0.00 [-0.00, 0.01] | snaive7 |
| 6 | -0.02 [-0.03, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.00] | -0.01 [-0.02, -0.00] | -0.03 [-0.05, -0.01] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, -0.00] | -0.03 [-0.05, -0.01] | -0.04 [-0.07, -0.01] | -0.03 [-0.05, -0.01] | -0.01 [-0.01, -0.00] |  | wt_med |
| 7 | -0.02 [-0.03, -0.01] | -0.02 [-0.04, -0.01] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  | -0.03 [-0.05, -0.01] | -0.02 [-0.03, -0.01] | -0.00 [-0.01, -0.00] | -0.03 [-0.05, -0.01] | -0.04 [-0.06, -0.01] | -0.03 [-0.05, -0.01] | -0.01 [-0.01, -0.00] |  | wt_med |
| 10 |  |  | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.00] |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | -0.00 [-0.01, -0.00] |  | wt_med |
| 14 |  |  | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | -0.00 [-0.01, -0.00] |  | wt_med |
| 21 |  |  | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | -0.00 [-0.01, 0.00] |  | wt_med |
| 30 |  |  | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | -0.01 [-0.01, -0.00] |  | wt_med |
| 45 |  |  | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.02 [-0.04, 0.01] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | -0.00 [-0.01, 0.00] |  | wt_med |
| 60 |  |  | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] |  | -0.00 [-0.01, -0.00] | 0.00 [0.00, 0.00] | -0.01 [-0.02, -0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | -0.00 [-0.01, 0.00] |  | wt_med |
| 90 |  |  | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] |  | -0.00 [-0.01, 0.00] |  | -0.02 [-0.03, -0.01] |  |  |  |  |  | -0.00 [-0.01, 0.00] |  |  |  | 0.00 [-0.00, 0.00] |  | wt_med |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_chronos2 | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_tirex2 | lvl_toto2 | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.19 [0.16, 0.23] | 0.47 [0.44, 0.50] | 0.34 [0.31, 0.37] | 0.44 [0.41, 0.47] | 0.41 [0.38, 0.44] | 0.47 [0.44, 0.50] | 0.45 [0.42, 0.48] | 0.47 [0.44, 0.49] | 0.37 [0.35, 0.40] |
| 2 | 0.19 [0.16, 0.22] | 0.42 [0.39, 0.45] | 0.32 [0.29, 0.34] | 0.39 [0.36, 0.42] | 0.41 [0.38, 0.43] | 0.40 [0.36, 0.44] | 0.42 [0.39, 0.45] | 0.41 [0.38, 0.44] | 0.36 [0.33, 0.39] |
| 3 | 0.20 [0.17, 0.24] | 0.41 [0.38, 0.44] | 0.30 [0.27, 0.33] | 0.43 [0.40, 0.45] | 0.41 [0.38, 0.44] | 0.40 [0.37, 0.44] | 0.39 [0.36, 0.42] | 0.41 [0.39, 0.44] | 0.35 [0.33, 0.38] |
| 4 | 0.19 [0.16, 0.23] | 0.42 [0.39, 0.45] | 0.32 [0.29, 0.35] | 0.38 [0.36, 0.41] | 0.40 [0.37, 0.43] | 0.39 [0.36, 0.43] | 0.42 [0.39, 0.45] | 0.40 [0.38, 0.43] | 0.34 [0.31, 0.37] |
| 5 | 0.21 [0.18, 0.24] | 0.41 [0.38, 0.44] | 0.33 [0.30, 0.36] | 0.42 [0.39, 0.45] | 0.42 [0.39, 0.45] | 0.43 [0.39, 0.46] | 0.42 [0.39, 0.45] | 0.43 [0.40, 0.46] | 0.36 [0.33, 0.39] |
| 6 | 0.16 [0.13, 0.19] | 0.42 [0.39, 0.44] | 0.32 [0.29, 0.35] | 0.45 [0.42, 0.48] | 0.41 [0.38, 0.44] | 0.37 [0.34, 0.41] | 0.41 [0.38, 0.44] | 0.43 [0.40, 0.45] | 0.37 [0.34, 0.40] |
| 7 | 0.18 [0.15, 0.22] | 0.38 [0.35, 0.41] | 0.30 [0.28, 0.33] | 0.35 [0.32, 0.38] |  | 0.40 [0.36, 0.44] | 0.40 [0.37, 0.43] | 0.38 [0.35, 0.41] | 0.34 [0.31, 0.37] |
| 10 | 0.20 [0.17, 0.24] | 0.38 [0.35, 0.41] | 0.33 [0.30, 0.35] | 0.38 [0.35, 0.41] |  | 0.40 [0.37, 0.44] | 0.39 [0.36, 0.42] | 0.37 [0.35, 0.40] | 0.34 [0.31, 0.37] |
| 14 | 0.18 [0.15, 0.22] | 0.37 [0.34, 0.40] | 0.30 [0.28, 0.33] | 0.33 [0.31, 0.36] |  | 0.36 [0.32, 0.39] | 0.38 [0.35, 0.41] | 0.37 [0.34, 0.40] | 0.33 [0.30, 0.36] |
| 21 | 0.15 [0.12, 0.18] | 0.35 [0.32, 0.38] | 0.29 [0.27, 0.32] | 0.34 [0.31, 0.37] |  | 0.33 [0.30, 0.37] | 0.35 [0.32, 0.38] | 0.34 [0.31, 0.37] | 0.33 [0.30, 0.36] |
| 30 | 0.16 [0.13, 0.20] | 0.35 [0.32, 0.38] | 0.32 [0.29, 0.35] | 0.32 [0.29, 0.35] |  | 0.32 [0.28, 0.36] | 0.36 [0.33, 0.39] | 0.35 [0.32, 0.38] | 0.34 [0.31, 0.37] |
| 45 | 0.29 [0.21, 0.37] | 0.34 [0.31, 0.37] | 0.32 [0.29, 0.35] | 0.31 [0.28, 0.34] |  | 0.34 [0.29, 0.39] | 0.33 [0.30, 0.36] | 0.33 [0.29, 0.36] | 0.33 [0.30, 0.36] |
| 60 |  | 0.34 [0.31, 0.38] | 0.30 [0.27, 0.33] | 0.33 [0.30, 0.37] |  | 1.00 [1.00, 1.00] | 0.35 [0.31, 0.38] | 0.32 [0.29, 0.35] | 0.34 [0.31, 0.37] |
| 90 |  | 0.29 [0.26, 0.33] | 0.27 [0.24, 0.31] | 0.34 [0.30, 0.37] |  |  | 0.31 [0.28, 0.35] | 0.31 [0.27, 0.34] | 0.31 [0.27, 0.35] |
| 120 |  | 0.29 [0.25, 0.33] | 0.26 [0.22, 0.30] | 0.32 [0.28, 0.36] |  |  | 0.28 [0.23, 0.32] | 0.28 [0.24, 0.32] | 0.29 [0.24, 0.33] |
| 180 |  | 0.23 [0.17, 0.29] | 0.19 [0.14, 0.25] | 0.28 [0.22, 0.35] |  |  | 0.28 [0.21, 0.35] | 0.27 [0.21, 0.34] | 0.20 [0.14, 0.25] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_chronos2 | lvl_clim | lvl_naive4 | lvl_tft | lvl_tirex2 | lvl_toto2 | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.24 [-0.30, -0.17] | 0.06 [0.03, 0.10]* | -0.07 [-0.11, -0.03] | 0.03 [-0.01, 0.07] | 0.05 [0.02, 0.09] | 0.04 [0.01, 0.08] | 0.05 [0.02, 0.10] | -0.03 [-0.08, 0.02] | lvl_snaive7 |
| 2 | -0.22 [-0.26, -0.17] | 0.02 [-0.03, 0.06] | -0.09 [-0.13, -0.05] | -0.01 [-0.05, 0.02] | -0.01 [-0.05, 0.04] | 0.01 [-0.02, 0.05] | 0.00 [-0.04, 0.04] | -0.04 [-0.08, -0.01] | lvl_snaive7 |
| 3 | -0.22 [-0.28, -0.16] | 0.00 [-0.03, 0.04] | -0.11 [-0.15, -0.07] | 0.01 [-0.02, 0.05] | -0.02 [-0.05, 0.03] | -0.02 [-0.05, 0.02] | 0.00 [-0.03, 0.04] | -0.06 [-0.09, -0.02] | lvl_snaive7 |
| 4 | -0.21 [-0.27, -0.14] | 0.02 [-0.01, 0.06] | -0.08 [-0.12, -0.03] | -0.01 [-0.04, 0.03] | 0.00 [-0.03, 0.04] | 0.02 [-0.01, 0.05] | 0.01 [-0.03, 0.04] | -0.06 [-0.10, -0.01] | lvl_snaive7 |
| 5 | -0.24 [-0.30, -0.18] | -0.01 [-0.04, 0.03] | -0.09 [-0.14, -0.04] | 0.00 [-0.03, 0.03] | -0.02 [-0.06, 0.02] | 0.00 [-0.04, 0.04] | 0.01 [-0.02, 0.05] | -0.06 [-0.10, -0.02] | lvl_snaive7 |
| 6 | -0.29 [-0.35, -0.22] | 0.01 [-0.03, 0.05] | -0.08 [-0.13, -0.04] | 0.04 [-0.00, 0.08] | -0.05 [-0.09, 0.00] | 0.00 [-0.04, 0.05] | 0.02 [-0.02, 0.06] | -0.03 [-0.07, 0.01] | lvl_snaive7 |
| 7 | -0.16 [-0.23, -0.10] | 0.04 [-0.00, 0.07] | -0.04 [-0.07, -0.01] |  | 0.07 [0.03, 0.11]* | 0.05 [0.02, 0.09]* | 0.03 [-0.01, 0.08] | -0.01 [-0.03, 0.02] | lvl_naive4 |
| 10 | -0.19 [-0.25, -0.13] | 0.00 [-0.03, 0.04] | -0.05 [-0.09, -0.01] |  | 0.00 [-0.03, 0.04] | 0.01 [-0.02, 0.05] | -0.01 [-0.04, 0.03] | -0.04 [-0.07, -0.00] | lvl_naive4 |
| 14 | -0.16 [-0.24, -0.08] | 0.03 [0.00, 0.07]* | -0.03 [-0.06, -0.00] |  | 0.01 [-0.03, 0.05] | 0.04 [0.02, 0.07]* | 0.04 [0.00, 0.07] | -0.00 [-0.04, 0.03] | lvl_naive4 |
| 21 | -0.19 [-0.27, -0.12] | 0.01 [-0.03, 0.05] | -0.04 [-0.09, 0.00] |  | 0.01 [-0.03, 0.06] | 0.02 [-0.02, 0.06] | 0.01 [-0.03, 0.04] | -0.01 [-0.04, 0.02] | lvl_naive4 |
| 30 | -0.18 [-0.25, -0.11] | 0.02 [-0.01, 0.06] | -0.00 [-0.04, 0.04] |  | -0.01 [-0.07, 0.04] | 0.03 [-0.00, 0.07] | 0.02 [-0.01, 0.06] | 0.02 [-0.01, 0.05] | lvl_naive4 |
| 45 | -0.12 [-0.28, 0.06] | 0.03 [-0.01, 0.06] | 0.00 [-0.03, 0.04] |  | 0.04 [-0.01, 0.09] | 0.02 [-0.02, 0.05] | 0.01 [-0.03, 0.05] | 0.02 [-0.01, 0.06] | lvl_naive4 |
| 60 |  | 0.01 [-0.04, 0.06] | -0.03 [-0.09, 0.02] |  | 1.00 [1.00, 1.00] | 0.01 [-0.03, 0.05] | -0.02 [-0.06, 0.02] | 0.00 [-0.02, 0.04] | lvl_naive4 |
| 90 |  | -0.04 [-0.07, -0.01] | -0.06 [-0.12, -0.00] |  |  | -0.02 [-0.07, 0.02] | -0.03 [-0.07, 0.02] | -0.02 [-0.06, 0.02] | lvl_naive4 |
| 120 |  | -0.03 [-0.09, 0.03] | -0.05 [-0.12, 0.01] |  |  | -0.04 [-0.10, 0.01] | -0.04 [-0.11, 0.02] | -0.03 [-0.07, 0.02] | lvl_naive4 |
| 180 |  | -0.04 [-0.12, 0.02] | -0.09 [-0.19, -0.00] |  |  | 0.00 [-0.06, 0.05] | -0.01 [-0.08, 0.07] | -0.09 [-0.17, -0.02] | lvl_naive4 |

| lead | model | park_days | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|
| 1 | lvl_naive4 | 1097 | 0.85 | 0.58 | 0.67 |
| 1 | lvl_wt56 | 1105 | 0.83 | 0.44 | 0.66 |
| 1 | lvl_snaive7 | 1095 | 0.82 | 0.64 | 0.62 |
| 1 | lvl_clim | 1105 | 0.81 | 0.36 | 0.72 |
| 1 | lvl_tft | 755 | 0.88 | 0.61 | 0.76 |
| 1 | lvl_cbd | 611 | 0.67 | 0.09 | 1.00 |
| 1 | lvl_tirex2 | 1105 | 0.89 | 0.54 | 0.74 |
| 1 | lvl_toto2 | 1105 | 0.87 | 0.53 | 0.72 |
| 1 | lvl_chronos2 | 1105 | 0.89 | 0.56 | 0.76 |
| 7 | lvl_naive4 | 1040 | 0.81 | 0.52 | 0.58 |
| 7 | lvl_wt56 | 1050 | 0.81 | 0.44 | 0.61 |
| 7 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 7 | lvl_clim | 1050 | 0.78 | 0.33 | 0.61 |
| 7 | lvl_tft | 696 | 0.82 | 0.54 | 0.67 |
| 7 | lvl_cbd | 565 | 0.55 | 0.05 | 0.90 |
| 7 | lvl_tirex2 | 1050 | 0.84 | 0.50 | 0.72 |
| 7 | lvl_toto2 | 1050 | 0.82 | 0.46 | 0.67 |
| 7 | lvl_chronos2 | 1050 | 0.86 | 0.51 | 0.70 |
| 30 | lvl_naive4 | 931 | 0.76 | 0.46 | 0.48 |
| 30 | lvl_wt56 | 943 | 0.77 | 0.41 | 0.55 |
| 30 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_clim | 943 | 0.79 | 0.37 | 0.69 |
| 30 | lvl_tft | 491 | 0.78 | 0.51 | 0.51 |
| 30 | lvl_cbd | 462 | 0.58 | 0.06 | 0.80 |
| 30 | lvl_tirex2 | 943 | 0.81 | 0.45 | 0.67 |
| 30 | lvl_toto2 | 943 | 0.76 | 0.39 | 0.55 |
| 30 | lvl_chronos2 | 943 | 0.81 | 0.47 | 0.69 |
| 90 | lvl_naive4 | 597 | 0.77 | 0.51 | 0.52 |
| 90 | lvl_wt56 | 606 | 0.78 | 0.36 | 0.58 |
| 90 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_clim | 606 | 0.73 | 0.29 | 0.59 |
| 90 | lvl_tft | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_cbd | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_tirex2 | 606 | 0.79 | 0.36 | 0.62 |
| 90 | lvl_toto2 | 606 | 0.76 | 0.28 | 0.59 |
| 90 | lvl_chronos2 | 606 | 0.78 | 0.39 | 0.54 |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 5807 | 4058 | 0.70 |
| 1 | lvl_chronos2 | 10666 | 8355 | 0.78 |
| 1 | lvl_clim | 10666 | 7018 | 0.66 |
| 1 | lvl_naive4 | 10518 | 7783 | 0.74 |
| 1 | lvl_snaive7 | 10459 | 7540 | 0.72 |
| 1 | lvl_tft | 8036 | 5948 | 0.74 |
| 1 | lvl_tirex2 | 10666 | 8352 | 0.78 |
| 1 | lvl_toto2 | 10666 | 8184 | 0.77 |
| 1 | lvl_wt56 | 10666 | 7409 | 0.69 |
| 7 | lvl_cbd | 5442 | 3390 | 0.62 |
| 7 | lvl_chronos2 | 10027 | 7378 | 0.74 |
| 7 | lvl_clim | 10027 | 6183 | 0.62 |
| 7 | lvl_naive4 | 9870 | 6824 | 0.69 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 7361 | 5097 | 0.69 |
| 7 | lvl_tirex2 | 10027 | 7404 | 0.74 |
| 7 | lvl_toto2 | 10027 | 7115 | 0.71 |
| 7 | lvl_wt56 | 10027 | 6672 | 0.67 |
| 30 | lvl_cbd | 4378 | 2640 | 0.60 |
| 30 | lvl_chronos2 | 8946 | 5895 | 0.66 |
| 30 | lvl_clim | 8946 | 5213 | 0.58 |
| 30 | lvl_naive4 | 8772 | 5392 | 0.61 |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 5302 | 3410 | 0.64 |
| 30 | lvl_tirex2 | 8946 | 5873 | 0.66 |
| 30 | lvl_toto2 | 8946 | 5573 | 0.62 |
| 30 | lvl_wt56 | 8946 | 5459 | 0.61 |
| 90 | lvl_cbd | 0 | 0 |  |
| 90 | lvl_chronos2 | 5274 | 3315 | 0.63 |
| 90 | lvl_clim | 5274 | 2689 | 0.51 |
| 90 | lvl_naive4 | 5155 | 3311 | 0.64 |
| 90 | lvl_snaive7 | 0 | 0 |  |
| 90 | lvl_tft | 0 | 0 |  |
| 90 | lvl_tirex2 | 5274 | 3385 | 0.64 |
| 90 | lvl_toto2 | 5274 | 3108 | 0.59 |
| 90 | lvl_wt56 | 5274 | 3256 | 0.62 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie (counted wrong); CI over (origin, park).

**day comparison winner accuracy**

| lead | lvl_cbd | lvl_chronos2 | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_tirex2 | lvl_toto2 | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|---|
| d1-7 | 0.18 [0.16, 0.20] | 0.26 [0.24, 0.28] | 0.19 [0.18, 0.21] | 0.39 [0.38, 0.41] | 0.44 [0.42, 0.45] | 0.35 [0.33, 0.37] | 0.20 [0.18, 0.21] | 0.19 [0.18, 0.21] | 0.20 [0.19, 0.22] |
| d8-30 | 0.12 [0.11, 0.13] | 0.24 [0.23, 0.26] | 0.18 [0.17, 0.19] | 0.30 [0.30, 0.31] |  | 0.30 [0.29, 0.32] | 0.19 [0.17, 0.20] | 0.15 [0.14, 0.16] | 0.17 [0.16, 0.18] |
| d31-90 | 0.11 [0.09, 0.12] | 0.26 [0.25, 0.27] | 0.20 [0.19, 0.21] | 0.29 [0.28, 0.30] |  | 0.29 [0.28, 0.31] | 0.20 [0.18, 0.21] | 0.15 [0.14, 0.16] | 0.17 [0.16, 0.18] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_chronos2 | lvl_clim | lvl_naive4 | lvl_tft | lvl_tirex2 | lvl_toto2 | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| d1-7 | -0.25 [-0.29, -0.21] | -0.18 [-0.22, -0.13] | -0.25 [-0.29, -0.21] | -0.04 [-0.07, -0.02] | -0.10 [-0.14, -0.07] | -0.24 [-0.28, -0.21] | -0.24 [-0.28, -0.20] | -0.23 [-0.27, -0.21] | lvl_snaive7 |
| d8-30 | -0.16 [-0.19, -0.13] | -0.06 [-0.10, -0.02] | -0.12 [-0.15, -0.09] |  | -0.00 [-0.03, 0.03] | -0.12 [-0.15, -0.09] | -0.16 [-0.18, -0.13] | -0.14 [-0.15, -0.12] | lvl_naive4 |
| d31-90 | -0.15 [-0.18, -0.12] | -0.03 [-0.06, 0.01] | -0.09 [-0.12, -0.06] |  | 0.02 [-0.01, 0.06] | -0.09 [-0.12, -0.06] | -0.14 [-0.17, -0.12] | -0.12 [-0.13, -0.11] | lvl_naive4 |

| lead | model | tie_rate |
|---|---|---|
| d1-7 | lvl_naive4 | 0.43 |
| d1-7 | lvl_wt56 | 0.69 |
| d1-7 | lvl_snaive7 | 0.33 |
| d1-7 | lvl_clim | 0.71 |
| d1-7 | lvl_tft | 0.49 |
| d1-7 | lvl_cbd | 0.72 |
| d1-7 | lvl_tirex2 | 0.74 |
| d1-7 | lvl_toto2 | 0.74 |
| d1-7 | lvl_chronos2 | 0.65 |
| d8-30 | lvl_naive4 | 0.51 |
| d8-30 | lvl_wt56 | 0.72 |
| d8-30 | lvl_clim | 0.72 |
| d8-30 | lvl_tft | 0.51 |
| d8-30 | lvl_cbd | 0.78 |
| d8-30 | lvl_tirex2 | 0.73 |
| d8-30 | lvl_toto2 | 0.76 |
| d8-30 | lvl_chronos2 | 0.64 |
| d31-90 | lvl_naive4 | 0.50 |
| d31-90 | lvl_wt56 | 0.72 |
| d31-90 | lvl_clim | 0.67 |
| d31-90 | lvl_tft | 0.51 |
| d31-90 | lvl_cbd | 0.80 |
| d31-90 | lvl_tirex2 | 0.71 |
| d31-90 | lvl_toto2 | 0.75 |
| d31-90 | lvl_chronos2 | 0.60 |

Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the 1st and 15th):

| model | star_precision | star_recall | pred_stars | true_stars |
|---|---|---|---|---|
| lvl_chronos2 | 0.40 [0.30, 0.51] | 0.17 [0.11, 0.25] | 187 | 430 |
| lvl_clim | 0.19 [0.04, 0.40] | 0.02 [0.00, 0.04] | 42 | 430 |
| lvl_naive4 | 0.32 [0.24, 0.40] | 0.22 [0.15, 0.29] | 292 | 425 |
| lvl_snaive7 | 0.24 [0.08, 0.40] | 0.30 [0.11, 0.47] | 25 | 20 |
| lvl_tirex2 | 0.48 [0.30, 0.64] | 0.13 [0.06, 0.21] | 112 | 430 |
| lvl_toto2 | 0.38 [0.26, 0.52] | 0.12 [0.06, 0.18] | 132 | 430 |
| lvl_wt56 | 0.20 [0.05, 0.40] | 0.02 [0.00, 0.05] | 50 | 430 |

## D8 — uncertainty

**What production states** (`forecast-accuracy.service.ts`): MAE of TFT's predicted peak against the day's max hourly P90, by predicted band × lead bucket over the 45 days before the origin (cells with ≥ 500 comparisons). Realised = the same error of the TFT forecasts served at the origin; ratio > 1 = production understates its error.

| bucket | band | n | realised_mae | stated_mae | ratio |
|---|---|---|---|---|---|
| d1 | busy | 1506.00 | 22.93 | 22.16 | 1.03 |
| d1 | mid | 4836.00 | 12.54 | 14.30 | 0.88 |
| d1 | quiet | 13404.00 | 5.59 | 6.79 | 0.82 |
| d14 | busy | 9354.00 | 23.96 | 24.59 | 0.97 |
| d14 | mid | 28730.00 | 14.38 | 15.77 | 0.91 |
| d14 | quiet | 80319.00 | 6.73 | 7.66 | 0.88 |
| d3 | busy | 3081.00 | 23.48 | 22.58 | 1.04 |
| d3 | mid | 9159.00 | 13.65 | 14.55 | 0.94 |
| d3 | quiet | 25638.00 | 6.07 | 6.92 | 0.88 |
| d30 | busy | 19217.00 | 24.97 | 25.20 | 0.99 |
| d30 | mid | 54936.00 | 14.82 | 16.24 | 0.91 |
| d30 | quiet | 153089.00 | 6.96 | 8.08 | 0.86 |
| d60 | busy | 14195.00 | 26.81 | 23.83 | 1.13 |
| d60 | mid | 43327.00 | 15.41 | 17.25 | 0.89 |
| d60 | quiet | 105818.00 | 6.60 | 9.79 | 0.67 |
| d7 | busy | 5858.00 | 23.42 | 23.53 | 1.00 |
| d7 | mid | 18028.00 | 13.72 | 14.96 | 0.92 |
| d7 | quiet | 50233.00 | 6.19 | 7.12 | 0.87 |

Empirical-quantile coverage (target 0.80 / 0.95) and the share of truth slots that HAVE an interval:

| L | wt_med cov_q80 | wt_med cov_q95 | wt_med field_coverage | tirex2 cov_q80 | tirex2 cov_q95 | tirex2 field_coverage | toto2 cov_q80 | toto2 cov_q95 | toto2 field_coverage | toto2_nocov cov_q80 | toto2_nocov cov_q95 | toto2_nocov field_coverage | tirex2_grid cov_q80 | tirex2_grid cov_q95 | tirex2_grid field_coverage | toto2_grid cov_q80 | toto2_grid cov_q95 | toto2_grid field_coverage | chronos2 cov_q80 | chronos2 cov_q95 | chronos2 field_coverage | chronos2_nocov cov_q80 | chronos2_nocov cov_q95 | chronos2_nocov field_coverage |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.86 | 0.94 | 0.98 | 0.80 |  | 0.99 | 0.80 |  | 0.99 | 0.79 |  | 0.99 | 0.81 |  | 0.99 | 0.54 |  | 0.99 | 0.79 | 0.93 | 0.99 | 0.77 | 0.93 | 0.99 |
| 1 | 0.87 | 0.94 | 0.98 | 0.82 |  | 1.00 | 0.81 |  | 1.00 | 0.81 |  | 1.00 | 0.81 |  | 0.99 | 0.54 |  | 1.00 | 0.81 | 0.95 | 1.00 | 0.79 | 0.94 | 1.00 |
| 2 | 0.86 | 0.93 | 0.98 | 0.82 |  | 1.00 | 0.80 |  | 1.00 | 0.80 |  | 1.00 | 0.81 |  | 0.99 | 0.54 |  | 1.00 | 0.80 | 0.95 | 1.00 | 0.79 | 0.94 | 1.00 |
| 3 | 0.86 | 0.93 | 0.97 | 0.83 |  | 0.99 | 0.81 |  | 0.99 | 0.81 |  | 0.99 | 0.70 |  | 0.99 | 0.55 |  | 0.99 | 0.81 | 0.95 | 0.99 | 0.80 | 0.95 | 0.99 |
| 4 | 0.85 | 0.93 | 0.97 | 0.82 |  | 0.99 | 0.80 |  | 0.99 | 0.80 |  | 0.99 | 0.65 |  | 0.99 | 0.54 |  | 0.99 | 0.80 | 0.95 | 0.99 | 0.79 | 0.95 | 0.99 |
| 5 | 0.86 | 0.93 | 0.97 | 0.81 |  | 0.99 | 0.81 |  | 0.99 | 0.81 |  | 0.99 | 0.66 |  | 0.99 | 0.54 |  | 0.99 | 0.81 | 0.95 | 0.99 | 0.80 | 0.95 | 0.99 |
| 6 | 0.86 | 0.94 | 0.97 | 0.77 |  | 0.99 | 0.82 |  | 0.99 | 0.82 |  | 0.99 | 0.68 |  | 0.99 | 0.55 |  | 0.99 | 0.82 | 0.96 | 0.99 | 0.82 | 0.96 | 0.99 |
| 7 | 0.85 | 0.93 | 0.97 | 0.70 |  | 0.99 | 0.80 |  | 0.99 | 0.80 |  | 0.99 | 0.61 |  | 0.99 | 0.54 |  | 0.99 | 0.81 | 0.96 | 0.99 | 0.80 | 0.96 | 0.99 |
| 10 | 0.85 | 0.93 | 0.97 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 14 | 0.85 | 0.92 | 0.96 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 21 | 0.85 | 0.92 | 0.95 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 30 | 0.85 | 0.92 | 0.94 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 45 | 0.85 | 0.92 | 0.93 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 60 | 0.86 | 0.92 | 0.91 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 90 | 0.87 | 0.93 | 0.88 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |

Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:

| model | L | realised | stated_weighted | ratio |
|---|---|---|---|---|
| snaive7 | 0 | 8.35 | 8.48 | 0.98 |
| snaive7 | 1 | 8.24 | 8.33 | 0.99 |
| snaive7 | 3 | 8.15 | 8.41 | 0.97 |
| wt_med | 0 | 7.09 | 7.22 | 0.98 |
| wt_med | 1 | 6.89 | 7.00 | 0.99 |
| wt_med | 3 | 7.26 | 7.39 | 0.98 |
| wt_med | 7 | 7.46 | 7.61 | 0.98 |
| wt_med | 14 | 7.66 | 7.72 | 0.99 |
| wt_med | 30 | 7.70 | 7.97 | 0.97 |
| wt_med | 60 | 7.82 | 8.20 | 0.95 |
| wt_med | 90 | 7.38 | 9.19 | 0.80 |
| clim | 0 | 7.62 | 7.79 | 0.98 |
| clim | 1 | 7.40 | 7.53 | 0.98 |
| clim | 3 | 7.79 | 7.87 | 0.99 |
| clim | 7 | 7.82 | 7.97 | 0.98 |
| clim | 14 | 8.03 | 8.08 | 0.99 |
| clim | 30 | 7.90 | 8.24 | 0.96 |
| clim | 60 | 8.01 | 8.45 | 0.95 |
| clim | 90 | 8.07 | 9.17 | 0.88 |
| h5 | 0 | 7.04 | 7.17 | 0.98 |
| h5 | 1 | 6.83 | 6.94 | 0.98 |
| h5 | 3 | 7.18 | 7.31 | 0.98 |
| h5 | 7 | 7.38 | 7.54 | 0.98 |
| h5 | 14 | 7.58 | 7.62 | 0.99 |
| h5 | 30 | 7.58 | 7.84 | 0.97 |
| h5 | 60 | 7.71 | 8.00 | 0.96 |
| h5 | 90 | 7.29 | 8.96 | 0.81 |
| lvlh5_naive | 0 | 7.38 | 7.49 | 0.99 |
| lvlh5_naive | 1 | 7.10 | 7.18 | 0.99 |
| lvlh5_naive | 3 | 7.45 | 7.58 | 0.98 |
| lvlh5_naive | 7 | 7.68 | 7.77 | 0.99 |
| lvlh5_naive | 14 | 8.05 | 8.06 | 1.00 |
| lvlh5_naive | 30 | 8.16 | 8.36 | 0.98 |
| lvlh5_naive | 60 | 8.23 | 8.38 | 0.98 |
| lvlh5_naive | 90 | 7.85 | 9.08 | 0.86 |
| lvlh5_tft | 0 | 6.80 | 6.98 | 0.97 |
| lvlh5_tft | 1 | 6.74 | 6.74 | 1.00 |
| lvlh5_tft | 3 | 7.23 | 7.21 | 1.00 |
| lvlh5_tft | 7 | 7.44 | 7.48 | 0.99 |
| lvlh5_tft | 14 | 7.91 | 7.70 | 1.03 |
| lvlh5_tft | 30 | 8.23 | 7.73 | 1.06 |
| lvlh5_cbd | 0 | 9.58 | 9.47 | 1.01 |
| lvlh5_cbd | 1 | 9.25 | 9.17 | 1.01 |
| lvlh5_cbd | 3 | 9.79 | 9.68 | 1.01 |
| lvlh5_cbd | 7 | 10.32 | 10.07 | 1.02 |
| lvlh5_cbd | 14 | 10.48 | 10.20 | 1.03 |
| lvlh5_cbd | 30 | 10.50 | 10.81 | 0.97 |
| prod_served | 3 | 7.82 | 7.76 | 1.01 |
| prod_served | 7 | 8.00 | 8.06 | 0.99 |
| prod_served | 14 | 8.44 | 8.40 | 1.00 |
| prod_served | 30 | 9.07 | 8.66 | 1.05 |
| prod_served_lin | 3 | 7.80 | 7.74 | 1.01 |
| prod_served_lin | 7 | 7.98 | 8.04 | 0.99 |
| prod_served_lin | 14 | 8.41 | 8.38 | 1.00 |
| prod_served_lin | 30 | 9.04 | 8.64 | 1.05 |
| tirex2 | 0 | 7.00 | 7.12 | 0.98 |
| tirex2 | 1 | 7.18 | 7.22 | 0.99 |
| tirex2 | 3 | 7.64 | 7.78 | 0.98 |
| tirex2 | 7 | 8.03 | 8.21 | 0.98 |
| tirex2_x_h5 | 1 | 6.79 | 6.91 | 0.98 |
| tirex2_x_h5 | 3 | 7.19 | 7.35 | 0.98 |
| tirex2_x_h5 | 7 | 7.35 | 7.60 | 0.97 |
| tirex2_x_h5 | 14 | 7.59 | 7.80 | 0.97 |
| tirex2_x_h5 | 30 | 7.75 | 8.12 | 0.96 |
| tirex2_x_h5 | 60 | 8.00 | 8.26 | 0.97 |
| tirex2_x_h5 | 90 | 7.84 | 9.24 | 0.85 |
| toto2 | 0 | 6.99 | 7.11 | 0.98 |
| toto2 | 1 | 7.23 | 7.26 | 1.00 |
| toto2 | 3 | 7.85 | 7.97 | 0.98 |
| toto2 | 7 | 8.22 | 8.36 | 0.98 |
| toto2_x_h5 | 1 | 6.82 | 6.97 | 0.98 |
| toto2_x_h5 | 3 | 7.25 | 7.45 | 0.97 |
| toto2_x_h5 | 7 | 7.48 | 7.87 | 0.95 |
| toto2_x_h5 | 14 | 7.70 | 8.13 | 0.95 |
| toto2_x_h5 | 30 | 7.92 | 8.50 | 0.93 |
| toto2_x_h5 | 60 | 8.14 | 8.47 | 0.96 |
| toto2_x_h5 | 90 | 7.94 | 9.05 | 0.88 |
| toto2_nocov | 0 | 7.03 | 7.15 | 0.98 |
| toto2_nocov | 1 | 7.27 | 7.29 | 1.00 |
| toto2_nocov | 3 | 7.89 | 8.01 | 0.98 |
| toto2_nocov | 7 | 8.26 | 8.39 | 0.98 |
| tirex2_grid | 0 | 6.56 | 6.69 | 0.98 |
| tirex2_grid | 1 | 6.81 | 6.86 | 0.99 |
| tirex2_grid | 3 | 7.27 | 7.40 | 0.98 |
| tirex2_grid | 7 | 7.68 | 7.89 | 0.97 |
| toto2_grid | 0 | 11.38 | 11.50 | 0.99 |
| toto2_grid | 1 | 11.56 | 11.54 | 1.00 |
| toto2_grid | 3 | 11.75 | 11.92 | 0.99 |
| toto2_grid | 7 | 11.90 | 11.89 | 1.00 |
| chronos2 | 0 | 6.50 | 6.64 | 0.98 |
| chronos2 | 1 | 6.74 | 6.79 | 0.99 |
| chronos2 | 3 | 7.22 | 7.38 | 0.98 |
| chronos2 | 7 | 7.58 | 7.76 | 0.98 |
| chronos2_x_h5 | 1 | 6.71 | 6.84 | 0.98 |
| chronos2_x_h5 | 3 | 7.16 | 7.33 | 0.98 |
| chronos2_x_h5 | 7 | 7.32 | 7.55 | 0.97 |
| chronos2_x_h5 | 14 | 7.59 | 7.77 | 0.98 |
| chronos2_x_h5 | 30 | 7.72 | 8.03 | 0.96 |
| chronos2_x_h5 | 60 | 7.95 | 8.05 | 0.99 |
| chronos2_x_h5 | 90 | 7.69 | 8.87 | 0.87 |
| chronos2_nocov | 0 | 6.93 | 7.05 | 0.98 |
| chronos2_nocov | 1 | 7.20 | 7.23 | 1.00 |
| chronos2_nocov | 3 | 7.72 | 7.87 | 0.98 |
| chronos2_nocov | 7 | 8.07 | 8.27 | 0.98 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | chronos2 | chronos2_nocov | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | tirex2 | tirex2_grid | tirex2_x_h5 | toto2 | toto2_grid | toto2_nocov | toto2_x_h5 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.99 | 0.99 |  | 0.94 | 0.98 | 0.53 | 0.95 | 0.65 | 0.98 | 0.95 |  |  | 0.86 | 0.99 | 0.99 |  | 0.99 | 0.99 | 0.99 |  | 0.98 |
| 1 | 1.00 | 1.00 | 0.98 | 0.93 | 0.98 | 0.52 | 0.95 | 0.64 | 0.98 | 0.95 |  |  | 0.88 | 1.00 | 0.99 | 0.98 | 1.00 | 1.00 | 1.00 | 0.98 | 0.98 |
| 2 | 1.00 | 1.00 | 0.98 | 0.93 | 0.98 | 0.52 | 0.96 | 0.64 | 0.98 | 0.96 |  |  | 0.87 | 1.00 | 0.99 | 0.98 | 1.00 | 1.00 | 1.00 | 0.98 | 0.98 |
| 3 | 0.99 | 0.99 | 0.98 | 0.93 | 0.98 | 0.52 | 0.96 | 0.64 | 0.98 | 0.96 | 0.64 | 0.64 | 0.87 | 0.99 | 0.99 | 0.98 | 0.99 | 0.99 | 0.99 | 0.98 | 0.97 |
| 4 | 0.99 | 0.99 | 0.98 | 0.93 | 0.98 | 0.52 | 0.96 | 0.63 | 0.98 | 0.96 | 0.64 | 0.64 | 0.87 | 0.99 | 0.99 | 0.98 | 0.99 | 0.99 | 0.99 | 0.98 | 0.97 |
| 5 | 0.99 | 0.99 | 0.98 | 0.93 | 0.98 | 0.52 | 0.96 | 0.64 | 0.98 | 0.96 | 0.64 | 0.64 | 0.87 | 0.99 | 0.99 | 0.98 | 0.99 | 0.99 | 0.99 | 0.98 | 0.97 |
| 6 | 0.99 | 0.99 | 0.98 | 0.93 | 0.98 | 0.51 | 0.96 | 0.63 | 0.97 | 0.96 | 0.63 | 0.63 | 0.89 | 0.99 | 0.99 | 0.98 | 0.99 | 0.99 | 0.99 | 0.98 | 0.97 |
| 7 | 0.99 | 0.99 | 0.98 | 0.93 | 0.98 | 0.51 | 0.94 | 0.63 | 0.97 | 0.94 | 0.63 | 0.63 |  | 0.99 | 0.99 | 0.98 | 0.99 | 0.99 | 0.99 | 0.98 | 0.97 |
| 10 |  |  | 0.97 | 0.93 | 0.97 | 0.50 | 0.95 | 0.62 | 0.97 | 0.95 | 0.63 | 0.63 |  |  |  | 0.97 |  |  |  | 0.97 | 0.97 |
| 14 |  |  | 0.97 | 0.92 | 0.97 | 0.49 | 0.93 | 0.61 | 0.97 | 0.94 | 0.62 | 0.62 |  |  |  | 0.97 |  |  |  | 0.97 | 0.96 |
| 21 |  |  | 0.96 | 0.91 | 0.96 | 0.47 | 0.92 | 0.59 | 0.96 | 0.93 | 0.59 | 0.59 |  |  |  | 0.96 |  |  |  | 0.96 | 0.95 |
| 30 |  |  | 0.96 | 0.90 | 0.96 | 0.45 | 0.92 | 0.47 | 0.95 | 0.92 | 0.49 | 0.49 |  |  |  | 0.96 |  |  |  | 0.96 | 0.94 |
| 45 |  |  | 0.94 | 0.89 | 0.94 | 0.11 | 0.90 | 0.34 | 0.94 | 0.91 | 0.37 | 0.37 |  |  |  | 0.94 |  |  |  | 0.94 | 0.93 |
| 60 |  |  | 0.92 | 0.87 | 0.92 |  | 0.88 | 0.01 | 0.92 | 0.89 | 0.01 | 0.01 |  |  |  | 0.92 |  |  |  | 0.92 | 0.91 |
| 90 |  |  | 0.90 | 0.84 | 0.90 |  | 0.86 |  | 0.89 | 0.87 |  |  |  |  |  | 0.90 |  |  |  | 0.90 | 0.88 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | lvl_tirex2 | lvl_toto2 | lvl_chronos2 | n_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.96 | 0.99 | 0.96 | 0.96 | 0.65 | 0.53 | 1.00 | 1.00 | 1.00 | 10515 |
| 2 | 0.97 | 0.99 | 0.95 | 0.96 | 0.64 | 0.52 | 1.00 | 1.00 | 1.00 | 10282 |
| 3 | 0.97 | 0.99 | 0.96 | 0.96 | 0.64 | 0.52 | 1.00 | 1.00 | 1.00 | 10282 |
| 4 | 0.97 | 0.99 | 0.96 | 0.96 | 0.64 | 0.52 | 1.00 | 1.00 | 1.00 | 10292 |
| 5 | 0.97 | 0.99 | 0.96 | 0.96 | 0.64 | 0.52 | 1.00 | 1.00 | 1.00 | 10331 |
| 6 | 0.97 | 0.99 | 0.97 | 0.96 | 0.63 | 0.51 | 1.00 | 1.00 | 1.00 | 10065 |
| 7 | 0.96 | 0.99 | 0.00 | 0.96 | 0.63 | 0.51 | 1.00 | 1.00 | 1.00 | 10041 |
| 10 | 0.96 | 0.99 | 0.00 | 0.96 | 0.63 | 0.51 | 1.00 | 1.00 | 1.00 | 10120 |
| 14 | 0.95 | 0.99 | 0.00 | 0.96 | 0.62 | 0.49 | 1.00 | 1.00 | 1.00 | 9850 |
| 21 | 0.94 | 0.98 | 0.00 | 0.96 | 0.59 | 0.47 | 1.00 | 1.00 | 1.00 | 9415 |
| 30 | 0.94 | 0.98 | 0.00 | 0.95 | 0.47 | 0.45 | 1.00 | 1.00 | 1.00 | 9256 |
| 45 | 0.94 | 0.98 | 0.00 | 0.95 | 0.36 | 0.14 | 1.00 | 1.00 | 1.00 | 8575 |
| 60 | 0.92 | 0.98 | 0.00 | 0.94 | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 | 7843 |
| 90 | 0.92 | 0.97 | 0.00 | 0.93 | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 | 6299 |
| 120 | 0.88 | 0.96 | 0.00 | 0.92 | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 | 4757 |
| 180 | 0.82 | 0.93 | 0.00 | 0.93 | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 | 1947 |

Forecast present vs ride operated. The baselines have **no openness logic** — they forecast whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never opened still got a curve; it is the bar a model with an open/closed decision has to beat:

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | prod_served_lin P(fc|operated) | prod_served_lin P(fc|not operated) | tirex2 P(fc|operated) | tirex2 P(fc|not operated) | tirex2_x_h5 P(fc|operated) | tirex2_x_h5 P(fc|not operated) | toto2 P(fc|operated) | toto2 P(fc|not operated) | toto2_x_h5 P(fc|operated) | toto2_x_h5 P(fc|not operated) | toto2_nocov P(fc|operated) | toto2_nocov P(fc|not operated) | tirex2_grid P(fc|operated) | tirex2_grid P(fc|not operated) | toto2_grid P(fc|operated) | toto2_grid P(fc|not operated) | chronos2 P(fc|operated) | chronos2 P(fc|not operated) | chronos2_x_h5 P(fc|operated) | chronos2_x_h5 P(fc|not operated) | chronos2_nocov P(fc|operated) | chronos2_nocov P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.88 | 0.28 | 0.96 | 0.88 | 0.93 | 0.91 | 0.96 | 0.89 | 0.93 | 0.80 | 0.63 | 0.55 | 0.50 | 0.42 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.96 | 0.00 | 0.00 | 1.00 | 1.00 | 0.00 | 0.00 | 1.00 | 1.00 | 0.99 | 0.69 | 1.00 | 0.90 | 1.00 | 1.00 | 0.00 | 0.00 | 1.00 | 1.00 | 34488.00 | 2521.00 |
| 1 | 0.89 | 0.26 | 0.96 | 0.88 | 0.93 | 0.91 | 0.96 | 0.88 | 0.93 | 0.80 | 0.63 | 0.54 | 0.49 | 0.42 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.95 | 0.95 | 0.87 | 1.00 | 1.00 | 0.95 | 0.87 | 1.00 | 1.00 | 0.99 | 0.69 | 1.00 | 0.90 | 1.00 | 1.00 | 0.95 | 0.87 | 1.00 | 1.00 | 33697.00 | 2343.00 |
| 2 | 0.89 | 0.28 | 0.96 | 0.88 | 0.93 | 0.89 | 0.96 | 0.89 | 0.93 | 0.80 | 0.62 | 0.55 | 0.49 | 0.43 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.96 | 0.95 | 0.88 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 0.98 | 0.70 | 1.00 | 0.90 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 33735.00 | 2429.00 |
| 3 | 0.90 | 0.27 | 0.96 | 0.89 | 0.93 | 0.89 | 0.96 | 0.89 | 0.93 | 0.79 | 0.62 | 0.54 | 0.49 | 0.42 | 0.62 | 0.53 | 0.62 | 0.53 | 1.00 | 0.96 | 0.95 | 0.87 | 1.00 | 1.00 | 0.95 | 0.87 | 1.00 | 1.00 | 0.98 | 0.70 | 0.99 | 0.90 | 1.00 | 1.00 | 0.95 | 0.87 | 1.00 | 1.00 | 33656.00 | 2475.00 |
| 4 | 0.90 | 0.29 | 0.96 | 0.89 | 0.93 | 0.89 | 0.96 | 0.89 | 0.93 | 0.81 | 0.62 | 0.55 | 0.50 | 0.41 | 0.62 | 0.55 | 0.62 | 0.55 | 1.00 | 0.96 | 0.95 | 0.88 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 0.98 | 0.72 | 0.99 | 0.91 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 33567.00 | 2542.00 |
| 5 | 0.90 | 0.28 | 0.96 | 0.89 | 0.93 | 0.90 | 0.96 | 0.89 | 0.93 | 0.81 | 0.62 | 0.54 | 0.50 | 0.42 | 0.62 | 0.54 | 0.62 | 0.54 | 1.00 | 0.96 | 0.95 | 0.88 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 0.98 | 0.72 | 0.99 | 0.90 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 33687.00 | 2513.00 |
| 6 | 0.91 | 0.26 | 0.96 | 0.89 | 0.93 | 0.91 | 0.96 | 0.90 | 0.93 | 0.81 | 0.62 | 0.53 | 0.49 | 0.42 | 0.61 | 0.53 | 0.61 | 0.53 | 1.00 | 0.96 | 0.95 | 0.88 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 0.98 | 0.72 | 1.00 | 0.92 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 32851.00 | 2321.00 |
| 7 | 0.00 | 0.00 | 0.96 | 0.89 | 0.93 | 0.90 | 0.96 | 0.90 | 0.92 | 0.82 | 0.61 | 0.55 | 0.49 | 0.42 | 0.61 | 0.54 | 0.61 | 0.54 | 1.00 | 0.96 | 0.95 | 0.88 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 0.98 | 0.73 | 0.99 | 0.92 | 1.00 | 1.00 | 0.95 | 0.88 | 1.00 | 1.00 | 32869.00 | 2407.00 |
| 10 | 0.00 | 0.00 | 0.96 | 0.90 | 0.93 | 0.90 | 0.96 | 0.91 | 0.93 | 0.82 | 0.61 | 0.53 | 0.48 | 0.41 | 0.61 | 0.53 | 0.61 | 0.53 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 32910.00 | 2501.00 |
| 14 | 0.00 | 0.00 | 0.95 | 0.90 | 0.93 | 0.89 | 0.96 | 0.90 | 0.92 | 0.83 | 0.60 | 0.51 | 0.47 | 0.39 | 0.60 | 0.50 | 0.60 | 0.50 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.95 | 0.89 | 0.00 | 0.00 | 31967.00 | 2525.00 |
| 21 | 0.00 | 0.00 | 0.95 | 0.90 | 0.93 | 0.88 | 0.95 | 0.90 | 0.92 | 0.82 | 0.58 | 0.47 | 0.45 | 0.37 | 0.57 | 0.47 | 0.57 | 0.47 | 0.00 | 0.00 | 0.94 | 0.89 | 0.00 | 0.00 | 0.94 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.94 | 0.89 | 0.00 | 0.00 | 30610.00 | 2309.00 |
| 30 | 0.00 | 0.00 | 0.95 | 0.91 | 0.93 | 0.87 | 0.95 | 0.91 | 0.92 | 0.83 | 0.46 | 0.40 | 0.43 | 0.36 | 0.47 | 0.42 | 0.47 | 0.42 | 0.00 | 0.00 | 0.94 | 0.90 | 0.00 | 0.00 | 0.94 | 0.90 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.94 | 0.90 | 0.00 | 0.00 | 30017.00 | 2444.00 |
| 45 | 0.00 | 0.00 | 0.95 | 0.89 | 0.92 | 0.86 | 0.95 | 0.90 | 0.92 | 0.83 | 0.35 | 0.33 | 0.11 | 0.03 | 0.36 | 0.31 | 0.36 | 0.31 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 27717.00 | 2345.00 |
| 60 | 0.00 | 0.00 | 0.95 | 0.90 | 0.92 | 0.85 | 0.95 | 0.90 | 0.91 | 0.83 | 0.01 | 0.00 | 0.00 | 0.00 | 0.01 | 0.00 | 0.01 | 0.00 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 25234.00 | 2238.00 |
| 90 | 0.00 | 0.00 | 0.94 | 0.90 | 0.91 | 0.85 | 0.94 | 0.91 | 0.91 | 0.84 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.94 | 0.88 | 0.00 | 0.00 | 20180.00 | 1935.00 |
