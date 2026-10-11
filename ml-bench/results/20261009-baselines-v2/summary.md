# ml-bench results

Run: `20261009-baselines-v2` · generated 2026-10-11T10:14+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with TARGET season. The common-window pass mixes lead with ORIGIN season instead. Both are confounded; neither alone identifies the horizon curve.

- code that produced `parts/` (the scoring): git `ea9a8438`, sources sha256 `25ed0c3b26d3accd`, image `sha256:a1e54cb8fea6`; plug-ins none
- code that produced THESE tables (the report): git `ca29858b` (from env), sources sha256 `efd8088545a5852a`, image `sha256:8fb8722a15eb` — recorded in `report-tables.json`
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 54.4 shard-minutes over 3 shard(s)
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
| MAE | UC1 | all | persistence | True | park-day | 8 | 8 | m120 | m120 |  |
| MAE | UC1 | busy | persistence | True | park-day | 8 | 8 | m120 | m120 |  |
| MAE | UC2 | all | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | lvlh5_tft | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2-intraday | all | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | wt_med | True | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | lvlh5_tft | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | busy | wt_med | True | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC3 | all | h5 | False | park-day | 14 | 14 | 45 | 90 |  |
| MAE | UC3 | all | wt_med | True | park-day | 14 | 14 | 30 | 30 |  |
| MAE | UC3 | busy | h5 | False | park-day | 14 | 14 | 45 | 90 |  |
| MAE | UC3 | busy | lvlh5_naive | False | park-day | 14 | 14 |  | 6 |  |
| MAE | UC3 | busy | lvlh5_tft | False | park-day | 13 | 12 | 2 | 2 |  |
| MAE | UC3 | busy | wt_med | True | park-day | 14 | 14 | 21 | 21 |  |
| MAE, schedule known at origin | UC3 | all | h5 | False | park-day | 15 | 15 | 45 | 90 |  |
| MAE, schedule known at origin | UC3 | all | wt_med | True | park-day | 15 | 15 | 45 | 90 |  |
| MAE, schedule projected at origin | UC3 | all | h5 | False | park-day | 15 | 15 | 14 | 14 |  |
| MAE, schedule projected at origin | UC3 | all | wt_med | True | park-day | 15 | 15 | 14 | 14 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | False | park-day | 15 | 15 | 90 | 90 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | False | park-day | 15 | 15 | 45 | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_tft | False | park-day | 14 | 13 | 30 | 30 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served | False | park-day | 11 | 10 |  | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served_lin | False | park-day | 11 | 10 |  | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| UC4 Spearman within park-month | UC4 | all | lvl_naive4 | True | park-month | 16 | 16 |  | 120 |  |
| UC4 Spearman within park-month | UC4 | all | lvl_snaive7 | True | park-month | 6 | 6 | 6 | 6 |  |
| best-time hit rate (top-2) | D3 | all | clim | True | park-day | 15 | 15 |  | 60 |  |
| best-time hit rate (top-2) | D3 | all | h5 | False | park-day | 15 | 15 | 60 | 60 |  |
| best-time hit rate (top-2) | D3 | all | wt_med | True | park-day | 15 | 15 | 14 | 14 |  |
| best-time hit rate (±30 min) | D3 | all | h5 | False | park-day | 15 | 15 | 60 | 60 |  |
| best-time hit rate (±30 min) | D3 | all | wt_med | True | park-day | 15 | 15 | 14 | 14 |  |
| best-time regret (min) | D3 | all | h5 | False | park-day | 15 | 15 | 60 | 60 |  |
| best-time regret (min) | D3 | all | wt_med | True | park-day | 15 | 15 | 14 | 14 |  |
| crowd bucket exact | D6 | all | lvl_naive4 | True | park-day | 16 | 16 |  | 120 |  |
| crowd bucket exact | D6 | all | lvl_snaive7 | True | park-day | 6 | 6 | 6 | 6 |  |
| day comparison winner accuracy | D7 | all | lvl_naive4 | True | park-origin | 3 | 3 |  | d31-90 |  |
| day comparison winner accuracy | D7 | all | lvl_snaive7 | True | park-origin | 1 | 1 | d1-7 | d1-7 |  |
| dayPeak Spearman across rides | UC3 | all | wt_med | True | park-day | 15 | 15 | 21 | 21 |  |
| dayPeak abs error (min) | UC3 | all | lvlh5_tft | False | park-day | 14 | 13 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | prod_served | False | park-day | 11 | 10 | 5 | 5 |  |
| dayPeak abs error (min) | UC3 | all | prod_served_lin | False | park-day | 11 | 10 | 5 | 5 |  |
| dayPeak abs error (min) | UC3 | all | wt_med | True | park-day | 15 | 15 | 5 | 10 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | False | park-day | 15 | 15 | 90 | 90 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | False | park-day | 15 | 15 | 90 | 90 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | False | park-day | 14 | 13 | 45 | 45 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served_lin | False | park-day | 11 | 10 | 45 | 45 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | True | park-day | 15 | 15 | 30 | 90 |  |
| first-hour MAE (opening-aligned) | D5 | all | h5 | False | park-day | 15 | 15 | 30 | 30 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | False | park-day | 15 | 15 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | False | park-day | 14 | 13 | 14 | 14 |  |
| first-hour MAE (opening-aligned) | D5 | all | wt_med | True | park-day | 15 | 15 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | h5 | False | park-day | 15 | 15 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 | 30 | 30 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 15 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | wt_med | True | park-day | 15 | 15 | 7 | 7 |  |
| first-hour MAE, schedule projected at origin | D5 | all | h5 | False | park-day | 15 | 15 |  | 6 |  |
| first-hour MAE, schedule projected at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 15 |  | 6 |  |
| first-hour MAE, schedule projected at origin | D5 | all | wt_med | True | park-day | 15 | 15 | 5 | 5 |  |
| live-window MAE (headliners) | D2 | headliners | persistence | True | park-day | 1 | 1 | w0-45 | w0-45 |  |
| next-best precision | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best recall | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| optimiser regret (min per park-day) | D4 | all | h5 | False | park-day | 5 | 5 | 30 | 30 |  |
| optimiser regret (min per park-day) | D4 | all | lvlh5_naive | False | park-day | 5 | 5 |  | 3 |  |
| optimiser regret (min per park-day) | D4 | all | lvlh5_tft | False | park-day | 5 | 5 |  | 3 |  |
| optimiser regret (min per park-day) | D4 | all | wt_med | True | park-day | 5 | 5 | 1 | 1 |  |
| rope-drop worth agreement | D5 | all | wt_med | True | park-day | 15 | 15 | 0 | 0 |  |


**Never tested** — `leads_tested = 0` means no lead passed the gates, which is NOT "nothing won". The reason per metric × model:

| metric | uc | segment | model | unit | leads_scored | not_tested_because |
|---|---|---|---|---|---|---|
| dayPeak pairwise ordering (headliners) | D4 | all | snaive7 | park-day | 7 | 0: coverage 0.26 < 0.30 against one of the candidates; 1: coverage 0.27 < 0.30 against one of the candidates; 2: coverage 0.27 < 0.30 against one of the candidates; 3: coverage 0.27 < 0.30 against one of the candidates; 4: coverage 0.27 < 0.30 against one of the candidates; 5: coverage 0.27 < 0.30 against one of the candidates |

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
| MAE | UC3 | busy | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
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
| first-hour MAE (opening-aligned) | D5 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 6 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
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
| MAE | UC2-intraday | busy | h2-4 | persistence/snaive7/wt_med/clim | wt_med | persistence is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | all | h4-8 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h4-8 | persistence/snaive7/wt_med/clim | wt_med | persistence is the ladder-first candidate but is significantly beaten by snaive7/wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | all | h8+ | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h8+ | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |

### Hand-over table (input for the serving router)

Per use case × lead: among the models that beat EVERY naive candidate (and pass the gates), the largest margin against the hardest candidate; otherwise the reference itself. The naive candidates themselves are excluded from the winner column even when one of them beats the others, because their margin is measured against a different and weaker pool — `usable_horizon.csv` keeps those rows, flagged `is_reference_candidate`. `park_day_unit_agrees = False` marks a row whose verdict changes under BENCH-SPEC's literal park-day bootstrap.

| uc | segment | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested | n_origin_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | wt_med | h5 | 7.484 | -0.062 | -0.079 | -0.047 | -0.066 | -0.057 | True | 0.997 | 1 | 7 | 231 |
| UC2 | busy | 0 | wt_med | lvlh5_tft | 13.690 | -0.565 | -0.862 | -0.288 | -0.682 | -0.447 | True | 0.659 | 2 | 7 | 231 |
| UC3 | all | 1 | wt_med | h5 | 7.586 | -0.067 | -0.085 | -0.052 | -0.072 | -0.062 | True | 0.996 | 1 | 7 | 230 |
| UC3 | busy | 1 | wt_med | lvlh5_tft | 13.902 | -0.495 | -0.796 | -0.207 | -0.613 | -0.382 | True | 0.655 | 2 | 7 | 230 |
| UC3 | all | 2 | wt_med | h5 | 7.623 | -0.070 | -0.088 | -0.054 | -0.074 | -0.065 | True | 0.995 | 1 | 7 | 229 |
| UC3 | busy | 2 | wt_med | lvlh5_tft | 14.038 | -0.415 | -0.699 | -0.149 | -0.536 | -0.304 | True | 0.653 | 2 | 7 | 229 |
| UC3 | all | 3 | wt_med | h5 | 7.654 | -0.072 | -0.090 | -0.057 | -0.077 | -0.068 | True | 0.994 | 1 | 9 | 228 |
| UC3 | busy | 3 | wt_med | h5 | 14.646 | -0.188 | -0.231 | -0.155 | -0.199 | -0.176 | True | 0.996 | 1 | 9 | 228 |
| UC3 | all | 4 | wt_med | h5 | 7.678 | -0.073 | -0.090 | -0.058 | -0.078 | -0.068 | True | 0.992 | 1 | 9 | 227 |
| UC3 | busy | 4 | wt_med | h5 | 14.684 | -0.190 | -0.232 | -0.156 | -0.202 | -0.178 | True | 0.994 | 1 | 9 | 227 |
| UC3 | all | 5 | wt_med | h5 | 7.710 | -0.074 | -0.091 | -0.059 | -0.079 | -0.070 | True | 0.990 | 1 | 9 | 226 |
| UC3 | busy | 5 | wt_med | h5 | 14.737 | -0.193 | -0.234 | -0.160 | -0.204 | -0.181 | True | 0.992 | 2 | 9 | 226 |
| UC3 | all | 6 | wt_med | h5 | 7.767 | -0.076 | -0.093 | -0.061 | -0.081 | -0.072 | True | 0.986 | 1 | 9 | 225 |
| UC3 | busy | 6 | wt_med | lvlh5_naive | 14.727 | -0.232 | -0.389 | -0.085 | -0.309 | -0.153 | True | 0.971 | 2 | 9 | 225 |
| UC3 | all | 7 | wt_med | h5 | 7.873 | -0.080 | -0.098 | -0.064 | -0.085 | -0.076 | True | 0.996 | 1 | 8 | 224 |
| UC3 | busy | 7 | wt_med | h5 | 15.031 | -0.206 | -0.250 | -0.170 | -0.217 | -0.194 | True | 0.998 | 1 | 8 | 224 |
| UC3 | all | 10 | wt_med | h5 | 7.971 | -0.084 | -0.102 | -0.068 | -0.089 | -0.079 | True | 0.995 | 1 | 8 | 221 |
| UC3 | busy | 10 | wt_med | h5 | 15.211 | -0.214 | -0.259 | -0.178 | -0.226 | -0.202 | True | 0.997 | 1 | 8 | 221 |
| UC3 | all | 14 | wt_med | h5 | 8.121 | -0.088 | -0.106 | -0.071 | -0.093 | -0.083 | True | 0.995 | 1 | 8 | 217 |
| UC3 | busy | 14 | wt_med | h5 | 15.482 | -0.219 | -0.268 | -0.178 | -0.230 | -0.207 | True | 0.997 | 1 | 8 | 217 |
| UC3 | all | 21 | wt_med | h5 | 8.280 | -0.095 | -0.116 | -0.078 | -0.100 | -0.090 | True | 0.994 | 1 | 8 | 210 |
| UC3 | busy | 21 | wt_med | h5 | 15.748 | -0.234 | -0.286 | -0.191 | -0.246 | -0.222 | True | 0.996 | 1 | 8 | 210 |
| UC3 | all | 30 | wt_med | h5 | 8.422 | -0.099 | -0.120 | -0.081 | -0.104 | -0.094 | True | 0.992 | 1 | 8 | 201 |
| UC3 | busy | 30 | wt_med | h5 | 15.952 | -0.243 | -0.298 | -0.199 | -0.256 | -0.231 | True | 0.995 | 1 | 8 | 201 |
| UC3 | all | 45 | wt_med | h5 | 8.537 | -0.104 | -0.128 | -0.085 | -0.109 | -0.099 | True | 0.991 | 1 | 7 | 186 |
| UC3 | busy | 45 | wt_med | h5 | 16.193 | -0.252 | -0.307 | -0.205 | -0.265 | -0.240 | True | 0.994 | 1 | 7 | 186 |
| UC3 | all | 60 | wt_med | wt_med | 8.829 | 0.000 |  |  |  |  | False |  | 0 | 4 | 171 |
| UC3 | busy | 60 | wt_med | wt_med | 16.786 | 0.000 |  |  |  |  | False |  | 0 | 4 | 171 |
| UC3 | all | 90 | wt_med | h5 | 8.754 | -0.096 | -0.120 | -0.075 | -0.102 | -0.090 | True | 0.987 | 1 | 4 | 141 |
| UC3 | busy | 90 | wt_med | h5 | 16.718 | -0.220 | -0.280 | -0.174 | -0.235 | -0.207 | True | 0.991 | 1 | 4 | 141 |
| UC2 | all | 1 | wt_med | h5 | 7.586 | -0.067 | -0.085 | -0.052 | -0.072 | -0.062 | True | 0.996 | 1 | 7 | 230 |
| UC2 | busy | 1 | wt_med | lvlh5_tft | 13.902 | -0.495 | -0.796 | -0.207 | -0.613 | -0.382 | True | 0.655 | 2 | 7 | 230 |
| UC1 | all | m015 | persistence | persistence | 2.457 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m015 | persistence | persistence | 4.528 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m030 | persistence | persistence | 4.148 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m030 | persistence | persistence | 7.739 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m045 | persistence | persistence | 5.073 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m045 | persistence | persistence | 9.450 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m060 | persistence | persistence | 5.697 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m060 | persistence | persistence | 10.618 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m075 | persistence | persistence | 6.284 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m075 | persistence | persistence | 11.566 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m090 | persistence | persistence | 6.792 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m090 | persistence | persistence | 12.516 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m105 | persistence | persistence | 7.164 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m105 | persistence | persistence | 13.153 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | all | m120 | persistence | persistence | 7.484 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC1 | busy | m120 | persistence | persistence | 13.758 | 0.000 |  |  |  |  | False |  | 0 | 7 | 231 |
| UC2-intraday | all | h2-4 | wt_med | h5 | 7.531 | -0.061 | -0.076 | -0.048 | -0.066 | -0.057 | True | 0.969 | 1 | 7 | 231 |
| UC2-intraday | busy | h2-4 | wt_med | lvlh5_tft | 13.794 | -0.570 | -0.879 | -0.287 | -0.689 | -0.450 | True | 0.655 | 2 | 7 | 231 |
| UC2-intraday | all | h4-8 | wt_med | h5 | 7.804 | -0.077 | -0.095 | -0.062 | -0.082 | -0.072 | True | 0.996 | 1 | 7 | 231 |
| UC2-intraday | busy | h4-8 | wt_med | lvlh5_tft | 14.076 | -0.532 | -0.835 | -0.248 | -0.652 | -0.403 | True | 0.656 | 2 | 7 | 231 |
| UC2-intraday | all | h8+ | wt_med | h5 | 7.509 | -0.091 | -0.124 | -0.062 | -0.100 | -0.082 | True | 0.994 | 1 | 6 | 231 |
| UC2-intraday | busy | h8+ | wt_med | h5 | 13.603 | -0.214 | -0.285 | -0.153 | -0.236 | -0.193 | True | 0.996 | 1 | 6 | 231 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | lvlh5_tft | 3.875 | -0.207 | -0.389 | -0.076 | -0.237 | -0.179 | True | 0.667 | 4 | 7 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | lvlh5_tft | 3.922 | -0.215 | -0.408 | -0.076 | -0.246 | -0.186 | True | 0.664 | 4 | 7 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | lvlh5_tft | 3.945 | -0.219 | -0.412 | -0.078 | -0.249 | -0.190 | True | 0.662 | 4 | 7 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | lvlh5_tft | 3.962 | -0.223 | -0.415 | -0.080 | -0.254 | -0.193 | True | 0.660 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | lvlh5_tft | 3.979 | -0.222 | -0.410 | -0.082 | -0.253 | -0.191 | True | 0.657 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | lvlh5_tft | 3.994 | -0.225 | -0.412 | -0.085 | -0.257 | -0.194 | True | 0.653 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | lvlh5_tft | 4.035 | -0.224 | -0.405 | -0.086 | -0.254 | -0.193 | True | 0.648 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | lvlh5_tft | 4.094 | -0.227 | -0.402 | -0.095 | -0.258 | -0.197 | True | 0.654 | 4 | 8 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | lvlh5_tft | 4.179 | -0.210 | -0.353 | -0.096 | -0.241 | -0.181 | True | 0.649 | 4 | 8 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | lvlh5_tft | 4.274 | -0.207 | -0.341 | -0.098 | -0.238 | -0.176 | True | 0.639 | 4 | 8 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | lvlh5_cbd | 4.561 | -0.201 | -0.357 | -0.075 | -0.245 | -0.163 | True | 0.485 | 3 | 8 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | h5 | 4.617 | -0.160 | -0.258 | -0.079 | -0.178 | -0.142 | True | 0.996 | 1 | 8 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | wt_med | 4.818 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| first-hour MAE (opening-aligned) | D5 | 60 | wt_med | wt_med | 4.883 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE (opening-aligned) | D5 | 90 | wt_med | wt_med | 4.987 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | h5 | 7.520 | -0.070 | -0.090 | -0.053 | -0.076 | -0.065 | True | 0.997 | 1 | 7 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | h5 | 7.633 | -0.076 | -0.096 | -0.059 | -0.082 | -0.071 | True | 0.995 | 1 | 7 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | h5 | 7.667 | -0.080 | -0.101 | -0.061 | -0.085 | -0.074 | True | 0.994 | 1 | 7 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 7.689 | -0.083 | -0.104 | -0.064 | -0.088 | -0.077 | True | 0.994 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | h5 | 7.700 | -0.084 | -0.105 | -0.065 | -0.089 | -0.078 | True | 0.992 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | h5 | 7.701 | -0.085 | -0.106 | -0.067 | -0.091 | -0.080 | True | 0.989 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 6 | wt_med | h5 | 7.724 | -0.088 | -0.109 | -0.069 | -0.093 | -0.082 | True | 0.986 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | h5 | 7.805 | -0.093 | -0.115 | -0.073 | -0.098 | -0.087 | True | 0.996 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | h5 | 7.844 | -0.099 | -0.121 | -0.080 | -0.105 | -0.093 | True | 0.995 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 14 | wt_med | h5 | 7.899 | -0.105 | -0.129 | -0.085 | -0.110 | -0.099 | True | 0.995 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 21 | wt_med | h5 | 7.938 | -0.117 | -0.144 | -0.095 | -0.124 | -0.112 | True | 0.994 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 30 | wt_med | h5 | 8.003 | -0.126 | -0.156 | -0.102 | -0.133 | -0.119 | True | 0.992 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 45 | wt_med | h5 | 7.875 | -0.127 | -0.158 | -0.100 | -0.134 | -0.120 | True | 0.990 | 1 | 7 |
| MAE, schedule known at origin | UC3 | 60 | wt_med | wt_med | 8.132 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule known at origin | UC3 | 90 | wt_med | h5 | 7.080 | -0.102 | -0.136 | -0.067 | -0.111 | -0.093 | True | 0.980 | 1 | 4 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | lvlh5_tft | 3.830 | -0.239 | -0.451 | -0.087 | -0.274 | -0.206 | True | 0.678 | 4 | 7 |
| first-hour MAE, schedule known at origin | D5 | 1 | wt_med | lvlh5_tft | 3.872 | -0.251 | -0.475 | -0.090 | -0.286 | -0.217 | True | 0.675 | 4 | 7 |
| first-hour MAE, schedule known at origin | D5 | 2 | wt_med | lvlh5_tft | 3.886 | -0.258 | -0.488 | -0.091 | -0.294 | -0.225 | True | 0.673 | 4 | 7 |
| first-hour MAE, schedule known at origin | D5 | 3 | wt_med | lvlh5_tft | 3.899 | -0.262 | -0.496 | -0.094 | -0.300 | -0.228 | True | 0.671 | 4 | 9 |
| first-hour MAE, schedule known at origin | D5 | 4 | wt_med | lvlh5_tft | 3.905 | -0.262 | -0.486 | -0.098 | -0.299 | -0.226 | True | 0.667 | 4 | 9 |
| first-hour MAE, schedule known at origin | D5 | 5 | wt_med | lvlh5_tft | 3.906 | -0.261 | -0.487 | -0.100 | -0.297 | -0.224 | True | 0.664 | 4 | 9 |
| first-hour MAE, schedule known at origin | D5 | 6 | wt_med | lvlh5_tft | 3.926 | -0.253 | -0.465 | -0.102 | -0.287 | -0.218 | True | 0.659 | 4 | 9 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | lvlh5_tft | 3.963 | -0.251 | -0.441 | -0.107 | -0.287 | -0.217 | True | 0.664 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 10 | wt_med | lvlh5_tft | 3.972 | -0.240 | -0.423 | -0.105 | -0.274 | -0.206 | True | 0.661 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 14 | wt_med | lvlh5_tft | 3.963 | -0.250 | -0.417 | -0.114 | -0.284 | -0.217 | True | 0.653 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 21 | wt_med | lvlh5_tft | 4.011 | -0.247 | -0.416 | -0.113 | -0.282 | -0.213 | True | 0.635 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 30 | wt_med | lvlh5_tft | 4.092 | -0.251 | -0.444 | -0.104 | -0.293 | -0.210 | True | 0.516 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 45 | wt_med | lvlh5_tft | 4.075 | -0.241 | -0.416 | -0.098 | -0.293 | -0.192 | True | 0.386 | 3 | 7 |
| first-hour MAE, schedule known at origin | D5 | 60 | wt_med | wt_med | 4.118 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE, schedule known at origin | D5 | 90 | wt_med | wt_med | 3.942 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule projected at origin | UC3 | 0 | wt_med | h5 | 7.306 | -0.019 | -0.034 | -0.006 | -0.025 | -0.011 | True | 0.997 | 1 | 7 |
| MAE, schedule projected at origin | UC3 | 1 | wt_med | h5 | 7.373 | -0.024 | -0.041 | -0.011 | -0.031 | -0.017 | True | 0.997 | 1 | 7 |
| MAE, schedule projected at origin | UC3 | 2 | wt_med | h5 | 7.428 | -0.025 | -0.042 | -0.012 | -0.032 | -0.018 | True | 0.997 | 1 | 7 |
| MAE, schedule projected at origin | UC3 | 3 | wt_med | h5 | 7.500 | -0.027 | -0.045 | -0.014 | -0.033 | -0.019 | True | 0.997 | 1 | 9 |
| MAE, schedule projected at origin | UC3 | 4 | wt_med | h5 | 7.584 | -0.028 | -0.048 | -0.014 | -0.035 | -0.021 | True | 0.996 | 1 | 9 |
| MAE, schedule projected at origin | UC3 | 5 | wt_med | h5 | 7.745 | -0.029 | -0.048 | -0.015 | -0.036 | -0.022 | True | 0.992 | 1 | 9 |
| MAE, schedule projected at origin | UC3 | 6 | wt_med | h5 | 7.944 | -0.031 | -0.051 | -0.017 | -0.038 | -0.024 | True | 0.987 | 1 | 9 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | h5 | 8.141 | -0.032 | -0.054 | -0.017 | -0.040 | -0.025 | True | 0.994 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | h5 | 8.434 | -0.031 | -0.052 | -0.016 | -0.039 | -0.023 | True | 0.995 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | h5 | 8.825 | -0.035 | -0.055 | -0.020 | -0.042 | -0.027 | True | 0.993 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 21 | wt_med | wt_med | 9.250 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule projected at origin | UC3 | 30 | wt_med | wt_med | 9.352 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule projected at origin | UC3 | 45 | wt_med | wt_med | 9.796 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| MAE, schedule projected at origin | UC3 | 60 | wt_med | wt_med | 9.747 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule projected at origin | UC3 | 90 | wt_med | wt_med | 10.103 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 0 | wt_med | wt_med | 4.504 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| first-hour MAE, schedule projected at origin | D5 | 1 | wt_med | h5 | 4.538 | -0.017 | -0.039 | -0.002 | -0.032 | -0.002 | True | 0.998 | 1 | 7 |
| first-hour MAE, schedule projected at origin | D5 | 2 | wt_med | h5 | 4.587 | -0.015 | -0.035 | -0.001 | -0.030 | -0.000 | True | 0.998 | 1 | 7 |
| first-hour MAE, schedule projected at origin | D5 | 3 | wt_med | h5 | 4.624 | -0.020 | -0.051 | -0.000 | -0.035 | -0.004 | True | 0.998 | 1 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 4 | wt_med | lvlh5_naive | 4.651 | -0.035 | -0.087 | -0.004 | -0.057 | -0.014 | True | 0.996 | 2 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 5 | wt_med | lvlh5_naive | 4.758 | -0.039 | -0.093 | -0.005 | -0.062 | -0.015 | True | 0.993 | 2 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 6 | wt_med | h5 | 4.925 | -0.045 | -0.104 | -0.010 | -0.062 | -0.029 | True | 0.989 | 2 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | wt_med | 5.126 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 10 | wt_med | wt_med | 5.398 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 14 | wt_med | wt_med | 5.656 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 21 | wt_med | wt_med | 5.814 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 30 | wt_med | wt_med | 5.818 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 45 | wt_med | wt_med | 6.073 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| first-hour MAE, schedule projected at origin | D5 | 60 | wt_med | wt_med | 6.113 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 90 | wt_med | wt_med | 5.996 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | persistence | 6.295 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | h5 | 0.825 | 0.008 | 0.004 | 0.012 | 0.007 | 0.010 | True | 0.995 | 1 | 7 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | h5 | 0.825 | 0.009 | 0.004 | 0.013 | 0.007 | 0.010 | True | 0.995 | 1 | 7 |
| best-time hit rate (top-2) | D3 | 2 | wt_med | h5 | 0.825 | 0.009 | 0.005 | 0.013 | 0.008 | 0.010 | True | 0.994 | 1 | 7 |
| best-time hit rate (top-2) | D3 | 3 | wt_med | h5 | 0.825 | 0.009 | 0.005 | 0.013 | 0.008 | 0.010 | True | 0.994 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 4 | wt_med | h5 | 0.825 | 0.009 | 0.005 | 0.013 | 0.008 | 0.010 | True | 0.994 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 5 | wt_med | h5 | 0.825 | 0.009 | 0.004 | 0.012 | 0.007 | 0.010 | True | 0.993 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | h5 | 0.824 | 0.008 | 0.004 | 0.012 | 0.007 | 0.010 | True | 0.993 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 7 | wt_med | h5 | 0.823 | 0.008 | 0.004 | 0.011 | 0.006 | 0.009 | True | 0.992 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 10 | wt_med | h5 | 0.823 | 0.007 | 0.003 | 0.011 | 0.006 | 0.008 | True | 0.991 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 14 | wt_med | h5 | 0.822 | 0.007 | 0.003 | 0.011 | 0.006 | 0.008 | True | 0.990 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 21 | wt_med | h5 | 0.822 | 0.008 | 0.004 | 0.013 | 0.007 | 0.010 | True | 0.989 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 30 | wt_med | h5 | 0.820 | 0.009 | 0.004 | 0.013 | 0.007 | 0.010 | True | 0.986 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 45 | wt_med | h5 | 0.820 | 0.009 | 0.005 | 0.013 | 0.008 | 0.011 | True | 0.984 | 1 | 7 |
| best-time hit rate (top-2) | D3 | 60 | clim | h5 | 0.823 | 0.006 | 0.001 | 0.010 | 0.004 | 0.007 | True | 0.989 | 1 | 4 |
| best-time hit rate (top-2) | D3 | 90 | wt_med | wt_med | 0.824 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| best-time hit rate (±30 min) | D3 | 0 | wt_med | h5 | 0.831 | 0.008 | 0.006 | 0.011 | 0.007 | 0.009 | True | 0.995 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | h5 | 0.831 | 0.008 | 0.006 | 0.011 | 0.007 | 0.009 | True | 0.995 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 2 | wt_med | h5 | 0.831 | 0.009 | 0.006 | 0.011 | 0.008 | 0.010 | True | 0.994 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 3 | wt_med | h5 | 0.830 | 0.008 | 0.006 | 0.011 | 0.007 | 0.009 | True | 0.994 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.831 | 0.009 | 0.006 | 0.011 | 0.007 | 0.010 | True | 0.994 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | h5 | 0.830 | 0.008 | 0.006 | 0.011 | 0.007 | 0.009 | True | 0.993 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | h5 | 0.830 | 0.008 | 0.005 | 0.011 | 0.007 | 0.009 | True | 0.993 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.829 | 0.008 | 0.005 | 0.010 | 0.006 | 0.009 | True | 0.992 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 10 | wt_med | h5 | 0.829 | 0.008 | 0.005 | 0.011 | 0.007 | 0.009 | True | 0.991 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 14 | wt_med | h5 | 0.829 | 0.008 | 0.005 | 0.011 | 0.006 | 0.009 | True | 0.990 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 21 | wt_med | h5 | 0.829 | 0.009 | 0.006 | 0.012 | 0.007 | 0.010 | True | 0.989 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 30 | wt_med | h5 | 0.829 | 0.009 | 0.006 | 0.013 | 0.008 | 0.011 | True | 0.986 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 45 | wt_med | h5 | 0.828 | 0.011 | 0.007 | 0.015 | 0.009 | 0.012 | True | 0.984 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 60 | wt_med | h5 | 0.829 | 0.008 | 0.005 | 0.012 | 0.007 | 0.010 | True | 0.989 | 1 | 4 |
| best-time hit rate (±30 min) | D3 | 90 | wt_med | wt_med | 0.832 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.820 | -0.294 | -0.380 | -0.224 | -0.322 | -0.267 | True | 0.995 | 1 | 7 |
| best-time regret (min) | D3 | 1 | wt_med | h5 | 2.834 | -0.291 | -0.376 | -0.219 | -0.318 | -0.264 | True | 0.995 | 1 | 7 |
| best-time regret (min) | D3 | 2 | wt_med | h5 | 2.827 | -0.297 | -0.384 | -0.224 | -0.325 | -0.270 | True | 0.994 | 1 | 7 |
| best-time regret (min) | D3 | 3 | wt_med | h5 | 2.836 | -0.295 | -0.384 | -0.222 | -0.324 | -0.269 | True | 0.994 | 1 | 9 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 2.836 | -0.298 | -0.388 | -0.221 | -0.329 | -0.271 | True | 0.994 | 1 | 9 |
| best-time regret (min) | D3 | 5 | wt_med | h5 | 2.843 | -0.291 | -0.379 | -0.212 | -0.320 | -0.263 | True | 0.993 | 1 | 9 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 2.860 | -0.294 | -0.382 | -0.215 | -0.324 | -0.265 | True | 0.993 | 1 | 9 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 2.881 | -0.281 | -0.367 | -0.204 | -0.308 | -0.255 | True | 0.992 | 1 | 8 |
| best-time regret (min) | D3 | 10 | wt_med | h5 | 2.892 | -0.290 | -0.382 | -0.210 | -0.321 | -0.261 | True | 0.991 | 1 | 8 |
| best-time regret (min) | D3 | 14 | wt_med | h5 | 2.911 | -0.276 | -0.373 | -0.196 | -0.307 | -0.245 | True | 0.990 | 1 | 8 |
| best-time regret (min) | D3 | 21 | wt_med | h5 | 2.900 | -0.306 | -0.407 | -0.211 | -0.337 | -0.277 | True | 0.989 | 1 | 8 |
| best-time regret (min) | D3 | 30 | wt_med | h5 | 2.911 | -0.308 | -0.415 | -0.217 | -0.340 | -0.276 | True | 0.986 | 1 | 8 |
| best-time regret (min) | D3 | 45 | wt_med | h5 | 2.922 | -0.340 | -0.455 | -0.241 | -0.372 | -0.308 | True | 0.984 | 1 | 7 |
| best-time regret (min) | D3 | 60 | wt_med | h5 | 2.937 | -0.295 | -0.387 | -0.220 | -0.326 | -0.260 | True | 0.989 | 1 | 4 |
| best-time regret (min) | D3 | 90 | wt_med | wt_med | 3.036 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | h5 | 0.465 | 0.018 | 0.014 | 0.022 | 0.017 | 0.019 | True | 0.954 | 3 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | h5 | 0.462 | 0.018 | 0.014 | 0.023 | 0.017 | 0.019 | True | 0.953 | 3 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | h5 | 0.461 | 0.019 | 0.015 | 0.024 | 0.018 | 0.020 | True | 0.952 | 3 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.460 | 0.019 | 0.015 | 0.023 | 0.018 | 0.020 | True | 0.951 | 3 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | h5 | 0.460 | 0.019 | 0.015 | 0.024 | 0.018 | 0.020 | True | 0.951 | 3 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | h5 | 0.458 | 0.019 | 0.015 | 0.024 | 0.018 | 0.020 | True | 0.950 | 3 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | h5 | 0.456 | 0.020 | 0.015 | 0.024 | 0.019 | 0.021 | True | 0.949 | 3 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.452 | 0.020 | 0.016 | 0.025 | 0.019 | 0.021 | True | 0.948 | 4 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.448 | 0.020 | 0.016 | 0.025 | 0.019 | 0.021 | True | 0.946 | 4 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.442 | 0.022 | 0.017 | 0.027 | 0.021 | 0.023 | True | 0.945 | 4 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | wt_med | prod_served_lin | 0.425 | 0.024 | 0.015 | 0.033 | 0.021 | 0.027 | True | 0.595 | 5 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | wt_med | prod_served_lin | 0.423 | 0.030 | 0.020 | 0.040 | 0.027 | 0.033 | True | 0.512 | 5 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | wt_med | prod_served_lin | 0.414 | 0.027 | 0.017 | 0.038 | 0.024 | 0.031 | True | 0.401 | 4 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | wt_med | h5 | 0.410 | 0.024 | 0.018 | 0.029 | 0.022 | 0.025 | True | 0.943 | 1 | 4 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | wt_med | h5 | 0.412 | 0.020 | 0.013 | 0.026 | 0.018 | 0.021 | True | 0.949 | 1 | 4 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | lvlh5_tft | 8.574 | -0.248 | -0.445 | -0.043 | -0.323 | -0.173 | True | 0.701 | 1 | 7 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | wt_med | 8.929 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 2 | wt_med | wt_med | 8.976 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 3 | wt_med | prod_served_lin | 8.544 | -0.441 | -0.671 | -0.216 | -0.530 | -0.349 | True | 0.664 | 2 | 9 |
| dayPeak abs error (min) | UC3 | 4 | wt_med | prod_served_lin | 8.597 | -0.429 | -0.660 | -0.209 | -0.523 | -0.346 | True | 0.661 | 2 | 9 |
| dayPeak abs error (min) | UC3 | 5 | wt_med | prod_served_lin | 8.710 | -0.335 | -0.560 | -0.110 | -0.433 | -0.242 | True | 0.656 | 2 | 9 |
| dayPeak abs error (min) | UC3 | 6 | snaive7 | snaive7 | 8.610 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | wt_med | 9.246 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 9.367 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 14 | wt_med | wt_med | 9.523 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 21 | wt_med | wt_med | 9.664 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 30 | wt_med | wt_med | 9.740 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 45 | wt_med | wt_med | 9.623 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 60 | wt_med | wt_med | 9.633 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| dayPeak abs error (min) | UC3 | 90 | wt_med | wt_med | 9.449 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| rope-drop worth agreement | D5 | 0 | wt_med | wt_med | 0.915 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| rope-drop worth agreement | D5 | 2 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| rope-drop worth agreement | D5 | 3 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 4 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 5 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 6 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 7 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 10 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 14 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 21 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 30 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| rope-drop worth agreement | D5 | 45 | wt_med | wt_med | 0.915 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| rope-drop worth agreement | D5 | 90 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | lvlh5_tft | 0.767 | 0.038 | 0.032 | 0.046 | 0.036 | 0.041 | True | 0.702 | 3 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | lvlh5_tft | 0.764 | 0.038 | 0.031 | 0.045 | 0.035 | 0.040 | True | 0.701 | 3 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | lvlh5_tft | 0.762 | 0.037 | 0.029 | 0.044 | 0.034 | 0.039 | True | 0.699 | 3 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | lvlh5_tft | 0.760 | 0.035 | 0.028 | 0.043 | 0.033 | 0.038 | True | 0.696 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | lvlh5_tft | 0.758 | 0.035 | 0.028 | 0.042 | 0.033 | 0.037 | True | 0.694 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | lvlh5_tft | 0.756 | 0.034 | 0.027 | 0.041 | 0.032 | 0.036 | True | 0.691 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | lvlh5_tft | 0.754 | 0.034 | 0.027 | 0.041 | 0.031 | 0.036 | True | 0.689 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | lvlh5_tft | 0.749 | 0.031 | 0.024 | 0.039 | 0.029 | 0.034 | True | 0.687 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | lvlh5_tft | 0.744 | 0.031 | 0.024 | 0.039 | 0.028 | 0.033 | True | 0.676 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | lvlh5_tft | 0.737 | 0.031 | 0.024 | 0.040 | 0.029 | 0.034 | True | 0.664 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | lvlh5_tft | 0.729 | 0.032 | 0.024 | 0.041 | 0.029 | 0.035 | True | 0.639 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | wt_med | lvlh5_tft | 0.724 | 0.032 | 0.024 | 0.041 | 0.029 | 0.035 | True | 0.508 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | wt_med | lvlh5_tft | 0.724 | 0.036 | 0.027 | 0.045 | 0.032 | 0.040 | True | 0.390 | 4 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | wt_med | lvlh5_naive | 0.714 | 0.020 | 0.013 | 0.028 | 0.018 | 0.023 | True | 0.948 | 2 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | wt_med | lvlh5_naive | 0.719 | 0.020 | 0.011 | 0.031 | 0.017 | 0.023 | True | 0.951 | 2 | 4 |
| dayPeak Spearman across rides | UC3 | 0 | wt_med | wt_med | 0.770 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 1 | wt_med | wt_med | 0.766 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 2 | wt_med | wt_med | 0.764 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 3 | wt_med | wt_med | 0.764 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 4 | wt_med | wt_med | 0.762 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 5 | wt_med | wt_med | 0.761 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 6 | wt_med | wt_med | 0.760 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.756 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.752 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.745 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 21 | wt_med | wt_med | 0.739 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 30 | wt_med | wt_med | 0.731 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 45 | wt_med | wt_med | 0.725 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 60 | wt_med | wt_med | 0.719 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| dayPeak Spearman across rides | UC3 | 90 | wt_med | wt_med | 0.719 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | h5 | 28.724 | -0.663 | -1.358 | -0.058 | -0.980 | -0.293 | True | 0.995 | 1 | 7 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | lvlh5_tft | 27.887 | -1.121 | -2.023 | -0.313 | -1.586 | -0.626 | True | 0.665 | 3 | 9 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | h5 | 29.274 | -0.755 | -1.279 | -0.269 | -1.091 | -0.423 | True | 0.995 | 1 | 8 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | h5 | 29.831 | -0.987 | -1.649 | -0.391 | -1.348 | -0.614 | True | 0.994 | 1 | 8 |
| optimiser regret (min per park-day) | D4 | 30 | wt_med | h5 | 30.441 | -1.101 | -1.755 | -0.437 | -1.463 | -0.709 | True | 0.991 | 1 | 8 |
| next-best precision | D1 | 0-120 | snaive7 | snaive7 | 0.455 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| next-best precision | D1 | 0-60 | snaive7 | snaive7 | 0.371 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| next-best precision (top-3) | D1 | 0-120 | snaive7 | snaive7 | 0.526 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| next-best precision (top-3) | D1 | 0-60 | snaive7 | snaive7 | 0.437 | 0.000 |  |  |  |  | True |  | 0 | 6 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.558 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.513 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 1 | lvl_snaive7 | lvl_snaive7 | 0.278 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 2 | lvl_snaive7 | lvl_snaive7 | 0.268 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 3 | lvl_snaive7 | lvl_snaive7 | 0.267 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 4 | lvl_snaive7 | lvl_snaive7 | 0.268 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 5 | lvl_snaive7 | lvl_snaive7 | 0.270 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 6 | lvl_snaive7 | lvl_snaive7 | 0.269 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| UC4 Spearman within park-month | UC4 | 7 | lvl_naive4 | lvl_naive4 | 0.172 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 10 | lvl_naive4 | lvl_naive4 | 0.176 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 14 | lvl_naive4 | lvl_naive4 | 0.145 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 21 | lvl_naive4 | lvl_naive4 | 0.129 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 30 | lvl_naive4 | lvl_naive4 | 0.145 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 45 | lvl_naive4 | lvl_naive4 | 0.159 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| UC4 Spearman within park-month | UC4 | 60 | lvl_naive4 | lvl_naive4 | 0.164 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| UC4 Spearman within park-month | UC4 | 90 | lvl_naive4 | lvl_naive4 | 0.150 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| UC4 Spearman within park-month | UC4 | 120 | lvl_naive4 | lvl_naive4 | 0.206 | 0.000 |  |  |  |  | False |  | 0 | 3 |
| UC4 Spearman within park-month | UC4 | 180 | lvl_naive4 | lvl_naive4 | 0.328 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_snaive7 | 0.398 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 2 | lvl_snaive7 | lvl_snaive7 | 0.398 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 3 | lvl_snaive7 | lvl_snaive7 | 0.399 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.401 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.401 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.401 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| crowd bucket exact | D6 | 7 | lvl_naive4 | lvl_naive4 | 0.336 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_naive4 | 0.338 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_naive4 | 0.307 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 21 | lvl_naive4 | lvl_naive4 | 0.290 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_naive4 | 0.280 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_naive4 | 0.271 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| crowd bucket exact | D6 | 60 | lvl_naive4 | lvl_naive4 | 0.267 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| crowd bucket exact | D6 | 90 | lvl_naive4 | lvl_naive4 | 0.281 | 0.000 |  |  |  |  | False |  | 0 | 3 |
| crowd bucket exact | D6 | 120 | lvl_naive4 | lvl_naive4 | 0.277 | 0.000 |  |  |  |  | False |  | 0 | 3 |
| crowd bucket exact | D6 | 180 | lvl_naive4 | lvl_naive4 | 0.262 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.475 | 0.000 |  |  |  |  | False |  | 0 | 6 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_naive4 | 0.362 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_naive4 | 0.337 | 0.000 |  |  |  |  | False |  | 0 | 3 |

### Low-coverage cells (paired rows < 50 % of the reference's)

These absolute values are not comparable with the rest of their column. `reaches_handover` = the cell still passes the 30 % gate and CAN appear as a winner.

| metric | uc | segment | lead | model | ref | value | diff_vs_ref | diff_lo_park | diff_hi_park | paired_share | n_paired | n_units_park | n_origin_days | wins | reaches_handover |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MAE | UC2-intraday | all | h4-8 | persistence | wt_med | 11.352 | 3.217 | 2.675 | 3.781 | 0.426 | 6969639.000 | 153.000 | 231 | False | True |
| MAE | UC2-intraday | all | h8+ | persistence | wt_med | 14.187 | 6.656 | 5.426 | 8.195 | 0.199 | 1435406.000 | 119.000 | 231 | False | False |
| MAE | UC2-intraday | busy | h4-8 | persistence | wt_med | 19.675 | 5.260 | 4.346 | 6.131 | 0.490 | 2530160.000 | 144.000 | 231 | False | True |
| MAE | UC2-intraday | busy | h8+ | persistence | wt_med | 23.264 | 10.833 | 8.613 | 13.301 | 0.241 | 602824.000 | 116.000 | 231 | False | False |
| MAE | UC3 | all | 30 | lvlh5_cbd | wt_med | 11.224 | 2.618 | 2.112 | 3.132 | 0.480 | 4613284.000 | 125.000 | 201 | False | True |
| MAE | UC3 | all | 45 | lvlh5_cbd | wt_med | 11.404 | 1.835 | 1.394 | 2.293 | 0.141 | 1224889.000 | 42.000 | 186 | False | False |
| MAE | UC3 | all | 45 | lvlh5_tft | wt_med | 9.317 | 0.487 | 0.309 | 0.649 | 0.376 | 3274054.000 | 141.000 | 186 | False | True |
| MAE | UC3 | all | 60 | lvlh5_tft | wt_med | 4.258 | 0.050 | -1.170 | 1.446 | 0.005 | 41803.000 | 13.000 | 171 | False | False |
| MAE | UC3 | all | 45 | prod_served | wt_med | 10.161 | 1.308 | 0.999 | 1.640 | 0.400 | 3474946.000 | 126.000 | 186 | False | True |
| MAE | UC3 | all | 60 | prod_served | wt_med | 4.299 | -0.102 | -1.215 | 0.715 | 0.005 | 40378.000 | 13.000 | 171 | False | False |
| MAE | UC3 | all | 45 | prod_served_lin | wt_med | 10.136 | 1.282 | 0.971 | 1.617 | 0.400 | 3474946.000 | 126.000 | 186 | False | True |
| MAE | UC3 | all | 60 | prod_served_lin | wt_med | 4.300 | -0.101 | -1.217 | 0.727 | 0.005 | 40378.000 | 13.000 | 171 | False | False |
| MAE | UC3 | busy | 14 | lvlh5_cbd | wt_med | 20.273 | 4.832 | 3.542 | 6.135 | 0.487 | 1603309.000 | 128.000 | 217 | False | True |
| MAE | UC3 | busy | 21 | lvlh5_cbd | wt_med | 20.523 | 4.659 | 3.268 | 6.007 | 0.463 | 1457124.000 | 125.000 | 210 | False | True |
| MAE | UC3 | busy | 30 | lvlh5_cbd | wt_med | 20.892 | 4.507 | 3.136 | 5.846 | 0.439 | 1302348.000 | 115.000 | 201 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_cbd | wt_med | 17.960 | 2.392 | 1.425 | 3.325 | 0.180 | 483628.000 | 38.000 | 186 | False | False |
| MAE | UC3 | busy | 30 | lvlh5_tft | wt_med | 16.873 | 0.420 | 0.116 | 0.708 | 0.470 | 1394650.000 | 132.000 | 201 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_tft | wt_med | 17.510 | 0.409 | 0.010 | 0.776 | 0.340 | 914197.000 | 130.000 | 186 | False | True |
| MAE | UC3 | busy | 60 | lvlh5_tft | wt_med | 19.637 | 2.028 | -1.273 | 5.812 | 0.001 | 1789.000 | 4.000 | 171 | False | False |
| MAE | UC3 | busy | 30 | prod_served | wt_med | 18.316 | 1.916 | 1.375 | 2.452 | 0.491 | 1457026.000 | 126.000 | 201 | False | True |
| MAE | UC3 | busy | 45 | prod_served | wt_med | 19.300 | 2.390 | 1.755 | 3.003 | 0.375 | 1010269.000 | 118.000 | 186 | False | True |
| MAE | UC3 | busy | 60 | prod_served | wt_med | 20.740 | 3.067 | 2.210 | 7.131 | 0.001 | 1789.000 | 4.000 | 171 | False | False |
| MAE | UC3 | busy | 30 | prod_served_lin | wt_med | 18.253 | 1.851 | 1.314 | 2.386 | 0.491 | 1457026.000 | 126.000 | 201 | False | True |
| MAE | UC3 | busy | 45 | prod_served_lin | wt_med | 19.234 | 2.323 | 1.680 | 2.946 | 0.375 | 1010269.000 | 118.000 | 186 | False | True |
| MAE | UC3 | busy | 60 | prod_served_lin | wt_med | 20.714 | 3.039 | 2.142 | 6.977 | 0.001 | 1789.000 | 4.000 | 171 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 12.072 | 1.857 | 1.418 | 2.272 | 0.120 | 665023.000 | 36.000 | 186 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 8.642 | 0.405 | 0.198 | 0.599 | 0.378 | 2094983.000 | 94.000 | 186 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 3.300 | -0.219 | -1.378 | 0.760 | 0.008 | 36573.000 | 11.000 | 171 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served | wt_med | 9.480 | 1.073 | 0.764 | 1.411 | 0.408 | 2265473.000 | 85.000 | 186 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served | wt_med | 3.465 | -0.254 | -1.651 | 0.508 | 0.008 | 35189.000 | 11.000 | 171 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served_lin | wt_med | 9.458 | 1.050 | 0.739 | 1.389 | 0.408 | 2265473.000 | 85.000 | 186 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served_lin | wt_med | 3.465 | -0.255 | -1.655 | 0.509 | 0.008 | 35189.000 | 11.000 | 171 | False | False |
| MAE, schedule projected at origin | UC3 | all | 0 | lvlh5_cbd | wt_med | 8.884 | 1.938 | 1.520 | 2.557 | 0.473 | 938334.000 | 68.000 | 231 | False | True |
| MAE, schedule projected at origin | UC3 | all | 1 | lvlh5_cbd | wt_med | 8.956 | 1.940 | 1.546 | 2.507 | 0.483 | 1004298.000 | 73.000 | 230 | False | True |
| MAE, schedule projected at origin | UC3 | all | 2 | lvlh5_cbd | wt_med | 9.022 | 1.939 | 1.561 | 2.461 | 0.482 | 1015107.000 | 76.000 | 229 | False | True |
| MAE, schedule projected at origin | UC3 | all | 3 | lvlh5_cbd | wt_med | 9.089 | 1.955 | 1.566 | 2.507 | 0.483 | 1024083.000 | 81.000 | 228 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | lvlh5_cbd | wt_med | 9.219 | 2.010 | 1.614 | 2.581 | 0.482 | 1032895.000 | 82.000 | 227 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | lvlh5_cbd | wt_med | 9.464 | 2.107 | 1.669 | 2.859 | 0.478 | 1044913.000 | 83.000 | 226 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | lvlh5_cbd | wt_med | 9.923 | 2.371 | 1.774 | 3.379 | 0.475 | 1057952.000 | 88.000 | 225 | False | True |
| MAE, schedule projected at origin | UC3 | all | 7 | lvlh5_cbd | wt_med | 10.357 | 2.575 | 1.774 | 3.927 | 0.474 | 1068191.000 | 89.000 | 224 | False | True |
| MAE, schedule projected at origin | UC3 | all | 10 | lvlh5_cbd | wt_med | 11.105 | 3.005 | 1.888 | 4.913 | 0.471 | 1117424.000 | 90.000 | 221 | False | True |
| MAE, schedule projected at origin | UC3 | all | 14 | lvlh5_cbd | wt_med | 11.802 | 3.175 | 2.006 | 4.854 | 0.461 | 1189213.000 | 97.000 | 217 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | lvlh5_cbd | wt_med | 12.382 | 3.387 | 2.142 | 5.030 | 0.436 | 1204289.000 | 95.000 | 210 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 12.419 | 3.154 | 2.127 | 4.427 | 0.419 | 1311764.000 | 95.000 | 201 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 10.588 | 1.809 | 1.047 | 2.733 | 0.178 | 559866.000 | 38.000 | 186 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_tft | wt_med | 10.027 | 0.707 | 0.376 | 1.026 | 0.467 | 1462855.000 | 121.000 | 201 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 10.538 | 0.632 | 0.299 | 0.914 | 0.374 | 1179071.000 | 121.000 | 186 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 10.980 | 1.935 | 0.292 | 2.865 | 0.002 | 5230.000 | 4.000 | 171 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served | wt_med | 10.810 | 1.570 | 0.934 | 2.250 | 0.473 | 1479556.000 | 114.000 | 201 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served | wt_med | 11.474 | 1.748 | 1.164 | 2.289 | 0.384 | 1209473.000 | 108.000 | 186 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served | wt_med | 10.076 | 0.932 | -0.122 | 4.112 | 0.002 | 5189.000 | 4.000 | 171 | False | False |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served_lin | wt_med | 10.781 | 1.542 | 0.896 | 2.216 | 0.473 | 1479556.000 | 114.000 | 201 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served_lin | wt_med | 11.444 | 1.718 | 1.109 | 2.264 | 0.384 | 1209473.000 | 108.000 | 186 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served_lin | wt_med | 10.088 | 0.944 | -0.089 | 4.087 | 0.002 | 5189.000 | 4.000 | 171 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | lvlh5_cbd | wt_med | 0.371 | -0.009 | -0.017 | -0.001 | 0.480 | 66942.000 | 121.000 | 100 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_cbd | wt_med | 0.343 | -0.011 | -0.022 | 0.002 | 0.154 | 18307.000 | 39.000 | 84 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_tft | wt_med | 0.375 | 0.014 | 0.007 | 0.020 | 0.394 | 47010.000 | 128.000 | 74 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | lvlh5_tft | wt_med | 0.314 | 0.015 | -0.024 | 0.038 | 0.003 | 287.000 | 10.000 | 48 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served | wt_med | 0.401 | 0.013 | 0.005 | 0.022 | 0.398 | 47427.000 | 123.000 | 85 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served | wt_med | 0.356 | -0.009 | -0.034 | 0.021 | 0.003 | 256.000 | 10.000 | 48 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served_lin | wt_med | 0.414 | 0.027 | 0.017 | 0.038 | 0.401 | 47866.000 | 123.000 | 85 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served_lin | wt_med | 0.367 | 0.007 | -0.021 | 0.042 | 0.003 | 258.000 | 10.000 | 48 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 0 | snaive7 | wt_med | 0.390 | -0.075 | -0.082 | -0.067 | 0.424 | 86383.000 | 140.000 | 231 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 1 | snaive7 | wt_med | 0.390 | -0.071 | -0.079 | -0.063 | 0.429 | 85781.000 | 140.000 | 230 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 2 | snaive7 | wt_med | 0.390 | -0.070 | -0.077 | -0.061 | 0.433 | 85380.000 | 140.000 | 229 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 3 | snaive7 | wt_med | 0.390 | -0.069 | -0.076 | -0.060 | 0.435 | 84885.000 | 140.000 | 228 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 4 | snaive7 | wt_med | 0.389 | -0.068 | -0.075 | -0.060 | 0.436 | 84123.000 | 140.000 | 227 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 5 | snaive7 | wt_med | 0.389 | -0.067 | -0.074 | -0.058 | 0.437 | 83237.000 | 140.000 | 226 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 6 | snaive7 | wt_med | 0.388 | -0.065 | -0.073 | -0.056 | 0.436 | 82010.000 | 140.000 | 225 | False | True |
| UC4 Spearman within park-month | UC4 | all | 45 | lvl_cbd | lvl_naive4 | 0.129 | -0.137 | -0.200 | -0.076 | 0.192 | 79.000 | 35.000 | 7 | False | False |
| UC4 Spearman within park-month | UC4 | all | 45 | lvl_tft | lvl_naive4 | 0.168 | 0.006 | -0.048 | 0.063 | 0.432 | 178.000 | 94.000 | 7 | False | True |
| UC4 Spearman within park-month | UC4 | all | 60 | lvl_tft | lvl_naive4 | 0.139 | 0.451 | 0.451 | 0.451 | 0.003 | 1.000 | 1.000 | 7 | False | False |
| best-time hit rate (top-2) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.725 | -0.076 | -0.090 | -0.059 | 0.492 | 76220.000 | 121.000 | 100 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.718 | -0.045 | -0.064 | -0.022 | 0.151 | 19852.000 | 39.000 | 84 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_tft | wt_med | 0.783 | -0.019 | -0.028 | -0.008 | 0.402 | 53044.000 | 129.000 | 74 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | lvlh5_tft | clim | 0.797 | -0.049 | -0.106 | 0.004 | 0.003 | 304.000 | 11.000 | 48 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served | wt_med | 0.751 | -0.047 | -0.062 | -0.033 | 0.429 | 56550.000 | 123.000 | 85 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served | clim | 0.753 | -0.046 | -0.081 | 0.030 | 0.003 | 304.000 | 11.000 | 48 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served_lin | wt_med | 0.768 | -0.025 | -0.037 | -0.013 | 0.429 | 56550.000 | 123.000 | 85 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served_lin | clim | 0.748 | -0.046 | -0.094 | 0.021 | 0.003 | 304.000 | 11.000 | 48 | False | False |
| best-time hit rate (top-2) | D3 | all | 0 | snaive7 | wt_med | 0.812 | -0.033 | -0.039 | -0.028 | 0.454 | 102059.000 | 142.000 | 231 | False | True |
| best-time hit rate (top-2) | D3 | all | 1 | snaive7 | wt_med | 0.812 | -0.032 | -0.038 | -0.027 | 0.459 | 101476.000 | 141.000 | 230 | False | True |
| best-time hit rate (top-2) | D3 | all | 2 | snaive7 | wt_med | 0.812 | -0.031 | -0.037 | -0.026 | 0.463 | 101079.000 | 141.000 | 229 | False | True |
| best-time hit rate (top-2) | D3 | all | 3 | snaive7 | wt_med | 0.812 | -0.031 | -0.037 | -0.026 | 0.466 | 100521.000 | 141.000 | 228 | False | True |
| best-time hit rate (top-2) | D3 | all | 4 | snaive7 | wt_med | 0.812 | -0.031 | -0.036 | -0.026 | 0.468 | 99700.000 | 141.000 | 227 | False | True |
| best-time hit rate (top-2) | D3 | all | 5 | snaive7 | wt_med | 0.812 | -0.031 | -0.036 | -0.026 | 0.468 | 98696.000 | 141.000 | 226 | False | True |
| best-time hit rate (top-2) | D3 | all | 6 | snaive7 | wt_med | 0.812 | -0.030 | -0.036 | -0.025 | 0.468 | 97440.000 | 141.000 | 225 | False | True |
| best-time hit rate (±30 min) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.760 | -0.049 | -0.065 | -0.032 | 0.492 | 76220.000 | 121.000 | 100 | False | True |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.755 | -0.020 | -0.037 | -0.001 | 0.151 | 19852.000 | 39.000 | 84 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_tft | wt_med | 0.802 | -0.007 | -0.017 | 0.004 | 0.402 | 53044.000 | 129.000 | 74 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | lvlh5_tft | wt_med | 0.819 | -0.018 | -0.048 | 0.017 | 0.003 | 336.000 | 11.000 | 48 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | prod_served | wt_med | 0.778 | -0.026 | -0.038 | -0.014 | 0.429 | 56550.000 | 123.000 | 85 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served | wt_med | 0.768 | -0.039 | -0.077 | 0.029 | 0.003 | 335.000 | 11.000 | 48 | False | False |
| best-time hit rate (±30 min) | D3 | all | 45 | prod_served_lin | wt_med | 0.790 | -0.010 | -0.022 | 0.002 | 0.429 | 56550.000 | 123.000 | 85 | False | True |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served_lin | wt_med | 0.779 | -0.027 | -0.050 | 0.044 | 0.003 | 335.000 | 11.000 | 48 | False | False |
| best-time hit rate (±30 min) | D3 | all | 0 | snaive7 | wt_med | 0.825 | -0.024 | -0.030 | -0.019 | 0.454 | 102059.000 | 142.000 | 231 | False | True |
| best-time hit rate (±30 min) | D3 | all | 1 | snaive7 | wt_med | 0.825 | -0.024 | -0.030 | -0.019 | 0.459 | 101476.000 | 141.000 | 230 | False | True |
| best-time hit rate (±30 min) | D3 | all | 2 | snaive7 | wt_med | 0.824 | -0.023 | -0.029 | -0.018 | 0.463 | 101079.000 | 141.000 | 229 | False | True |
| best-time hit rate (±30 min) | D3 | all | 3 | snaive7 | wt_med | 0.824 | -0.023 | -0.029 | -0.018 | 0.466 | 100521.000 | 141.000 | 228 | False | True |
| best-time hit rate (±30 min) | D3 | all | 4 | snaive7 | wt_med | 0.824 | -0.023 | -0.029 | -0.018 | 0.468 | 99700.000 | 141.000 | 227 | False | True |
| best-time hit rate (±30 min) | D3 | all | 5 | snaive7 | wt_med | 0.825 | -0.023 | -0.028 | -0.018 | 0.468 | 98696.000 | 141.000 | 226 | False | True |
| best-time hit rate (±30 min) | D3 | all | 6 | snaive7 | wt_med | 0.825 | -0.022 | -0.028 | -0.017 | 0.468 | 97440.000 | 141.000 | 225 | False | True |
| best-time regret (min) | D3 | all | 30 | lvlh5_cbd | wt_med | 4.587 | 1.097 | 0.706 | 1.446 | 0.492 | 76220.000 | 121.000 | 100 | False | True |
| best-time regret (min) | D3 | all | 45 | lvlh5_cbd | wt_med | 4.659 | 0.579 | 0.145 | 0.944 | 0.151 | 19852.000 | 39.000 | 84 | False | False |
| best-time regret (min) | D3 | all | 45 | lvlh5_tft | wt_med | 3.647 | 0.077 | -0.197 | 0.310 | 0.402 | 53044.000 | 129.000 | 74 | False | True |
| best-time regret (min) | D3 | all | 60 | lvlh5_tft | wt_med | 2.265 | 0.289 | -0.121 | 0.995 | 0.003 | 336.000 | 11.000 | 48 | False | False |
| best-time regret (min) | D3 | all | 45 | prod_served | wt_med | 4.454 | 0.801 | 0.483 | 1.142 | 0.429 | 56550.000 | 123.000 | 85 | False | True |
| best-time regret (min) | D3 | all | 60 | prod_served | wt_med | 3.384 | 0.836 | -0.603 | 1.524 | 0.003 | 335.000 | 11.000 | 48 | False | False |
| best-time regret (min) | D3 | all | 45 | prod_served_lin | wt_med | 3.891 | 0.127 | -0.153 | 0.408 | 0.429 | 56550.000 | 123.000 | 85 | False | True |
| best-time regret (min) | D3 | all | 60 | prod_served_lin | wt_med | 3.799 | 0.985 | -0.323 | 1.711 | 0.003 | 335.000 | 11.000 | 48 | False | False |
| best-time regret (min) | D3 | all | 0 | snaive7 | wt_med | 3.447 | 0.776 | 0.626 | 0.953 | 0.454 | 102059.000 | 142.000 | 231 | False | True |
| best-time regret (min) | D3 | all | 1 | snaive7 | wt_med | 3.443 | 0.760 | 0.614 | 0.941 | 0.459 | 101476.000 | 141.000 | 230 | False | True |
| best-time regret (min) | D3 | all | 2 | snaive7 | wt_med | 3.443 | 0.753 | 0.607 | 0.933 | 0.463 | 101079.000 | 141.000 | 229 | False | True |
| best-time regret (min) | D3 | all | 3 | snaive7 | wt_med | 3.441 | 0.740 | 0.594 | 0.914 | 0.466 | 100521.000 | 141.000 | 228 | False | True |
| best-time regret (min) | D3 | all | 4 | snaive7 | wt_med | 3.441 | 0.724 | 0.585 | 0.905 | 0.468 | 99700.000 | 141.000 | 227 | False | True |
| best-time regret (min) | D3 | all | 5 | snaive7 | wt_med | 3.440 | 0.715 | 0.576 | 0.889 | 0.468 | 98696.000 | 141.000 | 226 | False | True |
| best-time regret (min) | D3 | all | 6 | snaive7 | wt_med | 3.438 | 0.679 | 0.544 | 0.847 | 0.468 | 97440.000 | 141.000 | 225 | False | True |
| crowd bucket exact | D6 | all | 45 | lvl_cbd | lvl_naive4 | 0.233 | -0.071 | -0.119 | -0.019 | 0.179 | 2008.000 | 39.000 | 186 | False | False |
| crowd bucket exact | D6 | all | 45 | lvl_tft | lvl_naive4 | 0.257 | 0.001 | -0.020 | 0.021 | 0.430 | 4818.000 | 98.000 | 186 | False | True |
| crowd bucket exact | D6 | all | 60 | lvl_tft | lvl_naive4 | 0.206 | 0.048 | -0.062 | 0.132 | 0.006 | 63.000 | 10.000 | 171 | False | False |
| day comparison winner accuracy | D7 | all | d31-90 | lvl_cbd | lvl_naive4 | 0.144 | -0.172 | -0.191 | -0.152 | 0.085 | 1884697.000 | 107.000 | 200 | False | False |
| day comparison winner accuracy | D7 | all | d31-90 | lvl_tft | lvl_naive4 | 0.353 | 0.026 | 0.007 | 0.045 | 0.190 | 4178481.000 | 123.000 | 200 | True | False |
| dayPeak Spearman across rides | UC3 | all | 21 | lvlh5_cbd | wt_med | 0.547 | -0.190 | -0.223 | -0.157 | 0.484 | 6058.000 | 113.000 | 109 | False | True |
| dayPeak Spearman across rides | UC3 | all | 30 | lvlh5_cbd | wt_med | 0.538 | -0.189 | -0.220 | -0.159 | 0.460 | 5245.000 | 113.000 | 100 | False | True |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_cbd | wt_med | 0.527 | -0.180 | -0.215 | -0.139 | 0.168 | 1651.000 | 39.000 | 84 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_tft | wt_med | 0.682 | -0.023 | -0.033 | -0.013 | 0.403 | 3958.000 | 115.000 | 73 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | lvlh5_tft | wt_med | 0.585 | -0.182 | -0.319 | 0.069 | 0.006 | 47.000 | 6.000 | 37 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | prod_served | wt_med | 0.706 | -0.007 | -0.017 | 0.004 | 0.432 | 4236.000 | 104.000 | 85 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | prod_served | wt_med | 0.825 | 0.022 | -0.002 | 0.131 | 0.006 | 47.000 | 6.000 | 37 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | prod_served_lin | wt_med | 0.707 | -0.007 | -0.017 | 0.005 | 0.432 | 4239.000 | 104.000 | 85 | False | True |
| dayPeak Spearman across rides | UC3 | all | 60 | prod_served_lin | wt_med | 0.834 | 0.030 | 0.006 | 0.147 | 0.006 | 47.000 | 6.000 | 37 | False | False |
| dayPeak abs error (min) | UC3 | all | 30 | lvlh5_cbd | wt_med | 14.509 | 4.074 | 3.268 | 4.955 | 0.473 | 101874.000 | 123.000 | 100 | False | True |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_cbd | wt_med | 15.480 | 3.732 | 2.811 | 4.651 | 0.143 | 26330.000 | 40.000 | 84 | False | False |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_tft | wt_med | 10.786 | 0.515 | 0.244 | 0.774 | 0.406 | 74655.000 | 133.000 | 74 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | lvlh5_tft | wt_med | 2.977 | -0.371 | -2.151 | 0.429 | 0.007 | 1024.000 | 12.000 | 48 | False | False |
| dayPeak abs error (min) | UC3 | all | 45 | prod_served | wt_med | 11.273 | 0.952 | 0.611 | 1.311 | 0.428 | 78786.000 | 124.000 | 85 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served | wt_med | 3.477 | -0.527 | -2.561 | 0.478 | 0.006 | 981.000 | 12.000 | 48 | False | False |
| dayPeak abs error (min) | UC3 | all | 45 | prod_served_lin | wt_med | 11.224 | 0.898 | 0.560 | 1.257 | 0.428 | 78786.000 | 124.000 | 85 | False | True |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served_lin | wt_med | 3.467 | -0.550 | -2.595 | 0.348 | 0.006 | 981.000 | 12.000 | 48 | False | False |
| dayPeak abs error (min) | UC3 | all | 0 | snaive7 | wt_med | 8.654 | 0.647 | 0.422 | 0.887 | 0.483 | 148996.000 | 149.000 | 231 | False | True |
| dayPeak abs error (min) | UC3 | all | 1 | snaive7 | wt_med | 8.653 | 0.520 | 0.287 | 0.767 | 0.488 | 148181.000 | 147.000 | 230 | False | True |
| dayPeak abs error (min) | UC3 | all | 2 | snaive7 | wt_med | 8.649 | 0.457 | 0.219 | 0.705 | 0.493 | 147664.000 | 147.000 | 229 | False | True |
| dayPeak abs error (min) | UC3 | all | 3 | snaive7 | wt_med | 8.648 | 0.410 | 0.167 | 0.663 | 0.496 | 146929.000 | 147.000 | 228 | False | True |
| dayPeak abs error (min) | UC3 | all | 4 | snaive7 | wt_med | 8.635 | 0.368 | 0.120 | 0.623 | 0.497 | 145781.000 | 147.000 | 227 | False | True |
| dayPeak abs error (min) | UC3 | all | 5 | snaive7 | wt_med | 8.621 | 0.325 | 0.066 | 0.591 | 0.498 | 144363.000 | 147.000 | 226 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 30 | lvlh5_cbd | wt_med | 0.667 | -0.033 | -0.050 | -0.015 | 0.480 | 129437.000 | 114.000 | 100 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_cbd | wt_med | 0.661 | -0.041 | -0.064 | -0.018 | 0.176 | 40483.000 | 39.000 | 84 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_tft | wt_med | 0.724 | 0.036 | 0.027 | 0.045 | 0.390 | 89510.000 | 127.000 | 66 | True | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 60 | lvlh5_tft | wt_med | 0.752 | 0.050 | -0.025 | 0.225 | 0.002 | 360.000 | 6.000 | 26 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | prod_served | wt_med | 0.665 | -0.020 | -0.032 | -0.008 | 0.425 | 97661.000 | 117.000 | 85 | False | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 60 | prod_served | wt_med | 0.650 | -0.053 | -0.121 | 0.126 | 0.002 | 360.000 | 6.000 | 27 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | prod_served_lin | wt_med | 0.698 | 0.014 | 0.002 | 0.026 | 0.425 | 97661.000 | 117.000 | 85 | True | True |
| dayPeak pairwise ordering (headliners) | D4 | all | 60 | prod_served_lin | wt_med | 0.706 | 0.006 | -0.070 | 0.225 | 0.002 | 360.000 | 6.000 | 27 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 0 | snaive7 | wt_med | 0.727 | -0.023 | -0.030 | -0.016 | 0.262 | 103932.000 | 135.000 | 231 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 1 | snaive7 | wt_med | 0.726 | -0.020 | -0.027 | -0.013 | 0.266 | 103406.000 | 134.000 | 230 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 2 | snaive7 | wt_med | 0.726 | -0.019 | -0.026 | -0.013 | 0.268 | 103005.000 | 134.000 | 229 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 3 | snaive7 | wt_med | 0.726 | -0.019 | -0.026 | -0.012 | 0.270 | 102444.000 | 134.000 | 228 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 4 | snaive7 | wt_med | 0.726 | -0.018 | -0.025 | -0.010 | 0.271 | 101595.000 | 133.000 | 227 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 5 | snaive7 | wt_med | 0.726 | -0.017 | -0.024 | -0.010 | 0.271 | 100433.000 | 133.000 | 226 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 6 | snaive7 | wt_med | 0.726 | -0.014 | -0.022 | -0.007 | 0.270 | 98893.000 | 132.000 | 225 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 21 | lvlh5_cbd | wt_med | 4.561 | -0.201 | -0.357 | -0.075 | 0.485 | 363672.000 | 116.000 | 210 | True | True |
| first-hour MAE (opening-aligned) | D5 | all | 30 | lvlh5_cbd | wt_med | 4.699 | -0.229 | -0.406 | -0.081 | 0.462 | 326022.000 | 112.000 | 201 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_cbd | wt_med | 4.419 | -0.250 | -0.511 | -0.033 | 0.108 | 68944.000 | 37.000 | 186 | True | False |
| first-hour MAE (opening-aligned) | D5 | all | 30 | lvlh5_tft | wt_med | 4.627 | -0.166 | -0.313 | -0.046 | 0.498 | 351859.000 | 129.000 | 201 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_tft | wt_med | 4.790 | -0.155 | -0.285 | -0.046 | 0.381 | 242236.000 | 125.000 | 186 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | lvlh5_tft | wt_med | 1.337 | 0.000 | 0.000 | 0.000 | 0.008 | 4317.000 | 12.000 | 171 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 45 | prod_served | wt_med | 6.802 | 1.842 | 1.588 | 2.105 | 0.398 | 253000.000 | 117.000 | 186 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served | wt_med | 1.891 | 0.486 | 0.185 | 2.043 | 0.007 | 4133.000 | 12.000 | 171 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 45 | prod_served_lin | wt_med | 6.960 | 2.006 | 1.707 | 2.307 | 0.398 | 253000.000 | 117.000 | 186 | False | True |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served_lin | wt_med | 1.954 | 0.549 | 0.242 | 2.130 | 0.007 | 4133.000 | 12.000 | 171 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 30 | lvlh5_cbd | wt_med | 4.285 | -0.248 | -0.477 | -0.083 | 0.488 | 245117.000 | 89.000 | 201 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 45 | lvlh5_cbd | wt_med | 4.384 | -0.386 | -0.756 | -0.020 | 0.084 | 36276.000 | 31.000 | 186 | True | False |
| first-hour MAE, schedule known at origin | D5 | all | 45 | lvlh5_tft | wt_med | 4.075 | -0.241 | -0.416 | -0.098 | 0.386 | 167253.000 | 88.000 | 186 | True | True |
| first-hour MAE, schedule known at origin | D5 | all | 60 | lvlh5_tft | wt_med | 1.152 | 0.000 | 0.000 | 0.000 | 0.011 | 3770.000 | 10.000 | 171 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 45 | prod_served | wt_med | 6.130 | 1.738 | 1.451 | 2.024 | 0.403 | 174492.000 | 82.000 | 186 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 60 | prod_served | wt_med | 1.681 | 0.460 | 0.139 | 2.214 | 0.010 | 3592.000 | 10.000 | 171 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 45 | prod_served_lin | wt_med | 6.293 | 1.910 | 1.598 | 2.207 | 0.403 | 174492.000 | 82.000 | 186 | False | True |
| first-hour MAE, schedule known at origin | D5 | all | 60 | prod_served_lin | wt_med | 1.742 | 0.522 | 0.210 | 2.418 | 0.010 | 3592.000 | 10.000 | 171 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 0 | lvlh5_cbd | wt_med | 4.135 | 0.005 | -0.018 | 0.035 | 0.466 | 61029.000 | 56.000 | 231 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 1 | lvlh5_cbd | wt_med | 4.188 | -0.013 | -0.071 | 0.025 | 0.477 | 65638.000 | 61.000 | 230 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 2 | lvlh5_cbd | wt_med | 4.217 | -0.011 | -0.063 | 0.024 | 0.473 | 65875.000 | 64.000 | 229 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 3 | lvlh5_cbd | wt_med | 4.254 | -0.010 | -0.067 | 0.025 | 0.472 | 66303.000 | 67.000 | 228 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 4 | lvlh5_cbd | wt_med | 4.315 | -0.021 | -0.091 | 0.019 | 0.472 | 66693.000 | 68.000 | 227 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 5 | lvlh5_cbd | wt_med | 4.436 | -0.033 | -0.126 | 0.016 | 0.468 | 67454.000 | 69.000 | 226 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 6 | lvlh5_cbd | wt_med | 4.614 | -0.073 | -0.261 | 0.019 | 0.464 | 68104.000 | 73.000 | 225 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 7 | lvlh5_cbd | wt_med | 4.787 | -0.099 | -0.358 | 0.025 | 0.462 | 68850.000 | 72.000 | 224 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 10 | lvlh5_cbd | wt_med | 5.227 | -0.069 | -0.258 | 0.047 | 0.458 | 72111.000 | 76.000 | 221 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 14 | lvlh5_cbd | wt_med | 5.648 | -0.086 | -0.311 | 0.053 | 0.448 | 75970.000 | 82.000 | 217 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 21 | lvlh5_cbd | wt_med | 5.952 | -0.098 | -0.345 | 0.070 | 0.422 | 75626.000 | 80.000 | 210 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 30 | lvlh5_cbd | wt_med | 5.994 | -0.173 | -0.529 | 0.068 | 0.397 | 80905.000 | 83.000 | 201 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 45 | lvlh5_cbd | wt_med | 4.462 | -0.099 | -0.307 | 0.017 | 0.161 | 32668.000 | 35.000 | 186 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 30 | lvlh5_tft | wt_med | 6.169 | 0.070 | -0.075 | 0.302 | 0.454 | 92501.000 | 104.000 | 201 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 45 | lvlh5_tft | wt_med | 6.399 | 0.038 | -0.075 | 0.169 | 0.371 | 74983.000 | 103.000 | 186 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 60 | lvlh5_tft | wt_med | 2.600 | 0.000 | 0.000 | 0.000 | 0.003 | 547.000 | 4.000 | 171 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 30 | prod_served | wt_med | 7.843 | 1.766 | 1.400 | 2.256 | 0.464 | 94579.000 | 101.000 | 201 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 45 | prod_served | wt_med | 8.332 | 2.074 | 1.640 | 2.592 | 0.388 | 78508.000 | 96.000 | 186 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 60 | prod_served | wt_med | 3.271 | 0.657 | 0.000 | 1.531 | 0.003 | 541.000 | 4.000 | 171 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 30 | prod_served_lin | wt_med | 7.971 | 1.896 | 1.390 | 2.501 | 0.464 | 94579.000 | 101.000 | 201 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 45 | prod_served_lin | wt_med | 8.476 | 2.221 | 1.649 | 2.812 | 0.388 | 78508.000 | 96.000 | 186 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 60 | prod_served_lin | wt_med | 3.342 | 0.729 | 0.005 | 3.367 | 0.003 | 541.000 | 4.000 | 171 | False | False |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.85 [7.78, 7.94] | 7.50 [7.42, 7.60] | 5.42 [5.38, 5.47] | 6.54 [6.46, 6.63] | 7.59 [7.51, 7.67] | 7.66 [7.58, 7.74] |
| 2 | 7.86 [7.79, 7.95] | 7.57 [7.49, 7.67] | 5.43 [5.38, 5.48] | 6.54 [6.46, 6.63] | 7.62 [7.55, 7.71] | 7.70 [7.62, 7.78] |
| 3 | 7.87 [7.79, 7.96] | 7.65 [7.57, 7.75] | 5.43 [5.39, 5.48] | 6.54 [6.46, 6.63] | 7.65 [7.58, 7.74] | 7.73 [7.66, 7.82] |
| 4 | 7.87 [7.80, 7.96] | 7.70 [7.61, 7.80] | 5.44 [5.40, 5.49] | 6.53 [6.45, 6.62] | 7.68 [7.60, 7.76] | 7.76 [7.68, 7.84] |
| 5 | 7.88 [7.81, 7.97] | 7.78 [7.69, 7.88] | 5.45 [5.41, 5.50] | 6.53 [6.46, 6.62] | 7.71 [7.63, 7.79] | 7.79 [7.71, 7.88] |
| 6 | 7.90 [7.83, 7.99] | 7.85 [7.76, 7.96] | 5.47 [5.43, 5.52] | 6.53 [6.46, 6.62] | 7.77 [7.69, 7.85] | 7.85 [7.77, 7.94] |
| 7 | 8.21 [8.14, 8.30] | 8.04 [7.94, 8.14] | 5.51 [5.46, 5.55] | 6.99 [6.91, 7.08] | 7.87 [7.79, 7.96] | 7.96 [7.88, 8.05] |
| 10 | 8.25 [8.17, 8.34] | 8.20 [8.11, 8.31] | 5.53 [5.48, 5.58] | 6.99 [6.91, 7.08] | 7.97 [7.89, 8.06] | 8.07 [7.99, 8.16] |
| 14 | 8.50 [8.41, 8.60] | 8.45 [8.35, 8.57] | 5.59 [5.54, 5.65] | 7.30 [7.22, 7.40] | 8.12 [8.04, 8.21] | 8.22 [8.14, 8.32] |
| 21 | 8.71 [8.63, 8.81] | 8.67 [8.56, 8.78] | 5.64 [5.60, 5.70] | 7.55 [7.46, 7.65] | 8.28 [8.20, 8.37] | 8.40 [8.31, 8.49] |
| 30 | 8.83 [8.74, 8.92] | 9.03 [8.90, 9.17] | 5.66 [5.61, 5.71] | 7.67 [7.57, 7.76] | 8.42 [8.34, 8.52] | 8.55 [8.47, 8.65] |
| 45 | 8.96 [8.87, 9.06] | 9.32 [9.15, 9.48] | 5.68 [5.63, 5.74] | 7.82 [7.73, 7.93] | 8.54 [8.45, 8.63] | 8.68 [8.59, 8.78] |
| 60 | 9.12 [9.01, 9.22] | 4.26 [2.98, 5.74] | 5.70 [5.65, 5.77] | 8.01 [7.91, 8.12] | 8.69 [8.59, 8.79] | 8.83 [8.74, 8.93] |
| 90 | 9.27 [9.16, 9.40] |  | 5.70 [5.63, 5.76] | 8.09 [7.98, 8.22] | 8.75 [8.64, 8.87] | 8.91 [8.79, 9.03] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | 7.97 [7.88, 8.06] | 7.40 [7.32, 7.48] | 7.71 [7.63, 7.80] | 7.24 [7.15, 7.33] | 2.46 [2.43, 2.49] | 8.60 [8.50, 8.71] | 7.45 [7.37, 7.53] |
| m030 | 7.95 [7.87, 8.04] | 7.36 [7.29, 7.44] | 7.70 [7.62, 7.79] | 7.25 [7.17, 7.34] | 4.15 [4.11, 4.19] | 8.57 [8.48, 8.68] | 7.44 [7.36, 7.52] |
| m045 | 7.98 [7.90, 8.07] | 7.40 [7.32, 7.48] | 7.73 [7.66, 7.82] | 7.29 [7.21, 7.39] | 5.07 [5.03, 5.12] | 8.59 [8.50, 8.70] | 7.47 [7.40, 7.55] |
| m060 | 8.06 [7.97, 8.15] | 7.47 [7.40, 7.55] | 7.78 [7.71, 7.87] | 7.35 [7.27, 7.45] | 5.70 [5.65, 5.75] | 8.67 [8.57, 8.77] | 7.54 [7.46, 7.62] |
| m075 | 8.12 [8.03, 8.21] | 7.56 [7.49, 7.65] | 7.92 [7.84, 8.00] | 7.43 [7.35, 7.53] | 6.28 [6.23, 6.34] | 8.76 [8.66, 8.86] | 7.61 [7.53, 7.69] |
| m090 | 8.16 [8.08, 8.25] | 7.57 [7.50, 7.65] | 7.95 [7.87, 8.04] | 7.47 [7.39, 7.57] | 6.79 [6.73, 6.85] | 8.79 [8.69, 8.89] | 7.64 [7.56, 7.72] |
| m105 | 8.22 [8.13, 8.31] | 7.61 [7.53, 7.69] | 8.00 [7.92, 8.08] | 7.51 [7.43, 7.61] | 7.16 [7.10, 7.23] | 8.85 [8.75, 8.95] | 7.68 [7.61, 7.77] |
| m120 | 8.26 [8.18, 8.35] | 7.65 [7.58, 7.73] | 8.00 [7.92, 8.09] | 7.51 [7.43, 7.61] | 7.48 [7.42, 7.56] | 8.87 [8.77, 8.97] | 7.72 [7.64, 7.81] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| m015 | 5.98 [5.39, 6.53] | 5.41 [4.89, 5.89] | 5.75 [5.21, 6.26] | 5.33 [4.87, 5.81] | 6.57 [5.97, 7.17] | 5.45 [4.94, 5.93] | persistence |
| m030 | 4.31 [3.81, 4.78] | 3.71 [3.28, 4.12] | 4.08 [3.61, 4.52] | 3.71 [3.31, 4.11] | 4.93 [4.44, 5.40] | 3.78 [3.35, 4.20] | persistence |
| m045 | 3.41 [2.96, 3.84] | 2.78 [2.39, 3.15] | 3.17 [2.74, 3.58] | 2.80 [2.44, 3.15] | 4.00 [3.57, 4.42] | 2.85 [2.45, 3.23] | persistence |
| m060 | 2.78 [2.35, 3.17] | 2.14 [1.77, 2.48] | 2.50 [2.10, 2.88] | 2.14 [1.80, 2.46] | 3.35 [2.95, 3.75] | 2.19 [1.82, 2.54] | persistence |
| m075 | 2.39 [1.95, 2.79] | 1.70 [1.35, 2.03] | 2.07 [1.67, 2.45] | 1.65 [1.33, 1.95] | 2.98 [2.57, 3.36] | 1.76 [1.39, 2.10] | persistence |
| m090 | 1.87 [1.45, 2.25] | 1.14 [0.79, 1.48] | 1.55 [1.17, 1.92] | 1.14 [0.81, 1.46] | 2.44 [2.06, 2.79] | 1.22 [0.86, 1.57] | persistence |
| m105 | 1.50 [1.07, 1.90] | 0.73 [0.36, 1.05] | 1.16 [0.76, 1.53] | 0.75 [0.41, 1.06] | 2.07 [1.68, 2.46] | 0.82 [0.44, 1.15] | persistence |
| m120 | 1.15 [0.74, 1.51] | 0.36 [0.01, 0.68] | 0.77 [0.38, 1.13] | 0.37 [0.02, 0.68] | 1.70 [1.34, 2.08] | 0.45 [0.09, 0.76] | persistence |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 6.573 [5.966, 7.171] / [6.478, 6.671] | 5.453 [4.939, 5.933] / [5.380, 5.535] | 5.981 [5.387, 6.532] / [5.905, 6.065] | 5.410 [4.892, 5.888] / [5.340, 5.490] | 5.753 [5.212, 6.261] / [5.679, 5.836] | 5.334 [4.871, 5.813] / [5.252, 5.426] |
| m030 | 4.928 [4.436, 5.405] / [4.838, 5.021] | 3.783 [3.349, 4.196] / [3.712, 3.860] | 4.311 [3.806, 4.783] / [4.236, 4.391] | 3.709 [3.276, 4.117] / [3.638, 3.783] | 4.082 [3.608, 4.523] / [4.009, 4.161] | 3.709 [3.311, 4.114] / [3.633, 3.794] |
| m045 | 4.000 [3.567, 4.420] / [3.914, 4.093] | 2.848 [2.450, 3.228] / [2.782, 2.922] | 3.413 [2.956, 3.843] / [3.340, 3.489] | 2.781 [2.388, 3.155] / [2.714, 2.853] | 3.173 [2.740, 3.582] / [3.105, 3.248] | 2.801 [2.443, 3.154] / [2.732, 2.880] |
| m060 | 3.354 [2.947, 3.748] / [3.268, 3.444] | 2.194 [1.824, 2.543] / [2.131, 2.265] | 2.781 [2.345, 3.169] / [2.711, 2.858] | 2.136 [1.766, 2.480] / [2.074, 2.203] | 2.498 [2.098, 2.878] / [2.433, 2.571] | 2.135 [1.797, 2.461] / [2.069, 2.211] |
| m075 | 2.976 [2.574, 3.357] / [2.890, 3.068] | 1.756 [1.389, 2.103] / [1.687, 1.828] | 2.391 [1.951, 2.792] / [2.317, 2.465] | 1.699 [1.351, 2.035] / [1.630, 1.770] | 2.071 [1.674, 2.447] / [2.003, 2.144] | 1.647 [1.327, 1.949] / [1.580, 1.722] |
| m090 | 2.440 [2.058, 2.790] / [2.353, 2.529] | 1.217 [0.861, 1.566] / [1.147, 1.288] | 1.870 [1.449, 2.247] / [1.800, 1.946] | 1.136 [0.788, 1.481] / [1.070, 1.206] | 1.552 [1.173, 1.919] / [1.486, 1.626] | 1.142 [0.810, 1.455] / [1.072, 1.215] |
| m105 | 2.067 [1.676, 2.459] / [1.987, 2.159] | 0.818 [0.441, 1.151] / [0.751, 0.887] | 1.501 [1.070, 1.896] / [1.428, 1.575] | 0.726 [0.359, 1.055] / [0.662, 0.794] | 1.164 [0.757, 1.533] / [1.096, 1.238] | 0.754 [0.407, 1.059] / [0.686, 0.828] |
| m120 | 1.700 [1.341, 2.077] / [1.618, 1.790] | 0.449 [0.094, 0.765] / [0.381, 0.519] | 1.147 [0.741, 1.512] / [1.072, 1.219] | 0.359 [0.009, 0.683] / [0.290, 0.424] | 0.768 [0.380, 1.128] / [0.699, 0.840] | 0.367 [0.016, 0.675] / [0.299, 0.439] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -6.573 [-7.171, -5.966]* / [-6.671, -6.478]* | -1.149 [-1.348, -0.962]* / [-1.219, -1.081]* | -0.522 [-0.719, -0.346]* / [-0.601, -0.442]* | -1.180 [-1.379, -0.992]* / [-1.251, -1.112]* | -0.832 [-1.007, -0.666]* / [-0.896, -0.765]* | -1.003 [-1.177, -0.830]* / [-1.075, -0.933]* |
| m030 | -4.928 [-5.405, -4.436]* / [-5.021, -4.838]* | -1.160 [-1.351, -0.972]* / [-1.229, -1.092]* | -0.549 [-0.748, -0.370]* / [-0.629, -0.471]* | -1.220 [-1.425, -1.031]* / [-1.290, -1.154]* | -0.845 [-1.019, -0.683]* / [-0.908, -0.782]* | -1.010 [-1.189, -0.837]* / [-1.083, -0.937]* |
| m045 | -4.000 [-4.420, -3.567]* / [-4.093, -3.914]* | -1.157 [-1.348, -0.969]* / [-1.224, -1.088]* | -0.539 [-0.740, -0.353]* / [-0.616, -0.462]* | -1.215 [-1.415, -1.025]* / [-1.284, -1.146]* | -0.838 [-1.012, -0.675]* / [-0.901, -0.773]* | -0.995 [-1.177, -0.817]* / [-1.066, -0.927]* |
| m060 | -3.354 [-3.748, -2.947]* / [-3.444, -3.268]* | -1.165 [-1.363, -0.978]* / [-1.234, -1.096]* | -0.538 [-0.727, -0.348]* / [-0.616, -0.461]* | -1.213 [-1.416, -1.024]* / [-1.282, -1.145]* | -0.870 [-1.045, -0.704]* / [-0.933, -0.804]* | -1.017 [-1.202, -0.838]* / [-1.086, -0.946]* |
| m075 | -2.976 [-3.357, -2.574]* / [-3.068, -2.890]* | -1.173 [-1.376, -0.979]* / [-1.244, -1.106]* | -0.547 [-0.739, -0.363]* / [-0.624, -0.468]* | -1.202 [-1.413, -1.005]* / [-1.272, -1.134]* | -0.836 [-1.027, -0.661]* / [-0.897, -0.768]* | -0.992 [-1.193, -0.812]* / [-1.064, -0.917]* |
| m090 | -2.440 [-2.790, -2.058]* / [-2.529, -2.353]* | -1.174 [-1.369, -0.991]* / [-1.242, -1.103]* | -0.542 [-0.731, -0.363]* / [-0.616, -0.464]* | -1.229 [-1.433, -1.035]* / [-1.298, -1.160]* | -0.838 [-1.023, -0.668]* / [-0.900, -0.771]* | -0.993 [-1.182, -0.809]* / [-1.063, -0.920]* |
| m105 | -2.067 [-2.459, -1.676]* / [-2.159, -1.987]* | -1.187 [-1.380, -0.997]* / [-1.256, -1.117]* | -0.556 [-0.748, -0.377]* / [-0.633, -0.477]* | -1.245 [-1.443, -1.051]* / [-1.313, -1.177]* | -0.846 [-1.022, -0.681]* / [-0.908, -0.781]* | -1.005 [-1.192, -0.827]* / [-1.077, -0.933]* |
| m120 | -1.700 [-2.077, -1.341]* / [-1.790, -1.618]* | -1.172 [-1.360, -0.989]* / [-1.246, -1.102]* | -0.524 [-0.707, -0.345]* / [-0.604, -0.445]* | -1.225 [-1.413, -1.037]* / [-1.298, -1.156]* | -0.866 [-1.033, -0.705]* / [-0.933, -0.798]* | -1.031 [-1.211, -0.854]* / [-1.104, -0.958]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.453 [-5.933, -4.939]* / [-5.535, -5.380]* | 1.149 [0.962, 1.348] / [1.081, 1.219] | 0.587 [0.490, 0.701] / [0.555, 0.616] | -0.048 [-0.078, -0.023]* / [-0.057, -0.040]* | 0.314 [0.251, 0.380] / [0.282, 0.346] | 0.018 [-0.112, 0.147] / [-0.031, 0.067] |
| m030 | -3.783 [-4.196, -3.349]* / [-3.860, -3.712]* | 1.160 [0.972, 1.351] / [1.092, 1.229] | 0.569 [0.481, 0.674] / [0.540, 0.600] | -0.073 [-0.093, -0.056]* / [-0.079, -0.066]* | 0.314 [0.259, 0.371] / [0.284, 0.345] | 0.027 [-0.099, 0.153] / [-0.019, 0.074] |
| m045 | -2.848 [-3.228, -2.450]* / [-2.922, -2.782]* | 1.157 [0.969, 1.348] / [1.088, 1.224] | 0.577 [0.490, 0.679] / [0.547, 0.606] | -0.069 [-0.090, -0.052]* / [-0.075, -0.063]* | 0.320 [0.262, 0.381] / [0.291, 0.351] | 0.040 [-0.083, 0.162] / [-0.004, 0.088] |
| m060 | -2.194 [-2.543, -1.824]* / [-2.265, -2.131]* | 1.165 [0.978, 1.363] / [1.096, 1.234] | 0.585 [0.498, 0.685] / [0.555, 0.615] | -0.062 [-0.086, -0.041]* / [-0.070, -0.056]* | 0.294 [0.235, 0.356] / [0.266, 0.324] | 0.018 [-0.102, 0.137] / [-0.027, 0.065] |
| m075 | -1.756 [-2.103, -1.389]* / [-1.828, -1.687]* | 1.173 [0.979, 1.376] / [1.106, 1.244] | 0.593 [0.500, 0.696] / [0.561, 0.622] | -0.041 [-0.069, -0.018]* / [-0.049, -0.032]* | 0.349 [0.289, 0.411] / [0.318, 0.379] | 0.048 [-0.073, 0.173] / [-0.000, 0.097] |
| m090 | -1.217 [-1.566, -0.861]* / [-1.288, -1.147]* | 1.174 [0.991, 1.369] / [1.103, 1.242] | 0.596 [0.505, 0.702] / [0.566, 0.627] | -0.068 [-0.096, -0.044]* / [-0.075, -0.060]* | 0.344 [0.284, 0.409] / [0.315, 0.376] | 0.051 [-0.070, 0.175] / [0.005, 0.098] |
| m105 | -0.818 [-1.151, -0.441]* / [-0.887, -0.751]* | 1.187 [0.997, 1.380] / [1.117, 1.256] | 0.604 [0.509, 0.715] / [0.573, 0.635] | -0.069 [-0.085, -0.055]* / [-0.076, -0.062]* | 0.352 [0.295, 0.413] / [0.322, 0.382] | 0.065 [-0.057, 0.188] / [0.018, 0.111] |
| m120 | -0.449 [-0.765, -0.094]* / [-0.519, -0.381]* | 1.172 [0.989, 1.360] / [1.102, 1.246] | 0.613 [0.517, 0.723] / [0.582, 0.645] | -0.066 [-0.089, -0.045]* / [-0.074, -0.057]* | 0.314 [0.257, 0.377] / [0.285, 0.345] | 0.025 [-0.096, 0.148] / [-0.023, 0.072] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.981 [-6.532, -5.387]* / [-6.065, -5.905]* | 0.522 [0.346, 0.719] / [0.442, 0.601] | -0.587 [-0.701, -0.490]* / [-0.616, -0.555]* | -0.629 [-0.744, -0.530]* / [-0.658, -0.598]* | -0.272 [-0.380, -0.171]* / [-0.317, -0.228]* | -0.600 [-0.765, -0.458]* / [-0.659, -0.541]* |
| m030 | -4.311 [-4.783, -3.806]* / [-4.391, -4.236]* | 0.549 [0.370, 0.748] / [0.471, 0.629] | -0.569 [-0.674, -0.481]* / [-0.600, -0.540]* | -0.642 [-0.756, -0.547]* / [-0.671, -0.613]* | -0.259 [-0.359, -0.165]* / [-0.302, -0.216]* | -0.576 [-0.739, -0.430]* / [-0.633, -0.516]* |
| m045 | -3.413 [-3.843, -2.956]* / [-3.489, -3.340]* | 0.539 [0.353, 0.740] / [0.462, 0.616] | -0.577 [-0.679, -0.490]* / [-0.606, -0.547]* | -0.645 [-0.754, -0.551]* / [-0.674, -0.615]* | -0.260 [-0.364, -0.162]* / [-0.304, -0.218]* | -0.572 [-0.730, -0.420]* / [-0.627, -0.514]* |
| m060 | -2.781 [-3.169, -2.345]* / [-2.858, -2.711]* | 0.538 [0.348, 0.727] / [0.461, 0.616] | -0.585 [-0.685, -0.498]* / [-0.615, -0.555]* | -0.647 [-0.751, -0.556]* / [-0.678, -0.617]* | -0.294 [-0.401, -0.198]* / [-0.337, -0.252]* | -0.602 [-0.765, -0.451]* / [-0.661, -0.543]* |
| m075 | -2.391 [-2.792, -1.951]* / [-2.465, -2.317]* | 0.547 [0.363, 0.739] / [0.468, 0.624] | -0.593 [-0.696, -0.500]* / [-0.622, -0.561]* | -0.627 [-0.735, -0.528]* / [-0.657, -0.595]* | -0.245 [-0.355, -0.143]* / [-0.289, -0.202]* | -0.568 [-0.736, -0.417]* / [-0.628, -0.504]* |
| m090 | -1.870 [-2.247, -1.449]* / [-1.946, -1.800]* | 0.542 [0.363, 0.731] / [0.464, 0.616] | -0.596 [-0.702, -0.505]* / [-0.627, -0.566]* | -0.661 [-0.776, -0.561]* / [-0.691, -0.629]* | -0.253 [-0.365, -0.149]* / [-0.298, -0.211]* | -0.571 [-0.735, -0.417]* / [-0.631, -0.507]* |
| m105 | -1.501 [-1.896, -1.070]* / [-1.575, -1.428]* | 0.556 [0.377, 0.748] / [0.477, 0.633] | -0.604 [-0.715, -0.509]* / [-0.635, -0.573]* | -0.672 [-0.781, -0.571]* / [-0.703, -0.640]* | -0.254 [-0.366, -0.152]* / [-0.298, -0.212]* | -0.573 [-0.730, -0.427]* / [-0.633, -0.513]* |
| m120 | -1.147 [-1.512, -0.741]* / [-1.219, -1.072]* | 0.524 [0.345, 0.707] / [0.445, 0.604] | -0.613 [-0.723, -0.517]* / [-0.645, -0.582]* | -0.675 [-0.791, -0.571]* / [-0.707, -0.644]* | -0.301 [-0.413, -0.194]* / [-0.344, -0.256]* | -0.612 [-0.770, -0.465]* / [-0.672, -0.552]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | 15.52 [15.36, 15.69] | 14.31 [14.17, 14.46] | 14.62 [14.46, 14.77] | 13.59 [13.44, 13.76] | 4.53 [4.48, 4.58] | 16.61 [16.41, 16.79] | 14.47 [14.34, 14.62] |
| m030 | 15.51 [15.35, 15.67] | 14.31 [14.17, 14.44] | 14.64 [14.49, 14.79] | 13.64 [13.48, 13.81] | 7.74 [7.67, 7.80] | 16.58 [16.40, 16.77] | 14.49 [14.36, 14.64] |
| m045 | 15.53 [15.37, 15.68] | 14.33 [14.20, 14.47] | 14.66 [14.52, 14.81] | 13.67 [13.53, 13.84] | 9.45 [9.37, 9.52] | 16.57 [16.39, 16.76] | 14.49 [14.35, 14.64] |
| m060 | 15.58 [15.42, 15.73] | 14.38 [14.25, 14.52] | 14.67 [14.53, 14.82] | 13.72 [13.57, 13.89] | 10.62 [10.54, 10.70] | 16.62 [16.43, 16.80] | 14.54 [14.41, 14.68] |
| m075 | 15.58 [15.43, 15.74] | 14.43 [14.30, 14.57] | 14.71 [14.57, 14.87] | 13.68 [13.53, 13.84] | 11.57 [11.47, 11.66] | 16.68 [16.50, 16.87] | 14.57 [14.44, 14.72] |
| m090 | 15.70 [15.55, 15.86] | 14.49 [14.35, 14.63] | 14.82 [14.67, 14.97] | 13.78 [13.63, 13.95] | 12.52 [12.42, 12.62] | 16.73 [16.53, 16.92] | 14.65 [14.51, 14.80] |
| m105 | 15.77 [15.61, 15.93] | 14.53 [14.39, 14.67] | 14.87 [14.73, 15.03] | 13.81 [13.66, 13.98] | 13.15 [13.05, 13.26] | 16.80 [16.60, 16.99] | 14.70 [14.56, 14.84] |
| m120 | 15.82 [15.66, 15.99] | 14.54 [14.40, 14.69] | 14.84 [14.69, 15.00] | 13.78 [13.63, 13.95] | 13.76 [13.66, 13.87] | 16.79 [16.60, 16.98] | 14.73 [14.59, 14.88] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| m015 | 11.46 [10.34, 12.53] | 10.30 [9.38, 11.17] | 10.59 [9.61, 11.52] | 9.67 [8.80, 10.41] | 12.45 [11.34, 13.43] | 10.43 [9.48, 11.32] | persistence |
| m030 | 8.23 [7.19, 9.21] | 7.06 [6.20, 7.90] | 7.39 [6.45, 8.27] | 6.49 [5.67, 7.28] | 9.27 [8.26, 10.14] | 7.22 [6.37, 8.08] | persistence |
| m045 | 6.58 [5.64, 7.51] | 5.36 [4.53, 6.14] | 5.72 [4.88, 6.57] | 4.80 [4.06, 5.52] | 7.56 [6.72, 8.39] | 5.50 [4.66, 6.27] | persistence |
| m060 | 5.39 [4.47, 6.26] | 4.14 [3.34, 4.89] | 4.46 [3.62, 5.30] | 3.53 [2.81, 4.20] | 6.35 [5.56, 7.14] | 4.28 [3.50, 5.02] | persistence |
| m075 | 4.70 [3.78, 5.58] | 3.36 [2.63, 4.06] | 3.73 [2.94, 4.49] | 2.68 [2.01, 3.28] | 5.71 [4.93, 6.47] | 3.50 [2.75, 4.22] | persistence |
| m090 | 3.73 [2.87, 4.57] | 2.32 [1.63, 3.02] | 2.75 [2.01, 3.50] | 1.69 [1.05, 2.30] | 4.70 [3.97, 5.43] | 2.48 [1.77, 3.19] | persistence |
| m105 | 3.13 [2.25, 4.05] | 1.62 [0.89, 2.36] | 2.08 [1.27, 2.92] | 0.99 [0.31, 1.63] | 4.08 [3.27, 4.93] | 1.82 [1.08, 2.58] | persistence |
| m120 | 2.48 [1.65, 3.36] | 0.91 [0.19, 1.63] | 1.33 [0.56, 2.17] | 0.26 [-0.40, 0.90] | 3.39 [2.60, 4.20] | 1.13 [0.42, 1.86] | persistence |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 12.446 [11.344, 13.433] / [12.248, 12.638] | 10.429 [9.475, 11.321] / [10.276, 10.588] | 11.458 [10.339, 12.525] / [11.287, 11.632] | 10.296 [9.376, 11.173] / [10.144, 10.450] | 10.589 [9.606, 11.524] / [10.437, 10.758] | 9.669 [8.796, 10.411] / [9.503, 9.854] |
| m030 | 9.273 [8.258, 10.137] / [9.074, 9.473] | 7.219 [6.371, 8.077] / [7.061, 7.378] | 8.230 [7.190, 9.208] / [8.061, 8.396] | 7.056 [6.200, 7.904] / [6.899, 7.214] | 7.390 [6.450, 8.271] / [7.230, 7.557] | 6.492 [5.673, 7.277] / [6.322, 6.671] |
| m045 | 7.562 [6.719, 8.386] / [7.363, 7.764] | 5.495 [4.658, 6.271] / [5.342, 5.651] | 6.579 [5.641, 7.514] / [6.417, 6.739] | 5.360 [4.534, 6.140] / [5.211, 5.514] | 5.724 [4.882, 6.569] / [5.563, 5.884] | 4.803 [4.058, 5.515] / [4.642, 4.979] |
| m060 | 6.349 [5.557, 7.138] / [6.148, 6.537] | 4.277 [3.504, 5.025] / [4.128, 4.431] | 5.387 [4.470, 6.262] / [5.228, 5.554] | 4.140 [3.338, 4.890] / [3.992, 4.287] | 4.460 [3.620, 5.299] / [4.302, 4.622] | 3.534 [2.810, 4.203] / [3.378, 3.706] |
| m075 | 5.713 [4.926, 6.465] / [5.507, 5.905] | 3.498 [2.748, 4.217] / [3.346, 3.650] | 4.696 [3.780, 5.576] / [4.528, 4.863] | 3.361 [2.628, 4.064] / [3.211, 3.511] | 3.726 [2.937, 4.493] / [3.564, 3.892] | 2.675 [2.011, 3.283] / [2.520, 2.849] |
| m090 | 4.700 [3.971, 5.435] / [4.495, 4.895] | 2.482 [1.773, 3.185] / [2.327, 2.636] | 3.729 [2.874, 4.571] / [3.567, 3.893] | 2.317 [1.628, 3.023] / [2.164, 2.470] | 2.745 [2.006, 3.495] / [2.588, 2.912] | 1.691 [1.047, 2.299] / [1.529, 1.867] |
| m105 | 4.084 [3.272, 4.927] / [3.880, 4.282] | 1.817 [1.083, 2.580] / [1.657, 1.967] | 3.129 [2.248, 4.055] / [2.955, 3.297] | 1.615 [0.889, 2.360] / [1.460, 1.762] | 2.078 [1.270, 2.920] / [1.918, 2.244] | 0.995 [0.313, 1.631] / [0.831, 1.170] |
| m120 | 3.389 [2.600, 4.200] / [3.195, 3.587] | 1.132 [0.415, 1.857] / [0.976, 1.282] | 2.482 [1.645, 3.360] / [2.318, 2.647] | 0.910 [0.186, 1.630] / [0.749, 1.056] | 1.332 [0.556, 2.172] / [1.170, 1.487] | 0.263 [-0.401, 0.903] / [0.101, 0.440] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -12.446 [-13.433, -11.344]* / [-12.638, -12.248]* | -2.134 [-2.540, -1.741]* / [-2.286, -1.968]* | -0.914 [-1.359, -0.496]* / [-1.095, -0.721]* | -2.256 [-2.653, -1.881]* / [-2.406, -2.092]* | -1.882 [-2.186, -1.578]* / [-2.026, -1.730]* | -2.547 [-2.842, -2.243]* / [-2.718, -2.364]* |
| m030 | -9.273 [-10.137, -8.258]* / [-9.473, -9.074]* | -2.147 [-2.551, -1.749]* / [-2.302, -1.989]* | -0.959 [-1.415, -0.514]* / [-1.134, -0.774]* | -2.302 [-2.705, -1.903]* / [-2.457, -2.143]* | -1.891 [-2.199, -1.564]* / [-2.034, -1.740]* | -2.534 [-2.833, -2.202]* / [-2.698, -2.359]* |
| m045 | -7.562 [-8.386, -6.719]* / [-7.764, -7.363]* | -2.133 [-2.510, -1.740]* / [-2.288, -1.976]* | -0.920 [-1.372, -0.473]* / [-1.099, -0.732]* | -2.265 [-2.644, -1.878]* / [-2.418, -2.110]* | -1.851 [-2.158, -1.523]* / [-1.993, -1.698]* | -2.484 [-2.783, -2.159]* / [-2.644, -2.314]* |
| m060 | -6.349 [-7.138, -5.557]* / [-6.537, -6.148]* | -2.123 [-2.505, -1.733]* / [-2.276, -1.960]* | -0.918 [-1.339, -0.464]* / [-1.093, -0.734]* | -2.255 [-2.626, -1.867]* / [-2.412, -2.093]* | -1.892 [-2.198, -1.570]* / [-2.033, -1.742]* | -2.496 [-2.800, -2.170]* / [-2.660, -2.317]* |
| m075 | -5.713 [-6.465, -4.926]* / [-5.905, -5.507]* | -2.146 [-2.531, -1.755]* / [-2.302, -1.978]* | -0.956 [-1.399, -0.537]* / [-1.129, -0.764]* | -2.265 [-2.669, -1.860]* / [-2.420, -2.096]* | -1.915 [-2.242, -1.583]* / [-2.064, -1.756]* | -2.556 [-2.855, -2.230]* / [-2.722, -2.370]* |
| m090 | -4.700 [-5.435, -3.971]* / [-4.895, -4.495]* | -2.131 [-2.496, -1.762]* / [-2.285, -1.961]* | -0.904 [-1.326, -0.496]* / [-1.080, -0.712]* | -2.272 [-2.653, -1.889]* / [-2.425, -2.103]* | -1.883 [-2.193, -1.561]* / [-2.031, -1.726]* | -2.515 [-2.814, -2.204]* / [-2.680, -2.342]* |
| m105 | -4.084 [-4.927, -3.272]* / [-4.282, -3.880]* | -2.146 [-2.528, -1.750]* / [-2.301, -1.978]* | -0.927 [-1.348, -0.518]* / [-1.102, -0.736]* | -2.296 [-2.670, -1.898]* / [-2.447, -2.132]* | -1.892 [-2.195, -1.570]* / [-2.040, -1.732]* | -2.534 [-2.833, -2.216]* / [-2.694, -2.353]* |
| m120 | -3.389 [-4.200, -2.600]* / [-3.587, -3.195]* | -2.095 [-2.473, -1.714]* / [-2.255, -1.930]* | -0.842 [-1.277, -0.412]* / [-1.021, -0.652]* | -2.265 [-2.635, -1.892]* / [-2.422, -2.102]* | -1.916 [-2.217, -1.615]* / [-2.063, -1.758]* | -2.561 [-2.865, -2.241]* / [-2.726, -2.390]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -10.429 [-11.321, -9.475]* / [-10.588, -10.276]* | 2.134 [1.741, 2.540] / [1.968, 2.286] | 1.139 [0.915, 1.398] / [1.067, 1.211] | -0.154 [-0.225, -0.088]* / [-0.176, -0.131]* | 0.237 [0.103, 0.372] / [0.160, 0.317] | -0.578 [-0.895, -0.276]* / [-0.698, -0.453]* |
| m030 | -7.219 [-8.077, -6.371]* / [-7.378, -7.061]* | 2.147 [1.749, 2.551] / [1.989, 2.302] | 1.102 [0.889, 1.336] / [1.036, 1.175] | -0.180 [-0.227, -0.140]* / [-0.196, -0.164]* | 0.243 [0.123, 0.365] / [0.170, 0.318] | -0.548 [-0.857, -0.260]* / [-0.669, -0.426]* |
| m045 | -5.495 [-6.271, -4.658]* / [-5.651, -5.342]* | 2.133 [1.740, 2.510] / [1.976, 2.288] | 1.125 [0.909, 1.362] / [1.055, 1.198] | -0.157 [-0.204, -0.114]* / [-0.175, -0.141]* | 0.265 [0.134, 0.396] / [0.194, 0.338] | -0.508 [-0.801, -0.231]* / [-0.619, -0.389]* |
| m060 | -4.277 [-5.025, -3.504]* / [-4.431, -4.128]* | 2.123 [1.733, 2.505] / [1.960, 2.276] | 1.121 [0.911, 1.352] / [1.049, 1.193] | -0.160 [-0.212, -0.112]* / [-0.179, -0.143]* | 0.217 [0.086, 0.344] / [0.147, 0.287] | -0.547 [-0.830, -0.276]* / [-0.659, -0.429]* |
| m075 | -3.498 [-4.217, -2.748]* / [-3.650, -3.346]* | 2.146 [1.755, 2.531] / [1.978, 2.302] | 1.127 [0.905, 1.372] / [1.055, 1.196] | -0.141 [-0.204, -0.083]* / [-0.162, -0.119]* | 0.236 [0.101, 0.367] / [0.159, 0.313] | -0.603 [-0.913, -0.333]* / [-0.727, -0.481]* |
| m090 | -2.482 [-3.185, -1.773]* / [-2.636, -2.327]* | 2.131 [1.762, 2.496] / [1.961, 2.285] | 1.155 [0.937, 1.410] / [1.085, 1.227] | -0.165 [-0.231, -0.107]* / [-0.185, -0.146]* | 0.247 [0.115, 0.383] / [0.176, 0.319] | -0.580 [-0.880, -0.299]* / [-0.695, -0.462]* |
| m105 | -1.817 [-2.580, -1.083]* / [-1.967, -1.657]* | 2.146 [1.750, 2.528] / [1.978, 2.301] | 1.171 [0.944, 1.425] / [1.100, 1.245] | -0.168 [-0.210, -0.136]* / [-0.186, -0.150]* | 0.259 [0.134, 0.380] / [0.185, 0.333] | -0.551 [-0.851, -0.267]* / [-0.671, -0.430]* |
| m120 | -1.132 [-1.857, -0.415]* / [-1.282, -0.976]* | 2.095 [1.714, 2.473] / [1.930, 2.255] | 1.190 [0.956, 1.442] / [1.122, 1.264] | -0.188 [-0.241, -0.142]* / [-0.208, -0.166]* | 0.185 [0.060, 0.310] / [0.115, 0.261] | -0.624 [-0.925, -0.341]* / [-0.736, -0.493]* |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -11.458 [-12.525, -10.339]* / [-11.632, -11.287]* | 0.914 [0.496, 1.359] / [0.721, 1.095] | -1.139 [-1.398, -0.915]* / [-1.211, -1.067]* | -1.281 [-1.533, -1.047]* / [-1.351, -1.210]* | -0.895 [-1.142, -0.668]* / [-1.002, -0.789]* | -1.825 [-2.173, -1.496]* / [-1.966, -1.673]* |
| m030 | -8.230 [-9.208, -7.190]* / [-8.396, -8.061]* | 0.959 [0.514, 1.415] / [0.774, 1.134] | -1.102 [-1.336, -0.889]* / [-1.175, -1.036]* | -1.280 [-1.522, -1.056]* / [-1.351, -1.212]* | -0.861 [-1.091, -0.642]* / [-0.966, -0.757]* | -1.762 [-2.090, -1.440]* / [-1.897, -1.614]* |
| m045 | -6.579 [-7.514, -5.641]* / [-6.739, -6.417]* | 0.920 [0.473, 1.372] / [0.732, 1.099] | -1.125 [-1.362, -0.909]* / [-1.198, -1.055]* | -1.282 [-1.515, -1.067]* / [-1.354, -1.215]* | -0.861 [-1.097, -0.638]* / [-0.964, -0.759]* | -1.742 [-2.074, -1.420]* / [-1.877, -1.598]* |
| m060 | -5.387 [-6.262, -4.470]* / [-5.554, -5.228]* | 0.918 [0.464, 1.339] / [0.734, 1.093] | -1.121 [-1.352, -0.911]* / [-1.193, -1.049]* | -1.280 [-1.495, -1.072]* / [-1.353, -1.209]* | -0.905 [-1.133, -0.695]* / [-1.005, -0.807]* | -1.777 [-2.112, -1.459]* / [-1.908, -1.634]* |
| m075 | -4.696 [-5.576, -3.780]* / [-4.863, -4.528]* | 0.956 [0.537, 1.399] / [0.764, 1.129] | -1.127 [-1.372, -0.905]* / [-1.196, -1.055]* | -1.252 [-1.495, -1.018]* / [-1.321, -1.179]* | -0.885 [-1.119, -0.664]* / [-0.987, -0.784]* | -1.809 [-2.137, -1.494]* / [-1.938, -1.659]* |
| m090 | -3.729 [-4.571, -2.874]* / [-3.893, -3.567]* | 0.904 [0.496, 1.326] / [0.712, 1.080] | -1.155 [-1.410, -0.937]* / [-1.227, -1.085]* | -1.313 [-1.578, -1.075]* / [-1.389, -1.239]* | -0.905 [-1.165, -0.671]* / [-1.009, -0.803]* | -1.822 [-2.145, -1.505]* / [-1.953, -1.674]* |
| m105 | -3.129 [-4.055, -2.248]* / [-3.297, -2.955]* | 0.927 [0.518, 1.348] / [0.736, 1.102] | -1.171 [-1.425, -0.944]* / [-1.245, -1.100]* | -1.337 [-1.593, -1.102]* / [-1.410, -1.267]* | -0.912 [-1.178, -0.681]* / [-1.018, -0.811]* | -1.822 [-2.145, -1.508]* / [-1.962, -1.671]* |
| m120 | -2.482 [-3.360, -1.645]* / [-2.647, -2.318]* | 0.842 [0.412, 1.277] / [0.652, 1.021] | -1.190 [-1.442, -0.956]* / [-1.264, -1.122]* | -1.372 [-1.638, -1.138]* / [-1.443, -1.300]* | -1.005 [-1.284, -0.770]* / [-1.105, -0.905]* | -1.894 [-2.213, -1.594]* / [-2.034, -1.742]* |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| m015 | Asia | 9.16 | 8.51 | 8.93 | 8.59 | 2.75 | 9.64 | 8.52 |
| m015 | EU | 6.15 | 5.65 | 5.83 | 5.40 | 2.15 | 6.41 | 5.67 |
| m015 | NA | 9.05 | 8.46 | 8.84 | 8.31 | 2.55 | 10.27 | 8.56 |
| m060 | Asia | 9.17 | 8.58 | 8.99 | 8.71 | 6.34 | 9.72 | 8.58 |
| m060 | EU | 6.31 | 5.74 | 5.95 | 5.56 | 4.25 | 6.50 | 5.82 |
| m060 | NA | 9.13 | 8.54 | 8.90 | 8.39 | 6.69 | 10.34 | 8.64 |
| m120 | Asia | 9.46 | 8.72 | 9.16 | 8.81 | 8.53 | 9.91 | 8.77 |
| m120 | EU | 6.49 | 5.95 | 6.20 | 5.76 | 5.37 | 6.71 | 6.01 |
| m120 | NA | 9.26 | 8.66 | 9.04 | 8.51 | 8.82 | 10.46 | 8.76 |

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|---|
| m015 | -0.19 | 0.10 | -1.22 | -1.18 | -1.59 | -1.09 | -1.24 |
| m030 | -0.27 | 0.10 | -1.27 | -1.24 | -1.52 | -1.03 | -1.19 |
| m045 | -0.20 | 0.09 | -1.30 | -1.27 | -1.48 | -0.98 | -1.15 |
| m060 | -0.10 | 0.09 | -1.33 | -1.30 | -1.54 | -1.05 | -1.21 |
| m075 | -0.23 | 0.09 | -1.28 | -1.24 | -1.62 | -1.09 | -1.22 |
| m090 | -0.08 | 0.08 | -1.30 | -1.27 | -1.58 | -1.05 | -1.19 |
| m105 | 0.04 | 0.09 | -1.30 | -1.27 | -1.57 | -1.04 | -1.19 |
| m120 | 0.23 | 0.09 | -1.31 | -1.27 | -1.62 | -1.09 | -1.24 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | 8.12 [8.03, 8.21] | 7.53 [7.46, 7.61] | 7.89 [7.81, 7.97] | 7.41 [7.33, 7.51] | 8.72 [8.64, 8.79] | 8.74 [8.64, 8.84] | 7.60 [7.52, 7.68] |
| h4-8 | 8.42 [8.33, 8.51] | 7.80 [7.72, 7.89] | 8.19 [8.11, 8.28] | 7.72 [7.64, 7.83] | 11.35 [11.24, 11.47] | 9.05 [8.95, 9.16] | 7.88 [7.80, 7.97] |
| h8+ | 8.11 [8.02, 8.22] | 7.51 [7.41, 7.61] | 7.96 [7.86, 8.06] | 7.74 [7.63, 7.87] | 14.19 [13.98, 14.39] | 8.83 [8.71, 8.96] | 7.60 [7.51, 7.70] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| h2-4 | 0.59 [0.50, 0.70] | -0.06 [-0.08, -0.05]* | 0.34 [0.28, 0.40] | 0.04 [-0.08, 0.17] | 0.66 [0.31, 1.04] | 1.17 [0.98, 1.36] | wt_med |
| h4-8 | 0.61 [0.52, 0.72] | -0.08 [-0.09, -0.06]* | 0.34 [0.28, 0.41] | 0.05 [-0.08, 0.18] | 3.22 [2.68, 3.78] | 1.22 [1.03, 1.43] | wt_med |
| h8+ | 0.56 [0.47, 0.66] | -0.09 [-0.12, -0.06]* | 0.40 [0.33, 0.47] | 0.29 [0.17, 0.39] | 6.66 [5.43, 8.19] | 1.32 [1.13, 1.51] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.681 [0.323, 1.028] / [0.595, 0.776] | -0.661 [-1.038, -0.312]* / [-0.730, -0.590]* | 0.106 [-0.290, 0.482] / [0.029, 0.180] | -0.757 [-1.131, -0.406]* / [-0.824, -0.688]* | -0.299 [-0.696, 0.070] / [-0.368, -0.222]*  ! | -0.712 [-1.063, -0.403]* / [-0.782, -0.638]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.681 [-1.028, -0.323]* / [-0.776, -0.595]* | -1.165 [-1.365, -0.975]* / [-1.233, -1.099]* | -0.538 [-0.732, -0.359]* / [-0.614, -0.461]* | -1.212 [-1.415, -1.021]* / [-1.281, -1.146]* | -0.834 [-1.011, -0.670]* / [-0.897, -0.769]* | -1.001 [-1.183, -0.821]* / [-1.071, -0.930]* |
| h4-8 | 1.676 [1.184, 2.223] / [1.560, 1.789] | -1.224 [-1.427, -1.031]* / [-1.297, -1.153]* | -0.581 [-0.782, -0.389]* / [-0.660, -0.501]* | -1.286 [-1.492, -1.090]* / [-1.361, -1.216]* | -0.885 [-1.063, -0.720]* / [-0.952, -0.819]* | -1.057 [-1.251, -0.869]* / [-1.133, -0.982]* |
| h8+ | 4.951 [3.844, 6.393] / [4.760, 5.140] | -1.320 [-1.507, -1.135]* / [-1.404, -1.248]* | -0.752 [-0.943, -0.542]* / [-0.840, -0.666]* | -1.387 [-1.586, -1.194]* / [-1.473, -1.314]* | -0.922 [-1.105, -0.751]* / [-0.998, -0.852]* | -0.974 [-1.180, -0.777]* / [-1.058, -0.890]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.661 [0.312, 1.038] / [0.590, 0.730] | 1.165 [0.975, 1.365] / [1.099, 1.233] | 0.591 [0.500, 0.697] / [0.561, 0.621] | -0.061 [-0.076, -0.048]* / [-0.066, -0.057]* | 0.335 [0.280, 0.397] / [0.305, 0.366] | 0.042 [-0.080, 0.166] / [-0.004, 0.090] |
| h4-8 | 3.217 [2.675, 3.781] / [3.133, 3.309] | 1.224 [1.031, 1.427] / [1.153, 1.297] | 0.614 [0.522, 0.716] / [0.582, 0.644] | -0.077 [-0.095, -0.062]* / [-0.082, -0.072]* | 0.343 [0.282, 0.409] / [0.311, 0.376] | 0.054 [-0.076, 0.177] / [0.005, 0.105] |
| h8+ | 6.656 [5.426, 8.195] / [6.483, 6.823] | 1.320 [1.135, 1.507] / [1.248, 1.404] | 0.559 [0.470, 0.663] / [0.526, 0.590] | -0.091 [-0.124, -0.062]* / [-0.100, -0.082]* | 0.395 [0.327, 0.466] / [0.361, 0.430] | 0.290 [0.174, 0.394] / [0.238, 0.346] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.106 [-0.482, 0.290] / [-0.180, -0.029]*  ! | 0.538 [0.359, 0.732] / [0.461, 0.614] | -0.591 [-0.697, -0.500]* / [-0.621, -0.561]* | -0.649 [-0.756, -0.551]* / [-0.678, -0.620]* | -0.258 [-0.360, -0.161]* / [-0.301, -0.216]* | -0.581 [-0.742, -0.432]* / [-0.641, -0.521]* |
| h4-8 | 2.322 [1.810, 2.858] / [2.232, 2.418] | 0.581 [0.389, 0.782] / [0.501, 0.660] | -0.614 [-0.716, -0.522]* / [-0.644, -0.582]* | -0.688 [-0.797, -0.592]* / [-0.719, -0.656]* | -0.272 [-0.376, -0.174]* / [-0.318, -0.228]* | -0.600 [-0.762, -0.454]* / [-0.664, -0.537]* |
| h8+ | 5.571 [4.462, 7.043] / [5.418, 5.731] | 0.752 [0.542, 0.943] / [0.666, 0.840] | -0.559 [-0.663, -0.470]* / [-0.590, -0.526]* | -0.650 [-0.768, -0.548]* / [-0.682, -0.616]* | -0.169 [-0.286, -0.062]* / [-0.218, -0.121]* | -0.309 [-0.482, -0.153]* / [-0.375, -0.237]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | 15.73 [15.58, 15.89] | 14.51 [14.38, 14.66] | 14.84 [14.70, 14.99] | 13.79 [13.65, 13.96] | 15.81 [15.69, 15.94] | 16.76 [16.57, 16.94] | 14.68 [14.54, 14.82] |
| h4-8 | 15.95 [15.79, 16.11] | 14.71 [14.57, 14.86] | 15.06 [14.92, 15.22] | 14.08 [13.92, 14.26] | 19.67 [19.51, 19.84] | 17.01 [16.82, 17.19] | 14.89 [14.75, 15.04] |
| h8+ | 14.73 [14.57, 14.90] | 13.60 [13.45, 13.76] | 14.04 [13.87, 14.20] | 13.60 [13.41, 13.80] | 23.26 [22.97, 23.55] | 16.00 [15.81, 16.19] | 13.80 [13.65, 13.97] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| h2-4 | 1.14 [0.92, 1.39] | -0.16 [-0.20, -0.13]* | 0.25 [0.13, 0.38] | -0.57 [-0.88, -0.29]* | 0.94 [0.23, 1.62] | 2.13 [1.75, 2.52] | wt_med |
| h4-8 | 1.15 [0.93, 1.38] | -0.18 [-0.22, -0.15]* | 0.25 [0.12, 0.38] | -0.53 [-0.84, -0.25]* | 5.26 [4.35, 6.13] | 2.22 [1.84, 2.59] | wt_med |
| h8+ | 1.00 [0.83, 1.21] | -0.21 [-0.29, -0.15]* | 0.29 [0.16, 0.40] | -0.10 [-0.33, 0.10] | 10.83 [8.61, 13.30] | 2.33 [2.00, 2.64] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.472 [0.753, 2.252] / [1.262, 1.675] | -0.939 [-1.618, -0.229]* / [-1.104, -0.783]* | 0.494 [-0.310, 1.293] / [0.317, 0.660] | -1.141 [-1.843, -0.405]* / [-1.303, -0.988]* | -0.627 [-1.370, 0.179] / [-0.790, -0.454]*  ! | -1.702 [-2.325, -1.070]* / [-1.868, -1.527]* |
| h4-8 | -2.575 [-3.479, -1.628]* / [-2.813, -2.327]* | -5.260 [-6.131, -4.346]* / [-5.455, -5.090]* | -3.687 [-4.550, -2.784]* / [-3.890, -3.500]* | -5.502 [-6.386, -4.601]* / [-5.693, -5.333]* | -4.862 [-5.805, -3.909]* / [-5.054, -4.669]* | -5.655 [-6.481, -4.878]* / [-5.860, -5.452]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -1.472 [-2.252, -0.753]* / [-1.675, -1.262]* | -2.131 [-2.521, -1.746]* / [-2.282, -1.974]* | -0.915 [-1.358, -0.482]* / [-1.086, -0.733]* | -2.266 [-2.658, -1.885]* / [-2.413, -2.108]* | -1.871 [-2.180, -1.554]* / [-2.012, -1.718]* | -2.538 [-2.835, -2.223]* / [-2.700, -2.358]* |
| h4-8 | 2.575 [1.628, 3.479] / [2.327, 2.813] | -2.216 [-2.587, -1.835]* / [-2.372, -2.055]* | -1.009 [-1.433, -0.583]* / [-1.184, -0.820]* | -2.371 [-2.744, -1.992]* / [-2.524, -2.211]* | -1.960 [-2.256, -1.642]* / [-2.103, -1.808]* | -2.615 [-2.936, -2.276]* / [-2.779, -2.436]* |
| h8+ | 8.158 [6.041, 10.561] / [7.815, 8.508] | -2.330 [-2.638, -1.997]* / [-2.486, -2.177]* | -1.303 [-1.672, -0.912]* / [-1.484, -1.124]* | -2.500 [-2.830, -2.150]* / [-2.662, -2.349]* | -2.034 [-2.321, -1.726]* / [-2.187, -1.884]* | -2.421 [-2.732, -2.082]* / [-2.605, -2.235]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.939 [0.229, 1.618] / [0.783, 1.104] | 2.131 [1.746, 2.521] / [1.974, 2.282] | 1.144 [0.923, 1.386] / [1.077, 1.216] | -0.161 [-0.197, -0.134]* / [-0.172, -0.150]* | 0.253 [0.125, 0.382] / [0.177, 0.326] | -0.570 [-0.879, -0.287]* / [-0.689, -0.450]* |
| h4-8 | 5.260 [4.346, 6.131] / [5.090, 5.455] | 2.216 [1.835, 2.587] / [2.055, 2.372] | 1.155 [0.933, 1.379] / [1.086, 1.226] | -0.184 [-0.224, -0.152]* / [-0.196, -0.173]* | 0.249 [0.123, 0.379] / [0.170, 0.327] | -0.532 [-0.835, -0.248]* / [-0.652, -0.403]* |
| h8+ | 10.833 [8.613, 13.301] / [10.536, 11.130] | 2.330 [1.997, 2.638] / [2.177, 2.486] | 1.004 [0.831, 1.213] / [0.936, 1.068] | -0.214 [-0.285, -0.153]* / [-0.236, -0.193]* | 0.285 [0.159, 0.397] / [0.212, 0.362] | -0.103 [-0.332, 0.098] / [-0.223, 0.019] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.494 [-1.293, 0.310] / [-0.660, -0.317]*  ! | 0.915 [0.482, 1.358] / [0.733, 1.086] | -1.144 [-1.386, -0.923]* / [-1.216, -1.077]* | -1.299 [-1.532, -1.075]* / [-1.369, -1.230]* | -0.890 [-1.127, -0.672]* / [-0.993, -0.790]* | -1.817 [-2.136, -1.497]* / [-1.950, -1.668]* |
| h4-8 | 3.687 [2.784, 4.550] / [3.500, 3.890] | 1.009 [0.583, 1.433] / [0.820, 1.184] | -1.155 [-1.379, -0.933]* / [-1.226, -1.086]* | -1.332 [-1.556, -1.113]* / [-1.403, -1.263]* | -0.899 [-1.123, -0.686]* / [-1.005, -0.795]* | -1.815 [-2.153, -1.504]* / [-1.958, -1.658]* |
| h8+ | 9.105 [6.786, 11.617] / [8.821, 9.406] | 1.303 [0.912, 1.672] / [1.124, 1.484] | -1.004 [-1.213, -0.831]* / [-1.068, -0.936]* | -1.215 [-1.441, -1.023]* / [-1.282, -1.145]* | -0.726 [-0.928, -0.539]* / [-0.830, -0.625]* | -1.220 [-1.537, -0.939]* / [-1.364, -1.068]* |

**MAE by region (all rides)**

_no rows_

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|---|
| h2-4 | 0.21 | 0.09 | -1.31 | -1.28 | -1.59 | -1.07 | -1.22 |
| h4-8 | 0.44 | 0.07 | -1.41 | -1.33 | -1.67 | -1.11 | -1.28 |
| h8+ | 1.43 | 0.01 | -1.57 | -1.46 | -1.92 | -1.36 | -1.46 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.07 [7.99, 8.16] | 7.48 [7.41, 7.57] | 9.96 [9.83, 10.10] | 7.83 [7.75, 7.92] | 7.37 [7.28, 7.46] | 5.40 [5.35, 5.45] | 6.55 [6.47, 6.64] | 8.69 [8.59, 8.79] | 7.55 [7.47, 7.63] |
| 1 | 8.10 [8.01, 8.18] | 7.59 [7.51, 7.67] | 10.04 [9.91, 10.19] | 7.85 [7.78, 7.94] | 7.50 [7.42, 7.60] | 5.42 [5.38, 5.47] | 6.54 [6.46, 6.63] | 8.69 [8.59, 8.79] | 7.66 [7.58, 7.74] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.59 [0.50, 0.69] | -0.06 [-0.08, -0.05]* | 2.61 [2.23, 3.07] | 0.33 [0.27, 0.38] | 0.04 [-0.08, 0.16] | -2.17 [-2.44, -1.90]* | -0.97 [-1.20, -0.77]* | 1.16 [0.97, 1.36] | wt_med |
| 1 | 0.52 [0.43, 0.63] | -0.07 [-0.08, -0.05]* | 2.60 [2.21, 3.05] | 0.25 [0.19, 0.31] | 0.09 [-0.04, 0.21] | -2.25 [-2.53, -1.97]* | -1.08 [-1.31, -0.87]* | 1.06 [0.87, 1.25] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -1.164 [-1.359, -0.975]* / [-1.233, -1.099]* | -0.540 [-0.732, -0.361]* / [-0.616, -0.464]* | -1.213 [-1.415, -1.021]* / [-1.281, -1.147]* | -0.843 [-1.017, -0.684]* / [-0.905, -0.779]* | -1.000 [-1.181, -0.824]* / [-1.069, -0.930]* | 1.688 [1.286, 2.095] / [1.576, 1.804] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.164 [0.975, 1.359] / [1.099, 1.233] | 0.588 [0.495, 0.693] / [0.559, 0.617] | -0.062 [-0.079, -0.047]* / [-0.066, -0.057]* | 0.326 [0.271, 0.384] / [0.296, 0.355] | 0.039 [-0.083, 0.163] / [-0.005, 0.086] | 2.614 [2.228, 3.073] / [2.527, 2.702] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.540 [0.361, 0.732] / [0.464, 0.616] | -0.588 [-0.693, -0.495]* / [-0.617, -0.559]* | -0.647 [-0.757, -0.548]* / [-0.676, -0.618]* | -0.265 [-0.369, -0.167]* / [-0.307, -0.224]* | -0.579 [-0.738, -0.434]* / [-0.639, -0.521]* | 2.015 [1.610, 2.441] / [1.932, 2.103] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 15.60 [15.44, 15.76] | 14.39 [14.26, 14.53] | 18.53 [18.26, 18.80] | 14.70 [14.56, 14.85] | 13.69 [13.55, 13.86] | 9.67 [9.60, 9.74] | 11.70 [11.54, 11.87] | 16.64 [16.46, 16.83] | 14.56 [14.42, 14.70] |
| 1 | 15.62 [15.47, 15.78] | 14.55 [14.42, 14.69] | 18.68 [18.39, 18.96] | 14.71 [14.57, 14.86] | 13.90 [13.75, 14.06] | 9.68 [9.61, 9.75] | 11.66 [11.51, 11.83] | 16.61 [16.43, 16.80] | 14.73 [14.59, 14.87] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.14 [0.92, 1.38] | -0.16 [-0.20, -0.13]* | 4.40 [3.40, 5.52] | 0.24 [0.12, 0.36] | -0.56 [-0.86, -0.29]* | -4.91 [-5.45, -4.39]* | -2.76 [-3.00, -2.52]* | 2.13 [1.74, 2.50] | wt_med |
| 1 | 1.01 [0.79, 1.25] | -0.17 [-0.22, -0.14]* | 4.37 [3.36, 5.50] | 0.07 [-0.05, 0.20] | -0.49 [-0.80, -0.21]* | -5.07 [-5.62, -4.53]* | -2.97 [-3.20, -2.72]* | 1.93 [1.54, 2.31] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -2.127 [-2.502, -1.743]* / [-2.274, -1.970]* | -0.918 [-1.356, -0.492]* / [-1.086, -0.735]* | -2.266 [-2.641, -1.890]* / [-2.417, -2.107]* | -1.885 [-2.186, -1.569]* / [-2.026, -1.733]* | -2.520 [-2.806, -2.216]* / [-2.680, -2.344]* | 2.712 [1.645, 3.709] / [2.415, 3.015] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 2.127 [1.743, 2.502] / [1.970, 2.274] | 1.138 [0.919, 1.377] / [1.071, 1.208] | -0.163 [-0.203, -0.131]* / [-0.174, -0.152]* | 0.236 [0.118, 0.358] / [0.164, 0.306] | -0.565 [-0.862, -0.288]* / [-0.682, -0.447]* | 4.399 [3.396, 5.515] / [4.172, 4.617] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.918 [0.492, 1.356] / [0.735, 1.086] | -1.138 [-1.377, -0.919]* / [-1.208, -1.071]* | -1.296 [-1.537, -1.074]* / [-1.365, -1.230]* | -0.901 [-1.141, -0.683]* / [-1.001, -0.802]* | -1.800 [-2.114, -1.495]* / [-1.931, -1.655]* | 3.267 [2.180, 4.351] / [3.045, 3.497] |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 9.25 | 8.57 | 13.58 | 9.02 | 8.70 | 5.76 | 7.75 | 9.74 | 8.62 |
| 0 | EU | 6.31 | 5.78 | 7.96 | 6.02 | 5.60 | 4.13 | 5.00 | 6.54 | 5.83 |
| 0 | NA | 9.09 | 8.52 | 10.25 | 8.90 | 8.39 | 6.48 | 7.32 | 10.31 | 8.61 |
| 1 | Asia | 9.25 | 8.71 | 13.68 | 9.04 | 8.83 | 5.79 | 7.75 | 9.74 | 8.76 |
| 1 | EU | 6.33 | 5.88 | 8.02 | 6.04 | 5.75 | 4.16 | 5.00 | 6.53 | 5.93 |
| 1 | NA | 9.14 | 8.60 | 10.33 | 8.92 | 8.51 | 6.50 | 7.32 | 10.31 | 8.70 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 8.10 [8.01, 8.18] | 7.59 [7.51, 7.67] | 10.04 [9.91, 10.19] | 7.85 [7.78, 7.94] | 7.50 [7.42, 7.60] | 5.42 [5.38, 5.47] | 6.54 [6.46, 6.63] |  |  | 8.69 [8.59, 8.79] | 7.66 [7.58, 7.74] |
| 2 | 8.12 [8.03, 8.20] | 7.62 [7.55, 7.71] | 10.06 [9.92, 10.20] | 7.86 [7.79, 7.95] | 7.57 [7.49, 7.67] | 5.43 [5.38, 5.48] | 6.54 [6.46, 6.63] |  |  | 8.69 [8.59, 8.79] | 7.70 [7.62, 7.78] |
| 3 | 8.14 [8.06, 8.23] | 7.65 [7.58, 7.74] | 10.07 [9.94, 10.22] | 7.87 [7.79, 7.96] | 7.65 [7.57, 7.75] | 5.43 [5.39, 5.48] | 6.54 [6.46, 6.63] | 8.11 [8.02, 8.21] | 8.09 [8.00, 8.19] | 8.68 [8.58, 8.79] | 7.73 [7.66, 7.82] |
| 4 | 8.15 [8.07, 8.24] | 7.68 [7.60, 7.76] | 10.09 [9.95, 10.23] | 7.87 [7.80, 7.96] | 7.70 [7.61, 7.80] | 5.44 [5.40, 5.49] | 6.53 [6.45, 6.62] | 8.18 [8.08, 8.28] | 8.16 [8.06, 8.26] | 8.67 [8.58, 8.78] | 7.76 [7.68, 7.84] |
| 5 | 8.17 [8.09, 8.26] | 7.71 [7.63, 7.79] | 10.13 [9.99, 10.27] | 7.88 [7.81, 7.97] | 7.78 [7.69, 7.88] | 5.45 [5.41, 5.50] | 6.53 [6.46, 6.62] | 8.26 [8.16, 8.36] | 8.23 [8.14, 8.34] | 8.67 [8.57, 8.77] | 7.79 [7.71, 7.88] |
| 6 | 8.20 [8.12, 8.29] | 7.77 [7.69, 7.85] | 10.49 [10.35, 10.63] | 7.90 [7.83, 7.99] | 7.85 [7.76, 7.96] | 5.47 [5.43, 5.52] | 6.53 [6.46, 6.62] | 8.28 [8.19, 8.39] | 8.26 [8.17, 8.37] | 8.67 [8.57, 8.77] | 7.85 [7.77, 7.94] |
| 7 | 8.23 [8.15, 8.32] | 7.87 [7.79, 7.96] | 10.64 [10.50, 10.79] | 8.21 [8.14, 8.30] | 8.04 [7.94, 8.14] | 5.51 [5.46, 5.55] | 6.99 [6.91, 7.08] | 8.49 [8.39, 8.60] | 8.47 [8.37, 8.58] |  | 7.96 [7.88, 8.05] |
| 10 | 8.29 [8.20, 8.38] | 7.97 [7.89, 8.06] | 10.76 [10.60, 10.91] | 8.25 [8.17, 8.34] | 8.20 [8.11, 8.31] | 5.53 [5.48, 5.58] | 6.99 [6.91, 7.08] | 8.66 [8.57, 8.79] | 8.64 [8.55, 8.76] |  | 8.07 [7.99, 8.16] |
| 14 | 8.38 [8.30, 8.48] | 8.12 [8.04, 8.21] | 10.88 [10.72, 11.04] | 8.50 [8.41, 8.60] | 8.45 [8.35, 8.57] | 5.59 [5.54, 5.65] | 7.30 [7.22, 7.40] | 8.92 [8.82, 9.04] | 8.90 [8.80, 9.02] |  | 8.22 [8.14, 8.32] |
| 21 | 8.51 [8.42, 8.61] | 8.28 [8.20, 8.37] | 11.00 [10.85, 11.18] | 8.71 [8.63, 8.81] | 8.67 [8.56, 8.78] | 5.64 [5.60, 5.70] | 7.55 [7.46, 7.65] | 9.18 [9.07, 9.31] | 9.16 [9.05, 9.28] |  | 8.40 [8.31, 8.49] |
| 30 | 8.63 [8.54, 8.72] | 8.42 [8.34, 8.52] | 11.22 [11.06, 11.41] | 8.83 [8.74, 8.92] | 9.03 [8.90, 9.17] | 5.66 [5.61, 5.71] | 7.67 [7.57, 7.76] | 9.62 [9.49, 9.76] | 9.59 [9.46, 9.74] |  | 8.55 [8.47, 8.65] |
| 45 | 8.70 [8.61, 8.79] | 8.54 [8.45, 8.63] | 11.40 [11.13, 11.69] | 8.96 [8.87, 9.06] | 9.32 [9.15, 9.48] | 5.68 [5.63, 5.74] | 7.82 [7.73, 7.93] | 10.16 [9.99, 10.33] | 10.14 [9.97, 10.30] |  | 8.68 [8.59, 8.78] |
| 60 | 8.75 [8.66, 8.85] | 8.69 [8.59, 8.79] |  | 9.12 [9.01, 9.22] | 4.26 [2.98, 5.74] | 5.70 [5.65, 5.77] | 8.01 [7.91, 8.12] | 4.30 [3.02, 5.66] | 4.30 [3.03, 5.66] |  | 8.83 [8.74, 8.93] |
| 90 | 8.96 [8.84, 9.08] | 8.75 [8.64, 8.87] |  | 9.27 [9.16, 9.40] |  | 5.70 [5.63, 5.76] | 8.09 [7.98, 8.22] |  |  |  | 8.91 [8.79, 9.03] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.52 [0.43, 0.63] | -0.07 [-0.08, -0.05]* | 2.60 [2.21, 3.05] | 0.25 [0.19, 0.31] | 0.09 [-0.04, 0.21] | -2.25 [-2.53, -1.97]* | -1.08 [-1.31, -0.87]* |  |  | 1.06 [0.87, 1.25] | wt_med |
| 2 | 0.50 [0.42, 0.61] | -0.07 [-0.09, -0.05]* | 2.58 [2.20, 3.04] | 0.21 [0.16, 0.27] | 0.12 [-0.00, 0.25] | -2.29 [-2.57, -2.01]* | -1.12 [-1.36, -0.92]* |  |  | 1.02 [0.83, 1.20] | wt_med |
| 3 | 0.49 [0.40, 0.60] | -0.07 [-0.09, -0.06]* | 2.55 [2.16, 3.00] | 0.19 [0.13, 0.24] | 0.18 [0.05, 0.30] | -2.32 [-2.61, -2.03]* | -1.16 [-1.40, -0.95]* | 0.73 [0.52, 0.93] | 0.71 [0.50, 0.91] | 0.98 [0.79, 1.16] | wt_med |
| 4 | 0.48 [0.39, 0.59] | -0.07 [-0.09, -0.06]* | 2.54 [2.14, 3.00] | 0.16 [0.10, 0.22] | 0.19 [0.06, 0.31] | -2.34 [-2.63, -2.05]* | -1.19 [-1.44, -0.98]* | 0.77 [0.56, 0.97] | 0.74 [0.54, 0.95] | 0.94 [0.75, 1.13] | wt_med |
| 5 | 0.47 [0.38, 0.59] | -0.07 [-0.09, -0.06]* | 2.52 [2.11, 2.98] | 0.13 [0.07, 0.20] | 0.23 [0.10, 0.36] | -2.36 [-2.66, -2.07]* | -1.23 [-1.48, -1.02]* | 0.82 [0.61, 1.03] | 0.80 [0.59, 1.01] | 0.90 [0.71, 1.09] | wt_med |
| 6 | 0.44 [0.35, 0.56] | -0.08 [-0.09, -0.06]* | 2.80 [2.38, 3.26] | 0.10 [0.03, 0.16] | 0.24 [0.11, 0.37] | -2.40 [-2.70, -2.10]* | -1.29 [-1.54, -1.08]* | 0.79 [0.59, 1.01] | 0.77 [0.58, 0.98] | 0.83 [0.64, 1.03] | wt_med |
| 7 | 0.37 [0.28, 0.49] | -0.08 [-0.10, -0.06]* | 2.85 [2.42, 3.32] | 0.34 [0.28, 0.41] | 0.32 [0.19, 0.45] | -2.47 [-2.79, -2.18]* | -0.89 [-1.13, -0.68]* | 0.89 [0.67, 1.11] | 0.87 [0.65, 1.09] |  | wt_med |
| 10 | 0.32 [0.23, 0.44] | -0.08 [-0.10, -0.07]* | 2.84 [2.36, 3.36] | 0.26 [0.19, 0.33] | 0.37 [0.24, 0.52] | -2.55 [-2.88, -2.25]* | -1.01 [-1.25, -0.79]* | 0.96 [0.73, 1.21] | 0.94 [0.71, 1.19] |  | wt_med |
| 14 | 0.26 [0.16, 0.38] | -0.09 [-0.11, -0.07]* | 2.78 [2.29, 3.30] | 0.38 [0.32, 0.45] | 0.45 [0.28, 0.61] | -2.65 [-2.98, -2.33]* | -0.82 [-1.07, -0.59]* | 1.04 [0.76, 1.31] | 1.02 [0.74, 1.29] |  | wt_med |
| 21 | 0.19 [0.08, 0.33] | -0.10 [-0.12, -0.08]* | 2.68 [2.17, 3.20] | 0.41 [0.35, 0.46] | 0.43 [0.28, 0.58] | -2.77 [-3.11, -2.46]* | -0.77 [-1.03, -0.52]* | 1.04 [0.76, 1.34] | 1.02 [0.75, 1.32] |  | wt_med |
| 30 | 0.16 [0.04, 0.31] | -0.10 [-0.12, -0.08]* | 2.62 [2.11, 3.13] | 0.38 [0.32, 0.44] | 0.46 [0.32, 0.60] | -2.91 [-3.27, -2.58]* | -0.79 [-1.05, -0.54]* | 1.11 [0.84, 1.39] | 1.09 [0.81, 1.37] |  | wt_med |
| 45 | 0.13 [-0.00, 0.31] | -0.10 [-0.13, -0.08]* | 1.84 [1.39, 2.29] | 0.42 [0.33, 0.49] | 0.49 [0.31, 0.65] | -3.02 [-3.40, -2.67]* | -0.73 [-1.00, -0.46]* | 1.31 [1.00, 1.64] | 1.28 [0.97, 1.62] |  | wt_med |
| 60 | 0.07 [-0.11, 0.29] | -0.10 [-0.13, -0.08] |  | 0.47 [0.38, 0.56] | 0.05 [-1.17, 1.45] | -3.14 [-3.55, -2.76]* | -0.64 [-0.94, -0.35]* | -0.10 [-1.21, 0.72] | -0.10 [-1.22, 0.73] |  | wt_med |
| 90 | 0.20 [-0.01, 0.44] | -0.10 [-0.12, -0.07]* |  | 0.54 [0.43, 0.67] |  | -3.22 [-3.74, -2.77]* | -0.63 [-0.94, -0.34]* |  |  |  | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -1.061 [-1.250, -0.874]* / [-1.129, -0.994]* | -0.503 [-0.692, -0.321]* / [-0.577, -0.425]* | -1.113 [-1.308, -0.922]* / [-1.181, -1.047]* | -0.820 [-0.994, -0.659]* / [-0.882, -0.756]* | -0.837 [-1.018, -0.665]* / [-0.907, -0.761]* | 1.792 [1.393, 2.193] / [1.681, 1.918] |  |  |
| 2 | -1.016 [-1.203, -0.826]* / [-1.083, -0.950]* | -0.476 [-0.664, -0.289]* / [-0.551, -0.396]* | -1.070 [-1.263, -0.879]* / [-1.137, -1.005]* | -0.811 [-0.984, -0.650]* / [-0.873, -0.746]* | -0.763 [-0.939, -0.588]* / [-0.833, -0.688]* | 1.796 [1.395, 2.201] / [1.683, 1.914] |  |  |
| 3 | -0.976 [-1.162, -0.788]* / [-1.047, -0.910]* | -0.448 [-0.635, -0.262]* / [-0.523, -0.368]* | -1.032 [-1.221, -0.841]* / [-1.102, -0.967]* | -0.800 [-0.973, -0.639]* / [-0.864, -0.735]* | -0.660 [-0.835, -0.484]* / [-0.730, -0.585]* | 1.810 [1.408, 2.214] / [1.700, 1.929] | -0.079 [-0.293, 0.135] / [-0.157, 0.000] | -0.103 [-0.317, 0.110] / [-0.181, -0.024]*  ! |
| 4 | -0.938 [-1.126, -0.749]* / [-1.007, -0.869]* | -0.418 [-0.607, -0.230]* / [-0.493, -0.335]* | -0.995 [-1.186, -0.803]* / [-1.065, -0.928]* | -0.789 [-0.962, -0.627]* / [-0.852, -0.725]* | -0.619 [-0.793, -0.445]* / [-0.689, -0.548]* | 1.819 [1.411, 2.244] / [1.705, 1.936] | -0.014 [-0.226, 0.203] / [-0.091, 0.066] | -0.038 [-0.250, 0.182] / [-0.114, 0.042] |
| 5 | -0.900 [-1.092, -0.708]* / [-0.972, -0.833]* | -0.384 [-0.574, -0.197]* / [-0.463, -0.301]* | -0.958 [-1.151, -0.768]* / [-1.029, -0.891]* | -0.778 [-0.951, -0.615]* / [-0.840, -0.712]* | -0.542 [-0.713, -0.369]* / [-0.608, -0.468]* | 1.819 [1.410, 2.242] / [1.708, 1.933] | 0.078 [-0.117, 0.289] / [0.001, 0.158] | 0.055 [-0.137, 0.267] / [-0.022, 0.135] |
| 6 | -0.832 [-1.028, -0.636]* / [-0.903, -0.760]* | -0.341 [-0.531, -0.151]* / [-0.418, -0.257]* | -0.893 [-1.090, -0.702]* / [-0.964, -0.821]* | -0.750 [-0.925, -0.589]* / [-0.812, -0.686]* | -0.462 [-0.656, -0.274]* / [-0.533, -0.384]* | 2.207 [1.755, 2.657] / [2.090, 2.332] | 0.115 [-0.106, 0.341] / [0.036, 0.195] | 0.093 [-0.129, 0.319] / [0.014, 0.173] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.061 [0.874, 1.250] / [0.994, 1.129] | 0.522 [0.434, 0.626] / [0.493, 0.553] | -0.067 [-0.085, -0.052]* / [-0.072, -0.062]* | 0.248 [0.193, 0.306] / [0.218, 0.277] | 0.089 [-0.042, 0.215] / [0.041, 0.136] | 2.596 [2.213, 3.053] / [2.511, 2.687] |  |  |
| 2 | 1.016 [0.826, 1.203] / [0.950, 1.083] | 0.503 [0.415, 0.609] / [0.475, 0.534] | -0.070 [-0.088, -0.054]* / [-0.074, -0.065]* | 0.214 [0.156, 0.273] / [0.185, 0.244] | 0.123 [-0.002, 0.247] / [0.075, 0.170] | 2.583 [2.201, 3.039] / [2.498, 2.673] |  |  |
| 3 | 0.976 [0.788, 1.162] / [0.910, 1.047] | 0.490 [0.402, 0.597] / [0.461, 0.521] | -0.072 [-0.090, -0.057]* / [-0.077, -0.068]* | 0.186 [0.126, 0.245] / [0.156, 0.216] | 0.178 [0.051, 0.300] / [0.128, 0.227] | 2.551 [2.164, 3.003] / [2.465, 2.638] | 0.728 [0.525, 0.935] / [0.665, 0.793] | 0.705 [0.502, 0.912] / [0.643, 0.771] |
| 4 | 0.938 [0.749, 1.126] / [0.869, 1.007] | 0.479 [0.390, 0.593] / [0.449, 0.511] | -0.073 [-0.090, -0.058]* / [-0.078, -0.068]* | 0.160 [0.097, 0.219] / [0.130, 0.191] | 0.190 [0.060, 0.313] / [0.141, 0.236] | 2.535 [2.140, 3.003] / [2.450, 2.626] | 0.767 [0.565, 0.971] / [0.697, 0.830] | 0.744 [0.542, 0.947] / [0.675, 0.808] |
| 5 | 0.900 [0.708, 1.092] / [0.833, 0.972] | 0.471 [0.376, 0.588] / [0.440, 0.504] | -0.074 [-0.091, -0.059]* / [-0.079, -0.070]* | 0.134 [0.069, 0.195] / [0.105, 0.166] | 0.229 [0.100, 0.356] / [0.181, 0.279] | 2.515 [2.111, 2.978] / [2.423, 2.606] | 0.819 [0.613, 1.033] / [0.750, 0.887] | 0.797 [0.588, 1.011] / [0.728, 0.865] |
| 6 | 0.832 [0.636, 1.028] / [0.760, 0.903] | 0.443 [0.350, 0.561] / [0.411, 0.474] | -0.076 [-0.093, -0.061]* / [-0.081, -0.072]* | 0.095 [0.026, 0.161] / [0.065, 0.129] | 0.241 [0.107, 0.368] / [0.191, 0.290] | 2.800 [2.380, 3.259] / [2.707, 2.892] | 0.791 [0.595, 1.007] / [0.722, 0.862] | 0.771 [0.576, 0.984] / [0.701, 0.842] |
| 7 |  | 0.373 [0.283, 0.489] / [0.341, 0.404] | -0.080 [-0.098, -0.064]* / [-0.085, -0.076]* | 0.343 [0.282, 0.407] / [0.315, 0.375] | 0.324 [0.195, 0.454] / [0.274, 0.376] | 2.849 [2.421, 3.321] / [2.756, 2.942] | 0.890 [0.674, 1.112] / [0.821, 0.967] | 0.870 [0.654, 1.091] / [0.801, 0.947] |
| 10 |  | 0.323 [0.230, 0.436] / [0.292, 0.354] | -0.084 [-0.102, -0.068]* / [-0.089, -0.079]* | 0.260 [0.194, 0.328] / [0.231, 0.293] | 0.375 [0.236, 0.519] / [0.317, 0.430] | 2.837 [2.362, 3.362] / [2.743, 2.935] | 0.961 [0.728, 1.210] / [0.889, 1.037] | 0.940 [0.706, 1.188] / [0.867, 1.016] |
| 14 |  | 0.256 [0.159, 0.375] / [0.224, 0.289] | -0.088 [-0.106, -0.071]* / [-0.093, -0.083]* | 0.384 [0.322, 0.451] / [0.353, 0.416] | 0.447 [0.285, 0.607] / [0.393, 0.506] | 2.778 [2.285, 3.297] / [2.680, 2.873] | 1.037 [0.763, 1.314] / [0.966, 1.117] | 1.016 [0.743, 1.288] / [0.945, 1.095] |
| 21 |  | 0.192 [0.084, 0.326] / [0.159, 0.226] | -0.095 [-0.116, -0.078]* / [-0.100, -0.090]* | 0.405 [0.347, 0.461] / [0.374, 0.436] | 0.430 [0.282, 0.581] / [0.375, 0.484] | 2.679 [2.166, 3.198] / [2.579, 2.785] | 1.037 [0.765, 1.337] / [0.961, 1.119] | 1.016 [0.746, 1.315] / [0.939, 1.097] |
| 30 |  | 0.157 [0.036, 0.307] / [0.124, 0.192] | -0.099 [-0.120, -0.081]* / [-0.104, -0.094]* | 0.378 [0.317, 0.436] / [0.346, 0.413] | 0.461 [0.317, 0.605] / [0.395, 0.527] | 2.618 [2.112, 3.132] / [2.513, 2.733] | 1.109 [0.838, 1.394] / [1.014, 1.207] | 1.085 [0.815, 1.369] / [0.992, 1.183] |
| 45 |  | 0.134 [-0.004, 0.308] / [0.097, 0.171] | -0.104 [-0.128, -0.085]* / [-0.109, -0.099]* | 0.417 [0.335, 0.493] / [0.381, 0.455] | 0.487 [0.309, 0.649] / [0.405, 0.569] | 1.835 [1.394, 2.293] / [1.667, 2.003] | 1.308 [0.999, 1.640] / [1.190, 1.420] | 1.282 [0.971, 1.617] / [1.165, 1.393] |
| 60 |  | 0.072 [-0.112, 0.288] / [0.031, 0.113] | -0.104 [-0.130, -0.084]* / [-0.109, -0.098]* | 0.470 [0.377, 0.561] / [0.430, 0.508] | 0.050 [-1.170, 1.446] / [-0.389, 0.506] |  | -0.102 [-1.215, 0.715] / [-0.687, 0.452] | -0.101 [-1.217, 0.727] / [-0.690, 0.455] |
| 90 |  | 0.195 [-0.013, 0.435] / [0.152, 0.236] | -0.096 [-0.120, -0.075]* / [-0.102, -0.090]* | 0.540 [0.427, 0.670] / [0.494, 0.589] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.503 [0.321, 0.692] / [0.425, 0.577] | -0.522 [-0.626, -0.434]* / [-0.553, -0.493]* | -0.586 [-0.693, -0.494]* / [-0.616, -0.556]* | -0.274 [-0.384, -0.173]* / [-0.318, -0.232]* | -0.466 [-0.624, -0.315]* / [-0.523, -0.408]* | 2.069 [1.669, 2.493] / [1.986, 2.159] |  |  |
| 2 | 0.476 [0.289, 0.664] / [0.396, 0.551] | -0.503 [-0.609, -0.415]* / [-0.534, -0.475]* | -0.569 [-0.677, -0.476]* / [-0.599, -0.540]* | -0.291 [-0.402, -0.186]* / [-0.334, -0.250]* | -0.426 [-0.583, -0.277]* / [-0.485, -0.370]* | 2.058 [1.665, 2.480] / [1.971, 2.154] |  |  |
| 3 | 0.448 [0.262, 0.635] / [0.368, 0.523] | -0.490 [-0.597, -0.402]* / [-0.521, -0.461]* | -0.558 [-0.668, -0.464]* / [-0.588, -0.530]* | -0.306 [-0.418, -0.199]* / [-0.352, -0.264]* | -0.366 [-0.515, -0.228]* / [-0.427, -0.307]* | 2.043 [1.632, 2.465] / [1.957, 2.136] | 0.222 [0.035, 0.420] / [0.151, 0.293] | 0.200 [0.011, 0.400] / [0.129, 0.271] |
| 4 | 0.418 [0.230, 0.607] / [0.335, 0.493] | -0.479 [-0.593, -0.390]* / [-0.511, -0.449]* | -0.547 [-0.663, -0.450]* / [-0.579, -0.518]* | -0.322 [-0.441, -0.210]* / [-0.369, -0.279]* | -0.355 [-0.509, -0.205]* / [-0.415, -0.297]* | 2.029 [1.610, 2.466] / [1.940, 2.122] | 0.260 [0.063, 0.460] / [0.188, 0.335] | 0.238 [0.041, 0.438] / [0.165, 0.312] |
| 5 | 0.384 [0.197, 0.574] / [0.301, 0.463] | -0.471 [-0.588, -0.376]* / [-0.504, -0.440]* | -0.541 [-0.661, -0.440]* / [-0.572, -0.508]* | -0.343 [-0.470, -0.225]* / [-0.389, -0.298]* | -0.327 [-0.478, -0.183]* / [-0.388, -0.265]* | 2.008 [1.578, 2.442] / [1.914, 2.106] | 0.303 [0.103, 0.507] / [0.231, 0.378] | 0.282 [0.083, 0.483] / [0.207, 0.357] |
| 6 | 0.341 [0.151, 0.531] / [0.257, 0.418] | -0.443 [-0.561, -0.350]* / [-0.474, -0.411]* | -0.514 [-0.639, -0.416]* / [-0.544, -0.483]* | -0.359 [-0.494, -0.235]* / [-0.405, -0.314]* | -0.302 [-0.450, -0.153]* / [-0.365, -0.239]* | 2.295 [1.863, 2.727] / [2.203, 2.389] | 0.294 [0.097, 0.491] / [0.222, 0.371] | 0.273 [0.078, 0.469] / [0.201, 0.350] |
| 7 |  | -0.373 [-0.489, -0.283]* / [-0.404, -0.341]* | -0.448 [-0.568, -0.354]* / [-0.477, -0.417]* | -0.027 [-0.150, 0.089] / [-0.070, 0.018] | -0.144 [-0.300, -0.004]* / [-0.206, -0.084]* | 2.429 [1.984, 2.896] / [2.335, 2.526] | 0.465 [0.254, 0.677] / [0.387, 0.552] | 0.445 [0.234, 0.655] / [0.367, 0.531] |
| 10 |  | -0.323 [-0.436, -0.230]* / [-0.354, -0.292]* | -0.402 [-0.521, -0.304]* / [-0.432, -0.371]* | -0.064 [-0.189, 0.060] / [-0.112, -0.016]*  ! | -0.030 [-0.181, 0.120] / [-0.090, 0.031] | 2.477 [1.988, 2.988] / [2.375, 2.579] | 0.594 [0.366, 0.828] / [0.514, 0.672] | 0.572 [0.343, 0.804] / [0.492, 0.650] |
| 14 |  | -0.256 [-0.375, -0.159]* / [-0.289, -0.224]* | -0.340 [-0.462, -0.243]* / [-0.371, -0.307]* | 0.129 [0.002, 0.252] / [0.084, 0.177] | 0.137 [-0.027, 0.307] / [0.079, 0.202] | 2.511 [2.011, 3.036] / [2.410, 2.618] | 0.764 [0.516, 1.018] / [0.687, 0.848] | 0.742 [0.495, 0.998] / [0.664, 0.827] |
| 21 |  | -0.192 [-0.326, -0.084]* / [-0.226, -0.159]* | -0.283 [-0.422, -0.173]* / [-0.317, -0.252]* | 0.231 [0.093, 0.350] / [0.183, 0.282] | 0.244 [0.068, 0.405] / [0.179, 0.311] | 2.520 [1.988, 3.066] / [2.409, 2.628] | 0.876 [0.620, 1.161] / [0.795, 0.963] | 0.854 [0.597, 1.141] / [0.773, 0.940] |
| 30 |  | -0.157 [-0.307, -0.036]* / [-0.192, -0.124]* | -0.252 [-0.408, -0.126]* / [-0.288, -0.219]* | 0.242 [0.080, 0.382] / [0.189, 0.291] | 0.411 [0.223, 0.600] / [0.332, 0.491] | 2.542 [2.030, 3.090] / [2.425, 2.659] | 1.057 [0.808, 1.317] / [0.951, 1.158] | 1.033 [0.783, 1.296] / [0.928, 1.134] |
| 45 |  | -0.134 [-0.308, 0.004] / [-0.171, -0.097]*  ! | -0.232 [-0.413, -0.086]* / [-0.269, -0.194]* | 0.297 [0.103, 0.464] / [0.242, 0.353] | 0.509 [0.250, 0.764] / [0.410, 0.605] | 1.292 [0.687, 1.907] / [1.081, 1.473] | 1.303 [0.976, 1.639] / [1.180, 1.421] | 1.277 [0.946, 1.616] / [1.155, 1.395] |
| 60 |  | -0.072 [-0.288, 0.112] / [-0.113, -0.031]*  ! | -0.165 [-0.394, 0.023] / [-0.208, -0.124]*  ! | 0.403 [0.154, 0.617] / [0.342, 0.463] | 0.464 [-0.325, 2.059] / [-0.105, 1.049] |  | 0.334 [-0.728, 1.601] / [-0.332, 1.009] | 0.335 [-0.731, 1.594] / [-0.332, 1.012] |
| 90 |  | -0.195 [-0.435, 0.013] / [-0.236, -0.152]*  ! | -0.282 [-0.530, -0.065]* / [-0.323, -0.239]* | 0.344 [0.036, 0.647] / [0.277, 0.416] |  |  |  |  |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 15.62 [15.47, 15.78] | 14.55 [14.42, 14.69] | 18.68 [18.39, 18.96] | 14.71 [14.57, 14.86] | 13.90 [13.75, 14.06] | 9.68 [9.61, 9.75] | 11.66 [11.51, 11.83] |  |  | 16.61 [16.43, 16.80] | 14.73 [14.59, 14.87] |
| 2 | 15.63 [15.48, 15.79] | 14.60 [14.47, 14.74] | 18.74 [18.45, 19.03] | 14.70 [14.56, 14.85] | 14.04 [13.89, 14.20] | 9.68 [9.61, 9.75] | 11.64 [11.48, 11.81] |  |  | 16.59 [16.41, 16.78] | 14.79 [14.66, 14.93] |
| 3 | 15.64 [15.49, 15.80] | 14.65 [14.51, 14.79] | 18.77 [18.50, 19.06] | 14.69 [14.56, 14.85] | 14.24 [14.08, 14.41] | 9.67 [9.60, 9.75] | 11.62 [11.47, 11.78] | 15.30 [15.14, 15.48] | 15.25 [15.08, 15.42] | 16.57 [16.38, 16.75] | 14.84 [14.71, 14.98] |
| 4 | 15.64 [15.50, 15.80] | 14.68 [14.56, 14.83] | 18.79 [18.53, 19.09] | 14.69 [14.55, 14.83] | 14.33 [14.16, 14.49] | 9.68 [9.61, 9.75] | 11.60 [11.44, 11.76] | 15.43 [15.26, 15.60] | 15.37 [15.20, 15.54] | 16.54 [16.35, 16.73] | 14.88 [14.75, 15.03] |
| 5 | 15.67 [15.52, 15.83] | 14.74 [14.61, 14.88] | 18.86 [18.57, 19.17] | 14.69 [14.55, 14.84] | 14.51 [14.35, 14.68] | 9.69 [9.61, 9.76] | 11.59 [11.44, 11.76] | 15.64 [15.47, 15.82] | 15.58 [15.42, 15.77] | 16.52 [16.32, 16.71] | 14.94 [14.81, 15.09] |
| 6 | 15.72 [15.56, 15.88] | 14.84 [14.71, 14.99] | 19.49 [19.21, 19.79] | 14.73 [14.59, 14.87] | 14.62 [14.45, 14.80] | 9.72 [9.64, 9.80] | 11.58 [11.43, 11.75] | 15.67 [15.51, 15.86] | 15.62 [15.46, 15.81] | 16.49 [16.30, 16.67] | 15.04 [14.92, 15.20] |
| 7 | 15.76 [15.60, 15.92] | 15.03 [14.89, 15.19] | 19.77 [19.47, 20.08] | 15.38 [15.24, 15.54] | 14.96 [14.80, 15.15] | 9.77 [9.70, 9.85] | 12.44 [12.28, 12.62] | 16.09 [15.92, 16.29] | 16.04 [15.86, 16.23] |  | 15.24 [15.11, 15.40] |
| 10 | 15.85 [15.69, 16.01] | 15.21 [15.07, 15.37] | 20.07 [19.77, 20.39] | 15.42 [15.28, 15.59] | 15.30 [15.13, 15.50] | 9.80 [9.72, 9.88] | 12.42 [12.26, 12.60] | 16.53 [16.34, 16.75] | 16.47 [16.28, 16.69] |  | 15.43 [15.29, 15.59] |
| 14 | 15.97 [15.81, 16.14] | 15.48 [15.34, 15.64] | 20.27 [19.94, 20.60] | 15.89 [15.73, 16.06] | 15.78 [15.59, 15.99] | 9.89 [9.82, 9.97] | 12.94 [12.77, 13.13] | 17.04 [16.84, 17.25] | 16.98 [16.79, 17.19] |  | 15.71 [15.57, 15.87] |
| 21 | 16.16 [16.00, 16.33] | 15.75 [15.60, 15.90] | 20.52 [20.18, 20.86] | 16.23 [16.06, 16.40] | 16.15 [15.96, 16.36] | 9.95 [9.87, 10.04] | 13.30 [13.13, 13.49] | 17.52 [17.32, 17.75] | 17.46 [17.26, 17.69] |  | 16.00 [15.85, 16.16] |
| 30 | 16.33 [16.16, 16.50] | 15.95 [15.80, 16.11] | 20.89 [20.50, 21.26] | 16.37 [16.20, 16.53] | 16.87 [16.63, 17.13] | 9.90 [9.83, 9.98] | 13.45 [13.27, 13.64] | 18.32 [18.06, 18.58] | 18.25 [18.00, 18.51] |  | 16.21 [16.05, 16.38] |
| 45 | 16.52 [16.35, 16.69] | 16.19 [16.02, 16.36] | 17.96 [17.54, 18.38] | 16.61 [16.43, 16.78] | 17.51 [17.19, 17.84] | 9.92 [9.84, 10.00] | 13.71 [13.52, 13.90] | 19.30 [18.98, 19.61] | 19.23 [18.91, 19.55] |  | 16.46 [16.29, 16.64] |
| 60 | 16.67 [16.49, 16.85] | 16.53 [16.35, 16.70] |  | 16.92 [16.73, 17.11] | 19.64 [14.31, 24.24] | 9.97 [9.89, 10.07] | 14.09 [13.88, 14.31] | 20.74 [16.57, 24.00] | 20.71 [16.51, 24.00] |  | 16.79 [16.60, 16.96] |
| 90 | 16.98 [16.77, 17.20] | 16.72 [16.51, 16.92] |  | 17.24 [17.02, 17.46] |  | 9.99 [9.90, 10.09] | 14.38 [14.14, 14.63] |  |  |  | 16.96 [16.75, 17.17] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.01 [0.79, 1.25] | -0.17 [-0.22, -0.14]* | 4.37 [3.36, 5.50] | 0.07 [-0.05, 0.20] | -0.49 [-0.80, -0.21]* | -5.07 [-5.62, -4.53]* | -2.97 [-3.20, -2.72]* |  |  | 1.93 [1.54, 2.31] | wt_med |
| 2 | 0.96 [0.74, 1.21] | -0.18 [-0.22, -0.15]* | 4.36 [3.34, 5.48] | 0.00 [-0.12, 0.13] | -0.41 [-0.70, -0.15]* | -5.13 [-5.69, -4.59]* | -3.05 [-3.29, -2.80]* |  |  | 1.84 [1.45, 2.23] | wt_med |
| 3 | 0.92 [0.70, 1.18] | -0.19 [-0.23, -0.15]* | 4.31 [3.29, 5.43] | -0.05 [-0.19, 0.08] | -0.27 [-0.55, 0.00] | -5.18 [-5.74, -4.66]* | -3.13 [-3.37, -2.87]* | 0.84 [0.42, 1.23] | 0.79 [0.36, 1.18] | 1.76 [1.35, 2.16] | wt_med |
| 4 | 0.89 [0.66, 1.17] | -0.19 [-0.23, -0.16]* | 4.29 [3.24, 5.45] | -0.11 [-0.24, 0.03] | -0.25 [-0.54, 0.02] | -5.22 [-5.78, -4.69]* | -3.19 [-3.43, -2.94]* | 0.92 [0.52, 1.31] | 0.86 [0.45, 1.25] | 1.69 [1.27, 2.09] | wt_med |
| 5 | 0.87 [0.63, 1.16] | -0.19 [-0.23, -0.16]* | 4.26 [3.20, 5.40] | -0.16 [-0.30, -0.02]* | -0.12 [-0.43, 0.15] | -5.27 [-5.83, -4.73]* | -3.25 [-3.50, -2.98]* | 1.09 [0.67, 1.50] | 1.04 [0.61, 1.44] | 1.61 [1.20, 2.01] | wt_med |
| 6 | 0.82 [0.57, 1.12] | -0.20 [-0.24, -0.16]* | 4.77 [3.66, 5.93] | -0.23 [-0.39, -0.08]* | -0.11 [-0.40, 0.16] | -5.34 [-5.91, -4.80]* | -3.37 [-3.62, -3.09]* | 1.03 [0.63, 1.42] | 0.98 [0.58, 1.38] | 1.47 [1.04, 1.88] | wt_med |
| 7 | 0.68 [0.44, 0.97] | -0.21 [-0.25, -0.17]* | 4.87 [3.74, 6.00] | 0.31 [0.17, 0.47] | 0.03 [-0.27, 0.32] | -5.49 [-6.07, -4.94]* | -2.62 [-2.89, -2.35]* | 1.26 [0.83, 1.68] | 1.20 [0.77, 1.62] |  | wt_med |
| 10 | 0.59 [0.35, 0.88] | -0.21 [-0.26, -0.18]* | 4.93 [3.68, 6.22] | 0.15 [-0.02, 0.32] | 0.18 [-0.12, 0.45] | -5.65 [-6.25, -5.08]* | -2.85 [-3.12, -2.55]* | 1.51 [1.06, 1.95] | 1.45 [1.00, 1.89] |  | wt_med |
| 14 | 0.47 [0.22, 0.76] | -0.22 [-0.27, -0.18]* | 4.83 [3.54, 6.13] | 0.41 [0.25, 0.57] | 0.34 [-0.00, 0.65] | -5.84 [-6.47, -5.25]* | -2.53 [-2.83, -2.23]* | 1.70 [1.19, 2.19] | 1.64 [1.13, 2.13] |  | wt_med |
| 21 | 0.35 [0.08, 0.67] | -0.23 [-0.29, -0.19]* | 4.66 [3.27, 6.01] | 0.45 [0.32, 0.57] | 0.27 [-0.03, 0.56] | -6.07 [-6.70, -5.45]* | -2.47 [-2.74, -2.18]* | 1.71 [1.20, 2.25] | 1.65 [1.14, 2.19] |  | wt_med |
| 30 | 0.29 [-0.02, 0.65] | -0.24 [-0.30, -0.20]* | 4.51 [3.14, 5.85] | 0.38 [0.23, 0.52] | 0.42 [0.12, 0.71] | -6.34 [-6.99, -5.71]* | -2.53 [-2.82, -2.23]* | 1.92 [1.38, 2.45] | 1.85 [1.31, 2.39] |  | wt_med |
| 45 | 0.30 [-0.06, 0.72] | -0.25 [-0.31, -0.21]* | 2.39 [1.43, 3.33] | 0.43 [0.24, 0.61] | 0.41 [0.01, 0.78] | -6.58 [-7.29, -5.92]* | -2.47 [-2.80, -2.11]* | 2.39 [1.76, 3.00] | 2.32 [1.68, 2.95] |  | wt_med |
| 60 | 0.20 [-0.26, 0.74] | -0.25 [-0.31, -0.20] |  | 0.54 [0.33, 0.75] | 2.03 [-1.27, 5.81] | -6.84 [-7.67, -6.10]* | -2.28 [-2.66, -1.85]* | 3.07 [2.21, 7.13] | 3.04 [2.14, 6.98] |  | wt_med |
| 90 | 0.34 [-0.15, 0.91] | -0.22 [-0.28, -0.17]* |  | 0.72 [0.51, 0.93] |  | -6.99 [-8.02, -6.02]* | -2.11 [-2.53, -1.69]* |  |  |  | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -1.928 [-2.307, -1.538]* / [-2.083, -1.771]* | -0.852 [-1.291, -0.429]* / [-1.022, -0.670]* | -2.076 [-2.461, -1.681]* / [-2.229, -1.919]* | -1.852 [-2.160, -1.534]* / [-1.995, -1.697]* | -2.221 [-2.519, -1.907]* / [-2.392, -2.044]* | 2.901 [1.805, 3.938] / [2.590, 3.213] |  |  |
| 2 | -1.845 [-2.234, -1.450]* / [-1.995, -1.683]* | -0.816 [-1.253, -0.395]* / [-0.984, -0.629]* | -1.998 [-2.386, -1.600]* / [-2.150, -1.835]* | -1.843 [-2.145, -1.526]* / [-1.989, -1.689]* | -2.064 [-2.378, -1.718]* / [-2.234, -1.882]* | 2.903 [1.798, 3.924] / [2.592, 3.204] |  |  |
| 3 | -1.764 [-2.159, -1.354]* / [-1.916, -1.598]* | -0.776 [-1.215, -0.340]* / [-0.945, -0.582]* | -1.923 [-2.319, -1.511]* / [-2.076, -1.759]* | -1.825 [-2.129, -1.500]* / [-1.970, -1.678]* | -1.807 [-2.145, -1.448]* / [-1.978, -1.625]* | 2.943 [1.843, 3.966] / [2.643, 3.260] | -0.648 [-1.077, -0.177]* / [-0.840, -0.455]* | -0.709 [-1.136, -0.239]* / [-0.902, -0.515]* |
| 4 | -1.691 [-2.086, -1.274]* / [-1.845, -1.526]* | -0.723 [-1.168, -0.272]* / [-0.892, -0.535]* | -1.850 [-2.250, -1.425]* / [-2.003, -1.683]* | -1.803 [-2.106, -1.474]* / [-1.944, -1.655]* | -1.724 [-2.045, -1.389]* / [-1.892, -1.548]* | 2.964 [1.840, 4.021] / [2.655, 3.266] | -0.523 [-0.968, -0.061]* / [-0.708, -0.327]* | -0.584 [-1.027, -0.119]* / [-0.768, -0.388]* |
| 5 | -1.611 [-2.012, -1.195]* / [-1.762, -1.447]* | -0.660 [-1.115, -0.193]* / [-0.835, -0.463]* | -1.773 [-2.176, -1.342]* / [-1.927, -1.605]* | -1.777 [-2.078, -1.444]* / [-1.916, -1.628]* | -1.520 [-1.878, -1.161]* / [-1.684, -1.352]* | 2.968 [1.828, 4.020] / [2.649, 3.270] | -0.267 [-0.705, 0.196] / [-0.452, -0.077]*  ! | -0.326 [-0.769, 0.134] / [-0.511, -0.137]*  ! |
| 6 | -1.471 [-1.882, -1.039]* / [-1.633, -1.299]* | -0.565 [-1.018, -0.104]* / [-0.745, -0.370]* | -1.637 [-2.054, -1.199]* / [-1.799, -1.467]* | -1.713 [-2.021, -1.377]* / [-1.858, -1.562]* | -1.392 [-1.769, -0.993]* / [-1.568, -1.211]* | 3.674 [2.434, 4.781] / [3.351, 3.991] | -0.206 [-0.667, 0.275] / [-0.391, -0.014]*  ! | -0.263 [-0.731, 0.222] / [-0.449, -0.071]*  ! |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.928 [1.538, 2.307] / [1.771, 2.083] | 1.006 [0.789, 1.249] / [0.937, 1.079] | -0.175 [-0.218, -0.142]* / [-0.186, -0.163]* | 0.075 [-0.046, 0.197] / [0.004, 0.145] | -0.495 [-0.796, -0.207]* / [-0.613, -0.382]* | 4.371 [3.362, 5.504] / [4.142, 4.603] |  |  |
| 2 | 1.845 [1.450, 2.234] / [1.683, 1.995] | 0.957 [0.739, 1.214] / [0.888, 1.029] | -0.182 [-0.224, -0.148]* / [-0.193, -0.170]* | 0.005 [-0.121, 0.130] / [-0.067, 0.075] | -0.415 [-0.699, -0.149]* / [-0.536, -0.304]* | 4.358 [3.337, 5.484] / [4.120, 4.605] |  |  |
| 3 | 1.764 [1.354, 2.159] / [1.598, 1.916] | 0.920 [0.696, 1.182] / [0.853, 0.992] | -0.188 [-0.231, -0.155]* / [-0.199, -0.176]* | -0.055 [-0.186, 0.075] / [-0.128, 0.017] | -0.267 [-0.553, 0.002] / [-0.392, -0.150]*  ! | 4.305 [3.286, 5.426] / [4.072, 4.559] | 0.843 [0.422, 1.232] / [0.697, 0.990] | 0.785 [0.358, 1.184] / [0.639, 0.933] |
| 4 | 1.691 [1.274, 2.086] / [1.526, 1.845] | 0.893 [0.661, 1.166] / [0.818, 0.968] | -0.190 [-0.232, -0.156]* / [-0.202, -0.178]* | -0.106 [-0.241, 0.029] / [-0.182, -0.033]*  ! | -0.248 [-0.537, 0.017] / [-0.370, -0.131]*  ! | 4.290 [3.244, 5.450] / [4.047, 4.541] | 0.917 [0.516, 1.306] / [0.763, 1.072] | 0.860 [0.454, 1.250] / [0.704, 1.014] |
| 5 | 1.611 [1.195, 2.012] / [1.447, 1.762] | 0.871 [0.631, 1.161] / [0.793, 0.945] | -0.193 [-0.234, -0.160]* / [-0.204, -0.181]* | -0.157 [-0.298, -0.018]* / [-0.232, -0.081]* | -0.121 [-0.431, 0.154] / [-0.238, 0.000] | 4.258 [3.204, 5.402] / [4.005, 4.509] | 1.092 [0.672, 1.501] / [0.946, 1.248] | 1.035 [0.614, 1.442] / [0.888, 1.192] |
| 6 | 1.471 [1.039, 1.882] / [1.299, 1.633] | 0.819 [0.569, 1.117] / [0.743, 0.893] | -0.197 [-0.239, -0.164]* / [-0.209, -0.186]* | -0.232 [-0.389, -0.085]* / [-0.309, -0.153]* | -0.108 [-0.400, 0.163] / [-0.230, 0.015] | 4.768 [3.658, 5.927] / [4.522, 5.017] | 1.034 [0.633, 1.423] / [0.880, 1.200] | 0.980 [0.575, 1.377] / [0.827, 1.144] |
| 7 |  | 0.684 [0.443, 0.971] / [0.607, 0.757] | -0.206 [-0.250, -0.170]* / [-0.217, -0.194]* | 0.313 [0.174, 0.470] / [0.245, 0.383] | 0.033 [-0.271, 0.320] / [-0.093, 0.156] | 4.870 [3.737, 5.997] / [4.608, 5.124] | 1.256 [0.833, 1.675] / [1.102, 1.423] | 1.201 [0.775, 1.620] / [1.049, 1.367] |
| 10 |  | 0.593 [0.349, 0.878] / [0.516, 0.668] | -0.214 [-0.259, -0.178]* / [-0.226, -0.202]* | 0.146 [-0.017, 0.315] / [0.073, 0.224] | 0.176 [-0.122, 0.454] / [0.041, 0.308] | 4.927 [3.680, 6.225] / [4.657, 5.195] | 1.507 [1.063, 1.950] / [1.353, 1.680] | 1.449 [1.005, 1.892] / [1.294, 1.620] |
| 14 |  | 0.465 [0.216, 0.761] / [0.386, 0.543] | -0.219 [-0.268, -0.178]* / [-0.230, -0.207]* | 0.407 [0.248, 0.573] / [0.338, 0.480] | 0.341 [-0.004, 0.653] / [0.210, 0.475] | 4.832 [3.542, 6.135] / [4.576, 5.108] | 1.697 [1.191, 2.186] / [1.540, 1.864] | 1.640 [1.131, 2.131] / [1.482, 1.807] |
| 21 |  | 0.355 [0.080, 0.670] / [0.273, 0.438] | -0.234 [-0.286, -0.191]* / [-0.246, -0.222]* | 0.449 [0.316, 0.572] / [0.376, 0.525] | 0.269 [-0.031, 0.555] / [0.139, 0.400] | 4.659 [3.268, 6.007] / [4.365, 4.962] | 1.714 [1.202, 2.252] / [1.534, 1.895] | 1.655 [1.138, 2.187] / [1.475, 1.835] |
| 30 |  | 0.292 [-0.018, 0.645] / [0.211, 0.379] | -0.243 [-0.298, -0.199]* / [-0.256, -0.231]* | 0.380 [0.231, 0.523] / [0.299, 0.460] | 0.420 [0.116, 0.708] / [0.253, 0.583] | 4.507 [3.136, 5.846] / [4.189, 4.818] | 1.916 [1.375, 2.452] / [1.700, 2.145] | 1.851 [1.314, 2.386] / [1.635, 2.081] |
| 45 |  | 0.296 [-0.063, 0.723] / [0.204, 0.392] | -0.252 [-0.307, -0.205]* / [-0.265, -0.240]* | 0.429 [0.243, 0.609] / [0.343, 0.514] | 0.409 [0.010, 0.776] / [0.200, 0.612] | 2.392 [1.425, 3.325] / [2.051, 2.730] | 2.390 [1.755, 3.003] / [2.143, 2.639] | 2.323 [1.680, 2.946] / [2.078, 2.571] |
| 60 |  | 0.197 [-0.263, 0.736] / [0.097, 0.299] | -0.248 [-0.311, -0.197]* / [-0.260, -0.235]* | 0.542 [0.327, 0.753] / [0.447, 0.638] | 2.028 [-1.273, 5.812] / [-0.857, 4.433] |  | 3.067 [2.210, 7.131] / [0.913, 5.374] | 3.039 [2.142, 6.977] / [0.880, 5.348] |
| 90 |  | 0.337 [-0.149, 0.909] / [0.235, 0.437] | -0.220 [-0.280, -0.174]* / [-0.235, -0.207]* | 0.723 [0.510, 0.930] / [0.615, 0.842] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.852 [0.429, 1.291] / [0.670, 1.022] | -1.006 [-1.249, -0.789]* / [-1.079, -0.937]* | -1.173 [-1.410, -0.951]* / [-1.244, -1.103]* | -0.923 [-1.177, -0.698]* / [-1.024, -0.823]* | -1.592 [-1.915, -1.279]* / [-1.730, -1.449]* | 3.389 [2.286, 4.460] / [3.160, 3.644] |  |  |
| 2 | 0.816 [0.395, 1.253] / [0.629, 0.984] | -0.957 [-1.214, -0.739]* / [-1.029, -0.888]* | -1.130 [-1.372, -0.901]* / [-1.199, -1.063]* | -0.950 [-1.220, -0.717]* / [-1.054, -0.848]* | -1.496 [-1.811, -1.188]* / [-1.639, -1.363]* | 3.382 [2.291, 4.466] / [3.145, 3.643] |  |  |
| 3 | 0.776 [0.340, 1.215] / [0.582, 0.945] | -0.920 [-1.182, -0.696]* / [-0.992, -0.853]* | -1.097 [-1.355, -0.866]* / [-1.167, -1.029]* | -0.969 [-1.248, -0.725]* / [-1.076, -0.866]* | -1.326 [-1.623, -1.047]* / [-1.472, -1.178]* | 3.377 [2.271, 4.473] / [3.126, 3.650] | -0.119 [-0.503, 0.248] / [-0.286, 0.042] | -0.177 [-0.559, 0.195] / [-0.342, -0.016]*  ! |
| 4 | 0.723 [0.272, 1.168] / [0.535, 0.892] | -0.893 [-1.166, -0.661]* / [-0.968, -0.818]* | -1.071 [-1.338, -0.829]* / [-1.143, -0.997]* | -0.996 [-1.289, -0.738]* / [-1.108, -0.889]* | -1.311 [-1.623, -1.004]* / [-1.455, -1.171]* | 3.370 [2.233, 4.508] / [3.130, 3.647] | -0.046 [-0.438, 0.339] / [-0.218, 0.125] | -0.104 [-0.501, 0.286] / [-0.278, 0.066] |
| 5 | 0.660 [0.193, 1.115] / [0.463, 0.835] | -0.871 [-1.161, -0.631]* / [-0.945, -0.793]* | -1.052 [-1.335, -0.808]* / [-1.127, -0.976]* | -1.034 [-1.347, -0.759]* / [-1.150, -0.926]* | -1.211 [-1.528, -0.914]* / [-1.353, -1.067]* | 3.333 [2.178, 4.487] / [3.080, 3.608] | 0.107 [-0.298, 0.499] / [-0.065, 0.281] | 0.049 [-0.363, 0.440] / [-0.123, 0.222] |
| 6 | 0.565 [0.104, 1.018] / [0.370, 0.745] | -0.819 [-1.117, -0.569]* / [-0.893, -0.743]* | -1.004 [-1.300, -0.752]* / [-1.075, -0.930]* | -1.066 [-1.398, -0.768]* / [-1.181, -0.955]* | -1.190 [-1.497, -0.906]* / [-1.330, -1.033]* | 3.815 [2.621, 4.878] / [3.559, 4.091] | 0.075 [-0.286, 0.433] / [-0.090, 0.255] | 0.020 [-0.342, 0.378] / [-0.146, 0.200] |
| 7 |  | -0.684 [-0.971, -0.443]* / [-0.757, -0.607]* | -0.877 [-1.166, -0.627]* / [-0.947, -0.802]* | -0.353 [-0.667, -0.062]* / [-0.459, -0.240]* | -0.893 [-1.198, -0.608]* / [-1.033, -0.736]* | 4.101 [2.866, 5.237] / [3.842, 4.372] | 0.440 [0.048, 0.836] / [0.271, 0.625] | 0.385 [-0.013, 0.780] / [0.216, 0.569] |
| 10 |  | -0.593 [-0.878, -0.349]* / [-0.668, -0.516]* | -0.791 [-1.070, -0.544]* / [-0.861, -0.718]* | -0.428 [-0.746, -0.126]* / [-0.540, -0.309]* | -0.612 [-0.945, -0.311]* / [-0.768, -0.458]* | 4.263 [2.903, 5.591] / [3.990, 4.551] | 0.803 [0.400, 1.194] / [0.627, 0.990] | 0.745 [0.344, 1.142] / [0.568, 0.930] |
| 14 |  | -0.465 [-0.761, -0.216]* / [-0.543, -0.386]* | -0.670 [-0.967, -0.419]* / [-0.748, -0.590]* | -0.029 [-0.362, 0.279] / [-0.139, 0.090] | -0.266 [-0.630, 0.061] / [-0.408, -0.112]*  ! | 4.361 [2.976, 5.710] / [4.087, 4.667] | 1.178 [0.723, 1.648] / [1.007, 1.366] | 1.119 [0.664, 1.586] / [0.948, 1.307] |
| 21 |  | -0.355 [-0.670, -0.080]* / [-0.438, -0.273]* | -0.574 [-0.894, -0.302]* / [-0.659, -0.493]* | 0.148 [-0.182, 0.457] / [0.039, 0.266] | -0.088 [-0.503, 0.276] / [-0.238, 0.071] | 4.376 [2.909, 5.809] / [4.062, 4.681] | 1.410 [0.870, 1.959] / [1.219, 1.607] | 1.349 [0.804, 1.908] / [1.157, 1.547] |
| 30 |  | -0.292 [-0.645, 0.018] / [-0.379, -0.211]*  ! | -0.524 [-0.895, -0.206]* / [-0.609, -0.443]* | 0.136 [-0.259, 0.509] / [0.008, 0.259] | 0.332 [-0.162, 0.794] / [0.130, 0.535] | 4.360 [2.845, 5.715] / [4.035, 4.682] | 1.807 [1.211, 2.383] / [1.563, 2.056] | 1.742 [1.141, 2.320] / [1.496, 1.990] |
| 45 |  | -0.296 [-0.723, 0.063] / [-0.392, -0.204]*  ! | -0.533 [-0.982, -0.161]* / [-0.625, -0.443]* | 0.167 [-0.301, 0.592] / [0.031, 0.301] | 0.424 [-0.225, 1.083] / [0.171, 0.669] | 1.505 [0.169, 2.704] / [1.094, 1.874] | 2.317 [1.508, 3.105] / [2.036, 2.588] | 2.249 [1.440, 3.043] / [1.966, 2.521] |
| 60 |  | -0.197 [-0.736, 0.263] / [-0.299, -0.097]*  ! | -0.416 [-0.986, 0.050] / [-0.519, -0.318]*  ! | 0.354 [-0.268, 0.891] / [0.202, 0.496] | 6.877 [5.484, 10.454] / [4.526, 8.858] |  | 8.031 [4.623, 9.508] / [5.874, 10.351] | 7.999 [4.647, 9.355] / [5.851, 10.340] |
| 90 |  | -0.337 [-0.909, 0.149] / [-0.437, -0.235]*  ! | -0.529 [-1.108, -0.039]* / [-0.628, -0.425]* | 0.352 [-0.330, 0.984] / [0.196, 0.519] |  |  |  |  |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 9.25 | 8.71 | 13.68 | 9.04 | 8.83 | 5.79 | 7.75 |  |  | 9.74 | 8.76 |
| 1 | EU | 6.33 | 5.88 | 8.02 | 6.04 | 5.75 | 4.16 | 5.00 |  |  | 6.53 | 5.93 |
| 1 | NA | 9.14 | 8.60 | 10.33 | 8.92 | 8.51 | 6.50 | 7.32 |  |  | 10.31 | 8.70 |
| 3 | Asia | 9.22 | 8.77 | 13.74 | 9.05 | 9.07 | 5.79 | 7.73 | 9.15 | 9.11 | 9.74 | 8.83 |
| 3 | EU | 6.39 | 5.96 | 8.04 | 6.07 | 5.85 | 4.19 | 5.00 | 6.27 | 6.26 | 6.54 | 6.02 |
| 3 | NA | 9.22 | 8.66 | 10.40 | 8.94 | 8.68 | 6.50 | 7.32 | 9.50 | 9.49 | 10.31 | 8.77 |
| 7 | Asia | 9.21 | 9.01 | 14.25 | 9.44 | 9.51 | 5.87 | 8.24 | 9.49 | 9.46 |  | 9.08 |
| 7 | EU | 6.48 | 6.17 | 8.77 | 6.40 | 6.25 | 4.27 | 5.42 | 6.62 | 6.60 |  | 6.24 |
| 7 | NA | 9.39 | 8.88 | 10.81 | 9.28 | 9.02 | 6.56 | 7.77 | 9.95 | 9.93 |  | 9.00 |
| 14 | Asia | 9.31 | 9.28 | 14.85 | 9.70 | 10.13 | 5.96 | 8.50 | 10.19 | 10.15 |  | 9.34 |
| 14 | EU | 6.60 | 6.40 | 9.00 | 6.70 | 6.58 | 4.36 | 5.76 | 6.89 | 6.88 |  | 6.49 |
| 14 | NA | 9.63 | 9.16 | 10.96 | 9.57 | 9.43 | 6.65 | 8.11 | 10.43 | 10.42 |  | 9.30 |
| 30 | Asia | 9.46 | 9.48 | 15.28 | 10.04 | 10.83 | 6.00 | 8.94 | 11.14 | 11.09 |  | 9.55 |
| 30 | EU | 6.87 | 6.73 | 9.37 | 7.11 | 7.38 | 4.52 | 6.18 | 7.68 | 7.66 |  | 6.84 |
| 30 | NA | 9.95 | 9.56 | 11.22 | 9.85 | 9.81 | 6.67 | 8.40 | 10.97 | 10.95 |  | 9.74 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.09 | -1.29 | -1.26 | -1.56 | -1.05 | -1.20 | -5.61 |  |  | -0.49 | -0.01 |
| 1 | 0.10 | -1.28 | -1.21 | -1.56 | -1.04 | -1.28 | -5.65 |  |  | -0.49 | 0.00 |
| 2 | 0.10 | -1.26 | -1.16 | -1.54 | -1.02 | -1.28 | -5.64 |  |  | -0.50 | 0.02 |
| 3 | 0.10 | -1.24 | -1.14 | -1.52 | -1.01 | -1.26 | -5.66 | 1.49 | 1.48 | -0.50 | 0.03 |
| 4 | 0.10 | -1.22 | -1.13 | -1.51 | -0.99 | -1.23 | -5.67 | 1.56 | 1.55 | -0.49 | 0.04 |
| 5 | 0.10 | -1.21 | -1.10 | -1.49 | -0.99 | -1.23 | -5.70 | 1.57 | 1.56 | -0.49 | 0.04 |
| 6 | 0.10 | -1.18 | -1.07 | -1.47 | -0.97 | -1.35 | -6.12 | 1.43 | 1.42 | -0.48 | 0.05 |
| 7 |  | -1.17 | -1.04 | -1.46 | -0.93 | -1.30 | -6.07 | 1.44 | 1.43 | -0.48 | 0.14 |
| 10 |  | -1.15 | -0.97 | -1.44 | -0.91 | -1.20 | -6.19 | 1.55 | 1.54 | -0.48 | 0.14 |
| 14 |  | -1.13 | -0.98 | -1.42 | -0.86 | -1.13 | -6.26 | 1.65 | 1.64 | -0.46 | 0.21 |
| 21 |  | -1.06 | -0.91 | -1.35 | -0.80 | -1.02 | -6.31 | 1.77 | 1.76 | -0.43 | 0.26 |
| 30 |  | -0.91 | -0.68 | -1.21 | -0.68 | -1.20 | -6.50 | 1.53 | 1.52 | -0.44 | 0.39 |
| 45 |  | -0.64 | -0.25 | -0.94 | -0.52 | -0.77 | -5.44 | 1.66 | 1.65 | -0.40 | 0.51 |
| 60 |  | -0.40 | 0.11 | -0.70 | -0.41 | -1.62 |  | -0.76 | -0.76 | -0.38 | 0.61 |
| 90 |  | -0.00 | 0.58 | -0.31 | -0.10 |  |  |  |  | -0.38 | 0.91 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.59 [7.51, 7.68] | 7.52 [7.43, 7.61] | 7.89 [7.80, 7.98] | 7.44 [7.34, 7.54] |  |  |
| 1 | 7.71 [7.62, 7.80] | 7.63 [7.54, 7.72] | 7.92 [7.83, 8.01] | 7.58 [7.48, 7.69] |  |  |
| 2 | 7.75 [7.66, 7.84] | 7.67 [7.58, 7.76] | 7.93 [7.84, 8.02] | 7.64 [7.54, 7.74] |  |  |
| 3 | 7.78 [7.69, 7.87] | 7.69 [7.60, 7.78] | 7.93 [7.84, 8.02] | 7.71 [7.61, 7.81] | 8.09 [7.98, 8.19] | 8.06 [7.96, 8.17] |
| 4 | 7.79 [7.70, 7.88] | 7.70 [7.61, 7.79] | 7.91 [7.82, 8.01] | 7.74 [7.64, 7.85] | 8.15 [8.05, 8.26] | 8.12 [8.02, 8.23] |
| 5 | 7.80 [7.71, 7.89] | 7.70 [7.61, 7.79] | 7.89 [7.80, 7.98] | 7.81 [7.71, 7.92] | 8.22 [8.11, 8.33] | 8.20 [8.09, 8.31] |
| 6 | 7.82 [7.73, 7.91] | 7.72 [7.63, 7.81] | 7.88 [7.80, 7.98] | 7.86 [7.76, 7.97] | 8.22 [8.12, 8.34] | 8.20 [8.10, 8.32] |
| 7 | 7.91 [7.81, 8.00] | 7.81 [7.71, 7.89] | 8.16 [8.07, 8.26] | 8.00 [7.90, 8.11] | 8.36 [8.26, 8.48] | 8.34 [8.24, 8.46] |
| 10 | 7.96 [7.86, 8.05] | 7.84 [7.75, 7.94] | 8.12 [8.03, 8.22] | 8.11 [8.00, 8.22] | 8.48 [8.37, 8.60] | 8.45 [8.35, 8.58] |
| 14 | 8.02 [7.92, 8.12] | 7.90 [7.80, 8.00] | 8.25 [8.15, 8.35] | 8.27 [8.15, 8.39] | 8.65 [8.54, 8.78] | 8.63 [8.52, 8.76] |
| 21 | 8.08 [7.98, 8.18] | 7.94 [7.84, 8.03] | 8.35 [8.25, 8.46] | 8.35 [8.24, 8.48] | 8.74 [8.61, 8.87] | 8.72 [8.59, 8.85] |
| 30 | 8.17 [8.06, 8.28] | 8.00 [7.89, 8.11] | 8.39 [8.28, 8.50] | 8.62 [8.48, 8.77] | 9.14 [8.99, 9.30] | 9.12 [8.97, 9.28] |
| 45 | 8.05 [7.94, 8.17] | 7.87 [7.77, 7.99] | 8.34 [8.23, 8.46] | 8.64 [8.43, 8.84] | 9.48 [9.28, 9.66] | 9.46 [9.26, 9.64] |
| 60 | 8.13 [8.01, 8.26] | 7.95 [7.83, 8.08] | 8.45 [8.31, 8.59] | 3.30 [2.30, 4.56] | 3.47 [2.44, 4.74] | 3.46 [2.44, 4.74] |
| 90 | 7.27 [7.10, 7.44] | 7.08 [6.91, 7.24] | 7.61 [7.44, 7.79] |  |  |  |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.33 [7.19, 7.50] | 7.31 [7.16, 7.48] | 7.56 [7.41, 7.73] | 6.96 [6.79, 7.14] |  |  |
| 1 | 7.40 [7.26, 7.57] | 7.37 [7.23, 7.54] | 7.56 [7.41, 7.73] | 7.05 [6.88, 7.22] |  |  |
| 2 | 7.46 [7.32, 7.63] | 7.43 [7.29, 7.60] | 7.58 [7.43, 7.75] | 7.21 [7.04, 7.39] |  |  |
| 3 | 7.53 [7.38, 7.72] | 7.50 [7.35, 7.69] | 7.63 [7.48, 7.80] | 7.38 [7.21, 7.58] | 8.24 [8.05, 8.46] | 8.22 [8.03, 8.44] |
| 4 | 7.61 [7.47, 7.79] | 7.58 [7.43, 7.76] | 7.69 [7.54, 7.87] | 7.51 [7.34, 7.71] | 8.33 [8.14, 8.55] | 8.31 [8.12, 8.53] |
| 5 | 7.78 [7.62, 7.95] | 7.75 [7.59, 7.92] | 7.85 [7.69, 8.03] | 7.62 [7.45, 7.84] | 8.44 [8.24, 8.66] | 8.42 [8.22, 8.64] |
| 6 | 7.98 [7.81, 8.16] | 7.94 [7.78, 8.13] | 7.99 [7.83, 8.18] | 7.81 [7.62, 8.02] | 8.55 [8.35, 8.77] | 8.54 [8.33, 8.75] |
| 7 | 8.17 [8.00, 8.36] | 8.14 [7.97, 8.32] | 8.41 [8.24, 8.59] | 8.20 [8.00, 8.44] | 9.03 [8.79, 9.27] | 9.01 [8.78, 9.25] |
| 10 | 8.46 [8.29, 8.64] | 8.43 [8.26, 8.62] | 8.70 [8.52, 8.89] | 8.58 [8.37, 8.81] | 9.44 [9.21, 9.71] | 9.42 [9.19, 9.68] |
| 14 | 8.86 [8.69, 9.04] | 8.83 [8.65, 9.01] | 9.26 [9.09, 9.45] | 9.12 [8.90, 9.36] | 9.87 [9.64, 10.12] | 9.85 [9.62, 10.10] |
| 21 | 9.25 [9.08, 9.43] | 9.22 [9.05, 9.40] | 9.67 [9.49, 9.86] | 9.63 [9.41, 9.85] | 10.57 [10.34, 10.83] | 10.54 [10.32, 10.80] |
| 30 | 9.35 [9.20, 9.51] | 9.31 [9.15, 9.47] | 9.73 [9.56, 9.90] | 10.03 [9.79, 10.28] | 10.81 [10.56, 11.06] | 10.78 [10.53, 11.03] |
| 45 | 9.80 [9.65, 9.97] | 9.73 [9.58, 9.90] | 10.03 [9.86, 10.20] | 10.54 [10.27, 10.83] | 11.47 [11.19, 11.77] | 11.44 [11.16, 11.74] |
| 60 | 9.75 [9.61, 9.91] | 9.67 [9.53, 9.84] | 9.97 [9.83, 10.14] | 10.98 [6.02, 13.76] | 10.08 [5.51, 12.63] | 10.09 [5.51, 12.65] |
| 90 | 10.10 [9.95, 10.25] | 10.01 [9.86, 10.16] | 10.48 [10.32, 10.65] |  |  |  |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 1800199.00 | 8.26 | 8.18 | 8.08 | 7.98 | 8.16 | 7.63 | 9.66 |  |  | 5.11 | 7.17 |
| 0 | spring | 3863936.00 | 9.47 | 8.00 | 8.62 | 7.92 | 8.39 | 8.05 | 9.37 |  |  | 5.63 | 7.11 |
| 0 | summer | 6202500.00 | 8.26 | 7.06 | 7.69 | 7.03 | 7.36 | 7.25 | 10.05 |  |  | 5.31 | 6.00 |
| 0 | winter | 256346.00 | 10.59 | 8.39 | 9.88 | 8.43 | 8.99 |  |  |  |  | 6.17 | 7.49 |
| 1 | autumn | 1796586.00 | 8.26 | 8.29 | 8.15 | 8.09 | 8.20 | 7.79 | 9.65 |  |  | 5.12 | 7.16 |
| 1 | spring | 3835735.00 | 9.47 | 8.13 | 8.63 | 8.05 | 8.42 | 8.05 |  |  |  | 5.66 | 7.11 |
| 1 | summer | 6188203.00 | 8.26 | 7.15 | 7.72 | 7.12 | 7.39 | 7.38 | 10.15 |  |  | 5.34 | 6.00 |
| 1 | winter | 230254.00 | 10.67 | 8.50 | 9.73 | 8.53 | 8.93 |  |  |  |  | 6.15 | 7.38 |
| 3 | autumn | 1791629.00 | 8.26 | 8.38 | 8.19 | 8.17 | 8.22 | 7.93 | 9.74 | 8.37 | 8.36 | 5.13 | 7.16 |
| 3 | spring | 3796568.00 | 9.47 | 8.24 | 8.66 | 8.14 | 8.44 | 8.05 |  | 8.50 | 8.48 | 5.68 | 7.11 |
| 3 | summer | 6167419.00 | 8.26 | 7.23 | 7.79 | 7.19 | 7.41 | 7.56 | 10.17 | 8.02 | 7.99 | 5.36 | 6.00 |
| 3 | winter | 172421.00 | 10.89 | 8.28 | 9.50 | 8.30 | 8.67 |  |  |  |  | 5.88 | 7.20 |
| 7 | autumn | 1783013.00 |  | 8.61 | 8.30 | 8.40 | 8.76 | 8.35 | 10.35 | 8.93 | 8.91 | 5.19 | 7.89 |
| 7 | spring | 3708385.00 |  | 8.50 | 8.72 | 8.39 | 8.83 | 9.11 |  | 10.58 | 10.54 | 5.75 | 7.58 |
| 7 | summer | 6124268.00 |  | 7.46 | 7.93 | 7.41 | 7.70 | 7.94 | 10.73 | 8.35 | 8.32 | 5.45 | 6.39 |
| 7 | winter | 75587.00 |  | 7.98 | 8.83 | 7.98 | 8.50 |  |  |  |  | 5.78 | 7.05 |
| 14 | autumn | 1774797.00 |  | 8.86 | 8.30 | 8.65 | 9.21 | 8.83 | 10.60 | 9.53 | 9.51 | 5.23 | 8.46 |
| 14 | spring | 3459271.00 |  | 8.75 | 8.91 | 8.63 | 9.08 |  |  |  |  | 5.88 | 7.80 |
| 14 | summer | 6039659.00 |  | 7.74 | 8.13 | 7.68 | 7.97 | 8.33 | 10.98 | 8.72 | 8.70 | 5.54 | 6.68 |
| 30 | autumn | 1760630.00 |  | 8.95 | 8.24 | 8.75 | 9.53 | 9.44 | 10.59 | 10.59 | 10.56 | 5.28 | 8.88 |
| 30 | spring | 2774276.00 |  | 9.04 | 9.26 | 8.90 | 9.32 |  |  |  |  | 5.91 | 8.04 |
| 30 | summer | 5832721.00 |  | 8.21 | 8.48 | 8.10 | 8.39 | 8.83 | 11.52 | 9.15 | 9.14 | 5.66 | 7.11 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

**Spearman of the daily level within park-month** (paired on the same days)

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|
| 1 | 0.18 [0.15, 0.22] | 0.17 [0.14, 0.20] | 0.25 [0.23, 0.28] | 0.28 [0.26, 0.30] | 0.30 [0.27, 0.33] | 0.13 [0.11, 0.16] |
| 2 | 0.16 [0.13, 0.19] | 0.16 [0.14, 0.19] | 0.24 [0.21, 0.26] | 0.27 [0.25, 0.29] | 0.29 [0.26, 0.32] | 0.12 [0.09, 0.15] |
| 3 | 0.18 [0.15, 0.21] | 0.16 [0.12, 0.18] | 0.23 [0.20, 0.26] | 0.27 [0.24, 0.29] | 0.27 [0.25, 0.30] | 0.11 [0.08, 0.14] |
| 4 | 0.19 [0.16, 0.22] | 0.15 [0.12, 0.18] | 0.23 [0.20, 0.26] | 0.27 [0.24, 0.29] | 0.28 [0.25, 0.31] | 0.11 [0.08, 0.14] |
| 5 | 0.22 [0.19, 0.25] | 0.14 [0.11, 0.17] | 0.23 [0.21, 0.26] | 0.27 [0.25, 0.29] | 0.26 [0.23, 0.28] | 0.10 [0.07, 0.13] |
| 6 | 0.13 [0.10, 0.16] | 0.13 [0.10, 0.16] | 0.23 [0.20, 0.25] | 0.27 [0.25, 0.29] | 0.25 [0.22, 0.27] | 0.10 [0.07, 0.13] |
| 7 | 0.08 [0.05, 0.11] | 0.13 [0.10, 0.16] | 0.17 [0.14, 0.20] |  | 0.19 [0.16, 0.22] | 0.07 [0.04, 0.11] |
| 10 | 0.07 [0.04, 0.11] | 0.12 [0.09, 0.15] | 0.18 [0.15, 0.20] |  | 0.19 [0.16, 0.22] | 0.07 [0.04, 0.11] |
| 14 | 0.04 [0.00, 0.07] | 0.11 [0.08, 0.14] | 0.14 [0.12, 0.17] |  | 0.15 [0.12, 0.17] | 0.06 [0.03, 0.09] |
| 21 | 0.01 [-0.02, 0.05] | 0.12 [0.08, 0.15] | 0.13 [0.10, 0.16] |  | 0.12 [0.09, 0.15] | 0.06 [0.03, 0.09] |
| 30 | 0.01 [-0.03, 0.05] | 0.15 [0.12, 0.18] | 0.14 [0.11, 0.17] |  | 0.11 [0.07, 0.14] | 0.05 [0.01, 0.08] |
| 45 | 0.13 [0.06, 0.20] | 0.12 [0.08, 0.15] | 0.16 [0.13, 0.19] |  | 0.17 [0.13, 0.21] | 0.10 [0.07, 0.14] |
| 60 |  | 0.17 [0.13, 0.20] | 0.16 [0.13, 0.20] |  | 0.14 [0.14, 0.14] | 0.12 [0.08, 0.16] |
| 90 |  | 0.16 [0.11, 0.20] | 0.15 [0.11, 0.19] |  |  | 0.16 [0.12, 0.21] |
| 120 |  | 0.13 [0.07, 0.19] | 0.21 [0.15, 0.26] |  |  | 0.14 [0.08, 0.20] |
| 180 |  | 0.25 [0.15, 0.34] | 0.33 [0.25, 0.40] |  |  | 0.31 [0.20, 0.40] |

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|
| 1 | -0.10 [-0.13, -0.06] | -0.11 [-0.14, -0.08] | -0.02 [-0.05, -0.00] | -0.00 [-0.04, 0.03] | -0.14 [-0.17, -0.11] | lvl_snaive7 |
| 2 | -0.12 [-0.15, -0.08] | -0.11 [-0.14, -0.07] | -0.03 [-0.05, -0.01] | 0.00 [-0.03, 0.03] | -0.15 [-0.18, -0.12] | lvl_snaive7 |
| 3 | -0.10 [-0.13, -0.07] | -0.11 [-0.15, -0.08] | -0.03 [-0.06, -0.01] | -0.01 [-0.04, 0.02] | -0.16 [-0.19, -0.13] | lvl_snaive7 |
| 4 | -0.08 [-0.11, -0.05] | -0.12 [-0.15, -0.09] | -0.04 [-0.06, -0.01] | 0.00 [-0.03, 0.03] | -0.16 [-0.19, -0.13] | lvl_snaive7 |
| 5 | -0.05 [-0.08, -0.02] | -0.12 [-0.16, -0.09] | -0.04 [-0.06, -0.01] | -0.03 [-0.06, -0.00] | -0.17 [-0.20, -0.13] | lvl_snaive7 |
| 6 | -0.14 [-0.18, -0.10] | -0.13 [-0.17, -0.10] | -0.04 [-0.06, -0.01] | -0.04 [-0.06, -0.01] | -0.17 [-0.20, -0.14] | lvl_snaive7 |
| 7 | -0.10 [-0.13, -0.06] | -0.04 [-0.07, -0.01] |  | -0.01 [-0.04, 0.02] | -0.09 [-0.12, -0.07] | lvl_naive4 |
| 10 | -0.11 [-0.14, -0.07] | -0.05 [-0.08, -0.02] |  | -0.02 [-0.05, 0.01] | -0.10 [-0.12, -0.08] | lvl_naive4 |
| 14 | -0.08 [-0.12, -0.05] | -0.03 [-0.06, -0.00] |  | -0.01 [-0.04, 0.02] | -0.09 [-0.11, -0.06] | lvl_naive4 |
| 21 | -0.09 [-0.12, -0.05] | -0.01 [-0.04, 0.02] |  | -0.02 [-0.06, 0.01] | -0.07 [-0.10, -0.04] | lvl_naive4 |
| 30 | -0.10 [-0.14, -0.05] | 0.01 [-0.03, 0.04] |  | -0.04 [-0.08, 0.01] | -0.09 [-0.12, -0.07] | lvl_naive4 |
| 45 | -0.14 [-0.20, -0.08] | -0.04 [-0.09, 0.01] |  | 0.01 [-0.05, 0.06] | -0.06 [-0.09, -0.03] | lvl_naive4 |
| 60 |  | 0.00 [-0.04, 0.05] |  | 0.45 [0.45, 0.45] | -0.04 [-0.08, -0.01] | lvl_naive4 |
| 90 |  | 0.00 [-0.05, 0.05] |  |  | 0.01 [-0.03, 0.05] | lvl_naive4 |
| 120 |  | -0.06 [-0.12, -0.01] |  |  | -0.07 [-0.11, -0.02] | lvl_naive4 |
| 180 |  | -0.08 [-0.17, -0.01] |  |  | -0.02 [-0.10, 0.05] | lvl_naive4 |

**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the `≥ predicted q75` rule (ties included, which is what flat predictions cost)

| lead | model | park_months | busy_day_recall | busy_day_precision_q75_rule | iqr | iqr_true |
|---|---|---|---|---|---|---|
| 1 | lvl_cbd | 319 | 0.36 | 0.35 | 5.00 | 13.65 |
| 1 | lvl_clim | 615 | 0.37 | 0.35 | 6.25 | 14.50 |
| 1 | lvl_naive4 | 614 | 0.39 | 0.39 | 9.64 | 14.50 |
| 1 | lvl_snaive7 | 599 | 0.40 | 0.40 | 14.86 | 14.57 |
| 1 | lvl_tft | 429 | 0.42 | 0.42 | 10.51 | 13.67 |
| 1 | lvl_wt56 | 615 | 0.35 | 0.35 | 6.01 | 14.50 |
| 2 | lvl_cbd | 307 | 0.35 | 0.34 | 4.89 | 13.67 |
| 2 | lvl_clim | 563 | 0.36 | 0.34 | 5.93 | 14.50 |
| 2 | lvl_naive4 | 562 | 0.39 | 0.38 | 9.87 | 14.51 |
| 2 | lvl_snaive7 | 551 | 0.40 | 0.39 | 14.91 | 14.59 |
| 2 | lvl_tft | 380 | 0.43 | 0.43 | 11.11 | 13.90 |
| 2 | lvl_wt56 | 563 | 0.35 | 0.34 | 6.77 | 14.50 |
| 3 | lvl_cbd | 315 | 0.36 | 0.35 | 5.00 | 13.67 |
| 3 | lvl_clim | 534 | 0.36 | 0.34 | 5.82 | 14.50 |
| 3 | lvl_naive4 | 533 | 0.39 | 0.38 | 9.86 | 14.50 |
| 3 | lvl_snaive7 | 522 | 0.40 | 0.39 | 14.98 | 14.57 |
| 3 | lvl_tft | 380 | 0.42 | 0.42 | 11.03 | 13.90 |
| 3 | lvl_wt56 | 534 | 0.34 | 0.33 | 6.67 | 14.50 |
| 4 | lvl_cbd | 316 | 0.37 | 0.37 | 5.30 | 13.66 |
| 4 | lvl_clim | 534 | 0.36 | 0.34 | 5.65 | 14.51 |
| 4 | lvl_naive4 | 533 | 0.39 | 0.38 | 9.88 | 14.52 |
| 4 | lvl_snaive7 | 522 | 0.40 | 0.39 | 15.00 | 14.60 |
| 4 | lvl_tft | 379 | 0.41 | 0.41 | 11.57 | 13.91 |
| 4 | lvl_wt56 | 534 | 0.34 | 0.33 | 6.71 | 14.51 |
| 5 | lvl_cbd | 317 | 0.39 | 0.39 | 5.75 | 13.67 |
| 5 | lvl_clim | 529 | 0.36 | 0.34 | 5.42 | 14.50 |
| 5 | lvl_naive4 | 528 | 0.39 | 0.38 | 9.86 | 14.51 |
| 5 | lvl_snaive7 | 521 | 0.40 | 0.40 | 14.96 | 14.55 |
| 5 | lvl_tft | 375 | 0.41 | 0.41 | 11.75 | 13.94 |
| 5 | lvl_wt56 | 529 | 0.34 | 0.33 | 6.52 | 14.50 |
| 6 | lvl_cbd | 317 | 0.34 | 0.33 | 4.50 | 13.67 |
| 6 | lvl_clim | 527 | 0.36 | 0.33 | 5.44 | 14.53 |
| 6 | lvl_naive4 | 526 | 0.39 | 0.38 | 9.81 | 14.54 |
| 6 | lvl_snaive7 | 521 | 0.40 | 0.40 | 14.86 | 14.55 |
| 6 | lvl_tft | 375 | 0.41 | 0.41 | 11.33 | 13.94 |
| 6 | lvl_wt56 | 527 | 0.34 | 0.34 | 6.62 | 14.53 |
| 7 | lvl_cbd | 311 | 0.30 | 0.30 | 4.17 | 13.65 |
| 7 | lvl_clim | 527 | 0.35 | 0.33 | 5.60 | 14.45 |
| 7 | lvl_naive4 | 526 | 0.37 | 0.37 | 10.09 | 14.47 |
| 7 | lvl_tft | 375 | 0.38 | 0.38 | 11.66 | 13.94 |
| 7 | lvl_wt56 | 527 | 0.34 | 0.33 | 6.42 | 14.45 |
| 10 | lvl_cbd | 315 | 0.32 | 0.31 | 4.17 | 13.65 |
| 10 | lvl_clim | 527 | 0.35 | 0.33 | 6.74 | 14.37 |
| 10 | lvl_naive4 | 526 | 0.37 | 0.37 | 10.09 | 14.40 |
| 10 | lvl_tft | 373 | 0.37 | 0.37 | 11.33 | 13.91 |
| 10 | lvl_wt56 | 527 | 0.34 | 0.33 | 6.64 | 14.37 |
| 14 | lvl_cbd | 278 | 0.29 | 0.28 | 3.90 | 13.62 |
| 14 | lvl_clim | 524 | 0.34 | 0.33 | 7.09 | 14.24 |
| 14 | lvl_naive4 | 522 | 0.35 | 0.35 | 10.08 | 14.27 |
| 14 | lvl_tft | 373 | 0.34 | 0.34 | 11.63 | 13.67 |
| 14 | lvl_wt56 | 524 | 0.33 | 0.32 | 6.33 | 14.24 |
| 21 | lvl_cbd | 276 | 0.29 | 0.29 | 4.00 | 13.66 |
| 21 | lvl_clim | 510 | 0.34 | 0.33 | 6.29 | 14.17 |
| 21 | lvl_naive4 | 497 | 0.34 | 0.34 | 9.83 | 14.25 |
| 21 | lvl_tft | 366 | 0.34 | 0.35 | 11.28 | 13.77 |
| 21 | lvl_wt56 | 510 | 0.32 | 0.32 | 6.45 | 14.17 |
| 30 | lvl_cbd | 233 | 0.30 | 0.29 | 4.50 | 13.89 |
| 30 | lvl_clim | 451 | 0.35 | 0.34 | 6.00 | 14.25 |
| 30 | lvl_naive4 | 451 | 0.34 | 0.34 | 9.67 | 14.25 |
| 30 | lvl_tft | 267 | 0.33 | 0.33 | 10.44 | 14.05 |
| 30 | lvl_wt56 | 451 | 0.32 | 0.31 | 6.76 | 14.25 |
| 45 | lvl_cbd | 82 | 0.35 | 0.34 | 4.48 | 14.81 |
| 45 | lvl_clim | 418 | 0.34 | 0.33 | 6.18 | 13.63 |
| 45 | lvl_naive4 | 417 | 0.36 | 0.36 | 10.04 | 13.67 |
| 45 | lvl_tft | 180 | 0.35 | 0.36 | 10.97 | 14.25 |
| 45 | lvl_wt56 | 418 | 0.34 | 0.33 | 6.38 | 13.63 |
| 60 | lvl_clim | 354 | 0.36 | 0.34 | 4.73 | 13.16 |
| 60 | lvl_naive4 | 354 | 0.37 | 0.36 | 9.98 | 13.16 |
| 60 | lvl_tft | 1 | 0.12 | 0.20 | 9.56 | 10.47 |
| 60 | lvl_wt56 | 354 | 0.34 | 0.33 | 6.03 | 13.16 |
| 90 | lvl_clim | 264 | 0.36 | 0.34 | 5.77 | 12.74 |
| 90 | lvl_naive4 | 264 | 0.36 | 0.35 | 10.27 | 12.74 |
| 90 | lvl_wt56 | 264 | 0.38 | 0.37 | 6.82 | 12.74 |
| 120 | lvl_clim | 181 | 0.37 | 0.34 | 6.33 | 12.58 |
| 120 | lvl_naive4 | 181 | 0.38 | 0.37 | 10.55 | 12.58 |
| 120 | lvl_wt56 | 181 | 0.38 | 0.36 | 7.03 | 12.58 |
| 180 | lvl_clim | 64 | 0.50 | 0.43 | 9.16 | 12.02 |
| 180 | lvl_naive4 | 64 | 0.42 | 0.41 | 11.10 | 12.02 |
| 180 | lvl_wt56 | 64 | 0.49 | 0.46 | 8.20 | 12.02 |

## D1 — next-best ride

Candidates: every (intraday origin, ride) with a live OPERATING wait — the same set for every model. Suggest when the forecast maximum within the lookahead (60 or 120 min) is ≥ live + 10 (`next-best-ride.ts`, walking time 0); correct when the TRUE maximum there is also ≥ live + 10. False-suggestion rate = 1 − precision.

**next-best precision**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.33 [0.33, 0.34] | 0.32 [0.32, 0.32] | 0.35 [0.34, 0.35] | 0.31 [0.30, 0.32] | 0.37 [0.37, 0.38] | 0.33 [0.32, 0.33] |
| 0-120 | 0.41 [0.40, 0.41] | 0.40 [0.40, 0.41] | 0.43 [0.43, 0.44] | 0.39 [0.38, 0.40] | 0.46 [0.45, 0.46] | 0.41 [0.40, 0.41] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|
| 0-60 | -0.02 [-0.04, -0.00] | -0.02 [-0.03, -0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.00] | -0.02 [-0.03, -0.00] | snaive7 |
| 0-120 | -0.02 [-0.04, -0.01] | -0.02 [-0.04, 0.00] | 0.00 [-0.01, 0.02] | -0.01 [-0.03, 0.01] | -0.02 [-0.03, -0.00] | snaive7 |

**next-best precision (top-3)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.40 [0.40, 0.41] | 0.40 [0.39, 0.40] | 0.41 [0.40, 0.41] | 0.38 [0.37, 0.38] | 0.44 [0.43, 0.44] | 0.41 [0.40, 0.41] |
| 0-120 | 0.49 [0.48, 0.49] | 0.48 [0.47, 0.48] | 0.49 [0.49, 0.50] | 0.46 [0.45, 0.46] | 0.53 [0.52, 0.53] | 0.49 [0.48, 0.49] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|
| 0-60 | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.01] | -0.00 [-0.02, 0.01] | -0.01 [-0.03, -0.00] | 0.00 [-0.01, 0.01] | snaive7 |
| 0-120 | -0.01 [-0.03, -0.00] | -0.01 [-0.03, 0.00] | -0.01 [-0.02, 0.01] | -0.02 [-0.04, -0.01] | -0.01 [-0.02, 0.00] | snaive7 |

**next-best recall**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.44 [0.44, 0.45] | 0.40 [0.39, 0.40] | 0.36 [0.36, 0.37] | 0.25 [0.25, 0.26] | 0.51 [0.51, 0.52] | 0.48 [0.48, 0.49] |
| 0-120 | 0.47 [0.46, 0.47] | 0.42 [0.42, 0.43] | 0.38 [0.38, 0.39] | 0.27 [0.27, 0.28] | 0.56 [0.55, 0.56] | 0.51 [0.51, 0.52] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|
| 0-60 | -0.07 [-0.09, -0.05] | -0.12 [-0.14, -0.09] | -0.15 [-0.17, -0.13] | -0.26 [-0.29, -0.22] | -0.03 [-0.05, -0.01] | snaive7 |
| 0-120 | -0.09 [-0.11, -0.07] | -0.13 [-0.16, -0.11] | -0.17 [-0.20, -0.15] | -0.29 [-0.32, -0.25] | -0.04 [-0.06, -0.02] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| w0-45 | 13.28 [13.15, 13.40] | 12.21 [12.10, 12.33] | 12.50 [12.38, 12.63] | 11.54 [11.42, 11.67] | 6.29 [6.25, 6.35] | 14.15 [14.01, 14.31] | 12.34 [12.22, 12.46] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| w0-45 | 7.56 [6.71, 8.34] | 6.49 [5.78, 7.13] | 6.80 [6.05, 7.49] | 5.92 [5.28, 6.47] | 8.37 [7.56, 9.11] | 6.60 [5.88, 7.25] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|---|
| m015 | -0.22 | 0.19 | -1.43 | -1.33 | -2.05 | -1.56 | -2.10 |
| m030 | -0.30 | 0.19 | -1.53 | -1.45 | -1.94 | -1.45 | -2.02 |
| m045 | -0.09 | 0.17 | -1.57 | -1.51 | -1.85 | -1.35 | -1.92 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.75 [0.75, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.79] |  |  | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 1 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.75 [0.75, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.79] |  |  | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 2 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.79] |  |  | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 3 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 4 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 5 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 6 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.74 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 7 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.74 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] |  | 0.82 [0.82, 0.82] |
| 10 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.73 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.78] |  | 0.82 [0.82, 0.82] |
| 14 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.73 [0.73, 0.73] | 0.80 [0.79, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.77] |  | 0.82 [0.82, 0.82] |
| 21 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.82] | 0.73 [0.72, 0.73] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.75 [0.75, 0.75] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 30 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.82] | 0.73 [0.72, 0.73] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.75 [0.75, 0.76] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 45 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.82] | 0.72 [0.71, 0.73] | 0.79 [0.79, 0.79] | 0.78 [0.78, 0.79] | 0.75 [0.75, 0.76] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 60 | 0.83 [0.82, 0.83] | 0.82 [0.82, 0.83] |  | 0.79 [0.79, 0.80] | 0.80 [0.74, 0.84] | 0.75 [0.67, 0.83] | 0.75 [0.66, 0.83] |  | 0.82 [0.82, 0.82] |
| 90 | 0.83 [0.83, 0.84] | 0.83 [0.83, 0.83] |  | 0.80 [0.79, 0.80] |  |  |  |  | 0.82 [0.82, 0.83] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] |  |  | -0.03 [-0.04, -0.03] |  | wt_med |
| 1 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] |  |  | -0.03 [-0.04, -0.03] |  | wt_med |
| 2 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.01] |  |  | -0.03 [-0.04, -0.03] |  | wt_med |
| 3 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.01] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 4 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.01] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 5 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.01] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 6 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.08, -0.06] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.02] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 7 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.08, -0.06] | -0.02 [-0.03, -0.02] | -0.03 [-0.03, -0.02] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] |  |  | wt_med |
| 10 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.09, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.03, -0.02] | -0.06 [-0.07, -0.04] | -0.04 [-0.05, -0.02] |  |  | wt_med |
| 14 | -0.00 [-0.00, -0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.09, -0.06] | -0.02 [-0.03, -0.02] | -0.03 [-0.03, -0.02] | -0.06 [-0.07, -0.04] | -0.04 [-0.05, -0.02] |  |  | wt_med |
| 21 | -0.00 [-0.00, 0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.09, -0.06] | -0.02 [-0.03, -0.02] | -0.03 [-0.03, -0.02] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  |  | wt_med |
| 30 | -0.00 [-0.00, 0.00] | 0.01 [0.00, 0.01]* | -0.08 [-0.09, -0.06] | -0.02 [-0.03, -0.02] | -0.02 [-0.03, -0.02] | -0.05 [-0.07, -0.04] | -0.03 [-0.04, -0.02] |  |  | wt_med |
| 45 | -0.00 [-0.00, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.02 [-0.03, -0.02] | -0.02 [-0.03, -0.01] | -0.05 [-0.06, -0.03] | -0.02 [-0.04, -0.01] |  |  | wt_med |
| 60 |  | 0.01 [0.00, 0.01]* |  | -0.03 [-0.04, -0.02] | -0.05 [-0.11, 0.00] | -0.05 [-0.08, 0.03] | -0.05 [-0.09, 0.02] |  | -0.00 [-0.01, -0.00] | clim |
| 90 | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.01] |  | -0.03 [-0.04, -0.02] |  |  |  |  |  | wt_med |

**best-time hit rate (±30 min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 1 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 2 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 3 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 4 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 5 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 6 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 7 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 10 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.79 [0.79, 0.80] |  | 0.83 [0.82, 0.83] |
| 14 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 21 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 30 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 45 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.80] |  | 0.82 [0.82, 0.83] |
| 60 | 0.83 [0.83, 0.84] | 0.83 [0.83, 0.83] |  | 0.81 [0.81, 0.81] | 0.82 [0.76, 0.87] | 0.77 [0.70, 0.84] | 0.78 [0.70, 0.86] |  | 0.83 [0.82, 0.83] |
| 90 | 0.84 [0.83, 0.84] | 0.84 [0.83, 0.84] |  | 0.81 [0.81, 0.82] |  |  |  |  | 0.83 [0.83, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.04, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  | -0.02 [-0.03, -0.02] | wt_med |
| 1 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.04, -0.02] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  | -0.02 [-0.03, -0.02] | wt_med |
| 2 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.04, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  | -0.02 [-0.03, -0.02] | wt_med |
| 3 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.04, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.01] | -0.02 [-0.03, -0.02] | wt_med |
| 4 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.04, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.01] | -0.02 [-0.03, -0.02] | wt_med |
| 5 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.04 [-0.05, -0.03] | -0.02 [-0.04, -0.01] | -0.02 [-0.03, -0.02] | wt_med |
| 6 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.04 [-0.06, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.04 [-0.05, -0.03] | -0.02 [-0.04, -0.01] | -0.02 [-0.03, -0.02] | wt_med |
| 7 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.01, -0.01] | -0.01 [-0.02, -0.01] | -0.04 [-0.05, -0.03] | -0.02 [-0.04, -0.01] |  | wt_med |
| 10 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.04 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | wt_med |
| 14 | -0.00 [-0.00, -0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.01] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | wt_med |
| 21 | -0.00 [-0.00, 0.00] | 0.01 [0.01, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.01] | -0.03 [-0.04, -0.02] | -0.02 [-0.03, -0.01] |  | wt_med |
| 30 | -0.00 [-0.00, 0.00] | 0.01 [0.01, 0.01]* | -0.05 [-0.07, -0.03] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.00] | -0.03 [-0.04, -0.02] | -0.01 [-0.03, -0.00] |  | wt_med |
| 45 | 0.00 [-0.00, 0.00] | 0.01 [0.01, 0.01]* | -0.02 [-0.04, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, 0.00] | -0.03 [-0.04, -0.01] | -0.01 [-0.02, 0.00] |  | wt_med |
| 60 | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.01]* |  | -0.01 [-0.02, -0.01] | -0.02 [-0.05, 0.02] | -0.04 [-0.08, 0.03] | -0.03 [-0.05, 0.04] |  | wt_med |
| 90 | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.01] |  | -0.02 [-0.03, -0.01] |  |  |  |  | wt_med |

**best-time regret (min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 3.03 [2.98, 3.09] | 2.82 [2.77, 2.87] | 3.88 [3.82, 3.95] | 3.16 [3.12, 3.21] | 3.26 [3.20, 3.32] |  |  | 3.45 [3.37, 3.53] | 3.03 [2.97, 3.08] |
| 1 | 3.03 [2.98, 3.09] | 2.83 [2.78, 2.88] | 3.92 [3.85, 3.99] | 3.16 [3.12, 3.21] | 3.30 [3.24, 3.35] |  |  | 3.44 [3.37, 3.53] | 3.03 [2.98, 3.09] |
| 2 | 3.03 [2.97, 3.08] | 2.83 [2.78, 2.87] | 3.95 [3.88, 4.01] | 3.16 [3.12, 3.21] | 3.32 [3.26, 3.37] |  |  | 3.44 [3.37, 3.53] | 3.03 [2.98, 3.08] |
| 3 | 3.02 [2.96, 3.08] | 2.84 [2.79, 2.89] | 3.95 [3.88, 4.02] | 3.17 [3.12, 3.22] | 3.33 [3.27, 3.39] | 4.31 [4.22, 4.39] | 3.73 [3.65, 3.81] | 3.44 [3.37, 3.53] | 3.04 [2.98, 3.09] |
| 4 | 3.02 [2.96, 3.08] | 2.84 [2.79, 2.88] | 3.96 [3.90, 4.03] | 3.17 [3.13, 3.22] | 3.33 [3.27, 3.39] | 4.31 [4.22, 4.39] | 3.72 [3.65, 3.80] | 3.44 [3.37, 3.53] | 3.05 [2.99, 3.10] |
| 5 | 3.02 [2.96, 3.08] | 2.84 [2.80, 2.89] | 3.99 [3.92, 4.05] | 3.18 [3.13, 3.23] | 3.36 [3.29, 3.42] | 4.30 [4.21, 4.38] | 3.71 [3.63, 3.79] | 3.44 [3.36, 3.53] | 3.04 [2.99, 3.10] |
| 6 | 3.02 [2.97, 3.08] | 2.86 [2.81, 2.91] | 4.18 [4.11, 4.25] | 3.19 [3.14, 3.25] | 3.41 [3.35, 3.47] | 4.30 [4.22, 4.39] | 3.73 [3.66, 3.81] | 3.44 [3.36, 3.52] | 3.06 [3.00, 3.12] |
| 7 | 3.03 [2.97, 3.09] | 2.88 [2.83, 2.93] | 4.25 [4.17, 4.32] | 3.24 [3.19, 3.29] | 3.47 [3.41, 3.53] | 4.31 [4.22, 4.39] | 3.74 [3.66, 3.81] |  | 3.06 [3.00, 3.12] |
| 10 | 3.03 [2.97, 3.09] | 2.89 [2.84, 2.94] | 4.29 [4.22, 4.37] | 3.25 [3.20, 3.30] | 3.49 [3.43, 3.55] | 4.30 [4.21, 4.39] | 3.71 [3.63, 3.78] |  | 3.07 [3.01, 3.13] |
| 14 | 3.04 [2.98, 3.10] | 2.91 [2.86, 2.96] | 4.38 [4.30, 4.46] | 3.31 [3.26, 3.37] | 3.51 [3.44, 3.57] | 4.31 [4.21, 4.39] | 3.73 [3.65, 3.81] |  | 3.08 [3.02, 3.14] |
| 21 | 3.04 [2.98, 3.10] | 2.90 [2.85, 2.95] | 4.45 [4.38, 4.53] | 3.35 [3.29, 3.41] | 3.60 [3.52, 3.67] | 4.34 [4.24, 4.43] | 3.78 [3.70, 3.87] |  | 3.09 [3.03, 3.15] |
| 30 | 3.04 [2.98, 3.11] | 2.91 [2.86, 2.97] | 4.59 [4.50, 4.68] | 3.38 [3.32, 3.44] | 3.68 [3.59, 3.77] | 4.40 [4.29, 4.50] | 3.84 [3.75, 3.94] |  | 3.10 [3.04, 3.17] |
| 45 | 3.01 [2.94, 3.08] | 2.92 [2.86, 2.98] | 4.66 [4.49, 4.85] | 3.42 [3.35, 3.49] | 3.65 [3.54, 3.77] | 4.45 [4.32, 4.58] | 3.89 [3.77, 4.00] |  | 3.12 [3.05, 3.18] |
| 60 | 2.99 [2.92, 3.06] | 2.94 [2.87, 3.00] |  | 3.47 [3.40, 3.54] | 2.26 [1.70, 2.96] | 3.38 [2.20, 4.55] | 3.80 [2.41, 5.21] |  | 3.11 [3.03, 3.18] |
| 90 | 2.96 [2.88, 3.06] | 2.87 [2.80, 2.95] |  | 3.45 [3.37, 3.53] |  |  |  |  | 3.04 [2.95, 3.12] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.17 [0.11, 0.24] | -0.29 [-0.38, -0.22]* | 0.76 [0.57, 0.94] | 0.11 [-0.00, 0.20] | 0.08 [-0.04, 0.18] |  |  | 0.78 [0.63, 0.95] | wt_med |
| 1 | 0.16 [0.10, 0.22] | -0.29 [-0.38, -0.22]* | 0.77 [0.58, 0.96] | 0.09 [-0.01, 0.19] | 0.10 [-0.01, 0.21] |  |  | 0.76 [0.61, 0.94] | wt_med |
| 2 | 0.15 [0.10, 0.22] | -0.30 [-0.38, -0.22]* | 0.79 [0.59, 0.97] | 0.09 [-0.02, 0.18] | 0.11 [-0.00, 0.22] |  |  | 0.75 [0.61, 0.93] | wt_med |
| 3 | 0.14 [0.09, 0.21] | -0.30 [-0.38, -0.22]* | 0.78 [0.58, 0.97] | 0.09 [-0.02, 0.19] | 0.12 [-0.00, 0.23] | 1.14 [0.80, 1.58] | 0.54 [0.31, 0.78] | 0.74 [0.59, 0.91] | wt_med |
| 4 | 0.14 [0.08, 0.20] | -0.30 [-0.39, -0.22]* | 0.77 [0.57, 0.97] | 0.08 [-0.03, 0.18] | 0.10 [-0.03, 0.22] | 1.13 [0.79, 1.55] | 0.52 [0.31, 0.76] | 0.72 [0.58, 0.91] | wt_med |
| 5 | 0.13 [0.08, 0.19] | -0.29 [-0.38, -0.21]* | 0.78 [0.57, 0.98] | 0.09 [-0.02, 0.18] | 0.14 [0.01, 0.26] | 1.12 [0.78, 1.53] | 0.52 [0.30, 0.75] | 0.71 [0.58, 0.89] | wt_med |
| 6 | 0.11 [0.06, 0.17] | -0.29 [-0.38, -0.21]* | 0.95 [0.69, 1.19] | 0.08 [-0.03, 0.18] | 0.17 [0.03, 0.30] | 1.10 [0.75, 1.51] | 0.51 [0.29, 0.73] | 0.68 [0.54, 0.85] | wt_med |
| 7 | 0.10 [0.06, 0.16] | -0.28 [-0.37, -0.20]* | 1.01 [0.74, 1.27] | 0.16 [0.04, 0.26] | 0.22 [0.09, 0.34] | 1.10 [0.76, 1.52] | 0.51 [0.29, 0.73] |  | wt_med |
| 10 | 0.09 [0.03, 0.15] | -0.29 [-0.38, -0.21]* | 1.03 [0.74, 1.30] | 0.13 [0.01, 0.24] | 0.23 [0.09, 0.36] | 1.07 [0.72, 1.48] | 0.44 [0.22, 0.67] |  | wt_med |
| 14 | 0.08 [0.03, 0.13] | -0.28 [-0.37, -0.20]* | 1.08 [0.78, 1.36] | 0.20 [0.07, 0.32] | 0.22 [0.07, 0.36] | 1.04 [0.70, 1.43] | 0.41 [0.20, 0.62] |  | wt_med |
| 21 | 0.05 [-0.01, 0.11] | -0.31 [-0.41, -0.21]* | 1.08 [0.75, 1.40] | 0.22 [0.08, 0.35] | 0.24 [0.08, 0.40] | 0.96 [0.63, 1.32] | 0.35 [0.13, 0.57] |  | wt_med |
| 30 | 0.03 [-0.04, 0.10] | -0.31 [-0.41, -0.22]* | 1.10 [0.71, 1.45] | 0.22 [0.07, 0.36] | 0.18 [-0.02, 0.37] | 0.92 [0.57, 1.32] | 0.28 [0.01, 0.53] |  | wt_med |
| 45 | -0.01 [-0.11, 0.09] | -0.34 [-0.45, -0.24]* | 0.58 [0.15, 0.94] | 0.21 [0.04, 0.38] | 0.08 [-0.20, 0.31] | 0.80 [0.48, 1.14] | 0.13 [-0.15, 0.41] |  | wt_med |
| 60 | -0.08 [-0.21, 0.03] | -0.29 [-0.39, -0.22]* |  | 0.28 [0.10, 0.43] | 0.29 [-0.12, 0.99] | 0.84 [-0.60, 1.52] | 0.99 [-0.32, 1.71] |  | wt_med |
| 90 | -0.04 [-0.14, 0.07] | -0.19 [-0.28, -0.12] |  | 0.40 [0.23, 0.57] |  |  |  |  | wt_med |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 30.29 [29.73, 30.85] | 28.72 [28.22, 29.25] | 28.98 [28.28, 29.68] | 28.96 [28.43, 29.47] | 27.86 [27.27, 28.49] |  |  | 34.51 [33.91, 35.15] | 29.38 [28.89, 29.93] |
| 3 | 30.52 [29.92, 31.13] | 28.95 [28.44, 29.48] | 29.29 [28.57, 30.05] | 28.93 [28.38, 29.44] | 27.89 [27.29, 28.59] | 33.75 [33.13, 34.41] | 33.68 [33.05, 34.32] | 34.34 [33.75, 35.00] | 29.76 [29.25, 30.32] |
| 7 | 30.52 [29.91, 31.13] | 29.27 [28.77, 29.82] | 29.72 [28.99, 30.45] | 29.49 [28.94, 30.05] | 28.67 [28.08, 29.41] | 34.59 [33.92, 35.27] | 33.99 [33.37, 34.68] |  | 30.04 [29.52, 30.57] |
| 14 | 31.01 [30.39, 31.64] | 29.83 [29.29, 30.38] | 30.65 [29.88, 31.42] | 30.18 [29.60, 30.77] | 29.59 [28.91, 30.31] | 34.95 [34.29, 35.68] | 34.55 [33.88, 35.28] |  | 30.83 [30.26, 31.40] |
| 30 | 31.48 [30.85, 32.10] | 30.44 [29.88, 31.02] | 31.00 [30.19, 31.85] | 31.07 [30.50, 31.66] | 30.28 [29.46, 31.08] | 36.16 [35.34, 36.98] | 35.83 [35.01, 36.66] |  | 31.60 [31.02, 32.17] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.89 [0.25, 1.59] | -0.66 [-1.36, -0.06]* | -0.07 [-0.92, 0.87] | -0.47 [-1.18, 0.15] | -0.58 [-1.49, 0.23] |  |  | 4.84 [3.95, 5.74] | wt_med |
| 3 | 0.64 [-0.06, 1.41] | -0.80 [-1.47, -0.20]* | -0.29 [-1.30, 0.74] | -0.92 [-1.67, -0.26]* | -1.12 [-2.02, -0.31]* | 4.41 [3.14, 5.77] | 4.34 [3.02, 5.69] | 4.20 [3.27, 5.06] | wt_med |
| 7 | 0.52 [-0.19, 1.33] | -0.76 [-1.28, -0.27]* | -0.01 [-1.08, 1.16] | -0.46 [-1.18, 0.23] | -0.34 [-1.23, 0.48] | 5.33 [4.08, 6.69] | 4.74 [3.53, 5.96] |  | wt_med |
| 14 | 0.26 [-0.40, 0.89] | -0.99 [-1.65, -0.39]* | 0.06 [-1.02, 1.21] | -0.58 [-1.42, 0.25] | -0.23 [-1.10, 0.59] | 4.90 [3.34, 6.43] | 4.51 [3.12, 5.95] |  | wt_med |
| 30 | -0.04 [-0.83, 0.81] | -1.10 [-1.75, -0.44]* | -1.06 [-2.73, 0.41] | -0.64 [-1.46, 0.18] | -0.98 [-2.23, 0.12] | 4.86 [3.32, 6.41] | 4.51 [2.90, 6.10] |  | wt_med |

**dayPeak pairwise ordering (headliners)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.73 [0.72, 0.73] | 0.76 [0.76, 0.76] | 0.71 [0.70, 0.71] | 0.76 [0.76, 0.76] | 0.77 [0.76, 0.77] |  |  | 0.73 [0.72, 0.73] | 0.74 [0.74, 0.74] |
| 1 | 0.73 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.70 [0.70, 0.71] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.77] |  |  | 0.73 [0.72, 0.73] | 0.74 [0.73, 0.74] |
| 2 | 0.73 [0.72, 0.73] | 0.75 [0.75, 0.76] | 0.70 [0.70, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] |  |  | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.74] |
| 3 | 0.72 [0.72, 0.73] | 0.75 [0.75, 0.76] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.74] |
| 4 | 0.72 [0.72, 0.73] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.73] |
| 5 | 0.72 [0.72, 0.73] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.75, 0.76] | 0.70 [0.70, 0.71] | 0.74 [0.73, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.73] |
| 6 | 0.72 [0.72, 0.72] | 0.75 [0.75, 0.75] | 0.69 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.75 [0.75, 0.76] | 0.70 [0.70, 0.70] | 0.73 [0.73, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.73] |
| 7 | 0.72 [0.72, 0.72] | 0.74 [0.74, 0.75] | 0.68 [0.68, 0.68] | 0.75 [0.75, 0.75] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.73 [0.73, 0.73] |  | 0.73 [0.72, 0.73] |
| 10 | 0.72 [0.72, 0.72] | 0.74 [0.74, 0.74] | 0.68 [0.67, 0.68] | 0.75 [0.74, 0.75] | 0.74 [0.74, 0.75] | 0.69 [0.69, 0.69] | 0.72 [0.72, 0.73] |  | 0.72 [0.72, 0.73] |
| 14 | 0.72 [0.71, 0.72] | 0.74 [0.73, 0.74] | 0.67 [0.67, 0.68] | 0.74 [0.74, 0.74] | 0.74 [0.73, 0.74] | 0.68 [0.68, 0.69] | 0.72 [0.71, 0.72] |  | 0.72 [0.71, 0.72] |
| 21 | 0.71 [0.71, 0.72] | 0.73 [0.73, 0.73] | 0.67 [0.67, 0.68] | 0.73 [0.73, 0.74] | 0.73 [0.73, 0.73] | 0.68 [0.67, 0.68] | 0.71 [0.71, 0.71] |  | 0.71 [0.71, 0.72] |
| 30 | 0.71 [0.71, 0.71] | 0.72 [0.72, 0.72] | 0.67 [0.66, 0.67] | 0.73 [0.72, 0.73] | 0.72 [0.72, 0.73] | 0.67 [0.67, 0.67] | 0.70 [0.70, 0.71] |  | 0.71 [0.70, 0.71] |
| 45 | 0.70 [0.70, 0.71] | 0.71 [0.71, 0.72] | 0.66 [0.65, 0.67] | 0.72 [0.72, 0.72] | 0.72 [0.72, 0.73] | 0.67 [0.66, 0.67] | 0.70 [0.69, 0.70] |  | 0.70 [0.70, 0.70] |
| 60 | 0.70 [0.70, 0.71] | 0.71 [0.70, 0.71] |  | 0.71 [0.71, 0.72] | 0.75 [0.67, 0.85] | 0.65 [0.58, 0.72] | 0.71 [0.65, 0.77] |  | 0.70 [0.69, 0.70] |
| 90 | 0.71 [0.70, 0.71] | 0.71 [0.71, 0.72] |  | 0.72 [0.72, 0.72] |  |  |  |  | 0.70 [0.70, 0.71] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.03 [-0.04, -0.02] | 0.02 [0.02, 0.03]* | 0.04 [0.03, 0.05]* |  |  | -0.02 [-0.03, -0.02] | wt_med |
| 1 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.05]* |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 2 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.04]* |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 3 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.02 [-0.03, -0.01] | wt_med |
| 4 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.03 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.02 [0.01, 0.02]* | -0.02 [-0.02, -0.01] | wt_med |
| 5 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.03 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.02 [-0.02, -0.01] | wt_med |
| 6 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.03] | 0.03 [0.02, 0.04]* | 0.03 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.01 [-0.02, -0.01] | wt_med |
| 7 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.03] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | wt_med |
| 10 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.02] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | wt_med |
| 14 | -0.01 [-0.01, -0.00] | 0.02 [0.02, 0.03]* | -0.04 [-0.05, -0.02] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | wt_med |
| 21 | -0.01 [-0.01, -0.00] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.01] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | wt_med |
| 30 | -0.01 [-0.01, -0.00] | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.01] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | wt_med |
| 45 | -0.00 [-0.01, 0.00] | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.02] | 0.02 [0.02, 0.03]* | 0.04 [0.03, 0.05]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.03]* |  | wt_med |
| 60 | -0.01 [-0.01, 0.00] | 0.02 [0.02, 0.02]* |  | 0.02 [0.01, 0.03]* | 0.05 [-0.03, 0.23] | -0.05 [-0.12, 0.13] | 0.01 [-0.07, 0.23] |  | wt_med |
| 90 | -0.01 [-0.02, -0.00] | 0.02 [0.01, 0.02]* |  | 0.02 [0.01, 0.03]* |  |  |  |  | wt_med |

## D5 first-hour MAE vs EACH naive candidate

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.693 [-0.879, -0.524]* / [-0.749, -0.635]* | -0.376 [-0.541, -0.222]* / [-0.437, -0.315]* | -0.858 [-1.086, -0.663]* / [-0.916, -0.800]* | -0.858 [-1.086, -0.662]* / [-0.916, -0.799]* | -0.741 [-0.992, -0.546]* / [-0.805, -0.681]* | -0.723 [-0.981, -0.508]* / [-0.792, -0.653]* |  |  |
| 1 | -0.624 [-0.805, -0.450]* / [-0.682, -0.567]* | -0.357 [-0.520, -0.203]* / [-0.418, -0.296]* | -0.796 [-1.020, -0.603]* / [-0.855, -0.738]* | -0.798 [-1.019, -0.603]* / [-0.856, -0.739]* | -0.685 [-0.933, -0.492]* / [-0.749, -0.623]* | -0.659 [-0.919, -0.440]* / [-0.731, -0.589]* |  |  |
| 2 | -0.595 [-0.775, -0.419]* / [-0.653, -0.538]* | -0.343 [-0.507, -0.187]* / [-0.404, -0.280]* | -0.769 [-0.990, -0.578]* / [-0.827, -0.708]* | -0.770 [-0.989, -0.578]* / [-0.828, -0.709]* | -0.659 [-0.905, -0.463]* / [-0.724, -0.597]* | -0.633 [-0.889, -0.411]* / [-0.705, -0.560]* |  |  |
| 3 | -0.571 [-0.752, -0.397]* / [-0.628, -0.515]* | -0.332 [-0.495, -0.174]* / [-0.393, -0.269]* | -0.747 [-0.969, -0.557]* / [-0.805, -0.686]* | -0.748 [-0.970, -0.559]* / [-0.806, -0.688]* | -0.633 [-0.872, -0.440]* / [-0.697, -0.571]* | -0.605 [-0.858, -0.384]* / [-0.679, -0.534]* | 1.162 [0.917, 1.430] / [1.090, 1.229] | 1.310 [1.067, 1.564] / [1.237, 1.380] |
| 4 | -0.544 [-0.723, -0.372]* / [-0.600, -0.486]* | -0.314 [-0.475, -0.156]* / [-0.379, -0.250]* | -0.719 [-0.934, -0.532]* / [-0.776, -0.660]* | -0.721 [-0.935, -0.535]* / [-0.778, -0.663]* | -0.608 [-0.844, -0.420]* / [-0.672, -0.546]* | -0.586 [-0.819, -0.370]* / [-0.658, -0.514]* | 1.174 [0.940, 1.433] / [1.102, 1.245] | 1.324 [1.096, 1.568] / [1.249, 1.394] |
| 5 | -0.517 [-0.698, -0.345]* / [-0.572, -0.459]* | -0.297 [-0.459, -0.134]* / [-0.360, -0.232]* | -0.691 [-0.900, -0.506]* / [-0.749, -0.633]* | -0.692 [-0.897, -0.507]* / [-0.750, -0.632]* | -0.588 [-0.826, -0.394]* / [-0.652, -0.528]* | -0.564 [-0.810, -0.348]* / [-0.638, -0.490]* | 1.219 [0.976, 1.486] / [1.144, 1.290] | 1.371 [1.137, 1.628] / [1.294, 1.442] |
| 6 | -0.471 [-0.651, -0.299]* / [-0.530, -0.411]* | -0.280 [-0.444, -0.116]* / [-0.344, -0.215]* | -0.641 [-0.841, -0.454]* / [-0.700, -0.583]* | -0.640 [-0.840, -0.454]* / [-0.699, -0.581]* | -0.549 [-0.786, -0.348]* / [-0.617, -0.487]* | -0.513 [-0.757, -0.291]* / [-0.589, -0.435]* | 1.198 [0.949, 1.470] / [1.129, 1.266] | 1.345 [1.105, 1.603] / [1.272, 1.414] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.693 [0.524, 0.879] / [0.635, 0.749] | 0.279 [0.184, 0.387] / [0.254, 0.305] | -0.160 [-0.292, -0.066]* / [-0.183, -0.140]* | -0.160 [-0.292, -0.066]* / [-0.182, -0.140]* | -0.207 [-0.389, -0.076]* / [-0.237, -0.179]* | -0.165 [-0.362, -0.037]* / [-0.195, -0.134]* |  |  |
| 1 | 0.624 [0.450, 0.805] / [0.567, 0.682] | 0.234 [0.144, 0.337] / [0.207, 0.259] | -0.170 [-0.310, -0.071]* / [-0.192, -0.150]* | -0.171 [-0.310, -0.071]* / [-0.193, -0.150]* | -0.215 [-0.408, -0.076]* / [-0.246, -0.186]* | -0.181 [-0.385, -0.043]* / [-0.216, -0.148]* |  |  |
| 2 | 0.595 [0.419, 0.775] / [0.538, 0.653] | 0.219 [0.128, 0.326] / [0.194, 0.245] | -0.174 [-0.312, -0.073]* / [-0.197, -0.154]* | -0.175 [-0.313, -0.074]* / [-0.198, -0.155]* | -0.219 [-0.412, -0.078]* / [-0.249, -0.190]* | -0.188 [-0.394, -0.049]* / [-0.223, -0.155]* |  |  |
| 3 | 0.571 [0.397, 0.752] / [0.515, 0.628] | 0.207 [0.116, 0.312] / [0.181, 0.233] | -0.178 [-0.316, -0.077]* / [-0.200, -0.157]* | -0.179 [-0.318, -0.078]* / [-0.202, -0.157]* | -0.223 [-0.415, -0.080]* / [-0.254, -0.193]* | -0.201 [-0.410, -0.059]* / [-0.236, -0.166]* | 1.613 [1.366, 1.876] / [1.557, 1.667] | 1.765 [1.524, 2.027] / [1.701, 1.820] |
| 4 | 0.544 [0.372, 0.723] / [0.486, 0.600] | 0.199 [0.102, 0.305] / [0.171, 0.225] | -0.180 [-0.315, -0.079]* / [-0.202, -0.158]* | -0.181 [-0.316, -0.082]* / [-0.205, -0.160]* | -0.222 [-0.410, -0.082]* / [-0.253, -0.191]* | -0.205 [-0.408, -0.064]* / [-0.242, -0.170]* | 1.615 [1.379, 1.868] / [1.559, 1.669] | 1.769 [1.540, 2.023] / [1.710, 1.825] |
| 5 | 0.517 [0.345, 0.698] / [0.459, 0.572] | 0.189 [0.088, 0.298] / [0.162, 0.217] | -0.180 [-0.317, -0.080]* / [-0.202, -0.158]* | -0.181 [-0.316, -0.083]* / [-0.203, -0.159]* | -0.225 [-0.412, -0.085]* / [-0.257, -0.194]* | -0.204 [-0.400, -0.067]* / [-0.241, -0.168]* | 1.626 [1.394, 1.884] / [1.561, 1.681] | 1.782 [1.549, 2.040] / [1.715, 1.841] |
| 6 | 0.471 [0.299, 0.651] / [0.411, 0.530] | 0.161 [0.060, 0.275] / [0.134, 0.188] | -0.177 [-0.305, -0.082]* / [-0.199, -0.156]* | -0.176 [-0.297, -0.087]* / [-0.198, -0.155]* | -0.224 [-0.405, -0.086]* / [-0.254, -0.193]* | -0.201 [-0.388, -0.065]* / [-0.240, -0.167]* | 1.565 [1.329, 1.813] / [1.506, 1.622] | 1.715 [1.474, 1.976] / [1.653, 1.777] |
| 7 |  | 0.116 [0.012, 0.232] / [0.089, 0.141] | -0.180 [-0.296, -0.088]* / [-0.201, -0.160]* | -0.171 [-0.280, -0.085]* / [-0.193, -0.151]* | -0.227 [-0.402, -0.095]* / [-0.258, -0.197]* | -0.198 [-0.379, -0.069]* / [-0.236, -0.165]* | 1.598 [1.367, 1.846] / [1.540, 1.656] | 1.752 [1.522, 2.005] / [1.687, 1.814] |
| 10 |  | 0.071 [-0.032, 0.183] / [0.043, 0.097] | -0.175 [-0.279, -0.087]* / [-0.195, -0.155]* | -0.160 [-0.261, -0.073]* / [-0.182, -0.140]* | -0.210 [-0.353, -0.096]* / [-0.241, -0.181]* | -0.191 [-0.342, -0.076]* / [-0.232, -0.157]* | 1.605 [1.354, 1.864] / [1.544, 1.665] | 1.762 [1.512, 2.034] / [1.697, 1.828] |
| 14 |  | 0.040 [-0.068, 0.159] / [0.013, 0.068] | -0.174 [-0.277, -0.091]* / [-0.194, -0.156]* | -0.155 [-0.252, -0.069]* / [-0.175, -0.135]* | -0.207 [-0.341, -0.098]* / [-0.238, -0.176]* | -0.187 [-0.332, -0.072]* / [-0.225, -0.151]* | 1.639 [1.378, 1.903] / [1.576, 1.701] | 1.804 [1.538, 2.087] / [1.737, 1.870] |
| 21 |  | 0.012 [-0.103, 0.142] / [-0.016, 0.041] | -0.173 [-0.274, -0.092]* / [-0.192, -0.155]* | -0.149 [-0.243, -0.065]* / [-0.169, -0.130]* | -0.194 [-0.323, -0.087]* / [-0.226, -0.163]* | -0.201 [-0.357, -0.075]* / [-0.245, -0.163]* | 1.634 [1.392, 1.883] / [1.569, 1.699] | 1.812 [1.551, 2.081] / [1.745, 1.883] |
| 30 |  | -0.003 [-0.124, 0.128] / [-0.033, 0.028] | -0.160 [-0.258, -0.079]* / [-0.178, -0.142]* | -0.130 [-0.228, -0.046]* / [-0.149, -0.110]* | -0.166 [-0.313, -0.046]* / [-0.205, -0.129]* | -0.229 [-0.406, -0.081]* / [-0.272, -0.191]* | 1.725 [1.484, 1.961] / [1.649, 1.806] | 1.884 [1.625, 2.164] / [1.804, 1.969] |
| 45 |  | -0.026 [-0.157, 0.126] / [-0.055, 0.004] | -0.137 [-0.229, -0.064]* / [-0.155, -0.120]* | -0.113 [-0.198, -0.045]* / [-0.131, -0.095]* | -0.155 [-0.285, -0.046]* / [-0.198, -0.114]* | -0.250 [-0.511, -0.033]* / [-0.336, -0.168]* | 1.842 [1.588, 2.105] / [1.744, 1.939] | 2.006 [1.707, 2.307] / [1.900, 2.115] |
| 60 |  | -0.052 [-0.195, 0.128] / [-0.081, -0.021]*  ! | -0.117 [-0.203, -0.052]* / [-0.133, -0.101]* | -0.092 [-0.182, -0.023]* / [-0.110, -0.075]* | 0.000 [0.000, 0.000] / [0.000, 0.000] |  | 0.486 [0.185, 2.043] / [0.287, 0.710] | 0.549 [0.242, 2.130] / [0.339, 0.794] |
| 90 |  | 0.022 [-0.142, 0.243] / [-0.013, 0.060] | -0.087 [-0.151, -0.038]* / [-0.103, -0.073]* | -0.042 [-0.108, 0.015] / [-0.061, -0.023]*  ! |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.376 [0.222, 0.541] / [0.315, 0.437] | -0.279 [-0.387, -0.184]* / [-0.305, -0.254]* | -0.454 [-0.631, -0.335]* / [-0.482, -0.426]* | -0.454 [-0.630, -0.334]* / [-0.482, -0.426]* | -0.445 [-0.651, -0.306]* / [-0.481, -0.410]* | -0.451 [-0.674, -0.295]* / [-0.492, -0.409]* |  |  |
| 1 | 0.357 [0.203, 0.520] / [0.296, 0.418] | -0.234 [-0.337, -0.144]* / [-0.259, -0.207]* | -0.417 [-0.592, -0.297]* / [-0.445, -0.389]* | -0.418 [-0.592, -0.299]* / [-0.445, -0.390]* | -0.420 [-0.630, -0.277]* / [-0.456, -0.385]* | -0.423 [-0.650, -0.265]* / [-0.464, -0.379]* |  |  |
| 2 | 0.343 [0.187, 0.507] / [0.280, 0.404] | -0.219 [-0.326, -0.128]* / [-0.245, -0.194]* | -0.407 [-0.583, -0.287]* / [-0.434, -0.378]* | -0.408 [-0.582, -0.288]* / [-0.435, -0.378]* | -0.419 [-0.629, -0.275]* / [-0.455, -0.383]* | -0.425 [-0.653, -0.263]* / [-0.467, -0.382]* |  |  |
| 3 | 0.332 [0.174, 0.495] / [0.269, 0.393] | -0.207 [-0.312, -0.116]* / [-0.233, -0.181]* | -0.396 [-0.564, -0.279]* / [-0.424, -0.367]* | -0.397 [-0.563, -0.282]* / [-0.425, -0.368]* | -0.415 [-0.621, -0.271]* / [-0.450, -0.377]* | -0.417 [-0.642, -0.255]* / [-0.460, -0.375]* | 1.403 [1.177, 1.619] / [1.348, 1.454] | 1.556 [1.328, 1.786] / [1.497, 1.611] |
| 4 | 0.314 [0.156, 0.475] / [0.250, 0.379] | -0.199 [-0.305, -0.102]* / [-0.225, -0.171]* | -0.388 [-0.555, -0.271]* / [-0.417, -0.358]* | -0.390 [-0.558, -0.274]* / [-0.419, -0.360]* | -0.411 [-0.610, -0.272]* / [-0.447, -0.371]* | -0.418 [-0.647, -0.254]* / [-0.461, -0.374]* | 1.412 [1.192, 1.627] / [1.351, 1.467] | 1.567 [1.340, 1.794] / [1.502, 1.626] |
| 5 | 0.297 [0.134, 0.459] / [0.232, 0.360] | -0.189 [-0.298, -0.088]* / [-0.217, -0.162]* | -0.378 [-0.541, -0.259]* / [-0.407, -0.349]* | -0.378 [-0.540, -0.260]* / [-0.408, -0.349]* | -0.415 [-0.617, -0.273]* / [-0.450, -0.377]* | -0.417 [-0.648, -0.253]* / [-0.461, -0.374]* | 1.418 [1.195, 1.644] / [1.354, 1.478] | 1.576 [1.348, 1.800] / [1.506, 1.637] |
| 6 | 0.280 [0.116, 0.444] / [0.215, 0.344] | -0.161 [-0.275, -0.060]* / [-0.188, -0.134]* | -0.348 [-0.503, -0.229]* / [-0.378, -0.319]* | -0.347 [-0.497, -0.231]* / [-0.378, -0.316]* | -0.394 [-0.589, -0.250]* / [-0.430, -0.356]* | -0.397 [-0.621, -0.233]* / [-0.441, -0.353]* | 1.379 [1.164, 1.606] / [1.321, 1.438] | 1.530 [1.310, 1.765] / [1.468, 1.594] |
| 7 |  | -0.116 [-0.232, -0.012]* / [-0.141, -0.089]* | -0.304 [-0.451, -0.188]* / [-0.334, -0.275]* | -0.296 [-0.444, -0.179]* / [-0.328, -0.266]* | -0.357 [-0.540, -0.213]* / [-0.393, -0.320]* | -0.356 [-0.569, -0.194]* / [-0.400, -0.313]* | 1.456 [1.225, 1.679] / [1.398, 1.516] | 1.611 [1.389, 1.851] / [1.547, 1.676] |
| 10 |  | -0.071 [-0.183, 0.032] / [-0.097, -0.043]*  ! | -0.250 [-0.397, -0.129]* / [-0.279, -0.221]* | -0.234 [-0.387, -0.099]* / [-0.265, -0.203]* | -0.293 [-0.465, -0.141]* / [-0.328, -0.254]* | -0.296 [-0.500, -0.123]* / [-0.339, -0.251]* | 1.509 [1.266, 1.748] / [1.445, 1.572] | 1.666 [1.417, 1.927] / [1.599, 1.735] |
| 14 |  | -0.040 [-0.159, 0.068] / [-0.068, -0.013]*  ! | -0.216 [-0.363, -0.094]* / [-0.244, -0.187]* | -0.195 [-0.356, -0.046]* / [-0.226, -0.163]* | -0.244 [-0.427, -0.077]* / [-0.283, -0.203]* | -0.247 [-0.448, -0.072]* / [-0.291, -0.205]* | 1.595 [1.333, 1.869] / [1.529, 1.662] | 1.761 [1.502, 2.030] / [1.690, 1.833] |
| 21 |  | -0.012 [-0.142, 0.103] / [-0.041, 0.016] | -0.185 [-0.340, -0.058]* / [-0.212, -0.155]* | -0.159 [-0.329, -0.010]* / [-0.189, -0.126]* | -0.168 [-0.364, 0.007] / [-0.205, -0.130]*  ! | -0.186 [-0.387, -0.008]* / [-0.234, -0.142]* | 1.656 [1.396, 1.909] / [1.591, 1.725] | 1.834 [1.581, 2.091] / [1.761, 1.908] |
| 30 |  | 0.003 [-0.128, 0.124] / [-0.028, 0.033] | -0.160 [-0.321, -0.028]* / [-0.191, -0.130]* | -0.126 [-0.299, 0.034] / [-0.160, -0.093]*  ! | -0.092 [-0.302, 0.114] / [-0.144, -0.041]*  ! | -0.173 [-0.379, 0.012] / [-0.221, -0.126]*  ! | 1.802 [1.546, 2.046] / [1.719, 1.889] | 1.961 [1.707, 2.215] / [1.875, 2.053] |
| 45 |  | 0.026 [-0.126, 0.157] / [-0.004, 0.055] | -0.113 [-0.298, 0.011] / [-0.146, -0.080]*  ! | -0.088 [-0.273, 0.047] / [-0.120, -0.054]*  ! | -0.065 [-0.294, 0.141] / [-0.120, -0.007]*  ! | -0.686 [-1.223, -0.284]* / [-0.802, -0.576]* | 1.916 [1.633, 2.188] / [1.806, 2.024] | 2.079 [1.802, 2.352] / [1.963, 2.193] |
| 60 |  | 0.052 [-0.128, 0.195] / [0.021, 0.081] | -0.073 [-0.281, 0.074] / [-0.108, -0.042]*  ! | -0.045 [-0.262, 0.112] / [-0.082, -0.012]*  ! | 0.104 [-0.166, 0.975] / [0.024, 0.206] |  | 0.586 [0.195, 2.852] / [0.361, 0.873] | 0.649 [0.257, 2.907] / [0.417, 0.929] |
| 90 |  | -0.022 [-0.243, 0.142] / [-0.060, 0.013] | -0.110 [-0.360, 0.068] / [-0.150, -0.072]*  ! | -0.060 [-0.309, 0.129] / [-0.100, -0.020]*  ! |  |  |  |  |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.54 [4.47, 4.61] | 4.11 [4.05, 4.18] | 3.95 [3.88, 4.03] | 4.11 [4.05, 4.18] | 3.87 [3.81, 3.95] | 4.10 [4.04, 4.17] | 4.51 [4.44, 4.58] |  |  | 5.01 [4.93, 5.11] | 4.26 [4.19, 4.33] |
| 1 | 4.55 [4.48, 4.63] | 4.17 [4.11, 4.24] | 4.02 [3.94, 4.10] | 4.17 [4.11, 4.24] | 3.92 [3.85, 4.00] | 4.16 [4.10, 4.24] | 4.50 [4.44, 4.58] |  |  | 5.01 [4.92, 5.10] | 4.33 [4.26, 4.41] |
| 2 | 4.57 [4.50, 4.64] | 4.20 [4.13, 4.27] | 4.04 [3.96, 4.12] | 4.20 [4.13, 4.27] | 3.94 [3.88, 4.02] | 4.19 [4.12, 4.26] | 4.50 [4.43, 4.58] |  |  | 5.01 [4.92, 5.10] | 4.36 [4.29, 4.43] |
| 3 | 4.57 [4.50, 4.65] | 4.21 [4.15, 4.28] | 4.05 [3.97, 4.14] | 4.21 [4.15, 4.28] | 3.96 [3.89, 4.04] | 4.20 [4.14, 4.27] | 4.50 [4.43, 4.58] | 5.80 [5.71, 5.89] | 5.95 [5.86, 6.04] | 5.01 [4.92, 5.10] | 4.38 [4.31, 4.45] |
| 4 | 4.58 [4.50, 4.66] | 4.23 [4.16, 4.30] | 4.07 [3.99, 4.15] | 4.22 [4.16, 4.29] | 3.98 [3.91, 4.06] | 4.21 [4.15, 4.29] | 4.50 [4.43, 4.57] | 5.81 [5.72, 5.90] | 5.96 [5.87, 6.05] | 5.00 [4.91, 5.09] | 4.40 [4.33, 4.47] |
| 5 | 4.59 [4.51, 4.67] | 4.25 [4.19, 4.32] | 4.11 [4.02, 4.19] | 4.25 [4.18, 4.32] | 3.99 [3.92, 4.07] | 4.24 [4.17, 4.31] | 4.50 [4.43, 4.57] | 5.84 [5.75, 5.94] | 5.99 [5.90, 6.09] | 4.99 [4.91, 5.09] | 4.42 [4.35, 4.49] |
| 6 | 4.59 [4.52, 4.68] | 4.29 [4.23, 4.36] | 4.16 [4.08, 4.25] | 4.29 [4.23, 4.36] | 4.03 [3.96, 4.11] | 4.27 [4.21, 4.35] | 4.49 [4.43, 4.57] | 5.81 [5.72, 5.91] | 5.96 [5.87, 6.06] | 4.99 [4.90, 5.08] | 4.46 [4.39, 4.53] |
| 7 | 4.61 [4.54, 4.69] | 4.36 [4.29, 4.43] | 4.23 [4.15, 4.32] | 4.36 [4.29, 4.43] | 4.09 [4.02, 4.17] | 4.34 [4.27, 4.41] | 4.87 [4.79, 4.95] | 5.90 [5.81, 6.00] | 6.05 [5.96, 6.16] |  | 4.53 [4.46, 4.60] |
| 10 | 4.63 [4.56, 4.71] | 4.42 [4.35, 4.48] | 4.32 [4.23, 4.42] | 4.42 [4.35, 4.50] | 4.18 [4.10, 4.26] | 4.39 [4.32, 4.46] | 4.87 [4.79, 4.95] | 5.98 [5.88, 6.09] | 6.14 [6.04, 6.24] |  | 4.58 [4.51, 4.66] |
| 14 | 4.68 [4.60, 4.76] | 4.49 [4.42, 4.56] | 4.42 [4.33, 4.53] | 4.50 [4.43, 4.58] | 4.27 [4.19, 4.36] | 4.46 [4.39, 4.54] | 5.11 [5.02, 5.19] | 6.11 [6.01, 6.22] | 6.27 [6.17, 6.38] |  | 4.66 [4.58, 4.74] |
| 21 | 4.74 [4.66, 4.82] | 4.56 [4.48, 4.63] | 4.56 [4.46, 4.66] | 4.58 [4.50, 4.65] | 4.40 [4.32, 4.49] | 4.53 [4.46, 4.60] | 5.30 [5.21, 5.39] | 6.24 [6.13, 6.35] | 6.41 [6.31, 6.52] |  | 4.73 [4.65, 4.81] |
| 30 | 4.80 [4.73, 4.89] | 4.62 [4.54, 4.70] | 4.70 [4.59, 4.81] | 4.64 [4.56, 4.72] | 4.63 [4.52, 4.74] | 4.58 [4.50, 4.66] | 5.40 [5.31, 5.48] | 6.56 [6.45, 6.70] | 6.72 [6.60, 6.85] |  | 4.78 [4.71, 4.87] |
| 45 | 4.83 [4.75, 4.91] | 4.66 [4.59, 4.75] | 4.42 [4.23, 4.59] | 4.68 [4.61, 4.77] | 4.79 [4.65, 4.94] | 4.63 [4.55, 4.71] | 5.48 [5.39, 5.57] | 6.80 [6.65, 6.95] | 6.96 [6.80, 7.10] |  | 4.82 [4.74, 4.90] |
| 60 | 4.86 [4.77, 4.96] | 4.76 [4.68, 4.86] |  | 4.78 [4.70, 4.87] | 1.34 [0.93, 1.82] | 4.73 [4.65, 4.82] | 5.59 [5.50, 5.70] | 1.89 [1.36, 2.49] | 1.95 [1.42, 2.56] |  | 4.88 [4.80, 4.97] |
| 90 | 5.04 [4.94, 5.16] | 4.90 [4.81, 5.00] |  | 4.95 [4.86, 5.05] |  | 4.89 [4.80, 5.00] | 5.59 [5.49, 5.70] |  |  |  | 4.99 [4.89, 5.09] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.28 [0.18, 0.39] | -0.16 [-0.29, -0.07]* | -0.16 [-0.36, -0.04]* | -0.16 [-0.29, -0.07]* | -0.21 [-0.39, -0.08]* | -0.17 [-0.29, -0.07]* | 0.23 [-0.14, 0.53] |  |  | 0.69 [0.52, 0.88] | wt_med |
| 1 | 0.23 [0.14, 0.34] | -0.17 [-0.31, -0.07]* | -0.18 [-0.39, -0.04]* | -0.17 [-0.31, -0.07]* | -0.22 [-0.41, -0.08]* | -0.18 [-0.32, -0.08]* | 0.16 [-0.22, 0.47] |  |  | 0.62 [0.45, 0.81] | wt_med |
| 2 | 0.22 [0.13, 0.33] | -0.17 [-0.31, -0.07]* | -0.19 [-0.39, -0.05]* | -0.18 [-0.31, -0.07]* | -0.22 [-0.41, -0.08]* | -0.19 [-0.32, -0.09]* | 0.13 [-0.25, 0.44] |  |  | 0.59 [0.42, 0.78] | wt_med |
| 3 | 0.21 [0.12, 0.31] | -0.18 [-0.32, -0.08]* | -0.20 [-0.41, -0.06]* | -0.18 [-0.32, -0.08]* | -0.22 [-0.41, -0.08]* | -0.19 [-0.33, -0.09]* | 0.11 [-0.28, 0.42] | 1.61 [1.37, 1.88] | 1.76 [1.52, 2.03] | 0.57 [0.40, 0.75] | wt_med |
| 4 | 0.20 [0.10, 0.31] | -0.18 [-0.31, -0.08]* | -0.20 [-0.41, -0.06]* | -0.18 [-0.32, -0.08]* | -0.22 [-0.41, -0.08]* | -0.19 [-0.33, -0.09]* | 0.09 [-0.30, 0.41] | 1.62 [1.38, 1.87] | 1.77 [1.54, 2.02] | 0.54 [0.37, 0.72] | wt_med |
| 5 | 0.19 [0.09, 0.30] | -0.18 [-0.32, -0.08]* | -0.20 [-0.40, -0.07]* | -0.18 [-0.32, -0.08]* | -0.23 [-0.41, -0.09]* | -0.19 [-0.33, -0.10]* | 0.06 [-0.33, 0.39] | 1.63 [1.39, 1.88] | 1.78 [1.55, 2.04] | 0.52 [0.35, 0.70] | wt_med |
| 6 | 0.16 [0.06, 0.28] | -0.18 [-0.31, -0.08]* | -0.20 [-0.39, -0.07]* | -0.18 [-0.30, -0.09]* | -0.22 [-0.40, -0.09]* | -0.19 [-0.33, -0.10]* | 0.02 [-0.38, 0.35] | 1.56 [1.33, 1.81] | 1.71 [1.47, 1.98] | 0.47 [0.30, 0.65] | wt_med |
| 7 | 0.12 [0.01, 0.23] | -0.18 [-0.30, -0.09]* | -0.20 [-0.38, -0.07]* | -0.17 [-0.28, -0.08]* | -0.23 [-0.40, -0.09]* | -0.20 [-0.33, -0.11]* | 0.36 [-0.05, 0.70] | 1.60 [1.37, 1.85] | 1.75 [1.52, 2.00] |  | wt_med |
| 10 | 0.07 [-0.03, 0.18] | -0.17 [-0.28, -0.09]* | -0.19 [-0.34, -0.08]* | -0.16 [-0.26, -0.07]* | -0.21 [-0.35, -0.10]* | -0.20 [-0.32, -0.11]* | 0.29 [-0.13, 0.63] | 1.60 [1.35, 1.86] | 1.76 [1.51, 2.03] |  | wt_med |
| 14 | 0.04 [-0.07, 0.16] | -0.17 [-0.28, -0.09]* | -0.19 [-0.33, -0.07]* | -0.15 [-0.25, -0.07]* | -0.21 [-0.34, -0.10]* | -0.20 [-0.31, -0.11]* | 0.47 [0.01, 0.84] | 1.64 [1.38, 1.90] | 1.80 [1.54, 2.09] |  | wt_med |
| 21 | 0.01 [-0.10, 0.14] | -0.17 [-0.27, -0.09]* | -0.20 [-0.36, -0.08]* | -0.15 [-0.24, -0.06]* | -0.19 [-0.32, -0.09] | -0.20 [-0.31, -0.11]* | 0.56 [0.10, 0.94] | 1.63 [1.39, 1.88] | 1.81 [1.55, 2.08] |  | wt_med |
| 30 | -0.00 [-0.12, 0.13] | -0.16 [-0.26, -0.08]* | -0.23 [-0.41, -0.08] | -0.13 [-0.23, -0.05] | -0.17 [-0.31, -0.05] | -0.20 [-0.31, -0.11]* | 0.59 [0.13, 0.97] | 1.73 [1.48, 1.96] | 1.88 [1.63, 2.16] |  | wt_med |
| 45 | -0.03 [-0.16, 0.13] | -0.14 [-0.23, -0.06] | -0.25 [-0.51, -0.03]* | -0.11 [-0.20, -0.04] | -0.15 [-0.29, -0.05] | -0.18 [-0.27, -0.09]* | 0.65 [0.19, 1.05] | 1.84 [1.59, 2.11] | 2.01 [1.71, 2.31] |  | wt_med |
| 60 | -0.05 [-0.19, 0.13] | -0.12 [-0.20, -0.05] |  | -0.09 [-0.18, -0.02] | 0.00 [0.00, 0.00] | -0.15 [-0.25, -0.08] | 0.69 [0.19, 1.11] | 0.49 [0.18, 2.04] | 0.55 [0.24, 2.13] |  | wt_med |
| 90 | 0.02 [-0.14, 0.24] | -0.09 [-0.15, -0.04] |  | -0.04 [-0.11, 0.02] |  | -0.09 [-0.20, -0.00] | 0.56 [0.02, 1.04] |  |  |  | wt_med |

**first-hour MAE, schedule known at origin**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.49 [4.41, 4.57] | 4.04 [3.97, 4.12] | 3.93 [3.84, 4.02] | 4.04 [3.97, 4.12] | 3.83 [3.75, 3.91] | 4.04 [3.97, 4.12] | 4.55 [4.47, 4.63] |  |  | 4.93 [4.83, 5.03] | 4.22 [4.14, 4.29] |
| 1 | 4.50 [4.42, 4.58] | 4.11 [4.03, 4.18] | 3.99 [3.90, 4.08] | 4.11 [4.03, 4.18] | 3.87 [3.79, 3.95] | 4.11 [4.03, 4.18] | 4.54 [4.47, 4.62] |  |  | 4.93 [4.83, 5.03] | 4.29 [4.21, 4.37] |
| 2 | 4.51 [4.42, 4.60] | 4.12 [4.05, 4.20] | 4.01 [3.92, 4.10] | 4.12 [4.05, 4.20] | 3.89 [3.81, 3.97] | 4.12 [4.05, 4.20] | 4.54 [4.46, 4.62] |  |  | 4.92 [4.82, 5.02] | 4.31 [4.23, 4.39] |
| 3 | 4.51 [4.42, 4.60] | 4.13 [4.06, 4.21] | 4.02 [3.93, 4.11] | 4.13 [4.06, 4.21] | 3.90 [3.82, 3.98] | 4.13 [4.06, 4.21] | 4.53 [4.45, 4.61] | 5.82 [5.72, 5.92] | 5.95 [5.85, 6.05] | 4.92 [4.82, 5.02] | 4.33 [4.24, 4.40] |
| 4 | 4.51 [4.42, 4.60] | 4.14 [4.06, 4.22] | 4.03 [3.94, 4.12] | 4.14 [4.06, 4.22] | 3.90 [3.82, 3.99] | 4.14 [4.07, 4.22] | 4.52 [4.44, 4.60] | 5.83 [5.72, 5.93] | 5.96 [5.86, 6.06] | 4.91 [4.81, 5.01] | 4.34 [4.26, 4.41] |
| 5 | 4.50 [4.42, 4.59] | 4.14 [4.07, 4.22] | 4.05 [3.96, 4.14] | 4.14 [4.07, 4.22] | 3.91 [3.83, 3.99] | 4.14 [4.07, 4.22] | 4.50 [4.42, 4.58] | 5.84 [5.73, 5.94] | 5.98 [5.87, 6.08] | 4.89 [4.79, 4.99] | 4.34 [4.26, 4.41] |
| 6 | 4.48 [4.40, 4.58] | 4.16 [4.08, 4.23] | 4.08 [3.99, 4.17] | 4.16 [4.08, 4.23] | 3.93 [3.85, 4.01] | 4.16 [4.08, 4.23] | 4.48 [4.41, 4.57] | 5.79 [5.69, 5.90] | 5.93 [5.83, 6.04] | 4.87 [4.77, 4.97] | 4.35 [4.26, 4.42] |
| 7 | 4.48 [4.40, 4.57] | 4.20 [4.12, 4.28] | 4.12 [4.04, 4.22] | 4.20 [4.12, 4.28] | 3.96 [3.88, 4.05] | 4.20 [4.12, 4.28] | 4.85 [4.76, 4.94] | 5.84 [5.73, 5.95] | 5.98 [5.87, 6.10] |  | 4.39 [4.31, 4.47] |
| 10 | 4.43 [4.35, 4.53] | 4.20 [4.12, 4.28] | 4.14 [4.04, 4.23] | 4.20 [4.12, 4.28] | 3.97 [3.89, 4.06] | 4.20 [4.12, 4.28] | 4.80 [4.71, 4.89] | 5.84 [5.73, 5.95] | 5.99 [5.89, 6.10] |  | 4.38 [4.30, 4.47] |
| 14 | 4.41 [4.33, 4.50] | 4.19 [4.11, 4.28] | 4.14 [4.05, 4.24] | 4.19 [4.11, 4.28] | 3.96 [3.88, 4.05] | 4.19 [4.11, 4.28] | 5.01 [4.91, 5.10] | 5.90 [5.79, 6.01] | 6.07 [5.96, 6.19] |  | 4.38 [4.30, 4.47] |
| 21 | 4.41 [4.32, 4.50] | 4.19 [4.11, 4.27] | 4.21 [4.11, 4.32] | 4.19 [4.11, 4.27] | 4.01 [3.92, 4.10] | 4.19 [4.11, 4.28] | 5.15 [5.06, 5.25] | 5.90 [5.78, 6.02] | 6.07 [5.96, 6.20] |  | 4.39 [4.30, 4.48] |
| 30 | 4.44 [4.34, 4.54] | 4.17 [4.08, 4.25] | 4.28 [4.17, 4.40] | 4.17 [4.08, 4.25] | 4.09 [3.98, 4.21] | 4.17 [4.09, 4.25] | 5.12 [5.02, 5.22] | 6.13 [5.99, 6.27] | 6.29 [6.15, 6.43] |  | 4.36 [4.27, 4.46] |
| 45 | 4.29 [4.19, 4.39] | 4.07 [3.99, 4.16] | 4.38 [4.11, 4.64] | 4.07 [3.99, 4.16] | 4.08 [3.93, 4.23] | 4.07 [3.99, 4.16] | 5.09 [4.97, 5.19] | 6.13 [5.95, 6.30] | 6.29 [6.11, 6.46] |  | 4.23 [4.14, 4.33] |
| 60 | 4.03 [3.92, 4.14] | 4.01 [3.92, 4.12] |  | 4.01 [3.92, 4.12] | 1.15 [0.78, 1.63] | 4.02 [3.93, 4.12] | 5.12 [5.00, 5.24] | 1.68 [1.17, 2.28] | 1.74 [1.22, 2.34] |  | 4.12 [4.01, 4.23] |
| 90 | 3.89 [3.75, 4.02] | 3.88 [3.75, 4.00] |  | 3.88 [3.75, 4.00] |  | 3.88 [3.76, 4.01] | 4.82 [4.68, 4.97] |  |  |  | 3.94 [3.81, 4.08] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.28 [0.16, 0.40] | -0.19 [-0.34, -0.08]* | -0.19 [-0.42, -0.04]* | -0.19 [-0.34, -0.08]* | -0.24 [-0.45, -0.09]* | -0.19 [-0.34, -0.08]* | 0.32 [-0.10, 0.65] |  |  | 0.67 [0.48, 0.86] | wt_med |
| 1 | 0.23 [0.12, 0.35] | -0.20 [-0.37, -0.08]* | -0.21 [-0.46, -0.05]* | -0.20 [-0.37, -0.08]* | -0.25 [-0.47, -0.09]* | -0.20 [-0.37, -0.08]* | 0.25 [-0.19, 0.60] |  |  | 0.59 [0.41, 0.78] | wt_med |
| 2 | 0.21 [0.10, 0.34] | -0.21 [-0.38, -0.08]* | -0.22 [-0.47, -0.06]* | -0.21 [-0.38, -0.08]* | -0.26 [-0.49, -0.09]* | -0.21 [-0.38, -0.08]* | 0.22 [-0.22, 0.57] |  |  | 0.56 [0.38, 0.76] | wt_med |
| 3 | 0.20 [0.09, 0.33] | -0.21 [-0.38, -0.09]* | -0.23 [-0.49, -0.06]* | -0.21 [-0.38, -0.09]* | -0.26 [-0.50, -0.09]* | -0.21 [-0.38, -0.09]* | 0.19 [-0.25, 0.55] | 1.66 [1.40, 1.94] | 1.79 [1.51, 2.08] | 0.54 [0.35, 0.73] | wt_med |
| 4 | 0.19 [0.08, 0.32] | -0.21 [-0.37, -0.09]* | -0.24 [-0.48, -0.07]* | -0.21 [-0.37, -0.09]* | -0.26 [-0.49, -0.10]* | -0.21 [-0.38, -0.09]* | 0.17 [-0.27, 0.54] | 1.66 [1.41, 1.93] | 1.80 [1.55, 2.07] | 0.52 [0.34, 0.71] | wt_med |
| 5 | 0.18 [0.08, 0.32] | -0.21 [-0.37, -0.09]* | -0.24 [-0.47, -0.08]* | -0.21 [-0.37, -0.09]* | -0.26 [-0.49, -0.10]* | -0.21 [-0.38, -0.09]* | 0.15 [-0.30, 0.52] | 1.68 [1.41, 1.95] | 1.82 [1.56, 2.10] | 0.50 [0.32, 0.69] | wt_med |
| 6 | 0.16 [0.05, 0.29] | -0.21 [-0.36, -0.09]* | -0.23 [-0.43, -0.08]* | -0.21 [-0.36, -0.09]* | -0.25 [-0.47, -0.10]* | -0.20 [-0.36, -0.09]* | 0.12 [-0.32, 0.49] | 1.62 [1.36, 1.90] | 1.76 [1.49, 2.05] | 0.47 [0.29, 0.65] | wt_med |
| 7 | 0.12 [0.01, 0.24] | -0.21 [-0.35, -0.10]* | -0.22 [-0.41, -0.08]* | -0.21 [-0.35, -0.10]* | -0.25 [-0.44, -0.11]* | -0.21 [-0.35, -0.10]* | 0.47 [0.01, 0.86] | 1.64 [1.38, 1.91] | 1.78 [1.52, 2.05] |  | wt_med |
| 10 | 0.08 [-0.03, 0.20] | -0.20 [-0.33, -0.10]* | -0.22 [-0.41, -0.08]* | -0.20 [-0.33, -0.10]* | -0.24 [-0.42, -0.11]* | -0.20 [-0.33, -0.10]* | 0.43 [-0.04, 0.83] | 1.64 [1.36, 1.93] | 1.80 [1.52, 2.10] |  | wt_med |
| 14 | 0.06 [-0.05, 0.19] | -0.20 [-0.34, -0.10]* | -0.21 [-0.39, -0.07]* | -0.20 [-0.34, -0.10]* | -0.25 [-0.42, -0.11]* | -0.20 [-0.34, -0.10]* | 0.65 [0.15, 1.07] | 1.70 [1.41, 1.98] | 1.88 [1.58, 2.17] |  | wt_med |
| 21 | 0.04 [-0.09, 0.17] | -0.21 [-0.34, -0.10]* | -0.23 [-0.42, -0.08]* | -0.21 [-0.34, -0.10]* | -0.25 [-0.42, -0.11]* | -0.21 [-0.34, -0.10]* | 0.76 [0.26, 1.20] | 1.64 [1.35, 1.91] | 1.82 [1.53, 2.11] |  | wt_med |
| 30 | 0.05 [-0.07, 0.19] | -0.20 [-0.33, -0.09]* | -0.25 [-0.48, -0.08]* | -0.20 [-0.33, -0.09]* | -0.25 [-0.44, -0.10]* | -0.20 [-0.33, -0.09]* | 0.74 [0.25, 1.21] | 1.71 [1.42, 2.00] | 1.88 [1.59, 2.19] |  | wt_med |
| 45 | 0.03 [-0.10, 0.17] | -0.16 [-0.28, -0.07]* | -0.39 [-0.76, -0.02]* | -0.16 [-0.28, -0.07]* | -0.24 [-0.42, -0.10]* | -0.16 [-0.28, -0.07]* | 0.88 [0.41, 1.33] | 1.74 [1.45, 2.02] | 1.91 [1.60, 2.21] |  | wt_med |
| 60 | -0.08 [-0.25, 0.09] | -0.13 [-0.26, -0.04] |  | -0.13 [-0.26, -0.04] | 0.00 [0.00, 0.00] | -0.13 [-0.26, -0.04] | 1.03 [0.57, 1.51] | 0.46 [0.14, 2.21] | 0.52 [0.21, 2.42] |  | wt_med |
| 90 | -0.08 [-0.28, 0.09] | -0.09 [-0.20, -0.02] |  | -0.09 [-0.20, -0.02] |  | -0.09 [-0.20, -0.02] | 0.87 [0.31, 1.44] |  |  |  | wt_med |

**first-hour MAE, schedule projected at origin**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.83 [4.69, 4.99] | 4.49 [4.36, 4.63] | 4.13 [3.96, 4.32] | 4.48 [4.34, 4.62] | 4.16 [4.01, 4.31] | 4.43 [4.30, 4.57] | 4.29 [4.13, 4.48] |  |  | 5.46 [5.28, 5.63] | 4.50 [4.37, 4.65] |
| 1 | 4.84 [4.70, 5.00] | 4.54 [4.40, 4.68] | 4.19 [4.01, 4.37] | 4.53 [4.39, 4.67] | 4.22 [4.06, 4.37] | 4.47 [4.35, 4.61] | 4.30 [4.15, 4.48] |  |  | 5.44 [5.27, 5.61] | 4.56 [4.42, 4.70] |
| 2 | 4.86 [4.72, 5.02] | 4.59 [4.45, 4.73] | 4.22 [4.03, 4.40] | 4.57 [4.44, 4.71] | 4.27 [4.12, 4.44] | 4.51 [4.38, 4.65] | 4.32 [4.17, 4.50] |  |  | 5.46 [5.29, 5.63] | 4.60 [4.47, 4.74] |
| 3 | 4.89 [4.75, 5.05] | 4.62 [4.49, 4.77] | 4.25 [4.06, 4.45] | 4.61 [4.48, 4.75] | 4.31 [4.15, 4.47] | 4.55 [4.42, 4.69] | 4.35 [4.20, 4.53] | 5.69 [5.52, 5.85] | 5.94 [5.77, 6.10] | 5.47 [5.30, 5.64] | 4.64 [4.51, 4.79] |
| 4 | 4.91 [4.77, 5.06] | 4.67 [4.54, 4.82] | 4.31 [4.13, 4.51] | 4.65 [4.52, 4.80] | 4.38 [4.22, 4.56] | 4.59 [4.46, 4.73] | 4.38 [4.23, 4.57] | 5.73 [5.56, 5.90] | 5.97 [5.79, 6.13] | 5.45 [5.29, 5.63] | 4.70 [4.56, 4.85] |
| 5 | 4.98 [4.85, 5.14] | 4.77 [4.63, 4.92] | 4.44 [4.24, 4.63] | 4.76 [4.62, 4.90] | 4.46 [4.29, 4.63] | 4.69 [4.55, 4.83] | 4.47 [4.31, 4.65] | 5.85 [5.66, 6.03] | 6.06 [5.88, 6.24] | 5.49 [5.33, 5.66] | 4.80 [4.66, 4.95] |
| 6 | 5.09 [4.94, 5.23] | 4.93 [4.78, 5.08] | 4.61 [4.40, 4.83] | 4.92 [4.77, 5.07] | 4.59 [4.42, 4.76] | 4.82 [4.68, 4.96] | 4.55 [4.38, 4.74] | 5.93 [5.74, 6.11] | 6.12 [5.94, 6.29] | 5.54 [5.37, 5.71] | 4.97 [4.81, 5.12] |
| 7 | 5.17 [5.03, 5.33] | 5.07 [4.92, 5.22] | 4.79 [4.56, 5.02] | 5.08 [4.93, 5.26] | 4.74 [4.56, 4.92] | 4.95 [4.81, 5.10] | 4.96 [4.79, 5.16] | 6.22 [6.03, 6.42] | 6.42 [6.23, 6.62] |  | 5.13 [4.98, 5.29] |
| 10 | 5.39 [5.24, 5.54] | 5.33 [5.18, 5.49] | 5.23 [4.96, 5.49] | 5.38 [5.22, 5.56] | 5.13 [4.92, 5.34] | 5.19 [5.04, 5.35] | 5.13 [4.96, 5.33] | 6.64 [6.41, 6.86] | 6.78 [6.57, 7.00] |  | 5.40 [5.25, 5.56] |
| 14 | 5.58 [5.44, 5.73] | 5.59 [5.44, 5.75] | 5.65 [5.40, 5.90] | 5.65 [5.49, 5.82] | 5.55 [5.34, 5.75] | 5.46 [5.32, 5.61] | 5.48 [5.30, 5.67] | 6.95 [6.73, 7.17] | 7.07 [6.87, 7.27] |  | 5.66 [5.50, 5.82] |
| 21 | 5.74 [5.59, 5.89] | 5.75 [5.60, 5.90] | 5.95 [5.67, 6.21] | 5.83 [5.67, 6.00] | 5.80 [5.57, 6.02] | 5.63 [5.48, 5.77] | 5.78 [5.59, 5.97] | 7.44 [7.21, 7.70] | 7.60 [7.39, 7.85] |  | 5.81 [5.66, 5.97] |
| 30 | 5.66 [5.51, 5.80] | 5.76 [5.62, 5.90] | 5.99 [5.73, 6.24] | 5.85 [5.70, 6.00] | 6.17 [5.90, 6.44] | 5.61 [5.47, 5.75] | 6.07 [5.90, 6.26] | 7.84 [7.57, 8.12] | 7.97 [7.71, 8.24] |  | 5.82 [5.67, 5.96] |
| 45 | 5.90 [5.76, 6.05] | 5.99 [5.85, 6.13] | 4.46 [4.24, 4.69] | 6.06 [5.91, 6.21] | 6.40 [6.11, 6.68] | 5.86 [5.72, 6.01] | 6.32 [6.16, 6.49] | 8.33 [8.04, 8.65] | 8.48 [8.19, 8.77] |  | 6.07 [5.92, 6.22] |
| 60 | 6.08 [5.94, 6.24] | 6.02 [5.87, 6.17] |  | 6.08 [5.93, 6.23] | 2.60 [1.22, 3.62] | 5.91 [5.77, 6.07] | 6.34 [6.19, 6.51] | 3.27 [1.55, 4.34] | 3.34 [1.64, 4.46] |  | 6.11 [5.97, 6.26] |
| 90 | 6.08 [5.92, 6.24] | 5.94 [5.79, 6.09] |  | 6.04 [5.89, 6.19] |  | 5.92 [5.77, 6.07] | 6.32 [6.17, 6.46] |  |  |  | 6.00 [5.84, 6.15] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.30 [0.20, 0.41] | -0.02 [-0.04, 0.00] | 0.01 [-0.02, 0.04] | -0.01 [-0.04, 0.00] | -0.02 [-0.07, 0.03] | -0.07 [-0.18, -0.01]* | -0.25 [-0.62, 0.24] |  |  | 0.84 [0.44, 1.30] | wt_med |
| 1 | 0.27 [0.19, 0.39] | -0.02 [-0.04, -0.00]* | -0.01 [-0.07, 0.02] | -0.02 [-0.05, 0.00] | -0.01 [-0.07, 0.03] | -0.08 [-0.20, -0.01]* | -0.28 [-0.66, 0.22] |  |  | 0.77 [0.34, 1.28] | wt_med |
| 2 | 0.25 [0.16, 0.37] | -0.02 [-0.03, -0.00]* | -0.01 [-0.06, 0.02] | -0.02 [-0.05, 0.00] | -0.00 [-0.06, 0.05] | -0.08 [-0.21, -0.02]* | -0.30 [-0.69, 0.22] |  |  | 0.75 [0.33, 1.25] | wt_med |
| 3 | 0.25 [0.15, 0.38] | -0.02 [-0.05, -0.00]* | -0.01 [-0.07, 0.03] | -0.03 [-0.06, 0.00] | -0.00 [-0.06, 0.04] | -0.09 [-0.23, -0.02]* | -0.32 [-0.71, 0.20] | 1.38 [0.83, 1.91] | 1.63 [1.01, 2.18] | 0.73 [0.32, 1.18] | wt_med |
| 4 | 0.25 [0.14, 0.37] | -0.03 [-0.06, -0.00]* | -0.02 [-0.09, 0.02] | -0.04 [-0.09, -0.00]* | -0.01 [-0.07, 0.04] | -0.11 [-0.27, -0.03]* | -0.34 [-0.74, 0.17] | 1.35 [0.79, 1.87] | 1.59 [0.97, 2.13] | 0.65 [0.25, 1.09] | wt_med |
| 5 | 0.21 [0.08, 0.35] | -0.03 [-0.08, -0.01]* | -0.03 [-0.13, 0.02] | -0.04 [-0.09, -0.00]* | -0.04 [-0.14, 0.02] | -0.12 [-0.28, -0.03]* | -0.36 [-0.77, 0.15] | 1.37 [0.86, 1.89] | 1.58 [1.02, 2.13] | 0.58 [0.18, 0.99] | wt_med |
| 6 | 0.16 [-0.01, 0.32] | -0.05 [-0.10, -0.01]* | -0.07 [-0.26, 0.02] | -0.04 [-0.10, -0.01]* | -0.08 [-0.27, 0.03] | -0.15 [-0.34, -0.04]* | -0.43 [-0.85, 0.06] | 1.28 [0.80, 1.77] | 1.47 [0.91, 2.00] | 0.46 [0.03, 0.88] | wt_med |
| 7 | 0.10 [-0.10, 0.28] | -0.06 [-0.14, -0.01] | -0.10 [-0.36, 0.02] | -0.01 [-0.06, 0.04] | -0.11 [-0.36, 0.02] | -0.18 [-0.40, -0.05]* | -0.14 [-0.57, 0.43] | 1.40 [0.91, 1.87] | 1.60 [1.02, 2.14] |  | wt_med |
| 10 | 0.03 [-0.22, 0.26] | -0.07 [-0.19, -0.01] | -0.07 [-0.26, 0.05] | -0.00 [-0.06, 0.07] | -0.08 [-0.25, 0.04] | -0.21 [-0.46, -0.05]* | -0.26 [-0.74, 0.34] | 1.46 [0.89, 2.01] | 1.61 [0.96, 2.24] |  | wt_med |
| 14 | -0.04 [-0.29, 0.19] | -0.07 [-0.18, -0.00] | -0.09 [-0.31, 0.05] | 0.02 [-0.04, 0.11] | -0.04 [-0.15, 0.06] | -0.20 [-0.41, -0.06] | -0.17 [-0.68, 0.44] | 1.40 [0.83, 1.96] | 1.52 [0.90, 2.16] |  | wt_med |
| 21 | -0.06 [-0.31, 0.18] | -0.07 [-0.18, -0.00] | -0.10 [-0.35, 0.07] | 0.03 [-0.02, 0.12] | -0.01 [-0.09, 0.07] | -0.19 [-0.38, -0.06] | -0.07 [-0.62, 0.62] | 1.63 [1.20, 2.14] | 1.79 [1.28, 2.44] |  | wt_med |
| 30 | -0.13 [-0.38, 0.12] | -0.07 [-0.17, -0.01] | -0.17 [-0.53, 0.07] | 0.04 [-0.02, 0.13] | 0.07 [-0.08, 0.30] | -0.21 [-0.40, -0.09] | 0.21 [-0.46, 0.87] | 1.77 [1.40, 2.26] | 1.90 [1.39, 2.50] |  | wt_med |
| 45 | -0.13 [-0.38, 0.11] | -0.09 [-0.19, -0.02] | -0.10 [-0.31, 0.02] | -0.01 [-0.07, 0.05] | 0.04 [-0.07, 0.17] | -0.21 [-0.37, -0.10] | 0.18 [-0.52, 0.86] | 2.07 [1.64, 2.59] | 2.22 [1.65, 2.81] |  | wt_med |
| 60 | -0.01 [-0.23, 0.26] | -0.10 [-0.20, -0.03] |  | -0.03 [-0.13, 0.06] | 0.00 [0.00, 0.00] | -0.20 [-0.35, -0.08] | 0.15 [-0.60, 0.89] | 0.66 [0.00, 1.53] | 0.73 [0.01, 3.37] |  | wt_med |
| 90 | 0.12 [-0.13, 0.48] | -0.08 [-0.16, -0.03] |  | 0.00 [-0.09, 0.09] |  | -0.10 [-0.26, 0.06] | 0.27 [-0.50, 1.00] |  |  |  | wt_med |

**rope-drop worth agreement**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 1 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 2 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 3 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 4 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 5 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 6 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 7 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 10 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 14 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 21 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 30 | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] | 0.90 [0.89, 0.90] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.92] |
| 45 | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.87 [0.86, 0.87] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.92] |
| 60 | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] |  | 0.91 [0.91, 0.91] | 0.99 [0.99, 1.00] | 0.90 [0.90, 0.90] | 0.99 [0.99, 1.00] | 0.99 [0.99, 1.00] |  | 0.91 [0.91, 0.92] |
| 90 | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |  | 0.91 [0.91, 0.92] |  | 0.90 [0.90, 0.91] |  |  |  | 0.91 [0.91, 0.92] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, 0.00] |  |  | -0.01 [-0.01, -0.01] | wt_med |
| 1 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, 0.00] |  |  | -0.01 [-0.01, -0.01] | wt_med |
| 2 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] |  |  | -0.01 [-0.01, -0.01] | wt_med |
| 3 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 4 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 5 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 6 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 7 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  | wt_med |
| 10 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 14 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 21 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 30 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 45 | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] | -0.01 [-0.02, 0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  | wt_med |
| 60 | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] |  | -0.00 [-0.00, -0.00] | -0.00 [-0.02, 0.00] | -0.01 [-0.02, -0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] |  | wt_med |
| 90 | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] |  | -0.00 [-0.01, -0.00] |  | -0.01 [-0.02, -0.00] |  |  |  | wt_med |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|
| 1 | 0.20 [0.20, 0.21] | 0.27 [0.27, 0.28] | 0.37 [0.36, 0.37] | 0.40 [0.39, 0.41] | 0.40 [0.39, 0.41] | 0.32 [0.31, 0.32] |
| 2 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.37 [0.36, 0.37] | 0.40 [0.39, 0.41] | 0.38 [0.37, 0.39] | 0.31 [0.31, 0.32] |
| 3 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.38 [0.37, 0.39] | 0.31 [0.31, 0.32] |
| 4 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.37 [0.36, 0.38] | 0.31 [0.31, 0.32] |
| 5 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.37 [0.36, 0.38] | 0.31 [0.30, 0.32] |
| 6 | 0.19 [0.18, 0.20] | 0.27 [0.26, 0.27] | 0.37 [0.37, 0.38] | 0.40 [0.39, 0.41] | 0.36 [0.35, 0.37] | 0.31 [0.30, 0.31] |
| 7 | 0.19 [0.18, 0.20] | 0.26 [0.26, 0.27] | 0.34 [0.33, 0.34] |  | 0.34 [0.33, 0.35] | 0.30 [0.29, 0.30] |
| 10 | 0.19 [0.18, 0.20] | 0.26 [0.25, 0.27] | 0.34 [0.33, 0.35] |  | 0.33 [0.33, 0.34] | 0.29 [0.29, 0.30] |
| 14 | 0.18 [0.17, 0.19] | 0.26 [0.25, 0.27] | 0.31 [0.30, 0.31] |  | 0.31 [0.31, 0.32] | 0.28 [0.28, 0.29] |
| 21 | 0.18 [0.17, 0.19] | 0.25 [0.25, 0.26] | 0.29 [0.28, 0.30] |  | 0.29 [0.28, 0.30] | 0.27 [0.27, 0.28] |
| 30 | 0.17 [0.16, 0.18] | 0.25 [0.24, 0.26] | 0.28 [0.27, 0.29] |  | 0.27 [0.26, 0.28] | 0.27 [0.26, 0.27] |
| 45 | 0.23 [0.21, 0.25] | 0.25 [0.24, 0.25] | 0.27 [0.26, 0.28] |  | 0.26 [0.24, 0.27] | 0.26 [0.25, 0.27] |
| 60 |  | 0.24 [0.23, 0.24] | 0.27 [0.26, 0.28] |  | 0.21 [0.11, 0.31] | 0.26 [0.25, 0.27] |
| 90 |  | 0.23 [0.22, 0.24] | 0.28 [0.27, 0.29] |  |  | 0.26 [0.25, 0.27] |
| 120 |  | 0.22 [0.21, 0.23] | 0.28 [0.26, 0.29] |  |  | 0.25 [0.23, 0.26] |
| 180 |  | 0.21 [0.19, 0.23] | 0.26 [0.24, 0.28] |  |  | 0.24 [0.22, 0.26] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|
| 1 | -0.20 [-0.23, -0.17] | -0.12 [-0.14, -0.10] | -0.03 [-0.04, -0.01] | -0.00 [-0.02, 0.01] | -0.08 [-0.10, -0.06] | lvl_snaive7 |
| 2 | -0.20 [-0.23, -0.17] | -0.12 [-0.15, -0.10] | -0.03 [-0.04, -0.01] | -0.02 [-0.03, -0.01] | -0.08 [-0.10, -0.06] | lvl_snaive7 |
| 3 | -0.20 [-0.23, -0.17] | -0.13 [-0.15, -0.10] | -0.03 [-0.04, -0.01] | -0.02 [-0.04, -0.01] | -0.08 [-0.10, -0.06] | lvl_snaive7 |
| 4 | -0.20 [-0.23, -0.17] | -0.13 [-0.15, -0.10] | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.08 [-0.10, -0.07] | lvl_snaive7 |
| 5 | -0.20 [-0.23, -0.17] | -0.13 [-0.16, -0.11] | -0.02 [-0.04, -0.01] | -0.04 [-0.05, -0.02] | -0.09 [-0.11, -0.07] | lvl_snaive7 |
| 6 | -0.21 [-0.25, -0.18] | -0.13 [-0.16, -0.11] | -0.02 [-0.04, -0.01] | -0.04 [-0.06, -0.03] | -0.09 [-0.11, -0.07] | lvl_snaive7 |
| 7 | -0.16 [-0.18, -0.13] | -0.07 [-0.09, -0.06] |  | 0.00 [-0.01, 0.02] | -0.04 [-0.05, -0.03] | lvl_naive4 |
| 10 | -0.15 [-0.18, -0.12] | -0.08 [-0.10, -0.06] |  | -0.01 [-0.02, 0.01] | -0.05 [-0.06, -0.03] | lvl_naive4 |
| 14 | -0.12 [-0.15, -0.09] | -0.05 [-0.07, -0.03] |  | 0.01 [-0.01, 0.02] | -0.02 [-0.03, -0.01] | lvl_naive4 |
| 21 | -0.11 [-0.14, -0.08] | -0.04 [-0.06, -0.02] |  | 0.01 [-0.01, 0.02] | -0.02 [-0.03, -0.01] | lvl_naive4 |
| 30 | -0.10 [-0.13, -0.07] | -0.03 [-0.05, -0.01] |  | 0.00 [-0.02, 0.02] | -0.01 [-0.03, -0.00] | lvl_naive4 |
| 45 | -0.07 [-0.12, -0.02] | -0.02 [-0.05, -0.01] |  | 0.00 [-0.02, 0.02] | -0.01 [-0.02, 0.00] | lvl_naive4 |
| 60 |  | -0.03 [-0.05, -0.01] |  | 0.05 [-0.06, 0.13] | -0.01 [-0.02, 0.01] | lvl_naive4 |
| 90 |  | -0.05 [-0.07, -0.03] |  |  | -0.02 [-0.03, -0.00] | lvl_naive4 |
| 120 |  | -0.06 [-0.08, -0.03] |  |  | -0.03 [-0.05, -0.01] | lvl_naive4 |
| 180 |  | -0.05 [-0.10, 0.01] |  |  | -0.02 [-0.06, 0.02] | lvl_naive4 |

| lead | model | park_days | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|
| 1 | lvl_naive4 | 15663 | 0.77 | 0.61 | 0.63 |
| 1 | lvl_wt56 | 15812 | 0.75 | 0.50 | 0.62 |
| 1 | lvl_snaive7 | 15240 | 0.78 | 0.66 | 0.65 |
| 1 | lvl_clim | 15812 | 0.70 | 0.38 | 0.60 |
| 1 | lvl_tft | 11511 | 0.81 | 0.65 | 0.69 |
| 1 | lvl_cbd | 8757 | 0.58 | 0.15 | 0.68 |
| 7 | lvl_naive4 | 14958 | 0.73 | 0.57 | 0.59 |
| 7 | lvl_wt56 | 15157 | 0.72 | 0.48 | 0.59 |
| 7 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 7 | lvl_clim | 15155 | 0.69 | 0.37 | 0.59 |
| 7 | lvl_tft | 10824 | 0.76 | 0.59 | 0.64 |
| 7 | lvl_cbd | 8380 | 0.52 | 0.11 | 0.58 |
| 30 | lvl_naive4 | 12695 | 0.67 | 0.52 | 0.53 |
| 30 | lvl_wt56 | 12935 | 0.67 | 0.42 | 0.54 |
| 30 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_clim | 12928 | 0.68 | 0.36 | 0.59 |
| 30 | lvl_tft | 7196 | 0.66 | 0.57 | 0.55 |
| 30 | lvl_cbd | 6517 | 0.49 | 0.10 | 0.56 |
| 90 | lvl_naive4 | 7373 | 0.68 | 0.51 | 0.55 |
| 90 | lvl_wt56 | 7516 | 0.70 | 0.40 | 0.58 |
| 90 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_clim | 7522 | 0.67 | 0.36 | 0.59 |
| 90 | lvl_tft | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_cbd | 0 | 0.00 | 0.00 | 0.00 |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 264430 | 167732 | 0.63 |
| 1 | lvl_clim | 518286 | 315421 | 0.61 |
| 1 | lvl_naive4 | 510202 | 358132 | 0.70 |
| 1 | lvl_snaive7 | 481216 | 349823 | 0.73 |
| 1 | lvl_tft | 422992 | 309474 | 0.73 |
| 1 | lvl_wt56 | 518286 | 347917 | 0.67 |
| 7 | lvl_cbd | 255948 | 144318 | 0.56 |
| 7 | lvl_clim | 491060 | 290028 | 0.59 |
| 7 | lvl_naive4 | 480665 | 321110 | 0.67 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 394632 | 269245 | 0.68 |
| 7 | lvl_wt56 | 491180 | 318394 | 0.65 |
| 30 | lvl_cbd | 192437 | 106259 | 0.55 |
| 30 | lvl_clim | 398256 | 224344 | 0.56 |
| 30 | lvl_naive4 | 386101 | 236674 | 0.61 |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 257158 | 161278 | 0.63 |
| 30 | lvl_wt56 | 398676 | 235638 | 0.59 |
| 90 | lvl_cbd | 0 | 0 |  |
| 90 | lvl_clim | 187545 | 107128 | 0.57 |
| 90 | lvl_naive4 | 181049 | 109197 | 0.60 |
| 90 | lvl_snaive7 | 0 | 0 |  |
| 90 | lvl_tft | 0 | 0 |  |
| 90 | lvl_wt56 | 187238 | 111572 | 0.60 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie (counted wrong); CI over (origin, park).

**day comparison winner accuracy**

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|
| d1-7 | 0.23 [0.22, 0.23] | 0.27 [0.26, 0.27] | 0.45 [0.44, 0.45] | 0.48 [0.47, 0.48] | 0.41 [0.41, 0.42] | 0.27 [0.27, 0.28] |
| d8-30 | 0.16 [0.16, 0.16] | 0.24 [0.24, 0.24] | 0.36 [0.36, 0.36] |  | 0.36 [0.36, 0.37] | 0.23 [0.22, 0.23] |
| d31-90 | 0.14 [0.14, 0.15] | 0.24 [0.24, 0.24] | 0.34 [0.33, 0.34] |  | 0.35 [0.35, 0.36] | 0.22 [0.22, 0.22] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|
| d1-7 | -0.23 [-0.24, -0.21] | -0.21 [-0.23, -0.19] | -0.02 [-0.04, -0.01] | -0.06 [-0.08, -0.05] | -0.20 [-0.22, -0.18] | lvl_snaive7 |
| d8-30 | -0.18 [-0.20, -0.16] | -0.12 [-0.14, -0.11] |  | -0.00 [-0.01, 0.01] | -0.13 [-0.15, -0.12] | lvl_naive4 |
| d31-90 | -0.17 [-0.19, -0.15] | -0.09 [-0.11, -0.08] |  | 0.03 [0.01, 0.04]* | -0.12 [-0.13, -0.11] | lvl_naive4 |

| lead | model | tie_rate |
|---|---|---|
| d1-7 | lvl_naive4 | 0.37 |
| d1-7 | lvl_wt56 | 0.61 |
| d1-7 | lvl_snaive7 | 0.30 |
| d1-7 | lvl_clim | 0.61 |
| d1-7 | lvl_tft | 0.42 |
| d1-7 | lvl_cbd | 0.65 |
| d8-30 | lvl_naive4 | 0.44 |
| d8-30 | lvl_wt56 | 0.65 |
| d8-30 | lvl_clim | 0.64 |
| d8-30 | lvl_tft | 0.43 |
| d8-30 | lvl_cbd | 0.73 |
| d31-90 | lvl_naive4 | 0.44 |
| d31-90 | lvl_wt56 | 0.65 |
| d31-90 | lvl_clim | 0.61 |
| d31-90 | lvl_tft | 0.44 |
| d31-90 | lvl_cbd | 0.75 |

Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the 1st and 15th):

| model | star_precision | star_recall | pred_stars | true_stars |
|---|---|---|---|---|
| lvl_cbd | 0.32 [0.27, 0.36] | 0.08 [0.07, 0.10] | 900 | 3442 |
| lvl_clim | 0.37 [0.34, 0.41] | 0.08 [0.07, 0.10] | 1707 | 7538 |
| lvl_naive4 | 0.40 [0.38, 0.41] | 0.29 [0.27, 0.31] | 5492 | 7454 |
| lvl_snaive7 | 0.32 [0.28, 0.36] | 0.33 [0.29, 0.38] | 543 | 522 |
| lvl_tft | 0.36 [0.33, 0.39] | 0.25 [0.23, 0.27] | 2828 | 4114 |
| lvl_wt56 | 0.36 [0.32, 0.40] | 0.09 [0.08, 0.10] | 1867 | 7549 |

## D8 — uncertainty

**What production states** (`forecast-accuracy.service.ts`): MAE of TFT's predicted peak against the day's max hourly P90, by predicted band × lead bucket over the 45 days before the origin (cells with ≥ 500 comparisons). Realised = the same error of the TFT forecasts served at the origin; ratio > 1 = production understates its error.

| bucket | band | n | realised_mae | stated_mae | ratio |
|---|---|---|---|---|---|
| d1 | busy | 16350.00 | 22.56 | 22.10 | 1.02 |
| d1 | mid | 59100.00 | 14.32 | 14.28 | 1.00 |
| d1 | quiet | 169178.00 | 6.84 | 6.82 | 1.00 |
| d14 | busy | 101820.00 | 25.02 | 24.66 | 1.01 |
| d14 | mid | 353356.00 | 16.05 | 15.76 | 1.02 |
| d14 | quiet | 1020966.00 | 7.88 | 7.66 | 1.03 |
| d3 | busy | 32593.00 | 22.93 | 22.50 | 1.02 |
| d3 | mid | 115885.00 | 14.63 | 14.54 | 1.01 |
| d3 | quiet | 329291.00 | 6.97 | 6.94 | 1.00 |
| d30 | busy | 199794.00 | 26.21 | 25.20 | 1.04 |
| d30 | mid | 682116.00 | 16.70 | 16.25 | 1.03 |
| d30 | quiet | 1895434.00 | 8.33 | 8.05 | 1.03 |
| d60 | busy | 156031.00 | 28.56 | 23.77 | 1.20 |
| d60 | mid | 472036.00 | 17.14 | 17.38 | 0.99 |
| d60 | quiet | 1143122.00 | 7.62 | 9.83 | 0.78 |
| d7 | busy | 63557.00 | 23.72 | 23.43 | 1.01 |
| d7 | mid | 221276.00 | 15.12 | 14.93 | 1.01 |
| d7 | quiet | 632562.00 | 7.25 | 7.12 | 1.02 |

Empirical-quantile coverage (target 0.80 / 0.95) and the share of truth slots that HAVE an interval:

| L | wt_med cov_q80 | wt_med cov_q95 | wt_med field_coverage |
|---|---|---|---|
| 0 | 0.84 | 0.92 | 0.96 |
| 1 | 0.84 | 0.92 | 0.96 |
| 2 | 0.84 | 0.92 | 0.96 |
| 3 | 0.84 | 0.92 | 0.96 |
| 4 | 0.84 | 0.92 | 0.96 |
| 5 | 0.84 | 0.92 | 0.96 |
| 6 | 0.84 | 0.92 | 0.96 |
| 7 | 0.83 | 0.91 | 0.95 |
| 10 | 0.83 | 0.91 | 0.95 |
| 14 | 0.83 | 0.91 | 0.94 |
| 21 | 0.83 | 0.90 | 0.94 |
| 30 | 0.82 | 0.90 | 0.93 |
| 45 | 0.83 | 0.90 | 0.91 |
| 60 | 0.83 | 0.91 | 0.89 |
| 90 | 0.84 | 0.91 | 0.86 |

Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:

| model | L | realised | stated_weighted | ratio |
|---|---|---|---|---|
| snaive7 | 0 | 8.69 | 8.80 | 0.99 |
| snaive7 | 1 | 8.68 | 8.81 | 0.98 |
| snaive7 | 3 | 8.65 | 8.83 | 0.98 |
| wt_med | 0 | 7.55 | 7.61 | 0.99 |
| wt_med | 1 | 7.65 | 7.72 | 0.99 |
| wt_med | 3 | 7.72 | 7.79 | 0.99 |
| wt_med | 7 | 7.92 | 8.02 | 0.99 |
| wt_med | 14 | 8.09 | 8.25 | 0.98 |
| wt_med | 30 | 8.30 | 8.66 | 0.96 |
| wt_med | 60 | 8.44 | 8.62 | 0.98 |
| wt_med | 90 | 7.97 | 8.96 | 0.89 |
| clim | 0 | 8.06 | 8.16 | 0.99 |
| clim | 1 | 8.08 | 8.18 | 0.99 |
| clim | 3 | 8.11 | 8.20 | 0.99 |
| clim | 7 | 8.19 | 8.27 | 0.99 |
| clim | 14 | 8.29 | 8.43 | 0.98 |
| clim | 30 | 8.36 | 8.75 | 0.96 |
| clim | 60 | 8.27 | 8.62 | 0.96 |
| clim | 90 | 8.24 | 8.89 | 0.93 |
| h5 | 0 | 7.48 | 7.54 | 0.99 |
| h5 | 1 | 7.58 | 7.66 | 0.99 |
| h5 | 3 | 7.64 | 7.71 | 0.99 |
| h5 | 7 | 7.83 | 7.92 | 0.99 |
| h5 | 14 | 7.99 | 8.14 | 0.98 |
| h5 | 30 | 8.17 | 8.50 | 0.96 |
| h5 | 60 | 8.31 | 8.39 | 0.99 |
| h5 | 90 | 7.89 | 8.70 | 0.91 |
| lvlh5_naive | 0 | 7.83 | 7.91 | 0.99 |
| lvlh5_naive | 1 | 7.84 | 7.93 | 0.99 |
| lvlh5_naive | 3 | 7.86 | 7.94 | 0.99 |
| lvlh5_naive | 7 | 8.19 | 8.25 | 0.99 |
| lvlh5_naive | 14 | 8.42 | 8.45 | 1.00 |
| lvlh5_naive | 30 | 8.63 | 8.82 | 0.98 |
| lvlh5_naive | 60 | 8.80 | 8.74 | 1.01 |
| lvlh5_naive | 90 | 8.43 | 8.80 | 0.96 |
| lvlh5_tft | 0 | 7.35 | 7.41 | 0.99 |
| lvlh5_tft | 1 | 7.49 | 7.51 | 1.00 |
| lvlh5_tft | 3 | 7.64 | 7.64 | 1.00 |
| lvlh5_tft | 7 | 8.00 | 7.96 | 1.00 |
| lvlh5_tft | 14 | 8.42 | 8.25 | 1.02 |
| lvlh5_tft | 30 | 9.07 | 8.74 | 1.04 |
| lvlh5_cbd | 0 | 9.97 | 9.88 | 1.01 |
| lvlh5_cbd | 1 | 10.05 | 9.95 | 1.01 |
| lvlh5_cbd | 3 | 10.10 | 9.92 | 1.02 |
| lvlh5_cbd | 7 | 10.67 | 10.44 | 1.02 |
| lvlh5_cbd | 14 | 10.95 | 10.58 | 1.03 |
| lvlh5_cbd | 30 | 11.19 | 11.21 | 1.00 |
| prod_served | 3 | 8.10 | 8.08 | 1.00 |
| prod_served | 7 | 8.45 | 8.41 | 1.00 |
| prod_served | 14 | 8.92 | 8.79 | 1.01 |
| prod_served | 30 | 9.83 | 9.25 | 1.06 |
| prod_served_lin | 3 | 8.07 | 8.06 | 1.00 |
| prod_served_lin | 7 | 8.43 | 8.39 | 1.00 |
| prod_served_lin | 14 | 8.90 | 8.77 | 1.01 |
| prod_served_lin | 30 | 9.80 | 9.23 | 1.06 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.89 | 0.97 | 0.54 | 0.92 | 0.67 | 0.97 | 0.92 |  |  | 0.82 | 0.96 |
| 1 | 0.89 | 0.97 | 0.53 | 0.92 | 0.67 | 0.97 | 0.92 |  |  | 0.83 | 0.96 |
| 2 | 0.89 | 0.97 | 0.53 | 0.93 | 0.67 | 0.97 | 0.92 |  |  | 0.83 | 0.96 |
| 3 | 0.89 | 0.97 | 0.53 | 0.93 | 0.66 | 0.96 | 0.93 | 0.65 | 0.65 | 0.83 | 0.96 |
| 4 | 0.89 | 0.97 | 0.53 | 0.93 | 0.66 | 0.96 | 0.93 | 0.65 | 0.65 | 0.83 | 0.96 |
| 5 | 0.88 | 0.97 | 0.53 | 0.93 | 0.66 | 0.96 | 0.93 | 0.64 | 0.64 | 0.84 | 0.96 |
| 6 | 0.88 | 0.97 | 0.53 | 0.93 | 0.65 | 0.96 | 0.93 | 0.64 | 0.64 | 0.84 | 0.96 |
| 7 | 0.88 | 0.96 | 0.52 | 0.91 | 0.65 | 0.96 | 0.91 | 0.64 | 0.64 |  | 0.95 |
| 10 | 0.88 | 0.96 | 0.52 | 0.92 | 0.64 | 0.96 | 0.92 | 0.63 | 0.63 |  | 0.95 |
| 14 | 0.88 | 0.96 | 0.50 | 0.90 | 0.63 | 0.95 | 0.90 | 0.62 | 0.62 |  | 0.94 |
| 21 | 0.87 | 0.95 | 0.48 | 0.89 | 0.61 | 0.95 | 0.89 | 0.60 | 0.60 |  | 0.94 |
| 30 | 0.86 | 0.94 | 0.45 | 0.88 | 0.48 | 0.94 | 0.88 | 0.51 | 0.51 |  | 0.93 |
| 45 | 0.84 | 0.93 | 0.13 | 0.86 | 0.35 | 0.93 | 0.86 | 0.38 | 0.38 |  | 0.91 |
| 60 | 0.82 | 0.91 |  | 0.84 | 0.00 | 0.91 | 0.85 | 0.00 | 0.00 |  | 0.89 |
| 90 | 0.77 | 0.88 |  | 0.81 |  | 0.88 | 0.81 |  |  |  | 0.86 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | n_ride_days |
|---|---|---|---|---|---|---|---|
| 1 | 0.93 | 0.98 | 0.91 | 0.92 | 0.69 | 0.53 | 145986 |
| 2 | 0.93 | 0.98 | 0.91 | 0.92 | 0.68 | 0.53 | 144715 |
| 3 | 0.93 | 0.98 | 0.91 | 0.92 | 0.68 | 0.54 | 143994 |
| 4 | 0.94 | 0.98 | 0.92 | 0.92 | 0.68 | 0.54 | 143309 |
| 5 | 0.94 | 0.98 | 0.92 | 0.91 | 0.68 | 0.53 | 142638 |
| 6 | 0.94 | 0.98 | 0.92 | 0.91 | 0.67 | 0.53 | 141930 |
| 7 | 0.91 | 0.98 | 0.00 | 0.91 | 0.67 | 0.53 | 141084 |
| 10 | 0.92 | 0.98 | 0.00 | 0.92 | 0.66 | 0.52 | 138660 |
| 14 | 0.91 | 0.98 | 0.00 | 0.91 | 0.65 | 0.51 | 135709 |
| 21 | 0.90 | 0.97 | 0.00 | 0.91 | 0.63 | 0.48 | 130683 |
| 30 | 0.90 | 0.97 | 0.00 | 0.91 | 0.50 | 0.46 | 123609 |
| 45 | 0.89 | 0.97 | 0.00 | 0.90 | 0.37 | 0.15 | 113174 |
| 60 | 0.87 | 0.96 | 0.00 | 0.89 | 0.00 | 0.00 | 102285 |
| 90 | 0.86 | 0.96 | 0.00 | 0.87 | 0.00 | 0.00 | 79830 |
| 120 | 0.82 | 0.94 | 0.00 | 0.85 | 0.00 | 0.00 | 57445 |
| 180 | 0.79 | 0.91 | 0.00 | 0.87 | 0.00 | 0.00 | 19826 |

Forecast present vs ride operated. The baselines have **no openness logic** — they forecast whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never opened still got a curve; it is the bar a model with an open/closed decision has to beat:

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | prod_served_lin P(fc|operated) | prod_served_lin P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.81 | 0.25 | 0.94 | 0.75 | 0.86 | 0.71 | 0.94 | 0.76 | 0.88 | 0.65 | 0.66 | 0.52 | 0.50 | 0.34 | 0.00 | 0.00 | 0.00 | 0.00 | 435371.00 | 34758.00 |
| 1 | 0.81 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.88 | 0.65 | 0.65 | 0.52 | 0.50 | 0.34 | 0.00 | 0.00 | 0.00 | 0.00 | 432343.00 | 34647.00 |
| 2 | 0.81 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.88 | 0.65 | 0.65 | 0.52 | 0.50 | 0.34 | 0.00 | 0.00 | 0.00 | 0.00 | 429640.00 | 34483.00 |
| 3 | 0.82 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.51 | 0.50 | 0.34 | 0.61 | 0.46 | 0.61 | 0.46 | 427074.00 | 34312.00 |
| 4 | 0.82 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.51 | 0.50 | 0.35 | 0.60 | 0.46 | 0.60 | 0.46 | 424518.00 | 34130.00 |
| 5 | 0.82 | 0.26 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.51 | 0.50 | 0.34 | 0.60 | 0.46 | 0.60 | 0.46 | 422064.00 | 34014.00 |
| 6 | 0.83 | 0.26 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.64 | 0.51 | 0.50 | 0.34 | 0.60 | 0.45 | 0.60 | 0.45 | 419640.00 | 33894.00 |
| 7 | 0.00 | 0.00 | 0.93 | 0.76 | 0.86 | 0.70 | 0.94 | 0.77 | 0.88 | 0.66 | 0.64 | 0.51 | 0.50 | 0.34 | 0.60 | 0.45 | 0.60 | 0.45 | 416862.00 | 33811.00 |
| 10 | 0.00 | 0.00 | 0.93 | 0.76 | 0.86 | 0.70 | 0.94 | 0.77 | 0.89 | 0.66 | 0.64 | 0.51 | 0.49 | 0.34 | 0.59 | 0.45 | 0.59 | 0.45 | 408905.00 | 33427.00 |
| 14 | 0.00 | 0.00 | 0.93 | 0.77 | 0.86 | 0.70 | 0.94 | 0.78 | 0.88 | 0.67 | 0.63 | 0.49 | 0.48 | 0.33 | 0.58 | 0.44 | 0.58 | 0.44 | 399433.00 | 32769.00 |
| 21 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.71 | 0.94 | 0.78 | 0.88 | 0.68 | 0.60 | 0.47 | 0.46 | 0.33 | 0.56 | 0.42 | 0.56 | 0.42 | 383417.00 | 31395.00 |
| 30 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.71 | 0.93 | 0.79 | 0.88 | 0.69 | 0.48 | 0.41 | 0.44 | 0.33 | 0.47 | 0.39 | 0.47 | 0.39 | 362041.00 | 29930.00 |
| 45 | 0.00 | 0.00 | 0.93 | 0.79 | 0.85 | 0.71 | 0.93 | 0.80 | 0.88 | 0.70 | 0.35 | 0.34 | 0.13 | 0.11 | 0.36 | 0.31 | 0.36 | 0.31 | 326652.00 | 27344.00 |
| 60 | 0.00 | 0.00 | 0.93 | 0.81 | 0.85 | 0.72 | 0.93 | 0.81 | 0.88 | 0.72 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 289193.00 | 24515.00 |
| 90 | 0.00 | 0.00 | 0.93 | 0.83 | 0.84 | 0.72 | 0.93 | 0.84 | 0.88 | 0.74 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 223863.00 | 19451.00 |
