# ml-bench results

Run: `20261011-par829-stageA` · generated 2026-10-11T16:24+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with TARGET season. The common-window pass mixes lead with ORIGIN season instead. Both are confounded; neither alone identifies the horizon curve.

- code: git `af043b170441a23be21f1773b06760bc38f6bb50`, sources sha256 `c29ab0ab897ea60b`, image `sha256:186ebb484468`; plug-ins ['nf_nhits', 'nf_tide', 'nf_tide_wx_owx']
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 25.3 shard-minutes over 1 shard(s)
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
| MAE | UC1 | all | nf_nhits | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | all | nf_tide | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | all | persistence | True | park-day | 8 | 8 | m120 | m120 |  |
| MAE | UC1 | busy | nf_nhits | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | nf_tide | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | persistence | True | park-day | 8 | 8 | m120 | m120 |  |
| MAE | UC2 | all | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | lvlh5_tft | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | all | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | lvlh5_naive | False | park-day | 2 | 2 |  | 1 |  |
| MAE | UC2 | busy | lvlh5_tft | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | nf_tide | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | busy | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2-intraday | all | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | lvlh5_tft | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | all | nf_tide | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | all | wt_med | True | park-day | 3 | 3 |  | h8+ |  |
| MAE | UC2-intraday | busy | h5 | False | park-day | 3 | 3 |  | h8+ |  |
| MAE | UC2-intraday | busy | lvlh5_tft | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | nf_tide | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | busy | wt_med | True | park-day | 3 | 3 |  | h8+ |  |
| MAE | UC3 | all | clim | True | park-day | 13 | 11 |  | 30 |  |
| MAE | UC3 | all | h5 | False | park-day | 13 | 11 | 6 | 6 |  |
| MAE | UC3 | all | wt_med | True | park-day | 13 | 11 | 4 | 4 |  |
| MAE | UC3 | busy | clim | True | park-day | 13 | 11 |  | 30 |  |
| MAE | UC3 | busy | h5 | False | park-day | 13 | 11 | 3 | 3 |  |
| MAE | UC3 | busy | lvlh5_naive | False | park-day | 13 | 11 | 6 | 6 |  |
| MAE | UC3 | busy | lvlh5_tft | False | park-day | 13 | 11 | 10 | 10 |  |
| MAE | UC3 | busy | wt_med | True | park-day | 13 | 11 | 1 | 1 |  |
| MAE, schedule known at origin | UC3 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| MAE, schedule known at origin | UC3 | all | h5 | False | park-day | 14 | 12 | 6 | 6 |  |
| MAE, schedule known at origin | UC3 | all | lvlh5_tft | False | park-day | 14 | 12 | 0 | 0 |  |
| MAE, schedule known at origin | UC3 | all | wt_med | True | park-day | 14 | 12 | 3 | 3 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | False | park-day | 14 | 12 | 10 | 10 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | False | park-day | 14 | 12 | 7 | 7 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | True | park-day | 14 | 12 | 5 | 5 |  |
| UC4 Spearman within park-month | UC4 | all | lvl_clim | True | park-month | 12 | 12 |  | 45 |  |
| UC4 Spearman within park-month | UC4 | all | lvl_snaive7 | True | park-month | 6 | 6 |  | 6 |  |
| best-time hit rate (top-2) | D3 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| best-time hit rate (top-2) | D3 | all | h5 | False | park-day | 14 | 12 | 14 | 14 |  |
| best-time hit rate (top-2) | D3 | all | wt_med | True | park-day | 14 | 12 | 5 | 5 |  |
| best-time hit rate (±30 min) | D3 | all | h5 | False | park-day | 14 | 12 | 30 | 30 |  |
| best-time hit rate (±30 min) | D3 | all | wt_med | True | park-day | 14 | 12 | 5 | 5 |  |
| best-time regret (min) | D3 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| best-time regret (min) | D3 | all | h5 | False | park-day | 14 | 12 | 14 | 14 |  |
| best-time regret (min) | D3 | all | wt_med | True | park-day | 14 | 12 | 5 | 5 |  |
| crowd bucket exact | D6 | all | lvl_naive4 | True | park-day | 13 | 11 |  | 14 |  |
| crowd bucket exact | D6 | all | lvl_snaive7 | True | park-day | 6 | 6 | 6 | 6 |  |
| crowd bucket exact | D6 | all | lvl_tft | False | park-day | 13 | 11 |  | 21 |  |
| day comparison winner accuracy | D7 | all | lvl_naive4 | True | park-origin | 3 | 3 |  | d8-30 |  |
| day comparison winner accuracy | D7 | all | lvl_snaive7 | True | park-origin | 1 | 1 | d1-7 | d1-7 |  |
| day comparison winner accuracy | D7 | all | lvl_tft | False | park-origin | 3 | 3 |  | d31-90 |  |
| dayPeak Spearman across rides | UC3 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| dayPeak Spearman across rides | UC3 | all | nf_tide | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak Spearman across rides | UC3 | all | wt_med | True | park-day | 14 | 12 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| dayPeak abs error (min) | UC3 | all | lvlh5_tft | False | park-day | 14 | 12 | 3 | 3 |  |
| dayPeak abs error (min) | UC3 | all | nf_tide | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | prod_served | False | park-day | 11 | 9 | 3 | 3 |  |
| dayPeak abs error (min) | UC3 | all | prod_served_lin | False | park-day | 11 | 9 | 4 | 4 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | False | park-day | 14 | 12 | 14 | 14 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | False | park-day | 14 | 12 | 21 | 21 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | False | park-day | 13 | 12 | 30 | 30 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_nhits | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_tide | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served_lin | False | park-day | 10 | 9 | 14 | 14 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | True | park-day | 14 | 12 | 3 | 3 |  |
| first-hour MAE (opening-aligned) | D5 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| first-hour MAE (opening-aligned) | D5 | all | h5 | False | park-day | 14 | 12 | 7 | 7 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 | 6 | 6 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | False | park-day | 14 | 12 | 7 | 7 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | False | park-day | 14 | 12 | 7 | 7 |  |
| first-hour MAE (opening-aligned) | D5 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE (opening-aligned) | D5 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| first-hour MAE, schedule known at origin | D5 | all | h5 | False | park-day | 14 | 12 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 | 6 | 6 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | False | park-day | 14 | 12 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | False | park-day | 14 | 12 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE, schedule projected at origin | D5 | all | clim | True | park-day | 14 | 12 |  | 30 |  |
| live-window MAE (headliners) | D2 | headliners | persistence | True | park-day | 1 | 1 | w0-45 | w0-45 |  |
| next-best precision | D1 | all | nf_tide | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | nf_tide | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best recall | D1 | all | snaive7 | True | park-day | 2 | 2 |  | 0-120 |  |
| optimiser regret (min per park-day) | D4 | all | clim | True | park-day | 5 | 5 |  | 30 |  |
| optimiser regret (min per park-day) | D4 | all | h5 | False | park-day | 5 | 5 |  | 3 |  |
| optimiser regret (min per park-day) | D4 | all | lvlh5_tft | False | park-day | 5 | 5 |  | 3 |  |
| optimiser regret (min per park-day) | D4 | all | nf_nhits_x_h5 | False | park-day | 3 | 3 | 7 | 7 |  |
| optimiser regret (min per park-day) | D4 | all | nf_tide_x_h5 | False | park-day | 3 | 3 | 7 | 7 |  |
| rope-drop worth agreement | D5 | all | clim | True | park-day | 14 | 12 |  | 30 |  |


**Never tested** — `leads_tested = 0` means no lead passed the gates, which is NOT "nothing won". The reason per metric × model:

| metric | uc | segment | model | unit | leads_scored | not_tested_because |
|---|---|---|---|---|---|---|
| dayPeak pairwise ordering (headliners) | D4 | all | snaive7 | park-day | 7 | 0: coverage 0.28 < 0.30 against one of the candidates; 1: coverage 0.28 < 0.30 against one of the candidates; 2: coverage 0.28 < 0.30 against one of the candidates; 3: coverage 0.28 < 0.30 against one of the candidates; 4: coverage 0.28 < 0.30 against one of the candidates; 5: coverage 0.28 < 0.30 against one of the candidates |

### Reference choice per cell

Candidates (coverage ≥ 50 % of the best-covered), their own-coverage values, and why the reference is the one it is. `reason` says `ladder order` when no candidate significantly beats the ladder-first one — i.e. the choice was NOT made on the point estimates.

| metric | uc | segment | lead | candidates | selected | reason |
|---|---|---|---|---|---|---|
| MAE | UC2 | all | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC2 | busy | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 1 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | all | 2 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 2 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 3 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 3 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 4 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 4 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 5 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC3 | busy | 5 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 6 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 6 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 7 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 7 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 10 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 10 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 14 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | busy | 14 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| MAE | UC3 | all | 21 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | busy | 21 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | all | 30 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | busy | 30 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | all | 45 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | busy | 45 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | all | 60 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| MAE | UC3 | busy | 60 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 0 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 1 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 2 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 3 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 4 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 5 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 6 | snaive7/wt_med/clim | snaive7 | ladder order (snaive7 first of snaive7/wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 7 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
| first-hour MAE (opening-aligned) | D5 | all | 10 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 14 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 21 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 30 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 45 | wt_med/clim | clim | wt_med is the ladder-first candidate but is significantly beaten by clim; clim has 0 significant loss(es) |
| first-hour MAE (opening-aligned) | D5 | all | 60 | wt_med/clim | wt_med | ladder order (wt_med first of wt_med/clim); no candidate beats it significantly |
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
| MAE | UC2-intraday | all | h2-4 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC2-intraday | busy | h2-4 | persistence/snaive7/wt_med/clim | persistence | ladder order (persistence first of persistence/snaive7/wt_med/clim); no candidate beats it significantly |
| MAE | UC2-intraday | all | h4-8 | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h4-8 | persistence/snaive7/wt_med/clim | wt_med | persistence is the ladder-first candidate but is significantly beaten by snaive7/wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | all | h8+ | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |
| MAE | UC2-intraday | busy | h8+ | snaive7/wt_med/clim | wt_med | snaive7 is the ladder-first candidate but is significantly beaten by wt_med/clim; wt_med has 0 significant loss(es) |

### Hand-over table (input for the serving router)

Per use case × lead: among the models that beat EVERY naive candidate (and pass the gates), the largest margin against the hardest candidate; otherwise the reference itself. The naive candidates themselves are excluded from the winner column even when one of them beats the others, because their margin is measured against a different and weaker pool — `usable_horizon.csv` keeps those rows, flagged `is_reference_candidate`. `park_day_unit_agrees = False` marks a row whose verdict changes under BENCH-SPEC's literal park-day bootstrap.

| uc | segment | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested | n_origin_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | wt_med | lvlh5_tft | 7.739 | -0.230 | -0.462 | -0.023 | -0.321 | -0.147 | True | 0.976 | 2 | 9 | 61 |
| UC2 | busy | 0 | wt_med | lvlh5_tft | 13.934 | -1.183 | -1.681 | -0.732 | -1.382 | -0.977 | True | 0.978 | 3 | 9 | 61 |
| UC3 | all | 1 | wt_med | h5 | 7.924 | -0.118 | -0.151 | -0.091 | -0.129 | -0.108 | True | 0.997 | 1 | 11 | 60 |
| UC3 | busy | 1 | wt_med | lvlh5_tft | 14.148 | -1.142 | -1.658 | -0.675 | -1.363 | -0.942 | True | 0.978 | 3 | 11 | 60 |
| UC3 | all | 2 | wt_med | h5 | 7.982 | -0.122 | -0.156 | -0.093 | -0.133 | -0.111 | True | 0.996 | 1 | 11 | 59 |
| UC3 | busy | 2 | snaive7 | lvlh5_tft | 14.318 | -1.845 | -2.289 | -1.349 | -2.142 | -1.550 | True | 0.978 | 3 | 11 | 59 |
| UC3 | all | 3 | wt_med | h5 | 7.994 | -0.127 | -0.163 | -0.098 | -0.139 | -0.117 | True | 0.996 | 1 | 13 | 58 |
| UC3 | busy | 3 | snaive7 | lvlh5_tft | 14.508 | -1.570 | -2.026 | -1.030 | -1.852 | -1.302 | True | 0.978 | 3 | 13 | 58 |
| UC3 | all | 4 | wt_med | h5 | 8.010 | -0.132 | -0.167 | -0.103 | -0.143 | -0.121 | True | 0.995 | 1 | 13 | 57 |
| UC3 | busy | 4 | snaive7 | lvlh5_tft | 14.668 | -1.416 | -1.854 | -0.918 | -1.682 | -1.147 | True | 0.978 | 2 | 13 | 57 |
| UC3 | all | 5 | wt_med | h5 | 8.069 | -0.134 | -0.170 | -0.105 | -0.145 | -0.123 | True | 0.994 | 1 | 13 | 56 |
| UC3 | busy | 5 | snaive7 | lvlh5_tft | 14.803 | -1.285 | -1.748 | -0.740 | -1.571 | -0.997 | True | 0.976 | 2 | 13 | 56 |
| UC3 | all | 6 | snaive7 | h5 | 8.168 | -0.390 | -0.758 | -0.026 | -0.542 | -0.230 | True | 0.992 | 1 | 13 | 55 |
| UC3 | busy | 6 | snaive7 | lvlh5_tft | 15.026 | -1.015 | -1.470 | -0.477 | -1.314 | -0.702 | True | 0.973 | 2 | 13 | 55 |
| UC3 | all | 7 | wt_med | wt_med | 8.451 | 0.000 |  |  |  |  | False |  | 0 | 12 | 54 |
| UC3 | busy | 7 | wt_med | lvlh5_tft | 15.477 | -0.652 | -1.154 | -0.119 | -0.879 | -0.420 | True | 0.976 | 1 | 12 | 54 |
| UC3 | all | 10 | wt_med | wt_med | 8.557 | 0.000 |  |  |  |  | True |  | 0 | 8 | 51 |
| UC3 | busy | 10 | wt_med | lvlh5_tft | 15.759 | -0.668 | -1.196 | -0.154 | -0.893 | -0.435 | True | 0.979 | 1 | 8 | 51 |
| UC3 | all | 14 | wt_med | wt_med | 8.749 | 0.000 |  |  |  |  | False |  | 0 | 8 | 47 |
| UC3 | busy | 14 | wt_med | wt_med | 16.865 | 0.000 |  |  |  |  | False |  | 0 | 8 | 47 |
| UC3 | all | 21 | clim | clim | 8.268 | 0.000 |  |  |  |  | False |  | 0 | 8 | 40 |
| UC3 | busy | 21 | clim | clim | 15.989 | 0.000 |  |  |  |  | False |  | 0 | 8 | 40 |
| UC3 | all | 30 | clim | clim | 8.317 | 0.000 |  |  |  |  | False |  | 0 | 8 | 31 |
| UC3 | busy | 30 | clim | clim | 15.998 | 0.000 |  |  |  |  | False |  | 0 | 8 | 31 |
| UC3 | all | 45 | clim | clim | 8.264 | 0.000 |  |  |  |  | True |  | 0 | 0 | 16 |
| UC3 | busy | 45 | clim | clim | 15.940 | 0.000 |  |  |  |  | True |  | 0 | 0 | 16 |
| UC3 | all | 60 | clim | clim | 7.416 | 0.000 |  |  |  |  | True |  | 0 | 0 | 1 |
| UC3 | busy | 60 | clim | clim | 15.390 | 0.000 |  |  |  |  | True |  | 0 | 0 | 1 |
| UC2 | all | 1 | wt_med | h5 | 7.924 | -0.118 | -0.151 | -0.091 | -0.129 | -0.108 | True | 0.997 | 1 | 11 | 60 |
| UC2 | busy | 1 | wt_med | lvlh5_tft | 14.148 | -1.142 | -1.658 | -0.675 | -1.363 | -0.942 | True | 0.978 | 3 | 11 | 60 |
| UC1 | all | m015 | persistence | persistence | 2.464 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | busy | m015 | persistence | persistence | 4.423 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | all | m030 | persistence | persistence | 4.087 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | busy | m030 | persistence | persistence | 7.310 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | all | m045 | persistence | persistence | 5.037 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | busy | m045 | persistence | persistence | 8.988 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | all | m060 | persistence | persistence | 5.699 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | busy | m060 | persistence | nf_tide | 10.088 | -0.619 | -1.006 | -0.184 | -0.724 | -0.510 | True | 0.975 | 1 | 9 | 61 |
| UC1 | all | m075 | persistence | persistence | 6.314 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | busy | m075 | persistence | persistence | 11.197 | 0.000 |  |  |  |  | False |  | 0 | 9 | 61 |
| UC1 | all | m090 | persistence | nf_tide | 6.387 | -0.515 | -0.777 | -0.250 | -0.591 | -0.449 | True | 0.969 | 1 | 9 | 61 |
| UC1 | busy | m090 | persistence | nf_tide | 11.402 | -1.189 | -1.601 | -0.710 | -1.345 | -1.048 | True | 0.974 | 1 | 9 | 61 |
| UC1 | all | m105 | persistence | nf_tide | 6.507 | -0.902 | -1.190 | -0.611 | -0.983 | -0.832 | True | 0.969 | 2 | 9 | 61 |
| UC1 | busy | m105 | persistence | nf_tide | 11.581 | -1.899 | -2.351 | -1.398 | -2.059 | -1.759 | True | 0.974 | 2 | 9 | 61 |
| UC1 | all | m120 | persistence | nf_tide | 6.712 | -1.194 | -1.512 | -0.878 | -1.278 | -1.122 | True | 0.968 | 2 | 9 | 61 |
| UC1 | busy | m120 | persistence | nf_tide | 11.940 | -2.516 | -3.021 | -1.955 | -2.678 | -2.371 | True | 0.973 | 2 | 9 | 61 |
| UC2-intraday | all | h2-4 | persistence | nf_tide | 7.053 | -1.831 | -2.227 | -1.461 | -1.930 | -1.744 | True | 0.965 | 3 | 9 | 61 |
| UC2-intraday | busy | h2-4 | persistence | nf_tide | 12.739 | -3.520 | -4.076 | -2.882 | -3.714 | -3.348 | True | 0.971 | 2 | 9 | 61 |
| UC2-intraday | all | h4-8 | wt_med | nf_tide | 7.890 | -0.470 | -0.678 | -0.274 | -0.542 | -0.397 | True | 0.981 | 3 | 9 | 61 |
| UC2-intraday | busy | h4-8 | wt_med | nf_tide | 14.248 | -1.339 | -1.825 | -0.901 | -1.514 | -1.172 | True | 0.964 | 3 | 9 | 61 |
| UC2-intraday | all | h8+ | wt_med | h5 | 8.128 | -0.158 | -0.205 | -0.116 | -0.179 | -0.139 | True | 0.992 | 1 | 8 | 61 |
| UC2-intraday | busy | h8+ | wt_med | lvlh5_tft | 14.385 | -0.594 | -0.994 | -0.224 | -0.821 | -0.370 | True | 0.974 | 2 | 8 | 61 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | lvlh5_tft | 4.214 | -0.398 | -0.723 | -0.159 | -0.463 | -0.341 | True | 0.974 | 4 | 9 |
| first-hour MAE (opening-aligned) | D5 | 1 | snaive7 | lvlh5_tft | 4.275 | -0.629 | -1.047 | -0.347 | -0.745 | -0.514 | True | 0.976 | 6 | 11 |
| first-hour MAE (opening-aligned) | D5 | 2 | snaive7 | lvlh5_tft | 4.305 | -0.591 | -0.998 | -0.308 | -0.711 | -0.477 | True | 0.977 | 6 | 11 |
| first-hour MAE (opening-aligned) | D5 | 3 | snaive7 | nf_tide_x_h5 | 4.286 | -0.565 | -0.952 | -0.285 | -0.685 | -0.454 | True | 0.995 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 4 | snaive7 | nf_tide_x_h5 | 4.299 | -0.546 | -0.931 | -0.267 | -0.667 | -0.432 | True | 0.995 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 5 | snaive7 | nf_nhits_x_h5 | 4.325 | -0.516 | -0.915 | -0.233 | -0.638 | -0.403 | True | 0.995 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 6 | snaive7 | nf_nhits_x_h5 | 4.378 | -0.460 | -0.864 | -0.177 | -0.584 | -0.340 | True | 0.995 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | nf_nhits_x_h5 | 4.448 | -0.481 | -0.855 | -0.209 | -0.558 | -0.419 | True | 0.995 | 5 | 12 |
| first-hour MAE (opening-aligned) | D5 | 10 | clim | clim | 4.711 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE (opening-aligned) | D5 | 14 | clim | clim | 4.723 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE (opening-aligned) | D5 | 21 | clim | clim | 4.732 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE (opening-aligned) | D5 | 30 | clim | clim | 4.865 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE (opening-aligned) | D5 | 45 | clim | clim | 4.820 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE (opening-aligned) | D5 | 60 | wt_med | wt_med | 4.777 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | lvlh5_tft | 7.913 | -0.209 | -0.417 | -0.013 | -0.312 | -0.114 | True | 0.973 | 2 | 9 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | h5 | 8.060 | -0.134 | -0.173 | -0.102 | -0.146 | -0.121 | True | 0.996 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | h5 | 8.112 | -0.138 | -0.178 | -0.106 | -0.151 | -0.125 | True | 0.996 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 8.106 | -0.145 | -0.186 | -0.111 | -0.159 | -0.132 | True | 0.996 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | h5 | 8.103 | -0.150 | -0.192 | -0.115 | -0.163 | -0.137 | True | 0.995 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | h5 | 8.136 | -0.152 | -0.195 | -0.117 | -0.166 | -0.139 | True | 0.993 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 6 | snaive7 | h5 | 8.191 | -0.453 | -0.849 | -0.078 | -0.623 | -0.277 | True | 0.991 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | wt_med | 8.460 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | wt_med | 8.435 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule known at origin | UC3 | 14 | clim | clim | 8.036 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule known at origin | UC3 | 21 | clim | clim | 7.821 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule known at origin | UC3 | 30 | clim | clim | 7.946 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule known at origin | UC3 | 45 | clim | clim | 7.873 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule known at origin | UC3 | 60 | wt_med | wt_med | 5.752 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | lvlh5_tft | 4.212 | -0.453 | -0.835 | -0.175 | -0.528 | -0.387 | True | 0.971 | 4 | 9 |
| first-hour MAE, schedule known at origin | D5 | 1 | snaive7 | lvlh5_tft | 4.273 | -0.721 | -1.198 | -0.409 | -0.848 | -0.594 | True | 0.974 | 6 | 11 |
| first-hour MAE, schedule known at origin | D5 | 2 | snaive7 | lvlh5_tft | 4.296 | -0.685 | -1.153 | -0.376 | -0.815 | -0.558 | True | 0.974 | 6 | 11 |
| first-hour MAE, schedule known at origin | D5 | 3 | snaive7 | lvlh5_tft | 4.300 | -0.653 | -1.106 | -0.344 | -0.785 | -0.522 | True | 0.975 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 4 | snaive7 | lvlh5_tft | 4.306 | -0.637 | -1.094 | -0.323 | -0.770 | -0.500 | True | 0.974 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 5 | snaive7 | lvlh5_tft | 4.309 | -0.616 | -1.079 | -0.294 | -0.750 | -0.471 | True | 0.973 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 6 | snaive7 | nf_nhits_x_h5 | 4.309 | -0.564 | -1.031 | -0.237 | -0.703 | -0.422 | True | 0.994 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | nf_nhits_x_h5 | 4.354 | -0.507 | -0.902 | -0.218 | -0.593 | -0.432 | True | 0.994 | 5 | 12 |
| first-hour MAE, schedule known at origin | D5 | 10 | clim | clim | 4.511 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule known at origin | D5 | 14 | clim | clim | 4.430 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule known at origin | D5 | 21 | clim | clim | 4.385 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule known at origin | D5 | 30 | clim | clim | 4.470 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule known at origin | D5 | 45 | clim | clim | 4.318 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | 60 | wt_med | wt_med | 3.029 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 0 | snaive7 | snaive7 | 7.484 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| MAE, schedule projected at origin | UC3 | 1 | snaive7 | snaive7 | 7.561 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule projected at origin | UC3 | 2 | snaive7 | snaive7 | 7.599 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| MAE, schedule projected at origin | UC3 | 3 | snaive7 | snaive7 | 7.637 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| MAE, schedule projected at origin | UC3 | 4 | snaive7 | snaive7 | 7.725 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| MAE, schedule projected at origin | UC3 | 5 | snaive7 | snaive7 | 7.835 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| MAE, schedule projected at origin | UC3 | 6 | snaive7 | snaive7 | 7.991 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | wt_med | 8.411 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | wt_med | 9.005 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | wt_med | 9.538 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| MAE, schedule projected at origin | UC3 | 21 | wt_med | wt_med | 9.769 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule projected at origin | UC3 | 30 | wt_med | wt_med | 9.691 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| MAE, schedule projected at origin | UC3 | 45 | wt_med | wt_med | 10.529 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| MAE, schedule projected at origin | UC3 | 60 | clim | clim | 9.036 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 0 | snaive7 | snaive7 | 4.547 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 1 | snaive7 | snaive7 | 4.541 | 0.000 |  |  |  |  | True |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 2 | snaive7 | snaive7 | 4.532 | 0.000 |  |  |  |  | True |  | 0 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 3 | snaive7 | snaive7 | 4.517 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 4 | snaive7 | snaive7 | 4.516 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 5 | snaive7 | snaive7 | 4.630 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 6 | snaive7 | snaive7 | 4.738 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | wt_med | 5.240 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| first-hour MAE, schedule projected at origin | D5 | 10 | wt_med | wt_med | 5.854 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 14 | wt_med | wt_med | 6.458 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 21 | wt_med | wt_med | 6.627 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 30 | clim | clim | 5.832 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 45 | clim | clim | 5.878 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 60 | wt_med | wt_med | 6.675 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | persistence | 6.091 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | h5 | 0.819 | 0.011 | 0.006 | 0.017 | 0.008 | 0.013 | True | 0.994 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | h5 | 0.819 | 0.012 | 0.007 | 0.018 | 0.010 | 0.014 | True | 0.993 | 1 | 11 |
| best-time hit rate (top-2) | D3 | 2 | wt_med | h5 | 0.820 | 0.013 | 0.007 | 0.019 | 0.010 | 0.015 | True | 0.993 | 1 | 11 |
| best-time hit rate (top-2) | D3 | 3 | wt_med | h5 | 0.820 | 0.012 | 0.007 | 0.019 | 0.010 | 0.015 | True | 0.992 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 4 | wt_med | h5 | 0.820 | 0.013 | 0.007 | 0.019 | 0.010 | 0.015 | True | 0.992 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 5 | wt_med | h5 | 0.820 | 0.012 | 0.007 | 0.018 | 0.009 | 0.014 | True | 0.992 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | h5 | 0.820 | 0.012 | 0.007 | 0.017 | 0.009 | 0.014 | True | 0.991 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 7 | wt_med | h5 | 0.819 | 0.011 | 0.006 | 0.017 | 0.009 | 0.014 | True | 0.991 | 1 | 12 |
| best-time hit rate (top-2) | D3 | 10 | wt_med | h5 | 0.820 | 0.011 | 0.005 | 0.017 | 0.008 | 0.013 | True | 0.989 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 14 | wt_med | h5 | 0.820 | 0.011 | 0.005 | 0.018 | 0.008 | 0.014 | True | 0.988 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 21 | clim | clim | 0.827 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| best-time hit rate (top-2) | D3 | 30 | clim | clim | 0.827 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| best-time hit rate (top-2) | D3 | 45 | wt_med | wt_med | 0.812 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (top-2) | D3 | 60 | wt_med | wt_med | 0.774 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 0 | wt_med | h5 | 0.825 | 0.011 | 0.007 | 0.015 | 0.009 | 0.013 | True | 0.994 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | h5 | 0.826 | 0.013 | 0.008 | 0.017 | 0.010 | 0.015 | True | 0.993 | 1 | 11 |
| best-time hit rate (±30 min) | D3 | 2 | wt_med | h5 | 0.826 | 0.013 | 0.008 | 0.018 | 0.010 | 0.015 | True | 0.993 | 1 | 11 |
| best-time hit rate (±30 min) | D3 | 3 | wt_med | h5 | 0.827 | 0.012 | 0.007 | 0.017 | 0.010 | 0.015 | True | 0.992 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.827 | 0.013 | 0.008 | 0.018 | 0.011 | 0.015 | True | 0.992 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | h5 | 0.827 | 0.012 | 0.007 | 0.017 | 0.009 | 0.014 | True | 0.992 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | h5 | 0.827 | 0.012 | 0.007 | 0.017 | 0.009 | 0.014 | True | 0.991 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.825 | 0.011 | 0.006 | 0.017 | 0.009 | 0.013 | True | 0.991 | 1 | 12 |
| best-time hit rate (±30 min) | D3 | 10 | wt_med | h5 | 0.826 | 0.011 | 0.006 | 0.018 | 0.009 | 0.014 | True | 0.989 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 14 | wt_med | h5 | 0.826 | 0.013 | 0.006 | 0.020 | 0.010 | 0.015 | True | 0.988 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 21 | wt_med | h5 | 0.829 | 0.012 | 0.006 | 0.021 | 0.009 | 0.015 | True | 0.987 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 30 | wt_med | h5 | 0.827 | 0.014 | 0.006 | 0.025 | 0.010 | 0.018 | True | 0.983 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 45 | wt_med | wt_med | 0.818 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time hit rate (±30 min) | D3 | 60 | wt_med | wt_med | 0.789 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.931 | -0.410 | -0.574 | -0.271 | -0.475 | -0.344 | True | 0.994 | 1 | 9 |
| best-time regret (min) | D3 | 1 | wt_med | h5 | 2.935 | -0.412 | -0.570 | -0.269 | -0.476 | -0.344 | True | 0.993 | 1 | 11 |
| best-time regret (min) | D3 | 2 | wt_med | h5 | 2.924 | -0.415 | -0.580 | -0.270 | -0.489 | -0.344 | True | 0.993 | 1 | 11 |
| best-time regret (min) | D3 | 3 | wt_med | h5 | 2.920 | -0.409 | -0.588 | -0.265 | -0.476 | -0.338 | True | 0.992 | 1 | 13 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 2.916 | -0.435 | -0.623 | -0.279 | -0.508 | -0.366 | True | 0.992 | 1 | 13 |
| best-time regret (min) | D3 | 5 | wt_med | h5 | 2.926 | -0.415 | -0.594 | -0.262 | -0.487 | -0.345 | True | 0.992 | 1 | 13 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 2.955 | -0.405 | -0.586 | -0.253 | -0.472 | -0.333 | True | 0.991 | 1 | 13 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 2.998 | -0.390 | -0.582 | -0.224 | -0.454 | -0.326 | True | 0.991 | 1 | 12 |
| best-time regret (min) | D3 | 10 | wt_med | h5 | 2.968 | -0.402 | -0.608 | -0.227 | -0.474 | -0.335 | True | 0.989 | 1 | 8 |
| best-time regret (min) | D3 | 14 | wt_med | h5 | 2.984 | -0.414 | -0.636 | -0.231 | -0.490 | -0.339 | True | 0.988 | 1 | 8 |
| best-time regret (min) | D3 | 21 | clim | clim | 2.912 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| best-time regret (min) | D3 | 30 | clim | clim | 2.878 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| best-time regret (min) | D3 | 45 | wt_med | wt_med | 3.473 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| best-time regret (min) | D3 | 60 | wt_med | wt_med | 3.474 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | h5 | 0.458 | 0.016 | 0.013 | 0.020 | 0.015 | 0.018 | True | 0.964 | 2 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | h5 | 0.455 | 0.016 | 0.013 | 0.020 | 0.015 | 0.018 | True | 0.963 | 2 | 11 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | h5 | 0.454 | 0.017 | 0.014 | 0.021 | 0.016 | 0.018 | True | 0.963 | 2 | 11 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.453 | 0.017 | 0.014 | 0.020 | 0.015 | 0.018 | True | 0.963 | 2 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | h5 | 0.452 | 0.017 | 0.014 | 0.021 | 0.016 | 0.019 | True | 0.964 | 2 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | h5 | 0.450 | 0.017 | 0.014 | 0.020 | 0.015 | 0.018 | True | 0.963 | 2 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | h5 | 0.448 | 0.017 | 0.014 | 0.020 | 0.015 | 0.018 | True | 0.962 | 2 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.444 | 0.017 | 0.014 | 0.021 | 0.016 | 0.019 | True | 0.960 | 2 | 12 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.438 | 0.017 | 0.014 | 0.021 | 0.016 | 0.019 | True | 0.959 | 1 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | clim | clim | 0.427 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | clim | clim | 0.427 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | clim | clim | 0.428 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | clim | clim | 0.431 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | clim | clim | 0.408 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 0 | snaive7 | lvlh5_tft | 8.708 | -0.725 | -0.979 | -0.465 | -0.936 | -0.525 | True | 0.971 | 2 | 9 |
| dayPeak abs error (min) | UC3 | 1 | snaive7 | lvlh5_tft | 8.906 | -0.510 | -0.756 | -0.246 | -0.709 | -0.316 | True | 0.971 | 1 | 11 |
| dayPeak abs error (min) | UC3 | 2 | snaive7 | lvlh5_tft | 9.020 | -0.425 | -0.675 | -0.179 | -0.628 | -0.227 | True | 0.972 | 1 | 11 |
| dayPeak abs error (min) | UC3 | 3 | snaive7 | prod_served_lin | 8.866 | -0.417 | -0.670 | -0.148 | -0.619 | -0.214 | True | 0.970 | 3 | 13 |
| dayPeak abs error (min) | UC3 | 4 | snaive7 | prod_served_lin | 8.965 | -0.311 | -0.562 | -0.033 | -0.513 | -0.122 | True | 0.969 | 1 | 13 |
| dayPeak abs error (min) | UC3 | 5 | snaive7 | snaive7 | 8.332 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak abs error (min) | UC3 | 6 | snaive7 | snaive7 | 8.279 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | wt_med | 9.625 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 9.694 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 14 | clim | clim | 9.286 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 21 | clim | clim | 8.978 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 30 | clim | clim | 8.676 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 45 | clim | clim | 8.496 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak abs error (min) | UC3 | 60 | clim | clim | 7.860 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 0 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | True |  | 0 | 9 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| rope-drop worth agreement | D5 | 2 | wt_med | wt_med | 0.916 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| rope-drop worth agreement | D5 | 3 | wt_med | wt_med | 0.917 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 4 | clim | clim | 0.920 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 5 | clim | clim | 0.920 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 6 | clim | clim | 0.920 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 7 | clim | clim | 0.920 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| rope-drop worth agreement | D5 | 10 | clim | clim | 0.923 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 14 | clim | clim | 0.924 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 21 | clim | clim | 0.927 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 30 | clim | clim | 0.932 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 45 | clim | clim | 0.931 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.950 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | nf_tide | 0.799 | 0.076 | 0.066 | 0.087 | 0.072 | 0.080 | True | 0.993 | 5 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | nf_tide | 0.790 | 0.071 | 0.061 | 0.082 | 0.067 | 0.075 | True | 0.992 | 7 | 10 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | nf_tide | 0.785 | 0.067 | 0.057 | 0.079 | 0.064 | 0.071 | True | 0.992 | 7 | 10 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | nf_tide | 0.782 | 0.065 | 0.054 | 0.076 | 0.061 | 0.069 | True | 0.991 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | nf_tide | 0.783 | 0.066 | 0.056 | 0.078 | 0.063 | 0.070 | True | 0.991 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | nf_tide | 0.780 | 0.064 | 0.053 | 0.075 | 0.060 | 0.068 | True | 0.991 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | nf_tide | 0.779 | 0.067 | 0.056 | 0.080 | 0.063 | 0.071 | True | 0.992 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | nf_tide | 0.773 | 0.064 | 0.053 | 0.077 | 0.061 | 0.069 | True | 0.992 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | lvlh5_tft | 0.746 | 0.044 | 0.035 | 0.054 | 0.041 | 0.048 | True | 0.968 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | lvlh5_tft | 0.741 | 0.047 | 0.036 | 0.059 | 0.043 | 0.051 | True | 0.970 | 4 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | clim | lvlh5_tft | 0.731 | 0.029 | 0.014 | 0.045 | 0.024 | 0.035 | True | 0.968 | 2 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | clim | lvlh5_tft | 0.721 | 0.022 | 0.003 | 0.040 | 0.015 | 0.028 | True | 0.955 | 1 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | clim | clim | 0.709 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | wt_med | wt_med | 0.692 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 0 | wt_med | nf_tide | 0.769 | 0.014 | 0.005 | 0.022 | 0.010 | 0.018 | True | 0.979 | 1 | 9 |
| dayPeak Spearman across rides | UC3 | 1 | snaive7 | snaive7 | 0.777 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| dayPeak Spearman across rides | UC3 | 2 | snaive7 | snaive7 | 0.776 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| dayPeak Spearman across rides | UC3 | 3 | snaive7 | snaive7 | 0.776 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 4 | snaive7 | snaive7 | 0.776 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 5 | snaive7 | snaive7 | 0.775 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 6 | snaive7 | snaive7 | 0.774 | 0.000 |  |  |  |  | True |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.739 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.732 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.725 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 21 | clim | clim | 0.726 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 30 | clim | clim | 0.717 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 45 | clim | clim | 0.721 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| dayPeak Spearman across rides | UC3 | 60 | wt_med | wt_med | 0.678 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | nf_tide_x_h5 | 27.301 | -2.248 | -3.725 | -0.948 | -3.002 | -1.548 | True | 0.973 | 2 | 11 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | nf_tide_x_h5 | 27.347 | -2.701 | -4.107 | -1.462 | -3.430 | -1.978 | True | 0.967 | 4 | 13 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | nf_nhits_x_h5 | 27.990 | -2.452 | -3.800 | -1.219 | -3.312 | -1.662 | True | 0.971 | 2 | 12 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | wt_med | 31.264 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| optimiser regret (min per park-day) | D4 | 30 | clim | clim | 32.045 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| next-best precision | D1 | 0-120 | snaive7 | nf_tide | 0.410 | 0.049 | 0.018 | 0.086 | 0.039 | 0.059 | True | 0.963 | 1 | 8 |
| next-best precision | D1 | 0-60 | snaive7 | nf_tide | 0.331 | 0.049 | 0.019 | 0.086 | 0.039 | 0.059 | True | 0.955 | 1 | 8 |
| next-best precision (top-3) | D1 | 0-120 | snaive7 | nf_tide | 0.482 | 0.044 | 0.028 | 0.060 | 0.037 | 0.050 | True | 0.946 | 1 | 8 |
| next-best precision (top-3) | D1 | 0-60 | snaive7 | nf_tide | 0.403 | 0.049 | 0.033 | 0.067 | 0.043 | 0.056 | True | 0.934 | 1 | 8 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.566 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.522 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 1 | lvl_snaive7 | lvl_snaive7 | 0.329 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 2 | lvl_snaive7 | lvl_snaive7 | 0.308 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 3 | lvl_snaive7 | lvl_snaive7 | 0.310 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 4 | lvl_snaive7 | lvl_snaive7 | 0.321 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 5 | lvl_snaive7 | lvl_snaive7 | 0.325 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 6 | lvl_snaive7 | lvl_snaive7 | 0.328 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 7 | lvl_naive4 | lvl_naive4 | 0.196 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| UC4 Spearman within park-month | UC4 | 10 | lvl_naive4 | lvl_naive4 | 0.184 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 14 | lvl_clim | lvl_clim | 0.198 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 21 | lvl_clim | lvl_clim | 0.253 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 30 | lvl_clim | lvl_clim | 0.281 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 45 | lvl_clim | lvl_clim | 0.206 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_snaive7 | 0.424 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 2 | lvl_snaive7 | lvl_snaive7 | 0.424 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 3 | lvl_snaive7 | lvl_snaive7 | 0.427 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.431 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.434 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.437 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 7 | lvl_naive4 | lvl_tft | 0.355 | 0.034 | 0.014 | 0.054 | 0.017 | 0.049 | True | 0.978 | 1 | 7 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_tft | 0.344 | 0.023 | 0.002 | 0.046 | 0.008 | 0.039 | True | 0.981 | 1 | 5 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_tft | 0.326 | 0.040 | 0.020 | 0.063 | 0.023 | 0.057 | True | 0.987 | 1 | 5 |
| crowd bucket exact | D6 | 21 | lvl_naive4 | lvl_tft | 0.300 | 0.044 | 0.021 | 0.066 | 0.025 | 0.062 | True | 0.986 | 1 | 5 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_naive4 | 0.249 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_naive4 | 0.223 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| crowd bucket exact | D6 | 60 | lvl_naive4 | lvl_naive4 | 0.268 | 0.000 |  |  |  |  | True |  | 0 | 0 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.502 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_tft | 0.414 | 0.087 | 0.057 | 0.116 | 0.079 | 0.094 | True | 0.985 | 1 | 5 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_tft | 0.394 | 0.105 | 0.067 | 0.142 | 0.095 | 0.116 | True | 0.982 | 1 | 5 |

### Low-coverage cells (paired rows < 50 % of the reference's)

These absolute values are not comparable with the rest of their column. `reaches_handover` = the cell still passes the 30 % gate and CAN appear as a winner.

| metric | uc | segment | lead | model | ref | value | diff_vs_ref | diff_lo_park | diff_hi_park | paired_share | n_paired | n_units_park | n_origin_days | wins | reaches_handover |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MAE | UC2-intraday | all | h4-8 | persistence | wt_med | 12.004 | 3.180 | 2.570 | 3.888 | 0.436 | 2138655.000 | 151.000 | 61 | False | True |
| MAE | UC2-intraday | all | h8+ | persistence | wt_med | 16.433 | 7.814 | 6.015 | 10.136 | 0.199 | 448309.000 | 113.000 | 61 | False | False |
| MAE | UC2-intraday | busy | h4-8 | persistence | wt_med | 20.227 | 5.059 | 3.988 | 6.007 | 0.500 | 791036.000 | 138.000 | 61 | False | True |
| MAE | UC2-intraday | busy | h8+ | persistence | wt_med | 26.539 | 12.656 | 10.276 | 14.989 | 0.250 | 191779.000 | 111.000 | 61 | False | False |
| MAE | UC3 | all | 45 | lvlh5_cbd | clim | 10.489 | 0.820 | -0.027 | 1.588 | 0.242 | 166969.000 | 37.000 | 16 | False | False |
| MAE | UC3 | all | 60 | lvlh5_tft | clim | 1.377 | 0.979 | 0.178 | 7.473 | 0.013 | 502.000 | 3.000 | 1 | False | False |
| MAE | UC3 | all | 60 | prod_served | clim | 1.663 | 1.265 | 0.000 | 12.700 | 0.013 | 502.000 | 3.000 | 1 | False | False |
| MAE | UC3 | all | 60 | prod_served_lin | clim | 1.663 | 1.265 | 0.000 | 12.700 | 0.013 | 502.000 | 3.000 | 1 | False | False |
| MAE | UC3 | busy | 45 | lvlh5_cbd | clim | 16.339 | 0.516 | -1.256 | 2.213 | 0.303 | 63056.000 | 35.000 | 16 | False | True |
| MAE | UC3 | busy | 60 | lvlh5_tft | clim | 11.473 | 7.473 | 7.473 | 7.473 | 0.005 | 50.000 | 1.000 | 1 | False | False |
| MAE | UC3 | busy | 60 | prod_served | clim | 16.700 | 12.700 | 12.700 | 12.700 | 0.005 | 50.000 | 1.000 | 1 | False | False |
| MAE | UC3 | busy | 60 | prod_served_lin | clim | 16.700 | 12.700 | 12.700 | 12.700 | 0.005 | 50.000 | 1.000 | 1 | False | False |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_cbd | clim | 11.559 | 0.025 | -1.148 | 1.287 | 0.164 | 69443.000 | 28.000 | 16 | False | False |
| MAE, schedule known at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 1.459 | 0.140 | -0.927 | 0.267 | 0.032 | 470.000 | 2.000 | 1 | False | False |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served | wt_med | 1.777 | 0.457 | 0.000 | 4.300 | 0.032 | 470.000 | 2.000 | 1 | False | False |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served_lin | wt_med | 1.777 | 0.457 | 0.000 | 4.300 | 0.032 | 470.000 | 2.000 | 1 | False | False |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 9.692 | 1.852 | 0.198 | 2.924 | 0.311 | 97555.000 | 21.000 | 16 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | lvlh5_tft | clim | 0.178 | 0.178 | 0.178 | 0.178 | 0.001 | 32.000 | 1.000 | 1 | False | False |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served | clim | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 32.000 | 1.000 | 1 | False | False |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served_lin | clim | 0.000 | 0.000 | 0.000 | 0.000 | 0.001 | 32.000 | 1.000 | 1 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_cbd | clim | 0.342 | -0.041 | -0.073 | -0.012 | 0.243 | 2572.000 | 36.000 | 16 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | lvlh5_tft | clim | 0.576 | 0.109 | 0.109 | 0.109 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served | clim | 0.616 | 0.149 | 0.149 | 0.149 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served_lin | clim | 0.610 | 0.143 | 0.143 | 0.143 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 0 | snaive7 | wt_med | 0.388 | -0.068 | -0.076 | -0.058 | 0.447 | 28150.000 | 138.000 | 61 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 1 | snaive7 | wt_med | 0.387 | -0.065 | -0.074 | -0.055 | 0.449 | 27403.000 | 138.000 | 60 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 2 | snaive7 | wt_med | 0.386 | -0.063 | -0.072 | -0.052 | 0.451 | 26673.000 | 138.000 | 59 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 3 | snaive7 | wt_med | 0.386 | -0.062 | -0.071 | -0.051 | 0.453 | 26023.000 | 138.000 | 58 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 4 | snaive7 | wt_med | 0.386 | -0.061 | -0.070 | -0.050 | 0.453 | 25320.000 | 138.000 | 57 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 5 | snaive7 | wt_med | 0.385 | -0.061 | -0.071 | -0.050 | 0.453 | 24622.000 | 138.000 | 56 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 6 | snaive7 | wt_med | 0.385 | -0.059 | -0.069 | -0.047 | 0.453 | 23870.000 | 138.000 | 55 | False | True |
| UC4 Spearman within park-month | UC4 | all | 45 | lvl_cbd | lvl_clim | -0.125 | -0.420 | -0.714 | -0.091 | 0.277 | 13.000 | 13.000 | 2 | False | False |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.731 | -0.043 | -0.074 | -0.006 | 0.213 | 2648.000 | 36.000 | 16 | False | False |
| best-time hit rate (top-2) | D3 | all | 60 | lvlh5_tft | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time hit rate (top-2) | D3 | all | 0 | snaive7 | wt_med | 0.803 | -0.031 | -0.038 | -0.024 | 0.473 | 32580.000 | 140.000 | 61 | False | True |
| best-time hit rate (top-2) | D3 | all | 1 | snaive7 | wt_med | 0.803 | -0.029 | -0.036 | -0.021 | 0.476 | 31789.000 | 139.000 | 60 | False | True |
| best-time hit rate (top-2) | D3 | all | 2 | snaive7 | wt_med | 0.804 | -0.027 | -0.034 | -0.019 | 0.478 | 30985.000 | 139.000 | 59 | False | True |
| best-time hit rate (top-2) | D3 | all | 3 | snaive7 | wt_med | 0.804 | -0.026 | -0.034 | -0.018 | 0.480 | 30192.000 | 139.000 | 58 | False | True |
| best-time hit rate (top-2) | D3 | all | 4 | snaive7 | wt_med | 0.804 | -0.026 | -0.033 | -0.018 | 0.480 | 29370.000 | 139.000 | 57 | False | True |
| best-time hit rate (top-2) | D3 | all | 5 | snaive7 | wt_med | 0.804 | -0.027 | -0.034 | -0.019 | 0.480 | 28544.000 | 139.000 | 56 | False | True |
| best-time hit rate (top-2) | D3 | all | 6 | snaive7 | wt_med | 0.804 | -0.027 | -0.035 | -0.018 | 0.481 | 27690.000 | 139.000 | 55 | False | True |
| best-time hit rate (±30 min) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.775 | -0.017 | -0.050 | 0.016 | 0.213 | 2648.000 | 36.000 | 16 | False | False |
| best-time hit rate (±30 min) | D3 | all | 60 | lvlh5_tft | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time hit rate (±30 min) | D3 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time hit rate (±30 min) | D3 | all | 0 | snaive7 | wt_med | 0.820 | -0.019 | -0.026 | -0.012 | 0.473 | 32580.000 | 140.000 | 61 | False | True |
| best-time hit rate (±30 min) | D3 | all | 1 | snaive7 | wt_med | 0.819 | -0.018 | -0.024 | -0.010 | 0.476 | 31789.000 | 139.000 | 60 | False | True |
| best-time hit rate (±30 min) | D3 | all | 2 | snaive7 | wt_med | 0.821 | -0.017 | -0.023 | -0.009 | 0.478 | 30985.000 | 139.000 | 59 | False | True |
| best-time hit rate (±30 min) | D3 | all | 3 | snaive7 | wt_med | 0.821 | -0.016 | -0.023 | -0.008 | 0.480 | 30192.000 | 139.000 | 58 | False | True |
| best-time hit rate (±30 min) | D3 | all | 4 | snaive7 | wt_med | 0.821 | -0.016 | -0.023 | -0.008 | 0.480 | 29370.000 | 139.000 | 57 | False | True |
| best-time hit rate (±30 min) | D3 | all | 5 | snaive7 | wt_med | 0.821 | -0.016 | -0.023 | -0.008 | 0.480 | 28544.000 | 139.000 | 56 | False | True |
| best-time hit rate (±30 min) | D3 | all | 6 | snaive7 | wt_med | 0.821 | -0.017 | -0.024 | -0.008 | 0.481 | 27690.000 | 139.000 | 55 | False | True |
| best-time regret (min) | D3 | all | 45 | lvlh5_cbd | wt_med | 4.504 | 0.184 | -0.726 | 0.901 | 0.213 | 2648.000 | 36.000 | 16 | False | False |
| best-time regret (min) | D3 | all | 60 | lvlh5_tft | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time regret (min) | D3 | all | 60 | prod_served | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time regret (min) | D3 | all | 60 | prod_served_lin | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.002 | 1.000 | 1.000 | 1 | False | False |
| best-time regret (min) | D3 | all | 0 | snaive7 | wt_med | 3.571 | 0.626 | 0.452 | 0.813 | 0.473 | 32580.000 | 140.000 | 61 | False | True |
| best-time regret (min) | D3 | all | 1 | snaive7 | wt_med | 3.583 | 0.611 | 0.427 | 0.799 | 0.476 | 31789.000 | 139.000 | 60 | False | True |
| best-time regret (min) | D3 | all | 2 | snaive7 | wt_med | 3.563 | 0.582 | 0.391 | 0.776 | 0.478 | 30985.000 | 139.000 | 59 | False | True |
| best-time regret (min) | D3 | all | 3 | snaive7 | wt_med | 3.545 | 0.566 | 0.371 | 0.762 | 0.480 | 30192.000 | 139.000 | 58 | False | True |
| best-time regret (min) | D3 | all | 4 | snaive7 | wt_med | 3.548 | 0.542 | 0.353 | 0.737 | 0.480 | 29370.000 | 139.000 | 57 | False | True |
| best-time regret (min) | D3 | all | 5 | snaive7 | wt_med | 3.545 | 0.539 | 0.354 | 0.733 | 0.480 | 28544.000 | 139.000 | 56 | False | True |
| best-time regret (min) | D3 | all | 6 | snaive7 | wt_med | 3.542 | 0.484 | 0.283 | 0.683 | 0.481 | 27690.000 | 139.000 | 55 | False | True |
| crowd bucket exact | D6 | all | 45 | lvl_cbd | lvl_naive4 | 0.243 | -0.029 | -0.171 | 0.102 | 0.279 | 313.000 | 37.000 | 16 | False | False |
| crowd bucket exact | D6 | all | 60 | lvl_tft | lvl_naive4 | 0.000 | -1.000 | -1.000 | -1.000 | 0.018 | 1.000 | 1.000 | 1 | False | False |
| dayPeak Spearman across rides | UC3 | all | 45 | lvlh5_cbd | clim | 0.583 | -0.140 | -0.189 | -0.089 | 0.271 | 248.000 | 31.000 | 16 | False | False |
| dayPeak abs error (min) | UC3 | all | 45 | lvlh5_cbd | clim | 14.351 | 3.819 | 1.753 | 5.571 | 0.203 | 3583.000 | 37.000 | 16 | False | False |
| dayPeak abs error (min) | UC3 | all | 60 | lvlh5_tft | clim | 0.279 | -0.034 | -0.744 | 0.203 | 0.017 | 16.000 | 3.000 | 1 | False | False |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served | clim | 0.625 | 0.312 | 0.000 | 5.000 | 0.017 | 16.000 | 3.000 | 1 | False | False |
| dayPeak abs error (min) | UC3 | all | 60 | prod_served_lin | clim | 0.625 | 0.312 | 0.000 | 5.000 | 0.017 | 16.000 | 3.000 | 1 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 45 | lvlh5_cbd | clim | 0.718 | -0.004 | -0.026 | 0.019 | 0.270 | 5647.000 | 32.000 | 16 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 0 | snaive7 | wt_med | 0.730 | -0.004 | -0.016 | 0.009 | 0.281 | 33080.000 | 133.000 | 61 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 1 | snaive7 | wt_med | 0.730 | -0.000 | -0.013 | 0.013 | 0.284 | 32404.000 | 132.000 | 60 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 2 | snaive7 | wt_med | 0.730 | 0.001 | -0.013 | 0.015 | 0.286 | 31684.000 | 132.000 | 59 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 3 | snaive7 | wt_med | 0.730 | 0.001 | -0.013 | 0.015 | 0.288 | 31006.000 | 132.000 | 58 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 4 | snaive7 | wt_med | 0.731 | 0.004 | -0.011 | 0.019 | 0.288 | 30159.000 | 131.000 | 57 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 5 | snaive7 | wt_med | 0.730 | 0.004 | -0.011 | 0.019 | 0.289 | 29430.000 | 131.000 | 56 | False | False |
| dayPeak pairwise ordering (headliners) | D4 | all | 6 | snaive7 | wt_med | 0.731 | 0.008 | -0.007 | 0.023 | 0.289 | 28523.000 | 130.000 | 55 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 45 | lvlh5_cbd | clim | 4.420 | -1.378 | -2.351 | -0.588 | 0.194 | 10480.000 | 31.000 | 16 | True | False |
| first-hour MAE (opening-aligned) | D5 | all | 60 | lvlh5_tft | wt_med | 0.469 | 0.000 | 0.000 | 0.000 | 0.023 | 64.000 | 3.000 | 1 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served | wt_med | 0.781 | 0.312 | 0.000 | 5.000 | 0.023 | 64.000 | 3.000 | 1 | False | False |
| first-hour MAE (opening-aligned) | D5 | all | 60 | prod_served_lin | wt_med | 0.781 | 0.312 | 0.000 | 5.000 | 0.023 | 64.000 | 3.000 | 1 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 45 | lvlh5_cbd | clim | 4.602 | -2.414 | -3.966 | -1.094 | 0.123 | 4496.000 | 23.000 | 16 | True | False |
| first-hour MAE, schedule known at origin | D5 | all | 60 | lvlh5_tft | wt_med | 0.500 | 0.000 | 0.000 | 0.000 | 0.042 | 60.000 | 2.000 | 1 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 60 | prod_served | wt_med | 0.833 | 0.333 | 0.000 | 5.000 | 0.042 | 60.000 | 2.000 | 1 | False | False |
| first-hour MAE, schedule known at origin | D5 | all | 60 | prod_served_lin | wt_med | 0.833 | 0.333 | 0.000 | 5.000 | 0.042 | 60.000 | 2.000 | 1 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 45 | lvlh5_cbd | clim | 4.279 | -0.599 | -1.214 | -0.197 | 0.344 | 5984.000 | 16.000 | 16 | False | True |
| first-hour MAE, schedule projected at origin | D5 | all | 60 | lvlh5_tft | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.003 | 4.000 | 1.000 | 1 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 60 | prod_served | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.003 | 4.000 | 1.000 | 1 | False | False |
| first-hour MAE, schedule projected at origin | D5 | all | 60 | prod_served_lin | wt_med | 0.000 | 0.000 | 0.000 | 0.000 | 0.003 | 4.000 | 1.000 | 1 | False | False |
| rope-drop worth agreement | D5 | all | 45 | lvlh5_cbd | clim | 0.869 | -0.002 | -0.011 | 0.008 | 0.203 | 3583.000 | 37.000 | 16 | False | False |
| rope-drop worth agreement | D5 | all | 60 | lvlh5_tft | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.017 | 16.000 | 3.000 | 1 | False | False |
| rope-drop worth agreement | D5 | all | 60 | prod_served | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.017 | 16.000 | 3.000 | 1 | False | False |
| rope-drop worth agreement | D5 | all | 60 | prod_served_lin | wt_med | 1.000 | 0.000 | 0.000 | 0.000 | 0.017 | 16.000 | 3.000 | 1 | False | False |
| rope-drop worth agreement | D5 | all | 0 | snaive7 | wt_med | 0.916 | -0.010 | -0.015 | -0.005 | 0.498 | 47443.000 | 146.000 | 61 | False | True |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 8.10 [7.96, 8.25] | 7.86 [7.72, 8.01] | 5.37 [5.28, 5.46] | 6.86 [6.71, 7.02] | 7.92 [7.78, 8.07] | 8.04 [7.89, 8.19] |
| 2 | 8.13 [7.99, 8.28] | 7.94 [7.81, 8.09] | 5.37 [5.28, 5.46] | 6.89 [6.74, 7.05] | 7.98 [7.84, 8.13] | 8.10 [7.95, 8.26] |
| 3 | 8.13 [7.99, 8.29] | 8.01 [7.87, 8.17] | 5.36 [5.27, 5.45] | 6.89 [6.74, 7.05] | 7.99 [7.84, 8.15] | 8.12 [7.97, 8.28] |
| 4 | 8.12 [7.97, 8.28] | 8.08 [7.93, 8.24] | 5.35 [5.26, 5.44] | 6.88 [6.73, 7.05] | 8.01 [7.86, 8.17] | 8.14 [7.98, 8.30] |
| 5 | 8.15 [8.01, 8.32] | 8.13 [7.97, 8.30] | 5.36 [5.27, 5.46] | 6.91 [6.75, 7.08] | 8.07 [7.91, 8.23] | 8.20 [8.04, 8.37] |
| 6 | 8.20 [8.05, 8.37] | 8.27 [8.10, 8.44] | 5.39 [5.30, 5.49] | 6.93 [6.77, 7.09] | 8.17 [8.01, 8.33] | 8.30 [8.14, 8.47] |
| 7 | 8.65 [8.48, 8.83] | 8.51 [8.34, 8.70] | 5.43 [5.33, 5.53] | 7.59 [7.42, 7.76] | 8.32 [8.15, 8.48] | 8.45 [8.28, 8.62] |
| 10 | 8.72 [8.55, 8.90] | 8.61 [8.44, 8.80] | 5.37 [5.27, 5.47] | 7.66 [7.48, 7.84] | 8.41 [8.24, 8.58] | 8.56 [8.39, 8.73] |
| 14 | 9.10 [8.91, 9.30] | 8.94 [8.74, 9.16] | 5.36 [5.26, 5.48] | 8.20 [7.99, 8.40] | 8.59 [8.41, 8.78] | 8.75 [8.56, 8.94] |
| 21 | 9.39 [9.17, 9.62] | 9.07 [8.84, 9.29] | 5.30 [5.18, 5.42] | 8.66 [8.43, 8.90] | 8.72 [8.51, 8.95] | 8.92 [8.70, 9.15] |
| 30 | 9.70 [9.43, 9.96] | 9.47 [9.20, 9.73] | 5.31 [5.17, 5.45] | 9.08 [8.81, 9.36] | 8.94 [8.68, 9.20] | 9.16 [8.90, 9.42] |
| 45 | 10.09 [9.71, 10.48] | 10.01 [9.62, 10.38] | 5.75 [5.53, 5.96] | 9.39 [9.00, 9.83] | 9.33 [8.94, 9.72] | 9.50 [9.11, 9.88] |
| 60 | 9.10 [7.88, 10.51] | 1.38 [0.18, 11.47] | 4.94 [4.01, 6.03] | 9.19 [7.79, 10.83] | 8.12 [6.94, 9.35] | 8.30 [7.13, 9.54] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 8.07 [7.90, 8.23] | 7.71 [7.57, 7.86] | 7.92 [7.78, 8.08] | 7.59 [7.44, 7.73] | 4.86 [4.79, 4.94] | 4.60 [4.53, 4.66] | 4.55 [4.49, 4.62] | 2.46 [2.42, 2.52] | 8.42 [8.25, 8.61] | 7.81 [7.66, 7.96] |
| m030 | 8.07 [7.90, 8.23] | 7.71 [7.57, 7.86] | 7.94 [7.80, 8.09] | 7.63 [7.49, 7.77] | 5.38 [5.29, 5.45] | 5.02 [4.94, 5.10] | 4.98 [4.91, 5.06] | 4.09 [4.02, 4.16] | 8.43 [8.25, 8.60] | 7.85 [7.70, 7.99] |
| m045 | 8.14 [7.97, 8.30] | 7.76 [7.62, 7.91] | 7.99 [7.85, 8.14] | 7.69 [7.54, 7.83] | 5.81 [5.72, 5.89] | 5.36 [5.28, 5.44] | 5.32 [5.24, 5.40] | 5.04 [4.95, 5.13] | 8.46 [8.28, 8.64] | 7.89 [7.75, 8.04] |
| m060 | 8.21 [8.04, 8.38] | 7.83 [7.68, 7.98] | 8.04 [7.90, 8.19] | 7.73 [7.59, 7.87] | 6.25 [6.17, 6.35] | 5.68 [5.60, 5.76] | 5.64 [5.56, 5.72] | 5.70 [5.60, 5.80] | 8.52 [8.34, 8.69] | 7.96 [7.81, 8.11] |
| m075 | 8.23 [8.07, 8.39] | 7.90 [7.76, 8.05] | 8.14 [7.99, 8.29] | 7.79 [7.64, 7.93] | 6.99 [6.88, 7.09] | 6.34 [6.25, 6.44] | 6.28 [6.19, 6.38] | 6.31 [6.21, 6.42] | 8.60 [8.41, 8.77] | 7.98 [7.84, 8.13] |
| m090 | 8.30 [8.13, 8.46] | 7.91 [7.77, 8.07] | 8.18 [8.04, 8.34] | 7.84 [7.69, 7.98] | 7.06 [6.96, 7.17] | 6.39 [6.29, 6.49] | 6.31 [6.22, 6.41] | 6.85 [6.73, 6.97] | 8.62 [8.43, 8.79] | 8.03 [7.88, 8.18] |
| m105 | 8.34 [8.18, 8.51] | 7.95 [7.80, 8.09] | 8.21 [8.07, 8.37] | 7.87 [7.72, 8.01] | 7.22 [7.12, 7.33] | 6.51 [6.41, 6.60] | 6.41 [6.32, 6.51] | 7.28 [7.17, 7.41] | 8.66 [8.48, 8.83] | 8.06 [7.91, 8.21] |
| m120 | 8.41 [8.25, 8.58] | 7.99 [7.84, 8.14] | 8.22 [8.08, 8.37] | 7.87 [7.73, 8.01] | 7.51 [7.41, 7.62] | 6.71 [6.62, 6.82] | 6.60 [6.51, 6.71] | 7.66 [7.54, 7.79] | 8.71 [8.53, 8.89] | 8.11 [7.97, 8.26] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 6.16 [5.52, 6.88] | 5.74 [5.14, 6.37] | 5.97 [5.36, 6.65] | 5.61 [5.07, 6.23] | 2.34 [2.09, 2.62] | 2.20 [1.99, 2.44] | 2.15 [1.94, 2.38] | 6.42 [5.76, 7.21] | 5.83 [5.21, 6.48] | persistence |
| m030 | 4.63 [4.06, 5.24] | 4.14 [3.64, 4.69] | 4.40 [3.86, 4.97] | 4.06 [3.60, 4.60] | 1.18 [1.02, 1.36] | 0.98 [0.79, 1.18] | 0.93 [0.75, 1.13] | 4.88 [4.31, 5.52] | 4.27 [3.76, 4.84] | persistence |
| m045 | 3.77 [3.26, 4.34] | 3.23 [2.79, 3.69] | 3.52 [3.05, 4.01] | 3.16 [2.77, 3.61] | 0.55 [0.40, 0.70] | 0.31 [0.11, 0.50] | 0.25 [0.06, 0.45] | 3.98 [3.51, 4.52] | 3.35 [2.90, 3.82] | persistence |
| m060 | 3.16 [2.70, 3.67] | 2.58 [2.18, 3.01] | 2.85 [2.43, 3.30] | 2.47 [2.14, 2.90] | 0.17 [0.01, 0.33] | -0.14 [-0.35, 0.09] | -0.19 [-0.40, 0.03] | 3.32 [2.89, 3.82] | 2.68 [2.28, 3.12] | persistence |
| m075 | 2.73 [2.31, 3.22] | 2.06 [1.68, 2.48] | 2.37 [1.98, 2.79] | 1.93 [1.61, 2.29] | 0.55 [0.40, 0.70] | 0.04 [-0.20, 0.28] | 0.02 [-0.21, 0.25] | 2.90 [2.46, 3.37] | 2.16 [1.77, 2.59] | persistence |
| m090 | 2.23 [1.81, 2.71] | 1.48 [1.12, 1.90] | 1.84 [1.46, 2.25] | 1.42 [1.10, 1.76] | -0.03 [-0.16, 0.10] | -0.52 [-0.78, -0.25]* | -0.55 [-0.81, -0.27]* | 2.36 [1.96, 2.78] | 1.61 [1.23, 2.04] | persistence |
| m105 | 1.84 [1.44, 2.29] | 1.03 [0.66, 1.43] | 1.42 [1.03, 1.85] | 0.99 [0.68, 1.32] | -0.41 [-0.58, -0.25]* | -0.90 [-1.19, -0.61]* | -0.95 [-1.24, -0.65]* | 1.94 [1.54, 2.36] | 1.17 [0.79, 1.57] | persistence |
| m120 | 1.45 [1.06, 1.88] | 0.62 [0.25, 1.00] | 0.98 [0.61, 1.39] | 0.55 [0.23, 0.89] | -0.64 [-0.84, -0.44]* | -1.19 [-1.51, -0.88]* | -1.25 [-1.56, -0.93]* | 1.57 [1.20, 1.96] | 0.76 [0.39, 1.14] | persistence |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 6.423 [5.756, 7.207] / [6.254, 6.593] | 5.830 [5.212, 6.478] / [5.690, 5.979] | 6.164 [5.522, 6.875] / [6.001, 6.325] | 5.739 [5.144, 6.374] / [5.601, 5.882] | 5.967 [5.356, 6.648] / [5.826, 6.117] | 5.609 [5.069, 6.233] / [5.467, 5.747] |
| m030 | 4.884 [4.314, 5.517] / [4.721, 5.043] | 4.275 [3.762, 4.841] / [4.143, 4.414] | 4.627 [4.060, 5.237] / [4.472, 4.778] | 4.144 [3.644, 4.692] / [4.013, 4.282] | 4.395 [3.860, 4.970] / [4.260, 4.537] | 4.060 [3.598, 4.598] / [3.930, 4.193] |
| m045 | 3.984 [3.507, 4.516] / [3.830, 4.135] | 3.347 [2.896, 3.823] / [3.219, 3.476] | 3.774 [3.260, 4.340] / [3.631, 3.920] | 3.233 [2.795, 3.690] / [3.106, 3.362] | 3.522 [3.050, 4.010] / [3.392, 3.661] | 3.159 [2.770, 3.608] / [3.037, 3.283] |
| m060 | 3.321 [2.889, 3.819] / [3.170, 3.472] | 2.678 [2.285, 3.122] / [2.551, 2.807] | 3.162 [2.703, 3.672] / [3.014, 3.301] | 2.577 [2.179, 3.008] / [2.451, 2.705] | 2.848 [2.427, 3.299] / [2.714, 2.982] | 2.471 [2.136, 2.895] / [2.344, 2.587] |
| m075 | 2.898 [2.456, 3.368] / [2.736, 3.053] | 2.160 [1.773, 2.589] / [2.028, 2.297] | 2.731 [2.309, 3.218] / [2.589, 2.867] | 2.060 [1.683, 2.481] / [1.935, 2.182] | 2.372 [1.983, 2.790] / [2.245, 2.503] | 1.934 [1.614, 2.293] / [1.813, 2.046] |
| m090 | 2.359 [1.962, 2.777] / [2.202, 2.511] | 1.609 [1.231, 2.036] / [1.477, 1.738] | 2.226 [1.809, 2.711] / [2.083, 2.367] | 1.480 [1.121, 1.904] / [1.350, 1.609] | 1.840 [1.459, 2.251] / [1.712, 1.973] | 1.418 [1.104, 1.755] / [1.294, 1.533] |
| m105 | 1.943 [1.545, 2.364] / [1.790, 2.097] | 1.170 [0.791, 1.574] / [1.038, 1.303] | 1.840 [1.438, 2.285] / [1.698, 1.982] | 1.033 [0.663, 1.426] / [0.903, 1.167] | 1.421 [1.030, 1.845] / [1.290, 1.557] | 0.988 [0.679, 1.324] / [0.867, 1.103] |
| m120 | 1.569 [1.196, 1.963] / [1.417, 1.719] | 0.759 [0.391, 1.139] / [0.623, 0.887] | 1.453 [1.059, 1.876] / [1.312, 1.594] | 0.621 [0.247, 1.001] / [0.491, 0.748] | 0.983 [0.610, 1.390] / [0.851, 1.111] | 0.551 [0.230, 0.890] / [0.428, 0.667] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -6.423 [-7.207, -5.756]* / [-6.593, -6.254]* | -0.702 [-1.019, -0.368]* / [-0.829, -0.568]* | -0.219 [-0.489, 0.077] / [-0.366, -0.059]*  ! | -0.789 [-1.103, -0.468]* / [-0.917, -0.657]* | -0.577 [-0.818, -0.328]* / [-0.690, -0.462]* | -0.981 [-1.202, -0.764]* / [-1.098, -0.853]* |
| m030 | -4.884 [-5.517, -4.314]* / [-5.043, -4.721]* | -0.707 [-1.024, -0.380]* / [-0.832, -0.569]* | -0.249 [-0.514, 0.037] / [-0.392, -0.083]*  ! | -0.827 [-1.156, -0.509]* / [-0.956, -0.687]* | -0.597 [-0.848, -0.344]* / [-0.711, -0.479]* | -0.974 [-1.209, -0.757]* / [-1.091, -0.852]* |
| m045 | -3.984 [-4.516, -3.507]* / [-4.135, -3.830]* | -0.696 [-1.009, -0.370]* / [-0.826, -0.555]* | -0.227 [-0.498, 0.070] / [-0.370, -0.070]*  ! | -0.810 [-1.128, -0.489]* / [-0.937, -0.673]* | -0.578 [-0.847, -0.313]* / [-0.689, -0.465]* | -0.954 [-1.182, -0.739]* / [-1.064, -0.834]* |
| m060 | -3.321 [-3.819, -2.889]* / [-3.472, -3.170]* | -0.685 [-1.006, -0.354]* / [-0.815, -0.540]* | -0.206 [-0.479, 0.088] / [-0.351, -0.044]*  ! | -0.790 [-1.103, -0.462]* / [-0.922, -0.645]* | -0.591 [-0.860, -0.333]* / [-0.703, -0.471]* | -0.964 [-1.194, -0.749]* / [-1.076, -0.842]* |
| m075 | -2.898 [-3.368, -2.456]* / [-3.053, -2.736]* | -0.697 [-1.031, -0.351]* / [-0.829, -0.555]* | -0.250 [-0.525, 0.042] / [-0.400, -0.089]*  ! | -0.774 [-1.128, -0.425]* / [-0.903, -0.636]* | -0.555 [-0.835, -0.280]* / [-0.671, -0.438]* | -0.960 [-1.205, -0.732]* / [-1.072, -0.836]* |
| m090 | -2.359 [-2.777, -1.962]* / [-2.511, -2.202]* | -0.699 [-1.014, -0.381]* / [-0.834, -0.560]* | -0.225 [-0.478, 0.047] / [-0.372, -0.061]*  ! | -0.805 [-1.150, -0.472]* / [-0.943, -0.665]* | -0.552 [-0.828, -0.282]* / [-0.669, -0.431]* | -0.949 [-1.194, -0.725]* / [-1.063, -0.827]* |
| m105 | -1.943 [-2.364, -1.545]* / [-2.097, -1.790]* | -0.708 [-1.028, -0.387]* / [-0.846, -0.571]* | -0.228 [-0.482, 0.050] / [-0.380, -0.068]*  ! | -0.812 [-1.129, -0.495]* / [-0.951, -0.674]* | -0.557 [-0.816, -0.298]* / [-0.669, -0.442]* | -0.967 [-1.197, -0.748]* / [-1.078, -0.844]* |
| m120 | -1.569 [-1.963, -1.196]* / [-1.719, -1.417]* | -0.703 [-1.007, -0.383]* / [-0.837, -0.560]* | -0.214 [-0.478, 0.063] / [-0.366, -0.051]*  ! | -0.814 [-1.124, -0.496]* / [-0.948, -0.675]* | -0.601 [-0.855, -0.351]* / [-0.713, -0.482]* | -1.010 [-1.238, -0.796]* / [-1.123, -0.885]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.830 [-6.478, -5.212]* / [-5.979, -5.690]* | 0.702 [0.368, 1.019] / [0.568, 0.829] | 0.396 [0.239, 0.562] / [0.337, 0.467] | -0.099 [-0.154, -0.054]* / [-0.116, -0.081]* | 0.127 [0.016, 0.230] / [0.070, 0.185] | -0.250 [-0.506, -0.027]* / [-0.342, -0.165]* |
| m030 | -4.275 [-4.841, -3.762]* / [-4.414, -4.143]* | 0.707 [0.380, 1.024] / [0.569, 0.832] | 0.373 [0.225, 0.532] / [0.308, 0.445] | -0.132 [-0.180, -0.096]* / [-0.147, -0.117]* | 0.113 [0.010, 0.212] / [0.056, 0.169] | -0.242 [-0.486, -0.026]* / [-0.336, -0.159]* |
| m045 | -3.347 [-3.823, -2.896]* / [-3.476, -3.219]* | 0.696 [0.370, 1.009] / [0.555, 0.826] | 0.382 [0.232, 0.545] / [0.316, 0.455] | -0.124 [-0.170, -0.087]* / [-0.139, -0.109]* | 0.120 [0.021, 0.227] / [0.068, 0.173] | -0.229 [-0.460, -0.032]* / [-0.319, -0.146]* |
| m060 | -2.678 [-3.122, -2.285]* / [-2.807, -2.551]* | 0.685 [0.354, 1.006] / [0.540, 0.815] | 0.390 [0.243, 0.551] / [0.326, 0.461] | -0.118 [-0.158, -0.083]* / [-0.134, -0.104]* | 0.100 [-0.001, 0.208] / [0.047, 0.151] | -0.245 [-0.469, -0.048]* / [-0.336, -0.165]* |
| m075 | -2.160 [-2.589, -1.773]* / [-2.297, -2.028]* | 0.697 [0.351, 1.031] / [0.555, 0.829] | 0.372 [0.223, 0.531] / [0.308, 0.446] | -0.080 [-0.128, -0.041]* / [-0.098, -0.065]* | 0.164 [0.054, 0.277] / [0.111, 0.222] | -0.223 [-0.457, -0.005]* / [-0.319, -0.133]* |
| m090 | -1.609 [-2.036, -1.231]* / [-1.738, -1.477]* | 0.699 [0.381, 1.014] / [0.560, 0.834] | 0.391 [0.229, 0.560] / [0.328, 0.463] | -0.113 [-0.166, -0.071]* / [-0.129, -0.097]* | 0.158 [0.053, 0.271] / [0.107, 0.214] | -0.215 [-0.446, -0.004]* / [-0.310, -0.131]* |
| m105 | -1.170 [-1.574, -0.791]* / [-1.303, -1.038]* | 0.708 [0.387, 1.028] / [0.571, 0.846] | 0.409 [0.247, 0.572] / [0.346, 0.479] | -0.107 [-0.144, -0.078]* / [-0.123, -0.093]* | 0.170 [0.057, 0.280] / [0.115, 0.229] | -0.215 [-0.446, 0.006] / [-0.311, -0.125]*  ! |
| m120 | -0.759 [-1.139, -0.391]* / [-0.887, -0.623]* | 0.703 [0.383, 1.007] / [0.560, 0.837] | 0.412 [0.255, 0.574] / [0.350, 0.482] | -0.114 [-0.157, -0.079]* / [-0.132, -0.097]* | 0.119 [0.003, 0.231] / [0.060, 0.178] | -0.264 [-0.493, -0.050]* / [-0.359, -0.174]* |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -6.164 [-6.875, -5.522]* / [-6.325, -6.001]* | 0.219 [-0.077, 0.489] / [0.059, 0.366] | -0.396 [-0.562, -0.239]* / [-0.467, -0.337]* | -0.495 [-0.659, -0.338]* / [-0.561, -0.434]* | -0.295 [-0.441, -0.155]* / [-0.385, -0.205]* | -0.783 [-1.032, -0.550]* / [-0.887, -0.682]* |
| m030 | -4.627 [-5.237, -4.060]* / [-4.778, -4.472]* | 0.249 [-0.037, 0.514] / [0.083, 0.392] | -0.373 [-0.532, -0.225]* / [-0.445, -0.308]* | -0.507 [-0.677, -0.354]* / [-0.574, -0.445]* | -0.287 [-0.421, -0.149]* / [-0.377, -0.197]* | -0.755 [-0.988, -0.542]* / [-0.858, -0.655]* |
| m045 | -3.774 [-4.340, -3.260]* / [-3.920, -3.631]* | 0.227 [-0.070, 0.498] / [0.070, 0.370] | -0.382 [-0.545, -0.232]* / [-0.455, -0.316]* | -0.512 [-0.681, -0.350]* / [-0.581, -0.449]* | -0.292 [-0.440, -0.151]* / [-0.382, -0.207]* | -0.753 [-0.979, -0.535]* / [-0.852, -0.656]* |
| m060 | -3.162 [-3.672, -2.703]* / [-3.301, -3.014]* | 0.206 [-0.088, 0.479] / [0.044, 0.351] | -0.390 [-0.551, -0.243]* / [-0.461, -0.326]* | -0.514 [-0.677, -0.363]* / [-0.583, -0.454]* | -0.323 [-0.470, -0.182]* / [-0.408, -0.237]* | -0.776 [-1.010, -0.562]* / [-0.876, -0.678]* |
| m075 | -2.731 [-3.218, -2.309]* / [-2.867, -2.589]* | 0.250 [-0.042, 0.525] / [0.089, 0.400] | -0.372 [-0.531, -0.223]* / [-0.446, -0.308]* | -0.454 [-0.612, -0.294]* / [-0.522, -0.395]* | -0.237 [-0.384, -0.094]* / [-0.325, -0.150]* | -0.722 [-0.963, -0.496]* / [-0.831, -0.624]* |
| m090 | -2.226 [-2.711, -1.809]* / [-2.367, -2.083]* | 0.225 [-0.047, 0.478] / [0.061, 0.372] | -0.391 [-0.560, -0.229]* / [-0.463, -0.328]* | -0.507 [-0.686, -0.332]* / [-0.574, -0.447]* | -0.264 [-0.429, -0.100]* / [-0.353, -0.177]* | -0.738 [-0.977, -0.512]* / [-0.842, -0.640]* |
| m105 | -1.840 [-2.285, -1.438]* / [-1.982, -1.698]* | 0.228 [-0.050, 0.482] / [0.068, 0.380] | -0.409 [-0.572, -0.247]* / [-0.479, -0.346]* | -0.517 [-0.674, -0.357]* / [-0.584, -0.459]* | -0.271 [-0.411, -0.128]* / [-0.358, -0.185]* | -0.761 [-0.995, -0.552]* / [-0.866, -0.659]* |
| m120 | -1.453 [-1.876, -1.059]* / [-1.594, -1.312]* | 0.214 [-0.063, 0.478] / [0.051, 0.366] | -0.412 [-0.574, -0.255]* / [-0.482, -0.350]* | -0.528 [-0.693, -0.374]* / [-0.596, -0.468]* | -0.324 [-0.475, -0.175]* / [-0.410, -0.238]* | -0.816 [-1.052, -0.599]* / [-0.921, -0.712]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 15.85 [15.52, 16.16] | 14.67 [14.41, 14.92] | 14.88 [14.61, 15.16] | 13.77 [13.51, 14.03] | 8.76 [8.65, 8.88] | 8.22 [8.11, 8.32] | 8.14 [8.04, 8.24] | 4.42 [4.34, 4.51] | 16.05 [15.70, 16.41] | 14.94 [14.69, 15.20] |
| m030 | 15.83 [15.52, 16.14] | 14.70 [14.44, 14.95] | 14.91 [14.64, 15.19] | 13.85 [13.59, 14.11] | 9.65 [9.53, 9.77] | 8.95 [8.84, 9.06] | 8.88 [8.78, 8.99] | 7.31 [7.20, 7.43] | 16.02 [15.69, 16.37] | 14.99 [14.73, 15.26] |
| m045 | 15.90 [15.59, 16.19] | 14.75 [14.50, 15.00] | 14.98 [14.72, 15.25] | 13.93 [13.69, 14.19] | 10.44 [10.32, 10.56] | 9.55 [9.44, 9.67] | 9.49 [9.37, 9.60] | 8.99 [8.87, 9.11] | 16.05 [15.72, 16.39] | 15.04 [14.78, 15.28] |
| m060 | 15.96 [15.65, 16.26] | 14.82 [14.57, 15.08] | 15.02 [14.75, 15.31] | 13.98 [13.73, 14.24] | 11.25 [11.12, 11.38] | 10.09 [9.97, 10.22] | 10.04 [9.91, 10.16] | 10.19 [10.04, 10.32] | 16.08 [15.74, 16.42] | 15.11 [14.86, 15.36] |
| m075 | 15.89 [15.58, 16.18] | 14.85 [14.59, 15.11] | 15.04 [14.77, 15.31] | 13.92 [13.67, 14.19] | 12.57 [12.40, 12.76] | 11.35 [11.20, 11.50] | 11.26 [11.12, 11.41] | 11.20 [11.05, 11.35] | 16.15 [15.80, 16.49] | 15.09 [14.84, 15.34] |
| m090 | 16.02 [15.71, 16.32] | 14.88 [14.62, 15.14] | 15.12 [14.86, 15.40] | 14.01 [13.76, 14.27] | 12.66 [12.51, 12.83] | 11.40 [11.25, 11.55] | 11.28 [11.13, 11.43] | 12.12 [11.96, 12.30] | 16.11 [15.77, 16.45] | 15.15 [14.89, 15.40] |
| m105 | 16.07 [15.76, 16.35] | 14.90 [14.63, 15.15] | 15.13 [14.86, 15.41] | 14.00 [13.75, 14.26] | 12.89 [12.74, 13.04] | 11.58 [11.43, 11.73] | 11.40 [11.26, 11.55] | 12.87 [12.70, 13.04] | 16.14 [15.80, 16.49] | 15.15 [14.90, 15.42] |
| m120 | 16.20 [15.89, 16.49] | 14.94 [14.67, 15.20] | 15.12 [14.86, 15.40] | 13.99 [13.73, 14.25] | 13.38 [13.22, 13.56] | 11.94 [11.78, 12.11] | 11.72 [11.57, 11.88] | 13.58 [13.41, 13.77] | 16.22 [15.88, 16.57] | 15.24 [14.98, 15.49] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | 11.97 [10.93, 13.05] | 10.79 [9.87, 11.75] | 10.98 [10.00, 11.96] | 9.86 [9.07, 10.70] | 4.11 [3.67, 4.62] | 3.77 [3.42, 4.13] | 3.68 [3.32, 4.05] | 12.04 [10.91, 13.15] | 11.02 [10.05, 12.00] | persistence |
| m030 | 9.06 [8.00, 10.15] | 7.91 [6.96, 8.86] | 8.07 [7.07, 9.07] | 7.02 [6.28, 7.85] | 1.93 [1.60, 2.28] | 1.50 [1.14, 1.89] | 1.41 [1.05, 1.80] | 9.15 [8.13, 10.19] | 8.16 [7.21, 9.15] | persistence |
| m045 | 7.48 [6.49, 8.51] | 6.27 [5.40, 7.17] | 6.50 [5.60, 7.44] | 5.41 [4.74, 6.17] | 0.76 [0.46, 1.07] | 0.25 [-0.10, 0.64] | 0.16 [-0.20, 0.54] | 7.53 [6.62, 8.42] | 6.48 [5.62, 7.38] | persistence |
| m060 | 6.33 [5.35, 7.37] | 5.05 [4.24, 5.93] | 5.25 [4.40, 6.19] | 4.15 [3.51, 4.90] | -0.03 [-0.32, 0.32] | -0.62 [-1.01, -0.18]* | -0.71 [-1.09, -0.29]* | 6.27 [5.44, 7.12] | 5.27 [4.44, 6.15] | persistence |
| m075 | 5.53 [4.59, 6.53] | 4.10 [3.32, 4.92] | 4.40 [3.60, 5.24] | 3.22 [2.65, 3.87] | 0.86 [0.54, 1.18] | -0.16 [-0.53, 0.25] | -0.18 [-0.54, 0.22] | 5.56 [4.75, 6.38] | 4.33 [3.52, 5.16] | persistence |
| m090 | 4.65 [3.73, 5.64] | 3.09 [2.29, 3.94] | 3.45 [2.64, 4.36] | 2.29 [1.68, 2.89] | -0.24 [-0.47, 0.01] | -1.19 [-1.60, -0.71]* | -1.24 [-1.65, -0.77]* | 4.55 [3.83, 5.32] | 3.35 [2.53, 4.19] | persistence |
| m105 | 4.00 [3.09, 4.97] | 2.31 [1.53, 3.14] | 2.71 [1.92, 3.57] | 1.52 [0.93, 2.16] | -1.00 [-1.24, -0.70]* | -1.90 [-2.35, -1.40]* | -1.99 [-2.44, -1.48]* | 3.84 [3.05, 4.69] | 2.61 [1.81, 3.44] | persistence |
| m120 | 3.28 [2.39, 4.22] | 1.53 [0.72, 2.37] | 1.87 [1.04, 2.72] | 0.71 [0.14, 1.34] | -1.53 [-1.85, -1.17]* | -2.52 [-3.02, -1.95]* | -2.62 [-3.12, -2.06]* | 3.13 [2.37, 3.97] | 1.86 [1.06, 2.67] | persistence |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 12.039 [10.913, 13.153] / [11.679, 12.401] | 11.021 [10.045, 11.999] / [10.735, 11.296] | 11.972 [10.929, 13.048] / [11.636, 12.297] | 10.792 [9.869, 11.752] / [10.510, 11.065] | 10.977 [9.999, 11.961] / [10.670, 11.277] | 9.857 [9.065, 10.696] / [9.579, 10.139] |
| m030 | 9.154 [8.128, 10.189] / [8.787, 9.523] | 8.159 [7.210, 9.152] / [7.862, 8.447] | 9.055 [7.996, 10.150] / [8.727, 9.382] | 7.907 [6.963, 8.861] / [7.621, 8.189] | 8.071 [7.072, 9.068] / [7.764, 8.379] | 7.022 [6.283, 7.846] / [6.756, 7.306] |
| m045 | 7.535 [6.621, 8.424] / [7.178, 7.881] | 6.482 [5.619, 7.378] / [6.202, 6.752] | 7.478 [6.487, 8.511] / [7.174, 7.798] | 6.267 [5.401, 7.166] / [5.991, 6.542] | 6.495 [5.604, 7.435] / [6.203, 6.793] | 5.409 [4.735, 6.169] / [5.156, 5.676] |
| m060 | 6.270 [5.436, 7.124] / [5.927, 6.599] | 5.273 [4.443, 6.148] / [4.991, 5.553] | 6.326 [5.351, 7.367] / [6.012, 6.645] | 5.047 [4.237, 5.931] / [4.768, 5.318] | 5.252 [4.397, 6.191] / [4.961, 5.548] | 4.154 [3.514, 4.899] / [3.896, 4.418] |
| m075 | 5.558 [4.746, 6.385] / [5.194, 5.908] | 4.325 [3.520, 5.157] / [4.050, 4.610] | 5.531 [4.585, 6.529] / [5.217, 5.845] | 4.102 [3.318, 4.922] / [3.841, 4.386] | 4.400 [3.602, 5.239] / [4.122, 4.702] | 3.225 [2.653, 3.866] / [2.969, 3.494] |
| m090 | 4.553 [3.833, 5.316] / [4.209, 4.899] | 3.352 [2.528, 4.188] / [3.077, 3.631] | 4.646 [3.726, 5.639] / [4.327, 4.962] | 3.091 [2.292, 3.938] / [2.819, 3.381] | 3.450 [2.640, 4.356] / [3.156, 3.759] | 2.287 [1.685, 2.889] / [2.016, 2.550] |
| m105 | 3.839 [3.045, 4.693] / [3.501, 4.206] | 2.607 [1.814, 3.444] / [2.325, 2.905] | 3.998 [3.094, 4.974] / [3.673, 4.335] | 2.315 [1.531, 3.141] / [2.031, 2.610] | 2.711 [1.917, 3.574] / [2.402, 3.014] | 1.520 [0.930, 2.161] / [1.263, 1.784] |
| m120 | 3.134 [2.373, 3.973] / [2.801, 3.482] | 1.858 [1.058, 2.673] / [1.567, 2.162] | 3.284 [2.386, 4.216] / [2.964, 3.609] | 1.534 [0.721, 2.372] / [1.261, 1.830] | 1.871 [1.045, 2.717] / [1.570, 2.170] | 0.706 [0.138, 1.344] / [0.444, 0.969] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -12.039 [-13.153, -10.913]* / [-12.401, -11.679]* | -1.135 [-1.886, -0.317]* / [-1.446, -0.831]* | -0.175 [-0.848, 0.535] / [-0.510, 0.192] | -1.375 [-2.110, -0.617]* / [-1.681, -1.076]* | -1.264 [-1.811, -0.682]* / [-1.537, -0.980]* | -2.401 [-2.788, -1.971]* / [-2.668, -2.115]* |
| m030 | -9.154 [-10.189, -8.128]* / [-9.523, -8.787]* | -1.139 [-1.903, -0.316]* / [-1.445, -0.835]* | -0.236 [-0.906, 0.473] / [-0.586, 0.136] | -1.397 [-2.154, -0.606]* / [-1.698, -1.098]* | -1.278 [-1.833, -0.662]* / [-1.544, -1.012]* | -2.352 [-2.760, -1.913]* / [-2.610, -2.077]* |
| m045 | -7.535 [-8.424, -6.621]* / [-7.881, -7.178]* | -1.133 [-1.879, -0.315]* / [-1.448, -0.822]* | -0.223 [-0.884, 0.477] / [-0.560, 0.145] | -1.371 [-2.144, -0.574]* / [-1.669, -1.072]* | -1.236 [-1.843, -0.598]* / [-1.503, -0.970]* | -2.304 [-2.713, -1.869]* / [-2.557, -2.032]* |
| m060 | -6.270 [-7.124, -5.436]* / [-6.599, -5.927]* | -1.062 [-1.809, -0.252]* / [-1.368, -0.754]* | -0.168 [-0.848, 0.551] / [-0.506, 0.196] | -1.319 [-2.073, -0.536]* / [-1.619, -1.019]* | -1.231 [-1.846, -0.596]* / [-1.491, -0.965]* | -2.282 [-2.703, -1.849]* / [-2.539, -2.014]* |
| m075 | -5.558 [-6.385, -4.746]* / [-5.908, -5.194]* | -1.122 [-1.917, -0.299]* / [-1.431, -0.814]* | -0.289 [-0.954, 0.410] / [-0.633, 0.079] | -1.358 [-2.167, -0.537]* / [-1.672, -1.042]* | -1.275 [-1.908, -0.606]* / [-1.560, -1.005]* | -2.409 [-2.852, -1.952]* / [-2.678, -2.121]* |
| m090 | -4.553 [-5.316, -3.833]* / [-4.899, -4.209]* | -1.057 [-1.808, -0.298]* / [-1.373, -0.742]* | -0.136 [-0.777, 0.522] / [-0.477, 0.242] | -1.316 [-2.104, -0.539]* / [-1.621, -0.996]* | -1.177 [-1.798, -0.514]* / [-1.448, -0.904]* | -2.314 [-2.727, -1.880]* / [-2.566, -2.029]* |
| m105 | -3.839 [-4.693, -3.045]* / [-4.206, -3.501]* | -1.070 [-1.804, -0.301]* / [-1.379, -0.770]* | -0.132 [-0.770, 0.520] / [-0.473, 0.232] | -1.319 [-2.060, -0.572]* / [-1.626, -1.017]* | -1.184 [-1.785, -0.570]* / [-1.446, -0.910]* | -2.337 [-2.724, -1.928]* / [-2.602, -2.054]* |
| m120 | -3.134 [-3.973, -2.373]* / [-3.482, -2.801]* | -1.046 [-1.756, -0.277]* / [-1.358, -0.746]* | -0.091 [-0.738, 0.556] / [-0.428, 0.269] | -1.348 [-2.071, -0.621]* / [-1.641, -1.037]* | -1.277 [-1.851, -0.664]* / [-1.546, -1.001]* | -2.409 [-2.786, -2.001]* / [-2.684, -2.124]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -11.021 [-11.999, -10.045]* / [-11.296, -10.735]* | 1.135 [0.317, 1.886] / [0.831, 1.446] | 0.781 [0.407, 1.191] / [0.634, 0.954] | -0.253 [-0.387, -0.137]* / [-0.298, -0.212]* | -0.123 [-0.372, 0.115] / [-0.253, 0.010] | -1.201 [-1.766, -0.690]* / [-1.416, -0.992]* |
| m030 | -8.159 [-9.152, -7.210]* / [-8.447, -7.862]* | 1.139 [0.316, 1.903] / [0.835, 1.445] | 0.720 [0.373, 1.107] / [0.568, 0.886] | -0.276 [-0.383, -0.185]* / [-0.314, -0.239]* | -0.140 [-0.373, 0.098] / [-0.269, -0.011]*  ! | -1.165 [-1.704, -0.677]* / [-1.377, -0.944]* |
| m045 | -6.482 [-7.378, -5.619]* / [-6.752, -6.202]* | 1.133 [0.315, 1.879] / [0.822, 1.448] | 0.724 [0.375, 1.097] / [0.572, 0.895] | -0.266 [-0.372, -0.181]* / [-0.306, -0.231]* | -0.118 [-0.340, 0.114] / [-0.245, 0.010] | -1.124 [-1.620, -0.697]* / [-1.332, -0.911]* |
| m060 | -5.273 [-6.148, -4.443]* / [-5.553, -4.991]* | 1.062 [0.252, 1.809] / [0.754, 1.368] | 0.709 [0.361, 1.083] / [0.561, 0.878] | -0.280 [-0.371, -0.196]* / [-0.319, -0.245]* | -0.157 [-0.381, 0.094] / [-0.281, -0.032]*  ! | -1.148 [-1.638, -0.718]* / [-1.349, -0.943]* |
| m075 | -4.325 [-5.157, -3.520]* / [-4.610, -4.050]* | 1.122 [0.299, 1.917] / [0.814, 1.431] | 0.681 [0.320, 1.069] / [0.525, 0.850] | -0.233 [-0.345, -0.139]* / [-0.274, -0.192]* | -0.116 [-0.384, 0.160] / [-0.243, 0.021] | -1.210 [-1.741, -0.743]* / [-1.429, -0.990]* |
| m090 | -3.352 [-4.188, -2.528]* / [-3.631, -3.077]* | 1.057 [0.298, 1.808] / [0.742, 1.373] | 0.747 [0.336, 1.164] / [0.601, 0.908] | -0.262 [-0.391, -0.157]* / [-0.299, -0.224]* | -0.100 [-0.366, 0.175] / [-0.221, 0.034] | -1.179 [-1.692, -0.719]* / [-1.389, -0.969]* |
| m105 | -2.607 [-3.444, -1.814]* / [-2.905, -2.325]* | 1.070 [0.301, 1.804] / [0.770, 1.379] | 0.793 [0.426, 1.181] / [0.648, 0.960] | -0.245 [-0.321, -0.174]* / [-0.283, -0.210]* | -0.079 [-0.343, 0.188] / [-0.205, 0.059] | -1.184 [-1.704, -0.710]* / [-1.400, -0.970]* |
| m120 | -1.858 [-2.673, -1.058]* / [-2.162, -1.567]* | 1.046 [0.277, 1.756] / [0.746, 1.358] | 0.805 [0.444, 1.197] / [0.662, 0.970] | -0.291 [-0.387, -0.202]* / [-0.334, -0.250]* | -0.190 [-0.452, 0.084] / [-0.315, -0.055]*  ! | -1.278 [-1.777, -0.805]* / [-1.488, -1.061]* |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -11.972 [-13.048, -10.929]* / [-12.297, -11.636]* | 0.175 [-0.535, 0.848] / [-0.192, 0.510] | -0.781 [-1.191, -0.407]* / [-0.954, -0.634]* | -1.047 [-1.428, -0.678]* / [-1.213, -0.898]* | -0.987 [-1.292, -0.682]* / [-1.184, -0.782]* | -2.326 [-2.837, -1.801]* / [-2.577, -2.089]* |
| m030 | -9.055 [-10.150, -7.996]* / [-9.382, -8.727]* | 0.236 [-0.473, 0.906] / [-0.136, 0.586] | -0.720 [-1.107, -0.373]* / [-0.886, -0.568]* | -1.010 [-1.396, -0.652]* / [-1.177, -0.855]* | -0.942 [-1.250, -0.652]* / [-1.146, -0.740]* | -2.235 [-2.731, -1.758]* / [-2.474, -1.993]* |
| m045 | -7.478 [-8.511, -6.487]* / [-7.798, -7.174]* | 0.223 [-0.477, 0.884] / [-0.145, 0.560] | -0.724 [-1.097, -0.375]* / [-0.895, -0.572]* | -1.018 [-1.388, -0.667]* / [-1.184, -0.876]* | -0.938 [-1.247, -0.626]* / [-1.136, -0.749]* | -2.204 [-2.669, -1.764]* / [-2.440, -1.967]* |
| m060 | -6.326 [-7.367, -5.351]* / [-6.645, -6.012]* | 0.168 [-0.551, 0.848] / [-0.196, 0.506] | -0.709 [-1.083, -0.361]* / [-0.878, -0.561]* | -1.014 [-1.360, -0.678]* / [-1.171, -0.873]* | -0.966 [-1.266, -0.672]* / [-1.161, -0.778]* | -2.208 [-2.651, -1.782]* / [-2.441, -1.968]* |
| m075 | -5.531 [-6.529, -4.585]* / [-5.845, -5.217]* | 0.289 [-0.410, 0.954] / [-0.079, 0.633] | -0.681 [-1.069, -0.320]* / [-0.850, -0.525]* | -0.933 [-1.293, -0.553]* / [-1.083, -0.793]* | -0.888 [-1.209, -0.550]* / [-1.085, -0.689]* | -2.213 [-2.664, -1.786]* / [-2.458, -1.983]* |
| m090 | -4.646 [-5.639, -3.726]* / [-4.962, -4.327]* | 0.136 [-0.522, 0.777] / [-0.242, 0.477] | -0.747 [-1.164, -0.336]* / [-0.908, -0.601]* | -1.029 [-1.431, -0.626]* / [-1.186, -0.890]* | -0.942 [-1.302, -0.571]* / [-1.139, -0.753]* | -2.255 [-2.714, -1.809]* / [-2.494, -2.028]* |
| m105 | -3.998 [-4.974, -3.094]* / [-4.335, -3.673]* | 0.132 [-0.520, 0.770] / [-0.232, 0.473] | -0.793 [-1.181, -0.426]* / [-0.960, -0.648]* | -1.052 [-1.418, -0.703]* / [-1.214, -0.909]* | -0.966 [-1.280, -0.659]* / [-1.156, -0.771]* | -2.311 [-2.785, -1.891]* / [-2.558, -2.085]* |
| m120 | -3.284 [-4.216, -2.386]* / [-3.609, -2.964]* | 0.091 [-0.556, 0.738] / [-0.269, 0.428] | -0.805 [-1.197, -0.444]* / [-0.970, -0.662]* | -1.111 [-1.473, -0.775]* / [-1.268, -0.970]* | -1.090 [-1.407, -0.765]* / [-1.278, -0.898]* | -2.421 [-2.886, -1.987]* / [-2.658, -2.194]* |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| m015 | Asia | 10.08 | 9.57 | 10.12 | 9.58 | 5.82 | 5.36 | 5.29 | 2.81 | 10.45 | 9.68 |
| m015 | EU | 6.16 | 6.09 | 6.05 | 5.60 | 4.01 | 3.75 | 3.72 | 2.26 | 6.35 | 6.13 |
| m015 | NA | 8.79 | 7.96 | 8.29 | 8.18 | 5.02 | 4.93 | 4.91 | 2.36 | 9.34 | 8.12 |
| m060 | Asia | 10.12 | 9.67 | 10.21 | 9.71 | 7.26 | 6.56 | 6.47 | 6.83 | 10.54 | 9.78 |
| m060 | EU | 6.39 | 6.24 | 6.20 | 5.77 | 5.10 | 4.55 | 4.53 | 4.54 | 6.47 | 6.33 |
| m060 | NA | 8.93 | 8.11 | 8.42 | 8.33 | 6.79 | 6.29 | 6.29 | 5.95 | 9.49 | 8.29 |
| m120 | Asia | 10.29 | 9.77 | 10.35 | 9.79 | 9.09 | 7.85 | 7.67 | 9.65 | 10.63 | 9.85 |
| m120 | EU | 6.59 | 6.41 | 6.40 | 5.91 | 5.97 | 5.34 | 5.29 | 5.74 | 6.68 | 6.55 |
| m120 | NA | 9.11 | 8.26 | 8.58 | 8.46 | 7.93 | 7.33 | 7.21 | 7.92 | 9.65 | 8.41 |

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | nf_tide | nf_nhits | nf_tide_wx_owx |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | -0.27 | 0.41 | -0.70 | -1.37 | -1.12 | -0.62 | -0.99 | -0.47 | 0.01 | -0.68 |
| m030 | -0.44 | 0.41 | -0.74 | -1.46 | -1.08 | -0.60 | -0.95 | -0.67 | -0.25 | -0.86 |
| m045 | -0.46 | 0.39 | -0.79 | -1.51 | -1.07 | -0.58 | -0.91 | -0.74 | -0.31 | -0.93 |
| m060 | -0.45 | 0.38 | -0.83 | -1.54 | -1.12 | -0.63 | -0.96 | -0.80 | -0.27 | -0.97 |
| m075 | -0.65 | 0.36 | -0.82 | -1.50 | -1.17 | -0.64 | -1.00 | -0.18 | -0.42 | -0.26 |
| m090 | -0.55 | 0.39 | -0.77 | -1.50 | -1.10 | -0.58 | -0.94 | -0.22 | -0.72 | -0.35 |
| m105 | -0.51 | 0.41 | -0.74 | -1.45 | -1.08 | -0.56 | -0.94 | -0.22 | -0.92 | -0.39 |
| m120 | -0.35 | 0.41 | -0.74 | -1.43 | -1.13 | -0.61 | -1.00 | -0.17 | -0.95 | -0.38 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 8.25 [8.08, 8.40] | 7.89 [7.74, 8.04] | 8.13 [7.99, 8.28] | 7.79 [7.65, 7.93] | 8.08 [7.96, 8.22] | 7.05 [6.94, 7.17] | 6.98 [6.87, 7.10] | 8.98 [8.82, 9.13] | 8.58 [8.41, 8.75] | 8.00 [7.85, 8.15] |
| h4-8 | 8.61 [8.43, 8.78] | 8.22 [8.07, 8.38] | 8.49 [8.34, 8.65] | 8.15 [8.00, 8.31] | 8.92 [8.76, 9.07] | 7.89 [7.75, 8.03] | 7.75 [7.62, 7.89] | 12.00 [11.77, 12.22] | 8.97 [8.78, 9.15] | 8.35 [8.19, 8.50] |
| h8+ | 8.45 [8.24, 8.65] | 8.13 [7.94, 8.30] | 8.51 [8.32, 8.69] | 8.40 [8.21, 8.59] | 9.89 [9.67, 10.10] | 8.56 [8.37, 8.74] | 8.44 [8.26, 8.62] | 16.43 [15.97, 16.98] | 9.12 [8.91, 9.34] | 8.27 [8.09, 8.45] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 0.44 [0.05, 0.84] | -0.55 [-0.94, -0.15]* | -0.09 [-0.46, 0.28] | -0.55 [-0.86, -0.22]* | -0.94 [-1.17, -0.73] | -1.83 [-2.23, -1.46]* | -1.84 [-2.23, -1.47]* |  | 0.57 [0.24, 0.92] | -0.40 [-0.80, 0.01] | persistence |
| h4-8 | 0.39 [0.24, 0.54] | -0.12 [-0.15, -0.10]* | 0.15 [0.04, 0.26] | -0.22 [-0.46, -0.01]* | 0.53 [0.25, 0.80] | -0.47 [-0.68, -0.27]* | -0.61 [-0.83, -0.40]* | 3.18 [2.57, 3.89] | 0.76 [0.43, 1.09] |  | wt_med |
| h8+ | 0.33 [0.21, 0.47] | -0.16 [-0.21, -0.12]* | 0.23 [0.12, 0.34] | 0.08 [-0.13, 0.26] | 1.56 [1.21, 1.96] | 0.24 [-0.00, 0.52] | 0.12 [-0.13, 0.40] | 7.81 [6.02, 10.14] | 1.03 [0.73, 1.33] |  | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.567 [0.239, 0.923] / [0.412, 0.725] | -0.399 [-0.795, 0.007] / [-0.536, -0.264]*  ! | 0.439 [0.053, 0.841] / [0.300, 0.579] | -0.547 [-0.940, -0.146]* / [-0.679, -0.416]* | -0.093 [-0.464, 0.278] / [-0.226, 0.044] | -0.545 [-0.856, -0.218]* / [-0.671, -0.428]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.567 [-0.923, -0.239]* / [-0.725, -0.412]* | -0.694 [-1.011, -0.368]* / [-0.822, -0.556]* | -0.229 [-0.492, 0.058] / [-0.373, -0.069]*  ! | -0.794 [-1.106, -0.472]* / [-0.922, -0.656]* | -0.562 [-0.815, -0.301]* / [-0.670, -0.447]* | -0.963 [-1.192, -0.746]* / [-1.072, -0.843]* |
| h4-8 | 1.784 [1.195, 2.442] / [1.564, 1.986] | -0.758 [-1.090, -0.425]* / [-0.898, -0.611]* | -0.292 [-0.569, 0.010] / [-0.445, -0.129]*  ! | -0.869 [-1.194, -0.542]* / [-1.004, -0.723]* | -0.620 [-0.882, -0.357]* / [-0.737, -0.500]* | -1.020 [-1.258, -0.797]* / [-1.137, -0.892]* |
| h8+ | 5.936 [4.350, 8.211] / [5.538, 6.393] | -1.033 [-1.334, -0.727]* / [-1.189, -0.874]* | -0.629 [-0.925, -0.314]* / [-0.811, -0.448]* | -1.170 [-1.482, -0.858]* / [-1.324, -1.013]* | -0.804 [-1.091, -0.526]* / [-0.944, -0.654]* | -1.007 [-1.274, -0.768]* / [-1.154, -0.856]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.399 [-0.007, 0.795] / [0.264, 0.536] | 0.694 [0.368, 1.011] / [0.556, 0.822] | 0.385 [0.233, 0.542] / [0.324, 0.457] | -0.109 [-0.135, -0.086]* / [-0.120, -0.099]* | 0.142 [0.037, 0.249] / [0.088, 0.198] | -0.233 [-0.462, -0.021]* / [-0.326, -0.148]* |
| h4-8 | 3.180 [2.570, 3.888] / [3.007, 3.344] | 0.758 [0.425, 1.090] / [0.611, 0.898] | 0.390 [0.244, 0.537] / [0.324, 0.462] | -0.122 [-0.146, -0.099]* / [-0.133, -0.111]* | 0.146 [0.039, 0.256] / [0.087, 0.207] | -0.221 [-0.461, -0.008]* / [-0.317, -0.127]* |
| h8+ | 7.814 [6.015, 10.136] / [7.440, 8.200] | 1.033 [0.727, 1.334] / [0.874, 1.189] | 0.335 [0.210, 0.473] / [0.267, 0.400] | -0.158 [-0.205, -0.116]* / [-0.179, -0.139]* | 0.228 [0.119, 0.338] / [0.166, 0.295] | 0.077 [-0.129, 0.258] / [-0.022, 0.174] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.439 [-0.841, -0.053]* / [-0.579, -0.300]* | 0.229 [-0.058, 0.492] / [0.069, 0.373] | -0.385 [-0.542, -0.233]* / [-0.457, -0.324]* | -0.495 [-0.651, -0.345]* / [-0.561, -0.437]* | -0.272 [-0.413, -0.139]* / [-0.360, -0.185]* | -0.757 [-0.997, -0.546]* / [-0.862, -0.659]* |
| h4-8 | 1.968 [1.427, 2.519] / [1.793, 2.131] | 0.292 [-0.010, 0.569] / [0.129, 0.445] | -0.390 [-0.537, -0.244]* / [-0.462, -0.324]* | -0.513 [-0.669, -0.367]* / [-0.583, -0.450]* | -0.276 [-0.421, -0.138]* / [-0.369, -0.185]* | -0.766 [-1.016, -0.547]* / [-0.875, -0.661]* |
| h8+ | 5.839 [4.521, 7.739] / [5.517, 6.171] | 0.629 [0.314, 0.925] / [0.448, 0.811] | -0.335 [-0.473, -0.210]* / [-0.400, -0.267]* | -0.496 [-0.639, -0.367]* / [-0.558, -0.431]* | -0.145 [-0.284, -0.018]* / [-0.242, -0.050]* | -0.424 [-0.680, -0.215]* / [-0.540, -0.313]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 16.06 [15.75, 16.35] | 14.93 [14.67, 15.18] | 15.15 [14.89, 15.42] | 14.03 [13.77, 14.29] | 14.70 [14.51, 14.91] | 12.74 [12.57, 12.93] | 12.61 [12.45, 12.78] | 15.72 [15.51, 15.94] | 16.18 [15.86, 16.53] | 15.19 [14.94, 15.45] |
| h4-8 | 16.44 [16.12, 16.74] | 15.25 [14.98, 15.50] | 15.52 [15.25, 15.79] | 14.40 [14.14, 14.67] | 16.02 [15.77, 16.27] | 14.25 [14.01, 14.47] | 13.97 [13.76, 14.19] | 20.23 [19.96, 20.53] | 16.60 [16.25, 16.94] | 15.53 [15.26, 15.79] |
| h8+ | 15.77 [15.42, 16.10] | 14.59 [14.32, 14.87] | 15.07 [14.78, 15.36] | 14.38 [14.09, 14.69] | 17.69 [17.35, 18.03] | 15.16 [14.88, 15.45] | 14.94 [14.66, 15.23] | 26.54 [26.00, 27.17] | 16.50 [16.13, 16.89] | 14.96 [14.68, 15.24] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 1.34 [0.49, 2.24] | -0.57 [-1.35, 0.28] | -0.08 [-0.85, 0.73] | -1.22 [-1.80, -0.62]* | -1.94 [-2.25, -1.59] | -3.52 [-4.08, -2.88]* | -3.54 [-4.10, -2.90]* |  | 1.29 [0.58, 2.03] | -0.28 [-1.08, 0.58] | persistence |
| h4-8 | 0.72 [0.39, 1.07] | -0.27 [-0.32, -0.22]* | -0.11 [-0.35, 0.14] | -1.16 [-1.69, -0.68]* | 0.39 [-0.21, 1.02] | -1.34 [-1.82, -0.90]* | -1.62 [-2.08, -1.16]* | 5.06 [3.99, 6.01] | 1.19 [0.41, 1.91] |  | wt_med |
| h8+ | 0.63 [0.35, 0.94] | -0.36 [-0.46, -0.27]* | -0.01 [-0.24, 0.24] | -0.59 [-0.99, -0.22]* | 2.64 [1.91, 3.36] | 0.13 [-0.37, 0.69] | -0.11 [-0.63, 0.47] | 12.66 [10.28, 14.99] | 1.70 [1.05, 2.28] |  | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.288 [0.584, 2.029] / [0.938, 1.626] | -0.281 [-1.078, 0.579] / [-0.573, 0.022] | 1.339 [0.488, 2.243] / [1.022, 1.690] | -0.574 [-1.348, 0.282] / [-0.853, -0.284]*  ! | -0.081 [-0.854, 0.731] / [-0.380, 0.219] | -1.224 [-1.801, -0.618]* / [-1.498, -0.962]* |
| h4-8 | -2.840 [-3.856, -1.855]* / [-3.278, -2.437]* | -5.059 [-6.007, -3.988]* / [-5.369, -4.722]* | -2.985 [-3.870, -2.018]* / [-3.337, -2.608]* | -5.387 [-6.287, -4.391]* / [-5.690, -5.074]* | -4.581 [-5.453, -3.669]* / [-4.913, -4.248]* | -5.563 [-6.358, -4.714]* / [-5.894, -5.250]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -1.288 [-2.029, -0.584]* / [-1.626, -0.938]* | -1.075 [-1.810, -0.286]* / [-1.374, -0.774]* | -0.178 [-0.830, 0.514] / [-0.504, 0.192] | -1.321 [-2.065, -0.558]* / [-1.616, -1.024]* | -1.207 [-1.764, -0.620]* / [-1.470, -0.944]* | -2.348 [-2.732, -1.936]* / [-2.596, -2.079]* |
| h4-8 | 2.840 [1.855, 3.856] / [2.437, 3.278] | -1.188 [-1.909, -0.406]* / [-1.491, -0.880]* | -0.308 [-0.991, 0.403] / [-0.657, 0.075] | -1.442 [-2.176, -0.687]* / [-1.750, -1.134]* | -1.306 [-1.862, -0.697]* / [-1.582, -1.037]* | -2.421 [-2.803, -2.005]* / [-2.687, -2.133]* |
| h8+ | 10.173 [7.797, 12.842] / [9.525, 10.898] | -1.704 [-2.282, -1.049]* / [-2.030, -1.363]* | -0.958 [-1.581, -0.284]* / [-1.347, -0.552]* | -2.036 [-2.625, -1.364]* / [-2.356, -1.692]* | -1.722 [-2.260, -1.121]* / [-2.033, -1.415]* | -2.405 [-2.845, -1.921]* / [-2.715, -2.083]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.281 [-0.579, 1.078] / [-0.022, 0.573] | 1.075 [0.286, 1.810] / [0.774, 1.374] | 0.730 [0.369, 1.109] / [0.583, 0.897] | -0.257 [-0.311, -0.206]* / [-0.282, -0.235]* | -0.116 [-0.359, 0.124] / [-0.237, 0.017] | -1.201 [-1.718, -0.734]* / [-1.407, -0.988]* |
| h4-8 | 5.059 [3.988, 6.007] / [4.722, 5.369] | 1.188 [0.406, 1.909] / [0.880, 1.491] | 0.723 [0.393, 1.070] / [0.569, 0.891] | -0.270 [-0.322, -0.219]* / [-0.297, -0.245]* | -0.109 [-0.355, 0.140] / [-0.232, 0.025] | -1.160 [-1.695, -0.678]* / [-1.378, -0.938]* |
| h8+ | 12.656 [10.276, 14.989] / [12.094, 13.248] | 1.704 [1.049, 2.282] / [1.363, 2.030] | 0.633 [0.353, 0.936] / [0.480, 0.786] | -0.359 [-0.457, -0.267]* / [-0.408, -0.311]* | -0.005 [-0.239, 0.239] / [-0.134, 0.131] | -0.594 [-0.994, -0.224]* / [-0.821, -0.370]* |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -1.339 [-2.243, -0.488]* / [-1.690, -1.022]* | 0.178 [-0.514, 0.830] / [-0.192, 0.504] | -0.730 [-1.109, -0.369]* / [-0.897, -0.583]* | -1.003 [-1.370, -0.668]* / [-1.160, -0.864]* | -0.939 [-1.251, -0.639]* / [-1.132, -0.743]* | -2.284 [-2.772, -1.827]* / [-2.530, -2.050]* |
| h4-8 | 2.985 [2.018, 3.870] / [2.608, 3.337] | 0.308 [-0.403, 0.991] / [-0.075, 0.657] | -0.723 [-1.070, -0.393]* / [-0.891, -0.569]* | -1.010 [-1.347, -0.680]* / [-1.169, -0.860]* | -0.931 [-1.239, -0.633]* / [-1.127, -0.733]* | -2.269 [-2.786, -1.789]* / [-2.512, -2.030]* |
| h8+ | 9.893 [7.576, 12.292] / [9.337, 10.484] | 0.958 [0.284, 1.581] / [0.552, 1.347] | -0.633 [-0.936, -0.353]* / [-0.786, -0.480]* | -1.021 [-1.316, -0.732]* / [-1.169, -0.867]* | -0.742 [-1.002, -0.476]* / [-0.956, -0.535]* | -1.615 [-2.056, -1.180]* / [-1.875, -1.361]* |

**MAE by region (all rides)**

_no rows_

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | nf_tide | nf_nhits | nf_tide_wx_owx |
|---|---|---|---|---|---|---|---|---|---|---|
| h2-4 | -0.74 | 0.39 | -0.78 | -1.50 | -1.13 | -0.62 | -0.98 | -0.64 | -1.09 | -0.78 |
| h4-8 | -1.56 | 0.36 | -0.99 | -1.78 | -1.33 | -0.75 | -1.11 | -1.27 | -1.56 | -1.02 |
| h8+ | -3.13 | 0.18 | -1.60 | -2.44 | -2.08 | -1.40 | -1.52 | -1.17 | -2.28 | -1.10 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.20 [8.03, 8.36] | 7.83 [7.69, 7.98] | 10.28 [10.07, 10.50] | 8.06 [7.92, 8.21] | 7.74 [7.60, 7.88] | 9.21 [9.02, 9.40] |  | 7.81 [7.67, 7.95] | 7.66 [7.52, 7.79] |  |  | 5.38 [5.29, 5.47] | 6.84 [6.69, 6.99] | 8.53 [8.36, 8.71] | 7.94 [7.80, 8.09] |
| 1 | 8.24 [8.07, 8.40] | 7.92 [7.78, 8.07] | 10.30 [10.09, 10.52] | 8.10 [7.96, 8.25] | 7.86 [7.72, 8.01] | 9.25 [9.07, 9.45] | 9.09 [8.89, 9.30] | 8.18 [8.03, 8.34] | 8.05 [7.90, 8.20] | 8.07 [7.91, 8.23] | 8.30 [8.13, 8.47] | 5.37 [5.28, 5.46] | 6.86 [6.71, 7.02] | 8.52 [8.34, 8.70] | 8.04 [7.89, 8.19] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.39 [0.24, 0.55] | -0.11 [-0.14, -0.09]* | 2.34 [1.92, 2.87] | 0.14 [0.03, 0.24] | -0.23 [-0.46, -0.02]* | 1.24 [0.91, 1.58] |  | -0.15 [-0.35, 0.03] | -0.30 [-0.50, -0.11]* |  |  | -2.58 [-2.95, -2.20]* | -1.10 [-1.34, -0.88]* | 0.70 [0.38, 1.01] | wt_med |
| 1 | 0.33 [0.17, 0.49] | -0.12 [-0.15, -0.09]* | 2.28 [1.85, 2.83] | 0.06 [-0.04, 0.17] | -0.20 [-0.44, 0.01] | 1.19 [0.89, 1.52] | 1.06 [0.74, 1.42] | 0.14 [-0.04, 0.29] | 0.00 [-0.19, 0.19] | 0.04 [-0.17, 0.24] | 0.27 [0.06, 0.46] | -2.69 [-3.07, -2.30]* | -1.18 [-1.43, -0.95]* | 0.59 [0.26, 0.91] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -0.698 [-1.010, -0.376]* / [-0.828, -0.561]* | -0.230 [-0.491, 0.056] / [-0.376, -0.070]*  ! | -0.801 [-1.115, -0.478]* / [-0.930, -0.664]* | -0.573 [-0.834, -0.312]* / [-0.682, -0.460]* | -0.963 [-1.194, -0.746]* / [-1.070, -0.844]* | 1.649 [1.251, 2.065] / [1.467, 1.821] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.698 [0.376, 1.010] / [0.561, 0.828] | 0.389 [0.242, 0.546] / [0.329, 0.460] | -0.112 [-0.144, -0.086]* / [-0.123, -0.102]* | 0.135 [0.035, 0.241] / [0.083, 0.189] | -0.230 [-0.462, -0.023]* / [-0.321, -0.147]* | 2.337 [1.919, 2.869] / [2.209, 2.466] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.230 [-0.056, 0.491] / [0.070, 0.376] | -0.389 [-0.546, -0.242]* / [-0.460, -0.329]* | -0.503 [-0.661, -0.348]* / [-0.570, -0.445]* | -0.283 [-0.424, -0.150]* / [-0.369, -0.197]* | -0.756 [-0.985, -0.545]* / [-0.858, -0.659]* | 1.800 [1.396, 2.275] / [1.669, 1.922] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 15.95 [15.64, 16.24] | 14.81 [14.55, 15.06] | 18.47 [18.06, 18.90] | 15.02 [14.77, 15.29] | 13.93 [13.69, 14.20] | 17.29 [16.96, 17.65] |  | 14.33 [14.09, 14.58] | 14.07 [13.84, 14.30] |  |  | 9.27 [9.15, 9.39] | 12.14 [11.86, 12.44] | 16.09 [15.76, 16.44] | 15.08 [14.83, 15.34] |
| 1 | 16.01 [15.70, 16.31] | 14.98 [14.73, 15.23] | 18.49 [18.07, 18.94] | 15.05 [14.79, 15.34] | 14.15 [13.87, 14.40] | 17.32 [16.98, 17.68] | 16.75 [16.37, 17.16] | 15.16 [14.91, 15.43] | 14.92 [14.67, 15.17] | 14.57 [14.30, 14.86] | 14.94 [14.65, 15.26] | 9.24 [9.12, 9.36] | 12.14 [11.86, 12.45] | 16.05 [15.71, 16.40] | 15.28 [15.02, 15.53] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.74 [0.38, 1.12] | -0.27 [-0.33, -0.21]* | 3.62 [2.48, 4.82] | -0.13 [-0.36, 0.11] | -1.18 [-1.68, -0.73]* | 2.13 [1.41, 2.88] |  | -0.79 [-1.22, -0.40]* | -1.06 [-1.50, -0.62]* |  |  | -5.83 [-6.45, -5.25]* | -3.00 [-3.32, -2.62]* | 1.09 [0.31, 1.83] | wt_med |
| 1 | 0.62 [0.23, 1.03] | -0.28 [-0.35, -0.22]* | 3.50 [2.26, 4.77] | -0.29 [-0.52, -0.05]* | -1.14 [-1.66, -0.67]* | 1.99 [1.32, 2.70] | 1.45 [0.68, 2.28] | -0.14 [-0.56, 0.23] | -0.39 [-0.84, 0.05] | -0.73 [-1.20, -0.27]* | -0.37 [-0.83, 0.10] | -6.05 [-6.72, -5.43]* | -3.19 [-3.51, -2.81]* | 0.86 [0.05, 1.63] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -1.092 [-1.828, -0.306]* / [-1.395, -0.795]* | -0.187 [-0.835, 0.500] / [-0.513, 0.178] | -1.349 [-2.127, -0.584]* / [-1.643, -1.046]* | -1.238 [-1.816, -0.630]* / [-1.505, -0.973]* | -2.344 [-2.730, -1.922]* / [-2.591, -2.073]* | 2.585 [1.632, 3.573] / [2.174, 3.004] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.092 [0.306, 1.828] / [0.795, 1.395] | 0.744 [0.383, 1.123] / [0.600, 0.908] | -0.266 [-0.334, -0.207]* / [-0.292, -0.245]* | -0.129 [-0.359, 0.109] / [-0.246, -0.002]*  ! | -1.183 [-1.681, -0.732]* / [-1.382, -0.977]* | 3.624 [2.484, 4.816] / [3.284, 3.972] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.187 [-0.500, 0.835] / [-0.178, 0.513] | -0.744 [-1.123, -0.383]* / [-0.908, -0.600]* | -1.026 [-1.382, -0.672]* / [-1.180, -0.890]* | -0.962 [-1.269, -0.660]* / [-1.151, -0.774]* | -2.266 [-2.738, -1.831]* / [-2.505, -2.039]* | 2.566 [1.438, 3.771] / [2.227, 2.904] |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 10.13 | 9.63 | 14.38 | 10.20 | 9.69 | 12.41 |  | 9.46 | 9.32 |  |  | 6.32 | 8.86 | 10.50 | 9.74 |
| 0 | EU | 6.37 | 6.25 | 8.07 | 6.24 | 5.79 | 6.80 |  | 6.16 | 6.01 |  |  | 4.27 | 5.08 | 6.51 | 6.34 |
| 0 | NA | 8.88 | 8.09 | 9.88 | 8.44 | 8.33 | 9.24 |  | 8.32 | 8.16 |  |  | 5.88 | 7.23 | 9.46 | 8.24 |
| 1 | Asia | 10.15 | 9.78 | 14.44 | 10.26 | 9.85 | 12.22 | 12.26 | 10.16 | 10.02 | 10.12 | 10.35 | 6.35 | 8.88 | 10.52 | 9.90 |
| 1 | EU | 6.41 | 6.33 | 8.11 | 6.27 | 5.91 | 6.94 | 6.82 | 6.42 | 6.28 | 6.36 | 6.57 | 4.27 | 5.11 | 6.52 | 6.42 |
| 1 | NA | 8.93 | 8.16 | 9.82 | 8.44 | 8.41 | 9.39 | 9.02 | 8.55 | 8.44 | 8.32 | 8.58 | 5.83 | 7.24 | 9.38 | 8.31 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 8.24 [8.07, 8.40] | 7.92 [7.78, 8.07] | 10.30 [10.09, 10.52] | 8.10 [7.96, 8.25] | 7.86 [7.72, 8.01] | 9.25 [9.07, 9.45] | 9.09 [8.89, 9.30] | 8.18 [8.03, 8.34] | 8.05 [7.90, 8.20] | 8.07 [7.91, 8.23] | 8.30 [8.13, 8.47] | 5.37 [5.28, 5.46] | 6.86 [6.71, 7.02] |  |  | 8.52 [8.34, 8.70] | 8.04 [7.89, 8.19] |
| 2 | 8.29 [8.12, 8.45] | 7.98 [7.84, 8.13] | 10.30 [10.09, 10.53] | 8.13 [7.99, 8.28] | 7.94 [7.81, 8.09] | 9.25 [9.08, 9.45] | 9.19 [8.99, 9.40] | 8.41 [8.26, 8.58] | 8.29 [8.15, 8.45] | 8.18 [8.03, 8.35] | 8.44 [8.27, 8.62] | 5.37 [5.28, 5.46] | 6.89 [6.74, 7.05] |  |  | 8.52 [8.33, 8.70] | 8.10 [7.95, 8.26] |
| 3 | 8.27 [8.10, 8.43] | 7.99 [7.84, 8.15] | 10.25 [10.05, 10.48] | 8.13 [7.99, 8.29] | 8.01 [7.87, 8.17] | 9.24 [9.06, 9.43] | 9.25 [9.06, 9.47] | 8.50 [8.34, 8.67] | 8.26 [8.11, 8.42] | 8.21 [8.05, 8.38] | 8.44 [8.27, 8.62] | 5.36 [5.27, 5.45] | 6.89 [6.74, 7.05] | 8.51 [8.37, 8.68] | 8.49 [8.34, 8.65] | 8.49 [8.30, 8.66] | 8.12 [7.97, 8.28] |
| 4 | 8.24 [8.08, 8.42] | 8.01 [7.86, 8.17] | 10.20 [9.98, 10.42] | 8.12 [7.97, 8.28] | 8.08 [7.93, 8.24] | 9.29 [9.10, 9.48] | 9.13 [8.94, 9.35] | 8.47 [8.32, 8.64] | 8.40 [8.25, 8.56] | 8.24 [8.07, 8.41] | 8.41 [8.23, 8.59] | 5.35 [5.26, 5.44] | 6.88 [6.73, 7.05] | 8.60 [8.45, 8.76] | 8.57 [8.42, 8.73] | 8.47 [8.28, 8.65] | 8.14 [7.98, 8.30] |
| 5 | 8.28 [8.11, 8.46] | 8.07 [7.91, 8.23] | 10.27 [10.05, 10.50] | 8.15 [8.01, 8.32] | 8.13 [7.97, 8.30] | 9.33 [9.14, 9.52] | 9.21 [9.01, 9.44] | 8.60 [8.44, 8.78] | 8.42 [8.26, 8.58] | 8.22 [8.06, 8.40] | 8.53 [8.36, 8.72] | 5.36 [5.27, 5.46] | 6.91 [6.75, 7.08] | 8.64 [8.48, 8.82] | 8.62 [8.46, 8.79] | 8.47 [8.28, 8.65] | 8.20 [8.04, 8.37] |
| 6 | 8.32 [8.15, 8.50] | 8.17 [8.01, 8.33] | 10.75 [10.52, 10.99] | 8.20 [8.05, 8.37] | 8.27 [8.10, 8.44] | 9.42 [9.24, 9.61] | 9.27 [9.07, 9.48] | 8.55 [8.39, 8.73] | 8.46 [8.31, 8.63] | 8.30 [8.13, 8.48] | 8.53 [8.35, 8.72] | 5.39 [5.30, 5.49] | 6.93 [6.77, 7.09] | 8.74 [8.57, 8.92] | 8.72 [8.55, 8.89] | 8.44 [8.25, 8.63] | 8.30 [8.14, 8.47] |
| 7 | 8.38 [8.20, 8.56] | 8.32 [8.15, 8.48] | 10.92 [10.67, 11.18] | 8.65 [8.48, 8.83] | 8.51 [8.34, 8.70] | 9.43 [9.24, 9.62] | 9.31 [9.10, 9.52] | 8.95 [8.78, 9.12] | 8.75 [8.59, 8.92] | 8.50 [8.32, 8.68] | 8.72 [8.54, 8.92] | 5.43 [5.33, 5.53] | 7.59 [7.42, 7.76] | 9.01 [8.82, 9.19] | 8.98 [8.80, 9.17] |  | 8.45 [8.28, 8.62] |
| 10 | 8.37 [8.18, 8.56] | 8.41 [8.24, 8.58] | 10.87 [10.61, 11.14] | 8.72 [8.55, 8.90] | 8.61 [8.44, 8.80] |  |  |  |  |  |  | 5.37 [5.27, 5.47] | 7.66 [7.48, 7.84] | 9.20 [9.02, 9.38] | 9.17 [8.99, 9.36] |  | 8.56 [8.39, 8.73] |
| 14 | 8.35 [8.17, 8.55] | 8.59 [8.41, 8.78] | 10.94 [10.65, 11.24] | 9.10 [8.91, 9.30] | 8.94 [8.74, 9.16] |  |  |  |  |  |  | 5.36 [5.26, 5.48] | 8.20 [7.99, 8.40] | 9.60 [9.39, 9.81] | 9.57 [9.36, 9.78] |  | 8.75 [8.56, 8.94] |
| 21 | 8.27 [8.04, 8.49] | 8.72 [8.51, 8.95] | 10.67 [10.34, 11.01] | 9.39 [9.17, 9.62] | 9.07 [8.84, 9.29] |  |  |  |  |  |  | 5.30 [5.18, 5.42] | 8.66 [8.43, 8.90] | 10.00 [9.76, 10.24] | 9.97 [9.73, 10.21] |  | 8.92 [8.70, 9.15] |
| 30 | 8.32 [8.06, 8.57] | 8.94 [8.68, 9.20] | 10.71 [10.31, 11.12] | 9.70 [9.43, 9.96] | 9.47 [9.20, 9.73] |  |  |  |  |  |  | 5.31 [5.17, 5.45] | 9.08 [8.81, 9.36] | 10.60 [10.31, 10.87] | 10.57 [10.28, 10.84] |  | 9.16 [8.90, 9.42] |
| 45 | 8.26 [7.89, 8.62] | 9.33 [8.94, 9.72] | 10.49 [9.84, 11.21] | 10.09 [9.71, 10.48] | 10.01 [9.62, 10.38] |  |  |  |  |  |  | 5.75 [5.53, 5.96] | 9.39 [9.00, 9.83] | 11.00 [10.60, 11.42] | 10.97 [10.57, 11.38] |  | 9.50 [9.11, 9.88] |
| 60 | 7.42 [6.14, 8.71] | 8.12 [6.94, 9.35] |  | 9.10 [7.88, 10.51] | 1.38 [0.18, 11.47] |  |  |  |  |  |  | 4.94 [4.01, 6.03] | 9.19 [7.79, 10.83] | 1.66 [0.00, 16.70] | 1.66 [0.00, 16.70] |  | 8.30 [7.13, 9.54] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.33 [0.17, 0.49] | -0.12 [-0.15, -0.09]* | 2.28 [1.85, 2.83] | 0.06 [-0.04, 0.17] | -0.20 [-0.44, 0.01] | 1.19 [0.89, 1.52] | 1.06 [0.74, 1.42] | 0.14 [-0.04, 0.29] | 0.00 [-0.19, 0.19] | 0.04 [-0.17, 0.24] | 0.27 [0.06, 0.46] | -2.69 [-3.07, -2.30]* | -1.18 [-1.43, -0.95]* |  |  | 0.59 [0.26, 0.91] |  | wt_med |
| 2 | 0.31 [0.13, 0.48] | -0.12 [-0.16, -0.09]* | 2.25 [1.79, 2.80] | 0.03 [-0.08, 0.14] | -0.17 [-0.41, 0.04] | 1.14 [0.85, 1.45] | 1.11 [0.78, 1.47] | 0.32 [0.16, 0.50] | 0.20 [0.03, 0.37] | 0.11 [-0.09, 0.30] | 0.36 [0.16, 0.55] | -2.75 [-3.14, -2.36]* | -1.21 [-1.47, -0.97]* |  |  | 0.52 [0.19, 0.85] |  | wt_med |
| 3 | 0.27 [0.08, 0.46] | -0.13 [-0.16, -0.10]* | 2.18 [1.73, 2.74] | 0.01 [-0.11, 0.12] | -0.12 [-0.34, 0.10] | 1.12 [0.82, 1.45] | 1.16 [0.82, 1.52] | 0.40 [0.24, 0.55] | 0.16 [-0.01, 0.33] | 0.12 [-0.08, 0.32] | 0.35 [0.16, 0.54] | -2.78 [-3.18, -2.38]* | -1.23 [-1.49, -0.99]* | 0.49 [0.21, 0.78] | 0.47 [0.18, 0.75] | 0.48 [0.13, 0.81] |  | wt_med |
| 4 | 0.23 [0.03, 0.43] | -0.13 [-0.17, -0.10]* | 2.13 [1.64, 2.68] | -0.02 [-0.14, 0.10] | -0.08 [-0.32, 0.14] | 1.16 [0.87, 1.46] | 1.03 [0.71, 1.36] | 0.36 [0.20, 0.52] | 0.29 [0.13, 0.45] | 0.13 [-0.06, 0.32] | 0.30 [0.10, 0.49] | -2.81 [-3.21, -2.40]* | -1.26 [-1.53, -1.01]* | 0.57 [0.28, 0.85] | 0.54 [0.25, 0.83] | 0.45 [0.10, 0.79] |  | wt_med |
| 5 | 0.21 [-0.00, 0.43] | -0.13 [-0.17, -0.10]* | 2.09 [1.60, 2.65] | -0.05 [-0.17, 0.07] | -0.09 [-0.33, 0.14] | 1.14 [0.86, 1.43] | 1.05 [0.73, 1.37] | 0.42 [0.27, 0.59] | 0.24 [0.08, 0.40] | 0.06 [-0.13, 0.24] | 0.37 [0.18, 0.55] | -2.86 [-3.27, -2.44]* | -1.29 [-1.56, -1.04]* | 0.56 [0.27, 0.84] | 0.54 [0.25, 0.82] | 0.39 [0.04, 0.74] |  | wt_med |
| 6 | -0.03 [-0.31, 0.25] | -0.39 [-0.76, -0.03]* | 2.15 [1.69, 2.66] | -0.38 [-0.66, -0.11] | -0.33 [-0.55, -0.09] | 0.86 [0.63, 1.10] | 0.74 [0.51, 0.97] | -0.01 [-0.28, 0.24] | -0.09 [-0.37, 0.17] | -0.24 [-0.52, 0.01] | -0.01 [-0.29, 0.24] | -3.14 [-3.63, -2.68]* | -1.73 [-2.16, -1.32]* | 0.35 [0.10, 0.64] | 0.33 [0.07, 0.61] |  | -0.26 [-0.63, 0.11] | snaive7 |
| 7 | 0.08 [-0.16, 0.33] | -0.14 [-0.18, -0.11] | 2.49 [1.98, 3.11] | 0.24 [0.13, 0.34] | 0.04 [-0.19, 0.27] | 0.95 [0.70, 1.23] | 0.88 [0.55, 1.21] | 0.51 [0.36, 0.66] | 0.31 [0.17, 0.45] | 0.08 [-0.11, 0.27] | 0.30 [0.11, 0.48] | -3.04 [-3.47, -2.60]* | -0.83 [-1.09, -0.58]* | 0.70 [0.41, 0.98] | 0.67 [0.39, 0.96] |  |  | wt_med |
| 10 | -0.08 [-0.35, 0.20] | -0.15 [-0.19, -0.12] | 2.34 [1.77, 3.10] | 0.18 [0.06, 0.30] | 0.04 [-0.20, 0.27] |  |  |  |  |  |  | -3.21 [-3.68, -2.74]* | -0.88 [-1.16, -0.61]* | 0.75 [0.46, 1.04] | 0.72 [0.43, 1.01] |  |  | wt_med |
| 14 | -0.29 [-0.62, 0.02] | -0.16 [-0.21, -0.13] | 2.22 [1.57, 2.98] | 0.38 [0.25, 0.49] | 0.18 [-0.11, 0.44] |  |  |  |  |  |  | -3.40 [-3.91, -2.90]* | -0.54 [-0.84, -0.23] | 0.94 [0.59, 1.29] | 0.92 [0.57, 1.27] |  |  | wt_med |
| 21 |  | 0.40 [0.01, 0.80] | 2.43 [1.80, 3.16] | 1.08 [0.66, 1.55] | 0.71 [0.36, 1.06] |  |  |  |  |  |  | -3.03 [-3.52, -2.58]* | 0.34 [-0.27, 1.01] | 1.71 [1.30, 2.16] | 1.68 [1.27, 2.13] |  | 0.59 [0.21, 1.02] | clim |
| 30 |  | 0.57 [0.14, 1.05] | 2.42 [1.73, 3.22] | 1.32 [0.76, 1.91] | 1.07 [0.58, 1.59] |  |  |  |  |  |  | -3.08 [-3.61, -2.57]* | 0.69 [-0.09, 1.53] | 2.24 [1.66, 2.86] | 2.20 [1.63, 2.82] |  | 0.79 [0.33, 1.29] | clim |
| 45 |  | 0.50 [0.11, 0.94] | 0.82 [-0.03, 1.59] | 1.35 [0.87, 1.92] | 1.15 [0.71, 1.63] |  |  |  |  |  |  | -2.89 [-3.40, -2.40]* | 0.72 [-0.06, 1.59] | 2.48 [1.90, 3.13] | 2.44 [1.86, 3.10] |  | 0.69 [0.27, 1.15] | clim |
| 60 |  | 0.65 [-0.02, 1.35] |  | 1.66 [0.73, 2.76] | 0.98 [0.18, 7.47] |  |  |  |  |  |  | -2.56 [-3.44, -1.73]* | 1.76 [0.32, 3.39] | 1.26 [0.00, 12.70] | 1.26 [0.00, 12.70] |  | 0.82 [0.16, 1.56] | clim |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -0.587 [-0.910, -0.256]* / [-0.722, -0.448]* | -0.183 [-0.448, 0.101] / [-0.332, -0.025]*  ! | -0.696 [-1.017, -0.367]* / [-0.830, -0.561]* | -0.531 [-0.797, -0.266]* / [-0.643, -0.413]* | -0.816 [-1.030, -0.591]* / [-0.933, -0.700]* | 1.734 [1.323, 2.158] / [1.556, 1.920] |  |  |
| 2 | -0.523 [-0.848, -0.188]* / [-0.660, -0.380]* | -0.145 [-0.411, 0.139] / [-0.296, 0.021] | -0.637 [-0.962, -0.307]* / [-0.772, -0.495]* | -0.502 [-0.772, -0.237]* / [-0.620, -0.380]* | -0.726 [-0.952, -0.494]* / [-0.844, -0.604]* | 1.697 [1.290, 2.126] / [1.514, 1.892] |  |  |
| 3 | -0.481 [-0.809, -0.134]* / [-0.618, -0.330]* | -0.144 [-0.410, 0.135] / [-0.295, 0.026] | -0.601 [-0.934, -0.260]* / [-0.742, -0.453]* | -0.485 [-0.749, -0.223]* / [-0.606, -0.359]* | -0.615 [-0.829, -0.370]* / [-0.732, -0.488]* | 1.681 [1.272, 2.117] / [1.501, 1.869] | 0.044 [-0.190, 0.305] / [-0.084, 0.177] | 0.015 [-0.224, 0.279] / [-0.112, 0.148] |
| 4 | -0.447 [-0.785, -0.099]* / [-0.584, -0.290]* | -0.146 [-0.416, 0.126] / [-0.306, 0.025] | -0.570 [-0.909, -0.232]* / [-0.710, -0.413]* | -0.479 [-0.752, -0.218]* / [-0.603, -0.348]* | -0.543 [-0.752, -0.328]* / [-0.661, -0.422]* | 1.624 [1.220, 2.078] / [1.443, 1.811] | 0.150 [-0.090, 0.406] / [0.017, 0.278] | 0.121 [-0.125, 0.379] / [-0.011, 0.248] |
| 5 | -0.389 [-0.739, -0.042]* / [-0.531, -0.226]* | -0.107 [-0.372, 0.171] / [-0.267, 0.066] | -0.515 [-0.865, -0.169]* / [-0.657, -0.355]* | -0.450 [-0.725, -0.182]* / [-0.576, -0.313]* | -0.502 [-0.710, -0.270]* / [-0.625, -0.379]* | 1.626 [1.211, 2.079] / [1.446, 1.809] | 0.204 [-0.040, 0.483] / [0.067, 0.337] | 0.176 [-0.072, 0.459] / [0.038, 0.309] |
| 6 | -0.262 [-0.627, 0.114] / [-0.415, -0.095]*  ! | -0.031 [-0.312, 0.249] / [-0.197, 0.140] | -0.390 [-0.758, -0.026]* / [-0.542, -0.230]* | -0.381 [-0.662, -0.108]* / [-0.510, -0.246]* | -0.325 [-0.549, -0.088]* / [-0.453, -0.189]* | 2.152 [1.686, 2.663] / [1.953, 2.342] | 0.353 [0.103, 0.642] / [0.218, 0.498] | 0.326 [0.073, 0.611] / [0.190, 0.470] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.587 [0.256, 0.910] / [0.448, 0.722] | 0.330 [0.165, 0.493] / [0.264, 0.402] | -0.118 [-0.151, -0.091]* / [-0.129, -0.108]* | 0.065 [-0.042, 0.171] / [0.010, 0.122] | -0.204 [-0.443, 0.006] / [-0.297, -0.122]*  ! | 2.282 [1.848, 2.832] / [2.144, 2.425] |  |  |
| 2 | 0.523 [0.188, 0.848] / [0.380, 0.660] | 0.306 [0.128, 0.483] / [0.239, 0.380] | -0.122 [-0.156, -0.093]* / [-0.133, -0.111]* | 0.034 [-0.077, 0.143] / [-0.020, 0.091] | -0.172 [-0.411, 0.039] / [-0.278, -0.081]*  ! | 2.246 [1.791, 2.803] / [2.110, 2.390] |  |  |
| 3 | 0.481 [0.134, 0.809] / [0.330, 0.618] | 0.268 [0.081, 0.456] / [0.202, 0.348] | -0.127 [-0.163, -0.098]* / [-0.139, -0.117]* | 0.010 [-0.106, 0.122] / [-0.049, 0.068] | -0.118 [-0.345, 0.102] / [-0.215, -0.033]*  ! | 2.181 [1.726, 2.741] / [2.044, 2.325] | 0.492 [0.206, 0.777] / [0.378, 0.604] | 0.466 [0.178, 0.753] / [0.352, 0.578] |
| 4 | 0.447 [0.099, 0.785] / [0.290, 0.584] | 0.232 [0.030, 0.433] / [0.160, 0.312] | -0.132 [-0.167, -0.103]* / [-0.143, -0.121]* | -0.018 [-0.139, 0.100] / [-0.079, 0.042] | -0.079 [-0.322, 0.143] / [-0.178, 0.011] | 2.125 [1.643, 2.681] / [1.980, 2.277] | 0.572 [0.282, 0.850] / [0.456, 0.687] | 0.545 [0.249, 0.826] / [0.429, 0.660] |
| 5 | 0.389 [0.042, 0.739] / [0.226, 0.531] | 0.214 [-0.001, 0.426] / [0.139, 0.293] | -0.134 [-0.170, -0.105]* / [-0.145, -0.123]* | -0.046 [-0.171, 0.074] / [-0.107, 0.016] | -0.090 [-0.326, 0.141] / [-0.191, 0.008] | 2.092 [1.597, 2.654] / [1.949, 2.241] | 0.562 [0.273, 0.843] / [0.444, 0.686] | 0.536 [0.249, 0.817] / [0.419, 0.660] |
| 6 | 0.262 [-0.114, 0.627] / [0.095, 0.415] | 0.171 [-0.056, 0.399] / [0.096, 0.253] | -0.136 [-0.172, -0.106]* / [-0.147, -0.124]* | -0.101 [-0.233, 0.028] / [-0.163, -0.036]*  ! | -0.052 [-0.301, 0.193] / [-0.157, 0.045] | 2.459 [1.972, 3.054] / [2.306, 2.619] | 0.573 [0.273, 0.878] / [0.460, 0.691] | 0.548 [0.249, 0.852] / [0.434, 0.667] |
| 7 |  | 0.085 [-0.155, 0.327] / [0.005, 0.165] | -0.139 [-0.178, -0.107]* / [-0.151, -0.127]* | 0.237 [0.130, 0.341] / [0.175, 0.294] | 0.044 [-0.186, 0.269] / [-0.061, 0.140] | 2.492 [1.977, 3.111] / [2.331, 2.654] | 0.696 [0.412, 0.978] / [0.578, 0.815] | 0.671 [0.386, 0.955] / [0.553, 0.790] |
| 10 |  | -0.076 [-0.351, 0.197] / [-0.156, 0.006] | -0.150 [-0.191, -0.116]* / [-0.163, -0.138]* | 0.180 [0.058, 0.297] / [0.111, 0.242] | 0.043 [-0.199, 0.273] / [-0.056, 0.150] | 2.344 [1.769, 3.100] / [2.177, 2.523] | 0.749 [0.456, 1.039] / [0.623, 0.880] | 0.724 [0.430, 1.013] / [0.598, 0.855] |
| 14 |  | -0.290 [-0.623, 0.021] / [-0.373, -0.202]*  ! | -0.163 [-0.206, -0.125]* / [-0.176, -0.150]* | 0.375 [0.253, 0.494] / [0.309, 0.435] | 0.175 [-0.109, 0.442] / [0.064, 0.288] | 2.218 [1.572, 2.984] / [2.022, 2.412] | 0.945 [0.594, 1.294] / [0.809, 1.089] | 0.919 [0.572, 1.269] / [0.783, 1.063] |
| 21 |  | -0.595 [-1.017, -0.209]* / [-0.698, -0.496]* | -0.197 [-0.250, -0.153]* / [-0.212, -0.183]* | 0.476 [0.349, 0.607] / [0.402, 0.544] | 0.144 [-0.089, 0.379] / [0.024, 0.267] | 1.847 [1.165, 2.652] / [1.629, 2.069] | 1.132 [0.844, 1.457] / [0.973, 1.293] | 1.104 [0.820, 1.432] / [0.946, 1.264] |
| 30 |  | -0.787 [-1.295, -0.332]* / [-0.911, -0.673]* | -0.217 [-0.276, -0.167]* / [-0.236, -0.199]* | 0.529 [0.366, 0.706] / [0.440, 0.609] | 0.320 [0.054, 0.585] / [0.156, 0.467] | 1.639 [0.837, 2.527] / [1.379, 1.889] | 1.469 [1.111, 1.864] / [1.261, 1.663] | 1.437 [1.081, 1.837] / [1.231, 1.632] |
| 45 |  | -0.688 [-1.154, -0.271]* / [-0.861, -0.536]* | -0.175 [-0.223, -0.131]* / [-0.198, -0.152]* | 0.655 [0.496, 0.825] / [0.550, 0.763] | 0.501 [0.163, 0.809] / [0.312, 0.702] | 0.988 [-0.397, 1.959] / [0.473, 1.497] | 1.782 [1.281, 2.296] / [1.520, 2.069] | 1.746 [1.252, 2.260] / [1.486, 2.033] |
| 60 |  | -0.822 [-1.556, -0.160]* / [-1.537, -0.139]* | -0.176 [-0.247, -0.120]* / [-0.246, -0.115]* | 0.821 [0.403, 1.280] / [0.448, 1.274] | 0.142 [-0.927, 0.267] / [-0.927, 0.267] |  | 0.428 [0.000, 4.300] / [0.000, 4.300] | 0.428 [0.000, 4.300] / [0.000, 4.300] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.183 [-0.101, 0.448] / [0.025, 0.332] | -0.330 [-0.493, -0.165]* / [-0.402, -0.264]* | -0.449 [-0.613, -0.285]* / [-0.517, -0.388]* | -0.286 [-0.436, -0.143]* / [-0.374, -0.199]* | -0.648 [-0.872, -0.441]* / [-0.754, -0.552]* | 1.848 [1.427, 2.358] / [1.716, 1.981] |  |  |
| 2 | 0.145 [-0.139, 0.411] / [-0.021, 0.296] | -0.306 [-0.483, -0.128]* / [-0.380, -0.239]* | -0.428 [-0.604, -0.257]* / [-0.497, -0.364]* | -0.294 [-0.447, -0.143]* / [-0.385, -0.207]* | -0.582 [-0.796, -0.380]* / [-0.692, -0.481]* | 1.843 [1.415, 2.386] / [1.705, 1.981] |  |  |
| 3 | 0.144 [-0.135, 0.410] / [-0.026, 0.295] | -0.268 [-0.456, -0.081]* / [-0.348, -0.202]* | -0.396 [-0.575, -0.215]* / [-0.470, -0.333]* | -0.282 [-0.440, -0.124]* / [-0.380, -0.192]* | -0.495 [-0.685, -0.307]* / [-0.605, -0.394]* | 1.835 [1.395, 2.376] / [1.694, 1.971] | 0.194 [-0.040, 0.417] / [0.069, 0.312] | 0.168 [-0.070, 0.391] / [0.043, 0.285] |
| 4 | 0.146 [-0.126, 0.416] / [-0.025, 0.306] | -0.232 [-0.433, -0.030]* / [-0.312, -0.160]* | -0.364 [-0.564, -0.166]* / [-0.439, -0.296]* | -0.273 [-0.444, -0.106]* / [-0.370, -0.180]* | -0.420 [-0.618, -0.221]* / [-0.530, -0.323]* | 1.815 [1.372, 2.355] / [1.678, 1.958] | 0.306 [0.064, 0.543] / [0.175, 0.431] | 0.279 [0.033, 0.518] / [0.148, 0.405] |
| 5 | 0.107 [-0.171, 0.372] / [-0.066, 0.267] | -0.214 [-0.426, 0.001] / [-0.293, -0.139]*  ! | -0.348 [-0.555, -0.140]* / [-0.425, -0.276]* | -0.286 [-0.472, -0.101]* / [-0.384, -0.188]* | -0.427 [-0.622, -0.241]* / [-0.540, -0.324]* | 1.796 [1.344, 2.340] / [1.649, 1.945] | 0.311 [0.082, 0.537] / [0.183, 0.439] | 0.285 [0.052, 0.511] / [0.156, 0.412] |
| 6 | 0.031 [-0.249, 0.312] / [-0.140, 0.197] | -0.171 [-0.399, 0.056] / [-0.253, -0.096]*  ! | -0.307 [-0.532, -0.086]* / [-0.384, -0.233]* | -0.296 [-0.497, -0.096]* / [-0.397, -0.194]* | -0.357 [-0.561, -0.172]* / [-0.462, -0.253]* | 2.159 [1.714, 2.694] / [2.001, 2.305] | 0.361 [0.137, 0.575] / [0.233, 0.492] | 0.336 [0.111, 0.551] / [0.208, 0.468] |
| 7 |  | -0.085 [-0.327, 0.155] / [-0.165, -0.005]*  ! | -0.225 [-0.464, 0.007] / [-0.298, -0.148]*  ! | 0.140 [-0.086, 0.378] / [0.039, 0.238] | -0.152 [-0.351, 0.043] / [-0.259, -0.045]*  ! | 2.317 [1.873, 2.877] / [2.166, 2.476] | 0.578 [0.345, 0.805] / [0.442, 0.703] | 0.553 [0.321, 0.782] / [0.416, 0.680] |
| 10 |  | 0.076 [-0.197, 0.351] / [-0.006, 0.156] | -0.074 [-0.336, 0.189] / [-0.151, 0.006] | 0.248 [-0.009, 0.521] / [0.140, 0.354] | 0.017 [-0.177, 0.226] / [-0.097, 0.130] | 2.368 [1.857, 3.003] / [2.204, 2.537] | 0.798 [0.562, 1.044] / [0.661, 0.933] | 0.772 [0.523, 1.020] / [0.636, 0.908] |
| 14 |  | 0.290 [-0.021, 0.623] / [0.202, 0.373] | 0.125 [-0.182, 0.440] / [0.045, 0.208] | 0.667 [0.348, 1.010] / [0.559, 0.771] | 0.381 [0.136, 0.619] / [0.269, 0.496] | 2.470 [1.894, 3.159] / [2.281, 2.665] | 1.210 [0.919, 1.525] / [1.069, 1.350] | 1.184 [0.896, 1.497] / [1.044, 1.325] |
| 21 |  | 0.595 [0.209, 1.017] / [0.496, 0.698] | 0.397 [0.013, 0.799] / [0.302, 0.497] | 1.081 [0.655, 1.547] / [0.954, 1.211] | 0.707 [0.362, 1.057] / [0.576, 0.842] | 2.426 [1.804, 3.159] / [2.219, 2.650] | 1.711 [1.298, 2.156] / [1.542, 1.875] | 1.684 [1.266, 2.133] / [1.513, 1.847] |
| 30 |  | 0.787 [0.332, 1.295] / [0.673, 0.911] | 0.567 [0.139, 1.048] / [0.457, 0.690] | 1.321 [0.764, 1.912] / [1.166, 1.494] | 1.065 [0.580, 1.589] / [0.892, 1.234] | 2.417 [1.731, 3.223] / [2.172, 2.679] | 2.236 [1.656, 2.856] / [2.018, 2.454] | 2.204 [1.629, 2.819] / [1.989, 2.422] |
| 45 |  | 0.688 [0.271, 1.154] / [0.536, 0.861] | 0.505 [0.105, 0.939] / [0.350, 0.674] | 1.347 [0.868, 1.921] / [1.149, 1.569] | 1.153 [0.708, 1.634] / [0.918, 1.378] | 0.820 [-0.027, 1.588] / [0.228, 1.397] | 2.475 [1.897, 3.128] / [2.179, 2.756] | 2.439 [1.859, 3.101] / [2.142, 2.723] |
| 60 |  | 0.822 [0.160, 1.556] / [0.139, 1.537] | 0.650 [-0.023, 1.351] / [-0.037, 1.356] | 1.656 [0.731, 2.759] / [0.688, 2.684] | 0.979 [0.178, 7.473] / [0.178, 7.473] |  | 1.265 [0.000, 12.700] / [0.000, 12.700] | 1.265 [0.000, 12.700] / [0.000, 12.700] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 16.01 [15.70, 16.31] | 14.98 [14.73, 15.23] | 18.49 [18.07, 18.94] | 15.05 [14.79, 15.34] | 14.15 [13.87, 14.40] | 17.32 [16.98, 17.68] | 16.75 [16.37, 17.16] | 15.16 [14.91, 15.43] | 14.92 [14.67, 15.17] | 14.57 [14.30, 14.86] | 14.94 [14.65, 15.26] | 9.24 [9.12, 9.36] | 12.14 [11.86, 12.45] |  |  | 16.05 [15.71, 16.40] | 15.28 [15.02, 15.53] |
| 2 | 16.07 [15.75, 16.38] | 15.09 [14.84, 15.35] | 18.53 [18.10, 19.00] | 15.09 [14.83, 15.38] | 14.32 [14.06, 14.58] | 17.33 [17.01, 17.67] | 16.96 [16.60, 17.37] | 15.74 [15.47, 16.01] | 15.47 [15.22, 15.74] | 14.84 [14.54, 15.13] | 15.29 [14.97, 15.62] | 9.22 [9.10, 9.34] | 12.19 [11.90, 12.49] |  |  | 16.02 [15.68, 16.38] | 15.39 [15.14, 15.65] |
| 3 | 16.02 [15.71, 16.31] | 15.13 [14.87, 15.39] | 18.51 [18.08, 18.95] | 15.08 [14.82, 15.36] | 14.51 [14.25, 14.77] | 17.33 [17.00, 17.68] | 17.09 [16.71, 17.49] | 15.92 [15.65, 16.19] | 15.45 [15.19, 15.70] | 14.92 [14.62, 15.23] | 15.26 [14.95, 15.60] | 9.21 [9.08, 9.33] | 12.17 [11.88, 12.47] | 15.72 [15.44, 16.01] | 15.67 [15.38, 15.95] | 15.95 [15.61, 16.30] | 15.45 [15.19, 15.71] |
| 4 | 16.01 [15.70, 16.31] | 15.21 [14.95, 15.48] | 18.39 [17.94, 18.82] | 15.08 [14.81, 15.36] | 14.67 [14.41, 14.93] | 17.48 [17.15, 17.81] | 16.92 [16.54, 17.30] | 15.93 [15.66, 16.21] | 15.78 [15.51, 16.06] | 15.02 [14.72, 15.34] | 15.31 [14.99, 15.66] | 9.21 [9.09, 9.34] | 12.17 [11.88, 12.48] | 15.89 [15.61, 16.16] | 15.83 [15.55, 16.11] | 15.94 [15.60, 16.29] | 15.53 [15.27, 15.80] |
| 5 | 16.07 [15.76, 16.37] | 15.32 [15.06, 15.60] | 18.54 [18.10, 18.99] | 15.12 [14.85, 15.41] | 14.80 [14.52, 15.08] | 17.54 [17.19, 17.89] | 17.09 [16.71, 17.50] | 16.18 [15.90, 16.46] | 15.80 [15.53, 16.07] | 14.96 [14.66, 15.28] | 15.51 [15.19, 15.86] | 9.21 [9.09, 9.35] | 12.21 [11.91, 12.51] | 16.08 [15.79, 16.36] | 16.02 [15.73, 16.30] | 15.93 [15.58, 16.30] | 15.65 [15.38, 15.93] |
| 6 | 16.14 [15.82, 16.45] | 15.50 [15.23, 15.79] | 19.38 [18.90, 19.88] | 15.19 [14.91, 15.48] | 15.03 [14.71, 15.31] | 17.67 [17.33, 18.00] | 17.19 [16.79, 17.59] | 16.07 [15.78, 16.36] | 15.90 [15.62, 16.17] | 15.12 [14.82, 15.44] | 15.52 [15.19, 15.88] | 9.25 [9.12, 9.39] | 12.22 [11.92, 12.53] | 16.20 [15.91, 16.49] | 16.14 [15.85, 16.43] | 15.91 [15.56, 16.28] | 15.84 [15.56, 16.12] |
| 7 | 16.22 [15.91, 16.53] | 15.78 [15.51, 16.07] | 19.66 [19.19, 20.16] | 16.22 [15.92, 16.55] | 15.48 [15.17, 15.77] | 17.68 [17.32, 18.04] | 17.21 [16.82, 17.60] | 16.84 [16.54, 17.12] | 16.45 [16.17, 16.72] | 15.44 [15.13, 15.78] | 15.80 [15.46, 16.15] | 9.30 [9.18, 9.45] | 13.46 [13.12, 13.79] | 16.73 [16.41, 17.03] | 16.67 [16.35, 16.97] |  | 16.12 [15.84, 16.42] |
| 10 | 16.20 [15.89, 16.53] | 16.05 [15.76, 16.37] | 19.76 [19.23, 20.31] | 16.38 [16.06, 16.71] | 15.76 [15.45, 16.06] |  |  |  |  |  |  | 9.24 [9.11, 9.39] | 13.61 [13.26, 13.96] | 17.24 [16.91, 17.55] | 17.18 [16.85, 17.48] |  | 16.42 [16.13, 16.73] |
| 14 | 16.14 [15.82, 16.50] | 16.47 [16.17, 16.81] | 19.96 [19.38, 20.59] | 17.22 [16.88, 17.55] | 16.47 [16.12, 16.82] |  |  |  |  |  |  | 9.25 [9.10, 9.41] | 14.54 [14.16, 14.90] | 18.01 [17.64, 18.38] | 17.95 [17.58, 18.32] |  | 16.86 [16.55, 17.21] |
| 21 | 15.99 [15.58, 16.39] | 16.85 [16.49, 17.22] | 19.55 [18.88, 20.30] | 17.79 [17.40, 18.19] | 16.75 [16.38, 17.10] |  |  |  |  |  |  | 9.15 [8.97, 9.33] | 15.25 [14.83, 15.68] | 18.71 [18.29, 19.08] | 18.64 [18.22, 19.02] |  | 17.31 [16.94, 17.69] |
| 30 | 16.00 [15.55, 16.46] | 17.17 [16.73, 17.61] | 19.55 [18.78, 20.42] | 18.25 [17.77, 18.71] | 17.47 [16.98, 17.94] |  |  |  |  |  |  | 9.09 [8.89, 9.31] | 15.87 [15.35, 16.37] | 19.70 [19.19, 20.17] | 19.63 [19.11, 20.09] |  | 17.67 [17.22, 18.11] |
| 45 | 15.94 [15.25, 16.64] | 17.30 [16.64, 18.01] | 16.34 [15.28, 17.48] | 18.45 [17.78, 19.17] | 17.92 [17.25, 18.60] |  |  |  |  |  |  | 9.62 [9.33, 9.91] | 15.94 [15.24, 16.71] | 20.32 [19.60, 21.06] | 20.24 [19.52, 20.98] |  | 17.70 [17.03, 18.40] |
| 60 | 15.39 [13.24, 17.44] | 17.13 [15.30, 18.83] |  | 18.71 [16.57, 20.79] | 11.47 [11.47, 11.47] |  |  |  |  |  |  | 8.46 [7.26, 9.64] | 17.20 [14.24, 20.30] | 16.70 [16.70, 16.70] | 16.70 [16.70, 16.70] |  | 17.52 [15.67, 19.27] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.62 [0.23, 1.03] | -0.28 [-0.35, -0.22]* | 3.50 [2.26, 4.77] | -0.29 [-0.52, -0.05]* | -1.14 [-1.66, -0.67]* | 1.99 [1.32, 2.70] | 1.45 [0.68, 2.28] | -0.14 [-0.56, 0.23] | -0.39 [-0.84, 0.05] | -0.73 [-1.20, -0.27]* | -0.37 [-0.83, 0.10] | -6.05 [-6.72, -5.43]* | -3.19 [-3.51, -2.81]* |  |  | 0.86 [0.05, 1.63] |  | wt_med |
| 2 | -0.02 [-0.67, 0.67] | -1.00 [-1.81, -0.21]* | 2.66 [1.65, 3.69] | -1.10 [-1.71, -0.46]* | -1.84 [-2.29, -1.35]* | 1.09 [0.58, 1.61] | 0.75 [0.26, 1.26] | -0.40 [-0.94, 0.13] | -0.68 [-1.23, -0.16] | -1.34 [-1.85, -0.85]* | -0.89 [-1.36, -0.38] | -6.89 [-7.69, -6.14]* | -4.11 [-4.91, -3.29]* |  |  |  | -0.72 [-1.51, 0.10] | snaive7 |
| 3 | -0.02 [-0.69, 0.67] | -0.91 [-1.73, -0.09]* | 2.67 [1.64, 3.71] | -1.06 [-1.67, -0.42]* | -1.57 [-2.03, -1.03]* | 1.23 [0.70, 1.73] | 1.00 [0.50, 1.49] | -0.16 [-0.74, 0.41] | -0.64 [-1.21, -0.10] | -1.22 [-1.73, -0.72]* | -0.87 [-1.37, -0.36] | -6.83 [-7.62, -6.10]* | -4.08 [-4.88, -3.26]* | -0.33 [-0.89, 0.34] | -0.40 [-0.96, 0.28] |  | -0.62 [-1.43, 0.22] | snaive7 |
| 4 | -0.01 [-0.68, 0.66] | -0.82 [-1.67, 0.01] | 2.55 [1.51, 3.60] | -1.04 [-1.68, -0.42]* | -1.42 [-1.85, -0.92]* | 1.43 [0.92, 1.96] | 0.87 [0.41, 1.32] | -0.15 [-0.74, 0.44] | -0.29 [-0.91, 0.31] | -1.09 [-1.61, -0.56]* | -0.80 [-1.28, -0.30] | -6.82 [-7.63, -6.07]* | -4.06 [-4.88, -3.23]* | -0.16 [-0.73, 0.47] | -0.22 [-0.80, 0.41] |  | -0.52 [-1.34, 0.32] | snaive7 |
| 5 | 0.08 [-0.60, 0.78] | -0.70 [-1.57, 0.16] | 2.56 [1.50, 3.64] | -0.98 [-1.63, -0.34]* | -1.28 [-1.75, -0.74]* | 1.48 [0.94, 2.05] | 1.05 [0.54, 1.55] | 0.12 [-0.52, 0.78] | -0.27 [-0.89, 0.35] | -1.15 [-1.71, -0.61]* | -0.58 [-1.13, -0.01] | -6.80 [-7.62, -6.04]* | -4.01 [-4.86, -3.16]* | 0.05 [-0.52, 0.76] | -0.02 [-0.61, 0.70] |  | -0.40 [-1.25, 0.47] | snaive7 |
| 6 | 0.18 [-0.54, 0.89] | -0.50 [-1.38, 0.40] | 3.47 [2.37, 4.61] | -0.89 [-1.57, -0.22]* | -1.02 [-1.47, -0.48]* | 1.59 [1.03, 2.20] | 1.13 [0.62, 1.61] | 0.01 [-0.65, 0.66] | -0.14 [-0.83, 0.52] | -0.97 [-1.56, -0.39]* | -0.57 [-1.13, -0.02] | -6.74 [-7.58, -5.96]* | -3.97 [-4.84, -3.09]* | 0.24 [-0.35, 0.99] | 0.18 [-0.43, 0.93] |  | -0.19 [-1.09, 0.72] | snaive7 |
| 7 | 0.07 [-0.51, 0.64] | -0.33 [-0.40, -0.25] | 3.84 [2.42, 5.33] | 0.08 [-0.17, 0.34] | -0.65 [-1.15, -0.12]* | 1.50 [0.94, 2.13] | 1.08 [0.32, 1.90] | 0.71 [0.40, 1.04] | 0.32 [0.01, 0.67] | -0.68 [-1.11, -0.25]* | -0.33 [-0.76, 0.12] | -6.83 [-7.56, -6.13]* | -2.68 [-3.04, -2.24]* | 0.60 [-0.00, 1.25] | 0.54 [-0.06, 1.19] |  |  | wt_med |
| 10 | -0.27 [-0.95, 0.36] | -0.35 [-0.43, -0.27] | 3.58 [1.91, 5.38] | -0.06 [-0.35, 0.24] | -0.67 [-1.20, -0.15]* |  |  |  |  |  |  | -7.19 [-7.96, -6.43]* | -2.82 [-3.22, -2.35]* | 0.75 [0.10, 1.36] | 0.69 [0.05, 1.29] |  |  | wt_med |
| 14 | -0.71 [-1.54, 0.04] | -0.37 [-0.46, -0.28] | 3.30 [1.43, 5.20] | 0.35 [0.07, 0.65] | -0.43 [-1.05, 0.19] |  |  |  |  |  |  | -7.63 [-8.45, -6.80]* | -2.32 [-2.78, -1.81]* | 1.10 [0.35, 1.83] | 1.03 [0.30, 1.76] |  |  | wt_med |
| 21 |  | 0.86 [-0.08, 1.83] | 3.78 [2.14, 5.60] | 1.82 [0.77, 2.97] | 0.66 [-0.17, 1.50] |  |  |  |  |  |  | -6.89 [-7.65, -6.15]* | -0.74 [-1.98, 0.64] | 2.64 [1.70, 3.66] | 2.57 [1.61, 3.60] |  | 1.30 [0.30, 2.31] | clim |
| 30 |  | 1.20 [0.10, 2.37] | 3.75 [1.94, 5.73] | 2.27 [0.97, 3.70] | 1.41 [0.28, 2.66] |  |  |  |  |  |  | -6.95 [-7.84, -6.08]* | -0.15 [-1.62, 1.51] | 3.61 [2.32, 5.10] | 3.54 [2.24, 5.03] |  | 1.68 [0.55, 2.89] | clim |
| 45 |  | 0.98 [-0.02, 2.10] | 0.52 [-1.26, 2.21] | 2.16 [0.90, 3.59] | 1.52 [0.47, 2.78] |  |  |  |  |  |  | -6.72 [-7.73, -5.80]* | -0.30 [-1.75, 1.45] | 4.17 [2.80, 5.83] | 4.08 [2.71, 5.75] |  | 1.40 [0.36, 2.57] | clim |
| 60 |  | 1.88 [0.00, 3.70] |  | 3.27 [0.76, 5.93] | 7.47 [7.47, 7.47] |  |  |  |  |  |  | -7.02 [-9.04, -5.07]* | 1.80 [-1.44, 5.79] | 12.70 [12.70, 12.70] | 12.70 [12.70, 12.70] |  | 2.25 [0.35, 4.13] | clim |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -0.861 [-1.626, -0.052]* / [-1.181, -0.558]* | -0.087 [-0.742, 0.602] / [-0.434, 0.282] | -1.131 [-1.914, -0.349]* / [-1.442, -0.832]* | -1.161 [-1.770, -0.533]* / [-1.429, -0.892]* | -2.055 [-2.463, -1.613]* / [-2.348, -1.778]* | 2.725 [1.736, 3.778] / [2.295, 3.160] |  |  |
| 2 | -0.722 [-1.510, 0.096] / [-1.045, -0.408]*  ! | -0.020 [-0.671, 0.671] / [-0.367, 0.358] | -1.002 [-1.809, -0.209]* / [-1.313, -0.686]* | -1.095 [-1.706, -0.460]* / [-1.369, -0.810]* | -1.845 [-2.289, -1.349]* / [-2.142, -1.550]* | 2.656 [1.652, 3.690] / [2.226, 3.095] |  |  |
| 3 | -0.617 [-1.430, 0.224] / [-0.938, -0.276]*  ! | -0.024 [-0.689, 0.667] / [-0.365, 0.369] | -0.910 [-1.728, -0.090]* / [-1.231, -0.573]* | -1.055 [-1.669, -0.418]* / [-1.339, -0.765]* | -1.570 [-2.026, -1.030]* / [-1.852, -1.302]* | 2.666 [1.639, 3.710] / [2.228, 3.119] | -0.331 [-0.894, 0.341] / [-0.631, -0.031]*  ! | -0.396 [-0.965, 0.283] / [-0.695, -0.094]*  ! |
| 4 | -0.520 [-1.342, 0.322] / [-0.845, -0.179]*  ! | -0.010 [-0.677, 0.663] / [-0.375, 0.382] | -0.821 [-1.668, 0.006] / [-1.134, -0.480]*  ! | -1.037 [-1.678, -0.422]* / [-1.327, -0.742]* | -1.416 [-1.854, -0.918]* / [-1.682, -1.147]* | 2.547 [1.513, 3.597] / [2.108, 3.006] | -0.156 [-0.731, 0.475] / [-0.460, 0.141] | -0.220 [-0.799, 0.413] / [-0.523, 0.075] |
| 5 | -0.396 [-1.248, 0.470] / [-0.737, -0.042]*  ! | 0.082 [-0.598, 0.777] / [-0.293, 0.477] | -0.700 [-1.571, 0.156] / [-1.030, -0.352]*  ! | -0.981 [-1.635, -0.344]* / [-1.273, -0.678]* | -1.285 [-1.748, -0.740]* / [-1.571, -0.997]* | 2.565 [1.497, 3.635] / [2.120, 3.040] | 0.047 [-0.524, 0.763] / [-0.272, 0.362] | -0.018 [-0.607, 0.701] / [-0.337, 0.296] |
| 6 | -0.186 [-1.086, 0.721] / [-0.540, 0.180] | 0.178 [-0.541, 0.886] / [-0.214, 0.591] | -0.495 [-1.382, 0.403] / [-0.853, -0.138]*  ! | -0.886 [-1.565, -0.224]* / [-1.203, -0.581]* | -1.015 [-1.470, -0.477]* / [-1.314, -0.702]* | 3.470 [2.366, 4.611] / [3.008, 3.924] | 0.241 [-0.348, 0.985] / [-0.073, 0.573] | 0.178 [-0.432, 0.926] / [-0.137, 0.514] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.861 [0.052, 1.626] / [0.558, 1.181] | 0.620 [0.230, 1.031] / [0.470, 0.788] | -0.281 [-0.351, -0.220]* / [-0.306, -0.257]* | -0.286 [-0.517, -0.048]* / [-0.408, -0.155]* | -1.142 [-1.658, -0.675]* / [-1.363, -0.942]* | 3.503 [2.259, 4.769] / [3.165, 3.869] |  |  |
| 2 | 0.722 [-0.096, 1.510] / [0.408, 1.045] | 0.552 [0.145, 0.985] / [0.403, 0.724] | -0.288 [-0.363, -0.225]* / [-0.315, -0.264]* | -0.354 [-0.600, -0.097]* / [-0.480, -0.223]* | -1.068 [-1.581, -0.605]* / [-1.294, -0.869]* | 3.444 [2.144, 4.746] / [3.090, 3.804] |  |  |
| 3 | 0.617 [-0.224, 1.430] / [0.276, 0.938] | 0.453 [0.020, 0.901] / [0.295, 0.638] | -0.301 [-0.377, -0.233]* / [-0.327, -0.276]* | -0.414 [-0.666, -0.148]* / [-0.546, -0.277]* | -0.934 [-1.456, -0.435]* / [-1.160, -0.740]* | 3.345 [2.032, 4.657] / [2.994, 3.719] | 0.225 [-0.432, 0.824] / [-0.035, 0.475] | 0.166 [-0.490, 0.765] / [-0.092, 0.415] |
| 4 | 0.520 [-0.322, 1.342] / [0.179, 0.845] | 0.369 [-0.103, 0.851] / [0.208, 0.550] | -0.308 [-0.384, -0.241]* / [-0.335, -0.284]* | -0.488 [-0.757, -0.216]* / [-0.624, -0.346]* | -0.874 [-1.421, -0.362]* / [-1.110, -0.663]* | 3.206 [1.841, 4.580] / [2.833, 3.591] | 0.320 [-0.320, 0.935] / [0.046, 0.576] | 0.261 [-0.373, 0.876] / [-0.012, 0.512] |
| 5 | 0.396 [-0.470, 1.248] / [0.042, 0.737] | 0.329 [-0.176, 0.824] / [0.154, 0.507] | -0.314 [-0.389, -0.247]* / [-0.341, -0.289]* | -0.556 [-0.839, -0.270]* / [-0.697, -0.413]* | -0.855 [-1.391, -0.331]* / [-1.076, -0.634]* | 3.158 [1.779, 4.546] / [2.772, 3.562] | 0.391 [-0.245, 1.022] / [0.133, 0.662] | 0.331 [-0.309, 0.961] / [0.072, 0.606] |
| 6 | 0.186 [-0.721, 1.086] / [-0.180, 0.540] | 0.239 [-0.302, 0.775] / [0.065, 0.421] | -0.318 [-0.391, -0.253]* / [-0.345, -0.293]* | -0.662 [-0.971, -0.352]* / [-0.813, -0.505]* | -0.810 [-1.395, -0.257]* / [-1.061, -0.581]* | 3.804 [2.449, 5.285] / [3.420, 4.212] | 0.352 [-0.315, 0.979] / [0.080, 0.607] | 0.294 [-0.378, 0.924] / [0.020, 0.547] |
| 7 |  | 0.072 [-0.512, 0.636] / [-0.103, 0.267] | -0.326 [-0.403, -0.252]* / [-0.353, -0.299]* | 0.079 [-0.170, 0.338] / [-0.054, 0.219] | -0.652 [-1.154, -0.119]* / [-0.879, -0.420]* | 3.839 [2.422, 5.332] / [3.432, 4.273] | 0.600 [-0.000, 1.246] / [0.332, 0.876] | 0.542 [-0.058, 1.189] / [0.273, 0.817] |
| 10 |  | -0.270 [-0.949, 0.365] / [-0.457, -0.068]*  ! | -0.349 [-0.430, -0.271]* / [-0.376, -0.321]* | -0.063 [-0.345, 0.239] / [-0.220, 0.093] | -0.668 [-1.196, -0.154]* / [-0.893, -0.435]* | 3.583 [1.909, 5.378] / [3.145, 4.048] | 0.751 [0.096, 1.360] / [0.471, 1.034] | 0.688 [0.046, 1.294] / [0.411, 0.971] |
| 14 |  | -0.713 [-1.539, 0.045] / [-0.903, -0.503]*  ! | -0.371 [-0.464, -0.284]* / [-0.399, -0.340]* | 0.347 [0.072, 0.649] / [0.193, 0.491] | -0.427 [-1.053, 0.190] / [-0.681, -0.176]*  ! | 3.302 [1.431, 5.199] / [2.790, 3.811] | 1.095 [0.355, 1.834] / [0.807, 1.408] | 1.033 [0.302, 1.764] / [0.746, 1.346] |
| 21 |  | -1.302 [-2.305, -0.301]* / [-1.528, -1.061]* | -0.437 [-0.539, -0.336]* / [-0.470, -0.405]* | 0.497 [0.175, 0.829] / [0.329, 0.652] | -0.581 [-1.070, -0.072]* / [-0.860, -0.303]* | 2.518 [0.451, 4.541] / [1.911, 3.124] | 1.379 [0.797, 2.014] / [1.003, 1.708] | 1.313 [0.734, 1.951] / [0.936, 1.641] |
| 30 |  | -1.679 [-2.887, -0.547]* / [-1.971, -1.414]* | -0.466 [-0.570, -0.363]* / [-0.506, -0.427]* | 0.595 [0.180, 1.029] / [0.395, 0.781] | -0.166 [-0.775, 0.474] / [-0.525, 0.174] | 2.051 [-0.320, 4.345] / [1.389, 2.737] | 1.992 [1.244, 2.753] / [1.556, 2.407] | 1.918 [1.166, 2.669] / [1.483, 2.326] |
| 45 |  | -1.400 [-2.565, -0.359]* / [-1.811, -1.023]* | -0.369 [-0.478, -0.279]* / [-0.422, -0.319]* | 0.742 [0.412, 1.110] / [0.523, 0.959] | 0.251 [-0.514, 0.993] / [-0.173, 0.702] | 0.473 [-2.271, 2.655] / [-0.591, 1.554] | 2.700 [1.647, 3.736] / [2.115, 3.282] | 2.614 [1.555, 3.658] / [2.031, 3.199] |
| 60 |  | -2.247 [-4.128, -0.347]* / [-4.010, -0.370]* | -0.375 [-0.543, -0.223]* / [-0.552, -0.206]* | 0.934 [0.029, 2.012] / [0.057, 2.070] | -0.927 [-0.927, -0.927] / [-0.927, -0.927] |  | 4.300 [4.300, 4.300] / [4.300, 4.300] | 4.300 [4.300, 4.300] / [4.300, 4.300] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.087 [-0.602, 0.742] / [-0.282, 0.434] | -0.620 [-1.031, -0.230]* / [-0.788, -0.470]* | -0.915 [-1.295, -0.535]* / [-1.068, -0.774]* | -0.975 [-1.310, -0.655]* / [-1.169, -0.785]* | -2.032 [-2.442, -1.624]* / [-2.277, -1.810]* | 2.668 [1.499, 3.947] / [2.337, 3.015] |  |  |
| 2 | 0.020 [-0.671, 0.671] / [-0.358, 0.367] | -0.552 [-0.985, -0.145]* / [-0.724, -0.403]* | -0.854 [-1.247, -0.456]* / [-1.020, -0.707]* | -0.977 [-1.309, -0.644]* / [-1.184, -0.776]* | -1.865 [-2.278, -1.460]* / [-2.120, -1.632]* | 2.691 [1.486, 3.977] / [2.327, 3.044] |  |  |
| 3 | 0.024 [-0.667, 0.689] / [-0.369, 0.365] | -0.453 [-0.901, -0.020]* / [-0.638, -0.295]* | -0.764 [-1.191, -0.342]* / [-0.941, -0.612]* | -0.944 [-1.287, -0.594]* / [-1.162, -0.733]* | -1.643 [-2.011, -1.282]* / [-1.903, -1.422]* | 2.735 [1.506, 4.061] / [2.362, 3.099] | -0.301 [-0.801, 0.186] / [-0.594, -0.051]*  ! | -0.360 [-0.856, 0.123] / [-0.654, -0.112]*  ! |
| 4 | 0.010 [-0.663, 0.677] / [-0.382, 0.375] | -0.369 [-0.851, 0.103] / [-0.550, -0.208]*  ! | -0.688 [-1.148, -0.229]* / [-0.856, -0.528]* | -0.936 [-1.299, -0.556]* / [-1.151, -0.720]* | -1.496 [-1.918, -1.071]* / [-1.752, -1.268]* | 2.670 [1.421, 4.017] / [2.295, 3.057] | -0.128 [-0.650, 0.382] / [-0.426, 0.153] | -0.188 [-0.720, 0.326] / [-0.484, 0.090] |
| 5 | -0.082 [-0.777, 0.598] / [-0.477, 0.293] | -0.329 [-0.824, 0.176] / [-0.507, -0.154]*  ! | -0.656 [-1.135, -0.156]* / [-0.826, -0.485]* | -0.975 [-1.376, -0.553]* / [-1.196, -0.743]* | -1.487 [-1.896, -1.072]* / [-1.761, -1.241]* | 2.648 [1.385, 3.970] / [2.257, 3.063] | -0.037 [-0.564, 0.465] / [-0.324, 0.267] | -0.097 [-0.632, 0.407] / [-0.388, 0.207] |
| 6 | -0.178 [-0.886, 0.541] / [-0.591, 0.214] | -0.239 [-0.775, 0.302] / [-0.421, -0.065]*  ! | -0.570 [-1.085, -0.046]* / [-0.744, -0.403]* | -0.987 [-1.441, -0.515]* / [-1.226, -0.743]* | -1.391 [-1.784, -0.991]* / [-1.642, -1.155]* | 3.266 [2.030, 4.592] / [2.864, 3.689] | 0.009 [-0.483, 0.477] / [-0.297, 0.302] | -0.050 [-0.546, 0.421] / [-0.355, 0.241] |
| 7 |  | -0.072 [-0.636, 0.512] / [-0.267, 0.103] | -0.410 [-0.959, 0.160] / [-0.593, -0.234]*  ! | -0.030 [-0.579, 0.558] / [-0.269, 0.212] | -0.996 [-1.417, -0.579]* / [-1.246, -0.749]* | 3.576 [2.355, 4.906] / [3.187, 4.014] | 0.450 [-0.064, 0.967] / [0.145, 0.727] | 0.391 [-0.131, 0.910] / [0.091, 0.665] |
| 10 |  | 0.270 [-0.365, 0.949] / [0.068, 0.457] | -0.084 [-0.706, 0.571] / [-0.275, 0.096] | 0.180 [-0.462, 0.861] / [-0.074, 0.439] | -0.629 [-1.116, -0.193]* / [-0.877, -0.371]* | 3.737 [2.254, 5.414] / [3.312, 4.175] | 0.964 [0.421, 1.508] / [0.667, 1.278] | 0.901 [0.347, 1.440] / [0.604, 1.217] |
| 14 |  | 0.713 [-0.045, 1.539] / [0.503, 0.903] | 0.332 [-0.378, 1.108] / [0.142, 0.518] | 1.061 [0.259, 1.931] / [0.804, 1.315] | 0.097 [-0.510, 0.658] / [-0.165, 0.376] | 3.936 [2.273, 5.687] / [3.448, 4.459] | 1.759 [1.063, 2.453] / [1.455, 2.073] | 1.697 [1.008, 2.401] / [1.394, 2.012] |
| 21 |  | 1.302 [0.301, 2.305] / [1.061, 1.528] | 0.861 [-0.078, 1.831] / [0.633, 1.077] | 1.820 [0.771, 2.975] / [1.507, 2.120] | 0.663 [-0.172, 1.496] / [0.358, 0.986] | 3.785 [2.144, 5.596] / [3.253, 4.405] | 2.639 [1.698, 3.655] / [2.260, 3.010] | 2.573 [1.613, 3.603] / [2.190, 2.944] |
| 30 |  | 1.679 [0.547, 2.887] / [1.414, 1.971] | 1.203 [0.101, 2.365] / [0.938, 1.478] | 2.272 [0.969, 3.701] / [1.875, 2.662] | 1.413 [0.277, 2.662] / [1.002, 1.818] | 3.746 [1.944, 5.735] / [3.143, 4.419] | 3.614 [2.321, 5.098] / [3.137, 4.117] | 3.539 [2.244, 5.027] / [3.059, 4.047] |
| 45 |  | 1.400 [0.359, 2.565] / [1.023, 1.811] | 0.985 [-0.020, 2.103] / [0.627, 1.397] | 2.158 [0.900, 3.593] / [1.689, 2.692] | 1.524 [0.467, 2.777] / [0.985, 2.094] | 0.516 [-1.256, 2.213] / [-0.464, 1.556] | 4.167 [2.801, 5.832] / [3.522, 4.854] | 4.080 [2.714, 5.755] / [3.427, 4.763] |
| 60 |  | 2.247 [0.347, 4.128] / [0.370, 4.010] | 1.884 [0.004, 3.704] / [-0.022, 3.614] | 3.271 [0.757, 5.933] / [0.801, 5.920] | 7.473 [7.473, 7.473] / [7.473, 7.473] |  | 12.700 [12.700, 12.700] / [12.700, 12.700] | 12.700 [12.700, 12.700] / [12.700, 12.700] |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 10.15 | 9.78 | 14.44 | 10.26 | 9.85 | 12.22 | 12.26 | 10.16 | 10.02 | 10.12 | 10.35 | 6.35 | 8.88 |  |  | 10.52 | 9.90 |
| 1 | EU | 6.41 | 6.33 | 8.11 | 6.27 | 5.91 | 6.94 | 6.82 | 6.42 | 6.28 | 6.36 | 6.57 | 4.27 | 5.11 |  |  | 6.52 | 6.42 |
| 1 | NA | 8.93 | 8.16 | 9.82 | 8.44 | 8.41 | 9.39 | 9.02 | 8.55 | 8.44 | 8.32 | 8.58 | 5.83 | 7.24 |  |  | 9.38 | 8.31 |
| 3 | Asia | 10.00 | 9.79 | 14.35 | 10.23 | 10.04 | 12.08 | 12.27 | 10.51 | 10.28 | 10.32 | 10.55 | 6.33 | 8.84 | 10.06 | 10.02 | 10.43 | 9.92 |
| 3 | EU | 6.46 | 6.42 | 8.03 | 6.32 | 6.01 | 6.99 | 7.04 | 6.72 | 6.48 | 6.48 | 6.72 | 4.28 | 5.16 | 6.56 | 6.53 | 6.54 | 6.53 |
| 3 | NA | 9.08 | 8.27 | 10.00 | 8.50 | 8.63 | 9.44 | 9.29 | 8.88 | 8.64 | 8.47 | 8.68 | 5.81 | 7.30 | 9.67 | 9.65 | 9.32 | 8.43 |
| 7 | Asia | 9.96 | 10.18 | 15.11 | 10.84 | 10.51 | 12.33 | 12.38 | 11.11 | 10.86 | 10.65 | 10.93 | 6.40 | 9.62 | 10.45 | 10.41 |  | 10.31 |
| 7 | EU | 6.48 | 6.67 | 8.74 | 6.81 | 6.48 | 7.14 | 7.03 | 7.08 | 6.88 | 6.72 | 6.89 | 4.30 | 5.86 | 7.06 | 7.04 |  | 6.79 |
| 7 | NA | 9.41 | 8.61 | 10.48 | 9.00 | 9.19 | 9.60 | 9.35 | 9.28 | 9.13 | 8.76 | 9.01 | 5.94 | 7.93 | 10.24 | 10.23 |  | 8.78 |
| 14 | Asia | 9.77 | 10.34 | 15.46 | 11.17 | 11.04 |  |  |  |  |  |  | 6.28 | 10.05 | 11.21 | 11.17 |  | 10.46 |
| 14 | EU | 6.28 | 6.93 | 8.59 | 7.29 | 6.72 |  |  |  |  |  |  | 4.19 | 6.59 | 7.41 | 7.39 |  | 7.08 |
| 14 | NA | 9.69 | 9.01 | 10.57 | 9.47 | 9.73 |  |  |  |  |  |  | 5.97 | 8.50 | 10.92 | 10.90 |  | 9.22 |
| 30 | Asia | 9.98 | 10.61 | 15.33 | 11.64 | 11.76 |  |  |  |  |  |  | 6.37 | 10.70 | 12.44 | 12.39 |  | 10.78 |
| 30 | EU | 5.83 | 7.25 | 7.96 | 8.14 | 7.39 |  |  |  |  |  |  | 3.85 | 8.08 | 8.61 | 8.59 |  | 7.49 |
| 30 | NA | 9.58 | 9.20 | 10.09 | 9.53 | 9.53 |  |  |  |  |  |  | 5.93 | 8.58 | 11.11 | 11.09 |  | 9.46 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | nf_tide | nf_tide_x_h5 | nf_nhits | nf_nhits_x_h5 | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.39 | -0.78 | -1.47 | -1.12 | -0.61 | -0.97 | -6.02 |  |  | -1.04 |  | -2.52 |  | -0.92 |  | -0.48 | 0.48 |
| 1 | 0.43 | -0.69 | -1.36 | -1.04 | -0.56 | -1.08 | -5.95 |  |  | -1.18 | -4.42 | -2.14 | -5.52 | -1.09 | -4.03 | -0.48 | 0.52 |
| 2 | 0.50 | -0.65 | -1.31 | -1.00 | -0.50 | -1.02 | -5.87 |  |  | -0.83 | -4.39 | -2.45 | -5.76 | -0.87 | -3.88 | -0.48 | 0.57 |
| 3 | 0.56 | -0.54 | -1.20 | -0.89 | -0.43 | -0.91 | -5.82 | 1.72 | 1.71 | -0.66 | -4.23 | -2.73 | -5.97 | -0.87 | -3.97 | -0.47 | 0.64 |
| 4 | 0.59 | -0.44 | -1.09 | -0.80 | -0.35 | -0.89 | -5.74 | 1.80 | 1.79 | -0.70 | -4.28 | -2.11 | -5.45 | -0.38 | -3.52 | -0.47 | 0.71 |
| 5 | 0.56 | -0.39 | -0.99 | -0.75 | -0.32 | -0.86 | -5.79 | 1.84 | 1.83 | -0.55 | -4.00 | -2.18 | -5.59 | -0.35 | -3.41 | -0.47 | 0.74 |
| 6 | 0.51 | -0.36 | -0.89 | -0.73 | -0.31 | -0.95 | -6.36 | 1.72 | 1.71 | -0.88 | -4.35 | -2.23 | -5.57 | -0.54 | -3.79 | -0.47 | 0.75 |
| 7 |  | -0.35 | -0.80 | -0.73 | -0.11 | -0.91 | -6.28 | 1.73 | 1.72 | -0.34 | -3.84 | -2.22 | -5.51 | -0.33 | -3.51 | -0.47 | 1.05 |
| 10 |  | -0.02 | -0.56 | -0.40 | 0.17 | -0.50 | -6.15 | 2.06 | 2.05 |  |  |  |  |  |  | -0.46 | 1.31 |
| 14 |  | 0.32 | -0.19 | -0.07 | 0.70 | -0.19 | -6.06 | 2.46 | 2.44 |  |  |  |  |  |  | -0.43 | 1.89 |
| 21 |  | 1.12 | 0.59 | 0.71 | 1.69 | 0.79 | -5.42 | 3.46 | 3.44 |  |  |  |  |  |  | -0.28 | 2.90 |
| 30 |  | 1.45 | 0.81 | 1.03 | 2.25 | 1.78 | -5.29 | 4.49 | 4.47 |  |  |  |  |  |  | -0.18 | 3.44 |
| 45 |  | 0.13 | 0.22 | -0.29 | 1.26 | 1.52 | -5.06 | 4.74 | 4.72 |  |  |  |  |  |  | -0.20 | 2.46 |
| 60 |  | 2.84 | 2.30 | 2.58 | 3.67 | 0.92 |  | 1.66 | 1.66 |  |  |  |  |  |  | 0.12 | 5.13 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 8.09 [7.92, 8.26] | 7.96 [7.80, 8.13] | 8.22 [8.06, 8.39] | 7.91 [7.75, 8.07] |  |  |
| 1 | 8.19 [8.02, 8.37] | 8.06 [7.89, 8.23] | 8.25 [8.08, 8.43] | 8.06 [7.88, 8.22] |  |  |
| 2 | 8.25 [8.07, 8.43] | 8.11 [7.93, 8.29] | 8.28 [8.11, 8.46] | 8.12 [7.96, 8.30] |  |  |
| 3 | 8.25 [8.07, 8.43] | 8.11 [7.93, 8.29] | 8.26 [8.09, 8.44] | 8.16 [7.98, 8.34] | 8.59 [8.41, 8.77] | 8.56 [8.38, 8.75] |
| 4 | 8.25 [8.07, 8.44] | 8.10 [7.93, 8.29] | 8.23 [8.05, 8.41] | 8.21 [8.04, 8.39] | 8.68 [8.49, 8.86] | 8.65 [8.47, 8.83] |
| 5 | 8.29 [8.10, 8.49] | 8.14 [7.96, 8.33] | 8.22 [8.05, 8.41] | 8.24 [8.06, 8.43] | 8.70 [8.51, 8.89] | 8.67 [8.48, 8.86] |
| 6 | 8.35 [8.16, 8.54] | 8.19 [8.02, 8.39] | 8.22 [8.05, 8.40] | 8.34 [8.15, 8.52] | 8.75 [8.56, 8.95] | 8.72 [8.53, 8.92] |
| 7 | 8.46 [8.27, 8.66] | 8.30 [8.12, 8.50] | 8.61 [8.42, 8.81] | 8.53 [8.32, 8.73] | 8.96 [8.76, 9.15] | 8.93 [8.74, 9.13] |
| 10 | 8.43 [8.24, 8.64] | 8.26 [8.06, 8.47] | 8.55 [8.35, 8.76] | 8.51 [8.31, 8.71] | 9.05 [8.84, 9.26] | 9.02 [8.81, 9.23] |
| 14 | 8.50 [8.29, 8.71] | 8.31 [8.10, 8.52] | 8.75 [8.54, 8.98] | 8.75 [8.51, 8.99] | 9.42 [9.16, 9.67] | 9.39 [9.14, 9.65] |
| 21 | 8.57 [8.31, 8.84] | 8.33 [8.08, 8.59] | 8.93 [8.68, 9.19] | 8.69 [8.42, 8.95] | 9.67 [9.38, 9.96] | 9.64 [9.36, 9.93] |
| 30 | 8.87 [8.54, 9.20] | 8.58 [8.26, 8.90] | 9.28 [8.97, 9.59] | 9.06 [8.72, 9.40] | 10.41 [10.07, 10.77] | 10.38 [10.04, 10.74] |
| 45 | 8.74 [8.23, 9.26] | 8.50 [8.00, 9.01] | 9.39 [8.88, 9.90] | 9.12 [8.63, 9.61] | 10.54 [10.03, 11.05] | 10.51 [10.00, 11.02] |
| 60 | 5.75 [3.99, 7.75] | 5.49 [3.81, 7.40] | 6.95 [5.05, 9.22] | 1.46 [0.27, 11.47] | 1.78 [0.00, 16.70] | 1.78 [0.00, 16.70] |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.07 [6.79, 7.37] | 7.03 [6.75, 7.32] | 7.17 [6.88, 7.47] | 6.72 [6.47, 7.00] |  |  |
| 1 | 7.24 [6.94, 7.55] | 7.20 [6.91, 7.52] | 7.31 [7.03, 7.62] | 6.82 [6.56, 7.07] |  |  |
| 2 | 7.35 [7.07, 7.67] | 7.32 [7.04, 7.63] | 7.39 [7.11, 7.70] | 7.04 [6.80, 7.29] |  |  |
| 3 | 7.47 [7.18, 7.79] | 7.44 [7.14, 7.76] | 7.48 [7.19, 7.81] | 7.30 [7.03, 7.57] | 8.17 [7.87, 8.50] | 8.15 [7.85, 8.48] |
| 4 | 7.60 [7.29, 7.92] | 7.56 [7.25, 7.88] | 7.59 [7.30, 7.91] | 7.47 [7.18, 7.76] | 8.25 [7.97, 8.58] | 8.23 [7.95, 8.56] |
| 5 | 7.80 [7.49, 8.14] | 7.76 [7.44, 8.10] | 7.81 [7.49, 8.17] | 7.63 [7.33, 7.93] | 8.40 [8.10, 8.71] | 8.38 [8.09, 8.70] |
| 6 | 8.10 [7.75, 8.49] | 8.06 [7.71, 8.45] | 8.10 [7.74, 8.52] | 7.96 [7.63, 8.31] | 8.70 [8.37, 9.06] | 8.68 [8.35, 9.04] |
| 7 | 8.41 [8.04, 8.82] | 8.37 [8.00, 8.78] | 8.81 [8.42, 9.23] | 8.44 [8.08, 8.84] | 9.22 [8.83, 9.63] | 9.20 [8.81, 9.61] |
| 10 | 9.01 [8.62, 9.41] | 8.96 [8.58, 9.37] | 9.36 [8.96, 9.80] | 9.01 [8.61, 9.41] | 9.76 [9.34, 10.19] | 9.74 [9.32, 10.17] |
| 14 | 9.54 [9.14, 9.99] | 9.47 [9.07, 9.92] | 10.21 [9.80, 10.66] | 9.55 [9.15, 10.00] | 10.16 [9.77, 10.61] | 10.14 [9.74, 10.58] |
| 21 | 9.77 [9.39, 10.21] | 9.69 [9.31, 10.12] | 10.53 [10.12, 10.97] | 10.00 [9.60, 10.43] | 10.81 [10.37, 11.25] | 10.78 [10.34, 11.22] |
| 30 | 9.69 [9.30, 10.11] | 9.60 [9.21, 10.02] | 10.45 [10.04, 10.90] | 10.22 [9.78, 10.64] | 10.96 [10.52, 11.42] | 10.93 [10.49, 11.38] |
| 45 | 10.53 [10.00, 11.13] | 10.46 [9.94, 11.06] | 11.09 [10.56, 11.73] | 11.23 [10.67, 11.82] | 11.72 [11.13, 12.36] | 11.67 [11.09, 12.31] |
| 60 | 9.94 [8.46, 11.58] | 9.80 [8.34, 11.45] | 10.53 [8.94, 12.38] | 0.18 [0.18, 0.18] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | nf_tide | nf_tide_x_h5 | nf_nhits | nf_nhits_x_h5 | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 1800199.00 | 8.26 | 8.18 | 8.08 | 7.98 | 8.16 | 7.63 | 9.66 |  |  | 7.88 |  | 8.79 |  | 7.77 |  | 5.11 | 7.17 |
| 0 | summer | 1776204.00 | 8.79 | 7.70 | 8.33 | 7.67 | 7.96 | 7.85 | 10.84 |  |  | 7.73 |  | 9.64 |  | 7.53 |  | 5.64 | 6.50 |
| 1 | autumn | 1796586.00 | 8.26 | 8.29 | 8.15 | 8.09 | 8.20 | 7.79 | 9.65 |  |  | 8.34 | 8.25 | 8.90 | 8.75 | 8.20 | 7.99 | 5.12 | 7.16 |
| 1 | summer | 1682642.00 | 8.79 | 7.78 | 8.33 | 7.75 | 7.99 | 7.93 | 10.90 |  |  | 8.01 | 8.34 | 9.63 | 9.45 | 7.89 | 8.15 | 5.64 | 6.53 |
| 3 | autumn | 1791629.00 | 8.26 | 8.38 | 8.19 | 8.17 | 8.22 | 7.93 | 9.74 | 8.37 | 8.36 | 8.71 | 8.42 | 8.94 | 8.85 | 8.47 | 8.17 | 5.13 | 7.16 |
| 3 | summer | 1522680.00 | 8.74 | 7.82 | 8.36 | 7.78 | 8.02 | 8.12 | 10.80 | 8.69 | 8.65 | 8.24 | 8.47 | 9.58 | 9.73 | 8.01 | 8.25 | 5.63 | 6.56 |
| 7 | autumn | 1783013.00 |  | 8.61 | 8.30 | 8.40 | 8.76 | 8.35 | 10.35 | 8.93 | 8.91 | 9.05 | 8.60 | 9.20 | 8.93 | 8.84 | 8.37 | 5.19 | 7.89 |
| 7 | summer | 1225829.00 |  | 8.22 | 8.49 | 8.20 | 8.49 | 8.74 | 11.70 | 9.13 | 9.09 | 8.80 | 8.91 | 9.76 | 9.87 | 8.62 | 8.69 | 5.78 | 7.15 |
| 14 | autumn | 1774797.00 |  | 8.86 | 8.30 | 8.65 | 9.21 | 8.83 | 10.60 | 9.53 | 9.51 |  |  |  |  |  |  | 5.23 | 8.46 |
| 14 | summer | 693115.00 |  | 8.46 | 8.51 | 8.43 | 8.82 | 9.23 | 11.76 | 9.77 | 9.74 |  |  |  |  |  |  | 5.70 | 7.49 |
| 30 | autumn | 1446799.00 |  | 9.16 | 8.32 | 8.94 | 9.70 | 9.47 | 10.71 | 10.60 | 10.57 |  |  |  |  |  |  | 5.31 | 9.08 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

**Spearman of the daily level within park-month** (paired on the same days)

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_nf_tide_wx_owx | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.20 [0.15, 0.25] | 0.21 [0.17, 0.26] | 0.31 [0.27, 0.35] | 0.04 [-0.00, 0.09] | 0.10 [0.05, 0.15] | 0.22 [0.18, 0.26] | 0.33 [0.28, 0.37] | 0.32 [0.28, 0.36] | 0.18 [0.14, 0.23] |
| 2 | 0.17 [0.12, 0.22] | 0.20 [0.15, 0.26] | 0.27 [0.22, 0.31] | 0.02 [-0.02, 0.06] | 0.10 [0.06, 0.15] | 0.21 [0.16, 0.25] | 0.31 [0.26, 0.35] | 0.30 [0.26, 0.34] | 0.16 [0.11, 0.21] |
| 3 | 0.18 [0.14, 0.23] | 0.21 [0.15, 0.26] | 0.27 [0.22, 0.31] | 0.02 [-0.03, 0.06] | 0.09 [0.04, 0.13] | 0.20 [0.15, 0.24] | 0.31 [0.27, 0.36] | 0.29 [0.25, 0.34] | 0.16 [0.11, 0.21] |
| 4 | 0.21 [0.17, 0.26] | 0.20 [0.15, 0.26] | 0.27 [0.23, 0.31] | -0.03 [-0.07, 0.01] | 0.13 [0.09, 0.18] | 0.23 [0.19, 0.27] | 0.32 [0.28, 0.37] | 0.28 [0.24, 0.33] | 0.16 [0.11, 0.21] |
| 5 | 0.25 [0.20, 0.30] | 0.20 [0.15, 0.26] | 0.27 [0.23, 0.32] | -0.01 [-0.06, 0.03] | 0.15 [0.10, 0.20] | 0.23 [0.18, 0.27] | 0.32 [0.28, 0.37] | 0.28 [0.24, 0.32] | 0.15 [0.10, 0.20] |
| 6 | 0.12 [0.07, 0.17] | 0.19 [0.13, 0.24] | 0.27 [0.23, 0.32] | 0.02 [-0.03, 0.06] | 0.19 [0.14, 0.24] | 0.26 [0.22, 0.30] | 0.33 [0.28, 0.38] | 0.28 [0.23, 0.32] | 0.13 [0.08, 0.18] |
| 7 | 0.06 [0.01, 0.11] | 0.19 [0.13, 0.25] | 0.20 [0.15, 0.24] | 0.07 [0.03, 0.11] | 0.04 [-0.01, 0.09] | 0.18 [0.14, 0.22] |  | 0.22 [0.18, 0.26] | 0.11 [0.06, 0.16] |
| 10 | 0.09 [0.04, 0.14] | 0.19 [0.13, 0.24] | 0.18 [0.14, 0.23] |  |  |  |  | 0.19 [0.15, 0.24] | 0.10 [0.04, 0.15] |
| 14 | 0.05 [-0.01, 0.10] | 0.20 [0.14, 0.25] | 0.14 [0.09, 0.19] |  |  |  |  | 0.16 [0.11, 0.22] | 0.05 [-0.02, 0.12] |
| 21 | -0.09 [-0.16, -0.01] | 0.25 [0.18, 0.33] | 0.12 [0.05, 0.20] |  |  |  |  | 0.19 [0.12, 0.25] | 0.01 [-0.07, 0.10] |
| 30 | 0.03 [-0.07, 0.11] | 0.28 [0.19, 0.37] | 0.11 [0.04, 0.20] |  |  |  |  | 0.23 [0.16, 0.29] | 0.04 [-0.04, 0.13] |
| 45 | -0.12 [-0.24, -0.01] | 0.21 [0.06, 0.34] | 0.04 [-0.08, 0.16] |  |  |  |  | 0.30 [0.20, 0.40] | -0.08 [-0.22, 0.06] |

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_nf_tide_wx_owx | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.11 [-0.15, -0.06] | -0.13 [-0.18, -0.08] | -0.01 [-0.05, 0.02] | -0.26 [-0.31, -0.21] | -0.21 [-0.26, -0.17] | -0.07 [-0.11, -0.02] | -0.01 [-0.06, 0.03] | -0.15 [-0.20, -0.10] | lvl_snaive7 |
| 2 | -0.14 [-0.19, -0.09] | -0.12 [-0.18, -0.06] | -0.03 [-0.07, 0.00] | -0.27 [-0.32, -0.23] | -0.20 [-0.25, -0.16] | -0.08 [-0.12, -0.03] | 0.00 [-0.05, 0.05] | -0.15 [-0.20, -0.09] | lvl_snaive7 |
| 3 | -0.13 [-0.18, -0.08] | -0.12 [-0.18, -0.06] | -0.04 [-0.08, -0.00] | -0.28 [-0.34, -0.23] | -0.21 [-0.26, -0.17] | -0.10 [-0.15, -0.05] | -0.00 [-0.05, 0.04] | -0.15 [-0.21, -0.10] | lvl_snaive7 |
| 4 | -0.10 [-0.15, -0.06] | -0.13 [-0.19, -0.07] | -0.05 [-0.09, -0.01] | -0.34 [-0.39, -0.29] | -0.18 [-0.22, -0.13] | -0.07 [-0.12, -0.03] | -0.02 [-0.07, 0.02] | -0.17 [-0.22, -0.11] | lvl_snaive7 |
| 5 | -0.06 [-0.10, -0.03] | -0.12 [-0.18, -0.06] | -0.05 [-0.09, -0.01] | -0.33 [-0.38, -0.28] | -0.16 [-0.21, -0.12] | -0.09 [-0.14, -0.04] | -0.03 [-0.08, 0.02] | -0.17 [-0.22, -0.11] | lvl_snaive7 |
| 6 | -0.19 [-0.24, -0.13] | -0.13 [-0.19, -0.07] | -0.05 [-0.09, -0.02] | -0.30 [-0.35, -0.25] | -0.13 [-0.17, -0.09] | -0.06 [-0.10, -0.01] | -0.05 [-0.09, -0.00] | -0.19 [-0.25, -0.13] | lvl_snaive7 |
| 7 | -0.12 [-0.17, -0.06] | -0.01 [-0.05, 0.04] |  | -0.12 [-0.17, -0.07] | -0.16 [-0.20, -0.11] | -0.01 [-0.06, 0.03] | 0.02 [-0.02, 0.06] | -0.09 [-0.13, -0.04] | lvl_naive4 |
| 10 | -0.08 [-0.14, -0.02] | 0.00 [-0.05, 0.05] |  |  |  |  | 0.01 [-0.04, 0.05] | -0.09 [-0.14, -0.04] | lvl_naive4 |
| 14 | -0.13 [-0.20, -0.05] |  | -0.06 [-0.11, -0.00] |  |  |  | -0.03 [-0.10, 0.04] | -0.15 [-0.20, -0.10] | lvl_clim |
| 21 | -0.33 [-0.42, -0.23] |  | -0.13 [-0.21, -0.06] |  |  |  | -0.07 [-0.14, 0.00] | -0.24 [-0.32, -0.16] | lvl_clim |
| 30 | -0.24 [-0.34, -0.13] |  | -0.16 [-0.25, -0.07] |  |  |  | -0.06 [-0.16, 0.03] | -0.23 [-0.32, -0.13] | lvl_clim |
| 45 | -0.42 [-0.71, -0.09] |  | -0.17 [-0.32, -0.02] |  |  |  | 0.10 [-0.06, 0.24] | -0.29 [-0.45, -0.12] | lvl_clim |

**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the `≥ predicted q75` rule (ties included, which is what flat predictions cost)

| lead | model | park_months | busy_day_recall | busy_day_precision_q75_rule | iqr | iqr_true |
|---|---|---|---|---|---|---|
| 1 | lvl_cbd | 164 | 0.35 | 0.34 | 4.71 | 13.55 |
| 1 | lvl_clim | 254 | 0.42 | 0.38 | 6.37 | 13.55 |
| 1 | lvl_naive4 | 254 | 0.43 | 0.42 | 9.31 | 13.55 |
| 1 | lvl_nf_nhits | 202 | 0.30 | 0.30 | 5.00 | 13.79 |
| 1 | lvl_nf_tide | 202 | 0.30 | 0.30 | 4.22 | 13.79 |
| 1 | lvl_nf_tide_wx_owx | 202 | 0.37 | 0.37 | 5.10 | 13.79 |
| 1 | lvl_snaive7 | 240 | 0.45 | 0.44 | 14.04 | 13.98 |
| 1 | lvl_tft | 250 | 0.43 | 0.43 | 10.77 | 13.55 |
| 1 | lvl_wt56 | 254 | 0.38 | 0.37 | 5.09 | 13.55 |
| 2 | lvl_cbd | 152 | 0.35 | 0.34 | 4.33 | 13.58 |
| 2 | lvl_clim | 201 | 0.42 | 0.38 | 5.43 | 13.58 |
| 2 | lvl_naive4 | 201 | 0.42 | 0.42 | 9.57 | 13.58 |
| 2 | lvl_nf_nhits | 201 | 0.30 | 0.30 | 4.87 | 13.58 |
| 2 | lvl_nf_tide | 201 | 0.33 | 0.33 | 3.85 | 13.58 |
| 2 | lvl_nf_tide_wx_owx | 201 | 0.39 | 0.39 | 4.94 | 13.58 |
| 2 | lvl_snaive7 | 191 | 0.45 | 0.44 | 14.36 | 13.91 |
| 2 | lvl_tft | 200 | 0.42 | 0.43 | 11.24 | 13.63 |
| 2 | lvl_wt56 | 201 | 0.37 | 0.35 | 5.61 | 13.58 |
| 3 | lvl_cbd | 159 | 0.38 | 0.37 | 4.82 | 13.58 |
| 3 | lvl_clim | 200 | 0.43 | 0.39 | 5.57 | 13.64 |
| 3 | lvl_naive4 | 200 | 0.42 | 0.42 | 9.51 | 13.64 |
| 3 | lvl_nf_nhits | 200 | 0.29 | 0.29 | 4.67 | 13.64 |
| 3 | lvl_nf_tide | 200 | 0.32 | 0.32 | 3.70 | 13.64 |
| 3 | lvl_nf_tide_wx_owx | 200 | 0.39 | 0.39 | 4.95 | 13.64 |
| 3 | lvl_snaive7 | 190 | 0.45 | 0.44 | 14.30 | 13.87 |
| 3 | lvl_tft | 199 | 0.42 | 0.42 | 11.73 | 13.70 |
| 3 | lvl_wt56 | 200 | 0.37 | 0.35 | 5.54 | 13.64 |
| 4 | lvl_cbd | 159 | 0.38 | 0.37 | 5.00 | 13.58 |
| 4 | lvl_clim | 200 | 0.42 | 0.38 | 5.57 | 13.66 |
| 4 | lvl_naive4 | 200 | 0.41 | 0.40 | 9.70 | 13.66 |
| 4 | lvl_nf_nhits | 200 | 0.25 | 0.25 | 4.64 | 13.66 |
| 4 | lvl_nf_tide | 200 | 0.32 | 0.32 | 3.85 | 13.66 |
| 4 | lvl_nf_tide_wx_owx | 200 | 0.38 | 0.38 | 5.14 | 13.66 |
| 4 | lvl_snaive7 | 190 | 0.44 | 0.44 | 14.45 | 13.95 |
| 4 | lvl_tft | 198 | 0.41 | 0.41 | 12.30 | 13.66 |
| 4 | lvl_wt56 | 200 | 0.36 | 0.34 | 5.48 | 13.66 |
| 5 | lvl_cbd | 160 | 0.40 | 0.39 | 5.95 | 13.58 |
| 5 | lvl_clim | 196 | 0.42 | 0.38 | 5.88 | 13.98 |
| 5 | lvl_naive4 | 196 | 0.41 | 0.41 | 9.52 | 13.98 |
| 5 | lvl_nf_nhits | 196 | 0.27 | 0.27 | 4.45 | 13.98 |
| 5 | lvl_nf_tide | 196 | 0.34 | 0.34 | 3.71 | 13.98 |
| 5 | lvl_nf_tide_wx_owx | 196 | 0.38 | 0.38 | 5.21 | 13.98 |
| 5 | lvl_snaive7 | 188 | 0.46 | 0.45 | 14.35 | 14.11 |
| 5 | lvl_tft | 194 | 0.42 | 0.42 | 12.75 | 14.08 |
| 5 | lvl_wt56 | 196 | 0.36 | 0.35 | 5.59 | 13.98 |
| 6 | lvl_cbd | 157 | 0.33 | 0.32 | 4.20 | 13.41 |
| 6 | lvl_clim | 193 | 0.42 | 0.38 | 5.57 | 13.58 |
| 6 | lvl_naive4 | 193 | 0.42 | 0.42 | 9.52 | 13.58 |
| 6 | lvl_nf_nhits | 193 | 0.30 | 0.30 | 4.75 | 13.58 |
| 6 | lvl_nf_tide | 193 | 0.37 | 0.37 | 3.77 | 13.58 |
| 6 | lvl_nf_tide_wx_owx | 193 | 0.40 | 0.40 | 5.04 | 13.58 |
| 6 | lvl_snaive7 | 187 | 0.46 | 0.45 | 14.09 | 13.67 |
| 6 | lvl_tft | 193 | 0.44 | 0.44 | 12.74 | 13.58 |
| 6 | lvl_wt56 | 193 | 0.36 | 0.35 | 5.55 | 13.58 |
| 7 | lvl_cbd | 149 | 0.30 | 0.30 | 4.00 | 13.16 |
| 7 | lvl_clim | 192 | 0.42 | 0.38 | 5.53 | 13.41 |
| 7 | lvl_naive4 | 192 | 0.37 | 0.37 | 9.24 | 13.41 |
| 7 | lvl_nf_nhits | 192 | 0.32 | 0.32 | 4.69 | 13.41 |
| 7 | lvl_nf_tide | 192 | 0.28 | 0.28 | 3.49 | 13.41 |
| 7 | lvl_nf_tide_wx_owx | 192 | 0.36 | 0.36 | 4.45 | 13.41 |
| 7 | lvl_tft | 191 | 0.41 | 0.41 | 12.38 | 13.41 |
| 7 | lvl_wt56 | 192 | 0.36 | 0.34 | 5.17 | 13.41 |
| 10 | lvl_cbd | 150 | 0.32 | 0.31 | 3.64 | 12.47 |
| 10 | lvl_clim | 186 | 0.43 | 0.39 | 5.48 | 13.11 |
| 10 | lvl_naive4 | 186 | 0.36 | 0.36 | 8.96 | 13.11 |
| 10 | lvl_tft | 184 | 0.40 | 0.40 | 12.59 | 13.11 |
| 10 | lvl_wt56 | 186 | 0.35 | 0.34 | 5.04 | 13.11 |
| 14 | lvl_cbd | 140 | 0.29 | 0.27 | 3.87 | 13.11 |
| 14 | lvl_clim | 175 | 0.44 | 0.41 | 5.31 | 13.43 |
| 14 | lvl_naive4 | 175 | 0.35 | 0.34 | 8.20 | 13.43 |
| 14 | lvl_tft | 175 | 0.39 | 0.39 | 11.98 | 13.43 |
| 14 | lvl_wt56 | 175 | 0.33 | 0.32 | 4.46 | 13.43 |
| 21 | lvl_cbd | 74 | 0.24 | 0.24 | 4.38 | 14.50 |
| 21 | lvl_clim | 94 | 0.48 | 0.45 | 4.42 | 15.47 |
| 21 | lvl_naive4 | 94 | 0.31 | 0.31 | 9.02 | 15.47 |
| 21 | lvl_tft | 94 | 0.40 | 0.40 | 13.64 | 15.47 |
| 21 | lvl_wt56 | 94 | 0.31 | 0.30 | 6.05 | 15.47 |
| 30 | lvl_cbd | 61 | 0.28 | 0.28 | 3.75 | 13.25 |
| 30 | lvl_clim | 81 | 0.50 | 0.44 | 3.71 | 15.21 |
| 30 | lvl_naive4 | 81 | 0.34 | 0.34 | 8.41 | 15.21 |
| 30 | lvl_tft | 80 | 0.41 | 0.41 | 13.01 | 14.95 |
| 30 | lvl_wt56 | 81 | 0.32 | 0.30 | 4.67 | 15.21 |
| 45 | lvl_cbd | 13 | 0.22 | 0.24 | 4.00 | 11.28 |
| 45 | lvl_clim | 47 | 0.45 | 0.33 | 0.00 | 11.81 |
| 45 | lvl_naive4 | 47 | 0.37 | 0.37 | 5.42 | 11.81 |
| 45 | lvl_tft | 46 | 0.43 | 0.43 | 10.00 | 11.88 |
| 45 | lvl_wt56 | 47 | 0.31 | 0.30 | 1.14 | 11.81 |

## D1 — next-best ride

Candidates: every (intraday origin, ride) with a live OPERATING wait — the same set for every model. Suggest when the forecast maximum within the lookahead (60 or 120 min) is ≥ live + 10 (`next-best-ride.ts`, walking time 0); correct when the TRUE maximum there is also ≥ live + 10. False-suggestion rate = 1 − precision.

**next-best precision**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.27 [0.27, 0.28] | 0.24 [0.23, 0.25] | 0.28 [0.27, 0.28] | 0.26 [0.25, 0.27] | 0.27 [0.26, 0.28] | 0.33 [0.32, 0.34] | 0.33 [0.32, 0.34] | 0.35 [0.35, 0.36] | 0.26 [0.25, 0.26] |
| 0-120 | 0.35 [0.34, 0.35] | 0.31 [0.30, 0.32] | 0.35 [0.35, 0.36] | 0.33 [0.32, 0.34] | 0.34 [0.33, 0.35] | 0.41 [0.40, 0.42] | 0.41 [0.40, 0.42] | 0.44 [0.43, 0.44] | 0.33 [0.32, 0.33] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.04 [-0.06, -0.02] | -0.06 [-0.08, -0.04] | -0.03 [-0.05, -0.02] | -0.04 [-0.06, -0.02] | -0.04 [-0.06, -0.01] | 0.05 [0.02, 0.09]* | 0.05 [0.02, 0.08]* | -0.05 [-0.07, -0.03] | snaive7 |
| 0-120 | -0.04 [-0.07, -0.02] | -0.07 [-0.09, -0.05] | -0.03 [-0.05, -0.01] | -0.04 [-0.06, -0.02] | -0.04 [-0.07, -0.02] | 0.05 [0.02, 0.09]* | 0.05 [0.02, 0.09]* | -0.06 [-0.08, -0.04] | snaive7 |

**next-best precision (top-3)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.35 [0.34, 0.35] | 0.32 [0.31, 0.33] | 0.34 [0.34, 0.35] | 0.33 [0.32, 0.33] | 0.31 [0.31, 0.32] | 0.40 [0.39, 0.41] | 0.40 [0.39, 0.41] | 0.42 [0.41, 0.42] | 0.33 [0.33, 0.34] |
| 0-120 | 0.42 [0.41, 0.43] | 0.39 [0.39, 0.40] | 0.42 [0.42, 0.43] | 0.40 [0.39, 0.41] | 0.38 [0.38, 0.39] | 0.48 [0.47, 0.49] | 0.48 [0.47, 0.49] | 0.51 [0.50, 0.51] | 0.41 [0.40, 0.42] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.02 [-0.03, -0.00] | -0.04 [-0.05, -0.02] | -0.02 [-0.03, -0.01] | -0.03 [-0.05, -0.02] | -0.06 [-0.08, -0.04] | 0.05 [0.03, 0.07]* | 0.04 [0.03, 0.06]* | -0.03 [-0.04, -0.02] | snaive7 |
| 0-120 | -0.03 [-0.04, -0.01] | -0.05 [-0.06, -0.03] | -0.03 [-0.04, -0.01] | -0.04 [-0.05, -0.02] | -0.07 [-0.09, -0.06] | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.03 [-0.05, -0.02] | snaive7 |

**next-best recall**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.42 [0.41, 0.43] | 0.42 [0.41, 0.43] | 0.39 [0.38, 0.40] | 0.39 [0.38, 0.40] | 0.22 [0.22, 0.22] | 0.34 [0.33, 0.35] | 0.33 [0.32, 0.34] | 0.52 [0.51, 0.53] | 0.50 [0.49, 0.51] |
| 0-120 | 0.45 [0.44, 0.46] | 0.45 [0.44, 0.46] | 0.42 [0.41, 0.42] | 0.41 [0.40, 0.42] | 0.23 [0.23, 0.23] | 0.37 [0.36, 0.38] | 0.38 [0.37, 0.38] | 0.57 [0.56, 0.58] | 0.53 [0.52, 0.54] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.10 [-0.13, -0.07] | -0.10 [-0.13, -0.06] | -0.13 [-0.15, -0.10] | -0.13 [-0.17, -0.10] | -0.30 [-0.32, -0.28] | -0.18 [-0.21, -0.15] | -0.19 [-0.22, -0.16] | -0.02 [-0.05, 0.01] | snaive7 |
| 0-120 | -0.12 [-0.15, -0.09] | -0.11 [-0.14, -0.08] | -0.15 [-0.18, -0.12] | -0.15 [-0.19, -0.11] | -0.34 [-0.36, -0.31] | -0.20 [-0.23, -0.16] | -0.19 [-0.22, -0.15] | -0.03 [-0.06, -0.00] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| w0-45 | 13.57 [13.32, 13.82] | 12.61 [12.40, 12.83] | 12.79 [12.56, 13.00] | 11.93 [11.71, 12.13] | 8.38 [8.28, 8.48] | 7.74 [7.64, 7.84] | 7.69 [7.59, 7.78] | 6.09 [6.00, 6.18] | 13.71 [13.44, 13.98] | 12.84 [12.62, 13.05] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | nf_tide_wx_owx | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| w0-45 | 8.17 [7.30, 9.15] | 7.12 [6.34, 7.95] | 7.29 [6.46, 8.18] | 6.39 [5.72, 7.10] | 1.96 [1.70, 2.27] | 1.58 [1.31, 1.85] | 1.50 [1.22, 1.77] | 8.16 [7.23, 9.08] | 7.31 [6.49, 8.18] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | nf_tide | nf_nhits | nf_tide_wx_owx |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | -0.34 | 0.71 | -0.39 | -1.40 | -1.05 | -0.66 | -1.47 | -0.52 | 0.21 | -0.84 |
| m030 | -0.51 | 0.73 | -0.43 | -1.56 | -0.98 | -0.59 | -1.39 | -0.80 | -0.15 | -1.10 |
| m045 | -0.48 | 0.68 | -0.49 | -1.64 | -0.94 | -0.54 | -1.29 | -0.90 | -0.20 | -1.19 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.82 [0.81, 0.82] | 0.82 [0.81, 0.82] | 0.75 [0.75, 0.76] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.76 [0.75, 0.76] |  | 0.76 [0.76, 0.77] | 0.78 [0.78, 0.79] |  |  |  |  | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 1 | 0.82 [0.81, 0.82] | 0.82 [0.81, 0.82] | 0.75 [0.75, 0.76] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.75 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.75] | 0.77 [0.76, 0.77] | 0.76 [0.76, 0.77] |  |  | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 2 | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.82] | 0.75 [0.75, 0.76] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.74 [0.74, 0.75] | 0.75 [0.74, 0.75] | 0.75 [0.75, 0.76] | 0.73 [0.72, 0.73] | 0.77 [0.76, 0.77] | 0.76 [0.76, 0.77] |  |  | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 3 | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.82] | 0.75 [0.75, 0.76] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.73 [0.72, 0.73] | 0.75 [0.74, 0.75] | 0.73 [0.72, 0.73] | 0.74 [0.74, 0.75] | 0.77 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.78] | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 4 | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.83] | 0.75 [0.75, 0.76] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.73 [0.73, 0.74] | 0.75 [0.74, 0.75] | 0.77 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.78] | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 5 | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.83] | 0.75 [0.75, 0.76] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.75 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.75 [0.74, 0.75] | 0.77 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.78] | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 6 | 0.82 [0.81, 0.83] | 0.82 [0.82, 0.83] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] | 0.75 [0.74, 0.75] | 0.75 [0.75, 0.76] | 0.74 [0.73, 0.74] | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.78] | 0.80 [0.80, 0.81] | 0.81 [0.81, 0.82] |
| 7 | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] | 0.75 [0.75, 0.76] | 0.75 [0.74, 0.76] | 0.72 [0.71, 0.72] | 0.72 [0.72, 0.73] | 0.77 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.78] |  | 0.81 [0.81, 0.82] |
| 10 | 0.82 [0.82, 0.83] | 0.82 [0.81, 0.82] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] |  |  |  |  |  |  | 0.76 [0.75, 0.76] | 0.78 [0.77, 0.78] |  | 0.82 [0.81, 0.82] |
| 14 | 0.82 [0.82, 0.83] | 0.82 [0.81, 0.83] | 0.74 [0.73, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] |  |  |  |  |  |  | 0.76 [0.75, 0.76] | 0.78 [0.77, 0.78] |  | 0.82 [0.81, 0.82] |
| 21 | 0.83 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.80] |  |  |  |  |  |  | 0.76 [0.75, 0.76] | 0.78 [0.77, 0.79] |  | 0.82 [0.81, 0.82] |
| 30 | 0.83 [0.82, 0.83] | 0.82 [0.81, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.80] |  |  |  |  |  |  | 0.76 [0.75, 0.76] | 0.78 [0.77, 0.79] |  | 0.82 [0.81, 0.82] |
| 45 | 0.83 [0.82, 0.84] | 0.81 [0.80, 0.82] | 0.73 [0.71, 0.75] | 0.80 [0.79, 0.81] | 0.79 [0.78, 0.80] |  |  |  |  |  |  | 0.75 [0.74, 0.76] | 0.78 [0.76, 0.79] |  | 0.81 [0.80, 0.82] |
| 60 | 0.79 [0.73, 0.85] | 0.79 [0.73, 0.84] |  | 0.77 [0.71, 0.83] | 1.00 [1.00, 1.00] |  |  |  |  |  |  | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.77 [0.71, 0.83] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.05 [-0.06, -0.04] |  | -0.04 [-0.05, -0.03] | -0.03 [-0.03, -0.02] |  |  |  |  | -0.03 [-0.04, -0.02] |  | wt_med |
| 1 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.05 [-0.06, -0.04] | -0.06 [-0.06, -0.04] | -0.05 [-0.06, -0.04] | -0.05 [-0.06, -0.04] | -0.04 [-0.05, -0.03] | -0.05 [-0.06, -0.04] |  |  | -0.03 [-0.04, -0.02] |  | wt_med |
| 2 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.06 [-0.07, -0.05] | -0.06 [-0.07, -0.05] | -0.05 [-0.07, -0.04] | -0.08 [-0.09, -0.06] | -0.04 [-0.05, -0.03] | -0.05 [-0.06, -0.04] |  |  | -0.03 [-0.03, -0.02] |  | wt_med |
| 3 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.08 [-0.09, -0.07] | -0.06 [-0.07, -0.05] | -0.08 [-0.09, -0.07] | -0.06 [-0.08, -0.05] | -0.04 [-0.05, -0.03] | -0.05 [-0.06, -0.04] | -0.06 [-0.08, -0.05] | -0.04 [-0.05, -0.03] | -0.03 [-0.03, -0.02] |  | wt_med |
| 4 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.05 [-0.06, -0.04] | -0.06 [-0.07, -0.05] | -0.07 [-0.09, -0.06] | -0.06 [-0.07, -0.04] | -0.04 [-0.05, -0.03] | -0.05 [-0.06, -0.04] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] | -0.03 [-0.03, -0.02] |  | wt_med |
| 5 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.05 [-0.06, -0.04] | -0.06 [-0.07, -0.05] | -0.05 [-0.06, -0.04] | -0.06 [-0.07, -0.04] | -0.04 [-0.05, -0.03] | -0.05 [-0.06, -0.04] | -0.06 [-0.08, -0.05] | -0.04 [-0.05, -0.03] | -0.03 [-0.03, -0.02] |  | wt_med |
| 6 | -0.00 [-0.01, 0.00] | 0.01 [0.01, 0.02]* | -0.06 [-0.07, -0.05] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] | -0.06 [-0.07, -0.05] | -0.06 [-0.07, -0.05] | -0.07 [-0.08, -0.05] | -0.06 [-0.07, -0.05] | -0.04 [-0.05, -0.03] | -0.05 [-0.06, -0.04] | -0.06 [-0.08, -0.05] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.02] |  | wt_med |
| 7 | -0.00 [-0.01, 0.00] | 0.01 [0.01, 0.02]* | -0.06 [-0.08, -0.05] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] | -0.05 [-0.06, -0.04] | -0.06 [-0.07, -0.05] | -0.09 [-0.10, -0.07] | -0.08 [-0.10, -0.07] | -0.05 [-0.05, -0.03] | -0.05 [-0.06, -0.04] | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.02] |  |  | wt_med |
| 10 | -0.00 [-0.00, 0.00] | 0.01 [0.01, 0.02]* | -0.06 [-0.08, -0.05] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] |  |  |  |  |  |  | -0.06 [-0.07, -0.04] | -0.04 [-0.05, -0.02] |  |  | wt_med |
| 14 | 0.00 [-0.00, 0.00] | 0.01 [0.01, 0.02]* | -0.07 [-0.08, -0.05] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] |  |  |  |  |  |  | -0.06 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  |  | wt_med |
| 21 |  | 0.01 [-0.00, 0.01] | -0.07 [-0.09, -0.06] | -0.01 [-0.02, -0.01] | -0.03 [-0.04, -0.02] |  |  |  |  |  |  | -0.06 [-0.08, -0.04] | -0.03 [-0.05, -0.02] |  | -0.01 [-0.01, -0.00] | clim |
| 30 |  | 0.01 [-0.00, 0.02] | -0.07 [-0.09, -0.05] | -0.01 [-0.02, -0.00] | -0.02 [-0.04, -0.01] |  |  |  |  |  |  | -0.06 [-0.07, -0.04] | -0.03 [-0.04, -0.01] |  | -0.01 [-0.01, -0.00] | clim |
| 45 | 0.00 [-0.00, 0.01] | 0.02 [0.01, 0.02] | -0.04 [-0.07, -0.01] | -0.01 [-0.02, 0.01] | -0.01 [-0.02, 0.01] |  |  |  |  |  |  | -0.05 [-0.07, -0.04] | -0.02 [-0.04, -0.01] |  |  | wt_med |
| 60 | 0.02 [-0.00, 0.03] | 0.01 [-0.01, 0.04] |  | -0.00 [-0.03, 0.03] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  | wt_med |

**best-time hit rate (±30 min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.78 [0.78, 0.78] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] | 0.76 [0.75, 0.76] |  | 0.76 [0.76, 0.77] | 0.78 [0.77, 0.78] |  |  |  |  | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] |
| 1 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.78 [0.78, 0.79] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] | 0.75 [0.75, 0.76] | 0.79 [0.78, 0.79] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.80 [0.79, 0.80] | 0.79 [0.79, 0.80] |  |  | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] |
| 2 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.81, 0.82] | 0.81 [0.81, 0.81] | 0.75 [0.74, 0.75] | 0.78 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.74 [0.74, 0.75] | 0.80 [0.79, 0.80] | 0.79 [0.79, 0.79] |  |  | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] |
| 3 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.78 [0.78, 0.78] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] | 0.74 [0.73, 0.74] | 0.78 [0.78, 0.79] | 0.73 [0.73, 0.74] | 0.75 [0.74, 0.75] | 0.80 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.79] | 0.80 [0.79, 0.80] | 0.82 [0.82, 0.83] | 0.82 [0.81, 0.82] |
| 4 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.78 [0.78, 0.79] | 0.82 [0.81, 0.82] | 0.81 [0.81, 0.81] | 0.76 [0.75, 0.76] | 0.79 [0.78, 0.79] | 0.75 [0.74, 0.75] | 0.75 [0.75, 0.76] | 0.80 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.78 [0.78, 0.79] | 0.80 [0.79, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] |
| 5 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.78 [0.78, 0.79] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] | 0.75 [0.75, 0.76] | 0.79 [0.78, 0.79] | 0.76 [0.76, 0.77] | 0.76 [0.75, 0.76] | 0.80 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.78 [0.78, 0.79] | 0.80 [0.79, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] |
| 6 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.78] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] | 0.75 [0.74, 0.75] | 0.79 [0.78, 0.79] | 0.75 [0.74, 0.75] | 0.75 [0.75, 0.76] | 0.80 [0.79, 0.80] | 0.79 [0.79, 0.79] | 0.78 [0.78, 0.79] | 0.80 [0.79, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.82] |
| 7 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.77 [0.76, 0.78] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.81] | 0.76 [0.75, 0.76] | 0.78 [0.78, 0.79] | 0.72 [0.72, 0.73] | 0.73 [0.73, 0.74] | 0.79 [0.79, 0.80] | 0.79 [0.79, 0.79] | 0.78 [0.78, 0.79] | 0.80 [0.79, 0.80] |  | 0.82 [0.81, 0.82] |
| 10 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.78] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.81] |  |  |  |  |  |  | 0.78 [0.78, 0.79] | 0.80 [0.79, 0.80] |  | 0.82 [0.82, 0.83] |
| 14 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.81] |  |  |  |  |  |  | 0.78 [0.78, 0.79] | 0.80 [0.79, 0.81] |  | 0.82 [0.81, 0.83] |
| 21 | 0.83 [0.83, 0.84] | 0.83 [0.82, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.81, 0.83] | 0.81 [0.80, 0.82] |  |  |  |  |  |  | 0.79 [0.78, 0.79] | 0.80 [0.79, 0.81] |  | 0.82 [0.82, 0.83] |
| 30 | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.82] |  |  |  |  |  |  | 0.79 [0.78, 0.79] | 0.80 [0.79, 0.81] |  | 0.82 [0.81, 0.83] |
| 45 | 0.84 [0.83, 0.85] | 0.82 [0.81, 0.83] | 0.77 [0.75, 0.79] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] |  |  |  |  |  |  | 0.78 [0.77, 0.80] | 0.80 [0.79, 0.81] |  | 0.82 [0.81, 0.83] |
| 60 | 0.81 [0.75, 0.86] | 0.80 [0.74, 0.86] |  | 0.80 [0.73, 0.86] | 1.00 [1.00, 1.00] |  |  |  |  |  |  | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.79 [0.72, 0.85] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.06 [-0.06, -0.05] |  | -0.05 [-0.06, -0.04] | -0.03 [-0.04, -0.02] |  |  |  |  | -0.02 [-0.03, -0.01] | wt_med |
| 1 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.00] | -0.06 [-0.07, -0.05] | -0.03 [-0.04, -0.02] | -0.05 [-0.06, -0.04] | -0.06 [-0.07, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.02] |  |  | -0.02 [-0.02, -0.01] | wt_med |
| 2 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.00] | -0.06 [-0.07, -0.05] | -0.03 [-0.04, -0.02] | -0.06 [-0.07, -0.05] | -0.07 [-0.08, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.03, -0.02] |  |  | -0.02 [-0.02, -0.01] | wt_med |
| 3 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.01] | -0.01 [-0.01, 0.00] | -0.08 [-0.09, -0.07] | -0.04 [-0.04, -0.03] | -0.08 [-0.09, -0.07] | -0.07 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.03, -0.02] | -0.04 [-0.05, -0.03] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | wt_med |
| 4 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.00] | -0.06 [-0.07, -0.05] | -0.03 [-0.04, -0.02] | -0.06 [-0.08, -0.05] | -0.06 [-0.07, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.02] | -0.04 [-0.05, -0.03] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | wt_med |
| 5 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.01] | -0.01 [-0.01, 0.00] | -0.06 [-0.07, -0.05] | -0.03 [-0.04, -0.02] | -0.05 [-0.06, -0.04] | -0.06 [-0.07, -0.04] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.02] | -0.04 [-0.05, -0.03] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | wt_med |
| 6 | -0.00 [-0.01, 0.00] | 0.01 [0.01, 0.02]* | -0.04 [-0.05, -0.03] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, -0.00] | -0.06 [-0.07, -0.05] | -0.03 [-0.04, -0.02] | -0.06 [-0.07, -0.05] | -0.06 [-0.07, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.02] | -0.04 [-0.05, -0.03] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | wt_med |
| 7 | -0.00 [-0.01, 0.00] | 0.01 [0.01, 0.02]* | -0.04 [-0.05, -0.03] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.05 [-0.06, -0.04] | -0.03 [-0.04, -0.02] | -0.09 [-0.10, -0.07] | -0.08 [-0.09, -0.07] | -0.02 [-0.03, -0.02] | -0.03 [-0.04, -0.02] | -0.04 [-0.05, -0.03] | -0.02 [-0.03, -0.01] |  | wt_med |
| 10 | -0.00 [-0.00, 0.00] | 0.01 [0.01, 0.02]* | -0.04 [-0.05, -0.03] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, -0.00] |  |  |  |  |  |  | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | wt_med |
| 14 | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.02]* | -0.04 [-0.06, -0.03] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.00] |  |  |  |  |  |  | -0.03 [-0.04, -0.02] | -0.02 [-0.03, -0.01] |  | wt_med |
| 21 | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.02]* | -0.04 [-0.05, -0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.00] |  |  |  |  |  |  | -0.03 [-0.04, -0.02] | -0.01 [-0.02, -0.00] |  | wt_med |
| 30 | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.02]* | -0.04 [-0.06, -0.02] | 0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  |  |  |  |  |  | -0.03 [-0.04, -0.01] | -0.01 [-0.02, 0.01] |  | wt_med |
| 45 | 0.00 [-0.00, 0.01] | 0.02 [0.01, 0.03] | -0.02 [-0.05, 0.02] | 0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  |  |  |  |  |  | -0.02 [-0.04, -0.01] | -0.01 [-0.02, 0.01] |  | wt_med |
| 60 | 0.01 [-0.01, 0.03] | 0.01 [-0.01, 0.03] |  | 0.01 [-0.02, 0.04] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |

**best-time regret (min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3.19 [3.08, 3.30] | 2.93 [2.84, 3.03] | 3.97 [3.87, 4.07] | 3.18 [3.09, 3.27] | 3.33 [3.25, 3.43] | 4.83 [4.71, 4.94] |  | 4.35 [4.24, 4.46] | 4.00 [3.90, 4.11] |  |  |  |  | 3.57 [3.43, 3.72] | 3.21 [3.11, 3.32] |
| 1 | 3.18 [3.07, 3.29] | 2.93 [2.84, 3.03] | 3.95 [3.85, 4.05] | 3.17 [3.08, 3.27] | 3.33 [3.24, 3.42] | 4.95 [4.83, 5.08] | 3.92 [3.82, 4.02] | 4.58 [4.47, 4.69] | 4.66 [4.55, 4.78] | 3.69 [3.60, 3.78] | 3.79 [3.70, 3.89] |  |  | 3.58 [3.44, 3.73] | 3.21 [3.11, 3.31] |
| 2 | 3.16 [3.04, 3.28] | 2.92 [2.83, 3.03] | 3.96 [3.86, 4.07] | 3.17 [3.08, 3.27] | 3.33 [3.23, 3.43] | 5.16 [5.04, 5.30] | 3.98 [3.88, 4.08] | 4.78 [4.66, 4.91] | 4.99 [4.88, 5.11] | 3.68 [3.59, 3.78] | 3.82 [3.72, 3.91] |  |  | 3.56 [3.42, 3.72] | 3.20 [3.11, 3.32] |
| 3 | 3.14 [3.03, 3.25] | 2.92 [2.83, 3.02] | 3.94 [3.84, 4.05] | 3.16 [3.07, 3.26] | 3.32 [3.22, 3.42] | 5.37 [5.23, 5.50] | 4.02 [3.93, 4.13] | 5.08 [4.96, 5.21] | 4.79 [4.67, 4.92] | 3.69 [3.60, 3.79] | 3.80 [3.70, 3.90] | 4.47 [4.33, 4.63] | 3.78 [3.66, 3.91] | 3.55 [3.40, 3.70] | 3.19 [3.09, 3.30] |
| 4 | 3.13 [3.01, 3.25] | 2.92 [2.82, 3.02] | 3.93 [3.83, 4.04] | 3.17 [3.07, 3.27] | 3.33 [3.23, 3.43] | 4.93 [4.80, 5.05] | 3.93 [3.82, 4.04] | 4.97 [4.86, 5.10] | 4.73 [4.61, 4.86] | 3.64 [3.54, 3.74] | 3.78 [3.68, 3.88] | 4.46 [4.32, 4.62] | 3.77 [3.65, 3.90] | 3.55 [3.40, 3.71] | 3.21 [3.11, 3.32] |
| 5 | 3.11 [3.00, 3.23] | 2.93 [2.83, 3.03] | 3.93 [3.82, 4.05] | 3.18 [3.07, 3.28] | 3.35 [3.26, 3.45] | 5.05 [4.91, 5.19] | 3.94 [3.83, 4.05] | 4.69 [4.57, 4.80] | 4.66 [4.54, 4.79] | 3.66 [3.56, 3.76] | 3.79 [3.69, 3.89] | 4.44 [4.29, 4.59] | 3.73 [3.61, 3.86] | 3.54 [3.39, 3.71] | 3.20 [3.09, 3.31] |
| 6 | 3.10 [2.98, 3.22] | 2.96 [2.86, 3.06] | 4.17 [4.06, 4.28] | 3.21 [3.11, 3.31] | 3.43 [3.33, 3.52] | 5.04 [4.91, 5.17] | 3.99 [3.88, 4.10] | 4.87 [4.75, 5.00] | 4.75 [4.64, 4.87] | 3.76 [3.66, 3.86] | 3.87 [3.76, 3.97] | 4.46 [4.32, 4.62] | 3.78 [3.66, 3.91] | 3.54 [3.39, 3.70] | 3.21 [3.10, 3.33] |
| 7 | 3.10 [2.98, 3.22] | 3.00 [2.89, 3.10] | 4.24 [4.12, 4.37] | 3.26 [3.15, 3.36] | 3.52 [3.42, 3.63] | 5.04 [4.91, 5.17] | 4.05 [3.94, 4.16] | 5.40 [5.27, 5.55] | 5.19 [5.07, 5.32] | 3.85 [3.74, 3.96] | 3.90 [3.79, 4.01] | 4.46 [4.31, 4.62] | 3.78 [3.66, 3.92] |  | 3.24 [3.13, 3.36] |
| 10 | 3.05 [2.93, 3.18] | 2.97 [2.86, 3.09] | 4.23 [4.10, 4.35] | 3.22 [3.12, 3.34] | 3.48 [3.37, 3.60] |  |  |  |  |  |  | 4.41 [4.26, 4.58] | 3.72 [3.59, 3.86] |  | 3.20 [3.08, 3.33] |
| 14 | 3.03 [2.90, 3.17] | 2.98 [2.87, 3.12] | 4.29 [4.15, 4.43] | 3.25 [3.13, 3.38] | 3.50 [3.37, 3.64] |  |  |  |  |  |  | 4.40 [4.24, 4.58] | 3.72 [3.58, 3.87] |  | 3.22 [3.09, 3.36] |
| 21 | 2.91 [2.77, 3.07] | 2.96 [2.82, 3.11] | 4.19 [4.03, 4.36] | 3.23 [3.08, 3.37] | 3.48 [3.34, 3.62] |  |  |  |  |  |  | 4.42 [4.22, 4.62] | 3.71 [3.55, 3.88] |  | 3.20 [3.06, 3.38] |
| 30 | 2.88 [2.72, 3.04] | 3.05 [2.88, 3.23] | 4.26 [4.06, 4.46] | 3.29 [3.12, 3.46] | 3.54 [3.36, 3.72] |  |  |  |  |  |  | 4.54 [4.30, 4.79] | 3.79 [3.59, 4.00] |  | 3.28 [3.10, 3.48] |
| 45 | 2.93 [2.68, 3.18] | 3.25 [2.99, 3.52] | 4.50 [4.04, 4.97] | 3.52 [3.26, 3.80] | 3.62 [3.35, 3.89] |  |  |  |  |  |  | 4.68 [4.36, 5.01] | 3.95 [3.63, 4.25] |  | 3.47 [3.19, 3.79] |
| 60 | 2.98 [2.27, 3.75] | 3.78 [2.63, 5.22] |  | 3.90 [2.76, 5.38] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 3.47 [2.53, 4.45] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.17 [0.07, 0.28] | -0.41 [-0.57, -0.27]* | 0.64 [0.42, 0.84] | -0.10 [-0.27, 0.05] | -0.02 [-0.20, 0.15] | 1.45 [1.18, 1.75] |  | 0.96 [0.71, 1.19] | 0.60 [0.38, 0.81] |  |  |  |  | 0.63 [0.45, 0.81] |  | wt_med |
| 1 | 0.15 [0.05, 0.26] | -0.41 [-0.57, -0.27]* | 0.65 [0.42, 0.85] | -0.11 [-0.29, 0.05] | -0.02 [-0.20, 0.15] | 1.53 [1.26, 1.80] | 0.60 [0.40, 0.80] | 1.19 [0.91, 1.50] | 1.22 [0.95, 1.49] | 0.38 [0.19, 0.56] | 0.47 [0.27, 0.66] |  |  | 0.61 [0.43, 0.80] |  | wt_med |
| 2 | 0.13 [0.03, 0.25] | -0.41 [-0.58, -0.27]* | 0.65 [0.43, 0.85] | -0.12 [-0.30, 0.05] | -0.01 [-0.20, 0.15] | 1.70 [1.41, 1.98] | 0.67 [0.46, 0.88] | 1.36 [1.08, 1.65] | 1.55 [1.23, 1.85] | 0.38 [0.17, 0.58] | 0.50 [0.30, 0.69] |  |  | 0.58 [0.39, 0.78] |  | wt_med |
| 3 | 0.13 [0.03, 0.24] | -0.41 [-0.59, -0.26]* | 0.63 [0.41, 0.85] | -0.12 [-0.31, 0.04] | -0.00 [-0.19, 0.16] | 1.98 [1.70, 2.30] | 0.75 [0.53, 0.96] | 1.69 [1.38, 2.01] | 1.42 [1.11, 1.73] | 0.41 [0.20, 0.60] | 0.51 [0.30, 0.70] | 1.20 [0.80, 1.71] | 0.48 [0.20, 0.78] | 0.57 [0.37, 0.76] |  | wt_med |
| 4 | 0.11 [0.01, 0.21] | -0.44 [-0.62, -0.28]* | 0.60 [0.37, 0.81] | -0.14 [-0.34, 0.03] | -0.03 [-0.24, 0.15] | 1.48 [1.19, 1.80] | 0.63 [0.40, 0.85] | 1.56 [1.24, 1.86] | 1.32 [1.02, 1.64] | 0.33 [0.12, 0.53] | 0.47 [0.25, 0.66] | 1.18 [0.78, 1.68] | 0.46 [0.19, 0.75] | 0.54 [0.35, 0.74] |  | wt_med |
| 5 | 0.10 [0.00, 0.20] | -0.42 [-0.59, -0.26]* | 0.59 [0.37, 0.81] | -0.12 [-0.31, 0.04] | 0.01 [-0.19, 0.19] | 1.55 [1.28, 1.85] | 0.65 [0.42, 0.87] | 1.28 [0.97, 1.60] | 1.23 [0.92, 1.56] | 0.36 [0.15, 0.55] | 0.49 [0.28, 0.68] | 1.14 [0.76, 1.63] | 0.43 [0.17, 0.70] | 0.54 [0.35, 0.73] |  | wt_med |
| 6 | 0.06 [-0.04, 0.16] | -0.40 [-0.59, -0.25]* | 0.80 [0.52, 1.08] | -0.11 [-0.31, 0.06] | 0.05 [-0.15, 0.24] | 1.56 [1.27, 1.86] | 0.67 [0.43, 0.90] | 1.40 [1.06, 1.71] | 1.29 [0.97, 1.61] | 0.43 [0.23, 0.64] | 0.54 [0.31, 0.75] | 1.15 [0.78, 1.63] | 0.45 [0.20, 0.72] | 0.48 [0.28, 0.68] |  | wt_med |
| 7 | 0.01 [-0.08, 0.11] | -0.39 [-0.58, -0.22]* | 0.86 [0.56, 1.16] | -0.07 [-0.25, 0.11] | 0.12 [-0.09, 0.31] | 1.50 [1.20, 1.82] | 0.70 [0.45, 0.93] | 1.88 [1.49, 2.25] | 1.67 [1.32, 2.02] | 0.49 [0.28, 0.70] | 0.53 [0.29, 0.75] | 1.12 [0.75, 1.60] | 0.42 [0.16, 0.71] |  |  | wt_med |
| 10 | -0.01 [-0.11, 0.09] | -0.40 [-0.61, -0.23]* | 0.84 [0.54, 1.15] | -0.10 [-0.30, 0.09] | 0.12 [-0.10, 0.31] |  |  |  |  |  |  | 1.10 [0.73, 1.59] | 0.37 [0.12, 0.65] |  |  | wt_med |
| 14 | -0.08 [-0.18, 0.03] | -0.41 [-0.64, -0.23]* | 0.86 [0.52, 1.18] | -0.10 [-0.32, 0.10] | 0.09 [-0.16, 0.31] |  |  |  |  |  |  | 1.02 [0.67, 1.49] | 0.30 [0.04, 0.57] |  |  | wt_med |
| 21 |  | -0.21 [-0.46, 0.00] | 1.06 [0.68, 1.44] | 0.07 [-0.17, 0.29] | 0.31 [0.03, 0.54] |  |  |  |  |  |  | 1.08 [0.73, 1.52] | 0.37 [0.08, 0.68] |  | 0.19 [0.06, 0.32] | clim |
| 30 |  | -0.18 [-0.48, 0.08] | 1.06 [0.60, 1.53] | 0.10 [-0.18, 0.35] | 0.33 [0.02, 0.60] |  |  |  |  |  |  | 1.06 [0.68, 1.50] | 0.30 [-0.01, 0.64] |  | 0.28 [0.12, 0.46] | clim |
| 45 | -0.18 [-0.38, 0.00] | -0.58 [-0.91, -0.31]* | 0.18 [-0.73, 0.90] | -0.23 [-0.58, 0.03] | -0.21 [-0.56, 0.08] |  |  |  |  |  |  | 0.80 [0.46, 1.14] | -0.02 [-0.34, 0.27] |  |  | wt_med |
| 60 | -0.36 [-0.88, 0.13] | -0.29 [-0.77, 0.15] |  | -0.19 [-0.76, 0.36] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  | wt_med |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 30.32 [29.28, 31.36] | 28.44 [27.59, 29.32] | 29.47 [28.36, 30.49] | 28.83 [27.92, 29.67] | 28.51 [27.55, 29.42] | 40.44 [39.35, 41.50] | 28.04 [27.05, 28.93] | 36.87 [35.88, 37.85] | 37.40 [36.34, 38.44] | 27.37 [26.48, 28.24] | 27.30 [26.43, 28.17] |  |  | 34.42 [33.26, 35.70] | 29.46 [28.54, 30.37] |
| 3 | 30.49 [29.43, 31.60] | 28.36 [27.54, 29.29] | 29.48 [28.42, 30.54] | 28.74 [27.81, 29.68] | 27.88 [27.04, 28.84] | 39.84 [38.77, 40.90] | 27.63 [26.71, 28.62] | 39.40 [38.40, 40.54] | 38.01 [37.01, 39.06] | 27.31 [26.44, 28.21] | 27.35 [26.43, 28.26] | 34.93 [33.86, 35.92] | 34.86 [33.73, 35.86] | 33.87 [32.70, 35.12] | 29.85 [28.86, 30.89] |
| 7 | 30.46 [29.37, 31.68] | 29.12 [28.15, 30.13] | 29.47 [28.36, 30.54] | 29.68 [28.64, 30.66] | 28.80 [27.79, 29.71] | 40.65 [39.49, 41.80] | 27.99 [26.84, 29.01] | 41.97 [40.88, 43.07] | 41.48 [40.36, 42.70] | 28.26 [27.20, 29.28] | 28.09 [27.01, 29.12] | 35.72 [34.64, 36.77] | 35.38 [34.26, 36.37] |  | 30.26 [29.25, 31.33] |
| 14 | 30.72 [29.47, 32.00] | 30.01 [28.80, 31.08] | 30.16 [28.84, 31.44] | 30.96 [29.69, 32.10] | 30.17 [28.93, 31.36] |  |  |  |  |  |  | 35.89 [34.71, 37.10] | 36.20 [34.96, 37.39] |  | 31.26 [30.07, 32.41] |
| 30 | 32.04 [30.37, 33.66] | 32.18 [30.70, 33.65] | 31.65 [29.94, 33.40] | 33.97 [32.42, 35.58] | 32.66 [31.21, 34.23] |  |  |  |  |  |  | 40.42 [38.75, 42.13] | 40.23 [38.51, 41.85] |  | 34.37 [32.75, 36.00] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.48 [-0.74, 1.65] | -1.02 [-2.41, 0.11] | -0.65 [-2.11, 0.83] | -0.83 [-2.17, 0.37] | -0.93 [-2.31, 0.32] | 10.91 [8.92, 13.05] | -1.50 [-2.93, -0.25]* | 7.43 [5.63, 9.41] | 7.93 [6.12, 9.79] | -2.16 [-3.62, -0.96]* | -2.25 [-3.72, -0.95]* |  |  | 4.19 [2.71, 5.64] |  | wt_med |
| 3 | 0.32 [-0.89, 1.56] | -1.48 [-2.73, -0.42]* | -0.90 [-2.52, 0.86] | -1.26 [-2.63, -0.17] | -1.86 [-3.28, -0.57]* | 9.98 [7.87, 12.01] | -2.42 [-3.76, -1.15]* | 9.52 [7.52, 11.76] | 8.15 [6.42, 10.02] | -2.73 [-4.18, -1.48]* | -2.70 [-4.11, -1.46]* | 4.89 [3.21, 6.67] | 4.82 [3.28, 6.43] | 3.28 [1.71, 4.82] |  | wt_med |
| 7 | -0.19 [-1.75, 1.33] | -1.16 [-2.39, -0.03] | -1.41 [-3.07, 0.03] | -0.77 [-2.04, 0.36] | -1.27 [-2.81, 0.03] | 10.27 [8.51, 12.31] | -2.45 [-3.80, -1.22]* | 11.68 [9.72, 13.65] | 11.13 [9.39, 12.93] | -2.20 [-3.63, -0.92]* | -2.38 [-3.72, -1.15]* | 5.32 [3.70, 6.93] | 4.97 [3.47, 6.44] |  |  | wt_med |
| 14 | -0.88 [-2.50, 0.67] | -1.27 [-2.85, 0.18] | -1.93 [-3.95, -0.04] | -0.50 [-2.27, 1.18] | -1.15 [-2.92, 0.46] |  |  |  |  |  |  | 4.39 [2.42, 6.29] | 4.70 [3.00, 6.33] |  |  | wt_med |
| 30 |  | 0.40 [-2.97, 3.29] | -0.51 [-3.23, 1.99] | 1.91 [-1.59, 5.24] | 0.69 [-2.54, 3.46] |  |  |  |  |  |  | 8.34 [5.61, 11.17] | 8.14 [5.26, 11.14] |  | 2.63 [0.28, 5.15] | clim |

**dayPeak pairwise ordering (headliners)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.72 [0.71, 0.72] | 0.75 [0.75, 0.75] | 0.72 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.77 [0.77, 0.77] | 0.77 [0.77, 0.78] |  | 0.80 [0.80, 0.80] | 0.80 [0.79, 0.80] |  |  |  |  | 0.73 [0.72, 0.74] | 0.73 [0.72, 0.73] |
| 1 | 0.71 [0.71, 0.72] | 0.75 [0.74, 0.75] | 0.72 [0.72, 0.73] | 0.75 [0.75, 0.76] | 0.77 [0.76, 0.77] | 0.77 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.79 [0.79, 0.79] | 0.79 [0.78, 0.79] | 0.77 [0.77, 0.77] | 0.77 [0.77, 0.78] |  |  | 0.73 [0.72, 0.74] | 0.72 [0.72, 0.73] |
| 2 | 0.71 [0.71, 0.72] | 0.74 [0.74, 0.75] | 0.72 [0.71, 0.73] | 0.75 [0.75, 0.76] | 0.76 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.75] | 0.79 [0.78, 0.79] | 0.78 [0.78, 0.79] | 0.77 [0.76, 0.77] | 0.77 [0.77, 0.77] |  |  | 0.73 [0.72, 0.74] | 0.72 [0.72, 0.73] |
| 3 | 0.71 [0.71, 0.72] | 0.74 [0.74, 0.75] | 0.72 [0.71, 0.72] | 0.75 [0.75, 0.76] | 0.76 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.75 [0.74, 0.75] | 0.78 [0.78, 0.79] | 0.78 [0.78, 0.79] | 0.77 [0.76, 0.77] | 0.77 [0.76, 0.77] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.73 [0.72, 0.74] | 0.72 [0.72, 0.73] |
| 4 | 0.71 [0.71, 0.72] | 0.74 [0.74, 0.75] | 0.72 [0.71, 0.72] | 0.75 [0.75, 0.76] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.75] | 0.78 [0.78, 0.79] | 0.78 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.77 [0.76, 0.77] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.75] | 0.73 [0.72, 0.74] | 0.72 [0.71, 0.72] |
| 5 | 0.71 [0.71, 0.72] | 0.74 [0.74, 0.75] | 0.72 [0.71, 0.72] | 0.75 [0.75, 0.76] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.77] | 0.75 [0.74, 0.75] | 0.78 [0.78, 0.78] | 0.78 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.71 [0.70, 0.71] | 0.74 [0.73, 0.74] | 0.73 [0.72, 0.74] | 0.72 [0.71, 0.72] |
| 6 | 0.71 [0.70, 0.72] | 0.74 [0.73, 0.74] | 0.71 [0.70, 0.72] | 0.75 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.74 [0.74, 0.75] | 0.78 [0.77, 0.78] | 0.78 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.76 [0.76, 0.77] | 0.70 [0.70, 0.71] | 0.73 [0.73, 0.74] | 0.73 [0.72, 0.74] | 0.72 [0.71, 0.72] |
| 7 | 0.71 [0.70, 0.72] | 0.74 [0.73, 0.74] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.75] | 0.75 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.74 [0.74, 0.75] | 0.77 [0.77, 0.78] | 0.77 [0.77, 0.77] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.70 [0.69, 0.70] | 0.73 [0.73, 0.74] |  | 0.71 [0.71, 0.72] |
| 10 | 0.71 [0.70, 0.71] | 0.73 [0.72, 0.73] | 0.71 [0.70, 0.72] | 0.74 [0.73, 0.74] | 0.75 [0.74, 0.75] |  |  |  |  |  |  | 0.69 [0.69, 0.70] | 0.72 [0.72, 0.73] |  | 0.71 [0.70, 0.71] |
| 14 | 0.71 [0.70, 0.71] | 0.72 [0.72, 0.73] | 0.71 [0.70, 0.72] | 0.73 [0.72, 0.74] | 0.74 [0.74, 0.75] |  |  |  |  |  |  | 0.69 [0.68, 0.69] | 0.72 [0.71, 0.72] |  | 0.70 [0.69, 0.71] |
| 21 | 0.71 [0.70, 0.71] | 0.71 [0.71, 0.72] | 0.71 [0.70, 0.72] | 0.72 [0.71, 0.72] | 0.73 [0.72, 0.74] |  |  |  |  |  |  | 0.68 [0.68, 0.69] | 0.71 [0.71, 0.72] |  | 0.69 [0.68, 0.70] |
| 30 | 0.70 [0.69, 0.71] | 0.71 [0.70, 0.71] | 0.71 [0.70, 0.72] | 0.71 [0.70, 0.71] | 0.72 [0.71, 0.73] |  |  |  |  |  |  | 0.68 [0.67, 0.69] | 0.71 [0.70, 0.72] |  | 0.68 [0.67, 0.69] |
| 45 | 0.71 [0.70, 0.72] | 0.71 [0.70, 0.72] | 0.72 [0.70, 0.74] | 0.71 [0.70, 0.72] | 0.73 [0.72, 0.74] |  |  |  |  |  |  | 0.68 [0.67, 0.70] | 0.71 [0.70, 0.72] |  | 0.69 [0.68, 0.70] |
| 60 | 0.74 [0.69, 0.78] | 0.73 [0.66, 0.78] |  | 0.73 [0.67, 0.78] |  |  |  |  |  |  |  |  |  |  | 0.69 [0.62, 0.75] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.01 [-0.03, 0.00] | 0.03 [0.02, 0.04]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* |  | 0.08 [0.07, 0.09]* | 0.07 [0.06, 0.08]* |  |  |  |  | -0.00 [-0.02, 0.01] |  | wt_med |
| 1 | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.01 [-0.03, 0.01] | 0.03 [0.03, 0.04]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | 0.03 [0.02, 0.04]* | 0.07 [0.06, 0.08]* | 0.07 [0.06, 0.08]* | 0.05 [0.04, 0.06]* | 0.05 [0.05, 0.06]* |  |  | -0.00 [-0.01, 0.01] |  | wt_med |
| 2 | -0.01 [-0.02, -0.00] | 0.02 [0.02, 0.03]* | -0.01 [-0.02, 0.01] | 0.04 [0.03, 0.04]* | 0.05 [0.04, 0.05]* | 0.05 [0.04, 0.06]* | 0.03 [0.02, 0.04]* | 0.07 [0.06, 0.08]* | 0.06 [0.05, 0.08]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* |  |  | 0.00 [-0.01, 0.01] |  | wt_med |
| 3 | -0.01 [-0.02, -0.00] | 0.02 [0.02, 0.03]* | -0.01 [-0.02, 0.01] | 0.04 [0.03, 0.04]* | 0.04 [0.04, 0.05]* | 0.05 [0.04, 0.06]* | 0.03 [0.02, 0.04]* | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.07]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.03]* | 0.00 [-0.01, 0.02] |  | wt_med |
| 4 | -0.01 [-0.02, 0.00] | 0.03 [0.02, 0.03]* | -0.01 [-0.02, 0.01] | 0.04 [0.03, 0.04]* | 0.05 [0.04, 0.05]* | 0.05 [0.04, 0.06]* | 0.03 [0.03, 0.04]* | 0.07 [0.06, 0.08]* | 0.06 [0.05, 0.07]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, 0.00] | 0.03 [0.01, 0.04]* | 0.00 [-0.01, 0.02] |  | wt_med |
| 5 | -0.01 [-0.02, 0.00] | 0.03 [0.02, 0.03]* | -0.01 [-0.02, 0.01] | 0.04 [0.03, 0.04]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | 0.03 [0.02, 0.04]* | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.07]* | 0.05 [0.04, 0.05]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.03]* | 0.00 [-0.01, 0.02] |  | wt_med |
| 6 | -0.01 [-0.02, 0.00] | 0.03 [0.02, 0.03]* | -0.01 [-0.03, 0.01] | 0.04 [0.03, 0.05]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | 0.03 [0.02, 0.04]* | 0.07 [0.06, 0.08]* | 0.06 [0.05, 0.08]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.03]* | 0.01 [-0.01, 0.02] |  | wt_med |
| 7 | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.03]* | -0.01 [-0.03, 0.01] | 0.03 [0.02, 0.04]* | 0.05 [0.04, 0.05]* | 0.05 [0.04, 0.06]* | 0.03 [0.02, 0.04]* | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.07]* | 0.05 [0.04, 0.06]* | 0.05 [0.04, 0.06]* | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.03]* |  |  | wt_med |
| 10 | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.03]* | -0.00 [-0.02, 0.02] | 0.04 [0.03, 0.04]* | 0.04 [0.04, 0.05]* |  |  |  |  |  |  | -0.01 [-0.02, 0.00] | 0.02 [0.01, 0.03]* |  |  | wt_med |
| 14 | 0.00 [-0.01, 0.02] | 0.03 [0.02, 0.03]* | 0.00 [-0.02, 0.02] | 0.03 [0.02, 0.04]* | 0.05 [0.04, 0.06]* |  |  |  |  |  |  | -0.01 [-0.02, 0.01] | 0.03 [0.01, 0.04]* |  |  | wt_med |
| 21 |  | 0.01 [-0.00, 0.02] | -0.00 [-0.02, 0.02] | 0.01 [0.00, 0.03]* | 0.03 [0.01, 0.04]* |  |  |  |  |  |  | -0.02 [-0.03, -0.00] | 0.01 [-0.00, 0.03] |  | -0.01 [-0.02, -0.00] | clim |
| 30 |  | 0.01 [-0.01, 0.02] | -0.00 [-0.03, 0.02] | 0.01 [-0.01, 0.02] | 0.02 [0.00, 0.04]* |  |  |  |  |  |  | -0.02 [-0.03, -0.00] | 0.01 [-0.00, 0.03] |  | -0.02 [-0.03, -0.01] | clim |
| 45 |  | 0.01 [-0.01, 0.02] | -0.00 [-0.03, 0.02] | 0.01 [-0.01, 0.02] | 0.03 [0.01, 0.05]* |  |  |  |  |  |  | -0.01 [-0.03, 0.01] | 0.02 [-0.00, 0.03] |  | -0.02 [-0.03, -0.00] | clim |
| 60 | 0.04 [-0.00, 0.09] | 0.04 [0.02, 0.05] |  | 0.03 [0.01, 0.06] |  |  |  |  |  |  |  |  |  |  |  | wt_med |

## D5 first-hour MAE vs EACH naive candidate

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.296 [-0.569, -0.024]* / [-0.408, -0.180]* | -0.169 [-0.416, 0.078] / [-0.295, -0.040]*  ! | -0.685 [-1.089, -0.409]* / [-0.797, -0.577]* | -0.686 [-1.089, -0.410]* / [-0.798, -0.578]* | -0.689 [-1.103, -0.408]* / [-0.802, -0.578]* | -0.693 [-1.124, -0.392]* / [-0.808, -0.577]* |  |  |
| 1 | -0.220 [-0.498, 0.062] / [-0.337, -0.097]*  ! | -0.132 [-0.391, 0.127] / [-0.258, 0.006] | -0.628 [-1.033, -0.351]* / [-0.743, -0.516]* | -0.630 [-1.036, -0.351]* / [-0.745, -0.520]* | -0.629 [-1.047, -0.347]* / [-0.745, -0.514]* | -0.622 [-1.059, -0.314]* / [-0.744, -0.503]* |  |  |
| 2 | -0.174 [-0.453, 0.123] / [-0.298, -0.050]*  ! | -0.103 [-0.366, 0.164] / [-0.232, 0.033] | -0.591 [-0.988, -0.315]* / [-0.706, -0.477]* | -0.592 [-0.987, -0.314]* / [-0.706, -0.481]* | -0.591 [-0.998, -0.308]* / [-0.711, -0.477]* | -0.597 [-1.032, -0.299]* / [-0.723, -0.475]* |  |  |
| 3 | -0.134 [-0.406, 0.171] / [-0.261, -0.013]*  ! | -0.090 [-0.355, 0.173] / [-0.218, 0.047] | -0.560 [-0.947, -0.280]* / [-0.680, -0.445]* | -0.562 [-0.948, -0.282]* / [-0.680, -0.449]* | -0.559 [-0.961, -0.274]* / [-0.680, -0.445]* | -0.555 [-0.980, -0.263]* / [-0.681, -0.431]* | 1.313 [1.009, 1.640] / [1.193, 1.437] | 1.469 [1.173, 1.807] / [1.348, 1.594] |
| 4 | -0.110 [-0.385, 0.203] / [-0.239, 0.017] | -0.098 [-0.366, 0.170] / [-0.231, 0.041] | -0.537 [-0.919, -0.261]* / [-0.658, -0.420]* | -0.541 [-0.924, -0.265]* / [-0.664, -0.426]* | -0.531 [-0.926, -0.253]* / [-0.652, -0.404]* | -0.551 [-0.957, -0.262]* / [-0.683, -0.426]* | 1.344 [1.060, 1.677] / [1.219, 1.476] | 1.510 [1.218, 1.843] / [1.384, 1.646] |
| 5 | -0.077 [-0.359, 0.242] / [-0.207, 0.057] | -0.089 [-0.359, 0.180] / [-0.225, 0.053] | -0.501 [-0.897, -0.221]* / [-0.625, -0.379]* | -0.500 [-0.902, -0.224]* / [-0.624, -0.381]* | -0.512 [-0.915, -0.228]* / [-0.636, -0.388]* | -0.520 [-0.927, -0.233]* / [-0.646, -0.395]* | 1.376 [1.091, 1.715] / [1.252, 1.510] | 1.541 [1.252, 1.899] / [1.408, 1.676] |
| 6 | -0.016 [-0.304, 0.313] / [-0.150, 0.119] | -0.069 [-0.342, 0.194] / [-0.209, 0.078] | -0.431 [-0.845, -0.142]* / [-0.558, -0.304]* | -0.423 [-0.831, -0.121]* / [-0.553, -0.295]* | -0.457 [-0.864, -0.175]* / [-0.579, -0.332]* | -0.453 [-0.884, -0.152]* / [-0.589, -0.321]* | 1.379 [1.086, 1.706] / [1.248, 1.512] | 1.545 [1.254, 1.888] / [1.409, 1.680] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.296 [0.024, 0.569] / [0.180, 0.408] | 0.082 [-0.089, 0.235] / [0.023, 0.141] | -0.381 [-0.701, -0.149]* / [-0.443, -0.325]* | -0.382 [-0.702, -0.149]* / [-0.445, -0.327]* | -0.398 [-0.723, -0.159]* / [-0.463, -0.341]* | -0.299 [-0.625, -0.074]* / [-0.366, -0.240]* |  |  |
| 1 | 0.220 [-0.062, 0.498] / [0.097, 0.337] | 0.046 [-0.137, 0.209] / [-0.014, 0.106] | -0.402 [-0.744, -0.157]* / [-0.467, -0.346]* | -0.404 [-0.740, -0.157]* / [-0.470, -0.347]* | -0.417 [-0.759, -0.167]* / [-0.484, -0.360]* | -0.333 [-0.686, -0.087]* / [-0.407, -0.271]* |  |  |
| 2 | 0.174 [-0.123, 0.453] / [0.050, 0.298] | 0.031 [-0.160, 0.202] / [-0.028, 0.093] | -0.414 [-0.768, -0.161]* / [-0.480, -0.354]* | -0.415 [-0.767, -0.160]* / [-0.482, -0.356]* | -0.429 [-0.789, -0.166]* / [-0.500, -0.368]* | -0.345 [-0.710, -0.091]* / [-0.424, -0.281]* |  |  |
| 3 | 0.134 [-0.171, 0.406] / [0.013, 0.261] | 0.003 [-0.190, 0.176] / [-0.055, 0.067] | -0.426 [-0.793, -0.169]* / [-0.497, -0.365]* | -0.427 [-0.794, -0.167]* / [-0.499, -0.367]* | -0.436 [-0.808, -0.170]* / [-0.510, -0.376]* | -0.369 [-0.753, -0.110]* / [-0.447, -0.303]* | 1.477 [1.191, 1.748] / [1.378, 1.575] | 1.639 [1.345, 1.932] / [1.532, 1.743] |
| 4 | 0.110 [-0.203, 0.385] / [-0.017, 0.239] | -0.025 [-0.225, 0.156] / [-0.085, 0.040] | -0.430 [-0.793, -0.175]* / [-0.503, -0.368]* | -0.433 [-0.795, -0.175]* / [-0.508, -0.371]* | -0.435 [-0.796, -0.175]* / [-0.509, -0.372]* | -0.378 [-0.762, -0.115]* / [-0.460, -0.311]* | 1.500 [1.212, 1.776] / [1.399, 1.601] | 1.669 [1.385, 1.961] / [1.565, 1.777] |
| 5 | 0.077 [-0.242, 0.359] / [-0.057, 0.207] | -0.046 [-0.253, 0.141] / [-0.107, 0.020] | -0.429 [-0.778, -0.172]* / [-0.498, -0.366]* | -0.429 [-0.776, -0.176]* / [-0.500, -0.365]* | -0.442 [-0.797, -0.181]* / [-0.516, -0.381]* | -0.374 [-0.744, -0.118]* / [-0.458, -0.304]* | 1.486 [1.222, 1.741] / [1.383, 1.591] | 1.654 [1.382, 1.933] / [1.546, 1.765] |
| 6 | 0.016 [-0.313, 0.304] / [-0.119, 0.150] | -0.083 [-0.303, 0.114] / [-0.146, -0.016]*  ! | -0.425 [-0.754, -0.178]* / [-0.493, -0.365]* | -0.419 [-0.736, -0.182]* / [-0.490, -0.356]* | -0.451 [-0.808, -0.189]* / [-0.525, -0.390]* | -0.370 [-0.723, -0.119]* / [-0.452, -0.302]* | 1.419 [1.159, 1.687] / [1.315, 1.527] | 1.588 [1.311, 1.877] / [1.479, 1.697] |
| 7 |  | -0.144 [-0.380, 0.060] / [-0.208, -0.079]*  ! | -0.430 [-0.760, -0.187]* / [-0.500, -0.368]* | -0.400 [-0.703, -0.178]* / [-0.470, -0.334]* | -0.466 [-0.819, -0.204]* / [-0.540, -0.403]* | -0.368 [-0.696, -0.121]* / [-0.454, -0.302]* | 1.483 [1.218, 1.752] / [1.381, 1.581] | 1.656 [1.384, 1.944] / [1.544, 1.762] |
| 10 |  | -0.244 [-0.513, -0.012]* / [-0.313, -0.176]* | -0.419 [-0.727, -0.193]* / [-0.485, -0.360]* | -0.362 [-0.663, -0.118]* / [-0.433, -0.297]* | -0.434 [-0.754, -0.202]* / [-0.514, -0.369]* | -0.348 [-0.647, -0.133]* / [-0.431, -0.273]* | 1.492 [1.207, 1.786] / [1.383, 1.604] | 1.669 [1.373, 1.992] / [1.554, 1.780] |
| 14 |  | -0.365 [-0.673, -0.105]* / [-0.437, -0.291]* | -0.451 [-0.774, -0.208]* / [-0.528, -0.386]* | -0.371 [-0.702, -0.086]* / [-0.449, -0.300]* | -0.445 [-0.767, -0.201]* / [-0.530, -0.373]* | -0.370 [-0.676, -0.143]* / [-0.461, -0.293]* | 1.599 [1.292, 1.931] / [1.483, 1.713] | 1.797 [1.478, 2.132] / [1.676, 1.914] |
| 21 |  | -0.539 [-0.923, -0.217]* / [-0.622, -0.456]* | -0.512 [-0.897, -0.234]* / [-0.595, -0.435]* | -0.412 [-0.794, -0.091]* / [-0.505, -0.325]* | -0.483 [-0.857, -0.209]* / [-0.585, -0.397]* | -0.451 [-0.829, -0.178]* / [-0.566, -0.351]* | 1.713 [1.445, 2.044] / [1.567, 1.838] | 1.966 [1.655, 2.332] / [1.809, 2.099] |
| 30 |  | -0.607 [-1.064, -0.232]* / [-0.708, -0.509]* | -0.508 [-0.909, -0.209]* / [-0.608, -0.421]* | -0.367 [-0.791, -0.037]* / [-0.474, -0.268]* | -0.405 [-0.809, -0.095]* / [-0.531, -0.292]* | -0.612 [-1.153, -0.247]* / [-0.751, -0.492]* | 1.975 [1.651, 2.360] / [1.806, 2.145] | 2.252 [1.866, 2.702] / [2.081, 2.437] |
| 45 |  | -0.439 [-0.792, -0.145]* / [-0.560, -0.315]* | -0.333 [-0.628, -0.127]* / [-0.444, -0.241]* | -0.249 [-0.529, -0.054]* / [-0.360, -0.151]* | -0.254 [-0.544, -0.039]* / [-0.382, -0.139]* | -0.955 [-2.349, -0.168]* / [-1.380, -0.604]* | 2.359 [1.950, 2.766] / [2.133, 2.618] | 2.637 [2.194, 3.083] / [2.406, 2.904] |
| 60 |  | -0.291 [-0.838, 0.161] / [-0.792, 0.208] | -0.091 [-0.204, -0.010]* / [-0.215, -0.011]* | -0.008 [-0.146, 0.159] / [-0.144, 0.161] | 0.000 [0.000, 0.000] / [0.000, 0.000] |  | 0.312 [0.000, 5.000] / [0.000, 5.000] | 0.312 [0.000, 5.000] / [0.000, 5.000] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.169 [-0.078, 0.416] / [0.040, 0.295] | -0.082 [-0.235, 0.089] / [-0.141, -0.023]*  ! | -0.469 [-0.770, -0.233]* / [-0.541, -0.404]* | -0.469 [-0.768, -0.233]* / [-0.541, -0.404]* | -0.490 [-0.792, -0.250]* / [-0.563, -0.425]* | -0.447 [-0.776, -0.199]* / [-0.528, -0.377]* |  |  |
| 1 | 0.132 [-0.127, 0.391] / [-0.006, 0.258] | -0.046 [-0.209, 0.137] / [-0.106, 0.014] | -0.453 [-0.753, -0.210]* / [-0.527, -0.388]* | -0.454 [-0.758, -0.210]* / [-0.530, -0.389]* | -0.471 [-0.788, -0.223]* / [-0.546, -0.405]* | -0.424 [-0.763, -0.166]* / [-0.504, -0.349]* |  |  |
| 2 | 0.103 [-0.164, 0.366] / [-0.033, 0.232] | -0.031 [-0.202, 0.160] / [-0.093, 0.028] | -0.447 [-0.765, -0.196]* / [-0.527, -0.382]* | -0.449 [-0.766, -0.197]* / [-0.529, -0.382]* | -0.463 [-0.786, -0.209]* / [-0.545, -0.395]* | -0.420 [-0.772, -0.148]* / [-0.504, -0.343]* |  |  |
| 3 | 0.090 [-0.173, 0.355] / [-0.047, 0.218] | -0.003 [-0.176, 0.190] / [-0.067, 0.055] | -0.432 [-0.754, -0.174]* / [-0.512, -0.364]* | -0.433 [-0.757, -0.173]* / [-0.514, -0.365]* | -0.443 [-0.772, -0.181]* / [-0.523, -0.374]* | -0.399 [-0.743, -0.122]* / [-0.483, -0.321]* | 1.448 [1.196, 1.720] / [1.343, 1.550] | 1.610 [1.342, 1.904] / [1.496, 1.719] |
| 4 | 0.098 [-0.170, 0.366] / [-0.041, 0.231] | 0.025 [-0.156, 0.225] / [-0.040, 0.085] | -0.407 [-0.727, -0.148]* / [-0.488, -0.337]* | -0.411 [-0.727, -0.152]* / [-0.493, -0.337]* | -0.415 [-0.740, -0.154]* / [-0.501, -0.340]* | -0.389 [-0.727, -0.112]* / [-0.478, -0.312]* | 1.498 [1.239, 1.780] / [1.393, 1.604] | 1.666 [1.391, 1.954] / [1.557, 1.779] |
| 5 | 0.089 [-0.180, 0.359] / [-0.053, 0.225] | 0.046 [-0.141, 0.253] / [-0.020, 0.107] | -0.384 [-0.702, -0.126]* / [-0.463, -0.314]* | -0.385 [-0.699, -0.128]* / [-0.463, -0.313]* | -0.401 [-0.728, -0.136]* / [-0.483, -0.329]* | -0.362 [-0.680, -0.090]* / [-0.450, -0.281]* | 1.504 [1.250, 1.779] / [1.393, 1.610] | 1.671 [1.407, 1.961] / [1.551, 1.783] |
| 6 | 0.069 [-0.194, 0.342] / [-0.078, 0.209] | 0.083 [-0.114, 0.303] / [0.016, 0.146] | -0.343 [-0.656, -0.086]* / [-0.423, -0.270]* | -0.338 [-0.644, -0.088]* / [-0.419, -0.261]* | -0.370 [-0.696, -0.103]* / [-0.452, -0.298]* | -0.334 [-0.652, -0.065]* / [-0.424, -0.250]* | 1.482 [1.244, 1.748] / [1.376, 1.591] | 1.650 [1.399, 1.941] / [1.538, 1.762] |
| 7 |  | 0.144 [-0.060, 0.380] / [0.079, 0.208] | -0.287 [-0.609, -0.032]* / [-0.370, -0.213]* | -0.257 [-0.566, -0.005]* / [-0.345, -0.178]* | -0.321 [-0.654, -0.050]* / [-0.404, -0.250]* | -0.266 [-0.595, 0.009] / [-0.363, -0.181]*  ! | 1.610 [1.342, 1.901] / [1.491, 1.718] | 1.782 [1.501, 2.090] / [1.655, 1.892] |
| 10 |  | 0.244 [0.012, 0.513] / [0.176, 0.313] | -0.171 [-0.508, 0.109] / [-0.249, -0.092]*  ! | -0.115 [-0.471, 0.215] / [-0.208, -0.027]*  ! | -0.185 [-0.533, 0.109] / [-0.267, -0.104]*  ! | -0.136 [-0.481, 0.161] / [-0.231, -0.047]*  ! | 1.722 [1.437, 2.057] / [1.604, 1.837] | 1.898 [1.594, 2.239] / [1.772, 2.018] |
| 14 |  | 0.365 [0.105, 0.673] / [0.291, 0.437] | -0.081 [-0.460, 0.246] / [-0.168, 0.006] | 0.001 [-0.405, 0.437] / [-0.102, 0.102] | -0.073 [-0.461, 0.275] / [-0.171, 0.020] | -0.040 [-0.441, 0.315] / [-0.144, 0.058] | 1.961 [1.628, 2.370] / [1.829, 2.089] | 2.158 [1.806, 2.574] / [2.024, 2.290] |
| 21 |  | 0.539 [0.217, 0.923] / [0.456, 0.622] | 0.035 [-0.417, 0.443] / [-0.060, 0.138] | 0.136 [-0.355, 0.673] / [0.024, 0.254] | 0.068 [-0.404, 0.507] / [-0.045, 0.177] | 0.055 [-0.447, 0.489] / [-0.068, 0.175] | 2.260 [1.841, 2.781] / [2.105, 2.411] | 2.511 [2.068, 3.037] / [2.345, 2.668] |
| 30 |  | 0.607 [0.232, 1.064] / [0.509, 0.708] | 0.107 [-0.408, 0.565] / [-0.005, 0.231] | 0.249 [-0.335, 0.858] / [0.112, 0.394] | 0.216 [-0.340, 0.777] / [0.079, 0.359] | 0.028 [-0.535, 0.487] / [-0.115, 0.163] | 2.598 [2.094, 3.245] / [2.418, 2.789] | 2.872 [2.337, 3.507] / [2.679, 3.070] |
| 45 |  | 0.439 [0.145, 0.792] / [0.315, 0.560] | 0.102 [-0.211, 0.414] / [-0.029, 0.248] | 0.186 [-0.162, 0.551] / [0.040, 0.342] | 0.174 [-0.185, 0.584] / [0.012, 0.343] | -1.378 [-2.351, -0.588]* / [-1.756, -1.050]* | 2.770 [2.280, 3.317] / [2.527, 3.055] | 3.048 [2.554, 3.579] / [2.796, 3.348] |
| 60 |  | 0.291 [-0.161, 0.838] / [-0.208, 0.792] | 0.232 [-0.237, 0.740] / [-0.257, 0.691] | 0.315 [-0.207, 0.874] / [-0.203, 0.851] | 0.312 [0.000, 5.000] / [0.000, 5.000] |  | 0.625 [0.000, 10.000] / [0.000, 10.000] | 0.625 [0.000, 10.000] / [0.000, 10.000] |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.59 [4.46, 4.74] | 4.20 [4.09, 4.33] | 4.29 [4.17, 4.42] | 4.20 [4.08, 4.33] | 4.21 [4.10, 4.34] | 7.55 [7.38, 7.73] |  | 6.35 [6.21, 6.49] | 5.80 [5.67, 5.93] |  |  | 4.18 [4.07, 4.30] | 4.78 [4.64, 4.93] |  |  | 4.86 [4.71, 5.02] | 4.55 [4.42, 4.69] |
| 1 | 4.64 [4.50, 4.79] | 4.27 [4.14, 4.40] | 4.36 [4.24, 4.50] | 4.27 [4.14, 4.39] | 4.28 [4.15, 4.41] | 7.78 [7.60, 7.96] | 4.25 [4.13, 4.38] | 6.62 [6.47, 6.77] | 6.58 [6.43, 6.74] | 4.25 [4.13, 4.38] | 4.25 [4.13, 4.38] | 4.24 [4.12, 4.37] | 4.81 [4.66, 4.96] |  |  | 4.87 [4.72, 5.03] | 4.63 [4.50, 4.77] |
| 2 | 4.67 [4.53, 4.83] | 4.30 [4.18, 4.43] | 4.38 [4.25, 4.52] | 4.30 [4.18, 4.43] | 4.31 [4.18, 4.44] | 7.65 [7.48, 7.83] | 4.28 [4.16, 4.42] | 7.25 [7.08, 7.43] | 7.29 [7.12, 7.46] | 4.28 [4.16, 4.41] | 4.28 [4.16, 4.41] | 4.27 [4.15, 4.40] | 4.84 [4.69, 4.99] |  |  | 4.87 [4.71, 5.04] | 4.68 [4.54, 4.82] |
| 3 | 4.67 [4.52, 4.82] | 4.30 [4.18, 4.43] | 4.37 [4.24, 4.51] | 4.30 [4.18, 4.43] | 4.31 [4.19, 4.44] | 7.73 [7.55, 7.92] | 4.29 [4.17, 4.41] | 7.67 [7.50, 7.85] | 7.23 [7.05, 7.41] | 4.29 [4.16, 4.41] | 4.29 [4.16, 4.41] | 4.28 [4.15, 4.40] | 4.85 [4.70, 5.00] | 6.19 [6.05, 6.36] | 6.35 [6.21, 6.52] | 4.85 [4.69, 5.02] | 4.70 [4.55, 4.84] |
| 4 | 4.66 [4.50, 4.81] | 4.32 [4.20, 4.45] | 4.37 [4.23, 4.51] | 4.32 [4.19, 4.45] | 4.33 [4.21, 4.47] | 7.69 [7.51, 7.87] | 4.30 [4.18, 4.43] | 7.70 [7.52, 7.88] | 7.27 [7.10, 7.45] | 4.30 [4.18, 4.43] | 4.30 [4.18, 4.43] | 4.28 [4.16, 4.41] | 4.85 [4.71, 5.01] | 6.22 [6.07, 6.39] | 6.39 [6.23, 6.56] | 4.84 [4.69, 5.01] | 4.72 [4.57, 4.86] |
| 5 | 4.66 [4.51, 4.82] | 4.35 [4.23, 4.49] | 4.41 [4.27, 4.55] | 4.35 [4.23, 4.49] | 4.35 [4.23, 4.49] | 7.78 [7.60, 7.98] | 4.33 [4.20, 4.46] | 7.28 [7.10, 7.46] | 7.26 [7.09, 7.44] | 4.33 [4.20, 4.46] | 4.33 [4.21, 4.46] | 4.32 [4.19, 4.45] | 4.87 [4.72, 5.03] | 6.24 [6.08, 6.41] | 6.41 [6.24, 6.57] | 4.84 [4.68, 5.01] | 4.75 [4.60, 4.90] |
| 6 | 4.68 [4.52, 4.84] | 4.42 [4.28, 4.56] | 4.47 [4.33, 4.62] | 4.42 [4.29, 4.57] | 4.40 [4.27, 4.54] | 7.97 [7.78, 8.16] | 4.38 [4.25, 4.52] | 7.61 [7.42, 7.80] | 7.56 [7.37, 7.75] | 4.39 [4.26, 4.53] | 4.39 [4.26, 4.53] | 4.36 [4.23, 4.50] | 4.89 [4.73, 5.04] | 6.24 [6.08, 6.40] | 6.40 [6.25, 6.56] | 4.83 [4.67, 5.01] | 4.80 [4.66, 4.96] |
| 7 | 4.70 [4.55, 4.86] | 4.50 [4.36, 4.65] | 4.57 [4.43, 4.73] | 4.53 [4.38, 4.68] | 4.48 [4.34, 4.63] | 7.81 [7.62, 7.99] | 4.45 [4.31, 4.59] | 8.18 [7.98, 8.38] | 7.86 [7.66, 8.04] | 4.47 [4.33, 4.61] | 4.47 [4.33, 4.62] | 4.44 [4.30, 4.58] | 5.49 [5.31, 5.67] | 6.39 [6.22, 6.55] | 6.56 [6.38, 6.72] |  | 4.89 [4.74, 5.05] |
| 10 | 4.71 [4.55, 4.88] | 4.61 [4.46, 4.76] | 4.67 [4.51, 4.84] | 4.66 [4.50, 4.82] | 4.60 [4.45, 4.75] |  |  |  |  |  |  | 4.52 [4.38, 4.67] | 5.59 [5.39, 5.77] | 6.50 [6.32, 6.68] | 6.67 [6.50, 6.85] |  | 4.98 [4.82, 5.16] |
| 14 | 4.72 [4.55, 4.90] | 4.70 [4.55, 4.87] | 4.78 [4.61, 4.96] | 4.78 [4.61, 4.96] | 4.72 [4.56, 4.90] |  |  |  |  |  |  | 4.62 [4.46, 4.78] | 6.10 [5.88, 6.32] | 6.75 [6.56, 6.96] | 6.94 [6.75, 7.16] |  | 5.12 [4.95, 5.32] |
| 21 | 4.73 [4.54, 4.93] | 4.80 [4.62, 4.98] | 4.85 [4.65, 5.06] | 4.90 [4.70, 5.09] | 4.82 [4.63, 5.01] |  |  |  |  |  |  | 4.69 [4.50, 4.86] | 6.63 [6.38, 6.87] | 7.05 [6.82, 7.29] | 7.30 [7.07, 7.54] |  | 5.28 [5.09, 5.48] |
| 30 | 4.86 [4.67, 5.09] | 5.00 [4.79, 5.22] | 4.98 [4.76, 5.21] | 5.14 [4.90, 5.38] | 5.09 [4.85, 5.32] |  |  |  |  |  |  | 4.81 [4.60, 5.03] | 7.13 [6.85, 7.43] | 7.52 [7.26, 7.81] | 7.79 [7.52, 8.08] |  | 5.48 [5.25, 5.73] |
| 45 | 4.82 [4.53, 5.14] | 5.08 [4.78, 5.39] | 4.42 [4.00, 4.88] | 5.16 [4.84, 5.49] | 5.16 [4.84, 5.50] |  |  |  |  |  |  | 4.97 [4.68, 5.29] | 7.09 [6.74, 7.47] | 7.71 [7.33, 8.10] | 7.98 [7.59, 8.38] |  | 5.38 [5.07, 5.70] |
| 60 | 4.42 [3.26, 5.89] | 4.77 [3.51, 6.52] |  | 4.85 [3.53, 6.63] | 0.47 [0.00, 7.50] |  |  |  |  |  |  | 4.35 [3.31, 5.77] | 8.13 [6.47, 10.43] | 0.78 [0.00, 12.50] | 0.78 [0.00, 12.50] |  | 4.78 [3.52, 6.46] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.08 [-0.09, 0.23] | -0.38 [-0.70, -0.15]* | -0.30 [-0.62, -0.07]* | -0.38 [-0.70, -0.15]* | -0.40 [-0.72, -0.16]* | 2.95 [2.44, 3.44] |  | 1.75 [1.40, 2.13] | 1.20 [0.90, 1.50] |  |  | -0.39 [-0.71, -0.16]* | 0.21 [-0.26, 0.59] |  |  | 0.30 [0.02, 0.57] |  | wt_med |
| 1 | -0.13 [-0.39, 0.13] | -0.63 [-1.03, -0.35]* | -0.62 [-1.06, -0.31]* | -0.63 [-1.04, -0.35]* | -0.63 [-1.05, -0.35]* | 2.91 [2.35, 3.49] | -0.63 [-1.04, -0.35]* | 1.75 [1.29, 2.18] | 1.72 [1.26, 2.15] | -0.63 [-1.04, -0.35]* | -0.63 [-1.04, -0.35]* | -0.64 [-1.05, -0.36]* | -0.19 [-0.78, 0.29] |  |  |  | -0.22 [-0.50, 0.06] | snaive7 |
| 2 | -0.10 [-0.37, 0.16] | -0.59 [-0.99, -0.31]* | -0.60 [-1.03, -0.30]* | -0.59 [-0.99, -0.31]* | -0.59 [-1.00, -0.31]* | 2.79 [2.29, 3.32] | -0.59 [-0.99, -0.32]* | 2.42 [1.87, 2.97] | 2.47 [1.88, 3.07] | -0.60 [-0.99, -0.32]* | -0.60 [-0.99, -0.32]* | -0.61 [-1.01, -0.33]* | -0.16 [-0.75, 0.32] |  |  |  | -0.17 [-0.45, 0.12] | snaive7 |
| 3 | -0.09 [-0.35, 0.17] | -0.56 [-0.95, -0.28]* | -0.56 [-0.98, -0.26]* | -0.56 [-0.95, -0.28]* | -0.56 [-0.96, -0.27]* | 2.92 [2.38, 3.50] | -0.56 [-0.95, -0.28]* | 2.89 [2.24, 3.54] | 2.42 [1.81, 3.01] | -0.57 [-0.95, -0.29]* | -0.57 [-0.95, -0.29]* | -0.58 [-0.97, -0.30]* | -0.14 [-0.73, 0.34] | 1.31 [1.01, 1.64] | 1.47 [1.17, 1.81] |  | -0.13 [-0.41, 0.17] | snaive7 |
| 4 | -0.10 [-0.37, 0.17] | -0.54 [-0.92, -0.26]* | -0.55 [-0.96, -0.26]* | -0.54 [-0.92, -0.26]* | -0.53 [-0.93, -0.25]* | 2.88 [2.31, 3.47] | -0.55 [-0.93, -0.27]* | 2.92 [2.21, 3.63] | 2.46 [1.86, 3.05] | -0.55 [-0.93, -0.27]* | -0.55 [-0.93, -0.27]* | -0.57 [-0.96, -0.29]* | -0.13 [-0.72, 0.36] | 1.34 [1.06, 1.68] | 1.51 [1.22, 1.84] |  | -0.11 [-0.39, 0.20] | snaive7 |
| 5 | -0.09 [-0.36, 0.18] | -0.50 [-0.90, -0.22]* | -0.52 [-0.93, -0.23]* | -0.50 [-0.90, -0.22]* | -0.51 [-0.91, -0.23]* | 2.99 [2.43, 3.59] | -0.52 [-0.92, -0.23]* | 2.46 [1.86, 3.05] | 2.47 [1.89, 3.05] | -0.51 [-0.91, -0.23]* | -0.51 [-0.91, -0.23]* | -0.53 [-0.93, -0.25]* | -0.10 [-0.69, 0.38] | 1.38 [1.09, 1.71] | 1.54 [1.25, 1.90] |  | -0.08 [-0.36, 0.24] | snaive7 |
| 6 | -0.07 [-0.34, 0.19] | -0.43 [-0.84, -0.14]* | -0.45 [-0.88, -0.15]* | -0.42 [-0.83, -0.12]* | -0.46 [-0.86, -0.18]* | 3.18 [2.59, 3.79] | -0.46 [-0.86, -0.18]* | 2.85 [2.19, 3.47] | 2.80 [2.12, 3.43] | -0.45 [-0.86, -0.16]* | -0.45 [-0.85, -0.16]* | -0.48 [-0.88, -0.19]* | -0.09 [-0.69, 0.40] | 1.38 [1.09, 1.71] | 1.55 [1.25, 1.89] |  | -0.02 [-0.30, 0.31] | snaive7 |
| 7 | -0.14 [-0.38, 0.06] | -0.43 [-0.76, -0.19]* | -0.37 [-0.70, -0.12] | -0.40 [-0.70, -0.18]* | -0.47 [-0.82, -0.20]* | 2.86 [2.31, 3.43] | -0.48 [-0.85, -0.21]* | 3.26 [2.66, 3.87] | 2.93 [2.34, 3.54] | -0.46 [-0.80, -0.21]* | -0.46 [-0.80, -0.21]* | -0.48 [-0.83, -0.22]* | 0.58 [0.00, 1.06] | 1.48 [1.22, 1.75] | 1.66 [1.38, 1.94] |  |  | wt_med |
| 10 |  | -0.17 [-0.51, 0.11] | -0.14 [-0.48, 0.16] | -0.12 [-0.47, 0.22] | -0.18 [-0.53, 0.11] |  |  |  |  |  |  | -0.24 [-0.58, 0.04] | 0.84 [0.29, 1.37] | 1.72 [1.44, 2.06] | 1.90 [1.59, 2.24] |  | 0.24 [0.01, 0.51] | clim |
| 14 |  | -0.08 [-0.46, 0.25] | -0.04 [-0.44, 0.32] | 0.00 [-0.40, 0.44] | -0.07 [-0.46, 0.27] |  |  |  |  |  |  | -0.15 [-0.54, 0.18] | 1.34 [0.72, 1.93] | 1.96 [1.63, 2.37] | 2.16 [1.81, 2.57] |  | 0.37 [0.11, 0.67] | clim |
| 21 |  | 0.04 [-0.42, 0.44] | 0.05 [-0.45, 0.49] | 0.14 [-0.35, 0.67] | 0.07 [-0.40, 0.51] |  |  |  |  |  |  | -0.06 [-0.53, 0.35] | 1.88 [1.13, 2.61] | 2.26 [1.84, 2.78] | 2.51 [2.07, 3.04] |  | 0.54 [0.22, 0.92] | clim |
| 30 |  | 0.11 [-0.41, 0.56] | 0.03 [-0.54, 0.49] | 0.25 [-0.34, 0.86] | 0.22 [-0.34, 0.78] |  |  |  |  |  |  | -0.07 [-0.63, 0.36] | 2.25 [1.32, 3.22] | 2.60 [2.09, 3.25] | 2.87 [2.34, 3.51] |  | 0.61 [0.23, 1.06] | clim |
| 45 |  | 0.10 [-0.21, 0.41] | -1.38 [-2.35, -0.59]* | 0.19 [-0.16, 0.55] | 0.17 [-0.18, 0.58] |  |  |  |  |  |  | -0.00 [-0.34, 0.33] | 2.13 [1.18, 3.02] | 2.77 [2.28, 3.32] | 3.05 [2.55, 3.58] |  | 0.44 [0.15, 0.79] | clim |
| 60 | -0.29 [-0.84, 0.16] | -0.09 [-0.20, -0.01] |  | -0.01 [-0.15, 0.16] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | -0.50 [-1.29, -0.05] | 3.32 [1.68, 5.32] | 0.31 [0.00, 5.00] | 0.31 [0.00, 5.00] |  |  | wt_med |

**first-hour MAE, schedule known at origin**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.60 [4.44, 4.76] | 4.19 [4.06, 4.33] | 4.33 [4.20, 4.48] | 4.19 [4.06, 4.33] | 4.21 [4.08, 4.35] | 7.57 [7.38, 7.77] |  | 6.45 [6.29, 6.61] | 5.86 [5.71, 6.01] |  |  | 4.18 [4.05, 4.31] | 4.79 [4.64, 4.94] |  |  | 4.91 [4.74, 5.10] | 4.60 [4.45, 4.75] |
| 1 | 4.64 [4.49, 4.81] | 4.25 [4.12, 4.39] | 4.41 [4.27, 4.56] | 4.25 [4.12, 4.39] | 4.27 [4.14, 4.42] | 7.80 [7.60, 8.01] | 4.25 [4.11, 4.39] | 6.58 [6.42, 6.75] | 6.57 [6.41, 6.75] | 4.25 [4.11, 4.39] | 4.25 [4.11, 4.39] | 4.24 [4.11, 4.38] | 4.82 [4.66, 4.98] |  |  | 4.93 [4.75, 5.11] | 4.69 [4.54, 4.85] |
| 2 | 4.68 [4.51, 4.85] | 4.28 [4.15, 4.43] | 4.43 [4.29, 4.59] | 4.28 [4.15, 4.43] | 4.30 [4.16, 4.45] | 7.68 [7.48, 7.89] | 4.28 [4.14, 4.42] | 7.23 [7.04, 7.42] | 7.28 [7.09, 7.48] | 4.28 [4.14, 4.42] | 4.28 [4.14, 4.42] | 4.27 [4.14, 4.41] | 4.86 [4.70, 5.02] |  |  | 4.93 [4.76, 5.13] | 4.73 [4.58, 4.90] |
| 3 | 4.67 [4.50, 4.84] | 4.28 [4.14, 4.43] | 4.42 [4.27, 4.56] | 4.28 [4.14, 4.43] | 4.30 [4.16, 4.45] | 7.76 [7.56, 7.97] | 4.28 [4.14, 4.42] | 7.68 [7.48, 7.89] | 7.21 [7.02, 7.40] | 4.28 [4.14, 4.42] | 4.28 [4.14, 4.42] | 4.27 [4.14, 4.42] | 4.86 [4.70, 5.02] | 6.34 [6.16, 6.53] | 6.49 [6.32, 6.68] | 4.91 [4.73, 5.11] | 4.75 [4.60, 4.92] |
| 4 | 4.65 [4.47, 4.83] | 4.29 [4.15, 4.43] | 4.42 [4.26, 4.57] | 4.29 [4.15, 4.43] | 4.31 [4.17, 4.45] | 7.67 [7.47, 7.88] | 4.28 [4.15, 4.43] | 7.72 [7.52, 7.93] | 7.24 [7.05, 7.44] | 4.28 [4.15, 4.43] | 4.28 [4.15, 4.43] | 4.28 [4.14, 4.42] | 4.86 [4.69, 5.03] | 6.37 [6.19, 6.57] | 6.53 [6.35, 6.72] | 4.91 [4.73, 5.11] | 4.76 [4.61, 4.93] |
| 5 | 4.63 [4.46, 4.81] | 4.29 [4.16, 4.44] | 4.43 [4.29, 4.58] | 4.29 [4.16, 4.44] | 4.31 [4.17, 4.46] | 7.78 [7.58, 8.01] | 4.29 [4.15, 4.43] | 7.22 [7.03, 7.43] | 7.20 [7.01, 7.41] | 4.29 [4.15, 4.43] | 4.29 [4.15, 4.43] | 4.28 [4.15, 4.43] | 4.86 [4.70, 5.02] | 6.37 [6.19, 6.56] | 6.53 [6.35, 6.72] | 4.88 [4.70, 5.07] | 4.76 [4.60, 4.94] |
| 6 | 4.61 [4.44, 4.79] | 4.31 [4.17, 4.47] | 4.46 [4.30, 4.62] | 4.31 [4.17, 4.47] | 4.34 [4.19, 4.50] | 7.90 [7.69, 8.13] | 4.31 [4.16, 4.46] | 7.52 [7.31, 7.74] | 7.43 [7.22, 7.65] | 4.31 [4.16, 4.46] | 4.31 [4.16, 4.46] | 4.30 [4.16, 4.46] | 4.84 [4.67, 5.01] | 6.33 [6.15, 6.52] | 6.50 [6.32, 6.69] | 4.85 [4.66, 5.05] | 4.77 [4.61, 4.95] |
| 7 | 4.61 [4.44, 4.79] | 4.36 [4.21, 4.52] | 4.53 [4.36, 4.69] | 4.36 [4.21, 4.52] | 4.38 [4.22, 4.54] | 7.75 [7.54, 7.97] | 4.35 [4.20, 4.51] | 8.09 [7.85, 8.31] | 7.74 [7.51, 7.95] | 4.35 [4.20, 4.51] | 4.35 [4.20, 4.51] | 4.35 [4.20, 4.50] | 5.40 [5.21, 5.60] | 6.43 [6.23, 6.63] | 6.59 [6.40, 6.80] |  | 4.82 [4.65, 5.00] |
| 10 | 4.51 [4.33, 4.71] | 4.34 [4.18, 4.50] | 4.49 [4.32, 4.66] | 4.34 [4.18, 4.50] | 4.35 [4.19, 4.52] |  |  |  |  |  |  | 4.33 [4.17, 4.49] | 5.44 [5.24, 5.64] | 6.41 [6.21, 6.61] | 6.61 [6.41, 6.81] |  | 4.78 [4.60, 4.97] |
| 14 | 4.43 [4.25, 4.64] | 4.28 [4.13, 4.46] | 4.44 [4.27, 4.63] | 4.28 [4.13, 4.46] | 4.29 [4.13, 4.47] |  |  |  |  |  |  | 4.27 [4.11, 4.45] | 5.84 [5.62, 6.07] | 6.55 [6.33, 6.78] | 6.79 [6.57, 7.02] |  | 4.76 [4.58, 4.97] |
| 21 | 4.39 [4.16, 4.61] | 4.28 [4.09, 4.48] | 4.43 [4.23, 4.65] | 4.28 [4.09, 4.48] | 4.27 [4.08, 4.47] |  |  |  |  |  |  | 4.26 [4.07, 4.47] | 6.35 [6.09, 6.62] | 6.71 [6.46, 6.98] | 7.01 [6.74, 7.27] |  | 4.84 [4.62, 5.07] |
| 30 | 4.47 [4.20, 4.76] | 4.31 [4.08, 4.58] | 4.56 [4.32, 4.85] | 4.31 [4.08, 4.58] | 4.29 [4.06, 4.57] |  |  |  |  |  |  | 4.31 [4.08, 4.58] | 6.80 [6.47, 7.14] | 7.04 [6.74, 7.39] | 7.40 [7.08, 7.74] |  | 4.91 [4.64, 5.19] |
| 45 | 4.32 [3.97, 4.70] | 4.32 [3.98, 4.69] | 4.60 [3.89, 5.44] | 4.32 [3.98, 4.69] | 4.30 [3.97, 4.68] |  |  |  |  |  |  | 4.32 [3.98, 4.68] | 6.88 [6.43, 7.36] | 7.15 [6.71, 7.62] | 7.50 [7.05, 7.96] |  | 4.69 [4.35, 5.07] |
| 60 | 2.81 [1.75, 3.99] | 2.99 [1.92, 4.14] |  | 2.99 [1.92, 4.14] | 0.50 [0.00, 7.50] |  |  |  |  |  |  | 2.99 [1.92, 4.18] | 6.18 [4.65, 7.95] | 0.83 [0.00, 12.50] | 0.83 [0.00, 12.50] |  | 3.03 [1.97, 4.21] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.05 [-0.15, 0.22] | -0.44 [-0.82, -0.17]* | -0.35 [-0.73, -0.09]* | -0.44 [-0.82, -0.17]* | -0.45 [-0.83, -0.18]* | 2.91 [2.43, 3.44] |  | 1.81 [1.45, 2.20] | 1.21 [0.90, 1.51] |  |  | -0.44 [-0.82, -0.17]* | 0.17 [-0.35, 0.63] |  |  | 0.31 [0.01, 0.60] |  | wt_med |
| 1 | -0.20 [-0.48, 0.07] | -0.72 [-1.19, -0.42]* | -0.70 [-1.22, -0.39]* | -0.72 [-1.19, -0.42]* | -0.72 [-1.20, -0.41]* | 2.85 [2.25, 3.50] | -0.72 [-1.19, -0.42]* | 1.63 [1.16, 2.09] | 1.63 [1.16, 2.10] | -0.72 [-1.19, -0.42]* | -0.72 [-1.19, -0.42]* | -0.73 [-1.20, -0.42]* | -0.25 [-0.92, 0.28] |  |  |  | -0.24 [-0.54, 0.08] | snaive7 |
| 2 | -0.17 [-0.46, 0.12] | -0.69 [-1.15, -0.39]* | -0.67 [-1.18, -0.35]* | -0.69 [-1.15, -0.39]* | -0.69 [-1.15, -0.38]* | 2.73 [2.17, 3.34] | -0.69 [-1.15, -0.38]* | 2.32 [1.75, 2.91] | 2.39 [1.77, 3.03] | -0.69 [-1.15, -0.38]* | -0.69 [-1.15, -0.38]* | -0.69 [-1.15, -0.39]* | -0.21 [-0.88, 0.33] |  |  |  | -0.19 [-0.50, 0.14] | snaive7 |
| 3 | -0.16 [-0.44, 0.13] | -0.66 [-1.10, -0.36]* | -0.62 [-1.11, -0.31]* | -0.66 [-1.10, -0.36]* | -0.65 [-1.11, -0.34]* | 2.87 [2.30, 3.49] | -0.65 [-1.10, -0.35]* | 2.83 [2.15, 3.54] | 2.33 [1.69, 2.97] | -0.65 [-1.10, -0.35]* | -0.65 [-1.10, -0.35]* | -0.66 [-1.11, -0.36]* | -0.19 [-0.87, 0.35] | 1.37 [1.02, 1.74] | 1.52 [1.16, 1.92] |  | -0.15 [-0.45, 0.21] | snaive7 |
| 4 | -0.18 [-0.46, 0.13] | -0.64 [-1.09, -0.34]* | -0.62 [-1.09, -0.32]* | -0.64 [-1.09, -0.34]* | -0.64 [-1.09, -0.32]* | 2.79 [2.17, 3.45] | -0.64 [-1.09, -0.34]* | 2.87 [2.15, 3.66] | 2.35 [1.73, 2.99] | -0.64 [-1.09, -0.34]* | -0.64 [-1.09, -0.34]* | -0.65 [-1.11, -0.35]* | -0.19 [-0.90, 0.35] | 1.41 [1.07, 1.75] | 1.57 [1.23, 1.95] |  | -0.14 [-0.43, 0.19] | snaive7 |
| 5 | -0.17 [-0.46, 0.15] | -0.61 [-1.07, -0.30]* | -0.59 [-1.08, -0.28]* | -0.61 [-1.07, -0.30]* | -0.62 [-1.08, -0.29]* | 2.94 [2.31, 3.62] | -0.61 [-1.06, -0.30]* | 2.35 [1.71, 3.01] | 2.34 [1.74, 2.99] | -0.61 [-1.06, -0.30]* | -0.61 [-1.06, -0.30]* | -0.62 [-1.08, -0.31]* | -0.17 [-0.89, 0.39] | 1.44 [1.10, 1.80] | 1.60 [1.25, 2.03] |  | -0.11 [-0.41, 0.22] | snaive7 |
| 6 | -0.16 [-0.46, 0.14] | -0.57 [-1.03, -0.24]* | -0.54 [-1.04, -0.21]* | -0.57 [-1.03, -0.24]* | -0.57 [-1.04, -0.24]* | 3.08 [2.45, 3.78] | -0.56 [-1.03, -0.24]* | 2.72 [2.06, 3.42] | 2.63 [1.96, 3.33] | -0.56 [-1.03, -0.24]* | -0.56 [-1.03, -0.24]* | -0.57 [-1.04, -0.25]* | -0.16 [-0.88, 0.40] | 1.43 [1.09, 1.79] | 1.60 [1.26, 2.01] |  | -0.08 [-0.38, 0.24] | snaive7 |
| 7 | -0.16 [-0.41, 0.06] | -0.51 [-0.90, -0.22]* | -0.41 [-0.78, -0.14] | -0.51 [-0.90, -0.22]* | -0.50 [-0.90, -0.21]* | 2.87 [2.36, 3.48] | -0.51 [-0.90, -0.22]* | 3.23 [2.59, 3.94] | 2.88 [2.26, 3.58] | -0.51 [-0.90, -0.22]* | -0.51 [-0.90, -0.22]* | -0.50 [-0.90, -0.22]* | 0.57 [-0.04, 1.11] | 1.59 [1.29, 1.89] | 1.76 [1.45, 2.09] |  |  | wt_med |
| 10 |  | -0.24 [-0.62, 0.08] | -0.22 [-0.62, 0.13] | -0.24 [-0.62, 0.08] | -0.24 [-0.64, 0.08] |  |  |  |  |  |  | -0.24 [-0.62, 0.08] | 0.90 [0.26, 1.53] | 1.82 [1.50, 2.19] | 2.03 [1.68, 2.44] |  | 0.24 [0.00, 0.51] | clim |
| 14 |  | -0.21 [-0.63, 0.14] | -0.17 [-0.62, 0.19] | -0.21 [-0.63, 0.14] | -0.22 [-0.64, 0.13] |  |  |  |  |  |  | -0.21 [-0.64, 0.14] | 1.40 [0.70, 2.08] | 2.06 [1.68, 2.49] | 2.30 [1.89, 2.77] |  | 0.31 [0.05, 0.60] | clim |
| 21 |  | -0.14 [-0.66, 0.28] | -0.11 [-0.70, 0.34] | -0.14 [-0.66, 0.28] | -0.14 [-0.66, 0.29] |  |  |  |  |  |  | -0.14 [-0.66, 0.29] | 1.98 [1.12, 2.81] | 2.28 [1.84, 2.81] | 2.57 [2.06, 3.19] |  | 0.47 [0.15, 0.82] | clim |
| 30 |  | -0.18 [-0.84, 0.34] | -0.13 [-0.87, 0.45] | -0.18 [-0.84, 0.34] | -0.16 [-0.83, 0.35] |  |  |  |  |  |  | -0.18 [-0.85, 0.33] | 2.34 [1.18, 3.43] | 2.49 [1.98, 3.09] | 2.85 [2.24, 3.59] |  | 0.46 [0.08, 0.87] | clim |
| 45 |  | -0.04 [-0.46, 0.33] | -2.41 [-3.97, -1.09]* | -0.04 [-0.46, 0.33] | -0.04 [-0.47, 0.32] |  |  |  |  |  |  | -0.04 [-0.46, 0.33] | 2.53 [1.52, 3.60] | 2.80 [2.20, 3.46] | 3.15 [2.51, 3.92] |  | 0.39 [0.06, 0.74] | clim |
| 60 | -0.19 [-0.59, 0.15] | 0.00 [0.00, 0.00] |  | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | 0.00 [0.00, 0.00] | 3.16 [1.60, 4.66] | 0.33 [0.00, 5.00] | 0.33 [0.00, 5.00] |  |  | wt_med |

**first-hour MAE, schedule projected at origin**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.58 [4.33, 4.86] | 4.28 [4.06, 4.52] | 4.04 [3.80, 4.28] | 4.28 [4.06, 4.52] | 4.22 [4.00, 4.45] | 7.44 [7.17, 7.76] |  | 5.71 [5.48, 5.96] | 5.44 [5.22, 5.68] |  |  | 4.18 [3.96, 4.41] | 4.73 [4.31, 5.17] |  |  | 4.55 [4.31, 4.80] | 4.28 [4.06, 4.53] |
| 1 | 4.65 [4.39, 4.91] | 4.34 [4.11, 4.59] | 4.10 [3.84, 4.35] | 4.34 [4.11, 4.58] | 4.29 [4.06, 4.51] | 7.66 [7.37, 7.98] | 4.30 [4.06, 4.53] | 6.83 [6.57, 7.10] | 6.63 [6.38, 6.90] | 4.28 [4.03, 4.50] | 4.28 [4.04, 4.50] | 4.25 [4.02, 4.47] | 4.75 [4.37, 5.20] |  |  | 4.54 [4.31, 4.80] | 4.35 [4.11, 4.59] |
| 2 | 4.67 [4.42, 4.94] | 4.40 [4.16, 4.63] | 4.11 [3.85, 4.35] | 4.39 [4.16, 4.62] | 4.35 [4.12, 4.58] | 7.52 [7.25, 7.81] | 4.33 [4.10, 4.57] | 7.38 [7.09, 7.69] | 7.32 [7.04, 7.62] | 4.32 [4.08, 4.55] | 4.32 [4.08, 4.54] | 4.29 [4.05, 4.51] | 4.76 [4.38, 5.21] |  |  | 4.53 [4.31, 4.78] | 4.40 [4.16, 4.63] |
| 3 | 4.67 [4.42, 4.92] | 4.42 [4.18, 4.66] | 4.11 [3.86, 4.35] | 4.41 [4.18, 4.65] | 4.39 [4.16, 4.62] | 7.58 [7.28, 7.89] | 4.34 [4.11, 4.57] | 7.63 [7.32, 7.96] | 7.31 [6.99, 7.64] | 4.33 [4.10, 4.56] | 4.33 [4.10, 4.56] | 4.30 [4.07, 4.53] | 4.78 [4.39, 5.23] | 5.44 [5.21, 5.69] | 5.66 [5.42, 5.91] | 4.52 [4.31, 4.75] | 4.42 [4.18, 4.66] |
| 4 | 4.71 [4.46, 4.96] | 4.49 [4.25, 4.74] | 4.12 [3.89, 4.36] | 4.47 [4.24, 4.72] | 4.46 [4.21, 4.73] | 7.77 [7.46, 8.08] | 4.39 [4.17, 4.62] | 7.57 [7.25, 7.93] | 7.43 [7.11, 7.76] | 4.39 [4.16, 4.62] | 4.38 [4.15, 4.62] | 4.33 [4.10, 4.55] | 4.83 [4.44, 5.28] | 5.46 [5.22, 5.72] | 5.65 [5.42, 5.92] | 4.52 [4.30, 4.76] | 4.50 [4.25, 4.75] |
| 5 | 4.80 [4.56, 5.09] | 4.66 [4.41, 4.94] | 4.31 [4.03, 4.60] | 4.66 [4.39, 4.94] | 4.56 [4.31, 4.82] | 7.79 [7.47, 8.11] | 4.51 [4.27, 4.75] | 7.55 [7.24, 7.88] | 7.59 [7.28, 7.89] | 4.52 [4.28, 4.77] | 4.54 [4.30, 4.80] | 4.49 [4.24, 4.74] | 4.97 [4.56, 5.43] | 5.62 [5.36, 5.91] | 5.79 [5.54, 6.08] | 4.63 [4.38, 4.91] | 4.68 [4.42, 4.98] |
| 6 | 4.97 [4.69, 5.28] | 4.91 [4.61, 5.23] | 4.55 [4.24, 4.86] | 4.95 [4.62, 5.31] | 4.72 [4.45, 4.98] | 8.28 [7.95, 8.61] | 4.71 [4.45, 4.98] | 8.07 [7.72, 8.44] | 8.22 [7.84, 8.62] | 4.76 [4.47, 5.05] | 4.76 [4.48, 5.06] | 4.66 [4.39, 4.92] | 5.12 [4.72, 5.57] | 5.78 [5.51, 6.09] | 5.94 [5.66, 6.25] | 4.74 [4.47, 5.01] | 4.95 [4.64, 5.30] |
| 7 | 5.13 [4.84, 5.46] | 5.18 [4.86, 5.53] | 4.79 [4.45, 5.13] | 5.35 [4.97, 5.78] | 4.94 [4.66, 5.23] | 8.07 [7.75, 8.42] | 4.89 [4.62, 5.17] | 8.65 [8.26, 9.07] | 8.44 [8.05, 8.84] | 5.01 [4.71, 5.35] | 5.02 [4.72, 5.36] | 4.89 [4.58, 5.19] | 5.90 [5.42, 6.43] | 6.20 [5.89, 6.52] | 6.38 [6.08, 6.69] |  | 5.24 [4.90, 5.63] |
| 10 | 5.55 [5.24, 5.88] | 5.74 [5.40, 6.14] | 5.48 [5.06, 5.91] | 6.04 [5.60, 6.50] | 5.67 [5.31, 6.08] |  |  |  |  |  |  | 5.36 [5.05, 5.68] | 6.21 [5.73, 6.72] | 6.89 [6.48, 7.33] | 6.95 [6.57, 7.34] |  | 5.85 [5.48, 6.27] |
| 14 | 5.84 [5.52, 6.19] | 6.31 [5.93, 6.74] | 6.07 [5.62, 6.54] | 6.70 [6.22, 7.22] | 6.37 [5.93, 6.83] |  |  |  |  |  |  | 5.94 [5.58, 6.31] | 7.07 [6.56, 7.63] | 7.50 [7.06, 7.98] | 7.53 [7.12, 7.98] |  | 6.46 [6.04, 6.91] |
| 21 | 5.83 [5.50, 6.20] | 6.44 [6.07, 6.85] | 6.16 [5.70, 6.66] | 6.84 [6.39, 7.40] | 6.55 [6.11, 7.04] |  |  |  |  |  |  | 6.03 [5.67, 6.41] | 7.52 [7.01, 8.11] | 8.10 [7.59, 8.65] | 8.21 [7.72, 8.71] |  | 6.63 [6.23, 7.07] |
| 30 | 5.83 [5.50, 6.19] | 6.69 [6.26, 7.16] | 6.00 [5.58, 6.46] | 7.17 [6.62, 7.78] | 7.02 [6.45, 7.59] |  |  |  |  |  |  | 6.04 [5.64, 6.47] | 7.92 [7.32, 8.54] | 8.68 [8.08, 9.29] | 8.74 [8.18, 9.32] |  | 6.89 [6.43, 7.40] |
| 45 | 5.88 [5.43, 6.42] | 6.60 [6.08, 7.20] | 4.28 [3.79, 4.82] | 6.85 [6.28, 7.52] | 6.86 [6.25, 7.55] |  |  |  |  |  |  | 6.28 [5.75, 6.89] | 7.49 [6.88, 8.20] | 8.85 [8.17, 9.55] | 8.97 [8.33, 9.65] |  | 6.76 [6.22, 7.39] |
| 60 | 6.21 [4.09, 8.86] | 6.69 [4.37, 9.83] |  | 6.86 [4.45, 10.12] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | 5.81 [3.95, 8.44] | 10.24 [7.25, 14.35] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 6.68 [4.37, 9.72] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.12 [-0.41, 0.75] | -0.22 [-0.85, 0.53] | -0.32 [-1.08, 0.52] | -0.23 [-0.86, 0.50] | -0.24 [-0.86, 0.50] | 3.02 [1.85, 4.47] |  | 1.26 [0.71, 2.05] | 0.97 [0.36, 1.77] |  |  | -0.29 [-0.92, 0.44] | 0.09 [-1.14, 1.56] |  |  | -0.21 [-0.86, 0.54] | snaive7 |
| 1 | 0.22 [-0.40, 0.95] | -0.13 [-0.87, 0.75] | -0.22 [-1.11, 0.74] | -0.14 [-0.87, 0.72] | -0.13 [-0.87, 0.74] | 3.24 [2.22, 4.56] | -0.15 [-0.89, 0.71] | 2.40 [1.31, 3.59] | 2.18 [1.29, 3.24] | -0.17 [-0.91, 0.69] | -0.16 [-0.91, 0.70] | -0.19 [-0.93, 0.68] | 0.12 [-1.08, 1.49] |  |  | -0.12 [-0.87, 0.74] | snaive7 |
| 2 | 0.25 [-0.35, 0.94] | -0.07 [-0.79, 0.76] | -0.21 [-1.08, 0.72] | -0.09 [-0.80, 0.76] | -0.10 [-0.83, 0.74] | 3.09 [2.17, 4.23] | -0.12 [-0.84, 0.71] | 2.96 [1.75, 4.33] | 2.90 [1.78, 4.16] | -0.13 [-0.85, 0.70] | -0.12 [-0.85, 0.70] | -0.15 [-0.88, 0.68] | 0.13 [-1.07, 1.51] |  |  | -0.07 [-0.80, 0.77] | snaive7 |
| 3 | 0.26 [-0.30, 0.93] | -0.06 [-0.76, 0.75] | -0.20 [-1.03, 0.73] | -0.07 [-0.77, 0.73] | -0.08 [-0.79, 0.75] | 3.18 [2.24, 4.46] | -0.11 [-0.80, 0.68] | 3.22 [1.92, 4.70] | 2.88 [1.55, 4.39] | -0.12 [-0.83, 0.68] | -0.11 [-0.82, 0.69] | -0.15 [-0.86, 0.64] | 0.14 [-1.03, 1.49] | 1.00 [0.82, 1.27] | 1.22 [1.03, 1.47] | -0.06 [-0.76, 0.76] | snaive7 |
| 4 | 0.30 [-0.21, 0.91] | 0.01 [-0.66, 0.82] | -0.19 [-0.94, 0.65] | -0.02 [-0.67, 0.76] | -0.00 [-0.67, 0.83] | 3.37 [2.33, 4.74] | -0.06 [-0.72, 0.70] | 3.13 [1.83, 4.76] | 3.01 [1.85, 4.42] | -0.06 [-0.72, 0.70] | -0.06 [-0.72, 0.70] | -0.13 [-0.79, 0.64] | 0.17 [-0.97, 1.52] | 1.03 [0.80, 1.47] | 1.23 [1.01, 1.63] | 0.02 [-0.67, 0.84] | snaive7 |
| 5 | 0.28 [-0.20, 0.85] | 0.06 [-0.58, 0.82] | -0.15 [-0.87, 0.66] | 0.06 [-0.58, 0.83] | -0.01 [-0.64, 0.71] | 3.27 [2.23, 4.60] | -0.05 [-0.66, 0.66] | 3.00 [1.74, 4.37] | 3.07 [1.98, 4.27] | -0.04 [-0.66, 0.68] | -0.01 [-0.64, 0.71] | -0.09 [-0.72, 0.62] | 0.20 [-0.93, 1.49] | 1.06 [0.81, 1.43] | 1.24 [0.98, 1.60] | 0.09 [-0.57, 0.87] | snaive7 |
| 6 | 0.37 [-0.12, 0.93] | 0.21 [-0.47, 1.11] | -0.04 [-0.79, 0.84] | 0.26 [-0.48, 1.26] | 0.05 [-0.57, 0.77] | 3.61 [2.58, 5.03] | 0.03 [-0.59, 0.72] | 3.45 [2.30, 4.85] | 3.60 [2.44, 5.08] | 0.09 [-0.56, 0.87] | 0.11 [-0.55, 0.88] | -0.02 [-0.65, 0.69] | 0.25 [-0.91, 1.51] | 1.14 [0.88, 1.58] | 1.30 [1.06, 1.66] | 0.28 [-0.47, 1.26] | snaive7 |
| 7 | -0.05 [-0.72, 0.45] | -0.08 [-0.25, -0.00] | -0.17 [-0.62, 0.02] | 0.09 [-0.08, 0.43] | -0.29 [-0.88, -0.01] | 2.80 [1.48, 4.09] | -0.36 [-1.10, -0.01] | 3.41 [2.41, 4.70] | 3.20 [2.07, 4.52] | -0.24 [-0.60, -0.03] | -0.23 [-0.55, -0.03] | -0.34 [-0.88, -0.04] | 0.63 [-0.50, 1.98] | 0.98 [0.37, 1.63] | 1.17 [0.46, 1.88] |  | wt_med |
| 10 | -0.25 [-1.15, 0.39] | -0.13 [-0.39, 0.00] | -0.11 [-0.38, 0.04] | 0.17 [-0.11, 0.66] | -0.19 [-0.47, 0.01] |  |  |  |  |  |  | -0.49 [-1.21, -0.06] | 0.33 [-1.20, 1.83] | 1.05 [0.35, 1.71] | 1.11 [0.38, 1.84] |  | wt_med |
| 14 | -0.56 [-1.54, 0.29] | -0.14 [-0.42, -0.00] | -0.18 [-0.57, 0.03] | 0.24 [-0.09, 0.90] | -0.09 [-0.28, 0.02] |  |  |  |  |  |  | -0.49 [-1.21, -0.06] | 0.56 [-1.23, 2.37] | 1.07 [0.35, 1.83] | 1.09 [0.39, 1.91] |  | wt_med |
| 21 | -0.75 [-1.86, 0.17] | -0.17 [-0.48, -0.02] | -0.22 [-0.72, 0.03] | 0.24 [-0.07, 0.90] | -0.04 [-0.25, 0.14] |  |  |  |  |  |  | -0.55 [-1.20, -0.13] | 0.81 [-1.22, 2.89] | 1.49 [1.05, 2.08] | 1.59 [1.19, 2.16] |  | wt_med |
| 30 |  | 0.82 [0.05, 1.74] | 0.42 [-0.32, 1.13] | 1.32 [0.13, 2.97] | 1.14 [0.01, 2.73] |  |  |  |  |  |  | 0.21 [-0.61, 0.94] | 2.03 [0.24, 4.32] | 2.86 [1.64, 4.44] | 2.92 [1.91, 4.25] | 0.97 [0.10, 2.13] | clim |
| 45 |  | 0.40 [-0.05, 0.93] | -0.60 [-1.21, -0.20] | 0.66 [0.04, 1.50] | 0.64 [-0.04, 1.69] |  |  |  |  |  |  | 0.07 [-0.53, 0.76] | 1.30 [-0.19, 2.93] | 2.71 [1.90, 3.69] | 2.83 [2.20, 3.54] | 0.54 [0.02, 1.28] | clim |
| 60 | -0.41 [-1.50, 0.51] | -0.19 [-0.44, -0.02] |  | -0.02 [-0.30, 0.35] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | -1.04 [-2.84, -0.10] | 3.50 [0.48, 7.10] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |

**rope-drop worth agreement**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] |  | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] |  |  | 0.91 [0.90, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 1 | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 2 | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 3 | 0.92 [0.92, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 4 | 0.92 [0.92, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 5 | 0.92 [0.92, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 6 | 0.92 [0.92, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |
| 7 | 0.92 [0.92, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.92 [0.91, 0.92] |
| 10 | 0.92 [0.92, 0.93] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] |  |  |  |  |  |  | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.92 [0.92, 0.92] |
| 14 | 0.92 [0.92, 0.93] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] |  |  |  |  |  |  | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.92 [0.92, 0.92] |
| 21 | 0.93 [0.92, 0.93] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |  |  |  |  |  |  | 0.90 [0.89, 0.90] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] |  | 0.92 [0.92, 0.93] |
| 30 | 0.93 [0.93, 0.94] | 0.92 [0.92, 0.93] | 0.92 [0.91, 0.92] | 0.92 [0.92, 0.92] | 0.92 [0.92, 0.93] |  |  |  |  |  |  | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] |  | 0.93 [0.92, 0.93] |
| 45 | 0.93 [0.92, 0.94] | 0.92 [0.91, 0.93] | 0.87 [0.85, 0.89] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.93] |  |  |  |  |  |  | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] |  | 0.92 [0.92, 0.93] |
| 60 | 0.96 [0.94, 0.98] | 0.96 [0.94, 0.97] |  | 0.95 [0.93, 0.97] | 1.00 [1.00, 1.00] |  |  |  |  |  |  | 0.91 [0.87, 0.94] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.95 [0.93, 0.97] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] |  | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] |  |  | -0.01 [-0.01, -0.00] |  |  | -0.01 [-0.02, -0.01] |  | wt_med |
| 1 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  | -0.01 [-0.01, -0.00] |  | wt_med |
| 2 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  |  | -0.01 [-0.01, -0.00] |  | wt_med |
| 3 | 0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, 0.00] | -0.01 [-0.01, -0.00] |  | wt_med |
| 4 |  | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | clim |
| 5 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | clim |
| 6 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.02 [-0.02, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | clim |
| 7 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.02 [-0.02, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | -0.00 [-0.01, -0.00] | clim |
| 10 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] |  |  |  |  |  |  | -0.02 [-0.03, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | -0.00 [-0.01, -0.00] | clim |
| 14 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  |  |  |  |  | -0.02 [-0.03, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | -0.00 [-0.01, -0.00] | clim |
| 21 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  |  |  |  |  | -0.03 [-0.04, -0.02] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.00] |  | -0.01 [-0.01, -0.00] | clim |
| 30 |  | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  |  |  |  |  | -0.03 [-0.05, -0.02] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | -0.01 [-0.01, -0.00] | clim |
| 45 |  | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  |  |  |  |  | -0.03 [-0.05, -0.02] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | -0.01 [-0.01, -0.00] | clim |
| 60 | 0.01 [-0.00, 0.02] | 0.01 [0.00, 0.01] |  | -0.00 [-0.01, 0.00] | 0.00 [0.00, 0.00] |  |  |  |  |  |  | -0.04 [-0.07, -0.02] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  | wt_med |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_nf_tide_wx_owx | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.23 [0.21, 0.24] | 0.26 [0.24, 0.27] | 0.36 [0.35, 0.37] | 0.31 [0.30, 0.32] | 0.34 [0.33, 0.35] | 0.35 [0.34, 0.36] | 0.42 [0.41, 0.44] | 0.41 [0.40, 0.43] | 0.31 [0.29, 0.32] |
| 2 | 0.23 [0.22, 0.25] | 0.24 [0.23, 0.26] | 0.36 [0.34, 0.37] | 0.31 [0.29, 0.32] | 0.33 [0.32, 0.34] | 0.33 [0.32, 0.35] | 0.42 [0.41, 0.44] | 0.39 [0.38, 0.40] | 0.30 [0.29, 0.32] |
| 3 | 0.23 [0.22, 0.24] | 0.24 [0.23, 0.26] | 0.36 [0.34, 0.37] | 0.30 [0.29, 0.31] | 0.33 [0.31, 0.34] | 0.33 [0.32, 0.35] | 0.43 [0.41, 0.44] | 0.39 [0.38, 0.41] | 0.30 [0.29, 0.31] |
| 4 | 0.23 [0.22, 0.25] | 0.25 [0.23, 0.26] | 0.36 [0.34, 0.37] | 0.31 [0.30, 0.32] | 0.33 [0.31, 0.34] | 0.34 [0.32, 0.35] | 0.43 [0.42, 0.44] | 0.38 [0.37, 0.40] | 0.30 [0.29, 0.32] |
| 5 | 0.24 [0.22, 0.25] | 0.24 [0.23, 0.26] | 0.36 [0.35, 0.37] | 0.30 [0.29, 0.32] | 0.32 [0.31, 0.33] | 0.34 [0.33, 0.36] | 0.43 [0.42, 0.45] | 0.38 [0.37, 0.40] | 0.30 [0.29, 0.32] |
| 6 | 0.21 [0.20, 0.22] | 0.24 [0.23, 0.25] | 0.36 [0.35, 0.38] | 0.30 [0.28, 0.31] | 0.32 [0.31, 0.33] | 0.33 [0.32, 0.35] | 0.44 [0.42, 0.45] | 0.38 [0.36, 0.39] | 0.30 [0.29, 0.31] |
| 7 | 0.21 [0.19, 0.22] | 0.23 [0.22, 0.24] | 0.32 [0.31, 0.34] | 0.30 [0.29, 0.31] | 0.31 [0.29, 0.32] | 0.32 [0.30, 0.33] |  | 0.35 [0.34, 0.37] | 0.29 [0.28, 0.30] |
| 10 | 0.21 [0.20, 0.23] | 0.23 [0.22, 0.24] | 0.32 [0.31, 0.34] |  |  |  |  | 0.34 [0.33, 0.36] | 0.28 [0.27, 0.30] |
| 14 | 0.21 [0.19, 0.22] | 0.23 [0.22, 0.24] | 0.29 [0.27, 0.30] |  |  |  |  | 0.33 [0.31, 0.34] | 0.27 [0.25, 0.28] |
| 21 | 0.21 [0.20, 0.23] | 0.23 [0.21, 0.24] | 0.26 [0.24, 0.27] |  |  |  |  | 0.30 [0.28, 0.32] | 0.25 [0.23, 0.26] |
| 30 | 0.20 [0.18, 0.22] | 0.22 [0.20, 0.23] | 0.25 [0.23, 0.27] |  |  |  |  | 0.27 [0.26, 0.29] | 0.23 [0.22, 0.25] |
| 45 | 0.24 [0.20, 0.29] | 0.21 [0.19, 0.24] | 0.22 [0.20, 0.25] |  |  |  |  | 0.25 [0.23, 0.28] | 0.23 [0.21, 0.25] |
| 60 |  | 0.20 [0.10, 0.31] | 0.27 [0.15, 0.38] |  |  |  |  | 0.00 [0.00, 0.00] | 0.23 [0.12, 0.34] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_nf_tide_wx_owx | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.20 [-0.24, -0.16] | -0.17 [-0.20, -0.13] | -0.05 [-0.07, -0.03] | -0.11 [-0.14, -0.08] | -0.08 [-0.11, -0.05] | -0.07 [-0.09, -0.04] | -0.00 [-0.02, 0.02] | -0.11 [-0.14, -0.08] | lvl_snaive7 |
| 2 | -0.19 [-0.24, -0.15] | -0.18 [-0.21, -0.14] | -0.06 [-0.08, -0.04] | -0.11 [-0.14, -0.09] | -0.09 [-0.12, -0.06] | -0.08 [-0.11, -0.06] | -0.03 [-0.05, -0.01] | -0.11 [-0.14, -0.08] | lvl_snaive7 |
| 3 | -0.20 [-0.24, -0.16] | -0.18 [-0.22, -0.15] | -0.06 [-0.08, -0.04] | -0.13 [-0.15, -0.10] | -0.10 [-0.13, -0.07] | -0.09 [-0.11, -0.06] | -0.03 [-0.05, -0.01] | -0.12 [-0.15, -0.09] | lvl_snaive7 |
| 4 | -0.20 [-0.24, -0.16] | -0.18 [-0.22, -0.15] | -0.06 [-0.08, -0.04] | -0.12 [-0.15, -0.09] | -0.10 [-0.13, -0.07] | -0.09 [-0.12, -0.07] | -0.04 [-0.06, -0.02] | -0.12 [-0.15, -0.09] | lvl_snaive7 |
| 5 | -0.20 [-0.24, -0.16] | -0.19 [-0.23, -0.15] | -0.06 [-0.08, -0.04] | -0.13 [-0.16, -0.10] | -0.11 [-0.14, -0.08] | -0.09 [-0.11, -0.06] | -0.04 [-0.07, -0.02] | -0.12 [-0.16, -0.09] | lvl_snaive7 |
| 6 | -0.22 [-0.27, -0.19] | -0.20 [-0.24, -0.16] | -0.06 [-0.09, -0.04] | -0.14 [-0.17, -0.11] | -0.11 [-0.14, -0.08] | -0.10 [-0.12, -0.07] | -0.05 [-0.07, -0.03] | -0.13 [-0.16, -0.10] | lvl_snaive7 |
| 7 | -0.13 [-0.17, -0.08] | -0.09 [-0.12, -0.06] |  | -0.02 [-0.05, 0.01] | -0.01 [-0.04, 0.01] | -0.01 [-0.03, 0.02] | 0.03 [0.01, 0.05]* | -0.03 [-0.05, -0.02] | lvl_naive4 |
| 10 | -0.12 [-0.16, -0.07] | -0.10 [-0.13, -0.07] |  |  |  |  | 0.02 [0.00, 0.05]* | -0.04 [-0.06, -0.02] | lvl_naive4 |
| 14 | -0.08 [-0.13, -0.04] | -0.06 [-0.09, -0.03] |  |  |  |  | 0.04 [0.02, 0.06]* | -0.02 [-0.04, -0.00] | lvl_naive4 |
| 21 | -0.06 [-0.11, -0.01] | -0.03 [-0.07, 0.00] |  |  |  |  | 0.04 [0.02, 0.07]* | -0.01 [-0.03, 0.01] | lvl_naive4 |
| 30 | -0.05 [-0.11, 0.01] | -0.03 [-0.07, 0.01] |  |  |  |  | 0.03 [-0.00, 0.05] | -0.01 [-0.04, 0.01] | lvl_naive4 |
| 45 | -0.03 [-0.17, 0.10] | -0.01 [-0.05, 0.03] |  |  |  |  | 0.03 [-0.01, 0.06] | 0.01 [-0.02, 0.03] | lvl_naive4 |
| 60 |  | -0.07 [-0.22, 0.07] |  |  |  |  | -1.00 [-1.00, -1.00] | -0.04 [-0.14, 0.06] | lvl_naive4 |

| lead | model | park_days | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|
| 1 | lvl_naive4 | 5227 | 0.75 | 0.70 | 0.59 |
| 1 | lvl_wt56 | 5239 | 0.71 | 0.65 | 0.53 |
| 1 | lvl_snaive7 | 4931 | 0.79 | 0.70 | 0.64 |
| 1 | lvl_clim | 5239 | 0.66 | 0.39 | 0.55 |
| 1 | lvl_tft | 5127 | 0.81 | 0.68 | 0.67 |
| 1 | lvl_cbd | 4029 | 0.60 | 0.12 | 0.55 |
| 1 | lvl_nf_tide | 5164 | 0.72 | 0.32 | 0.72 |
| 1 | lvl_nf_nhits | 5164 | 0.70 | 0.31 | 0.72 |
| 1 | lvl_nf_tide_wx_owx | 5164 | 0.75 | 0.38 | 0.74 |
| 7 | lvl_naive4 | 4532 | 0.69 | 0.67 | 0.52 |
| 7 | lvl_wt56 | 4542 | 0.68 | 0.62 | 0.49 |
| 7 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 7 | lvl_clim | 4542 | 0.65 | 0.40 | 0.54 |
| 7 | lvl_tft | 4440 | 0.75 | 0.62 | 0.59 |
| 7 | lvl_cbd | 3614 | 0.54 | 0.07 | 0.39 |
| 7 | lvl_nf_tide | 4539 | 0.69 | 0.31 | 0.59 |
| 7 | lvl_nf_nhits | 4539 | 0.67 | 0.26 | 0.64 |
| 7 | lvl_nf_tide_wx_owx | 4539 | 0.72 | 0.36 | 0.62 |
| 30 | lvl_naive4 | 2356 | 0.60 | 0.64 | 0.35 |
| 30 | lvl_wt56 | 2364 | 0.60 | 0.57 | 0.35 |
| 30 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_clim | 2364 | 0.66 | 0.37 | 0.48 |
| 30 | lvl_tft | 2314 | 0.64 | 0.68 | 0.42 |
| 30 | lvl_cbd | 1886 | 0.60 | 0.09 | 0.34 |
| 30 | lvl_nf_tide | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_nf_nhits | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_nf_tide_wx_owx | 0 | 0.00 | 0.00 | 0.00 |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 119535 | 71482 | 0.60 |
| 1 | lvl_clim | 200908 | 119201 | 0.59 |
| 1 | lvl_naive4 | 200076 | 138976 | 0.69 |
| 1 | lvl_nf_nhits | 198409 | 132654 | 0.67 |
| 1 | lvl_nf_tide | 198409 | 139404 | 0.70 |
| 1 | lvl_nf_tide_wx_owx | 198409 | 142863 | 0.72 |
| 1 | lvl_snaive7 | 178795 | 131666 | 0.74 |
| 1 | lvl_tft | 192407 | 139943 | 0.73 |
| 1 | lvl_wt56 | 200908 | 130777 | 0.65 |
| 7 | lvl_cbd | 108231 | 54278 | 0.50 |
| 7 | lvl_clim | 170655 | 97645 | 0.57 |
| 7 | lvl_naive4 | 169968 | 111148 | 0.65 |
| 7 | lvl_nf_nhits | 170448 | 111080 | 0.65 |
| 7 | lvl_nf_tide | 170448 | 111955 | 0.66 |
| 7 | lvl_nf_tide_wx_owx | 170448 | 114735 | 0.67 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 162587 | 109260 | 0.67 |
| 7 | lvl_wt56 | 170655 | 106697 | 0.63 |
| 30 | lvl_cbd | 51551 | 22806 | 0.44 |
| 30 | lvl_clim | 81916 | 45990 | 0.56 |
| 30 | lvl_naive4 | 81325 | 45616 | 0.56 |
| 30 | lvl_nf_nhits | 0 | 0 |  |
| 30 | lvl_nf_tide | 0 | 0 |  |
| 30 | lvl_nf_tide_wx_owx | 0 | 0 |  |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 78371 | 47489 | 0.61 |
| 30 | lvl_wt56 | 81916 | 43777 | 0.53 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie (counted wrong); CI over (origin, park).

**day comparison winner accuracy**

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_nf_tide_wx_owx | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|---|
| d1-7 | 0.23 [0.22, 0.24] | 0.27 [0.26, 0.28] | 0.43 [0.42, 0.44] | 0.09 [0.08, 0.09] | 0.13 [0.13, 0.14] | 0.20 [0.20, 0.21] | 0.50 [0.49, 0.51] | 0.44 [0.43, 0.45] | 0.25 [0.24, 0.26] |
| d8-30 | 0.16 [0.16, 0.17] | 0.26 [0.26, 0.27] | 0.33 [0.32, 0.33] |  |  |  |  | 0.41 [0.41, 0.42] | 0.20 [0.20, 0.21] |
| d31-90 | 0.14 [0.14, 0.15] | 0.28 [0.27, 0.29] | 0.29 [0.28, 0.30] |  |  |  |  | 0.39 [0.38, 0.40] | 0.18 [0.17, 0.19] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_nf_tide_wx_owx | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| d1-7 | -0.26 [-0.29, -0.24] | -0.24 [-0.29, -0.20] | -0.07 [-0.09, -0.04] | -0.42 [-0.45, -0.39] | -0.37 [-0.40, -0.34] | -0.29 [-0.33, -0.26] | -0.06 [-0.09, -0.04] | -0.25 [-0.29, -0.22] | lvl_snaive7 |
| d8-30 | -0.14 [-0.18, -0.12] | -0.06 [-0.10, -0.03] |  |  |  |  | 0.09 [0.06, 0.12]* | -0.12 [-0.15, -0.10] | lvl_naive4 |
| d31-90 | -0.13 [-0.17, -0.10] | -0.01 [-0.05, 0.04] |  |  |  |  | 0.11 [0.07, 0.14]* | -0.11 [-0.14, -0.09] | lvl_naive4 |

| lead | model | tie_rate |
|---|---|---|
| d1-7 | lvl_naive4 | 0.40 |
| d1-7 | lvl_wt56 | 0.65 |
| d1-7 | lvl_snaive7 | 0.30 |
| d1-7 | lvl_clim | 0.63 |
| d1-7 | lvl_tft | 0.41 |
| d1-7 | lvl_cbd | 0.65 |
| d1-7 | lvl_nf_tide | 0.79 |
| d1-7 | lvl_nf_nhits | 0.84 |
| d1-7 | lvl_nf_tide_wx_owx | 0.72 |
| d8-30 | lvl_naive4 | 0.48 |
| d8-30 | lvl_wt56 | 0.67 |
| d8-30 | lvl_clim | 0.63 |
| d8-30 | lvl_tft | 0.41 |
| d8-30 | lvl_cbd | 0.72 |
| d31-90 | lvl_naive4 | 0.50 |
| d31-90 | lvl_wt56 | 0.69 |
| d31-90 | lvl_clim | 0.61 |
| d31-90 | lvl_tft | 0.42 |
| d31-90 | lvl_cbd | 0.73 |

Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the 1st and 15th):

| model | star_precision | star_recall | pred_stars | true_stars |
|---|---|---|---|---|
| lvl_cbd | 0.22 [0.14, 0.31] | 0.07 [0.04, 0.11] | 223 | 682 |
| lvl_clim | 0.45 [0.37, 0.52] | 0.14 [0.10, 0.18] | 312 | 1014 |
| lvl_naive4 | 0.33 [0.28, 0.38] | 0.23 [0.20, 0.27] | 719 | 1002 |
| lvl_nf_nhits | 0.44 [0.20, 0.67] | 0.05 [0.01, 0.10] | 16 | 140 |
| lvl_nf_tide | 0.43 [0.31, 0.56] | 0.16 [0.10, 0.24] | 53 | 140 |
| lvl_nf_tide_wx_owx | 0.56 [0.42, 0.67] | 0.24 [0.15, 0.33] | 59 | 140 |
| lvl_snaive7 | 0.38 [0.27, 0.49] | 0.34 [0.24, 0.45] | 98 | 108 |
| lvl_tft | 0.36 [0.31, 0.41] | 0.28 [0.24, 0.33] | 792 | 1010 |
| lvl_wt56 | 0.29 [0.23, 0.36] | 0.13 [0.10, 0.16] | 452 | 1014 |

## D8 — uncertainty

**What production states** (`forecast-accuracy.service.ts`): MAE of TFT's predicted peak against the day's max hourly P90, by predicted band × lead bucket over the 45 days before the origin (cells with ≥ 500 comparisons). Realised = the same error of the TFT forecasts served at the origin; ratio > 1 = production understates its error.

| bucket | band | n | realised_mae | stated_mae | ratio |
|---|---|---|---|---|---|
| d1 | busy | 7944.00 | 22.67 | 21.52 | 1.05 |
| d1 | mid | 27108.00 | 14.25 | 13.78 | 1.03 |
| d1 | quiet | 69175.00 | 6.91 | 6.73 | 1.03 |
| d14 | busy | 44794.00 | 26.34 | 23.93 | 1.10 |
| d14 | mid | 145602.00 | 16.06 | 15.89 | 1.01 |
| d14 | quiet | 372562.00 | 7.61 | 7.97 | 0.96 |
| d3 | busy | 15359.00 | 23.04 | 21.91 | 1.05 |
| d3 | mid | 52070.00 | 14.60 | 14.20 | 1.03 |
| d3 | quiet | 131394.00 | 6.99 | 6.91 | 1.01 |
| d30 | busy | 80730.00 | 28.65 | 24.64 | 1.16 |
| d30 | mid | 242896.00 | 16.67 | 16.79 | 0.99 |
| d30 | quiet | 596395.00 | 7.50 | 8.69 | 0.86 |
| d60 | busy | 68357.00 | 29.11 | 24.90 | 1.17 |
| d60 | mid | 193499.00 | 17.17 | 17.84 | 0.96 |
| d60 | quiet | 414265.00 | 7.81 | 10.17 | 0.77 |
| d7 | busy | 28915.00 | 24.25 | 22.82 | 1.06 |
| d7 | mid | 96225.00 | 15.20 | 14.74 | 1.03 |
| d7 | quiet | 245522.00 | 7.26 | 7.22 | 1.01 |

Empirical-quantile coverage (target 0.80 / 0.95) and the share of truth slots that HAVE an interval:

| L | wt_med cov_q80 | wt_med cov_q95 | wt_med field_coverage | nf_tide cov_q80 | nf_tide cov_q95 | nf_tide field_coverage | nf_nhits cov_q80 | nf_nhits cov_q95 | nf_nhits field_coverage | nf_tide_wx_owx cov_q80 | nf_tide_wx_owx cov_q95 | nf_tide_wx_owx field_coverage |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.84 | 0.92 | 0.97 | 0.79 | 0.93 | 0.97 | 0.75 | 0.89 | 0.97 | 0.79 | 0.92 | 0.97 |
| 1 | 0.84 | 0.92 | 0.97 | 0.79 | 0.94 | 0.97 | 0.77 | 0.90 | 0.97 | 0.80 | 0.93 | 0.97 |
| 2 | 0.84 | 0.92 | 0.97 | 0.79 | 0.94 | 0.97 | 0.76 | 0.90 | 0.97 | 0.79 | 0.93 | 0.97 |
| 3 | 0.84 | 0.92 | 0.97 | 0.81 | 0.94 | 0.96 | 0.76 | 0.89 | 0.96 | 0.80 | 0.94 | 0.96 |
| 4 | 0.84 | 0.92 | 0.97 | 0.80 | 0.94 | 0.96 | 0.77 | 0.89 | 0.96 | 0.79 | 0.94 | 0.96 |
| 5 | 0.84 | 0.92 | 0.97 | 0.81 | 0.94 | 0.96 | 0.77 | 0.90 | 0.96 | 0.82 | 0.94 | 0.96 |
| 6 | 0.84 | 0.92 | 0.97 | 0.81 | 0.94 | 0.96 | 0.76 | 0.90 | 0.96 | 0.81 | 0.94 | 0.96 |
| 7 | 0.84 | 0.91 | 0.96 | 0.81 | 0.94 | 0.96 | 0.77 | 0.91 | 0.96 | 0.81 | 0.94 | 0.96 |
| 10 | 0.84 | 0.91 | 0.97 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 14 | 0.84 | 0.91 | 0.96 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 21 | 0.85 | 0.91 | 0.97 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 30 | 0.86 | 0.91 | 0.96 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 45 | 0.83 | 0.89 | 0.93 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 60 | 0.90 | 0.94 | 0.94 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |

Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:

| model | L | realised | stated_weighted | ratio |
|---|---|---|---|---|
| snaive7 | 0 | 8.52 | 8.64 | 0.99 |
| snaive7 | 1 | 8.48 | 8.63 | 0.98 |
| snaive7 | 3 | 8.40 | 8.61 | 0.98 |
| wt_med | 0 | 7.93 | 7.93 | 1.00 |
| wt_med | 1 | 8.04 | 7.97 | 1.01 |
| wt_med | 3 | 8.18 | 8.03 | 1.02 |
| wt_med | 7 | 8.46 | 8.33 | 1.02 |
| wt_med | 14 | 8.99 | 8.36 | 1.07 |
| clim | 0 | 8.18 | 8.25 | 0.99 |
| clim | 1 | 8.24 | 8.22 | 1.00 |
| clim | 3 | 8.30 | 8.28 | 1.00 |
| clim | 7 | 8.33 | 8.37 | 0.99 |
| clim | 14 | 8.34 | 8.40 | 0.99 |
| h5 | 0 | 7.82 | 7.85 | 1.00 |
| h5 | 1 | 7.92 | 7.89 | 1.00 |
| h5 | 3 | 8.04 | 7.94 | 1.01 |
| h5 | 7 | 8.29 | 8.24 | 1.01 |
| h5 | 14 | 8.77 | 8.27 | 1.06 |
| lvlh5_naive | 0 | 8.07 | 8.08 | 1.00 |
| lvlh5_naive | 1 | 8.10 | 8.11 | 1.00 |
| lvlh5_naive | 3 | 8.15 | 8.19 | 1.00 |
| lvlh5_naive | 7 | 8.69 | 8.66 | 1.00 |
| lvlh5_naive | 14 | 9.22 | 8.96 | 1.03 |
| lvlh5_tft | 0 | 7.72 | 7.77 | 0.99 |
| lvlh5_tft | 1 | 7.83 | 7.87 | 0.99 |
| lvlh5_tft | 3 | 8.00 | 8.02 | 1.00 |
| lvlh5_tft | 7 | 8.41 | 8.56 | 0.98 |
| lvlh5_tft | 14 | 8.85 | 8.98 | 0.98 |
| lvlh5_cbd | 0 | 10.25 | 10.62 | 0.96 |
| lvlh5_cbd | 1 | 10.21 | 10.62 | 0.96 |
| lvlh5_cbd | 3 | 10.18 | 10.41 | 0.98 |
| lvlh5_cbd | 7 | 10.53 | 11.20 | 0.94 |
| lvlh5_cbd | 14 | 10.86 | 11.03 | 0.98 |
| prod_served | 3 | 8.49 | 8.61 | 0.99 |
| prod_served | 7 | 8.97 | 9.23 | 0.97 |
| prod_served | 14 | 9.38 | 10.10 | 0.93 |
| prod_served_lin | 3 | 8.47 | 8.58 | 0.99 |
| prod_served_lin | 7 | 8.94 | 9.20 | 0.97 |
| prod_served_lin | 14 | 9.36 | 10.06 | 0.93 |
| nf_tide | 0 | 7.78 | 7.81 | 1.00 |
| nf_tide | 1 | 8.16 | 8.08 | 1.01 |
| nf_tide | 3 | 8.55 | 8.29 | 1.03 |
| nf_tide | 7 | 8.94 | 8.82 | 1.01 |
| nf_tide_x_h5 | 1 | 8.25 | 8.28 | 1.00 |
| nf_tide_x_h5 | 3 | 8.46 | 8.32 | 1.02 |
| nf_tide_x_h5 | 7 | 8.57 | 8.68 | 0.99 |
| nf_nhits | 0 | 9.18 | 9.24 | 0.99 |
| nf_nhits | 1 | 9.21 | 9.20 | 1.00 |
| nf_nhits | 3 | 9.25 | 9.17 | 1.01 |
| nf_nhits | 7 | 9.20 | 9.42 | 0.98 |
| nf_nhits_x_h5 | 1 | 9.04 | 9.06 | 1.00 |
| nf_nhits_x_h5 | 3 | 9.26 | 9.24 | 1.00 |
| nf_nhits_x_h5 | 7 | 9.02 | 9.38 | 0.96 |
| nf_tide_wx_owx | 0 | 7.64 | 7.64 | 1.00 |
| nf_tide_wx_owx | 1 | 8.03 | 7.96 | 1.01 |
| nf_tide_wx_owx | 3 | 8.31 | 8.06 | 1.03 |
| nf_tide_wx_owx | 7 | 8.74 | 8.65 | 1.01 |
| nf_tide_wx_owx_x_h5 | 1 | 8.03 | 8.07 | 1.00 |
| nf_tide_wx_owx_x_h5 | 3 | 8.23 | 8.12 | 1.01 |
| nf_tide_wx_owx_x_h5 | 7 | 8.36 | 8.48 | 0.99 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_wx_owx | nf_tide_wx_owx_x_h5 | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.92 | 0.98 | 0.84 | 0.95 | 0.96 | 0.97 |  | 0.97 | 0.97 |  |  | 0.97 | 0.95 |  |  | 0.83 | 0.97 |
| 1 | 0.92 | 0.98 | 0.83 | 0.95 | 0.96 | 0.97 | 0.96 | 0.97 | 0.97 | 0.96 | 0.96 | 0.97 | 0.95 |  |  | 0.84 | 0.97 |
| 2 | 0.92 | 0.98 | 0.83 | 0.95 | 0.96 | 0.97 | 0.96 | 0.97 | 0.97 | 0.96 | 0.96 | 0.97 | 0.95 |  |  | 0.84 | 0.97 |
| 3 | 0.92 | 0.98 | 0.84 | 0.95 | 0.96 | 0.96 | 0.96 | 0.96 | 0.96 | 0.96 | 0.96 | 0.97 | 0.95 | 0.94 | 0.94 | 0.84 | 0.97 |
| 4 | 0.92 | 0.98 | 0.85 | 0.95 | 0.96 | 0.96 | 0.95 | 0.96 | 0.96 | 0.95 | 0.95 | 0.97 | 0.95 | 0.94 | 0.94 | 0.84 | 0.97 |
| 5 | 0.92 | 0.98 | 0.85 | 0.96 | 0.95 | 0.96 | 0.95 | 0.96 | 0.96 | 0.95 | 0.95 | 0.97 | 0.96 | 0.94 | 0.94 | 0.84 | 0.97 |
| 6 | 0.92 | 0.97 | 0.85 | 0.96 | 0.95 | 0.96 | 0.95 | 0.96 | 0.96 | 0.95 | 0.95 | 0.97 | 0.96 | 0.94 | 0.94 | 0.84 | 0.97 |
| 7 | 0.92 | 0.97 | 0.84 | 0.95 | 0.95 | 0.96 | 0.95 | 0.96 | 0.96 | 0.95 | 0.95 | 0.97 | 0.95 | 0.94 | 0.94 |  | 0.96 |
| 10 | 0.93 | 0.97 | 0.85 | 0.95 | 0.96 |  |  |  |  |  |  | 0.97 | 0.95 | 0.94 | 0.94 |  | 0.97 |
| 14 | 0.94 | 0.97 | 0.83 | 0.95 | 0.96 |  |  |  |  |  |  | 0.97 | 0.95 | 0.95 | 0.95 |  | 0.96 |
| 21 | 0.95 | 0.98 | 0.83 | 0.96 | 0.96 |  |  |  |  |  |  | 0.97 | 0.96 | 0.96 | 0.96 |  | 0.97 |
| 30 | 0.94 | 0.97 | 0.83 | 0.96 | 0.95 |  |  |  |  |  |  | 0.97 | 0.96 | 0.96 | 0.96 |  | 0.96 |
| 45 | 0.87 | 0.95 | 0.21 | 0.92 | 0.93 |  |  |  |  |  |  | 0.94 | 0.92 | 0.90 | 0.90 |  | 0.93 |
| 60 | 0.93 | 0.95 |  | 0.92 | 0.01 |  |  |  |  |  |  | 0.94 | 0.92 | 0.01 | 0.01 |  | 0.94 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | lvl_nf_tide | lvl_nf_nhits | lvl_nf_tide_wx_owx | n_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.96 | 0.99 | 0.90 | 0.93 | 0.98 | 0.81 | 0.97 | 0.97 | 0.97 | 43416 |
| 2 | 0.96 | 0.99 | 0.90 | 0.94 | 0.98 | 0.82 | 0.98 | 0.98 | 0.98 | 41872 |
| 3 | 0.97 | 0.99 | 0.90 | 0.94 | 0.98 | 0.83 | 0.97 | 0.97 | 0.97 | 40954 |
| 4 | 0.97 | 0.99 | 0.90 | 0.94 | 0.98 | 0.84 | 0.97 | 0.97 | 0.97 | 40114 |
| 5 | 0.97 | 0.99 | 0.90 | 0.93 | 0.98 | 0.83 | 0.97 | 0.97 | 0.97 | 39258 |
| 6 | 0.97 | 0.99 | 0.90 | 0.93 | 0.98 | 0.83 | 0.97 | 0.97 | 0.97 | 38354 |
| 7 | 0.96 | 0.99 | 0.00 | 0.93 | 0.98 | 0.83 | 0.97 | 0.97 | 0.97 | 37412 |
| 10 | 0.97 | 0.99 | 0.00 | 0.95 | 0.98 | 0.83 | 0.00 | 0.00 | 0.00 | 34447 |
| 14 | 0.97 | 0.99 | 0.00 | 0.95 | 0.98 | 0.82 | 0.00 | 0.00 | 0.00 | 30978 |
| 21 | 0.98 | 1.00 | 0.00 | 0.97 | 0.98 | 0.81 | 0.00 | 0.00 | 0.00 | 25040 |
| 30 | 0.98 | 1.00 | 0.00 | 0.97 | 0.97 | 0.80 | 0.00 | 0.00 | 0.00 | 18626 |
| 45 | 0.94 | 0.98 | 0.00 | 0.88 | 0.98 | 0.24 | 0.00 | 0.00 | 0.00 | 10240 |
| 60 | 0.91 | 0.96 | 0.00 | 0.93 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 508 |

Forecast present vs ride operated. The baselines have **no openness logic** — they forecast whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never opened still got a curve; it is the bar a model with an open/closed decision has to beat:

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | prod_served_lin P(fc|operated) | prod_served_lin P(fc|not operated) | nf_tide P(fc|operated) | nf_tide P(fc|not operated) | nf_tide_x_h5 P(fc|operated) | nf_tide_x_h5 P(fc|not operated) | nf_nhits P(fc|operated) | nf_nhits P(fc|not operated) | nf_nhits_x_h5 P(fc|operated) | nf_nhits_x_h5 P(fc|not operated) | nf_tide_wx_owx P(fc|operated) | nf_tide_wx_owx P(fc|not operated) | nf_tide_wx_owx_x_h5 P(fc|operated) | nf_tide_wx_owx_x_h5 P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.74 | 0.26 | 0.91 | 0.82 | 0.84 | 0.80 | 0.92 | 0.84 | 0.84 | 0.72 | 0.88 | 0.78 | 0.71 | 0.53 | 0.00 | 0.00 | 0.00 | 0.00 | 0.87 | 0.67 | 0.00 | 0.00 | 0.87 | 0.67 | 0.00 | 0.00 | 0.87 | 0.67 | 0.00 | 0.00 | 145327.00 | 13616.00 |
| 1 | 0.74 | 0.26 | 0.91 | 0.82 | 0.84 | 0.80 | 0.92 | 0.84 | 0.85 | 0.72 | 0.88 | 0.78 | 0.70 | 0.52 | 0.00 | 0.00 | 0.00 | 0.00 | 0.87 | 0.68 | 0.86 | 0.65 | 0.87 | 0.68 | 0.86 | 0.65 | 0.87 | 0.68 | 0.86 | 0.65 | 142115.00 | 13480.00 |
| 2 | 0.74 | 0.25 | 0.91 | 0.83 | 0.84 | 0.80 | 0.92 | 0.84 | 0.85 | 0.72 | 0.88 | 0.78 | 0.70 | 0.53 | 0.00 | 0.00 | 0.00 | 0.00 | 0.87 | 0.69 | 0.85 | 0.66 | 0.87 | 0.69 | 0.85 | 0.66 | 0.87 | 0.69 | 0.85 | 0.66 | 138986.00 | 13306.00 |
| 3 | 0.74 | 0.25 | 0.91 | 0.83 | 0.84 | 0.80 | 0.92 | 0.84 | 0.85 | 0.73 | 0.88 | 0.78 | 0.72 | 0.55 | 0.81 | 0.74 | 0.81 | 0.74 | 0.87 | 0.69 | 0.85 | 0.67 | 0.87 | 0.69 | 0.85 | 0.67 | 0.87 | 0.69 | 0.85 | 0.67 | 136069.00 | 13039.00 |
| 4 | 0.74 | 0.25 | 0.91 | 0.83 | 0.84 | 0.80 | 0.92 | 0.84 | 0.85 | 0.73 | 0.88 | 0.79 | 0.72 | 0.56 | 0.81 | 0.75 | 0.81 | 0.75 | 0.86 | 0.70 | 0.85 | 0.67 | 0.86 | 0.70 | 0.85 | 0.67 | 0.86 | 0.70 | 0.85 | 0.67 | 133232.00 | 12753.00 |
| 5 | 0.74 | 0.25 | 0.91 | 0.83 | 0.84 | 0.81 | 0.92 | 0.85 | 0.85 | 0.73 | 0.88 | 0.79 | 0.72 | 0.55 | 0.81 | 0.75 | 0.81 | 0.75 | 0.86 | 0.70 | 0.85 | 0.68 | 0.86 | 0.70 | 0.85 | 0.68 | 0.86 | 0.70 | 0.85 | 0.68 | 130375.00 | 12490.00 |
| 6 | 0.74 | 0.25 | 0.91 | 0.83 | 0.83 | 0.80 | 0.92 | 0.85 | 0.85 | 0.73 | 0.88 | 0.79 | 0.72 | 0.55 | 0.81 | 0.75 | 0.81 | 0.75 | 0.86 | 0.71 | 0.85 | 0.68 | 0.86 | 0.71 | 0.85 | 0.68 | 0.86 | 0.71 | 0.85 | 0.68 | 127440.00 | 12332.00 |
| 7 | 0.00 | 0.00 | 0.91 | 0.84 | 0.83 | 0.80 | 0.92 | 0.85 | 0.84 | 0.74 | 0.88 | 0.79 | 0.72 | 0.55 | 0.80 | 0.75 | 0.80 | 0.75 | 0.87 | 0.71 | 0.85 | 0.69 | 0.87 | 0.71 | 0.85 | 0.69 | 0.87 | 0.71 | 0.85 | 0.69 | 124451.00 | 12177.00 |
| 10 | 0.00 | 0.00 | 0.91 | 0.84 | 0.83 | 0.80 | 0.92 | 0.85 | 0.85 | 0.75 | 0.88 | 0.80 | 0.72 | 0.57 | 0.81 | 0.76 | 0.81 | 0.76 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 115655.00 | 11657.00 |
| 14 | 0.00 | 0.00 | 0.91 | 0.84 | 0.83 | 0.80 | 0.91 | 0.85 | 0.84 | 0.75 | 0.89 | 0.80 | 0.72 | 0.56 | 0.80 | 0.75 | 0.80 | 0.75 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 104948.00 | 10925.00 |
| 21 | 0.00 | 0.00 | 0.91 | 0.84 | 0.82 | 0.79 | 0.91 | 0.85 | 0.84 | 0.75 | 0.88 | 0.79 | 0.71 | 0.57 | 0.80 | 0.75 | 0.80 | 0.75 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 86425.00 | 9461.00 |
| 30 | 0.00 | 0.00 | 0.90 | 0.84 | 0.80 | 0.78 | 0.91 | 0.85 | 0.84 | 0.75 | 0.87 | 0.78 | 0.72 | 0.60 | 0.79 | 0.75 | 0.79 | 0.75 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 64066.00 | 7377.00 |
| 45 | 0.00 | 0.00 | 0.89 | 0.85 | 0.78 | 0.82 | 0.90 | 0.86 | 0.82 | 0.78 | 0.87 | 0.79 | 0.17 | 0.17 | 0.76 | 0.73 | 0.76 | 0.73 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 32520.00 | 3512.00 |
| 60 | 0.00 | 0.00 | 0.88 | 0.87 | 0.81 | 0.90 | 0.88 | 0.88 | 0.79 | 0.78 | 0.01 | 0.01 | 0.00 | 0.00 | 0.01 | 0.01 | 0.01 | 0.01 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 1620.00 | 157.00 |
