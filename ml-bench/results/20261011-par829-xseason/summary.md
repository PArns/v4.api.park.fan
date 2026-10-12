# ml-bench results

Run: `20261011-par829-xseason` · generated 2026-10-11T23:55+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with TARGET season. The common-window pass mixes lead with ORIGIN season instead. Both are confounded; neither alone identifies the horizon curve.

- code: git `af043b170441a23be21f1773b06760bc38f6bb50`, sources sha256 `c29ab0ab897ea60b`, image `sha256:186ebb484468`; plug-ins ['nf_nhits', 'nf_tide']
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 301.9 shard-minutes over 2 shard(s)
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
| MAE | UC1 | all | nf_tide | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | all | persistence | True | park-day | 8 | 8 | m105 | m105 |  |
| MAE | UC1 | busy | nf_nhits | False | park-day | 8 | 8 |  | m105 |  |
| MAE | UC1 | busy | nf_tide | False | park-day | 8 | 8 |  | m120 |  |
| MAE | UC1 | busy | persistence | True | park-day | 8 | 8 | m120 | m120 |  |
| MAE | UC2 | all | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | all | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | h5 | False | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2 | busy | nf_tide | False | park-day | 2 | 2 | 0 | 0 |  |
| MAE | UC2 | busy | wt_med | True | park-day | 2 | 2 | 1 | 1 |  |
| MAE | UC2-intraday | all | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | all | nf_tide | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | all | wt_med | True | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | h5 | False | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC2-intraday | busy | nf_tide | False | park-day | 3 | 3 | h4-8 | h4-8 |  |
| MAE | UC2-intraday | busy | wt_med | True | park-day | 3 | 3 | h8+ | h8+ |  |
| MAE | UC3 | all | h5 | False | park-day | 14 | 14 | 45 | 90 |  |
| MAE | UC3 | all | wt_med | True | park-day | 14 | 14 | 45 | 45 |  |
| MAE | UC3 | busy | h5 | False | park-day | 14 | 14 | 45 | 90 |  |
| MAE | UC3 | busy | wt_med | True | park-day | 14 | 14 | 45 | 45 |  |
| MAE, schedule known at origin | UC3 | all | h5 | False | park-day | 15 | 15 | 45 | 90 |  |
| MAE, schedule known at origin | UC3 | all | wt_med | True | park-day | 15 | 15 | 45 | 90 |  |
| MAE, schedule projected at origin | UC3 | all | h5 | False | park-day | 15 | 15 |  | 30 |  |
| MAE, schedule projected at origin | UC3 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | False | park-day | 15 | 15 | 90 | 90 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | False | park-day | 15 | 15 | 45 | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_tft | False | park-day | 14 | 13 | 45 | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served | False | park-day | 11 | 10 |  | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served_lin | False | park-day | 11 | 10 |  | 45 |  |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | True | park-day | 15 | 15 | 45 | 45 |  |
| UC4 Spearman within park-month | UC4 | all | lvl_naive4 | True | park-month | 16 | 16 |  | 120 |  |
| best-time hit rate (top-2) | D3 | all | clim | True | park-day | 15 | 15 |  | 60 |  |
| best-time hit rate (top-2) | D3 | all | h5 | False | park-day | 15 | 15 | 60 | 60 |  |
| best-time hit rate (top-2) | D3 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| best-time hit rate (±30 min) | D3 | all | h5 | False | park-day | 15 | 15 | 60 | 60 |  |
| best-time hit rate (±30 min) | D3 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| best-time regret (min) | D3 | all | h5 | False | park-day | 15 | 15 | 60 | 60 |  |
| best-time regret (min) | D3 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| crowd bucket exact | D6 | all | lvl_naive4 | True | park-day | 16 | 16 |  | 120 |  |
| crowd bucket exact | D6 | all | lvl_snaive7 | True | park-day | 6 | 6 | 1 | 1 |  |
| day comparison winner accuracy | D7 | all | lvl_naive4 | True | park-origin | 3 | 3 |  | d31-90 |  |
| dayPeak Spearman across rides | UC3 | all | nf_tide | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak Spearman across rides | UC3 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| dayPeak abs error (min) | UC3 | all | nf_tide | False | park-day | 8 | 8 | 0 | 0 |  |
| dayPeak abs error (min) | UC3 | all | prod_served | False | park-day | 11 | 10 | 4 | 4 |  |
| dayPeak abs error (min) | UC3 | all | prod_served_lin | False | park-day | 11 | 10 | 4 | 4 |  |
| dayPeak abs error (min) | UC3 | all | wt_med | True | park-day | 15 | 15 | 21 | 21 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | False | park-day | 15 | 15 | 90 | 90 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | False | park-day | 15 | 15 | 90 | 90 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | False | park-day | 14 | 13 | 45 | 45 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_nhits | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_tide | False | park-day | 8 | 8 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served_lin | False | park-day | 11 | 10 | 5 | 5 |  |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | True | park-day | 15 | 15 | 30 | 90 |  |
| first-hour MAE (opening-aligned) | D5 | all | h5 | False | park-day | 15 | 15 | 45 | 45 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 |  | 10 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | False | park-day | 15 | 15 | 30 | 30 |  |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | False | park-day | 14 | 13 | 21 | 21 |  |
| first-hour MAE (opening-aligned) | D5 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE (opening-aligned) | D5 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE (opening-aligned) | D5 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| first-hour MAE, schedule known at origin | D5 | all | h5 | False | park-day | 15 | 15 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | False | park-day | 13 | 12 |  | 30 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 15 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | False | park-day | 14 | 13 | 45 | 45 |  |
| first-hour MAE, schedule known at origin | D5 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE, schedule known at origin | D5 | all | wt_med | True | park-day | 15 | 15 | 30 | 30 |  |
| first-hour MAE, schedule projected at origin | D5 | all | h5 | False | park-day | 15 | 15 |  | 10 |  |
| first-hour MAE, schedule projected at origin | D5 | all | lvlh5_naive | False | park-day | 15 | 15 |  | 10 |  |
| first-hour MAE, schedule projected at origin | D5 | all | nf_nhits_x_h5 | False | park-day | 7 | 7 | 4 | 7 |  |
| first-hour MAE, schedule projected at origin | D5 | all | nf_tide_x_h5 | False | park-day | 7 | 7 | 7 | 7 |  |
| first-hour MAE, schedule projected at origin | D5 | all | wt_med | True | park-day | 15 | 15 | 21 | 21 |  |
| live-window MAE (headliners) | D2 | headliners | persistence | True | park-day | 1 | 1 | w0-45 | w0-45 |  |
| next-best precision | D1 | all | lvlh5_naive | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision | D1 | all | nf_tide | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | nf_tide | False | park-day | 2 | 2 | 0-120 | 0-120 |  |
| next-best precision (top-3) | D1 | all | wt_med | True | park-day | 2 | 2 | 0-60 | 0-60 |  |
| next-best recall | D1 | all | snaive7 | True | park-day | 2 | 2 | 0-120 | 0-120 |  |
| optimiser regret (min per park-day) | D4 | all | h5 | False | park-day | 5 | 5 |  | 30 |  |
| optimiser regret (min per park-day) | D4 | all | lvlh5_naive | False | park-day | 5 | 5 |  | 3 |  |
| optimiser regret (min per park-day) | D4 | all | nf_nhits_x_h5 | False | park-day | 3 | 3 | 7 | 7 |  |
| optimiser regret (min per park-day) | D4 | all | nf_tide_x_h5 | False | park-day | 3 | 3 | 7 | 7 |  |
| optimiser regret (min per park-day) | D4 | all | wt_med | True | park-day | 5 | 5 | 1 | 1 |  |
| rope-drop worth agreement | D5 | all | wt_med | True | park-day | 15 | 15 | 5 | 14 |  |


**Never tested** — `leads_tested = 0` means no lead passed the gates, which is NOT "nothing won". The reason per metric × model:

| metric | uc | segment | model | unit | leads_scored | not_tested_because |
|---|---|---|---|---|---|---|
| dayPeak pairwise ordering (headliners) | D4 | all | snaive7 | park-day | 7 | 0: coverage 0.25 < 0.30 against one of the candidates; 1: coverage 0.26 < 0.30 against one of the candidates; 2: coverage 0.26 < 0.30 against one of the candidates; 3: coverage 0.26 < 0.30 against one of the candidates; 4: coverage 0.26 < 0.30 against one of the candidates; 5: coverage 0.26 < 0.30 against one of the candidates |

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
| UC2 | all | 0 | wt_med | h5 | 7.338 | -0.040 | -0.055 | -0.027 | -0.045 | -0.035 | True | 0.997 | 1 | 9 | 170 |
| UC2 | busy | 0 | wt_med | nf_tide | 13.900 | -0.340 | -0.614 | -0.087 | -0.442 | -0.236 | True | 0.976 | 2 | 9 | 170 |
| UC3 | all | 1 | wt_med | h5 | 7.448 | -0.046 | -0.061 | -0.031 | -0.051 | -0.041 | True | 0.995 | 1 | 11 | 170 |
| UC3 | busy | 1 | wt_med | h5 | 14.366 | -0.129 | -0.165 | -0.097 | -0.143 | -0.117 | True | 0.996 | 1 | 11 | 170 |
| UC3 | all | 2 | wt_med | h5 | 7.480 | -0.049 | -0.064 | -0.034 | -0.054 | -0.044 | True | 0.995 | 1 | 11 | 170 |
| UC3 | busy | 2 | wt_med | h5 | 14.399 | -0.137 | -0.175 | -0.104 | -0.151 | -0.124 | True | 0.996 | 1 | 11 | 170 |
| UC3 | all | 3 | wt_med | h5 | 7.522 | -0.051 | -0.066 | -0.037 | -0.056 | -0.046 | True | 0.994 | 1 | 13 | 170 |
| UC3 | busy | 3 | wt_med | h5 | 14.450 | -0.142 | -0.178 | -0.109 | -0.155 | -0.130 | True | 0.995 | 1 | 13 | 170 |
| UC3 | all | 4 | wt_med | h5 | 7.553 | -0.051 | -0.066 | -0.037 | -0.056 | -0.046 | True | 0.991 | 1 | 13 | 170 |
| UC3 | busy | 4 | wt_med | h5 | 14.476 | -0.143 | -0.180 | -0.109 | -0.157 | -0.130 | True | 0.993 | 1 | 13 | 170 |
| UC3 | all | 5 | wt_med | h5 | 7.577 | -0.052 | -0.067 | -0.038 | -0.057 | -0.047 | True | 0.988 | 1 | 13 | 170 |
| UC3 | busy | 5 | wt_med | h5 | 14.512 | -0.146 | -0.182 | -0.114 | -0.160 | -0.133 | True | 0.991 | 1 | 13 | 170 |
| UC3 | all | 6 | wt_med | h5 | 7.623 | -0.055 | -0.071 | -0.041 | -0.060 | -0.050 | True | 0.984 | 1 | 13 | 170 |
| UC3 | busy | 6 | wt_med | h5 | 14.590 | -0.152 | -0.190 | -0.118 | -0.165 | -0.140 | True | 0.988 | 1 | 13 | 170 |
| UC3 | all | 7 | wt_med | h5 | 7.717 | -0.060 | -0.076 | -0.045 | -0.065 | -0.055 | True | 0.995 | 1 | 12 | 170 |
| UC3 | busy | 7 | wt_med | h5 | 14.759 | -0.162 | -0.201 | -0.126 | -0.175 | -0.150 | True | 0.997 | 1 | 12 | 170 |
| UC3 | all | 10 | wt_med | h5 | 7.830 | -0.063 | -0.080 | -0.048 | -0.068 | -0.058 | True | 0.995 | 1 | 8 | 170 |
| UC3 | busy | 10 | wt_med | h5 | 14.933 | -0.169 | -0.210 | -0.132 | -0.182 | -0.157 | True | 0.997 | 1 | 8 | 170 |
| UC3 | all | 14 | wt_med | h5 | 7.988 | -0.066 | -0.084 | -0.050 | -0.071 | -0.061 | True | 0.994 | 1 | 8 | 170 |
| UC3 | busy | 14 | wt_med | h5 | 15.190 | -0.174 | -0.217 | -0.133 | -0.185 | -0.161 | True | 0.997 | 1 | 8 | 170 |
| UC3 | all | 21 | wt_med | h5 | 8.179 | -0.072 | -0.091 | -0.054 | -0.077 | -0.066 | True | 0.993 | 1 | 8 | 170 |
| UC3 | busy | 21 | wt_med | h5 | 15.487 | -0.185 | -0.235 | -0.142 | -0.198 | -0.172 | True | 0.996 | 1 | 8 | 170 |
| UC3 | all | 30 | wt_med | h5 | 8.335 | -0.079 | -0.100 | -0.060 | -0.084 | -0.073 | True | 0.992 | 1 | 8 | 170 |
| UC3 | busy | 30 | wt_med | h5 | 15.737 | -0.204 | -0.258 | -0.157 | -0.216 | -0.190 | True | 0.995 | 1 | 8 | 170 |
| UC3 | all | 45 | wt_med | h5 | 8.463 | -0.098 | -0.119 | -0.076 | -0.103 | -0.092 | True | 0.991 | 1 | 7 | 170 |
| UC3 | busy | 45 | wt_med | h5 | 16.084 | -0.240 | -0.295 | -0.193 | -0.253 | -0.227 | True | 0.994 | 1 | 6 | 170 |
| UC3 | all | 60 | wt_med | wt_med | 8.832 | 0.000 |  |  |  |  | False |  | 0 | 4 | 170 |
| UC3 | busy | 60 | wt_med | wt_med | 16.782 | 0.000 |  |  |  |  | False |  | 0 | 4 | 170 |
| UC3 | all | 90 | wt_med | h5 | 8.754 | -0.096 | -0.120 | -0.074 | -0.102 | -0.090 | True | 0.987 | 1 | 4 | 141 |
| UC3 | busy | 90 | wt_med | h5 | 16.718 | -0.220 | -0.279 | -0.170 | -0.235 | -0.205 | True | 0.991 | 1 | 4 | 141 |
| UC2 | all | 1 | wt_med | h5 | 7.448 | -0.046 | -0.061 | -0.031 | -0.051 | -0.041 | True | 0.995 | 1 | 11 | 170 |
| UC2 | busy | 1 | wt_med | h5 | 14.366 | -0.129 | -0.165 | -0.097 | -0.143 | -0.117 | True | 0.996 | 1 | 11 | 170 |
| UC1 | all | m015 | persistence | persistence | 2.455 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | busy | m015 | persistence | persistence | 4.574 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | all | m030 | persistence | persistence | 4.173 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | busy | m030 | persistence | persistence | 7.929 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | all | m045 | persistence | persistence | 5.088 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | busy | m045 | persistence | persistence | 9.655 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | all | m060 | persistence | persistence | 5.697 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | busy | m060 | persistence | nf_tide | 10.578 | -0.654 | -1.053 | -0.175 | -0.725 | -0.586 | True | 0.959 | 1 | 9 | 170 |
| UC1 | all | m075 | persistence | persistence | 6.271 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | busy | m075 | persistence | persistence | 11.729 | 0.000 |  |  |  |  | False |  | 0 | 9 | 170 |
| UC1 | all | m090 | persistence | nf_tide | 6.354 | -0.514 | -0.769 | -0.222 | -0.557 | -0.475 | True | 0.958 | 1 | 9 | 170 |
| UC1 | busy | m090 | persistence | nf_tide | 11.725 | -1.351 | -1.771 | -0.886 | -1.437 | -1.264 | True | 0.957 | 1 | 9 | 170 |
| UC1 | all | m105 | persistence | nf_tide | 6.474 | -0.844 | -1.108 | -0.551 | -0.889 | -0.802 | True | 0.958 | 1 | 9 | 170 |
| UC1 | busy | m105 | persistence | nf_tide | 11.938 | -1.945 | -2.394 | -1.473 | -2.034 | -1.851 | True | 0.957 | 2 | 9 | 170 |
| UC1 | all | m120 | persistence | nf_tide | 6.677 | -1.067 | -1.338 | -0.769 | -1.111 | -1.025 | True | 0.958 | 1 | 9 | 170 |
| UC1 | busy | m120 | persistence | nf_tide | 12.313 | -2.399 | -2.841 | -1.916 | -2.490 | -2.304 | True | 0.957 | 1 | 9 | 170 |
| UC2-intraday | all | h2-4 | wt_med | nf_tide | 6.820 | -0.565 | -0.734 | -0.425 | -0.611 | -0.524 | True | 0.954 | 2 | 9 | 170 |
| UC2-intraday | busy | h2-4 | wt_med | nf_tide | 12.727 | -1.635 | -1.990 | -1.313 | -1.740 | -1.532 | True | 0.955 | 2 | 9 | 170 |
| UC2-intraday | all | h4-8 | wt_med | nf_tide | 7.417 | -0.221 | -0.345 | -0.108 | -0.268 | -0.179 | True | 0.976 | 2 | 9 | 170 |
| UC2-intraday | busy | h4-8 | wt_med | nf_tide | 13.668 | -0.843 | -1.118 | -0.581 | -0.948 | -0.737 | True | 0.950 | 2 | 9 | 170 |
| UC2-intraday | all | h8+ | wt_med | h5 | 7.228 | -0.061 | -0.092 | -0.030 | -0.073 | -0.051 | True | 0.995 | 1 | 8 | 170 |
| UC2-intraday | busy | h8+ | wt_med | h5 | 13.163 | -0.150 | -0.225 | -0.080 | -0.176 | -0.125 | True | 0.997 | 1 | 8 | 170 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | margin_lo_park | margin_hi_park | margin_lo_park_day | margin_hi_park_day | park_day_unit_agrees | winner_paired_share | significant_models | n_tested |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | h5 | 4.074 | -0.070 | -0.126 | -0.026 | -0.087 | -0.055 | True | 0.998 | 3 | 9 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | nf_nhits_x_h5 | 4.126 | -0.079 | -0.141 | -0.031 | -0.095 | -0.065 | True | 0.981 | 5 | 11 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | nf_nhits_x_h5 | 4.144 | -0.083 | -0.146 | -0.034 | -0.099 | -0.068 | True | 0.979 | 6 | 11 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | nf_nhits_x_h5 | 4.159 | -0.086 | -0.151 | -0.036 | -0.102 | -0.071 | True | 0.977 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | nf_tide_x_h5 | 4.169 | -0.088 | -0.153 | -0.037 | -0.104 | -0.074 | True | 0.974 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | nf_tide_x_h5 | 4.184 | -0.091 | -0.157 | -0.039 | -0.106 | -0.076 | True | 0.971 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | nf_tide_x_h5 | 4.212 | -0.092 | -0.158 | -0.040 | -0.109 | -0.077 | True | 0.968 | 6 | 13 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | nf_tide_x_h5 | 4.266 | -0.096 | -0.164 | -0.043 | -0.111 | -0.082 | True | 0.980 | 6 | 12 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | h5 | 4.354 | -0.096 | -0.162 | -0.043 | -0.113 | -0.081 | True | 0.998 | 4 | 8 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | h5 | 4.430 | -0.094 | -0.157 | -0.042 | -0.109 | -0.080 | True | 0.997 | 3 | 8 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | h5 | 4.502 | -0.093 | -0.156 | -0.040 | -0.107 | -0.079 | True | 0.997 | 3 | 8 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | h5 | 4.550 | -0.099 | -0.170 | -0.039 | -0.114 | -0.085 | True | 0.996 | 2 | 8 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | h5 | 4.627 | -0.118 | -0.203 | -0.049 | -0.136 | -0.101 | True | 0.996 | 1 | 7 |
| first-hour MAE (opening-aligned) | D5 | 60 | wt_med | wt_med | 4.883 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE (opening-aligned) | D5 | 90 | wt_med | wt_med | 4.987 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | h5 | 7.325 | -0.046 | -0.064 | -0.030 | -0.052 | -0.041 | True | 0.997 | 1 | 9 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | h5 | 7.451 | -0.052 | -0.069 | -0.035 | -0.058 | -0.046 | True | 0.995 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | h5 | 7.484 | -0.055 | -0.074 | -0.038 | -0.062 | -0.050 | True | 0.994 | 1 | 11 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 7.522 | -0.058 | -0.076 | -0.041 | -0.064 | -0.052 | True | 0.993 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | h5 | 7.543 | -0.058 | -0.076 | -0.041 | -0.064 | -0.052 | True | 0.990 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | h5 | 7.536 | -0.060 | -0.078 | -0.043 | -0.066 | -0.054 | True | 0.987 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 6 | wt_med | h5 | 7.551 | -0.063 | -0.082 | -0.046 | -0.069 | -0.057 | True | 0.984 | 1 | 13 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | h5 | 7.627 | -0.068 | -0.088 | -0.051 | -0.074 | -0.062 | True | 0.996 | 1 | 12 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | h5 | 7.709 | -0.074 | -0.094 | -0.056 | -0.080 | -0.068 | True | 0.995 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 14 | wt_med | h5 | 7.782 | -0.079 | -0.101 | -0.060 | -0.085 | -0.074 | True | 0.995 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 21 | wt_med | h5 | 7.852 | -0.090 | -0.114 | -0.068 | -0.096 | -0.083 | True | 0.994 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 30 | wt_med | h5 | 7.912 | -0.100 | -0.126 | -0.076 | -0.107 | -0.093 | True | 0.992 | 1 | 8 |
| MAE, schedule known at origin | UC3 | 45 | wt_med | h5 | 7.823 | -0.116 | -0.148 | -0.088 | -0.124 | -0.109 | True | 0.990 | 1 | 7 |
| MAE, schedule known at origin | UC3 | 60 | wt_med | wt_med | 8.140 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule known at origin | UC3 | 90 | wt_med | h5 | 7.080 | -0.102 | -0.136 | -0.067 | -0.111 | -0.091 | True | 0.980 | 1 | 4 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | lvlh5_tft | 3.551 | -0.082 | -0.185 | -0.008 | -0.108 | -0.059 | True | 0.554 | 3 | 9 |
| first-hour MAE, schedule known at origin | D5 | 1 | wt_med | h5 | 4.046 | -0.089 | -0.164 | -0.031 | -0.108 | -0.074 | True | 0.997 | 5 | 11 |
| first-hour MAE, schedule known at origin | D5 | 2 | wt_med | nf_nhits_x_h5 | 4.054 | -0.095 | -0.169 | -0.037 | -0.114 | -0.078 | True | 0.980 | 6 | 11 |
| first-hour MAE, schedule known at origin | D5 | 3 | wt_med | nf_nhits_x_h5 | 4.064 | -0.098 | -0.174 | -0.038 | -0.117 | -0.081 | True | 0.978 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 4 | wt_med | nf_nhits_x_h5 | 4.073 | -0.101 | -0.175 | -0.040 | -0.120 | -0.083 | True | 0.976 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 5 | wt_med | nf_nhits_x_h5 | 4.073 | -0.103 | -0.179 | -0.042 | -0.122 | -0.086 | True | 0.973 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 6 | wt_med | nf_nhits_x_h5 | 4.084 | -0.102 | -0.178 | -0.042 | -0.122 | -0.084 | True | 0.970 | 6 | 13 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | nf_nhits_x_h5 | 4.126 | -0.105 | -0.182 | -0.043 | -0.122 | -0.088 | True | 0.982 | 6 | 12 |
| first-hour MAE, schedule known at origin | D5 | 10 | wt_med | h5 | 4.150 | -0.106 | -0.191 | -0.040 | -0.124 | -0.088 | True | 0.998 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 14 | wt_med | h5 | 4.168 | -0.107 | -0.192 | -0.039 | -0.125 | -0.090 | True | 0.997 | 3 | 8 |
| first-hour MAE, schedule known at origin | D5 | 21 | wt_med | h5 | 4.173 | -0.108 | -0.195 | -0.040 | -0.126 | -0.092 | True | 0.997 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 30 | wt_med | h5 | 4.144 | -0.119 | -0.217 | -0.042 | -0.137 | -0.101 | True | 0.997 | 4 | 8 |
| first-hour MAE, schedule known at origin | D5 | 45 | wt_med | lvlh5_tft | 4.013 | -0.192 | -0.347 | -0.067 | -0.248 | -0.145 | True | 0.331 | 3 | 7 |
| first-hour MAE, schedule known at origin | D5 | 60 | wt_med | wt_med | 4.123 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE, schedule known at origin | D5 | 90 | wt_med | wt_med | 3.942 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule projected at origin | UC3 | 0 | wt_med | wt_med | 7.417 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| MAE, schedule projected at origin | UC3 | 1 | wt_med | h5 | 7.432 | -0.020 | -0.038 | -0.001 | -0.029 | -0.011 | True | 0.997 | 1 | 11 |
| MAE, schedule projected at origin | UC3 | 2 | wt_med | h5 | 7.465 | -0.020 | -0.039 | -0.002 | -0.029 | -0.012 | True | 0.996 | 1 | 11 |
| MAE, schedule projected at origin | UC3 | 3 | wt_med | h5 | 7.521 | -0.022 | -0.041 | -0.005 | -0.030 | -0.014 | True | 0.996 | 1 | 13 |
| MAE, schedule projected at origin | UC3 | 4 | wt_med | h5 | 7.592 | -0.022 | -0.042 | -0.004 | -0.031 | -0.013 | True | 0.996 | 1 | 13 |
| MAE, schedule projected at origin | UC3 | 5 | wt_med | h5 | 7.740 | -0.022 | -0.042 | -0.003 | -0.030 | -0.013 | True | 0.990 | 1 | 13 |
| MAE, schedule projected at origin | UC3 | 6 | wt_med | h5 | 7.904 | -0.024 | -0.045 | -0.005 | -0.033 | -0.015 | True | 0.984 | 1 | 13 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | h5 | 8.065 | -0.027 | -0.049 | -0.006 | -0.036 | -0.019 | True | 0.993 | 1 | 12 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | h5 | 8.268 | -0.025 | -0.048 | -0.002 | -0.034 | -0.016 | True | 0.994 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | h5 | 8.639 | -0.025 | -0.048 | -0.004 | -0.034 | -0.015 | True | 0.992 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 21 | wt_med | h5 | 9.098 | -0.022 | -0.047 | -0.001 | -0.031 | -0.013 | True | 0.991 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 30 | wt_med | h5 | 9.252 | -0.034 | -0.059 | -0.009 | -0.041 | -0.026 | True | 0.992 | 1 | 8 |
| MAE, schedule projected at origin | UC3 | 45 | wt_med | wt_med | 9.715 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| MAE, schedule projected at origin | UC3 | 60 | wt_med | wt_med | 9.745 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| MAE, schedule projected at origin | UC3 | 90 | wt_med | wt_med | 10.103 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 0 | wt_med | wt_med | 4.582 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| first-hour MAE, schedule projected at origin | D5 | 1 | wt_med | nf_nhits_x_h5 | 4.553 | -0.028 | -0.061 | -0.003 | -0.056 | -0.005 | True | 0.972 | 4 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 2 | wt_med | nf_nhits_x_h5 | 4.596 | -0.024 | -0.053 | -0.001 | -0.047 | -0.001 | True | 0.970 | 4 | 11 |
| first-hour MAE, schedule projected at origin | D5 | 3 | wt_med | lvlh5_naive | 4.680 | -0.030 | -0.073 | -0.003 | -0.055 | -0.008 | True | 0.996 | 4 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 4 | wt_med | lvlh5_naive | 4.715 | -0.035 | -0.085 | -0.005 | -0.060 | -0.012 | True | 0.995 | 4 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 5 | wt_med | lvlh5_naive | 4.793 | -0.041 | -0.101 | -0.006 | -0.067 | -0.017 | True | 0.991 | 3 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 6 | wt_med | lvlh5_naive | 4.908 | -0.050 | -0.113 | -0.009 | -0.076 | -0.028 | True | 0.985 | 4 | 13 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | nf_tide_x_h5 | 4.904 | -0.059 | -0.133 | -0.012 | -0.082 | -0.036 | True | 0.970 | 4 | 12 |
| first-hour MAE, schedule projected at origin | D5 | 10 | wt_med | h5 | 5.198 | -0.058 | -0.127 | -0.008 | -0.079 | -0.038 | True | 0.998 | 2 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 14 | wt_med | wt_med | 5.433 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 21 | wt_med | wt_med | 5.618 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 30 | wt_med | wt_med | 5.629 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| first-hour MAE, schedule projected at origin | D5 | 45 | wt_med | wt_med | 6.006 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| first-hour MAE, schedule projected at origin | D5 | 60 | wt_med | wt_med | 6.110 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 90 | wt_med | wt_med | 5.996 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | persistence | 6.383 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | h5 | 0.828 | 0.007 | 0.003 | 0.011 | 0.006 | 0.009 | True | 0.996 | 1 | 9 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | h5 | 0.827 | 0.007 | 0.002 | 0.011 | 0.006 | 0.009 | True | 0.995 | 1 | 11 |
| best-time hit rate (top-2) | D3 | 2 | wt_med | h5 | 0.827 | 0.007 | 0.002 | 0.011 | 0.006 | 0.009 | True | 0.995 | 1 | 11 |
| best-time hit rate (top-2) | D3 | 3 | wt_med | h5 | 0.827 | 0.008 | 0.003 | 0.012 | 0.006 | 0.009 | True | 0.995 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 4 | wt_med | h5 | 0.826 | 0.007 | 0.002 | 0.011 | 0.006 | 0.009 | True | 0.995 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 5 | wt_med | h5 | 0.826 | 0.007 | 0.002 | 0.011 | 0.006 | 0.009 | True | 0.994 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | h5 | 0.826 | 0.007 | 0.002 | 0.011 | 0.005 | 0.008 | True | 0.994 | 1 | 13 |
| best-time hit rate (top-2) | D3 | 7 | wt_med | h5 | 0.825 | 0.006 | 0.002 | 0.010 | 0.005 | 0.008 | True | 0.993 | 1 | 12 |
| best-time hit rate (top-2) | D3 | 10 | wt_med | h5 | 0.824 | 0.006 | 0.001 | 0.010 | 0.004 | 0.007 | True | 0.992 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 14 | wt_med | h5 | 0.823 | 0.006 | 0.001 | 0.010 | 0.004 | 0.008 | True | 0.991 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 21 | wt_med | h5 | 0.822 | 0.008 | 0.003 | 0.012 | 0.006 | 0.009 | True | 0.989 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 30 | wt_med | h5 | 0.821 | 0.008 | 0.003 | 0.012 | 0.007 | 0.010 | True | 0.986 | 1 | 8 |
| best-time hit rate (top-2) | D3 | 45 | wt_med | h5 | 0.821 | 0.008 | 0.004 | 0.012 | 0.007 | 0.010 | True | 0.985 | 1 | 7 |
| best-time hit rate (top-2) | D3 | 60 | clim | h5 | 0.824 | 0.006 | 0.001 | 0.010 | 0.004 | 0.007 | True | 0.989 | 1 | 4 |
| best-time hit rate (top-2) | D3 | 90 | wt_med | wt_med | 0.824 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| best-time hit rate (±30 min) | D3 | 0 | wt_med | h5 | 0.833 | 0.007 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.996 | 1 | 9 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | h5 | 0.833 | 0.006 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.995 | 1 | 11 |
| best-time hit rate (±30 min) | D3 | 2 | wt_med | h5 | 0.833 | 0.007 | 0.005 | 0.010 | 0.006 | 0.008 | True | 0.995 | 1 | 11 |
| best-time hit rate (±30 min) | D3 | 3 | wt_med | h5 | 0.832 | 0.007 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.995 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.832 | 0.007 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.995 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | h5 | 0.832 | 0.007 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.994 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | h5 | 0.831 | 0.007 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.994 | 1 | 13 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.831 | 0.006 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.993 | 1 | 12 |
| best-time hit rate (±30 min) | D3 | 10 | wt_med | h5 | 0.830 | 0.007 | 0.004 | 0.009 | 0.005 | 0.008 | True | 0.992 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 14 | wt_med | h5 | 0.830 | 0.006 | 0.004 | 0.009 | 0.005 | 0.007 | True | 0.991 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 21 | wt_med | h5 | 0.829 | 0.008 | 0.005 | 0.011 | 0.007 | 0.009 | True | 0.989 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 30 | wt_med | h5 | 0.829 | 0.008 | 0.006 | 0.012 | 0.007 | 0.010 | True | 0.986 | 1 | 8 |
| best-time hit rate (±30 min) | D3 | 45 | wt_med | h5 | 0.829 | 0.010 | 0.007 | 0.014 | 0.009 | 0.011 | True | 0.985 | 1 | 7 |
| best-time hit rate (±30 min) | D3 | 60 | wt_med | h5 | 0.829 | 0.008 | 0.005 | 0.011 | 0.007 | 0.010 | True | 0.989 | 1 | 4 |
| best-time hit rate (±30 min) | D3 | 90 | wt_med | wt_med | 0.832 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.772 | -0.243 | -0.317 | -0.181 | -0.274 | -0.215 | True | 0.996 | 1 | 9 |
| best-time regret (min) | D3 | 1 | wt_med | h5 | 2.792 | -0.239 | -0.311 | -0.176 | -0.270 | -0.211 | True | 0.995 | 1 | 11 |
| best-time regret (min) | D3 | 2 | wt_med | h5 | 2.787 | -0.248 | -0.321 | -0.185 | -0.278 | -0.218 | True | 0.995 | 1 | 11 |
| best-time regret (min) | D3 | 3 | wt_med | h5 | 2.802 | -0.248 | -0.323 | -0.186 | -0.281 | -0.219 | True | 0.995 | 1 | 13 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 2.804 | -0.243 | -0.314 | -0.182 | -0.276 | -0.213 | True | 0.995 | 1 | 13 |
| best-time regret (min) | D3 | 5 | wt_med | h5 | 2.811 | -0.242 | -0.315 | -0.179 | -0.273 | -0.211 | True | 0.994 | 1 | 13 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 2.825 | -0.251 | -0.326 | -0.189 | -0.286 | -0.221 | True | 0.994 | 1 | 13 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 2.839 | -0.241 | -0.313 | -0.182 | -0.273 | -0.209 | True | 0.993 | 1 | 12 |
| best-time regret (min) | D3 | 10 | wt_med | h5 | 2.867 | -0.251 | -0.325 | -0.191 | -0.283 | -0.219 | True | 0.992 | 1 | 8 |
| best-time regret (min) | D3 | 14 | wt_med | h5 | 2.889 | -0.232 | -0.304 | -0.171 | -0.264 | -0.200 | True | 0.991 | 1 | 8 |
| best-time regret (min) | D3 | 21 | wt_med | h5 | 2.885 | -0.273 | -0.350 | -0.199 | -0.305 | -0.243 | True | 0.989 | 1 | 8 |
| best-time regret (min) | D3 | 30 | wt_med | h5 | 2.886 | -0.275 | -0.362 | -0.198 | -0.307 | -0.244 | True | 0.986 | 1 | 8 |
| best-time regret (min) | D3 | 45 | wt_med | h5 | 2.890 | -0.316 | -0.413 | -0.231 | -0.348 | -0.280 | True | 0.985 | 1 | 7 |
| best-time regret (min) | D3 | 60 | wt_med | h5 | 2.933 | -0.295 | -0.374 | -0.222 | -0.328 | -0.259 | True | 0.989 | 1 | 4 |
| best-time regret (min) | D3 | 90 | wt_med | wt_med | 3.036 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | h5 | 0.468 | 0.019 | 0.013 | 0.024 | 0.018 | 0.020 | True | 0.948 | 3 | 9 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | h5 | 0.465 | 0.019 | 0.014 | 0.024 | 0.018 | 0.021 | True | 0.947 | 3 | 11 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | h5 | 0.465 | 0.020 | 0.014 | 0.025 | 0.019 | 0.021 | True | 0.947 | 3 | 11 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.463 | 0.020 | 0.014 | 0.025 | 0.019 | 0.021 | True | 0.946 | 3 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | h5 | 0.463 | 0.020 | 0.015 | 0.025 | 0.019 | 0.021 | True | 0.945 | 3 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | h5 | 0.462 | 0.020 | 0.015 | 0.025 | 0.019 | 0.022 | True | 0.945 | 3 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | h5 | 0.460 | 0.021 | 0.015 | 0.026 | 0.020 | 0.022 | True | 0.944 | 3 | 13 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.456 | 0.021 | 0.016 | 0.026 | 0.020 | 0.022 | True | 0.943 | 4 | 12 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.452 | 0.022 | 0.016 | 0.027 | 0.020 | 0.023 | True | 0.941 | 4 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.446 | 0.023 | 0.017 | 0.028 | 0.021 | 0.024 | True | 0.941 | 4 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | wt_med | prod_served_lin | 0.428 | 0.025 | 0.014 | 0.037 | 0.022 | 0.029 | True | 0.508 | 5 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | wt_med | prod_served_lin | 0.427 | 0.030 | 0.016 | 0.045 | 0.026 | 0.034 | True | 0.430 | 5 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | wt_med | prod_served_lin | 0.415 | 0.027 | 0.015 | 0.041 | 0.023 | 0.032 | True | 0.348 | 5 | 7 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | wt_med | h5 | 0.410 | 0.024 | 0.018 | 0.030 | 0.022 | 0.025 | True | 0.943 | 1 | 4 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | wt_med | h5 | 0.412 | 0.020 | 0.014 | 0.025 | 0.018 | 0.021 | True | 0.949 | 1 | 4 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | nf_tide | 8.667 | -0.190 | -0.332 | -0.055 | -0.259 | -0.123 | True | 0.982 | 1 | 9 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | wt_med | 8.790 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| dayPeak abs error (min) | UC3 | 2 | wt_med | wt_med | 8.832 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| dayPeak abs error (min) | UC3 | 3 | wt_med | prod_served_lin | 8.316 | -0.295 | -0.502 | -0.083 | -0.396 | -0.191 | True | 0.533 | 2 | 13 |
| dayPeak abs error (min) | UC3 | 4 | wt_med | prod_served_lin | 8.342 | -0.340 | -0.545 | -0.134 | -0.444 | -0.233 | True | 0.532 | 2 | 13 |
| dayPeak abs error (min) | UC3 | 5 | wt_med | wt_med | 8.958 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak abs error (min) | UC3 | 6 | wt_med | wt_med | 9.008 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | wt_med | 9.100 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 9.249 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 14 | wt_med | wt_med | 9.403 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 21 | wt_med | wt_med | 9.567 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 30 | wt_med | wt_med | 9.653 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak abs error (min) | UC3 | 45 | wt_med | wt_med | 9.569 | 0.000 |  |  |  |  | True |  | 0 | 7 |
| dayPeak abs error (min) | UC3 | 60 | wt_med | wt_med | 9.635 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| dayPeak abs error (min) | UC3 | 90 | wt_med | wt_med | 9.449 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| rope-drop worth agreement | D5 | 0 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 9 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| rope-drop worth agreement | D5 | 2 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| rope-drop worth agreement | D5 | 3 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 4 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 5 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 6 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| rope-drop worth agreement | D5 | 7 | wt_med | wt_med | 0.913 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| rope-drop worth agreement | D5 | 10 | wt_med | wt_med | 0.912 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 14 | wt_med | wt_med | 0.911 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 21 | wt_med | wt_med | 0.911 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 30 | wt_med | wt_med | 0.912 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| rope-drop worth agreement | D5 | 45 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| rope-drop worth agreement | D5 | 90 | wt_med | wt_med | 0.914 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | nf_tide | 0.800 | 0.061 | 0.053 | 0.070 | 0.059 | 0.063 | True | 0.979 | 5 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | nf_tide | 0.786 | 0.050 | 0.043 | 0.059 | 0.048 | 0.052 | True | 0.979 | 7 | 10 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | nf_tide | 0.784 | 0.049 | 0.042 | 0.058 | 0.047 | 0.051 | True | 0.979 | 7 | 10 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | nf_tide | 0.779 | 0.046 | 0.039 | 0.054 | 0.044 | 0.048 | True | 0.978 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | nf_tide | 0.777 | 0.045 | 0.037 | 0.054 | 0.042 | 0.047 | True | 0.978 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | nf_tide | 0.776 | 0.046 | 0.038 | 0.054 | 0.043 | 0.048 | True | 0.978 | 8 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | nf_tide | 0.773 | 0.044 | 0.036 | 0.052 | 0.041 | 0.046 | True | 0.978 | 7 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | nf_tide | 0.768 | 0.042 | 0.034 | 0.051 | 0.039 | 0.044 | True | 0.979 | 7 | 12 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | lvlh5_tft | 0.742 | 0.023 | 0.015 | 0.032 | 0.020 | 0.027 | True | 0.574 | 3 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | lvlh5_tft | 0.735 | 0.023 | 0.015 | 0.033 | 0.020 | 0.026 | True | 0.565 | 3 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | lvlh5_tft | 0.729 | 0.026 | 0.018 | 0.035 | 0.023 | 0.029 | True | 0.551 | 3 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | wt_med | lvlh5_tft | 0.725 | 0.027 | 0.019 | 0.037 | 0.023 | 0.031 | True | 0.420 | 3 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | wt_med | lvlh5_tft | 0.723 | 0.033 | 0.025 | 0.042 | 0.028 | 0.037 | True | 0.329 | 3 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | wt_med | lvlh5_naive | 0.714 | 0.020 | 0.014 | 0.028 | 0.018 | 0.023 | True | 0.948 | 2 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | wt_med | lvlh5_naive | 0.719 | 0.020 | 0.011 | 0.031 | 0.017 | 0.024 | True | 0.951 | 2 | 4 |
| dayPeak Spearman across rides | UC3 | 0 | wt_med | nf_tide | 0.783 | 0.010 | 0.002 | 0.017 | 0.007 | 0.013 | True | 0.981 | 1 | 9 |
| dayPeak Spearman across rides | UC3 | 1 | wt_med | wt_med | 0.772 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| dayPeak Spearman across rides | UC3 | 2 | wt_med | wt_med | 0.771 | 0.000 |  |  |  |  | False |  | 0 | 11 |
| dayPeak Spearman across rides | UC3 | 3 | wt_med | wt_med | 0.770 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 4 | wt_med | wt_med | 0.769 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 5 | wt_med | wt_med | 0.768 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 6 | wt_med | wt_med | 0.766 | 0.000 |  |  |  |  | False |  | 0 | 13 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.763 | 0.000 |  |  |  |  | False |  | 0 | 12 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.759 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.751 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 21 | wt_med | wt_med | 0.746 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 30 | wt_med | wt_med | 0.738 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| dayPeak Spearman across rides | UC3 | 45 | wt_med | wt_med | 0.730 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| dayPeak Spearman across rides | UC3 | 60 | wt_med | wt_med | 0.719 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| dayPeak Spearman across rides | UC3 | 90 | wt_med | wt_med | 0.719 | 0.000 |  |  |  |  | False |  | 0 | 4 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | nf_nhits_x_h5 | 27.906 | -1.367 | -2.079 | -0.757 | -1.864 | -0.897 | True | 0.963 | 2 | 11 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | nf_tide_x_h5 | 28.116 | -1.565 | -2.313 | -0.892 | -2.050 | -1.112 | True | 0.960 | 3 | 13 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | nf_nhits_x_h5 | 28.378 | -1.432 | -2.119 | -0.769 | -1.899 | -0.961 | True | 0.962 | 3 | 12 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | h5 | 29.785 | -0.913 | -1.491 | -0.310 | -1.324 | -0.485 | True | 0.993 | 1 | 8 |
| optimiser regret (min per park-day) | D4 | 30 | wt_med | h5 | 30.176 | -0.941 | -1.446 | -0.392 | -1.360 | -0.566 | True | 0.989 | 1 | 8 |
| next-best precision | D1 | 0-120 | snaive7 | nf_tide | 0.616 | 0.155 | 0.132 | 0.177 | 0.150 | 0.160 | True | 0.948 | 2 | 8 |
| next-best precision | D1 | 0-60 | snaive7 | nf_tide | 0.539 | 0.162 | 0.135 | 0.186 | 0.156 | 0.167 | True | 0.935 | 2 | 8 |
| next-best precision (top-3) | D1 | 0-120 | snaive7 | nf_tide | 0.628 | 0.093 | 0.079 | 0.110 | 0.089 | 0.098 | True | 0.931 | 1 | 8 |
| next-best precision (top-3) | D1 | 0-60 | wt_med | nf_tide | 0.551 | 0.083 | 0.068 | 0.100 | 0.079 | 0.088 | True | 0.917 | 1 | 8 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.554 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.509 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 1 | lvl_snaive7 | lvl_snaive7 | 0.229 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 2 | lvl_snaive7 | lvl_snaive7 | 0.230 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 3 | lvl_snaive7 | lvl_snaive7 | 0.224 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 4 | lvl_snaive7 | lvl_snaive7 | 0.225 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 5 | lvl_snaive7 | lvl_snaive7 | 0.220 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 6 | lvl_snaive7 | lvl_snaive7 | 0.213 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| UC4 Spearman within park-month | UC4 | 7 | lvl_naive4 | lvl_naive4 | 0.175 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| UC4 Spearman within park-month | UC4 | 10 | lvl_naive4 | lvl_naive4 | 0.187 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 14 | lvl_naive4 | lvl_naive4 | 0.156 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 21 | lvl_naive4 | lvl_naive4 | 0.133 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 30 | lvl_naive4 | lvl_naive4 | 0.149 | 0.000 |  |  |  |  | True |  | 0 | 5 |
| UC4 Spearman within park-month | UC4 | 45 | lvl_naive4 | lvl_naive4 | 0.159 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| UC4 Spearman within park-month | UC4 | 60 | lvl_naive4 | lvl_naive4 | 0.164 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| UC4 Spearman within park-month | UC4 | 90 | lvl_naive4 | lvl_naive4 | 0.150 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| UC4 Spearman within park-month | UC4 | 120 | lvl_naive4 | lvl_naive4 | 0.206 | 0.000 |  |  |  |  | False |  | 0 | 3 |
| UC4 Spearman within park-month | UC4 | 180 | lvl_naive4 | lvl_naive4 | 0.328 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_snaive7 | 0.385 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 2 | lvl_snaive7 | lvl_snaive7 | 0.386 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 3 | lvl_snaive7 | lvl_snaive7 | 0.387 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.388 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.387 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.386 | 0.000 |  |  |  |  | True |  | 0 | 8 |
| crowd bucket exact | D6 | 7 | lvl_naive4 | lvl_naive4 | 0.342 | 0.000 |  |  |  |  | False |  | 0 | 7 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_naive4 | 0.345 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_naive4 | 0.314 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 21 | lvl_naive4 | lvl_naive4 | 0.300 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_naive4 | 0.288 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_naive4 | 0.276 | 0.000 |  |  |  |  | True |  | 0 | 4 |
| crowd bucket exact | D6 | 60 | lvl_naive4 | lvl_naive4 | 0.267 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| crowd bucket exact | D6 | 90 | lvl_naive4 | lvl_naive4 | 0.281 | 0.000 |  |  |  |  | False |  | 0 | 3 |
| crowd bucket exact | D6 | 120 | lvl_naive4 | lvl_naive4 | 0.277 | 0.000 |  |  |  |  | False |  | 0 | 3 |
| crowd bucket exact | D6 | 180 | lvl_naive4 | lvl_naive4 | 0.262 | 0.000 |  |  |  |  | True |  | 0 | 3 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.464 | 0.000 |  |  |  |  | False |  | 0 | 8 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_naive4 | 0.371 | 0.000 |  |  |  |  | False |  | 0 | 5 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_naive4 | 0.339 | 0.000 |  |  |  |  | False |  | 0 | 3 |

### Low-coverage cells (paired rows < 50 % of the reference's)

These absolute values are not comparable with the rest of their column. `reaches_handover` = the cell still passes the 30 % gate and CAN appear as a winner.

| metric | uc | segment | lead | model | ref | value | diff_vs_ref | diff_lo_park | diff_hi_park | paired_share | n_paired | n_units_park | n_origin_days | wins | reaches_handover |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| MAE | UC2 | all | 0 | lvlh5_cbd | wt_med | 9.699 | 2.848 | 2.403 | 3.321 | 0.427 | 3511610.000 | 126.000 | 170 | False | True |
| MAE | UC2 | all | 1 | lvlh5_cbd | wt_med | 9.834 | 2.852 | 2.403 | 3.317 | 0.429 | 3524350.000 | 128.000 | 170 | False | True |
| MAE | UC2 | busy | 0 | lvlh5_cbd | wt_med | 18.597 | 5.167 | 3.932 | 6.492 | 0.382 | 965392.000 | 111.000 | 170 | False | True |
| MAE | UC2 | busy | 1 | lvlh5_cbd | wt_med | 18.859 | 5.195 | 4.011 | 6.500 | 0.383 | 968227.000 | 111.000 | 170 | False | True |
| MAE | UC2-intraday | all | h4-8 | persistence | wt_med | 11.068 | 3.233 | 2.633 | 3.811 | 0.422 | 4830984.000 | 145.000 | 170 | False | True |
| MAE | UC2-intraday | all | h8+ | persistence | wt_med | 13.192 | 6.130 | 4.949 | 7.435 | 0.199 | 987097.000 | 102.000 | 170 | False | False |
| MAE | UC2-intraday | busy | h4-8 | persistence | wt_med | 19.428 | 5.352 | 4.397 | 6.308 | 0.486 | 1739124.000 | 128.000 | 170 | False | True |
| MAE | UC2-intraday | busy | h8+ | persistence | wt_med | 21.793 | 9.983 | 7.584 | 12.770 | 0.237 | 411045.000 | 91.000 | 170 | False | False |
| MAE | UC3 | all | 1 | lvlh5_cbd | wt_med | 9.834 | 2.852 | 2.403 | 3.317 | 0.429 | 3524350.000 | 128.000 | 170 | False | True |
| MAE | UC3 | all | 2 | lvlh5_cbd | wt_med | 9.870 | 2.850 | 2.413 | 3.325 | 0.430 | 3536289.000 | 128.000 | 170 | False | True |
| MAE | UC3 | all | 3 | lvlh5_cbd | wt_med | 9.933 | 2.842 | 2.411 | 3.298 | 0.431 | 3545948.000 | 128.000 | 170 | False | True |
| MAE | UC3 | all | 4 | lvlh5_cbd | wt_med | 10.002 | 2.850 | 2.414 | 3.311 | 0.432 | 3558824.000 | 128.000 | 170 | False | True |
| MAE | UC3 | all | 5 | lvlh5_cbd | wt_med | 10.021 | 2.831 | 2.399 | 3.304 | 0.432 | 3564136.000 | 127.000 | 170 | False | True |
| MAE | UC3 | all | 6 | lvlh5_cbd | wt_med | 10.301 | 3.048 | 2.604 | 3.514 | 0.433 | 3571478.000 | 126.000 | 170 | False | True |
| MAE | UC3 | all | 7 | lvlh5_cbd | wt_med | 10.445 | 3.101 | 2.636 | 3.570 | 0.433 | 3563072.000 | 123.000 | 170 | False | True |
| MAE | UC3 | all | 10 | lvlh5_cbd | wt_med | 10.683 | 3.157 | 2.665 | 3.671 | 0.433 | 3578491.000 | 128.000 | 170 | False | True |
| MAE | UC3 | all | 14 | lvlh5_cbd | wt_med | 10.846 | 3.099 | 2.609 | 3.611 | 0.430 | 3559626.000 | 127.000 | 170 | False | True |
| MAE | UC3 | all | 21 | lvlh5_cbd | wt_med | 11.157 | 3.061 | 2.556 | 3.589 | 0.425 | 3526907.000 | 124.000 | 170 | False | True |
| MAE | UC3 | all | 30 | lvlh5_cbd | wt_med | 11.400 | 2.958 | 2.484 | 3.462 | 0.416 | 3426560.000 | 111.000 | 170 | False | True |
| MAE | UC3 | all | 45 | lvlh5_cbd | wt_med | 11.549 | 1.970 | 1.448 | 2.473 | 0.133 | 1057114.000 | 41.000 | 170 | False | False |
| MAE | UC3 | all | 30 | lvlh5_tft | wt_med | 8.868 | 0.515 | 0.350 | 0.678 | 0.434 | 3573336.000 | 125.000 | 170 | False | True |
| MAE | UC3 | all | 45 | lvlh5_tft | wt_med | 9.119 | 0.482 | 0.321 | 0.648 | 0.319 | 2542588.000 | 124.000 | 170 | False | True |
| MAE | UC3 | all | 60 | lvlh5_tft | wt_med | 4.292 | 0.049 | -1.186 | 1.571 | 0.005 | 41301.000 | 13.000 | 170 | False | False |
| MAE | UC3 | all | 30 | prod_served | wt_med | 9.264 | 0.978 | 0.662 | 1.333 | 0.455 | 3742366.000 | 102.000 | 170 | False | True |
| MAE | UC3 | all | 45 | prod_served | wt_med | 9.951 | 1.189 | 0.852 | 1.495 | 0.349 | 2777813.000 | 100.000 | 170 | False | True |
| MAE | UC3 | all | 60 | prod_served | wt_med | 4.331 | -0.108 | -1.275 | 0.793 | 0.005 | 39876.000 | 13.000 | 170 | False | False |
| MAE | UC3 | all | 30 | prod_served_lin | wt_med | 9.243 | 0.957 | 0.642 | 1.312 | 0.455 | 3742366.000 | 102.000 | 170 | False | True |
| MAE | UC3 | all | 45 | prod_served_lin | wt_med | 9.929 | 1.166 | 0.824 | 1.476 | 0.349 | 2777813.000 | 100.000 | 170 | False | True |
| MAE | UC3 | all | 60 | prod_served_lin | wt_med | 4.332 | -0.107 | -1.281 | 0.807 | 0.005 | 39876.000 | 13.000 | 170 | False | False |
| MAE | UC3 | busy | 1 | lvlh5_cbd | wt_med | 18.859 | 5.195 | 4.011 | 6.500 | 0.383 | 968227.000 | 111.000 | 170 | False | True |
| MAE | UC3 | busy | 2 | lvlh5_cbd | wt_med | 18.926 | 5.201 | 4.042 | 6.475 | 0.384 | 971010.000 | 110.000 | 170 | False | True |
| MAE | UC3 | busy | 3 | lvlh5_cbd | wt_med | 19.013 | 5.176 | 3.995 | 6.453 | 0.385 | 973981.000 | 111.000 | 170 | False | True |
| MAE | UC3 | busy | 4 | lvlh5_cbd | wt_med | 19.148 | 5.253 | 4.040 | 6.609 | 0.385 | 975993.000 | 111.000 | 170 | False | True |
| MAE | UC3 | busy | 5 | lvlh5_cbd | wt_med | 19.135 | 5.208 | 3.988 | 6.539 | 0.385 | 976891.000 | 111.000 | 170 | False | True |
| MAE | UC3 | busy | 6 | lvlh5_cbd | wt_med | 19.591 | 5.577 | 4.351 | 6.894 | 0.386 | 979203.000 | 108.000 | 170 | False | True |
| MAE | UC3 | busy | 7 | lvlh5_cbd | wt_med | 19.866 | 5.710 | 4.449 | 6.972 | 0.386 | 977336.000 | 108.000 | 170 | False | True |
| MAE | UC3 | busy | 10 | lvlh5_cbd | wt_med | 20.305 | 5.923 | 4.586 | 7.275 | 0.385 | 979544.000 | 113.000 | 170 | False | True |
| MAE | UC3 | busy | 14 | lvlh5_cbd | wt_med | 20.481 | 5.838 | 4.509 | 7.133 | 0.381 | 967416.000 | 112.000 | 170 | False | True |
| MAE | UC3 | busy | 21 | lvlh5_cbd | wt_med | 21.034 | 5.783 | 4.383 | 7.180 | 0.376 | 955188.000 | 108.000 | 170 | False | True |
| MAE | UC3 | busy | 30 | lvlh5_cbd | wt_med | 21.425 | 5.486 | 4.189 | 6.889 | 0.370 | 931090.000 | 99.000 | 170 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_cbd | wt_med | 18.204 | 2.681 | 1.731 | 3.611 | 0.171 | 420365.000 | 38.000 | 170 | False | False |
| MAE | UC3 | busy | 30 | lvlh5_tft | wt_med | 16.601 | 0.687 | 0.372 | 0.977 | 0.380 | 958403.000 | 109.000 | 170 | False | True |
| MAE | UC3 | busy | 45 | lvlh5_tft | wt_med | 17.364 | 0.465 | 0.106 | 0.840 | 0.276 | 676524.000 | 105.000 | 170 | False | False |
| MAE | UC3 | busy | 60 | lvlh5_tft | wt_med | 19.869 | 2.113 | -1.292 | 5.819 | 0.001 | 1739.000 | 4.000 | 170 | False | False |
| MAE | UC3 | busy | 21 | prod_served | wt_med | 16.969 | 1.870 | 1.266 | 2.531 | 0.496 | 1259330.000 | 95.000 | 170 | False | True |
| MAE | UC3 | busy | 30 | prod_served | wt_med | 17.726 | 1.884 | 1.232 | 2.569 | 0.405 | 1021153.000 | 94.000 | 170 | False | True |
| MAE | UC3 | busy | 45 | prod_served | wt_med | 19.017 | 2.304 | 1.624 | 2.928 | 0.323 | 791448.000 | 89.000 | 170 | False | True |
| MAE | UC3 | busy | 60 | prod_served | wt_med | 20.851 | 3.031 | 2.094 | 7.131 | 0.001 | 1739.000 | 4.000 | 170 | False | False |
| MAE | UC3 | busy | 21 | prod_served_lin | wt_med | 16.914 | 1.814 | 1.206 | 2.477 | 0.496 | 1259330.000 | 95.000 | 170 | False | True |
| MAE | UC3 | busy | 30 | prod_served_lin | wt_med | 17.666 | 1.823 | 1.168 | 2.512 | 0.405 | 1021153.000 | 94.000 | 170 | False | True |
| MAE | UC3 | busy | 45 | prod_served_lin | wt_med | 18.957 | 2.243 | 1.558 | 2.879 | 0.323 | 791448.000 | 89.000 | 170 | False | True |
| MAE | UC3 | busy | 60 | prod_served_lin | wt_med | 20.824 | 3.003 | 2.022 | 6.977 | 0.001 | 1739.000 | 4.000 | 170 | False | False |
| MAE, schedule known at origin | UC3 | all | 0 | lvlh5_cbd | wt_med | 9.708 | 2.907 | 2.425 | 3.472 | 0.449 | 3023753.000 | 125.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 1 | lvlh5_cbd | wt_med | 9.871 | 2.924 | 2.433 | 3.484 | 0.449 | 3001539.000 | 126.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 2 | lvlh5_cbd | wt_med | 9.908 | 2.923 | 2.443 | 3.482 | 0.451 | 3010454.000 | 126.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 3 | lvlh5_cbd | wt_med | 9.977 | 2.914 | 2.434 | 3.475 | 0.452 | 3010822.000 | 125.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 4 | lvlh5_cbd | wt_med | 10.033 | 2.913 | 2.415 | 3.480 | 0.454 | 3015838.000 | 125.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 5 | lvlh5_cbd | wt_med | 10.027 | 2.879 | 2.406 | 3.432 | 0.455 | 3008470.000 | 123.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 6 | lvlh5_cbd | wt_med | 10.301 | 3.098 | 2.611 | 3.640 | 0.457 | 3002615.000 | 119.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 7 | lvlh5_cbd | wt_med | 10.417 | 3.140 | 2.654 | 3.670 | 0.457 | 2983748.000 | 118.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 10 | lvlh5_cbd | wt_med | 10.613 | 3.142 | 2.654 | 3.680 | 0.458 | 2959250.000 | 123.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 14 | lvlh5_cbd | wt_med | 10.661 | 3.046 | 2.570 | 3.561 | 0.458 | 2868151.000 | 121.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 21 | lvlh5_cbd | wt_med | 10.843 | 2.935 | 2.474 | 3.424 | 0.459 | 2793897.000 | 114.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 11.014 | 2.872 | 2.401 | 3.369 | 0.454 | 2538448.000 | 93.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 12.133 | 2.102 | 1.646 | 2.582 | 0.116 | 594803.000 | 35.000 | 170 | False | False |
| MAE, schedule known at origin | UC3 | all | 30 | lvlh5_tft | wt_med | 8.480 | 0.409 | 0.266 | 0.559 | 0.465 | 2601419.000 | 104.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 8.524 | 0.409 | 0.238 | 0.569 | 0.327 | 1673448.000 | 90.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 3.324 | -0.224 | -1.412 | 0.728 | 0.008 | 36103.000 | 11.000 | 170 | False | False |
| MAE, schedule known at origin | UC3 | all | 30 | prod_served | wt_med | 8.742 | 0.736 | 0.481 | 0.979 | 0.490 | 2740691.000 | 85.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served | wt_med | 9.241 | 0.911 | 0.590 | 1.218 | 0.360 | 1845053.000 | 84.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served | wt_med | 3.488 | -0.264 | -1.801 | 0.368 | 0.008 | 34719.000 | 11.000 | 170 | False | False |
| MAE, schedule known at origin | UC3 | all | 30 | prod_served_lin | wt_med | 8.724 | 0.717 | 0.464 | 0.958 | 0.490 | 2740691.000 | 85.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 45 | prod_served_lin | wt_med | 9.221 | 0.890 | 0.571 | 1.195 | 0.360 | 1845053.000 | 84.000 | 170 | False | True |
| MAE, schedule known at origin | UC3 | all | 60 | prod_served_lin | wt_med | 3.487 | -0.264 | -1.790 | 0.370 | 0.008 | 34719.000 | 11.000 | 170 | False | False |
| MAE, schedule projected at origin | UC3 | all | 0 | lvlh5_cbd | wt_med | 9.645 | 2.480 | 1.868 | 3.155 | 0.329 | 487857.000 | 56.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 1 | lvlh5_cbd | wt_med | 9.624 | 2.436 | 1.794 | 3.109 | 0.339 | 522811.000 | 56.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 2 | lvlh5_cbd | wt_med | 9.651 | 2.432 | 1.792 | 3.095 | 0.336 | 525835.000 | 57.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 3 | lvlh5_cbd | wt_med | 9.683 | 2.433 | 1.818 | 3.085 | 0.339 | 535126.000 | 59.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | lvlh5_cbd | wt_med | 9.828 | 2.499 | 1.896 | 3.137 | 0.340 | 542986.000 | 61.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | lvlh5_cbd | wt_med | 9.991 | 2.571 | 1.981 | 3.261 | 0.339 | 555666.000 | 61.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | lvlh5_cbd | wt_med | 10.303 | 2.780 | 2.049 | 3.678 | 0.339 | 568863.000 | 61.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 7 | lvlh5_cbd | wt_med | 10.592 | 2.904 | 1.970 | 4.072 | 0.341 | 579324.000 | 60.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 10 | lvlh5_cbd | wt_med | 11.023 | 3.228 | 2.063 | 4.756 | 0.343 | 619241.000 | 59.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 14 | lvlh5_cbd | wt_med | 11.628 | 3.318 | 2.097 | 4.728 | 0.345 | 691475.000 | 64.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | lvlh5_cbd | wt_med | 12.380 | 3.542 | 2.155 | 5.097 | 0.332 | 733010.000 | 66.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_cbd | wt_med | 12.529 | 3.203 | 1.993 | 4.610 | 0.337 | 888112.000 | 81.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_cbd | wt_med | 10.777 | 1.800 | 0.880 | 2.866 | 0.163 | 462311.000 | 38.000 | 170 | False | False |
| MAE, schedule projected at origin | UC3 | all | 0 | lvlh5_tft | wt_med | 7.138 | 0.182 | -0.011 | 0.399 | 0.469 | 695371.000 | 72.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 1 | lvlh5_tft | wt_med | 7.212 | 0.231 | 0.019 | 0.438 | 0.473 | 728526.000 | 72.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 2 | lvlh5_tft | wt_med | 7.332 | 0.331 | 0.107 | 0.532 | 0.476 | 744494.000 | 74.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 3 | lvlh5_tft | wt_med | 7.441 | 0.375 | 0.124 | 0.572 | 0.481 | 759328.000 | 76.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | lvlh5_tft | wt_med | 7.544 | 0.386 | 0.120 | 0.590 | 0.483 | 772823.000 | 78.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | lvlh5_tft | wt_med | 7.623 | 0.342 | 0.076 | 0.543 | 0.486 | 795522.000 | 79.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | lvlh5_tft | wt_med | 7.713 | 0.346 | 0.137 | 0.536 | 0.484 | 811824.000 | 80.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 7 | lvlh5_tft | wt_med | 8.044 | 0.523 | 0.205 | 0.754 | 0.486 | 827476.000 | 78.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 10 | lvlh5_tft | wt_med | 8.306 | 0.636 | 0.264 | 0.917 | 0.487 | 878791.000 | 79.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 14 | lvlh5_tft | wt_med | 8.859 | 0.664 | 0.290 | 0.979 | 0.485 | 970941.000 | 81.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | lvlh5_tft | wt_med | 9.445 | 0.761 | 0.336 | 1.188 | 0.483 | 1065479.000 | 84.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | lvlh5_tft | wt_med | 9.932 | 0.799 | 0.399 | 1.179 | 0.369 | 971917.000 | 100.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | lvlh5_tft | wt_med | 10.290 | 0.624 | 0.282 | 0.979 | 0.306 | 869140.000 | 99.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | lvlh5_tft | wt_med | 11.046 | 1.946 | 0.273 | 4.434 | 0.002 | 5198.000 | 4.000 | 170 | False | False |
| MAE, schedule projected at origin | UC3 | all | 3 | prod_served | wt_med | 8.289 | 1.272 | 0.531 | 1.874 | 0.471 | 743409.000 | 67.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | prod_served | wt_med | 8.385 | 1.307 | 0.538 | 1.945 | 0.472 | 754447.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | prod_served | wt_med | 8.462 | 1.281 | 0.516 | 1.910 | 0.474 | 776729.000 | 71.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | prod_served | wt_med | 8.454 | 1.177 | 0.508 | 1.722 | 0.473 | 794030.000 | 71.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 7 | prod_served | wt_med | 8.907 | 1.468 | 0.564 | 2.231 | 0.476 | 810302.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 10 | prod_served | wt_med | 9.234 | 1.656 | 0.692 | 2.459 | 0.478 | 862823.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 14 | prod_served | wt_med | 9.696 | 1.611 | 0.623 | 2.493 | 0.478 | 956707.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | prod_served | wt_med | 10.441 | 1.829 | 0.746 | 2.906 | 0.475 | 1048336.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served | wt_med | 10.737 | 1.642 | 0.804 | 2.541 | 0.381 | 1001675.000 | 77.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served | wt_med | 11.402 | 1.738 | 1.112 | 2.311 | 0.329 | 932760.000 | 77.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served | wt_med | 10.139 | 0.938 | -0.122 | 7.052 | 0.002 | 5157.000 | 4.000 | 170 | False | False |
| MAE, schedule projected at origin | UC3 | all | 3 | prod_served_lin | wt_med | 8.269 | 1.252 | 0.505 | 1.848 | 0.471 | 743409.000 | 67.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 4 | prod_served_lin | wt_med | 8.364 | 1.286 | 0.523 | 1.920 | 0.472 | 754447.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 5 | prod_served_lin | wt_med | 8.441 | 1.260 | 0.492 | 1.889 | 0.474 | 776729.000 | 71.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 6 | prod_served_lin | wt_med | 8.435 | 1.158 | 0.489 | 1.702 | 0.473 | 794030.000 | 71.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 7 | prod_served_lin | wt_med | 8.887 | 1.447 | 0.546 | 2.211 | 0.476 | 810302.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 10 | prod_served_lin | wt_med | 9.212 | 1.633 | 0.672 | 2.436 | 0.478 | 862823.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 14 | prod_served_lin | wt_med | 9.675 | 1.590 | 0.609 | 2.474 | 0.478 | 956707.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 21 | prod_served_lin | wt_med | 10.414 | 1.803 | 0.715 | 2.874 | 0.475 | 1048336.000 | 70.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 30 | prod_served_lin | wt_med | 10.710 | 1.615 | 0.778 | 2.509 | 0.381 | 1001675.000 | 77.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 45 | prod_served_lin | wt_med | 11.376 | 1.712 | 1.079 | 2.288 | 0.329 | 932760.000 | 77.000 | 170 | False | True |
| MAE, schedule projected at origin | UC3 | all | 60 | prod_served_lin | wt_med | 10.150 | 0.950 | -0.089 | 6.987 | 0.002 | 5157.000 | 4.000 | 170 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 0 | lvlh5_cbd | wt_med | 0.412 | -0.007 | -0.016 | 0.001 | 0.429 | 60243.000 | 110.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 1 | lvlh5_cbd | wt_med | 0.408 | -0.007 | -0.016 | 0.002 | 0.429 | 59642.000 | 111.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 2 | lvlh5_cbd | wt_med | 0.408 | -0.006 | -0.015 | 0.002 | 0.429 | 59393.000 | 109.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 3 | lvlh5_cbd | wt_med | 0.407 | -0.006 | -0.015 | 0.002 | 0.430 | 59152.000 | 110.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 4 | lvlh5_cbd | wt_med | 0.406 | -0.006 | -0.015 | 0.003 | 0.431 | 58974.000 | 112.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 5 | lvlh5_cbd | wt_med | 0.406 | -0.005 | -0.014 | 0.004 | 0.431 | 58701.000 | 111.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 6 | lvlh5_cbd | wt_med | 0.400 | -0.009 | -0.017 | -0.000 | 0.431 | 58309.000 | 112.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 7 | lvlh5_cbd | wt_med | 0.396 | -0.009 | -0.018 | 0.001 | 0.431 | 57550.000 | 110.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 10 | lvlh5_cbd | wt_med | 0.392 | -0.008 | -0.017 | 0.001 | 0.427 | 56292.000 | 110.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 14 | lvlh5_cbd | wt_med | 0.388 | -0.006 | -0.015 | 0.003 | 0.421 | 53946.000 | 111.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 21 | lvlh5_cbd | wt_med | 0.382 | -0.004 | -0.013 | 0.006 | 0.414 | 50934.000 | 109.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | lvlh5_cbd | wt_med | 0.374 | -0.004 | -0.014 | 0.006 | 0.407 | 47594.000 | 103.000 | 69 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_cbd | wt_med | 0.344 | -0.008 | -0.018 | 0.004 | 0.146 | 15782.000 | 39.000 | 68 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | lvlh5_tft | wt_med | 0.385 | 0.021 | 0.012 | 0.031 | 0.420 | 49040.000 | 116.000 | 74 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | lvlh5_tft | wt_med | 0.376 | 0.014 | 0.005 | 0.024 | 0.333 | 35896.000 | 108.000 | 58 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | lvlh5_tft | wt_med | 0.313 | 0.015 | -0.022 | 0.036 | 0.003 | 286.000 | 10.000 | 47 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | prod_served | wt_med | 0.413 | 0.016 | 0.003 | 0.029 | 0.426 | 49722.000 | 100.000 | 74 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served | wt_med | 0.402 | 0.013 | 0.002 | 0.025 | 0.345 | 37189.000 | 98.000 | 69 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served | wt_med | 0.355 | -0.009 | -0.033 | 0.020 | 0.003 | 255.000 | 10.000 | 47 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 30 | prod_served_lin | wt_med | 0.427 | 0.030 | 0.016 | 0.045 | 0.430 | 50201.000 | 100.000 | 74 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 45 | prod_served_lin | wt_med | 0.415 | 0.027 | 0.015 | 0.041 | 0.348 | 37522.000 | 98.000 | 69 | True | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 60 | prod_served_lin | wt_med | 0.367 | 0.007 | -0.019 | 0.040 | 0.003 | 257.000 | 10.000 | 47 | False | False |
| Spearman of slots within ride-day | UC2/UC3 | all | 0 | snaive7 | wt_med | 0.391 | -0.078 | -0.085 | -0.070 | 0.414 | 58233.000 | 117.000 | 170 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 1 | snaive7 | wt_med | 0.392 | -0.074 | -0.082 | -0.066 | 0.420 | 58378.000 | 119.000 | 170 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 2 | snaive7 | wt_med | 0.391 | -0.073 | -0.080 | -0.065 | 0.424 | 58707.000 | 120.000 | 170 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 3 | snaive7 | wt_med | 0.391 | -0.072 | -0.079 | -0.064 | 0.428 | 58862.000 | 123.000 | 170 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 4 | snaive7 | wt_med | 0.391 | -0.071 | -0.078 | -0.063 | 0.429 | 58803.000 | 125.000 | 170 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 5 | snaive7 | wt_med | 0.390 | -0.069 | -0.076 | -0.061 | 0.430 | 58615.000 | 124.000 | 170 | False | True |
| Spearman of slots within ride-day | UC2/UC3 | all | 6 | snaive7 | wt_med | 0.389 | -0.067 | -0.075 | -0.059 | 0.429 | 58140.000 | 122.000 | 170 | False | True |
| UC4 Spearman within park-month | UC4 | all | 14 | lvl_cbd | lvl_naive4 | 0.047 | -0.105 | -0.148 | -0.064 | 0.467 | 194.000 | 80.000 | 6 | False | True |
| UC4 Spearman within park-month | UC4 | all | 21 | lvl_cbd | lvl_naive4 | 0.054 | -0.059 | -0.100 | -0.016 | 0.469 | 184.000 | 81.000 | 6 | False | True |
| UC4 Spearman within park-month | UC4 | all | 30 | lvl_cbd | lvl_naive4 | 0.013 | -0.105 | -0.153 | -0.053 | 0.435 | 154.000 | 81.000 | 7 | False | True |
| UC4 Spearman within park-month | UC4 | all | 45 | lvl_cbd | lvl_naive4 | 0.138 | -0.132 | -0.196 | -0.055 | 0.183 | 74.000 | 33.000 | 6 | False | False |
| UC4 Spearman within park-month | UC4 | all | 30 | lvl_tft | lvl_naive4 | 0.063 | -0.091 | -0.133 | -0.046 | 0.497 | 176.000 | 92.000 | 7 | False | True |
| UC4 Spearman within park-month | UC4 | all | 45 | lvl_tft | lvl_naive4 | 0.161 | 0.000 | -0.057 | 0.056 | 0.421 | 170.000 | 91.000 | 6 | False | True |
| UC4 Spearman within park-month | UC4 | all | 60 | lvl_tft | lvl_naive4 | 0.139 | 0.451 | 0.451 | 0.451 | 0.003 | 1.000 | 1.000 | 7 | False | False |
| best-time hit rate (top-2) | D3 | all | 0 | lvlh5_cbd | wt_med | 0.750 | -0.061 | -0.073 | -0.049 | 0.443 | 69068.000 | 114.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 1 | lvlh5_cbd | wt_med | 0.747 | -0.062 | -0.074 | -0.050 | 0.444 | 68402.000 | 114.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 2 | lvlh5_cbd | wt_med | 0.746 | -0.063 | -0.075 | -0.051 | 0.444 | 68093.000 | 113.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 3 | lvlh5_cbd | wt_med | 0.744 | -0.063 | -0.076 | -0.050 | 0.445 | 67888.000 | 112.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 4 | lvlh5_cbd | wt_med | 0.744 | -0.064 | -0.077 | -0.050 | 0.446 | 67805.000 | 114.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 5 | lvlh5_cbd | wt_med | 0.743 | -0.064 | -0.077 | -0.051 | 0.446 | 67517.000 | 113.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 6 | lvlh5_cbd | wt_med | 0.734 | -0.072 | -0.086 | -0.056 | 0.446 | 67116.000 | 114.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 7 | lvlh5_cbd | wt_med | 0.731 | -0.074 | -0.090 | -0.058 | 0.446 | 66230.000 | 114.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 10 | lvlh5_cbd | wt_med | 0.727 | -0.078 | -0.094 | -0.062 | 0.444 | 64958.000 | 112.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 14 | lvlh5_cbd | wt_med | 0.726 | -0.079 | -0.095 | -0.062 | 0.437 | 62461.000 | 113.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 21 | lvlh5_cbd | wt_med | 0.719 | -0.081 | -0.098 | -0.063 | 0.430 | 59084.000 | 111.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.718 | -0.081 | -0.099 | -0.063 | 0.423 | 54942.000 | 103.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_cbd | wt_med | 0.716 | -0.045 | -0.063 | -0.024 | 0.144 | 17204.000 | 39.000 | 68 | False | False |
| best-time hit rate (top-2) | D3 | all | 30 | lvlh5_tft | wt_med | 0.772 | -0.027 | -0.035 | -0.018 | 0.434 | 56394.000 | 117.000 | 74 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | lvlh5_tft | wt_med | 0.781 | -0.022 | -0.033 | -0.011 | 0.341 | 40705.000 | 109.000 | 58 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | lvlh5_tft | clim | 0.796 | -0.050 | -0.107 | 0.010 | 0.003 | 303.000 | 11.000 | 47 | False | False |
| best-time hit rate (top-2) | D3 | all | 30 | prod_served | wt_med | 0.750 | -0.050 | -0.068 | -0.035 | 0.465 | 60448.000 | 100.000 | 74 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served | wt_med | 0.751 | -0.047 | -0.066 | -0.032 | 0.373 | 44584.000 | 98.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served | clim | 0.753 | -0.046 | -0.084 | 0.030 | 0.003 | 303.000 | 11.000 | 47 | False | False |
| best-time hit rate (top-2) | D3 | all | 30 | prod_served_lin | wt_med | 0.764 | -0.031 | -0.045 | -0.021 | 0.465 | 60448.000 | 100.000 | 74 | False | True |
| best-time hit rate (top-2) | D3 | all | 45 | prod_served_lin | wt_med | 0.766 | -0.026 | -0.041 | -0.013 | 0.373 | 44584.000 | 98.000 | 69 | False | True |
| best-time hit rate (top-2) | D3 | all | 60 | prod_served_lin | clim | 0.747 | -0.046 | -0.094 | 0.020 | 0.003 | 303.000 | 11.000 | 47 | False | False |
| best-time hit rate (top-2) | D3 | all | 0 | snaive7 | wt_med | 0.816 | -0.033 | -0.040 | -0.027 | 0.446 | 69479.000 | 120.000 | 170 | False | True |
| best-time hit rate (top-2) | D3 | all | 1 | snaive7 | wt_med | 0.816 | -0.033 | -0.040 | -0.027 | 0.452 | 69687.000 | 122.000 | 170 | False | True |
| best-time hit rate (top-2) | D3 | all | 2 | snaive7 | wt_med | 0.815 | -0.033 | -0.040 | -0.027 | 0.457 | 70094.000 | 123.000 | 170 | False | True |
| best-time hit rate (top-2) | D3 | all | 3 | snaive7 | wt_med | 0.815 | -0.033 | -0.039 | -0.026 | 0.461 | 70329.000 | 127.000 | 170 | False | True |
| best-time hit rate (top-2) | D3 | all | 4 | snaive7 | wt_med | 0.815 | -0.033 | -0.039 | -0.026 | 0.463 | 70330.000 | 128.000 | 170 | False | True |
| best-time hit rate (top-2) | D3 | all | 5 | snaive7 | wt_med | 0.815 | -0.032 | -0.039 | -0.026 | 0.464 | 70152.000 | 127.000 | 170 | False | True |
| best-time hit rate (top-2) | D3 | all | 6 | snaive7 | wt_med | 0.815 | -0.032 | -0.038 | -0.026 | 0.463 | 69750.000 | 124.000 | 170 | False | True |
| best-time hit rate (±30 min) | D3 | all | 0 | lvlh5_cbd | wt_med | 0.779 | -0.039 | -0.049 | -0.028 | 0.443 | 69068.000 | 114.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 1 | lvlh5_cbd | wt_med | 0.777 | -0.039 | -0.050 | -0.028 | 0.444 | 68402.000 | 114.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 2 | lvlh5_cbd | wt_med | 0.775 | -0.039 | -0.050 | -0.028 | 0.444 | 68093.000 | 113.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 3 | lvlh5_cbd | wt_med | 0.775 | -0.039 | -0.050 | -0.028 | 0.445 | 67888.000 | 112.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 4 | lvlh5_cbd | wt_med | 0.774 | -0.040 | -0.052 | -0.029 | 0.446 | 67805.000 | 114.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 5 | lvlh5_cbd | wt_med | 0.773 | -0.041 | -0.053 | -0.029 | 0.446 | 67517.000 | 113.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 6 | lvlh5_cbd | wt_med | 0.765 | -0.047 | -0.060 | -0.034 | 0.446 | 67116.000 | 114.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 7 | lvlh5_cbd | wt_med | 0.764 | -0.048 | -0.062 | -0.035 | 0.446 | 66230.000 | 114.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 10 | lvlh5_cbd | wt_med | 0.761 | -0.052 | -0.066 | -0.037 | 0.444 | 64958.000 | 112.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 14 | lvlh5_cbd | wt_med | 0.759 | -0.053 | -0.069 | -0.038 | 0.437 | 62461.000 | 113.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 21 | lvlh5_cbd | wt_med | 0.755 | -0.053 | -0.070 | -0.035 | 0.430 | 59084.000 | 111.000 | 69 | False | True |
| best-time hit rate (±30 min) | D3 | all | 30 | lvlh5_cbd | wt_med | 0.754 | -0.054 | -0.073 | -0.036 | 0.423 | 54942.000 | 103.000 | 69 | False | True |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.75 [7.66, 7.84] | 7.25 [7.14, 7.35] | 5.44 [5.39, 5.50] | 6.41 [6.32, 6.50] | 7.45 [7.36, 7.54] | 7.50 [7.41, 7.59] |
| 2 | 7.75 [7.67, 7.84] | 7.31 [7.20, 7.42] | 5.45 [5.40, 5.51] | 6.39 [6.31, 6.48] | 7.48 [7.39, 7.57] | 7.54 [7.45, 7.63] |
| 3 | 7.77 [7.68, 7.86] | 7.41 [7.30, 7.53] | 5.46 [5.41, 5.52] | 6.39 [6.31, 6.48] | 7.52 [7.43, 7.61] | 7.58 [7.49, 7.67] |
| 4 | 7.78 [7.69, 7.87] | 7.45 [7.34, 7.57] | 5.47 [5.42, 5.53] | 6.39 [6.31, 6.48] | 7.55 [7.46, 7.65] | 7.61 [7.52, 7.71] |
| 5 | 7.78 [7.69, 7.87] | 7.55 [7.44, 7.67] | 5.48 [5.43, 5.54] | 6.39 [6.30, 6.48] | 7.58 [7.49, 7.67] | 7.64 [7.55, 7.73] |
| 6 | 7.80 [7.71, 7.89] | 7.59 [7.48, 7.71] | 5.50 [5.45, 5.56] | 6.38 [6.30, 6.47] | 7.62 [7.54, 7.71] | 7.69 [7.60, 7.78] |
| 7 | 8.05 [7.96, 8.15] | 7.75 [7.64, 7.87] | 5.53 [5.48, 5.59] | 6.77 [6.68, 6.86] | 7.72 [7.63, 7.81] | 7.79 [7.70, 7.88] |
| 10 | 8.09 [8.01, 8.18] | 7.97 [7.85, 8.10] | 5.58 [5.53, 5.64] | 6.76 [6.67, 6.85] | 7.83 [7.74, 7.92] | 7.91 [7.82, 8.00] |
| 14 | 8.32 [8.22, 8.41] | 8.20 [8.09, 8.32] | 5.66 [5.60, 5.72] | 7.03 [6.94, 7.13] | 7.99 [7.90, 8.08] | 8.07 [7.98, 8.17] |
| 21 | 8.55 [8.45, 8.64] | 8.50 [8.37, 8.62] | 5.72 [5.67, 5.78] | 7.28 [7.18, 7.37] | 8.18 [8.09, 8.27] | 8.28 [8.19, 8.37] |
| 30 | 8.67 [8.57, 8.76] | 8.87 [8.71, 9.01] | 5.72 [5.66, 5.77] | 7.42 [7.32, 7.51] | 8.33 [8.25, 8.43] | 8.45 [8.36, 8.54] |
| 45 | 8.85 [8.75, 8.95] | 9.12 [8.93, 9.29] | 5.67 [5.62, 5.73] | 7.67 [7.56, 7.76] | 8.46 [8.37, 8.56] | 8.61 [8.51, 8.71] |
| 60 | 9.12 [9.01, 9.22] | 4.29 [3.08, 5.69] | 5.71 [5.65, 5.76] | 8.01 [7.90, 8.11] | 8.69 [8.59, 8.79] | 8.83 [8.73, 8.94] |
| 90 | 9.27 [9.15, 9.39] |  | 5.70 [5.63, 5.76] | 8.09 [7.97, 8.21] | 8.75 [8.64, 8.87] | 8.91 [8.80, 9.02] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| m015 | 7.92 [7.83, 8.02] | 7.26 [7.18, 7.35] | 7.62 [7.53, 7.71] | 6.98 [6.87, 7.09] | 5.08 [5.03, 5.14] | 4.68 [4.64, 4.73] | 2.45 [2.42, 2.49] | 8.68 [8.56, 8.80] | 7.30 [7.21, 7.38] |
| m030 | 7.89 [7.79, 7.99] | 7.22 [7.13, 7.30] | 7.60 [7.51, 7.69] | 6.98 [6.87, 7.09] | 5.61 [5.56, 5.67] | 5.09 [5.05, 5.14] | 4.17 [4.12, 4.23] | 8.64 [8.52, 8.76] | 7.27 [7.18, 7.36] |
| m045 | 7.92 [7.82, 8.01] | 7.24 [7.15, 7.33] | 7.62 [7.53, 7.72] | 7.01 [6.90, 7.12] | 6.02 [5.96, 6.08] | 5.38 [5.33, 5.43] | 5.09 [5.03, 5.15] | 8.65 [8.54, 8.77] | 7.29 [7.21, 7.38] |
| m060 | 7.99 [7.89, 8.08] | 7.32 [7.23, 7.41] | 7.67 [7.58, 7.76] | 7.07 [6.96, 7.18] | 6.45 [6.38, 6.51] | 5.68 [5.63, 5.74] | 5.70 [5.63, 5.76] | 8.73 [8.62, 8.85] | 7.36 [7.28, 7.45] |
| m075 | 8.07 [7.97, 8.16] | 7.42 [7.33, 7.50] | 7.82 [7.73, 7.91] | 7.17 [7.07, 7.29] | 7.18 [7.11, 7.25] | 6.30 [6.25, 6.36] | 6.27 [6.20, 6.34] | 8.83 [8.71, 8.95] | 7.45 [7.36, 7.53] |
| m090 | 8.10 [8.00, 8.19] | 7.43 [7.34, 7.52] | 7.85 [7.76, 7.94] | 7.21 [7.10, 7.32] | 7.24 [7.17, 7.31] | 6.35 [6.29, 6.41] | 6.77 [6.69, 6.85] | 8.86 [8.74, 8.98] | 7.48 [7.39, 7.57] |
| m105 | 8.16 [8.06, 8.26] | 7.47 [7.38, 7.56] | 7.91 [7.81, 8.00] | 7.25 [7.15, 7.37] | 7.39 [7.32, 7.46] | 6.47 [6.42, 6.54] | 7.11 [7.04, 7.19] | 8.93 [8.81, 9.05] | 7.53 [7.44, 7.62] |
| m120 | 8.19 [8.10, 8.29] | 7.51 [7.41, 7.60] | 7.91 [7.81, 8.00] | 7.25 [7.15, 7.37] | 7.66 [7.59, 7.74] | 6.68 [6.61, 6.74] | 7.41 [7.33, 7.49] | 8.94 [8.82, 9.06] | 7.56 [7.47, 7.65] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| m015 | 5.90 [5.34, 6.51] | 5.27 [4.76, 5.84] | 5.66 [5.12, 6.24] | 5.13 [4.65, 5.67] | 2.60 [2.34, 2.90] | 2.31 [2.08, 2.56] | 6.64 [6.05, 7.29] | 5.29 [4.79, 5.86] | persistence |
| m030 | 4.17 [3.71, 4.70] | 3.52 [3.10, 3.98] | 3.94 [3.48, 4.43] | 3.45 [3.06, 3.91] | 1.35 [1.17, 1.55] | 0.98 [0.79, 1.20] | 4.95 [4.46, 5.48] | 3.57 [3.15, 4.03] | persistence |
| m045 | 3.26 [2.82, 3.74] | 2.59 [2.20, 3.00] | 3.02 [2.59, 3.49] | 2.54 [2.17, 2.96] | 0.71 [0.55, 0.88] | 0.28 [0.07, 0.51] | 4.01 [3.56, 4.48] | 2.64 [2.24, 3.04] | persistence |
| m060 | 2.62 [2.21, 3.06] | 1.95 [1.60, 2.34] | 2.34 [1.96, 2.79] | 1.89 [1.54, 2.28] | 0.36 [0.19, 0.54] | -0.14 [-0.38, 0.11] | 3.37 [2.98, 3.81] | 1.99 [1.64, 2.37] | persistence |
| m075 | 2.24 [1.84, 2.68] | 1.55 [1.21, 1.92] | 1.94 [1.55, 2.36] | 1.44 [1.10, 1.82] | 0.69 [0.53, 0.85] | -0.00 [-0.25, 0.27] | 3.01 [2.61, 3.46] | 1.58 [1.24, 1.97] | persistence |
| m090 | 1.71 [1.34, 2.13] | 0.99 [0.65, 1.34] | 1.42 [1.06, 1.82] | 0.94 [0.60, 1.32] | 0.14 [0.00, 0.30] | -0.51 [-0.77, -0.22]* | 2.47 [2.10, 2.90] | 1.05 [0.71, 1.41] | persistence |
| m105 | 1.35 [0.97, 1.79] | 0.60 [0.26, 0.98] | 1.05 [0.67, 1.48] | 0.58 [0.24, 0.98] | -0.20 [-0.36, -0.02] | -0.84 [-1.11, -0.55]* | 2.12 [1.72, 2.57] | 0.67 [0.32, 1.06] | persistence |
| m120 | 1.01 [0.63, 1.43] | 0.25 [-0.08, 0.61] | 0.67 [0.31, 1.10] | 0.23 [-0.12, 0.61] | -0.41 [-0.58, -0.22] | -1.07 [-1.34, -0.77]* | 1.76 [1.38, 2.19] | 0.32 [-0.01, 0.69] | persistence |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 6.637 [6.049, 7.289] / [6.531, 6.748] | 5.292 [4.789, 5.864] / [5.207, 5.375] | 5.901 [5.340, 6.512] / [5.812, 5.988] | 5.270 [4.761, 5.838] / [5.186, 5.353] | 5.659 [5.122, 6.245] / [5.573, 5.750] | 5.131 [4.649, 5.674] / [5.030, 5.238] |
| m030 | 4.946 [4.460, 5.482] / [4.842, 5.055] | 3.573 [3.145, 4.031] / [3.496, 3.651] | 4.173 [3.712, 4.701] / [4.084, 4.254] | 3.525 [3.096, 3.976] / [3.449, 3.602] | 3.945 [3.477, 4.432] / [3.862, 4.032] | 3.451 [3.058, 3.913] / [3.359, 3.552] |
| m045 | 4.007 [3.560, 4.478] / [3.908, 4.112] | 2.636 [2.243, 3.042] / [2.561, 2.711] | 3.255 [2.821, 3.739] / [3.172, 3.335] | 2.588 [2.203, 3.000] / [2.513, 2.666] | 3.020 [2.595, 3.486] / [2.937, 3.106] | 2.538 [2.169, 2.962] / [2.444, 2.631] |
| m060 | 3.368 [2.979, 3.808] / [3.268, 3.477] | 1.988 [1.637, 2.374] / [1.916, 2.062] | 2.615 [2.207, 3.058] / [2.533, 2.696] | 1.948 [1.601, 2.336] / [1.875, 2.023] | 2.345 [1.959, 2.785] / [2.264, 2.430] | 1.888 [1.540, 2.276] / [1.796, 1.983] |
| m075 | 3.010 [2.610, 3.456] / [2.908, 3.122] | 1.583 [1.239, 1.967] / [1.508, 1.664] | 2.242 [1.843, 2.680] / [2.154, 2.325] | 1.546 [1.209, 1.921] / [1.471, 1.624] | 1.938 [1.553, 2.361] / [1.858, 2.026] | 1.435 [1.098, 1.821] / [1.344, 1.530] |
| m090 | 2.474 [2.102, 2.898] / [2.376, 2.584] | 1.049 [0.709, 1.405] / [0.971, 1.126] | 1.714 [1.342, 2.128] / [1.625, 1.796] | 0.989 [0.653, 1.342] / [0.912, 1.066] | 1.424 [1.059, 1.817] / [1.342, 1.511] | 0.938 [0.598, 1.324] / [0.847, 1.033] |
| m105 | 2.120 [1.721, 2.566] / [2.017, 2.228] | 0.667 [0.322, 1.057] / [0.593, 0.744] | 1.352 [0.972, 1.788] / [1.262, 1.435] | 0.595 [0.255, 0.979] / [0.519, 0.670] | 1.050 [0.669, 1.477] / [0.966, 1.138] | 0.582 [0.237, 0.984] / [0.490, 0.677] |
| m120 | 1.756 [1.381, 2.187] / [1.652, 1.867] | 0.316 [-0.009, 0.686] / [0.241, 0.392] | 1.012 [0.630, 1.435] / [0.926, 1.095] | 0.247 [-0.075, 0.609] / [0.173, 0.321] | 0.673 [0.307, 1.101] / [0.591, 0.760] | 0.231 [-0.124, 0.610] / [0.142, 0.319] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -6.637 [-7.289, -6.049]* / [-6.748, -6.531]* | -1.340 [-1.536, -1.165]* / [-1.425, -1.263]* | -0.655 [-0.855, -0.447]* / [-0.747, -0.565]* | -1.346 [-1.544, -1.165]* / [-1.430, -1.266]* | -0.943 [-1.121, -0.772]* / [-1.015, -0.872]* | -1.019 [-1.211, -0.824]* / [-1.114, -0.933]* |
| m030 | -4.946 [-5.482, -4.460]* / [-5.055, -4.842]* | -1.351 [-1.555, -1.171]* / [-1.435, -1.272]* | -0.679 [-0.878, -0.486]* / [-0.768, -0.590]* | -1.386 [-1.595, -1.197]* / [-1.468, -1.306]* | -0.952 [-1.124, -0.782]* / [-1.023, -0.883]* | -1.036 [-1.229, -0.834]* / [-1.126, -0.946]* |
| m045 | -4.007 [-4.478, -3.560]* / [-4.112, -3.908]* | -1.352 [-1.543, -1.171]* / [-1.434, -1.277]* | -0.675 [-0.878, -0.474]* / [-0.765, -0.587]* | -1.387 [-1.588, -1.208]* / [-1.469, -1.310]* | -0.950 [-1.117, -0.782]* / [-1.020, -0.882]* | -1.026 [-1.221, -0.821]* / [-1.115, -0.939]* |
| m060 | -3.368 [-3.808, -2.979]* / [-3.477, -3.268]* | -1.369 [-1.561, -1.192]* / [-1.452, -1.295]* | -0.684 [-0.882, -0.489]* / [-0.775, -0.597]* | -1.392 [-1.594, -1.212]* / [-1.476, -1.314]* | -0.991 [-1.168, -0.817]* / [-1.061, -0.924]* | -1.057 [-1.259, -0.858]* / [-1.150, -0.968]* |
| m075 | -3.010 [-3.456, -2.610]* / [-3.122, -2.908]* | -1.375 [-1.579, -1.201]* / [-1.459, -1.297]* | -0.676 [-0.880, -0.470]* / [-0.770, -0.584]* | -1.383 [-1.584, -1.205]* / [-1.466, -1.305]* | -0.957 [-1.140, -0.784]* / [-1.029, -0.887]* | -1.016 [-1.229, -0.801]* / [-1.111, -0.927]* |
| m090 | -2.474 [-2.898, -2.102]* / [-2.584, -2.376]* | -1.374 [-1.565, -1.204]* / [-1.461, -1.299]* | -0.679 [-0.874, -0.477]* / [-0.774, -0.589]* | -1.408 [-1.605, -1.228]* / [-1.493, -1.332]* | -0.962 [-1.142, -0.789]* / [-1.033, -0.892]* | -1.024 [-1.231, -0.810]* / [-1.117, -0.937]* |
| m105 | -2.120 [-2.566, -1.721]* / [-2.228, -2.017]* | -1.391 [-1.583, -1.218]* / [-1.474, -1.312]* | -0.699 [-0.897, -0.502]* / [-0.793, -0.609]* | -1.429 [-1.626, -1.255]* / [-1.513, -1.351]* | -0.971 [-1.147, -0.798]* / [-1.043, -0.900]* | -1.033 [-1.231, -0.830]* / [-1.124, -0.942]* |
| m120 | -1.756 [-2.187, -1.381]* / [-1.867, -1.652]* | -1.372 [-1.559, -1.206]* / [-1.457, -1.296]* | -0.660 [-0.857, -0.465]* / [-0.754, -0.570]* | -1.400 [-1.590, -1.226]* / [-1.484, -1.322]* | -0.981 [-1.145, -0.813]* / [-1.055, -0.910]* | -1.047 [-1.243, -0.850]* / [-1.137, -0.957]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.292 [-5.864, -4.789]* / [-5.375, -5.207]* | 1.340 [1.165, 1.536] / [1.263, 1.425] | 0.671 [0.574, 0.783] / [0.640, 0.702] | -0.027 [-0.053, -0.001]* / [-0.036, -0.017]* | 0.396 [0.332, 0.462] / [0.360, 0.434] | 0.213 [0.110, 0.323] / [0.157, 0.267] |
| m030 | -3.573 [-4.031, -3.145]* / [-3.651, -3.496]* | 1.351 [1.171, 1.555] / [1.272, 1.435] | 0.655 [0.566, 0.757] / [0.625, 0.685] | -0.048 [-0.064, -0.033]* / [-0.055, -0.041]* | 0.401 [0.342, 0.462] / [0.366, 0.437] | 0.222 [0.120, 0.329] / [0.168, 0.274] |
| m045 | -2.636 [-3.042, -2.243]* / [-2.711, -2.561]* | 1.352 [1.171, 1.543] / [1.277, 1.434] | 0.662 [0.571, 0.770] / [0.631, 0.694] | -0.045 [-0.062, -0.029]* / [-0.052, -0.040]* | 0.407 [0.348, 0.468] / [0.373, 0.444] | 0.235 [0.128, 0.347] / [0.181, 0.287] |
| m060 | -1.988 [-2.374, -1.637]* / [-2.062, -1.916]* | 1.369 [1.192, 1.561] / [1.295, 1.452] | 0.671 [0.581, 0.774] / [0.640, 0.702] | -0.039 [-0.064, -0.015]* / [-0.046, -0.031]* | 0.379 [0.320, 0.442] / [0.345, 0.415] | 0.211 [0.109, 0.315] / [0.158, 0.261] |
| m075 | -1.583 [-1.967, -1.239]* / [-1.664, -1.508]* | 1.375 [1.201, 1.579] / [1.297, 1.459] | 0.689 [0.594, 0.791] / [0.656, 0.720] | -0.024 [-0.049, -0.001]* / [-0.034, -0.015]* | 0.430 [0.368, 0.498] / [0.393, 0.466] | 0.245 [0.147, 0.351] / [0.191, 0.299] |
| m090 | -1.049 [-1.405, -0.709]* / [-1.126, -0.971]* | 1.374 [1.204, 1.565] / [1.299, 1.461] | 0.685 [0.589, 0.787] / [0.652, 0.717] | -0.049 [-0.072, -0.028]* / [-0.058, -0.041]* | 0.425 [0.366, 0.488] / [0.390, 0.463] | 0.243 [0.148, 0.340] / [0.191, 0.296] |
| m105 | -0.667 [-1.057, -0.322]* / [-0.744, -0.593]* | 1.391 [1.218, 1.583] / [1.312, 1.474] | 0.690 [0.590, 0.801] / [0.658, 0.722] | -0.053 [-0.069, -0.037]* / [-0.061, -0.046]* | 0.432 [0.372, 0.496] / [0.395, 0.469] | 0.268 [0.171, 0.368] / [0.215, 0.319] |
| m120 | -0.316 [-0.686, 0.009] / [-0.392, -0.241]*  ! | 1.372 [1.206, 1.559] / [1.296, 1.457] | 0.701 [0.599, 0.812] / [0.668, 0.732] | -0.046 [-0.068, -0.022]* / [-0.055, -0.037]* | 0.400 [0.339, 0.466] / [0.362, 0.437] | 0.236 [0.143, 0.336] / [0.181, 0.289] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -5.901 [-6.512, -5.340]* / [-5.988, -5.812]* | 0.655 [0.447, 0.855] / [0.565, 0.747] | -0.671 [-0.783, -0.574]* / [-0.702, -0.640]* | -0.687 [-0.796, -0.590]* / [-0.719, -0.655]* | -0.262 [-0.378, -0.166]* / [-0.309, -0.215]* | -0.467 [-0.628, -0.307]* / [-0.535, -0.402]* |
| m030 | -4.173 [-4.701, -3.712]* / [-4.254, -4.084]* | 0.679 [0.486, 0.878] / [0.590, 0.768] | -0.655 [-0.757, -0.566]* / [-0.685, -0.625]* | -0.701 [-0.811, -0.608]* / [-0.731, -0.670]* | -0.247 [-0.358, -0.150]* / [-0.294, -0.200]* | -0.446 [-0.610, -0.291]* / [-0.512, -0.384]* |
| m045 | -3.255 [-3.739, -2.821]* / [-3.335, -3.172]* | 0.675 [0.474, 0.878] / [0.587, 0.765] | -0.662 [-0.770, -0.571]* / [-0.694, -0.631]* | -0.704 [-0.810, -0.609]* / [-0.736, -0.673]* | -0.245 [-0.354, -0.146]* / [-0.292, -0.199]* | -0.439 [-0.606, -0.283]* / [-0.504, -0.376]* |
| m060 | -2.615 [-3.058, -2.207]* / [-2.696, -2.533]* | 0.684 [0.489, 0.882] / [0.597, 0.775] | -0.671 [-0.774, -0.581]* / [-0.702, -0.640]* | -0.706 [-0.811, -0.610]* / [-0.738, -0.675]* | -0.282 [-0.384, -0.187]* / [-0.325, -0.235]* | -0.476 [-0.645, -0.317]* / [-0.542, -0.415]* |
| m075 | -2.242 [-2.680, -1.843]* / [-2.325, -2.154]* | 0.676 [0.470, 0.880] / [0.584, 0.770] | -0.689 [-0.791, -0.594]* / [-0.720, -0.656]* | -0.702 [-0.813, -0.606]* / [-0.734, -0.669]* | -0.248 [-0.365, -0.146]* / [-0.295, -0.199]* | -0.455 [-0.619, -0.298]* / [-0.524, -0.390]* |
| m090 | -1.714 [-2.128, -1.342]* / [-1.796, -1.625]* | 0.679 [0.477, 0.874] / [0.589, 0.774] | -0.685 [-0.787, -0.589]* / [-0.717, -0.652]* | -0.727 [-0.840, -0.627]* / [-0.759, -0.694]* | -0.248 [-0.364, -0.146]* / [-0.294, -0.199]* | -0.450 [-0.613, -0.301]* / [-0.518, -0.383]* |
| m105 | -1.352 [-1.788, -0.972]* / [-1.435, -1.262]* | 0.699 [0.502, 0.897] / [0.609, 0.793] | -0.690 [-0.801, -0.590]* / [-0.722, -0.658]* | -0.739 [-0.850, -0.638]* / [-0.773, -0.705]* | -0.247 [-0.369, -0.144]* / [-0.293, -0.199]* | -0.436 [-0.589, -0.284]* / [-0.504, -0.370]* |
| m120 | -1.012 [-1.435, -0.630]* / [-1.095, -0.926]* | 0.660 [0.465, 0.857] / [0.570, 0.754] | -0.701 [-0.812, -0.599]* / [-0.732, -0.668]* | -0.740 [-0.856, -0.632]* / [-0.771, -0.704]* | -0.290 [-0.419, -0.186]* / [-0.337, -0.242]* | -0.463 [-0.621, -0.309]* / [-0.534, -0.398]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| m015 | 15.38 [15.18, 15.55] | 14.15 [13.99, 14.31] | 14.50 [14.32, 14.67] | 13.44 [13.25, 13.64] | 9.60 [9.51, 9.68] | 8.76 [8.68, 8.83] | 4.57 [4.51, 4.64] | 16.85 [16.64, 17.07] | 14.26 [14.10, 14.42] |
| m030 | 15.37 [15.17, 15.54] | 14.13 [13.96, 14.28] | 14.52 [14.34, 14.69] | 13.46 [13.27, 13.66] | 10.67 [10.59, 10.75] | 9.60 [9.53, 9.68] | 7.93 [7.85, 8.00] | 16.83 [16.61, 17.05] | 14.27 [14.10, 14.43] |
| m045 | 15.36 [15.17, 15.53] | 14.14 [13.96, 14.29] | 14.52 [14.35, 14.68] | 13.46 [13.27, 13.65] | 11.40 [11.31, 11.48] | 10.09 [10.01, 10.17] | 9.65 [9.56, 9.74] | 16.80 [16.58, 17.01] | 14.25 [14.08, 14.40] |
| m060 | 15.41 [15.22, 15.57] | 14.19 [14.02, 14.34] | 14.51 [14.35, 14.68] | 13.49 [13.31, 13.69] | 12.14 [12.04, 12.22] | 10.58 [10.49, 10.66] | 10.81 [10.71, 10.91] | 16.85 [16.64, 17.05] | 14.29 [14.12, 14.44] |
| m075 | 15.45 [15.26, 15.61] | 14.25 [14.08, 14.40] | 14.57 [14.41, 14.74] | 13.48 [13.28, 13.66] | 13.38 [13.26, 13.49] | 11.59 [11.49, 11.69] | 11.73 [11.62, 11.84] | 16.91 [16.70, 17.13] | 14.34 [14.17, 14.50] |
| m090 | 15.56 [15.36, 15.73] | 14.32 [14.15, 14.47] | 14.68 [14.51, 14.85] | 13.59 [13.40, 13.78] | 13.51 [13.39, 13.61] | 11.73 [11.62, 11.82] | 12.69 [12.57, 12.81] | 16.99 [16.77, 17.21] | 14.43 [14.27, 14.59] |
| m105 | 15.64 [15.43, 15.82] | 14.37 [14.20, 14.52] | 14.76 [14.58, 14.93] | 13.65 [13.46, 13.85] | 13.72 [13.60, 13.82] | 11.94 [11.83, 12.04] | 13.28 [13.15, 13.41] | 17.08 [16.87, 17.30] | 14.50 [14.32, 14.66] |
| m120 | 15.66 [15.46, 15.83] | 14.37 [14.20, 14.52] | 14.72 [14.54, 14.89] | 13.62 [13.42, 13.81] | 14.21 [14.08, 14.32] | 12.31 [12.20, 12.42] | 13.83 [13.70, 13.96] | 17.04 [16.82, 17.25] | 14.51 [14.34, 14.66] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| m015 | 11.23 [10.10, 12.34] | 10.07 [9.14, 10.97] | 10.41 [9.39, 11.39] | 9.51 [8.60, 10.40] | 4.81 [4.27, 5.42] | 4.16 [3.67, 4.60] | 12.62 [11.57, 13.63] | 10.16 [9.24, 11.07] | persistence |
| m030 | 7.87 [6.87, 8.93] | 6.68 [5.80, 7.59] | 7.08 [6.13, 8.05] | 6.05 [5.19, 6.99] | 2.34 [1.90, 2.75] | 1.55 [1.10, 2.02] | 9.32 [8.37, 10.28] | 6.80 [5.93, 7.71] | persistence |
| m045 | 6.18 [5.24, 7.18] | 4.95 [4.13, 5.84] | 5.37 [4.48, 6.30] | 4.30 [3.49, 5.21] | 1.06 [0.71, 1.43] | 0.20 [-0.22, 0.66] | 7.57 [6.68, 8.48] | 5.05 [4.21, 5.94] | persistence |
| m060 | 4.97 [4.12, 5.93] | 3.73 [2.97, 4.55] | 4.10 [3.28, 4.98] | 3.02 [2.24, 3.88] | 0.32 [-0.03, 0.69] | -0.65 [-1.05, -0.18]* | 6.38 [5.55, 7.26] | 3.83 [3.07, 4.65] | persistence |
| m075 | 4.33 [3.49, 5.32] | 3.03 [2.32, 3.82] | 3.42 [2.66, 4.25] | 2.22 [1.49, 3.02] | 1.10 [0.74, 1.44] | -0.35 [-0.75, 0.11] | 5.78 [4.96, 6.60] | 3.13 [2.41, 3.93] | persistence |
| m090 | 3.33 [2.52, 4.27] | 1.97 [1.27, 2.76] | 2.42 [1.68, 3.23] | 1.20 [0.45, 2.02] | -0.01 [-0.30, 0.29] | -1.35 [-1.77, -0.89]* | 4.76 [3.99, 5.57] | 2.09 [1.40, 2.89] | persistence |
| m105 | 2.75 [1.87, 3.76] | 1.30 [0.57, 2.15] | 1.79 [0.98, 2.72] | 0.56 [-0.26, 1.41] | -0.70 [-0.97, -0.41]* | -1.95 [-2.39, -1.47]* | 4.19 [3.32, 5.17] | 1.46 [0.72, 2.32] | persistence |
| m120 | 2.13 [1.29, 3.06] | 0.63 [-0.08, 1.46] | 1.08 [0.32, 1.97] | -0.11 [-0.89, 0.75] | -1.18 [-1.49, -0.86] | -2.40 [-2.84, -1.92]* | 3.50 [2.69, 4.44] | 0.81 [0.11, 1.63] | persistence |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | 12.624 [11.566, 13.634] / [12.393, 12.858] | 10.164 [9.242, 11.075] / [9.982, 10.329] | 11.232 [10.104, 12.343] / [11.015, 11.413] | 10.074 [9.136, 10.971] / [9.889, 10.237] | 10.412 [9.387, 11.386] / [10.214, 10.593] | 9.513 [8.599, 10.397] / [9.302, 9.723] |
| m030 | 9.324 [8.372, 10.277] / [9.094, 9.558] | 6.799 [5.932, 7.712] / [6.614, 6.974] | 7.867 [6.871, 8.929] / [7.661, 8.047] | 6.676 [5.800, 7.586] / [6.498, 6.852] | 7.079 [6.129, 8.052] / [6.887, 7.257] | 6.053 [5.189, 6.990] / [5.830, 6.281] |
| m045 | 7.574 [6.681, 8.481] / [7.353, 7.815] | 5.053 [4.212, 5.942] / [4.874, 5.234] | 6.183 [5.238, 7.179] / [5.986, 6.369] | 4.953 [4.125, 5.840] / [4.772, 5.129] | 5.369 [4.481, 6.304] / [5.178, 5.556] | 4.299 [3.487, 5.211] / [4.076, 4.518] |
| m060 | 6.383 [5.547, 7.260] / [6.153, 6.614] | 3.830 [3.068, 4.647] / [3.654, 3.996] | 4.973 [4.115, 5.926] / [4.781, 5.156] | 3.734 [2.966, 4.553] / [3.558, 3.897] | 4.097 [3.284, 4.982] / [3.911, 4.282] | 3.017 [2.236, 3.877] / [2.797, 3.236] |
| m075 | 5.780 [4.963, 6.604] / [5.543, 6.015] | 3.129 [2.411, 3.934] / [2.949, 3.309] | 4.330 [3.495, 5.316] / [4.120, 4.517] | 3.031 [2.316, 3.822] / [2.859, 3.206] | 3.418 [2.664, 4.254] / [3.228, 3.609] | 2.220 [1.492, 3.019] / [2.010, 2.436] |
| m090 | 4.765 [3.992, 5.575] / [4.536, 5.000] | 2.093 [1.401, 2.889] / [1.911, 2.266] | 3.326 [2.515, 4.273] / [3.110, 3.513] | 1.970 [1.275, 2.763] / [1.795, 2.142] | 2.423 [1.683, 3.227] / [2.241, 2.612] | 1.198 [0.454, 2.015] / [0.981, 1.410] |
| m105 | 4.192 [3.324, 5.168] / [3.960, 4.433] | 1.462 [0.724, 2.324] / [1.277, 1.643] | 2.748 [1.871, 3.760] / [2.530, 2.939] | 1.301 [0.570, 2.148] / [1.118, 1.480] | 1.788 [0.979, 2.718] / [1.594, 1.984] | 0.557 [-0.261, 1.413] / [0.340, 0.773] |
| m120 | 3.502 [2.686, 4.441] / [3.261, 3.747] | 0.805 [0.110, 1.629] / [0.635, 0.981] | 2.129 [1.293, 3.062] / [1.916, 2.315] | 0.630 [-0.078, 1.459] / [0.455, 0.806] | 1.084 [0.322, 1.971] / [0.898, 1.277] | -0.106 [-0.890, 0.750] / [-0.322, 0.100] |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -12.624 [-13.634, -11.566]* / [-12.858, -12.393]* | -2.571 [-2.947, -2.247]* / [-2.747, -2.382]* | -1.233 [-1.672, -0.766]* / [-1.439, -1.019]* | -2.642 [-3.018, -2.302]* / [-2.815, -2.456]* | -2.157 [-2.440, -1.879]* / [-2.322, -1.981]* | -2.667 [-3.041, -2.273]* / [-2.887, -2.436]* |
| m030 | -9.324 [-10.277, -8.372]* / [-9.558, -9.094]* | -2.585 [-2.951, -2.259]* / [-2.760, -2.397]* | -1.268 [-1.715, -0.816]* / [-1.476, -1.056]* | -2.696 [-3.060, -2.372]* / [-2.869, -2.507]* | -2.161 [-2.452, -1.879]* / [-2.326, -1.996]* | -2.683 [-3.052, -2.287]* / [-2.902, -2.458]* |
| m045 | -7.574 [-8.481, -6.681]* / [-7.815, -7.353]* | -2.571 [-2.921, -2.252]* / [-2.756, -2.387]* | -1.221 [-1.645, -0.745]* / [-1.433, -1.013]* | -2.656 [-3.001, -2.336]* / [-2.837, -2.466]* | -2.124 [-2.404, -1.842]* / [-2.291, -1.963]* | -2.634 [-2.995, -2.242]* / [-2.851, -2.418]* |
| m060 | -6.383 [-7.260, -5.547]* / [-6.614, -6.153]* | -2.590 [-2.925, -2.272]* / [-2.763, -2.417]* | -1.243 [-1.642, -0.822]* / [-1.450, -1.032]* | -2.667 [-3.000, -2.344]* / [-2.841, -2.493]* | -2.187 [-2.461, -1.902]* / [-2.347, -2.028]* | -2.673 [-3.036, -2.264]* / [-2.889, -2.459]* |
| m075 | -5.780 [-6.604, -4.963]* / [-6.015, -5.543]* | -2.592 [-2.938, -2.266]* / [-2.763, -2.406]* | -1.242 [-1.654, -0.801]* / [-1.447, -1.027]* | -2.659 [-3.007, -2.312]* / [-2.836, -2.470]* | -2.198 [-2.483, -1.915]* / [-2.368, -2.020]* | -2.677 [-3.049, -2.272]* / [-2.903, -2.460]* |
| m090 | -4.765 [-5.575, -3.992]* / [-5.000, -4.536]* | -2.597 [-2.930, -2.266]* / [-2.769, -2.413]* | -1.235 [-1.634, -0.791]* / [-1.452, -1.022]* | -2.688 [-3.040, -2.336]* / [-2.864, -2.506]* | -2.195 [-2.478, -1.906]* / [-2.361, -2.025]* | -2.681 [-3.044, -2.283]* / [-2.909, -2.463]* |
| m105 | -4.192 [-5.168, -3.324]* / [-4.433, -3.960]* | -2.615 [-2.943, -2.276]* / [-2.790, -2.433]* | -1.269 [-1.673, -0.826]* / [-1.480, -1.055]* | -2.722 [-3.048, -2.383]* / [-2.902, -2.539]* | -2.206 [-2.469, -1.929]* / [-2.366, -2.035]* | -2.695 [-3.048, -2.316]* / [-2.914, -2.472]* |
| m120 | -3.502 [-4.441, -2.686]* / [-3.747, -3.261]* | -2.554 [-2.881, -2.238]* / [-2.730, -2.376]* | -1.166 [-1.579, -0.721]* / [-1.373, -0.957]* | -2.666 [-2.996, -2.338]* / [-2.842, -2.495]* | -2.200 [-2.455, -1.928]* / [-2.368, -2.033]* | -2.686 [-3.064, -2.313]* / [-2.898, -2.476]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -10.164 [-11.075, -9.242]* / [-10.329, -9.982]* | 2.571 [2.247, 2.947] / [2.382, 2.747] | 1.297 [1.096, 1.551] / [1.223, 1.371] | -0.110 [-0.176, -0.052]* / [-0.135, -0.084]* | 0.401 [0.255, 0.549] / [0.310, 0.492] | -0.064 [-0.338, 0.211] / [-0.203, 0.085] |
| m030 | -6.799 [-7.712, -5.932]* / [-6.974, -6.614]* | 2.585 [2.259, 2.951] / [2.397, 2.760] | 1.270 [1.077, 1.502] / [1.198, 1.342] | -0.137 [-0.176, -0.105]* / [-0.156, -0.119]* | 0.416 [0.275, 0.560] / [0.328, 0.506] | -0.042 [-0.313, 0.239] / [-0.183, 0.101] |
| m045 | -5.053 [-5.942, -4.212]* / [-5.234, -4.874]* | 2.571 [2.252, 2.921] / [2.387, 2.756] | 1.302 [1.106, 1.548] / [1.227, 1.378] | -0.109 [-0.150, -0.069]* / [-0.125, -0.093]* | 0.439 [0.301, 0.582] / [0.354, 0.527] | -0.000 [-0.271, 0.278] / [-0.139, 0.137] |
| m060 | -3.830 [-4.647, -3.068]* / [-3.996, -3.654]* | 2.590 [2.272, 2.925] / [2.417, 2.763] | 1.302 [1.106, 1.535] / [1.229, 1.376] | -0.107 [-0.158, -0.055]* / [-0.126, -0.088]* | 0.386 [0.255, 0.523] / [0.302, 0.474] | -0.051 [-0.316, 0.220] / [-0.186, 0.081] |
| m075 | -3.129 [-3.934, -2.411]* / [-3.309, -2.949]* | 2.592 [2.266, 2.938] / [2.406, 2.763] | 1.321 [1.113, 1.565] / [1.243, 1.396] | -0.100 [-0.158, -0.050]* / [-0.125, -0.078]* | 0.395 [0.259, 0.521] / [0.306, 0.486] | -0.109 [-0.372, 0.163] / [-0.249, 0.034] |
| m090 | -2.093 [-2.889, -1.401]* / [-2.266, -1.911]* | 2.597 [2.266, 2.930] / [2.413, 2.769] | 1.333 [1.118, 1.574] / [1.255, 1.410] | -0.122 [-0.180, -0.072]* / [-0.145, -0.100]* | 0.403 [0.272, 0.524] / [0.319, 0.492] | -0.094 [-0.354, 0.164] / [-0.229, 0.041] |
| m105 | -1.462 [-2.324, -0.724]* / [-1.643, -1.277]* | 2.615 [2.276, 2.943] / [2.433, 2.790] | 1.336 [1.108, 1.599] / [1.259, 1.416] | -0.133 [-0.172, -0.098]* / [-0.155, -0.114]* | 0.411 [0.286, 0.534] / [0.327, 0.501] | -0.035 [-0.300, 0.220] / [-0.176, 0.103] |
| m120 | -0.805 [-1.629, -0.110]* / [-0.981, -0.635]* | 2.554 [2.238, 2.881] / [2.376, 2.730] | 1.359 [1.142, 1.614] / [1.279, 1.432] | -0.143 [-0.194, -0.091]* / [-0.166, -0.122]* | 0.355 [0.228, 0.483] / [0.273, 0.442] | -0.088 [-0.354, 0.169] / [-0.223, 0.047] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| m015 | -11.232 [-12.343, -10.104]* / [-11.413, -11.015]* | 1.233 [0.766, 1.672] / [1.019, 1.439] | -1.297 [-1.551, -1.096]* / [-1.371, -1.223]* | -1.384 [-1.641, -1.176]* / [-1.457, -1.303]* | -0.854 [-1.143, -0.615]* / [-0.965, -0.736]* | -1.425 [-1.874, -1.008]* / [-1.597, -1.255]* |
| m030 | -7.867 [-8.929, -6.871]* / [-8.047, -7.661]* | 1.268 [0.816, 1.715] / [1.056, 1.476] | -1.270 [-1.502, -1.077]* / [-1.342, -1.198]* | -1.399 [-1.649, -1.194]* / [-1.473, -1.327]* | -0.825 [-1.107, -0.595]* / [-0.933, -0.711]* | -1.386 [-1.815, -0.987]* / [-1.557, -1.223]* |
| m045 | -6.183 [-7.179, -5.238]* / [-6.369, -5.986]* | 1.221 [0.745, 1.645] / [1.013, 1.433] | -1.302 [-1.548, -1.106]* / [-1.378, -1.227]* | -1.398 [-1.649, -1.194]* / [-1.479, -1.323]* | -0.827 [-1.108, -0.596]* / [-0.942, -0.715]* | -1.373 [-1.813, -0.973]* / [-1.540, -1.213]* |
| m060 | -4.973 [-5.926, -4.115]* / [-5.156, -4.781]* | 1.243 [0.822, 1.642] / [1.032, 1.450] | -1.302 [-1.535, -1.106]* / [-1.376, -1.229]* | -1.397 [-1.629, -1.193]* / [-1.475, -1.322]* | -0.878 [-1.126, -0.665]* / [-0.987, -0.769]* | -1.432 [-1.876, -1.017]* / [-1.598, -1.272]* |
| m075 | -4.330 [-5.316, -3.495]* / [-4.517, -4.120]* | 1.242 [0.801, 1.654] / [1.027, 1.447] | -1.321 [-1.565, -1.113]* / [-1.396, -1.243]* | -1.391 [-1.640, -1.171]* / [-1.470, -1.314]* | -0.884 [-1.168, -0.651]* / [-0.998, -0.773]* | -1.488 [-1.930, -1.087]* / [-1.660, -1.329]* |
| m090 | -3.326 [-4.273, -2.515]* / [-3.513, -3.110]* | 1.235 [0.791, 1.634] / [1.022, 1.452] | -1.333 [-1.574, -1.118]* / [-1.410, -1.255]* | -1.438 [-1.696, -1.215]* / [-1.516, -1.356]* | -0.889 [-1.180, -0.658]* / [-1.000, -0.774]* | -1.480 [-1.901, -1.086]* / [-1.651, -1.325]* |
| m105 | -2.748 [-3.760, -1.871]* / [-2.939, -2.530]* | 1.269 [0.826, 1.673] / [1.055, 1.480] | -1.336 [-1.599, -1.108]* / [-1.416, -1.259]* | -1.462 [-1.736, -1.229]* / [-1.541, -1.385]* | -0.888 [-1.205, -0.647]* / [-1.001, -0.773]* | -1.434 [-1.848, -1.040]* / [-1.612, -1.270]* |
| m120 | -2.129 [-3.062, -1.293]* / [-2.315, -1.916]* | 1.166 [0.721, 1.579] / [0.957, 1.373] | -1.359 [-1.614, -1.142]* / [-1.432, -1.279]* | -1.487 [-1.769, -1.255]* / [-1.561, -1.404]* | -0.967 [-1.294, -0.729]* / [-1.078, -0.853]* | -1.476 [-1.900, -1.084]* / [-1.645, -1.317]* |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| m015 | Asia | 8.71 | 7.96 | 8.32 | 7.55 | 5.83 | 5.07 | 2.72 | 9.24 | 7.91 |
| m015 | EU | 6.14 | 5.45 | 5.72 | 5.26 | 3.86 | 3.51 | 2.11 | 6.44 | 5.47 |
| m015 | NA | 9.15 | 8.63 | 9.03 | 8.39 | 5.85 | 5.61 | 2.61 | 10.59 | 8.71 |
| m060 | Asia | 8.70 | 8.00 | 8.36 | 7.65 | 6.84 | 5.85 | 6.07 | 9.32 | 7.95 |
| m060 | EU | 6.27 | 5.53 | 5.83 | 5.41 | 4.83 | 4.24 | 4.13 | 6.52 | 5.59 |
| m060 | NA | 9.20 | 8.68 | 9.06 | 8.42 | 7.83 | 7.03 | 6.93 | 10.63 | 8.76 |
| m120 | Asia | 9.06 | 8.16 | 8.55 | 7.75 | 8.37 | 7.19 | 7.89 | 9.56 | 8.20 |
| m120 | EU | 6.45 | 5.75 | 6.11 | 5.66 | 5.82 | 4.97 | 5.21 | 6.73 | 5.77 |
| m120 | NA | 9.31 | 8.79 | 9.20 | 8.53 | 9.03 | 8.02 | 9.12 | 10.74 | 8.88 |

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | nf_tide | nf_nhits |
|---|---|---|---|---|---|---|---|---|---|
| m015 | -0.15 | -0.03 | -1.44 | -1.09 | -1.79 | -1.30 | -1.42 | -0.55 | -0.16 |
| m030 | -0.21 | -0.03 | -1.49 | -1.14 | -1.70 | -1.21 | -1.37 | -0.68 | -0.40 |
| m045 | -0.09 | -0.04 | -1.51 | -1.17 | -1.65 | -1.16 | -1.33 | -0.71 | -0.45 |
| m060 | 0.04 | -0.03 | -1.55 | -1.20 | -1.72 | -1.23 | -1.39 | -0.75 | -0.43 |
| m075 | -0.05 | -0.03 | -1.48 | -1.12 | -1.81 | -1.28 | -1.37 | -0.66 | -0.37 |
| m090 | 0.12 | -0.05 | -1.52 | -1.18 | -1.78 | -1.26 | -1.37 | -0.72 | -0.65 |
| m105 | 0.28 | -0.04 | -1.53 | -1.20 | -1.78 | -1.25 | -1.36 | -0.68 | -0.75 |
| m120 | 0.48 | -0.04 | -1.54 | -1.20 | -1.83 | -1.30 | -1.41 | -0.58 | -0.70 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 8.06 [7.97, 8.16] | 7.38 [7.29, 7.47] | 7.78 [7.69, 7.88] | 7.14 [7.03, 7.25] | 8.05 [7.97, 8.13] | 6.82 [6.75, 6.88] | 8.60 [8.51, 8.69] | 8.80 [8.69, 8.92] | 7.43 [7.34, 7.51] |
| h4-8 | 8.34 [8.24, 8.44] | 7.63 [7.53, 7.72] | 8.06 [7.97, 8.16] | 7.41 [7.30, 7.54] | 8.64 [8.55, 8.73] | 7.42 [7.34, 7.49] | 11.07 [10.95, 11.18] | 9.09 [8.97, 9.21] | 7.69 [7.60, 7.78] |
| h8+ | 7.96 [7.85, 8.06] | 7.23 [7.13, 7.33] | 7.71 [7.61, 7.82] | 7.27 [7.13, 7.40] | 9.07 [8.89, 9.31] | 7.67 [7.57, 7.77] | 13.19 [12.98, 13.40] | 8.69 [8.56, 8.82] | 7.29 [7.19, 7.40] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 0.68 [0.59, 0.79] | -0.04 [-0.06, -0.03]* | 0.42 [0.36, 0.48] | 0.24 [0.14, 0.34] | 0.65 [0.47, 0.84] | -0.56 [-0.73, -0.42]* | 0.77 [0.37, 1.16] | 1.37 [1.19, 1.56] | wt_med |
| h4-8 | 0.71 [0.61, 0.82] | -0.06 [-0.07, -0.04]* | 0.43 [0.37, 0.50] | 0.25 [0.15, 0.36] | 0.99 [0.82, 1.18] | -0.22 [-0.34, -0.11]* | 3.23 [2.63, 3.81] | 1.42 [1.24, 1.61] | wt_med |
| h8+ | 0.66 [0.55, 0.79] | -0.06 [-0.09, -0.03]* | 0.47 [0.40, 0.56] | 0.44 [0.32, 0.59] | 1.83 [1.35, 2.34] | 0.43 [0.22, 0.64] | 6.13 [4.95, 7.43] | 1.45 [1.27, 1.64] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.730 [0.341, 1.158] / [0.624, 0.837] | -0.774 [-1.160, -0.368]* / [-0.853, -0.693]* | -0.041 [-0.448, 0.379] / [-0.128, 0.043] | -0.847 [-1.232, -0.448]* / [-0.926, -0.769]* | -0.391 [-0.800, 0.062] / [-0.478, -0.308]*  ! | -0.836 [-1.226, -0.419]* / [-0.933, -0.743]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.730 [-1.158, -0.341]* / [-0.837, -0.624]* | -1.366 [-1.561, -1.189]* / [-1.450, -1.290]* | -0.673 [-0.872, -0.480]* / [-0.766, -0.584]* | -1.391 [-1.594, -1.211]* / [-1.474, -1.314]* | -0.953 [-1.130, -0.781]* / [-1.023, -0.885]* | -1.030 [-1.233, -0.824]* / [-1.117, -0.943]* |
| h4-8 | 1.629 [1.035, 2.193] / [1.498, 1.757] | -1.424 [-1.614, -1.242]* / [-1.510, -1.343]* | -0.708 [-0.913, -0.506]* / [-0.803, -0.612]* | -1.465 [-1.660, -1.277]* / [-1.550, -1.383]* | -1.002 [-1.182, -0.829]* / [-1.075, -0.931]* | -1.084 [-1.301, -0.852]* / [-1.179, -0.992]* |
| h8+ | 4.519 [3.391, 5.840] / [4.336, 4.724] | -1.450 [-1.643, -1.266]* / [-1.534, -1.362]* | -0.808 [-1.007, -0.582]* / [-0.906, -0.715]* | -1.485 [-1.699, -1.294]* / [-1.565, -1.394]* | -0.976 [-1.165, -0.798]* / [-1.054, -0.899]* | -0.949 [-1.181, -0.700]* / [-1.052, -0.843]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.774 [0.368, 1.160] / [0.693, 0.853] | 1.366 [1.189, 1.561] / [1.290, 1.450] | 0.682 [0.588, 0.788] / [0.651, 0.713] | -0.041 [-0.055, -0.027]* / [-0.046, -0.036]* | 0.420 [0.362, 0.480] / [0.384, 0.458] | 0.241 [0.140, 0.340] / [0.187, 0.293] |
| h4-8 | 3.233 [2.633, 3.811] / [3.130, 3.333] | 1.424 [1.242, 1.614] / [1.343, 1.510] | 0.713 [0.614, 0.820] / [0.681, 0.746] | -0.058 [-0.074, -0.041]* / [-0.064, -0.053]* | 0.431 [0.369, 0.497] / [0.392, 0.471] | 0.253 [0.149, 0.361] / [0.197, 0.308] |
| h8+ | 6.130 [4.949, 7.435] / [5.962, 6.302] | 1.450 [1.266, 1.643] / [1.362, 1.534] | 0.662 [0.552, 0.787] / [0.629, 0.696] | -0.061 [-0.092, -0.030]* / [-0.073, -0.051]* | 0.473 [0.400, 0.559] / [0.433, 0.513] | 0.444 [0.323, 0.587] / [0.383, 0.503] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 0.041 [-0.379, 0.448] / [-0.043, 0.128] | 0.673 [0.480, 0.872] / [0.584, 0.766] | -0.682 [-0.788, -0.588]* / [-0.713, -0.651]* | -0.717 [-0.827, -0.620]* / [-0.747, -0.685]* | -0.251 [-0.362, -0.150]* / [-0.298, -0.205]* | -0.452 [-0.611, -0.297]* / [-0.520, -0.388]* |
| h4-8 | 2.478 [1.862, 3.070] / [2.381, 2.585] | 0.708 [0.506, 0.913] / [0.612, 0.803] | -0.713 [-0.820, -0.614]* / [-0.746, -0.681]* | -0.765 [-0.881, -0.663]* / [-0.799, -0.731]* | -0.270 [-0.388, -0.172]* / [-0.321, -0.221]* | -0.481 [-0.657, -0.318]* / [-0.554, -0.414]* |
| h8+ | 5.457 [4.338, 6.760] / [5.290, 5.639] | 0.808 [0.582, 1.007] / [0.715, 0.906] | -0.662 [-0.787, -0.552]* / [-0.696, -0.629]* | -0.721 [-0.864, -0.597]* / [-0.759, -0.684]* | -0.181 [-0.311, -0.067]* / [-0.232, -0.126]* | -0.224 [-0.437, -0.023]* / [-0.297, -0.153]* |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 15.59 [15.39, 15.75] | 14.33 [14.17, 14.48] | 14.71 [14.54, 14.87] | 13.61 [13.42, 13.79] | 15.12 [14.98, 15.24] | 12.73 [12.61, 12.83] | 15.85 [15.71, 16.00] | 17.02 [16.80, 17.22] | 14.45 [14.28, 14.60] |
| h4-8 | 15.73 [15.54, 15.89] | 14.47 [14.30, 14.62] | 14.85 [14.69, 15.03] | 13.82 [13.62, 14.02] | 15.97 [15.81, 16.11] | 13.67 [13.52, 13.79] | 19.43 [19.23, 19.61] | 17.19 [16.97, 17.40] | 14.61 [14.44, 14.76] |
| h8+ | 14.29 [14.11, 14.47] | 13.16 [13.01, 13.33] | 13.58 [13.40, 13.76] | 12.99 [12.78, 13.21] | 16.12 [15.94, 16.30] | 13.69 [13.52, 13.84] | 21.79 [21.44, 22.11] | 15.79 [15.56, 16.01] | 13.29 [13.12, 13.45] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 1.33 [1.12, 1.58] | -0.12 [-0.15, -0.09]* | 0.42 [0.29, 0.55] | -0.05 [-0.32, 0.22] | 0.74 [0.26, 1.20] | -1.64 [-1.99, -1.31]* | 1.24 [0.44, 1.99] | 2.59 [2.27, 2.95] | wt_med |
| h4-8 | 1.34 [1.14, 1.60] | -0.15 [-0.19, -0.11]* | 0.41 [0.28, 0.54] | -0.03 [-0.30, 0.25] | 1.43 [1.06, 1.82] | -0.84 [-1.12, -0.58]* | 5.35 [4.40, 6.31] | 2.66 [2.33, 3.01] | wt_med |
| h8+ | 1.16 [0.97, 1.41] | -0.15 [-0.22, -0.08]* | 0.41 [0.28, 0.56] | 0.27 [0.03, 0.56] | 2.88 [2.21, 3.57] | 0.48 [0.02, 0.96] | 9.98 [7.58, 12.77] | 2.60 [2.29, 2.95] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `persistence`:

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.554 [0.725, 2.471] / [1.318, 1.786] | -1.237 [-1.990, -0.443]* / [-1.414, -1.061]* | 0.125 [-0.732, 0.993] / [-0.080, 0.310] | -1.397 [-2.137, -0.594]* / [-1.573, -1.222]* | -0.878 [-1.677, 0.003] / [-1.069, -0.690]*  ! | -2.100 [-2.854, -1.239]* / [-2.318, -1.888]* |
| h4-8 | -2.459 [-3.510, -1.365]* / [-2.716, -2.192]* | -5.352 [-6.308, -4.397]* / [-5.554, -5.144]* | -3.985 [-4.942, -3.008]* / [-4.204, -3.776]* | -5.554 [-6.496, -4.578]* / [-5.752, -5.344]* | -4.989 [-6.004, -3.934]* / [-5.203, -4.784]* | -5.732 [-6.638, -4.772]* / [-5.989, -5.468]* |

vs `snaive7`:

| lead | persistence | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -1.554 [-2.471, -0.725]* / [-1.786, -1.318]* | -2.595 [-2.945, -2.267]* / [-2.766, -2.418]* | -1.235 [-1.665, -0.787]* / [-1.441, -1.018]* | -2.681 [-3.036, -2.343]* / [-2.852, -2.502]* | -2.168 [-2.443, -1.885]* / [-2.328, -2.004]* | -2.694 [-3.064, -2.300]* / [-2.907, -2.479]* |
| h4-8 | 2.459 [1.365, 3.510] / [2.192, 2.716] | -2.665 [-3.006, -2.328]* / [-2.838, -2.487]* | -1.308 [-1.729, -0.876]* / [-1.510, -1.088]* | -2.776 [-3.119, -2.434]* / [-2.956, -2.596]* | -2.249 [-2.528, -1.964]* / [-2.412, -2.083]* | -2.771 [-3.174, -2.338]* / [-2.980, -2.557]* |
| h8+ | 7.280 [4.943, 10.043] / [6.908, 7.642] | -2.600 [-2.945, -2.288]* / [-2.768, -2.421]* | -1.445 [-1.849, -1.038]* / [-1.649, -1.250]* | -2.701 [-3.063, -2.368]* / [-2.868, -2.522]* | -2.169 [-2.446, -1.862]* / [-2.330, -2.006]* | -2.433 [-2.863, -2.002]* / [-2.649, -2.191]* |

vs `wt_med`:

| lead | persistence | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | 1.237 [0.443, 1.990] / [1.061, 1.414] | 2.595 [2.267, 2.945] / [2.418, 2.766] | 1.327 [1.121, 1.580] / [1.254, 1.401] | -0.118 [-0.152, -0.087]* / [-0.131, -0.107]* | 0.420 [0.291, 0.550] / [0.335, 0.513] | -0.053 [-0.324, 0.221] / [-0.191, 0.085] |
| h4-8 | 5.352 [4.397, 6.308] / [5.144, 5.554] | 2.665 [2.328, 3.006] / [2.487, 2.838] | 1.343 [1.137, 1.597] / [1.268, 1.421] | -0.146 [-0.186, -0.109]* / [-0.160, -0.133]* | 0.412 [0.278, 0.545] / [0.320, 0.505] | -0.029 [-0.305, 0.246] / [-0.168, 0.114] |
| h8+ | 9.983 [7.584, 12.770] / [9.655, 10.315] | 2.600 [2.288, 2.945] / [2.421, 2.768] | 1.162 [0.967, 1.412] / [1.090, 1.233] | -0.150 [-0.225, -0.080]* / [-0.176, -0.125]* | 0.414 [0.278, 0.556] / [0.323, 0.500] | 0.271 [0.033, 0.564] / [0.140, 0.408] |

vs `clim`:

| lead | persistence | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|
| h2-4 | -0.125 [-0.993, 0.732] / [-0.310, 0.080] | 1.235 [0.787, 1.665] / [1.018, 1.441] | -1.327 [-1.580, -1.121]* / [-1.401, -1.254]* | -1.430 [-1.684, -1.221]* / [-1.504, -1.356]* | -0.868 [-1.149, -0.646]* / [-0.978, -0.756]* | -1.447 [-1.886, -1.044]* / [-1.615, -1.284]* |
| h4-8 | 3.985 [3.008, 4.942] / [3.776, 4.204] | 1.308 [0.876, 1.729] / [1.088, 1.510] | -1.343 [-1.597, -1.137]* / [-1.421, -1.268]* | -1.472 [-1.730, -1.262]* / [-1.551, -1.398]* | -0.885 [-1.162, -0.660]* / [-0.997, -0.769]* | -1.465 [-1.925, -1.053]* / [-1.639, -1.302]* |
| h8+ | 8.783 [6.412, 11.606] / [8.447, 9.126] | 1.445 [1.038, 1.849] / [1.250, 1.649] | -1.162 [-1.412, -0.967]* / [-1.233, -1.090]* | -1.297 [-1.576, -1.076]* / [-1.378, -1.223]* | -0.719 [-0.990, -0.499]* / [-0.826, -0.609]* | -0.934 [-1.395, -0.506]* / [-1.118, -0.773]* |

**MAE by region (all rides)**

_no rows_

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | nf_tide | nf_nhits |
|---|---|---|---|---|---|---|---|---|---|
| h2-4 | 0.62 | -0.04 | -1.53 | -1.18 | -1.78 | -1.26 | -1.39 | -0.91 | -0.97 |
| h4-8 | 1.31 | -0.05 | -1.59 | -1.13 | -1.81 | -1.27 | -1.40 | -1.29 | -0.81 |
| h8+ | 3.45 | -0.06 | -1.56 | -1.01 | -1.85 | -1.34 | -1.42 | -0.50 | 0.23 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.01 [7.92, 8.11] | 7.34 [7.25, 7.43] | 9.70 [9.54, 9.88] | 7.73 [7.64, 7.82] | 7.10 [7.00, 7.21] | 8.52 [8.39, 8.66] |  | 7.40 [7.32, 7.47] |  | 5.41 [5.36, 5.46] | 6.42 [6.33, 6.51] | 8.76 [8.65, 8.88] | 7.38 [7.30, 7.47] |
| 1 | 8.03 [7.94, 8.13] | 7.45 [7.36, 7.54] | 9.83 [9.67, 10.01] | 7.75 [7.66, 7.84] | 7.25 [7.14, 7.35] | 8.59 [8.49, 8.70] | 8.60 [8.48, 8.74] | 7.85 [7.77, 7.93] | 7.94 [7.85, 8.03] | 5.44 [5.39, 5.50] | 6.41 [6.32, 6.50] | 8.76 [8.64, 8.87] | 7.50 [7.41, 7.59] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.68 [0.58, 0.78] | -0.04 [-0.06, -0.03]* | 2.85 [2.40, 3.32] | 0.41 [0.35, 0.47] | 0.24 [0.14, 0.34] | 1.17 [0.96, 1.42] |  | 0.06 [-0.06, 0.17] |  | -1.99 [-2.27, -1.76]* | -0.92 [-1.15, -0.67]* | 1.36 [1.19, 1.56] | wt_med |
| 1 | 0.60 [0.52, 0.71] | -0.05 [-0.06, -0.03]* | 2.85 [2.40, 3.32] | 0.33 [0.27, 0.38] | 0.30 [0.19, 0.41] | 1.13 [0.95, 1.35] | 1.16 [0.96, 1.40] | 0.40 [0.28, 0.51] | 0.49 [0.41, 0.57] | -2.07 [-2.36, -1.83]* | -1.03 [-1.26, -0.80]* | 1.26 [1.08, 1.44] | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -1.363 [-1.560, -1.188]* / [-1.445, -1.287]* | -0.676 [-0.871, -0.478]* / [-0.767, -0.588]* | -1.388 [-1.585, -1.211]* / [-1.470, -1.312]* | -0.960 [-1.136, -0.791]* / [-1.029, -0.893]* | -1.028 [-1.225, -0.826]* / [-1.114, -0.941]* | 1.722 [1.267, 2.227] / [1.583, 1.866] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.363 [1.188, 1.560] / [1.287, 1.445] | 0.676 [0.581, 0.783] / [0.645, 0.707] | -0.040 [-0.055, -0.027]* / [-0.045, -0.035]* | 0.409 [0.354, 0.469] / [0.374, 0.445] | 0.236 [0.138, 0.335] / [0.183, 0.287] | 2.848 [2.403, 3.321] / [2.744, 2.960] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 0.676 [0.478, 0.871] / [0.588, 0.767] | -0.676 [-0.783, -0.581]* / [-0.707, -0.645]* | -0.711 [-0.821, -0.612]* / [-0.741, -0.679]* | -0.257 [-0.372, -0.156]* / [-0.303, -0.210]* | -0.451 [-0.608, -0.297]* / [-0.517, -0.387]* | 2.196 [1.750, 2.692] / [2.082, 2.313] |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 15.44 [15.25, 15.61] | 14.21 [14.04, 14.35] | 18.60 [18.25, 18.95] | 14.56 [14.39, 14.73] | 13.49 [13.30, 13.68] | 16.06 [15.89, 16.22] |  | 13.90 [13.75, 14.03] |  | 9.84 [9.76, 9.92] | 11.50 [11.32, 11.68] | 16.88 [16.66, 17.09] | 14.32 [14.15, 14.47] |
| 1 | 15.46 [15.26, 15.62] | 14.37 [14.20, 14.51] | 18.86 [18.50, 19.22] | 14.56 [14.39, 14.72] | 13.71 [13.52, 13.88] | 16.21 [16.03, 16.37] | 15.86 [15.68, 16.05] | 14.81 [14.65, 14.94] | 14.55 [14.39, 14.72] | 9.87 [9.79, 9.95] | 11.45 [11.27, 11.63] | 16.85 [16.64, 17.06] | 14.49 [14.32, 14.64] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.31 [1.11, 1.56] | -0.12 [-0.15, -0.09]* | 5.17 [3.93, 6.49] | 0.40 [0.28, 0.53] | -0.06 [-0.32, 0.20] | 1.81 [1.49, 2.12] |  | -0.34 [-0.61, -0.09]* |  | -4.50 [-5.03, -4.03]* | -2.66 [-2.91, -2.38]* | 2.58 [2.26, 2.92] | wt_med |
| 1 | 1.17 [0.97, 1.41] | -0.13 [-0.17, -0.10]* | 5.20 [4.01, 6.50] | 0.23 [0.11, 0.35] | 0.02 [-0.24, 0.29] | 1.78 [1.48, 2.09] | 1.47 [1.16, 1.77] | 0.40 [0.13, 0.65] | 0.16 [-0.05, 0.36] | -4.64 [-5.20, -4.15]* | -2.87 [-3.11, -2.61]* | 2.38 [2.05, 2.73] | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | -2.579 [-2.922, -2.262]* / [-2.744, -2.404]* | -1.234 [-1.649, -0.790]* / [-1.437, -1.020]* | -2.666 [-3.009, -2.339]* / [-2.833, -2.493]* | -2.172 [-2.442, -1.900]* / [-2.328, -2.010]* | -2.665 [-3.024, -2.278]* / [-2.875, -2.455]* | 2.840 [1.572, 4.257] / [2.455, 3.250] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 2.579 [2.262, 2.922] / [2.404, 2.744] | 1.311 [1.105, 1.560] / [1.238, 1.384] | -0.117 [-0.152, -0.088]* / [-0.131, -0.105]* | 0.401 [0.275, 0.525] / [0.319, 0.491] | -0.058 [-0.322, 0.203] / [-0.193, 0.076] | 5.167 [3.932, 6.492] / [4.870, 5.465] |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd |
|---|---|---|---|---|---|---|
| 0 | 1.234 [0.790, 1.649] / [1.020, 1.437] | -1.311 [-1.560, -1.105]* / [-1.384, -1.238]* | -1.414 [-1.669, -1.206]* / [-1.486, -1.342]* | -0.873 [-1.161, -0.652]* / [-0.981, -0.764]* | -1.431 [-1.868, -1.039]* / [-1.595, -1.271]* | 3.926 [2.572, 5.302] / [3.588, 4.272] |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 8.83 | 8.01 | 12.53 | 8.41 | 7.65 | 9.48 |  | 8.20 |  | 5.47 | 7.18 | 9.37 | 8.03 |
| 0 | EU | 6.28 | 5.57 | 7.86 | 5.91 | 5.47 | 6.53 |  | 5.58 |  | 4.08 | 4.96 | 6.55 | 5.60 |
| 0 | NA | 9.16 | 8.66 | 10.49 | 9.07 | 8.42 | 9.91 |  | 8.72 |  | 6.69 | 7.35 | 10.60 | 8.73 |
| 1 | Asia | 8.82 | 8.15 | 12.75 | 8.43 | 7.79 | 9.58 | 9.31 | 8.77 | 8.61 | 5.50 | 7.18 | 9.37 | 8.18 |
| 1 | EU | 6.30 | 5.69 | 7.94 | 5.94 | 5.64 | 6.56 | 6.79 | 5.97 | 6.20 | 4.12 | 4.94 | 6.54 | 5.72 |
| 1 | NA | 9.21 | 8.75 | 10.63 | 9.09 | 8.56 | 10.01 | 9.95 | 9.15 | 9.24 | 6.72 | 7.34 | 10.62 | 8.83 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 8.03 [7.94, 8.13] | 7.45 [7.36, 7.54] | 9.83 [9.67, 10.01] | 7.75 [7.66, 7.84] | 7.25 [7.14, 7.35] | 8.59 [8.49, 8.70] | 8.60 [8.48, 8.74] | 7.85 [7.77, 7.93] | 7.94 [7.85, 8.03] | 5.44 [5.39, 5.50] | 6.41 [6.32, 6.50] |  |  | 8.76 [8.64, 8.87] | 7.50 [7.41, 7.59] |
| 2 | 8.05 [7.95, 8.14] | 7.48 [7.39, 7.57] | 9.87 [9.71, 10.04] | 7.75 [7.67, 7.84] | 7.31 [7.20, 7.42] | 8.71 [8.59, 8.84] | 8.68 [8.55, 8.84] | 7.96 [7.88, 8.04] | 8.01 [7.92, 8.09] | 5.45 [5.40, 5.51] | 6.39 [6.31, 6.48] |  |  | 8.75 [8.64, 8.87] | 7.54 [7.45, 7.63] |
| 3 | 8.09 [7.99, 8.18] | 7.52 [7.43, 7.61] | 9.93 [9.78, 10.10] | 7.77 [7.68, 7.86] | 7.41 [7.30, 7.53] | 8.81 [8.69, 8.96] | 8.78 [8.65, 8.92] | 8.04 [7.96, 8.13] | 8.11 [8.01, 8.20] | 5.46 [5.41, 5.52] | 6.39 [6.31, 6.48] | 7.84 [7.72, 7.95] | 7.82 [7.71, 7.93] | 8.76 [8.65, 8.88] | 7.58 [7.49, 7.67] |
| 4 | 8.11 [8.02, 8.20] | 7.55 [7.46, 7.65] | 10.00 [9.84, 10.18] | 7.78 [7.69, 7.87] | 7.45 [7.34, 7.57] | 8.73 [8.63, 8.84] | 8.67 [8.57, 8.78] | 8.07 [7.98, 8.15] | 8.12 [8.03, 8.21] | 5.47 [5.42, 5.53] | 6.39 [6.31, 6.48] | 7.90 [7.79, 8.02] | 7.88 [7.77, 8.00] | 8.75 [8.64, 8.87] | 7.61 [7.52, 7.71] |
| 5 | 8.13 [8.04, 8.22] | 7.58 [7.49, 7.67] | 10.02 [9.87, 10.19] | 7.78 [7.69, 7.87] | 7.55 [7.44, 7.67] | 8.77 [8.67, 8.87] | 8.70 [8.60, 8.81] | 8.07 [7.98, 8.15] | 8.13 [8.04, 8.22] | 5.48 [5.43, 5.54] | 6.39 [6.30, 6.48] | 8.01 [7.89, 8.12] | 7.99 [7.88, 8.10] | 8.74 [8.63, 8.86] | 7.64 [7.55, 7.73] |
| 6 | 8.16 [8.06, 8.25] | 7.62 [7.54, 7.71] | 10.30 [10.13, 10.48] | 7.80 [7.71, 7.89] | 7.59 [7.48, 7.71] | 8.79 [8.69, 8.89] | 8.72 [8.61, 8.82] | 8.14 [8.06, 8.22] | 8.19 [8.10, 8.28] | 5.50 [5.45, 5.56] | 6.38 [6.30, 6.47] | 8.00 [7.88, 8.13] | 7.98 [7.87, 8.11] | 8.75 [8.64, 8.86] | 7.69 [7.60, 7.78] |
| 7 | 8.18 [8.08, 8.28] | 7.72 [7.63, 7.81] | 10.45 [10.28, 10.62] | 8.05 [7.96, 8.15] | 7.75 [7.64, 7.87] | 8.84 [8.75, 8.94] | 8.77 [8.67, 8.87] | 8.30 [8.21, 8.39] | 8.27 [8.18, 8.37] | 5.53 [5.48, 5.59] | 6.77 [6.68, 6.86] | 8.17 [8.05, 8.30] | 8.15 [8.03, 8.29] |  | 7.79 [7.70, 7.88] |
| 10 | 8.27 [8.17, 8.36] | 7.83 [7.74, 7.92] | 10.68 [10.51, 10.87] | 8.09 [8.01, 8.18] | 7.97 [7.85, 8.10] |  |  |  |  | 5.58 [5.53, 5.64] | 6.76 [6.67, 6.85] | 8.37 [8.24, 8.50] | 8.35 [8.22, 8.48] |  | 7.91 [7.82, 8.00] |
| 14 | 8.39 [8.30, 8.49] | 7.99 [7.90, 8.08] | 10.85 [10.67, 11.04] | 8.32 [8.22, 8.41] | 8.20 [8.09, 8.32] |  |  |  |  | 5.66 [5.60, 5.72] | 7.03 [6.94, 7.13] | 8.58 [8.46, 8.71] | 8.56 [8.44, 8.69] |  | 8.07 [7.98, 8.17] |
| 21 | 8.57 [8.48, 8.67] | 8.18 [8.09, 8.27] | 11.16 [10.97, 11.35] | 8.55 [8.45, 8.64] | 8.50 [8.37, 8.62] |  |  |  |  | 5.72 [5.67, 5.78] | 7.28 [7.18, 7.37] | 8.85 [8.72, 8.99] | 8.83 [8.70, 8.97] |  | 8.28 [8.19, 8.37] |
| 30 | 8.68 [8.58, 8.78] | 8.33 [8.25, 8.43] | 11.40 [11.20, 11.62] | 8.67 [8.57, 8.76] | 8.87 [8.71, 9.01] |  |  |  |  | 5.72 [5.66, 5.77] | 7.42 [7.32, 7.51] | 9.26 [9.11, 9.41] | 9.24 [9.09, 9.39] |  | 8.45 [8.36, 8.54] |
| 45 | 8.74 [8.64, 8.84] | 8.46 [8.37, 8.56] | 11.55 [11.24, 11.87] | 8.85 [8.75, 8.95] | 9.12 [8.93, 9.29] |  |  |  |  | 5.67 [5.62, 5.73] | 7.67 [7.56, 7.76] | 9.95 [9.77, 10.12] | 9.93 [9.75, 10.10] |  | 8.61 [8.51, 8.71] |
| 60 | 8.76 [8.66, 8.86] | 8.69 [8.59, 8.79] |  | 9.12 [9.01, 9.22] | 4.29 [3.08, 5.69] |  |  |  |  | 5.71 [5.65, 5.76] | 8.01 [7.90, 8.11] | 4.33 [3.14, 5.67] | 4.33 [3.14, 5.67] |  | 8.83 [8.73, 8.94] |
| 90 | 8.96 [8.84, 9.07] | 8.75 [8.64, 8.87] |  | 9.27 [9.15, 9.39] |  |  |  |  |  | 5.70 [5.63, 5.76] | 8.09 [7.97, 8.21] |  |  |  | 8.91 [8.80, 9.02] |

**Paired difference vs the DISPLAY reference, all** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.60 [0.52, 0.71] | -0.05 [-0.06, -0.03]* | 2.85 [2.40, 3.32] | 0.33 [0.27, 0.38] | 0.30 [0.19, 0.41] | 1.13 [0.95, 1.35] | 1.16 [0.96, 1.40] | 0.40 [0.28, 0.51] | 0.49 [0.41, 0.57] | -2.07 [-2.36, -1.83]* | -1.03 [-1.26, -0.80]* |  |  | 1.26 [1.08, 1.44] | wt_med |
| 2 | 0.59 [0.50, 0.69] | -0.05 [-0.06, -0.03]* | 2.85 [2.41, 3.32] | 0.29 [0.23, 0.35] | 0.33 [0.23, 0.43] | 1.22 [1.03, 1.43] | 1.21 [1.03, 1.42] | 0.48 [0.36, 0.60] | 0.53 [0.45, 0.61] | -2.10 [-2.39, -1.86]* | -1.09 [-1.31, -0.84]* |  |  | 1.21 [1.04, 1.40] | wt_med |
| 3 | 0.58 [0.49, 0.68] | -0.05 [-0.07, -0.04]* | 2.84 [2.41, 3.30] | 0.26 [0.20, 0.32] | 0.38 [0.28, 0.48] | 1.28 [1.06, 1.53] | 1.26 [1.07, 1.50] | 0.52 [0.40, 0.62] | 0.59 [0.51, 0.66] | -2.14 [-2.42, -1.89]* | -1.13 [-1.36, -0.88]* | 0.89 [0.70, 1.08] | 0.87 [0.69, 1.06] | 1.17 [1.00, 1.36] | wt_med |
| 4 | 0.58 [0.49, 0.68] | -0.05 [-0.07, -0.04]* | 2.85 [2.41, 3.31] | 0.23 [0.17, 0.29] | 0.37 [0.27, 0.47] | 1.17 [0.99, 1.38] | 1.13 [0.98, 1.29] | 0.52 [0.39, 0.64] | 0.57 [0.49, 0.66] | -2.16 [-2.45, -1.91]* | -1.17 [-1.40, -0.92]* | 0.90 [0.71, 1.10] | 0.88 [0.69, 1.08] | 1.12 [0.95, 1.31] | wt_med |
| 5 | 0.57 [0.48, 0.67] | -0.05 [-0.07, -0.04]* | 2.83 [2.40, 3.30] | 0.20 [0.14, 0.27] | 0.43 [0.33, 0.54] | 1.19 [1.01, 1.39] | 1.14 [0.99, 1.29] | 0.50 [0.37, 0.61] | 0.56 [0.47, 0.65] | -2.18 [-2.47, -1.92]* | -1.20 [-1.43, -0.96]* | 0.98 [0.79, 1.20] | 0.96 [0.77, 1.18] | 1.09 [0.90, 1.28] | wt_med |
| 6 | 0.55 [0.45, 0.64] | -0.05 [-0.07, -0.04]* | 3.05 [2.60, 3.51] | 0.17 [0.10, 0.23] | 0.42 [0.32, 0.53] | 1.16 [0.99, 1.35] | 1.10 [0.96, 1.26] | 0.52 [0.40, 0.63] | 0.58 [0.49, 0.66] | -2.20 [-2.51, -1.95]* | -1.26 [-1.49, -1.01]* | 0.93 [0.74, 1.13] | 0.91 [0.73, 1.11] | 1.04 [0.86, 1.23] | wt_med |
| 7 | 0.48 [0.39, 0.57] | -0.06 [-0.08, -0.04]* | 3.10 [2.64, 3.57] | 0.38 [0.32, 0.45] | 0.49 [0.38, 0.61] | 1.11 [0.94, 1.30] | 1.06 [0.92, 1.20] | 0.58 [0.45, 0.69] | 0.56 [0.47, 0.65] | -2.27 [-2.58, -2.02]* | -0.92 [-1.14, -0.67]* | 1.01 [0.79, 1.25] | 0.99 [0.77, 1.23] |  | wt_med |
| 10 | 0.46 [0.38, 0.55] | -0.06 [-0.08, -0.05]* | 3.16 [2.66, 3.67] | 0.29 [0.23, 0.36] | 0.56 [0.43, 0.69] |  |  |  |  | -2.34 [-2.66, -2.08]* | -1.05 [-1.29, -0.81]* | 1.08 [0.83, 1.36] | 1.06 [0.81, 1.34] |  | wt_med |
| 14 | 0.42 [0.34, 0.51] | -0.07 [-0.08, -0.05]* | 3.10 [2.61, 3.61] | 0.39 [0.33, 0.45] | 0.58 [0.44, 0.74] |  |  |  |  | -2.43 [-2.76, -2.15]* | -0.91 [-1.14, -0.66]* | 1.08 [0.81, 1.41] | 1.07 [0.79, 1.39] |  | wt_med |
| 21 | 0.39 [0.30, 0.48] | -0.07 [-0.09, -0.05]* | 3.06 [2.56, 3.59] | 0.39 [0.33, 0.44] | 0.55 [0.39, 0.71] |  |  |  |  | -2.58 [-2.91, -2.28]* | -0.90 [-1.13, -0.66]* | 1.00 [0.68, 1.36] | 0.98 [0.66, 1.34] |  | wt_med |
| 30 | 0.33 [0.23, 0.43] | -0.08 [-0.10, -0.06]* | 2.96 [2.48, 3.46] | 0.35 [0.29, 0.41] | 0.52 [0.35, 0.68] |  |  |  |  | -2.75 [-3.10, -2.43]* | -0.92 [-1.16, -0.67]* | 0.98 [0.66, 1.33] | 0.96 [0.64, 1.31] |  | wt_med |
| 45 | 0.21 [0.07, 0.35] | -0.10 [-0.12, -0.08]* | 1.97 [1.45, 2.47] | 0.39 [0.31, 0.48] | 0.48 [0.32, 0.65] |  |  |  |  | -2.95 [-3.35, -2.61]* | -0.80 [-1.05, -0.54]* | 1.19 [0.85, 1.50] | 1.17 [0.82, 1.48] |  | wt_med |
| 60 | 0.08 [-0.12, 0.26] | -0.10 [-0.13, -0.08] |  | 0.47 [0.38, 0.56] | 0.05 [-1.19, 1.57] |  |  |  |  | -3.14 [-3.57, -2.76]* | -0.65 [-0.92, -0.36]* | -0.11 [-1.27, 0.79] | -0.11 [-1.28, 0.81] |  | wt_med |
| 90 | 0.20 [-0.02, 0.41] | -0.10 [-0.12, -0.07]* |  | 0.54 [0.44, 0.66] |  |  |  |  |  | -3.22 [-3.70, -2.79]* | -0.63 [-0.92, -0.32]* |  |  |  | wt_med |

**Paired difference vs EACH naive candidate, all** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -1.256 [-1.443, -1.085]* / [-1.340, -1.180]* | -0.638 [-0.839, -0.437]* / [-0.729, -0.550]* | -1.284 [-1.478, -1.108]* / [-1.366, -1.209]* | -0.942 [-1.119, -0.772]* / [-1.011, -0.874]* | -0.852 [-1.047, -0.660]* / [-0.938, -0.766]* | 1.841 [1.397, 2.342] / [1.703, 1.988] |  |  |
| 2 | -1.212 [-1.398, -1.044]* / [-1.290, -1.135]* | -0.613 [-0.815, -0.407]* / [-0.707, -0.527]* | -1.243 [-1.437, -1.069]* / [-1.320, -1.167]* | -0.936 [-1.112, -0.767]* / [-1.005, -0.867]* | -0.788 [-0.987, -0.601]* / [-0.878, -0.703]* | 1.876 [1.434, 2.368] / [1.735, 2.021] |  |  |
| 3 | -1.168 [-1.359, -0.995]* / [-1.250, -1.093]* | -0.571 [-0.776, -0.363]* / [-0.664, -0.485]* | -1.200 [-1.399, -1.027]* / [-1.285, -1.125]* | -0.925 [-1.105, -0.757]* / [-0.994, -0.857]* | -0.690 [-0.884, -0.504]* / [-0.779, -0.605]* | 1.913 [1.471, 2.397] / [1.773, 2.060] | -0.164 [-0.408, 0.075] / [-0.262, -0.071]*  ! | -0.184 [-0.425, 0.054] / [-0.282, -0.092]*  ! |
| 4 | -1.125 [-1.314, -0.950]* / [-1.203, -1.050]* | -0.524 [-0.739, -0.311]* / [-0.616, -0.436]* | -1.156 [-1.355, -0.982]* / [-1.237, -1.083]* | -0.909 [-1.087, -0.738]* / [-0.978, -0.840]* | -0.668 [-0.861, -0.478]* / [-0.751, -0.586]* | 1.970 [1.502, 2.477] / [1.830, 2.122] | -0.123 [-0.369, 0.123] / [-0.214, -0.023]*  ! | -0.143 [-0.393, 0.100] / [-0.234, -0.044]*  ! |
| 5 | -1.088 [-1.279, -0.904]* / [-1.170, -1.011]* | -0.490 [-0.703, -0.273]* / [-0.581, -0.402]* | -1.122 [-1.319, -0.942]* / [-1.203, -1.047]* | -0.900 [-1.078, -0.731]* / [-0.969, -0.832]* | -0.567 [-0.755, -0.374]* / [-0.648, -0.488]* | 1.965 [1.497, 2.454] / [1.828, 2.108] | -0.002 [-0.230, 0.227] / [-0.090, 0.094] | -0.022 [-0.247, 0.207] / [-0.108, 0.075] |
| 6 | -1.037 [-1.229, -0.859]* / [-1.117, -0.959]* | -0.457 [-0.670, -0.236]* / [-0.550, -0.364]* | -1.073 [-1.271, -0.893]* / [-1.152, -0.996]* | -0.885 [-1.062, -0.718]* / [-0.950, -0.818]* | -0.547 [-0.747, -0.340]* / [-0.629, -0.458]* | 2.247 [1.774, 2.766] / [2.101, 2.405] | -0.035 [-0.259, 0.196] / [-0.127, 0.065] | -0.053 [-0.279, 0.180] / [-0.145, 0.047] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.256 [1.085, 1.443] / [1.180, 1.340] | 0.604 [0.515, 0.707] / [0.575, 0.634] | -0.046 [-0.061, -0.031]* / [-0.051, -0.041]* | 0.325 [0.269, 0.381] / [0.290, 0.362] | 0.296 [0.193, 0.407] / [0.239, 0.351] | 2.852 [2.403, 3.317] / [2.747, 2.964] |  |  |
| 2 | 1.212 [1.044, 1.398] / [1.135, 1.290] | 0.585 [0.497, 0.686] / [0.557, 0.615] | -0.049 [-0.064, -0.034]* / [-0.054, -0.044]* | 0.289 [0.232, 0.345] / [0.254, 0.326] | 0.326 [0.230, 0.432] / [0.272, 0.381] | 2.850 [2.413, 3.325] / [2.748, 2.969] |  |  |
| 3 | 1.168 [0.995, 1.359] / [1.093, 1.250] | 0.581 [0.493, 0.682] / [0.550, 0.612] | -0.051 [-0.066, -0.037]* / [-0.056, -0.046]* | 0.256 [0.199, 0.315] / [0.222, 0.293] | 0.377 [0.285, 0.482] / [0.320, 0.432] | 2.842 [2.411, 3.298] / [2.741, 2.959] | 0.887 [0.703, 1.083] / [0.808, 0.967] | 0.867 [0.686, 1.063] / [0.789, 0.947] |
| 4 | 1.125 [0.950, 1.314] / [1.050, 1.203] | 0.577 [0.488, 0.677] / [0.547, 0.609] | -0.051 [-0.066, -0.037]* / [-0.056, -0.046]* | 0.229 [0.169, 0.290] / [0.194, 0.265] | 0.365 [0.265, 0.468] / [0.306, 0.421] | 2.850 [2.414, 3.311] / [2.750, 2.964] | 0.895 [0.706, 1.105] / [0.818, 0.974] | 0.876 [0.688, 1.083] / [0.799, 0.955] |
| 5 | 1.088 [0.904, 1.279] / [1.011, 1.170] | 0.571 [0.478, 0.667] / [0.541, 0.603] | -0.052 [-0.067, -0.038]* / [-0.057, -0.047]* | 0.203 [0.142, 0.266] / [0.168, 0.240] | 0.433 [0.334, 0.538] / [0.374, 0.491] | 2.831 [2.399, 3.304] / [2.733, 2.948] | 0.983 [0.791, 1.202] / [0.903, 1.065] | 0.965 [0.775, 1.178] / [0.885, 1.047] |
| 6 | 1.037 [0.859, 1.229] / [0.959, 1.117] | 0.546 [0.453, 0.641] / [0.515, 0.579] | -0.055 [-0.071, -0.041]* / [-0.060, -0.050]* | 0.167 [0.102, 0.232] / [0.131, 0.203] | 0.423 [0.319, 0.534] / [0.368, 0.481] | 3.048 [2.604, 3.514] / [2.948, 3.163] | 0.928 [0.745, 1.129] / [0.853, 1.012] | 0.910 [0.730, 1.112] / [0.835, 0.993] |
| 7 |  | 0.479 [0.392, 0.571] / [0.448, 0.511] | -0.060 [-0.076, -0.045]* / [-0.065, -0.055]* | 0.382 [0.325, 0.448] / [0.345, 0.418] | 0.493 [0.384, 0.608] / [0.436, 0.554] | 3.101 [2.636, 3.570] / [3.001, 3.213] | 1.010 [0.791, 1.254] / [0.928, 1.096] | 0.992 [0.775, 1.235] / [0.910, 1.078] |
| 10 |  | 0.458 [0.378, 0.550] / [0.426, 0.489] | -0.063 [-0.080, -0.048]* / [-0.068, -0.058]* | 0.287 [0.225, 0.356] / [0.251, 0.322] | 0.558 [0.435, 0.692] / [0.499, 0.622] | 3.157 [2.665, 3.671] / [3.051, 3.271] | 1.080 [0.825, 1.359] / [0.988, 1.168] | 1.061 [0.808, 1.338] / [0.969, 1.149] |
| 14 |  | 0.423 [0.343, 0.513] / [0.392, 0.455] | -0.066 [-0.084, -0.050]* / [-0.071, -0.061]* | 0.387 [0.326, 0.451] / [0.350, 0.424] | 0.583 [0.441, 0.735] / [0.521, 0.644] | 3.099 [2.609, 3.611] / [2.991, 3.211] | 1.084 [0.806, 1.412] / [0.994, 1.176] | 1.065 [0.787, 1.391] / [0.975, 1.158] |
| 21 |  | 0.387 [0.298, 0.482] / [0.355, 0.419] | -0.072 [-0.091, -0.054]* / [-0.077, -0.066]* | 0.388 [0.330, 0.445] / [0.354, 0.422] | 0.546 [0.393, 0.708] / [0.480, 0.611] | 3.061 [2.556, 3.589] / [2.955, 3.183] | 0.998 [0.675, 1.359] / [0.901, 1.095] | 0.979 [0.657, 1.338] / [0.882, 1.076] |
| 30 |  | 0.328 [0.227, 0.431] / [0.296, 0.362] | -0.079 [-0.100, -0.060]* / [-0.084, -0.073]* | 0.351 [0.290, 0.413] / [0.314, 0.384] | 0.515 [0.350, 0.678] / [0.441, 0.588] | 2.958 [2.484, 3.462] / [2.853, 3.075] | 0.978 [0.662, 1.333] / [0.865, 1.087] | 0.957 [0.642, 1.312] / [0.844, 1.065] |
| 45 |  | 0.212 [0.071, 0.354] / [0.177, 0.248] | -0.098 [-0.119, -0.076]* / [-0.103, -0.092]* | 0.394 [0.310, 0.482] / [0.355, 0.432] | 0.482 [0.321, 0.648] / [0.396, 0.574] | 1.970 [1.448, 2.473] / [1.798, 2.157] | 1.189 [0.852, 1.495] / [1.070, 1.322] | 1.166 [0.824, 1.476] / [1.048, 1.300] |
| 60 |  | 0.077 [-0.119, 0.264] / [0.037, 0.115] | -0.103 [-0.127, -0.080]* / [-0.109, -0.097]* | 0.468 [0.380, 0.562] / [0.427, 0.506] | 0.049 [-1.186, 1.571] / [-0.396, 0.513] |  | -0.108 [-1.275, 0.793] / [-0.673, 0.470] | -0.107 [-1.281, 0.807] / [-0.672, 0.470] |
| 90 |  | 0.195 [-0.018, 0.408] / [0.153, 0.236] | -0.096 [-0.120, -0.074]* / [-0.102, -0.090]* | 0.540 [0.436, 0.665] / [0.491, 0.587] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.638 [0.437, 0.839] / [0.550, 0.729] | -0.604 [-0.707, -0.515]* / [-0.634, -0.575]* | -0.644 [-0.750, -0.550]* / [-0.675, -0.614]* | -0.269 [-0.387, -0.167]* / [-0.315, -0.222]* | -0.337 [-0.500, -0.172]* / [-0.402, -0.269]* | 2.247 [1.793, 2.745] / [2.136, 2.367] |  |  |
| 2 | 0.613 [0.407, 0.815] / [0.527, 0.707] | -0.585 [-0.686, -0.497]* / [-0.615, -0.557]* | -0.628 [-0.730, -0.536]* / [-0.658, -0.598]* | -0.289 [-0.410, -0.184]* / [-0.336, -0.242]* | -0.317 [-0.482, -0.157]* / [-0.383, -0.249]* | 2.229 [1.789, 2.720] / [2.117, 2.362] |  |  |
| 3 | 0.571 [0.363, 0.776] / [0.485, 0.664] | -0.581 [-0.682, -0.493]* / [-0.612, -0.550]* | -0.624 [-0.729, -0.530]* / [-0.655, -0.593]* | -0.315 [-0.436, -0.206]* / [-0.362, -0.263]* | -0.279 [-0.451, -0.120]* / [-0.343, -0.207]* | 2.206 [1.768, 2.691] / [2.093, 2.334] | 0.242 [0.016, 0.474] / [0.163, 0.327] | 0.222 [-0.001, 0.454] / [0.143, 0.308] |
| 4 | 0.524 [0.311, 0.739] / [0.436, 0.616] | -0.577 [-0.677, -0.488]* / [-0.609, -0.547]* | -0.620 [-0.723, -0.528]* / [-0.652, -0.590]* | -0.342 [-0.470, -0.225]* / [-0.390, -0.291]* | -0.313 [-0.488, -0.147]* / [-0.383, -0.239]* | 2.194 [1.736, 2.687] / [2.081, 2.316] | 0.229 [-0.008, 0.465] / [0.142, 0.313] | 0.210 [-0.028, 0.446] / [0.123, 0.294] |
| 5 | 0.490 [0.273, 0.703] / [0.402, 0.581] | -0.571 [-0.667, -0.478]* / [-0.603, -0.541]* | -0.615 [-0.714, -0.522]* / [-0.647, -0.585]* | -0.366 [-0.496, -0.245]* / [-0.415, -0.316]* | -0.262 [-0.438, -0.102]* / [-0.333, -0.191]* | 2.167 [1.708, 2.652] / [2.056, 2.288] | 0.299 [0.060, 0.530] / [0.212, 0.381] | 0.280 [0.041, 0.511] / [0.192, 0.361] |
| 6 | 0.457 [0.236, 0.670] / [0.364, 0.550] | -0.546 [-0.641, -0.453]* / [-0.579, -0.515]* | -0.592 [-0.692, -0.500]* / [-0.625, -0.562]* | -0.383 [-0.515, -0.259]* / [-0.436, -0.333]* | -0.267 [-0.443, -0.091]* / [-0.334, -0.198]* | 2.395 [1.937, 2.875] / [2.286, 2.515] | 0.250 [0.021, 0.480] / [0.167, 0.336] | 0.232 [0.002, 0.461] / [0.149, 0.318] |
| 7 |  | -0.479 [-0.571, -0.392]* / [-0.511, -0.448]* | -0.531 [-0.625, -0.441]* / [-0.562, -0.500]* | -0.091 [-0.205, 0.022] / [-0.138, -0.040]*  ! | -0.139 [-0.324, 0.037] / [-0.206, -0.068]*  ! | 2.508 [2.049, 3.000] / [2.397, 2.631] | 0.395 [0.129, 0.666] / [0.305, 0.490] | 0.377 [0.112, 0.649] / [0.287, 0.471] |
| 10 |  | -0.458 [-0.550, -0.378]* / [-0.489, -0.426]* | -0.513 [-0.609, -0.427]* / [-0.544, -0.481]* | -0.172 [-0.289, -0.050]* / [-0.223, -0.118]* | -0.056 [-0.260, 0.141] / [-0.127, 0.025] | 2.548 [2.045, 3.073] / [2.437, 2.668] | 0.476 [0.176, 0.783] / [0.378, 0.573] | 0.457 [0.159, 0.764] / [0.359, 0.554] |
| 14 |  | -0.423 [-0.513, -0.343]* / [-0.455, -0.392]* | -0.481 [-0.574, -0.392]* / [-0.512, -0.449]* | -0.041 [-0.148, 0.068] / [-0.090, 0.008] | 0.011 [-0.213, 0.223] / [-0.058, 0.084] | 2.535 [2.039, 3.062] / [2.417, 2.652] | 0.531 [0.197, 0.879] / [0.438, 0.629] | 0.511 [0.182, 0.859] / [0.419, 0.609] |
| 21 |  | -0.387 [-0.482, -0.298]* / [-0.419, -0.355]* | -0.451 [-0.549, -0.355]* / [-0.483, -0.419]* | 0.013 [-0.092, 0.118] / [-0.037, 0.063] | 0.049 [-0.167, 0.260] / [-0.026, 0.124] | 2.565 [2.048, 3.104] / [2.446, 2.692] | 0.521 [0.159, 0.908] / [0.415, 0.626] | 0.501 [0.137, 0.884] / [0.395, 0.606] |
| 30 |  | -0.328 [-0.431, -0.227]* / [-0.362, -0.296]* | -0.401 [-0.514, -0.292]* / [-0.434, -0.368]* | 0.039 [-0.071, 0.152] / [-0.015, 0.089] | 0.154 [-0.034, 0.337] / [0.066, 0.237] | 2.587 [2.063, 3.112] / [2.469, 2.719] | 0.620 [0.303, 0.972] / [0.502, 0.739] | 0.598 [0.278, 0.949] / [0.482, 0.717] |
| 45 |  | -0.212 [-0.354, -0.071]* / [-0.248, -0.177]* | -0.302 [-0.453, -0.152]* / [-0.337, -0.266]* | 0.193 [0.012, 0.358] / [0.134, 0.248] | 0.330 [0.102, 0.556] / [0.227, 0.440] | 1.372 [0.739, 2.052] / [1.162, 1.594] | 1.010 [0.689, 1.334] / [0.886, 1.144] | 0.987 [0.664, 1.307] / [0.861, 1.121] |
| 60 |  | -0.077 [-0.264, 0.119] / [-0.115, -0.037]*  ! | -0.169 [-0.360, 0.030] / [-0.208, -0.129]*  ! | 0.396 [0.171, 0.639] / [0.336, 0.453] | 0.458 [-0.348, 2.119] / [-0.112, 1.060] |  | 0.322 [-0.914, 1.630] / [-0.339, 1.010] | 0.323 [-0.907, 1.625] / [-0.335, 1.008] |
| 90 |  | -0.195 [-0.408, 0.018] / [-0.236, -0.153]*  ! | -0.282 [-0.504, -0.064]* / [-0.324, -0.238]* | 0.344 [0.065, 0.652] / [0.276, 0.413] |  |  |  |  |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 15.46 [15.26, 15.62] | 14.37 [14.20, 14.51] | 18.86 [18.50, 19.22] | 14.56 [14.39, 14.72] | 13.71 [13.52, 13.88] | 16.21 [16.03, 16.37] | 15.86 [15.68, 16.05] | 14.81 [14.65, 14.94] | 14.55 [14.39, 14.72] | 9.87 [9.79, 9.95] | 11.45 [11.27, 11.63] |  |  | 16.85 [16.64, 17.06] | 14.49 [14.32, 14.64] |
| 2 | 15.45 [15.26, 15.62] | 14.40 [14.23, 14.55] | 18.93 [18.58, 19.32] | 14.53 [14.36, 14.69] | 13.82 [13.63, 14.00] | 16.42 [16.25, 16.58] | 16.02 [15.84, 16.21] | 15.06 [14.91, 15.20] | 14.70 [14.54, 14.86] | 9.87 [9.78, 9.95] | 11.41 [11.23, 11.58] |  |  | 16.83 [16.62, 17.03] | 14.54 [14.37, 14.69] |
| 3 | 15.49 [15.29, 15.66] | 14.45 [14.28, 14.59] | 19.01 [18.67, 19.39] | 14.53 [14.37, 14.70] | 14.04 [13.84, 14.24] | 16.55 [16.37, 16.72] | 16.14 [15.95, 16.32] | 15.20 [15.05, 15.35] | 14.84 [14.67, 15.00] | 9.86 [9.77, 9.94] | 11.39 [11.21, 11.56] | 14.99 [14.78, 15.19] | 14.93 [14.73, 15.13] | 16.82 [16.60, 17.01] | 14.59 [14.43, 14.74] |
| 4 | 15.50 [15.31, 15.67] | 14.48 [14.31, 14.63] | 19.15 [18.81, 19.53] | 14.53 [14.36, 14.69] | 14.08 [13.87, 14.26] | 16.49 [16.32, 16.65] | 16.00 [15.83, 16.19] | 15.24 [15.09, 15.39] | 14.85 [14.69, 15.01] | 9.86 [9.78, 9.95] | 11.36 [11.19, 11.54] | 15.09 [14.89, 15.29] | 15.03 [14.83, 15.23] | 16.77 [16.56, 16.97] | 14.62 [14.46, 14.78] |
| 5 | 15.52 [15.34, 15.69] | 14.51 [14.35, 14.67] | 19.14 [18.79, 19.49] | 14.52 [14.36, 14.68] | 14.30 [14.09, 14.50] | 16.57 [16.40, 16.73] | 16.09 [15.91, 16.26] | 15.23 [15.07, 15.37] | 14.88 [14.72, 15.05] | 9.87 [9.78, 9.95] | 11.35 [11.17, 11.52] | 15.32 [15.12, 15.54] | 15.27 [15.07, 15.48] | 16.74 [16.52, 16.93] | 14.66 [14.50, 14.82] |
| 6 | 15.56 [15.38, 15.73] | 14.59 [14.43, 14.74] | 19.59 [19.23, 19.97] | 14.55 [14.39, 14.71] | 14.33 [14.13, 14.54] | 16.61 [16.44, 16.78] | 16.12 [15.93, 16.30] | 15.34 [15.19, 15.49] | 14.95 [14.78, 15.11] | 9.89 [9.81, 9.98] | 11.34 [11.16, 11.51] | 15.31 [15.10, 15.52] | 15.26 [15.04, 15.47] | 16.70 [16.49, 16.89] | 14.75 [14.58, 14.91] |
| 7 | 15.59 [15.41, 15.77] | 14.76 [14.60, 14.92] | 19.87 [19.51, 20.23] | 15.07 [14.90, 15.23] | 14.62 [14.41, 14.83] | 16.73 [16.55, 16.89] | 16.23 [16.03, 16.40] | 15.68 [15.53, 15.84] | 15.10 [14.94, 15.26] | 9.94 [9.85, 10.03] | 12.05 [11.87, 12.23] | 15.66 [15.46, 15.88] | 15.61 [15.41, 15.83] |  | 14.93 [14.76, 15.09] |
| 10 | 15.73 [15.54, 15.90] | 14.93 [14.76, 15.10] | 20.31 [19.94, 20.70] | 15.10 [14.94, 15.26] | 15.02 [14.80, 15.23] |  |  |  |  | 9.99 [9.89, 10.07] | 12.02 [11.84, 12.19] | 16.09 [15.87, 16.33] | 16.04 [15.81, 16.28] |  | 15.11 [14.94, 15.28] |
| 14 | 15.92 [15.73, 16.09] | 15.19 [15.02, 15.36] | 20.48 [20.11, 20.88] | 15.48 [15.31, 15.66] | 15.40 [15.19, 15.61] |  |  |  |  | 10.08 [9.99, 10.17] | 12.45 [12.26, 12.63] | 16.49 [16.26, 16.73] | 16.43 [16.21, 16.67] |  | 15.37 [15.20, 15.53] |
| 21 | 16.21 [16.02, 16.39] | 15.49 [15.31, 15.65] | 21.03 [20.66, 21.47] | 15.83 [15.65, 16.01] | 15.88 [15.66, 16.09] |  |  |  |  | 10.14 [10.05, 10.23] | 12.81 [12.62, 13.01] | 16.97 [16.75, 17.21] | 16.91 [16.69, 17.16] |  | 15.68 [15.51, 15.85] |
| 30 | 16.39 [16.19, 16.57] | 15.74 [15.56, 15.90] | 21.42 [21.02, 21.90] | 16.01 [15.84, 16.18] | 16.60 [16.32, 16.88] |  |  |  |  | 10.04 [9.95, 10.14] | 12.99 [12.80, 13.18] | 17.73 [17.45, 18.01] | 17.67 [17.39, 17.95] |  | 15.96 [15.78, 16.12] |
| 45 | 16.57 [16.38, 16.75] | 16.08 [15.91, 16.25] | 18.20 [17.75, 18.71] | 16.42 [16.23, 16.59] | 17.36 [17.00, 17.70] |  |  |  |  | 9.95 [9.86, 10.03] | 13.48 [13.27, 13.67] | 19.02 [18.66, 19.35] | 18.96 [18.60, 19.29] |  | 16.34 [16.16, 16.51] |
| 60 | 16.67 [16.49, 16.86] | 16.52 [16.36, 16.71] |  | 16.91 [16.72, 17.11] | 19.87 [13.89, 24.41] |  |  |  |  | 9.98 [9.89, 10.07] | 14.08 [13.86, 14.28] | 20.85 [16.11, 24.08] | 20.82 [16.03, 24.08] |  | 16.78 [16.61, 16.97] |
| 90 | 16.98 [16.76, 17.19] | 16.72 [16.52, 16.91] |  | 17.24 [17.02, 17.44] |  |  |  |  |  | 9.99 [9.90, 10.09] | 14.38 [14.13, 14.63] |  |  |  | 16.96 [16.75, 17.16] |

**Paired difference vs the DISPLAY reference, busy** (park-cluster CI). `*` means the model beats the whole naive envelope, not just this one column — see the per-candidate tables below for the individual verdicts.

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.17 [0.97, 1.41] | -0.13 [-0.17, -0.10]* | 5.20 [4.01, 6.50] | 0.23 [0.11, 0.35] | 0.02 [-0.24, 0.29] | 1.78 [1.48, 2.09] | 1.47 [1.16, 1.77] | 0.40 [0.13, 0.65] | 0.16 [-0.05, 0.36] | -4.64 [-5.20, -4.15]* | -2.87 [-3.11, -2.61]* |  |  | 2.38 [2.05, 2.73] | wt_med |
| 2 | 1.13 [0.92, 1.37] | -0.14 [-0.17, -0.10]* | 5.20 [4.04, 6.48] | 0.16 [0.03, 0.28] | 0.09 [-0.16, 0.34] | 1.96 [1.65, 2.25] | 1.59 [1.30, 1.88] | 0.61 [0.33, 0.86] | 0.26 [0.06, 0.46] | -4.69 [-5.25, -4.20]* | -2.97 [-3.22, -2.70]* |  |  | 2.31 [1.97, 2.66] | wt_med |
| 3 | 1.11 [0.91, 1.36] | -0.14 [-0.18, -0.11]* | 5.18 [4.00, 6.45] | 0.09 [-0.04, 0.23] | 0.23 [0.00, 0.49] | 2.03 [1.70, 2.34] | 1.66 [1.35, 1.99] | 0.70 [0.43, 0.92] | 0.36 [0.15, 0.55] | -4.75 [-5.32, -4.26]* | -3.05 [-3.30, -2.78]* | 1.30 [0.95, 1.68] | 1.25 [0.89, 1.62] | 2.22 [1.86, 2.60] | wt_med |
| 4 | 1.10 [0.88, 1.35] | -0.14 [-0.18, -0.11]* | 5.25 [4.04, 6.61] | 0.05 [-0.09, 0.18] | 0.21 [-0.02, 0.44] | 1.95 [1.62, 2.26] | 1.50 [1.21, 1.80] | 0.73 [0.44, 0.98] | 0.34 [0.14, 0.54] | -4.78 [-5.35, -4.28]* | -3.11 [-3.37, -2.84]* | 1.35 [0.97, 1.74] | 1.30 [0.91, 1.68] | 2.15 [1.78, 2.54] | wt_med |
| 5 | 1.08 [0.86, 1.34] | -0.15 [-0.18, -0.11]* | 5.21 [3.99, 6.54] | 0.00 [-0.15, 0.14] | 0.40 [0.17, 0.61] | 2.00 [1.67, 2.33] | 1.55 [1.24, 1.84] | 0.68 [0.40, 0.91] | 0.34 [0.12, 0.56] | -4.81 [-5.39, -4.31]* | -3.17 [-3.43, -2.90]* | 1.59 [1.21, 1.96] | 1.53 [1.16, 1.91] | 2.07 [1.70, 2.47] | wt_med |
| 6 | 1.04 [0.81, 1.29] | -0.15 [-0.19, -0.12]* | 5.58 [4.35, 6.89] | -0.07 [-0.23, 0.08] | 0.38 [0.17, 0.60] | 1.96 [1.62, 2.29] | 1.50 [1.19, 1.81] | 0.71 [0.45, 0.92] | 0.33 [0.10, 0.52] | -4.87 [-5.45, -4.37]* | -3.27 [-3.53, -2.99]* | 1.51 [1.17, 1.85] | 1.46 [1.12, 1.80] | 1.94 [1.55, 2.35] | wt_med |
| 7 | 0.91 [0.70, 1.14] | -0.16 [-0.20, -0.13]* | 5.71 [4.45, 6.97] | 0.40 [0.26, 0.54] | 0.50 [0.25, 0.72] | 1.91 [1.56, 2.25] | 1.44 [1.12, 1.74] | 0.88 [0.60, 1.09] | 0.31 [0.07, 0.51] | -5.01 [-5.60, -4.50]* | -2.60 [-2.87, -2.32]* | 1.70 [1.27, 2.12] | 1.65 [1.22, 2.07] |  | wt_med |
| 10 | 0.89 [0.69, 1.12] | -0.17 [-0.21, -0.13]* | 5.92 [4.59, 7.27] | 0.22 [0.06, 0.38] | 0.70 [0.44, 0.95] |  |  |  |  | -5.15 [-5.77, -4.62]* | -2.85 [-3.13, -2.56]* | 1.97 [1.53, 2.44] | 1.92 [1.47, 2.38] |  | wt_med |
| 14 | 0.83 [0.63, 1.06] | -0.17 [-0.22, -0.13]* | 5.84 [4.51, 7.13] | 0.43 [0.29, 0.58] | 0.77 [0.49, 1.05] |  |  |  |  | -5.32 [-5.97, -4.79]* | -2.59 [-2.87, -2.28]* | 2.04 [1.53, 2.59] | 1.98 [1.48, 2.53] |  | wt_med |
| 21 | 0.78 [0.56, 1.02] | -0.19 [-0.23, -0.14]* | 5.78 [4.38, 7.18] | 0.44 [0.31, 0.56] | 0.66 [0.33, 0.95] |  |  |  |  | -5.58 [-6.25, -5.04]* | -2.58 [-2.83, -2.29]* | 1.87 [1.27, 2.53] | 1.81 [1.21, 2.48] |  | wt_med |
| 30 | 0.66 [0.40, 0.95] | -0.20 [-0.26, -0.16]* | 5.49 [4.19, 6.89] | 0.34 [0.19, 0.48] | 0.69 [0.37, 0.98] |  |  |  |  | -5.94 [-6.59, -5.39]* | -2.67 [-2.93, -2.37]* | 1.88 [1.23, 2.57] | 1.82 [1.17, 2.51] |  | wt_med |
| 45 | 0.45 [0.09, 0.82] | -0.24 [-0.29, -0.19]* | 2.68 [1.73, 3.61] | 0.40 [0.20, 0.60] | 0.47 [0.11, 0.84] |  |  |  |  | -6.42 [-7.10, -5.79]* | -2.54 [-2.85, -2.16]* | 2.30 [1.62, 2.93] | 2.24 [1.56, 2.88] |  | wt_med |
| 60 | 0.21 [-0.27, 0.67] | -0.25 [-0.31, -0.20] |  | 0.54 [0.34, 0.75] | 2.11 [-1.29, 5.82] |  |  |  |  | -6.83 [-7.62, -6.10]* | -2.29 [-2.65, -1.87]* | 3.03 [2.09, 7.13] | 3.00 [2.02, 6.98] |  | wt_med |
| 90 | 0.34 [-0.17, 0.90] | -0.22 [-0.28, -0.17]* |  | 0.72 [0.53, 0.95] |  |  |  |  |  | -6.99 [-7.95, -6.07]* | -2.11 [-2.48, -1.71]* |  |  |  | wt_med |

**Paired difference vs EACH naive candidate, busy** — park-cluster CI / park-day CI, `*` per unit, `!` = the two units disagree. "Nothing beats the reference" is a statement about the best-of-N envelope; these tables are the statement about each individual naive.

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | -2.380 [-2.733, -2.053]* / [-2.548, -2.206]* | -1.173 [-1.600, -0.712]* / [-1.374, -0.963]* | -2.475 [-2.830, -2.144]* / [-2.641, -2.301]* | -2.149 [-2.430, -1.867]* / [-2.304, -1.994]* | -2.354 [-2.691, -1.990]* / [-2.554, -2.148]* | 3.071 [1.762, 4.448] / [2.685, 3.464] |  |  |
| 2 | -2.305 [-2.662, -1.968]* / [-2.470, -2.127]* | -1.142 [-1.582, -0.662]* / [-1.347, -0.930]* | -2.407 [-2.780, -2.062]* / [-2.572, -2.227]* | -2.154 [-2.442, -1.872]* / [-2.309, -1.996]* | -2.233 [-2.575, -1.865]* / [-2.449, -2.024]* | 3.133 [1.868, 4.468] / [2.750, 3.538] |  |  |
| 3 | -2.222 [-2.601, -1.864]* / [-2.388, -2.045]* | -1.077 [-1.530, -0.584]* / [-1.282, -0.860]* | -2.327 [-2.713, -1.965]* / [-2.496, -2.150]* | -2.137 [-2.437, -1.842]* / [-2.296, -1.974]* | -1.984 [-2.328, -1.600]* / [-2.196, -1.779]* | 3.198 [1.926, 4.561] / [2.815, 3.627] | -0.885 [-1.370, -0.362]* / [-1.113, -0.657]* | -0.943 [-1.429, -0.426]* / [-1.167, -0.717]* |
| 4 | -2.147 [-2.536, -1.781]* / [-2.307, -1.969]* | -1.001 [-1.464, -0.496]* / [-1.204, -0.780]* | -2.251 [-2.652, -1.878]* / [-2.417, -2.074]* | -2.106 [-2.413, -1.804]* / [-2.259, -1.944]* | -1.949 [-2.298, -1.582]* / [-2.146, -1.742]* | 3.337 [2.040, 4.740] / [2.958, 3.769] | -0.790 [-1.307, -0.230]* / [-0.999, -0.546]* | -0.848 [-1.361, -0.291]* / [-1.057, -0.602]* |
| 5 | -2.072 [-2.472, -1.703]* / [-2.241, -1.893]* | -0.942 [-1.418, -0.429]* / [-1.147, -0.719]* | -2.180 [-2.588, -1.805]* / [-2.345, -1.998]* | -2.083 [-2.387, -1.777]* / [-2.233, -1.918]* | -1.687 [-2.056, -1.305]* / [-1.883, -1.488]* | 3.319 [2.031, 4.748] / [2.939, 3.733] | -0.489 [-1.024, 0.059] / [-0.700, -0.259]*  ! | -0.545 [-1.075, 0.000] / [-0.755, -0.315]*  ! |
| 6 | -1.944 [-2.350, -1.554]* / [-2.116, -1.762]* | -0.840 [-1.321, -0.320]* / [-1.050, -0.616]* | -2.058 [-2.468, -1.674]* / [-2.231, -1.879]* | -2.022 [-2.327, -1.723]* / [-2.173, -1.858]* | -1.652 [-2.020, -1.234]* / [-1.843, -1.444]* | 3.845 [2.494, 5.273] / [3.463, 4.268] | -0.513 [-0.985, 0.012] / [-0.736, -0.276]*  ! | -0.566 [-1.041, -0.043]* / [-0.787, -0.328]* |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 2.380 [2.053, 2.733] / [2.206, 2.548] | 1.171 [0.972, 1.412] / [1.103, 1.244] | -0.129 [-0.165, -0.097]* / [-0.143, -0.117]* | 0.233 [0.109, 0.353] / [0.150, 0.322] | 0.019 [-0.244, 0.292] / [-0.116, 0.159] | 5.195 [4.011, 6.500] / [4.880, 5.504] |  |  |
| 2 | 2.305 [1.968, 2.662] / [2.127, 2.470] | 1.126 [0.924, 1.372] / [1.057, 1.202] | -0.137 [-0.175, -0.104]* / [-0.151, -0.124]* | 0.157 [0.030, 0.281] / [0.074, 0.244] | 0.089 [-0.160, 0.339] / [-0.044, 0.223] | 5.201 [4.042, 6.475] / [4.894, 5.527] |  |  |
| 3 | 2.222 [1.864, 2.601] / [2.045, 2.388] | 1.111 [0.907, 1.358] / [1.040, 1.186] | -0.142 [-0.178, -0.109]* / [-0.155, -0.130]* | 0.094 [-0.042, 0.226] / [0.009, 0.179] | 0.234 [0.003, 0.488] / [0.099, 0.378] | 5.176 [3.995, 6.453] / [4.874, 5.504] | 1.304 [0.952, 1.681] / [1.139, 1.483] | 1.248 [0.894, 1.620] / [1.082, 1.423] |
| 4 | 2.147 [1.781, 2.536] / [1.969, 2.307] | 1.102 [0.880, 1.350] / [1.029, 1.178] | -0.143 [-0.180, -0.109]* / [-0.157, -0.130]* | 0.048 [-0.095, 0.185] / [-0.036, 0.137] | 0.211 [-0.016, 0.443] / [0.069, 0.356] | 5.253 [4.040, 6.609] / [4.946, 5.590] | 1.352 [0.971, 1.735] / [1.182, 1.529] | 1.296 [0.911, 1.681] / [1.127, 1.472] |
| 5 | 2.072 [1.703, 2.472] / [1.893, 2.241] | 1.082 [0.861, 1.335] / [1.009, 1.160] | -0.146 [-0.182, -0.114]* / [-0.160, -0.133]* | 0.000 [-0.145, 0.139] / [-0.086, 0.088] | 0.402 [0.169, 0.614] / [0.262, 0.539] | 5.208 [3.988, 6.539] / [4.908, 5.537] | 1.589 [1.212, 1.957] / [1.418, 1.759] | 1.535 [1.159, 1.905] / [1.365, 1.704] |
| 6 | 1.944 [1.554, 2.350] / [1.762, 2.116] | 1.039 [0.813, 1.290] / [0.968, 1.119] | -0.152 [-0.190, -0.118]* / [-0.165, -0.140]* | -0.068 [-0.229, 0.084] / [-0.154, 0.021] | 0.380 [0.168, 0.604] / [0.244, 0.521] | 5.577 [4.351, 6.894] / [5.265, 5.912] | 1.506 [1.173, 1.846] / [1.331, 1.689] | 1.455 [1.121, 1.798] / [1.281, 1.639] |
| 7 |  | 0.911 [0.704, 1.139] / [0.838, 0.988] | -0.162 [-0.201, -0.126]* / [-0.175, -0.150]* | 0.401 [0.264, 0.543] / [0.317, 0.489] | 0.496 [0.255, 0.722] / [0.368, 0.644] | 5.710 [4.449, 6.972] / [5.409, 6.012] | 1.699 [1.273, 2.120] / [1.513, 1.899] | 1.646 [1.219, 2.066] / [1.463, 1.847] |
| 10 |  | 0.889 [0.691, 1.123] / [0.810, 0.967] | -0.169 [-0.210, -0.132]* / [-0.182, -0.157]* | 0.217 [0.060, 0.384] / [0.131, 0.307] | 0.696 [0.444, 0.947] / [0.548, 0.854] | 5.923 [4.586, 7.275] / [5.598, 6.243] | 1.973 [1.528, 2.435] / [1.773, 2.179] | 1.919 [1.472, 2.383] / [1.720, 2.125] |
| 14 |  | 0.829 [0.629, 1.058] / [0.751, 0.908] | -0.174 [-0.217, -0.133]* / [-0.185, -0.161]* | 0.426 [0.285, 0.579] / [0.343, 0.505] | 0.769 [0.489, 1.047] / [0.622, 0.925] | 5.838 [4.509, 7.133] / [5.520, 6.157] | 2.036 [1.528, 2.591] / [1.825, 2.232] | 1.980 [1.476, 2.533] / [1.769, 2.177] |
| 21 |  | 0.775 [0.556, 1.015] / [0.698, 0.861] | -0.185 [-0.235, -0.142]* / [-0.198, -0.172]* | 0.437 [0.312, 0.557] / [0.356, 0.521] | 0.656 [0.327, 0.955] / [0.501, 0.820] | 5.783 [4.383, 7.180] / [5.477, 6.151] | 1.870 [1.266, 2.531] / [1.647, 2.082] | 1.814 [1.206, 2.477] / [1.589, 2.026] |
| 30 |  | 0.663 [0.399, 0.946] / [0.578, 0.746] | -0.204 [-0.258, -0.157]* / [-0.216, -0.190]* | 0.340 [0.193, 0.483] / [0.249, 0.422] | 0.687 [0.372, 0.977] / [0.505, 0.857] | 5.486 [4.189, 6.889] / [5.168, 5.837] | 1.884 [1.232, 2.569] / [1.617, 2.134] | 1.823 [1.168, 2.512] / [1.557, 2.073] |
| 45 |  | 0.454 [0.095, 0.818] / [0.368, 0.543] | -0.240 [-0.295, -0.193]* / [-0.253, -0.227]* | 0.397 [0.200, 0.596] / [0.306, 0.492] | 0.465 [0.106, 0.840] / [0.246, 0.678] | 2.681 [1.731, 3.611] / [2.314, 3.086] | 2.304 [1.624, 2.928] / [2.035, 2.584] | 2.243 [1.558, 2.879] / [1.974, 2.522] |
| 60 |  | 0.209 [-0.270, 0.671] / [0.114, 0.303] | -0.247 [-0.308, -0.195]* / [-0.261, -0.234]* | 0.540 [0.338, 0.752] / [0.442, 0.631] | 2.113 [-1.292, 5.819] / [-1.385, 4.598] |  | 3.031 [2.094, 7.131] / [0.480, 5.645] | 3.003 [2.022, 6.977] / [0.416, 5.629] |
| 90 |  | 0.337 [-0.174, 0.897] / [0.241, 0.439] | -0.220 [-0.279, -0.170]* / [-0.235, -0.205]* | 0.723 [0.530, 0.948] / [0.608, 0.836] |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 1 | 1.173 [0.712, 1.600] / [0.963, 1.374] | -1.171 [-1.412, -0.972]* / [-1.244, -1.103]* | -1.284 [-1.532, -1.078]* / [-1.356, -1.215]* | -0.900 [-1.191, -0.664]* / [-1.007, -0.787]* | -1.251 [-1.666, -0.829]* / [-1.420, -1.086]* | 4.044 [2.728, 5.398] / [3.691, 4.395] |  |  |
| 2 | 1.142 [0.662, 1.582] / [0.930, 1.347] | -1.126 [-1.372, -0.924]* / [-1.202, -1.057]* | -1.246 [-1.499, -1.042]* / [-1.319, -1.175]* | -0.938 [-1.241, -0.689]* / [-1.052, -0.822]* | -1.216 [-1.632, -0.830]* / [-1.379, -1.056]* | 3.995 [2.652, 5.307] / [3.658, 4.361] |  |  |
| 3 | 1.077 [0.584, 1.530] / [0.860, 1.282] | -1.111 [-1.358, -0.907]* / [-1.186, -1.040]* | -1.233 [-1.487, -1.022]* / [-1.311, -1.162]* | -0.979 [-1.300, -0.713]* / [-1.097, -0.855]* | -1.092 [-1.500, -0.700]* / [-1.263, -0.913]* | 3.940 [2.611, 5.265] / [3.611, 4.308] | 0.017 [-0.504, 0.518] / [-0.172, 0.216] | -0.040 [-0.560, 0.462] / [-0.228, 0.160] |
| 4 | 1.001 [0.496, 1.464] / [0.780, 1.204] | -1.102 [-1.350, -0.880]* / [-1.178, -1.029]* | -1.223 [-1.475, -1.002]* / [-1.300, -1.149]* | -1.020 [-1.353, -0.733]* / [-1.145, -0.896]* | -1.177 [-1.591, -0.788]* / [-1.356, -1.008]* | 3.971 [2.616, 5.343] / [3.630, 4.324] | 0.014 [-0.526, 0.522] / [-0.182, 0.208] | -0.043 [-0.590, 0.466] / [-0.242, 0.148] |
| 5 | 0.942 [0.429, 1.418] / [0.719, 1.147] | -1.082 [-1.335, -0.861]* / [-1.160, -1.009]* | -1.206 [-1.460, -0.970]* / [-1.285, -1.133]* | -1.057 [-1.399, -0.758]* / [-1.181, -0.932]* | -1.018 [-1.420, -0.669]* / [-1.189, -0.851]* | 3.908 [2.573, 5.279] / [3.570, 4.254] | 0.209 [-0.303, 0.669] / [0.016, 0.392] | 0.153 [-0.365, 0.616] / [-0.041, 0.337] |
| 6 | 0.840 [0.320, 1.321] / [0.616, 1.050] | -1.039 [-1.290, -0.813]* / [-1.119, -0.968]* | -1.168 [-1.425, -0.930]* / [-1.251, -1.096]* | -1.097 [-1.454, -0.777]* / [-1.223, -0.966]* | -1.052 [-1.452, -0.685]* / [-1.215, -0.884]* | 4.263 [2.943, 5.641] / [3.930, 4.604] | 0.121 [-0.378, 0.582] / [-0.080, 0.314] | 0.069 [-0.423, 0.532] / [-0.133, 0.263] |
| 7 |  | -0.911 [-1.139, -0.704]* / [-0.988, -0.838]* | -1.050 [-1.289, -0.833]* / [-1.125, -0.978]* | -0.476 [-0.771, -0.194]* / [-0.598, -0.363]* | -0.823 [-1.235, -0.452]* / [-0.995, -0.654]* | 4.518 [3.185, 5.877] / [4.193, 4.858] | 0.434 [-0.133, 0.986] / [0.226, 0.649] | 0.381 [-0.185, 0.935] / [0.174, 0.598] |
| 10 |  | -0.889 [-1.123, -0.691]* / [-0.967, -0.810]* | -1.033 [-1.279, -0.821]* / [-1.113, -0.958]* | -0.640 [-0.930, -0.339]* / [-0.764, -0.517]* | -0.602 [-1.039, -0.200]* / [-0.775, -0.407]* | 4.651 [3.253, 6.060] / [4.292, 4.998] | 0.702 [0.084, 1.337] / [0.477, 0.921] | 0.647 [0.026, 1.282] / [0.422, 0.864] |
| 14 |  | -0.829 [-1.058, -0.629]* / [-0.908, -0.751]* | -0.979 [-1.225, -0.771]* / [-1.060, -0.899]* | -0.375 [-0.621, -0.113]* / [-0.492, -0.253]* | -0.473 [-0.950, -0.042]* / [-0.646, -0.295]* | 4.643 [3.247, 6.040] / [4.309, 4.997] | 0.844 [0.180, 1.536] / [0.629, 1.064] | 0.787 [0.128, 1.478] / [0.573, 1.008] |
| 21 |  | -0.775 [-1.015, -0.556]* / [-0.861, -0.698]* | -0.938 [-1.190, -0.703]* / [-1.021, -0.858]* | -0.289 [-0.519, -0.055]* / [-0.408, -0.169]* | -0.444 [-0.920, 0.004] / [-0.635, -0.263]*  ! | 4.697 [3.237, 6.184] / [4.353, 5.070] | 0.821 [0.054, 1.620] / [0.580, 1.057] | 0.764 [-0.003, 1.565] / [0.522, 1.001] |
| 30 |  | -0.663 [-0.946, -0.399]* / [-0.746, -0.578]* | -0.849 [-1.159, -0.570]* / [-0.934, -0.762]* | -0.278 [-0.550, 0.010] / [-0.410, -0.155]*  ! | -0.171 [-0.600, 0.230] / [-0.389, 0.038] | 4.613 [3.149, 6.136] / [4.274, 4.994] | 1.029 [0.355, 1.723] / [0.741, 1.313] | 0.968 [0.290, 1.675] / [0.677, 1.253] |
| 45 |  | -0.454 [-0.818, -0.095]* / [-0.543, -0.368]* | -0.675 [-1.057, -0.303]* / [-0.760, -0.589]* | -0.025 [-0.439, 0.401] / [-0.165, 0.104] | 0.078 [-0.498, 0.651] / [-0.186, 0.354] | 1.662 [0.350, 2.945] / [1.251, 2.118] | 1.827 [1.069, 2.600] / [1.524, 2.146] | 1.764 [0.999, 2.546] / [1.463, 2.078] |
| 60 |  | -0.209 [-0.671, 0.270] / [-0.303, -0.114]*  ! | -0.427 [-0.911, 0.065] / [-0.520, -0.326]*  ! | 0.340 [-0.191, 0.919] / [0.194, 0.477] | 6.858 [5.373, 10.553] / [4.023, 8.988] |  | 7.888 [4.623, 9.508] / [5.416, 10.395] | 7.855 [4.647, 9.355] / [5.382, 10.359] |
| 90 |  | -0.337 [-0.897, 0.174] / [-0.439, -0.241]*  ! | -0.529 [-1.101, -0.006]* / [-0.629, -0.431]* | 0.352 [-0.307, 0.969] / [0.198, 0.518] |  |  |  |  |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 8.82 | 8.15 | 12.75 | 8.43 | 7.79 | 9.58 | 9.31 | 8.77 | 8.61 | 5.50 | 7.18 |  |  | 9.37 | 8.18 |
| 1 | EU | 6.30 | 5.69 | 7.94 | 5.94 | 5.64 | 6.56 | 6.79 | 5.97 | 6.20 | 4.12 | 4.94 |  |  | 6.54 | 5.72 |
| 1 | NA | 9.21 | 8.75 | 10.63 | 9.09 | 8.56 | 10.01 | 9.95 | 9.15 | 9.24 | 6.72 | 7.34 |  |  | 10.62 | 8.83 |
| 3 | Asia | 8.86 | 8.27 | 13.04 | 8.48 | 8.13 | 9.90 | 9.58 | 9.03 | 8.87 | 5.53 | 7.19 | 8.29 | 8.26 | 9.42 | 8.31 |
| 3 | EU | 6.35 | 5.77 | 8.05 | 5.96 | 5.75 | 6.79 | 6.97 | 6.13 | 6.38 | 4.16 | 4.93 | 6.08 | 6.07 | 6.53 | 5.81 |
| 3 | NA | 9.27 | 8.78 | 10.64 | 9.09 | 8.70 | 10.17 | 10.09 | 9.35 | 9.36 | 6.71 | 7.33 | 9.42 | 9.40 | 10.61 | 8.87 |
| 7 | Asia | 8.89 | 8.49 | 13.35 | 8.83 | 8.63 | 10.03 | 9.66 | 9.34 | 9.07 | 5.63 | 7.63 | 8.67 | 8.64 |  | 8.53 |
| 7 | EU | 6.49 | 5.99 | 8.80 | 6.24 | 6.12 | 6.72 | 6.90 | 6.35 | 6.59 | 4.26 | 5.25 | 6.35 | 6.34 |  | 6.04 |
| 7 | NA | 9.39 | 8.96 | 10.99 | 9.36 | 8.95 | 10.26 | 10.10 | 9.62 | 9.48 | 6.74 | 7.72 | 9.81 | 9.79 |  | 9.07 |
| 14 | Asia | 9.14 | 8.89 | 14.34 | 9.16 | 9.47 |  |  |  |  | 5.84 | 7.93 | 9.45 | 9.40 |  | 8.93 |
| 14 | EU | 6.71 | 6.25 | 9.23 | 6.51 | 6.52 |  |  |  |  | 4.41 | 5.50 | 6.64 | 6.63 |  | 6.32 |
| 14 | NA | 9.61 | 9.20 | 11.14 | 9.60 | 9.32 |  |  |  |  | 6.81 | 8.01 | 10.23 | 10.22 |  | 9.32 |
| 30 | Asia | 9.33 | 9.20 | 15.26 | 9.65 | 10.26 |  |  |  |  | 5.91 | 8.52 | 10.37 | 10.33 |  | 9.26 |
| 30 | EU | 7.04 | 6.65 | 9.81 | 6.93 | 7.37 |  |  |  |  | 4.62 | 5.86 | 7.40 | 7.39 |  | 6.73 |
| 30 | NA | 10.01 | 9.61 | 11.53 | 9.90 | 9.90 |  |  |  |  | 6.77 | 8.37 | 10.92 | 10.90 |  | 9.77 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | nf_tide | nf_tide_x_h5 | nf_nhits | nf_nhits_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.04 | -1.51 | -1.16 | -1.75 | -1.25 | -1.37 | -5.27 |  |  | -0.98 |  | -1.24 |  | -0.49 | -0.22 |
| 1 | -0.04 | -1.52 | -1.15 | -1.78 | -1.24 | -1.43 | -5.41 |  |  | -1.14 | -4.22 | -1.33 | -5.00 | -0.50 | -0.22 |
| 2 | -0.05 | -1.50 | -1.10 | -1.76 | -1.24 | -1.47 | -5.46 |  |  | -1.06 | -4.23 | -1.52 | -5.17 | -0.50 | -0.21 |
| 3 | -0.07 | -1.51 | -1.12 | -1.77 | -1.24 | -1.49 | -5.53 | 1.34 | 1.33 | -1.14 | -4.29 | -1.51 | -5.17 | -0.51 | -0.22 |
| 4 | -0.08 | -1.52 | -1.14 | -1.77 | -1.24 | -1.45 | -5.62 | 1.40 | 1.39 | -1.15 | -4.27 | -1.38 | -5.05 | -0.51 | -0.22 |
| 5 | -0.07 | -1.51 | -1.15 | -1.77 | -1.24 | -1.47 | -5.63 | 1.40 | 1.39 | -1.22 | -4.29 | -1.30 | -4.99 | -0.50 | -0.22 |
| 6 | -0.05 | -1.48 | -1.14 | -1.74 | -1.21 | -1.59 | -5.95 | 1.24 | 1.24 | -1.23 | -4.27 | -1.50 | -5.04 | -0.49 | -0.21 |
| 7 |  | -1.45 | -1.12 | -1.71 | -1.23 | -1.54 | -5.92 | 1.26 | 1.25 | -0.99 | -4.20 | -1.47 | -5.10 | -0.48 | -0.20 |
| 10 |  | -1.51 | -1.12 | -1.77 | -1.27 | -1.59 | -6.21 | 1.27 | 1.26 |  |  |  |  | -0.48 | -0.24 |
| 14 |  | -1.54 | -1.22 | -1.81 | -1.33 | -1.59 | -6.38 | 1.24 | 1.23 |  |  |  |  | -0.46 | -0.30 |
| 21 |  | -1.56 | -1.28 | -1.83 | -1.41 | -1.74 | -6.71 | 1.08 | 1.07 |  |  |  |  | -0.46 | -0.38 |
| 30 |  | -1.31 | -0.95 | -1.58 | -1.21 | -2.32 | -6.92 | 0.47 | 0.47 |  |  |  |  | -0.49 | -0.15 |
| 45 |  | -0.71 | -0.30 | -1.00 | -0.70 | -1.42 | -5.50 | 0.89 | 0.88 |  |  |  |  | -0.42 | 0.32 |
| 60 |  | -0.41 | 0.09 | -0.72 | -0.43 | -1.65 |  | -0.79 | -0.79 |  |  |  |  | -0.38 | 0.59 |
| 90 |  | -0.00 | 0.58 | -0.31 | -0.10 |  |  |  |  |  |  |  |  | -0.38 | 0.91 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.38 [7.28, 7.48] | 7.33 [7.23, 7.43] | 7.74 [7.64, 7.84] | 7.09 [6.97, 7.21] |  |  |
| 1 | 7.51 [7.41, 7.61] | 7.45 [7.35, 7.55] | 7.78 [7.68, 7.88] | 7.25 [7.13, 7.37] |  |  |
| 2 | 7.55 [7.45, 7.65] | 7.48 [7.39, 7.59] | 7.78 [7.68, 7.88] | 7.31 [7.19, 7.44] |  |  |
| 3 | 7.59 [7.49, 7.69] | 7.52 [7.43, 7.63] | 7.79 [7.69, 7.89] | 7.41 [7.28, 7.53] | 7.75 [7.63, 7.88] | 7.73 [7.61, 7.86] |
| 4 | 7.61 [7.52, 7.72] | 7.54 [7.45, 7.65] | 7.79 [7.69, 7.89] | 7.44 [7.31, 7.56] | 7.80 [7.68, 7.93] | 7.79 [7.66, 7.91] |
| 5 | 7.61 [7.51, 7.71] | 7.54 [7.43, 7.64] | 7.76 [7.66, 7.87] | 7.54 [7.41, 7.67] | 7.91 [7.79, 8.04] | 7.90 [7.77, 8.02] |
| 6 | 7.63 [7.53, 7.73] | 7.55 [7.45, 7.65] | 7.76 [7.66, 7.86] | 7.57 [7.44, 7.70] | 7.90 [7.77, 8.03] | 7.88 [7.76, 8.02] |
| 7 | 7.71 [7.61, 7.81] | 7.63 [7.53, 7.73] | 7.99 [7.89, 8.10] | 7.69 [7.56, 7.82] | 8.01 [7.88, 8.15] | 7.99 [7.86, 8.13] |
| 10 | 7.80 [7.69, 7.91] | 7.71 [7.60, 7.81] | 7.98 [7.87, 8.08] | 7.90 [7.76, 8.04] | 8.17 [8.04, 8.30] | 8.15 [8.02, 8.28] |
| 14 | 7.89 [7.78, 7.99] | 7.78 [7.67, 7.89] | 8.10 [7.99, 8.20] | 8.04 [7.90, 8.18] | 8.29 [8.15, 8.42] | 8.27 [8.13, 8.40] |
| 21 | 7.98 [7.88, 8.08] | 7.85 [7.75, 7.96] | 8.21 [8.10, 8.32] | 8.23 [8.09, 8.37] | 8.38 [8.24, 8.52] | 8.37 [8.23, 8.50] |
| 30 | 8.06 [7.94, 8.16] | 7.91 [7.79, 8.02] | 8.23 [8.12, 8.34] | 8.48 [8.30, 8.65] | 8.74 [8.58, 8.89] | 8.72 [8.56, 8.88] |
| 45 | 7.99 [7.88, 8.12] | 7.82 [7.71, 7.94] | 8.25 [8.13, 8.37] | 8.52 [8.30, 8.74] | 9.24 [9.02, 9.45] | 9.22 [9.00, 9.43] |
| 60 | 8.14 [8.01, 8.27] | 7.96 [7.84, 8.09] | 8.46 [8.33, 8.59] | 3.32 [2.40, 4.48] | 3.49 [2.50, 4.69] | 3.49 [2.51, 4.69] |
| 90 | 7.27 [7.09, 7.43] | 7.08 [6.91, 7.24] | 7.61 [7.42, 7.79] |  |  |  |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.42 [7.24, 7.60] | 7.40 [7.22, 7.58] | 7.69 [7.50, 7.88] | 7.14 [6.91, 7.36] |  |  |
| 1 | 7.46 [7.28, 7.64] | 7.43 [7.26, 7.61] | 7.65 [7.46, 7.83] | 7.21 [6.98, 7.45] |  |  |
| 2 | 7.49 [7.30, 7.67] | 7.47 [7.28, 7.65] | 7.65 [7.46, 7.84] | 7.33 [7.09, 7.58] |  |  |
| 3 | 7.55 [7.36, 7.74] | 7.52 [7.34, 7.71] | 7.68 [7.49, 7.87] | 7.44 [7.20, 7.68] | 8.29 [8.03, 8.58] | 8.27 [8.01, 8.56] |
| 4 | 7.62 [7.43, 7.81] | 7.59 [7.41, 7.78] | 7.73 [7.54, 7.92] | 7.54 [7.30, 7.79] | 8.38 [8.13, 8.67] | 8.36 [8.11, 8.65] |
| 5 | 7.77 [7.59, 7.96] | 7.74 [7.56, 7.93] | 7.86 [7.67, 8.06] | 7.62 [7.37, 7.88] | 8.46 [8.19, 8.75] | 8.44 [8.17, 8.73] |
| 6 | 7.93 [7.74, 8.13] | 7.90 [7.71, 8.10] | 7.96 [7.77, 8.16] | 7.71 [7.47, 7.99] | 8.45 [8.18, 8.75] | 8.44 [8.17, 8.72] |
| 7 | 8.10 [7.91, 8.30] | 8.07 [7.88, 8.27] | 8.27 [8.07, 8.47] | 8.04 [7.79, 8.32] | 8.91 [8.61, 9.22] | 8.89 [8.59, 9.20] |
| 10 | 8.29 [8.10, 8.49] | 8.27 [8.07, 8.47] | 8.49 [8.28, 8.69] | 8.31 [8.05, 8.60] | 9.23 [8.91, 9.56] | 9.21 [8.89, 9.53] |
| 14 | 8.67 [8.47, 8.86] | 8.64 [8.45, 8.83] | 8.98 [8.78, 9.18] | 8.86 [8.58, 9.12] | 9.70 [9.41, 10.00] | 9.68 [9.39, 9.98] |
| 21 | 9.12 [8.92, 9.31] | 9.10 [8.90, 9.29] | 9.45 [9.24, 9.64] | 9.44 [9.19, 9.70] | 10.44 [10.14, 10.77] | 10.41 [10.12, 10.74] |
| 30 | 9.29 [9.11, 9.46] | 9.25 [9.08, 9.43] | 9.59 [9.41, 9.77] | 9.93 [9.64, 10.24] | 10.74 [10.44, 11.04] | 10.71 [10.41, 11.01] |
| 45 | 9.72 [9.53, 9.88] | 9.65 [9.47, 9.81] | 9.91 [9.73, 10.07] | 10.29 [9.97, 10.62] | 11.40 [11.08, 11.73] | 11.38 [11.05, 11.70] |
| 60 | 9.75 [9.58, 9.91] | 9.67 [9.51, 9.83] | 9.97 [9.80, 10.14] | 11.05 [5.67, 13.71] | 10.14 [5.08, 12.58] | 10.15 [5.09, 12.59] |
| 90 | 10.10 [9.94, 10.25] | 10.01 [9.85, 10.15] | 10.48 [10.32, 10.64] |  |  |  |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | nf_tide | nf_tide_x_h5 | nf_nhits | nf_nhits_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | spring | 3863936.00 | 9.47 | 8.00 | 8.62 | 7.92 | 8.39 | 8.05 | 9.37 |  |  | 7.94 |  | 9.09 |  | 5.63 | 7.11 |
| 0 | summer | 4426296.00 | 8.03 | 6.80 | 7.44 | 6.78 | 7.12 | 7.01 | 9.70 |  |  | 6.88 |  | 7.93 |  | 5.18 | 5.80 |
| 0 | winter | 256346.00 | 10.59 | 8.39 | 9.88 | 8.43 | 8.99 |  |  |  |  | 8.52 |  | 10.25 |  | 6.17 | 7.49 |
| 1 | spring | 3835735.00 | 9.47 | 8.13 | 8.63 | 8.05 | 8.42 | 8.05 |  |  |  | 8.50 | 8.40 | 9.21 | 9.03 | 5.66 | 7.11 |
| 1 | summer | 4505561.00 | 8.05 | 6.92 | 7.50 | 6.89 | 7.16 | 7.18 | 9.83 |  |  | 7.25 | 7.51 | 7.98 | 8.16 | 5.23 | 5.80 |
| 1 | winter | 230254.00 | 10.67 | 8.50 | 9.73 | 8.53 | 8.93 |  |  |  |  | 9.01 | 8.85 | 10.60 | 10.40 | 6.15 | 7.38 |
| 3 | spring | 3796568.00 | 9.47 | 8.24 | 8.66 | 8.14 | 8.44 | 8.05 |  | 8.50 | 8.48 | 8.74 | 8.52 | 9.41 | 9.15 | 5.68 | 7.11 |
| 3 | summer | 4644739.00 | 8.10 | 7.03 | 7.61 | 7.00 | 7.22 | 7.38 | 9.93 | 7.80 | 7.79 | 7.43 | 7.76 | 8.26 | 8.43 | 5.28 | 5.81 |
| 3 | winter | 172421.00 | 10.89 | 8.28 | 9.50 | 8.30 | 8.67 |  |  |  |  | 9.64 | 8.83 | 10.78 | 10.47 | 5.88 | 7.20 |
| 7 | spring | 3708385.00 |  | 8.50 | 8.72 | 8.39 | 8.83 | 9.11 |  | 10.58 | 10.54 | 9.05 | 8.77 | 9.44 | 9.17 | 5.75 | 7.58 |
| 7 | summer | 4898439.00 |  | 7.27 | 7.79 | 7.22 | 7.50 | 7.74 | 10.45 | 8.16 | 8.14 | 7.71 | 7.90 | 8.38 | 8.46 | 5.37 | 6.19 |
| 7 | winter | 75587.00 |  | 7.98 | 8.83 | 7.98 | 8.50 |  |  |  |  | 10.33 | 9.08 | 10.42 | 9.85 | 5.78 | 7.05 |
| 14 | spring | 3459271.00 |  | 8.75 | 8.91 | 8.63 | 9.08 |  |  |  |  |  |  |  |  | 5.88 | 7.80 |
| 14 | summer | 5346544.00 |  | 7.65 | 8.08 | 7.58 | 7.86 | 8.20 | 10.85 | 8.58 | 8.56 |  |  |  |  | 5.52 | 6.57 |
| 30 | autumn | 313831.00 |  | 7.98 | 7.92 | 7.88 | 8.76 | 9.29 | 10.05 | 10.53 | 10.50 |  |  |  |  | 5.13 | 8.00 |
| 30 | spring | 2774276.00 |  | 9.04 | 9.26 | 8.90 | 9.32 |  |  |  |  |  |  |  |  | 5.91 | 8.04 |
| 30 | summer | 5832721.00 |  | 8.21 | 8.48 | 8.10 | 8.39 | 8.83 | 11.52 | 9.15 | 9.14 |  |  |  |  | 5.66 | 7.11 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

**Spearman of the daily level within park-month** (paired on the same days)

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.14 [0.09, 0.19] | 0.15 [0.12, 0.19] | 0.22 [0.19, 0.25] | 0.04 [0.01, 0.07] | 0.21 [0.18, 0.24] | 0.23 [0.20, 0.26] | 0.22 [0.18, 0.25] | 0.12 [0.08, 0.15] |
| 2 | 0.14 [0.09, 0.18] | 0.16 [0.13, 0.19] | 0.23 [0.20, 0.26] | -0.01 [-0.04, 0.02] | 0.15 [0.11, 0.18] | 0.23 [0.20, 0.26] | 0.22 [0.18, 0.26] | 0.11 [0.08, 0.15] |
| 3 | 0.18 [0.14, 0.21] | 0.14 [0.11, 0.18] | 0.22 [0.19, 0.25] | -0.01 [-0.04, 0.02] | 0.14 [0.11, 0.17] | 0.22 [0.20, 0.25] | 0.22 [0.19, 0.26] | 0.11 [0.07, 0.14] |
| 4 | 0.20 [0.16, 0.23] | 0.14 [0.11, 0.17] | 0.22 [0.19, 0.25] | 0.03 [-0.00, 0.06] | 0.17 [0.14, 0.20] | 0.22 [0.20, 0.25] | 0.25 [0.22, 0.28] | 0.11 [0.08, 0.15] |
| 5 | 0.20 [0.16, 0.24] | 0.13 [0.09, 0.16] | 0.22 [0.19, 0.25] | 0.03 [0.00, 0.06] | 0.17 [0.14, 0.20] | 0.22 [0.19, 0.25] | 0.21 [0.17, 0.24] | 0.10 [0.07, 0.13] |
| 6 | 0.12 [0.08, 0.16] | 0.11 [0.07, 0.14] | 0.21 [0.18, 0.24] | 0.06 [0.03, 0.09] | 0.15 [0.12, 0.18] | 0.21 [0.19, 0.24] | 0.19 [0.16, 0.22] | 0.09 [0.05, 0.12] |
| 7 | 0.09 [0.05, 0.12] | 0.10 [0.06, 0.13] | 0.18 [0.14, 0.21] | -0.00 [-0.03, 0.03] | 0.10 [0.07, 0.13] |  | 0.15 [0.12, 0.19] | 0.06 [0.03, 0.09] |
| 10 | 0.07 [0.03, 0.11] | 0.10 [0.06, 0.14] | 0.19 [0.15, 0.22] |  |  |  | 0.17 [0.13, 0.20] | 0.09 [0.05, 0.12] |
| 14 | 0.05 [0.01, 0.08] | 0.09 [0.06, 0.13] | 0.16 [0.13, 0.19] |  |  |  | 0.12 [0.08, 0.15] | 0.07 [0.03, 0.10] |
| 21 | 0.05 [0.02, 0.09] | 0.08 [0.05, 0.12] | 0.13 [0.10, 0.16] |  |  |  | 0.10 [0.06, 0.14] | 0.07 [0.04, 0.11] |
| 30 | 0.01 [-0.03, 0.06] | 0.12 [0.09, 0.15] | 0.15 [0.12, 0.18] |  |  |  | 0.06 [0.01, 0.11] | 0.04 [0.00, 0.08] |
| 45 | 0.14 [0.06, 0.21] | 0.12 [0.09, 0.16] | 0.16 [0.13, 0.19] |  |  |  | 0.16 [0.12, 0.21] | 0.10 [0.07, 0.14] |
| 60 |  | 0.16 [0.13, 0.20] | 0.16 [0.13, 0.20] |  |  |  | 0.14 [0.14, 0.14] | 0.12 [0.08, 0.16] |
| 90 |  | 0.15 [0.11, 0.20] | 0.15 [0.11, 0.19] |  |  |  |  | 0.16 [0.11, 0.21] |
| 120 |  | 0.13 [0.07, 0.19] | 0.21 [0.15, 0.26] |  |  |  |  | 0.14 [0.09, 0.20] |
| 180 |  | 0.25 [0.14, 0.35] | 0.33 [0.25, 0.40] |  |  |  |  | 0.31 [0.21, 0.40] |

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|
| 1 | -0.08 [-0.13, -0.03] | -0.07 [-0.11, -0.04] | -0.01 [-0.04, 0.02] | -0.19 [-0.22, -0.15] | -0.02 [-0.06, 0.01] | -0.01 [-0.06, 0.03] | -0.11 [-0.15, -0.07] | lvl_snaive7 |
| 2 | -0.09 [-0.13, -0.04] | -0.07 [-0.11, -0.03] | -0.00 [-0.03, 0.03] | -0.24 [-0.27, -0.21] | -0.08 [-0.11, -0.05] | -0.02 [-0.06, 0.03] | -0.12 [-0.16, -0.08] | lvl_snaive7 |
| 3 | -0.04 [-0.09, 0.01] | -0.08 [-0.12, -0.04] | -0.00 [-0.03, 0.02] | -0.23 [-0.27, -0.19] | -0.08 [-0.11, -0.05] | -0.01 [-0.05, 0.04] | -0.12 [-0.16, -0.08] | lvl_snaive7 |
| 4 | -0.02 [-0.06, 0.02] | -0.08 [-0.12, -0.04] | -0.00 [-0.03, 0.02] | -0.19 [-0.23, -0.15] | -0.05 [-0.08, -0.03] | 0.02 [-0.02, 0.06] | -0.11 [-0.15, -0.07] | lvl_snaive7 |
| 5 | -0.00 [-0.04, 0.03] | -0.09 [-0.13, -0.05] | -0.00 [-0.03, 0.03] | -0.19 [-0.23, -0.16] | -0.05 [-0.08, -0.02] | -0.01 [-0.05, 0.02] | -0.12 [-0.16, -0.08] | lvl_snaive7 |
| 6 | -0.07 [-0.11, -0.03] | -0.10 [-0.14, -0.06] | -0.00 [-0.03, 0.02] | -0.15 [-0.18, -0.12] | -0.07 [-0.09, -0.04] | -0.02 [-0.05, 0.02] | -0.12 [-0.16, -0.09] | lvl_snaive7 |
| 7 | -0.10 [-0.14, -0.06] | -0.07 [-0.11, -0.04] |  | -0.17 [-0.21, -0.14] | -0.07 [-0.10, -0.03] | -0.07 [-0.11, -0.03] | -0.11 [-0.14, -0.08] | lvl_naive4 |
| 10 | -0.14 [-0.19, -0.09] | -0.08 [-0.12, -0.05] |  |  |  | -0.08 [-0.11, -0.04] | -0.10 [-0.13, -0.07] | lvl_naive4 |
| 14 | -0.10 [-0.15, -0.06] | -0.06 [-0.10, -0.03] |  |  |  | -0.07 [-0.10, -0.03] | -0.09 [-0.12, -0.06] | lvl_naive4 |
| 21 | -0.06 [-0.10, -0.02] | -0.05 [-0.08, -0.01] |  |  |  | -0.06 [-0.09, -0.02] | -0.06 [-0.09, -0.03] | lvl_naive4 |
| 30 | -0.10 [-0.15, -0.05] | -0.03 [-0.06, 0.01] |  |  |  | -0.09 [-0.13, -0.05] | -0.10 [-0.13, -0.07] | lvl_naive4 |
| 45 | -0.13 [-0.20, -0.05] | -0.04 [-0.08, 0.01] |  |  |  | 0.00 [-0.06, 0.06] | -0.06 [-0.09, -0.02] | lvl_naive4 |
| 60 |  | 0.00 [-0.04, 0.04] |  |  |  | 0.45 [0.45, 0.45] | -0.04 [-0.08, -0.00] | lvl_naive4 |
| 90 |  | -0.00 [-0.05, 0.05] |  |  |  |  | 0.01 [-0.03, 0.05] | lvl_naive4 |
| 120 |  | -0.07 [-0.13, -0.01] |  |  |  |  | -0.07 [-0.11, -0.02] | lvl_naive4 |
| 180 |  | -0.08 [-0.17, -0.01] |  |  |  |  | -0.02 [-0.10, 0.06] | lvl_naive4 |

**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the `≥ predicted q75` rule (ties included, which is what flat predictions cost)

| lead | model | park_months | busy_day_recall | busy_day_precision_q75_rule | iqr | iqr_true |
|---|---|---|---|---|---|---|
| 1 | lvl_cbd | 225 | 0.34 | 0.34 | 4.75 | 12.35 |
| 1 | lvl_clim | 448 | 0.35 | 0.33 | 5.90 | 13.62 |
| 1 | lvl_naive4 | 447 | 0.38 | 0.38 | 9.39 | 13.62 |
| 1 | lvl_nf_nhits | 446 | 0.31 | 0.31 | 4.84 | 13.64 |
| 1 | lvl_nf_tide | 446 | 0.37 | 0.37 | 4.81 | 13.64 |
| 1 | lvl_snaive7 | 444 | 0.38 | 0.38 | 14.08 | 13.64 |
| 1 | lvl_tft | 247 | 0.42 | 0.42 | 9.22 | 12.21 |
| 1 | lvl_wt56 | 448 | 0.35 | 0.35 | 6.84 | 13.62 |
| 2 | lvl_cbd | 230 | 0.35 | 0.35 | 4.47 | 12.42 |
| 2 | lvl_clim | 452 | 0.35 | 0.34 | 5.95 | 13.75 |
| 2 | lvl_naive4 | 451 | 0.39 | 0.38 | 9.36 | 13.75 |
| 2 | lvl_nf_nhits | 450 | 0.28 | 0.28 | 4.69 | 13.75 |
| 2 | lvl_nf_tide | 450 | 0.35 | 0.35 | 4.80 | 13.75 |
| 2 | lvl_snaive7 | 446 | 0.39 | 0.38 | 14.10 | 13.66 |
| 2 | lvl_tft | 257 | 0.43 | 0.43 | 9.05 | 12.45 |
| 2 | lvl_wt56 | 452 | 0.35 | 0.35 | 6.89 | 13.75 |
| 3 | lvl_cbd | 230 | 0.35 | 0.35 | 4.74 | 12.50 |
| 3 | lvl_clim | 423 | 0.34 | 0.33 | 5.86 | 13.75 |
| 3 | lvl_naive4 | 422 | 0.38 | 0.38 | 9.41 | 13.75 |
| 3 | lvl_nf_nhits | 421 | 0.27 | 0.27 | 4.85 | 13.75 |
| 3 | lvl_nf_tide | 421 | 0.33 | 0.33 | 4.57 | 13.75 |
| 3 | lvl_snaive7 | 419 | 0.38 | 0.38 | 14.20 | 13.75 |
| 3 | lvl_tft | 270 | 0.42 | 0.42 | 9.18 | 12.42 |
| 3 | lvl_wt56 | 423 | 0.34 | 0.34 | 6.84 | 13.75 |
| 4 | lvl_cbd | 231 | 0.35 | 0.36 | 4.88 | 12.50 |
| 4 | lvl_clim | 423 | 0.34 | 0.33 | 5.79 | 13.75 |
| 4 | lvl_naive4 | 422 | 0.38 | 0.38 | 9.40 | 13.75 |
| 4 | lvl_nf_nhits | 421 | 0.28 | 0.28 | 5.29 | 13.75 |
| 4 | lvl_nf_tide | 421 | 0.35 | 0.35 | 4.74 | 13.75 |
| 4 | lvl_snaive7 | 420 | 0.38 | 0.37 | 14.14 | 13.71 |
| 4 | lvl_tft | 270 | 0.40 | 0.40 | 9.68 | 12.42 |
| 4 | lvl_wt56 | 423 | 0.34 | 0.34 | 6.77 | 13.75 |
| 5 | lvl_cbd | 231 | 0.36 | 0.37 | 5.43 | 12.62 |
| 5 | lvl_clim | 422 | 0.34 | 0.32 | 5.65 | 13.88 |
| 5 | lvl_naive4 | 421 | 0.38 | 0.38 | 9.38 | 13.90 |
| 5 | lvl_nf_nhits | 420 | 0.28 | 0.28 | 5.33 | 13.88 |
| 5 | lvl_nf_tide | 420 | 0.35 | 0.35 | 4.86 | 13.88 |
| 5 | lvl_snaive7 | 420 | 0.37 | 0.37 | 14.18 | 13.88 |
| 5 | lvl_tft | 270 | 0.40 | 0.40 | 9.13 | 12.56 |
| 5 | lvl_wt56 | 422 | 0.34 | 0.33 | 6.61 | 13.88 |
| 6 | lvl_cbd | 231 | 0.33 | 0.33 | 4.44 | 12.71 |
| 6 | lvl_clim | 422 | 0.34 | 0.32 | 5.95 | 14.00 |
| 6 | lvl_naive4 | 421 | 0.39 | 0.38 | 9.22 | 14.00 |
| 6 | lvl_nf_nhits | 420 | 0.32 | 0.32 | 5.56 | 14.00 |
| 6 | lvl_nf_tide | 420 | 0.35 | 0.35 | 4.77 | 14.00 |
| 6 | lvl_snaive7 | 420 | 0.38 | 0.37 | 14.16 | 14.00 |
| 6 | lvl_tft | 270 | 0.40 | 0.40 | 9.04 | 12.76 |
| 6 | lvl_wt56 | 422 | 0.35 | 0.34 | 6.66 | 14.00 |
| 7 | lvl_cbd | 231 | 0.29 | 0.29 | 3.94 | 12.56 |
| 7 | lvl_clim | 422 | 0.33 | 0.32 | 5.90 | 13.89 |
| 7 | lvl_naive4 | 421 | 0.38 | 0.38 | 9.75 | 13.96 |
| 7 | lvl_nf_nhits | 420 | 0.27 | 0.27 | 5.36 | 13.89 |
| 7 | lvl_nf_tide | 420 | 0.33 | 0.33 | 4.12 | 13.89 |
| 7 | lvl_tft | 270 | 0.36 | 0.36 | 9.65 | 12.77 |
| 7 | lvl_wt56 | 422 | 0.34 | 0.34 | 6.38 | 13.89 |
| 10 | lvl_cbd | 231 | 0.32 | 0.31 | 4.00 | 13.08 |
| 10 | lvl_clim | 422 | 0.33 | 0.32 | 7.34 | 14.00 |
| 10 | lvl_naive4 | 421 | 0.38 | 0.38 | 9.75 | 14.00 |
| 10 | lvl_tft | 270 | 0.37 | 0.37 | 10.00 | 13.30 |
| 10 | lvl_wt56 | 422 | 0.35 | 0.34 | 6.45 | 14.00 |
| 14 | lvl_cbd | 199 | 0.30 | 0.30 | 3.86 | 13.17 |
| 14 | lvl_clim | 422 | 0.32 | 0.32 | 7.03 | 13.85 |
| 14 | lvl_naive4 | 420 | 0.36 | 0.36 | 9.78 | 13.87 |
| 14 | lvl_tft | 271 | 0.34 | 0.34 | 10.65 | 13.17 |
| 14 | lvl_wt56 | 422 | 0.34 | 0.34 | 6.32 | 13.85 |
| 21 | lvl_cbd | 198 | 0.31 | 0.30 | 3.89 | 13.25 |
| 21 | lvl_clim | 411 | 0.32 | 0.31 | 6.46 | 13.83 |
| 21 | lvl_naive4 | 398 | 0.35 | 0.34 | 9.90 | 13.90 |
| 21 | lvl_tft | 267 | 0.33 | 0.34 | 10.53 | 13.17 |
| 21 | lvl_wt56 | 411 | 0.33 | 0.32 | 6.60 | 13.83 |
| 30 | lvl_cbd | 156 | 0.30 | 0.29 | 4.66 | 13.26 |
| 30 | lvl_clim | 360 | 0.32 | 0.32 | 6.27 | 13.90 |
| 30 | lvl_naive4 | 360 | 0.35 | 0.34 | 9.76 | 13.90 |
| 30 | lvl_tft | 178 | 0.31 | 0.31 | 9.24 | 13.62 |
| 30 | lvl_wt56 | 360 | 0.32 | 0.31 | 6.91 | 13.90 |
| 45 | lvl_cbd | 77 | 0.37 | 0.36 | 4.65 | 14.88 |
| 45 | lvl_clim | 410 | 0.34 | 0.33 | 6.20 | 13.80 |
| 45 | lvl_naive4 | 409 | 0.37 | 0.37 | 9.87 | 13.82 |
| 45 | lvl_tft | 172 | 0.35 | 0.36 | 10.60 | 14.68 |
| 45 | lvl_wt56 | 410 | 0.35 | 0.34 | 6.40 | 13.80 |
| 60 | lvl_clim | 354 | 0.36 | 0.33 | 4.73 | 13.16 |
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

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.36 [0.36, 0.37] | 0.38 [0.38, 0.39] | 0.39 [0.39, 0.40] | 0.38 [0.37, 0.39] | 0.40 [0.39, 0.40] | 0.54 [0.53, 0.55] | 0.38 [0.37, 0.39] | 0.38 [0.37, 0.38] |
| 0-120 | 0.44 [0.44, 0.45] | 0.47 [0.46, 0.47] | 0.48 [0.48, 0.49] | 0.47 [0.46, 0.47] | 0.47 [0.47, 0.48] | 0.62 [0.61, 0.62] | 0.46 [0.46, 0.47] | 0.46 [0.46, 0.47] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.01 [-0.02, 0.00] | 0.01 [-0.00, 0.03] | 0.02 [0.00, 0.04]* | 0.01 [-0.01, 0.04] | 0.01 [-0.01, 0.03] | 0.16 [0.13, 0.19]* | 0.01 [-0.01, 0.02] | snaive7 |
| 0-120 | -0.01 [-0.03, 0.00] | 0.02 [-0.00, 0.04] | 0.02 [0.01, 0.04]* | 0.02 [-0.01, 0.04] | 0.00 [-0.01, 0.02] | 0.16 [0.13, 0.18]* | 0.01 [-0.01, 0.02] | snaive7 |

**next-best precision (top-3)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.44 [0.43, 0.44] | 0.45 [0.44, 0.45] | 0.45 [0.44, 0.46] | 0.43 [0.42, 0.44] | 0.42 [0.41, 0.42] | 0.55 [0.55, 0.56] | 0.45 [0.44, 0.45] | 0.45 [0.45, 0.46] |
| 0-120 | 0.52 [0.51, 0.52] | 0.53 [0.53, 0.54] | 0.54 [0.53, 0.54] | 0.52 [0.51, 0.52] | 0.49 [0.48, 0.49] | 0.63 [0.62, 0.63] | 0.54 [0.53, 0.54] | 0.53 [0.53, 0.54] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.02 [-0.02, -0.01] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.05 [-0.07, -0.04] | 0.08 [0.07, 0.10]* | -0.02 [-0.03, -0.01] |  | wt_med |
| 0-120 | -0.01 [-0.02, 0.00] | 0.01 [-0.00, 0.02] | 0.00 [-0.01, 0.02] | -0.00 [-0.02, 0.02] | -0.05 [-0.07, -0.04] | 0.09 [0.08, 0.11]* |  | 0.01 [-0.00, 0.02] | snaive7 |

**next-best recall**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0-60 | 0.45 [0.44, 0.46] | 0.38 [0.38, 0.39] | 0.35 [0.34, 0.35] | 0.19 [0.19, 0.20] | 0.20 [0.20, 0.21] | 0.31 [0.31, 0.32] | 0.51 [0.50, 0.52] | 0.47 [0.47, 0.48] |
| 0-120 | 0.48 [0.47, 0.49] | 0.41 [0.40, 0.42] | 0.37 [0.36, 0.37] | 0.21 [0.20, 0.22] | 0.22 [0.22, 0.23] | 0.34 [0.34, 0.35] | 0.55 [0.55, 0.56] | 0.50 [0.50, 0.51] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| 0-60 | -0.06 [-0.08, -0.04] | -0.13 [-0.15, -0.10] | -0.16 [-0.18, -0.14] | -0.32 [-0.35, -0.28] | -0.30 [-0.33, -0.28] | -0.20 [-0.22, -0.17] | -0.04 [-0.05, -0.02] | snaive7 |
| 0-120 | -0.07 [-0.09, -0.06] | -0.14 [-0.17, -0.12] | -0.19 [-0.21, -0.16] | -0.34 [-0.38, -0.31] | -0.33 [-0.36, -0.30] | -0.21 [-0.24, -0.19] | -0.05 [-0.07, -0.03] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| w0-45 | 13.15 [13.00, 13.29] | 12.03 [11.90, 12.16] | 12.37 [12.23, 12.51] | 11.25 [11.10, 11.41] | 9.04 [8.97, 9.11] | 8.13 [8.06, 8.19] | 6.38 [6.32, 6.45] | 14.34 [14.17, 14.51] | 12.12 [11.99, 12.24] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | nf_nhits | nf_tide | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| w0-45 | 7.29 [6.50, 8.11] | 6.22 [5.55, 6.92] | 6.58 [5.85, 7.32] | 5.56 [4.95, 6.19] | 2.34 [2.01, 2.69] | 1.69 [1.38, 2.00] | 8.46 [7.64, 9.24] | 6.29 [5.62, 7.00] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | nf_tide | nf_nhits |
|---|---|---|---|---|---|---|---|---|---|
| m015 | -0.17 | -0.03 | -1.88 | -1.30 | -2.48 | -1.95 | -2.57 | -0.67 | -0.13 |
| m030 | -0.21 | -0.04 | -2.00 | -1.41 | -2.36 | -1.83 | -2.49 | -0.90 | -0.52 |
| m045 | 0.07 | -0.05 | -2.03 | -1.45 | -2.24 | -1.71 | -2.40 | -0.91 | -0.55 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.75 [0.75, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.80] | 0.75 [0.75, 0.75] |  | 0.76 [0.76, 0.77] |  |  |  | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 1 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] | 0.74 [0.74, 0.74] | 0.75 [0.75, 0.76] | 0.74 [0.74, 0.75] | 0.77 [0.76, 0.77] |  |  | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 2 | 0.83 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] | 0.74 [0.74, 0.74] | 0.75 [0.75, 0.75] | 0.74 [0.74, 0.75] | 0.76 [0.76, 0.77] |  |  | 0.82 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 3 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] | 0.72 [0.72, 0.72] | 0.75 [0.75, 0.76] | 0.73 [0.73, 0.74] | 0.76 [0.76, 0.77] | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 4 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.78, 0.79] | 0.74 [0.74, 0.75] | 0.75 [0.75, 0.76] | 0.73 [0.73, 0.74] | 0.76 [0.76, 0.77] | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 5 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.78 [0.78, 0.79] | 0.74 [0.73, 0.74] | 0.75 [0.75, 0.76] | 0.75 [0.74, 0.75] | 0.76 [0.76, 0.77] | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 6 | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] | 0.73 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.79] | 0.74 [0.74, 0.74] | 0.75 [0.75, 0.76] | 0.72 [0.72, 0.73] | 0.76 [0.76, 0.76] | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.83] |
| 7 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.73 [0.73, 0.74] | 0.80 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.73 [0.73, 0.74] | 0.75 [0.74, 0.75] | 0.72 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.83] |
| 10 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.73 [0.72, 0.73] | 0.80 [0.79, 0.80] | 0.78 [0.77, 0.78] |  |  |  |  | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.83] |
| 14 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.73 [0.72, 0.73] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] |  |  |  |  | 0.75 [0.74, 0.75] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.83] |
| 21 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.83] | 0.72 [0.71, 0.72] | 0.79 [0.79, 0.79] | 0.77 [0.77, 0.78] |  |  |  |  | 0.75 [0.74, 0.75] | 0.76 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 30 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.82] | 0.72 [0.71, 0.72] | 0.79 [0.79, 0.79] | 0.77 [0.77, 0.78] |  |  |  |  | 0.75 [0.74, 0.76] | 0.76 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 45 | 0.82 [0.82, 0.83] | 0.82 [0.82, 0.82] | 0.72 [0.71, 0.72] | 0.79 [0.79, 0.79] | 0.78 [0.77, 0.79] |  |  |  |  | 0.75 [0.74, 0.76] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 60 | 0.83 [0.82, 0.83] | 0.82 [0.82, 0.83] |  | 0.79 [0.79, 0.80] | 0.80 [0.74, 0.85] |  |  |  |  | 0.75 [0.67, 0.83] | 0.75 [0.66, 0.83] |  | 0.82 [0.82, 0.82] |
| 90 | 0.83 [0.83, 0.84] | 0.83 [0.83, 0.83] |  | 0.80 [0.79, 0.80] |  |  |  |  |  |  |  |  | 0.82 [0.82, 0.83] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.07 [-0.08, -0.06] |  | -0.05 [-0.06, -0.04] |  |  |  | -0.03 [-0.04, -0.03] |  | wt_med |
| 1 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.07, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.08 [-0.09, -0.07] | -0.07 [-0.08, -0.05] | -0.07 [-0.09, -0.06] | -0.05 [-0.06, -0.04] |  |  | -0.03 [-0.04, -0.03] |  | wt_med |
| 2 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.08 [-0.09, -0.07] | -0.07 [-0.08, -0.06] | -0.07 [-0.08, -0.06] | -0.06 [-0.07, -0.05] |  |  | -0.03 [-0.04, -0.03] |  | wt_med |
| 3 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.10 [-0.11, -0.08] | -0.07 [-0.08, -0.05] | -0.08 [-0.10, -0.07] | -0.06 [-0.07, -0.05] | -0.06 [-0.08, -0.04] | -0.04 [-0.06, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 4 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | -0.07 [-0.09, -0.06] | -0.07 [-0.08, -0.05] | -0.08 [-0.09, -0.07] | -0.06 [-0.07, -0.05] | -0.06 [-0.08, -0.04] | -0.04 [-0.06, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 5 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.02] | -0.08 [-0.09, -0.07] | -0.07 [-0.08, -0.05] | -0.07 [-0.08, -0.06] | -0.06 [-0.07, -0.05] | -0.06 [-0.08, -0.04] | -0.04 [-0.06, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 6 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.09, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.03, -0.02] | -0.08 [-0.09, -0.07] | -0.07 [-0.08, -0.06] | -0.09 [-0.10, -0.08] | -0.06 [-0.07, -0.05] | -0.06 [-0.08, -0.04] | -0.04 [-0.06, -0.03] | -0.03 [-0.04, -0.03] |  | wt_med |
| 7 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.07 [-0.09, -0.06] | -0.03 [-0.03, -0.02] | -0.03 [-0.04, -0.02] | -0.08 [-0.09, -0.07] | -0.07 [-0.08, -0.06] | -0.09 [-0.11, -0.08] | -0.06 [-0.07, -0.05] | -0.06 [-0.08, -0.04] | -0.04 [-0.06, -0.03] |  |  | wt_med |
| 10 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.08 [-0.09, -0.06] | -0.02 [-0.03, -0.02] | -0.03 [-0.04, -0.02] |  |  |  |  | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.03] |  |  | wt_med |
| 14 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.08 [-0.10, -0.06] | -0.03 [-0.03, -0.02] | -0.03 [-0.04, -0.02] |  |  |  |  | -0.06 [-0.08, -0.04] | -0.04 [-0.05, -0.02] |  |  | wt_med |
| 21 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.08 [-0.10, -0.06] | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] |  |  |  |  | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  |  | wt_med |
| 30 | -0.00 [-0.00, -0.00] | 0.01 [0.00, 0.01]* | -0.08 [-0.10, -0.06] | -0.03 [-0.03, -0.02] | -0.03 [-0.03, -0.02] |  |  |  |  | -0.05 [-0.07, -0.03] | -0.03 [-0.04, -0.02] |  |  | wt_med |
| 45 | -0.00 [-0.00, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.03 [-0.03, -0.02] | -0.02 [-0.03, -0.01] |  |  |  |  | -0.05 [-0.07, -0.03] | -0.03 [-0.04, -0.01] |  |  | wt_med |
| 60 |  | 0.01 [0.00, 0.01]* |  | -0.03 [-0.04, -0.02] | -0.05 [-0.11, 0.01] |  |  |  |  | -0.05 [-0.08, 0.03] | -0.05 [-0.09, 0.02] |  | -0.00 [-0.01, -0.00] | clim |
| 90 | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.01] |  | -0.03 [-0.04, -0.02] |  |  |  |  |  |  |  |  |  | wt_med |

**best-time hit rate (±30 min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.84] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.75 [0.75, 0.75] |  | 0.76 [0.76, 0.77] |  |  |  | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 1 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.84] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.74 [0.74, 0.74] | 0.79 [0.79, 0.79] | 0.75 [0.75, 0.76] | 0.80 [0.79, 0.80] |  |  | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 2 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.84] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.80 [0.80, 0.81] | 0.74 [0.74, 0.74] | 0.79 [0.78, 0.79] | 0.75 [0.74, 0.75] | 0.79 [0.79, 0.80] |  |  | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 3 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.77 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.80 [0.80, 0.81] | 0.73 [0.72, 0.73] | 0.79 [0.78, 0.79] | 0.74 [0.74, 0.75] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 4 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.84] | 0.77 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.80 [0.80, 0.81] | 0.74 [0.74, 0.75] | 0.79 [0.78, 0.79] | 0.74 [0.74, 0.74] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 5 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.77 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.80 [0.80, 0.81] | 0.74 [0.73, 0.74] | 0.79 [0.79, 0.79] | 0.75 [0.75, 0.76] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 6 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.82, 0.82] | 0.80 [0.80, 0.81] | 0.74 [0.74, 0.74] | 0.79 [0.78, 0.79] | 0.74 [0.74, 0.74] | 0.79 [0.79, 0.79] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] | 0.83 [0.82, 0.83] | 0.83 [0.83, 0.83] |
| 7 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.74 [0.73, 0.74] | 0.78 [0.78, 0.79] | 0.73 [0.73, 0.74] | 0.79 [0.79, 0.79] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] |  | 0.83 [0.83, 0.83] |
| 10 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.79, 0.80] |  |  |  |  | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] |  | 0.83 [0.82, 0.83] |
| 14 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.75, 0.76] | 0.81 [0.81, 0.82] | 0.80 [0.79, 0.80] |  |  |  |  | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] |  | 0.83 [0.82, 0.83] |
| 21 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.76 [0.75, 0.76] | 0.81 [0.81, 0.81] | 0.79 [0.79, 0.80] |  |  |  |  | 0.77 [0.77, 0.78] | 0.78 [0.78, 0.79] |  | 0.83 [0.82, 0.83] |
| 30 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.79, 0.80] |  |  |  |  | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] |  | 0.83 [0.82, 0.83] |
| 45 | 0.83 [0.83, 0.83] | 0.83 [0.83, 0.83] | 0.75 [0.74, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.80, 0.81] |  |  |  |  | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.79] |  | 0.83 [0.82, 0.83] |
| 60 | 0.83 [0.83, 0.84] | 0.83 [0.83, 0.83] |  | 0.81 [0.81, 0.81] | 0.82 [0.77, 0.87] |  |  |  |  | 0.77 [0.70, 0.84] | 0.78 [0.70, 0.85] |  | 0.83 [0.82, 0.83] |
| 90 | 0.84 [0.83, 0.84] | 0.84 [0.83, 0.84] |  | 0.81 [0.81, 0.82] |  |  |  |  |  |  |  |  | 0.83 [0.83, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.08 [-0.08, -0.07] |  | -0.06 [-0.07, -0.05] |  |  |  | -0.03 [-0.03, -0.02] | wt_med |
| 1 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.01] | -0.01 [-0.02, -0.01] | -0.08 [-0.09, -0.07] | -0.04 [-0.05, -0.03] | -0.07 [-0.08, -0.06] | -0.03 [-0.04, -0.02] |  |  | -0.03 [-0.03, -0.02] | wt_med |
| 2 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.08 [-0.09, -0.07] | -0.04 [-0.05, -0.03] | -0.07 [-0.08, -0.07] | -0.03 [-0.04, -0.02] |  |  | -0.03 [-0.03, -0.02] | wt_med |
| 3 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.10 [-0.11, -0.09] | -0.04 [-0.05, -0.03] | -0.08 [-0.09, -0.07] | -0.03 [-0.04, -0.02] | -0.04 [-0.06, -0.03] | -0.03 [-0.04, -0.02] | -0.03 [-0.03, -0.02] | wt_med |
| 4 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.01] | -0.01 [-0.02, -0.01] | -0.08 [-0.09, -0.07] | -0.04 [-0.05, -0.03] | -0.08 [-0.09, -0.07] | -0.03 [-0.04, -0.02] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.02] | -0.03 [-0.03, -0.02] | wt_med |
| 5 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.05, -0.03] | -0.01 [-0.01, -0.01] | -0.01 [-0.02, -0.01] | -0.09 [-0.10, -0.08] | -0.04 [-0.05, -0.03] | -0.07 [-0.08, -0.06] | -0.03 [-0.04, -0.03] | -0.04 [-0.06, -0.03] | -0.03 [-0.04, -0.02] | -0.03 [-0.03, -0.02] | wt_med |
| 6 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.08 [-0.09, -0.07] | -0.04 [-0.05, -0.03] | -0.08 [-0.09, -0.07] | -0.03 [-0.04, -0.03] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.02] | -0.02 [-0.03, -0.02] | wt_med |
| 7 | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.01] | -0.02 [-0.02, -0.01] | -0.09 [-0.10, -0.08] | -0.04 [-0.05, -0.03] | -0.09 [-0.10, -0.08] | -0.04 [-0.04, -0.03] | -0.04 [-0.05, -0.03] | -0.03 [-0.04, -0.02] |  | wt_med |
| 10 | -0.00 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.07, -0.04] | -0.01 [-0.02, -0.01] | -0.02 [-0.02, -0.01] |  |  |  |  | -0.04 [-0.05, -0.02] | -0.02 [-0.04, -0.01] |  | wt_med |
| 14 | -0.00 [-0.00, -0.00] | 0.01 [0.00, 0.01]* | -0.05 [-0.07, -0.04] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] |  |  |  |  | -0.04 [-0.05, -0.02] | -0.02 [-0.04, -0.01] |  | wt_med |
| 21 | -0.00 [-0.00, -0.00] | 0.01 [0.01, 0.01]* | -0.05 [-0.07, -0.04] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] |  |  |  |  | -0.03 [-0.05, -0.02] | -0.02 [-0.04, -0.01] |  | wt_med |
| 30 | -0.00 [-0.00, -0.00] | 0.01 [0.01, 0.01]* | -0.05 [-0.07, -0.04] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.01] |  |  |  |  | -0.03 [-0.04, -0.02] | -0.02 [-0.03, -0.01] |  | wt_med |
| 45 | -0.00 [-0.00, 0.00] | 0.01 [0.01, 0.01]* | -0.02 [-0.04, -0.00] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, 0.00] |  |  |  |  | -0.03 [-0.04, -0.01] | -0.01 [-0.03, 0.00] |  | wt_med |
| 60 | 0.00 [-0.00, 0.01] | 0.01 [0.01, 0.01]* |  | -0.01 [-0.02, -0.01] | -0.02 [-0.05, 0.01] |  |  |  |  | -0.04 [-0.08, 0.03] | -0.03 [-0.05, 0.05] |  | wt_med |
| 90 | 0.00 [-0.00, 0.01] | 0.01 [0.00, 0.01] |  | -0.02 [-0.03, -0.01] |  |  |  |  |  |  |  |  | wt_med |

**best-time regret (min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 2.96 [2.89, 3.02] | 2.77 [2.72, 2.83] | 3.81 [3.72, 3.90] | 3.16 [3.10, 3.21] | 3.21 [3.14, 3.28] | 4.99 [4.91, 5.06] |  | 4.24 [4.17, 4.31] |  |  |  | 3.39 [3.30, 3.48] | 2.94 [2.89, 3.01] |
| 1 | 2.96 [2.89, 3.03] | 2.79 [2.74, 2.85] | 3.89 [3.80, 3.99] | 3.16 [3.10, 3.22] | 3.28 [3.21, 3.35] | 5.14 [5.06, 5.22] | 3.88 [3.81, 3.94] | 4.73 [4.66, 4.80] | 3.71 [3.65, 3.77] |  |  | 3.38 [3.29, 3.47] | 2.96 [2.90, 3.02] |
| 2 | 2.96 [2.89, 3.03] | 2.79 [2.73, 2.84] | 3.93 [3.85, 4.03] | 3.16 [3.10, 3.22] | 3.31 [3.23, 3.39] | 5.15 [5.07, 5.23] | 3.92 [3.86, 3.98] | 4.77 [4.70, 4.85] | 3.74 [3.68, 3.80] |  |  | 3.39 [3.30, 3.49] | 2.96 [2.90, 3.02] |
| 3 | 2.97 [2.90, 3.04] | 2.80 [2.74, 2.86] | 3.96 [3.87, 4.06] | 3.17 [3.11, 3.23] | 3.34 [3.26, 3.41] | 5.41 [5.33, 5.49] | 3.94 [3.87, 4.00] | 4.92 [4.84, 4.99] | 3.79 [3.73, 3.85] | 4.20 [4.10, 4.31] | 3.70 [3.61, 3.79] | 3.40 [3.30, 3.49] | 2.98 [2.92, 3.04] |
| 4 | 2.97 [2.90, 3.04] | 2.80 [2.75, 2.86] | 3.99 [3.90, 4.08] | 3.18 [3.11, 3.23] | 3.33 [3.26, 3.41] | 5.12 [5.04, 5.20] | 3.92 [3.85, 3.98] | 4.89 [4.82, 4.97] | 3.78 [3.72, 3.84] | 4.20 [4.10, 4.31] | 3.69 [3.60, 3.79] | 3.40 [3.30, 3.49] | 2.98 [2.92, 3.04] |
| 5 | 2.98 [2.91, 3.05] | 2.81 [2.76, 2.87] | 4.03 [3.94, 4.12] | 3.18 [3.12, 3.24] | 3.36 [3.28, 3.44] | 5.22 [5.13, 5.29] | 3.90 [3.83, 3.96] | 4.64 [4.56, 4.72] | 3.79 [3.73, 3.85] | 4.21 [4.10, 4.32] | 3.70 [3.60, 3.79] | 3.40 [3.31, 3.49] | 2.98 [2.92, 3.04] |
| 6 | 2.99 [2.93, 3.06] | 2.83 [2.77, 2.88] | 4.19 [4.10, 4.29] | 3.19 [3.13, 3.25] | 3.40 [3.32, 3.48] | 5.17 [5.09, 5.25] | 3.94 [3.88, 4.00] | 5.01 [4.93, 5.09] | 3.82 [3.76, 3.88] | 4.21 [4.09, 4.31] | 3.70 [3.61, 3.79] | 3.40 [3.31, 3.49] | 3.00 [2.94, 3.07] |
| 7 | 3.00 [2.94, 3.07] | 2.84 [2.78, 2.90] | 4.25 [4.15, 4.35] | 3.23 [3.17, 3.29] | 3.43 [3.35, 3.51] | 5.27 [5.19, 5.35] | 4.00 [3.94, 4.07] | 5.12 [5.04, 5.20] | 3.86 [3.80, 3.92] | 4.21 [4.10, 4.32] | 3.71 [3.61, 3.81] |  | 2.99 [2.93, 3.06] |
| 10 | 3.02 [2.95, 3.09] | 2.87 [2.81, 2.93] | 4.34 [4.25, 4.44] | 3.26 [3.19, 3.32] | 3.50 [3.42, 3.58] |  |  |  |  | 4.24 [4.13, 4.34] | 3.71 [3.61, 3.80] |  | 3.03 [2.97, 3.10] |
| 14 | 3.04 [2.97, 3.11] | 2.89 [2.83, 2.95] | 4.44 [4.33, 4.54] | 3.33 [3.27, 3.40] | 3.51 [3.43, 3.60] |  |  |  |  | 4.25 [4.14, 4.36] | 3.73 [3.63, 3.82] |  | 3.03 [2.97, 3.10] |
| 21 | 3.08 [3.01, 3.15] | 2.88 [2.83, 2.94] | 4.58 [4.47, 4.69] | 3.38 [3.32, 3.45] | 3.65 [3.56, 3.74] |  |  |  |  | 4.30 [4.19, 4.41] | 3.81 [3.72, 3.91] |  | 3.06 [3.00, 3.13] |
| 30 | 3.08 [3.01, 3.15] | 2.89 [2.82, 2.95] | 4.70 [4.58, 4.82] | 3.40 [3.34, 3.46] | 3.74 [3.63, 3.85] |  |  |  |  | 4.34 [4.22, 4.46] | 3.86 [3.75, 3.97] |  | 3.07 [3.00, 3.14] |
| 45 | 3.02 [2.95, 3.09] | 2.89 [2.83, 2.95] | 4.68 [4.50, 4.88] | 3.41 [3.34, 3.48] | 3.65 [3.53, 3.79] |  |  |  |  | 4.40 [4.24, 4.56] | 3.88 [3.74, 4.02] |  | 3.08 [3.01, 3.15] |
| 60 | 2.99 [2.92, 3.06] | 2.93 [2.87, 3.00] |  | 3.47 [3.40, 3.54] | 2.27 [1.67, 2.86] |  |  |  |  | 3.39 [2.24, 4.51] | 3.81 [2.47, 5.20] |  | 3.11 [3.03, 3.18] |
| 90 | 2.96 [2.87, 3.06] | 2.87 [2.79, 2.95] |  | 3.45 [3.36, 3.53] |  |  |  |  |  |  |  |  | 3.04 [2.95, 3.12] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.17 [0.12, 0.23] | -0.24 [-0.32, -0.18]* | 0.85 [0.64, 1.08] | 0.20 [0.10, 0.29] | 0.15 [0.04, 0.25] | 1.97 [1.71, 2.24] |  | 1.17 [0.99, 1.33] |  |  |  | 0.85 [0.68, 1.05] | wt_med |
| 1 | 0.16 [0.11, 0.22] | -0.24 [-0.31, -0.18]* | 0.88 [0.66, 1.11] | 0.19 [0.09, 0.27] | 0.19 [0.07, 0.29] | 2.06 [1.80, 2.36] | 0.85 [0.67, 1.03] | 1.65 [1.42, 1.89] | 0.69 [0.53, 0.84] |  |  | 0.83 [0.66, 1.03] | wt_med |
| 2 | 0.16 [0.11, 0.22] | -0.25 [-0.32, -0.19]* | 0.90 [0.68, 1.13] | 0.18 [0.08, 0.27] | 0.21 [0.09, 0.32] | 2.08 [1.81, 2.38] | 0.89 [0.71, 1.07] | 1.69 [1.47, 1.91] | 0.71 [0.55, 0.87] |  |  | 0.83 [0.66, 1.02] | wt_med |
| 3 | 0.15 [0.10, 0.21] | -0.25 [-0.32, -0.19]* | 0.89 [0.66, 1.13] | 0.18 [0.07, 0.27] | 0.20 [0.08, 0.32] | 2.33 [2.04, 2.63] | 0.89 [0.71, 1.08] | 1.85 [1.62, 2.11] | 0.75 [0.58, 0.91] | 1.11 [0.77, 1.49] | 0.58 [0.36, 0.85] | 0.81 [0.65, 1.01] | wt_med |
| 4 | 0.15 [0.10, 0.20] | -0.24 [-0.31, -0.18]* | 0.91 [0.68, 1.15] | 0.17 [0.07, 0.26] | 0.19 [0.06, 0.31] | 2.04 [1.76, 2.33] | 0.87 [0.69, 1.06] | 1.79 [1.57, 2.02] | 0.74 [0.57, 0.90] | 1.10 [0.75, 1.50] | 0.57 [0.35, 0.83] | 0.80 [0.64, 0.99] | wt_med |
| 5 | 0.15 [0.10, 0.20] | -0.24 [-0.31, -0.18]* | 0.92 [0.68, 1.18] | 0.17 [0.07, 0.26] | 0.23 [0.10, 0.35] | 2.13 [1.84, 2.43] | 0.86 [0.68, 1.04] | 1.55 [1.32, 1.79] | 0.75 [0.58, 0.91] | 1.10 [0.76, 1.50] | 0.58 [0.35, 0.83] | 0.79 [0.62, 0.98] | wt_med |
| 6 | 0.14 [0.09, 0.19] | -0.25 [-0.33, -0.19]* | 1.06 [0.77, 1.32] | 0.16 [0.06, 0.25] | 0.24 [0.10, 0.38] | 2.09 [1.80, 2.36] | 0.87 [0.69, 1.05] | 1.88 [1.65, 2.11] | 0.75 [0.59, 0.92] | 1.07 [0.72, 1.47] | 0.54 [0.32, 0.79] | 0.76 [0.59, 0.94] | wt_med |
| 7 | 0.14 [0.10, 0.20] | -0.24 [-0.31, -0.18]* | 1.12 [0.82, 1.41] | 0.25 [0.14, 0.34] | 0.29 [0.15, 0.42] | 2.17 [1.88, 2.48] | 0.92 [0.73, 1.11] | 1.98 [1.73, 2.23] | 0.78 [0.61, 0.96] | 1.09 [0.74, 1.49] | 0.56 [0.35, 0.82] |  | wt_med |
| 10 | 0.13 [0.08, 0.18] | -0.25 [-0.33, -0.19]* | 1.16 [0.85, 1.47] | 0.21 [0.11, 0.31] | 0.30 [0.16, 0.43] |  |  |  |  | 1.05 [0.70, 1.44] | 0.48 [0.26, 0.73] |  | wt_med |
| 14 | 0.13 [0.08, 0.19] | -0.23 [-0.30, -0.17]* | 1.21 [0.89, 1.55] | 0.30 [0.18, 0.41] | 0.29 [0.16, 0.42] |  |  |  |  | 1.04 [0.70, 1.42] | 0.48 [0.26, 0.73] |  | wt_med |
| 21 | 0.13 [0.07, 0.19] | -0.27 [-0.35, -0.20]* | 1.24 [0.87, 1.64] | 0.32 [0.19, 0.45] | 0.32 [0.14, 0.48] |  |  |  |  | 1.00 [0.66, 1.38] | 0.44 [0.20, 0.71] |  | wt_med |
| 30 | 0.11 [0.04, 0.17] | -0.27 [-0.36, -0.20]* | 1.26 [0.84, 1.71] | 0.30 [0.16, 0.45] | 0.27 [0.06, 0.47] |  |  |  |  | 0.97 [0.62, 1.39] | 0.39 [0.11, 0.72] |  | wt_med |
| 45 | 0.01 [-0.08, 0.11] | -0.32 [-0.41, -0.23]* | 0.64 [0.25, 1.00] | 0.26 [0.10, 0.42] | 0.16 [-0.13, 0.40] |  |  |  |  | 0.80 [0.48, 1.20] | 0.16 [-0.14, 0.49] |  | wt_med |
| 60 | -0.08 [-0.22, 0.03] | -0.29 [-0.37, -0.22]* |  | 0.28 [0.12, 0.45] | 0.29 [-0.13, 1.00] |  |  |  |  | 0.84 [-0.60, 1.52] | 0.99 [-0.29, 1.72] |  | wt_med |
| 90 | -0.04 [-0.15, 0.07] | -0.19 [-0.26, -0.11] |  | 0.40 [0.23, 0.57] |  |  |  |  |  |  |  |  | wt_med |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 30.28 [29.61, 30.97] | 28.83 [28.20, 29.46] | 28.61 [27.64, 29.53] | 29.01 [28.35, 29.63] | 27.42 [26.63, 28.21] | 39.27 [38.53, 39.92] | 27.91 [27.27, 28.52] | 36.23 [35.55, 36.86] | 27.98 [27.35, 28.62] |  |  | 34.55 [33.81, 35.24] | 29.35 [28.67, 30.05] |
| 3 | 30.53 [29.88, 31.21] | 29.17 [28.52, 29.81] | 29.14 [28.18, 30.09] | 29.00 [28.33, 29.63] | 27.89 [26.99, 28.70] | 40.04 [39.31, 40.72] | 28.30 [27.65, 28.95] | 37.13 [36.45, 37.81] | 28.12 [27.44, 28.75] | 32.99 [32.21, 33.76] | 32.92 [32.13, 33.77] | 34.51 [33.81, 35.20] | 29.73 [29.08, 30.39] |
| 7 | 30.54 [29.89, 31.21] | 29.32 [28.68, 29.98] | 29.88 [29.02, 30.85] | 29.43 [28.75, 30.07] | 28.60 [27.83, 29.37] | 40.50 [39.83, 41.14] | 28.38 [27.74, 29.00] | 38.66 [38.01, 39.33] | 28.57 [27.95, 29.21] | 33.93 [33.13, 34.74] | 33.18 [32.37, 34.04] |  | 29.97 [29.32, 30.64] |
| 14 | 31.10 [30.41, 31.74] | 29.78 [29.14, 30.42] | 30.90 [30.01, 31.89] | 29.96 [29.29, 30.63] | 29.32 [28.46, 30.18] |  |  |  |  | 34.51 [33.66, 35.28] | 33.76 [32.93, 34.58] |  | 30.72 [30.04, 31.36] |
| 30 | 31.38 [30.68, 32.00] | 30.18 [29.56, 30.75] | 30.81 [29.88, 31.74] | 30.59 [29.93, 31.19] | 29.42 [28.49, 30.34] |  |  |  |  | 34.70 [33.81, 35.60] | 34.32 [33.46, 35.20] |  | 31.17 [30.56, 31.76] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.05 [0.31, 1.86] | -0.53 [-1.10, 0.04] | 0.37 [-0.76, 1.45] | -0.33 [-0.92, 0.33] | -0.35 [-1.26, 0.57] | 10.01 [8.52, 11.68] | -1.37 [-2.08, -0.76]* | 7.01 [5.65, 8.46] | -1.25 [-1.90, -0.65]* |  |  | 5.09 [4.05, 6.06] | wt_med |
| 3 | 0.76 [-0.13, 1.63] | -0.56 [-1.20, 0.08] | 0.16 [-1.06, 1.29] | -0.79 [-1.48, -0.07]* | -0.65 [-1.47, 0.15] | 10.37 [8.80, 12.10] | -1.41 [-2.17, -0.75]* | 7.50 [6.04, 9.08] | -1.56 [-2.31, -0.89]* | 4.10 [2.53, 5.58] | 4.02 [2.47, 5.54] | 4.53 [3.49, 5.52] | wt_med |
| 7 | 0.77 [-0.09, 1.55] | -0.62 [-1.16, -0.12]* | 0.91 [-0.36, 2.26] | -0.35 [-1.07, 0.41] | 0.20 [-0.69, 1.16] | 10.69 [9.13, 12.36] | -1.43 [-2.12, -0.77]* | 8.96 [7.49, 10.61] | -1.20 [-1.87, -0.56]* | 5.34 [3.64, 6.97] | 4.61 [3.09, 6.13] |  | wt_med |
| 14 | 0.59 [-0.18, 1.36] | -0.91 [-1.49, -0.31]* | 1.11 [-0.17, 2.32] | -0.61 [-1.33, 0.18] | 0.20 [-0.57, 1.06] |  |  |  |  | 5.15 [3.24, 6.94] | 4.42 [2.68, 6.23] |  | wt_med |
| 30 | 0.40 [-0.60, 1.32] | -0.94 [-1.45, -0.39]* | -0.12 [-1.72, 1.34] | -0.64 [-1.32, 0.07] | -0.70 [-1.82, 0.41] |  |  |  |  | 4.54 [2.65, 6.10] | 4.14 [2.30, 5.99] |  | wt_med |

**dayPeak pairwise ordering (headliners)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.73 [0.73, 0.74] | 0.76 [0.76, 0.76] | 0.69 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.77 [0.76, 0.77] | 0.78 [0.78, 0.79] |  | 0.80 [0.80, 0.80] |  |  |  | 0.72 [0.72, 0.73] | 0.74 [0.74, 0.75] |
| 1 | 0.73 [0.73, 0.74] | 0.76 [0.76, 0.76] | 0.69 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.77] | 0.78 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.79 [0.78, 0.79] | 0.77 [0.77, 0.78] |  |  | 0.72 [0.72, 0.73] | 0.74 [0.74, 0.74] |
| 2 | 0.73 [0.73, 0.74] | 0.76 [0.75, 0.76] | 0.68 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.77 [0.77, 0.78] | 0.76 [0.76, 0.76] | 0.78 [0.78, 0.79] | 0.77 [0.77, 0.77] |  |  | 0.72 [0.72, 0.73] | 0.74 [0.74, 0.74] |
| 3 | 0.73 [0.73, 0.73] | 0.76 [0.75, 0.76] | 0.68 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.77 [0.77, 0.77] | 0.76 [0.76, 0.76] | 0.78 [0.78, 0.78] | 0.77 [0.76, 0.77] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.75] | 0.72 [0.72, 0.73] | 0.74 [0.73, 0.74] |
| 4 | 0.73 [0.73, 0.73] | 0.76 [0.75, 0.76] | 0.68 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.76 [0.75, 0.76] | 0.77 [0.77, 0.77] | 0.76 [0.75, 0.76] | 0.78 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.70 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.72 [0.72, 0.73] | 0.74 [0.73, 0.74] |
| 5 | 0.73 [0.72, 0.73] | 0.75 [0.75, 0.76] | 0.68 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.75 [0.75, 0.76] | 0.77 [0.76, 0.77] | 0.75 [0.75, 0.76] | 0.78 [0.77, 0.78] | 0.76 [0.76, 0.77] | 0.70 [0.70, 0.71] | 0.74 [0.73, 0.74] | 0.72 [0.72, 0.73] | 0.74 [0.73, 0.74] |
| 6 | 0.73 [0.72, 0.73] | 0.75 [0.75, 0.75] | 0.67 [0.66, 0.67] | 0.76 [0.76, 0.76] | 0.75 [0.75, 0.76] | 0.76 [0.76, 0.77] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.78] | 0.76 [0.76, 0.76] | 0.70 [0.70, 0.70] | 0.73 [0.73, 0.74] | 0.72 [0.72, 0.73] | 0.73 [0.73, 0.74] |
| 7 | 0.72 [0.72, 0.73] | 0.75 [0.75, 0.75] | 0.66 [0.66, 0.67] | 0.75 [0.75, 0.75] | 0.75 [0.74, 0.75] | 0.76 [0.76, 0.76] | 0.75 [0.75, 0.75] | 0.77 [0.76, 0.77] | 0.76 [0.75, 0.76] | 0.69 [0.69, 0.70] | 0.73 [0.73, 0.73] |  | 0.73 [0.73, 0.73] |
| 10 | 0.72 [0.72, 0.73] | 0.74 [0.74, 0.75] | 0.66 [0.65, 0.66] | 0.75 [0.75, 0.75] | 0.74 [0.74, 0.75] |  |  |  |  | 0.69 [0.69, 0.69] | 0.73 [0.72, 0.73] |  | 0.73 [0.73, 0.73] |
| 14 | 0.72 [0.71, 0.72] | 0.74 [0.74, 0.74] | 0.65 [0.65, 0.66] | 0.74 [0.74, 0.74] | 0.74 [0.73, 0.74] |  |  |  |  | 0.68 [0.68, 0.69] | 0.72 [0.71, 0.72] |  | 0.72 [0.72, 0.73] |
| 21 | 0.71 [0.71, 0.72] | 0.73 [0.73, 0.74] | 0.65 [0.65, 0.66] | 0.74 [0.73, 0.74] | 0.73 [0.72, 0.73] |  |  |  |  | 0.67 [0.67, 0.68] | 0.71 [0.70, 0.71] |  | 0.72 [0.71, 0.72] |
| 30 | 0.71 [0.71, 0.71] | 0.72 [0.72, 0.73] | 0.65 [0.65, 0.66] | 0.73 [0.73, 0.73] | 0.73 [0.72, 0.73] |  |  |  |  | 0.67 [0.66, 0.67] | 0.70 [0.70, 0.71] |  | 0.71 [0.71, 0.72] |
| 45 | 0.70 [0.70, 0.71] | 0.71 [0.71, 0.72] | 0.65 [0.64, 0.66] | 0.72 [0.72, 0.72] | 0.72 [0.72, 0.73] |  |  |  |  | 0.66 [0.65, 0.67] | 0.70 [0.69, 0.70] |  | 0.70 [0.70, 0.71] |
| 60 | 0.70 [0.70, 0.71] | 0.71 [0.70, 0.71] |  | 0.71 [0.71, 0.72] | 0.75 [0.67, 0.84] |  |  |  |  | 0.65 [0.58, 0.72] | 0.71 [0.65, 0.77] |  | 0.70 [0.69, 0.70] |
| 90 | 0.71 [0.70, 0.71] | 0.71 [0.71, 0.72] |  | 0.72 [0.72, 0.72] |  |  |  |  |  |  |  |  | 0.70 [0.70, 0.71] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02]* | -0.05 [-0.06, -0.04] | 0.02 [0.01, 0.02]* | 0.03 [0.02, 0.04]* | 0.04 [0.04, 0.05]* |  | 0.06 [0.05, 0.07]* |  |  |  | -0.03 [-0.04, -0.03] | wt_med |
| 1 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02]* | -0.05 [-0.06, -0.04] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | 0.03 [0.02, 0.04]* | 0.05 [0.04, 0.06]* | 0.03 [0.03, 0.04]* |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 2 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.05 [-0.06, -0.04] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | 0.02 [0.02, 0.03]* | 0.05 [0.04, 0.06]* | 0.03 [0.03, 0.04]* |  |  | -0.03 [-0.04, -0.02] | wt_med |
| 3 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.05 [-0.06, -0.04] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | 0.02 [0.02, 0.03]* | 0.05 [0.04, 0.05]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.02] | 0.01 [0.00, 0.02]* | -0.03 [-0.03, -0.02] | wt_med |
| 4 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.05 [-0.06, -0.04] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | 0.04 [0.03, 0.05]* | 0.02 [0.02, 0.03]* | 0.04 [0.04, 0.05]* | 0.03 [0.02, 0.04]* | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.02]* | -0.03 [-0.03, -0.02] | wt_med |
| 5 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.05 [-0.07, -0.04] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.05]* | 0.02 [0.02, 0.03]* | 0.05 [0.04, 0.05]* | 0.03 [0.02, 0.04]* | -0.03 [-0.04, -0.02] | 0.01 [0.00, 0.02]* | -0.03 [-0.03, -0.02] | wt_med |
| 6 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02]* | -0.06 [-0.08, -0.05] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.03]* | 0.03 [0.03, 0.04]* | 0.02 [0.01, 0.03]* | 0.04 [0.04, 0.05]* | 0.03 [0.02, 0.04]* | -0.03 [-0.04, -0.02] | 0.01 [-0.00, 0.01] | -0.02 [-0.03, -0.02] | wt_med |
| 7 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.02]* | -0.07 [-0.08, -0.05] | 0.02 [0.01, 0.03]* | 0.02 [0.02, 0.03]* | 0.04 [0.03, 0.05]* | 0.02 [0.02, 0.03]* | 0.04 [0.03, 0.05]* | 0.03 [0.02, 0.04]* | -0.03 [-0.04, -0.02] | 0.00 [-0.00, 0.01] |  | wt_med |
| 10 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02]* | -0.07 [-0.08, -0.05] | 0.02 [0.02, 0.03]* | 0.02 [0.01, 0.03]* |  |  |  |  | -0.03 [-0.04, -0.02] | 0.00 [-0.00, 0.01] |  | wt_med |
| 14 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.06 [-0.08, -0.04] | 0.02 [0.01, 0.03]* | 0.02 [0.01, 0.03]* |  |  |  |  | -0.03 [-0.04, -0.02] | 0.00 [-0.00, 0.01] |  | wt_med |
| 21 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | -0.05 [-0.07, -0.04] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* |  |  |  |  | -0.03 [-0.04, -0.02] | 0.01 [-0.00, 0.02] |  | wt_med |
| 30 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.02]* | -0.05 [-0.07, -0.03] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* |  |  |  |  | -0.03 [-0.04, -0.02] | 0.00 [-0.01, 0.01] |  | wt_med |
| 45 | -0.01 [-0.01, 0.00] | 0.02 [0.02, 0.03]* | -0.05 [-0.08, -0.02] | 0.02 [0.01, 0.03]* | 0.03 [0.03, 0.04]* |  |  |  |  | -0.03 [-0.04, -0.01] | 0.01 [-0.00, 0.02] |  | wt_med |
| 60 | -0.01 [-0.01, 0.00] | 0.02 [0.02, 0.02]* |  | 0.02 [0.01, 0.03]* | 0.05 [-0.02, 0.23] |  |  |  |  | -0.05 [-0.12, 0.13] | 0.01 [-0.08, 0.23] |  | wt_med |
| 90 | -0.01 [-0.02, -0.00] | 0.02 [0.02, 0.02]* |  | 0.02 [0.01, 0.03]* |  |  |  |  |  |  |  |  | wt_med |

## D5 first-hour MAE vs EACH naive candidate

vs `snaive7`:

| lead | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.858 [-1.042, -0.683]* / [-0.925, -0.796]* | -0.468 [-0.633, -0.275]* / [-0.543, -0.399]* | -0.930 [-1.150, -0.739]* / [-0.997, -0.868]* | -0.929 [-1.150, -0.738]* / [-0.996, -0.868]* | -0.780 [-0.987, -0.607]* / [-0.846, -0.717]* | -0.749 [-0.973, -0.558]* / [-0.825, -0.680]* |  |  |
| 1 | -0.787 [-0.966, -0.614]* / [-0.855, -0.725]* | -0.455 [-0.620, -0.263]* / [-0.531, -0.386]* | -0.864 [-1.079, -0.673]* / [-0.934, -0.802]* | -0.865 [-1.079, -0.673]* / [-0.935, -0.804]* | -0.725 [-0.933, -0.558]* / [-0.789, -0.663]* | -0.689 [-0.916, -0.497]* / [-0.760, -0.619]* |  |  |
| 2 | -0.760 [-0.938, -0.588]* / [-0.828, -0.697]* | -0.445 [-0.611, -0.253]* / [-0.519, -0.375]* | -0.838 [-1.050, -0.648]* / [-0.908, -0.777]* | -0.839 [-1.051, -0.649]* / [-0.908, -0.778]* | -0.707 [-0.914, -0.539]* / [-0.770, -0.644]* | -0.662 [-0.881, -0.470]* / [-0.737, -0.591]* |  |  |
| 3 | -0.739 [-0.916, -0.565]* / [-0.807, -0.678]* | -0.433 [-0.601, -0.234]* / [-0.509, -0.363]* | -0.818 [-1.029, -0.628]* / [-0.890, -0.756]* | -0.820 [-1.032, -0.628]* / [-0.892, -0.757]* | -0.685 [-0.890, -0.517]* / [-0.756, -0.622]* | -0.645 [-0.859, -0.451]* / [-0.722, -0.573]* | 1.054 [0.793, 1.373] / [0.971, 1.136] | 1.198 [0.939, 1.504] / [1.113, 1.279] |
| 4 | -0.706 [-0.881, -0.533]* / [-0.776, -0.645]* | -0.403 [-0.570, -0.200]* / [-0.478, -0.331]* | -0.787 [-0.991, -0.601]* / [-0.857, -0.727]* | -0.788 [-0.995, -0.601]* / [-0.859, -0.728]* | -0.660 [-0.859, -0.487]* / [-0.728, -0.600]* | -0.613 [-0.829, -0.415]* / [-0.691, -0.542]* | 1.057 [0.798, 1.363] / [0.970, 1.141] | 1.196 [0.945, 1.483] / [1.105, 1.277] |
| 5 | -0.678 [-0.847, -0.507]* / [-0.746, -0.615]* | -0.380 [-0.552, -0.172]* / [-0.454, -0.308]* | -0.760 [-0.961, -0.576]* / [-0.830, -0.698]* | -0.762 [-0.962, -0.580]* / [-0.830, -0.698]* | -0.639 [-0.836, -0.463]* / [-0.706, -0.578]* | -0.598 [-0.820, -0.391]* / [-0.673, -0.526]* | 1.112 [0.842, 1.427] / [1.031, 1.192] | 1.257 [0.990, 1.563] / [1.176, 1.337] |
| 6 | -0.634 [-0.804, -0.464]* / [-0.705, -0.570]* | -0.362 [-0.528, -0.154]* / [-0.442, -0.289]* | -0.716 [-0.906, -0.534]* / [-0.786, -0.653]* | -0.718 [-0.908, -0.537]* / [-0.788, -0.653]* | -0.609 [-0.810, -0.419]* / [-0.679, -0.541]* | -0.557 [-0.777, -0.343]* / [-0.638, -0.486]* | 1.079 [0.816, 1.390] / [0.999, 1.159] | 1.213 [0.957, 1.511] / [1.131, 1.295] |

vs `wt_med`:

| lead | snaive7 | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.858 [0.683, 1.042] / [0.796, 0.925] | 0.367 [0.278, 0.463] / [0.342, 0.391] | -0.070 [-0.126, -0.026]* / [-0.087, -0.055]* | -0.070 [-0.127, -0.026]* / [-0.087, -0.054]* | -0.068 [-0.156, -0.003]* / [-0.090, -0.049]* | -0.051 [-0.134, 0.006] / [-0.075, -0.029]*  ! |  |  |
| 1 | 0.787 [0.614, 0.966] / [0.725, 0.855] | 0.316 [0.239, 0.402] / [0.291, 0.339] | -0.078 [-0.140, -0.028]* / [-0.093, -0.064]* | -0.078 [-0.141, -0.028]* / [-0.094, -0.065]* | -0.071 [-0.165, -0.002]* / [-0.093, -0.052]* | -0.057 [-0.145, 0.003] / [-0.081, -0.033]*  ! |  |  |
| 2 | 0.760 [0.588, 0.938] / [0.697, 0.828] | 0.300 [0.224, 0.385] / [0.276, 0.325] | -0.082 [-0.144, -0.032]* / [-0.098, -0.067]* | -0.082 [-0.145, -0.033]* / [-0.098, -0.068]* | -0.073 [-0.165, -0.005]* / [-0.095, -0.052]* | -0.063 [-0.147, -0.004]* / [-0.090, -0.040]* |  |  |
| 3 | 0.739 [0.565, 0.916] / [0.678, 0.807] | 0.292 [0.215, 0.378] / [0.267, 0.318] | -0.084 [-0.145, -0.036]* / [-0.100, -0.069]* | -0.085 [-0.146, -0.035]* / [-0.101, -0.070]* | -0.076 [-0.167, -0.010]* / [-0.099, -0.057]* | -0.068 [-0.152, -0.006]* / [-0.093, -0.044]* | 1.709 [1.467, 1.977] / [1.640, 1.776] | 1.853 [1.599, 2.117] / [1.782, 1.922] |
| 4 | 0.706 [0.533, 0.881] / [0.645, 0.776] | 0.290 [0.209, 0.378] / [0.266, 0.315] | -0.087 [-0.149, -0.037]* / [-0.102, -0.072]* | -0.088 [-0.150, -0.037]* / [-0.104, -0.072]* | -0.079 [-0.171, -0.012]* / [-0.100, -0.060]* | -0.070 [-0.156, -0.008]* / [-0.094, -0.047]* | 1.694 [1.457, 1.955] / [1.621, 1.759] | 1.837 [1.588, 2.087] / [1.764, 1.901] |
| 5 | 0.678 [0.507, 0.847] / [0.615, 0.746] | 0.283 [0.199, 0.372] / [0.258, 0.308] | -0.089 [-0.152, -0.038]* / [-0.105, -0.074]* | -0.090 [-0.152, -0.038]* / [-0.106, -0.075]* | -0.083 [-0.179, -0.014]* / [-0.105, -0.063]* | -0.075 [-0.167, -0.008]* / [-0.101, -0.051]* | 1.720 [1.472, 1.992] / [1.654, 1.782] | 1.868 [1.606, 2.121] / [1.800, 1.936] |
| 6 | 0.634 [0.464, 0.804] / [0.570, 0.705] | 0.257 [0.174, 0.344] / [0.232, 0.282] | -0.088 [-0.151, -0.038]* / [-0.105, -0.073]* | -0.090 [-0.150, -0.039]* / [-0.106, -0.074]* | -0.079 [-0.173, -0.011]* / [-0.100, -0.058]* | -0.077 [-0.172, -0.007]* / [-0.102, -0.051]* | 1.660 [1.417, 1.921] / [1.595, 1.726] | 1.798 [1.548, 2.044] / [1.731, 1.865] |
| 7 |  | 0.215 [0.137, 0.300] / [0.191, 0.241] | -0.093 [-0.155, -0.041]* / [-0.108, -0.078]* | -0.092 [-0.155, -0.041]* / [-0.107, -0.077]* | -0.078 [-0.165, -0.014]* / [-0.098, -0.057]* | -0.075 [-0.172, -0.007]* / [-0.100, -0.051]* | 1.671 [1.430, 1.919] / [1.600, 1.740] | 1.814 [1.558, 2.058] / [1.736, 1.888] |
| 10 |  | 0.183 [0.111, 0.257] / [0.157, 0.207] | -0.096 [-0.162, -0.043]* / [-0.113, -0.081]* | -0.095 [-0.161, -0.043]* / [-0.112, -0.081]* | -0.080 [-0.168, -0.012]* / [-0.102, -0.058]* | -0.084 [-0.196, -0.005]* / [-0.110, -0.059]* | 1.672 [1.412, 1.948] / [1.594, 1.744] | 1.818 [1.550, 2.100] / [1.736, 1.894] |
| 14 |  | 0.170 [0.094, 0.245] / [0.145, 0.196] | -0.094 [-0.157, -0.042]* / [-0.109, -0.080]* | -0.092 [-0.156, -0.040]* / [-0.107, -0.078]* | -0.080 [-0.167, -0.014]* / [-0.102, -0.059]* | -0.076 [-0.184, 0.001] / [-0.103, -0.051]*  ! | 1.661 [1.400, 1.936] / [1.582, 1.730] | 1.808 [1.527, 2.098] / [1.726, 1.883] |
| 21 |  | 0.156 [0.081, 0.233] / [0.132, 0.183] | -0.093 [-0.156, -0.040]* / [-0.107, -0.079]* | -0.086 [-0.151, -0.034]* / [-0.101, -0.072]* | -0.068 [-0.167, -0.001]* / [-0.088, -0.047]* | -0.076 [-0.205, 0.002] / [-0.104, -0.050]*  ! | 1.599 [1.341, 1.879] / [1.524, 1.673] | 1.743 [1.459, 2.036] / [1.664, 1.824] |
| 30 |  | 0.115 [0.028, 0.194] / [0.087, 0.142] | -0.099 [-0.170, -0.039]* / [-0.114, -0.085]* | -0.088 [-0.158, -0.031]* / [-0.102, -0.073]* | -0.067 [-0.174, 0.013] / [-0.090, -0.043]*  ! | -0.085 [-0.237, 0.011] / [-0.113, -0.055]*  ! | 1.626 [1.346, 1.897] / [1.541, 1.710] | 1.738 [1.464, 2.028] / [1.644, 1.826] |
| 45 |  | 0.016 [-0.106, 0.135] / [-0.014, 0.046] | -0.118 [-0.203, -0.049]* / [-0.136, -0.101]* | -0.100 [-0.182, -0.031]* / [-0.118, -0.083]* | -0.126 [-0.258, -0.018]* / [-0.171, -0.088]* | -0.127 [-0.280, 0.004] / [-0.203, -0.060]*  ! | 1.704 [1.434, 1.983] / [1.598, 1.815] | 1.837 [1.515, 2.159] / [1.718, 1.954] |
| 60 |  | -0.051 [-0.209, 0.111] / [-0.083, -0.020]*  ! | -0.117 [-0.200, -0.050]* / [-0.135, -0.100]* | -0.092 [-0.172, -0.023]* / [-0.112, -0.073]* | 0.000 [0.000, 0.000] / [0.000, 0.000] |  | 0.489 [0.170, 1.980] / [0.284, 0.716] | 0.553 [0.234, 2.151] / [0.339, 0.801] |
| 90 |  | 0.022 [-0.155, 0.224] / [-0.015, 0.058] | -0.087 [-0.146, -0.039]* / [-0.103, -0.072]* | -0.042 [-0.106, 0.016] / [-0.061, -0.022]*  ! |  |  |  |  |

vs `clim`:

| lead | snaive7 | wt_med | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.468 [0.275, 0.633] / [0.399, 0.543] | -0.367 [-0.463, -0.278]* / [-0.391, -0.342]* | -0.447 [-0.571, -0.338]* / [-0.474, -0.421]* | -0.447 [-0.572, -0.337]* / [-0.474, -0.421]* | -0.411 [-0.561, -0.292]* / [-0.449, -0.379]* | -0.453 [-0.630, -0.313]* / [-0.497, -0.414]* |  |  |
| 1 | 0.455 [0.263, 0.620] / [0.386, 0.531] | -0.316 [-0.402, -0.239]* / [-0.339, -0.291]* | -0.401 [-0.520, -0.298]* / [-0.427, -0.375]* | -0.402 [-0.521, -0.299]* / [-0.428, -0.375]* | -0.382 [-0.534, -0.263]* / [-0.417, -0.348]* | -0.422 [-0.596, -0.284]* / [-0.468, -0.386]* |  |  |
| 2 | 0.445 [0.253, 0.611] / [0.375, 0.519] | -0.300 [-0.385, -0.224]* / [-0.325, -0.276]* | -0.390 [-0.508, -0.288]* / [-0.417, -0.364]* | -0.390 [-0.508, -0.288]* / [-0.418, -0.364]* | -0.387 [-0.536, -0.269]* / [-0.423, -0.352]* | -0.429 [-0.601, -0.288]* / [-0.473, -0.393]* |  |  |
| 3 | 0.433 [0.234, 0.601] / [0.363, 0.509] | -0.292 [-0.378, -0.215]* / [-0.318, -0.267]* | -0.381 [-0.496, -0.282]* / [-0.410, -0.356]* | -0.382 [-0.498, -0.282]* / [-0.411, -0.358]* | -0.395 [-0.538, -0.279]* / [-0.428, -0.361]* | -0.433 [-0.598, -0.294]* / [-0.473, -0.397]* | 1.370 [1.150, 1.604] / [1.306, 1.435] | 1.517 [1.283, 1.746] / [1.447, 1.589] |
| 4 | 0.403 [0.200, 0.570] / [0.331, 0.478] | -0.290 [-0.378, -0.209]* / [-0.315, -0.266]* | -0.381 [-0.499, -0.281]* / [-0.408, -0.354]* | -0.382 [-0.500, -0.281]* / [-0.409, -0.355]* | -0.407 [-0.552, -0.286]* / [-0.440, -0.373]* | -0.442 [-0.615, -0.301]* / [-0.483, -0.405]* | 1.350 [1.130, 1.587] / [1.283, 1.417] | 1.496 [1.267, 1.727] / [1.426, 1.563] |
| 5 | 0.380 [0.172, 0.552] / [0.308, 0.454] | -0.283 [-0.372, -0.199]* / [-0.308, -0.258]* | -0.376 [-0.495, -0.272]* / [-0.404, -0.349]* | -0.376 [-0.493, -0.272]* / [-0.405, -0.348]* | -0.424 [-0.577, -0.297]* / [-0.458, -0.389]* | -0.461 [-0.638, -0.312]* / [-0.505, -0.424]* | 1.358 [1.120, 1.590] / [1.294, 1.423] | 1.509 [1.273, 1.759] / [1.439, 1.577] |
| 6 | 0.362 [0.154, 0.528] / [0.289, 0.442] | -0.257 [-0.344, -0.174]* / [-0.282, -0.232]* | -0.350 [-0.466, -0.247]* / [-0.379, -0.323]* | -0.350 [-0.467, -0.246]* / [-0.378, -0.322]* | -0.410 [-0.563, -0.280]* / [-0.444, -0.376]* | -0.446 [-0.624, -0.300]* / [-0.489, -0.407]* | 1.308 [1.087, 1.539] / [1.241, 1.376] | 1.448 [1.211, 1.684] / [1.376, 1.516] |
| 7 |  | -0.215 [-0.300, -0.137]* / [-0.241, -0.191]* | -0.311 [-0.423, -0.213]* / [-0.339, -0.285]* | -0.311 [-0.423, -0.213]* / [-0.338, -0.284]* | -0.380 [-0.526, -0.255]* / [-0.413, -0.346]* | -0.424 [-0.599, -0.282]* / [-0.467, -0.385]* | 1.353 [1.125, 1.583] / [1.284, 1.421] | 1.498 [1.253, 1.745] / [1.421, 1.570] |
| 10 |  | -0.183 [-0.257, -0.111]* / [-0.207, -0.157]* | -0.278 [-0.390, -0.186]* / [-0.305, -0.250]* | -0.277 [-0.389, -0.183]* / [-0.303, -0.249]* | -0.359 [-0.504, -0.233]* / [-0.393, -0.326]* | -0.409 [-0.586, -0.270]* / [-0.449, -0.371]* | 1.376 [1.129, 1.638] / [1.299, 1.448] | 1.522 [1.255, 1.811] / [1.439, 1.598] |
| 14 |  | -0.170 [-0.245, -0.094]* / [-0.196, -0.145]* | -0.259 [-0.372, -0.164]* / [-0.287, -0.233]* | -0.257 [-0.369, -0.161]* / [-0.285, -0.231]* | -0.340 [-0.483, -0.213]* / [-0.372, -0.306]* | -0.380 [-0.546, -0.249]* / [-0.417, -0.342]* | 1.387 [1.130, 1.667] / [1.307, 1.461] | 1.536 [1.259, 1.838] / [1.448, 1.614] |
| 21 |  | -0.156 [-0.233, -0.081]* / [-0.183, -0.132]* | -0.243 [-0.354, -0.148]* / [-0.271, -0.216]* | -0.236 [-0.348, -0.140]* / [-0.264, -0.209]* | -0.277 [-0.406, -0.158]* / [-0.309, -0.245]* | -0.313 [-0.455, -0.201]* / [-0.351, -0.273]* | 1.375 [1.105, 1.645] / [1.292, 1.452] | 1.519 [1.225, 1.819] / [1.431, 1.603] |
| 30 |  | -0.115 [-0.194, -0.028]* / [-0.142, -0.087]* | -0.212 [-0.333, -0.113]* / [-0.242, -0.182]* | -0.200 [-0.319, -0.098]* / [-0.230, -0.171]* | -0.227 [-0.345, -0.121]* / [-0.262, -0.190]* | -0.252 [-0.395, -0.132]* / [-0.291, -0.212]* | 1.472 [1.213, 1.715] / [1.382, 1.563] | 1.584 [1.291, 1.857] / [1.487, 1.682] |
| 45 |  | -0.016 [-0.135, 0.106] / [-0.046, 0.014] | -0.135 [-0.290, -0.004]* / [-0.168, -0.103]* | -0.116 [-0.269, 0.018] / [-0.150, -0.083]*  ! | -0.135 [-0.315, 0.037] / [-0.200, -0.078]*  ! | -0.549 [-0.955, -0.200]* / [-0.660, -0.445]* | 1.680 [1.416, 1.960] / [1.561, 1.795] | 1.811 [1.515, 2.126] / [1.688, 1.933] |
| 60 |  | 0.051 [-0.111, 0.209] / [0.020, 0.083] | -0.074 [-0.265, 0.095] / [-0.109, -0.041]*  ! | -0.047 [-0.240, 0.126] / [-0.085, -0.013]*  ! | 0.101 [-0.194, 0.922] / [0.023, 0.200] |  | 0.586 [0.194, 2.831] / [0.350, 0.871] | 0.649 [0.257, 2.914] / [0.406, 0.950] |
| 90 |  | -0.022 [-0.224, 0.155] / [-0.058, 0.015] | -0.110 [-0.319, 0.076] / [-0.147, -0.071]*  ! | -0.060 [-0.284, 0.133] / [-0.099, -0.018]*  ! |  |  |  |  |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.51 [4.44, 4.60] | 4.07 [4.00, 4.15] | 3.67 [3.57, 3.77] | 4.07 [4.00, 4.15] | 3.63 [3.55, 3.71] | 7.32 [7.21, 7.43] |  | 5.85 [5.76, 5.93] |  | 4.07 [4.00, 4.15] | 4.39 [4.31, 4.47] |  |  | 5.08 [4.98, 5.19] | 4.14 [4.07, 4.22] |
| 1 | 4.52 [4.44, 4.60] | 4.14 [4.06, 4.22] | 3.74 [3.64, 3.85] | 4.14 [4.06, 4.22] | 3.67 [3.59, 3.76] | 7.43 [7.32, 7.54] | 4.13 [4.05, 4.20] | 6.64 [6.54, 6.74] | 4.13 [4.05, 4.21] | 4.13 [4.06, 4.21] | 4.38 [4.29, 4.46] |  |  | 5.07 [4.96, 5.17] | 4.21 [4.14, 4.29] |
| 2 | 4.52 [4.44, 4.60] | 4.16 [4.09, 4.24] | 3.76 [3.66, 3.87] | 4.16 [4.08, 4.24] | 3.69 [3.61, 3.78] | 7.45 [7.34, 7.57] | 4.14 [4.07, 4.22] | 6.78 [6.68, 6.88] | 4.14 [4.07, 4.22] | 4.15 [4.08, 4.24] | 4.36 [4.28, 4.45] |  |  | 5.06 [4.96, 5.17] | 4.24 [4.16, 4.32] |
| 3 | 4.53 [4.45, 4.62] | 4.18 [4.10, 4.26] | 3.80 [3.70, 3.91] | 4.18 [4.10, 4.26] | 3.72 [3.64, 3.81] | 7.58 [7.46, 7.70] | 4.16 [4.08, 4.24] | 6.81 [6.71, 6.91] | 4.16 [4.08, 4.24] | 4.18 [4.10, 4.25] | 4.36 [4.28, 4.44] | 5.52 [5.41, 5.64] | 5.66 [5.56, 5.78] | 5.07 [4.97, 5.18] | 4.26 [4.18, 4.34] |
| 4 | 4.54 [4.46, 4.63] | 4.19 [4.12, 4.27] | 3.84 [3.74, 3.94] | 4.19 [4.11, 4.27] | 3.74 [3.66, 3.83] | 7.49 [7.37, 7.62] | 4.17 [4.09, 4.25] | 6.85 [6.75, 6.95] | 4.17 [4.09, 4.25] | 4.19 [4.11, 4.27] | 4.36 [4.27, 4.44] | 5.53 [5.42, 5.65] | 5.67 [5.57, 5.78] | 5.06 [4.96, 5.16] | 4.28 [4.20, 4.36] |
| 5 | 4.55 [4.47, 4.64] | 4.21 [4.13, 4.29] | 3.87 [3.77, 3.98] | 4.21 [4.13, 4.29] | 3.76 [3.68, 3.85] | 7.53 [7.39, 7.69] | 4.18 [4.11, 4.26] | 6.75 [6.65, 6.85] | 4.18 [4.11, 4.26] | 4.21 [4.13, 4.29] | 4.35 [4.27, 4.44] | 5.57 [5.46, 5.68] | 5.71 [5.60, 5.83] | 5.05 [4.95, 5.16] | 4.30 [4.22, 4.38] |
| 6 | 4.56 [4.48, 4.65] | 4.25 [4.17, 4.33] | 3.93 [3.83, 4.04] | 4.24 [4.17, 4.32] | 3.80 [3.71, 3.89] | 7.55 [7.44, 7.66] | 4.21 [4.14, 4.29] | 6.93 [6.83, 7.03] | 4.21 [4.14, 4.29] | 4.24 [4.17, 4.32] | 4.35 [4.27, 4.43] | 5.54 [5.43, 5.65] | 5.67 [5.57, 5.78] | 5.04 [4.94, 5.15] | 4.33 [4.26, 4.41] |
| 7 | 4.58 [4.49, 4.66] | 4.31 [4.23, 4.39] | 3.98 [3.88, 4.10] | 4.30 [4.22, 4.38] | 3.85 [3.76, 3.95] | 7.60 [7.49, 7.72] | 4.27 [4.19, 4.35] | 7.18 [7.07, 7.28] | 4.27 [4.19, 4.35] | 4.30 [4.22, 4.38] | 4.64 [4.56, 4.73] | 5.60 [5.49, 5.71] | 5.74 [5.63, 5.85] |  | 4.40 [4.32, 4.48] |
| 10 | 4.60 [4.52, 4.68] | 4.35 [4.28, 4.44] | 4.09 [3.99, 4.20] | 4.35 [4.27, 4.43] | 3.93 [3.85, 4.03] |  |  |  |  | 4.35 [4.27, 4.43] | 4.63 [4.54, 4.71] | 5.67 [5.56, 5.79] | 5.82 [5.70, 5.93] |  | 4.45 [4.37, 4.54] |
| 14 | 4.66 [4.58, 4.75] | 4.43 [4.35, 4.51] | 4.21 [4.11, 4.33] | 4.42 [4.34, 4.50] | 4.04 [3.95, 4.14] |  |  |  |  | 4.42 [4.34, 4.50] | 4.81 [4.72, 4.89] | 5.77 [5.65, 5.88] | 5.91 [5.80, 6.02] |  | 4.53 [4.44, 4.61] |
| 21 | 4.74 [4.65, 4.83] | 4.50 [4.42, 4.59] | 4.42 [4.31, 4.55] | 4.50 [4.42, 4.59] | 4.22 [4.12, 4.32] |  |  |  |  | 4.49 [4.41, 4.58] | 4.97 [4.88, 5.06] | 5.88 [5.76, 5.99] | 6.02 [5.90, 6.14] |  | 4.60 [4.52, 4.68] |
| 30 | 4.79 [4.70, 4.88] | 4.55 [4.46, 4.64] | 4.59 [4.47, 4.73] | 4.55 [4.47, 4.64] | 4.44 [4.31, 4.57] |  |  |  |  | 4.54 [4.45, 4.62] | 5.08 [4.99, 5.17] | 6.18 [6.05, 6.32] | 6.29 [6.16, 6.43] |  | 4.66 [4.57, 4.75] |
| 45 | 4.83 [4.74, 4.92] | 4.63 [4.54, 4.71] | 4.42 [4.22, 4.61] | 4.64 [4.56, 4.72] | 4.69 [4.54, 4.84] |  |  |  |  | 4.60 [4.51, 4.68] | 5.33 [5.24, 5.43] | 6.56 [6.40, 6.73] | 6.69 [6.53, 6.85] |  | 4.77 [4.68, 4.85] |
| 60 | 4.87 [4.78, 4.97] | 4.76 [4.68, 4.85] |  | 4.78 [4.70, 4.87] | 1.35 [0.98, 1.82] |  |  |  |  | 4.73 [4.65, 4.82] | 5.58 [5.49, 5.68] | 1.91 [1.39, 2.50] | 1.97 [1.44, 2.60] |  | 4.88 [4.80, 4.98] |
| 90 | 5.04 [4.94, 5.15] | 4.90 [4.81, 5.00] |  | 4.95 [4.85, 5.05] |  |  |  |  |  | 4.89 [4.80, 4.99] | 5.59 [5.49, 5.70] |  |  |  | 4.99 [4.89, 5.09] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.37 [0.28, 0.46] | -0.07 [-0.13, -0.03]* | -0.05 [-0.13, 0.01] | -0.07 [-0.13, -0.03]* | -0.07 [-0.16, -0.00]* | 3.18 [2.59, 3.82] |  | 1.70 [1.40, 2.07] |  | -0.08 [-0.13, -0.03]* | 0.24 [-0.09, 0.56] |  |  | 0.86 [0.68, 1.04] | wt_med |
| 1 | 0.32 [0.24, 0.40] | -0.08 [-0.14, -0.03]* | -0.06 [-0.14, 0.00] | -0.08 [-0.14, -0.03]* | -0.07 [-0.17, -0.00]* | 3.22 [2.64, 3.91] | -0.08 [-0.14, -0.03]* | 2.44 [1.99, 2.98] | -0.08 [-0.14, -0.03]* | -0.09 [-0.15, -0.03]* | 0.16 [-0.17, 0.49] |  |  | 0.79 [0.61, 0.97] | wt_med |
| 2 | 0.30 [0.22, 0.39] | -0.08 [-0.14, -0.03]* | -0.06 [-0.15, -0.00]* | -0.08 [-0.14, -0.03]* | -0.07 [-0.17, -0.01]* | 3.23 [2.65, 3.91] | -0.08 [-0.15, -0.03]* | 2.55 [2.06, 3.12] | -0.08 [-0.15, -0.03]* | -0.09 [-0.16, -0.04]* | 0.13 [-0.20, 0.45] |  |  | 0.76 [0.59, 0.94] | wt_med |
| 3 | 0.29 [0.22, 0.38] | -0.08 [-0.15, -0.04]* | -0.07 [-0.15, -0.01]* | -0.08 [-0.15, -0.03]* | -0.08 [-0.17, -0.01]* | 3.33 [2.75, 4.03] | -0.09 [-0.15, -0.04]* | 2.57 [2.06, 3.17] | -0.09 [-0.15, -0.04]* | -0.09 [-0.16, -0.04]* | 0.10 [-0.24, 0.43] | 1.71 [1.47, 1.98] | 1.85 [1.60, 2.12] | 0.74 [0.56, 0.92] | wt_med |
| 4 | 0.29 [0.21, 0.38] | -0.09 [-0.15, -0.04]* | -0.07 [-0.16, -0.01]* | -0.09 [-0.15, -0.04]* | -0.08 [-0.17, -0.01]* | 3.23 [2.65, 3.89] | -0.09 [-0.15, -0.04]* | 2.60 [2.08, 3.21] | -0.09 [-0.15, -0.04]* | -0.10 [-0.16, -0.04]* | 0.08 [-0.26, 0.41] | 1.69 [1.46, 1.95] | 1.84 [1.59, 2.09] | 0.71 [0.53, 0.88] | wt_med |
| 5 | 0.28 [0.20, 0.37] | -0.09 [-0.15, -0.04]* | -0.08 [-0.17, -0.01]* | -0.09 [-0.15, -0.04]* | -0.08 [-0.18, -0.01]* | 3.25 [2.69, 3.90] | -0.09 [-0.16, -0.04]* | 2.48 [1.97, 3.06] | -0.09 [-0.16, -0.04]* | -0.10 [-0.16, -0.04]* | 0.05 [-0.29, 0.39] | 1.72 [1.47, 1.99] | 1.87 [1.61, 2.12] | 0.68 [0.51, 0.85] | wt_med |
| 6 | 0.26 [0.17, 0.34] | -0.09 [-0.15, -0.04]* | -0.08 [-0.17, -0.01]* | -0.09 [-0.15, -0.04]* | -0.08 [-0.17, -0.01]* | 3.24 [2.68, 3.90] | -0.09 [-0.16, -0.04]* | 2.64 [2.15, 3.19] | -0.09 [-0.16, -0.04]* | -0.10 [-0.16, -0.04]* | 0.01 [-0.34, 0.36] | 1.66 [1.42, 1.92] | 1.80 [1.55, 2.04] | 0.63 [0.46, 0.80] | wt_med |
| 7 | 0.22 [0.14, 0.30] | -0.09 [-0.16, -0.04]* | -0.08 [-0.17, -0.01]* | -0.09 [-0.15, -0.04]* | -0.08 [-0.17, -0.01]* | 3.23 [2.66, 3.90] | -0.10 [-0.16, -0.04]* | 2.82 [2.28, 3.46] | -0.10 [-0.16, -0.04]* | -0.11 [-0.17, -0.05]* | 0.28 [-0.07, 0.62] | 1.67 [1.43, 1.92] | 1.81 [1.56, 2.06] |  | wt_med |
| 10 | 0.18 [0.11, 0.26] | -0.10 [-0.16, -0.04]* | -0.08 [-0.20, -0.01]* | -0.10 [-0.16, -0.04]* | -0.08 [-0.17, -0.01]* |  |  |  |  | -0.11 [-0.18, -0.05]* | 0.19 [-0.18, 0.54] | 1.67 [1.41, 1.95] | 1.82 [1.55, 2.10] |  | wt_med |
| 14 | 0.17 [0.09, 0.25] | -0.09 [-0.16, -0.04]* | -0.08 [-0.18, 0.00] | -0.09 [-0.16, -0.04]* | -0.08 [-0.17, -0.01]* |  |  |  |  | -0.11 [-0.18, -0.05]* | 0.31 [-0.08, 0.68] | 1.66 [1.40, 1.94] | 1.81 [1.53, 2.10] |  | wt_med |
| 21 | 0.16 [0.08, 0.23] | -0.09 [-0.16, -0.04]* | -0.08 [-0.21, 0.00] | -0.09 [-0.15, -0.03]* | -0.07 [-0.17, -0.00]* |  |  |  |  | -0.11 [-0.18, -0.05]* | 0.36 [-0.02, 0.73] | 1.60 [1.34, 1.88] | 1.74 [1.46, 2.04] |  | wt_med |
| 30 | 0.12 [0.03, 0.19] | -0.10 [-0.17, -0.04]* | -0.09 [-0.24, 0.01] | -0.09 [-0.16, -0.03]* | -0.07 [-0.17, 0.01] |  |  |  |  | -0.12 [-0.19, -0.05]* | 0.39 [0.01, 0.76] | 1.63 [1.35, 1.90] | 1.74 [1.46, 2.03] |  | wt_med |
| 45 | 0.02 [-0.11, 0.14] | -0.12 [-0.20, -0.05]* | -0.13 [-0.28, 0.00] | -0.10 [-0.18, -0.03] | -0.13 [-0.26, -0.02] |  |  |  |  | -0.15 [-0.24, -0.07]* | 0.55 [0.16, 0.94] | 1.70 [1.43, 1.98] | 1.84 [1.51, 2.16] |  | wt_med |
| 60 | -0.05 [-0.21, 0.11] | -0.12 [-0.20, -0.05] |  | -0.09 [-0.17, -0.02] | 0.00 [0.00, 0.00] |  |  |  |  | -0.15 [-0.25, -0.08] | 0.67 [0.24, 1.09] | 0.49 [0.17, 1.98] | 0.55 [0.23, 2.15] |  | wt_med |
| 90 | 0.02 [-0.15, 0.22] | -0.09 [-0.15, -0.04] |  | -0.04 [-0.11, 0.02] |  |  |  |  |  | -0.09 [-0.19, 0.00] | 0.56 [0.09, 1.04] |  |  |  | wt_med |

**first-hour MAE, schedule known at origin**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.44 [4.34, 4.53] | 3.98 [3.90, 4.07] | 3.59 [3.49, 3.70] | 3.98 [3.90, 4.07] | 3.55 [3.46, 3.65] | 7.07 [6.95, 7.19] |  | 5.77 [5.68, 5.87] |  | 3.99 [3.91, 4.07] | 4.44 [4.35, 4.53] |  |  | 4.94 [4.83, 5.06] | 4.06 [3.97, 4.15] |
| 1 | 4.43 [4.34, 4.53] | 4.05 [3.96, 4.13] | 3.66 [3.56, 3.78] | 4.05 [3.96, 4.13] | 3.59 [3.50, 3.68] | 7.22 [7.10, 7.36] | 4.04 [3.96, 4.13] | 6.43 [6.32, 6.54] | 4.04 [3.96, 4.13] | 4.05 [3.97, 4.14] | 4.42 [4.33, 4.51] |  |  | 4.92 [4.82, 5.04] | 4.13 [4.04, 4.22] |
| 2 | 4.43 [4.34, 4.53] | 4.06 [3.98, 4.15] | 3.68 [3.57, 3.79] | 4.06 [3.98, 4.15] | 3.60 [3.51, 3.70] | 7.21 [7.08, 7.34] | 4.05 [3.97, 4.14] | 6.52 [6.41, 6.63] | 4.05 [3.97, 4.14] | 4.07 [3.98, 4.15] | 4.41 [4.31, 4.49] |  |  | 4.91 [4.81, 5.03] | 4.15 [4.06, 4.24] |
| 3 | 4.44 [4.34, 4.53] | 4.08 [3.99, 4.16] | 3.71 [3.61, 3.82] | 4.08 [3.99, 4.16] | 3.63 [3.54, 3.73] | 7.36 [7.23, 7.49] | 4.06 [3.98, 4.15] | 6.54 [6.43, 6.66] | 4.06 [3.98, 4.15] | 4.08 [4.00, 4.17] | 4.40 [4.31, 4.49] | 5.46 [5.33, 5.59] | 5.58 [5.45, 5.70] | 4.92 [4.81, 5.03] | 4.16 [4.08, 4.25] |
| 4 | 4.45 [4.35, 4.54] | 4.08 [3.99, 4.17] | 3.74 [3.63, 3.85] | 4.08 [3.99, 4.17] | 3.64 [3.55, 3.74] | 7.26 [7.13, 7.40] | 4.07 [3.98, 4.16] | 6.57 [6.45, 6.69] | 4.07 [3.98, 4.16] | 4.09 [4.00, 4.18] | 4.39 [4.30, 4.47] | 5.46 [5.33, 5.58] | 5.58 [5.45, 5.70] | 4.91 [4.80, 5.02] | 4.17 [4.09, 4.26] |
| 5 | 4.44 [4.35, 4.54] | 4.09 [4.00, 4.18] | 3.76 [3.66, 3.88] | 4.09 [4.00, 4.18] | 3.65 [3.55, 3.75] | 7.32 [7.16, 7.51] | 4.07 [3.98, 4.16] | 6.41 [6.30, 6.53] | 4.07 [3.98, 4.16] | 4.09 [4.00, 4.18] | 4.37 [4.28, 4.45] | 5.49 [5.37, 5.61] | 5.61 [5.49, 5.74] | 4.89 [4.78, 5.01] | 4.18 [4.09, 4.27] |
| 6 | 4.43 [4.34, 4.53] | 4.10 [4.01, 4.19] | 3.81 [3.70, 3.93] | 4.10 [4.01, 4.19] | 3.67 [3.58, 3.77] | 7.26 [7.13, 7.39] | 4.08 [4.00, 4.17] | 6.66 [6.55, 6.78] | 4.08 [4.00, 4.17] | 4.10 [4.02, 4.20] | 4.35 [4.27, 4.44] | 5.44 [5.32, 5.57] | 5.56 [5.44, 5.69] | 4.88 [4.77, 4.99] | 4.19 [4.11, 4.28] |
| 7 | 4.43 [4.33, 4.53] | 4.15 [4.06, 4.23] | 3.85 [3.73, 3.96] | 4.15 [4.06, 4.23] | 3.71 [3.61, 3.81] | 7.33 [7.20, 7.47] | 4.13 [4.04, 4.21] | 6.81 [6.70, 6.94] | 4.13 [4.04, 4.21] | 4.15 [4.06, 4.24] | 4.64 [4.55, 4.73] | 5.47 [5.35, 5.59] | 5.60 [5.48, 5.72] |  | 4.24 [4.15, 4.34] |
| 10 | 4.41 [4.30, 4.50] | 4.15 [4.06, 4.24] | 3.91 [3.80, 4.03] | 4.15 [4.06, 4.24] | 3.76 [3.66, 3.86] |  |  |  |  | 4.16 [4.06, 4.25] | 4.59 [4.49, 4.68] | 5.51 [5.38, 5.62] | 5.64 [5.51, 5.76] |  | 4.26 [4.16, 4.35] |
| 14 | 4.41 [4.31, 4.51] | 4.17 [4.07, 4.26] | 3.97 [3.86, 4.09] | 4.17 [4.07, 4.26] | 3.80 [3.70, 3.90] |  |  |  |  | 4.17 [4.08, 4.27] | 4.75 [4.64, 4.84] | 5.56 [5.44, 5.69] | 5.70 [5.58, 5.82] |  | 4.27 [4.18, 4.37] |
| 21 | 4.41 [4.31, 4.51] | 4.17 [4.08, 4.26] | 4.10 [3.99, 4.23] | 4.17 [4.08, 4.26] | 3.91 [3.80, 4.01] |  |  |  |  | 4.18 [4.09, 4.27] | 4.85 [4.75, 4.95] | 5.55 [5.43, 5.67] | 5.68 [5.56, 5.80] |  | 4.28 [4.19, 4.37] |
| 30 | 4.43 [4.32, 4.54] | 4.14 [4.05, 4.23] | 4.19 [4.06, 4.32] | 4.14 [4.05, 4.23] | 4.02 [3.89, 4.15] |  |  |  |  | 4.15 [4.05, 4.23] | 4.81 [4.71, 4.91] | 5.79 [5.65, 5.94] | 5.88 [5.74, 6.03] |  | 4.27 [4.17, 4.37] |
| 45 | 4.28 [4.17, 4.39] | 4.05 [3.96, 4.14] | 4.36 [4.08, 4.63] | 4.05 [3.96, 4.14] | 4.01 [3.86, 4.18] |  |  |  |  | 4.05 [3.96, 4.14] | 4.92 [4.81, 5.02] | 5.87 [5.69, 6.05] | 5.99 [5.81, 6.16] |  | 4.19 [4.10, 4.28] |
| 60 | 4.03 [3.93, 4.13] | 4.02 [3.92, 4.11] |  | 4.02 [3.92, 4.11] | 1.16 [0.80, 1.68] |  |  |  |  | 4.02 [3.93, 4.12] | 5.11 [5.00, 5.22] | 1.70 [1.22, 2.30] | 1.76 [1.27, 2.38] |  | 4.12 [4.02, 4.22] |
| 90 | 3.89 [3.76, 4.02] | 3.88 [3.76, 4.00] |  | 3.88 [3.76, 4.00] |  |  |  |  |  | 3.88 [3.76, 4.01] | 4.82 [4.68, 4.98] |  |  |  | 3.94 [3.81, 4.07] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.38 [0.28, 0.49] | -0.08 [-0.15, -0.03]* | -0.06 [-0.16, 0.00] | -0.08 [-0.15, -0.03]* | -0.08 [-0.18, -0.01]* | 3.00 [2.39, 3.72] |  | 1.71 [1.35, 2.11] |  | -0.08 [-0.15, -0.03]* | 0.38 [-0.01, 0.74] |  |  | 0.82 [0.63, 1.01] | wt_med |
| 1 | 0.33 [0.24, 0.43] | -0.09 [-0.16, -0.03]* | -0.06 [-0.16, 0.00] | -0.09 [-0.16, -0.03]* | -0.09 [-0.20, -0.01]* | 3.09 [2.47, 3.83] | -0.09 [-0.16, -0.03]* | 2.30 [1.84, 2.86] | -0.09 [-0.16, -0.03]* | -0.09 [-0.16, -0.03]* | 0.30 [-0.09, 0.67] |  |  | 0.74 [0.57, 0.92] | wt_med |
| 2 | 0.31 [0.22, 0.42] | -0.09 [-0.17, -0.04]* | -0.07 [-0.17, -0.00]* | -0.09 [-0.17, -0.04]* | -0.09 [-0.20, -0.01]* | 3.05 [2.43, 3.83] | -0.09 [-0.17, -0.04]* | 2.37 [1.88, 2.95] | -0.09 [-0.17, -0.04]* | -0.09 [-0.17, -0.04]* | 0.26 [-0.14, 0.63] |  |  | 0.71 [0.54, 0.89] | wt_med |
| 3 | 0.30 [0.21, 0.41] | -0.10 [-0.17, -0.04]* | -0.08 [-0.18, -0.00]* | -0.10 [-0.17, -0.04]* | -0.09 [-0.20, -0.02]* | 3.18 [2.55, 3.96] | -0.10 [-0.17, -0.04]* | 2.38 [1.89, 2.96] | -0.10 [-0.17, -0.04]* | -0.10 [-0.17, -0.04]* | 0.24 [-0.16, 0.61] | 1.72 [1.46, 2.02] | 1.84 [1.57, 2.14] | 0.69 [0.52, 0.88] | wt_med |
| 4 | 0.30 [0.20, 0.41] | -0.10 [-0.17, -0.04]* | -0.08 [-0.18, -0.01]* | -0.10 [-0.17, -0.04]* | -0.10 [-0.20, -0.02]* | 3.07 [2.43, 3.84] | -0.10 [-0.18, -0.04]* | 2.39 [1.88, 3.01] | -0.10 [-0.18, -0.04]* | -0.10 [-0.17, -0.04]* | 0.21 [-0.19, 0.58] | 1.71 [1.44, 2.00] | 1.83 [1.57, 2.11] | 0.67 [0.49, 0.85] | wt_med |
| 5 | 0.30 [0.20, 0.40] | -0.10 [-0.18, -0.04]* | -0.08 [-0.19, -0.01]* | -0.10 [-0.18, -0.04]* | -0.10 [-0.21, -0.02]* | 3.13 [2.49, 3.85] | -0.10 [-0.18, -0.04]* | 2.23 [1.76, 2.77] | -0.10 [-0.18, -0.04]* | -0.10 [-0.18, -0.04]* | 0.19 [-0.23, 0.56] | 1.73 [1.46, 2.03] | 1.86 [1.59, 2.15] | 0.65 [0.47, 0.83] | wt_med |
| 6 | 0.27 [0.18, 0.37] | -0.10 [-0.17, -0.04]* | -0.08 [-0.20, -0.01]* | -0.10 [-0.17, -0.04]* | -0.10 [-0.21, -0.02]* | 3.06 [2.47, 3.76] | -0.10 [-0.18, -0.04]* | 2.47 [1.98, 3.05] | -0.10 [-0.18, -0.04]* | -0.10 [-0.17, -0.04]* | 0.16 [-0.26, 0.53] | 1.67 [1.42, 1.95] | 1.80 [1.54, 2.08] | 0.62 [0.44, 0.79] | wt_med |
| 7 | 0.23 [0.14, 0.33] | -0.10 [-0.18, -0.04]* | -0.08 [-0.20, -0.00]* | -0.10 [-0.18, -0.04]* | -0.10 [-0.20, -0.02]* | 3.08 [2.45, 3.84] | -0.10 [-0.18, -0.04]* | 2.58 [2.03, 3.22] | -0.10 [-0.18, -0.04]* | -0.10 [-0.18, -0.04]* | 0.44 [0.02, 0.82] | 1.67 [1.41, 1.95] | 1.80 [1.53, 2.08] |  | wt_med |
| 10 | 0.20 [0.12, 0.28] | -0.11 [-0.19, -0.04]* | -0.09 [-0.24, -0.00]* | -0.11 [-0.19, -0.04]* | -0.10 [-0.21, -0.01]* |  |  |  |  | -0.11 [-0.19, -0.04]* | 0.35 [-0.08, 0.75] | 1.66 [1.38, 1.96] | 1.79 [1.50, 2.10] |  | wt_med |
| 14 | 0.19 [0.10, 0.28] | -0.11 [-0.19, -0.04]* | -0.09 [-0.22, 0.00] | -0.11 [-0.19, -0.04]* | -0.10 [-0.21, -0.02]* |  |  |  |  | -0.11 [-0.19, -0.04]* | 0.51 [0.05, 0.93] | 1.68 [1.40, 1.97] | 1.82 [1.53, 2.12] |  | wt_med |
| 21 | 0.17 [0.08, 0.26] | -0.11 [-0.20, -0.04]* | -0.09 [-0.24, -0.00]* | -0.11 [-0.20, -0.04]* | -0.09 [-0.21, -0.01]* |  |  |  |  | -0.11 [-0.20, -0.04]* | 0.57 [0.12, 0.99] | 1.57 [1.29, 1.87] | 1.70 [1.42, 2.02] |  | wt_med |
| 30 | 0.15 [0.07, 0.25] | -0.12 [-0.22, -0.04]* | -0.12 [-0.31, -0.01]* | -0.12 [-0.22, -0.04]* | -0.10 [-0.24, -0.01]* |  |  |  |  | -0.12 [-0.22, -0.04]* | 0.53 [0.06, 0.97] | 1.59 [1.31, 1.89] | 1.69 [1.42, 1.99] |  | wt_med |
| 45 | 0.07 [-0.04, 0.19] | -0.14 [-0.25, -0.05]* | -0.15 [-0.32, 0.03] | -0.14 [-0.25, -0.05]* | -0.19 [-0.35, -0.07]* |  |  |  |  | -0.14 [-0.25, -0.05]* | 0.76 [0.31, 1.20] | 1.56 [1.27, 1.84] | 1.68 [1.38, 2.01] |  | wt_med |
| 60 | -0.08 [-0.25, 0.08] | -0.13 [-0.25, -0.04] |  | -0.13 [-0.25, -0.04] | 0.00 [0.00, 0.00] |  |  |  |  | -0.13 [-0.25, -0.04] | 1.02 [0.55, 1.46] | 0.46 [0.14, 2.20] | 0.52 [0.21, 2.37] |  | wt_med |
| 90 | -0.08 [-0.30, 0.08] | -0.09 [-0.19, -0.02] |  | -0.09 [-0.19, -0.02] |  |  |  |  |  | -0.09 [-0.19, -0.02] | 0.87 [0.31, 1.46] |  |  |  | wt_med |

**first-hour MAE, schedule projected at origin**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.92 [4.73, 5.12] | 4.56 [4.38, 4.74] | 4.22 [3.96, 4.50] | 4.55 [4.38, 4.73] | 4.11 [3.91, 4.31] | 8.66 [8.44, 8.87] |  | 6.23 [6.07, 6.39] |  | 4.52 [4.35, 4.70] | 4.13 [3.96, 4.29] |  |  | 5.79 [5.57, 6.01] | 4.58 [4.41, 4.76] |
| 1 | 4.92 [4.73, 5.11] | 4.61 [4.44, 4.79] | 4.27 [4.03, 4.55] | 4.60 [4.43, 4.77] | 4.16 [3.96, 4.39] | 8.49 [8.29, 8.71] | 4.55 [4.38, 4.73] | 7.72 [7.54, 7.92] | 4.56 [4.38, 4.74] | 4.55 [4.39, 4.73] | 4.13 [3.97, 4.30] |  |  | 5.78 [5.56, 5.99] | 4.63 [4.46, 4.81] |
| 2 | 4.93 [4.75, 5.13] | 4.66 [4.49, 4.84] | 4.32 [4.05, 4.58] | 4.64 [4.47, 4.83] | 4.22 [4.01, 4.44] | 8.70 [8.50, 8.92] | 4.60 [4.42, 4.78] | 8.10 [7.89, 8.31] | 4.60 [4.42, 4.78] | 4.60 [4.43, 4.78] | 4.16 [4.00, 4.33] |  |  | 5.80 [5.58, 6.01] | 4.68 [4.51, 4.87] |
| 3 | 4.98 [4.79, 5.17] | 4.70 [4.52, 4.88] | 4.38 [4.11, 4.65] | 4.68 [4.50, 4.86] | 4.26 [4.04, 4.47] | 8.68 [8.47, 8.90] | 4.63 [4.45, 4.82] | 8.15 [7.94, 8.36] | 4.63 [4.46, 4.82] | 4.64 [4.47, 4.82] | 4.19 [4.02, 4.36] | 5.88 [5.64, 6.11] | 6.15 [5.92, 6.38] | 5.81 [5.60, 6.03] | 4.73 [4.55, 4.91] |
| 4 | 4.99 [4.80, 5.17] | 4.74 [4.56, 4.92] | 4.49 [4.21, 4.76] | 4.71 [4.54, 4.90] | 4.32 [4.09, 4.53] | 8.63 [8.44, 8.83] | 4.64 [4.47, 4.82] | 8.25 [8.05, 8.46] | 4.64 [4.47, 4.82] | 4.68 [4.51, 4.87] | 4.22 [4.06, 4.39] | 5.93 [5.69, 6.17] | 6.20 [5.96, 6.43] | 5.79 [5.57, 6.00] | 4.77 [4.58, 4.96] |
| 5 | 5.05 [4.87, 5.24] | 4.81 [4.64, 4.99] | 4.54 [4.27, 4.82] | 4.79 [4.62, 4.98] | 4.39 [4.15, 4.59] | 8.54 [8.34, 8.74] | 4.72 [4.55, 4.89] | 8.37 [8.16, 8.57] | 4.72 [4.55, 4.89] | 4.76 [4.59, 4.94] | 4.29 [4.12, 4.47] | 6.01 [5.77, 6.26] | 6.26 [6.02, 6.50] | 5.79 [5.59, 6.00] | 4.85 [4.67, 5.03] |
| 6 | 5.13 [4.94, 5.32] | 4.93 [4.75, 5.12] | 4.67 [4.37, 4.95] | 4.91 [4.73, 5.10] | 4.50 [4.26, 4.73] | 8.90 [8.69, 9.10] | 4.81 [4.65, 4.99] | 8.20 [7.99, 8.40] | 4.81 [4.65, 4.98] | 4.87 [4.70, 5.06] | 4.36 [4.19, 4.53] | 6.04 [5.80, 6.29] | 6.24 [6.02, 6.49] | 5.81 [5.61, 6.02] | 4.97 [4.79, 5.16] |
| 7 | 5.19 [5.01, 5.38] | 5.03 [4.86, 5.22] | 4.78 [4.48, 5.08] | 4.99 [4.82, 5.18] | 4.60 [4.37, 4.85] | 8.84 [8.64, 9.04] | 4.91 [4.74, 5.09] | 8.82 [8.60, 9.04] | 4.90 [4.74, 5.08] | 4.97 [4.80, 5.15] | 4.64 [4.47, 4.82] | 6.24 [5.98, 6.49] | 6.44 [6.20, 6.70] |  | 5.09 [4.91, 5.28] |
| 10 | 5.34 [5.16, 5.53] | 5.20 [5.03, 5.38] | 5.03 [4.72, 5.31] | 5.17 [5.00, 5.34] | 4.78 [4.55, 5.00] |  |  |  |  | 5.13 [4.97, 5.31] | 4.78 [4.60, 4.96] | 6.48 [6.23, 6.71] | 6.67 [6.43, 6.90] |  | 5.26 [5.09, 5.43] |
| 14 | 5.51 [5.34, 5.69] | 5.39 [5.22, 5.57] | 5.35 [5.04, 5.68] | 5.36 [5.20, 5.54] | 5.07 [4.83, 5.32] |  |  |  |  | 5.32 [5.16, 5.50] | 5.02 [4.84, 5.19] | 6.63 [6.39, 6.89] | 6.80 [6.57, 7.04] |  | 5.43 [5.27, 5.61] |
| 21 | 5.72 [5.54, 5.90] | 5.59 [5.42, 5.76] | 5.82 [5.49, 6.16] | 5.58 [5.42, 5.76] | 5.42 [5.18, 5.69] |  |  |  |  | 5.53 [5.36, 5.70] | 5.34 [5.16, 5.51] | 7.10 [6.87, 7.37] | 7.29 [7.05, 7.55] |  | 5.62 [5.45, 5.80] |
| 30 | 5.63 [5.46, 5.78] | 5.59 [5.43, 5.76] | 5.99 [5.68, 6.31] | 5.61 [5.45, 5.77] | 5.75 [5.48, 6.05] |  |  |  |  | 5.54 [5.38, 5.69] | 5.73 [5.56, 5.90] | 7.44 [7.18, 7.71] | 7.60 [7.34, 7.87] |  | 5.63 [5.46, 5.78] |
| 45 | 5.91 [5.74, 6.06] | 5.93 [5.78, 6.09] | 4.50 [4.26, 4.76] | 5.98 [5.82, 6.14] | 6.25 [5.94, 6.59] |  |  |  |  | 5.82 [5.67, 5.97] | 6.20 [6.03, 6.37] | 8.18 [7.84, 8.52] | 8.33 [8.02, 8.64] |  | 6.01 [5.85, 6.17] |
| 60 | 6.08 [5.93, 6.24] | 6.01 [5.87, 6.17] |  | 6.07 [5.93, 6.23] | 2.62 [1.17, 3.62] |  |  |  |  | 5.91 [5.77, 6.07] | 6.31 [6.14, 6.47] | 3.29 [1.43, 4.41] | 3.37 [1.50, 4.52] |  | 6.11 [5.96, 6.27] |
| 90 | 6.08 [5.92, 6.23] | 5.94 [5.79, 6.10] |  | 6.04 [5.89, 6.21] |  |  |  |  |  | 5.92 [5.78, 6.07] | 6.32 [6.17, 6.47] |  |  |  | 6.00 [5.84, 6.16] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.30 [0.19, 0.42] | -0.02 [-0.04, 0.00] | 0.01 [-0.03, 0.07] | -0.02 [-0.04, 0.00] | 0.02 [-0.01, 0.06] | 4.14 [2.54, 5.76] |  | 1.69 [1.10, 2.20] |  | -0.06 [-0.20, -0.00]* | -0.49 [-0.86, -0.04]* |  |  | 1.07 [0.74, 1.45] | wt_med |
| 1 | 0.26 [0.17, 0.37] | -0.02 [-0.05, -0.00]* | -0.02 [-0.13, 0.05] | -0.02 [-0.05, -0.00]* | 0.02 [-0.01, 0.07] | 3.91 [2.50, 5.38] | -0.03 [-0.06, -0.00]* | 3.15 [1.75, 4.57] | -0.02 [-0.05, -0.00]* | -0.08 [-0.23, -0.01]* | -0.53 [-0.91, -0.08]* |  |  | 1.02 [0.70, 1.40] | wt_med |
| 2 | 0.24 [0.16, 0.34] | -0.02 [-0.04, -0.00]* | -0.03 [-0.14, 0.03] | -0.02 [-0.05, -0.00]* | 0.04 [0.00, 0.10] | 4.09 [2.64, 5.58] | -0.02 [-0.05, -0.00]* | 3.50 [1.97, 5.02] | -0.02 [-0.05, -0.00]* | -0.08 [-0.23, -0.01]* | -0.54 [-0.91, -0.07]* |  |  | 1.00 [0.68, 1.38] | wt_med |
| 3 | 0.24 [0.16, 0.35] | -0.03 [-0.06, -0.00]* | -0.02 [-0.14, 0.04] | -0.03 [-0.07, -0.00]* | 0.02 [-0.00, 0.06] | 4.04 [2.64, 5.48] | -0.03 [-0.07, -0.00]* | 3.52 [1.91, 5.14] | -0.03 [-0.06, -0.00]* | -0.09 [-0.26, -0.01]* | -0.56 [-0.93, -0.10]* | 1.63 [1.18, 2.12] | 1.90 [1.39, 2.43] | 0.97 [0.64, 1.33] | wt_med |
| 4 | 0.25 [0.17, 0.37] | -0.03 [-0.06, -0.01]* | -0.02 [-0.13, 0.03] | -0.03 [-0.09, -0.00]* | 0.01 [-0.02, 0.05] | 4.00 [2.60, 5.42] | -0.03 [-0.06, -0.00]* | 3.62 [2.07, 5.21] | -0.03 [-0.06, -0.00]* | -0.09 [-0.27, -0.01]* | -0.56 [-0.95, -0.10]* | 1.63 [1.17, 2.10] | 1.90 [1.38, 2.38] | 0.90 [0.57, 1.23] | wt_med |
| 5 | 0.23 [0.15, 0.34] | -0.04 [-0.08, -0.01]* | -0.03 [-0.15, 0.03] | -0.04 [-0.10, -0.01]* | 0.00 [-0.05, 0.05] | 3.83 [2.61, 5.08] | -0.03 [-0.08, 0.00] | 3.66 [2.13, 5.23] | -0.03 [-0.08, -0.00]* | -0.09 [-0.28, -0.01]* | -0.57 [-0.95, -0.10]* | 1.65 [1.22, 2.13] | 1.90 [1.40, 2.40] | 0.82 [0.48, 1.15] | wt_med |
| 6 | 0.20 [0.11, 0.30] | -0.04 [-0.09, -0.01]* | -0.03 [-0.16, 0.04] | -0.05 [-0.11, -0.01]* | 0.02 [-0.02, 0.08] | 4.07 [2.73, 5.41] | -0.04 [-0.11, -0.00]* | 3.38 [2.13, 4.65] | -0.05 [-0.11, -0.01]* | -0.10 [-0.29, -0.02]* | -0.63 [-1.02, -0.15]* | 1.59 [1.15, 2.08] | 1.80 [1.28, 2.28] | 0.71 [0.36, 1.06] | wt_med |
| 7 | 0.16 [0.07, 0.26] | -0.05 [-0.11, -0.02]* | -0.04 [-0.18, 0.05] | -0.05 [-0.11, -0.01]* | 0.01 [-0.05, 0.07] | 3.92 [2.68, 5.18] | -0.06 [-0.13, -0.01]* | 3.90 [2.44, 5.35] | -0.06 [-0.13, -0.01]* | -0.12 [-0.32, -0.03]* | -0.40 [-0.78, 0.02] | 1.68 [1.19, 2.18] | 1.89 [1.30, 2.45] |  | wt_med |
| 10 | 0.13 [0.03, 0.23] | -0.06 [-0.13, -0.01]* | -0.04 [-0.19, 0.08] | -0.05 [-0.12, -0.00]* | -0.01 [-0.13, 0.11] |  |  |  |  | -0.13 [-0.31, -0.02]* | -0.45 [-0.80, -0.03]* | 1.73 [1.15, 2.30] | 1.93 [1.24, 2.54] |  | wt_med |
| 14 | 0.11 [0.02, 0.20] | -0.05 [-0.11, 0.00] | -0.02 [-0.17, 0.09] | -0.04 [-0.10, 0.01] | -0.00 [-0.13, 0.10] |  |  |  |  | -0.11 [-0.25, -0.03]* | -0.38 [-0.72, 0.03] | 1.59 [1.00, 2.13] | 1.76 [1.08, 2.40] |  | wt_med |
| 21 | 0.11 [0.01, 0.21] | -0.04 [-0.11, 0.00] | -0.02 [-0.18, 0.13] | -0.02 [-0.06, 0.03] | 0.01 [-0.12, 0.13] |  |  |  |  | -0.10 [-0.22, -0.03]* | -0.28 [-0.66, 0.16] | 1.70 [1.09, 2.28] | 1.89 [1.14, 2.64] |  | wt_med |
| 30 | 0.03 [-0.09, 0.15] | -0.05 [-0.12, 0.00] | 0.03 [-0.09, 0.16] | -0.01 [-0.05, 0.03] | 0.03 [-0.11, 0.18] |  |  |  |  | -0.11 [-0.22, -0.03] | 0.05 [-0.45, 0.66] | 1.73 [1.19, 2.30] | 1.89 [1.21, 2.58] |  | wt_med |
| 45 | -0.09 [-0.30, 0.13] | -0.08 [-0.17, -0.02] | -0.10 [-0.33, 0.04] | -0.02 [-0.09, 0.04] | 0.02 [-0.15, 0.19] |  |  |  |  | -0.19 [-0.33, -0.07] | 0.12 [-0.47, 0.76] | 2.03 [1.54, 2.60] | 2.19 [1.59, 2.84] |  | wt_med |
| 60 | -0.01 [-0.24, 0.25] | -0.10 [-0.19, -0.03] |  | -0.03 [-0.13, 0.06] | 0.00 [0.00, 0.00] |  |  |  |  | -0.19 [-0.34, -0.08] | 0.13 [-0.57, 0.88] | 0.66 [0.00, 2.21] | 0.73 [0.00, 4.71] |  | wt_med |
| 90 | 0.12 [-0.14, 0.44] | -0.08 [-0.15, -0.03] |  | 0.00 [-0.08, 0.09] |  |  |  |  |  | -0.10 [-0.25, 0.06] | 0.27 [-0.41, 0.99] |  |  |  | wt_med |

**rope-drop worth agreement**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.91] |  | 0.91 [0.91, 0.92] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 1 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 2 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 3 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 4 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.91] |
| 5 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 6 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 7 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.89, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.91 [0.91, 0.91] |
| 10 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.89, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  |  |  |  | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.91] |
| 14 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.89 [0.89, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  |  |  |  | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.91] |
| 21 | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.89 [0.89, 0.89] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] |  |  |  |  | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.91] |
| 30 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.89 [0.89, 0.89] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.90] |  |  |  |  | 0.91 [0.90, 0.91] | 0.89 [0.89, 0.90] | 0.89 [0.89, 0.90] |  | 0.91 [0.91, 0.91] |
| 45 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.87 [0.86, 0.87] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] |  |  |  |  | 0.90 [0.90, 0.91] | 0.90 [0.89, 0.90] | 0.90 [0.89, 0.90] |  | 0.91 [0.91, 0.92] |
| 60 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] |  | 0.91 [0.91, 0.91] | 0.99 [0.99, 1.00] |  |  |  |  | 0.90 [0.90, 0.90] | 0.99 [0.99, 1.00] | 0.99 [0.99, 1.00] |  | 0.91 [0.91, 0.92] |
| 90 | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |  | 0.91 [0.91, 0.92] |  |  |  |  |  | 0.90 [0.90, 0.91] |  |  |  | 0.91 [0.91, 0.92] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | -0.00 [-0.01, -0.00] |  | -0.00 [-0.00, 0.00] |  |  | -0.01 [-0.01, -0.01] | wt_med |
| 1 | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, 0.00] |  |  | -0.01 [-0.01, -0.01] | wt_med |
| 2 | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] |  |  | -0.01 [-0.01, -0.01] | wt_med |
| 3 | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 4 | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 5 | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 6 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.01] | wt_med |
| 7 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 10 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] |  |  |  |  | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 14 | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] |  |  |  |  | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 21 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  |  |  | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 30 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.00] |  |  |  |  | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 45 | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] |  |  |  |  | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  | wt_med |
| 60 | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] |  | -0.00 [-0.00, -0.00] | -0.00 [-0.02, 0.00] |  |  |  |  | -0.01 [-0.02, -0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] |  | wt_med |
| 90 | -0.00 [-0.00, 0.00] | 0.00 [-0.00, 0.00] |  | -0.00 [-0.01, -0.00] |  |  |  |  |  | -0.01 [-0.02, -0.00] |  |  |  | wt_med |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.19 [0.18, 0.20] | 0.28 [0.27, 0.29] | 0.37 [0.36, 0.38] | 0.27 [0.26, 0.28] | 0.31 [0.30, 0.32] | 0.39 [0.38, 0.39] | 0.38 [0.37, 0.39] | 0.32 [0.31, 0.33] |
| 2 | 0.19 [0.18, 0.20] | 0.28 [0.28, 0.29] | 0.37 [0.36, 0.38] | 0.27 [0.26, 0.28] | 0.31 [0.30, 0.32] | 0.39 [0.38, 0.40] | 0.37 [0.36, 0.38] | 0.32 [0.31, 0.33] |
| 3 | 0.19 [0.18, 0.20] | 0.28 [0.27, 0.29] | 0.38 [0.37, 0.38] | 0.26 [0.26, 0.27] | 0.30 [0.29, 0.31] | 0.39 [0.38, 0.40] | 0.37 [0.35, 0.38] | 0.32 [0.31, 0.33] |
| 4 | 0.19 [0.18, 0.20] | 0.28 [0.27, 0.29] | 0.38 [0.37, 0.38] | 0.27 [0.26, 0.28] | 0.30 [0.29, 0.31] | 0.39 [0.38, 0.40] | 0.37 [0.36, 0.38] | 0.32 [0.31, 0.33] |
| 5 | 0.19 [0.18, 0.20] | 0.28 [0.27, 0.29] | 0.38 [0.37, 0.39] | 0.26 [0.26, 0.27] | 0.29 [0.28, 0.30] | 0.39 [0.38, 0.40] | 0.35 [0.34, 0.37] | 0.31 [0.31, 0.32] |
| 6 | 0.18 [0.17, 0.19] | 0.28 [0.27, 0.29] | 0.38 [0.37, 0.39] | 0.27 [0.26, 0.28] | 0.29 [0.28, 0.30] | 0.39 [0.38, 0.40] | 0.35 [0.34, 0.36] | 0.31 [0.30, 0.32] |
| 7 | 0.17 [0.16, 0.18] | 0.28 [0.27, 0.29] | 0.34 [0.33, 0.35] | 0.26 [0.25, 0.27] | 0.29 [0.28, 0.30] |  | 0.34 [0.33, 0.35] | 0.30 [0.29, 0.31] |
| 10 | 0.17 [0.16, 0.18] | 0.27 [0.27, 0.28] | 0.34 [0.34, 0.35] |  |  |  | 0.33 [0.32, 0.34] | 0.30 [0.29, 0.31] |
| 14 | 0.17 [0.16, 0.18] | 0.27 [0.26, 0.28] | 0.31 [0.30, 0.32] |  |  |  | 0.31 [0.30, 0.32] | 0.29 [0.28, 0.30] |
| 21 | 0.16 [0.15, 0.17] | 0.26 [0.25, 0.27] | 0.30 [0.29, 0.31] |  |  |  | 0.29 [0.27, 0.30] | 0.28 [0.27, 0.29] |
| 30 | 0.15 [0.14, 0.16] | 0.26 [0.25, 0.27] | 0.29 [0.28, 0.30] |  |  |  | 0.26 [0.25, 0.28] | 0.27 [0.26, 0.28] |
| 45 | 0.23 [0.21, 0.25] | 0.25 [0.24, 0.26] | 0.28 [0.27, 0.29] |  |  |  | 0.26 [0.24, 0.27] | 0.27 [0.26, 0.27] |
| 60 |  | 0.24 [0.23, 0.24] | 0.27 [0.26, 0.28] |  |  |  | 0.21 [0.12, 0.31] | 0.26 [0.25, 0.27] |
| 90 |  | 0.23 [0.22, 0.24] | 0.28 [0.27, 0.29] |  |  |  |  | 0.26 [0.25, 0.27] |
| 120 |  | 0.22 [0.21, 0.23] | 0.28 [0.26, 0.29] |  |  |  |  | 0.25 [0.23, 0.26] |
| 180 |  | 0.21 [0.19, 0.23] | 0.26 [0.24, 0.28] |  |  |  |  | 0.24 [0.22, 0.26] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|
| 1 | -0.20 [-0.24, -0.17] | -0.10 [-0.13, -0.08] | -0.01 [-0.03, -0.00] | -0.12 [-0.13, -0.10] | -0.07 [-0.09, -0.05] | -0.00 [-0.02, 0.02] | -0.06 [-0.09, -0.05] | lvl_snaive7 |
| 2 | -0.20 [-0.24, -0.17] | -0.10 [-0.13, -0.08] | -0.01 [-0.03, 0.00] | -0.12 [-0.14, -0.10] | -0.08 [-0.10, -0.06] | -0.01 [-0.03, 0.01] | -0.07 [-0.09, -0.05] | lvl_snaive7 |
| 3 | -0.20 [-0.24, -0.17] | -0.10 [-0.13, -0.08] | -0.01 [-0.03, 0.00] | -0.12 [-0.14, -0.10] | -0.09 [-0.11, -0.07] | -0.02 [-0.04, -0.00] | -0.07 [-0.09, -0.05] | lvl_snaive7 |
| 4 | -0.20 [-0.24, -0.17] | -0.10 [-0.13, -0.08] | -0.01 [-0.03, 0.00] | -0.12 [-0.13, -0.10] | -0.09 [-0.11, -0.07] | -0.02 [-0.04, 0.00] | -0.07 [-0.09, -0.05] | lvl_snaive7 |
| 5 | -0.20 [-0.23, -0.16] | -0.10 [-0.13, -0.08] | -0.01 [-0.02, 0.01] | -0.12 [-0.14, -0.10] | -0.10 [-0.12, -0.07] | -0.03 [-0.05, -0.01] | -0.07 [-0.09, -0.05] | lvl_snaive7 |
| 6 | -0.21 [-0.25, -0.17] | -0.11 [-0.13, -0.08] | -0.01 [-0.02, 0.01] | -0.12 [-0.14, -0.10] | -0.10 [-0.12, -0.08] | -0.04 [-0.06, -0.01] | -0.07 [-0.10, -0.05] | lvl_snaive7 |
| 7 | -0.18 [-0.21, -0.14] | -0.06 [-0.08, -0.05] |  | -0.08 [-0.10, -0.06] | -0.06 [-0.07, -0.04] | -0.02 [-0.03, 0.00] | -0.04 [-0.05, -0.03] | lvl_naive4 |
| 10 | -0.18 [-0.22, -0.15] | -0.07 [-0.09, -0.05] |  |  |  | -0.03 [-0.04, -0.01] | -0.05 [-0.06, -0.03] | lvl_naive4 |
| 14 | -0.15 [-0.18, -0.12] | -0.04 [-0.06, -0.02] |  |  |  | -0.01 [-0.03, 0.00] | -0.02 [-0.04, -0.01] | lvl_naive4 |
| 21 | -0.14 [-0.17, -0.11] | -0.04 [-0.06, -0.02] |  |  |  | -0.01 [-0.03, 0.00] | -0.02 [-0.03, -0.01] | lvl_naive4 |
| 30 | -0.12 [-0.15, -0.09] | -0.03 [-0.05, -0.01] |  |  |  | -0.01 [-0.03, 0.01] | -0.01 [-0.03, -0.00] | lvl_naive4 |
| 45 | -0.08 [-0.13, -0.03] | -0.03 [-0.05, -0.01] |  |  |  | -0.01 [-0.03, 0.02] | -0.01 [-0.02, 0.00] | lvl_naive4 |
| 60 |  | -0.03 [-0.05, -0.01] |  |  |  | 0.06 [-0.08, 0.13] | -0.00 [-0.02, 0.01] | lvl_naive4 |
| 90 |  | -0.05 [-0.07, -0.03] |  |  |  |  | -0.02 [-0.03, -0.00] | lvl_naive4 |
| 120 |  | -0.06 [-0.08, -0.03] |  |  |  |  | -0.03 [-0.05, -0.01] | lvl_naive4 |
| 180 |  | -0.05 [-0.10, 0.01] |  |  |  |  | -0.02 [-0.06, 0.02] | lvl_naive4 |

| lead | model | park_days | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|
| 1 | lvl_naive4 | 10436 | 0.78 | 0.57 | 0.66 |
| 1 | lvl_wt56 | 10573 | 0.76 | 0.44 | 0.68 |
| 1 | lvl_snaive7 | 10309 | 0.77 | 0.65 | 0.65 |
| 1 | lvl_clim | 10573 | 0.72 | 0.38 | 0.63 |
| 1 | lvl_tft | 6384 | 0.82 | 0.63 | 0.71 |
| 1 | lvl_cbd | 4728 | 0.56 | 0.17 | 0.78 |
| 1 | lvl_nf_tide | 10567 | 0.71 | 0.33 | 0.81 |
| 1 | lvl_nf_nhits | 10567 | 0.66 | 0.26 | 0.76 |
| 7 | lvl_naive4 | 10426 | 0.75 | 0.54 | 0.64 |
| 7 | lvl_wt56 | 10615 | 0.74 | 0.42 | 0.66 |
| 7 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 7 | lvl_clim | 10613 | 0.71 | 0.37 | 0.62 |
| 7 | lvl_tft | 6384 | 0.76 | 0.57 | 0.67 |
| 7 | lvl_cbd | 4766 | 0.50 | 0.13 | 0.70 |
| 7 | lvl_nf_tide | 10597 | 0.68 | 0.29 | 0.76 |
| 7 | lvl_nf_nhits | 10597 | 0.63 | 0.24 | 0.74 |
| 30 | lvl_naive4 | 10339 | 0.69 | 0.50 | 0.59 |
| 30 | lvl_wt56 | 10571 | 0.69 | 0.40 | 0.62 |
| 30 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_clim | 10564 | 0.68 | 0.36 | 0.61 |
| 30 | lvl_tft | 4882 | 0.66 | 0.55 | 0.62 |
| 30 | lvl_cbd | 4631 | 0.45 | 0.11 | 0.64 |
| 30 | lvl_nf_tide | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_nf_nhits | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_naive4 | 7373 | 0.68 | 0.51 | 0.55 |
| 90 | lvl_wt56 | 7516 | 0.70 | 0.40 | 0.58 |
| 90 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_clim | 7522 | 0.67 | 0.36 | 0.59 |
| 90 | lvl_tft | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_cbd | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_nf_tide | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_nf_nhits | 0 | 0.00 | 0.00 | 0.00 |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 144895 | 96250 | 0.66 |
| 1 | lvl_clim | 317378 | 196220 | 0.62 |
| 1 | lvl_naive4 | 310126 | 219156 | 0.71 |
| 1 | lvl_nf_nhits | 317009 | 223753 | 0.71 |
| 1 | lvl_nf_tide | 317009 | 232300 | 0.73 |
| 1 | lvl_snaive7 | 302421 | 218157 | 0.72 |
| 1 | lvl_tft | 230585 | 169531 | 0.74 |
| 1 | lvl_wt56 | 317378 | 217140 | 0.68 |
| 7 | lvl_cbd | 147717 | 90040 | 0.61 |
| 7 | lvl_clim | 320405 | 192383 | 0.60 |
| 7 | lvl_naive4 | 310697 | 209962 | 0.68 |
| 7 | lvl_nf_nhits | 319396 | 218382 | 0.68 |
| 7 | lvl_nf_tide | 319396 | 221059 | 0.69 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 232045 | 159985 | 0.69 |
| 7 | lvl_wt56 | 320525 | 211697 | 0.66 |
| 30 | lvl_cbd | 140886 | 83453 | 0.59 |
| 30 | lvl_clim | 316340 | 178354 | 0.56 |
| 30 | lvl_naive4 | 304776 | 191058 | 0.63 |
| 30 | lvl_nf_nhits | 0 | 0 |  |
| 30 | lvl_nf_tide | 0 | 0 |  |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 178787 | 113789 | 0.64 |
| 30 | lvl_wt56 | 316760 | 191861 | 0.61 |
| 90 | lvl_cbd | 0 | 0 |  |
| 90 | lvl_clim | 187545 | 107128 | 0.57 |
| 90 | lvl_naive4 | 181049 | 109197 | 0.60 |
| 90 | lvl_nf_nhits | 0 | 0 |  |
| 90 | lvl_nf_tide | 0 | 0 |  |
| 90 | lvl_snaive7 | 0 | 0 |  |
| 90 | lvl_tft | 0 | 0 |  |
| 90 | lvl_wt56 | 187238 | 111572 | 0.60 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie (counted wrong); CI over (origin, park).

**day comparison winner accuracy**

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|
| d1-7 | 0.23 [0.23, 0.24] | 0.27 [0.26, 0.27] | 0.45 [0.45, 0.46] | 0.10 [0.10, 0.11] | 0.21 [0.21, 0.22] | 0.46 [0.46, 0.47] | 0.40 [0.39, 0.40] | 0.28 [0.27, 0.28] |
| d8-30 | 0.16 [0.16, 0.17] | 0.23 [0.23, 0.24] | 0.37 [0.37, 0.37] |  |  |  | 0.34 [0.34, 0.35] | 0.23 [0.23, 0.24] |
| d31-90 | 0.14 [0.14, 0.15] | 0.24 [0.24, 0.24] | 0.34 [0.34, 0.34] |  |  |  | 0.34 [0.34, 0.35] | 0.22 [0.22, 0.22] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_naive4 | lvl_nf_nhits | lvl_nf_tide | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|
| d1-7 | -0.20 [-0.23, -0.18] | -0.20 [-0.22, -0.17] | -0.01 [-0.02, 0.01] | -0.36 [-0.38, -0.34] | -0.25 [-0.27, -0.24] | -0.06 [-0.08, -0.05] | -0.18 [-0.21, -0.16] | lvl_snaive7 |
| d8-30 | -0.19 [-0.22, -0.17] | -0.14 [-0.16, -0.12] |  |  |  | -0.04 [-0.05, -0.03] | -0.14 [-0.15, -0.12] | lvl_naive4 |
| d31-90 | -0.18 [-0.21, -0.16] | -0.10 [-0.12, -0.08] |  |  |  | 0.01 [-0.01, 0.03] | -0.12 [-0.13, -0.11] | lvl_naive4 |

| lead | model | tie_rate |
|---|---|---|
| d1-7 | lvl_naive4 | 0.35 |
| d1-7 | lvl_wt56 | 0.60 |
| d1-7 | lvl_snaive7 | 0.30 |
| d1-7 | lvl_clim | 0.61 |
| d1-7 | lvl_tft | 0.43 |
| d1-7 | lvl_cbd | 0.65 |
| d1-7 | lvl_nf_tide | 0.70 |
| d1-7 | lvl_nf_nhits | 0.82 |
| d8-30 | lvl_naive4 | 0.43 |
| d8-30 | lvl_wt56 | 0.64 |
| d8-30 | lvl_clim | 0.64 |
| d8-30 | lvl_tft | 0.45 |
| d8-30 | lvl_cbd | 0.73 |
| d31-90 | lvl_naive4 | 0.44 |
| d31-90 | lvl_wt56 | 0.65 |
| d31-90 | lvl_clim | 0.61 |
| d31-90 | lvl_tft | 0.45 |
| d31-90 | lvl_cbd | 0.75 |

Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the 1st and 15th):

| model | star_precision | star_recall | pred_stars | true_stars |
|---|---|---|---|---|
| lvl_cbd | 0.35 [0.29, 0.41] | 0.09 [0.07, 0.10] | 677 | 2760 |
| lvl_clim | 0.36 [0.32, 0.40] | 0.08 [0.06, 0.09] | 1395 | 6524 |
| lvl_naive4 | 0.41 [0.39, 0.42] | 0.30 [0.28, 0.32] | 4773 | 6452 |
| lvl_nf_nhits | 0.38 [0.29, 0.48] | 0.09 [0.06, 0.12] | 100 | 427 |
| lvl_nf_tide | 0.28 [0.20, 0.37] | 0.09 [0.06, 0.13] | 143 | 427 |
| lvl_snaive7 | 0.31 [0.27, 0.35] | 0.33 [0.28, 0.38] | 445 | 414 |
| lvl_tft | 0.36 [0.33, 0.39] | 0.23 [0.21, 0.26] | 2036 | 3104 |
| lvl_wt56 | 0.38 [0.34, 0.42] | 0.08 [0.07, 0.10] | 1415 | 6535 |

## D8 — uncertainty

**What production states** (`forecast-accuracy.service.ts`): MAE of TFT's predicted peak against the day's max hourly P90, by predicted band × lead bucket over the 45 days before the origin (cells with ≥ 500 comparisons). Realised = the same error of the TFT forecasts served at the origin; ratio > 1 = production understates its error.

| bucket | band | n | realised_mae | stated_mae | ratio |
|---|---|---|---|---|---|
| d1 | busy | 8406.00 | 22.46 | 22.65 | 0.99 |
| d1 | mid | 31992.00 | 14.38 | 14.71 | 0.98 |
| d1 | quiet | 100003.00 | 6.80 | 6.88 | 0.99 |
| d14 | busy | 57026.00 | 23.97 | 25.24 | 0.95 |
| d14 | mid | 207754.00 | 16.05 | 15.68 | 1.02 |
| d14 | quiet | 648404.00 | 8.03 | 7.49 | 1.07 |
| d3 | busy | 17234.00 | 22.82 | 23.03 | 0.99 |
| d3 | mid | 63815.00 | 14.65 | 14.81 | 0.99 |
| d3 | quiet | 197897.00 | 6.95 | 6.96 | 1.00 |
| d30 | busy | 119064.00 | 24.55 | 25.57 | 0.96 |
| d30 | mid | 439220.00 | 16.72 | 15.95 | 1.05 |
| d30 | quiet | 1299039.00 | 8.71 | 7.75 | 1.12 |
| d60 | busy | 87674.00 | 28.12 | 22.88 | 1.23 |
| d60 | mid | 278537.00 | 17.12 | 17.05 | 1.00 |
| d60 | quiet | 728857.00 | 7.52 | 9.64 | 0.78 |
| d7 | busy | 34642.00 | 23.27 | 23.93 | 0.97 |
| d7 | mid | 125051.00 | 15.06 | 15.07 | 1.00 |
| d7 | quiet | 387040.00 | 7.24 | 7.06 | 1.03 |

Empirical-quantile coverage (target 0.80 / 0.95) and the share of truth slots that HAVE an interval:

| L | wt_med cov_q80 | wt_med cov_q95 | wt_med field_coverage | nf_tide cov_q80 | nf_tide cov_q95 | nf_tide field_coverage | nf_nhits cov_q80 | nf_nhits cov_q95 | nf_nhits field_coverage |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.84 | 0.92 | 0.96 | 0.80 | 0.93 | 0.96 | 0.77 | 0.91 | 0.96 |
| 1 | 0.84 | 0.92 | 0.96 | 0.80 | 0.93 | 0.96 | 0.78 | 0.91 | 0.96 |
| 2 | 0.84 | 0.92 | 0.96 | 0.80 | 0.93 | 0.95 | 0.78 | 0.91 | 0.95 |
| 3 | 0.84 | 0.92 | 0.96 | 0.81 | 0.93 | 0.95 | 0.78 | 0.91 | 0.95 |
| 4 | 0.84 | 0.92 | 0.95 | 0.81 | 0.94 | 0.95 | 0.78 | 0.91 | 0.95 |
| 5 | 0.84 | 0.92 | 0.95 | 0.80 | 0.94 | 0.95 | 0.78 | 0.91 | 0.95 |
| 6 | 0.84 | 0.92 | 0.95 | 0.80 | 0.93 | 0.95 | 0.77 | 0.91 | 0.95 |
| 7 | 0.83 | 0.92 | 0.95 | 0.81 | 0.94 | 0.95 | 0.77 | 0.91 | 0.95 |
| 10 | 0.83 | 0.91 | 0.95 |  |  | 0.00 |  |  | 0.00 |
| 14 | 0.82 | 0.91 | 0.94 |  |  | 0.00 |  |  | 0.00 |
| 21 | 0.82 | 0.90 | 0.93 |  |  | 0.00 |  |  | 0.00 |
| 30 | 0.82 | 0.90 | 0.92 |  |  | 0.00 |  |  | 0.00 |
| 45 | 0.83 | 0.90 | 0.91 |  |  | 0.00 |  |  | 0.00 |
| 60 | 0.83 | 0.91 | 0.89 |  |  | 0.00 |  |  | 0.00 |
| 90 | 0.84 | 0.91 | 0.86 |  |  | 0.00 |  |  | 0.00 |

Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:

| model | L | realised | stated_weighted | ratio |
|---|---|---|---|---|
| snaive7 | 0 | 8.76 | 8.93 | 0.98 |
| snaive7 | 1 | 8.75 | 8.95 | 0.98 |
| snaive7 | 3 | 8.72 | 8.96 | 0.97 |
| wt_med | 0 | 7.38 | 7.53 | 0.98 |
| wt_med | 1 | 7.48 | 7.66 | 0.98 |
| wt_med | 3 | 7.56 | 7.72 | 0.98 |
| wt_med | 7 | 7.73 | 7.95 | 0.97 |
| wt_med | 14 | 7.91 | 8.24 | 0.96 |
| wt_med | 30 | 8.15 | 8.64 | 0.94 |
| wt_med | 60 | 8.44 | 8.62 | 0.98 |
| wt_med | 90 | 7.97 | 8.96 | 0.89 |
| clim | 0 | 8.00 | 8.17 | 0.98 |
| clim | 1 | 8.01 | 8.18 | 0.98 |
| clim | 3 | 8.05 | 8.20 | 0.98 |
| clim | 7 | 8.13 | 8.25 | 0.99 |
| clim | 14 | 8.29 | 8.43 | 0.98 |
| clim | 30 | 8.42 | 8.73 | 0.96 |
| clim | 60 | 8.27 | 8.62 | 0.96 |
| clim | 90 | 8.24 | 8.89 | 0.93 |
| h5 | 0 | 7.33 | 7.47 | 0.98 |
| h5 | 1 | 7.43 | 7.59 | 0.98 |
| h5 | 3 | 7.50 | 7.64 | 0.98 |
| h5 | 7 | 7.66 | 7.84 | 0.98 |
| h5 | 14 | 7.82 | 8.11 | 0.96 |
| h5 | 30 | 8.04 | 8.47 | 0.95 |
| h5 | 60 | 8.31 | 8.39 | 0.99 |
| h5 | 90 | 7.89 | 8.70 | 0.91 |
| lvlh5_naive | 0 | 7.72 | 7.85 | 0.98 |
| lvlh5_naive | 1 | 7.74 | 7.87 | 0.98 |
| lvlh5_naive | 3 | 7.75 | 7.87 | 0.99 |
| lvlh5_naive | 7 | 8.03 | 8.17 | 0.98 |
| lvlh5_naive | 14 | 8.23 | 8.41 | 0.98 |
| lvlh5_naive | 30 | 8.45 | 8.82 | 0.96 |
| lvlh5_naive | 60 | 8.80 | 8.74 | 1.01 |
| lvlh5_naive | 90 | 8.43 | 8.80 | 0.96 |
| lvlh5_tft | 0 | 7.07 | 7.20 | 0.98 |
| lvlh5_tft | 1 | 7.22 | 7.29 | 0.99 |
| lvlh5_tft | 3 | 7.38 | 7.41 | 1.00 |
| lvlh5_tft | 7 | 7.67 | 7.62 | 1.01 |
| lvlh5_tft | 14 | 8.14 | 7.91 | 1.03 |
| lvlh5_tft | 30 | 8.89 | 8.39 | 1.06 |
| lvlh5_cbd | 0 | 9.71 | 9.33 | 1.04 |
| lvlh5_cbd | 1 | 9.85 | 9.44 | 1.04 |
| lvlh5_cbd | 3 | 10.01 | 9.39 | 1.07 |
| lvlh5_cbd | 7 | 10.52 | 9.84 | 1.07 |
| lvlh5_cbd | 14 | 11.04 | 9.98 | 1.11 |
| lvlh5_cbd | 30 | 11.64 | 10.60 | 1.10 |
| prod_served | 3 | 7.82 | 7.88 | 0.99 |
| prod_served | 7 | 8.11 | 8.14 | 1.00 |
| prod_served | 14 | 8.55 | 8.49 | 1.01 |
| prod_served | 30 | 9.50 | 9.28 | 1.02 |
| prod_served_lin | 3 | 7.80 | 7.86 | 0.99 |
| prod_served_lin | 7 | 8.09 | 8.13 | 1.00 |
| prod_served_lin | 14 | 8.54 | 8.48 | 1.01 |
| prod_served_lin | 30 | 9.48 | 9.27 | 1.02 |
| nf_tide | 0 | 7.39 | 7.52 | 0.98 |
| nf_tide | 1 | 7.83 | 7.97 | 0.98 |
| nf_tide | 3 | 8.00 | 8.16 | 0.98 |
| nf_tide | 7 | 8.18 | 8.45 | 0.97 |
| nf_tide_x_h5 | 1 | 7.92 | 8.02 | 0.99 |
| nf_tide_x_h5 | 3 | 8.07 | 8.12 | 0.99 |
| nf_tide_x_h5 | 7 | 8.19 | 8.31 | 0.99 |
| nf_nhits | 0 | 8.50 | 8.68 | 0.98 |
| nf_nhits | 1 | 8.55 | 8.73 | 0.98 |
| nf_nhits | 3 | 8.74 | 8.92 | 0.98 |
| nf_nhits | 7 | 8.70 | 8.93 | 0.97 |
| nf_nhits_x_h5 | 1 | 8.56 | 8.65 | 0.99 |
| nf_nhits_x_h5 | 3 | 8.71 | 8.73 | 1.00 |
| nf_nhits_x_h5 | 7 | 8.67 | 8.65 | 1.00 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | nf_nhits | nf_nhits_x_h5 | nf_tide | nf_tide_x_h5 | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.88 | 0.97 | 0.41 | 0.91 | 0.55 | 0.96 |  | 0.96 |  | 0.97 | 0.91 |  |  | 0.82 | 0.96 |
| 1 | 0.88 | 0.97 | 0.41 | 0.91 | 0.55 | 0.96 | 0.94 | 0.96 | 0.94 | 0.96 | 0.91 |  |  | 0.82 | 0.96 |
| 2 | 0.87 | 0.97 | 0.42 | 0.92 | 0.55 | 0.95 | 0.94 | 0.95 | 0.94 | 0.96 | 0.91 |  |  | 0.83 | 0.96 |
| 3 | 0.87 | 0.97 | 0.42 | 0.92 | 0.55 | 0.95 | 0.94 | 0.95 | 0.94 | 0.96 | 0.92 | 0.54 | 0.54 | 0.83 | 0.96 |
| 4 | 0.87 | 0.96 | 0.42 | 0.92 | 0.55 | 0.95 | 0.94 | 0.95 | 0.94 | 0.96 | 0.92 | 0.53 | 0.53 | 0.83 | 0.95 |
| 5 | 0.87 | 0.96 | 0.42 | 0.92 | 0.55 | 0.95 | 0.94 | 0.95 | 0.94 | 0.96 | 0.92 | 0.53 | 0.53 | 0.84 | 0.95 |
| 6 | 0.87 | 0.96 | 0.42 | 0.92 | 0.55 | 0.95 | 0.94 | 0.95 | 0.94 | 0.96 | 0.92 | 0.53 | 0.53 | 0.84 | 0.95 |
| 7 | 0.87 | 0.96 | 0.42 | 0.90 | 0.55 | 0.95 | 0.94 | 0.95 | 0.94 | 0.96 | 0.89 | 0.53 | 0.53 |  | 0.95 |
| 10 | 0.86 | 0.96 | 0.41 | 0.90 | 0.55 |  |  |  |  | 0.95 | 0.90 | 0.53 | 0.53 |  | 0.95 |
| 14 | 0.86 | 0.95 | 0.41 | 0.88 | 0.54 |  |  |  |  | 0.95 | 0.88 | 0.53 | 0.53 |  | 0.94 |
| 21 | 0.85 | 0.95 | 0.40 | 0.87 | 0.53 |  |  |  |  | 0.94 | 0.87 | 0.52 | 0.52 |  | 0.93 |
| 30 | 0.85 | 0.94 | 0.39 | 0.87 | 0.41 |  |  |  |  | 0.94 | 0.87 | 0.43 | 0.43 |  | 0.92 |
| 45 | 0.83 | 0.93 | 0.12 | 0.86 | 0.30 |  |  |  |  | 0.93 | 0.86 | 0.33 | 0.33 |  | 0.91 |
| 60 | 0.82 | 0.91 |  | 0.84 | 0.00 |  |  |  |  | 0.91 | 0.84 | 0.00 | 0.00 |  | 0.89 |
| 90 | 0.77 | 0.88 |  | 0.81 |  |  |  |  |  | 0.88 | 0.81 |  |  |  | 0.86 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | lvl_nf_tide | lvl_nf_nhits | n_ride_days |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.91 | 0.98 | 0.91 | 0.91 | 0.56 | 0.42 | 0.96 | 0.96 | 102570 |
| 2 | 0.92 | 0.98 | 0.91 | 0.91 | 0.56 | 0.42 | 0.95 | 0.95 | 102843 |
| 3 | 0.92 | 0.98 | 0.92 | 0.91 | 0.56 | 0.42 | 0.95 | 0.95 | 103040 |
| 4 | 0.92 | 0.98 | 0.92 | 0.91 | 0.56 | 0.42 | 0.95 | 0.95 | 103195 |
| 5 | 0.93 | 0.98 | 0.92 | 0.91 | 0.56 | 0.42 | 0.95 | 0.95 | 103380 |
| 6 | 0.93 | 0.97 | 0.93 | 0.91 | 0.56 | 0.42 | 0.95 | 0.95 | 103576 |
| 7 | 0.90 | 0.97 | 0.00 | 0.91 | 0.56 | 0.42 | 0.95 | 0.95 | 103672 |
| 10 | 0.91 | 0.97 | 0.00 | 0.91 | 0.56 | 0.42 | 0.00 | 0.00 | 104213 |
| 14 | 0.89 | 0.97 | 0.00 | 0.90 | 0.55 | 0.41 | 0.00 | 0.00 | 104731 |
| 21 | 0.88 | 0.97 | 0.00 | 0.90 | 0.55 | 0.41 | 0.00 | 0.00 | 105643 |
| 30 | 0.89 | 0.97 | 0.00 | 0.90 | 0.41 | 0.39 | 0.00 | 0.00 | 104983 |
| 45 | 0.89 | 0.97 | 0.00 | 0.90 | 0.31 | 0.15 | 0.00 | 0.00 | 102934 |
| 60 | 0.87 | 0.96 | 0.00 | 0.89 | 0.00 | 0.00 | 0.00 | 0.00 | 101777 |
| 90 | 0.86 | 0.96 | 0.00 | 0.87 | 0.00 | 0.00 | 0.00 | 0.00 | 79830 |
| 120 | 0.82 | 0.94 | 0.00 | 0.85 | 0.00 | 0.00 | 0.00 | 0.00 | 57445 |
| 180 | 0.79 | 0.91 | 0.00 | 0.87 | 0.00 | 0.00 | 0.00 | 0.00 | 19826 |

Forecast present vs ride operated. The baselines have **no openness logic** — they forecast whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never opened still got a curve; it is the bar a model with an open/closed decision has to beat:

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | prod_served_lin P(fc|operated) | prod_served_lin P(fc|not operated) | nf_tide P(fc|operated) | nf_tide P(fc|not operated) | nf_tide_x_h5 P(fc|operated) | nf_tide_x_h5 P(fc|not operated) | nf_nhits P(fc|operated) | nf_nhits P(fc|not operated) | nf_nhits_x_h5 P(fc|operated) | nf_nhits_x_h5 P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.84 | 0.25 | 0.95 | 0.71 | 0.87 | 0.65 | 0.95 | 0.72 | 0.90 | 0.60 | 0.54 | 0.35 | 0.40 | 0.22 | 0.00 | 0.00 | 0.00 | 0.00 | 0.92 | 0.57 | 0.00 | 0.00 | 0.92 | 0.57 | 0.00 | 0.00 | 290044.00 | 21142.00 |
| 1 | 0.85 | 0.25 | 0.95 | 0.71 | 0.87 | 0.65 | 0.95 | 0.72 | 0.90 | 0.61 | 0.54 | 0.35 | 0.40 | 0.22 | 0.00 | 0.00 | 0.00 | 0.00 | 0.92 | 0.58 | 0.91 | 0.54 | 0.92 | 0.58 | 0.91 | 0.54 | 290228.00 | 21167.00 |
| 2 | 0.85 | 0.25 | 0.95 | 0.72 | 0.87 | 0.65 | 0.95 | 0.72 | 0.90 | 0.61 | 0.54 | 0.35 | 0.40 | 0.22 | 0.00 | 0.00 | 0.00 | 0.00 | 0.92 | 0.58 | 0.91 | 0.55 | 0.92 | 0.58 | 0.91 | 0.55 | 290654.00 | 21177.00 |
| 3 | 0.85 | 0.25 | 0.94 | 0.72 | 0.87 | 0.65 | 0.95 | 0.73 | 0.90 | 0.61 | 0.54 | 0.35 | 0.40 | 0.22 | 0.51 | 0.28 | 0.51 | 0.28 | 0.92 | 0.59 | 0.91 | 0.55 | 0.92 | 0.59 | 0.91 | 0.55 | 291005.00 | 21273.00 |
| 4 | 0.86 | 0.26 | 0.94 | 0.72 | 0.87 | 0.65 | 0.95 | 0.73 | 0.90 | 0.61 | 0.54 | 0.35 | 0.40 | 0.22 | 0.51 | 0.28 | 0.51 | 0.28 | 0.92 | 0.59 | 0.90 | 0.56 | 0.92 | 0.59 | 0.90 | 0.56 | 291286.00 | 21377.00 |
| 5 | 0.86 | 0.26 | 0.94 | 0.72 | 0.87 | 0.65 | 0.95 | 0.73 | 0.91 | 0.62 | 0.54 | 0.35 | 0.40 | 0.22 | 0.51 | 0.28 | 0.51 | 0.28 | 0.92 | 0.59 | 0.90 | 0.56 | 0.92 | 0.59 | 0.90 | 0.56 | 291689.00 | 21524.00 |
| 6 | 0.87 | 0.26 | 0.94 | 0.72 | 0.87 | 0.65 | 0.95 | 0.73 | 0.91 | 0.62 | 0.54 | 0.35 | 0.40 | 0.22 | 0.51 | 0.28 | 0.51 | 0.28 | 0.92 | 0.60 | 0.90 | 0.56 | 0.92 | 0.60 | 0.90 | 0.56 | 292200.00 | 21562.00 |
| 7 | 0.00 | 0.00 | 0.94 | 0.73 | 0.87 | 0.65 | 0.94 | 0.73 | 0.90 | 0.62 | 0.54 | 0.35 | 0.40 | 0.22 | 0.51 | 0.29 | 0.51 | 0.29 | 0.91 | 0.60 | 0.90 | 0.56 | 0.91 | 0.60 | 0.90 | 0.56 | 292411.00 | 21634.00 |
| 10 | 0.00 | 0.00 | 0.94 | 0.72 | 0.87 | 0.65 | 0.95 | 0.73 | 0.90 | 0.62 | 0.54 | 0.35 | 0.40 | 0.22 | 0.51 | 0.28 | 0.51 | 0.28 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 293250.00 | 21770.00 |
| 14 | 0.00 | 0.00 | 0.94 | 0.73 | 0.87 | 0.66 | 0.94 | 0.74 | 0.89 | 0.63 | 0.53 | 0.34 | 0.40 | 0.21 | 0.50 | 0.28 | 0.50 | 0.28 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 294485.00 | 21844.00 |
| 21 | 0.00 | 0.00 | 0.94 | 0.75 | 0.87 | 0.67 | 0.94 | 0.76 | 0.89 | 0.65 | 0.52 | 0.33 | 0.39 | 0.22 | 0.49 | 0.29 | 0.49 | 0.29 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 296992.00 | 21934.00 |
| 30 | 0.00 | 0.00 | 0.94 | 0.76 | 0.87 | 0.68 | 0.94 | 0.77 | 0.89 | 0.67 | 0.40 | 0.28 | 0.38 | 0.24 | 0.41 | 0.27 | 0.41 | 0.27 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 297975.00 | 22553.00 |
| 45 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.70 | 0.94 | 0.79 | 0.88 | 0.69 | 0.30 | 0.27 | 0.13 | 0.10 | 0.32 | 0.25 | 0.32 | 0.25 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 294132.00 | 23832.00 |
| 60 | 0.00 | 0.00 | 0.93 | 0.80 | 0.85 | 0.72 | 0.93 | 0.81 | 0.88 | 0.72 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 287573.00 | 24358.00 |
| 90 | 0.00 | 0.00 | 0.93 | 0.83 | 0.84 | 0.72 | 0.93 | 0.84 | 0.88 | 0.74 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 223863.00 | 19451.00 |
