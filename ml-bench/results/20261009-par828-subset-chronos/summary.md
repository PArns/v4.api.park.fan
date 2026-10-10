# ml-bench results

Run: `20261009-par828-subset-chronos` · generated 2026-10-10T06:11+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with season — the headline horizon curves come from the common-window report.

- code: git `unknown`, sources sha256 `d82b5a17a2b7f815`, image `sha256:8df8c8d09abc`; plug-ins ['chronos2', 'chronos2_grid', 'chronos2_nocov', 'chronos2_owx']
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 109.1 shard-minutes over 1 shard(s)
- Values: point [95 % park-day bootstrap, 1000 reps]. Paired difference vs the per-lead reference: point [95 % **park-cluster** bootstrap] — `*` = the model wins (that CI excludes 0 in its favour). Park-day CIs of the differences are in `cells.csv`.
- MAE is in minutes on 15-min slots, each model on its own coverage — compare models through the paired difference, never through two unpaired values.
- Opening-aligned forecasts use the window KNOWN at the origin: the published one if its schedule row was last written before the origin, else a projection from the last 56 days. Weather: no forecast archive exists; baselines use none, plug-ins only by opting in (scored as `<name>_owx`).

## Horizon — the headline

### Lead availability (full period)

| lead | origin_days | status | measurable_from |
|---|---|---|---|
| 0 | 231 | ok |  |
| 1 | 230 | ok |  |
| 2 | 229 | ok |  |
| 3 | 228 | ok |  |
| 4 | 227 | ok |  |
| 5 | 226 | ok |  |
| 6 | 225 | ok |  |
| 7 | 224 | ok |  |
| 10 | 221 | ok |  |
| 14 | 217 | ok |  |
| 21 | 210 | ok |  |
| 30 | 201 | ok |  |
| 45 | 186 | ok |  |
| 60 | 171 | ok |  |
| 90 | 141 | ok |  |
| 120 | 111 | ok |  |
| 180 | 51 | ok |  |
| 270 | 0 | not measurable yet | 2026-12-15 |
| 365 | 0 | not measurable yet | 2027-03-20 |

### Coverage horizon of the inputs

| input | horizon_days | note |
|---|---|---|
| TFT daily level | 60 | last run 2026-10-08 00:00:00; served only if <= 3 days stale |
| CatBoost daily level | 45 | last run 2026-10-09 00:00:00; served only if <= 3 days stale |
| weather forecast (Open-Meteo) | 16 | rows 2026-10-10 00:00:00..2026-10-24 00:00:00 only; NO archive -> only actuals exist (ORACLE, stripped unless a plug-in opts in) |
| published operator schedule (per park) | 39 | p10 -32 d, median 39 d, p90 155 d past the last truth day; a window last written after the origin is treated as unknown and projected (schedule_entries.updatedAt) |

### Usable horizon

Largest lead, contiguous from the model's first scored lead, at which it wins (park-cluster CI); cells need ≥ 30 origin days and paired rows ≥ 30 % of the reference's. Empty = never.

| metric | uc | segment | model | leads_tested | usable_horizon | max_significant_lead |
|---|---|---|---|---|---|---|
| MAE | UC1 | all | chronos2 | 8 |  | m120 |
| MAE | UC1 | busy | chronos2 | 8 |  | m120 |
| MAE | UC2 | all | chronos2 | 2 | 1 | 1 |
| MAE | UC2 | all | chronos2_grid | 2 | 1 | 1 |
| MAE | UC2 | all | chronos2_nocov | 2 | 0 | 0 |
| MAE | UC2 | all | chronos2_owx | 2 | 1 | 1 |
| MAE | UC2 | all | chronos2_owx_x_h5 | 1 | 1 | 1 |
| MAE | UC2 | all | chronos2_x_h5 | 1 | 1 | 1 |
| MAE | UC2 | all | h5 | 2 | 1 | 1 |
| MAE | UC2 | busy | chronos2 | 2 | 1 | 1 |
| MAE | UC2 | busy | chronos2_grid | 2 | 1 | 1 |
| MAE | UC2 | busy | chronos2_nocov | 2 | 0 | 0 |
| MAE | UC2 | busy | chronos2_owx | 2 | 1 | 1 |
| MAE | UC2 | busy | chronos2_owx_x_h5 | 1 | 1 | 1 |
| MAE | UC2 | busy | chronos2_x_h5 | 1 | 1 | 1 |
| MAE | UC2 | busy | h5 | 2 | 1 | 1 |
| MAE | UC2-intraday | all | chronos2 | 3 | h8+ | h8+ |
| MAE | UC2-intraday | all | h5 | 3 | h4-8 | h4-8 |
| MAE | UC2-intraday | busy | chronos2 | 3 | h8+ | h8+ |
| MAE | UC2-intraday | busy | h5 | 3 | h8+ | h8+ |
| MAE | UC3 | all | chronos2 | 7 | 1 | 1 |
| MAE | UC3 | all | chronos2_grid | 7 | 1 | 7 |
| MAE | UC3 | all | chronos2_owx | 7 | 1 | 6 |
| MAE | UC3 | all | chronos2_owx_x_h5 | 10 | 1 | 7 |
| MAE | UC3 | all | chronos2_x_h5 | 10 | 1 | 7 |
| MAE | UC3 | all | h5 | 10 | 21 | 21 |
| MAE | UC3 | busy | chronos2 | 7 | 1 | 6 |
| MAE | UC3 | busy | chronos2_grid | 7 | 1 | 7 |
| MAE | UC3 | busy | chronos2_owx | 7 | 1 | 6 |
| MAE | UC3 | busy | chronos2_owx_x_h5 | 10 | 1 | 21 |
| MAE | UC3 | busy | chronos2_x_h5 | 10 | 1 | 21 |
| MAE | UC3 | busy | h5 | 10 | 21 | 21 |
| MAE | UC3 | busy | lvlh5_naive | 10 |  | 10 |
| MAE, schedule known at origin | UC3 | all | chronos2 | 8 | 1 | 6 |
| MAE, schedule known at origin | UC3 | all | chronos2_grid | 8 | 1 | 7 |
| MAE, schedule known at origin | UC3 | all | chronos2_nocov | 8 | 0 | 0 |
| MAE, schedule known at origin | UC3 | all | chronos2_owx | 8 | 1 | 6 |
| MAE, schedule known at origin | UC3 | all | chronos2_owx_x_h5 | 10 | 1 | 21 |
| MAE, schedule known at origin | UC3 | all | chronos2_x_h5 | 10 | 1 | 14 |
| MAE, schedule known at origin | UC3 | all | h5 | 11 | 1 | 21 |
| MAE, schedule projected at origin | UC3 | all | chronos2 | 8 | 1 | 1 |
| MAE, schedule projected at origin | UC3 | all | chronos2_grid | 8 | 1 | 1 |
| MAE, schedule projected at origin | UC3 | all | chronos2_owx | 8 | 1 | 1 |
| MAE, schedule projected at origin | UC3 | all | chronos2_owx_x_h5 | 10 | 1 | 1 |
| MAE, schedule projected at origin | UC3 | all | chronos2_x_h5 | 10 | 1 | 1 |
| MAE, schedule projected at origin | UC3 | all | h5 | 11 |  | 10 |
| Spearman of slots within ride-day | UC2/UC3 | all | chronos2_grid | 8 | 1 | 6 |
| Spearman of slots within ride-day | UC2/UC3 | all | chronos2_owx_x_h5 | 10 |  | 4 |
| Spearman of slots within ride-day | UC2/UC3 | all | chronos2_x_h5 | 10 |  | 4 |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | 11 |  | 21 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | 11 |  | 21 |
| best-time hit rate (top-2) | D3 | all | h5 | 11 |  | 7 |
| best-time hit rate (top-2) | D3 | all | wt_med | 8 |  | 10 |
| best-time hit rate (±30 min) | D3 | all | clim | 6 |  | 2 |
| best-time hit rate (±30 min) | D3 | all | h5 | 11 | 0 | 7 |
| best-time hit rate (±30 min) | D3 | all | wt_med | 6 |  | 2 |
| best-time regret (min) | D3 | all | h5 | 11 | 0 | 21 |
| crowd bucket exact | D6 | all | lvl_chronos2 | 10 | 1 | 7 |
| crowd bucket exact | D6 | all | lvl_chronos2_owx | 10 | 1 | 7 |
| crowd bucket exact | D6 | all | lvl_tft | 10 |  | 7 |
| dayPeak Spearman across rides | UC3 | all | h5 | 11 |  | 10 |
| dayPeak Spearman across rides | UC3 | all | wt_med | 1 |  | 10 |
| dayPeak abs error (min) | UC3 | all | chronos2 | 8 | 0 | 0 |
| dayPeak abs error (min) | UC3 | all | chronos2_grid | 8 | 0 | 6 |
| dayPeak abs error (min) | UC3 | all | chronos2_owx | 8 | 0 | 6 |
| dayPeak abs error (min) | UC3 | all | chronos2_owx_x_h5 | 10 | 1 | 7 |
| dayPeak abs error (min) | UC3 | all | chronos2_x_h5 | 10 | 1 | 7 |
| dayPeak abs error (min) | UC3 | all | lvlh5_naive | 11 |  | 2 |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2 | 8 | 7 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_grid | 8 | 7 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_nocov | 8 | 7 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_owx | 8 | 7 | 7 |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_owx_x_h5 | 10 | 21 | 21 |
| dayPeak pairwise ordering (headliners) | D4 | all | chronos2_x_h5 | 10 | 21 | 21 |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | 11 | 21 | 21 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | 11 | 21 | 21 |
| first-hour MAE (opening-aligned) | D5 | all | chronos2_grid | 8 | 1 | 1 |
| first-hour MAE (opening-aligned) | D5 | all | chronos2_owx_x_h5 | 10 | 1 | 21 |
| first-hour MAE (opening-aligned) | D5 | all | chronos2_x_h5 | 10 | 1 | 21 |
| first-hour MAE (opening-aligned) | D5 | all | h5 | 11 | 21 | 21 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | 11 |  | 21 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | 11 | 21 | 21 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | 11 | 1 | 21 |
| first-hour MAE, schedule known at origin | D5 | all | chronos2_grid | 8 | 0 | 0 |
| first-hour MAE, schedule known at origin | D5 | all | chronos2_owx_x_h5 | 10 | 21 | 21 |
| first-hour MAE, schedule known at origin | D5 | all | chronos2_x_h5 | 10 | 21 | 21 |
| first-hour MAE, schedule known at origin | D5 | all | h5 | 11 | 21 | 21 |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | 11 |  | 21 |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | 11 | 21 | 21 |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | 11 | 3 | 21 |
| first-hour MAE, schedule projected at origin | D5 | all | wt_med | 1 |  | 4 |
| live-window MAE (headliners) | D2 | headliners | chronos2 | 1 | w0-45 | w0-45 |
| next-best precision | D1 | all | h5 | 2 | 0-120 | 0-120 |
| next-best precision | D1 | all | lvlh5_tft | 2 | 0-120 | 0-120 |
| next-best precision (top-3) | D1 | all | chronos2 | 2 | 0-120 | 0-120 |
| optimiser regret (min per park-day) | D4 | all | chronos2_owx_x_h5 | 4 |  | 7 |
| optimiser regret (min per park-day) | D4 | all | chronos2_x_h5 | 4 |  | 7 |
| rope-drop worth agreement | D5 | all | chronos2_grid | 6 |  | 3 |
| rope-drop worth agreement | D5 | all | chronos2_owx_x_h5 | 9 |  | 4 |
| rope-drop worth agreement | D5 | all | chronos2_x_h5 | 9 |  | 4 |
| rope-drop worth agreement | D5 | all | clim | 9 |  | 4 |
| rope-drop worth agreement | D5 | all | h5 | 9 |  | 3 |
| rope-drop worth agreement | D5 | all | lvlh5_naive | 9 |  | 3 |
| rope-drop worth agreement | D5 | all | wt_med | 5 |  | 4 |

### Hand-over table (input for the serving router)

Per use case × lead: among models that win against the reference (and pass the gates), the largest PAIRED margin; otherwise the reference itself.

| uc | segment | lead | reference | winner | winner_value | paired_margin | significant_models | n_origin_days |
|---|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | wt_med | chronos2_grid | 6.00 | -1.02 | 5 | 33 |
| UC2 | busy | 0 | wt_med | chronos2_owx | 11.68 | -2.48 | 5 | 33 |
| UC3 | all | 1 | wt_med | chronos2_owx | 6.48 | -0.63 | 6 | 33 |
| UC3 | busy | 1 | wt_med | chronos2_owx | 12.69 | -1.50 | 6 | 33 |
| UC3 | all | 2 | wt_med | h5 | 7.03 | -0.03 | 1 | 33 |
| UC3 | busy | 2 | wt_med | h5 | 13.66 | -0.13 | 1 | 33 |
| UC3 | all | 3 | wt_med | h5 | 6.80 | -0.10 | 1 | 33 |
| UC3 | busy | 3 | wt_med | chronos2_owx_x_h5 | 13.70 | -0.65 | 4 | 33 |
| UC3 | all | 4 | wt_med | chronos2_grid | 7.20 | -0.43 | 5 | 33 |
| UC3 | busy | 4 | wt_med | chronos2_owx_x_h5 | 14.00 | -1.12 | 6 | 33 |
| UC3 | all | 5 | wt_med | chronos2_grid | 7.14 | -0.43 | 4 | 33 |
| UC3 | busy | 5 | wt_med | chronos2_owx_x_h5 | 13.89 | -1.05 | 4 | 33 |
| UC3 | all | 6 | wt_med | chronos2_grid | 6.83 | -0.44 | 5 | 33 |
| UC3 | busy | 6 | wt_med | chronos2_grid | 13.69 | -1.17 | 7 | 33 |
| UC3 | all | 7 | wt_med | chronos2_grid | 7.24 | -0.23 | 4 | 32 |
| UC3 | busy | 7 | wt_med | chronos2_owx_x_h5 | 14.14 | -0.85 | 4 | 32 |
| UC3 | all | 10 | wt_med | h5 | 6.96 | -0.11 | 1 | 32 |
| UC3 | busy | 10 | wt_med | lvlh5_naive | 13.91 | -0.65 | 4 | 32 |
| UC3 | all | 14 | wt_med | h5 | 7.69 | -0.08 | 1 | 31 |
| UC3 | busy | 14 | wt_med | chronos2_owx_x_h5 | 14.96 | -0.62 | 3 | 31 |
| UC3 | all | 21 | wt_med | h5 | 7.83 | -0.08 | 1 | 30 |
| UC3 | busy | 21 | wt_med | chronos2_owx_x_h5 | 15.31 | -0.50 | 3 | 30 |
| UC3 | all | 30 | wt_med | wt_med | 7.76 | 0.00 | 0 | 29 |
| UC3 | busy | 30 | wt_med | wt_med | 14.71 | 0.00 | 0 | 29 |
| UC3 | all | 45 | wt_med | wt_med | 7.80 | 0.00 | 0 | 27 |
| UC3 | busy | 45 | wt_med | wt_med | 15.95 | 0.00 | 0 | 27 |
| UC3 | all | 60 | wt_med | wt_med | 8.34 | 0.00 | 0 | 25 |
| UC3 | busy | 60 | wt_med | wt_med | 16.13 | 0.00 | 0 | 25 |
| UC3 | all | 90 | wt_med | wt_med | 8.34 | 0.00 | 0 | 21 |
| UC3 | busy | 90 | wt_med | wt_med | 16.65 | 0.00 | 0 | 21 |
| UC2 | all | 1 | wt_med | chronos2_owx | 6.48 | -0.63 | 6 | 33 |
| UC2 | busy | 1 | wt_med | chronos2_owx | 12.69 | -1.50 | 6 | 33 |
| UC1 | all | m015 | persistence | persistence | 2.64 | 0.00 | 0 | 33 |
| UC1 | busy | m015 | persistence | persistence | 5.18 | 0.00 | 0 | 33 |
| UC1 | all | m030 | persistence | chronos2 | 3.86 | -0.42 | 1 | 33 |
| UC1 | busy | m030 | persistence | chronos2 | 7.61 | -0.92 | 1 | 33 |
| UC1 | all | m045 | persistence | chronos2 | 4.24 | -0.90 | 1 | 33 |
| UC1 | busy | m045 | persistence | chronos2 | 8.27 | -1.87 | 1 | 33 |
| UC1 | all | m060 | persistence | chronos2 | 4.49 | -1.26 | 1 | 33 |
| UC1 | busy | m060 | persistence | chronos2 | 8.74 | -2.62 | 1 | 33 |
| UC1 | all | m075 | persistence | chronos2 | 4.72 | -1.56 | 1 | 33 |
| UC1 | busy | m075 | persistence | chronos2 | 9.10 | -3.07 | 1 | 33 |
| UC1 | all | m090 | persistence | chronos2 | 4.84 | -1.88 | 1 | 33 |
| UC1 | busy | m090 | persistence | chronos2 | 9.37 | -3.76 | 1 | 33 |
| UC1 | all | m105 | persistence | chronos2 | 4.95 | -2.06 | 1 | 33 |
| UC1 | busy | m105 | persistence | chronos2 | 9.55 | -4.06 | 1 | 33 |
| UC1 | all | m120 | persistence | chronos2 | 5.02 | -2.27 | 1 | 33 |
| UC1 | busy | m120 | persistence | chronos2 | 9.69 | -4.51 | 1 | 33 |
| UC2-intraday | all | h2-4 | wt_med | chronos2 | 5.32 | -1.76 | 2 | 33 |
| UC2-intraday | busy | h2-4 | wt_med | chronos2 | 10.22 | -4.09 | 2 | 33 |
| UC2-intraday | all | h4-8 | wt_med | chronos2 | 5.80 | -1.30 | 2 | 33 |
| UC2-intraday | busy | h4-8 | wt_med | chronos2 | 10.98 | -3.08 | 2 | 33 |
| UC2-intraday | all | h8+ | wt_med | chronos2 | 5.98 | -0.52 | 1 | 33 |
| UC2-intraday | busy | h8+ | wt_med | chronos2 | 11.10 | -1.35 | 2 | 33 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | significant_models |
|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | chronos2_grid | 4.18 | -0.55 | 4 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | lvlh5_tft | 4.09 | -0.35 | 6 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | h5 | 4.35 | -0.11 | 2 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | h5 | 4.57 | -0.16 | 4 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | h5 | 4.76 | -0.14 | 4 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | chronos2_owx_x_h5 | 4.87 | -0.12 | 4 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | h5 | 4.54 | -0.16 | 5 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | lvlh5_tft | 4.38 | -0.24 | 6 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | h5 | 4.66 | -0.17 | 4 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | lvlh5_tft | 4.55 | -0.27 | 6 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | lvlh5_tft | 4.77 | -0.23 | 6 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | wt_med | 4.77 | 0.00 | 0 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | wt_med | 5.17 | 0.00 | 0 |
| first-hour MAE (opening-aligned) | D5 | 60 | wt_med | wt_med | 5.30 | 0.00 | 0 |
| first-hour MAE (opening-aligned) | D5 | 90 | clim | clim | 5.36 | 0.00 | 0 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | chronos2_owx | 6.01 | -1.14 | 5 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | chronos2_owx | 6.47 | -0.68 | 6 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | wt_med | 7.25 | 0.00 | 0 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 7.05 | -0.10 | 1 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | chronos2_grid | 7.22 | -0.50 | 6 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | chronos2_grid | 7.18 | -0.50 | 4 |
| MAE, schedule known at origin | UC3 | 6 | wt_med | chronos2_grid | 6.86 | -0.49 | 6 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | chronos2_grid | 7.31 | -0.33 | 4 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | h5 | 7.02 | -0.12 | 1 |
| MAE, schedule known at origin | UC3 | 14 | wt_med | chronos2_owx_x_h5 | 7.49 | -0.25 | 3 |
| MAE, schedule known at origin | UC3 | 21 | wt_med | chronos2_owx_x_h5 | 7.53 | -0.24 | 2 |
| MAE, schedule known at origin | UC3 | 30 | wt_med | wt_med | 7.50 | 0.00 | 0 |
| MAE, schedule known at origin | UC3 | 45 | wt_med | wt_med | 7.23 | 0.00 | 0 |
| MAE, schedule known at origin | UC3 | 60 | wt_med | wt_med | 7.71 | 0.00 | 0 |
| MAE, schedule known at origin | UC3 | 90 | wt_med | wt_med | 7.07 | 0.00 | 0 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | chronos2_grid | 4.31 | -0.64 | 4 |
| first-hour MAE, schedule known at origin | D5 | 1 | wt_med | lvlh5_tft | 4.25 | -0.45 | 5 |
| first-hour MAE, schedule known at origin | D5 | 2 | wt_med | lvlh5_tft | 4.14 | -0.21 | 6 |
| first-hour MAE, schedule known at origin | D5 | 3 | wt_med | lvlh5_tft | 4.40 | -0.22 | 6 |
| first-hour MAE, schedule known at origin | D5 | 4 | wt_med | chronos2_owx_x_h5 | 4.88 | -0.19 | 4 |
| first-hour MAE, schedule known at origin | D5 | 5 | wt_med | chronos2_owx_x_h5 | 5.05 | -0.15 | 4 |
| first-hour MAE, schedule known at origin | D5 | 6 | wt_med | lvlh5_cbd | 4.66 | -0.21 | 5 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | lvlh5_tft | 4.62 | -0.31 | 5 |
| first-hour MAE, schedule known at origin | D5 | 10 | wt_med | lvlh5_cbd | 4.48 | -0.30 | 6 |
| first-hour MAE, schedule known at origin | D5 | 14 | wt_med | lvlh5_tft | 4.53 | -0.36 | 6 |
| first-hour MAE, schedule known at origin | D5 | 21 | wt_med | lvlh5_tft | 4.59 | -0.33 | 6 |
| first-hour MAE, schedule known at origin | D5 | 30 | wt_med | wt_med | 4.61 | 0.00 | 0 |
| first-hour MAE, schedule known at origin | D5 | 45 | wt_med | wt_med | 4.99 | 0.00 | 0 |
| first-hour MAE, schedule known at origin | D5 | 60 | wt_med | wt_med | 4.69 | 0.00 | 0 |
| first-hour MAE, schedule known at origin | D5 | 90 | wt_med | wt_med | 4.42 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 0 | wt_med | chronos2_grid | 5.95 | -0.64 | 3 |
| MAE, schedule projected at origin | UC3 | 1 | wt_med | chronos2_grid | 6.27 | -0.70 | 5 |
| MAE, schedule projected at origin | UC3 | 2 | wt_med | wt_med | 6.55 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 3 | wt_med | h5 | 5.99 | -0.11 | 1 |
| MAE, schedule projected at origin | UC3 | 4 | wt_med | wt_med | 7.30 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 5 | wt_med | wt_med | 7.23 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 6 | wt_med | wt_med | 7.00 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | wt_med | 6.93 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | h5 | 6.78 | -0.10 | 1 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | wt_med | 7.86 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 21 | wt_med | wt_med | 8.26 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 30 | wt_med | wt_med | 8.14 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 45 | wt_med | wt_med | 8.58 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 60 | clim | clim | 8.89 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 90 | wt_med | wt_med | 9.17 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 0 | wt_med | wt_med | 3.92 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 1 | wt_med | wt_med | 4.04 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 2 | wt_med | wt_med | 3.80 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 3 | wt_med | wt_med | 3.97 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 4 | clim | wt_med | 4.40 | -0.20 | 1 |
| first-hour MAE, schedule projected at origin | D5 | 5 | wt_med | wt_med | 4.32 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 6 | wt_med | wt_med | 4.28 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | wt_med | 4.17 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 10 | wt_med | wt_med | 4.35 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 14 | wt_med | wt_med | 5.02 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 21 | wt_med | wt_med | 5.39 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 30 | wt_med | wt_med | 5.03 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 45 | clim | clim | 5.40 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 60 | wt_med | wt_med | 5.95 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 90 | clim | clim | 6.11 | 0.00 | 0 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | chronos2 | 6.77 | -0.64 | 1 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | wt_med | 0.83 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | wt_med | 0.82 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 2 | clim | wt_med | 0.83 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 3 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 4 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 5 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 7 | clim | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 10 | clim | wt_med | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 14 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 21 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 30 | clim | clim | 0.84 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 45 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 60 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 90 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 0 | clim | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | wt_med | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 2 | snaive7 | clim | 0.84 | 0.02 | 3 |
| best-time hit rate (±30 min) | D3 | 3 | clim | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | wt_med | 0.82 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 10 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 14 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 21 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 30 | clim | clim | 0.84 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 45 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 60 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 90 | clim | clim | 0.83 | 0.00 | 0 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.93 | -0.27 | 1 |
| best-time regret (min) | D3 | 1 | wt_med | wt_med | 3.23 | 0.00 | 0 |
| best-time regret (min) | D3 | 2 | clim | clim | 3.01 | 0.00 | 0 |
| best-time regret (min) | D3 | 3 | clim | h5 | 3.09 | -0.26 | 1 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 3.19 | -0.18 | 1 |
| best-time regret (min) | D3 | 5 | wt_med | wt_med | 3.21 | 0.00 | 0 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 3.13 | -0.27 | 1 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 3.02 | -0.24 | 1 |
| best-time regret (min) | D3 | 10 | clim | clim | 3.14 | 0.00 | 0 |
| best-time regret (min) | D3 | 14 | clim | h5 | 3.15 | -0.18 | 1 |
| best-time regret (min) | D3 | 21 | clim | h5 | 3.22 | -0.20 | 1 |
| best-time regret (min) | D3 | 30 | clim | clim | 3.10 | 0.00 | 0 |
| best-time regret (min) | D3 | 45 | clim | clim | 3.05 | 0.00 | 0 |
| best-time regret (min) | D3 | 60 | clim | clim | 3.33 | 0.00 | 0 |
| best-time regret (min) | D3 | 90 | clim | clim | 3.29 | 0.00 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | chronos2_grid | 0.48 | 0.02 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | chronos2_grid | 0.45 | 0.03 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | wt_med | 0.50 | 0.00 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.51 | 0.02 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | chronos2_grid | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | chronos2_grid | 0.46 | 0.02 | 2 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | chronos2_grid | 0.45 | 0.01 | 2 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.49 | 0.01 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.50 | 0.02 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.48 | 0.01 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | clim | h5 | 0.47 | 0.02 | 2 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | wt_med | wt_med | 0.48 | 0.00 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | wt_med | wt_med | 0.46 | 0.00 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | clim | clim | 0.45 | 0.00 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | clim | clim | 0.44 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | chronos2_grid | 7.40 | -0.84 | 3 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | chronos2_owx_x_h5 | 7.55 | -0.62 | 2 |
| dayPeak abs error (min) | UC3 | 2 | snaive7 | lvlh5_naive | 8.05 | -0.33 | 1 |
| dayPeak abs error (min) | UC3 | 3 | wt_med | wt_med | 7.29 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 4 | snaive7 | chronos2_owx_x_h5 | 8.30 | -0.97 | 3 |
| dayPeak abs error (min) | UC3 | 5 | snaive7 | chronos2_x_h5 | 8.09 | -0.85 | 2 |
| dayPeak abs error (min) | UC3 | 6 | snaive7 | chronos2_owx_x_h5 | 7.77 | -0.72 | 4 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | chronos2_owx_x_h5 | 8.31 | -0.38 | 2 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 7.48 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 14 | wt_med | wt_med | 8.91 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 21 | clim | clim | 8.91 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 30 | clim | clim | 8.36 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 45 | wt_med | wt_med | 8.48 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 60 | wt_med | wt_med | 8.96 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 90 | wt_med | wt_med | 8.42 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 0 | prod_ropedrop_hist | prod_ropedrop_hist |  | 0.00 | 0 |
| rope-drop worth agreement | D5 | 1 | prod_ropedrop_hist | prod_ropedrop_hist |  | 0.00 | 0 |
| rope-drop worth agreement | D5 | 2 | snaive7 | snaive7 | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 3 | snaive7 | wt_med | 0.92 | 0.02 | 7 |
| rope-drop worth agreement | D5 | 4 | snaive7 | chronos2_x_h5 | 0.91 | 0.01 | 4 |
| rope-drop worth agreement | D5 | 5 | snaive7 | snaive7 | 0.93 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 6 | snaive7 | snaive7 | 0.93 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 7 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 10 | wt_med | wt_med | 0.92 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 14 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 21 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 30 | prod_ropedrop_hist | prod_ropedrop_hist |  | 0.00 | 0 |
| rope-drop worth agreement | D5 | 45 | clim | clim | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 90 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | chronos2_owx | 0.84 | 0.07 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | chronos2_owx | 0.83 | 0.07 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | chronos2_owx_x_h5 | 0.82 | 0.03 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | chronos2_owx_x_h5 | 0.81 | 0.04 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | chronos2_owx | 0.81 | 0.06 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | chronos2_owx | 0.82 | 0.06 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | chronos2_owx | 0.81 | 0.06 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | chronos2_owx | 0.81 | 0.05 | 8 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | chronos2_owx_x_h5 | 0.81 | 0.04 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | chronos2_owx_x_h5 | 0.80 | 0.05 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | chronos2_owx_x_h5 | 0.79 | 0.04 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | wt_med | wt_med | 0.77 | 0.00 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | wt_med | wt_med | 0.75 | 0.00 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | wt_med | wt_med | 0.74 | 0.00 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | clim | clim | 0.75 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 0 | wt_med | wt_med | 0.84 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 1 | wt_med | wt_med | 0.84 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 2 | wt_med | wt_med | 0.88 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 3 | wt_med | wt_med | 0.86 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 4 | wt_med | wt_med | 0.84 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 5 | wt_med | wt_med | 0.84 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 6 | wt_med | wt_med | 0.83 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.83 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 10 | clim | h5 | 0.86 | 0.01 | 2 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.83 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 21 | wt_med | wt_med | 0.82 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 30 | clim | clim | 0.86 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 45 | clim | clim | 0.85 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 60 | wt_med | wt_med | 0.83 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 90 | wt_med | wt_med | 0.82 | 0.00 | 0 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | wt_med | 37.53 | 0.00 | 0 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | wt_med | 37.84 | 0.00 | 0 |
| optimiser regret (min per park-day) | D4 | 7 | clim | chronos2_owx_x_h5 | 36.55 | -2.68 | 2 |
| optimiser regret (min per park-day) | D4 | 14 | clim | clim | 39.66 | 0.00 | 0 |
| optimiser regret (min per park-day) | D4 | 30 | wt_med | wt_med | 39.68 | 0.00 | 0 |
| next-best precision | D1 | 0-120 | wt_med | lvlh5_tft | 0.54 | 0.04 | 2 |
| next-best precision | D1 | 0-60 | wt_med | lvlh5_tft | 0.45 | 0.03 | 2 |
| next-best precision (top-3) | D1 | 0-120 | wt_med | chronos2 | 0.71 | 0.09 | 1 |
| next-best precision (top-3) | D1 | 0-60 | wt_med | chronos2 | 0.65 | 0.10 | 1 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.59 | 0.00 | 0 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.55 | 0.00 | 0 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_chronos2 | 0.46 | 0.07 | 2 |
| crowd bucket exact | D6 | 2 | lvl_naive4 | lvl_naive4 | 0.47 | 0.00 | 0 |
| crowd bucket exact | D6 | 3 | lvl_naive4 | lvl_naive4 | 0.45 | 0.00 | 0 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.38 | 0.00 | 0 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.43 | 0.00 | 0 |
| crowd bucket exact | D6 | 7 | lvl_wt56 | lvl_tft | 0.41 | 0.07 | 3 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_naive4 | 0.43 | 0.00 | 0 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_naive4 | 0.31 | 0.00 | 0 |
| crowd bucket exact | D6 | 21 | lvl_wt56 | lvl_wt56 | 0.31 | 0.00 | 0 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_naive4 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_naive4 | 0.38 | 0.00 | 0 |
| crowd bucket exact | D6 | 60 | lvl_wt56 | lvl_wt56 | 0.29 | 0.00 | 0 |
| crowd bucket exact | D6 | 90 | lvl_naive4 | lvl_naive4 | 0.33 | 0.00 | 0 |
| crowd bucket exact | D6 | 120 | lvl_wt56 | lvl_wt56 | 0.28 | 0.00 | 0 |
| crowd bucket exact | D6 | 180 | lvl_naive4 | lvl_naive4 | 0.21 | 0.00 | 0 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.44 | 0.00 | 0 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_naive4 | 0.31 | 0.00 | 0 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_naive4 | 0.29 | 0.00 | 0 |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.44 [7.18, 7.74] | 6.66 [6.33, 6.99] | 5.31 [5.13, 5.49] | 5.94 [5.68, 6.25] | 7.04 [6.77, 7.34] | 7.09 [6.81, 7.40] |
| 2 | 7.25 [6.98, 7.53] | 7.44 [7.13, 7.78] | 5.61 [5.42, 5.79] | 5.40 [5.17, 5.64] | 7.03 [6.77, 7.30] | 7.07 [6.80, 7.34] |
| 3 | 6.88 [6.58, 7.15] | 6.84 [6.50, 7.18] | 4.99 [4.82, 5.16] | 5.39 [5.11, 5.65] | 6.80 [6.52, 7.07] | 6.90 [6.62, 7.18] |
| 4 | 7.83 [7.51, 8.17] | 7.46 [7.07, 7.84] | 5.40 [5.21, 5.62] | 6.30 [5.98, 6.62] | 7.56 [7.24, 7.89] | 7.60 [7.28, 7.93] |
| 5 | 7.68 [7.32, 8.05] | 7.34 [6.90, 7.78] | 5.21 [5.02, 5.39] | 6.23 [5.89, 6.61] | 7.50 [7.14, 7.86] | 7.56 [7.20, 7.92] |
| 6 | 7.31 [7.02, 7.60] | 7.05 [6.72, 7.41] | 5.02 [4.86, 5.20] | 5.91 [5.62, 6.20] | 7.18 [6.88, 7.48] | 7.26 [6.95, 7.56] |
| 7 | 7.93 [7.59, 8.29] | 7.31 [6.93, 7.71] | 5.21 [5.02, 5.39] | 6.70 [6.36, 7.06] | 7.40 [7.08, 7.73] | 7.47 [7.15, 7.80] |
| 10 | 7.00 [6.72, 7.28] | 7.24 [6.85, 7.61] | 5.02 [4.85, 5.19] | 5.54 [5.26, 5.81] | 6.96 [6.67, 7.24] | 7.08 [6.78, 7.36] |
| 14 | 8.24 [7.90, 8.59] | 7.73 [7.35, 8.19] | 5.31 [5.12, 5.51] | 7.08 [6.71, 7.46] | 7.69 [7.37, 8.02] | 7.78 [7.45, 8.11] |
| 21 | 8.52 [8.18, 8.89] | 8.02 [7.62, 8.44] | 5.41 [5.21, 5.62] | 7.42 [7.04, 7.82] | 7.83 [7.51, 8.17] | 7.93 [7.60, 8.27] |
| 30 | 8.07 [7.77, 8.40] | 8.38 [7.99, 8.80] | 5.84 [5.64, 6.04] | 6.32 [6.05, 6.66] | 7.67 [7.39, 7.99] | 7.76 [7.47, 8.08] |
| 45 | 7.57 [7.27, 7.90] | 7.85 [7.33, 8.44] | 5.13 [4.96, 5.31] | 6.21 [5.92, 6.54] | 7.61 [7.31, 7.98] | 7.80 [7.49, 8.17] |
| 60 | 8.63 [8.24, 9.03] | 0.47 [0.14, 1.16] | 5.70 [5.45, 5.97] | 7.24 [6.84, 7.63] | 8.25 [7.84, 8.66] | 8.34 [7.93, 8.74] |
| 90 | 8.70 [8.26, 9.14] |  | 5.31 [5.09, 5.57] | 7.47 [7.03, 7.93] | 8.21 [7.80, 8.61] | 8.34 [7.93, 8.75] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| m015 | 3.00 [2.88, 3.13] | 7.75 [7.41, 8.10] | 7.03 [6.72, 7.33] | 7.51 [7.20, 7.84] | 6.60 [6.25, 6.98] | 2.64 [2.52, 2.76] | 8.35 [8.00, 8.75] | 6.99 [6.68, 7.31] |
| m030 | 3.86 [3.71, 4.01] | 7.68 [7.35, 8.04] | 6.92 [6.63, 7.23] | 7.43 [7.12, 7.74] | 6.56 [6.22, 6.93] | 4.23 [4.04, 4.43] | 8.27 [7.92, 8.67] | 6.95 [6.65, 7.26] |
| m045 | 4.24 [4.09, 4.40] | 7.67 [7.33, 8.01] | 6.91 [6.62, 7.21] | 7.42 [7.12, 7.74] | 6.56 [6.23, 6.93] | 5.06 [4.85, 5.28] | 8.28 [7.93, 8.68] | 6.95 [6.66, 7.25] |
| m060 | 4.49 [4.32, 4.66] | 7.75 [7.41, 8.09] | 7.02 [6.72, 7.34] | 7.50 [7.19, 7.83] | 6.65 [6.30, 7.03] | 5.62 [5.39, 5.87] | 8.36 [8.01, 8.76] | 7.04 [6.74, 7.36] |
| m075 | 4.72 [4.54, 4.90] | 7.79 [7.44, 8.13] | 7.02 [6.73, 7.35] | 7.52 [7.22, 7.84] | 6.60 [6.27, 6.98] | 6.12 [5.86, 6.38] | 8.40 [8.03, 8.81] | 7.07 [6.76, 7.39] |
| m090 | 4.84 [4.66, 5.02] | 7.83 [7.47, 8.18] | 7.00 [6.70, 7.32] | 7.53 [7.22, 7.85] | 6.63 [6.30, 7.03] | 6.52 [6.24, 6.80] | 8.41 [8.04, 8.81] | 7.09 [6.78, 7.41] |
| m105 | 4.95 [4.76, 5.14] | 7.87 [7.52, 8.21] | 7.05 [6.75, 7.37] | 7.60 [7.28, 7.93] | 6.69 [6.36, 7.07] | 6.74 [6.46, 7.04] | 8.44 [8.08, 8.85] | 7.12 [6.81, 7.42] |
| m120 | 5.02 [4.84, 5.21] | 7.91 [7.56, 8.25] | 7.08 [6.77, 7.39] | 7.59 [7.28, 7.91] | 6.67 [6.33, 7.04] | 7.00 [6.70, 7.30] | 8.43 [8.07, 8.84] | 7.13 [6.82, 7.45] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| m015 | 0.26 [0.18, 0.36] | 5.41 [4.42, 6.35] | 4.70 [3.84, 5.52] | 5.19 [4.29, 6.07] | 4.44 [3.61, 5.32] | 5.95 [5.00, 6.89] | 4.67 [3.82, 5.47] | persistence |
| m030 | -0.42 [-0.61, -0.21]* | 3.75 [2.95, 4.55] | 3.02 [2.35, 3.70] | 3.53 [2.79, 4.28] | 2.92 [2.28, 3.65] | 4.33 [3.59, 5.06] | 3.05 [2.37, 3.74] | persistence |
| m045 | -0.90 [-1.17, -0.62]* | 2.83 [2.15, 3.56] | 2.10 [1.48, 2.73] | 2.62 [1.96, 3.28] | 2.07 [1.49, 2.70] | 3.44 [2.82, 4.06] | 2.13 [1.52, 2.77] | persistence |
| m060 | -1.26 [-1.60, -0.93]* | 2.25 [1.59, 2.93] | 1.55 [0.94, 2.17] | 2.04 [1.37, 2.75] | 1.53 [0.94, 2.20] | 2.89 [2.28, 3.54] | 1.56 [0.96, 2.19] | persistence |
| m075 | -1.56 [-1.96, -1.15]* | 1.95 [1.21, 2.66] | 1.19 [0.58, 1.81] | 1.67 [1.00, 2.35] | 1.11 [0.51, 1.77] | 2.58 [1.95, 3.20] | 1.22 [0.57, 1.86] | persistence |
| m090 | -1.88 [-2.39, -1.40]* | 1.48 [0.81, 2.12] | 0.67 [0.09, 1.27] | 1.20 [0.55, 1.85] | 0.71 [0.13, 1.35] | 2.10 [1.51, 2.71] | 0.74 [0.12, 1.36] | persistence |
| m105 | -2.06 [-2.64, -1.56]* | 1.27 [0.56, 2.00] | 0.43 [-0.22, 1.08] | 0.95 [0.24, 1.70] | 0.47 [-0.17, 1.13] | 1.85 [1.18, 2.61] | 0.50 [-0.16, 1.17] | persistence |
| m120 | -2.27 [-2.92, -1.71]* | 0.93 [0.23, 1.65] | 0.10 [-0.57, 0.73] | 0.59 [-0.14, 1.31] | 0.12 [-0.53, 0.79] | 1.53 [0.83, 2.26] | 0.16 [-0.50, 0.81] | persistence |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| m015 | 5.91 [5.76, 6.06] | 15.59 [14.93, 16.23] | 14.13 [13.60, 14.68] | 14.68 [14.12, 15.24] | 12.97 [12.36, 13.69] | 5.18 [5.00, 5.36] | 16.42 [15.78, 17.09] | 14.11 [13.55, 14.66] |
| m030 | 7.61 [7.42, 7.78] | 15.46 [14.83, 16.09] | 13.96 [13.45, 14.53] | 14.52 [13.99, 15.08] | 12.85 [12.26, 13.59] | 8.29 [8.04, 8.54] | 16.20 [15.58, 16.85] | 14.05 [13.52, 14.62] |
| m045 | 8.27 [8.07, 8.45] | 15.36 [14.73, 15.99] | 13.90 [13.41, 14.44] | 14.45 [13.91, 14.99] | 12.85 [12.24, 13.55] | 9.88 [9.59, 10.16] | 16.14 [15.54, 16.80] | 13.98 [13.48, 14.53] |
| m060 | 8.74 [8.51, 8.93] | 15.54 [14.90, 16.17] | 14.12 [13.60, 14.67] | 14.64 [14.10, 15.19] | 13.04 [12.42, 13.80] | 11.00 [10.67, 11.31] | 16.40 [15.78, 17.08] | 14.20 [13.68, 14.74] |
| m075 | 9.10 [8.87, 9.33] | 15.64 [15.01, 16.29] | 14.08 [13.56, 14.65] | 14.58 [14.04, 15.14] | 12.81 [12.20, 13.58] | 11.66 [11.28, 12.02] | 16.43 [15.79, 17.12] | 14.22 [13.70, 14.78] |
| m090 | 9.37 [9.12, 9.60] | 15.76 [15.12, 16.42] | 14.07 [13.56, 14.64] | 14.64 [14.11, 15.19] | 12.90 [12.30, 13.60] | 12.49 [12.06, 12.91] | 16.45 [15.83, 17.18] | 14.29 [13.77, 14.87] |
| m105 | 9.55 [9.30, 9.78] | 15.80 [15.15, 16.49] | 14.13 [13.61, 14.69] | 14.74 [14.19, 15.33] | 12.96 [12.34, 13.70] | 12.86 [12.44, 13.28] | 16.44 [15.79, 17.15] | 14.27 [13.75, 14.85] |
| m120 | 9.69 [9.42, 9.93] | 15.84 [15.21, 16.54] | 14.17 [13.66, 14.73] | 14.71 [14.18, 15.28] | 12.93 [12.33, 13.66] | 13.48 [13.04, 13.92] | 16.48 [15.83, 17.17] | 14.34 [13.81, 14.94] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| m015 | 0.44 [0.24, 0.65] | 10.75 [8.73, 12.75] | 9.36 [7.71, 10.87] | 9.90 [8.20, 11.40] | 8.40 [6.84, 9.66] | 11.46 [9.74, 13.01] | 9.31 [7.65, 10.81] | persistence |
| m030 | -0.92 [-1.28, -0.49]* | 7.45 [5.59, 9.36] | 6.04 [4.48, 7.49] | 6.57 [5.03, 8.05] | 5.17 [3.82, 6.46] | 8.20 [6.73, 9.49] | 6.09 [4.50, 7.58] | persistence |
| m045 | -1.87 [-2.27, -1.40]* | 5.70 [4.00, 7.55] | 4.30 [2.90, 5.82] | 4.86 [3.43, 6.38] | 3.51 [2.27, 4.88] | 6.48 [5.15, 7.69] | 4.34 [2.92, 5.86] | persistence |
| m060 | -2.62 [-3.12, -2.10]* | 4.61 [2.96, 6.38] | 3.24 [1.81, 4.84] | 3.76 [2.27, 5.33] | 2.42 [1.17, 3.82] | 5.53 [4.16, 6.94] | 3.27 [1.85, 4.88] | persistence |
| m075 | -3.07 [-3.79, -2.40]* | 4.34 [2.63, 6.09] | 2.76 [1.35, 4.27] | 3.28 [1.85, 4.73] | 1.80 [0.53, 3.16] | 5.12 [3.71, 6.48] | 2.84 [1.38, 4.37] | persistence |
| m090 | -3.76 [-4.65, -2.90]* | 3.35 [1.84, 4.96] | 1.68 [0.42, 2.99] | 2.29 [0.91, 3.68] | 0.87 [-0.32, 2.15] | 4.10 [2.79, 5.39] | 1.84 [0.52, 3.21] | persistence |
| m105 | -4.06 [-5.12, -3.17]* | 3.04 [1.31, 4.90] | 1.27 [-0.22, 2.93] | 1.88 [0.27, 3.63] | 0.42 [-1.02, 2.05] | 3.67 [2.19, 5.49] | 1.44 [-0.07, 3.16] | persistence |
| m120 | -4.51 [-5.73, -3.54]* | 2.31 [0.68, 4.10] | 0.54 [-0.87, 2.08] | 1.13 [-0.42, 2.83] | -0.26 [-1.65, 1.31] | 3.07 [1.49, 4.86] | 0.76 [-0.71, 2.28] | persistence |

**MAE by region (all rides)**

| lead | region | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| m015 | Asia | 3.34 | 9.95 | 8.93 | 9.35 | 8.61 | 3.03 | 9.92 | 8.81 |
| m015 | EU | 2.04 | 5.45 | 5.00 | 5.37 | 4.94 | 1.78 | 5.96 | 4.98 |
| m015 | NA | 3.90 | 8.42 | 7.75 | 8.36 | 7.03 | 3.34 | 9.80 | 7.79 |
| m060 | Asia | 5.14 | 9.91 | 8.90 | 9.33 | 8.63 | 6.60 | 10.05 | 8.90 |
| m060 | EU | 3.10 | 5.49 | 5.02 | 5.37 | 5.03 | 3.78 | 5.91 | 5.06 |
| m060 | NA | 5.70 | 8.53 | 7.86 | 8.48 | 7.16 | 6.99 | 9.94 | 7.89 |
| m120 | Asia | 5.90 | 10.33 | 9.05 | 9.44 | 8.60 | 8.57 | 10.07 | 9.07 |
| m120 | EU | 3.56 | 5.70 | 5.10 | 5.55 | 5.11 | 4.61 | 6.11 | 5.19 |
| m120 | NA | 6.05 | 8.37 | 7.74 | 8.36 | 7.09 | 8.45 | 9.77 | 7.79 |

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | chronos2 |
|---|---|---|---|---|---|---|---|---|
| m015 | -0.18 | 0.29 | -0.84 | -0.67 | -1.27 | -0.90 | -1.71 | 0.03 |
| m030 | -0.15 | 0.24 | -0.90 | -0.75 | -1.15 | -0.79 | -1.59 | -0.22 |
| m045 | 0.05 | 0.23 | -0.93 | -0.76 | -1.07 | -0.72 | -1.53 | -0.31 |
| m060 | 0.26 | 0.27 | -0.96 | -0.80 | -1.15 | -0.80 | -1.60 | -0.51 |
| m075 | 0.28 | 0.22 | -0.92 | -0.78 | -1.15 | -0.77 | -1.55 | -0.59 |
| m090 | 0.54 | 0.22 | -0.92 | -0.80 | -1.10 | -0.73 | -1.50 | -0.68 |
| m105 | 0.60 | 0.23 | -0.91 | -0.80 | -1.14 | -0.76 | -1.59 | -0.85 |
| m120 | 0.90 | 0.26 | -0.85 | -0.74 | -1.14 | -0.76 | -1.55 | -0.90 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| h2-4 | 5.32 [5.12, 5.52] | 7.83 [7.48, 8.16] | 7.04 [6.74, 7.36] | 7.57 [7.26, 7.90] | 6.66 [6.32, 7.03] | 8.07 [7.75, 8.39] | 8.41 [8.05, 8.82] | 7.08 [6.77, 7.39] |
| h4-8 | 5.80 [5.57, 6.02] | 7.83 [7.50, 8.16] | 7.04 [6.75, 7.35] | 7.59 [7.28, 7.90] | 6.76 [6.42, 7.15] | 10.42 [10.00, 10.82] | 8.49 [8.14, 8.90] | 7.09 [6.80, 7.39] |
| h8+ | 5.98 [5.71, 6.22] | 7.20 [6.88, 7.54] | 6.44 [6.16, 6.74] | 7.03 [6.75, 7.35] | 6.52 [6.16, 6.92] | 12.56 [12.07, 13.11] | 7.95 [7.62, 8.34] | 6.47 [6.19, 6.79] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|
| h2-4 | -1.76 [-2.21, -1.32]* | 0.69 [0.48, 0.94] | -0.03 [-0.06, -0.01]* | 0.47 [0.29, 0.61] | 0.11 [-0.13, 0.32] | 0.89 [0.22, 1.60] | 1.25 [0.86, 1.60] | wt_med |
| h4-8 | -1.30 [-1.64, -0.95]* | 0.67 [0.46, 0.91] | -0.05 [-0.08, -0.02]* | 0.47 [0.29, 0.62] | 0.13 [-0.11, 0.34] | 3.31 [2.42, 4.35] | 1.32 [0.97, 1.65] | wt_med |
| h8+ | -0.52 [-0.75, -0.32]* | 0.57 [0.39, 0.80] | -0.03 [-0.08, 0.01] | 0.56 [0.37, 0.72] | 0.39 [0.20, 0.58] | 6.29 [4.98, 8.19] | 1.43 [1.10, 1.71] | wt_med |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| h2-4 | 10.22 [9.93, 10.49] | 15.74 [15.11, 16.39] | 14.18 [13.67, 14.75] | 14.75 [14.21, 15.30] | 12.99 [12.39, 13.68] | 15.19 [14.73, 15.64] | 16.47 [15.83, 17.13] | 14.28 [13.77, 14.85] |
| h4-8 | 10.98 [10.64, 11.31] | 15.42 [14.84, 16.04] | 13.89 [13.42, 14.40] | 14.46 [13.95, 15.01] | 12.96 [12.36, 13.68] | 18.90 [18.31, 19.48] | 16.28 [15.64, 16.92] | 14.00 [13.52, 14.53] |
| h8+ | 11.10 [10.71, 11.52] | 13.65 [13.10, 14.21] | 12.26 [11.82, 12.76] | 12.86 [12.38, 13.41] | 12.09 [11.47, 12.80] | 22.22 [21.29, 23.19] | 14.81 [14.20, 15.45] | 12.36 [11.90, 12.85] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|
| h2-4 | -4.09 [-5.18, -3.01]* | 1.43 [0.96, 2.01] | -0.10 [-0.17, -0.05]* | 0.51 [0.05, 0.84] | -0.47 [-1.27, 0.15] | 1.19 [-0.19, 2.51] | 2.15 [1.31, 2.78] | wt_med |
| h4-8 | -3.08 [-3.90, -2.29]* | 1.38 [0.93, 1.91] | -0.13 [-0.21, -0.06]* | 0.49 [0.04, 0.81] | -0.40 [-1.16, 0.17] | 5.53 [3.90, 7.29] | 2.25 [1.51, 2.80] | wt_med |
| h8+ | -1.35 [-1.81, -0.95]* | 1.19 [0.83, 1.67] | -0.11 [-0.22, -0.00]* | 0.53 [0.18, 0.80] | 0.05 [-0.40, 0.48] | 11.04 [8.05, 15.05] | 2.47 [1.80, 2.99] | wt_med |

**MAE by region (all rides)**

_no rows_

**Bias**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | chronos2 |
|---|---|---|---|---|---|---|---|---|
| h2-4 | 1.33 | 0.24 | -0.92 | -0.77 | -1.17 | -0.79 | -1.61 | -1.18 |
| h4-8 | 2.41 | 0.21 | -1.06 | -0.88 | -1.26 | -0.86 | -1.71 | -1.49 |
| h8+ | 4.68 | 0.11 | -1.29 | -1.18 | -1.56 | -1.26 | -1.93 | -1.22 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 6.15 [5.88, 6.42] | 6.00 [5.73, 6.26] | 6.43 [6.16, 6.71] | 6.04 [5.79, 6.28] |  |  | 7.75 [7.41, 8.08] | 6.97 [6.68, 7.28] | 9.47 [8.90, 10.08] | 7.48 [7.18, 7.80] | 6.59 [6.26, 6.97] | 5.08 [4.90, 5.27] | 6.19 [5.90, 6.51] | 8.34 [7.98, 8.74] | 7.01 [6.71, 7.32] |
| 1 | 6.57 [6.31, 6.81] | 6.47 [6.22, 6.72] | 6.98 [6.71, 7.24] | 6.48 [6.24, 6.71] | 6.79 [6.55, 7.04] | 6.79 [6.55, 7.04] | 7.68 [7.36, 8.00] | 7.04 [6.77, 7.34] | 9.03 [8.55, 9.60] | 7.44 [7.18, 7.74] | 6.66 [6.33, 6.99] | 5.31 [5.13, 5.49] | 5.94 [5.68, 6.25] | 8.40 [8.06, 8.79] | 7.09 [6.81, 7.40] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.87 [-1.16, -0.59]* | -1.02 [-1.29, -0.78]* | -0.58 [-0.88, -0.30]* | -0.98 [-1.31, -0.66]* |  |  | 0.68 [0.48, 0.93] | -0.04 [-0.07, -0.01]* | 2.75 [1.95, 3.83] | 0.45 [0.28, 0.58] | 0.10 [-0.12, 0.30] | -1.93 [-2.39, -1.48]* | -0.84 [-1.18, -0.48]* | 1.24 [0.86, 1.58] | wt_med |
| 1 | -0.54 [-0.85, -0.27]* | -0.62 [-0.86, -0.39]* | -0.12 [-0.41, 0.16] | -0.63 [-0.98, -0.35]* | -0.30 [-0.49, -0.15]* | -0.30 [-0.49, -0.16]* | 0.57 [0.38, 0.82] | -0.04 [-0.08, -0.02]* | 2.57 [1.69, 3.65] | 0.35 [0.18, 0.53] | 0.18 [-0.12, 0.46] | -1.79 [-2.27, -1.40]* | -1.16 [-1.58, -0.71]* | 1.24 [0.95, 1.55] | wt_med |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 11.95 [11.49, 12.42] | 11.69 [11.19, 12.19] | 12.61 [12.11, 13.10] | 11.68 [11.27, 12.11] |  |  | 15.56 [14.93, 16.21] | 14.01 [13.52, 14.57] | 18.64 [17.34, 20.02] | 14.56 [14.03, 15.10] | 12.86 [12.26, 13.57] | 9.41 [9.17, 9.65] | 11.61 [11.01, 12.22] | 16.31 [15.69, 16.97] | 14.12 [13.61, 14.68] |
| 1 | 12.90 [12.43, 13.35] | 12.72 [12.22, 13.20] | 13.92 [13.40, 14.40] | 12.69 [12.27, 13.11] | 13.07 [12.63, 13.52] | 13.07 [12.63, 13.50] | 15.28 [14.70, 15.94] | 14.01 [13.48, 14.56] | 17.63 [16.54, 18.87] | 14.31 [13.81, 14.88] | 13.07 [12.42, 13.72] | 9.78 [9.53, 10.03] | 10.82 [10.27, 11.39] | 16.47 [15.82, 17.14] | 14.14 [13.58, 14.72] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -2.21 [-2.94, -1.55]* | -2.47 [-3.08, -1.92]* | -1.55 [-2.37, -0.83]* | -2.48 [-3.33, -1.74]* |  |  | 1.41 [0.94, 1.99] | -0.11 [-0.18, -0.05]* | 5.00 [2.73, 7.85] | 0.48 [0.05, 0.78] | -0.46 [-1.22, 0.10] | -4.71 [-5.79, -3.64]* | -2.47 [-3.01, -1.96]* | 2.13 [1.31, 2.74] | wt_med |
| 1 | -1.29 [-2.15, -0.59]* | -1.44 [-2.09, -0.88]* | -0.27 [-1.09, 0.41] | -1.50 [-2.42, -0.78]* | -1.07 [-1.55, -0.68]* | -1.08 [-1.58, -0.69]* | 1.19 [0.76, 1.80] | -0.12 [-0.20, -0.06]* | 4.71 [2.42, 7.48] | 0.25 [-0.08, 0.54] | -0.29 [-1.21, 0.43] | -4.37 [-5.44, -3.42]* | -3.23 [-3.71, -2.70]* | 2.28 [1.61, 2.83] | wt_med |

**MAE by region (all rides)**

| lead | region | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 7.81 | 7.78 | 8.13 | 7.54 |  |  | 10.05 | 8.91 | 13.24 | 9.34 | 8.57 | 6.12 | 7.89 | 10.00 | 8.94 |
| 0 | EU | 4.26 | 4.08 | 4.48 | 4.22 |  |  | 5.53 | 5.00 | 7.38 | 5.41 | 5.02 | 3.58 | 4.68 | 5.96 | 5.05 |
| 0 | NA | 6.99 | 6.75 | 7.33 | 6.94 |  |  | 8.28 | 7.65 | 9.33 | 8.27 | 7.00 | 6.00 | 6.45 | 9.70 | 7.68 |
| 1 | Asia | 8.48 | 8.55 | 9.08 | 8.25 | 8.72 | 8.70 | 10.01 | 9.08 | 13.34 | 9.82 | 8.99 | 6.44 | 8.12 | 10.97 | 9.15 |
| 1 | EU | 4.48 | 4.42 | 4.74 | 4.46 | 4.77 | 4.77 | 5.36 | 4.95 | 6.59 | 5.31 | 4.74 | 3.74 | 4.50 | 5.78 | 4.99 |
| 1 | NA | 7.38 | 7.10 | 7.83 | 7.33 | 7.50 | 7.51 | 8.29 | 7.74 | 9.26 | 7.84 | 7.19 | 6.16 | 5.72 | 9.16 | 7.77 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 6.57 [6.31, 6.81] | 6.47 [6.22, 6.72] | 6.98 [6.71, 7.24] | 6.48 [6.24, 6.71] | 6.79 [6.55, 7.04] | 6.79 [6.55, 7.04] | 7.68 [7.36, 8.00] | 7.04 [6.77, 7.34] | 9.03 [8.55, 9.60] | 7.44 [7.18, 7.74] | 6.66 [6.33, 6.99] | 5.31 [5.13, 5.49] | 5.94 [5.68, 6.25] |  |  | 8.40 [8.06, 8.79] | 7.09 [6.81, 7.40] |
| 2 | 7.78 [7.46, 8.12] | 7.64 [7.31, 7.97] | 8.59 [8.21, 8.95] | 7.68 [7.38, 8.00] | 7.35 [7.08, 7.63] | 7.37 [7.10, 7.64] | 7.36 [7.09, 7.64] | 7.03 [6.77, 7.30] | 10.17 [9.61, 10.72] | 7.25 [6.98, 7.53] | 7.44 [7.13, 7.78] | 5.61 [5.42, 5.79] | 5.40 [5.17, 5.64] |  |  | 8.19 [7.89, 8.52] | 7.07 [6.80, 7.34] |
| 3 | 7.03 [6.72, 7.33] | 6.90 [6.58, 7.20] | 7.55 [7.20, 7.86] | 6.97 [6.67, 7.26] | 6.88 [6.60, 7.15] | 6.88 [6.60, 7.16] | 7.35 [7.04, 7.65] | 6.80 [6.52, 7.07] | 9.35 [8.81, 9.97] | 6.88 [6.58, 7.15] | 6.84 [6.50, 7.18] | 4.99 [4.82, 5.16] | 5.39 [5.11, 5.65] | 7.54 [7.20, 7.91] | 7.52 [7.17, 7.88] | 8.10 [7.71, 8.48] | 6.90 [6.62, 7.18] |
| 4 | 7.42 [7.08, 7.77] | 7.20 [6.85, 7.54] | 7.89 [7.54, 8.25] | 7.35 [7.02, 7.69] | 7.29 [6.99, 7.59] | 7.30 [6.99, 7.60] | 8.00 [7.66, 8.36] | 7.56 [7.24, 7.89] | 10.05 [9.43, 10.69] | 7.83 [7.51, 8.17] | 7.46 [7.07, 7.84] | 5.40 [5.21, 5.62] | 6.30 [5.98, 6.62] | 7.86 [7.45, 8.24] | 7.84 [7.43, 8.22] | 8.71 [8.30, 9.14] | 7.60 [7.28, 7.93] |
| 5 | 7.43 [7.04, 7.82] | 7.14 [6.75, 7.55] | 7.90 [7.48, 8.30] | 7.43 [7.05, 7.82] | 7.28 [6.95, 7.62] | 7.29 [6.95, 7.63] | 8.03 [7.65, 8.40] | 7.50 [7.14, 7.86] | 10.09 [9.40, 10.81] | 7.68 [7.32, 8.05] | 7.34 [6.90, 7.78] | 5.21 [5.02, 5.39] | 6.23 [5.89, 6.61] | 7.67 [7.24, 8.11] | 7.65 [7.22, 8.08] | 8.55 [8.14, 9.00] | 7.56 [7.20, 7.92] |
| 6 | 7.05 [6.74, 7.36] | 6.83 [6.51, 7.15] | 7.44 [7.11, 7.76] | 7.01 [6.70, 7.30] | 6.97 [6.69, 7.25] | 6.98 [6.69, 7.25] | 7.77 [7.45, 8.12] | 7.18 [6.88, 7.48] | 9.52 [9.01, 10.12] | 7.31 [7.02, 7.60] | 7.05 [6.72, 7.41] | 5.02 [4.86, 5.20] | 5.91 [5.62, 6.20] | 7.49 [7.15, 7.85] | 7.47 [7.14, 7.84] | 8.05 [7.69, 8.43] | 7.26 [6.95, 7.56] |
| 7 | 7.41 [7.10, 7.75] | 7.24 [6.89, 7.60] | 7.84 [7.50, 8.21] | 7.37 [7.06, 7.70] | 7.28 [6.97, 7.60] | 7.29 [6.97, 7.61] | 7.99 [7.66, 8.33] | 7.40 [7.08, 7.73] | 9.92 [9.35, 10.55] | 7.93 [7.59, 8.29] | 7.31 [6.93, 7.71] | 5.21 [5.02, 5.39] | 6.70 [6.36, 7.06] | 7.80 [7.41, 8.21] | 7.79 [7.40, 8.20] |  | 7.47 [7.15, 7.80] |
| 10 |  |  |  |  | 7.05 [6.77, 7.34] | 7.07 [6.79, 7.36] | 7.49 [7.20, 7.81] | 6.96 [6.67, 7.24] | 9.82 [9.24, 10.45] | 7.00 [6.72, 7.28] | 7.24 [6.85, 7.61] | 5.02 [4.85, 5.19] | 5.54 [5.26, 5.81] | 8.01 [7.60, 8.44] | 8.00 [7.58, 8.42] |  | 7.08 [6.78, 7.36] |
| 14 |  |  |  |  | 7.64 [7.32, 7.97] | 7.65 [7.33, 7.98] | 8.11 [7.78, 8.46] | 7.69 [7.37, 8.02] | 10.15 [9.57, 10.78] | 8.24 [7.90, 8.59] | 7.73 [7.35, 8.19] | 5.31 [5.12, 5.51] | 7.08 [6.71, 7.46] | 8.22 [7.80, 8.69] | 8.21 [7.79, 8.67] |  | 7.78 [7.45, 8.11] |
| 21 |  |  |  |  | 7.84 [7.53, 8.17] | 7.85 [7.54, 8.18] | 8.18 [7.84, 8.53] | 7.83 [7.51, 8.17] | 10.33 [9.71, 11.01] | 8.52 [8.18, 8.89] | 8.02 [7.62, 8.44] | 5.41 [5.21, 5.62] | 7.42 [7.04, 7.82] | 8.62 [8.18, 9.08] | 8.61 [8.17, 9.07] |  | 7.93 [7.60, 8.27] |
| 30 |  |  |  |  | 8.04 [7.74, 8.36] | 8.06 [7.76, 8.38] | 7.98 [7.68, 8.32] | 7.67 [7.39, 7.99] | 10.94 [10.33, 11.61] | 8.07 [7.77, 8.40] | 8.38 [7.99, 8.80] | 5.84 [5.64, 6.04] | 6.32 [6.05, 6.66] | 9.46 [8.99, 9.97] | 9.43 [8.96, 9.93] |  | 7.76 [7.47, 8.08] |
| 45 |  |  |  |  | 7.63 [7.32, 7.96] | 7.66 [7.35, 7.99] | 8.17 [7.85, 8.51] | 7.61 [7.31, 7.98] | 10.54 [9.48, 11.76] | 7.57 [7.27, 7.90] | 7.85 [7.33, 8.44] | 5.13 [4.96, 5.31] | 6.21 [5.92, 6.54] | 8.66 [8.08, 9.25] | 8.63 [8.06, 9.22] |  | 7.80 [7.49, 8.17] |
| 60 |  |  |  |  | 8.30 [7.90, 8.68] | 8.33 [7.94, 8.72] | 8.41 [8.03, 8.83] | 8.25 [7.84, 8.66] |  | 8.63 [8.24, 9.03] | 0.47 [0.14, 1.16] | 5.70 [5.45, 5.97] | 7.24 [6.84, 7.63] | 0.34 [0.04, 0.90] | 0.34 [0.04, 0.90] |  | 8.34 [7.93, 8.74] |
| 90 |  |  |  |  | 8.30 [7.93, 8.70] | 8.32 [7.94, 8.71] | 8.61 [8.19, 9.04] | 8.21 [7.80, 8.61] |  | 8.70 [8.26, 9.14] |  | 5.31 [5.09, 5.57] | 7.47 [7.03, 7.93] |  |  |  | 8.34 [7.93, 8.75] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.54 [-0.85, -0.27]* | -0.62 [-0.86, -0.39]* | -0.12 [-0.41, 0.16] | -0.63 [-0.98, -0.35]* | -0.30 [-0.49, -0.15]* | -0.30 [-0.49, -0.16]* | 0.57 [0.38, 0.82] | -0.04 [-0.08, -0.02]* | 2.57 [1.69, 3.65] | 0.35 [0.18, 0.53] | 0.18 [-0.12, 0.46] | -1.79 [-2.27, -1.40]* | -1.16 [-1.58, -0.71]* |  |  | 1.24 [0.95, 1.55] | wt_med |
| 2 | 0.68 [0.51, 0.86] | 0.55 [0.34, 0.79] | 1.50 [1.12, 1.99] | 0.58 [0.41, 0.77] | 0.29 [0.17, 0.41] | 0.30 [0.18, 0.42] | 0.39 [0.28, 0.53] | -0.03 [-0.07, -0.00]* | 3.26 [2.62, 4.03] | 0.22 [0.12, 0.32] | 0.54 [0.29, 0.77] | -1.47 [-1.86, -1.10]* | -1.64 [-1.99, -1.27]* |  |  | 1.19 [0.86, 1.52] | wt_med |
| 3 | 0.07 [-0.10, 0.22] | -0.06 [-0.24, 0.13] | 0.59 [0.33, 0.87] | 0.00 [-0.17, 0.17] | -0.02 [-0.16, 0.12] | -0.02 [-0.15, 0.12] | 0.54 [0.40, 0.71] | -0.10 [-0.13, -0.07]* | 2.79 [2.12, 3.61] | 0.00 [-0.14, 0.14] | 0.25 [0.04, 0.46] | -1.92 [-2.43, -1.46]* | -1.49 [-1.85, -1.10]* | 1.00 [0.66, 1.36] | 0.98 [0.63, 1.35] | 1.25 [0.88, 1.65] | wt_med |
| 4 | -0.20 [-0.49, 0.02] | -0.43 [-0.66, -0.23]* | 0.26 [-0.10, 0.59] | -0.28 [-0.54, -0.07]* | -0.31 [-0.50, -0.15]* | -0.31 [-0.50, -0.15]* | 0.43 [0.26, 0.64] | -0.03 [-0.06, -0.00]* | 2.66 [1.86, 3.75] | 0.25 [0.08, 0.41] | 0.18 [-0.14, 0.46] | -2.22 [-2.73, -1.72]* | -1.28 [-1.69, -0.88]* | 0.60 [0.09, 1.07] | 0.58 [0.07, 1.05] | 1.09 [0.70, 1.47] | wt_med |
| 5 | -0.14 [-0.54, 0.16] | -0.43 [-0.76, -0.18]* | 0.33 [-0.16, 0.73] | -0.14 [-0.52, 0.16] | -0.28 [-0.49, -0.11]* | -0.27 [-0.49, -0.10]* | 0.48 [0.30, 0.70] | -0.05 [-0.08, -0.02]* | 2.53 [1.81, 3.39] | 0.16 [-0.03, 0.30] | 0.08 [-0.29, 0.43] | -2.36 [-2.86, -1.89]* | -1.29 [-1.68, -0.87]* | 0.42 [-0.15, 0.93] | 0.40 [-0.16, 0.90] | 0.93 [0.47, 1.31] | wt_med |
| 6 | -0.22 [-0.57, 0.04] | -0.44 [-0.69, -0.24]* | 0.17 [-0.21, 0.49] | -0.26 [-0.60, -0.00]* | -0.29 [-0.50, -0.12]* | -0.28 [-0.49, -0.11]* | 0.47 [0.28, 0.70] | -0.07 [-0.11, -0.04]* | 2.53 [1.76, 3.58] | 0.08 [-0.09, 0.21] | 0.21 [-0.09, 0.48] | -2.25 [-2.80, -1.74]* | -1.32 [-1.71, -0.92]* | 0.68 [0.26, 1.02] | 0.66 [0.25, 1.00] | 0.77 [0.41, 1.08] | wt_med |
| 7 | -0.06 [-0.40, 0.23] | -0.23 [-0.50, -0.01]* | 0.38 [-0.03, 0.72] | -0.10 [-0.43, 0.18] | -0.19 [-0.40, -0.03]* | -0.19 [-0.40, -0.02]* | 0.46 [0.25, 0.74] | -0.06 [-0.10, -0.03]* | 2.77 [1.97, 3.74] | 0.48 [0.28, 0.66] | 0.41 [0.14, 0.65] | -2.27 [-2.79, -1.78]* | -0.75 [-1.18, -0.27]* | 0.92 [0.57, 1.21] | 0.91 [0.56, 1.20] |  | wt_med |
| 10 |  |  |  |  | -0.03 [-0.20, 0.14] | -0.00 [-0.17, 0.16] | 0.52 [0.36, 0.69] | -0.11 [-0.15, -0.08]* | 2.96 [2.26, 3.75] | -0.04 [-0.23, 0.13] | 0.38 [0.08, 0.64] | -2.06 [-2.56, -1.57]* | -1.50 [-1.88, -1.06]* | 1.20 [0.75, 1.62] | 1.19 [0.73, 1.60] |  | wt_med |
| 14 |  |  |  |  | -0.14 [-0.36, 0.04] | -0.13 [-0.35, 0.04] | 0.30 [0.08, 0.53] | -0.08 [-0.12, -0.04]* | 2.71 [1.86, 3.74] | 0.52 [0.29, 0.75] | 0.50 [0.14, 0.92] | -2.48 [-3.06, -1.91]* | -0.64 [-1.14, -0.07]* | 1.01 [0.54, 1.50] | 0.99 [0.53, 1.47] |  | wt_med |
| 21 |  |  |  |  | -0.08 [-0.31, 0.11] | -0.06 [-0.29, 0.12] | 0.21 [-0.01, 0.46] | -0.08 [-0.13, -0.05]* | 2.62 [1.77, 3.65] | 0.61 [0.42, 0.80] | 0.49 [0.26, 0.77] | -2.53 [-3.10, -1.98]* | -0.49 [-0.97, 0.09] | 1.10 [0.72, 1.47] | 1.08 [0.71, 1.46] |  | wt_med |
| 30 |  |  |  |  | 0.31 [0.17, 0.42] | 0.32 [0.19, 0.44] | 0.33 [0.17, 0.51] | -0.06 [-0.10, -0.03]* | 3.27 [2.63, 3.97] | 0.37 [0.24, 0.50] | 0.84 [0.41, 1.25] | -1.93 [-2.39, -1.51]* | -1.36 [-1.71, -1.04]* | 1.91 [1.17, 2.62] | 1.88 [1.14, 2.59] |  | wt_med |
| 45 |  |  |  |  | -0.14 [-0.34, 0.05] | -0.11 [-0.31, 0.07] | 0.47 [0.27, 0.73] | -0.14 [-0.19, -0.10]* | 1.67 [0.94, 2.83] | -0.17 [-0.42, 0.06] | 0.34 [0.03, 0.68] | -2.66 [-3.28, -2.06]* | -1.50 [-1.93, -1.05]* | 1.09 [0.61, 1.51] | 1.06 [0.57, 1.49] |  | wt_med |
| 60 |  |  |  |  | -0.02 [-0.21, 0.13] | 0.01 [-0.18, 0.18] | 0.08 [-0.22, 0.38] | -0.06 [-0.10, -0.03]* |  | 0.37 [0.15, 0.59] | 0.09 [0.01, 1.05] | -2.66 [-3.24, -2.11]* | -0.99 [-1.52, -0.45]* | -0.07 [-0.28, 0.57] | -0.07 [-0.28, 0.60] |  | wt_med |
| 90 |  |  |  |  | 0.00 [-0.18, 0.16] | 0.02 [-0.19, 0.20] | 0.28 [-0.12, 0.74] | -0.09 [-0.12, -0.06]* |  | 0.44 [0.19, 0.68] |  | -3.04 [-3.75, -2.31]* | -0.74 [-1.28, -0.32]* |  |  |  | wt_med |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 12.90 [12.43, 13.35] | 12.72 [12.22, 13.20] | 13.92 [13.40, 14.40] | 12.69 [12.27, 13.11] | 13.07 [12.63, 13.52] | 13.07 [12.63, 13.50] | 15.28 [14.70, 15.94] | 14.01 [13.48, 14.56] | 17.63 [16.54, 18.87] | 14.31 [13.81, 14.88] | 13.07 [12.42, 13.72] | 9.78 [9.53, 10.03] | 10.82 [10.27, 11.39] |  |  | 16.47 [15.82, 17.14] | 14.14 [13.58, 14.72] |
| 2 | 15.14 [14.57, 15.72] | 14.97 [14.39, 15.57] | 17.08 [16.39, 17.74] | 14.91 [14.38, 15.47] | 13.98 [13.54, 14.45] | 14.01 [13.58, 14.47] | 14.29 [13.86, 14.78] | 13.66 [13.21, 14.10] | 20.02 [18.92, 21.15] | 13.87 [13.41, 14.32] | 14.26 [13.73, 14.81] | 10.33 [10.05, 10.60] | 9.94 [9.48, 10.45] |  |  | 16.27 [15.73, 16.87] | 13.78 [13.34, 14.24] |
| 3 | 14.35 [13.77, 14.94] | 14.06 [13.46, 14.70] | 15.53 [14.88, 16.20] | 14.21 [13.65, 14.78] | 13.70 [13.18, 14.25] | 13.71 [13.20, 14.27] | 15.18 [14.63, 15.72] | 14.10 [13.55, 14.64] | 18.79 [17.67, 20.19] | 13.81 [13.26, 14.41] | 13.67 [13.04, 14.35] | 9.50 [9.27, 9.75] | 10.30 [9.70, 10.90] | 15.10 [14.46, 15.75] | 15.03 [14.40, 15.69] | 17.06 [16.25, 17.84] | 14.35 [13.79, 14.90] |
| 4 | 14.52 [13.85, 15.22] | 14.14 [13.45, 14.89] | 15.61 [14.91, 16.32] | 14.35 [13.70, 14.97] | 14.00 [13.41, 14.58] | 14.02 [13.43, 14.62] | 15.86 [15.15, 16.60] | 15.00 [14.39, 15.65] | 19.59 [18.17, 20.94] | 15.11 [14.48, 15.72] | 14.44 [13.70, 15.17] | 9.82 [9.55, 10.12] | 11.69 [11.04, 12.33] | 15.41 [14.69, 16.14] | 15.35 [14.64, 16.09] | 17.35 [16.56, 18.20] | 15.11 [14.50, 15.74] |
| 5 | 14.44 [13.77, 15.10] | 13.94 [13.27, 14.68] | 15.56 [14.85, 16.30] | 14.45 [13.74, 15.12] | 13.89 [13.28, 14.54] | 13.93 [13.31, 14.58] | 15.88 [15.20, 16.58] | 14.80 [14.18, 15.46] | 19.57 [18.07, 21.11] | 14.79 [14.17, 15.53] | 14.23 [13.40, 15.06] | 9.52 [9.24, 9.77] | 11.63 [10.95, 12.38] | 15.02 [14.25, 15.87] | 14.96 [14.19, 15.80] | 16.78 [16.00, 17.63] | 14.94 [14.31, 15.59] |
| 6 | 14.04 [13.47, 14.62] | 13.69 [13.09, 14.29] | 15.03 [14.42, 15.62] | 13.96 [13.41, 14.51] | 13.70 [13.18, 14.22] | 13.72 [13.20, 14.24] | 15.75 [15.14, 16.43] | 14.62 [14.09, 15.19] | 18.64 [17.51, 19.85] | 14.38 [13.82, 14.94] | 13.79 [13.17, 14.43] | 9.37 [9.14, 9.60] | 11.16 [10.56, 11.78] | 14.77 [14.13, 15.41] | 14.73 [14.08, 15.37] | 16.19 [15.45, 16.84] | 14.80 [14.26, 15.37] |
| 7 | 14.59 [14.03, 15.18] | 14.23 [13.59, 14.89] | 15.64 [15.03, 16.29] | 14.49 [13.96, 15.05] | 14.14 [13.58, 14.69] | 14.16 [13.61, 14.72] | 15.96 [15.33, 16.65] | 14.82 [14.25, 15.43] | 19.43 [18.10, 20.77] | 15.51 [14.89, 16.14] | 14.20 [13.55, 14.94] | 9.54 [9.31, 9.80] | 12.62 [11.94, 13.29] | 15.36 [14.63, 16.14] | 15.32 [14.58, 16.11] |  | 14.98 [14.39, 15.61] |
| 10 |  |  |  |  | 14.07 [13.53, 14.62] | 14.12 [13.58, 14.67] | 15.38 [14.84, 15.95] | 14.35 [13.83, 14.87] | 19.48 [18.29, 20.87] | 13.91 [13.38, 14.45] | 14.57 [13.84, 15.31] | 9.48 [9.25, 9.72] | 10.41 [9.85, 10.98] | 16.35 [15.53, 17.20] | 16.29 [15.47, 17.14] |  | 14.63 [14.09, 15.17] |
| 14 |  |  |  |  | 14.96 [14.39, 15.53] | 14.99 [14.42, 15.56] | 16.17 [15.52, 16.87] | 15.39 [14.81, 16.06] | 19.98 [18.68, 21.27] | 16.22 [15.55, 16.89] | 15.17 [14.50, 15.94] | 9.70 [9.44, 9.97] | 13.40 [12.63, 14.17] | 16.42 [15.69, 17.29] | 16.37 [15.66, 17.23] |  | 15.59 [14.98, 16.25] |
| 21 |  |  |  |  | 15.31 [14.77, 15.90] | 15.36 [14.83, 15.94] | 16.23 [15.61, 16.90] | 15.61 [15.03, 16.24] | 20.28 [18.96, 21.73] | 16.69 [16.02, 17.38] | 15.61 [14.91, 16.34] | 9.84 [9.56, 10.13] | 13.96 [13.20, 14.79] | 17.12 [16.32, 17.95] | 17.07 [16.28, 17.91] |  | 15.82 [15.22, 16.46] |
| 30 |  |  |  |  | 15.02 [14.52, 15.53] | 15.05 [14.56, 15.55] | 15.28 [14.79, 15.81] | 14.51 [14.04, 15.01] | 20.86 [19.64, 22.13] | 15.26 [14.74, 15.83] | 15.99 [15.34, 16.64] | 10.53 [10.24, 10.82] | 11.50 [10.91, 12.15] | 18.61 [17.81, 19.44] | 18.51 [17.71, 19.33] |  | 14.71 [14.24, 15.21] |
| 45 |  |  |  |  | 15.10 [14.54, 15.71] | 15.17 [14.63, 15.81] | 16.77 [16.17, 17.43] | 15.59 [15.01, 16.20] | 17.10 [15.60, 18.70] | 14.88 [14.32, 15.50] | 15.61 [14.60, 16.75] | 9.45 [9.22, 9.70] | 11.65 [11.03, 12.31] | 17.28 [16.26, 18.39] | 17.19 [16.16, 18.31] |  | 15.95 [15.35, 16.58] |
| 60 |  |  |  |  | 15.77 [15.08, 16.49] | 15.85 [15.16, 16.57] | 16.18 [15.49, 16.91] | 15.97 [15.28, 16.72] |  | 16.39 [15.68, 17.11] |  | 10.07 [9.70, 10.45] | 13.19 [12.45, 13.93] |  |  |  | 16.13 [15.44, 16.88] |
| 90 |  |  |  |  | 16.24 [15.60, 16.93] | 16.29 [15.63, 16.97] | 17.08 [16.34, 17.84] | 16.45 [15.78, 17.12] |  | 16.94 [16.17, 17.74] |  | 9.75 [9.44, 10.08] | 13.94 [13.10, 14.83] |  |  |  | 16.65 [15.97, 17.32] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -1.29 [-2.15, -0.59]* | -1.44 [-2.09, -0.88]* | -0.27 [-1.09, 0.41] | -1.50 [-2.42, -0.78]* | -1.07 [-1.55, -0.68]* | -1.08 [-1.58, -0.69]* | 1.19 [0.76, 1.80] | -0.12 [-0.20, -0.06]* | 4.71 [2.42, 7.48] | 0.25 [-0.08, 0.54] | -0.29 [-1.21, 0.43] | -4.37 [-5.44, -3.42]* | -3.23 [-3.71, -2.70]* |  |  | 2.28 [1.61, 2.83] | wt_med |
| 2 | 1.25 [0.81, 1.82] | 1.09 [0.54, 1.78] | 3.18 [2.24, 4.65] | 1.01 [0.59, 1.61] | 0.20 [-0.07, 0.50] | 0.22 [-0.05, 0.53] | 0.73 [0.47, 1.04] | -0.13 [-0.22, -0.06]* | 6.40 [4.84, 8.32] | 0.15 [-0.05, 0.36] | 0.42 [-0.17, 0.99] | -3.46 [-4.19, -2.69]* | -3.76 [-4.25, -3.24]* |  |  | 2.57 [2.00, 3.06] | wt_med |
| 3 | -0.10 [-0.48, 0.26] | -0.37 [-0.76, 0.05] | 1.06 [0.43, 1.67] | -0.25 [-0.66, 0.13] | -0.65 [-0.98, -0.31]* | -0.64 [-0.97, -0.29]* | 1.06 [0.76, 1.42] | -0.25 [-0.31, -0.20]* | 4.99 [3.28, 6.81] | -0.51 [-0.83, -0.21]* | -0.34 [-0.80, 0.14] | -4.85 [-5.72, -3.89]* | -4.01 [-4.38, -3.59]* | 1.09 [0.44, 1.80] | 1.03 [0.37, 1.75] | 2.71 [1.93, 3.43] | wt_med |
| 4 | -0.66 [-1.38, -0.17]* | -1.02 [-1.62, -0.59]* | 0.44 [-0.55, 1.16] | -0.83 [-1.49, -0.35]* | -1.12 [-1.55, -0.77]* | -1.10 [-1.52, -0.74]* | 0.86 [0.43, 1.38] | -0.10 [-0.15, -0.04]* | 4.77 [2.42, 7.53] | 0.11 [-0.21, 0.44] | -0.36 [-1.15, 0.33] | -5.31 [-6.40, -4.29]* | -3.30 [-3.74, -2.85]* | 0.57 [-0.51, 1.63] | 0.51 [-0.55, 1.57] | 2.31 [1.46, 3.03] | wt_med |
| 5 | -0.55 [-1.61, 0.16] | -1.04 [-1.92, -0.42]* | 0.56 [-0.69, 1.39] | -0.55 [-1.53, 0.14] | -1.05 [-1.62, -0.67]* | -1.02 [-1.58, -0.64]* | 0.96 [0.51, 1.52] | -0.13 [-0.19, -0.08]* | 4.44 [2.35, 6.69] | -0.06 [-0.50, 0.26] | -0.46 [-1.45, 0.38] | -5.43 [-6.43, -4.52]* | -3.22 [-3.75, -2.64]* | 0.28 [-1.12, 1.43] | 0.23 [-1.16, 1.37] | 1.80 [0.93, 2.45] | wt_med |
| 6 | -0.83 [-1.75, -0.21]* | -1.17 [-1.90, -0.68]* | 0.16 [-0.84, 0.86] | -0.91 [-1.81, -0.31]* | -1.12 [-1.64, -0.71]* | -1.10 [-1.62, -0.69]* | 0.93 [0.41, 1.58] | -0.18 [-0.27, -0.11]* | 4.40 [2.22, 7.15] | -0.36 [-0.80, -0.04]* | -0.33 [-1.11, 0.35] | -5.45 [-6.72, -4.27]* | -3.57 [-4.03, -3.12]* | 0.65 [-0.26, 1.33] | 0.61 [-0.29, 1.29] | 1.27 [0.46, 1.84] | wt_med |
| 7 | -0.43 [-1.37, 0.22] | -0.77 [-1.46, -0.24]* | 0.63 [-0.40, 1.34] | -0.53 [-1.43, 0.12] | -0.85 [-1.39, -0.46]* | -0.83 [-1.38, -0.44]* | 0.94 [0.38, 1.67] | -0.15 [-0.23, -0.09]* | 4.99 [2.69, 7.68] | 0.63 [0.11, 1.05] | 0.14 [-0.55, 0.71] | -5.44 [-6.68, -4.23]* | -2.26 [-2.97, -1.52]* | 1.24 [0.50, 1.83] | 1.19 [0.47, 1.78] |  | wt_med |
| 10 |  |  |  |  | -0.57 [-1.03, -0.17]* | -0.52 [-0.97, -0.14]* | 1.01 [0.66, 1.41] | -0.28 [-0.35, -0.22]* | 5.16 [3.32, 6.96] | -0.65 [-1.04, -0.29]* | 0.01 [-0.67, 0.59] | -5.16 [-6.00, -4.19]* | -4.13 [-4.56, -3.65]* | 1.79 [0.86, 2.58] | 1.74 [0.80, 2.55] |  | wt_med |
| 14 |  |  |  |  | -0.62 [-1.19, -0.23]* | -0.59 [-1.15, -0.21]* | 0.59 [0.01, 1.20] | -0.18 [-0.28, -0.10]* | 5.03 [2.57, 7.75] | 0.78 [0.20, 1.34] | 0.48 [-0.46, 1.45] | -5.90 [-7.28, -4.57]* | -2.04 [-2.83, -1.11]* | 1.65 [0.58, 2.68] | 1.59 [0.55, 2.62] |  | wt_med |
| 21 |  |  |  |  | -0.50 [-1.10, -0.07]* | -0.45 [-1.06, -0.02]* | 0.43 [-0.17, 1.06] | -0.20 [-0.32, -0.12]* | 4.76 [2.26, 7.50] | 0.97 [0.52, 1.40] | 0.29 [-0.26, 0.89] | -5.98 [-7.31, -4.74]* | -1.74 [-2.51, -0.77]* | 1.69 [0.98, 2.44] | 1.64 [0.93, 2.39] |  | wt_med |
| 30 |  |  |  |  | 0.33 [0.01, 0.59] | 0.36 [0.04, 0.62] | 0.77 [0.42, 1.17] | -0.19 [-0.27, -0.12]* | 6.12 [4.44, 7.96] | 0.59 [0.31, 0.84] | 1.21 [0.44, 2.02] | -4.20 [-5.03, -3.39]* | -3.15 [-3.69, -2.67]* | 3.73 [2.49, 5.06] | 3.64 [2.42, 4.96] |  | wt_med |
| 45 |  |  |  |  | -0.84 [-1.29, -0.39]* | -0.77 [-1.24, -0.33]* | 1.01 [0.57, 1.57] | -0.34 [-0.43, -0.25]* | 2.76 [1.25, 4.58] | -1.04 [-1.55, -0.53]* | -0.27 [-0.99, 0.57] | -6.51 [-7.51, -5.48]* | -4.23 [-4.77, -3.64]* | 1.55 [0.63, 2.51] | 1.47 [0.54, 2.46] |  | wt_med |
| 60 |  |  |  |  | -0.36 [-0.82, 0.05] | -0.28 [-0.78, 0.19] | 0.12 [-0.69, 0.81] | -0.15 [-0.23, -0.07]* |  | 0.45 [-0.10, 0.96] |  | -6.08 [-7.17, -5.06]* | -2.69 [-3.52, -1.87]* |  |  |  | wt_med |
| 90 |  |  |  |  | -0.41 [-0.95, 0.03] | -0.36 [-0.95, 0.14] | 0.46 [-0.39, 1.52] | -0.19 [-0.27, -0.13]* |  | 0.44 [-0.19, 1.00] |  | -6.93 [-8.47, -5.42]* | -2.46 [-3.27, -1.66]* |  |  |  | wt_med |

**MAE by region (all rides)**

| lead | region | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 8.48 | 8.55 | 9.08 | 8.25 | 8.72 | 8.70 | 10.01 | 9.08 | 13.34 | 9.82 | 8.99 | 6.44 | 8.12 |  |  | 10.97 | 9.15 |
| 1 | EU | 4.48 | 4.42 | 4.74 | 4.46 | 4.77 | 4.77 | 5.36 | 4.95 | 6.59 | 5.31 | 4.74 | 3.74 | 4.50 |  |  | 5.78 | 4.99 |
| 1 | NA | 7.38 | 7.10 | 7.83 | 7.33 | 7.50 | 7.51 | 8.29 | 7.74 | 9.26 | 7.84 | 7.19 | 6.16 | 5.72 |  |  | 9.16 | 7.77 |
| 3 | Asia | 9.24 | 8.79 | 9.85 | 9.11 | 8.84 | 8.85 | 9.64 | 9.07 | 13.11 | 9.05 | 8.81 | 6.08 | 7.40 | 9.17 | 9.14 | 10.76 | 9.16 |
| 3 | EU | 4.77 | 4.82 | 5.23 | 4.73 | 4.74 | 4.74 | 4.97 | 4.47 | 7.02 | 4.68 | 4.75 | 3.50 | 3.62 | 5.31 | 5.29 | 5.12 | 4.51 |
| 3 | NA | 8.01 | 7.93 | 8.53 | 7.98 | 7.93 | 7.94 | 8.41 | 7.83 | 9.57 | 7.82 | 8.04 | 5.97 | 5.90 | 9.32 | 9.30 | 9.70 | 8.00 |
| 7 | Asia | 9.42 | 9.12 | 10.02 | 9.34 | 9.18 | 9.19 | 10.24 | 9.42 | 13.48 | 10.06 | 9.25 | 6.27 | 8.76 | 9.63 | 9.60 |  | 9.48 |
| 7 | EU | 5.20 | 5.20 | 5.47 | 5.20 | 5.24 | 5.24 | 5.74 | 5.45 | 7.86 | 5.74 | 5.71 | 3.75 | 5.04 | 6.05 | 6.05 |  | 5.53 |
| 7 | NA | 8.36 | 8.07 | 8.83 | 8.30 | 8.08 | 8.09 | 8.62 | 7.99 | 9.82 | 8.61 | 7.81 | 6.04 | 6.81 | 8.63 | 8.63 |  | 8.05 |
| 14 | Asia |  |  |  |  | 9.72 | 9.72 | 10.27 | 9.79 | 13.98 | 10.57 | 10.26 | 6.39 | 9.41 | 10.81 | 10.77 |  | 9.87 |
| 14 | EU |  |  |  |  | 5.48 | 5.49 | 5.86 | 5.68 | 8.04 | 5.98 | 5.88 | 3.86 | 5.35 | 6.16 | 6.15 |  | 5.78 |
| 14 | NA |  |  |  |  | 8.42 | 8.45 | 8.83 | 8.29 | 9.97 | 8.84 | 8.12 | 6.13 | 7.05 | 8.86 | 8.86 |  | 8.36 |
| 30 | Asia |  |  |  |  | 10.14 | 10.14 | 9.89 | 9.78 | 14.15 | 10.44 | 10.04 | 7.00 | 8.86 | 11.19 | 11.13 |  | 9.87 |
| 30 | EU |  |  |  |  | 5.93 | 5.93 | 6.11 | 5.71 | 8.81 | 5.92 | 6.44 | 4.27 | 4.66 | 7.00 | 6.98 |  | 5.74 |
| 30 | NA |  |  |  |  | 8.99 | 9.04 | 8.72 | 8.42 | 11.40 | 8.83 | 9.81 | 6.87 | 6.36 | 11.56 | 11.52 |  | 8.52 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | chronos2 | chronos2_x_h5 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.24 | -0.90 | -0.76 | -1.14 | -0.78 | -1.57 | -5.46 |  |  | -1.63 |  | -1.70 | -2.16 | -1.55 |  | -0.45 | 0.14 |
| 1 | 0.13 | -0.88 | -0.70 | -1.14 | -0.65 | -0.50 | -4.86 |  |  | -1.57 | -0.80 | -1.68 | -2.09 | -1.48 | -0.79 | -0.31 | 0.16 |
| 2 | 0.11 | -2.04 | -1.30 | -2.24 | -0.83 | -0.57 | -6.70 |  |  | -3.57 | -1.91 | -3.70 | -4.61 | -3.45 | -1.91 | -0.57 | 0.16 |
| 3 | 0.12 | 0.75 | 1.50 | 0.49 | -0.59 | -0.46 | -5.58 | 2.02 | 2.01 | -1.44 | -0.43 | -1.73 | -2.04 | -1.34 | -0.43 | -0.27 | 0.06 |
| 4 | 0.06 | -1.50 | -1.28 | -1.75 | -1.03 | -1.57 | -5.93 | 0.97 | 0.96 | -2.52 | -1.62 | -2.53 | -2.73 | -2.44 | -1.62 | -0.48 | -0.10 |
| 5 | 0.08 | -1.04 | -0.79 | -1.28 | -0.87 | -1.74 | -5.95 | 0.66 | 0.66 | -2.10 | -1.28 | -2.09 | -2.19 | -1.93 | -1.28 | -0.51 | 0.06 |
| 6 | 0.19 | -0.28 | -0.08 | -0.55 | -0.56 | -1.26 | -5.38 | 1.04 | 1.03 | -1.50 | -0.74 | -1.54 | -1.59 | -1.40 | -0.74 | -0.38 | 0.25 |
| 7 |  | -0.75 | -0.50 | -1.01 | -0.57 | -1.35 | -5.71 | 0.98 | 0.97 | -1.90 | -0.95 | -2.12 | -2.03 | -1.80 | -0.94 | -0.42 | 0.37 |
| 10 |  | 0.97 | 1.73 | 0.71 | -0.37 | -0.14 | -5.90 | 2.24 | 2.23 |  | -0.05 |  |  |  | -0.06 | -0.24 | 0.27 |
| 14 |  | -0.62 | -0.36 | -0.89 | -0.37 | -1.16 | -5.79 | 1.06 | 1.06 |  | -0.84 |  |  |  | -0.84 | -0.35 | 0.57 |
| 21 |  | -0.57 | -0.29 | -0.84 | -0.34 | -0.62 | -5.85 | 1.68 | 1.67 |  | -0.70 |  |  |  | -0.69 | -0.31 | 0.62 |
| 30 |  | -1.42 | -0.44 | -1.63 | -0.25 | 0.07 | -7.29 | 2.73 | 2.73 |  | -1.33 |  |  |  | -1.35 | -0.47 | 0.69 |
| 45 |  | 1.85 | 2.96 | 1.57 | 0.32 | -0.42 | -4.98 | 2.02 | 2.01 |  | 0.87 |  |  |  | 0.85 | -0.14 | 0.94 |
| 60 |  | -0.46 | 0.11 | -0.75 | -0.11 | -0.14 |  | -0.04 | -0.04 |  | -0.07 |  |  |  | -0.09 | -0.19 | 0.75 |
| 90 |  | 1.39 | 2.02 | 1.10 | 0.96 |  |  |  |  |  | 1.25 |  |  |  | 1.26 | -0.19 | 1.63 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.14 [6.76, 7.52] | 7.08 [6.71, 7.47] | 7.55 [7.18, 7.94] | 6.73 [6.33, 7.16] |  |  |
| 1 | 7.12 [6.78, 7.47] | 7.06 [6.72, 7.41] | 7.64 [7.32, 8.00] | 6.77 [6.38, 7.22] |  |  |
| 2 | 7.25 [6.90, 7.58] | 7.21 [6.86, 7.54] | 7.48 [7.15, 7.81] | 7.51 [7.14, 7.91] |  |  |
| 3 | 7.15 [6.80, 7.48] | 7.05 [6.70, 7.37] | 7.14 [6.78, 7.47] | 6.99 [6.61, 7.41] | 7.51 [7.12, 7.92] | 7.48 [7.10, 7.90] |
| 4 | 7.71 [7.31, 8.11] | 7.66 [7.26, 8.06] | 7.87 [7.47, 8.26] | 7.42 [6.93, 7.87] | 7.62 [7.16, 8.08] | 7.60 [7.14, 8.06] |
| 5 | 7.67 [7.22, 8.12] | 7.59 [7.16, 8.04] | 7.76 [7.32, 8.21] | 7.49 [6.94, 8.06] | 7.68 [7.13, 8.23] | 7.66 [7.12, 8.21] |
| 6 | 7.34 [6.97, 7.72] | 7.25 [6.89, 7.62] | 7.36 [6.99, 7.74] | 7.14 [6.73, 7.59] | 7.49 [7.06, 7.96] | 7.47 [7.05, 7.94] |
| 7 | 7.65 [7.23, 8.05] | 7.55 [7.15, 7.97] | 8.06 [7.64, 8.49] | 7.44 [6.98, 7.92] | 7.87 [7.41, 8.37] | 7.86 [7.39, 8.36] |
| 10 | 7.14 [6.77, 7.48] | 7.02 [6.65, 7.34] | 7.08 [6.73, 7.39] | 7.10 [6.67, 7.53] | 7.63 [7.17, 8.10] | 7.61 [7.15, 8.08] |
| 14 | 7.75 [7.34, 8.14] | 7.62 [7.22, 8.02] | 8.06 [7.63, 8.47] | 7.73 [7.24, 8.28] | 8.15 [7.60, 8.73] | 8.13 [7.59, 8.72] |
| 21 | 7.78 [7.37, 8.18] | 7.64 [7.23, 8.03] | 8.20 [7.77, 8.63] | 7.83 [7.32, 8.36] | 8.24 [7.72, 8.77] | 8.23 [7.70, 8.75] |
| 30 | 7.50 [7.09, 7.91] | 7.41 [7.00, 7.81] | 7.83 [7.41, 8.26] | 7.89 [7.40, 8.47] | 8.73 [8.18, 9.35] | 8.69 [8.14, 9.32] |
| 45 | 7.23 [6.78, 7.69] | 7.03 [6.60, 7.47] | 7.13 [6.72, 7.55] | 7.52 [6.77, 8.31] | 8.41 [7.63, 9.21] | 8.38 [7.60, 9.18] |
| 60 | 7.71 [7.19, 8.27] | 7.55 [7.04, 8.11] | 8.13 [7.55, 8.72] | 0.43 [0.06, 1.20] | 0.31 [0.01, 0.93] | 0.31 [0.01, 0.93] |
| 90 | 7.07 [6.35, 7.83] | 6.88 [6.19, 7.61] | 7.08 [6.39, 7.85] |  |  |  |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 6.59 [6.18, 7.00] | 6.60 [6.19, 7.01] | 7.22 [6.85, 7.66] | 6.05 [5.67, 6.52] |  |  |
| 1 | 6.99 [6.50, 7.64] | 7.00 [6.49, 7.65] | 6.87 [6.41, 7.43] | 6.26 [5.82, 6.80] |  |  |
| 2 | 6.55 [6.21, 6.96] | 6.51 [6.16, 6.92] | 6.60 [6.26, 6.99] | 7.21 [6.70, 7.77] |  |  |
| 3 | 6.10 [5.75, 6.49] | 5.99 [5.64, 6.37] | 6.03 [5.68, 6.42] | 6.26 [5.71, 6.88] | 7.68 [6.99, 8.42] | 7.67 [6.98, 8.40] |
| 4 | 7.30 [6.76, 7.87] | 7.29 [6.75, 7.87] | 7.70 [7.13, 8.36] | 7.58 [7.02, 8.18] | 8.62 [7.90, 9.36] | 8.61 [7.88, 9.34] |
| 5 | 7.23 [6.74, 7.68] | 7.24 [6.74, 7.69] | 7.43 [6.94, 7.91] | 6.84 [6.35, 7.38] | 7.65 [7.10, 8.28] | 7.63 [7.09, 8.25] |
| 6 | 7.00 [6.52, 7.51] | 6.98 [6.50, 7.49] | 7.16 [6.73, 7.62] | 6.75 [6.27, 7.29] | 7.48 [6.95, 8.10] | 7.47 [6.94, 8.08] |
| 7 | 6.93 [6.53, 7.35] | 6.93 [6.51, 7.35] | 7.52 [7.09, 7.97] | 6.87 [6.40, 7.40] | 7.57 [7.00, 8.19] | 7.58 [7.01, 8.20] |
| 10 | 6.89 [6.42, 7.44] | 6.78 [6.32, 7.33] | 6.76 [6.31, 7.31] | 7.64 [6.83, 8.54] | 9.12 [8.13, 10.06] | 9.11 [8.13, 10.05] |
| 14 | 7.86 [7.36, 8.43] | 7.86 [7.35, 8.44] | 8.68 [8.11, 9.35] | 7.74 [7.12, 8.41] | 8.42 [7.70, 9.19] | 8.41 [7.69, 9.18] |
| 21 | 8.26 [7.73, 8.82] | 8.26 [7.73, 8.84] | 9.23 [8.62, 9.90] | 8.45 [7.74, 9.28] | 9.50 [8.67, 10.48] | 9.49 [8.67, 10.47] |
| 30 | 8.14 [7.71, 8.62] | 8.07 [7.64, 8.55] | 8.46 [8.04, 8.92] | 9.23 [8.59, 9.93] | 10.77 [9.95, 11.67] | 10.73 [9.92, 11.62] |
| 45 | 8.58 [8.09, 9.08] | 8.43 [7.95, 8.94] | 8.18 [7.71, 8.65] | 8.32 [7.52, 9.17] | 8.99 [8.21, 9.81] | 8.97 [8.19, 9.79] |
| 60 | 8.91 [8.36, 9.53] | 8.90 [8.33, 9.53] | 9.08 [8.54, 9.70] | 0.98 [0.24, 2.09] | 0.68 [0.00, 1.95] | 0.67 [0.00, 1.94] |
| 90 | 9.17 [8.70, 9.66] | 9.11 [8.63, 9.59] | 9.81 [9.29, 10.36] |  |  |  |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | chronos2 | chronos2_x_h5 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 106241.00 | 6.87 | 7.03 | 7.06 | 6.81 | 6.98 | 6.31 | 8.06 |  |  | 5.37 |  | 5.32 | 5.71 | 5.22 |  | 4.49 | 6.06 |
| 0 | spring | 267514.00 | 9.40 | 7.73 | 8.85 | 7.68 | 8.32 | 7.23 |  |  |  | 6.48 |  | 6.35 | 6.80 | 6.44 |  | 5.44 | 6.99 |
| 0 | summer | 365726.00 | 7.85 | 6.35 | 7.00 | 6.36 | 6.91 | 6.63 | 9.84 |  |  | 5.96 |  | 5.74 | 6.22 | 5.81 |  | 4.90 | 5.59 |
| 0 | winter | 29209.00 | 10.52 | 8.66 | 10.28 | 8.76 | 9.35 |  |  |  |  | 8.41 |  | 8.49 | 8.48 | 8.28 |  | 6.31 | 7.64 |
| 1 | autumn | 124017.00 | 7.10 | 7.00 | 7.14 | 6.81 | 6.79 | 6.73 | 8.59 |  |  | 5.96 | 6.19 | 5.94 | 6.42 | 5.89 | 6.20 | 4.75 | 5.52 |
| 1 | spring | 288635.00 | 9.92 | 8.06 | 8.91 | 8.02 | 8.62 | 7.53 |  |  |  | 7.48 | 7.69 | 7.37 | 8.01 | 7.44 | 7.69 | 5.91 | 6.90 |
| 1 | summer | 367857.00 | 7.50 | 6.28 | 6.84 | 6.26 | 6.67 | 6.57 | 9.17 |  |  | 5.94 | 6.21 | 5.80 | 6.25 | 5.82 | 6.20 | 4.94 | 5.24 |
| 1 | winter | 31024.00 | 10.51 | 8.23 | 9.49 | 8.34 | 9.10 |  |  |  |  | 7.92 | 7.86 | 8.20 | 8.50 | 7.82 | 7.88 | 6.37 | 7.63 |
| 3 | autumn | 138439.00 | 8.99 | 7.30 | 7.73 | 7.21 | 7.53 | 7.58 | 9.82 | 8.11 | 8.08 | 7.96 | 7.54 | 7.84 | 8.58 | 7.87 | 7.53 | 5.19 | 6.05 |
| 3 | spring | 317163.00 | 8.79 | 7.26 | 7.76 | 7.13 | 7.12 | 6.62 |  | 7.45 | 7.43 | 7.47 | 7.30 | 7.46 | 8.13 | 7.44 | 7.30 | 5.16 | 5.52 |
| 3 | summer | 381756.00 | 7.11 | 6.36 | 6.81 | 6.28 | 6.33 | 6.59 | 9.19 | 7.35 | 7.33 | 6.20 | 6.21 | 5.96 | 6.59 | 6.12 | 6.20 | 4.73 | 4.94 |
| 3 | winter | 16545.00 | 10.41 | 9.49 | 9.84 | 9.41 | 9.86 |  |  |  |  | 9.98 | 9.30 | 9.73 | 10.03 | 9.91 | 9.30 | 6.29 | 7.94 |
| 7 | autumn | 105678.00 |  | 7.64 | 7.71 | 7.38 | 7.74 | 7.26 | 8.56 | 7.80 | 7.79 | 6.64 | 6.58 | 6.60 | 7.10 | 6.69 | 6.56 | 4.61 | 6.99 |
| 7 | spring | 259981.00 |  | 8.41 | 9.07 | 8.32 | 8.88 |  |  |  |  | 8.03 | 8.05 | 8.01 | 8.59 | 8.00 | 8.05 | 5.63 | 7.54 |
| 7 | summer | 364561.00 |  | 6.70 | 7.26 | 6.69 | 7.30 | 7.32 | 10.39 | 7.80 | 7.79 | 7.12 | 6.86 | 6.77 | 7.43 | 7.05 | 6.85 | 5.04 | 6.02 |
| 7 | winter | 13722.00 |  | 9.41 | 10.51 | 9.48 | 9.82 |  |  |  |  | 9.59 | 9.58 | 9.72 | 10.50 | 9.49 | 9.67 | 6.21 | 8.09 |
| 14 | autumn | 105319.00 |  | 8.05 | 7.71 | 7.79 | 8.28 | 7.92 | 8.95 | 8.46 | 8.45 |  | 7.13 |  |  |  | 7.13 | 4.68 | 7.68 |
| 14 | spring | 251700.00 |  | 8.86 | 9.34 | 8.74 | 9.28 |  |  |  |  |  | 8.55 |  |  |  | 8.53 | 5.78 | 8.01 |
| 14 | summer | 363372.00 |  | 6.97 | 7.43 | 6.94 | 7.59 | 7.68 | 10.61 | 8.14 | 8.13 |  | 7.18 |  |  |  | 7.17 | 5.17 | 6.34 |
| 30 | autumn | 147265.00 |  | 8.24 | 8.11 | 8.20 | 8.42 | 8.60 | 11.96 | 9.23 | 9.19 |  | 8.25 |  |  |  | 8.26 | 6.17 | 6.54 |
| 30 | spring | 250380.00 |  | 7.95 | 8.36 | 7.88 | 8.17 |  |  |  |  |  | 8.25 |  |  |  | 8.24 | 6.14 | 6.15 |
| 30 | summer | 394950.00 |  | 7.45 | 7.71 | 7.34 | 7.89 | 8.25 | 10.29 | 9.60 | 9.57 |  | 7.86 |  |  |  | 7.84 | 5.53 | 6.34 |

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
| 0-60 | 0.58 [0.52, 0.64] | 0.41 [0.39, 0.43] | 0.45 [0.43, 0.47] | 0.45 [0.42, 0.47] | 0.45 [0.42, 0.48] | 0.41 [0.40, 0.43] | 0.43 [0.41, 0.46] |
| 0-120 | 0.66 [0.60, 0.71] | 0.50 [0.47, 0.52] | 0.54 [0.51, 0.56] | 0.54 [0.51, 0.56] | 0.54 [0.50, 0.57] | 0.50 [0.48, 0.52] | 0.52 [0.50, 0.54] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.12 [-0.02, 0.27] | -0.02 [-0.04, -0.00] | 0.01 [0.00, 0.02]* | 0.01 [-0.01, 0.02] | 0.03 [0.00, 0.06]* | -0.03 [-0.06, -0.00] | wt_med |
| 0-120 | 0.12 [-0.01, 0.25] | -0.02 [-0.04, -0.01] | 0.02 [0.01, 0.03]* | 0.02 [-0.00, 0.03] | 0.04 [0.01, 0.07]* | -0.03 [-0.06, 0.00] | wt_med |

**next-best precision (top-3)**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.65 [0.61, 0.68] | 0.49 [0.47, 0.51] | 0.52 [0.50, 0.54] | 0.51 [0.49, 0.53] | 0.51 [0.49, 0.54] | 0.50 [0.48, 0.52] | 0.52 [0.50, 0.54] |
| 0-120 | 0.71 [0.68, 0.74] | 0.57 [0.55, 0.59] | 0.60 [0.58, 0.62] | 0.59 [0.57, 0.61] | 0.59 [0.57, 0.62] | 0.59 [0.57, 0.60] | 0.60 [0.58, 0.62] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.10 [0.02, 0.17]* | -0.03 [-0.04, -0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.01 [-0.02, 0.03] | -0.03 [-0.05, -0.01] | wt_med |
| 0-120 | 0.09 [0.02, 0.15]* | -0.03 [-0.05, -0.02] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.00 [-0.02, 0.03] | -0.02 [-0.04, -0.00] | wt_med |

**next-best recall**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| 0-60 | 0.22 [0.21, 0.23] | 0.49 [0.47, 0.51] | 0.41 [0.39, 0.43] | 0.37 [0.35, 0.39] | 0.20 [0.18, 0.22] | 0.55 [0.52, 0.57] | 0.50 [0.48, 0.52] |
| 0-120 | 0.24 [0.23, 0.25] | 0.51 [0.49, 0.53] | 0.42 [0.40, 0.44] | 0.39 [0.37, 0.41] | 0.21 [0.19, 0.23] | 0.59 [0.57, 0.61] | 0.52 [0.50, 0.54] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|---|
| 0-60 | -0.33 [-0.35, -0.30] | -0.06 [-0.08, -0.03] | -0.14 [-0.17, -0.11] | -0.18 [-0.20, -0.15] | -0.35 [-0.39, -0.30] | -0.05 [-0.07, -0.02] | snaive7 |
| 0-120 | -0.35 [-0.38, -0.32] | -0.08 [-0.11, -0.06] | -0.17 [-0.19, -0.13] | -0.21 [-0.23, -0.18] | -0.38 [-0.42, -0.32] | -0.07 [-0.10, -0.04] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`): `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| w0-45 | 6.77 [6.60, 6.94] | 14.21 [13.62, 14.73] | 12.89 [12.38, 13.37] | 13.38 [12.87, 13.88] | 11.74 [11.21, 12.37] | 7.15 [6.91, 7.36] | 15.13 [14.57, 15.75] | 12.94 [12.43, 13.44] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|
| w0-45 | -0.64 [-0.94, -0.33]* | 7.41 [5.78, 9.01] | 6.15 [4.76, 7.43] | 6.64 [5.23, 8.02] | 5.28 [4.12, 6.42] | 8.32 [6.89, 9.64] | 6.17 [4.78, 7.45] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | chronos2 |
|---|---|---|---|---|---|---|---|---|
| m015 | -0.19 | 0.62 | -0.82 | -0.48 | -1.70 | -1.34 | -3.64 | 0.25 |
| m030 | 0.03 | 0.54 | -0.98 | -0.65 | -1.46 | -1.11 | -3.38 | -0.16 |
| m045 | 0.52 | 0.53 | -1.06 | -0.71 | -1.27 | -0.93 | -3.17 | -0.29 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.76 [0.74, 0.77] | 0.81 [0.80, 0.82] | 0.70 [0.69, 0.72] | 0.76 [0.75, 0.77] |  |  | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.76 [0.74, 0.77] | 0.81 [0.79, 0.82] | 0.79 [0.77, 0.80] |  |  | 0.81 [0.80, 0.83] | 0.83 [0.82, 0.84] |
| 1 | 0.72 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.64 [0.62, 0.65] | 0.72 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.74 [0.73, 0.76] | 0.79 [0.78, 0.80] | 0.78 [0.77, 0.79] |  |  | 0.80 [0.79, 0.82] | 0.82 [0.81, 0.83] |
| 2 | 0.68 [0.67, 0.69] | 0.78 [0.77, 0.79] | 0.57 [0.55, 0.58] | 0.69 [0.67, 0.70] | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.84 [0.82, 0.85] | 0.82 [0.81, 0.83] | 0.73 [0.71, 0.74] | 0.80 [0.79, 0.81] | 0.79 [0.77, 0.80] |  |  | 0.83 [0.82, 0.85] | 0.83 [0.82, 0.84] |
| 3 | 0.69 [0.67, 0.70] | 0.79 [0.78, 0.80] | 0.58 [0.56, 0.59] | 0.69 [0.67, 0.70] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.84] | 0.82 [0.81, 0.83] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.79 [0.77, 0.80] | 0.72 [0.71, 0.74] | 0.76 [0.74, 0.77] | 0.80 [0.79, 0.82] | 0.82 [0.81, 0.83] |
| 4 | 0.68 [0.67, 0.69] | 0.79 [0.78, 0.80] | 0.57 [0.55, 0.59] | 0.68 [0.67, 0.70] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.78 [0.76, 0.79] | 0.73 [0.71, 0.74] | 0.75 [0.74, 0.77] | 0.80 [0.78, 0.81] | 0.82 [0.80, 0.83] |
| 5 | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.81] | 0.56 [0.54, 0.58] | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.74 [0.72, 0.76] | 0.80 [0.79, 0.81] | 0.78 [0.76, 0.79] | 0.73 [0.71, 0.74] | 0.76 [0.74, 0.77] | 0.81 [0.79, 0.82] | 0.82 [0.81, 0.83] |
| 6 | 0.67 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.56 [0.54, 0.58] | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.74 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.77 [0.76, 0.78] | 0.72 [0.71, 0.74] | 0.75 [0.74, 0.77] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] |
| 7 | 0.66 [0.64, 0.68] | 0.78 [0.76, 0.79] | 0.55 [0.53, 0.57] | 0.66 [0.65, 0.68] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.75 [0.74, 0.77] | 0.80 [0.79, 0.81] | 0.78 [0.77, 0.80] | 0.72 [0.71, 0.74] | 0.75 [0.73, 0.77] |  | 0.82 [0.81, 0.83] |
| 10 |  |  |  |  | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.84] | 0.82 [0.81, 0.83] | 0.73 [0.71, 0.74] | 0.79 [0.78, 0.80] | 0.78 [0.76, 0.79] | 0.73 [0.71, 0.74] | 0.76 [0.74, 0.77] |  | 0.82 [0.81, 0.83] |
| 14 |  |  |  |  | 0.79 [0.77, 0.80] | 0.79 [0.77, 0.80] | 0.83 [0.81, 0.84] | 0.82 [0.81, 0.83] | 0.74 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.77 [0.76, 0.78] | 0.72 [0.70, 0.74] | 0.75 [0.73, 0.76] |  | 0.82 [0.81, 0.83] |
| 21 |  |  |  |  | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.83 [0.81, 0.84] | 0.82 [0.81, 0.83] | 0.73 [0.72, 0.75] | 0.79 [0.78, 0.80] | 0.77 [0.75, 0.78] | 0.72 [0.70, 0.74] | 0.74 [0.72, 0.76] |  | 0.82 [0.81, 0.83] |
| 30 |  |  |  |  | 0.77 [0.76, 0.78] | 0.77 [0.76, 0.78] | 0.84 [0.83, 0.85] | 0.81 [0.80, 0.83] | 0.70 [0.68, 0.72] | 0.80 [0.78, 0.81] | 0.78 [0.76, 0.80] | 0.72 [0.70, 0.74] | 0.73 [0.71, 0.75] |  | 0.83 [0.82, 0.84] |
| 45 |  |  |  |  | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.83 [0.81, 0.84] | 0.81 [0.80, 0.83] | 0.71 [0.68, 0.73] | 0.78 [0.77, 0.79] | 0.78 [0.76, 0.80] | 0.73 [0.71, 0.75] | 0.75 [0.73, 0.78] |  | 0.82 [0.81, 0.83] |
| 60 |  |  |  |  | 0.79 [0.77, 0.80] | 0.78 [0.77, 0.80] | 0.83 [0.81, 0.84] | 0.81 [0.80, 0.82] |  | 0.79 [0.78, 0.80] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.82 [0.81, 0.83] |
| 90 |  |  |  |  | 0.78 [0.76, 0.79] | 0.78 [0.77, 0.79] | 0.83 [0.81, 0.84] | 0.81 [0.80, 0.82] |  | 0.78 [0.76, 0.79] |  |  |  |  | 0.82 [0.80, 0.83] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.07 [-0.09, -0.05] | -0.01 [-0.03, 0.00] | -0.12 [-0.14, -0.10] | -0.06 [-0.09, -0.04] |  |  | -0.01 [-0.01, -0.00] | 0.01 [-0.00, 0.01] | -0.06 [-0.08, -0.04] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.01] |  |  | -0.03 [-0.05, -0.02] |  | wt_med |
| 1 | -0.09 [-0.12, -0.06] | -0.02 [-0.04, -0.00] | -0.17 [-0.21, -0.13] | -0.09 [-0.12, -0.06] | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] | -0.01 [-0.02, -0.00] | 0.00 [-0.01, 0.01] | -0.07 [-0.09, -0.05] | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.02] |  |  | -0.03 [-0.04, -0.02] |  | wt_med |
| 2 | -0.14 [-0.17, -0.11] | -0.03 [-0.05, -0.01] | -0.26 [-0.30, -0.22] | -0.13 [-0.17, -0.10] | -0.04 [-0.06, -0.02] | -0.04 [-0.06, -0.02] |  | -0.00 [-0.02, 0.01] | -0.08 [-0.11, -0.06] | -0.02 [-0.03, -0.00] | -0.03 [-0.05, -0.01] |  |  | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.01]* | clim |
| 3 | -0.13 [-0.15, -0.10] | -0.02 [-0.04, 0.00] | -0.24 [-0.27, -0.20] | -0.13 [-0.15, -0.10] | -0.02 [-0.04, -0.01] | -0.02 [-0.04, -0.01] |  | 0.01 [0.00, 0.02]* | -0.07 [-0.10, -0.05] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.00] | -0.08 [-0.11, -0.05] | -0.04 [-0.07, -0.02] | -0.04 [-0.05, -0.03] | 0.00 [-0.00, 0.01] | clim |
| 4 | -0.12 [-0.15, -0.09] | -0.01 [-0.03, -0.00] | -0.24 [-0.28, -0.20] | -0.12 [-0.15, -0.09] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.01 [0.00, 0.02]* | -0.06 [-0.08, -0.05] | -0.01 [-0.02, 0.00] | -0.02 [-0.04, -0.01] | -0.07 [-0.10, -0.04] | -0.04 [-0.07, -0.02] | -0.03 [-0.05, -0.02] | 0.00 [-0.00, 0.01] | clim |
| 5 | -0.14 [-0.18, -0.10] | -0.01 [-0.03, 0.01] | -0.26 [-0.31, -0.21] | -0.14 [-0.17, -0.10] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.01 [0.00, 0.02]* | -0.06 [-0.08, -0.04] | -0.02 [-0.03, -0.00] | -0.03 [-0.04, -0.01] | -0.07 [-0.10, -0.04] | -0.04 [-0.06, -0.02] | -0.03 [-0.04, -0.02] | 0.00 [-0.00, 0.01] | clim |
| 6 | -0.14 [-0.18, -0.11] | -0.02 [-0.03, -0.01] | -0.25 [-0.30, -0.20] | -0.14 [-0.17, -0.10] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.01] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.06 [-0.08, -0.05] | -0.02 [-0.03, -0.01] | -0.03 [-0.04, -0.02] | -0.08 [-0.12, -0.05] | -0.05 [-0.09, -0.02] | -0.03 [-0.04, -0.02] |  | wt_med |
| 7 | -0.16 [-0.20, -0.12] | -0.03 [-0.05, -0.01] | -0.28 [-0.32, -0.23] | -0.16 [-0.20, -0.12] | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.01 [0.00, 0.02]* | -0.06 [-0.08, -0.04] | -0.01 [-0.02, -0.00] | -0.03 [-0.04, -0.01] | -0.09 [-0.12, -0.05] | -0.06 [-0.09, -0.03] |  | 0.00 [-0.00, 0.01] | clim |
| 10 |  |  |  |  | -0.03 [-0.05, -0.01] | -0.03 [-0.05, -0.01] |  | 0.00 [-0.01, 0.02] | -0.08 [-0.11, -0.06] | -0.02 [-0.04, -0.01] | -0.03 [-0.06, -0.01] | -0.08 [-0.11, -0.05] | -0.04 [-0.07, -0.02] |  | 0.01 [0.00, 0.01]* | clim |
| 14 |  |  |  |  | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] |  | 0.01 [-0.00, 0.01] | -0.07 [-0.09, -0.05] | -0.02 [-0.03, -0.01] | -0.04 [-0.05, -0.02] | -0.08 [-0.13, -0.05] | -0.05 [-0.09, -0.03] |  | -0.00 [-0.01, 0.00] | clim |
| 21 |  |  |  |  | -0.03 [-0.05, -0.02] | -0.03 [-0.05, -0.02] |  | 0.00 [-0.01, 0.01] | -0.08 [-0.10, -0.05] | -0.02 [-0.04, -0.01] | -0.04 [-0.05, -0.02] | -0.08 [-0.13, -0.05] | -0.05 [-0.10, -0.03] |  | -0.00 [-0.01, 0.00] | clim |
| 30 |  |  |  |  | -0.04 [-0.05, -0.02] | -0.04 [-0.05, -0.02] |  | 0.00 [-0.01, 0.01] | -0.11 [-0.13, -0.08] | -0.02 [-0.03, -0.01] | -0.02 [-0.04, -0.01] | -0.09 [-0.13, -0.05] | -0.07 [-0.11, -0.04] |  | 0.00 [-0.00, 0.01] | clim |
| 45 |  |  |  |  | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.01] |  | 0.01 [0.00, 0.02]* | -0.06 [-0.09, -0.00] | -0.02 [-0.04, -0.01] | -0.02 [-0.04, 0.00] | -0.07 [-0.10, -0.04] | -0.03 [-0.06, -0.01] |  | 0.00 [-0.01, 0.01] | clim |
| 60 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.00 [-0.00, 0.01] |  | -0.02 [-0.03, -0.01] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | -0.00 [-0.01, 0.00] | clim |
| 90 |  |  |  |  | -0.04 [-0.05, -0.03] | -0.04 [-0.05, -0.02] |  | -0.01 [-0.02, 0.00] |  | -0.03 [-0.05, -0.02] |  |  |  |  | -0.01 [-0.01, 0.00] | clim |

**best-time hit rate (±30 min)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.77 [0.76, 0.78] | 0.81 [0.80, 0.82] | 0.72 [0.71, 0.74] | 0.77 [0.76, 0.78] |  |  | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.77, 0.79] | 0.82 [0.81, 0.83] | 0.80 [0.79, 0.82] |  |  | 0.82 [0.80, 0.83] | 0.83 [0.82, 0.84] |
| 1 | 0.74 [0.73, 0.75] | 0.80 [0.79, 0.81] | 0.67 [0.65, 0.68] | 0.74 [0.73, 0.75] | 0.81 [0.80, 0.81] | 0.80 [0.79, 0.81] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] |  |  | 0.81 [0.80, 0.83] | 0.83 [0.82, 0.84] |
| 2 | 0.69 [0.68, 0.71] | 0.79 [0.77, 0.80] | 0.59 [0.57, 0.60] | 0.70 [0.68, 0.71] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.84 [0.83, 0.85] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.78] | 0.82 [0.81, 0.83] | 0.81 [0.80, 0.82] |  |  | 0.85 [0.83, 0.86] | 0.83 [0.82, 0.84] |
| 3 | 0.70 [0.69, 0.72] | 0.79 [0.78, 0.80] | 0.60 [0.59, 0.62] | 0.71 [0.69, 0.72] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.79] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.82 [0.80, 0.83] | 0.82 [0.81, 0.83] |
| 4 | 0.70 [0.68, 0.71] | 0.80 [0.78, 0.81] | 0.60 [0.58, 0.62] | 0.70 [0.69, 0.71] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.76 [0.75, 0.78] | 0.81 [0.81, 0.82] | 0.80 [0.79, 0.81] | 0.76 [0.74, 0.77] | 0.77 [0.76, 0.79] | 0.81 [0.80, 0.83] | 0.82 [0.81, 0.83] |
| 5 | 0.70 [0.69, 0.71] | 0.80 [0.79, 0.81] | 0.59 [0.57, 0.61] | 0.70 [0.69, 0.72] | 0.81 [0.80, 0.81] | 0.81 [0.79, 0.81] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.77 [0.75, 0.78] | 0.81 [0.80, 0.82] | 0.80 [0.78, 0.81] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] |
| 6 | 0.70 [0.68, 0.71] | 0.80 [0.79, 0.81] | 0.59 [0.57, 0.61] | 0.70 [0.69, 0.71] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.78 [0.76, 0.79] | 0.81 [0.81, 0.82] | 0.79 [0.78, 0.80] | 0.76 [0.74, 0.77] | 0.78 [0.76, 0.79] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] |
| 7 | 0.68 [0.66, 0.69] | 0.79 [0.78, 0.80] | 0.57 [0.56, 0.59] | 0.68 [0.66, 0.69] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.78 [0.76, 0.79] | 0.82 [0.81, 0.83] | 0.80 [0.79, 0.81] | 0.76 [0.74, 0.77] | 0.77 [0.76, 0.78] |  | 0.83 [0.82, 0.84] |
| 10 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.76 [0.75, 0.77] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.77] | 0.77 [0.76, 0.79] |  | 0.82 [0.81, 0.83] |
| 14 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.83 [0.82, 0.84] | 0.77 [0.76, 0.79] | 0.81 [0.80, 0.82] | 0.79 [0.78, 0.81] | 0.75 [0.74, 0.77] | 0.77 [0.75, 0.78] |  | 0.82 [0.81, 0.83] |
| 21 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.83 [0.81, 0.83] | 0.77 [0.75, 0.78] | 0.81 [0.80, 0.82] | 0.79 [0.78, 0.81] | 0.75 [0.73, 0.77] | 0.76 [0.75, 0.78] |  | 0.82 [0.81, 0.83] |
| 30 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.84 [0.83, 0.85] | 0.83 [0.82, 0.84] | 0.74 [0.72, 0.76] | 0.82 [0.80, 0.83] | 0.80 [0.79, 0.82] | 0.75 [0.73, 0.77] | 0.76 [0.74, 0.78] |  | 0.84 [0.83, 0.85] |
| 45 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.74 [0.71, 0.76] | 0.80 [0.79, 0.81] | 0.80 [0.78, 0.81] | 0.76 [0.74, 0.78] | 0.78 [0.76, 0.80] |  | 0.82 [0.81, 0.83] |
| 60 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.83 [0.81, 0.84] | 0.82 [0.81, 0.83] |  | 0.80 [0.79, 0.82] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.83 [0.81, 0.84] |
| 90 |  |  |  |  | 0.80 [0.78, 0.81] | 0.79 [0.78, 0.81] | 0.83 [0.81, 0.84] | 0.82 [0.80, 0.83] |  | 0.79 [0.78, 0.81] |  |  |  |  | 0.82 [0.81, 0.83] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.05 [-0.07, -0.03] | -0.00 [-0.02, 0.01] | -0.10 [-0.12, -0.08] | -0.05 [-0.07, -0.03] |  |  |  | 0.01 [0.01, 0.02]* | -0.04 [-0.06, -0.02] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.00] |  |  | -0.02 [-0.04, -0.01] | 0.00 [-0.00, 0.01] | clim |
| 1 | -0.08 [-0.11, -0.06] | -0.02 [-0.03, -0.00] | -0.15 [-0.18, -0.12] | -0.08 [-0.10, -0.06] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.01] | -0.01 [-0.01, -0.00] | 0.00 [-0.00, 0.01] | -0.04 [-0.06, -0.03] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.01] |  |  | -0.03 [-0.04, -0.01] |  | wt_med |
| 2 | -0.12 [-0.16, -0.09] | -0.03 [-0.05, -0.00] | -0.23 [-0.26, -0.19] | -0.12 [-0.16, -0.08] | -0.01 [-0.03, 0.01] | -0.01 [-0.03, 0.00] | 0.02 [0.01, 0.03]* | 0.02 [0.00, 0.03]* | -0.05 [-0.07, -0.02] | 0.00 [-0.01, 0.02] | 0.00 [-0.01, 0.02] |  |  |  | 0.01 [0.00, 0.02]* | snaive7 |
| 3 | -0.11 [-0.14, -0.08] | -0.02 [-0.04, -0.00] | -0.22 [-0.25, -0.19] | -0.11 [-0.14, -0.08] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] |  | 0.01 [0.00, 0.02]* | -0.04 [-0.06, -0.02] | -0.01 [-0.02, 0.01] | -0.01 [-0.02, 0.01] | -0.05 [-0.08, -0.03] | -0.03 [-0.06, -0.01] | -0.02 [-0.03, -0.01] | 0.00 [-0.00, 0.01] | clim |
| 4 | -0.12 [-0.14, -0.09] | -0.02 [-0.03, -0.00] | -0.21 [-0.25, -0.17] | -0.11 [-0.14, -0.09] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.01 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.05 [-0.08, -0.03] | -0.04 [-0.06, -0.02] | -0.02 [-0.04, -0.01] |  | wt_med |
| 5 | -0.12 [-0.15, -0.08] | -0.01 [-0.03, 0.00] | -0.23 [-0.27, -0.18] | -0.11 [-0.15, -0.08] | -0.02 [-0.03, -0.00] | -0.02 [-0.03, -0.00] | -0.00 [-0.01, 0.00] | 0.01 [-0.00, 0.01] | -0.04 [-0.06, -0.02] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.00] | -0.06 [-0.08, -0.03] | -0.04 [-0.06, -0.02] | -0.02 [-0.04, -0.01] |  | wt_med |
| 6 | -0.12 [-0.15, -0.09] | -0.02 [-0.03, -0.00] | -0.23 [-0.27, -0.18] | -0.12 [-0.15, -0.08] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.02]* | -0.04 [-0.05, -0.02] | -0.01 [-0.02, 0.00] | -0.02 [-0.03, -0.00] | -0.05 [-0.08, -0.03] | -0.03 [-0.06, -0.01] | -0.02 [-0.03, -0.01] |  | wt_med |
| 7 | -0.15 [-0.18, -0.11] | -0.03 [-0.05, -0.02] | -0.25 [-0.29, -0.20] | -0.14 [-0.18, -0.11] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.00 [-0.01, 0.00] | 0.01 [0.00, 0.01]* | -0.04 [-0.06, -0.02] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.00] | -0.06 [-0.10, -0.03] | -0.05 [-0.08, -0.02] |  |  | wt_med |
| 10 |  |  |  |  | -0.02 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  | 0.01 [-0.00, 0.02] | -0.05 [-0.08, -0.03] | -0.01 [-0.02, 0.00] | -0.02 [-0.03, 0.00] | -0.05 [-0.07, -0.03] | -0.03 [-0.05, -0.01] |  | 0.00 [-0.00, 0.01] | clim |
| 14 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.01 [-0.00, 0.01] | -0.04 [-0.07, -0.02] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, -0.01] | -0.06 [-0.10, -0.03] | -0.04 [-0.08, -0.02] |  | -0.00 [-0.01, 0.00] | clim |
| 21 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.00 [-0.00, 0.01] | -0.05 [-0.07, -0.02] | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.00] | -0.05 [-0.10, -0.03] | -0.04 [-0.08, -0.01] |  | -0.00 [-0.01, 0.00] | clim |
| 30 |  |  |  |  | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] |  | 0.01 [-0.00, 0.01] | -0.07 [-0.09, -0.05] | -0.01 [-0.02, 0.00] | -0.01 [-0.03, 0.01] | -0.06 [-0.09, -0.03] | -0.05 [-0.08, -0.02] |  | 0.01 [0.00, 0.01]* | clim |
| 45 |  |  |  |  | -0.01 [-0.03, -0.00] | -0.01 [-0.03, -0.00] |  | 0.01 [0.00, 0.02]* | -0.04 [-0.09, 0.01] | -0.01 [-0.02, -0.00] | -0.02 [-0.03, 0.00] | -0.04 [-0.07, -0.02] | -0.02 [-0.05, 0.00] |  | -0.00 [-0.01, 0.00] | clim |
| 60 |  |  |  |  | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  | 0.00 [-0.00, 0.01] |  | -0.01 [-0.02, -0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | -0.00 [-0.01, 0.01] | clim |
| 90 |  |  |  |  | -0.03 [-0.04, -0.01] | -0.03 [-0.05, -0.01] |  | -0.01 [-0.02, 0.00] |  | -0.03 [-0.05, -0.01] |  |  |  |  | -0.01 [-0.01, 0.00] | clim |

**best-time regret (min)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.35 [4.12, 4.59] | 3.37 [3.17, 3.60] | 5.68 [5.39, 5.97] | 4.29 [4.07, 4.52] |  |  | 3.20 [3.00, 3.42] | 2.93 [2.76, 3.13] | 4.12 [3.82, 4.48] | 3.33 [3.15, 3.56] | 3.47 [3.25, 3.72] |  |  | 3.76 [3.42, 4.15] | 3.14 [2.94, 3.36] |
| 1 | 5.07 [4.81, 5.33] | 3.82 [3.59, 4.07] | 7.54 [7.09, 8.05] | 5.03 [4.77, 5.30] | 3.62 [3.40, 3.83] | 3.63 [3.42, 3.85] | 3.31 [3.10, 3.51] | 3.15 [2.96, 3.36] | 4.06 [3.78, 4.39] | 3.56 [3.37, 3.76] | 3.55 [3.31, 3.82] |  |  | 3.94 [3.60, 4.32] | 3.23 [3.02, 3.43] |
| 2 | 6.45 [6.06, 6.84] | 4.56 [4.25, 4.90] | 10.44 [9.80, 11.12] | 6.37 [6.00, 6.76] | 3.80 [3.61, 4.03] | 3.82 [3.61, 4.04] | 3.01 [2.83, 3.21] | 3.21 [3.03, 3.44] | 4.59 [4.30, 4.89] | 3.40 [3.21, 3.60] | 3.50 [3.28, 3.78] |  |  | 3.50 [3.18, 3.89] | 3.23 [3.03, 3.46] |
| 3 | 6.07 [5.73, 6.42] | 4.04 [3.77, 4.32] | 9.57 [8.98, 10.16] | 6.02 [5.69, 6.36] | 3.56 [3.37, 3.76] | 3.55 [3.37, 3.75] | 3.15 [2.95, 3.35] | 3.09 [2.90, 3.31] | 4.26 [4.00, 4.53] | 3.43 [3.24, 3.64] | 3.63 [3.39, 3.87] | 4.75 [4.42, 5.09] | 3.94 [3.67, 4.23] | 3.60 [3.30, 3.89] | 3.30 [3.10, 3.53] |
| 4 | 6.48 [6.04, 6.99] | 3.87 [3.63, 4.11] | 10.30 [9.59, 11.01] | 6.42 [6.01, 6.93] | 3.62 [3.40, 3.86] | 3.63 [3.41, 3.86] | 3.40 [3.14, 3.69] | 3.19 [2.96, 3.45] | 4.36 [4.09, 4.70] | 3.44 [3.21, 3.70] | 3.78 [3.50, 4.07] | 5.10 [4.72, 5.48] | 4.25 [3.94, 4.56] | 3.97 [3.64, 4.34] | 3.32 [3.10, 3.54] |
| 5 | 6.51 [6.01, 7.05] | 3.76 [3.50, 4.02] | 10.38 [9.61, 11.18] | 6.37 [5.89, 6.88] | 3.62 [3.38, 3.86] | 3.64 [3.39, 3.88] | 3.32 [3.07, 3.60] | 3.17 [2.94, 3.42] | 4.47 [4.06, 4.90] | 3.54 [3.29, 3.81] | 3.85 [3.52, 4.20] | 4.95 [4.55, 5.34] | 4.15 [3.80, 4.51] | 3.65 [3.32, 4.00] | 3.21 [3.00, 3.43] |
| 6 | 6.44 [6.05, 6.85] | 3.91 [3.68, 4.18] | 10.03 [9.38, 10.74] | 6.32 [5.94, 6.73] | 3.62 [3.41, 3.85] | 3.63 [3.41, 3.86] | 3.37 [3.14, 3.61] | 3.13 [2.93, 3.35] | 4.39 [4.04, 4.74] | 3.54 [3.31, 3.77] | 4.01 [3.70, 4.33] | 5.13 [4.73, 5.57] | 4.26 [3.94, 4.63] | 3.65 [3.35, 3.98] | 3.29 [3.08, 3.52] |
| 7 | 6.88 [6.45, 7.30] | 4.17 [3.90, 4.45] | 10.54 [9.84, 11.28] | 6.76 [6.32, 7.18] | 3.58 [3.38, 3.82] | 3.61 [3.40, 3.86] | 3.20 [2.99, 3.41] | 3.02 [2.83, 3.25] | 4.35 [4.00, 4.75] | 3.45 [3.25, 3.72] | 3.83 [3.53, 4.18] | 5.05 [4.68, 5.47] | 4.27 [3.95, 4.63] |  | 3.19 [2.98, 3.42] |
| 10 |  |  |  |  | 3.69 [3.48, 3.90] | 3.71 [3.50, 3.93] | 3.14 [2.94, 3.35] | 3.14 [2.94, 3.35] | 4.54 [4.25, 4.83] | 3.54 [3.34, 3.76] | 3.76 [3.52, 4.04] | 4.74 [4.41, 5.09] | 3.92 [3.65, 4.23] |  | 3.26 [3.05, 3.46] |
| 14 |  |  |  |  | 3.75 [3.53, 4.00] | 3.77 [3.54, 4.02] | 3.20 [2.99, 3.43] | 3.15 [2.95, 3.37] | 4.56 [4.16, 5.01] | 3.58 [3.36, 3.83] | 3.94 [3.62, 4.32] | 5.13 [4.75, 5.57] | 4.31 [3.96, 4.69] |  | 3.26 [3.06, 3.49] |
| 21 |  |  |  |  | 3.80 [3.58, 4.06] | 3.82 [3.60, 4.09] | 3.25 [3.04, 3.49] | 3.22 [3.00, 3.47] | 4.78 [4.30, 5.29] | 3.68 [3.45, 3.95] | 4.15 [3.78, 4.59] | 5.10 [4.72, 5.52] | 4.32 [3.96, 4.69] |  | 3.30 [3.09, 3.55] |
| 30 |  |  |  |  | 3.96 [3.73, 4.20] | 3.99 [3.76, 4.22] | 3.10 [2.90, 3.34] | 3.31 [3.09, 3.52] | 5.35 [4.96, 5.77] | 3.57 [3.35, 3.79] | 3.82 [3.47, 4.18] | 5.18 [4.75, 5.69] | 4.75 [4.31, 5.21] |  | 3.18 [2.96, 3.40] |
| 45 |  |  |  |  | 3.68 [3.46, 3.89] | 3.71 [3.48, 3.91] | 3.05 [2.83, 3.26] | 3.19 [2.97, 3.41] | 5.05 [4.42, 5.74] | 3.66 [3.44, 3.88] | 3.91 [3.50, 4.32] | 4.83 [4.32, 5.34] | 4.06 [3.65, 4.46] |  | 3.24 [3.03, 3.46] |
| 60 |  |  |  |  | 4.18 [3.78, 4.59] | 4.18 [3.77, 4.59] | 3.33 [3.09, 3.57] | 3.73 [3.34, 4.15] |  | 4.19 [3.77, 4.61] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 3.55 [3.24, 3.87] |
| 90 |  |  |  |  | 4.25 [3.88, 4.67] | 4.26 [3.89, 4.68] | 3.29 [2.99, 3.62] | 3.68 [3.35, 4.04] |  | 4.29 [3.89, 4.71] |  |  |  |  | 3.42 [3.15, 3.76] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.09 [0.73, 1.48] | 0.14 [-0.21, 0.54] | 2.45 [1.86, 3.03] | 1.03 [0.67, 1.45] |  |  | 0.14 [-0.02, 0.30] | -0.27 [-0.44, -0.12]* | 0.97 [0.66, 1.27] | 0.19 [0.01, 0.37] | 0.21 [0.01, 0.39] |  |  | 1.07 [0.72, 1.44] |  | wt_med |
| 1 | 1.63 [1.17, 2.09] | 0.37 [0.04, 0.75] | 4.09 [2.99, 5.30] | 1.60 [1.16, 2.06] | 0.34 [0.16, 0.51] | 0.35 [0.17, 0.52] | 0.17 [0.01, 0.34] | -0.13 [-0.31, 0.03] | 0.93 [0.59, 1.30] | 0.32 [0.13, 0.49] | 0.27 [0.07, 0.46] |  |  | 0.84 [0.43, 1.39] |  | wt_med |
| 2 | 3.00 [2.22, 3.75] | 0.85 [0.36, 1.37] | 7.06 [5.54, 8.73] | 2.87 [2.12, 3.59] | 0.39 [0.15, 0.63] | 0.41 [0.16, 0.65] |  | -0.16 [-0.34, 0.02] | 1.24 [0.73, 1.67] | 0.06 [-0.13, 0.25] | 0.20 [-0.05, 0.48] |  |  | 0.66 [0.40, 0.97] | -0.07 [-0.16, 0.04] | clim |
| 3 | 2.62 [1.77, 3.34] | 0.37 [-0.07, 0.79] | 6.16 [4.90, 7.45] | 2.56 [1.75, 3.26] | 0.21 [-0.06, 0.46] | 0.20 [-0.07, 0.46] |  | -0.26 [-0.48, -0.09]* | 0.98 [0.53, 1.35] | 0.13 [-0.14, 0.33] | 0.22 [-0.08, 0.49] | 1.31 [0.68, 2.03] | 0.47 [0.04, 0.96] | 0.64 [0.45, 0.87] | -0.05 [-0.17, 0.05] | clim |
| 4 | 2.99 [2.12, 3.81] | 0.34 [0.03, 0.66] | 6.82 [5.13, 8.66] | 2.93 [2.09, 3.74] | 0.25 [0.06, 0.45] | 0.26 [0.07, 0.45] | 0.17 [0.03, 0.30] | -0.18 [-0.34, -0.02]* | 0.81 [0.47, 1.12] | 0.10 [-0.07, 0.27] | 0.34 [0.10, 0.59] | 1.70 [0.89, 2.74] | 0.82 [0.30, 1.45] | 0.91 [0.58, 1.26] |  | wt_med |
| 5 | 3.17 [2.18, 4.23] | 0.26 [-0.05, 0.60] | 6.97 [5.08, 8.97] | 3.00 [2.05, 4.06] | 0.33 [0.11, 0.52] | 0.35 [0.13, 0.54] | 0.19 [0.03, 0.36] | -0.12 [-0.29, 0.04] | 1.01 [0.58, 1.48] | 0.29 [0.10, 0.48] | 0.44 [0.16, 0.74] | 1.49 [0.86, 2.20] | 0.67 [0.20, 1.17] | 0.79 [0.44, 1.10] |  | wt_med |
| 6 | 2.98 [2.01, 3.98] | 0.29 [-0.03, 0.66] | 6.61 [4.90, 8.42] | 2.85 [1.92, 3.81] | 0.20 [-0.02, 0.42] | 0.22 [-0.01, 0.43] | 0.08 [-0.02, 0.19] | -0.27 [-0.41, -0.15]* | 0.94 [0.56, 1.32] | 0.17 [-0.05, 0.36] | 0.41 [0.12, 0.66] | 1.63 [0.95, 2.35] | 0.74 [0.34, 1.17] | 0.76 [0.40, 1.17] |  | wt_med |
| 7 | 3.61 [2.60, 4.68] | 0.75 [0.34, 1.20] | 7.35 [5.45, 9.21] | 3.49 [2.48, 4.51] | 0.33 [0.15, 0.50] | 0.36 [0.17, 0.54] | 0.07 [-0.09, 0.21] | -0.24 [-0.40, -0.10]* | 1.00 [0.57, 1.39] | 0.24 [0.04, 0.41] | 0.43 [0.15, 0.70] | 1.70 [0.97, 2.48] | 0.92 [0.48, 1.40] |  |  | wt_med |
| 10 |  |  |  |  | 0.35 [0.06, 0.62] | 0.37 [0.09, 0.65] |  | -0.17 [-0.37, 0.01] | 1.18 [0.71, 1.62] | 0.28 [-0.00, 0.56] | 0.40 [0.07, 0.72] | 1.32 [0.69, 2.05] | 0.47 [0.04, 0.99] |  | -0.07 [-0.20, 0.07] | clim |
| 14 |  |  |  |  | 0.39 [0.15, 0.65] | 0.41 [0.16, 0.67] |  | -0.18 [-0.35, -0.03]* | 1.18 [0.54, 1.93] | 0.28 [0.04, 0.50] | 0.49 [0.11, 0.87] | 1.58 [0.91, 2.39] | 0.77 [0.25, 1.40] |  | -0.02 [-0.13, 0.12] | clim |
| 21 |  |  |  |  | 0.32 [0.09, 0.57] | 0.34 [0.10, 0.59] |  | -0.20 [-0.40, -0.02]* | 1.31 [0.61, 2.20] | 0.29 [0.06, 0.54] | 0.49 [0.07, 0.93] | 1.53 [0.85, 2.32] | 0.72 [0.17, 1.32] |  | -0.01 [-0.12, 0.13] | clim |
| 30 |  |  |  |  | 0.39 [0.13, 0.65] | 0.41 [0.16, 0.66] |  | -0.16 [-0.35, 0.00] | 1.79 [1.27, 2.34] | 0.15 [-0.06, 0.33] | 0.20 [-0.08, 0.52] | 1.75 [0.89, 2.74] | 1.18 [0.40, 2.09] |  | -0.14 [-0.27, -0.01]* | clim |
| 45 |  |  |  |  | 0.33 [0.10, 0.54] | 0.36 [0.12, 0.58] |  | -0.20 [-0.40, -0.04]* | 0.90 [0.05, 1.54] | 0.33 [0.08, 0.54] | 0.28 [-0.14, 0.68] | 1.29 [0.61, 2.04] | 0.45 [-0.11, 1.08] |  | 0.03 [-0.12, 0.16] | clim |
| 60 |  |  |  |  | 0.23 [-0.07, 0.57] | 0.24 [-0.06, 0.58] |  | -0.18 [-0.43, 0.09] |  | 0.23 [-0.08, 0.56] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 0.04 [-0.18, 0.28] | clim |
| 90 |  |  |  |  | 0.49 [0.12, 0.90] | 0.50 [0.14, 0.91] |  | -0.03 [-0.28, 0.23] |  | 0.54 [0.18, 0.96] |  |  |  |  | 0.08 [-0.09, 0.28] | clim |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 40.87 [38.50, 43.22] | 36.06 [33.71, 38.40] | 46.60 [43.87, 49.50] | 40.48 [38.00, 42.92] | 36.80 [34.28, 39.57] | 37.08 [34.54, 39.86] | 39.15 [36.49, 41.97] | 36.75 [34.26, 39.57] | 31.32 [27.94, 34.82] | 36.74 [34.17, 39.54] | 34.32 [31.25, 37.93] |  |  | 43.67 [40.78, 46.51] | 37.53 [35.12, 40.50] |
| 3 | 42.47 [39.90, 45.04] | 36.86 [34.33, 39.44] | 48.55 [45.61, 51.38] | 43.78 [41.09, 46.53] | 36.77 [33.92, 39.51] | 36.97 [34.03, 39.80] | 39.68 [36.79, 42.99] | 37.30 [34.61, 40.25] | 36.95 [32.77, 41.73] | 36.05 [33.42, 38.73] | 34.58 [31.33, 37.99] | 41.72 [38.25, 45.34] | 42.10 [38.64, 45.78] | 41.38 [38.45, 44.15] | 37.84 [35.15, 40.72] |
| 7 | 44.46 [41.69, 47.22] | 36.83 [34.49, 39.53] | 51.79 [48.47, 54.97] | 45.27 [42.50, 48.09] | 36.55 [33.78, 39.22] | 36.61 [33.87, 39.34] | 39.21 [36.07, 42.25] | 37.44 [34.63, 40.15] | 33.53 [30.02, 36.99] | 37.99 [35.24, 40.65] | 33.99 [31.11, 37.15] | 39.57 [36.37, 42.80] | 42.10 [38.49, 45.80] |  | 39.34 [36.45, 42.15] |
| 14 |  |  |  |  | 37.53 [34.69, 40.25] | 37.70 [34.91, 40.43] | 39.66 [36.83, 42.51] | 38.31 [35.53, 41.03] | 36.83 [33.15, 41.03] | 39.40 [36.43, 42.29] | 35.66 [32.10, 39.21] | 41.11 [37.89, 44.27] | 40.71 [37.53, 44.01] |  | 40.53 [37.60, 43.44] |
| 30 |  |  |  |  | 38.29 [35.60, 41.05] | 38.81 [36.08, 41.50] | 41.85 [38.84, 44.80] | 38.07 [35.20, 40.72] | 38.96 [34.04, 43.28] | 39.68 [36.78, 42.63] | 37.57 [33.80, 41.54] | 49.42 [45.31, 54.13] | 49.15 [44.75, 53.96] |  | 39.68 [36.73, 42.53] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2.99 [-1.02, 6.74] | -2.08 [-4.89, 0.63] | 8.63 [4.21, 12.46] | 2.49 [-1.62, 6.23] | -1.04 [-3.61, 1.45] | -0.76 [-3.33, 1.75] | 1.60 [-1.25, 4.26] | -0.69 [-3.05, 1.48] | -2.40 [-5.02, -0.07]* | -0.88 [-3.36, 1.32] | -1.49 [-4.39, 1.53] |  |  | 5.63 [2.57, 8.83] |  | wt_med |
| 3 | 4.64 [0.26, 9.17] | -1.21 [-5.20, 2.24] | 10.46 [6.80, 14.04] | 5.87 [1.74, 10.29] | -1.25 [-3.28, 0.92] | -1.05 [-2.98, 1.01] | 1.62 [-0.99, 5.07] | -0.66 [-2.60, 1.54] | 0.33 [-2.60, 4.28] | -2.16 [-4.52, 0.44] | -2.23 [-4.96, 0.50] | 4.70 [0.35, 9.12] | 5.07 [0.47, 9.69] | 3.58 [1.16, 6.15] |  | wt_med |
| 7 | 5.65 [2.50, 8.41] | -2.27 [-4.96, 0.54] | 13.23 [8.84, 17.29] | 6.47 [3.01, 9.33] | -2.68 [-5.41, -0.33]* | -2.66 [-5.40, -0.42]* |  | -2.02 [-4.57, 0.14] | -4.25 [-8.77, -0.57]* | -1.39 [-3.96, 0.90] | -3.46 [-7.57, -0.07]* | 2.18 [-1.80, 5.43] | 4.75 [1.71, 7.63] |  | 0.42 [-2.02, 2.68] | clim |
| 14 |  |  |  |  | -1.94 [-4.18, 0.30] | -1.80 [-3.92, 0.39] |  | -1.17 [-3.04, 1.05] | 0.66 [-4.52, 5.37] | -0.43 [-2.72, 1.98] | -0.31 [-4.24, 3.67] | 4.67 [0.73, 8.59] | 4.28 [0.88, 7.62] |  | 1.11 [-0.55, 3.29] | clim |
| 30 |  |  |  |  | -1.39 [-4.60, 1.33] | -0.87 [-3.94, 1.85] | 1.43 [-1.44, 3.98] | -1.61 [-4.25, 0.96] | -1.92 [-6.88, 3.47] | -0.29 [-2.85, 2.58] | -3.09 [-8.66, 1.41] | 8.38 [3.23, 13.67] | 8.11 [2.80, 13.79] |  |  | wt_med |

**dayPeak pairwise ordering (headliners)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.84 [0.83, 0.85] | 0.84 [0.83, 0.85] | 0.84 [0.83, 0.84] | 0.84 [0.83, 0.85] |  |  | 0.76 [0.75, 0.77] | 0.80 [0.79, 0.81] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] |  |  | 0.77 [0.75, 0.78] | 0.77 [0.76, 0.78] |
| 1 | 0.83 [0.82, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.83 [0.82, 0.84] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.75 [0.73, 0.76] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.82] |  |  | 0.76 [0.74, 0.78] | 0.77 [0.76, 0.78] |
| 2 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.82 [0.81, 0.82] | 0.81 [0.80, 0.82] | 0.78 [0.77, 0.79] | 0.81 [0.80, 0.82] | 0.74 [0.72, 0.75] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] |  |  | 0.77 [0.76, 0.79] | 0.79 [0.78, 0.80] |
| 3 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.81 [0.81, 0.82] | 0.81 [0.81, 0.82] | 0.77 [0.76, 0.78] | 0.80 [0.79, 0.81] | 0.74 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.81] | 0.76 [0.74, 0.77] | 0.78 [0.77, 0.79] |
| 4 | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.81 [0.81, 0.82] | 0.82 [0.81, 0.82] | 0.81 [0.81, 0.82] | 0.75 [0.74, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.71, 0.75] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.75 [0.74, 0.77] | 0.79 [0.77, 0.80] | 0.75 [0.73, 0.77] | 0.77 [0.76, 0.78] |
| 5 | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.82 [0.81, 0.83] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.73 [0.72, 0.75] | 0.80 [0.79, 0.81] | 0.80 [0.78, 0.81] | 0.76 [0.75, 0.77] | 0.79 [0.78, 0.80] | 0.77 [0.75, 0.78] | 0.76 [0.75, 0.77] |
| 6 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.82] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.78 [0.77, 0.79] | 0.73 [0.72, 0.75] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.74 [0.73, 0.76] | 0.78 [0.76, 0.79] | 0.76 [0.74, 0.78] | 0.76 [0.75, 0.77] |
| 7 | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.81 [0.80, 0.81] | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.81] | 0.76 [0.74, 0.77] | 0.78 [0.77, 0.79] | 0.72 [0.70, 0.73] | 0.78 [0.77, 0.80] | 0.78 [0.77, 0.80] | 0.75 [0.73, 0.76] | 0.78 [0.77, 0.80] |  | 0.76 [0.75, 0.77] |
| 10 |  |  |  |  | 0.81 [0.80, 0.82] | 0.81 [0.80, 0.82] | 0.77 [0.76, 0.78] | 0.79 [0.78, 0.80] | 0.72 [0.70, 0.73] | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.75 [0.73, 0.76] | 0.78 [0.77, 0.79] |  | 0.77 [0.76, 0.78] |
| 14 |  |  |  |  | 0.80 [0.79, 0.81] | 0.80 [0.79, 0.81] | 0.75 [0.74, 0.76] | 0.78 [0.76, 0.79] | 0.72 [0.70, 0.73] | 0.78 [0.77, 0.79] | 0.78 [0.77, 0.79] | 0.74 [0.72, 0.75] | 0.77 [0.76, 0.78] |  | 0.75 [0.74, 0.76] |
| 21 |  |  |  |  | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.75 [0.73, 0.76] | 0.77 [0.76, 0.78] | 0.72 [0.70, 0.73] | 0.77 [0.76, 0.78] | 0.77 [0.76, 0.78] | 0.73 [0.71, 0.74] | 0.76 [0.75, 0.78] |  | 0.75 [0.74, 0.76] |
| 30 |  |  |  |  | 0.79 [0.78, 0.80] | 0.79 [0.78, 0.80] | 0.76 [0.75, 0.77] | 0.78 [0.77, 0.79] | 0.71 [0.69, 0.72] | 0.79 [0.78, 0.80] | 0.78 [0.76, 0.80] | 0.72 [0.71, 0.74] | 0.75 [0.74, 0.77] |  | 0.77 [0.75, 0.78] |
| 45 |  |  |  |  | 0.78 [0.77, 0.80] | 0.78 [0.77, 0.79] | 0.75 [0.74, 0.76] | 0.77 [0.76, 0.78] | 0.70 [0.67, 0.73] | 0.77 [0.76, 0.79] | 0.77 [0.75, 0.79] | 0.71 [0.69, 0.73] | 0.75 [0.72, 0.76] |  | 0.75 [0.74, 0.77] |
| 60 |  |  |  |  | 0.77 [0.76, 0.78] | 0.77 [0.75, 0.78] | 0.74 [0.72, 0.75] | 0.76 [0.74, 0.77] |  | 0.76 [0.74, 0.77] |  |  |  |  | 0.74 [0.73, 0.76] |
| 90 |  |  |  |  | 0.78 [0.76, 0.79] | 0.77 [0.76, 0.79] | 0.75 [0.73, 0.76] | 0.76 [0.75, 0.77] |  | 0.77 [0.75, 0.78] |  |  |  |  | 0.74 [0.72, 0.75] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.07 [0.05, 0.10]* | 0.07 [0.05, 0.10]* | 0.07 [0.05, 0.09]* | 0.07 [0.06, 0.10]* |  |  | -0.02 [-0.03, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.07, -0.01] | 0.02 [0.01, 0.04]* | 0.04 [0.03, 0.06]* |  |  | -0.02 [-0.04, 0.01] |  | wt_med |
| 1 | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.08]* | 0.06 [0.05, 0.08]* | 0.07 [0.05, 0.09]* | 0.06 [0.04, 0.07]* | 0.06 [0.04, 0.07]* | -0.02 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.02 [-0.05, 0.01] | 0.03 [0.02, 0.05]* | 0.05 [0.03, 0.06]* |  |  | -0.02 [-0.04, 0.00] |  | wt_med |
| 2 | 0.03 [0.01, 0.05]* | 0.03 [0.01, 0.04]* | 0.02 [0.01, 0.04]* | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.04]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.05 [-0.08, -0.01] | 0.03 [0.02, 0.04]* | 0.02 [0.01, 0.04]* |  |  | -0.01 [-0.02, -0.00] |  | wt_med |
| 3 | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.00] | 0.02 [0.02, 0.03]* | -0.04 [-0.07, -0.01] | 0.03 [0.02, 0.04]* | 0.04 [0.02, 0.06]* | -0.01 [-0.02, 0.01] | 0.03 [0.01, 0.04]* | -0.02 [-0.04, -0.01] |  | wt_med |
| 4 | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.06 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.02 [-0.03, -0.01] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, -0.00] | 0.02 [0.01, 0.04]* | 0.03 [0.02, 0.05]* | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.04]* | -0.01 [-0.03, 0.01] |  | wt_med |
| 5 | 0.06 [0.04, 0.07]* | 0.06 [0.05, 0.07]* | 0.06 [0.05, 0.07]* | 0.06 [0.05, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.04]* | -0.03 [-0.06, -0.00] | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.05]* | 0.00 [-0.01, 0.01] | 0.03 [0.02, 0.05]* | 0.00 [-0.01, 0.02] |  | wt_med |
| 6 | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.06 [0.04, 0.08]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, 0.00] | 0.04 [0.03, 0.05]* | 0.04 [0.03, 0.05]* | -0.01 [-0.02, 0.01] | 0.03 [0.02, 0.04]* | 0.00 [-0.02, 0.02] |  | wt_med |
| 7 | 0.05 [0.03, 0.07]* | 0.05 [0.03, 0.06]* | 0.05 [0.03, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | 0.05 [0.03, 0.07]* | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | -0.04 [-0.08, -0.01] | 0.02 [0.01, 0.04]* | 0.03 [0.02, 0.05]* | -0.00 [-0.02, 0.01] | 0.03 [0.02, 0.05]* |  |  | wt_med |
| 10 |  |  |  |  | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.01 [-0.02, 0.00] | 0.02 [0.02, 0.03]* | -0.05 [-0.09, -0.02] | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | -0.01 [-0.02, 0.01] | 0.03 [0.01, 0.04]* |  |  | wt_med |
| 14 |  |  |  |  | 0.05 [0.04, 0.07]* | 0.05 [0.04, 0.07]* | -0.01 [-0.02, -0.00] | 0.03 [0.02, 0.03]* | -0.03 [-0.06, -0.01] | 0.03 [0.01, 0.04]* | 0.04 [0.03, 0.05]* | -0.00 [-0.01, 0.01] | 0.03 [0.02, 0.05]* |  |  | wt_med |
| 21 |  |  |  |  | 0.04 [0.03, 0.06]* | 0.04 [0.03, 0.06]* | -0.01 [-0.02, -0.00] | 0.02 [0.02, 0.03]* | -0.03 [-0.07, -0.00] | 0.02 [0.01, 0.03]* | 0.04 [0.02, 0.05]* | 0.00 [-0.02, 0.02] | 0.03 [0.02, 0.05]* |  |  | wt_med |
| 30 |  |  |  |  | 0.03 [0.02, 0.05]* | 0.03 [0.02, 0.04]* | -0.01 [-0.02, 0.00] | 0.03 [0.02, 0.03]* | -0.05 [-0.09, -0.00] | 0.03 [0.02, 0.04]* | 0.03 [0.01, 0.05]* | -0.02 [-0.04, 0.00] | 0.01 [-0.01, 0.04] |  |  | wt_med |
| 45 |  |  |  |  | 0.04 [0.03, 0.05]* | 0.04 [0.02, 0.05]* | -0.00 [-0.02, 0.01] | 0.02 [0.02, 0.03]* | -0.10 [-0.16, -0.06] | 0.03 [0.01, 0.04]* | 0.03 [0.01, 0.05]* | -0.02 [-0.05, 0.00] | 0.01 [-0.02, 0.03] |  |  | wt_med |
| 60 |  |  |  |  | 0.04 [0.02, 0.06]* | 0.03 [0.01, 0.05]* | -0.01 [-0.02, 0.00] | 0.02 [0.02, 0.03]* |  | 0.02 [-0.00, 0.03] |  |  |  |  |  | wt_med |
| 90 |  |  |  |  | 0.05 [0.03, 0.07]* | 0.05 [0.03, 0.07]* |  | 0.03 [0.02, 0.05]* |  | 0.03 [0.02, 0.05]* |  |  |  |  | 0.01 [-0.01, 0.02] | clim |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.78 [4.52, 5.05] | 4.18 [3.92, 4.45] | 5.44 [5.13, 5.76] | 4.74 [4.48, 5.00] |  |  | 5.14 [4.81, 5.49] | 4.55 [4.29, 4.85] | 4.16 [3.81, 4.51] | 4.56 [4.29, 4.86] | 4.14 [3.82, 4.48] | 4.53 [4.28, 4.83] | 4.53 [4.26, 4.81] |  |  | 5.43 [5.06, 5.85] | 4.72 [4.42, 5.02] |
| 1 | 5.42 [5.13, 5.73] | 4.62 [4.34, 4.93] | 6.14 [5.81, 6.53] | 5.38 [5.10, 5.70] | 4.60 [4.31, 4.90] | 4.60 [4.31, 4.90] | 5.17 [4.84, 5.52] | 4.59 [4.31, 4.90] | 4.05 [3.70, 4.41] | 4.60 [4.31, 4.91] | 4.09 [3.80, 4.42] | 4.61 [4.32, 4.92] | 4.36 [4.11, 4.67] |  |  | 5.70 [5.34, 6.14] | 4.82 [4.52, 5.16] |
| 2 | 5.92 [5.59, 6.25] | 5.10 [4.79, 5.44] | 6.78 [6.40, 7.20] | 5.89 [5.57, 6.23] | 4.36 [4.10, 4.64] | 4.36 [4.10, 4.65] | 4.63 [4.35, 4.91] | 4.35 [4.09, 4.64] | 3.98 [3.65, 4.35] | 4.36 [4.09, 4.64] | 4.00 [3.70, 4.31] | 4.36 [4.09, 4.64] | 3.86 [3.62, 4.11] |  |  | 5.20 [4.87, 5.57] | 4.44 [4.18, 4.74] |
| 3 | 5.89 [5.56, 6.23] | 4.96 [4.66, 5.25] | 6.72 [6.35, 7.11] | 5.86 [5.55, 6.20] | 4.57 [4.26, 4.85] | 4.57 [4.26, 4.85] | 4.92 [4.60, 5.25] | 4.57 [4.26, 4.84] | 4.29 [3.92, 4.67] | 4.57 [4.26, 4.84] | 4.27 [3.96, 4.61] | 4.55 [4.25, 4.83] | 4.05 [3.82, 4.28] | 6.22 [5.78, 6.67] | 6.27 [5.84, 6.71] | 5.46 [5.09, 5.83] | 4.72 [4.41, 5.01] |
| 4 | 6.20 [5.83, 6.62] | 4.90 [4.60, 5.22] | 7.22 [6.81, 7.69] | 6.18 [5.81, 6.58] | 4.76 [4.46, 5.10] | 4.76 [4.46, 5.10] | 5.10 [4.79, 5.45] | 4.76 [4.46, 5.10] | 4.60 [4.20, 5.05] | 4.76 [4.47, 5.10] | 4.52 [4.14, 4.93] | 4.76 [4.46, 5.10] | 4.47 [4.19, 4.75] | 6.32 [5.91, 6.78] | 6.39 [5.98, 6.83] | 5.91 [5.46, 6.36] | 4.89 [4.59, 5.24] |
| 5 | 6.38 [5.99, 6.77] | 4.98 [4.68, 5.27] | 7.37 [6.90, 7.84] | 6.40 [6.02, 6.79] | 4.87 [4.55, 5.18] | 4.87 [4.55, 5.18] | 5.24 [4.88, 5.56] | 4.87 [4.55, 5.18] | 4.84 [4.34, 5.33] | 4.87 [4.55, 5.18] | 4.69 [4.26, 5.09] | 4.86 [4.54, 5.16] | 4.63 [4.34, 4.96] | 6.31 [5.82, 6.80] | 6.37 [5.87, 6.88] | 5.61 [5.23, 6.02] | 4.98 [4.67, 5.29] |
| 6 | 6.04 [5.69, 6.41] | 4.77 [4.49, 5.07] | 7.04 [6.60, 7.50] | 6.06 [5.70, 6.43] | 4.54 [4.28, 4.82] | 4.54 [4.28, 4.82] | 4.89 [4.59, 5.20] | 4.54 [4.28, 4.82] | 4.40 [4.04, 4.80] | 4.54 [4.28, 4.82] | 4.38 [4.05, 4.73] | 4.54 [4.27, 4.81] | 4.34 [4.08, 4.61] | 5.98 [5.58, 6.46] | 6.08 [5.69, 6.54] | 5.19 [4.86, 5.57] | 4.67 [4.39, 4.98] |
| 7 | 6.52 [6.13, 6.95] | 5.13 [4.81, 5.48] | 7.48 [7.00, 7.96] | 6.53 [6.15, 6.97] | 4.85 [4.56, 5.18] | 4.85 [4.56, 5.18] | 5.28 [4.92, 5.64] | 4.85 [4.56, 5.18] | 4.53 [4.15, 4.95] | 4.86 [4.56, 5.18] | 4.38 [4.06, 4.76] | 4.82 [4.52, 5.14] | 4.94 [4.63, 5.26] | 6.38 [5.95, 6.87] | 6.50 [6.08, 6.95] |  | 5.04 [4.69, 5.38] |
| 10 |  |  |  |  | 4.67 [4.36, 4.96] | 4.68 [4.36, 4.96] | 5.01 [4.68, 5.35] | 4.66 [4.35, 4.93] | 4.45 [4.09, 4.86] | 4.67 [4.36, 4.95] | 4.46 [4.12, 4.83] | 4.64 [4.34, 4.93] | 4.17 [3.93, 4.41] | 6.42 [5.97, 6.87] | 6.52 [6.07, 6.97] |  | 4.83 [4.52, 5.15] |
| 14 |  |  |  |  | 5.00 [4.70, 5.33] | 5.00 [4.70, 5.33] | 5.36 [5.00, 5.71] | 5.00 [4.70, 5.33] | 4.68 [4.28, 5.11] | 5.00 [4.70, 5.34] | 4.55 [4.18, 4.95] | 4.96 [4.67, 5.27] | 5.24 [4.91, 5.59] | 6.43 [5.96, 6.92] | 6.56 [6.09, 7.06] |  | 5.21 [4.87, 5.53] |
| 21 |  |  |  |  | 5.09 [4.77, 5.43] | 5.09 [4.77, 5.43] | 5.43 [5.07, 5.80] | 5.09 [4.78, 5.44] | 4.85 [4.43, 5.31] | 5.09 [4.78, 5.44] | 4.77 [4.37, 5.19] | 5.05 [4.74, 5.39] | 5.51 [5.16, 5.86] | 6.70 [6.21, 7.23] | 6.91 [6.42, 7.46] |  | 5.27 [4.94, 5.61] |
| 30 |  |  |  |  | 4.65 [4.38, 4.94] | 4.65 [4.38, 4.94] | 4.98 [4.66, 5.31] | 4.63 [4.36, 4.93] | 4.34 [3.94, 4.74] | 4.65 [4.38, 4.94] | 4.20 [3.83, 4.60] | 4.63 [4.37, 4.92] | 4.56 [4.27, 4.87] | 7.49 [6.90, 8.10] | 7.72 [7.16, 8.32] |  | 4.77 [4.47, 5.07] |
| 45 |  |  |  |  | 4.99 [4.66, 5.33] | 4.99 [4.66, 5.33] | 5.29 [4.92, 5.64] | 4.97 [4.64, 5.29] | 5.58 [4.83, 6.48] | 4.97 [4.65, 5.29] | 4.38 [3.92, 4.90] | 4.93 [4.61, 5.25] | 4.77 [4.47, 5.09] | 6.59 [6.00, 7.21] | 6.71 [6.14, 7.32] |  | 5.17 [4.84, 5.54] |
| 60 |  |  |  |  | 5.26 [4.91, 5.70] | 5.27 [4.92, 5.71] | 5.38 [4.98, 5.85] | 5.26 [4.91, 5.70] |  | 5.27 [4.90, 5.72] | 0.00 [0.00, 0.02] | 5.26 [4.90, 5.69] | 5.34 [4.97, 5.72] | 0.05 [0.00, 0.20] | 0.05 [0.00, 0.20] |  | 5.30 [4.94, 5.74] |
| 90 |  |  |  |  | 5.38 [4.97, 5.86] | 5.40 [4.98, 5.88] | 5.36 [4.92, 5.81] | 5.37 [4.96, 5.85] |  | 5.41 [4.98, 5.89] |  | 5.39 [4.99, 5.89] | 5.38 [5.01, 5.78] |  |  |  | 5.43 [5.02, 5.91] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.05 [-0.31, 0.37] | -0.55 [-0.86, -0.29]* | 0.72 [0.27, 1.16] | 0.01 [-0.35, 0.34] |  |  | 0.40 [0.20, 0.63] | -0.17 [-0.39, -0.01]* | -0.20 [-0.51, 0.02] | -0.17 [-0.39, -0.01]* | -0.23 [-0.56, -0.01]* | -0.17 [-0.38, -0.01]* | -0.21 [-0.76, 0.28] |  |  | 0.68 [0.30, 1.10] |  | wt_med |
| 1 | 0.54 [0.12, 0.99] | -0.23 [-0.44, -0.03]* | 1.28 [0.54, 2.08] | 0.51 [0.07, 0.95] | -0.23 [-0.54, -0.02]* | -0.23 [-0.54, -0.02]* | 0.35 [0.10, 0.62] | -0.23 [-0.55, -0.02]* | -0.28 [-0.72, 0.03] | -0.23 [-0.54, -0.02]* | -0.35 [-0.80, -0.02]* | -0.22 [-0.53, -0.01]* | -0.47 [-1.07, 0.06] |  |  | 0.87 [0.46, 1.32] |  | wt_med |
| 2 | 1.40 [0.98, 1.87] | 0.59 [0.35, 0.91] | 2.26 [1.62, 2.98] | 1.36 [0.96, 1.82] | -0.10 [-0.27, 0.00] | -0.10 [-0.26, 0.00] | 0.39 [0.20, 0.64] | -0.11 [-0.28, -0.01]* | -0.10 [-0.35, 0.03] | -0.10 [-0.27, -0.00]* | -0.16 [-0.48, 0.00] | -0.10 [-0.27, -0.00]* | -0.55 [-1.19, -0.04]* |  |  | 0.83 [0.53, 1.14] |  | wt_med |
| 3 | 1.11 [0.72, 1.54] | 0.17 [-0.00, 0.35] | 1.94 [1.25, 2.70] | 1.08 [0.69, 1.52] | -0.15 [-0.34, -0.02]* | -0.15 [-0.34, -0.02]* | 0.33 [0.16, 0.53] | -0.16 [-0.34, -0.03]* | -0.15 [-0.34, 0.02] | -0.16 [-0.34, -0.02]* | -0.15 [-0.40, 0.01] | -0.18 [-0.35, -0.03]* | -0.64 [-1.30, -0.10]* | 1.78 [1.32, 2.33] | 1.83 [1.41, 2.33] | 0.72 [0.47, 0.99] |  | wt_med |
| 4 | 1.28 [0.77, 1.81] | -0.02 [-0.23, 0.15] | 2.31 [1.36, 3.32] | 1.26 [0.75, 1.79] | -0.14 [-0.34, -0.02]* | -0.14 [-0.34, -0.02]* | 0.26 [0.10, 0.46] | -0.14 [-0.35, -0.02]* | -0.14 [-0.45, 0.03] | -0.14 [-0.34, -0.02]* | -0.16 [-0.47, 0.01] | -0.14 [-0.35, -0.01]* | -0.45 [-1.12, 0.14] | 1.64 [1.16, 2.24] | 1.70 [1.23, 2.27] | 0.92 [0.57, 1.36] |  | wt_med |
| 5 | 1.32 [0.69, 1.90] | -0.07 [-0.37, 0.16] | 2.31 [1.35, 3.32] | 1.35 [0.73, 1.94] | -0.12 [-0.28, -0.01]* | -0.12 [-0.28, -0.01]* | 0.28 [0.11, 0.48] | -0.12 [-0.28, -0.01]* | -0.09 [-0.32, 0.07] | -0.12 [-0.28, -0.01]* | -0.11 [-0.35, 0.04] | -0.12 [-0.28, -0.01]* | -0.39 [-0.95, 0.14] | 1.44 [0.86, 2.09] | 1.49 [0.94, 2.09] | 0.55 [0.19, 0.94] |  | wt_med |
| 6 | 1.32 [0.72, 1.95] | 0.05 [-0.17, 0.23] | 2.33 [1.36, 3.31] | 1.34 [0.76, 1.97] | -0.15 [-0.34, -0.02]* | -0.15 [-0.35, -0.02]* | 0.19 [0.00, 0.40] | -0.16 [-0.35, -0.02]* | -0.15 [-0.42, -0.00]* | -0.15 [-0.34, -0.02]* | -0.18 [-0.48, 0.00] | -0.16 [-0.35, -0.02]* | -0.34 [-0.98, 0.22] | 1.43 [0.99, 1.97] | 1.52 [1.10, 2.03] | 0.49 [0.21, 0.75] |  | wt_med |
| 7 | 1.46 [0.77, 2.16] | 0.07 [-0.18, 0.29] | 2.42 [1.39, 3.52] | 1.48 [0.81, 2.15] | -0.19 [-0.42, -0.03]* | -0.19 [-0.42, -0.03]* | 0.24 [-0.01, 0.53] | -0.19 [-0.42, -0.03]* | -0.23 [-0.54, -0.00]* | -0.19 [-0.42, -0.02]* | -0.24 [-0.57, -0.00]* | -0.20 [-0.43, -0.03]* | -0.09 [-0.75, 0.51] | 1.76 [1.27, 2.39] | 1.87 [1.42, 2.44] |  |  | wt_med |
| 10 |  |  |  |  | -0.16 [-0.36, -0.01]* | -0.16 [-0.35, -0.01]* | 0.27 [0.09, 0.48] | -0.17 [-0.37, -0.03]* | -0.17 [-0.38, 0.00] | -0.16 [-0.35, -0.02]* | -0.14 [-0.40, 0.04] | -0.19 [-0.37, -0.04]* | -0.64 [-1.34, -0.07]* | 1.80 [1.33, 2.32] | 1.90 [1.46, 2.38] |  |  | wt_med |
| 14 |  |  |  |  | -0.21 [-0.44, -0.03]* | -0.21 [-0.44, -0.03]* | 0.18 [-0.08, 0.45] | -0.21 [-0.43, -0.03]* | -0.24 [-0.55, -0.02]* | -0.20 [-0.42, -0.03]* | -0.27 [-0.60, -0.03]* | -0.22 [-0.45, -0.04]* | 0.06 [-0.65, 0.71] | 1.62 [1.04, 2.36] | 1.73 [1.20, 2.43] |  |  | wt_med |
| 21 |  |  |  |  | -0.19 [-0.39, -0.03]* | -0.19 [-0.38, -0.03]* | 0.19 [-0.09, 0.50] | -0.18 [-0.38, -0.03]* | -0.21 [-0.48, -0.01]* | -0.17 [-0.36, -0.03]* | -0.23 [-0.52, -0.03]* | -0.20 [-0.40, -0.04]* | 0.23 [-0.44, 0.88] | 1.70 [1.23, 2.31] | 1.87 [1.39, 2.47] |  |  | wt_med |
| 30 |  |  |  |  | -0.15 [-0.38, -0.02]* | -0.15 [-0.38, -0.02]* | 0.36 [0.17, 0.63] | -0.16 [-0.40, -0.03]* | -0.13 [-0.43, 0.03] | -0.15 [-0.38, -0.02]* | -0.14 [-0.51, 0.08] | -0.16 [-0.41, -0.02]* | -0.22 [-0.81, 0.29] | 3.08 [2.39, 3.88] | 3.28 [2.56, 4.04] |  |  | wt_med |
| 45 |  |  |  |  | -0.17 [-0.40, -0.02]* | -0.17 [-0.40, -0.02]* | 0.30 [0.07, 0.58] | -0.19 [-0.43, -0.04]* | 0.04 [-0.27, 0.30] | -0.19 [-0.42, -0.05]* | -0.24 [-0.61, -0.00]* | -0.24 [-0.46, -0.06]* | -0.38 [-1.08, 0.20] | 1.86 [1.25, 2.52] | 1.94 [1.38, 2.57] |  |  | wt_med |
| 60 |  |  |  |  | -0.05 [-0.17, 0.05] | -0.04 [-0.16, 0.05] | 0.10 [-0.20, 0.46] | -0.05 [-0.17, 0.04] |  | -0.04 [-0.15, 0.05] | 0.00 [0.00, 0.00] | -0.05 [-0.21, 0.08] | -0.04 [-0.82, 0.63] | 0.05 [0.00, 1.28] | 0.05 [0.00, 1.28] |  |  | wt_med |
| 90 |  |  |  |  | -0.14 [-0.66, 0.25] | -0.13 [-0.64, 0.26] |  | -0.15 [-0.67, 0.24] |  | -0.15 [-0.66, 0.24] |  | -0.15 [-0.70, 0.28] | -0.00 [-1.01, 0.81] |  |  |  | -0.07 [-0.53, 0.30] | clim |

**first-hour MAE, schedule known at origin**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.79 [4.47, 5.11] | 4.31 [3.98, 4.63] | 5.36 [4.99, 5.74] | 4.75 [4.44, 5.06] |  |  | 5.38 [4.96, 5.82] | 4.72 [4.38, 5.06] | 4.46 [4.04, 4.90] | 4.72 [4.38, 5.06] | 4.37 [3.99, 4.77] | 4.69 [4.36, 5.03] | 4.74 [4.42, 5.10] |  |  | 5.48 [5.04, 5.97] | 4.92 [4.56, 5.29] |
| 1 | 5.47 [5.13, 5.86] | 4.83 [4.48, 5.22] | 6.12 [5.71, 6.57] | 5.42 [5.07, 5.80] | 4.75 [4.40, 5.13] | 4.75 [4.40, 5.13] | 5.38 [4.96, 5.81] | 4.75 [4.40, 5.13] | 4.26 [3.87, 4.70] | 4.75 [4.40, 5.13] | 4.25 [3.89, 4.65] | 4.76 [4.40, 5.13] | 4.67 [4.38, 5.04] |  |  | 5.95 [5.49, 6.46] | 5.04 [4.66, 5.47] |
| 2 | 6.02 [5.60, 6.44] | 5.39 [4.99, 5.78] | 6.81 [6.32, 7.32] | 5.99 [5.57, 6.39] | 4.53 [4.20, 4.88] | 4.53 [4.20, 4.88] | 4.78 [4.43, 5.13] | 4.53 [4.20, 4.88] | 4.13 [3.73, 4.57] | 4.53 [4.20, 4.88] | 4.14 [3.79, 4.52] | 4.53 [4.21, 4.89] | 4.10 [3.81, 4.42] |  |  | 5.41 [5.00, 5.85] | 4.63 [4.31, 4.98] |
| 3 | 5.88 [5.48, 6.28] | 5.13 [4.78, 5.47] | 6.63 [6.16, 7.10] | 5.84 [5.45, 6.25] | 4.74 [4.39, 5.09] | 4.74 [4.39, 5.09] | 5.17 [4.79, 5.56] | 4.74 [4.39, 5.09] | 4.46 [4.04, 4.91] | 4.74 [4.39, 5.09] | 4.40 [4.04, 4.76] | 4.75 [4.40, 5.09] | 4.33 [4.04, 4.60] | 6.53 [6.03, 7.05] | 6.51 [6.02, 7.01] | 5.62 [5.15, 6.07] | 4.93 [4.56, 5.32] |
| 4 | 6.05 [5.62, 6.53] | 4.98 [4.62, 5.36] | 6.99 [6.47, 7.53] | 6.03 [5.59, 6.49] | 4.88 [4.53, 5.25] | 4.88 [4.53, 5.25] | 5.35 [4.95, 5.78] | 4.88 [4.53, 5.25] | 4.90 [4.40, 5.51] | 4.88 [4.53, 5.25] | 4.76 [4.29, 5.26] | 4.87 [4.52, 5.24] | 4.62 [4.28, 4.94] | 6.58 [6.03, 7.16] | 6.59 [6.05, 7.16] | 6.05 [5.49, 6.62] | 5.05 [4.70, 5.41] |
| 5 | 6.31 [5.82, 6.83] | 5.11 [4.72, 5.51] | 7.22 [6.66, 7.81] | 6.33 [5.86, 6.86] | 5.05 [4.65, 5.47] | 5.05 [4.65, 5.47] | 5.44 [4.99, 5.87] | 5.05 [4.65, 5.47] | 5.22 [4.61, 5.84] | 5.05 [4.65, 5.47] | 4.94 [4.44, 5.47] | 5.04 [4.64, 5.46] | 4.91 [4.54, 5.30] | 6.66 [6.03, 7.33] | 6.67 [6.04, 7.32] | 5.72 [5.23, 6.24] | 5.19 [4.79, 5.60] |
| 6 | 5.80 [5.38, 6.26] | 4.82 [4.48, 5.19] | 6.75 [6.24, 7.27] | 5.84 [5.41, 6.29] | 4.62 [4.30, 4.97] | 4.62 [4.30, 4.97] | 5.02 [4.66, 5.41] | 4.62 [4.30, 4.97] | 4.66 [4.23, 5.13] | 4.62 [4.30, 4.97] | 4.57 [4.18, 5.02] | 4.62 [4.30, 4.97] | 4.53 [4.22, 4.87] | 6.28 [5.79, 6.82] | 6.32 [5.83, 6.87] | 5.17 [4.75, 5.61] | 4.79 [4.44, 5.15] |
| 7 | 6.35 [5.88, 6.84] | 5.29 [4.89, 5.72] | 7.25 [6.67, 7.85] | 6.37 [5.91, 6.88] | 5.05 [4.67, 5.43] | 5.05 [4.67, 5.43] | 5.52 [5.08, 5.97] | 5.05 [4.67, 5.43] | 4.85 [4.37, 5.34] | 5.05 [4.67, 5.43] | 4.62 [4.22, 5.05] | 5.02 [4.65, 5.41] | 5.24 [4.86, 5.66] | 6.82 [6.29, 7.38] | 6.87 [6.36, 7.43] |  | 5.28 [4.87, 5.68] |
| 10 |  |  |  |  | 4.75 [4.38, 5.11] | 4.75 [4.38, 5.11] | 5.16 [4.77, 5.58] | 4.75 [4.38, 5.11] | 4.48 [4.04, 4.94] | 4.75 [4.38, 5.11] | 4.46 [4.07, 4.89] | 4.76 [4.38, 5.12] | 4.36 [4.07, 4.65] | 6.59 [6.00, 7.13] | 6.56 [5.99, 7.10] |  | 4.99 [4.61, 5.38] |
| 14 |  |  |  |  | 5.01 [4.62, 5.39] | 5.01 [4.62, 5.39] | 5.40 [4.96, 5.82] | 5.01 [4.62, 5.39] | 4.76 [4.26, 5.28] | 5.01 [4.62, 5.39] | 4.53 [4.11, 4.95] | 4.97 [4.59, 5.33] | 5.37 [4.95, 5.77] | 6.67 [6.07, 7.28] | 6.73 [6.13, 7.34] |  | 5.27 [4.86, 5.66] |
| 21 |  |  |  |  | 4.97 [4.59, 5.38] | 4.97 [4.59, 5.38] | 5.43 [4.97, 5.87] | 4.97 [4.59, 5.38] | 4.77 [4.27, 5.32] | 4.97 [4.59, 5.38] | 4.59 [4.13, 5.05] | 4.93 [4.54, 5.33] | 5.54 [5.14, 5.95] | 6.76 [6.16, 7.40] | 6.84 [6.23, 7.48] |  | 5.22 [4.81, 5.64] |
| 30 |  |  |  |  | 4.43 [4.06, 4.81] | 4.43 [4.06, 4.81] | 4.82 [4.37, 5.23] | 4.43 [4.06, 4.81] | 4.17 [3.69, 4.67] | 4.43 [4.06, 4.81] | 3.98 [3.52, 4.46] | 4.43 [4.06, 4.82] | 4.26 [3.90, 4.64] | 7.34 [6.59, 8.20] | 7.36 [6.64, 8.21] |  | 4.61 [4.19, 5.02] |
| 45 |  |  |  |  | 4.69 [4.29, 5.11] | 4.69 [4.29, 5.11] | 5.21 [4.68, 5.75] | 4.69 [4.29, 5.11] | 9.81 [8.57, 11.30] | 4.69 [4.29, 5.11] | 4.07 [3.49, 4.68] | 4.70 [4.30, 5.12] | 4.41 [4.03, 4.84] | 6.81 [5.93, 7.78] | 6.79 [5.92, 7.72] |  | 4.99 [4.54, 5.47] |
| 60 |  |  |  |  | 4.59 [4.11, 5.10] | 4.59 [4.11, 5.10] | 4.73 [4.22, 5.28] | 4.59 [4.11, 5.10] |  | 4.59 [4.11, 5.10] | 0.00 [0.00, 0.00] | 4.60 [4.12, 5.10] | 5.16 [4.65, 5.69] | 0.05 [0.00, 0.24] | 0.05 [0.00, 0.24] |  | 4.69 [4.18, 5.24] |
| 90 |  |  |  |  | 4.33 [3.70, 5.01] | 4.33 [3.70, 5.01] | 4.44 [3.87, 5.11] | 4.33 [3.70, 5.01] |  | 4.33 [3.70, 5.01] |  | 4.36 [3.74, 5.06] | 4.61 [3.99, 5.27] |  |  |  | 4.42 [3.77, 5.13] |

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
| 21 |  |  |  |  | -0.26 [-0.53, -0.05]* | -0.26 [-0.53, -0.05]* | 0.23 [-0.09, 0.55] | -0.26 [-0.53, -0.05]* | -0.29 [-0.70, -0.02]* | -0.26 [-0.53, -0.05]* | -0.33 [-0.73, -0.03]* | -0.26 [-0.54, -0.05]* | 0.31 [-0.47, 1.04] | 1.80 [1.22, 2.49] | 1.88 [1.34, 2.54] |  | wt_med |
| 30 |  |  |  |  | -0.20 [-0.53, -0.03]* | -0.20 [-0.53, -0.03]* | 0.35 [0.10, 0.65] | -0.20 [-0.53, -0.03]* | -0.21 [-0.67, -0.00]* | -0.20 [-0.53, -0.03]* | -0.23 [-0.75, 0.02] | -0.20 [-0.53, -0.03]* | -0.38 [-1.22, 0.24] | 3.07 [2.13, 4.14] | 3.09 [2.22, 4.00] |  | wt_med |
| 45 |  |  |  |  | -0.25 [-0.63, -0.04]* | -0.25 [-0.63, -0.04]* | 0.36 [0.07, 0.71] | -0.25 [-0.63, -0.04]* | -0.48 [-1.45, -0.05]* | -0.25 [-0.63, -0.04]* | -0.31 [-0.81, -0.02]* | -0.25 [-0.64, -0.04]* | -0.54 [-1.45, 0.15] | 2.11 [1.19, 3.10] | 2.09 [1.25, 3.05] |  | wt_med |
| 60 |  |  |  |  | -0.08 [-0.29, 0.04] | -0.08 [-0.29, 0.04] | 0.03 [-0.36, 0.49] | -0.08 [-0.29, 0.04] |  | -0.08 [-0.29, 0.04] | 0.00 [0.00, 0.00] | -0.08 [-0.29, 0.04] | 0.42 [-0.50, 1.15] | 0.05 [0.00, 1.33] | 0.05 [0.00, 1.33] |  | wt_med |
| 90 |  |  |  |  | -0.07 [-0.29, 0.01] | -0.07 [-0.29, 0.01] | -0.09 [-0.57, 0.21] | -0.07 [-0.29, 0.01] |  | -0.07 [-0.29, 0.01] |  | -0.07 [-0.29, 0.01] | 0.20 [-0.83, 0.90] |  |  |  | wt_med |

**first-hour MAE, schedule projected at origin**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.73 [4.45, 5.06] | 3.67 [3.42, 3.97] | 5.75 [5.37, 6.20] | 4.73 [4.45, 5.06] |  |  | 4.20 [3.86, 4.58] | 3.91 [3.57, 4.29] | 2.96 [2.68, 3.31] | 3.92 [3.58, 4.31] | 3.15 [2.82, 3.50] | 3.93 [3.58, 4.34] | 3.70 [3.36, 4.09] |  |  | 5.23 [4.70, 5.83] | 3.92 [3.59, 4.28] |
| 1 | 5.22 [4.79, 5.71] | 3.81 [3.44, 4.23] | 6.24 [5.74, 6.81] | 5.26 [4.82, 5.75] | 4.04 [3.60, 4.56] | 4.04 [3.60, 4.56] | 4.45 [4.00, 4.96] | 4.03 [3.59, 4.55] | 3.31 [2.81, 4.01] | 4.04 [3.60, 4.56] | 3.46 [3.02, 4.02] | 4.07 [3.63, 4.61] | 3.30 [2.97, 3.73] |  |  | 4.85 [4.30, 5.49] | 4.04 [3.60, 4.54] |
| 2 | 5.55 [5.14, 5.95] | 4.06 [3.71, 4.51] | 6.70 [6.16, 7.24] | 5.52 [5.13, 5.93] | 3.81 [3.47, 4.17] | 3.81 [3.48, 4.18] | 4.12 [3.74, 4.56] | 3.77 [3.44, 4.11] | 3.51 [3.00, 4.15] | 3.81 [3.48, 4.17] | 3.51 [3.08, 4.00] | 3.77 [3.45, 4.13] | 3.03 [2.67, 3.44] |  |  | 4.46 [4.10, 4.88] | 3.80 [3.47, 4.16] |
| 3 | 5.93 [5.38, 6.48] | 4.33 [3.86, 4.85] | 7.07 [6.46, 7.70] | 5.96 [5.39, 6.55] | 3.97 [3.51, 4.51] | 3.97 [3.51, 4.51] | 4.08 [3.65, 4.59] | 3.93 [3.51, 4.45] | 3.70 [3.09, 4.50] | 3.95 [3.52, 4.44] | 3.82 [3.22, 4.55] | 3.85 [3.45, 4.32] | 3.06 [2.77, 3.41] | 5.00 [4.42, 5.72] | 5.41 [4.77, 6.16] | 4.87 [4.38, 5.44] | 3.97 [3.56, 4.43] |
| 4 | 6.67 [6.10, 7.39] | 4.66 [4.15, 5.31] | 7.94 [7.23, 8.77] | 6.67 [6.10, 7.40] | 4.39 [3.81, 5.22] | 4.39 [3.81, 5.22] | 4.36 [3.95, 4.88] | 4.38 [3.80, 5.21] | 3.69 [3.20, 4.34] | 4.40 [3.82, 5.21] | 3.73 [3.31, 4.24] | 4.41 [3.83, 5.22] | 4.01 [3.53, 4.60] | 5.46 [4.98, 6.00] | 5.74 [5.25, 6.30] | 5.44 [4.74, 6.35] | 4.40 [3.82, 5.21] |
| 5 | 6.61 [6.11, 7.13] | 4.55 [4.19, 4.94] | 7.86 [7.20, 8.58] | 6.61 [6.12, 7.14] | 4.28 [3.92, 4.64] | 4.28 [3.92, 4.65] | 4.64 [4.22, 5.12] | 4.30 [3.94, 4.71] | 3.68 [3.25, 4.20] | 4.30 [3.94, 4.67] | 3.84 [3.43, 4.32] | 4.27 [3.91, 4.68] | 3.78 [3.41, 4.19] | 5.16 [4.69, 5.73] | 5.37 [4.93, 5.90] | 5.23 [4.77, 5.75] | 4.32 [3.95, 4.73] |
| 6 | 6.91 [6.28, 7.57] | 4.60 [4.17, 5.07] | 8.09 [7.40, 8.85] | 6.87 [6.27, 7.53] | 4.28 [3.87, 4.77] | 4.28 [3.87, 4.77] | 4.43 [4.00, 4.89] | 4.27 [3.86, 4.77] | 3.49 [3.01, 4.06] | 4.28 [3.87, 4.78] | 3.65 [3.22, 4.17] | 4.26 [3.85, 4.74] | 3.67 [3.32, 4.09] | 4.85 [4.40, 5.39] | 5.14 [4.65, 5.74] | 5.30 [4.79, 5.91] | 4.28 [3.87, 4.76] |
| 7 | 7.14 [6.63, 7.72] | 4.53 [4.15, 5.00] | 8.30 [7.62, 9.05] | 7.12 [6.61, 7.70] | 4.14 [3.75, 4.62] | 4.14 [3.75, 4.62] | 4.43 [4.03, 4.89] | 4.14 [3.75, 4.62] | 3.43 [2.95, 4.09] | 4.16 [3.75, 4.65] | 3.49 [3.05, 4.10] | 4.12 [3.74, 4.60] | 3.88 [3.50, 4.28] | 4.79 [4.27, 5.42] | 5.17 [4.64, 5.83] |  | 4.17 [3.78, 4.64] |
| 10 |  |  |  |  | 4.44 [3.96, 4.97] | 4.44 [3.96, 4.97] | 4.55 [4.09, 5.07] | 4.37 [3.93, 4.85] | 4.37 [3.76, 5.18] | 4.42 [3.96, 4.92] | 4.46 [3.82, 5.27] | 4.30 [3.89, 4.77] | 3.59 [3.22, 4.02] | 5.92 [5.25, 6.71] | 6.41 [5.67, 7.26] |  | 4.35 [3.93, 4.81] |
| 14 |  |  |  |  | 4.99 [4.51, 5.57] | 4.99 [4.51, 5.57] | 5.25 [4.77, 5.81] | 4.99 [4.52, 5.58] | 4.47 [3.81, 5.32] | 5.00 [4.53, 5.58] | 4.63 [3.96, 5.47] | 4.94 [4.47, 5.53] | 4.89 [4.31, 5.56] | 5.75 [5.11, 6.54] | 6.08 [5.43, 6.86] |  | 5.02 [4.54, 5.61] |
| 21 |  |  |  |  | 5.40 [4.84, 6.05] | 5.40 [4.85, 6.05] | 5.45 [4.90, 6.08] | 5.41 [4.85, 6.06] | 5.04 [4.24, 5.98] | 5.42 [4.87, 6.06] | 5.22 [4.45, 6.13] | 5.35 [4.80, 5.99] | 5.42 [4.81, 6.19] | 6.54 [5.80, 7.44] | 7.09 [6.31, 8.03] |  | 5.39 [4.84, 6.05] |
| 30 |  |  |  |  | 5.03 [4.64, 5.42] | 5.03 [4.64, 5.42] | 5.24 [4.81, 5.65] | 4.99 [4.61, 5.38] | 4.63 [3.99, 5.35] | 5.02 [4.63, 5.40] | 4.62 [4.03, 5.30] | 4.98 [4.61, 5.37] | 5.05 [4.59, 5.61] | 7.77 [7.13, 8.56] | 8.37 [7.70, 9.21] |  | 5.03 [4.64, 5.43] |
| 45 |  |  |  |  | 5.46 [4.95, 6.01] | 5.47 [4.95, 6.02] | 5.40 [4.91, 5.89] | 5.40 [4.90, 5.90] | 4.09 [3.44, 4.90] | 5.40 [4.91, 5.90] | 4.83 [4.07, 5.67] | 5.29 [4.84, 5.78] | 5.30 [4.88, 5.78] | 6.28 [5.49, 7.11] | 6.61 [5.86, 7.43] |  | 5.46 [4.93, 5.96] |
| 60 |  |  |  |  | 5.98 [5.40, 6.64] | 5.99 [5.41, 6.66] | 6.00 [5.41, 6.69] | 5.97 [5.39, 6.64] |  | 5.99 [5.41, 6.68] | 0.06 [0.00, 0.24] | 5.97 [5.39, 6.65] | 5.51 [4.99, 6.12] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | 5.95 [5.38, 6.60] |
| 90 |  |  |  |  | 6.32 [5.75, 6.94] | 6.35 [5.77, 6.97] | 6.11 [5.49, 6.77] | 6.30 [5.73, 6.92] |  | 6.39 [5.81, 7.04] |  | 6.31 [5.74, 6.96] | 6.04 [5.60, 6.53] |  |  |  | 6.31 [5.73, 6.97] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.86 [0.15, 1.37] | -0.21 [-0.58, 0.12] | 1.89 [0.55, 3.11] | 0.86 [0.13, 1.38] |  |  | 0.33 [0.18, 0.53] | -0.00 [-0.02, 0.04] | -0.02 [-0.06, 0.01] | 0.01 [-0.01, 0.08] | -0.01 [-0.04, 0.03] | 0.02 [-0.01, 0.16] | -0.20 [-0.57, 0.24] |  |  | 1.27 [0.99, 1.81] |  | wt_med |
| 1 | 1.18 [-0.19, 2.03] | -0.25 [-0.75, 0.07] | 2.20 [0.15, 3.61] | 1.21 [-0.14, 2.07] | 0.01 [0.00, 0.05] | 0.01 [0.00, 0.05] | 0.45 [0.12, 0.98] | -0.00 [-0.05, 0.06] | 0.06 [0.01, 0.23] | 0.01 [-0.02, 0.07] | 0.02 [0.00, 0.05] | 0.04 [0.00, 0.14] | -0.74 [-1.21, -0.37]* |  |  | 0.83 [0.40, 1.63] |  | wt_med |
| 2 | 1.84 [0.82, 2.68] | 0.32 [0.09, 0.63] | 3.01 [1.53, 4.31] | 1.81 [0.77, 2.68] | 0.02 [-0.13, 0.13] | 0.02 [-0.11, 0.13] | 0.39 [0.19, 0.72] | -0.02 [-0.13, 0.01] | 0.12 [0.00, 0.34] | 0.01 [-0.09, 0.09] | 0.01 [-0.11, 0.11] | -0.00 [-0.19, 0.11] | -0.74 [-1.16, -0.25]* |  |  | 0.67 [0.36, 1.06] |  | wt_med |
| 3 | 1.97 [0.69, 3.31] | 0.35 [0.16, 0.62] | 3.13 [1.22, 4.83] | 2.01 [0.73, 3.32] | 0.01 [-0.16, 0.15] | 0.01 [-0.16, 0.15] | 0.09 [-0.05, 0.23] | -0.03 [-0.18, 0.04] | -0.02 [-0.92, 0.42] | -0.02 [-0.23, 0.08] | 0.09 [0.01, 0.33] | -0.10 [-0.58, 0.09] | -0.90 [-1.67, -0.39]* | 1.24 [0.83, 1.70] | 1.61 [1.22, 2.19] | 0.85 [0.50, 1.23] |  | wt_med |
| 4 | 2.24 [0.50, 3.86] | 0.13 [-0.20, 0.66] | 3.56 [1.37, 5.68] | 2.23 [0.45, 3.85] | -0.21 [-0.40, 0.05] | -0.21 [-0.40, 0.05] |  | -0.21 [-0.41, 0.04] | -0.18 [-0.45, 0.13] | -0.20 [-0.38, 0.04] | -0.22 [-0.47, 0.03] | -0.19 [-0.39, 0.10] | -0.45 [-1.21, 0.21] | 1.57 [0.78, 2.49] | 1.79 [0.93, 2.76] | 0.73 [0.33, 1.78] | -0.20 [-0.35, -0.04]* | clim |
| 5 | 2.35 [0.79, 3.34] | 0.25 [-0.05, 0.69] | 3.62 [1.83, 5.03] | 2.36 [0.77, 3.38] | -0.03 [-0.10, 0.01] | -0.03 [-0.10, 0.01] | 0.27 [0.07, 0.55] | -0.01 [-0.06, 0.09] | 0.06 [-0.00, 0.33] | -0.01 [-0.03, 0.02] | 0.01 [-0.05, 0.14] | -0.01 [-0.05, 0.04] | -0.52 [-1.05, -0.04]* | 1.34 [1.00, 1.89] | 1.51 [1.09, 1.98] | 0.90 [0.58, 1.60] |  | wt_med |
| 6 | 2.70 [0.65, 3.97] | 0.35 [0.14, 0.60] | 3.90 [1.36, 5.53] | 2.66 [0.66, 3.97] | -0.01 [-0.07, 0.01] | -0.02 [-0.08, 0.01] | 0.20 [0.09, 0.37] | -0.02 [-0.08, 0.02] | 0.03 [0.00, 0.17] | -0.01 [-0.06, 0.03] | 0.02 [-0.00, 0.08] | -0.02 [-0.07, 0.02] | -0.58 [-1.04, -0.18]* | 1.25 [0.89, 1.63] | 1.49 [0.97, 2.03] | 0.97 [0.60, 1.64] |  | wt_med |
| 7 | 3.08 [0.77, 4.66] | 0.41 [0.25, 0.70] | 4.25 [1.79, 6.34] | 3.05 [0.81, 4.66] | -0.02 [-0.06, 0.01] | -0.02 [-0.06, 0.01] | 0.31 [0.15, 0.49] | -0.02 [-0.07, 0.01] | 0.01 [-0.04, 0.11] | 0.00 [-0.03, 0.04] | 0.03 [-0.02, 0.20] | -0.04 [-0.16, 0.00] | -0.25 [-0.70, 0.16] | 1.39 [1.18, 1.99] | 1.70 [1.45, 2.46] |  |  | wt_med |
| 10 |  |  |  |  | 0.09 [-0.09, 0.31] | 0.09 [-0.09, 0.32] | 0.16 [-0.01, 0.47] | 0.02 [-0.13, 0.16] | 0.16 [-0.16, 0.70] | 0.07 [-0.08, 0.23] | 0.23 [0.05, 0.54] | -0.04 [-0.32, 0.18] | -0.75 [-1.35, -0.24]* | 1.67 [1.21, 2.25] | 2.14 [1.62, 2.88] |  |  | wt_med |
| 14 |  |  |  |  | -0.02 [-0.07, 0.01] | -0.02 [-0.07, 0.02] | 0.24 [-0.05, 0.53] | -0.02 [-0.06, 0.02] | 0.01 [-0.05, 0.10] | 0.01 [-0.02, 0.08] | -0.01 [-0.05, 0.03] | -0.07 [-0.21, -0.00]* | -0.10 [-0.56, 0.73] | 1.12 [0.18, 2.42] | 1.39 [0.41, 2.65] |  |  | wt_med |
| 21 |  |  |  |  | -0.01 [-0.04, 0.01] | -0.01 [-0.03, 0.01] | 0.08 [-0.38, 0.57] | -0.00 [-0.02, 0.03] | -0.02 [-0.13, 0.05] | 0.04 [-0.01, 0.16] | -0.01 [-0.07, 0.06] | -0.04 [-0.19, 0.02] | 0.04 [-0.80, 1.32] | 1.46 [0.70, 2.95] | 1.86 [0.98, 3.59] |  |  | wt_med |
| 30 |  |  |  |  | -0.06 [-0.22, 0.00] | -0.05 [-0.21, 0.01] | 0.38 [0.16, 0.79] | -0.10 [-0.28, -0.01]* | 0.00 [-0.16, 0.18] | -0.07 [-0.23, -0.00]* | 0.00 [-0.21, 0.22] | -0.09 [-0.33, 0.03] | 0.05 [-0.95, 0.99] | 3.12 [2.13, 4.19] | 3.63 [2.42, 4.79] |  |  | wt_med |
| 45 |  |  |  |  | -0.26 [-0.72, 0.00] | -0.26 [-0.72, 0.01] |  | -0.31 [-0.77, -0.08]* | -0.63 [-2.23, 0.06] | -0.31 [-0.74, -0.08]* | -0.64 [-1.42, -0.08]* | -0.41 [-0.84, -0.15]* | -0.24 [-1.31, 0.74] | 1.06 [0.36, 1.60] | 1.29 [0.62, 1.77] |  | -0.22 [-0.53, -0.03]* | clim |
| 60 |  |  |  |  | -0.01 [-0.13, 0.11] | -0.00 [-0.11, 0.11] | 0.17 [-0.18, 0.58] | -0.01 [-0.12, 0.09] |  | 0.01 [-0.09, 0.13] | 0.00 [0.00, 0.00] | -0.01 [-0.21, 0.17] | -0.49 [-1.75, 0.42] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  |  | wt_med |
| 90 |  |  |  |  | -0.27 [-1.14, 0.38] | -0.24 [-1.05, 0.38] |  | -0.29 [-1.13, 0.35] |  | -0.28 [-1.11, 0.37] |  | -0.30 [-1.17, 0.40] | -0.22 [-1.99, 1.20] |  |  |  | -0.21 [-0.97, 0.38] | clim |

**rope-drop worth agreement**

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] |  |  | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] |
| 1 | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] |
| 2 | 0.87 [0.86, 0.88] | 0.88 [0.87, 0.89] | 0.87 [0.86, 0.88] | 0.87 [0.86, 0.88] | 0.89 [0.88, 0.89] | 0.89 [0.88, 0.89] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.89] | 0.89 [0.88, 0.90] | 0.90 [0.89, 0.91] |  |  | 0.91 [0.90, 0.92] | 0.89 [0.89, 0.90] |
| 3 | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.93] | 0.92 [0.91, 0.92] |
| 4 | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.89 [0.89, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] | 0.92 [0.91, 0.93] | 0.91 [0.91, 0.92] |
| 5 | 0.90 [0.89, 0.91] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.93 [0.92, 0.94] | 0.92 [0.91, 0.92] |
| 6 | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.93 [0.92, 0.94] | 0.92 [0.91, 0.92] |
| 7 | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.90] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] |  | 0.91 [0.91, 0.92] |
| 10 |  |  |  |  | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] |  | 0.92 [0.91, 0.93] |
| 14 |  |  |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.89, 0.92] | 0.90 [0.89, 0.91] |  | 0.91 [0.91, 0.92] |
| 21 |  |  |  |  | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] |  | 0.91 [0.91, 0.92] |
| 30 |  |  |  |  | 0.88 [0.87, 0.89] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.87 [0.86, 0.88] | 0.88 [0.88, 0.89] | 0.88 [0.87, 0.89] | 0.89 [0.88, 0.90] | 0.88 [0.87, 0.89] | 0.88 [0.86, 0.89] |  | 0.89 [0.88, 0.90] |
| 45 |  |  |  |  | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.91, 0.92] | 0.85 [0.82, 0.88] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.90 [0.89, 0.91] | 0.90 [0.89, 0.91] | 0.90 [0.88, 0.91] |  | 0.91 [0.90, 0.92] |
| 60 |  |  |  |  | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.91] | 0.91 [0.90, 0.92] |  | 0.91 [0.90, 0.91] | 1.00 [1.00, 1.00] | 0.90 [0.89, 0.90] | 1.00 [1.00, 1.00] | 1.00 [1.00, 1.00] |  | 0.91 [0.90, 0.92] |
| 90 |  |  |  |  | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.91 [0.90, 0.92] | 0.92 [0.91, 0.92] |  | 0.91 [0.90, 0.92] |  | 0.89 [0.88, 0.90] |  |  |  | 0.91 [0.90, 0.92] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2 | -0.02 [-0.03, -0.00] | -0.01 [-0.02, 0.00] | -0.02 [-0.04, -0.00] | -0.02 [-0.03, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.01] | 0.01 [-0.00, 0.02] |  |  | 0.00 [-0.01, 0.01] | snaive7 |
| 3 | 0.00 [-0.01, 0.02] | 0.01 [0.00, 0.02]* | 0.00 [-0.02, 0.02] | 0.00 [-0.01, 0.02] | 0.01 [0.01, 0.02]* | 0.01 [0.01, 0.02]* | 0.01 [0.01, 0.02]* | 0.01 [0.01, 0.02]* | 0.00 [-0.01, 0.02] | 0.01 [0.00, 0.02]* | 0.01 [0.00, 0.02]* | 0.01 [0.00, 0.02]* | 0.00 [-0.02, 0.01] | -0.00 [-0.02, 0.01] | 0.02 [0.01, 0.02]* | snaive7 |
| 4 | -0.01 [-0.02, 0.01] | 0.00 [-0.00, 0.01] | -0.01 [-0.04, 0.00] | -0.01 [-0.02, 0.01] | 0.01 [0.00, 0.02]* | 0.01 [0.00, 0.02]* | 0.01 [0.00, 0.02]* | 0.01 [-0.00, 0.01] | -0.01 [-0.02, 0.00] | 0.01 [-0.00, 0.01] | 0.00 [-0.00, 0.01] | 0.00 [-0.00, 0.01] | -0.01 [-0.03, 0.01] | -0.01 [-0.03, 0.01] | 0.01 [0.00, 0.02]* | snaive7 |
| 5 | -0.02 [-0.04, 0.00] | -0.00 [-0.01, 0.01] | -0.02 [-0.05, 0.00] | -0.01 [-0.04, 0.00] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.00, 0.01] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.00] | 0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, 0.00] | -0.02 [-0.04, 0.00] | 0.00 [-0.00, 0.01] | snaive7 |
| 6 | -0.01 [-0.03, 0.00] | 0.00 [-0.01, 0.01] | -0.02 [-0.04, 0.00] | -0.01 [-0.03, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.00, 0.01] | 0.00 [-0.00, 0.01] | -0.01 [-0.03, 0.01] | -0.01 [-0.03, 0.01] | 0.00 [-0.01, 0.01] | snaive7 |
| 7 | -0.02 [-0.03, -0.00] | -0.01 [-0.02, -0.00] | -0.02 [-0.04, -0.01] | -0.02 [-0.03, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.01] | -0.02 [-0.03, -0.00] | -0.02 [-0.04, -0.00] |  | wt_med |
| 10 |  |  |  |  | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  | wt_med |
| 14 |  |  |  |  | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.04, -0.00] |  | wt_med |
| 21 |  |  |  |  | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.01 [-0.03, -0.00] | -0.02 [-0.03, -0.00] |  | wt_med |
| 45 |  |  |  |  | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.00] |  | -0.00 [-0.01, 0.00] | -0.02 [-0.04, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.03, -0.00] | 0.00 [-0.00, 0.00] | clim |
| 60 |  |  |  |  | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] |  | -0.00 [-0.01, -0.00] | 0.00 [0.00, 0.00] | -0.01 [-0.02, -0.00] | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] |  | wt_med |
| 90 |  |  |  |  | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] |  | -0.00 [-0.01, 0.00] |  | -0.02 [-0.03, -0.01] |  |  |  | wt_med |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.20 [0.16, 0.24] | 0.46 [0.43, 0.49] | 0.45 [0.42, 0.49] | 0.30 [0.27, 0.33] | 0.39 [0.36, 0.43] | 0.40 [0.36, 0.43] | 0.45 [0.40, 0.49] | 0.37 [0.34, 0.40] |
| 2 | 0.13 [0.10, 0.16] | 0.44 [0.40, 0.47] | 0.45 [0.41, 0.48] | 0.43 [0.40, 0.46] | 0.47 [0.43, 0.50] | 0.43 [0.40, 0.47] | 0.43 [0.39, 0.47] | 0.42 [0.39, 0.46] |
| 3 | 0.17 [0.14, 0.21] | 0.43 [0.39, 0.46] | 0.44 [0.40, 0.47] | 0.36 [0.33, 0.39] | 0.45 [0.41, 0.48] | 0.39 [0.36, 0.43] | 0.41 [0.37, 0.46] | 0.42 [0.38, 0.45] |
| 4 | 0.20 [0.16, 0.23] | 0.41 [0.37, 0.44] | 0.41 [0.37, 0.44] | 0.31 [0.28, 0.34] | 0.39 [0.35, 0.43] | 0.40 [0.37, 0.43] | 0.40 [0.35, 0.44] | 0.32 [0.29, 0.35] |
| 5 | 0.25 [0.21, 0.29] | 0.38 [0.35, 0.42] | 0.39 [0.35, 0.42] | 0.29 [0.26, 0.33] | 0.37 [0.34, 0.41] | 0.38 [0.34, 0.41] | 0.42 [0.37, 0.46] | 0.32 [0.29, 0.36] |
| 6 | 0.23 [0.19, 0.27] | 0.44 [0.40, 0.47] | 0.43 [0.40, 0.47] | 0.28 [0.25, 0.31] | 0.41 [0.38, 0.45] | 0.43 [0.40, 0.46] | 0.40 [0.36, 0.45] | 0.34 [0.30, 0.37] |
| 7 | 0.21 [0.17, 0.25] | 0.38 [0.34, 0.42] | 0.39 [0.35, 0.42] | 0.27 [0.24, 0.30] | 0.31 [0.28, 0.35] |  | 0.41 [0.36, 0.45] | 0.32 [0.29, 0.36] |
| 10 | 0.17 [0.13, 0.21] | 0.44 [0.40, 0.47] | 0.43 [0.39, 0.47] | 0.35 [0.31, 0.38] | 0.43 [0.40, 0.47] |  | 0.40 [0.36, 0.44] | 0.38 [0.35, 0.42] |
| 14 | 0.22 [0.17, 0.26] | 0.33 [0.30, 0.37] | 0.34 [0.31, 0.38] | 0.26 [0.22, 0.29] | 0.31 [0.27, 0.34] |  | 0.34 [0.30, 0.39] | 0.30 [0.26, 0.33] |
| 21 | 0.20 [0.16, 0.24] | 0.31 [0.28, 0.34] | 0.33 [0.29, 0.36] | 0.26 [0.23, 0.29] | 0.29 [0.26, 0.32] |  | 0.32 [0.28, 0.37] | 0.31 [0.27, 0.34] |
| 30 | 0.12 [0.08, 0.15] | 0.40 [0.36, 0.44] | 0.40 [0.37, 0.44] | 0.39 [0.35, 0.43] | 0.40 [0.37, 0.44] |  | 0.36 [0.31, 0.41] | 0.39 [0.36, 0.43] |
| 45 | 0.25 [0.17, 0.34] | 0.34 [0.31, 0.38] | 0.36 [0.32, 0.40] | 0.30 [0.26, 0.33] | 0.38 [0.34, 0.42] |  | 0.38 [0.32, 0.44] | 0.34 [0.31, 0.38] |
| 60 |  | 0.29 [0.25, 0.33] | 0.27 [0.24, 0.31] | 0.27 [0.23, 0.30] | 0.29 [0.25, 0.32] |  |  | 0.29 [0.25, 0.33] |
| 90 |  | 0.28 [0.24, 0.33] | 0.30 [0.26, 0.34] | 0.22 [0.18, 0.26] | 0.33 [0.29, 0.37] |  |  | 0.27 [0.23, 0.31] |
| 120 |  | 0.27 [0.22, 0.31] | 0.26 [0.21, 0.31] | 0.24 [0.19, 0.29] | 0.21 [0.16, 0.25] |  |  | 0.28 [0.23, 0.33] |
| 180 |  | 0.20 [0.13, 0.28] | 0.21 [0.14, 0.29] | 0.12 [0.06, 0.18] | 0.21 [0.14, 0.29] |  |  | 0.20 [0.13, 0.27] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_chronos2 | lvl_chronos2_owx | lvl_clim | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.26 [-0.35, -0.16] | 0.07 [0.02, 0.10]* | 0.06 [0.02, 0.10]* | -0.10 [-0.16, -0.04] | 0.00 [-0.05, 0.05] |  | 0.02 [-0.05, 0.09] | -0.03 [-0.08, 0.03] | lvl_snaive7 |
| 2 | -0.32 [-0.40, -0.24] | -0.03 [-0.07, 0.02] | -0.02 [-0.07, 0.03] | -0.04 [-0.07, -0.00] |  | -0.03 [-0.07, 0.01] | -0.02 [-0.09, 0.04] | -0.04 [-0.09, 0.00] | lvl_naive4 |
| 3 | -0.31 [-0.39, -0.23] | -0.02 [-0.06, 0.02] | -0.01 [-0.05, 0.03] | -0.08 [-0.13, -0.04] |  | -0.06 [-0.10, -0.02] | -0.04 [-0.08, 0.01] | -0.03 [-0.08, 0.01] | lvl_naive4 |
| 4 | -0.24 [-0.33, -0.15] | 0.01 [-0.01, 0.04] | 0.01 [-0.02, 0.04] | -0.09 [-0.13, -0.05] | -0.01 [-0.05, 0.02] |  | -0.02 [-0.07, 0.04] | -0.08 [-0.12, -0.04] | lvl_snaive7 |
| 5 | -0.14 [-0.21, -0.07] | 0.01 [-0.04, 0.05] | 0.01 [-0.03, 0.05] | -0.09 [-0.15, -0.03] | -0.01 [-0.05, 0.04] |  | 0.04 [-0.01, 0.08] | -0.06 [-0.13, -0.00] | lvl_snaive7 |
| 6 | -0.20 [-0.27, -0.14] | 0.01 [-0.02, 0.05] | 0.01 [-0.03, 0.05] | -0.15 [-0.21, -0.09] | -0.01 [-0.07, 0.03] |  | -0.03 [-0.07, 0.01] | -0.10 [-0.16, -0.04] | lvl_snaive7 |
| 7 | -0.11 [-0.19, -0.03] | 0.06 [0.02, 0.10]* | 0.06 [0.03, 0.10]* | -0.05 [-0.09, -0.02] | -0.01 [-0.06, 0.04] |  | 0.07 [0.01, 0.13]* |  | lvl_wt56 |
| 10 | -0.28 [-0.36, -0.19] | 0.00 [-0.04, 0.05] | -0.00 [-0.05, 0.04] | -0.09 [-0.15, -0.02] |  |  | -0.03 [-0.08, 0.03] | -0.05 [-0.11, 0.00] | lvl_naive4 |
| 14 | -0.08 [-0.16, 0.01] | 0.03 [-0.01, 0.07] | 0.04 [-0.00, 0.08] | -0.05 [-0.10, 0.00] |  |  | 0.04 [-0.00, 0.09] | -0.00 [-0.05, 0.04] | lvl_naive4 |
| 21 | -0.10 [-0.16, -0.03] | 0.00 [-0.04, 0.05] | 0.02 [-0.02, 0.07] | -0.05 [-0.10, -0.01] | -0.02 [-0.06, 0.03] |  | 0.02 [-0.03, 0.09] |  | lvl_wt56 |
| 30 | -0.30 [-0.38, -0.22] | 0.00 [-0.04, 0.04] | 0.00 [-0.04, 0.05] | -0.01 [-0.05, 0.02] |  |  | -0.06 [-0.14, 0.01] | -0.01 [-0.04, 0.03] | lvl_naive4 |
| 45 | -0.17 [-0.29, -0.03] | -0.04 [-0.08, 0.00] | -0.02 [-0.06, 0.02] | -0.08 [-0.14, -0.02] |  |  | -0.00 [-0.10, 0.10] | -0.04 [-0.09, 0.02] | lvl_naive4 |
| 60 |  | -0.00 [-0.05, 0.04] | -0.02 [-0.07, 0.03] | -0.02 [-0.08, 0.02] | -0.00 [-0.05, 0.04] |  |  |  | lvl_wt56 |
| 90 |  | -0.04 [-0.08, 0.00] | -0.02 [-0.07, 0.02] | -0.11 [-0.17, -0.05] |  |  |  | -0.05 [-0.10, 0.00] | lvl_naive4 |
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
| d1-7 | 0.20 [0.17, 0.22] | 0.25 [0.22, 0.27] | 0.25 [0.23, 0.27] | 0.19 [0.17, 0.21] | 0.38 [0.36, 0.40] | 0.44 [0.42, 0.46] | 0.36 [0.34, 0.39] | 0.20 [0.18, 0.22] |
| d8-30 | 0.12 [0.10, 0.13] | 0.24 [0.23, 0.26] | 0.25 [0.23, 0.26] | 0.19 [0.18, 0.20] | 0.31 [0.30, 0.32] |  | 0.31 [0.30, 0.33] | 0.17 [0.16, 0.18] |
| d31-90 | 0.10 [0.09, 0.11] | 0.26 [0.25, 0.28] | 0.27 [0.25, 0.28] | 0.20 [0.19, 0.22] | 0.29 [0.28, 0.30] |  | 0.30 [0.28, 0.32] | 0.17 [0.16, 0.18] |

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
| 21 | 0.85 | 0.93 | 0.97 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 30 | 0.82 | 0.89 | 0.94 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 45 | 0.88 | 0.93 | 0.95 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 60 | 0.84 | 0.92 | 0.94 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |
| 90 | 0.88 | 0.94 | 0.92 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |  |  | 0.00 |

Secondary diagnostic — trailing 28-day slot MAE at the same lead and park as the 'stated' error:

| model | L | realised | stated_weighted | ratio |
|---|---|---|---|---|
| snaive7 | 0 | 8.33 | 8.58 | 0.97 |
| snaive7 | 1 | 8.37 | 8.60 | 0.97 |
| snaive7 | 3 | 8.03 | 8.24 | 0.97 |
| wt_med | 0 | 7.01 | 7.13 | 0.98 |
| wt_med | 1 | 7.08 | 7.22 | 0.98 |
| wt_med | 3 | 6.83 | 7.05 | 0.97 |
| wt_med | 7 | 7.36 | 7.57 | 0.97 |
| wt_med | 14 | 7.56 | 7.93 | 0.95 |
| wt_med | 30 | 7.52 | 7.86 | 0.96 |
| wt_med | 60 | 7.90 | 8.18 | 0.97 |
| wt_med | 90 | 7.36 | 9.40 | 0.78 |
| clim | 0 | 7.72 | 7.93 | 0.97 |
| clim | 1 | 7.65 | 7.86 | 0.97 |
| clim | 3 | 7.26 | 7.49 | 0.97 |
| clim | 7 | 7.87 | 8.12 | 0.97 |
| clim | 14 | 7.92 | 8.30 | 0.95 |
| clim | 30 | 7.71 | 8.19 | 0.94 |
| clim | 60 | 7.97 | 8.25 | 0.97 |
| clim | 90 | 8.04 | 9.25 | 0.87 |
| h5 | 0 | 6.96 | 7.10 | 0.98 |
| h5 | 1 | 7.02 | 7.18 | 0.98 |
| h5 | 3 | 6.73 | 6.93 | 0.97 |
| h5 | 7 | 7.28 | 7.51 | 0.97 |
| h5 | 14 | 7.47 | 7.84 | 0.95 |
| h5 | 30 | 7.43 | 7.74 | 0.96 |
| h5 | 60 | 7.82 | 7.99 | 0.98 |
| h5 | 90 | 7.27 | 9.10 | 0.80 |
| lvlh5_naive | 0 | 7.45 | 7.60 | 0.98 |
| lvlh5_naive | 1 | 7.41 | 7.58 | 0.98 |
| lvlh5_naive | 3 | 6.82 | 7.01 | 0.97 |
| lvlh5_naive | 7 | 7.83 | 7.97 | 0.98 |
| lvlh5_naive | 14 | 8.10 | 8.19 | 0.99 |
| lvlh5_naive | 30 | 7.88 | 8.19 | 0.96 |
| lvlh5_naive | 60 | 8.14 | 8.23 | 0.99 |
| lvlh5_naive | 90 | 7.95 | 9.05 | 0.88 |
| lvlh5_tft | 0 | 6.55 | 6.65 | 0.99 |
| lvlh5_tft | 1 | 6.60 | 6.73 | 0.98 |
| lvlh5_tft | 3 | 6.84 | 6.83 | 1.00 |
| lvlh5_tft | 7 | 7.28 | 7.36 | 0.99 |
| lvlh5_tft | 14 | 7.70 | 7.68 | 1.00 |
| lvlh5_tft | 30 | 8.31 | 8.27 | 1.00 |
| lvlh5_cbd | 0 | 9.49 | 9.55 | 0.99 |
| lvlh5_cbd | 1 | 9.01 | 9.03 | 1.00 |
| lvlh5_cbd | 3 | 9.41 | 9.33 | 1.01 |
| lvlh5_cbd | 7 | 9.90 | 10.03 | 0.99 |
| lvlh5_cbd | 14 | 10.24 | 10.12 | 1.01 |
| lvlh5_cbd | 30 | 11.35 | 10.15 | 1.12 |
| prod_served | 3 | 7.54 | 7.58 | 0.99 |
| prod_served | 7 | 7.70 | 7.89 | 0.97 |
| prod_served | 14 | 8.09 | 8.27 | 0.98 |
| prod_served | 30 | 9.13 | 9.98 | 0.92 |
| prod_served_lin | 3 | 7.52 | 7.56 | 0.99 |
| prod_served_lin | 7 | 7.69 | 7.89 | 0.97 |
| prod_served_lin | 14 | 8.08 | 8.26 | 0.98 |
| prod_served_lin | 30 | 9.10 | 9.94 | 0.92 |
| chronos2 | 0 | 6.12 | 6.26 | 0.98 |
| chronos2 | 1 | 6.53 | 6.69 | 0.98 |
| chronos2 | 3 | 6.92 | 7.14 | 0.97 |
| chronos2 | 7 | 7.28 | 7.56 | 0.96 |
| chronos2_x_h5 | 1 | 6.77 | 6.94 | 0.98 |
| chronos2_x_h5 | 3 | 6.81 | 7.02 | 0.97 |
| chronos2_x_h5 | 7 | 7.15 | 7.48 | 0.95 |
| chronos2_x_h5 | 14 | 7.40 | 7.91 | 0.94 |
| chronos2_x_h5 | 30 | 7.84 | 8.21 | 0.95 |
| chronos2_x_h5 | 60 | 7.95 | 8.10 | 0.98 |
| chronos2_x_h5 | 90 | 7.50 | 8.97 | 0.84 |
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
| chronos2_owx_x_h5 | 1 | 6.77 | 6.94 | 0.98 |
| chronos2_owx_x_h5 | 3 | 6.80 | 7.02 | 0.97 |
| chronos2_owx_x_h5 | 7 | 7.14 | 7.48 | 0.95 |
| chronos2_owx_x_h5 | 14 | 7.39 | 7.90 | 0.94 |
| chronos2_owx_x_h5 | 30 | 7.82 | 8.20 | 0.95 |
| chronos2_owx_x_h5 | 60 | 7.92 | 8.09 | 0.98 |
| chronos2_owx_x_h5 | 90 | 7.44 | 9.01 | 0.83 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | chronos2 | chronos2_grid | chronos2_nocov | chronos2_owx | chronos2_owx_x_h5 | chronos2_x_h5 | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.00 | 1.00 | 1.00 | 1.00 |  |  | 0.95 | 0.99 | 0.51 | 0.96 | 0.64 | 0.99 | 0.95 |  |  | 0.86 | 0.99 |
| 1 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.94 | 0.99 | 0.48 | 0.96 | 0.63 | 0.99 | 0.96 |  |  | 0.86 | 0.98 |
| 2 | 0.99 | 0.99 | 0.99 | 0.99 | 0.98 | 0.98 | 0.91 | 0.98 | 0.49 | 0.96 | 0.62 | 0.98 | 0.97 |  |  | 0.89 | 0.97 |
| 3 | 1.00 | 1.00 | 1.00 | 1.00 | 0.98 | 0.98 | 0.92 | 0.98 | 0.50 | 0.97 | 0.63 | 0.98 | 0.97 | 0.63 | 0.63 | 0.89 | 0.98 |
| 4 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.95 | 0.99 | 0.51 | 0.96 | 0.64 | 0.99 | 0.96 | 0.64 | 0.64 | 0.87 | 0.98 |
| 5 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.95 | 0.99 | 0.51 | 0.96 | 0.63 | 0.99 | 0.96 | 0.63 | 0.63 | 0.87 | 0.98 |
| 6 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.94 | 0.99 | 0.51 | 0.96 | 0.63 | 0.99 | 0.96 | 0.63 | 0.63 | 0.88 | 0.98 |
| 7 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.99 | 0.94 | 0.99 | 0.51 | 0.95 | 0.62 | 0.98 | 0.94 | 0.62 | 0.62 |  | 0.98 |
| 10 |  |  |  |  | 0.98 | 0.98 | 0.92 | 0.98 | 0.50 | 0.96 | 0.61 | 0.98 | 0.97 | 0.61 | 0.62 |  | 0.97 |
| 14 |  |  |  |  | 0.98 | 0.98 | 0.94 | 0.98 | 0.49 | 0.94 | 0.60 | 0.98 | 0.94 | 0.60 | 0.60 |  | 0.97 |
| 21 |  |  |  |  | 0.98 | 0.98 | 0.93 | 0.98 | 0.47 | 0.93 | 0.58 | 0.97 | 0.93 | 0.58 | 0.58 |  | 0.97 |
| 30 |  |  |  |  | 0.96 | 0.96 | 0.89 | 0.96 | 0.44 | 0.94 | 0.47 | 0.96 | 0.96 | 0.50 | 0.50 |  | 0.94 |
| 45 |  |  |  |  | 0.96 | 0.96 | 0.89 | 0.96 | 0.11 | 0.94 | 0.36 | 0.96 | 0.96 | 0.38 | 0.38 |  | 0.95 |
| 60 |  |  |  |  | 0.96 | 0.96 | 0.90 | 0.96 |  | 0.90 | 0.01 | 0.96 | 0.91 | 0.01 | 0.01 |  | 0.94 |
| 90 |  |  |  |  | 0.93 | 0.93 | 0.87 | 0.94 |  | 0.87 |  | 0.93 | 0.89 |  |  |  | 0.92 |

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
