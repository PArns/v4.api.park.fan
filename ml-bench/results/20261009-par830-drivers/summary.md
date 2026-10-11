# ml-bench results

Run: `20261009-par830-drivers` · generated 2026-10-10T09:58+00:00

Full evaluation period. Leads start at different target days here (first origin + L), so the curves mix lead with season — the headline horizon curves come from the common-window report.

- code: git `e0a4547aaa3c66bc93e89c20fb753a152dff95ac`, sources sha256 `915dc7843c0b0afe`, image `sha256:7b5e872f614f`; plug-ins ['driver_level']
- export: `/data/exports/20261009` (finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 196.7 shard-minutes over 3 shard(s)
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
| MAE | UC2 | all | h5 | 2 | 1 | 1 |
| MAE | UC2 | busy | h5 | 2 | 1 | 1 |
| MAE | UC2 | busy | lvlh5_tft | 2 | 1 | 1 |
| MAE | UC2-intraday | all | h5 | 3 | h8+ | h8+ |
| MAE | UC2-intraday | busy | h5 | 3 | h8+ | h8+ |
| MAE | UC2-intraday | busy | lvlh5_tft | 3 | h4-8 | h4-8 |
| MAE | UC3 | all | h5 | 14 | 45 | 90 |
| MAE | UC3 | busy | h5 | 14 | 45 | 90 |
| MAE | UC3 | busy | lvlh5_naive | 14 |  | 6 |
| MAE | UC3 | busy | lvlh5_tft | 12 | 2 | 2 |
| MAE, schedule known at origin | UC3 | all | h5 | 15 | 45 | 90 |
| MAE, schedule projected at origin | UC3 | all | h5 | 15 | 14 | 14 |
| Spearman of slots within ride-day | UC2/UC3 | all | driver_level | 15 |  | 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | driver_level_x_h5 | 14 | 30 | 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | 15 | 90 | 90 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | 15 | 45 | 45 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_tft | 13 | 30 | 30 |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served | 10 |  | 45 |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served_lin | 10 |  | 45 |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | 5 |  | 30 |
| best-time hit rate (top-2) | D3 | all | h5 | 15 | 60 | 60 |
| best-time hit rate (top-2) | D3 | all | wt_med | 15 | 14 | 14 |
| best-time hit rate (±30 min) | D3 | all | h5 | 15 | 45 | 45 |
| best-time hit rate (±30 min) | D3 | all | wt_med | 15 | 14 | 14 |
| best-time regret (min) | D3 | all | h5 | 15 | 60 | 60 |
| best-time regret (min) | D3 | all | wt_med | 14 |  | 14 |
| crowd bucket exact | D6 | all | lvl_driver_level | 16 |  | 90 |
| day comparison winner accuracy | D7 | all | lvl_driver_level | 3 |  | d31-90 |
| dayPeak Spearman across rides | UC3 | all | h5 | 15 | 6 | 21 |
| dayPeak Spearman across rides | UC3 | all | prod_served | 10 | 6 | 6 |
| dayPeak Spearman across rides | UC3 | all | prod_served_lin | 10 | 6 | 6 |
| dayPeak Spearman across rides | UC3 | all | wt_med | 13 | 6 | 21 |
| dayPeak abs error (min) | UC3 | all | driver_level | 15 |  | 4 |
| dayPeak abs error (min) | UC3 | all | driver_level_x_h5 | 14 |  | 4 |
| dayPeak abs error (min) | UC3 | all | h5 | 15 |  | 4 |
| dayPeak abs error (min) | UC3 | all | lvlh5_naive | 15 |  | 6 |
| dayPeak abs error (min) | UC3 | all | lvlh5_tft | 13 | 0 | 3 |
| dayPeak abs error (min) | UC3 | all | prod_served | 10 | 5 | 5 |
| dayPeak abs error (min) | UC3 | all | prod_served_lin | 10 | 5 | 5 |
| dayPeak abs error (min) | UC3 | all | wt_med | 11 |  | 5 |
| dayPeak pairwise ordering (headliners) | D4 | all | driver_level | 15 | 90 | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | driver_level_x_h5 | 14 | 90 | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | 15 | 90 | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | 15 | 90 | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | 13 | 45 | 45 |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served_lin | 10 | 45 | 45 |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | 4 |  | 90 |
| first-hour MAE (opening-aligned) | D5 | all | driver_level | 15 | 45 | 45 |
| first-hour MAE (opening-aligned) | D5 | all | driver_level_x_h5 | 14 | 45 | 45 |
| first-hour MAE (opening-aligned) | D5 | all | h5 | 15 | 45 | 90 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | 12 | 30 | 30 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | 15 | 45 | 45 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | 13 | 45 | 45 |
| first-hour MAE, schedule known at origin | D5 | all | driver_level | 15 | 45 | 45 |
| first-hour MAE, schedule known at origin | D5 | all | driver_level_x_h5 | 14 | 45 | 45 |
| first-hour MAE, schedule known at origin | D5 | all | h5 | 15 | 45 | 45 |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_cbd | 12 | 30 | 30 |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_naive | 15 | 45 | 45 |
| first-hour MAE, schedule known at origin | D5 | all | lvlh5_tft | 13 | 45 | 45 |
| first-hour MAE, schedule projected at origin | D5 | all | driver_level | 15 |  | 7 |
| first-hour MAE, schedule projected at origin | D5 | all | driver_level_x_h5 | 14 | 7 | 7 |
| first-hour MAE, schedule projected at origin | D5 | all | h5 | 15 |  | 90 |
| first-hour MAE, schedule projected at origin | D5 | all | lvlh5_naive | 15 |  | 6 |
| optimiser regret (min per park-day) | D4 | all | driver_level | 5 | 30 | 30 |
| optimiser regret (min per park-day) | D4 | all | driver_level_x_h5 | 5 | 30 | 30 |
| optimiser regret (min per park-day) | D4 | all | h5 | 5 | 30 | 30 |
| optimiser regret (min per park-day) | D4 | all | lvlh5_naive | 5 |  | 3 |
| optimiser regret (min per park-day) | D4 | all | lvlh5_tft | 5 |  | 3 |
| rope-drop worth agreement | D5 | all | clim | 14 |  | 6 |
| rope-drop worth agreement | D5 | all | driver_level | 15 |  | 6 |
| rope-drop worth agreement | D5 | all | driver_level_x_h5 | 14 |  | 6 |
| rope-drop worth agreement | D5 | all | h5 | 15 |  | 6 |
| rope-drop worth agreement | D5 | all | lvlh5_naive | 15 |  | 6 |
| rope-drop worth agreement | D5 | all | lvlh5_tft | 13 |  | 6 |
| rope-drop worth agreement | D5 | all | wt_med | 6 |  | 6 |

### Hand-over table (input for the serving router)

Per use case × lead: among models that win against the reference (and pass the gates), the largest PAIRED margin; otherwise the reference itself.

| uc | segment | lead | reference | winner | winner_value | paired_margin | significant_models | n_origin_days |
|---|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | wt_med | h5 | 7.48 | -0.06 | 1 | 231 |
| UC2 | busy | 0 | wt_med | lvlh5_tft | 13.69 | -0.56 | 2 | 231 |
| UC3 | all | 1 | wt_med | h5 | 7.59 | -0.07 | 1 | 230 |
| UC3 | busy | 1 | wt_med | lvlh5_tft | 13.90 | -0.49 | 2 | 230 |
| UC3 | all | 2 | wt_med | h5 | 7.62 | -0.07 | 1 | 229 |
| UC3 | busy | 2 | wt_med | lvlh5_tft | 14.04 | -0.41 | 2 | 229 |
| UC3 | all | 3 | wt_med | h5 | 7.65 | -0.07 | 1 | 228 |
| UC3 | busy | 3 | wt_med | h5 | 14.65 | -0.19 | 1 | 228 |
| UC3 | all | 4 | wt_med | h5 | 7.68 | -0.07 | 1 | 227 |
| UC3 | busy | 4 | wt_med | h5 | 14.68 | -0.19 | 1 | 227 |
| UC3 | all | 5 | wt_med | h5 | 7.71 | -0.07 | 1 | 226 |
| UC3 | busy | 5 | wt_med | h5 | 14.74 | -0.19 | 2 | 226 |
| UC3 | all | 6 | wt_med | h5 | 7.77 | -0.08 | 1 | 225 |
| UC3 | busy | 6 | wt_med | lvlh5_naive | 14.73 | -0.23 | 2 | 225 |
| UC3 | all | 7 | wt_med | h5 | 7.87 | -0.08 | 1 | 224 |
| UC3 | busy | 7 | wt_med | h5 | 15.03 | -0.21 | 1 | 224 |
| UC3 | all | 10 | wt_med | h5 | 7.97 | -0.08 | 1 | 221 |
| UC3 | busy | 10 | wt_med | h5 | 15.21 | -0.21 | 1 | 221 |
| UC3 | all | 14 | wt_med | h5 | 8.12 | -0.09 | 1 | 217 |
| UC3 | busy | 14 | wt_med | h5 | 15.48 | -0.22 | 1 | 217 |
| UC3 | all | 21 | wt_med | h5 | 8.28 | -0.10 | 1 | 210 |
| UC3 | busy | 21 | wt_med | h5 | 15.75 | -0.23 | 1 | 210 |
| UC3 | all | 30 | wt_med | h5 | 8.42 | -0.10 | 1 | 201 |
| UC3 | busy | 30 | wt_med | h5 | 15.95 | -0.24 | 1 | 201 |
| UC3 | all | 45 | wt_med | h5 | 8.54 | -0.10 | 1 | 186 |
| UC3 | busy | 45 | wt_med | h5 | 16.19 | -0.25 | 1 | 186 |
| UC3 | all | 60 | clim | clim | 8.75 | 0.00 | 0 | 171 |
| UC3 | busy | 60 | clim | clim | 16.67 | 0.00 | 0 | 171 |
| UC3 | all | 90 | wt_med | h5 | 8.75 | -0.10 | 1 | 141 |
| UC3 | busy | 90 | wt_med | h5 | 16.72 | -0.22 | 1 | 141 |
| UC2 | all | 1 | wt_med | h5 | 7.59 | -0.07 | 1 | 230 |
| UC2 | busy | 1 | wt_med | lvlh5_tft | 13.90 | -0.49 | 2 | 230 |
| UC1 | all | m015 | persistence | persistence | 2.46 | 0.00 | 0 | 231 |
| UC1 | busy | m015 | persistence | persistence | 4.53 | 0.00 | 0 | 231 |
| UC1 | all | m030 | persistence | persistence | 4.15 | 0.00 | 0 | 231 |
| UC1 | busy | m030 | persistence | persistence | 7.74 | 0.00 | 0 | 231 |
| UC1 | all | m045 | persistence | persistence | 5.07 | 0.00 | 0 | 231 |
| UC1 | busy | m045 | persistence | persistence | 9.45 | 0.00 | 0 | 231 |
| UC1 | all | m060 | persistence | persistence | 5.70 | 0.00 | 0 | 231 |
| UC1 | busy | m060 | persistence | persistence | 10.62 | 0.00 | 0 | 231 |
| UC1 | all | m075 | persistence | persistence | 6.28 | 0.00 | 0 | 231 |
| UC1 | busy | m075 | persistence | persistence | 11.57 | 0.00 | 0 | 231 |
| UC1 | all | m090 | persistence | persistence | 6.79 | 0.00 | 0 | 231 |
| UC1 | busy | m090 | persistence | persistence | 12.52 | 0.00 | 0 | 231 |
| UC1 | all | m105 | persistence | persistence | 7.16 | 0.00 | 0 | 231 |
| UC1 | busy | m105 | persistence | persistence | 13.15 | 0.00 | 0 | 231 |
| UC1 | all | m120 | persistence | persistence | 7.48 | 0.00 | 0 | 231 |
| UC1 | busy | m120 | persistence | persistence | 13.76 | 0.00 | 0 | 231 |
| UC2-intraday | all | h2-4 | wt_med | h5 | 7.53 | -0.06 | 1 | 231 |
| UC2-intraday | busy | h2-4 | wt_med | lvlh5_tft | 13.79 | -0.57 | 2 | 231 |
| UC2-intraday | all | h4-8 | wt_med | h5 | 7.80 | -0.08 | 1 | 231 |
| UC2-intraday | busy | h4-8 | wt_med | lvlh5_tft | 14.08 | -0.53 | 2 | 231 |
| UC2-intraday | all | h8+ | wt_med | h5 | 7.51 | -0.09 | 1 | 231 |
| UC2-intraday | busy | h8+ | wt_med | h5 | 13.60 | -0.21 | 1 | 231 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | paired_margin | significant_models |
|---|---|---|---|---|---|---|---|
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | lvlh5_tft | 3.87 | -0.21 | 5 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | lvlh5_tft | 3.92 | -0.22 | 6 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | lvlh5_tft | 3.94 | -0.22 | 6 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | lvlh5_tft | 3.96 | -0.22 | 6 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | lvlh5_tft | 3.98 | -0.22 | 6 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | lvlh5_tft | 3.99 | -0.23 | 6 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | lvlh5_tft | 4.03 | -0.22 | 6 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | lvlh5_tft | 4.09 | -0.23 | 6 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | lvlh5_tft | 4.18 | -0.21 | 6 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | lvlh5_tft | 4.27 | -0.21 | 6 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | lvlh5_cbd | 4.56 | -0.20 | 6 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | lvlh5_cbd | 4.70 | -0.23 | 6 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | lvlh5_tft | 4.79 | -0.15 | 5 |
| first-hour MAE (opening-aligned) | D5 | 60 | clim | clim | 4.86 | 0.00 | 0 |
| first-hour MAE (opening-aligned) | D5 | 90 | wt_med | h5 | 4.90 | -0.09 | 1 |
| MAE, schedule known at origin | UC3 | 0 | wt_med | h5 | 7.52 | -0.07 | 1 |
| MAE, schedule known at origin | UC3 | 1 | wt_med | h5 | 7.63 | -0.08 | 1 |
| MAE, schedule known at origin | UC3 | 2 | wt_med | h5 | 7.67 | -0.08 | 1 |
| MAE, schedule known at origin | UC3 | 3 | wt_med | h5 | 7.69 | -0.08 | 1 |
| MAE, schedule known at origin | UC3 | 4 | wt_med | h5 | 7.70 | -0.08 | 1 |
| MAE, schedule known at origin | UC3 | 5 | wt_med | h5 | 7.70 | -0.09 | 1 |
| MAE, schedule known at origin | UC3 | 6 | wt_med | h5 | 7.72 | -0.09 | 1 |
| MAE, schedule known at origin | UC3 | 7 | wt_med | h5 | 7.81 | -0.09 | 1 |
| MAE, schedule known at origin | UC3 | 10 | wt_med | h5 | 7.84 | -0.10 | 1 |
| MAE, schedule known at origin | UC3 | 14 | wt_med | h5 | 7.90 | -0.10 | 1 |
| MAE, schedule known at origin | UC3 | 21 | wt_med | h5 | 7.94 | -0.12 | 1 |
| MAE, schedule known at origin | UC3 | 30 | wt_med | h5 | 8.00 | -0.13 | 1 |
| MAE, schedule known at origin | UC3 | 45 | wt_med | h5 | 7.87 | -0.13 | 1 |
| MAE, schedule known at origin | UC3 | 60 | clim | clim | 8.07 | 0.00 | 0 |
| MAE, schedule known at origin | UC3 | 90 | wt_med | h5 | 7.08 | -0.10 | 1 |
| first-hour MAE, schedule known at origin | D5 | 0 | wt_med | lvlh5_tft | 3.83 | -0.24 | 5 |
| first-hour MAE, schedule known at origin | D5 | 1 | wt_med | lvlh5_tft | 3.87 | -0.25 | 6 |
| first-hour MAE, schedule known at origin | D5 | 2 | wt_med | lvlh5_tft | 3.89 | -0.26 | 6 |
| first-hour MAE, schedule known at origin | D5 | 3 | wt_med | lvlh5_tft | 3.90 | -0.26 | 6 |
| first-hour MAE, schedule known at origin | D5 | 4 | wt_med | lvlh5_tft | 3.90 | -0.26 | 6 |
| first-hour MAE, schedule known at origin | D5 | 5 | wt_med | lvlh5_tft | 3.91 | -0.26 | 6 |
| first-hour MAE, schedule known at origin | D5 | 6 | wt_med | lvlh5_tft | 3.93 | -0.25 | 6 |
| first-hour MAE, schedule known at origin | D5 | 7 | wt_med | lvlh5_tft | 3.96 | -0.25 | 6 |
| first-hour MAE, schedule known at origin | D5 | 10 | wt_med | lvlh5_tft | 3.97 | -0.24 | 6 |
| first-hour MAE, schedule known at origin | D5 | 14 | wt_med | lvlh5_tft | 3.96 | -0.25 | 6 |
| first-hour MAE, schedule known at origin | D5 | 21 | wt_med | lvlh5_tft | 4.01 | -0.25 | 6 |
| first-hour MAE, schedule known at origin | D5 | 30 | wt_med | lvlh5_tft | 4.09 | -0.25 | 6 |
| first-hour MAE, schedule known at origin | D5 | 45 | wt_med | lvlh5_tft | 4.08 | -0.24 | 5 |
| first-hour MAE, schedule known at origin | D5 | 60 | clim | clim | 4.03 | 0.00 | 0 |
| first-hour MAE, schedule known at origin | D5 | 90 | clim | clim | 3.89 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 0 | wt_med | h5 | 7.31 | -0.02 | 1 |
| MAE, schedule projected at origin | UC3 | 1 | wt_med | h5 | 7.37 | -0.02 | 1 |
| MAE, schedule projected at origin | UC3 | 2 | wt_med | h5 | 7.43 | -0.02 | 1 |
| MAE, schedule projected at origin | UC3 | 3 | wt_med | h5 | 7.50 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 4 | wt_med | h5 | 7.58 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 5 | wt_med | h5 | 7.75 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 6 | wt_med | h5 | 7.94 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 7 | wt_med | h5 | 8.14 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 10 | wt_med | h5 | 8.43 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 14 | wt_med | h5 | 8.83 | -0.03 | 1 |
| MAE, schedule projected at origin | UC3 | 21 | clim | clim | 9.25 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 30 | clim | clim | 9.24 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 45 | clim | clim | 9.56 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 60 | clim | clim | 9.59 | 0.00 | 0 |
| MAE, schedule projected at origin | UC3 | 90 | clim | clim | 9.99 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 0 | wt_med | wt_med | 4.50 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 1 | wt_med | driver_level | 4.53 | -0.05 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 2 | wt_med | driver_level | 4.57 | -0.04 | 3 |
| first-hour MAE, schedule projected at origin | D5 | 3 | wt_med | driver_level | 4.61 | -0.05 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 4 | wt_med | driver_level | 4.65 | -0.06 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 5 | wt_med | driver_level | 4.74 | -0.07 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 6 | wt_med | driver_level | 4.89 | -0.09 | 4 |
| first-hour MAE, schedule projected at origin | D5 | 7 | wt_med | driver_level | 5.02 | -0.08 | 3 |
| first-hour MAE, schedule projected at origin | D5 | 10 | clim | clim | 5.39 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 14 | clim | clim | 5.58 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 21 | clim | clim | 5.74 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 30 | clim | clim | 5.66 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 45 | clim | clim | 5.90 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 60 | clim | clim | 6.08 | 0.00 | 0 |
| first-hour MAE, schedule projected at origin | D5 | 90 | wt_med | h5 | 5.94 | -0.08 | 1 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | persistence | 6.29 | 0.00 | 0 |
| best-time hit rate (top-2) | D3 | 0 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 1 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 2 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 3 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 4 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 5 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 6 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 7 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 10 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 14 | clim | h5 | 0.82 | 0.01 | 2 |
| best-time hit rate (top-2) | D3 | 21 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 30 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 45 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 60 | clim | h5 | 0.82 | 0.01 | 1 |
| best-time hit rate (top-2) | D3 | 90 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 0 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 1 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 2 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 3 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 4 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 5 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 6 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 7 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 10 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 14 | clim | h5 | 0.83 | 0.01 | 2 |
| best-time hit rate (±30 min) | D3 | 21 | clim | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 30 | clim | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 45 | clim | h5 | 0.83 | 0.01 | 1 |
| best-time hit rate (±30 min) | D3 | 60 | clim | clim | 0.83 | 0.00 | 0 |
| best-time hit rate (±30 min) | D3 | 90 | clim | clim | 0.84 | 0.00 | 0 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.82 | -0.29 | 1 |
| best-time regret (min) | D3 | 1 | clim | h5 | 2.83 | -0.41 | 2 |
| best-time regret (min) | D3 | 2 | clim | h5 | 2.83 | -0.41 | 2 |
| best-time regret (min) | D3 | 3 | clim | h5 | 2.84 | -0.40 | 2 |
| best-time regret (min) | D3 | 4 | clim | h5 | 2.84 | -0.40 | 2 |
| best-time regret (min) | D3 | 5 | clim | h5 | 2.84 | -0.39 | 2 |
| best-time regret (min) | D3 | 6 | clim | h5 | 2.86 | -0.37 | 2 |
| best-time regret (min) | D3 | 7 | clim | h5 | 2.88 | -0.36 | 2 |
| best-time regret (min) | D3 | 10 | clim | h5 | 2.89 | -0.34 | 2 |
| best-time regret (min) | D3 | 14 | clim | h5 | 2.91 | -0.32 | 2 |
| best-time regret (min) | D3 | 21 | clim | h5 | 2.90 | -0.32 | 1 |
| best-time regret (min) | D3 | 30 | clim | h5 | 2.91 | -0.31 | 1 |
| best-time regret (min) | D3 | 45 | clim | h5 | 2.92 | -0.30 | 1 |
| best-time regret (min) | D3 | 60 | clim | h5 | 2.94 | -0.17 | 1 |
| best-time regret (min) | D3 | 90 | clim | clim | 2.96 | 0.00 | 0 |
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | h5 | 0.46 | 0.02 | 3 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | h5 | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | h5 | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | h5 | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | h5 | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | h5 | 0.46 | 0.02 | 5 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.45 | 0.02 | 6 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.45 | 0.02 | 6 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.44 | 0.02 | 6 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | clim | h5 | 0.43 | 0.03 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | clim | prod_served_lin | 0.42 | 0.04 | 8 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | clim | prod_served_lin | 0.41 | 0.03 | 4 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | clim | h5 | 0.41 | 0.02 | 1 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | clim | h5 | 0.41 | 0.02 | 1 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | lvlh5_tft | 8.57 | -0.25 | 1 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | wt_med | 8.93 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 2 | snaive7 | wt_med | 8.98 | -0.46 | 6 |
| dayPeak abs error (min) | UC3 | 3 | snaive7 | prod_served_lin | 8.54 | -0.49 | 8 |
| dayPeak abs error (min) | UC3 | 4 | snaive7 | prod_served_lin | 8.60 | -0.41 | 7 |
| dayPeak abs error (min) | UC3 | 5 | snaive7 | lvlh5_naive | 9.30 | -0.35 | 4 |
| dayPeak abs error (min) | UC3 | 6 | snaive7 | lvlh5_naive | 9.30 | -0.33 | 1 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | wt_med | 9.25 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | wt_med | 9.37 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 14 | clim | clim | 9.47 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 21 | clim | clim | 9.47 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 30 | clim | clim | 9.43 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 45 | clim | clim | 9.42 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 60 | clim | clim | 9.43 | 0.00 | 0 |
| dayPeak abs error (min) | UC3 | 90 | clim | clim | 9.34 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 0 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 2 | snaive7 | wt_med | 0.91 | 0.01 | 7 |
| rope-drop worth agreement | D5 | 3 | snaive7 | wt_med | 0.91 | 0.01 | 7 |
| rope-drop worth agreement | D5 | 4 | snaive7 | wt_med | 0.91 | 0.01 | 7 |
| rope-drop worth agreement | D5 | 5 | snaive7 | wt_med | 0.91 | 0.01 | 7 |
| rope-drop worth agreement | D5 | 6 | snaive7 | clim | 0.91 | 0.01 | 7 |
| rope-drop worth agreement | D5 | 7 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 10 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 14 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 21 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 30 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 45 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 60 | wt_med | wt_med | 0.91 | 0.00 | 0 |
| rope-drop worth agreement | D5 | 90 | clim | clim | 0.92 | 0.00 | 0 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | lvlh5_tft | 0.77 | 0.04 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | lvlh5_tft | 0.76 | 0.04 | 5 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | lvlh5_tft | 0.76 | 0.04 | 5 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | lvlh5_tft | 0.76 | 0.04 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | lvlh5_tft | 0.76 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | lvlh5_tft | 0.76 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | lvlh5_tft | 0.75 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | lvlh5_tft | 0.75 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | lvlh5_tft | 0.74 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | lvlh5_tft | 0.74 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | lvlh5_tft | 0.73 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | clim | lvlh5_tft | 0.72 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | clim | lvlh5_tft | 0.72 | 0.03 | 6 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | clim | lvlh5_naive | 0.71 | 0.03 | 4 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | clim | lvlh5_naive | 0.72 | 0.03 | 5 |
| dayPeak Spearman across rides | UC3 | 0 | snaive7 | wt_med | 0.77 | 0.02 | 2 |
| dayPeak Spearman across rides | UC3 | 1 | snaive7 | wt_med | 0.77 | 0.02 | 2 |
| dayPeak Spearman across rides | UC3 | 2 | snaive7 | wt_med | 0.76 | 0.02 | 2 |
| dayPeak Spearman across rides | UC3 | 3 | snaive7 | wt_med | 0.76 | 0.02 | 4 |
| dayPeak Spearman across rides | UC3 | 4 | snaive7 | wt_med | 0.76 | 0.02 | 4 |
| dayPeak Spearman across rides | UC3 | 5 | snaive7 | prod_served_lin | 0.76 | 0.02 | 4 |
| dayPeak Spearman across rides | UC3 | 6 | snaive7 | wt_med | 0.76 | 0.01 | 4 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.76 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.75 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 14 | clim | wt_med | 0.74 | 0.01 | 2 |
| dayPeak Spearman across rides | UC3 | 21 | clim | wt_med | 0.74 | 0.01 | 2 |
| dayPeak Spearman across rides | UC3 | 30 | clim | clim | 0.73 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 45 | clim | clim | 0.73 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 60 | clim | clim | 0.72 | 0.00 | 0 |
| dayPeak Spearman across rides | UC3 | 90 | clim | clim | 0.73 | 0.00 | 0 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | driver_level_x_h5 | 28.23 | -1.23 | 3 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | driver_level | 28.35 | -1.52 | 5 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | driver_level_x_h5 | 28.81 | -1.17 | 3 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | driver_level | 29.35 | -1.47 | 3 |
| optimiser regret (min per park-day) | D4 | 30 | clim | driver_level_x_h5 | 30.06 | -1.44 | 3 |
| next-best precision | D1 | 0-120 | snaive7 | snaive7 | 0.46 | 0.00 | 0 |
| next-best precision | D1 | 0-60 | snaive7 | snaive7 | 0.37 | 0.00 | 0 |
| next-best precision (top-3) | D1 | 0-120 | snaive7 | snaive7 | 0.53 | 0.00 | 0 |
| next-best precision (top-3) | D1 | 0-60 | snaive7 | snaive7 | 0.44 | 0.00 | 0 |
| next-best recall | D1 | 0-120 | snaive7 | snaive7 | 0.56 | 0.00 | 0 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.51 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 1 | lvl_snaive7 | lvl_snaive7 | 0.28 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 2 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 3 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 4 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 5 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 6 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 7 | lvl_naive4 | lvl_naive4 | 0.17 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 10 | lvl_naive4 | lvl_naive4 | 0.18 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 14 | lvl_naive4 | lvl_naive4 | 0.14 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 21 | lvl_naive4 | lvl_naive4 | 0.13 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 30 | lvl_clim | lvl_clim | 0.15 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 45 | lvl_naive4 | lvl_naive4 | 0.16 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 60 | lvl_naive4 | lvl_naive4 | 0.16 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 90 | lvl_wt56 | lvl_wt56 | 0.16 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 120 | lvl_naive4 | lvl_naive4 | 0.21 | 0.00 | 0 |
| UC4 Spearman within park-month | UC4 | 180 | lvl_naive4 | lvl_naive4 | 0.33 | 0.00 | 0 |
| crowd bucket exact | D6 | 1 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 2 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 3 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 4 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 5 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 6 | lvl_snaive7 | lvl_snaive7 | 0.40 | 0.00 | 0 |
| crowd bucket exact | D6 | 7 | lvl_naive4 | lvl_naive4 | 0.34 | 0.00 | 0 |
| crowd bucket exact | D6 | 10 | lvl_naive4 | lvl_naive4 | 0.34 | 0.00 | 0 |
| crowd bucket exact | D6 | 14 | lvl_naive4 | lvl_driver_level | 0.33 | 0.02 | 1 |
| crowd bucket exact | D6 | 21 | lvl_naive4 | lvl_driver_level | 0.32 | 0.03 | 1 |
| crowd bucket exact | D6 | 30 | lvl_naive4 | lvl_driver_level | 0.31 | 0.03 | 1 |
| crowd bucket exact | D6 | 45 | lvl_naive4 | lvl_driver_level | 0.30 | 0.03 | 1 |
| crowd bucket exact | D6 | 60 | lvl_naive4 | lvl_driver_level | 0.30 | 0.04 | 1 |
| crowd bucket exact | D6 | 90 | lvl_naive4 | lvl_driver_level | 0.30 | 0.02 | 1 |
| crowd bucket exact | D6 | 120 | lvl_naive4 | lvl_naive4 | 0.28 | 0.00 | 0 |
| crowd bucket exact | D6 | 180 | lvl_naive4 | lvl_naive4 | 0.26 | 0.00 | 0 |
| day comparison winner accuracy | D7 | d1-7 | lvl_snaive7 | lvl_snaive7 | 0.48 | 0.00 | 0 |
| day comparison winner accuracy | D7 | d8-30 | lvl_naive4 | lvl_driver_level | 0.43 | 0.07 | 1 |
| day comparison winner accuracy | D7 | d31-90 | lvl_naive4 | lvl_driver_level | 0.43 | 0.09 | 1 |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.85 [7.77, 7.93] | 7.50 [7.41, 7.59] | 5.42 [5.38, 5.47] | 6.54 [6.46, 6.62] | 7.59 [7.50, 7.66] | 7.66 [7.57, 7.73] |
| 2 | 7.86 [7.78, 7.94] | 7.57 [7.48, 7.66] | 5.43 [5.38, 5.48] | 6.54 [6.46, 6.61] | 7.62 [7.54, 7.70] | 7.70 [7.61, 7.77] |
| 3 | 7.87 [7.79, 7.95] | 7.65 [7.57, 7.75] | 5.43 [5.39, 5.48] | 6.54 [6.45, 6.61] | 7.65 [7.57, 7.73] | 7.73 [7.65, 7.81] |
| 4 | 7.87 [7.79, 7.95] | 7.70 [7.62, 7.79] | 5.44 [5.39, 5.49] | 6.53 [6.45, 6.60] | 7.68 [7.60, 7.75] | 7.76 [7.68, 7.83] |
| 5 | 7.88 [7.80, 7.96] | 7.78 [7.68, 7.87] | 5.45 [5.40, 5.50] | 6.53 [6.45, 6.61] | 7.71 [7.63, 7.79] | 7.79 [7.71, 7.87] |
| 6 | 7.90 [7.82, 7.98] | 7.85 [7.75, 7.94] | 5.47 [5.42, 5.52] | 6.53 [6.45, 6.61] | 7.77 [7.68, 7.85] | 7.85 [7.77, 7.93] |
| 7 | 8.21 [8.13, 8.29] | 8.04 [7.94, 8.13] | 5.51 [5.46, 5.56] | 6.99 [6.90, 7.07] | 7.87 [7.79, 7.95] | 7.96 [7.88, 8.05] |
| 10 | 8.25 [8.16, 8.33] | 8.20 [8.10, 8.30] | 5.53 [5.48, 5.58] | 6.99 [6.90, 7.06] | 7.97 [7.89, 8.05] | 8.07 [7.98, 8.15] |
| 14 | 8.50 [8.41, 8.58] | 8.45 [8.35, 8.55] | 5.59 [5.54, 5.64] | 7.30 [7.22, 7.38] | 8.12 [8.04, 8.20] | 8.22 [8.14, 8.31] |
| 21 | 8.71 [8.62, 8.80] | 8.67 [8.55, 8.77] | 5.64 [5.59, 5.70] | 7.55 [7.46, 7.63] | 8.28 [8.19, 8.36] | 8.40 [8.31, 8.48] |
| 30 | 8.83 [8.73, 8.92] | 9.03 [8.91, 9.16] | 5.66 [5.61, 5.71] | 7.67 [7.57, 7.76] | 8.42 [8.33, 8.51] | 8.55 [8.46, 8.64] |
| 45 | 8.96 [8.86, 9.05] | 9.32 [9.16, 9.48] | 5.68 [5.63, 5.73] | 7.82 [7.72, 7.91] | 8.54 [8.44, 8.63] | 8.68 [8.59, 8.77] |
| 60 | 9.12 [9.00, 9.22] | 4.26 [3.05, 5.62] | 5.70 [5.65, 5.76] | 8.01 [7.91, 8.12] | 8.69 [8.58, 8.79] | 8.83 [8.72, 8.93] |
| 90 | 9.27 [9.16, 9.39] |  | 5.70 [5.63, 5.76] | 8.09 [7.97, 8.22] | 8.75 [8.64, 8.87] | 8.91 [8.79, 9.03] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape).

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | 7.97 [7.88, 8.05] | 7.40 [7.31, 7.47] | 7.71 [7.63, 7.79] | 7.24 [7.15, 7.32] | 2.46 [2.43, 2.49] | 8.60 [8.50, 8.70] | 7.45 [7.37, 7.52] |
| m030 | 7.95 [7.86, 8.03] | 7.36 [7.28, 7.44] | 7.70 [7.61, 7.78] | 7.25 [7.16, 7.33] | 4.15 [4.11, 4.19] | 8.57 [8.48, 8.67] | 7.44 [7.36, 7.51] |
| m045 | 7.98 [7.90, 8.06] | 7.40 [7.31, 7.47] | 7.73 [7.65, 7.81] | 7.29 [7.20, 7.38] | 5.07 [5.02, 5.12] | 8.59 [8.50, 8.69] | 7.47 [7.39, 7.54] |
| m060 | 8.06 [7.97, 8.14] | 7.47 [7.39, 7.55] | 7.78 [7.70, 7.86] | 7.35 [7.26, 7.43] | 5.70 [5.64, 5.76] | 8.67 [8.57, 8.76] | 7.54 [7.46, 7.61] |
| m075 | 8.12 [8.03, 8.20] | 7.56 [7.48, 7.64] | 7.92 [7.83, 7.99] | 7.43 [7.34, 7.52] | 6.28 [6.22, 6.35] | 8.76 [8.66, 8.86] | 7.61 [7.52, 7.68] |
| m090 | 8.16 [8.07, 8.24] | 7.57 [7.49, 7.65] | 7.95 [7.86, 8.03] | 7.47 [7.38, 7.56] | 6.79 [6.73, 6.86] | 8.79 [8.69, 8.89] | 7.64 [7.56, 7.72] |
| m105 | 8.22 [8.12, 8.30] | 7.61 [7.53, 7.69] | 8.00 [7.91, 8.08] | 7.51 [7.42, 7.60] | 7.16 [7.09, 7.23] | 8.85 [8.75, 8.95] | 7.68 [7.60, 7.76] |
| m120 | 8.26 [8.17, 8.34] | 7.65 [7.57, 7.73] | 8.00 [7.92, 8.08] | 7.51 [7.42, 7.60] | 7.48 [7.41, 7.56] | 8.87 [8.78, 8.97] | 7.72 [7.64, 7.80] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| m015 | 5.98 [5.47, 6.57] | 5.41 [4.95, 5.94] | 5.75 [5.25, 6.32] | 5.33 [4.89, 5.90] | 6.57 [6.01, 7.22] | 5.45 [4.98, 6.00] | persistence |
| m030 | 4.31 [3.87, 4.79] | 3.71 [3.32, 4.14] | 4.08 [3.66, 4.56] | 3.71 [3.34, 4.17] | 4.93 [4.46, 5.46] | 3.78 [3.39, 4.22] | persistence |
| m045 | 3.41 [3.02, 3.85] | 2.78 [2.42, 3.17] | 3.17 [2.79, 3.61] | 2.80 [2.44, 3.21] | 4.00 [3.59, 4.46] | 2.85 [2.49, 3.24] | persistence |
| m060 | 2.78 [2.40, 3.20] | 2.14 [1.78, 2.51] | 2.50 [2.12, 2.90] | 2.14 [1.81, 2.51] | 3.35 [2.98, 3.78] | 2.19 [1.84, 2.57] | persistence |
| m075 | 2.39 [1.99, 2.81] | 1.70 [1.33, 2.07] | 2.07 [1.69, 2.47] | 1.65 [1.32, 2.01] | 2.98 [2.61, 3.41] | 1.76 [1.39, 2.13] | persistence |
| m090 | 1.87 [1.47, 2.25] | 1.14 [0.79, 1.49] | 1.55 [1.18, 1.93] | 1.14 [0.83, 1.50] | 2.44 [2.09, 2.81] | 1.22 [0.87, 1.57] | persistence |
| m105 | 1.50 [1.09, 1.92] | 0.73 [0.38, 1.10] | 1.16 [0.78, 1.58] | 0.75 [0.42, 1.12] | 2.07 [1.70, 2.48] | 0.82 [0.46, 1.19] | persistence |
| m120 | 1.15 [0.76, 1.52] | 0.36 [0.02, 0.72] | 0.77 [0.40, 1.16] | 0.37 [0.03, 0.73] | 1.70 [1.35, 2.09] | 0.45 [0.12, 0.81] | persistence |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | 15.52 [15.36, 15.68] | 14.31 [14.16, 14.46] | 14.62 [14.46, 14.76] | 13.59 [13.44, 13.76] | 4.53 [4.48, 4.58] | 16.61 [16.42, 16.80] | 14.47 [14.32, 14.61] |
| m030 | 15.51 [15.35, 15.67] | 14.31 [14.16, 14.45] | 14.64 [14.49, 14.78] | 13.64 [13.48, 13.81] | 7.74 [7.67, 7.81] | 16.58 [16.40, 16.76] | 14.49 [14.35, 14.63] |
| m045 | 15.53 [15.37, 15.68] | 14.33 [14.19, 14.47] | 14.66 [14.52, 14.80] | 13.67 [13.53, 13.84] | 9.45 [9.37, 9.52] | 16.57 [16.40, 16.75] | 14.49 [14.35, 14.63] |
| m060 | 15.58 [15.42, 15.73] | 14.38 [14.24, 14.52] | 14.67 [14.53, 14.81] | 13.72 [13.57, 13.88] | 10.62 [10.53, 10.71] | 16.62 [16.42, 16.79] | 14.54 [14.40, 14.68] |
| m075 | 15.58 [15.42, 15.73] | 14.43 [14.29, 14.57] | 14.71 [14.56, 14.86] | 13.68 [13.53, 13.84] | 11.57 [11.47, 11.66] | 16.68 [16.48, 16.86] | 14.57 [14.42, 14.71] |
| m090 | 15.70 [15.54, 15.86] | 14.49 [14.34, 14.63] | 14.82 [14.66, 14.96] | 13.78 [13.63, 13.95] | 12.52 [12.41, 12.62] | 16.73 [16.53, 16.91] | 14.65 [14.51, 14.79] |
| m105 | 15.77 [15.61, 15.93] | 14.53 [14.38, 14.67] | 14.87 [14.72, 15.02] | 13.81 [13.65, 13.97] | 13.15 [13.05, 13.26] | 16.80 [16.60, 16.98] | 14.70 [14.55, 14.84] |
| m120 | 15.82 [15.66, 15.98] | 14.54 [14.39, 14.68] | 14.84 [14.69, 14.99] | 13.78 [13.63, 13.95] | 13.76 [13.65, 13.87] | 16.79 [16.60, 16.97] | 14.73 [14.59, 14.88] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| m015 | 11.46 [10.33, 12.52] | 10.30 [9.39, 11.17] | 10.59 [9.60, 11.56] | 9.67 [8.80, 10.47] | 12.45 [11.35, 13.49] | 10.43 [9.50, 11.31] | persistence |
| m030 | 8.23 [7.21, 9.23] | 7.06 [6.19, 7.89] | 7.39 [6.46, 8.31] | 6.49 [5.66, 7.26] | 9.27 [8.32, 10.22] | 7.22 [6.34, 8.05] | persistence |
| m045 | 6.58 [5.63, 7.52] | 5.36 [4.54, 6.18] | 5.72 [4.84, 6.56] | 4.80 [4.08, 5.56] | 7.56 [6.73, 8.42] | 5.50 [4.67, 6.33] | persistence |
| m060 | 5.39 [4.46, 6.32] | 4.14 [3.35, 4.95] | 4.46 [3.61, 5.29] | 3.53 [2.84, 4.30] | 6.35 [5.54, 7.16] | 4.28 [3.48, 5.12] | persistence |
| m075 | 4.70 [3.79, 5.58] | 3.36 [2.62, 4.12] | 3.73 [2.95, 4.52] | 2.68 [2.06, 3.37] | 5.71 [4.94, 6.50] | 3.50 [2.74, 4.29] | persistence |
| m090 | 3.73 [2.86, 4.58] | 2.32 [1.61, 3.09] | 2.75 [1.96, 3.56] | 1.69 [1.04, 2.39] | 4.70 [3.95, 5.46] | 2.48 [1.76, 3.27] | persistence |
| m105 | 3.13 [2.21, 4.06] | 1.62 [0.84, 2.43] | 2.08 [1.27, 2.95] | 0.99 [0.35, 1.76] | 4.08 [3.26, 4.96] | 1.82 [1.05, 2.63] | persistence |
| m120 | 2.48 [1.63, 3.36] | 0.91 [0.19, 1.71] | 1.33 [0.57, 2.21] | 0.26 [-0.38, 0.99] | 3.39 [2.60, 4.24] | 1.13 [0.43, 1.92] | persistence |

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
| h2-4 | 8.12 [8.03, 8.20] | 7.53 [7.45, 7.60] | 7.89 [7.80, 7.97] | 7.41 [7.32, 7.50] | 8.72 [8.63, 8.80] | 8.74 [8.64, 8.83] | 7.60 [7.51, 7.67] |
| h4-8 | 8.42 [8.33, 8.51] | 7.80 [7.72, 7.88] | 8.19 [8.10, 8.28] | 7.72 [7.63, 7.82] | 11.35 [11.25, 11.47] | 9.05 [8.95, 9.16] | 7.88 [7.80, 7.96] |
| h8+ | 8.11 [8.02, 8.21] | 7.51 [7.41, 7.60] | 7.96 [7.86, 8.06] | 7.74 [7.64, 7.85] | 14.19 [13.98, 14.40] | 8.83 [8.72, 8.94] | 7.60 [7.50, 7.69] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| h2-4 | 0.59 [0.50, 0.69] | -0.06 [-0.08, -0.05]* | 0.34 [0.28, 0.40] | 0.04 [-0.09, 0.17] | 0.66 [0.30, 1.03] | 1.17 [0.97, 1.36] | wt_med |
| h4-8 | 0.61 [0.53, 0.71] | -0.08 [-0.10, -0.06]* | 0.34 [0.28, 0.41] | 0.05 [-0.08, 0.19] | 3.22 [2.69, 3.80] | 1.22 [1.03, 1.41] | wt_med |
| h8+ | 0.56 [0.46, 0.66] | -0.09 [-0.13, -0.06]* | 0.40 [0.33, 0.47] | 0.29 [0.17, 0.41] | 6.66 [5.45, 8.20] | 1.32 [1.14, 1.51] | wt_med |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | 15.73 [15.57, 15.88] | 14.51 [14.37, 14.65] | 14.84 [14.70, 14.99] | 13.79 [13.65, 13.96] | 15.81 [15.69, 15.94] | 16.76 [16.57, 16.94] | 14.68 [14.53, 14.81] |
| h4-8 | 15.95 [15.80, 16.10] | 14.71 [14.56, 14.86] | 15.06 [14.91, 15.21] | 14.08 [13.93, 14.25] | 19.67 [19.52, 19.85] | 17.01 [16.82, 17.19] | 14.89 [14.74, 15.03] |
| h8+ | 14.73 [14.58, 14.89] | 13.60 [13.46, 13.75] | 14.04 [13.87, 14.20] | 13.60 [13.42, 13.79] | 23.26 [22.97, 23.56] | 16.00 [15.80, 16.19] | 13.80 [13.65, 13.95] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| h2-4 | 1.14 [0.93, 1.37] | -0.16 [-0.20, -0.13]* | 0.25 [0.12, 0.38] | -0.57 [-0.89, -0.27]* | 0.94 [0.15, 1.69] | 2.13 [1.72, 2.50] | wt_med |
| h4-8 | 1.15 [0.95, 1.36] | -0.18 [-0.23, -0.15]* | 0.25 [0.12, 0.38] | -0.53 [-0.86, -0.24]* | 5.26 [4.39, 6.17] | 2.22 [1.81, 2.58] | wt_med |
| h8+ | 1.00 [0.82, 1.21] | -0.21 [-0.29, -0.15]* | 0.29 [0.16, 0.41] | -0.10 [-0.34, 0.11] | 10.83 [8.51, 13.29] | 2.33 [1.99, 2.65] | wt_med |

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

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.07 [7.98, 8.15] | 7.79 [7.71, 7.87] |  | 7.48 [7.40, 7.56] | 9.96 [9.83, 10.10] | 7.83 [7.75, 7.91] | 7.37 [7.28, 7.45] | 5.40 [5.35, 5.45] | 6.55 [6.46, 6.62] | 8.69 [8.59, 8.79] | 7.55 [7.47, 7.62] |
| 1 | 8.10 [8.00, 8.18] | 7.82 [7.74, 7.91] | 7.82 [7.74, 7.91] | 7.59 [7.50, 7.66] | 10.04 [9.90, 10.18] | 7.85 [7.77, 7.93] | 7.50 [7.41, 7.59] | 5.42 [5.38, 5.47] | 6.54 [6.46, 6.62] | 8.69 [8.59, 8.79] | 7.66 [7.57, 7.73] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.59 [0.50, 0.68] | 0.27 [0.17, 0.39] |  | -0.06 [-0.08, -0.05]* | 2.61 [2.22, 3.07] | 0.33 [0.27, 0.39] | 0.04 [-0.09, 0.17] | -2.17 [-2.46, -1.92]* | -0.97 [-1.19, -0.77]* | 1.16 [0.97, 1.35] | wt_med |
| 1 | 0.52 [0.43, 0.62] | 0.21 [0.09, 0.33] | 0.21 [0.09, 0.33] | -0.07 [-0.09, -0.05]* | 2.60 [2.21, 3.06] | 0.25 [0.19, 0.31] | 0.09 [-0.05, 0.23] | -2.25 [-2.55, -2.00]* | -1.08 [-1.29, -0.87]* | 1.06 [0.87, 1.25] | wt_med |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 15.60 [15.44, 15.75] | 14.50 [14.34, 14.64] |  | 14.39 [14.25, 14.53] | 18.53 [18.27, 18.80] | 14.70 [14.56, 14.84] | 13.69 [13.54, 13.85] | 9.67 [9.59, 9.74] | 11.70 [11.54, 11.85] | 16.64 [16.45, 16.82] | 14.56 [14.41, 14.70] |
| 1 | 15.62 [15.47, 15.78] | 14.53 [14.38, 14.67] | 14.53 [14.38, 14.67] | 14.55 [14.41, 14.69] | 18.68 [18.40, 18.96] | 14.71 [14.57, 14.85] | 13.90 [13.76, 14.06] | 9.68 [9.61, 9.76] | 11.66 [11.51, 11.81] | 16.61 [16.42, 16.79] | 14.73 [14.58, 14.87] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.14 [0.93, 1.37] | 0.03 [-0.25, 0.30] |  | -0.16 [-0.20, -0.13]* | 4.40 [3.26, 5.58] | 0.24 [0.10, 0.36] | -0.56 [-0.88, -0.28]* | -4.91 [-5.44, -4.42]* | -2.76 [-2.98, -2.51]* | 2.13 [1.72, 2.48] | wt_med |
| 1 | 1.01 [0.79, 1.24] | -0.11 [-0.41, 0.17] | -0.11 [-0.41, 0.17] | -0.17 [-0.22, -0.14]* | 4.37 [3.21, 5.59] | 0.07 [-0.06, 0.20] | -0.49 [-0.82, -0.21]* | -5.07 [-5.62, -4.57]* | -2.97 [-3.18, -2.71]* | 1.93 [1.50, 2.29] | wt_med |

**MAE by region (all rides)**

| lead | region | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 9.25 | 9.14 |  | 8.57 | 13.58 | 9.02 | 8.70 | 5.76 | 7.75 | 9.74 | 8.62 |
| 0 | EU | 6.31 | 5.87 |  | 5.78 | 7.96 | 6.02 | 5.60 | 4.13 | 5.00 | 6.54 | 5.83 |
| 0 | NA | 9.09 | 8.85 |  | 8.52 | 10.25 | 8.90 | 8.39 | 6.48 | 7.32 | 10.31 | 8.61 |
| 1 | Asia | 9.25 | 9.21 | 9.21 | 8.71 | 13.68 | 9.04 | 8.83 | 5.79 | 7.75 | 9.74 | 8.76 |
| 1 | EU | 6.33 | 5.90 | 5.90 | 5.88 | 8.02 | 6.04 | 5.75 | 4.16 | 5.00 | 6.53 | 5.93 |
| 1 | NA | 9.14 | 8.87 | 8.87 | 8.60 | 10.33 | 8.92 | 8.51 | 6.50 | 7.32 | 10.31 | 8.70 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 8.10 [8.00, 8.18] | 7.82 [7.74, 7.91] | 7.82 [7.74, 7.91] | 7.59 [7.50, 7.66] | 10.04 [9.90, 10.18] | 7.85 [7.77, 7.93] | 7.50 [7.41, 7.59] | 5.42 [5.38, 5.47] | 6.54 [6.46, 6.62] |  |  | 8.69 [8.59, 8.79] | 7.66 [7.57, 7.73] |
| 2 | 8.12 [8.03, 8.20] | 7.86 [7.78, 7.94] | 7.86 [7.78, 7.94] | 7.62 [7.54, 7.70] | 10.06 [9.93, 10.20] | 7.86 [7.78, 7.94] | 7.57 [7.48, 7.66] | 5.43 [5.38, 5.48] | 6.54 [6.46, 6.61] |  |  | 8.69 [8.59, 8.78] | 7.70 [7.61, 7.77] |
| 3 | 8.14 [8.05, 8.22] | 7.87 [7.79, 7.96] | 7.87 [7.79, 7.96] | 7.65 [7.57, 7.73] | 10.07 [9.94, 10.22] | 7.87 [7.79, 7.95] | 7.65 [7.57, 7.75] | 5.43 [5.39, 5.48] | 6.54 [6.45, 6.61] | 8.11 [8.02, 8.21] | 8.09 [8.00, 8.18] | 8.68 [8.59, 8.78] | 7.73 [7.65, 7.81] |
| 4 | 8.15 [8.06, 8.23] | 7.89 [7.80, 7.97] | 7.89 [7.80, 7.97] | 7.68 [7.60, 7.75] | 10.09 [9.95, 10.23] | 7.87 [7.79, 7.95] | 7.70 [7.62, 7.79] | 5.44 [5.39, 5.49] | 6.53 [6.45, 6.60] | 8.18 [8.09, 8.27] | 8.16 [8.07, 8.25] | 8.67 [8.58, 8.77] | 7.76 [7.68, 7.83] |
| 5 | 8.17 [8.08, 8.25] | 7.91 [7.83, 7.99] | 7.91 [7.83, 7.99] | 7.71 [7.63, 7.79] | 10.13 [10.00, 10.27] | 7.88 [7.80, 7.96] | 7.78 [7.68, 7.87] | 5.45 [5.40, 5.50] | 6.53 [6.45, 6.61] | 8.26 [8.16, 8.35] | 8.23 [8.14, 8.33] | 8.67 [8.57, 8.77] | 7.79 [7.71, 7.87] |
| 6 | 8.20 [8.11, 8.29] | 7.93 [7.84, 8.01] | 7.93 [7.84, 8.01] | 7.77 [7.68, 7.85] | 10.49 [10.35, 10.64] | 7.90 [7.82, 7.98] | 7.85 [7.75, 7.94] | 5.47 [5.42, 5.52] | 6.53 [6.45, 6.61] | 8.28 [8.18, 8.38] | 8.26 [8.16, 8.36] | 8.67 [8.57, 8.77] | 7.85 [7.77, 7.93] |
| 7 | 8.23 [8.14, 8.32] | 8.15 [8.06, 8.23] | 8.15 [8.06, 8.23] | 7.87 [7.79, 7.95] | 10.64 [10.50, 10.79] | 8.21 [8.13, 8.29] | 8.04 [7.94, 8.13] | 5.51 [5.46, 5.56] | 6.99 [6.90, 7.07] | 8.49 [8.39, 8.59] | 8.47 [8.37, 8.57] |  | 7.96 [7.88, 8.05] |
| 10 | 8.29 [8.20, 8.37] | 8.22 [8.13, 8.30] | 8.22 [8.13, 8.30] | 7.97 [7.89, 8.05] | 10.76 [10.61, 10.91] | 8.25 [8.16, 8.33] | 8.20 [8.10, 8.30] | 5.53 [5.48, 5.58] | 6.99 [6.90, 7.06] | 8.66 [8.56, 8.77] | 8.64 [8.54, 8.75] |  | 8.07 [7.98, 8.15] |
| 14 | 8.38 [8.29, 8.47] | 8.39 [8.30, 8.48] | 8.39 [8.30, 8.48] | 8.12 [8.04, 8.20] | 10.88 [10.73, 11.04] | 8.50 [8.41, 8.58] | 8.45 [8.35, 8.55] | 5.59 [5.54, 5.64] | 7.30 [7.22, 7.38] | 8.92 [8.81, 9.03] | 8.90 [8.79, 9.01] |  | 8.22 [8.14, 8.31] |
| 21 | 8.51 [8.42, 8.60] | 8.61 [8.51, 8.70] | 8.61 [8.51, 8.70] | 8.28 [8.19, 8.36] | 11.00 [10.84, 11.17] | 8.71 [8.62, 8.80] | 8.67 [8.55, 8.77] | 5.64 [5.59, 5.70] | 7.55 [7.46, 7.63] | 9.18 [9.06, 9.30] | 9.16 [9.04, 9.28] |  | 8.40 [8.31, 8.48] |
| 30 | 8.63 [8.53, 8.72] | 8.76 [8.65, 8.85] | 8.76 [8.65, 8.85] | 8.42 [8.33, 8.51] | 11.22 [11.05, 11.41] | 8.83 [8.73, 8.92] | 9.03 [8.91, 9.16] | 5.66 [5.61, 5.71] | 7.67 [7.57, 7.76] | 9.62 [9.48, 9.75] | 9.59 [9.46, 9.73] |  | 8.55 [8.46, 8.64] |
| 45 | 8.70 [8.60, 8.79] | 8.97 [8.86, 9.07] | 8.97 [8.86, 9.07] | 8.54 [8.44, 8.63] | 11.40 [11.11, 11.69] | 8.96 [8.86, 9.05] | 9.32 [9.16, 9.48] | 5.68 [5.63, 5.73] | 7.82 [7.72, 7.91] | 10.16 [10.00, 10.34] | 10.14 [9.98, 10.31] |  | 8.68 [8.59, 8.77] |
| 60 | 8.75 [8.65, 8.85] | 9.08 [8.97, 9.20] | 9.08 [8.97, 9.20] | 8.69 [8.58, 8.79] |  | 9.12 [9.00, 9.22] | 4.26 [3.05, 5.62] | 5.70 [5.65, 5.76] | 8.01 [7.91, 8.12] | 4.30 [3.09, 5.62] | 4.30 [3.09, 5.63] |  | 8.83 [8.72, 8.93] |
| 90 | 8.96 [8.84, 9.08] | 9.61 [9.47, 9.75] | 9.61 [9.47, 9.75] | 8.75 [8.64, 8.87] |  | 9.27 [9.16, 9.39] |  | 5.70 [5.63, 5.76] | 8.09 [7.97, 8.22] |  |  |  | 8.91 [8.79, 9.03] |

**Paired difference vs the reference, all** (park-cluster CI; `*` = wins)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.52 [0.43, 0.62] | 0.21 [0.09, 0.33] | 0.21 [0.09, 0.33] | -0.07 [-0.09, -0.05]* | 2.60 [2.21, 3.06] | 0.25 [0.19, 0.31] | 0.09 [-0.05, 0.23] | -2.25 [-2.55, -2.00]* | -1.08 [-1.29, -0.87]* |  |  | 1.06 [0.87, 1.25] |  | wt_med |
| 2 | 0.50 [0.41, 0.61] | 0.20 [0.09, 0.33] | 0.20 [0.09, 0.33] | -0.07 [-0.09, -0.05]* | 2.58 [2.19, 3.04] | 0.21 [0.16, 0.27] | 0.12 [-0.01, 0.26] | -2.29 [-2.59, -2.03]* | -1.12 [-1.34, -0.91]* |  |  | 1.02 [0.82, 1.20] |  | wt_med |
| 3 | 0.49 [0.40, 0.59] | 0.18 [0.06, 0.30] | 0.18 [0.06, 0.30] | -0.07 [-0.09, -0.06]* | 2.55 [2.17, 3.02] | 0.19 [0.13, 0.25] | 0.18 [0.05, 0.31] | -2.32 [-2.61, -2.06]* | -1.16 [-1.38, -0.95]* | 0.73 [0.53, 0.92] | 0.71 [0.51, 0.90] | 0.98 [0.78, 1.16] |  | wt_med |
| 4 | 0.48 [0.39, 0.59] | 0.16 [0.04, 0.30] | 0.16 [0.04, 0.30] | -0.07 [-0.09, -0.06]* | 2.54 [2.13, 3.01] | 0.16 [0.10, 0.22] | 0.19 [0.05, 0.33] | -2.34 [-2.63, -2.08]* | -1.19 [-1.42, -0.98]* | 0.77 [0.56, 0.97] | 0.74 [0.54, 0.95] | 0.94 [0.74, 1.13] |  | wt_med |
| 5 | 0.47 [0.37, 0.58] | 0.16 [0.03, 0.29] | 0.16 [0.03, 0.29] | -0.07 [-0.09, -0.06]* | 2.52 [2.11, 3.00] | 0.13 [0.07, 0.20] | 0.23 [0.09, 0.37] | -2.36 [-2.66, -2.10]* | -1.23 [-1.46, -1.01]* | 0.82 [0.61, 1.03] | 0.80 [0.59, 1.01] | 0.90 [0.71, 1.09] |  | wt_med |
| 6 | 0.44 [0.34, 0.56] | 0.11 [-0.02, 0.25] | 0.11 [-0.02, 0.25] | -0.08 [-0.10, -0.06]* | 2.80 [2.40, 3.27] | 0.10 [0.03, 0.17] | 0.24 [0.10, 0.38] | -2.40 [-2.70, -2.14]* | -1.29 [-1.53, -1.07]* | 0.79 [0.59, 0.99] | 0.77 [0.57, 0.97] | 0.83 [0.63, 1.02] |  | wt_med |
| 7 | 0.37 [0.27, 0.48] | 0.27 [0.13, 0.42] | 0.27 [0.13, 0.42] | -0.08 [-0.10, -0.06]* | 2.85 [2.43, 3.33] | 0.34 [0.28, 0.41] | 0.32 [0.19, 0.47] | -2.47 [-2.79, -2.21]* | -0.89 [-1.13, -0.68]* | 0.89 [0.67, 1.11] | 0.87 [0.65, 1.09] |  |  | wt_med |
| 10 | 0.32 [0.22, 0.43] | 0.22 [0.07, 0.37] | 0.22 [0.07, 0.37] | -0.08 [-0.10, -0.07]* | 2.84 [2.38, 3.36] | 0.26 [0.20, 0.33] | 0.37 [0.24, 0.51] | -2.55 [-2.88, -2.28]* | -1.01 [-1.25, -0.79]* | 0.96 [0.72, 1.19] | 0.94 [0.70, 1.17] |  |  | wt_med |
| 14 | 0.26 [0.15, 0.37] | 0.26 [0.11, 0.41] | 0.26 [0.11, 0.41] | -0.09 [-0.11, -0.07]* | 2.78 [2.31, 3.28] | 0.38 [0.32, 0.45] | 0.45 [0.28, 0.61] | -2.65 [-2.99, -2.36]* | -0.82 [-1.06, -0.59]* | 1.04 [0.76, 1.31] | 1.02 [0.75, 1.29] |  |  | wt_med |
| 21 | 0.19 [0.08, 0.31] | 0.28 [0.12, 0.44] | 0.28 [0.12, 0.44] | -0.10 [-0.12, -0.08]* | 2.68 [2.21, 3.19] | 0.41 [0.35, 0.47] | 0.43 [0.28, 0.58] | -2.77 [-3.12, -2.47]* | -0.77 [-1.02, -0.54]* | 1.04 [0.78, 1.33] | 1.02 [0.76, 1.31] |  |  | wt_med |
| 30 | 0.16 [0.03, 0.29] | 0.28 [0.12, 0.44] | 0.28 [0.12, 0.44] | -0.10 [-0.12, -0.08]* | 2.62 [2.14, 3.12] | 0.38 [0.32, 0.44] | 0.46 [0.31, 0.61] | -2.91 [-3.26, -2.59]* | -0.79 [-1.05, -0.55]* | 1.11 [0.84, 1.40] | 1.09 [0.82, 1.37] |  |  | wt_med |
| 45 | 0.13 [-0.00, 0.28] | 0.40 [0.22, 0.60] | 0.40 [0.22, 0.60] | -0.10 [-0.13, -0.08]* | 1.84 [1.38, 2.28] | 0.42 [0.34, 0.50] | 0.49 [0.33, 0.66] | -3.02 [-3.41, -2.70]* | -0.73 [-1.02, -0.47]* | 1.31 [1.02, 1.63] | 1.28 [1.00, 1.60] |  |  | wt_med |
| 60 |  | 0.33 [0.06, 0.59] | 0.33 [0.06, 0.59] | -0.16 [-0.37, 0.03] |  | 0.40 [0.16, 0.64] | 0.46 [-0.30, 2.17] | -3.14 [-3.61, -2.76]* | -0.72 [-1.11, -0.32]* | 0.33 [-0.81, 1.63] | 0.33 [-0.80, 1.63] |  | -0.07 [-0.27, 0.12] | clim |
| 90 | 0.20 [-0.01, 0.41] | 0.84 [0.63, 1.04] | 0.84 [0.63, 1.04] | -0.10 [-0.12, -0.07]* |  | 0.54 [0.44, 0.66] |  | -3.22 [-3.72, -2.80]* | -0.63 [-0.92, -0.32]* |  |  |  |  | wt_med |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 15.62 [15.47, 15.78] | 14.53 [14.38, 14.67] | 14.53 [14.38, 14.67] | 14.55 [14.41, 14.69] | 18.68 [18.40, 18.96] | 14.71 [14.57, 14.85] | 13.90 [13.76, 14.06] | 9.68 [9.61, 9.76] | 11.66 [11.51, 11.81] |  |  | 16.61 [16.42, 16.79] | 14.73 [14.58, 14.87] |
| 2 | 15.63 [15.48, 15.79] | 14.59 [14.44, 14.74] | 14.59 [14.44, 14.74] | 14.60 [14.46, 14.74] | 18.74 [18.46, 19.01] | 14.70 [14.55, 14.84] | 14.04 [13.88, 14.20] | 9.68 [9.60, 9.75] | 11.64 [11.48, 11.79] |  |  | 16.59 [16.41, 16.77] | 14.79 [14.64, 14.93] |
| 3 | 15.64 [15.48, 15.79] | 14.58 [14.43, 14.73] | 14.58 [14.43, 14.73] | 14.65 [14.50, 14.79] | 18.77 [18.51, 19.06] | 14.69 [14.55, 14.84] | 14.24 [14.09, 14.41] | 9.67 [9.60, 9.75] | 11.62 [11.46, 11.76] | 15.30 [15.14, 15.48] | 15.25 [15.09, 15.42] | 16.57 [16.38, 16.74] | 14.84 [14.70, 14.98] |
| 4 | 15.64 [15.49, 15.80] | 14.61 [14.45, 14.75] | 14.61 [14.45, 14.75] | 14.68 [14.54, 14.83] | 18.79 [18.53, 19.08] | 14.69 [14.54, 14.83] | 14.33 [14.16, 14.50] | 9.68 [9.60, 9.75] | 11.60 [11.44, 11.74] | 15.43 [15.26, 15.60] | 15.37 [15.20, 15.54] | 16.54 [16.35, 16.71] | 14.88 [14.73, 15.02] |
| 5 | 15.67 [15.52, 15.83] | 14.65 [14.49, 14.79] | 14.65 [14.49, 14.79] | 14.74 [14.59, 14.88] | 18.86 [18.59, 19.14] | 14.69 [14.54, 14.84] | 14.51 [14.35, 14.69] | 9.69 [9.61, 9.76] | 11.59 [11.44, 11.74] | 15.64 [15.45, 15.82] | 15.58 [15.40, 15.76] | 16.52 [16.33, 16.69] | 14.94 [14.79, 15.08] |
| 6 | 15.72 [15.56, 15.87] | 14.66 [14.50, 14.80] | 14.66 [14.50, 14.80] | 14.84 [14.70, 14.99] | 19.49 [19.22, 19.78] | 14.73 [14.58, 14.87] | 14.62 [14.46, 14.79] | 9.72 [9.64, 9.80] | 11.58 [11.43, 11.73] | 15.67 [15.49, 15.85] | 15.62 [15.44, 15.80] | 16.49 [16.30, 16.67] | 15.04 [14.90, 15.19] |
| 7 | 15.76 [15.60, 15.92] | 15.11 [14.94, 15.25] | 15.11 [14.94, 15.25] | 15.03 [14.89, 15.18] | 19.77 [19.49, 20.06] | 15.38 [15.22, 15.53] | 14.96 [14.80, 15.14] | 9.77 [9.69, 9.84] | 12.44 [12.27, 12.59] | 16.09 [15.91, 16.28] | 16.04 [15.86, 16.22] |  | 15.24 [15.10, 15.39] |
| 10 | 15.85 [15.69, 16.01] | 15.21 [15.05, 15.36] | 15.21 [15.05, 15.36] | 15.21 [15.07, 15.37] | 20.07 [19.77, 20.37] | 15.42 [15.26, 15.57] | 15.30 [15.14, 15.50] | 9.80 [9.72, 9.88] | 12.42 [12.26, 12.58] | 16.53 [16.34, 16.73] | 16.47 [16.28, 16.68] |  | 15.43 [15.29, 15.59] |
| 14 | 15.97 [15.82, 16.13] | 15.48 [15.32, 15.64] | 15.48 [15.32, 15.64] | 15.48 [15.34, 15.64] | 20.27 [19.95, 20.60] | 15.89 [15.74, 16.04] | 15.78 [15.60, 15.98] | 9.89 [9.81, 9.97] | 12.94 [12.77, 13.10] | 17.04 [16.84, 17.26] | 16.98 [16.78, 17.20] |  | 15.71 [15.57, 15.86] |
| 21 | 16.16 [16.00, 16.33] | 15.78 [15.61, 15.96] | 15.78 [15.61, 15.96] | 15.75 [15.60, 15.90] | 20.52 [20.20, 20.85] | 16.23 [16.06, 16.39] | 16.15 [15.96, 16.36] | 9.95 [9.87, 10.03] | 13.30 [13.13, 13.47] | 17.52 [17.31, 17.75] | 17.46 [17.25, 17.69] |  | 16.00 [15.84, 16.15] |
| 30 | 16.33 [16.16, 16.50] | 15.97 [15.78, 16.16] | 15.97 [15.78, 16.16] | 15.95 [15.80, 16.11] | 20.89 [20.55, 21.28] | 16.37 [16.19, 16.53] | 16.87 [16.65, 17.10] | 9.90 [9.82, 9.98] | 13.45 [13.25, 13.62] | 18.32 [18.07, 18.56] | 18.25 [18.01, 18.50] |  | 16.21 [16.06, 16.37] |
| 45 | 16.52 [16.33, 16.69] | 16.33 [16.11, 16.54] | 16.33 [16.11, 16.54] | 16.19 [16.03, 16.36] | 17.96 [17.50, 18.40] | 16.61 [16.43, 16.78] | 17.51 [17.21, 17.82] | 9.92 [9.83, 10.01] | 13.71 [13.51, 13.90] | 19.30 [19.00, 19.63] | 19.23 [18.93, 19.56] |  | 16.46 [16.30, 16.62] |
| 60 | 16.67 [16.48, 16.86] | 16.48 [16.27, 16.69] | 16.48 [16.27, 16.69] | 16.53 [16.35, 16.70] |  | 16.92 [16.75, 17.10] | 19.64 [14.20, 24.26] | 9.97 [9.89, 10.06] | 14.09 [13.90, 14.30] | 20.74 [16.48, 24.06] | 20.71 [16.42, 24.06] |  | 16.79 [16.60, 16.96] |
| 90 | 16.98 [16.77, 17.21] | 17.41 [17.14, 17.68] | 17.41 [17.14, 17.68] | 16.72 [16.51, 16.92] |  | 17.24 [17.02, 17.46] |  | 9.99 [9.90, 10.09] | 14.38 [14.14, 14.64] |  |  |  | 16.96 [16.75, 17.17] |

**Paired difference vs the reference, busy** (park-cluster CI; `*` = wins)

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.01 [0.79, 1.24] | -0.11 [-0.41, 0.17] | -0.11 [-0.41, 0.17] | -0.17 [-0.22, -0.14]* | 4.37 [3.21, 5.59] | 0.07 [-0.06, 0.20] | -0.49 [-0.82, -0.21]* | -5.07 [-5.62, -4.57]* | -2.97 [-3.18, -2.71]* |  |  | 1.93 [1.50, 2.29] |  | wt_med |
| 2 | 0.96 [0.73, 1.20] | -0.11 [-0.42, 0.19] | -0.11 [-0.42, 0.19] | -0.18 [-0.22, -0.15]* | 4.36 [3.20, 5.53] | 0.00 [-0.13, 0.13] | -0.41 [-0.75, -0.12]* | -5.13 [-5.68, -4.63]* | -3.05 [-3.27, -2.79]* |  |  | 1.84 [1.42, 2.21] |  | wt_med |
| 3 | 0.92 [0.69, 1.18] | -0.17 [-0.49, 0.12] | -0.17 [-0.49, 0.12] | -0.19 [-0.23, -0.15]* | 4.31 [3.14, 5.49] | -0.05 [-0.19, 0.08] | -0.27 [-0.58, 0.01] | -5.18 [-5.73, -4.67]* | -3.13 [-3.35, -2.87]* | 0.84 [0.42, 1.24] | 0.79 [0.36, 1.18] | 1.76 [1.33, 2.13] |  | wt_med |
| 4 | 0.89 [0.65, 1.17] | -0.19 [-0.53, 0.13] | -0.19 [-0.53, 0.13] | -0.19 [-0.23, -0.15]* | 4.29 [3.09, 5.54] | -0.11 [-0.25, 0.03] | -0.25 [-0.57, 0.03] | -5.22 [-5.77, -4.71]* | -3.19 [-3.42, -2.93]* | 0.92 [0.48, 1.33] | 0.86 [0.43, 1.28] | 1.69 [1.26, 2.07] |  | wt_med |
| 5 | 0.87 [0.62, 1.16] | -0.20 [-0.54, 0.12] | -0.20 [-0.54, 0.12] | -0.19 [-0.23, -0.16]* | 4.26 [3.05, 5.50] | -0.16 [-0.30, -0.02]* | -0.12 [-0.46, 0.19] | -5.27 [-5.82, -4.75]* | -3.25 [-3.48, -2.99]* | 1.09 [0.64, 1.53] | 1.04 [0.58, 1.46] | 1.61 [1.16, 1.99] |  | wt_med |
| 6 | 0.82 [0.55, 1.12] | -0.30 [-0.65, 0.01] | -0.30 [-0.65, 0.01] | -0.20 [-0.24, -0.16]* | 4.77 [3.58, 5.95] | -0.23 [-0.39, -0.08]* | -0.11 [-0.43, 0.19] | -5.34 [-5.90, -4.82]* | -3.37 [-3.61, -3.10]* | 1.03 [0.61, 1.42] | 0.98 [0.56, 1.37] | 1.47 [1.01, 1.87] |  | wt_med |
| 7 | 0.68 [0.42, 0.99] | 0.03 [-0.34, 0.40] | 0.03 [-0.34, 0.40] | -0.21 [-0.25, -0.17]* | 4.87 [3.70, 6.00] | 0.31 [0.17, 0.47] | 0.03 [-0.29, 0.33] | -5.49 [-6.06, -4.96]* | -2.62 [-2.87, -2.35]* | 1.26 [0.84, 1.69] | 1.20 [0.78, 1.63] |  |  | wt_med |
| 10 | 0.59 [0.33, 0.88] | -0.07 [-0.47, 0.28] | -0.07 [-0.47, 0.28] | -0.21 [-0.26, -0.18]* | 4.93 [3.59, 6.21] | 0.15 [-0.02, 0.32] | 0.18 [-0.14, 0.46] | -5.65 [-6.24, -5.11]* | -2.85 [-3.10, -2.56]* | 1.51 [1.06, 1.93] | 1.45 [1.01, 1.87] |  |  | wt_med |
| 14 | 0.47 [0.19, 0.75] | -0.01 [-0.41, 0.36] | -0.01 [-0.41, 0.36] | -0.22 [-0.27, -0.18]* | 4.83 [3.45, 6.10] | 0.41 [0.26, 0.57] | 0.34 [-0.02, 0.69] | -5.84 [-6.45, -5.29]* | -2.53 [-2.80, -2.23]* | 1.70 [1.21, 2.19] | 1.64 [1.15, 2.14] |  |  | wt_med |
| 21 | 0.35 [0.06, 0.66] | -0.01 [-0.42, 0.39] | -0.01 [-0.42, 0.39] | -0.23 [-0.29, -0.19]* | 4.66 [3.25, 6.03] | 0.45 [0.32, 0.59] | 0.27 [-0.07, 0.57] | -6.07 [-6.70, -5.50]* | -2.47 [-2.73, -2.18]* | 1.71 [1.21, 2.27] | 1.65 [1.15, 2.22] |  |  | wt_med |
| 30 | 0.29 [-0.04, 0.64] | -0.02 [-0.44, 0.38] | -0.02 [-0.44, 0.38] | -0.24 [-0.30, -0.20]* | 4.51 [3.15, 5.85] | 0.38 [0.24, 0.54] | 0.42 [0.09, 0.74] | -6.34 [-6.98, -5.75]* | -2.53 [-2.82, -2.23]* | 1.92 [1.38, 2.45] | 1.85 [1.31, 2.40] |  |  | wt_med |
| 45 | 0.30 [-0.06, 0.69] | 0.14 [-0.34, 0.65] | 0.14 [-0.34, 0.65] | -0.25 [-0.31, -0.21]* | 2.39 [1.50, 3.26] | 0.43 [0.25, 0.61] | 0.41 [0.04, 0.81] | -6.58 [-7.24, -5.96]* | -2.47 [-2.79, -2.12]* | 2.39 [1.79, 3.01] | 2.32 [1.71, 2.94] |  |  | wt_med |
| 60 |  | -0.09 [-0.77, 0.52] | -0.09 [-0.77, 0.52] | -0.42 [-0.93, 0.07] |  | 0.35 [-0.20, 0.92] | 6.88 [5.48, 10.77] | -6.85 [-7.84, -6.01]* | -2.47 [-3.13, -1.75]* | 8.03 [4.62, 9.51] | 8.00 [4.65, 9.35] |  | -0.20 [-0.70, 0.27] | clim |
| 90 | 0.34 [-0.15, 0.85] | 0.89 [0.39, 1.38] | 0.89 [0.39, 1.38] | -0.22 [-0.28, -0.17]* |  | 0.72 [0.52, 0.94] |  | -6.99 [-7.94, -6.04]* | -2.11 [-2.52, -1.70]* |  |  |  |  | wt_med |

**MAE by region (all rides)**

| lead | region | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 9.25 | 9.21 | 9.21 | 8.71 | 13.68 | 9.04 | 8.83 | 5.79 | 7.75 |  |  | 9.74 | 8.76 |
| 1 | EU | 6.33 | 5.90 | 5.90 | 5.88 | 8.02 | 6.04 | 5.75 | 4.16 | 5.00 |  |  | 6.53 | 5.93 |
| 1 | NA | 9.14 | 8.87 | 8.87 | 8.60 | 10.33 | 8.92 | 8.51 | 6.50 | 7.32 |  |  | 10.31 | 8.70 |
| 3 | Asia | 9.22 | 9.25 | 9.25 | 8.77 | 13.74 | 9.05 | 9.07 | 5.79 | 7.73 | 9.15 | 9.11 | 9.74 | 8.83 |
| 3 | EU | 6.39 | 5.94 | 5.94 | 5.96 | 8.04 | 6.07 | 5.85 | 4.19 | 5.00 | 6.27 | 6.26 | 6.54 | 6.02 |
| 3 | NA | 9.22 | 8.94 | 8.94 | 8.66 | 10.40 | 8.94 | 8.68 | 6.50 | 7.32 | 9.50 | 9.49 | 10.31 | 8.77 |
| 7 | Asia | 9.21 | 9.65 | 9.65 | 9.01 | 14.25 | 9.44 | 9.51 | 5.87 | 8.24 | 9.49 | 9.46 |  | 9.08 |
| 7 | EU | 6.48 | 6.17 | 6.17 | 6.17 | 8.77 | 6.40 | 6.25 | 4.27 | 5.42 | 6.62 | 6.60 |  | 6.24 |
| 7 | NA | 9.39 | 9.19 | 9.19 | 8.88 | 10.81 | 9.28 | 9.02 | 6.56 | 7.77 | 9.95 | 9.93 |  | 9.00 |
| 14 | Asia | 9.31 | 9.86 | 9.86 | 9.28 | 14.85 | 9.70 | 10.13 | 5.96 | 8.50 | 10.19 | 10.15 |  | 9.34 |
| 14 | EU | 6.60 | 6.43 | 6.43 | 6.40 | 9.00 | 6.70 | 6.58 | 4.36 | 5.76 | 6.89 | 6.88 |  | 6.49 |
| 14 | NA | 9.63 | 9.44 | 9.44 | 9.16 | 10.96 | 9.57 | 9.43 | 6.65 | 8.11 | 10.43 | 10.42 |  | 9.30 |
| 30 | Asia | 9.46 | 10.23 | 10.23 | 9.48 | 15.28 | 10.04 | 10.83 | 6.00 | 8.94 | 11.14 | 11.09 |  | 9.55 |
| 30 | EU | 6.87 | 6.87 | 6.87 | 6.73 | 9.37 | 7.11 | 7.38 | 4.52 | 6.18 | 7.68 | 7.66 |  | 6.84 |
| 30 | NA | 9.95 | 9.77 | 9.77 | 9.56 | 11.22 | 9.85 | 9.81 | 6.67 | 8.40 | 10.97 | 10.95 |  | 9.74 |

**Bias (all rides)**

| lead | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | driver_level | driver_level_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.09 | -1.29 | -1.26 | -1.56 | -1.05 | -1.20 | -5.61 |  |  | -1.80 |  | -0.49 | -0.01 |
| 1 | 0.10 | -1.28 | -1.21 | -1.56 | -1.04 | -1.28 | -5.65 |  |  | -1.78 | -1.78 | -0.49 | 0.00 |
| 2 | 0.10 | -1.26 | -1.16 | -1.54 | -1.02 | -1.28 | -5.64 |  |  | -1.76 | -1.76 | -0.50 | 0.02 |
| 3 | 0.10 | -1.24 | -1.14 | -1.52 | -1.01 | -1.26 | -5.66 | 1.49 | 1.48 | -1.78 | -1.78 | -0.50 | 0.03 |
| 4 | 0.10 | -1.22 | -1.13 | -1.51 | -0.99 | -1.23 | -5.67 | 1.56 | 1.55 | -1.78 | -1.78 | -0.49 | 0.04 |
| 5 | 0.10 | -1.21 | -1.10 | -1.49 | -0.99 | -1.23 | -5.70 | 1.57 | 1.56 | -1.79 | -1.79 | -0.49 | 0.04 |
| 6 | 0.10 | -1.18 | -1.07 | -1.47 | -0.97 | -1.35 | -6.12 | 1.43 | 1.42 | -1.77 | -1.77 | -0.48 | 0.05 |
| 7 |  | -1.17 | -1.04 | -1.46 | -0.93 | -1.30 | -6.07 | 1.44 | 1.43 | -1.86 | -1.86 | -0.48 | 0.14 |
| 10 |  | -1.15 | -0.97 | -1.44 | -0.91 | -1.20 | -6.19 | 1.55 | 1.54 | -1.89 | -1.89 | -0.48 | 0.14 |
| 14 |  | -1.13 | -0.98 | -1.42 | -0.86 | -1.13 | -6.26 | 1.65 | 1.64 | -1.96 | -1.96 | -0.46 | 0.21 |
| 21 |  | -1.06 | -0.91 | -1.35 | -0.80 | -1.02 | -6.31 | 1.77 | 1.76 | -1.87 | -1.87 | -0.43 | 0.26 |
| 30 |  | -0.91 | -0.68 | -1.21 | -0.68 | -1.20 | -6.50 | 1.53 | 1.52 | -1.79 | -1.79 | -0.44 | 0.39 |
| 45 |  | -0.64 | -0.25 | -0.94 | -0.52 | -0.77 | -5.44 | 1.66 | 1.65 | -1.65 | -1.65 | -0.40 | 0.51 |
| 60 |  | -0.40 | 0.11 | -0.70 | -0.41 | -1.62 |  | -0.76 | -0.76 | -1.66 | -1.66 | -0.38 | 0.61 |
| 90 |  | -0.00 | 0.58 | -0.31 | -0.10 |  |  |  |  | -0.33 | -0.33 | -0.38 | 0.91 |

**MAE where the opening hours were known at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.59 [7.50, 7.68] | 7.52 [7.43, 7.60] | 7.89 [7.80, 7.98] | 7.44 [7.34, 7.53] |  |  |
| 1 | 7.71 [7.61, 7.80] | 7.63 [7.54, 7.72] | 7.92 [7.83, 8.01] | 7.58 [7.49, 7.68] |  |  |
| 2 | 7.75 [7.65, 7.84] | 7.67 [7.57, 7.75] | 7.93 [7.83, 8.01] | 7.64 [7.54, 7.73] |  |  |
| 3 | 7.78 [7.68, 7.86] | 7.69 [7.60, 7.78] | 7.93 [7.83, 8.01] | 7.71 [7.61, 7.81] | 8.09 [7.99, 8.20] | 8.06 [7.97, 8.18] |
| 4 | 7.79 [7.70, 7.88] | 7.70 [7.61, 7.79] | 7.91 [7.82, 8.00] | 7.74 [7.64, 7.85] | 8.15 [8.05, 8.26] | 8.12 [8.03, 8.24] |
| 5 | 7.80 [7.70, 7.89] | 7.70 [7.61, 7.79] | 7.89 [7.80, 7.98] | 7.81 [7.71, 7.92] | 8.22 [8.11, 8.33] | 8.20 [8.09, 8.31] |
| 6 | 7.82 [7.73, 7.91] | 7.72 [7.63, 7.81] | 7.88 [7.79, 7.97] | 7.86 [7.75, 7.97] | 8.22 [8.12, 8.33] | 8.20 [8.10, 8.31] |
| 7 | 7.91 [7.81, 8.00] | 7.81 [7.71, 7.90] | 8.16 [8.06, 8.25] | 8.00 [7.89, 8.11] | 8.36 [8.26, 8.48] | 8.34 [8.23, 8.46] |
| 10 | 7.96 [7.85, 8.05] | 7.84 [7.74, 7.94] | 8.12 [8.02, 8.22] | 8.11 [8.00, 8.22] | 8.48 [8.36, 8.60] | 8.45 [8.34, 8.58] |
| 14 | 8.02 [7.93, 8.12] | 7.90 [7.80, 8.00] | 8.25 [8.15, 8.34] | 8.27 [8.15, 8.39] | 8.65 [8.53, 8.78] | 8.63 [8.51, 8.76] |
| 21 | 8.08 [7.98, 8.19] | 7.94 [7.83, 8.04] | 8.35 [8.24, 8.45] | 8.35 [8.22, 8.48] | 8.74 [8.61, 8.86] | 8.72 [8.59, 8.84] |
| 30 | 8.17 [8.06, 8.28] | 8.00 [7.89, 8.11] | 8.39 [8.27, 8.49] | 8.62 [8.48, 8.77] | 9.14 [8.99, 9.29] | 9.12 [8.97, 9.27] |
| 45 | 8.05 [7.93, 8.16] | 7.87 [7.76, 7.99] | 8.34 [8.22, 8.46] | 8.64 [8.44, 8.84] | 9.48 [9.28, 9.68] | 9.46 [9.25, 9.66] |
| 60 | 8.13 [8.00, 8.26] | 7.95 [7.82, 8.08] | 8.45 [8.31, 8.60] | 3.30 [2.33, 4.52] | 3.47 [2.44, 4.76] | 3.46 [2.44, 4.76] |
| 90 | 7.27 [7.10, 7.43] | 7.08 [6.92, 7.24] | 7.61 [7.44, 7.80] |  |  |  |

**MAE where the opening hours were projected at the origin**

| lead | wt_med | h5 | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin |
|---|---|---|---|---|---|---|
| 0 | 7.33 [7.18, 7.48] | 7.31 [7.16, 7.46] | 7.56 [7.41, 7.71] | 6.96 [6.79, 7.16] |  |  |
| 1 | 7.40 [7.25, 7.56] | 7.37 [7.22, 7.53] | 7.56 [7.42, 7.72] | 7.05 [6.89, 7.23] |  |  |
| 2 | 7.46 [7.31, 7.61] | 7.43 [7.28, 7.59] | 7.58 [7.44, 7.75] | 7.21 [7.04, 7.40] |  |  |
| 3 | 7.53 [7.37, 7.69] | 7.50 [7.34, 7.66] | 7.63 [7.48, 7.79] | 7.38 [7.21, 7.57] | 8.24 [8.04, 8.46] | 8.22 [8.03, 8.44] |
| 4 | 7.61 [7.46, 7.77] | 7.58 [7.43, 7.74] | 7.69 [7.55, 7.86] | 7.51 [7.33, 7.71] | 8.33 [8.13, 8.55] | 8.31 [8.11, 8.53] |
| 5 | 7.78 [7.61, 7.94] | 7.75 [7.58, 7.91] | 7.85 [7.69, 8.02] | 7.62 [7.44, 7.83] | 8.44 [8.23, 8.66] | 8.42 [8.22, 8.64] |
| 6 | 7.98 [7.81, 8.15] | 7.94 [7.78, 8.12] | 7.99 [7.83, 8.17] | 7.81 [7.61, 8.02] | 8.55 [8.34, 8.78] | 8.54 [8.32, 8.76] |
| 7 | 8.17 [8.00, 8.34] | 8.14 [7.97, 8.31] | 8.41 [8.24, 8.58] | 8.20 [8.00, 8.43] | 9.03 [8.81, 9.29] | 9.01 [8.79, 9.27] |
| 10 | 8.46 [8.29, 8.64] | 8.43 [8.26, 8.61] | 8.70 [8.52, 8.87] | 8.58 [8.37, 8.81] | 9.44 [9.21, 9.69] | 9.42 [9.19, 9.67] |
| 14 | 8.86 [8.70, 9.02] | 8.83 [8.66, 8.99] | 9.26 [9.10, 9.43] | 9.12 [8.91, 9.34] | 9.87 [9.64, 10.12] | 9.85 [9.62, 10.10] |
| 21 | 9.25 [9.09, 9.41] | 9.22 [9.06, 9.38] | 9.67 [9.50, 9.85] | 9.63 [9.42, 9.85] | 10.57 [10.33, 10.81] | 10.54 [10.30, 10.78] |
| 30 | 9.35 [9.20, 9.50] | 9.31 [9.16, 9.46] | 9.73 [9.57, 9.89] | 10.03 [9.80, 10.27] | 10.81 [10.55, 11.06] | 10.78 [10.52, 11.03] |
| 45 | 9.80 [9.65, 9.95] | 9.73 [9.58, 9.88] | 10.03 [9.87, 10.19] | 10.54 [10.27, 10.82] | 11.47 [11.19, 11.77] | 11.44 [11.16, 11.73] |
| 60 | 9.75 [9.59, 9.90] | 9.67 [9.52, 9.83] | 9.97 [9.82, 10.13] | 10.98 [5.16, 13.87] | 10.08 [4.65, 12.69] | 10.09 [4.65, 12.71] |
| 90 | 10.10 [9.95, 10.27] | 10.01 [9.86, 10.17] | 10.48 [10.33, 10.64] |  |  |  |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | prod_served_lin | driver_level | driver_level_x_h5 | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 1800199.00 | 8.26 | 8.18 | 8.08 | 7.98 | 8.16 | 7.63 | 9.66 |  |  | 7.60 |  | 5.11 | 7.17 |
| 0 | spring | 3863936.00 | 9.47 | 8.00 | 8.62 | 7.92 | 8.39 | 8.05 | 9.37 |  |  | 8.45 |  | 5.63 | 7.11 |
| 0 | summer | 6202500.00 | 8.26 | 7.06 | 7.69 | 7.03 | 7.36 | 7.25 | 10.05 |  |  | 7.42 |  | 5.31 | 6.00 |
| 0 | winter | 256346.00 | 10.59 | 8.39 | 9.88 | 8.43 | 8.99 |  |  |  |  | 9.08 |  | 6.17 | 7.49 |
| 1 | autumn | 1796586.00 | 8.26 | 8.29 | 8.15 | 8.09 | 8.20 | 7.79 | 9.65 |  |  | 7.62 | 7.62 | 5.12 | 7.16 |
| 1 | spring | 3835735.00 | 9.47 | 8.13 | 8.63 | 8.05 | 8.42 | 8.05 |  |  |  | 8.47 | 8.47 | 5.66 | 7.11 |
| 1 | summer | 6188203.00 | 8.26 | 7.15 | 7.72 | 7.12 | 7.39 | 7.38 | 10.15 |  |  | 7.44 | 7.44 | 5.34 | 6.00 |
| 1 | winter | 230254.00 | 10.67 | 8.50 | 9.73 | 8.53 | 8.93 |  |  |  |  | 9.67 | 9.67 | 6.15 | 7.38 |
| 3 | autumn | 1791629.00 | 8.26 | 8.38 | 8.19 | 8.17 | 8.22 | 7.93 | 9.74 | 8.37 | 8.36 | 7.65 | 7.65 | 5.13 | 7.16 |
| 3 | spring | 3796568.00 | 9.47 | 8.24 | 8.66 | 8.14 | 8.44 | 8.05 |  | 8.50 | 8.48 | 8.57 | 8.57 | 5.68 | 7.11 |
| 3 | summer | 6167419.00 | 8.26 | 7.23 | 7.79 | 7.19 | 7.41 | 7.56 | 10.17 | 8.02 | 7.99 | 7.48 | 7.48 | 5.36 | 6.00 |
| 3 | winter | 172421.00 | 10.89 | 8.28 | 9.50 | 8.30 | 8.67 |  |  |  |  | 9.73 | 9.73 | 5.88 | 7.20 |
| 7 | autumn | 1783013.00 |  | 8.61 | 8.30 | 8.40 | 8.76 | 8.35 | 10.35 | 8.93 | 8.91 | 7.96 | 7.96 | 5.19 | 7.89 |
| 7 | spring | 3708385.00 |  | 8.50 | 8.72 | 8.39 | 8.83 | 9.11 |  | 10.58 | 10.54 | 8.92 | 8.92 | 5.75 | 7.58 |
| 7 | summer | 6124268.00 |  | 7.46 | 7.93 | 7.41 | 7.70 | 7.94 | 10.73 | 8.35 | 8.32 | 7.76 | 7.76 | 5.45 | 6.39 |
| 7 | winter | 75587.00 |  | 7.98 | 8.83 | 7.98 | 8.50 |  |  |  |  | 10.36 | 10.36 | 5.78 | 7.05 |
| 14 | autumn | 1774797.00 |  | 8.86 | 8.30 | 8.65 | 9.21 | 8.83 | 10.60 | 9.53 | 9.51 | 8.23 | 8.23 | 5.23 | 8.46 |
| 14 | spring | 3459271.00 |  | 8.75 | 8.91 | 8.63 | 9.08 |  |  |  |  | 9.14 | 9.14 | 5.88 | 7.80 |
| 14 | summer | 6039659.00 |  | 7.74 | 8.13 | 7.68 | 7.97 | 8.33 | 10.98 | 8.72 | 8.70 | 8.05 | 8.05 | 5.54 | 6.68 |
| 30 | autumn | 1760630.00 |  | 8.95 | 8.24 | 8.75 | 9.53 | 9.44 | 10.59 | 10.59 | 10.56 | 8.36 | 8.36 | 5.28 | 8.88 |
| 30 | spring | 2774276.00 |  | 9.04 | 9.26 | 8.90 | 9.32 |  |  |  |  | 9.57 | 9.57 | 5.91 | 8.04 |
| 30 | summer | 5832721.00 |  | 8.21 | 8.48 | 8.10 | 8.39 | 8.83 | 11.52 | 9.15 | 9.14 | 8.54 | 8.54 | 5.66 | 7.11 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

**Spearman of the daily level within park-month** (paired on the same days)

| lead | lvl_cbd | lvl_clim | lvl_driver_level | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|
| 1 | 0.18 [0.15, 0.21] | 0.18 [0.15, 0.20] | 0.34 [0.31, 0.36] | 0.25 [0.23, 0.28] | 0.28 [0.25, 0.30] | 0.30 [0.27, 0.32] | 0.13 [0.11, 0.16] |
| 2 | 0.16 [0.13, 0.19] | 0.17 [0.14, 0.19] | 0.32 [0.30, 0.34] | 0.24 [0.21, 0.26] | 0.27 [0.25, 0.29] | 0.29 [0.26, 0.31] | 0.12 [0.09, 0.15] |
| 3 | 0.18 [0.15, 0.21] | 0.16 [0.13, 0.18] | 0.32 [0.29, 0.34] | 0.23 [0.21, 0.26] | 0.27 [0.24, 0.29] | 0.27 [0.24, 0.30] | 0.11 [0.08, 0.14] |
| 4 | 0.19 [0.16, 0.23] | 0.15 [0.12, 0.18] | 0.32 [0.29, 0.34] | 0.23 [0.21, 0.26] | 0.27 [0.24, 0.29] | 0.28 [0.25, 0.31] | 0.11 [0.08, 0.14] |
| 5 | 0.22 [0.19, 0.25] | 0.14 [0.12, 0.17] | 0.32 [0.29, 0.34] | 0.23 [0.21, 0.26] | 0.27 [0.25, 0.29] | 0.26 [0.23, 0.28] | 0.10 [0.07, 0.13] |
| 6 | 0.13 [0.09, 0.16] | 0.13 [0.11, 0.16] | 0.31 [0.29, 0.34] | 0.23 [0.20, 0.26] | 0.27 [0.25, 0.29] | 0.25 [0.22, 0.27] | 0.10 [0.07, 0.13] |
| 7 | 0.08 [0.05, 0.11] | 0.13 [0.10, 0.16] | 0.27 [0.25, 0.30] | 0.17 [0.15, 0.20] |  | 0.19 [0.17, 0.22] | 0.07 [0.04, 0.10] |
| 10 | 0.07 [0.04, 0.11] | 0.12 [0.09, 0.15] | 0.26 [0.23, 0.29] | 0.18 [0.15, 0.20] |  | 0.19 [0.16, 0.22] | 0.07 [0.04, 0.11] |
| 14 | 0.04 [0.00, 0.07] | 0.12 [0.08, 0.14] | 0.24 [0.21, 0.27] | 0.14 [0.12, 0.17] |  | 0.15 [0.12, 0.17] | 0.06 [0.03, 0.09] |
| 21 | 0.01 [-0.02, 0.05] | 0.12 [0.09, 0.14] | 0.22 [0.19, 0.25] | 0.13 [0.10, 0.16] |  | 0.12 [0.09, 0.16] | 0.06 [0.03, 0.09] |
| 30 | 0.01 [-0.03, 0.05] | 0.15 [0.12, 0.18] | 0.26 [0.23, 0.29] | 0.14 [0.12, 0.17] |  | 0.11 [0.07, 0.15] | 0.05 [0.01, 0.08] |
| 45 | 0.13 [0.06, 0.20] | 0.12 [0.08, 0.15] | 0.26 [0.22, 0.29] | 0.16 [0.13, 0.19] |  | 0.17 [0.13, 0.21] | 0.10 [0.07, 0.14] |
| 60 |  | 0.16 [0.13, 0.20] | 0.25 [0.22, 0.28] | 0.16 [0.13, 0.20] |  | 0.14 [0.14, 0.14] | 0.12 [0.08, 0.16] |
| 90 |  | 0.15 [0.11, 0.20] | 0.19 [0.14, 0.23] | 0.15 [0.11, 0.19] |  |  | 0.16 [0.12, 0.21] |
| 120 |  | 0.13 [0.07, 0.19] | 0.17 [0.12, 0.22] | 0.21 [0.15, 0.26] |  |  | 0.14 [0.09, 0.20] |
| 180 |  | 0.25 [0.14, 0.35] | 0.33 [0.26, 0.39] | 0.33 [0.25, 0.39] |  |  | 0.31 [0.20, 0.40] |

| lead | lvl_cbd | lvl_clim | lvl_driver_level | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|
| 1 | -0.10 [-0.13, -0.06] | -0.11 [-0.14, -0.08] | 0.06 [0.04, 0.09]* | -0.02 [-0.04, -0.00] | -0.00 [-0.03, 0.03] | -0.14 [-0.18, -0.11] | lvl_snaive7 |
| 2 | -0.12 [-0.15, -0.08] | -0.11 [-0.14, -0.07] | 0.06 [0.03, 0.08]* | -0.03 [-0.05, -0.01] | 0.00 [-0.03, 0.03] | -0.15 [-0.18, -0.12] | lvl_snaive7 |
| 3 | -0.10 [-0.13, -0.06] | -0.11 [-0.15, -0.08] | 0.05 [0.03, 0.07]* | -0.03 [-0.06, -0.01] | -0.01 [-0.04, 0.02] | -0.16 [-0.19, -0.12] | lvl_snaive7 |
| 4 | -0.08 [-0.11, -0.05] | -0.12 [-0.15, -0.09] | 0.05 [0.03, 0.08]* | -0.04 [-0.06, -0.01] | 0.00 [-0.03, 0.03] | -0.16 [-0.19, -0.13] | lvl_snaive7 |
| 5 | -0.05 [-0.08, -0.02] | -0.12 [-0.16, -0.09] | 0.05 [0.02, 0.07]* | -0.04 [-0.06, -0.02] | -0.03 [-0.06, 0.00] | -0.16 [-0.20, -0.13] | lvl_snaive7 |
| 6 | -0.14 [-0.18, -0.10] | -0.13 [-0.17, -0.10] | 0.04 [0.02, 0.07]* | -0.04 [-0.06, -0.02] | -0.04 [-0.07, -0.01] | -0.17 [-0.20, -0.13] | lvl_snaive7 |
| 7 | -0.10 [-0.13, -0.06] | -0.04 [-0.07, -0.01] | 0.10 [0.08, 0.12]* |  | -0.01 [-0.04, 0.02] | -0.09 [-0.12, -0.07] | lvl_naive4 |
| 10 | -0.11 [-0.14, -0.07] | -0.05 [-0.08, -0.02] | 0.09 [0.06, 0.11]* |  | -0.02 [-0.05, 0.01] | -0.10 [-0.12, -0.08] | lvl_naive4 |
| 14 | -0.08 [-0.12, -0.05] | -0.03 [-0.06, 0.00] | 0.10 [0.07, 0.12]* |  | -0.01 [-0.05, 0.02] | -0.09 [-0.11, -0.06] | lvl_naive4 |
| 21 | -0.09 [-0.12, -0.05] | -0.01 [-0.04, 0.02] | 0.10 [0.07, 0.12]* |  | -0.02 [-0.05, 0.01] | -0.07 [-0.10, -0.04] | lvl_naive4 |
| 30 | -0.08 [-0.13, -0.04] |  | 0.11 [0.07, 0.15]* | -0.01 [-0.04, 0.03] | -0.03 [-0.07, 0.02] | -0.10 [-0.14, -0.07] | lvl_clim |
| 45 | -0.14 [-0.20, -0.07] | -0.04 [-0.09, 0.00] | 0.10 [0.07, 0.13]* |  | 0.01 [-0.05, 0.06] | -0.06 [-0.09, -0.03] | lvl_naive4 |
| 60 |  | -0.00 [-0.04, 0.04] | 0.09 [0.05, 0.12]* |  | 0.45 [0.45, 0.45]* | -0.04 [-0.08, -0.01] | lvl_naive4 |
| 90 |  | -0.01 [-0.06, 0.03] | 0.03 [-0.01, 0.07] | -0.01 [-0.05, 0.03] |  |  | lvl_wt56 |
| 120 |  | -0.07 [-0.13, -0.01] | -0.03 [-0.05, -0.01] |  |  | -0.07 [-0.11, -0.02] | lvl_naive4 |
| 180 |  | -0.08 [-0.16, -0.01] | 0.01 [-0.02, 0.05] |  |  | -0.02 [-0.10, 0.05] | lvl_naive4 |

**Busy days** (true top quartile of the month): recall with ties broken fractionally, precision of the `≥ predicted q75` rule (ties included, which is what flat predictions cost)

| lead | model | park_months | busy_day_recall | busy_day_precision_q75_rule | iqr | iqr_true |
|---|---|---|---|---|---|---|
| 1 | lvl_cbd | 319 | 0.36 | 0.35 | 5.00 | 13.65 |
| 1 | lvl_clim | 615 | 0.37 | 0.35 | 6.25 | 14.50 |
| 1 | lvl_driver_level | 614 | 0.44 | 0.44 | 10.29 | 14.50 |
| 1 | lvl_naive4 | 614 | 0.39 | 0.39 | 9.64 | 14.50 |
| 1 | lvl_snaive7 | 599 | 0.40 | 0.40 | 14.86 | 14.57 |
| 1 | lvl_tft | 429 | 0.42 | 0.42 | 10.51 | 13.67 |
| 1 | lvl_wt56 | 615 | 0.35 | 0.35 | 6.01 | 14.50 |
| 2 | lvl_cbd | 307 | 0.35 | 0.34 | 4.89 | 13.67 |
| 2 | lvl_clim | 563 | 0.37 | 0.34 | 5.93 | 14.50 |
| 2 | lvl_driver_level | 562 | 0.43 | 0.43 | 10.38 | 14.51 |
| 2 | lvl_naive4 | 562 | 0.39 | 0.38 | 9.87 | 14.51 |
| 2 | lvl_snaive7 | 551 | 0.40 | 0.39 | 14.91 | 14.59 |
| 2 | lvl_tft | 380 | 0.43 | 0.43 | 11.11 | 13.90 |
| 2 | lvl_wt56 | 563 | 0.34 | 0.34 | 6.77 | 14.50 |
| 3 | lvl_cbd | 315 | 0.36 | 0.35 | 5.00 | 13.67 |
| 3 | lvl_clim | 534 | 0.36 | 0.34 | 5.82 | 14.50 |
| 3 | lvl_driver_level | 533 | 0.43 | 0.42 | 10.38 | 14.50 |
| 3 | lvl_naive4 | 533 | 0.39 | 0.38 | 9.86 | 14.50 |
| 3 | lvl_snaive7 | 522 | 0.40 | 0.39 | 14.98 | 14.57 |
| 3 | lvl_tft | 380 | 0.42 | 0.42 | 11.03 | 13.90 |
| 3 | lvl_wt56 | 534 | 0.34 | 0.33 | 6.67 | 14.50 |
| 4 | lvl_cbd | 316 | 0.37 | 0.37 | 5.30 | 13.66 |
| 4 | lvl_clim | 534 | 0.36 | 0.34 | 5.65 | 14.51 |
| 4 | lvl_driver_level | 533 | 0.43 | 0.43 | 10.26 | 14.52 |
| 4 | lvl_naive4 | 533 | 0.39 | 0.38 | 9.88 | 14.52 |
| 4 | lvl_snaive7 | 522 | 0.40 | 0.40 | 15.00 | 14.60 |
| 4 | lvl_tft | 379 | 0.41 | 0.41 | 11.57 | 13.91 |
| 4 | lvl_wt56 | 534 | 0.34 | 0.33 | 6.71 | 14.51 |
| 5 | lvl_cbd | 317 | 0.39 | 0.39 | 5.75 | 13.67 |
| 5 | lvl_clim | 529 | 0.36 | 0.34 | 5.42 | 14.50 |
| 5 | lvl_driver_level | 528 | 0.43 | 0.43 | 10.26 | 14.51 |
| 5 | lvl_naive4 | 528 | 0.39 | 0.38 | 9.86 | 14.51 |
| 5 | lvl_snaive7 | 521 | 0.40 | 0.40 | 14.96 | 14.55 |
| 5 | lvl_tft | 375 | 0.41 | 0.41 | 11.75 | 13.94 |
| 5 | lvl_wt56 | 529 | 0.34 | 0.33 | 6.52 | 14.50 |
| 6 | lvl_cbd | 317 | 0.34 | 0.33 | 4.50 | 13.67 |
| 6 | lvl_clim | 527 | 0.36 | 0.33 | 5.44 | 14.53 |
| 6 | lvl_driver_level | 526 | 0.42 | 0.42 | 10.44 | 14.54 |
| 6 | lvl_naive4 | 526 | 0.39 | 0.38 | 9.81 | 14.54 |
| 6 | lvl_snaive7 | 521 | 0.40 | 0.40 | 14.86 | 14.55 |
| 6 | lvl_tft | 375 | 0.41 | 0.41 | 11.33 | 13.94 |
| 6 | lvl_wt56 | 527 | 0.34 | 0.34 | 6.62 | 14.53 |
| 7 | lvl_cbd | 311 | 0.30 | 0.30 | 4.17 | 13.65 |
| 7 | lvl_clim | 527 | 0.35 | 0.33 | 5.60 | 14.45 |
| 7 | lvl_driver_level | 526 | 0.41 | 0.41 | 10.35 | 14.47 |
| 7 | lvl_naive4 | 526 | 0.37 | 0.37 | 10.09 | 14.47 |
| 7 | lvl_tft | 375 | 0.38 | 0.38 | 11.66 | 13.94 |
| 7 | lvl_wt56 | 527 | 0.34 | 0.33 | 6.42 | 14.45 |
| 10 | lvl_cbd | 315 | 0.32 | 0.31 | 4.17 | 13.65 |
| 10 | lvl_clim | 527 | 0.35 | 0.33 | 6.74 | 14.37 |
| 10 | lvl_driver_level | 526 | 0.41 | 0.41 | 10.47 | 14.40 |
| 10 | lvl_naive4 | 526 | 0.37 | 0.37 | 10.09 | 14.40 |
| 10 | lvl_tft | 373 | 0.37 | 0.37 | 11.33 | 13.91 |
| 10 | lvl_wt56 | 527 | 0.34 | 0.33 | 6.64 | 14.37 |
| 14 | lvl_cbd | 278 | 0.29 | 0.28 | 3.90 | 13.62 |
| 14 | lvl_clim | 524 | 0.34 | 0.33 | 7.09 | 14.24 |
| 14 | lvl_driver_level | 522 | 0.39 | 0.39 | 10.17 | 14.27 |
| 14 | lvl_naive4 | 522 | 0.35 | 0.35 | 10.08 | 14.27 |
| 14 | lvl_tft | 373 | 0.34 | 0.34 | 11.63 | 13.67 |
| 14 | lvl_wt56 | 524 | 0.33 | 0.32 | 6.33 | 14.24 |
| 21 | lvl_cbd | 276 | 0.29 | 0.29 | 4.00 | 13.66 |
| 21 | lvl_clim | 510 | 0.34 | 0.33 | 6.29 | 14.17 |
| 21 | lvl_driver_level | 497 | 0.39 | 0.39 | 10.09 | 14.25 |
| 21 | lvl_naive4 | 497 | 0.34 | 0.34 | 9.83 | 14.25 |
| 21 | lvl_tft | 366 | 0.34 | 0.35 | 11.28 | 13.77 |
| 21 | lvl_wt56 | 510 | 0.32 | 0.32 | 6.45 | 14.17 |
| 30 | lvl_cbd | 233 | 0.30 | 0.29 | 4.50 | 13.89 |
| 30 | lvl_clim | 451 | 0.35 | 0.34 | 6.00 | 14.25 |
| 30 | lvl_driver_level | 451 | 0.40 | 0.40 | 10.20 | 14.25 |
| 30 | lvl_naive4 | 451 | 0.34 | 0.34 | 9.67 | 14.25 |
| 30 | lvl_tft | 267 | 0.33 | 0.33 | 10.44 | 14.05 |
| 30 | lvl_wt56 | 451 | 0.32 | 0.31 | 6.76 | 14.25 |
| 45 | lvl_cbd | 82 | 0.35 | 0.34 | 4.48 | 14.81 |
| 45 | lvl_clim | 418 | 0.35 | 0.33 | 6.18 | 13.63 |
| 45 | lvl_driver_level | 417 | 0.40 | 0.40 | 10.29 | 13.67 |
| 45 | lvl_naive4 | 417 | 0.36 | 0.36 | 10.04 | 13.67 |
| 45 | lvl_tft | 180 | 0.35 | 0.36 | 10.97 | 14.25 |
| 45 | lvl_wt56 | 418 | 0.34 | 0.33 | 6.38 | 13.63 |
| 60 | lvl_clim | 354 | 0.36 | 0.34 | 4.73 | 13.16 |
| 60 | lvl_driver_level | 354 | 0.39 | 0.39 | 9.83 | 13.16 |
| 60 | lvl_naive4 | 354 | 0.37 | 0.36 | 9.98 | 13.16 |
| 60 | lvl_tft | 1 | 0.12 | 0.20 | 9.56 | 10.47 |
| 60 | lvl_wt56 | 354 | 0.34 | 0.33 | 6.03 | 13.16 |
| 90 | lvl_clim | 264 | 0.36 | 0.34 | 5.77 | 12.74 |
| 90 | lvl_driver_level | 264 | 0.39 | 0.38 | 11.11 | 12.74 |
| 90 | lvl_naive4 | 264 | 0.36 | 0.35 | 10.27 | 12.74 |
| 90 | lvl_wt56 | 264 | 0.38 | 0.37 | 6.82 | 12.74 |
| 120 | lvl_clim | 181 | 0.37 | 0.34 | 6.33 | 12.58 |
| 120 | lvl_driver_level | 181 | 0.37 | 0.37 | 11.45 | 12.58 |
| 120 | lvl_naive4 | 181 | 0.38 | 0.37 | 10.55 | 12.58 |
| 120 | lvl_wt56 | 181 | 0.38 | 0.36 | 7.03 | 12.58 |
| 180 | lvl_clim | 64 | 0.50 | 0.43 | 9.16 | 12.02 |
| 180 | lvl_driver_level | 64 | 0.44 | 0.44 | 12.88 | 12.02 |
| 180 | lvl_naive4 | 64 | 0.42 | 0.41 | 11.10 | 12.02 |
| 180 | lvl_wt56 | 64 | 0.49 | 0.46 | 8.20 | 12.02 |

## D1 — next-best ride

Candidates: every (intraday origin, ride) with a live OPERATING wait — the same set for every model. Suggest when the forecast maximum within the lookahead (60 or 120 min) is ≥ live + 10 (`next-best-ride.ts`, walking time 0); correct when the TRUE maximum there is also ≥ live + 10. False-suggestion rate = 1 − precision.

**next-best precision**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.33 [0.33, 0.34] | 0.32 [0.32, 0.32] | 0.35 [0.34, 0.35] | 0.31 [0.31, 0.32] | 0.37 [0.37, 0.38] | 0.33 [0.32, 0.33] |
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
| 0-60 | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.01] | -0.00 [-0.01, 0.01] | -0.01 [-0.03, 0.00] | 0.00 [-0.01, 0.01] | snaive7 |
| 0-120 | -0.01 [-0.03, -0.00] | -0.01 [-0.02, 0.00] | -0.01 [-0.02, 0.00] | -0.02 [-0.04, -0.00] | -0.01 [-0.02, 0.00] | snaive7 |

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
| w0-45 | 13.28 [13.14, 13.41] | 12.21 [12.08, 12.32] | 12.50 [12.37, 12.62] | 11.54 [11.41, 11.67] | 6.29 [6.24, 6.35] | 14.15 [14.00, 14.29] | 12.34 [12.21, 12.45] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| w0-45 | 7.56 [6.75, 8.38] | 6.49 [5.83, 7.18] | 6.80 [6.08, 7.56] | 5.92 [5.32, 6.56] | 8.37 [7.61, 9.20] | 6.60 [5.92, 7.30] | persistence |

**live-window bias (headliners)**

| lead | persistence | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft |
|---|---|---|---|---|---|---|---|
| m015 | -0.22 | 0.19 | -1.43 | -1.33 | -2.05 | -1.56 | -2.10 |
| m030 | -0.30 | 0.19 | -1.53 | -1.45 | -1.94 | -1.45 | -2.02 |
| m045 | -0.09 | 0.17 | -1.57 | -1.51 | -1.85 | -1.35 | -1.92 |

## D3 — best time of the day

Paired ride-day by ride-day: only ride-days a model covers in full (every truth slot), ≥ 8 slots and a non-flat truth. Hit = a true-minimum slot is among the predicted lowest two / within ±30 min of the predicted best (ties in the truth count as minima); regret = true wait at the predicted best − true minimum.

**best-time hit rate (top-2)**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.80] |  | 0.83 [0.82, 0.83] | 0.75 [0.75, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.79, 0.79] |  |  | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 1 | 0.82 [0.82, 0.82] | 0.79 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.82 [0.82, 0.83] | 0.75 [0.75, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.79, 0.79] |  |  | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 2 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.79] |  |  | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 3 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.79, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 4 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 5 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.79] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 6 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.79] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.74 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.82 [0.82, 0.82] |
| 7 | 0.82 [0.82, 0.83] | 0.79 [0.79, 0.79] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.74 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.75] | 0.77 [0.77, 0.77] |  | 0.82 [0.82, 0.82] |
| 10 | 0.82 [0.82, 0.83] | 0.79 [0.78, 0.79] | 0.79 [0.78, 0.79] | 0.82 [0.82, 0.83] | 0.73 [0.73, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.77] |  | 0.82 [0.82, 0.82] |
| 14 | 0.82 [0.82, 0.83] | 0.78 [0.78, 0.79] | 0.78 [0.78, 0.79] | 0.82 [0.82, 0.83] | 0.73 [0.73, 0.73] | 0.80 [0.79, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.76] | 0.77 [0.77, 0.77] |  | 0.82 [0.82, 0.82] |
| 21 | 0.82 [0.82, 0.83] | 0.78 [0.78, 0.78] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.82] | 0.73 [0.72, 0.73] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.75 [0.75, 0.75] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 30 | 0.82 [0.82, 0.83] | 0.78 [0.78, 0.78] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.82] | 0.73 [0.72, 0.73] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.75 [0.75, 0.76] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 45 | 0.82 [0.82, 0.83] | 0.78 [0.78, 0.78] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.82] | 0.72 [0.71, 0.73] | 0.79 [0.79, 0.79] | 0.78 [0.78, 0.79] | 0.75 [0.75, 0.76] | 0.77 [0.76, 0.77] |  | 0.82 [0.82, 0.82] |
| 60 | 0.83 [0.82, 0.83] | 0.78 [0.78, 0.78] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.83] |  | 0.79 [0.79, 0.80] | 0.80 [0.75, 0.84] | 0.75 [0.66, 0.83] | 0.75 [0.65, 0.83] |  | 0.82 [0.82, 0.82] |
| 90 | 0.83 [0.83, 0.84] | 0.79 [0.79, 0.80] | 0.79 [0.79, 0.80] | 0.83 [0.83, 0.83] |  | 0.80 [0.79, 0.80] |  |  |  |  | 0.82 [0.82, 0.83] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.02 [-0.03, -0.01] |  | 0.01 [0.01, 0.02]* | -0.05 [-0.06, -0.04] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.01] |  |  | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 1 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.05 [-0.07, -0.04] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.01] |  |  | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 2 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.05 [-0.07, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] |  |  | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 3 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.06 [-0.07, -0.04] | -0.01 [-0.02, -0.00] | -0.02 [-0.02, -0.01] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 4 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.06 [-0.07, -0.04] | -0.01 [-0.02, -0.01] | -0.02 [-0.02, -0.01] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 5 | -0.02 [-0.03, -0.02] | -0.02 [-0.03, -0.02] | 0.01 [0.01, 0.02]* | -0.05 [-0.07, -0.04] | -0.01 [-0.02, -0.01] | -0.02 [-0.02, -0.01] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 6 | -0.02 [-0.03, -0.02] | -0.02 [-0.03, -0.02] | 0.01 [0.01, 0.02]* | -0.06 [-0.08, -0.05] | -0.01 [-0.02, -0.01] | -0.02 [-0.03, -0.01] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] | -0.03 [-0.03, -0.02] | 0.01 [0.00, 0.01]* | clim |
| 7 | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] | 0.01 [0.01, 0.02]* | -0.07 [-0.08, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.02] | -0.06 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  | 0.00 [0.00, 0.01]* | clim |
| 10 | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] | 0.01 [0.01, 0.02]* | -0.07 [-0.08, -0.05] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.02] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  | 0.00 [0.00, 0.01]* | clim |
| 14 | -0.03 [-0.04, -0.03] | -0.03 [-0.04, -0.03] | 0.01 [0.01, 0.01]* | -0.07 [-0.09, -0.06] | -0.02 [-0.03, -0.01] | -0.03 [-0.03, -0.02] | -0.06 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  | 0.00 [0.00, 0.00]* | clim |
| 21 | -0.04 [-0.05, -0.03] | -0.04 [-0.05, -0.03] | 0.01 [0.00, 0.01]* | -0.07 [-0.09, -0.06] | -0.02 [-0.03, -0.02] | -0.03 [-0.04, -0.02] | -0.05 [-0.07, -0.04] | -0.03 [-0.05, -0.02] |  | 0.00 [-0.00, 0.00] | clim |
| 30 | -0.04 [-0.05, -0.03] | -0.04 [-0.05, -0.03] | 0.01 [0.01, 0.01]* | -0.08 [-0.09, -0.06] | -0.02 [-0.03, -0.02] | -0.02 [-0.03, -0.02] | -0.05 [-0.07, -0.04] | -0.03 [-0.04, -0.02] |  | 0.00 [-0.00, 0.00] | clim |
| 45 | -0.04 [-0.05, -0.03] | -0.04 [-0.05, -0.03] | 0.01 [0.01, 0.02]* | -0.04 [-0.07, -0.02] | -0.02 [-0.03, -0.02] | -0.02 [-0.03, -0.01] | -0.05 [-0.07, -0.03] | -0.03 [-0.04, -0.01] |  | 0.00 [-0.00, 0.00] | clim |
| 60 | -0.04 [-0.05, -0.03] | -0.04 [-0.05, -0.03] | 0.01 [0.00, 0.01]* |  | -0.03 [-0.04, -0.02] | -0.05 [-0.11, 0.02] | -0.05 [-0.08, 0.04] | -0.05 [-0.09, 0.03] |  | -0.00 [-0.01, -0.00] | clim |
| 90 | -0.04 [-0.05, -0.03] | -0.04 [-0.05, -0.03] | 0.00 [-0.00, 0.01] |  | -0.03 [-0.04, -0.02] |  |  |  |  | -0.00 [-0.01, 0.00] | clim |

**best-time hit rate (±30 min)**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] |  | 0.83 [0.83, 0.83] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 1 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.78 [0.78, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 2 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] |  |  | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 3 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 4 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 5 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.78 [0.77, 0.78] | 0.82 [0.82, 0.82] | 0.81 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 6 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.82, 0.82] | 0.80 [0.80, 0.81] | 0.78 [0.78, 0.78] | 0.79 [0.79, 0.79] | 0.82 [0.82, 0.83] | 0.83 [0.82, 0.83] |
| 7 | 0.83 [0.83, 0.83] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 10 | 0.83 [0.83, 0.83] | 0.81 [0.80, 0.81] | 0.81 [0.80, 0.81] | 0.83 [0.83, 0.83] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.79 [0.79, 0.80] |  | 0.83 [0.82, 0.83] |
| 14 | 0.83 [0.83, 0.83] | 0.81 [0.80, 0.81] | 0.81 [0.80, 0.81] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 21 | 0.83 [0.83, 0.83] | 0.80 [0.80, 0.81] | 0.80 [0.80, 0.81] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 30 | 0.83 [0.83, 0.83] | 0.80 [0.80, 0.81] | 0.80 [0.80, 0.81] | 0.83 [0.83, 0.83] | 0.76 [0.76, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.80, 0.80] | 0.78 [0.77, 0.78] | 0.79 [0.79, 0.79] |  | 0.83 [0.82, 0.83] |
| 45 | 0.83 [0.83, 0.83] | 0.80 [0.80, 0.81] | 0.80 [0.80, 0.81] | 0.83 [0.83, 0.83] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.80, 0.81] | 0.78 [0.77, 0.78] | 0.79 [0.78, 0.80] |  | 0.82 [0.82, 0.83] |
| 60 | 0.83 [0.83, 0.84] | 0.80 [0.80, 0.81] | 0.80 [0.80, 0.81] | 0.83 [0.83, 0.83] |  | 0.81 [0.81, 0.81] | 0.82 [0.77, 0.86] | 0.77 [0.69, 0.84] | 0.78 [0.70, 0.85] |  | 0.83 [0.82, 0.83] |
| 90 | 0.84 [0.83, 0.84] | 0.81 [0.81, 0.81] | 0.81 [0.81, 0.81] | 0.84 [0.83, 0.84] |  | 0.81 [0.81, 0.82] |  |  |  |  | 0.83 [0.83, 0.84] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.02, -0.00] |  | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] |  |  | -0.02 [-0.02, -0.01] | 0.01 [0.00, 0.01]* | clim |
| 1 | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] |  |  | -0.02 [-0.02, -0.01] | 0.01 [0.00, 0.01]* | clim |
| 2 | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, 0.00] |  |  | -0.02 [-0.02, -0.01] | 0.00 [0.00, 0.01]* | clim |
| 3 | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | 0.00 [0.00, 0.01]* | clim |
| 4 | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, 0.00] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | 0.00 [0.00, 0.01]* | clim |
| 5 | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.01 [0.01, 0.02]* | -0.03 [-0.04, -0.02] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, 0.00] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | 0.01 [0.00, 0.01]* | clim |
| 6 | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] | 0.01 [0.01, 0.02]* | -0.04 [-0.06, -0.03] | -0.00 [-0.01, 0.00] | -0.01 [-0.01, -0.00] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] | -0.02 [-0.02, -0.01] | 0.00 [0.00, 0.01]* | clim |
| 7 | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] | 0.01 [0.01, 0.01]* | -0.04 [-0.06, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | 0.00 [0.00, 0.01]* | clim |
| 10 | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.01] | 0.01 [0.01, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | 0.00 [0.00, 0.01]* | clim |
| 14 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.01]* | -0.05 [-0.06, -0.03] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.01] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | 0.00 [0.00, 0.00]* | clim |
| 21 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.01]* | -0.05 [-0.07, -0.03] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.01] | -0.03 [-0.05, -0.02] | -0.02 [-0.03, -0.01] |  | 0.00 [-0.00, 0.00] | clim |
| 30 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.01]* | -0.05 [-0.07, -0.03] | -0.01 [-0.02, -0.01] | -0.01 [-0.02, -0.00] | -0.03 [-0.04, -0.02] | -0.02 [-0.03, -0.01] |  | 0.00 [-0.00, 0.00] | clim |
| 45 | -0.02 [-0.03, -0.01] | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.01]* | -0.02 [-0.04, 0.00] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, 0.00] | -0.03 [-0.04, -0.02] | -0.01 [-0.03, -0.00] |  | -0.00 [-0.00, 0.00] | clim |
| 60 | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] | 0.00 [-0.00, 0.01] |  | -0.02 [-0.03, -0.01] | -0.02 [-0.05, 0.03] | -0.03 [-0.05, 0.04] | -0.02 [-0.04, 0.05] |  | -0.00 [-0.01, 0.00] | clim |
| 90 | -0.03 [-0.04, -0.02] | -0.03 [-0.04, -0.02] | 0.00 [-0.00, 0.01] |  | -0.02 [-0.03, -0.01] |  |  |  |  | -0.00 [-0.01, 0.00] | clim |

**best-time regret (min)**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 3.03 [2.97, 3.09] | 3.32 [3.27, 3.37] |  | 2.82 [2.77, 2.87] | 3.88 [3.82, 3.95] | 3.16 [3.12, 3.21] | 3.26 [3.20, 3.32] |  |  | 3.45 [3.37, 3.52] | 3.03 [2.97, 3.08] |
| 1 | 3.03 [2.97, 3.10] | 3.33 [3.28, 3.38] | 3.33 [3.28, 3.38] | 2.83 [2.78, 2.88] | 3.92 [3.85, 3.99] | 3.16 [3.12, 3.21] | 3.30 [3.24, 3.36] |  |  | 3.44 [3.36, 3.52] | 3.03 [2.98, 3.08] |
| 2 | 3.03 [2.97, 3.08] | 3.33 [3.29, 3.38] | 3.33 [3.29, 3.38] | 2.83 [2.78, 2.87] | 3.95 [3.88, 4.02] | 3.16 [3.12, 3.21] | 3.32 [3.26, 3.37] |  |  | 3.44 [3.36, 3.52] | 3.03 [2.98, 3.09] |
| 3 | 3.02 [2.97, 3.09] | 3.34 [3.30, 3.39] | 3.34 [3.30, 3.39] | 2.84 [2.78, 2.88] | 3.95 [3.88, 4.02] | 3.17 [3.12, 3.22] | 3.33 [3.27, 3.39] | 4.31 [4.23, 4.39] | 3.73 [3.66, 3.81] | 3.44 [3.36, 3.52] | 3.04 [2.99, 3.09] |
| 4 | 3.02 [2.96, 3.08] | 3.35 [3.30, 3.40] | 3.35 [3.30, 3.40] | 2.84 [2.79, 2.88] | 3.96 [3.90, 4.03] | 3.17 [3.13, 3.22] | 3.33 [3.27, 3.39] | 4.31 [4.23, 4.39] | 3.72 [3.65, 3.80] | 3.44 [3.36, 3.51] | 3.05 [2.99, 3.10] |
| 5 | 3.02 [2.96, 3.08] | 3.36 [3.32, 3.41] | 3.36 [3.32, 3.41] | 2.84 [2.79, 2.89] | 3.99 [3.91, 4.05] | 3.18 [3.13, 3.23] | 3.36 [3.30, 3.41] | 4.30 [4.21, 4.39] | 3.71 [3.64, 3.79] | 3.44 [3.36, 3.51] | 3.04 [2.99, 3.09] |
| 6 | 3.02 [2.97, 3.09] | 3.38 [3.33, 3.43] | 3.38 [3.33, 3.43] | 2.86 [2.81, 2.91] | 4.18 [4.11, 4.26] | 3.19 [3.14, 3.24] | 3.41 [3.35, 3.47] | 4.30 [4.22, 4.39] | 3.73 [3.66, 3.81] | 3.44 [3.36, 3.51] | 3.06 [3.01, 3.11] |
| 7 | 3.03 [2.97, 3.09] | 3.43 [3.38, 3.48] | 3.43 [3.38, 3.48] | 2.88 [2.83, 2.93] | 4.25 [4.17, 4.32] | 3.24 [3.19, 3.29] | 3.47 [3.41, 3.53] | 4.31 [4.22, 4.39] | 3.74 [3.66, 3.81] |  | 3.06 [3.01, 3.11] |
| 10 | 3.03 [2.97, 3.09] | 3.46 [3.40, 3.51] | 3.46 [3.40, 3.51] | 2.89 [2.84, 2.94] | 4.29 [4.22, 4.37] | 3.25 [3.20, 3.30] | 3.49 [3.43, 3.56] | 4.30 [4.21, 4.39] | 3.71 [3.63, 3.79] |  | 3.07 [3.02, 3.13] |
| 14 | 3.04 [2.97, 3.10] | 3.53 [3.48, 3.59] | 3.53 [3.48, 3.59] | 2.91 [2.86, 2.97] | 4.38 [4.30, 4.46] | 3.31 [3.26, 3.37] | 3.51 [3.44, 3.58] | 4.31 [4.21, 4.39] | 3.73 [3.65, 3.81] |  | 3.08 [3.02, 3.13] |
| 21 | 3.04 [2.98, 3.10] | 3.58 [3.52, 3.64] | 3.58 [3.52, 3.64] | 2.90 [2.85, 2.95] | 4.45 [4.37, 4.55] | 3.35 [3.29, 3.41] | 3.60 [3.53, 3.68] | 4.34 [4.24, 4.44] | 3.78 [3.70, 3.88] |  | 3.09 [3.03, 3.15] |
| 30 | 3.04 [2.98, 3.11] | 3.62 [3.55, 3.69] | 3.62 [3.55, 3.69] | 2.91 [2.85, 2.97] | 4.59 [4.49, 4.69] | 3.38 [3.32, 3.45] | 3.68 [3.59, 3.77] | 4.40 [4.29, 4.51] | 3.84 [3.75, 3.95] |  | 3.10 [3.04, 3.17] |
| 45 | 3.01 [2.94, 3.08] | 3.63 [3.56, 3.70] | 3.63 [3.56, 3.70] | 2.92 [2.86, 2.98] | 4.66 [4.49, 4.84] | 3.42 [3.35, 3.49] | 3.65 [3.53, 3.76] | 4.45 [4.32, 4.59] | 3.89 [3.78, 4.02] |  | 3.12 [3.05, 3.19] |
| 60 | 2.99 [2.92, 3.06] | 3.67 [3.59, 3.74] | 3.67 [3.59, 3.74] | 2.94 [2.87, 3.01] |  | 3.47 [3.40, 3.54] | 2.26 [1.69, 2.85] | 3.38 [2.22, 4.65] | 3.80 [2.44, 5.42] |  | 3.11 [3.04, 3.18] |
| 90 | 2.96 [2.88, 3.06] | 3.55 [3.46, 3.64] | 3.55 [3.46, 3.64] | 2.87 [2.80, 2.94] |  | 3.45 [3.36, 3.54] |  |  |  |  | 3.04 [2.95, 3.12] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.17 [0.11, 0.23] | 0.27 [0.15, 0.38] |  | -0.29 [-0.38, -0.22]* | 0.76 [0.57, 0.94] | 0.11 [-0.01, 0.20] | 0.08 [-0.04, 0.18] |  |  | 0.78 [0.62, 0.95] |  | wt_med |
| 1 |  | 0.14 [-0.02, 0.28] | 0.14 [-0.02, 0.28] | -0.41 [-0.53, -0.31]* | 0.64 [0.39, 0.88] | -0.04 [-0.18, 0.08] | -0.02 [-0.17, 0.13] |  |  | 0.62 [0.47, 0.79] | -0.16 [-0.22, -0.10]* | clim |
| 2 |  | 0.16 [-0.01, 0.29] | 0.16 [-0.01, 0.29] | -0.41 [-0.52, -0.30]* | 0.66 [0.42, 0.90] | -0.03 [-0.18, 0.09] | 0.01 [-0.14, 0.16] |  |  | 0.63 [0.47, 0.79] | -0.15 [-0.21, -0.10]* | clim |
| 3 |  | 0.16 [0.00, 0.30] | 0.16 [0.00, 0.30] | -0.40 [-0.52, -0.30]* | 0.67 [0.42, 0.91] | -0.03 [-0.17, 0.09] | 0.02 [-0.13, 0.17] | 0.99 [0.64, 1.39] | 0.36 [0.14, 0.60] | 0.63 [0.48, 0.79] | -0.14 [-0.20, -0.09]* | clim |
| 4 |  | 0.17 [0.00, 0.31] | 0.17 [0.00, 0.31] | -0.40 [-0.51, -0.29]* | 0.67 [0.42, 0.91] | -0.03 [-0.17, 0.10] | 0.02 [-0.14, 0.17] | 0.99 [0.65, 1.40] | 0.36 [0.14, 0.59] | 0.63 [0.48, 0.79] | -0.14 [-0.19, -0.09]* | clim |
| 5 |  | 0.18 [0.02, 0.31] | 0.18 [0.02, 0.31] | -0.39 [-0.51, -0.28]* | 0.68 [0.43, 0.94] | -0.02 [-0.16, 0.10] | 0.05 [-0.12, 0.20] | 0.99 [0.64, 1.38] | 0.35 [0.12, 0.58] | 0.62 [0.47, 0.78] | -0.13 [-0.19, -0.08]* | clim |
| 6 |  | 0.19 [0.03, 0.33] | 0.19 [0.03, 0.33] | -0.37 [-0.49, -0.27]* | 0.87 [0.57, 1.17] | -0.01 [-0.15, 0.12] | 0.10 [-0.07, 0.25] | 0.98 [0.64, 1.39] | 0.36 [0.14, 0.61] | 0.62 [0.47, 0.78] | -0.11 [-0.16, -0.06]* | clim |
| 7 |  | 0.28 [0.11, 0.43] | 0.28 [0.11, 0.43] | -0.36 [-0.47, -0.25]* | 0.92 [0.59, 1.25] | 0.07 [-0.08, 0.20] | 0.15 [-0.01, 0.30] | 0.99 [0.64, 1.39] | 0.36 [0.14, 0.61] |  | -0.10 [-0.16, -0.06]* | clim |
| 10 |  | 0.30 [0.14, 0.44] | 0.30 [0.14, 0.44] | -0.34 [-0.45, -0.24]* | 0.95 [0.62, 1.29] | 0.07 [-0.07, 0.19] | 0.18 [0.02, 0.34] | 1.00 [0.64, 1.40] | 0.34 [0.11, 0.60] |  | -0.09 [-0.14, -0.04]* | clim |
| 14 |  | 0.39 [0.21, 0.56] | 0.39 [0.21, 0.56] | -0.32 [-0.44, -0.21]* | 1.03 [0.66, 1.39] | 0.15 [-0.00, 0.29] | 0.19 [0.02, 0.35] | 1.00 [0.65, 1.39] | 0.34 [0.10, 0.59] |  | -0.08 [-0.13, -0.03]* | clim |
| 21 |  | 0.46 [0.25, 0.66] | 0.46 [0.25, 0.66] | -0.32 [-0.45, -0.21]* | 1.11 [0.71, 1.50] | 0.20 [0.03, 0.36] | 0.26 [0.06, 0.44] | 0.99 [0.63, 1.39] | 0.34 [0.09, 0.60] |  | -0.05 [-0.11, 0.01] | clim |
| 30 |  | 0.49 [0.27, 0.72] | 0.49 [0.27, 0.72] | -0.31 [-0.44, -0.19]* | 1.17 [0.72, 1.62] | 0.22 [0.05, 0.38] | 0.25 [0.02, 0.46] | 1.00 [0.62, 1.42] | 0.32 [0.04, 0.63] |  | -0.03 [-0.10, 0.03] | clim |
| 45 |  | 0.52 [0.25, 0.79] | 0.52 [0.25, 0.79] | -0.30 [-0.46, -0.17]* | 0.64 [0.18, 1.03] | 0.27 [0.06, 0.47] | 0.21 [-0.10, 0.48] | 0.95 [0.59, 1.35] | 0.24 [-0.05, 0.55] |  | 0.01 [-0.09, 0.11] | clim |
| 60 |  | 0.62 [0.33, 0.95] | 0.62 [0.33, 0.95] | -0.17 [-0.33, -0.00]* |  | 0.39 [0.18, 0.62] | 0.17 [-0.96, 0.95] | 0.49 [-0.65, 1.07] | 0.66 [-0.39, 1.19] |  | 0.08 [-0.03, 0.22] | clim |
| 90 |  | 0.60 [0.36, 0.88] | 0.60 [0.36, 0.88] | -0.12 [-0.27, 0.02] |  | 0.44 [0.25, 0.64] |  |  |  |  | 0.04 [-0.06, 0.14] | clim |

## D4 — planner optimiser

Regret: the park's top 4 rides (headliners first, ex-ante level), objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; true cost of the forecast's plan − truth-optimal cost; paired per park-day. Not simulated: overflow / headliner dropping, fixed blocks, opensAt floors, early entry, live corrections; waits are read per 15-min slot (the frontend reads the hourly point today). Ordering: headliner pairs with a true dayPeak gap ≥ 1 min, paired on the same pairs.

**optimiser regret (min per park-day)**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 30.29 [29.70, 30.92] | 28.23 [27.66, 28.78] | 28.23 [27.67, 28.78] | 28.72 [28.21, 29.24] | 28.98 [28.26, 29.68] | 28.96 [28.42, 29.50] | 27.86 [27.23, 28.48] |  |  | 34.51 [33.89, 35.15] | 29.38 [28.88, 29.93] |
| 3 | 30.52 [29.92, 31.10] | 28.35 [27.75, 28.92] | 28.35 [27.76, 28.91] | 28.95 [28.43, 29.48] | 29.29 [28.56, 30.06] | 28.93 [28.37, 29.48] | 27.89 [27.28, 28.51] | 33.75 [33.12, 34.38] | 33.68 [33.02, 34.33] | 34.34 [33.71, 34.95] | 29.76 [29.23, 30.29] |
| 7 | 30.52 [29.97, 31.12] | 28.82 [28.25, 29.37] | 28.81 [28.25, 29.38] | 29.27 [28.75, 29.83] | 29.72 [28.99, 30.52] | 29.49 [28.92, 30.04] | 28.67 [28.05, 29.32] | 34.59 [33.93, 35.27] | 33.99 [33.32, 34.62] |  | 30.04 [29.50, 30.60] |
| 14 | 31.01 [30.38, 31.58] | 29.35 [28.76, 29.89] | 29.35 [28.76, 29.89] | 29.83 [29.25, 30.37] | 30.65 [29.85, 31.45] | 30.18 [29.59, 30.75] | 29.59 [28.94, 30.26] | 34.95 [34.27, 35.66] | 34.55 [33.90, 35.26] |  | 30.83 [30.25, 31.37] |
| 30 | 31.48 [30.89, 32.04] | 30.07 [29.48, 30.68] | 30.06 [29.45, 30.67] | 30.44 [29.89, 31.02] | 31.00 [30.21, 31.84] | 31.07 [30.49, 31.63] | 30.28 [29.56, 31.04] | 36.16 [35.41, 37.00] | 35.83 [35.01, 36.66] |  | 31.60 [31.01, 32.17] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.89 [0.28, 1.55] | -1.23 [-1.93, -0.55]* | -1.23 [-1.93, -0.53]* | -0.66 [-1.39, -0.01]* | -0.07 [-1.01, 0.81] | -0.47 [-1.25, 0.15] | -0.58 [-1.54, 0.22] |  |  | 4.84 [3.92, 5.72] |  | wt_med |
| 3 | 0.64 [-0.05, 1.35] | -1.52 [-2.23, -0.85]* | -1.52 [-2.24, -0.85]* | -0.80 [-1.52, -0.20]* | -0.29 [-1.30, 0.70] | -0.92 [-1.72, -0.21]* | -1.12 [-2.03, -0.36]* | 4.41 [3.11, 5.78] | 4.34 [3.09, 5.57] | 4.20 [3.27, 5.08] |  | wt_med |
| 7 | 0.52 [-0.19, 1.31] | -1.17 [-1.90, -0.39]* | -1.17 [-1.91, -0.40]* | -0.76 [-1.29, -0.26]* | -0.01 [-1.12, 1.11] | -0.46 [-1.20, 0.22] | -0.34 [-1.25, 0.41] | 5.33 [4.04, 6.74] | 4.74 [3.57, 6.05] |  |  | wt_med |
| 14 | 0.26 [-0.35, 0.90] | -1.47 [-2.31, -0.62]* | -1.46 [-2.30, -0.61]* | -0.99 [-1.69, -0.39]* | 0.06 [-1.05, 1.14] | -0.58 [-1.41, 0.20] | -0.23 [-1.13, 0.64] | 4.90 [3.43, 6.46] | 4.51 [3.16, 5.94] |  |  | wt_med |
| 30 |  | -1.42 [-2.43, -0.51]* | -1.44 [-2.46, -0.53]* | -1.14 [-2.11, -0.21]* | -0.42 [-2.14, 0.98] | -0.41 [-1.38, 0.57] | -0.46 [-1.78, 0.82] | 5.40 [3.56, 7.19] | 5.15 [3.35, 6.84] |  | 0.04 [-0.78, 0.82] | clim |

**dayPeak pairwise ordering (headliners)**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.73 [0.72, 0.73] | 0.76 [0.75, 0.76] |  | 0.76 [0.76, 0.76] | 0.71 [0.70, 0.71] | 0.76 [0.76, 0.76] | 0.77 [0.76, 0.77] |  |  | 0.73 [0.72, 0.73] | 0.74 [0.74, 0.74] |
| 1 | 0.73 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.70 [0.70, 0.71] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.77] |  |  | 0.73 [0.72, 0.73] | 0.74 [0.73, 0.74] |
| 2 | 0.73 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.70 [0.70, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] |  |  | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.74] |
| 3 | 0.72 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.74] |
| 4 | 0.72 [0.72, 0.73] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.73] |
| 5 | 0.72 [0.72, 0.73] | 0.75 [0.75, 0.76] | 0.75 [0.75, 0.76] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.70 [0.70, 0.71] | 0.74 [0.73, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.73] |
| 6 | 0.72 [0.72, 0.72] | 0.76 [0.75, 0.76] | 0.76 [0.75, 0.76] | 0.75 [0.75, 0.75] | 0.69 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.75 [0.75, 0.76] | 0.70 [0.70, 0.70] | 0.73 [0.73, 0.74] | 0.73 [0.72, 0.73] | 0.73 [0.73, 0.73] |
| 7 | 0.72 [0.72, 0.72] | 0.75 [0.74, 0.75] | 0.75 [0.74, 0.75] | 0.74 [0.74, 0.75] | 0.68 [0.68, 0.68] | 0.75 [0.74, 0.75] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.73 [0.73, 0.73] |  | 0.73 [0.72, 0.73] |
| 10 | 0.72 [0.72, 0.72] | 0.74 [0.74, 0.75] | 0.74 [0.74, 0.75] | 0.74 [0.74, 0.74] | 0.68 [0.67, 0.68] | 0.75 [0.74, 0.75] | 0.74 [0.74, 0.75] | 0.69 [0.69, 0.69] | 0.72 [0.72, 0.73] |  | 0.72 [0.72, 0.73] |
| 14 | 0.72 [0.71, 0.72] | 0.74 [0.73, 0.74] | 0.74 [0.73, 0.74] | 0.74 [0.73, 0.74] | 0.67 [0.67, 0.68] | 0.74 [0.74, 0.74] | 0.74 [0.73, 0.74] | 0.68 [0.68, 0.69] | 0.72 [0.71, 0.72] |  | 0.72 [0.72, 0.72] |
| 21 | 0.71 [0.71, 0.72] | 0.73 [0.73, 0.73] | 0.73 [0.73, 0.73] | 0.73 [0.73, 0.73] | 0.67 [0.67, 0.68] | 0.73 [0.73, 0.74] | 0.73 [0.73, 0.73] | 0.68 [0.67, 0.68] | 0.71 [0.71, 0.71] |  | 0.71 [0.71, 0.72] |
| 30 | 0.71 [0.71, 0.71] | 0.72 [0.72, 0.73] | 0.72 [0.72, 0.73] | 0.72 [0.72, 0.72] | 0.67 [0.66, 0.67] | 0.73 [0.72, 0.73] | 0.72 [0.72, 0.73] | 0.67 [0.67, 0.67] | 0.70 [0.70, 0.71] |  | 0.71 [0.70, 0.71] |
| 45 | 0.70 [0.70, 0.71] | 0.72 [0.71, 0.72] | 0.72 [0.71, 0.72] | 0.71 [0.71, 0.72] | 0.66 [0.65, 0.67] | 0.72 [0.72, 0.72] | 0.72 [0.72, 0.73] | 0.67 [0.66, 0.67] | 0.70 [0.69, 0.70] |  | 0.70 [0.70, 0.70] |
| 60 | 0.70 [0.70, 0.71] | 0.71 [0.71, 0.71] | 0.71 [0.71, 0.71] | 0.71 [0.70, 0.71] |  | 0.71 [0.71, 0.72] | 0.75 [0.67, 0.85] | 0.65 [0.58, 0.73] | 0.71 [0.65, 0.77] |  | 0.70 [0.69, 0.70] |
| 90 | 0.71 [0.70, 0.71] | 0.72 [0.71, 0.72] | 0.72 [0.71, 0.72] | 0.71 [0.71, 0.72] |  | 0.72 [0.71, 0.72] |  |  |  |  | 0.70 [0.70, 0.71] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.03]* |  | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.02 [0.02, 0.03]* | 0.04 [0.03, 0.05]* |  |  | -0.02 [-0.03, -0.02] |  | wt_med |
| 1 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.05]* |  |  | -0.02 [-0.03, -0.01] |  | wt_med |
| 2 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.04]* |  |  | -0.02 [-0.03, -0.01] |  | wt_med |
| 3 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.04 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.02 [-0.03, -0.01] |  | wt_med |
| 4 | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.03]* | 0.03 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.02 [0.01, 0.02]* | -0.02 [-0.02, -0.01] |  | wt_med |
| 5 | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.04]* | 0.03 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.02 [-0.02, -0.01] |  | wt_med |
| 6 | -0.01 [-0.02, -0.01] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.03] | 0.03 [0.02, 0.04]* | 0.03 [0.03, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.01, 0.02]* | -0.01 [-0.02, -0.01] |  | wt_med |
| 7 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.03] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  |  | wt_med |
| 10 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | 0.02 [0.02, 0.03]* | -0.04 [-0.06, -0.03] | 0.03 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  |  | wt_med |
| 14 | -0.01 [-0.01, -0.00] | 0.02 [0.01, 0.03]* | 0.02 [0.01, 0.03]* | 0.02 [0.02, 0.03]* | -0.04 [-0.05, -0.02] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  |  | wt_med |
| 21 | -0.01 [-0.01, -0.00] | 0.02 [0.01, 0.03]* | 0.02 [0.01, 0.03]* | 0.02 [0.02, 0.03]* | -0.03 [-0.05, -0.01] | 0.02 [0.02, 0.03]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  |  | wt_med |
| 30 |  | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.04]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.05]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | 0.01 [-0.00, 0.01] | clim |
| 45 |  | 0.02 [0.01, 0.03]* | 0.02 [0.01, 0.03]* | 0.03 [0.02, 0.03]* | -0.03 [-0.05, -0.02] | 0.03 [0.02, 0.04]* | 0.03 [0.02, 0.04]* | -0.02 [-0.03, -0.01] | 0.01 [0.00, 0.02]* |  | 0.00 [-0.00, 0.01] | clim |
| 60 |  | 0.02 [0.01, 0.04]* | 0.02 [0.01, 0.04]* | 0.02 [0.02, 0.03]* |  | 0.03 [0.01, 0.04]* | 0.10 [0.03, 0.14]* | 0.02 [-0.04, 0.05] | 0.06 [-0.02, 0.13] |  | 0.01 [-0.00, 0.01] | clim |
| 90 |  | 0.03 [0.01, 0.04]* | 0.03 [0.01, 0.04]* | 0.03 [0.02, 0.04]* |  | 0.03 [0.02, 0.04]* |  |  |  |  | 0.01 [0.00, 0.02]* | clim |

## D5 — rope drop

First hour = the first 4 slots of the PUBLISHED window, paired slot by slot. worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`) per ride-day, paired ride-day by ride-day; `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.54 [4.46, 4.61] | 4.10 [4.04, 4.16] |  | 4.11 [4.05, 4.17] | 3.95 [3.88, 4.03] | 4.11 [4.05, 4.17] | 3.87 [3.80, 3.94] | 4.10 [4.04, 4.16] | 4.51 [4.44, 4.58] |  |  | 5.01 [4.93, 5.10] | 4.26 [4.19, 4.32] |
| 1 | 4.55 [4.48, 4.63] | 4.16 [4.09, 4.22] | 4.16 [4.09, 4.22] | 4.17 [4.11, 4.24] | 4.02 [3.94, 4.10] | 4.17 [4.11, 4.24] | 3.92 [3.85, 3.99] | 4.16 [4.10, 4.23] | 4.50 [4.44, 4.57] |  |  | 5.01 [4.92, 5.10] | 4.33 [4.26, 4.40] |
| 2 | 4.57 [4.49, 4.64] | 4.18 [4.11, 4.24] | 4.18 [4.11, 4.24] | 4.20 [4.13, 4.26] | 4.04 [3.95, 4.12] | 4.20 [4.13, 4.26] | 3.94 [3.87, 4.01] | 4.19 [4.12, 4.25] | 4.50 [4.43, 4.57] |  |  | 5.01 [4.92, 5.09] | 4.36 [4.29, 4.43] |
| 3 | 4.57 [4.49, 4.64] | 4.20 [4.13, 4.26] | 4.20 [4.13, 4.26] | 4.21 [4.15, 4.28] | 4.05 [3.97, 4.14] | 4.21 [4.14, 4.27] | 3.96 [3.89, 4.03] | 4.20 [4.13, 4.26] | 4.50 [4.43, 4.57] | 5.80 [5.70, 5.88] | 5.95 [5.85, 6.03] | 5.01 [4.92, 5.10] | 4.38 [4.31, 4.45] |
| 4 | 4.58 [4.50, 4.65] | 4.21 [4.14, 4.27] | 4.21 [4.14, 4.27] | 4.23 [4.16, 4.29] | 4.07 [3.99, 4.15] | 4.22 [4.16, 4.29] | 3.98 [3.91, 4.05] | 4.21 [4.15, 4.28] | 4.50 [4.43, 4.56] | 5.81 [5.72, 5.90] | 5.96 [5.87, 6.05] | 5.00 [4.92, 5.09] | 4.40 [4.33, 4.47] |
| 5 | 4.59 [4.51, 4.66] | 4.23 [4.16, 4.30] | 4.23 [4.16, 4.30] | 4.25 [4.18, 4.31] | 4.11 [4.02, 4.19] | 4.25 [4.18, 4.31] | 3.99 [3.92, 4.07] | 4.24 [4.17, 4.30] | 4.50 [4.43, 4.56] | 5.84 [5.75, 5.92] | 5.99 [5.90, 6.08] | 4.99 [4.91, 5.08] | 4.42 [4.35, 4.49] |
| 6 | 4.59 [4.51, 4.67] | 4.27 [4.20, 4.34] | 4.27 [4.20, 4.34] | 4.29 [4.22, 4.36] | 4.16 [4.08, 4.25] | 4.29 [4.22, 4.35] | 4.03 [3.96, 4.11] | 4.27 [4.20, 4.34] | 4.49 [4.43, 4.56] | 5.81 [5.72, 5.90] | 5.96 [5.87, 6.05] | 4.99 [4.90, 5.07] | 4.46 [4.38, 4.53] |
| 7 | 4.61 [4.53, 4.68] | 4.32 [4.24, 4.38] | 4.32 [4.24, 4.38] | 4.36 [4.28, 4.42] | 4.23 [4.14, 4.31] | 4.36 [4.29, 4.42] | 4.09 [4.02, 4.17] | 4.34 [4.27, 4.40] | 4.87 [4.80, 4.94] | 5.90 [5.81, 6.00] | 6.05 [5.96, 6.15] |  | 4.53 [4.45, 4.59] |
| 10 | 4.63 [4.55, 4.70] | 4.39 [4.32, 4.46] | 4.39 [4.32, 4.46] | 4.42 [4.34, 4.48] | 4.32 [4.23, 4.42] | 4.42 [4.35, 4.49] | 4.18 [4.10, 4.26] | 4.39 [4.32, 4.45] | 4.87 [4.79, 4.94] | 5.98 [5.88, 6.08] | 6.14 [6.04, 6.24] |  | 4.58 [4.51, 4.65] |
| 14 | 4.68 [4.60, 4.75] | 4.46 [4.39, 4.53] | 4.46 [4.39, 4.53] | 4.49 [4.42, 4.56] | 4.42 [4.33, 4.52] | 4.50 [4.43, 4.57] | 4.27 [4.19, 4.36] | 4.46 [4.39, 4.53] | 5.11 [5.03, 5.19] | 6.11 [6.01, 6.21] | 6.27 [6.17, 6.37] |  | 4.66 [4.58, 4.73] |
| 21 | 4.74 [4.66, 4.82] | 4.56 [4.48, 4.63] | 4.56 [4.48, 4.63] | 4.56 [4.48, 4.63] | 4.56 [4.46, 4.66] | 4.58 [4.50, 4.65] | 4.40 [4.31, 4.49] | 4.53 [4.46, 4.60] | 5.30 [5.22, 5.39] | 6.24 [6.13, 6.35] | 6.41 [6.30, 6.52] |  | 4.73 [4.65, 4.80] |
| 30 | 4.80 [4.72, 4.88] | 4.63 [4.55, 4.70] | 4.63 [4.55, 4.70] | 4.62 [4.54, 4.68] | 4.70 [4.59, 4.81] | 4.64 [4.56, 4.71] | 4.63 [4.52, 4.74] | 4.58 [4.50, 4.64] | 5.40 [5.31, 5.49] | 6.56 [6.44, 6.69] | 6.72 [6.60, 6.84] |  | 4.78 [4.70, 4.86] |
| 45 | 4.83 [4.74, 4.91] | 4.67 [4.58, 4.74] | 4.67 [4.58, 4.74] | 4.66 [4.59, 4.74] | 4.42 [4.25, 4.60] | 4.68 [4.61, 4.76] | 4.79 [4.66, 4.93] | 4.63 [4.55, 4.70] | 5.48 [5.39, 5.57] | 6.80 [6.65, 6.97] | 6.96 [6.81, 7.12] |  | 4.82 [4.73, 4.90] |
| 60 | 4.86 [4.77, 4.95] | 4.77 [4.68, 4.85] | 4.77 [4.68, 4.85] | 4.76 [4.67, 4.84] |  | 4.78 [4.70, 4.86] | 1.34 [0.93, 1.82] | 4.73 [4.64, 4.81] | 5.59 [5.50, 5.69] | 1.89 [1.37, 2.48] | 1.95 [1.42, 2.55] |  | 4.88 [4.79, 4.97] |
| 90 | 5.04 [4.94, 5.15] | 4.98 [4.88, 5.08] | 4.98 [4.88, 5.08] | 4.90 [4.81, 4.99] |  | 4.95 [4.85, 5.04] |  | 4.89 [4.80, 4.99] | 5.59 [5.48, 5.70] |  |  |  | 4.99 [4.89, 5.08] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.28 [0.19, 0.38] | -0.17 [-0.30, -0.07]* |  | -0.16 [-0.29, -0.07]* | -0.16 [-0.34, -0.04]* | -0.16 [-0.29, -0.07]* | -0.21 [-0.39, -0.08]* | -0.17 [-0.30, -0.07]* | 0.23 [-0.10, 0.58] |  |  | 0.69 [0.51, 0.88] |  | wt_med |
| 1 | 0.23 [0.15, 0.33] | -0.18 [-0.32, -0.08]* | -0.18 [-0.32, -0.08]* | -0.17 [-0.31, -0.07]* | -0.18 [-0.37, -0.05]* | -0.17 [-0.31, -0.08]* | -0.22 [-0.41, -0.08]* | -0.18 [-0.32, -0.08]* | 0.16 [-0.19, 0.51] |  |  | 0.62 [0.44, 0.80] |  | wt_med |
| 2 | 0.22 [0.13, 0.32] | -0.18 [-0.33, -0.08]* | -0.18 [-0.33, -0.08]* | -0.17 [-0.31, -0.08]* | -0.19 [-0.38, -0.06]* | -0.18 [-0.31, -0.08]* | -0.22 [-0.41, -0.08]* | -0.19 [-0.32, -0.08]* | 0.13 [-0.22, 0.49] |  |  | 0.59 [0.41, 0.77] |  | wt_med |
| 3 | 0.21 [0.11, 0.31] | -0.19 [-0.33, -0.08]* | -0.19 [-0.33, -0.08]* | -0.18 [-0.32, -0.08]* | -0.20 [-0.40, -0.06]* | -0.18 [-0.32, -0.08]* | -0.22 [-0.42, -0.09]* | -0.19 [-0.33, -0.09]* | 0.11 [-0.24, 0.47] | 1.61 [1.39, 1.87] | 1.76 [1.54, 2.01] | 0.57 [0.39, 0.75] |  | wt_med |
| 4 | 0.20 [0.10, 0.31] | -0.19 [-0.33, -0.09]* | -0.19 [-0.33, -0.09]* | -0.18 [-0.32, -0.08]* | -0.20 [-0.40, -0.07]* | -0.18 [-0.32, -0.08]* | -0.22 [-0.42, -0.09]* | -0.19 [-0.34, -0.09]* | 0.09 [-0.27, 0.45] | 1.62 [1.40, 1.86] | 1.77 [1.55, 2.00] | 0.54 [0.37, 0.72] |  | wt_med |
| 5 | 0.19 [0.09, 0.30] | -0.19 [-0.33, -0.09]* | -0.19 [-0.33, -0.09]* | -0.18 [-0.32, -0.09]* | -0.20 [-0.39, -0.07]* | -0.18 [-0.31, -0.09]* | -0.23 [-0.42, -0.09]* | -0.19 [-0.33, -0.09]* | 0.06 [-0.30, 0.43] | 1.63 [1.40, 1.88] | 1.78 [1.56, 2.02] | 0.52 [0.34, 0.69] |  | wt_med |
| 6 | 0.16 [0.06, 0.27] | -0.19 [-0.32, -0.09]* | -0.19 [-0.32, -0.09]* | -0.18 [-0.31, -0.09]* | -0.20 [-0.38, -0.07]* | -0.18 [-0.30, -0.09]* | -0.22 [-0.41, -0.09]* | -0.19 [-0.33, -0.10]* | 0.02 [-0.35, 0.39] | 1.56 [1.35, 1.80] | 1.71 [1.49, 1.96] | 0.47 [0.29, 0.64] |  | wt_med |
| 7 | 0.12 [0.01, 0.23] | -0.19 [-0.32, -0.09]* | -0.19 [-0.32, -0.09]* | -0.18 [-0.31, -0.09]* | -0.20 [-0.38, -0.07]* | -0.17 [-0.30, -0.09]* | -0.23 [-0.41, -0.10]* | -0.20 [-0.34, -0.10]* | 0.36 [-0.02, 0.74] | 1.60 [1.39, 1.84] | 1.75 [1.53, 1.99] |  |  | wt_med |
| 10 | 0.07 [-0.03, 0.18] | -0.18 [-0.30, -0.10]* | -0.18 [-0.30, -0.10]* | -0.17 [-0.29, -0.09]* | -0.19 [-0.35, -0.08]* | -0.16 [-0.27, -0.08]* | -0.21 [-0.37, -0.10]* | -0.20 [-0.33, -0.11]* | 0.29 [-0.10, 0.67] | 1.60 [1.38, 1.86] | 1.76 [1.52, 2.02] |  |  | wt_med |
| 14 | 0.04 [-0.07, 0.16] | -0.18 [-0.29, -0.09]* | -0.18 [-0.29, -0.09]* | -0.17 [-0.29, -0.09]* | -0.19 [-0.34, -0.08]* | -0.15 [-0.27, -0.07]* | -0.21 [-0.35, -0.10]* | -0.20 [-0.32, -0.11]* | 0.47 [0.06, 0.86] | 1.64 [1.40, 1.90] | 1.80 [1.54, 2.08] |  |  | wt_med |
| 21 | 0.01 [-0.11, 0.13] | -0.17 [-0.28, -0.09]* | -0.17 [-0.28, -0.09]* | -0.17 [-0.29, -0.09]* | -0.20 [-0.36, -0.08]* | -0.15 [-0.26, -0.06]* | -0.19 [-0.34, -0.09]* | -0.20 [-0.32, -0.11]* | 0.56 [0.14, 0.95] | 1.63 [1.40, 1.89] | 1.81 [1.56, 2.07] |  |  | wt_med |
| 30 | -0.00 [-0.12, 0.12] | -0.16 [-0.26, -0.08]* | -0.16 [-0.26, -0.08]* | -0.16 [-0.27, -0.09]* | -0.23 [-0.41, -0.10]* | -0.13 [-0.24, -0.05]* | -0.17 [-0.31, -0.05]* | -0.20 [-0.32, -0.11]* | 0.59 [0.16, 0.99] | 1.73 [1.49, 1.98] | 1.88 [1.63, 2.15] |  |  | wt_med |
| 45 | -0.03 [-0.15, 0.11] | -0.14 [-0.24, -0.07]* | -0.14 [-0.24, -0.07]* | -0.14 [-0.23, -0.07]* | -0.25 [-0.53, -0.04]* | -0.11 [-0.20, -0.05]* | -0.15 [-0.29, -0.05]* | -0.18 [-0.28, -0.10]* | 0.65 [0.23, 1.07] | 1.84 [1.62, 2.12] | 2.01 [1.72, 2.32] |  |  | wt_med |
| 60 |  | -0.09 [-0.29, 0.07] | -0.09 [-0.29, 0.07] | -0.07 [-0.26, 0.08] |  | -0.05 [-0.24, 0.12] | 0.10 [-0.16, 0.93] | -0.11 [-0.31, 0.05] | 0.69 [0.16, 1.21] | 0.59 [0.19, 2.79] | 0.65 [0.25, 2.90] |  | 0.05 [-0.11, 0.20] | clim |
| 90 | 0.02 [-0.15, 0.22] | -0.06 [-0.13, 0.01] | -0.06 [-0.13, 0.01] | -0.09 [-0.14, -0.04]* |  | -0.04 [-0.10, 0.02] |  | -0.09 [-0.18, 0.00] | 0.56 [0.10, 1.09] |  |  |  |  | wt_med |

**first-hour MAE, schedule known at origin**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.49 [4.40, 4.57] | 4.03 [3.95, 4.10] |  | 4.04 [3.97, 4.11] | 3.93 [3.84, 4.02] | 4.04 [3.97, 4.11] | 3.83 [3.75, 3.90] | 4.04 [3.97, 4.12] | 4.55 [4.47, 4.62] |  |  | 4.93 [4.84, 5.03] | 4.22 [4.14, 4.29] |
| 1 | 4.50 [4.41, 4.58] | 4.08 [4.01, 4.16] | 4.08 [4.01, 4.16] | 4.11 [4.03, 4.18] | 3.99 [3.90, 4.08] | 4.11 [4.03, 4.18] | 3.87 [3.79, 3.95] | 4.11 [4.03, 4.18] | 4.54 [4.47, 4.62] |  |  | 4.93 [4.83, 5.02] | 4.29 [4.21, 4.36] |
| 2 | 4.51 [4.41, 4.59] | 4.10 [4.02, 4.17] | 4.10 [4.02, 4.17] | 4.12 [4.04, 4.20] | 4.01 [3.92, 4.10] | 4.12 [4.04, 4.20] | 3.89 [3.80, 3.96] | 4.12 [4.05, 4.20] | 4.54 [4.46, 4.61] |  |  | 4.92 [4.82, 5.02] | 4.31 [4.23, 4.39] |
| 3 | 4.51 [4.41, 4.59] | 4.11 [4.04, 4.19] | 4.11 [4.04, 4.19] | 4.13 [4.05, 4.21] | 4.02 [3.92, 4.11] | 4.13 [4.05, 4.21] | 3.90 [3.82, 3.98] | 4.13 [4.05, 4.21] | 4.53 [4.46, 4.60] | 5.82 [5.71, 5.92] | 5.95 [5.84, 6.05] | 4.92 [4.82, 5.01] | 4.33 [4.24, 4.40] |
| 4 | 4.51 [4.41, 4.59] | 4.12 [4.04, 4.19] | 4.12 [4.04, 4.19] | 4.14 [4.06, 4.21] | 4.03 [3.94, 4.12] | 4.14 [4.06, 4.21] | 3.90 [3.82, 3.98] | 4.14 [4.06, 4.21] | 4.52 [4.45, 4.59] | 5.83 [5.72, 5.93] | 5.96 [5.86, 6.07] | 4.91 [4.81, 5.01] | 4.34 [4.25, 4.41] |
| 5 | 4.50 [4.41, 4.59] | 4.12 [4.04, 4.19] | 4.12 [4.04, 4.19] | 4.14 [4.06, 4.21] | 4.05 [3.95, 4.14] | 4.14 [4.06, 4.21] | 3.91 [3.83, 3.98] | 4.14 [4.06, 4.21] | 4.50 [4.43, 4.57] | 5.84 [5.73, 5.93] | 5.98 [5.87, 6.08] | 4.89 [4.79, 4.99] | 4.34 [4.25, 4.41] |
| 6 | 4.48 [4.39, 4.57] | 4.14 [4.06, 4.21] | 4.14 [4.06, 4.21] | 4.16 [4.07, 4.23] | 4.08 [3.99, 4.17] | 4.16 [4.07, 4.23] | 3.93 [3.84, 4.00] | 4.16 [4.07, 4.23] | 4.48 [4.41, 4.56] | 5.79 [5.68, 5.89] | 5.93 [5.83, 6.03] | 4.87 [4.77, 4.97] | 4.35 [4.26, 4.42] |
| 7 | 4.48 [4.39, 4.57] | 4.16 [4.08, 4.23] | 4.16 [4.08, 4.23] | 4.20 [4.12, 4.28] | 4.12 [4.03, 4.22] | 4.20 [4.12, 4.28] | 3.96 [3.88, 4.04] | 4.20 [4.12, 4.28] | 4.85 [4.76, 4.93] | 5.84 [5.73, 5.95] | 5.98 [5.87, 6.09] |  | 4.39 [4.30, 4.47] |
| 10 | 4.43 [4.34, 4.52] | 4.17 [4.09, 4.24] | 4.17 [4.09, 4.24] | 4.20 [4.12, 4.27] | 4.14 [4.04, 4.23] | 4.20 [4.12, 4.27] | 3.97 [3.89, 4.05] | 4.20 [4.12, 4.27] | 4.80 [4.72, 4.88] | 5.84 [5.73, 5.94] | 5.99 [5.88, 6.10] |  | 4.38 [4.30, 4.46] |
| 14 | 4.41 [4.32, 4.50] | 4.15 [4.07, 4.23] | 4.15 [4.07, 4.23] | 4.19 [4.11, 4.27] | 4.14 [4.04, 4.24] | 4.19 [4.11, 4.27] | 3.96 [3.87, 4.05] | 4.19 [4.11, 4.27] | 5.01 [4.92, 5.10] | 5.90 [5.78, 6.01] | 6.07 [5.95, 6.19] |  | 4.38 [4.29, 4.46] |
| 21 | 4.41 [4.31, 4.50] | 4.18 [4.09, 4.26] | 4.18 [4.09, 4.26] | 4.19 [4.11, 4.27] | 4.21 [4.10, 4.31] | 4.19 [4.11, 4.27] | 4.01 [3.91, 4.10] | 4.19 [4.11, 4.27] | 5.15 [5.06, 5.25] | 5.90 [5.79, 6.01] | 6.07 [5.96, 6.19] |  | 4.39 [4.29, 4.47] |
| 30 | 4.44 [4.33, 4.54] | 4.16 [4.07, 4.25] | 4.16 [4.07, 4.25] | 4.17 [4.08, 4.25] | 4.28 [4.17, 4.40] | 4.17 [4.08, 4.25] | 4.09 [3.98, 4.21] | 4.17 [4.08, 4.25] | 5.12 [5.02, 5.21] | 6.13 [5.99, 6.27] | 6.29 [6.15, 6.43] |  | 4.36 [4.27, 4.45] |
| 45 | 4.29 [4.18, 4.38] | 4.05 [3.96, 4.14] | 4.05 [3.96, 4.14] | 4.07 [3.98, 4.16] | 4.38 [4.12, 4.67] | 4.07 [3.98, 4.16] | 4.08 [3.92, 4.23] | 4.07 [3.98, 4.16] | 5.09 [4.98, 5.19] | 6.13 [5.96, 6.31] | 6.29 [6.12, 6.47] |  | 4.23 [4.13, 4.32] |
| 60 | 4.03 [3.92, 4.13] | 3.98 [3.88, 4.08] | 3.98 [3.88, 4.08] | 4.01 [3.92, 4.11] |  | 4.01 [3.92, 4.11] | 1.15 [0.77, 1.63] | 4.02 [3.92, 4.11] | 5.12 [5.01, 5.24] | 1.68 [1.18, 2.26] | 1.74 [1.23, 2.32] |  | 4.12 [4.01, 4.22] |
| 90 | 3.89 [3.75, 4.02] | 3.89 [3.75, 4.02] | 3.89 [3.75, 4.02] | 3.88 [3.75, 4.01] |  | 3.88 [3.75, 4.01] |  | 3.88 [3.76, 4.01] | 4.82 [4.67, 4.97] |  |  |  | 3.94 [3.81, 4.07] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.28 [0.18, 0.39] | -0.19 [-0.35, -0.08]* |  | -0.19 [-0.34, -0.08]* | -0.19 [-0.39, -0.05]* | -0.19 [-0.34, -0.08]* | -0.24 [-0.45, -0.09]* | -0.19 [-0.34, -0.08]* | 0.32 [-0.07, 0.71] |  |  | 0.67 [0.48, 0.85] |  | wt_med |
| 1 | 0.23 [0.12, 0.34] | -0.20 [-0.37, -0.09]* | -0.20 [-0.37, -0.09]* | -0.20 [-0.36, -0.09]* | -0.21 [-0.42, -0.06]* | -0.20 [-0.36, -0.09]* | -0.25 [-0.47, -0.10]* | -0.20 [-0.36, -0.08]* | 0.25 [-0.16, 0.66] |  |  | 0.59 [0.41, 0.77] |  | wt_med |
| 2 | 0.21 [0.11, 0.33] | -0.21 [-0.37, -0.09]* | -0.21 [-0.37, -0.09]* | -0.21 [-0.36, -0.09]* | -0.22 [-0.43, -0.06]* | -0.21 [-0.36, -0.09]* | -0.26 [-0.48, -0.10]* | -0.21 [-0.36, -0.09]* | 0.22 [-0.19, 0.62] |  |  | 0.56 [0.38, 0.74] |  | wt_med |
| 3 | 0.20 [0.09, 0.31] | -0.21 [-0.38, -0.09]* | -0.21 [-0.38, -0.09]* | -0.21 [-0.37, -0.09]* | -0.23 [-0.45, -0.07]* | -0.21 [-0.37, -0.09]* | -0.26 [-0.49, -0.11]* | -0.21 [-0.37, -0.09]* | 0.19 [-0.22, 0.60] | 1.66 [1.41, 1.93] | 1.79 [1.55, 2.06] | 0.54 [0.36, 0.71] |  | wt_med |
| 4 | 0.19 [0.08, 0.31] | -0.22 [-0.38, -0.09]* | -0.22 [-0.38, -0.09]* | -0.21 [-0.37, -0.10]* | -0.24 [-0.45, -0.08]* | -0.21 [-0.37, -0.10]* | -0.26 [-0.48, -0.11]* | -0.21 [-0.37, -0.10]* | 0.17 [-0.24, 0.58] | 1.66 [1.43, 1.93] | 1.80 [1.56, 2.06] | 0.52 [0.35, 0.69] |  | wt_med |
| 5 | 0.18 [0.07, 0.31] | -0.21 [-0.38, -0.10]* | -0.21 [-0.38, -0.10]* | -0.21 [-0.37, -0.10]* | -0.24 [-0.45, -0.08]* | -0.21 [-0.37, -0.10]* | -0.26 [-0.48, -0.11]* | -0.21 [-0.37, -0.10]* | 0.15 [-0.26, 0.57] | 1.68 [1.44, 1.94] | 1.82 [1.57, 2.08] | 0.50 [0.33, 0.67] |  | wt_med |
| 6 | 0.16 [0.05, 0.28] | -0.21 [-0.37, -0.10]* | -0.21 [-0.37, -0.10]* | -0.21 [-0.36, -0.10]* | -0.23 [-0.42, -0.08]* | -0.21 [-0.36, -0.10]* | -0.25 [-0.46, -0.11]* | -0.20 [-0.36, -0.10]* | 0.12 [-0.30, 0.54] | 1.62 [1.38, 1.88] | 1.76 [1.52, 2.03] | 0.47 [0.31, 0.64] |  | wt_med |
| 7 | 0.12 [0.01, 0.24] | -0.21 [-0.37, -0.10]* | -0.21 [-0.37, -0.10]* | -0.21 [-0.36, -0.10]* | -0.22 [-0.42, -0.08]* | -0.21 [-0.36, -0.10]* | -0.25 [-0.45, -0.11]* | -0.21 [-0.36, -0.10]* | 0.47 [0.04, 0.89] | 1.64 [1.41, 1.90] | 1.78 [1.55, 2.05] |  |  | wt_med |
| 10 | 0.08 [-0.03, 0.19] | -0.20 [-0.36, -0.09]* | -0.20 [-0.36, -0.09]* | -0.20 [-0.35, -0.09]* | -0.22 [-0.41, -0.08]* | -0.20 [-0.35, -0.09]* | -0.24 [-0.44, -0.10]* | -0.20 [-0.35, -0.09]* | 0.43 [-0.02, 0.84] | 1.64 [1.39, 1.91] | 1.80 [1.54, 2.08] |  |  | wt_med |
| 14 | 0.06 [-0.05, 0.18] | -0.20 [-0.36, -0.10]* | -0.20 [-0.36, -0.10]* | -0.20 [-0.35, -0.10]* | -0.21 [-0.40, -0.07]* | -0.20 [-0.35, -0.10]* | -0.25 [-0.44, -0.11]* | -0.20 [-0.35, -0.10]* | 0.65 [0.19, 1.09] | 1.70 [1.44, 1.99] | 1.88 [1.60, 2.19] |  |  | wt_med |
| 21 | 0.04 [-0.08, 0.16] | -0.20 [-0.36, -0.10]* | -0.20 [-0.36, -0.10]* | -0.21 [-0.35, -0.10]* | -0.23 [-0.42, -0.08]* | -0.21 [-0.35, -0.10]* | -0.25 [-0.44, -0.12]* | -0.21 [-0.35, -0.10]* | 0.76 [0.30, 1.19] | 1.64 [1.38, 1.91] | 1.82 [1.55, 2.13] |  |  | wt_med |
| 30 | 0.05 [-0.07, 0.18] | -0.19 [-0.34, -0.09]* | -0.19 [-0.34, -0.09]* | -0.20 [-0.34, -0.09]* | -0.25 [-0.47, -0.09]* | -0.20 [-0.34, -0.09]* | -0.25 [-0.45, -0.10]* | -0.20 [-0.34, -0.09]* | 0.74 [0.24, 1.20] | 1.71 [1.44, 2.00] | 1.88 [1.60, 2.17] |  |  | wt_med |
| 45 | 0.03 [-0.09, 0.16] | -0.15 [-0.26, -0.07]* | -0.15 [-0.26, -0.07]* | -0.16 [-0.28, -0.07]* | -0.39 [-0.77, -0.04]* | -0.16 [-0.28, -0.07]* | -0.24 [-0.43, -0.10]* | -0.16 [-0.28, -0.07]* | 0.88 [0.38, 1.35] | 1.74 [1.45, 2.03] | 1.91 [1.61, 2.23] |  |  | wt_med |
| 60 |  | -0.06 [-0.27, 0.10] | -0.06 [-0.27, 0.10] | -0.06 [-0.27, 0.09] |  | -0.06 [-0.27, 0.09] | 0.13 [-0.21, 1.21] | -0.06 [-0.27, 0.10] | 1.06 [0.55, 1.56] | 0.59 [0.15, 3.15] | 0.65 [0.23, 3.10] |  | 0.08 [-0.08, 0.25] | clim |
| 90 |  | -0.02 [-0.24, 0.21] | -0.02 [-0.24, 0.21] | -0.02 [-0.22, 0.20] |  | -0.02 [-0.22, 0.20] |  | -0.02 [-0.22, 0.20] | 0.96 [0.46, 1.56] |  |  |  | 0.08 [-0.09, 0.32] | clim |

**first-hour MAE, schedule projected at origin**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 4.83 [4.67, 4.99] | 4.50 [4.36, 4.65] |  | 4.49 [4.35, 4.64] | 4.13 [3.97, 4.33] | 4.48 [4.34, 4.63] | 4.16 [4.01, 4.32] | 4.43 [4.30, 4.58] | 4.29 [4.12, 4.45] |  |  | 5.46 [5.28, 5.64] | 4.50 [4.37, 4.65] |
| 1 | 4.84 [4.69, 5.00] | 4.53 [4.39, 4.68] | 4.53 [4.39, 4.68] | 4.54 [4.40, 4.68] | 4.19 [4.02, 4.38] | 4.53 [4.39, 4.68] | 4.22 [4.06, 4.39] | 4.47 [4.34, 4.62] | 4.30 [4.14, 4.47] |  |  | 5.44 [5.27, 5.62] | 4.56 [4.41, 4.71] |
| 2 | 4.86 [4.71, 5.02] | 4.57 [4.43, 4.72] | 4.57 [4.43, 4.72] | 4.59 [4.44, 4.74] | 4.22 [4.05, 4.41] | 4.57 [4.43, 4.72] | 4.27 [4.11, 4.46] | 4.51 [4.38, 4.66] | 4.32 [4.16, 4.49] |  |  | 5.46 [5.29, 5.64] | 4.60 [4.46, 4.76] |
| 3 | 4.89 [4.74, 5.06] | 4.61 [4.46, 4.76] | 4.61 [4.46, 4.76] | 4.62 [4.48, 4.78] | 4.25 [4.08, 4.45] | 4.61 [4.47, 4.76] | 4.31 [4.15, 4.49] | 4.55 [4.41, 4.70] | 4.35 [4.18, 4.51] | 5.69 [5.52, 5.87] | 5.94 [5.77, 6.12] | 5.47 [5.30, 5.65] | 4.64 [4.50, 4.80] |
| 4 | 4.91 [4.76, 5.06] | 4.65 [4.50, 4.80] | 4.65 [4.50, 4.80] | 4.67 [4.53, 4.83] | 4.31 [4.14, 4.51] | 4.65 [4.51, 4.81] | 4.38 [4.21, 4.56] | 4.59 [4.45, 4.73] | 4.38 [4.22, 4.54] | 5.73 [5.56, 5.91] | 5.97 [5.80, 6.15] | 5.45 [5.29, 5.63] | 4.70 [4.55, 4.85] |
| 5 | 4.98 [4.83, 5.13] | 4.74 [4.60, 4.90] | 4.74 [4.60, 4.90] | 4.77 [4.63, 4.93] | 4.44 [4.24, 4.65] | 4.76 [4.62, 4.91] | 4.46 [4.29, 4.64] | 4.69 [4.55, 4.84] | 4.47 [4.30, 4.64] | 5.85 [5.67, 6.04] | 6.06 [5.89, 6.25] | 5.49 [5.33, 5.67] | 4.80 [4.66, 4.96] |
| 6 | 5.09 [4.94, 5.24] | 4.89 [4.74, 5.04] | 4.89 [4.74, 5.04] | 4.93 [4.77, 5.08] | 4.61 [4.40, 4.85] | 4.92 [4.76, 5.08] | 4.59 [4.41, 4.78] | 4.82 [4.68, 4.97] | 4.55 [4.38, 4.72] | 5.93 [5.75, 6.13] | 6.12 [5.94, 6.31] | 5.54 [5.38, 5.71] | 4.97 [4.81, 5.13] |
| 7 | 5.17 [5.03, 5.32] | 5.02 [4.87, 5.17] | 5.02 [4.87, 5.17] | 5.07 [4.91, 5.23] | 4.79 [4.56, 5.02] | 5.08 [4.92, 5.26] | 4.74 [4.55, 4.93] | 4.95 [4.81, 5.10] | 4.96 [4.78, 5.13] | 6.22 [6.03, 6.42] | 6.42 [6.22, 6.61] |  | 5.13 [4.96, 5.29] |
| 10 | 5.39 [5.25, 5.54] | 5.29 [5.14, 5.45] | 5.29 [5.14, 5.45] | 5.33 [5.18, 5.49] | 5.23 [4.98, 5.50] | 5.38 [5.21, 5.55] | 5.13 [4.91, 5.35] | 5.19 [5.05, 5.34] | 5.13 [4.94, 5.30] | 6.64 [6.43, 6.86] | 6.78 [6.58, 6.99] |  | 5.40 [5.24, 5.56] |
| 14 | 5.58 [5.43, 5.74] | 5.56 [5.40, 5.72] | 5.56 [5.40, 5.72] | 5.59 [5.43, 5.74] | 5.65 [5.39, 5.92] | 5.65 [5.48, 5.82] | 5.55 [5.31, 5.78] | 5.46 [5.32, 5.61] | 5.48 [5.31, 5.65] | 6.95 [6.71, 7.19] | 7.07 [6.85, 7.30] |  | 5.66 [5.50, 5.82] |
| 21 | 5.74 [5.59, 5.90] | 5.75 [5.59, 5.92] | 5.75 [5.59, 5.92] | 5.75 [5.59, 5.91] | 5.95 [5.69, 6.22] | 5.83 [5.65, 6.00] | 5.80 [5.58, 6.03] | 5.63 [5.47, 5.78] | 5.78 [5.61, 5.96] | 7.44 [7.22, 7.68] | 7.60 [7.40, 7.83] |  | 5.81 [5.65, 5.98] |
| 30 | 5.66 [5.52, 5.81] | 5.77 [5.62, 5.92] | 5.77 [5.62, 5.92] | 5.76 [5.61, 5.92] | 5.99 [5.75, 6.25] | 5.85 [5.69, 6.02] | 6.17 [5.91, 6.45] | 5.61 [5.47, 5.76] | 6.07 [5.90, 6.24] | 7.84 [7.60, 8.11] | 7.97 [7.74, 8.24] |  | 5.82 [5.66, 5.98] |
| 45 | 5.90 [5.76, 6.06] | 5.97 [5.83, 6.13] | 5.97 [5.83, 6.13] | 5.99 [5.85, 6.14] | 4.46 [4.25, 4.69] | 6.06 [5.92, 6.21] | 6.40 [6.14, 6.69] | 5.86 [5.73, 6.01] | 6.32 [6.15, 6.49] | 8.33 [8.06, 8.65] | 8.48 [8.21, 8.78] |  | 6.07 [5.93, 6.23] |
| 60 | 6.08 [5.93, 6.24] | 6.02 [5.88, 6.17] | 6.02 [5.88, 6.17] | 6.02 [5.87, 6.16] |  | 6.08 [5.93, 6.23] | 2.60 [1.23, 3.62] | 5.91 [5.77, 6.06] | 6.34 [6.19, 6.50] | 3.27 [1.62, 4.38] | 3.34 [1.69, 4.45] |  | 6.11 [5.97, 6.26] |
| 90 | 6.08 [5.93, 6.24] | 6.02 [5.88, 6.17] | 6.02 [5.88, 6.17] | 5.94 [5.80, 6.09] |  | 6.04 [5.91, 6.20] |  | 5.92 [5.78, 6.06] | 6.32 [6.17, 6.46] |  |  |  | 6.00 [5.86, 6.15] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.30 [0.20, 0.43] | -0.04 [-0.10, 0.00] |  | -0.02 [-0.04, 0.00] | 0.01 [-0.02, 0.03] | -0.01 [-0.04, 0.00] | -0.02 [-0.08, 0.03] | -0.07 [-0.18, -0.01]* | -0.25 [-0.61, 0.24] |  |  | 0.84 [0.47, 1.24] |  | wt_med |
| 1 | 0.27 [0.18, 0.40] | -0.05 [-0.12, -0.00]* | -0.05 [-0.12, -0.00]* | -0.02 [-0.04, -0.00]* | -0.01 [-0.08, 0.02] | -0.02 [-0.05, -0.00]* | -0.01 [-0.08, 0.03] | -0.08 [-0.21, -0.01]* | -0.28 [-0.63, 0.21] |  |  | 0.77 [0.37, 1.21] |  | wt_med |
| 2 | 0.25 [0.16, 0.38] | -0.04 [-0.12, -0.00]* | -0.04 [-0.12, -0.00]* | -0.02 [-0.04, -0.00]* | -0.01 [-0.07, 0.02] | -0.02 [-0.05, 0.00] | -0.00 [-0.06, 0.04] | -0.08 [-0.22, -0.02]* | -0.30 [-0.65, 0.21] |  |  | 0.75 [0.34, 1.17] |  | wt_med |
| 3 | 0.25 [0.15, 0.39] | -0.05 [-0.13, -0.00]* | -0.05 [-0.13, -0.00]* | -0.02 [-0.05, -0.00]* | -0.01 [-0.07, 0.02] | -0.03 [-0.07, -0.00]* | -0.00 [-0.06, 0.03] | -0.09 [-0.23, -0.02]* | -0.32 [-0.68, 0.18] | 1.38 [0.83, 1.95] | 1.63 [1.01, 2.21] | 0.73 [0.33, 1.13] |  | wt_med |
| 4 | 0.25 [0.13, 0.40] | -0.06 [-0.16, -0.01]* | -0.06 [-0.16, -0.01]* | -0.03 [-0.06, -0.00]* | -0.02 [-0.10, 0.02] | -0.04 [-0.09, -0.01]* | -0.01 [-0.07, 0.03] | -0.11 [-0.27, -0.03]* | -0.34 [-0.72, 0.14] | 1.35 [0.80, 1.89] | 1.59 [0.97, 2.14] | 0.65 [0.25, 1.02] |  | wt_med |
| 5 | 0.21 [0.07, 0.37] | -0.07 [-0.19, -0.01]* | -0.07 [-0.19, -0.01]* | -0.03 [-0.08, -0.01]* | -0.03 [-0.14, 0.02] | -0.04 [-0.09, -0.01]* | -0.04 [-0.14, 0.02] | -0.12 [-0.28, -0.03]* | -0.36 [-0.74, 0.10] | 1.37 [0.86, 1.90] | 1.58 [1.02, 2.12] | 0.58 [0.19, 0.94] |  | wt_med |
| 6 | 0.16 [-0.02, 0.34] | -0.09 [-0.21, -0.02]* | -0.09 [-0.21, -0.02]* | -0.05 [-0.11, -0.01]* | -0.07 [-0.27, 0.02] | -0.04 [-0.10, -0.01]* | -0.08 [-0.28, 0.03] | -0.15 [-0.34, -0.04]* | -0.43 [-0.84, 0.04] | 1.28 [0.81, 1.79] | 1.47 [0.91, 2.00] | 0.46 [0.01, 0.83] |  | wt_med |
| 7 | 0.10 [-0.11, 0.30] | -0.08 [-0.18, -0.02]* | -0.08 [-0.18, -0.02]* | -0.06 [-0.14, -0.02]* | -0.10 [-0.37, 0.03] | -0.01 [-0.06, 0.04] | -0.11 [-0.38, 0.02] | -0.18 [-0.40, -0.05]* | -0.14 [-0.58, 0.43] | 1.40 [0.91, 1.88] | 1.60 [1.02, 2.14] |  |  | wt_med |
| 10 |  | -0.11 [-0.32, 0.11] | -0.11 [-0.32, 0.11] | -0.11 [-0.29, 0.09] | -0.11 [-0.43, 0.20] | -0.03 [-0.29, 0.30] | -0.15 [-0.40, 0.11] | -0.20 [-0.39, -0.00]* | -0.31 [-0.80, 0.28] | 1.37 [0.84, 1.96] | 1.51 [0.99, 1.99] |  | -0.03 [-0.26, 0.23] | clim |
| 14 |  | -0.03 [-0.25, 0.19] | -0.03 [-0.25, 0.19] | -0.02 [-0.22, 0.17] | 0.01 [-0.34, 0.36] | 0.07 [-0.20, 0.41] | 0.03 [-0.28, 0.38] | -0.12 [-0.31, 0.08] | -0.14 [-0.64, 0.44] | 1.44 [0.96, 2.00] | 1.56 [1.08, 2.06] |  | 0.04 [-0.19, 0.30] | clim |
| 21 |  | 0.01 [-0.21, 0.22] | 0.01 [-0.21, 0.22] | -0.00 [-0.20, 0.19] | 0.08 [-0.30, 0.45] | 0.10 [-0.17, 0.43] | 0.14 [-0.24, 0.54] | -0.09 [-0.30, 0.10] | -0.03 [-0.56, 0.62] | 1.75 [1.31, 2.34] | 1.91 [1.49, 2.43] |  | 0.06 [-0.17, 0.32] | clim |
| 30 |  | 0.07 [-0.15, 0.28] | 0.07 [-0.15, 0.28] | 0.06 [-0.14, 0.26] | 0.09 [-0.22, 0.41] | 0.17 [-0.09, 0.49] | 0.33 [-0.09, 0.89] | -0.06 [-0.28, 0.13] | 0.32 [-0.30, 1.06] | 2.02 [1.58, 2.58] | 2.15 [1.73, 2.70] |  | 0.13 [-0.10, 0.38] | clim |
| 45 |  | 0.02 [-0.20, 0.21] | 0.02 [-0.20, 0.21] | 0.05 [-0.18, 0.26] | -0.57 [-1.19, -0.19]* | 0.13 [-0.13, 0.37] | 0.22 [-0.22, 0.69] | -0.07 [-0.31, 0.16] | 0.32 [-0.38, 1.03] | 2.22 [1.70, 2.84] | 2.36 [1.86, 2.94] |  | 0.13 [-0.11, 0.37] | clim |
| 60 |  | -0.12 [-0.40, 0.13] | -0.12 [-0.40, 0.13] | -0.09 [-0.35, 0.15] |  | -0.02 [-0.29, 0.25] | -0.10 [-0.24, 1.09] | -0.18 [-0.46, 0.07] | 0.16 [-0.66, 1.01] | 0.54 [0.00, 2.81] | 0.61 [0.00, 5.47] |  | 0.01 [-0.25, 0.24] | clim |
| 90 | 0.12 [-0.13, 0.43] | -0.04 [-0.15, 0.08] | -0.04 [-0.15, 0.08] | -0.08 [-0.16, -0.03]* |  | 0.00 [-0.08, 0.10] |  | -0.10 [-0.24, 0.06] | 0.27 [-0.46, 1.01] |  |  |  |  | wt_med |

**rope-drop worth agreement**

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] |  | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 1 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 2 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] |  |  | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 3 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 4 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 5 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 6 | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.92] |
| 7 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 10 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 14 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 21 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.92] |
| 30 | 0.91 [0.91, 0.92] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.89, 0.90] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.92] |
| 45 | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] | 0.87 [0.86, 0.87] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.92] |
| 60 | 0.91 [0.91, 0.92] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.92] |  | 0.91 [0.91, 0.91] | 0.99 [0.99, 1.00] | 0.90 [0.90, 0.90] | 0.99 [0.99, 1.00] | 0.99 [0.99, 1.00] |  | 0.91 [0.91, 0.92] |
| 90 | 0.92 [0.91, 0.92] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.92 [0.91, 0.92] |  | 0.91 [0.91, 0.92] |  | 0.90 [0.90, 0.91] |  |  |  | 0.91 [0.91, 0.92] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | prod_served_lin | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.00] |  | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, 0.00] |  |  | -0.01 [-0.01, -0.01] |  | wt_med |
| 1 | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] |  |  | -0.01 [-0.01, -0.01] |  | wt_med |
| 2 | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.02, 0.01] | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* |  |  |  | 0.01 [0.01, 0.01]* | snaive7 |
| 3 | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.02, 0.01] | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  | 0.01 [0.01, 0.01]* | snaive7 |
| 4 | 0.01 [0.01, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.02, 0.01] | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  | 0.01 [0.01, 0.01]* | snaive7 |
| 5 | 0.01 [0.01, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.01, 0.01] | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  | 0.01 [0.01, 0.01]* | snaive7 |
| 6 | 0.01 [0.01, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | -0.00 [-0.02, 0.01] | 0.01 [0.00, 0.01]* | 0.01 [0.00, 0.01]* | 0.00 [0.00, 0.01]* | -0.00 [-0.01, 0.01] | -0.00 [-0.01, 0.01] |  | 0.01 [0.01, 0.01]* | snaive7 |
| 7 | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  |  | wt_med |
| 10 | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.02, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  |  | wt_med |
| 14 | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  |  | wt_med |
| 21 | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.02, -0.00] |  |  | wt_med |
| 30 | 0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, 0.00] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.02, -0.00] | -0.01 [-0.02, -0.00] |  |  | wt_med |
| 45 | -0.00 [-0.00, 0.00] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] | -0.01 [-0.02, 0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.02, -0.01] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.00] |  |  | wt_med |
| 60 | -0.00 [-0.00, 0.00] | -0.00 [-0.01, 0.00] | -0.00 [-0.01, 0.00] | 0.00 [-0.00, 0.00] |  | -0.00 [-0.00, -0.00] | -0.00 [-0.02, 0.00] | -0.01 [-0.02, -0.01] | 0.00 [-0.01, 0.01] | 0.00 [-0.01, 0.01] |  |  | wt_med |
| 90 |  | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | 0.00 [-0.00, 0.00] |  | -0.00 [-0.01, 0.00] |  | -0.01 [-0.02, -0.01] |  |  |  | 0.00 [-0.00, 0.00] | clim |

## D6 — crowd bucket per park-day

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder; paired per (origin, park-day).

**crowd bucket exact**

| lead | lvl_cbd | lvl_clim | lvl_driver_level | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|
| 1 | 0.20 [0.20, 0.21] | 0.27 [0.27, 0.28] | 0.38 [0.37, 0.38] | 0.37 [0.36, 0.37] | 0.40 [0.39, 0.41] | 0.40 [0.39, 0.40] | 0.32 [0.31, 0.32] |
| 2 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.38 [0.37, 0.38] | 0.37 [0.36, 0.37] | 0.40 [0.39, 0.41] | 0.38 [0.37, 0.39] | 0.31 [0.31, 0.32] |
| 3 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.37 [0.37, 0.38] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.38 [0.37, 0.39] | 0.31 [0.30, 0.32] |
| 4 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.37 [0.37, 0.38] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.37 [0.37, 0.38] | 0.31 [0.30, 0.32] |
| 5 | 0.21 [0.20, 0.22] | 0.27 [0.26, 0.28] | 0.38 [0.37, 0.38] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.37 [0.36, 0.37] | 0.31 [0.30, 0.32] |
| 6 | 0.19 [0.18, 0.20] | 0.27 [0.26, 0.27] | 0.37 [0.36, 0.38] | 0.37 [0.36, 0.38] | 0.40 [0.39, 0.41] | 0.36 [0.35, 0.37] | 0.31 [0.30, 0.31] |
| 7 | 0.19 [0.18, 0.20] | 0.26 [0.26, 0.27] | 0.35 [0.34, 0.35] | 0.34 [0.33, 0.34] |  | 0.34 [0.33, 0.35] | 0.30 [0.29, 0.30] |
| 10 | 0.19 [0.18, 0.20] | 0.26 [0.25, 0.27] | 0.34 [0.33, 0.35] | 0.34 [0.33, 0.35] |  | 0.33 [0.33, 0.34] | 0.29 [0.29, 0.30] |
| 14 | 0.18 [0.17, 0.19] | 0.26 [0.25, 0.27] | 0.33 [0.32, 0.33] | 0.31 [0.30, 0.31] |  | 0.31 [0.31, 0.32] | 0.28 [0.28, 0.29] |
| 21 | 0.18 [0.17, 0.18] | 0.25 [0.25, 0.26] | 0.32 [0.31, 0.32] | 0.29 [0.28, 0.30] |  | 0.29 [0.28, 0.30] | 0.27 [0.26, 0.28] |
| 30 | 0.17 [0.16, 0.18] | 0.25 [0.24, 0.26] | 0.31 [0.30, 0.32] | 0.28 [0.27, 0.29] |  | 0.27 [0.26, 0.28] | 0.27 [0.26, 0.27] |
| 45 | 0.23 [0.22, 0.25] | 0.25 [0.24, 0.25] | 0.30 [0.30, 0.31] | 0.27 [0.26, 0.28] |  | 0.26 [0.25, 0.27] | 0.26 [0.25, 0.27] |
| 60 |  | 0.24 [0.23, 0.24] | 0.30 [0.30, 0.31] | 0.27 [0.26, 0.28] |  | 0.21 [0.11, 0.31] | 0.26 [0.25, 0.27] |
| 90 |  | 0.23 [0.22, 0.24] | 0.30 [0.29, 0.32] | 0.28 [0.27, 0.29] |  |  | 0.26 [0.25, 0.27] |
| 120 |  | 0.22 [0.21, 0.23] | 0.24 [0.23, 0.25] | 0.28 [0.26, 0.29] |  |  | 0.25 [0.23, 0.26] |
| 180 |  | 0.21 [0.19, 0.23] | 0.24 [0.22, 0.26] | 0.26 [0.24, 0.28] |  |  | 0.24 [0.22, 0.26] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_driver_level | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|
| 1 | -0.20 [-0.23, -0.17] | -0.12 [-0.15, -0.10] | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.00 [-0.02, 0.01] | -0.08 [-0.10, -0.06] | lvl_snaive7 |
| 2 | -0.20 [-0.23, -0.17] | -0.12 [-0.15, -0.10] | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.02 [-0.03, -0.00] | -0.08 [-0.10, -0.06] | lvl_snaive7 |
| 3 | -0.20 [-0.23, -0.17] | -0.13 [-0.15, -0.10] | -0.02 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.02 [-0.04, -0.01] | -0.08 [-0.10, -0.06] | lvl_snaive7 |
| 4 | -0.20 [-0.23, -0.17] | -0.13 [-0.15, -0.10] | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.03 [-0.04, -0.01] | -0.08 [-0.10, -0.07] | lvl_snaive7 |
| 5 | -0.20 [-0.23, -0.17] | -0.13 [-0.15, -0.11] | -0.02 [-0.04, -0.01] | -0.02 [-0.04, -0.01] | -0.04 [-0.05, -0.02] | -0.09 [-0.11, -0.07] | lvl_snaive7 |
| 6 | -0.21 [-0.25, -0.18] | -0.13 [-0.16, -0.11] | -0.03 [-0.04, -0.01] | -0.02 [-0.04, -0.01] | -0.04 [-0.06, -0.03] | -0.09 [-0.11, -0.07] | lvl_snaive7 |
| 7 | -0.16 [-0.18, -0.13] | -0.07 [-0.09, -0.05] | 0.01 [-0.00, 0.02] |  | 0.00 [-0.01, 0.02] | -0.04 [-0.05, -0.03] | lvl_naive4 |
| 10 | -0.15 [-0.18, -0.13] | -0.08 [-0.10, -0.06] | 0.00 [-0.01, 0.02] |  | -0.01 [-0.02, 0.01] | -0.05 [-0.06, -0.03] | lvl_naive4 |
| 14 | -0.12 [-0.15, -0.10] | -0.05 [-0.07, -0.03] | 0.02 [0.00, 0.03]* |  | 0.01 [-0.01, 0.02] | -0.02 [-0.03, -0.01] | lvl_naive4 |
| 21 | -0.11 [-0.14, -0.08] | -0.04 [-0.06, -0.02] | 0.03 [0.01, 0.04]* |  | 0.01 [-0.01, 0.02] | -0.02 [-0.03, -0.01] | lvl_naive4 |
| 30 | -0.10 [-0.13, -0.07] | -0.03 [-0.05, -0.01] | 0.03 [0.01, 0.05]* |  | 0.00 [-0.02, 0.02] | -0.01 [-0.02, -0.00] | lvl_naive4 |
| 45 | -0.07 [-0.12, -0.02] | -0.02 [-0.05, -0.00] | 0.03 [0.01, 0.05]* |  | 0.00 [-0.02, 0.02] | -0.01 [-0.02, 0.00] | lvl_naive4 |
| 60 |  | -0.03 [-0.05, -0.01] | 0.04 [0.01, 0.06]* |  | 0.05 [-0.10, 0.14] | -0.01 [-0.02, 0.01] | lvl_naive4 |
| 90 |  | -0.05 [-0.07, -0.02] | 0.02 [0.00, 0.04]* |  |  | -0.02 [-0.03, -0.00] | lvl_naive4 |
| 120 |  | -0.06 [-0.08, -0.03] | -0.03 [-0.05, -0.02] |  |  | -0.03 [-0.05, -0.01] | lvl_naive4 |
| 180 |  | -0.05 [-0.10, 0.00] | -0.02 [-0.05, 0.00] |  |  | -0.02 [-0.06, 0.01] | lvl_naive4 |

| lead | model | park_days | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|
| 1 | lvl_naive4 | 15663 | 0.77 | 0.61 | 0.63 |
| 1 | lvl_wt56 | 15812 | 0.75 | 0.50 | 0.62 |
| 1 | lvl_snaive7 | 15240 | 0.78 | 0.66 | 0.65 |
| 1 | lvl_clim | 15812 | 0.70 | 0.38 | 0.60 |
| 1 | lvl_tft | 11511 | 0.81 | 0.65 | 0.69 |
| 1 | lvl_cbd | 8757 | 0.58 | 0.15 | 0.68 |
| 1 | lvl_driver_level | 15663 | 0.79 | 0.57 | 0.71 |
| 7 | lvl_naive4 | 14958 | 0.73 | 0.57 | 0.59 |
| 7 | lvl_wt56 | 15157 | 0.72 | 0.48 | 0.59 |
| 7 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 7 | lvl_clim | 15155 | 0.69 | 0.37 | 0.59 |
| 7 | lvl_tft | 10824 | 0.76 | 0.59 | 0.64 |
| 7 | lvl_cbd | 8380 | 0.52 | 0.11 | 0.58 |
| 7 | lvl_driver_level | 14958 | 0.76 | 0.52 | 0.68 |
| 30 | lvl_naive4 | 12695 | 0.67 | 0.52 | 0.53 |
| 30 | lvl_wt56 | 12935 | 0.67 | 0.42 | 0.54 |
| 30 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 30 | lvl_clim | 12928 | 0.68 | 0.36 | 0.59 |
| 30 | lvl_tft | 7196 | 0.66 | 0.57 | 0.55 |
| 30 | lvl_cbd | 6517 | 0.49 | 0.10 | 0.56 |
| 30 | lvl_driver_level | 12695 | 0.71 | 0.48 | 0.64 |
| 90 | lvl_naive4 | 7373 | 0.68 | 0.51 | 0.55 |
| 90 | lvl_wt56 | 7516 | 0.70 | 0.40 | 0.58 |
| 90 | lvl_snaive7 | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_clim | 7522 | 0.67 | 0.36 | 0.59 |
| 90 | lvl_tft | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_cbd | 0 | 0.00 | 0.00 | 0.00 |
| 90 | lvl_driver_level | 7373 | 0.70 | 0.49 | 0.57 |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 264430 | 167732 | 0.63 |
| 1 | lvl_clim | 518286 | 315421 | 0.61 |
| 1 | lvl_driver_level | 510202 | 370499 | 0.73 |
| 1 | lvl_naive4 | 510202 | 358132 | 0.70 |
| 1 | lvl_snaive7 | 481216 | 349823 | 0.73 |
| 1 | lvl_tft | 422992 | 309474 | 0.73 |
| 1 | lvl_wt56 | 518286 | 347917 | 0.67 |
| 7 | lvl_cbd | 255948 | 144318 | 0.56 |
| 7 | lvl_clim | 491060 | 290028 | 0.59 |
| 7 | lvl_driver_level | 480665 | 336549 | 0.70 |
| 7 | lvl_naive4 | 480665 | 321110 | 0.67 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 394632 | 269245 | 0.68 |
| 7 | lvl_wt56 | 491180 | 318394 | 0.65 |
| 30 | lvl_cbd | 192437 | 106259 | 0.55 |
| 30 | lvl_clim | 398256 | 224344 | 0.56 |
| 30 | lvl_driver_level | 386101 | 255052 | 0.66 |
| 30 | lvl_naive4 | 386101 | 236674 | 0.61 |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 257158 | 161278 | 0.63 |
| 30 | lvl_wt56 | 398676 | 235638 | 0.59 |
| 90 | lvl_cbd | 0 | 0 |  |
| 90 | lvl_clim | 187545 | 107128 | 0.57 |
| 90 | lvl_driver_level | 181049 | 116711 | 0.64 |
| 90 | lvl_naive4 | 181049 | 109197 | 0.60 |
| 90 | lvl_snaive7 | 0 | 0 |  |
| 90 | lvl_tft | 0 | 0 |  |
| 90 | lvl_wt56 | 187238 | 111572 | 0.60 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5, paired on the same day pairs; a predicted gap < 0.1 is a tie (counted wrong); CI over (origin, park).

**day comparison winner accuracy**

| lead | lvl_cbd | lvl_clim | lvl_driver_level | lvl_naive4 | lvl_snaive7 | lvl_tft | lvl_wt56 |
|---|---|---|---|---|---|---|---|
| d1-7 | 0.23 [0.23, 0.24] | 0.27 [0.26, 0.27] | 0.47 [0.46, 0.47] | 0.45 [0.44, 0.45] | 0.48 [0.47, 0.48] | 0.41 [0.41, 0.42] | 0.27 [0.27, 0.28] |
| d8-30 | 0.16 [0.16, 0.16] | 0.24 [0.24, 0.24] | 0.43 [0.43, 0.43] | 0.36 [0.36, 0.36] |  | 0.36 [0.36, 0.37] | 0.23 [0.22, 0.23] |
| d31-90 | 0.14 [0.14, 0.15] | 0.24 [0.24, 0.24] | 0.43 [0.43, 0.44] | 0.34 [0.33, 0.34] |  | 0.35 [0.35, 0.36] | 0.22 [0.22, 0.22] |

paired difference vs reference (park-cluster CI; `*` = wins):

| lead | lvl_cbd | lvl_clim | lvl_driver_level | lvl_naive4 | lvl_tft | lvl_wt56 | ref |
|---|---|---|---|---|---|---|---|
| d1-7 | -0.23 [-0.25, -0.21] | -0.21 [-0.23, -0.18] | -0.01 [-0.02, 0.01] | -0.02 [-0.04, -0.01] | -0.06 [-0.08, -0.05] | -0.20 [-0.23, -0.18] | lvl_snaive7 |
| d8-30 | -0.18 [-0.20, -0.16] | -0.12 [-0.14, -0.10] | 0.07 [0.05, 0.08]* |  | -0.00 [-0.02, 0.01] | -0.13 [-0.15, -0.12] | lvl_naive4 |
| d31-90 | -0.17 [-0.19, -0.15] | -0.09 [-0.11, -0.08] | 0.09 [0.08, 0.11]* |  | 0.03 [0.00, 0.05]* | -0.12 [-0.13, -0.11] | lvl_naive4 |

| lead | model | tie_rate |
|---|---|---|
| d1-7 | lvl_naive4 | 0.37 |
| d1-7 | lvl_wt56 | 0.61 |
| d1-7 | lvl_snaive7 | 0.30 |
| d1-7 | lvl_clim | 0.61 |
| d1-7 | lvl_tft | 0.42 |
| d1-7 | lvl_cbd | 0.65 |
| d1-7 | lvl_driver_level | 0.36 |
| d8-30 | lvl_naive4 | 0.44 |
| d8-30 | lvl_wt56 | 0.65 |
| d8-30 | lvl_clim | 0.64 |
| d8-30 | lvl_tft | 0.43 |
| d8-30 | lvl_cbd | 0.73 |
| d8-30 | lvl_driver_level | 0.39 |
| d31-90 | lvl_naive4 | 0.44 |
| d31-90 | lvl_wt56 | 0.65 |
| d31-90 | lvl_clim | 0.61 |
| d31-90 | lvl_tft | 0.44 |
| d31-90 | lvl_cbd | 0.75 |
| d31-90 | lvl_driver_level | 0.38 |

Calendar star (rank ≤ month median − 0.5 among ≥ 4 days of the month ~30 days ahead, origins on the 1st and 15th):

| model | star_precision | star_recall | pred_stars | true_stars |
|---|---|---|---|---|
| lvl_cbd | 0.32 [0.27, 0.36] | 0.08 [0.07, 0.10] | 900 | 3442 |
| lvl_clim | 0.37 [0.34, 0.41] | 0.08 [0.07, 0.10] | 1707 | 7538 |
| lvl_driver_level | 0.44 [0.42, 0.46] | 0.34 [0.32, 0.36] | 5660 | 7454 |
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
| driver_level | 0 | 7.78 | 7.91 | 0.98 |
| driver_level | 1 | 7.81 | 7.95 | 0.98 |
| driver_level | 3 | 7.84 | 8.00 | 0.98 |
| driver_level | 7 | 8.07 | 8.31 | 0.97 |
| driver_level | 14 | 8.26 | 8.51 | 0.97 |
| driver_level | 30 | 8.47 | 9.04 | 0.94 |
| driver_level | 60 | 8.59 | 9.29 | 0.92 |
| driver_level | 90 | 8.12 | 10.74 | 0.76 |
| driver_level_x_h5 | 1 | 7.81 | 7.95 | 0.98 |
| driver_level_x_h5 | 3 | 7.84 | 8.00 | 0.98 |
| driver_level_x_h5 | 7 | 8.07 | 8.31 | 0.97 |
| driver_level_x_h5 | 14 | 8.26 | 8.51 | 0.97 |
| driver_level_x_h5 | 30 | 8.47 | 9.04 | 0.94 |
| driver_level_x_h5 | 60 | 8.59 | 9.29 | 0.92 |
| driver_level_x_h5 | 90 | 8.12 | 10.74 | 0.76 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | clim | driver_level | driver_level_x_h5 | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | prod_served_lin | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.89 | 0.92 |  | 0.97 | 0.54 | 0.92 | 0.67 | 0.97 | 0.92 |  |  | 0.82 | 0.96 |
| 1 | 0.89 | 0.92 | 0.92 | 0.97 | 0.53 | 0.92 | 0.67 | 0.97 | 0.92 |  |  | 0.83 | 0.96 |
| 2 | 0.89 | 0.92 | 0.92 | 0.97 | 0.53 | 0.93 | 0.67 | 0.97 | 0.92 |  |  | 0.83 | 0.96 |
| 3 | 0.89 | 0.93 | 0.93 | 0.97 | 0.53 | 0.93 | 0.66 | 0.96 | 0.93 | 0.65 | 0.65 | 0.83 | 0.96 |
| 4 | 0.89 | 0.93 | 0.93 | 0.97 | 0.53 | 0.93 | 0.66 | 0.96 | 0.93 | 0.65 | 0.65 | 0.83 | 0.96 |
| 5 | 0.88 | 0.93 | 0.93 | 0.97 | 0.53 | 0.93 | 0.66 | 0.96 | 0.93 | 0.64 | 0.64 | 0.84 | 0.96 |
| 6 | 0.88 | 0.93 | 0.93 | 0.97 | 0.53 | 0.93 | 0.65 | 0.96 | 0.93 | 0.64 | 0.64 | 0.84 | 0.96 |
| 7 | 0.88 | 0.91 | 0.91 | 0.96 | 0.52 | 0.91 | 0.65 | 0.96 | 0.91 | 0.64 | 0.64 |  | 0.95 |
| 10 | 0.88 | 0.91 | 0.91 | 0.96 | 0.52 | 0.92 | 0.64 | 0.96 | 0.92 | 0.63 | 0.63 |  | 0.95 |
| 14 | 0.88 | 0.89 | 0.89 | 0.96 | 0.50 | 0.90 | 0.63 | 0.95 | 0.90 | 0.62 | 0.62 |  | 0.94 |
| 21 | 0.87 | 0.88 | 0.88 | 0.95 | 0.48 | 0.89 | 0.61 | 0.95 | 0.89 | 0.60 | 0.60 |  | 0.94 |
| 30 | 0.86 | 0.88 | 0.88 | 0.94 | 0.45 | 0.88 | 0.48 | 0.94 | 0.88 | 0.51 | 0.51 |  | 0.93 |
| 45 | 0.84 | 0.86 | 0.86 | 0.93 | 0.13 | 0.86 | 0.35 | 0.93 | 0.86 | 0.38 | 0.38 |  | 0.91 |
| 60 | 0.82 | 0.84 | 0.84 | 0.91 |  | 0.84 | 0.00 | 0.91 | 0.85 | 0.00 | 0.00 |  | 0.89 |
| 90 | 0.77 | 0.80 | 0.80 | 0.88 |  | 0.81 |  | 0.88 | 0.81 |  |  |  | 0.86 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | lvl_driver_level | n_ride_days |
|---|---|---|---|---|---|---|---|---|
| 1 | 0.93 | 0.98 | 0.91 | 0.92 | 0.69 | 0.53 | 0.93 | 145986 |
| 2 | 0.93 | 0.98 | 0.91 | 0.92 | 0.68 | 0.53 | 0.93 | 144715 |
| 3 | 0.93 | 0.98 | 0.91 | 0.92 | 0.68 | 0.54 | 0.93 | 143994 |
| 4 | 0.94 | 0.98 | 0.92 | 0.92 | 0.68 | 0.54 | 0.94 | 143309 |
| 5 | 0.94 | 0.98 | 0.92 | 0.91 | 0.68 | 0.53 | 0.94 | 142638 |
| 6 | 0.94 | 0.98 | 0.92 | 0.91 | 0.67 | 0.53 | 0.94 | 141930 |
| 7 | 0.91 | 0.98 | 0.00 | 0.91 | 0.67 | 0.53 | 0.91 | 141084 |
| 10 | 0.92 | 0.98 | 0.00 | 0.92 | 0.66 | 0.52 | 0.92 | 138660 |
| 14 | 0.91 | 0.98 | 0.00 | 0.91 | 0.65 | 0.51 | 0.91 | 135709 |
| 21 | 0.90 | 0.97 | 0.00 | 0.91 | 0.63 | 0.48 | 0.90 | 130683 |
| 30 | 0.90 | 0.97 | 0.00 | 0.91 | 0.50 | 0.46 | 0.90 | 123609 |
| 45 | 0.89 | 0.97 | 0.00 | 0.90 | 0.37 | 0.15 | 0.89 | 113174 |
| 60 | 0.87 | 0.96 | 0.00 | 0.89 | 0.00 | 0.00 | 0.87 | 102285 |
| 90 | 0.86 | 0.96 | 0.00 | 0.87 | 0.00 | 0.00 | 0.86 | 79830 |
| 120 | 0.82 | 0.94 | 0.00 | 0.85 | 0.00 | 0.00 | 0.82 | 57445 |
| 180 | 0.79 | 0.91 | 0.00 | 0.87 | 0.00 | 0.00 | 0.79 | 19826 |

Forecast present vs ride operated. The baselines have **no openness logic** — they forecast whenever their profile exists — so `P(fc|not operated)` only shows how often a ride that never opened still got a curve; it is the bar a model with an open/closed decision has to beat:

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | prod_served_lin P(fc|operated) | prod_served_lin P(fc|not operated) | driver_level P(fc|operated) | driver_level P(fc|not operated) | driver_level_x_h5 P(fc|operated) | driver_level_x_h5 P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.81 | 0.25 | 0.94 | 0.75 | 0.86 | 0.71 | 0.94 | 0.76 | 0.88 | 0.65 | 0.66 | 0.52 | 0.50 | 0.34 | 0.00 | 0.00 | 0.00 | 0.00 | 0.83 | 0.54 | 0.00 | 0.00 | 435371.00 | 34758.00 |
| 1 | 0.81 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.88 | 0.65 | 0.65 | 0.52 | 0.50 | 0.34 | 0.00 | 0.00 | 0.00 | 0.00 | 0.83 | 0.54 | 0.83 | 0.54 | 432343.00 | 34647.00 |
| 2 | 0.81 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.88 | 0.65 | 0.65 | 0.52 | 0.50 | 0.34 | 0.00 | 0.00 | 0.00 | 0.00 | 0.84 | 0.55 | 0.84 | 0.55 | 429640.00 | 34483.00 |
| 3 | 0.82 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.51 | 0.50 | 0.34 | 0.61 | 0.46 | 0.61 | 0.46 | 0.84 | 0.55 | 0.84 | 0.55 | 427074.00 | 34312.00 |
| 4 | 0.82 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.51 | 0.50 | 0.35 | 0.60 | 0.46 | 0.60 | 0.46 | 0.85 | 0.55 | 0.85 | 0.55 | 424518.00 | 34130.00 |
| 5 | 0.82 | 0.26 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.51 | 0.50 | 0.34 | 0.60 | 0.46 | 0.60 | 0.46 | 0.85 | 0.55 | 0.85 | 0.55 | 422064.00 | 34014.00 |
| 6 | 0.83 | 0.26 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.64 | 0.51 | 0.50 | 0.34 | 0.60 | 0.45 | 0.60 | 0.45 | 0.85 | 0.55 | 0.85 | 0.55 | 419640.00 | 33894.00 |
| 7 | 0.00 | 0.00 | 0.93 | 0.76 | 0.86 | 0.70 | 0.94 | 0.77 | 0.88 | 0.66 | 0.64 | 0.51 | 0.50 | 0.34 | 0.60 | 0.45 | 0.60 | 0.45 | 0.83 | 0.56 | 0.83 | 0.56 | 416862.00 | 33811.00 |
| 10 | 0.00 | 0.00 | 0.93 | 0.76 | 0.86 | 0.70 | 0.94 | 0.77 | 0.89 | 0.66 | 0.64 | 0.51 | 0.49 | 0.34 | 0.59 | 0.45 | 0.59 | 0.45 | 0.84 | 0.56 | 0.84 | 0.56 | 408905.00 | 33427.00 |
| 14 | 0.00 | 0.00 | 0.93 | 0.77 | 0.86 | 0.70 | 0.94 | 0.78 | 0.88 | 0.67 | 0.63 | 0.49 | 0.48 | 0.33 | 0.58 | 0.44 | 0.58 | 0.44 | 0.82 | 0.57 | 0.82 | 0.57 | 399433.00 | 32769.00 |
| 21 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.71 | 0.94 | 0.78 | 0.88 | 0.68 | 0.60 | 0.47 | 0.46 | 0.33 | 0.56 | 0.42 | 0.56 | 0.42 | 0.81 | 0.58 | 0.81 | 0.58 | 383417.00 | 31395.00 |
| 30 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.71 | 0.93 | 0.79 | 0.88 | 0.69 | 0.48 | 0.41 | 0.44 | 0.33 | 0.47 | 0.39 | 0.47 | 0.39 | 0.81 | 0.59 | 0.81 | 0.59 | 362041.00 | 29930.00 |
| 45 | 0.00 | 0.00 | 0.93 | 0.79 | 0.85 | 0.71 | 0.93 | 0.80 | 0.88 | 0.70 | 0.35 | 0.34 | 0.13 | 0.11 | 0.36 | 0.31 | 0.36 | 0.31 | 0.80 | 0.59 | 0.80 | 0.59 | 326652.00 | 27344.00 |
| 60 | 0.00 | 0.00 | 0.93 | 0.81 | 0.85 | 0.72 | 0.93 | 0.81 | 0.88 | 0.72 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.80 | 0.60 | 0.80 | 0.60 | 289193.00 | 24515.00 |
| 90 | 0.00 | 0.00 | 0.93 | 0.83 | 0.84 | 0.72 | 0.93 | 0.84 | 0.88 | 0.74 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.80 | 0.61 | 0.80 | 0.61 | 223863.00 | 19451.00 |
