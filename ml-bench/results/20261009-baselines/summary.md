# ml-bench results

Run: `20261009-baselines` · generated 2026-10-09T19:10+00:00

- code: `par-827-wip`; models: snaive7, wt_med, clim, h5, lvlh5_naive, lvlh5_tft, lvlh5_cbd, prod_served + plug-ins none
- export: `/data/exports/20261009` (git b70fedf7d48722e79c98516f3168b3cc9dc0b4fb, finished 2026-10-09T17:35:33+00:00); truth ['2025-12-23', '2026-10-09'], 3601 rides, 157 parks, 13455954 truth slots
- runtime: 70.7 CPU-shard-minutes over 3 shard(s)
- MAE in minutes on 15-min slots; CIs = 95 % park-day cluster bootstrap (1000 reps); `diff` = model − reference on paired slots (negative = better).
- Weather in the known-future covariates is ORACLE (actuals; no forecast archive exists). Schedules are the latest published version (no history), flagged `published_final`.

## Horizon — the headline

### Lead availability (origin days per lead)

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
| TFT daily level | 60 | last run 2026-10-08 00:00:00; beyond: no forecast -> composed level falls back |
| CatBoost daily level | 45 | last run 2026-10-09 00:00:00; beyond: no forecast -> composed level falls back |
| weather forecast (Open-Meteo) | 16 | forecast rows 2026-10-10 00:00:00..2026-10-24 00:00:00; NO archive -> backtest uses actuals (ORACLE) |
| published operator schedule (per park) | 39 | p10 -32 d, median 39 d, p90 155 d past the last truth day; latest version only (no history) -> flagged published_final |

### Usable horizon (contiguous leads where the paired CI vs the per-lead reference excludes 0)

| metric | uc | segment | model | leads_tested | usable_horizon | max_significant_lead |
|---|---|---|---|---|---|---|
| MAE | UC1 | all | clim | 8 |  |  |
| MAE | UC1 | all | h5 | 8 |  |  |
| MAE | UC1 | all | lvlh5_naive | 8 |  |  |
| MAE | UC1 | all | lvlh5_tft | 8 |  |  |
| MAE | UC1 | all | persistence | 0 |  |  |
| MAE | UC1 | all | snaive7 | 8 |  |  |
| MAE | UC1 | all | wt_med | 8 |  |  |
| MAE | UC1 | busy | clim | 8 |  |  |
| MAE | UC1 | busy | h5 | 8 |  |  |
| MAE | UC1 | busy | lvlh5_naive | 8 |  |  |
| MAE | UC1 | busy | lvlh5_tft | 8 |  |  |
| MAE | UC1 | busy | persistence | 0 |  |  |
| MAE | UC1 | busy | snaive7 | 8 |  |  |
| MAE | UC1 | busy | wt_med | 8 |  |  |
| MAE | UC2 | all | clim | 2 |  |  |
| MAE | UC2 | all | h5 | 2 | 1 | 1 |
| MAE | UC2 | all | lvlh5_cbd | 2 |  |  |
| MAE | UC2 | all | lvlh5_naive | 2 |  |  |
| MAE | UC2 | all | lvlh5_tft | 2 |  |  |
| MAE | UC2 | all | snaive7 | 2 |  |  |
| MAE | UC2 | all | wt_med | 0 |  |  |
| MAE | UC2 | busy | clim | 2 |  |  |
| MAE | UC2 | busy | h5 | 2 | 1 | 1 |
| MAE | UC2 | busy | lvlh5_cbd | 2 |  |  |
| MAE | UC2 | busy | lvlh5_naive | 2 |  |  |
| MAE | UC2 | busy | lvlh5_tft | 2 | 1 | 1 |
| MAE | UC2 | busy | snaive7 | 2 |  |  |
| MAE | UC2 | busy | wt_med | 0 |  |  |
| MAE | UC2-intraday | all | clim | 3 |  |  |
| MAE | UC2-intraday | all | h5 | 3 | h8+ | h8+ |
| MAE | UC2-intraday | all | lvlh5_naive | 3 |  |  |
| MAE | UC2-intraday | all | lvlh5_tft | 3 |  |  |
| MAE | UC2-intraday | all | persistence | 3 |  |  |
| MAE | UC2-intraday | all | snaive7 | 3 |  |  |
| MAE | UC2-intraday | all | wt_med | 0 |  |  |
| MAE | UC2-intraday | busy | clim | 3 |  |  |
| MAE | UC2-intraday | busy | h5 | 3 | h8+ | h8+ |
| MAE | UC2-intraday | busy | lvlh5_naive | 3 |  |  |
| MAE | UC2-intraday | busy | lvlh5_tft | 3 | h4-8 | h4-8 |
| MAE | UC2-intraday | busy | persistence | 3 |  |  |
| MAE | UC2-intraday | busy | snaive7 | 3 |  |  |
| MAE | UC2-intraday | busy | wt_med | 0 |  |  |
| MAE | UC3 | all | clim | 13 |  |  |
| MAE | UC3 | all | h5 | 14 | 90 | 90 |
| MAE | UC3 | all | lvlh5_cbd | 12 |  |  |
| MAE | UC3 | all | lvlh5_naive | 14 |  |  |
| MAE | UC3 | all | lvlh5_tft | 13 |  |  |
| MAE | UC3 | all | prod_served | 11 |  |  |
| MAE | UC3 | all | snaive7 | 6 |  |  |
| MAE | UC3 | all | wt_med | 1 |  | 60 |
| MAE | UC3 | busy | clim | 13 |  |  |
| MAE | UC3 | busy | h5 | 14 | 90 | 90 |
| MAE | UC3 | busy | lvlh5_cbd | 12 |  |  |
| MAE | UC3 | busy | lvlh5_naive | 14 |  | 6 |
| MAE | UC3 | busy | lvlh5_tft | 13 | 6 | 6 |
| MAE | UC3 | busy | prod_served | 11 |  |  |
| MAE | UC3 | busy | snaive7 | 6 |  |  |
| MAE | UC3 | busy | wt_med | 1 |  | 60 |
| Spearman of slots within ride-day | UC2/UC3 | all | clim | 14 |  |  |
| Spearman of slots within ride-day | UC2/UC3 | all | h5 | 15 | 90 | 90 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_cbd | 13 |  |  |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_naive | 15 | 90 | 90 |
| Spearman of slots within ride-day | UC2/UC3 | all | lvlh5_tft | 14 |  |  |
| Spearman of slots within ride-day | UC2/UC3 | all | prod_served | 11 |  | 45 |
| Spearman of slots within ride-day | UC2/UC3 | all | snaive7 | 7 |  |  |
| Spearman of slots within ride-day | UC2/UC3 | all | wt_med | 1 |  |  |
| UC4 Spearman within park-month | UC4 | all | lvl_cbd | 12 |  |  |
| UC4 Spearman within park-month | UC4 | all | lvl_clim | 15 |  |  |
| UC4 Spearman within park-month | UC4 | all | lvl_naive4 | 8 |  |  |
| UC4 Spearman within park-month | UC4 | all | lvl_snaive7 | 0 |  |  |
| UC4 Spearman within park-month | UC4 | all | lvl_tft | 12 |  |  |
| UC4 Spearman within park-month | UC4 | all | lvl_wt56 | 15 |  |  |
| best-time hit rate (top-2) | D3 | all | clim | 15 |  |  |
| best-time hit rate (top-2) | D3 | all | h5 | 15 | 90 | 90 |
| best-time hit rate (top-2) | D3 | all | lvlh5_cbd | 13 |  |  |
| best-time hit rate (top-2) | D3 | all | lvlh5_naive | 15 |  |  |
| best-time hit rate (top-2) | D3 | all | lvlh5_tft | 14 |  |  |
| best-time hit rate (top-2) | D3 | all | prod_served | 11 |  |  |
| best-time hit rate (top-2) | D3 | all | snaive7 | 7 |  |  |
| best-time hit rate (top-2) | D3 | all | wt_med | 0 |  |  |
| best-time hit rate (±30 min) | D3 | all | clim | 15 |  |  |
| best-time hit rate (±30 min) | D3 | all | h5 | 15 | 90 | 90 |
| best-time hit rate (±30 min) | D3 | all | lvlh5_cbd | 13 |  |  |
| best-time hit rate (±30 min) | D3 | all | lvlh5_naive | 15 |  |  |
| best-time hit rate (±30 min) | D3 | all | lvlh5_tft | 14 |  |  |
| best-time hit rate (±30 min) | D3 | all | prod_served | 11 |  |  |
| best-time hit rate (±30 min) | D3 | all | snaive7 | 7 |  |  |
| best-time hit rate (±30 min) | D3 | all | wt_med | 0 |  |  |
| best-time regret (min) | D3 | all | clim | 15 |  |  |
| best-time regret (min) | D3 | all | h5 | 15 | 90 | 90 |
| best-time regret (min) | D3 | all | lvlh5_cbd | 13 |  |  |
| best-time regret (min) | D3 | all | lvlh5_naive | 15 |  |  |
| best-time regret (min) | D3 | all | lvlh5_tft | 14 |  |  |
| best-time regret (min) | D3 | all | prod_served | 11 |  |  |
| best-time regret (min) | D3 | all | snaive7 | 7 |  |  |
| best-time regret (min) | D3 | all | wt_med | 0 |  |  |
| dayPeak Spearman across rides | UC3 | all | clim | 13 |  |  |
| dayPeak Spearman across rides | UC3 | all | h5 | 15 |  | 60 |
| dayPeak Spearman across rides | UC3 | all | lvlh5_cbd | 13 |  |  |
| dayPeak Spearman across rides | UC3 | all | lvlh5_naive | 15 |  |  |
| dayPeak Spearman across rides | UC3 | all | lvlh5_tft | 14 |  |  |
| dayPeak Spearman across rides | UC3 | all | prod_served | 11 |  | 60 |
| dayPeak Spearman across rides | UC3 | all | snaive7 | 7 |  |  |
| dayPeak Spearman across rides | UC3 | all | wt_med | 2 |  | 60 |
| dayPeak abs error (min) | UC3 | all | clim | 10 |  |  |
| dayPeak abs error (min) | UC3 | all | h5 | 15 |  | 90 |
| dayPeak abs error (min) | UC3 | all | lvlh5_cbd | 13 |  |  |
| dayPeak abs error (min) | UC3 | all | lvlh5_naive | 15 |  |  |
| dayPeak abs error (min) | UC3 | all | lvlh5_tft | 14 | 1 | 1 |
| dayPeak abs error (min) | UC3 | all | prod_served | 11 | 10 | 10 |
| dayPeak abs error (min) | UC3 | all | snaive7 | 7 |  |  |
| dayPeak abs error (min) | UC3 | all | wt_med | 5 |  | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | clim | 14 |  |  |
| dayPeak pairwise ordering (headliners) | D4 | all | h5 | 15 | 90 | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_cbd | 13 |  |  |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_naive | 15 | 90 | 90 |
| dayPeak pairwise ordering (headliners) | D4 | all | lvlh5_tft | 14 | 60 | 60 |
| dayPeak pairwise ordering (headliners) | D4 | all | prod_served | 11 |  |  |
| dayPeak pairwise ordering (headliners) | D4 | all | snaive7 | 7 |  |  |
| dayPeak pairwise ordering (headliners) | D4 | all | wt_med | 1 |  |  |
| first-hour MAE (opening-aligned) | D5 | all | clim | 14 |  |  |
| first-hour MAE (opening-aligned) | D5 | all | h5 | 15 | 90 | 90 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_cbd | 13 | 45 | 45 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_naive | 15 | 90 | 90 |
| first-hour MAE (opening-aligned) | D5 | all | lvlh5_tft | 14 | 60 | 60 |
| first-hour MAE (opening-aligned) | D5 | all | prod_served | 11 |  |  |
| first-hour MAE (opening-aligned) | D5 | all | snaive7 | 7 |  |  |
| first-hour MAE (opening-aligned) | D5 | all | wt_med | 1 |  |  |
| live-window MAE (headliners) | D2 | all | clim | 1 |  |  |
| live-window MAE (headliners) | D2 | all | h5 | 1 |  |  |
| live-window MAE (headliners) | D2 | all | lvlh5_naive | 1 |  |  |
| live-window MAE (headliners) | D2 | all | lvlh5_tft | 1 |  |  |
| live-window MAE (headliners) | D2 | all | persistence | 0 |  |  |
| live-window MAE (headliners) | D2 | all | snaive7 | 1 |  |  |
| live-window MAE (headliners) | D2 | all | wt_med | 1 |  |  |
| next-best precision | D1 | all | clim | 2 |  |  |
| next-best precision | D1 | all | h5 | 2 |  | 60-120 |
| next-best precision | D1 | all | lvlh5_naive | 2 |  | 60-120 |
| next-best precision | D1 | all | lvlh5_tft | 2 |  | 60-120 |
| next-best precision | D1 | all | snaive7 | 0 |  |  |
| next-best precision | D1 | all | wt_med | 2 |  |  |
| next-best precision (top-3) | D1 | all | clim | 2 |  |  |
| next-best precision (top-3) | D1 | all | h5 | 2 |  | 60-120 |
| next-best precision (top-3) | D1 | all | lvlh5_naive | 2 |  | 60-120 |
| next-best precision (top-3) | D1 | all | lvlh5_tft | 2 |  |  |
| next-best precision (top-3) | D1 | all | snaive7 | 0 |  |  |
| next-best precision (top-3) | D1 | all | wt_med | 2 |  | 60-120 |
| next-best recall | D1 | all | clim | 2 |  |  |
| next-best recall | D1 | all | h5 | 2 |  |  |
| next-best recall | D1 | all | lvlh5_naive | 2 |  |  |
| next-best recall | D1 | all | lvlh5_tft | 2 |  |  |
| next-best recall | D1 | all | snaive7 | 0 |  |  |
| next-best recall | D1 | all | wt_med | 2 |  |  |
| optimiser regret (min per park-day) | D4 | all | clim | 4 |  |  |
| optimiser regret (min per park-day) | D4 | all | h5 | 5 | 30 | 30 |
| optimiser regret (min per park-day) | D4 | all | lvlh5_cbd | 5 |  |  |
| optimiser regret (min per park-day) | D4 | all | lvlh5_naive | 5 | 30 | 30 |
| optimiser regret (min per park-day) | D4 | all | lvlh5_tft | 5 | 3 | 30 |
| optimiser regret (min per park-day) | D4 | all | prod_served | 4 |  |  |
| optimiser regret (min per park-day) | D4 | all | snaive7 | 2 |  |  |
| optimiser regret (min per park-day) | D4 | all | wt_med | 1 |  |  |
| rope-drop worth agreement | D5 | all | clim | 7 |  |  |
| rope-drop worth agreement | D5 | all | h5 | 15 |  |  |
| rope-drop worth agreement | D5 | all | lvlh5_cbd | 13 |  |  |
| rope-drop worth agreement | D5 | all | lvlh5_naive | 15 |  |  |
| rope-drop worth agreement | D5 | all | lvlh5_tft | 14 |  |  |
| rope-drop worth agreement | D5 | all | prod_ropedrop_hist | 15 |  |  |
| rope-drop worth agreement | D5 | all | prod_served | 11 |  |  |
| rope-drop worth agreement | D5 | all | snaive7 | 7 |  |  |
| rope-drop worth agreement | D5 | all | wt_med | 8 |  |  |

### Hand-over table (input for the serving router)

| uc | segment | lead | reference | winner | winner_value | margin_vs_ref | n_origin_days |
|---|---|---|---|---|---|---|---|
| UC2 | all | 0 | wt_med | h5 | 7.47 | -0.07 | 231 |
| UC2 | busy | 0 | wt_med | lvlh5_tft | 13.69 | -0.56 | 231 |
| UC3 | all | 1 | wt_med | h5 | 7.58 | -0.07 | 230 |
| UC3 | busy | 1 | wt_med | lvlh5_tft | 13.91 | -0.49 | 230 |
| UC3 | all | 2 | wt_med | h5 | 7.61 | -0.08 | 229 |
| UC3 | busy | 2 | wt_med | lvlh5_tft | 14.05 | -0.41 | 229 |
| UC3 | all | 3 | wt_med | h5 | 7.64 | -0.08 | 228 |
| UC3 | busy | 3 | wt_med | lvlh5_tft | 14.25 | -0.27 | 228 |
| UC3 | all | 4 | wt_med | h5 | 7.67 | -0.08 | 227 |
| UC3 | busy | 4 | wt_med | lvlh5_tft | 14.33 | -0.25 | 227 |
| UC3 | all | 5 | wt_med | h5 | 7.70 | -0.08 | 226 |
| UC3 | busy | 5 | wt_med | lvlh5_tft | 14.50 | -0.13 | 226 |
| UC3 | all | 6 | wt_med | h5 | 7.75 | -0.09 | 225 |
| UC3 | busy | 6 | wt_med | lvlh5_tft | 14.61 | -0.12 | 225 |
| UC3 | all | 7 | wt_med | h5 | 7.86 | -0.09 | 224 |
| UC3 | busy | 7 | wt_med | h5 | 14.99 | -0.23 | 224 |
| UC3 | all | 10 | wt_med | h5 | 7.96 | -0.10 | 221 |
| UC3 | busy | 10 | wt_med | h5 | 15.17 | -0.24 | 221 |
| UC3 | all | 14 | wt_med | h5 | 8.10 | -0.10 | 217 |
| UC3 | busy | 14 | wt_med | h5 | 15.43 | -0.26 | 217 |
| UC3 | all | 21 | wt_med | h5 | 8.26 | -0.11 | 210 |
| UC3 | busy | 21 | wt_med | h5 | 15.68 | -0.28 | 210 |
| UC3 | all | 30 | wt_med | h5 | 8.40 | -0.13 | 201 |
| UC3 | busy | 30 | wt_med | h5 | 15.86 | -0.31 | 201 |
| UC3 | all | 45 | wt_med | h5 | 8.51 | -0.14 | 186 |
| UC3 | busy | 45 | wt_med | h5 | 16.08 | -0.33 | 186 |
| UC3 | all | 60 | clim | h5 | 8.63 | -0.20 | 171 |
| UC3 | busy | 60 | clim | h5 | 16.37 | -0.50 | 171 |
| UC3 | all | 90 | wt_med | h5 | 8.71 | -0.14 | 141 |
| UC3 | busy | 90 | wt_med | h5 | 16.51 | -0.33 | 141 |
| UC2 | all | 1 | wt_med | h5 | 7.58 | -0.07 | 230 |
| UC2 | busy | 1 | wt_med | lvlh5_tft | 13.91 | -0.49 | 230 |
| UC1 | all | m015 | persistence | persistence | 2.46 | 0.00 | 231 |
| UC1 | busy | m015 | persistence | persistence | 4.53 | 0.00 | 231 |
| UC1 | all | m030 | persistence | persistence | 4.15 | 0.00 | 231 |
| UC1 | busy | m030 | persistence | persistence | 7.74 | 0.00 | 231 |
| UC1 | all | m045 | persistence | persistence | 5.07 | 0.00 | 231 |
| UC1 | busy | m045 | persistence | persistence | 9.45 | 0.00 | 231 |
| UC1 | all | m060 | persistence | persistence | 5.70 | 0.00 | 231 |
| UC1 | busy | m060 | persistence | persistence | 10.62 | 0.00 | 231 |
| UC1 | all | m075 | persistence | persistence | 6.28 | 0.00 | 231 |
| UC1 | busy | m075 | persistence | persistence | 11.57 | 0.00 | 231 |
| UC1 | all | m090 | persistence | persistence | 6.79 | 0.00 | 231 |
| UC1 | busy | m090 | persistence | persistence | 12.52 | 0.00 | 231 |
| UC1 | all | m105 | persistence | persistence | 7.16 | 0.00 | 231 |
| UC1 | busy | m105 | persistence | persistence | 13.15 | 0.00 | 231 |
| UC1 | all | m120 | persistence | persistence | 7.48 | 0.00 | 231 |
| UC1 | busy | m120 | persistence | persistence | 13.76 | 0.00 | 231 |
| UC2-intraday | all | h2-4 | wt_med | h5 | 7.52 | -0.07 | 231 |
| UC2-intraday | busy | h2-4 | wt_med | lvlh5_tft | 13.79 | -0.56 | 231 |
| UC2-intraday | all | h4-8 | wt_med | h5 | 7.79 | -0.09 | 231 |
| UC2-intraday | busy | h4-8 | wt_med | lvlh5_tft | 14.07 | -0.53 | 231 |
| UC2-intraday | all | h8+ | wt_med | h5 | 7.48 | -0.10 | 231 |
| UC2-intraday | busy | h8+ | wt_med | h5 | 13.56 | -0.24 | 231 |

Decision metrics:

| metric | uc | lead | reference | winner | winner_value | margin_vs_ref |
|---|---|---|---|---|---|---|
| Spearman of slots within ride-day | UC2/UC3 | 0 | wt_med | h5 | 0.46 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 1 | wt_med | h5 | 0.46 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 2 | wt_med | h5 | 0.46 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 3 | wt_med | h5 | 0.46 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 4 | wt_med | h5 | 0.46 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 5 | wt_med | h5 | 0.45 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 6 | wt_med | h5 | 0.45 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 7 | wt_med | h5 | 0.45 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 10 | wt_med | h5 | 0.44 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 14 | wt_med | h5 | 0.44 | 0.03 |
| Spearman of slots within ride-day | UC2/UC3 | 21 | wt_med | h5 | 0.43 | 0.04 |
| Spearman of slots within ride-day | UC2/UC3 | 30 | wt_med | h5 | 0.42 | 0.04 |
| Spearman of slots within ride-day | UC2/UC3 | 45 | wt_med | h5 | 0.41 | 0.04 |
| Spearman of slots within ride-day | UC2/UC3 | 60 | clim | h5 | 0.40 | 0.04 |
| Spearman of slots within ride-day | UC2/UC3 | 90 | wt_med | h5 | 0.40 | 0.04 |
| best-time hit rate (top-2) | D3 | 0 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 1 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 2 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 3 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 4 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 5 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 6 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 7 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 10 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (top-2) | D3 | 14 | wt_med | h5 | 0.81 | 0.01 |
| best-time hit rate (top-2) | D3 | 21 | wt_med | h5 | 0.81 | 0.01 |
| best-time hit rate (top-2) | D3 | 30 | wt_med | h5 | 0.81 | 0.01 |
| best-time hit rate (top-2) | D3 | 45 | wt_med | h5 | 0.81 | 0.01 |
| best-time hit rate (top-2) | D3 | 60 | wt_med | h5 | 0.80 | 0.01 |
| best-time hit rate (top-2) | D3 | 90 | wt_med | h5 | 0.80 | 0.01 |
| best-time hit rate (±30 min) | D3 | 0 | wt_med | h5 | 0.83 | 0.01 |
| best-time hit rate (±30 min) | D3 | 1 | wt_med | h5 | 0.83 | 0.01 |
| best-time hit rate (±30 min) | D3 | 2 | wt_med | h5 | 0.83 | 0.01 |
| best-time hit rate (±30 min) | D3 | 3 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 4 | wt_med | h5 | 0.83 | 0.01 |
| best-time hit rate (±30 min) | D3 | 5 | wt_med | h5 | 0.83 | 0.01 |
| best-time hit rate (±30 min) | D3 | 6 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 7 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 10 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 14 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 21 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 30 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 45 | wt_med | h5 | 0.82 | 0.01 |
| best-time hit rate (±30 min) | D3 | 60 | wt_med | h5 | 0.81 | 0.01 |
| best-time hit rate (±30 min) | D3 | 90 | wt_med | h5 | 0.81 | 0.01 |
| best-time regret (min) | D3 | 0 | wt_med | h5 | 2.94 | -0.34 |
| best-time regret (min) | D3 | 1 | wt_med | h5 | 2.96 | -0.34 |
| best-time regret (min) | D3 | 2 | wt_med | h5 | 2.96 | -0.33 |
| best-time regret (min) | D3 | 3 | wt_med | h5 | 2.97 | -0.34 |
| best-time regret (min) | D3 | 4 | wt_med | h5 | 2.97 | -0.34 |
| best-time regret (min) | D3 | 5 | wt_med | h5 | 2.98 | -0.34 |
| best-time regret (min) | D3 | 6 | wt_med | h5 | 3.00 | -0.36 |
| best-time regret (min) | D3 | 7 | wt_med | h5 | 3.03 | -0.35 |
| best-time regret (min) | D3 | 10 | wt_med | h5 | 3.05 | -0.37 |
| best-time regret (min) | D3 | 14 | wt_med | h5 | 3.09 | -0.37 |
| best-time regret (min) | D3 | 21 | wt_med | h5 | 3.11 | -0.41 |
| best-time regret (min) | D3 | 30 | wt_med | h5 | 3.13 | -0.44 |
| best-time regret (min) | D3 | 45 | wt_med | h5 | 3.20 | -0.50 |
| best-time regret (min) | D3 | 60 | wt_med | h5 | 3.23 | -0.50 |
| best-time regret (min) | D3 | 90 | wt_med | h5 | 3.27 | -0.48 |
| dayPeak Spearman across rides | UC3 | 0 | wt_med | wt_med | 0.77 | 0.00 |
| dayPeak Spearman across rides | UC3 | 1 | wt_med | wt_med | 0.76 | 0.00 |
| dayPeak Spearman across rides | UC3 | 2 | wt_med | wt_med | 0.76 | 0.00 |
| dayPeak Spearman across rides | UC3 | 3 | wt_med | wt_med | 0.76 | 0.00 |
| dayPeak Spearman across rides | UC3 | 4 | wt_med | wt_med | 0.76 | 0.00 |
| dayPeak Spearman across rides | UC3 | 5 | wt_med | wt_med | 0.76 | 0.00 |
| dayPeak Spearman across rides | UC3 | 6 | wt_med | wt_med | 0.76 | 0.00 |
| dayPeak Spearman across rides | UC3 | 7 | wt_med | wt_med | 0.75 | 0.00 |
| dayPeak Spearman across rides | UC3 | 10 | wt_med | wt_med | 0.75 | 0.00 |
| dayPeak Spearman across rides | UC3 | 14 | wt_med | wt_med | 0.74 | 0.00 |
| dayPeak Spearman across rides | UC3 | 21 | wt_med | wt_med | 0.74 | 0.00 |
| dayPeak Spearman across rides | UC3 | 30 | wt_med | wt_med | 0.73 | 0.00 |
| dayPeak Spearman across rides | UC3 | 45 | clim | wt_med | 0.72 | 0.01 |
| dayPeak Spearman across rides | UC3 | 60 | clim | prod_served | 0.82 | 0.12 |
| dayPeak Spearman across rides | UC3 | 90 | wt_med | wt_med | 0.72 | 0.00 |
| dayPeak abs error (min) | UC3 | 0 | wt_med | lvlh5_tft | 8.75 | -0.19 |
| dayPeak abs error (min) | UC3 | 1 | wt_med | lvlh5_tft | 8.97 | -0.09 |
| dayPeak abs error (min) | UC3 | 2 | wt_med | wt_med | 9.31 | 0.00 |
| dayPeak abs error (min) | UC3 | 3 | wt_med | prod_served | 8.51 | -0.52 |
| dayPeak abs error (min) | UC3 | 4 | wt_med | prod_served | 8.57 | -0.51 |
| dayPeak abs error (min) | UC3 | 5 | wt_med | prod_served | 8.68 | -0.43 |
| dayPeak abs error (min) | UC3 | 6 | wt_med | prod_served | 8.79 | -0.39 |
| dayPeak abs error (min) | UC3 | 7 | wt_med | prod_served | 9.12 | -0.19 |
| dayPeak abs error (min) | UC3 | 10 | wt_med | prod_served | 9.37 | -0.10 |
| dayPeak abs error (min) | UC3 | 14 | wt_med | wt_med | 9.97 | 0.00 |
| dayPeak abs error (min) | UC3 | 21 | clim | clim | 10.08 | 0.00 |
| dayPeak abs error (min) | UC3 | 30 | clim | clim | 10.17 | 0.00 |
| dayPeak abs error (min) | UC3 | 45 | clim | clim | 10.17 | 0.00 |
| dayPeak abs error (min) | UC3 | 60 | clim | clim | 10.15 | 0.00 |
| dayPeak abs error (min) | UC3 | 90 | clim | wt_med | 10.36 | -0.10 |
| dayPeak pairwise ordering (headliners) | D4 | 0 | wt_med | lvlh5_tft | 0.77 | 0.04 |
| dayPeak pairwise ordering (headliners) | D4 | 1 | wt_med | lvlh5_tft | 0.76 | 0.04 |
| dayPeak pairwise ordering (headliners) | D4 | 2 | wt_med | lvlh5_tft | 0.76 | 0.04 |
| dayPeak pairwise ordering (headliners) | D4 | 3 | wt_med | lvlh5_tft | 0.76 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 4 | wt_med | lvlh5_naive | 0.76 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 5 | wt_med | lvlh5_naive | 0.76 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 6 | wt_med | lvlh5_naive | 0.76 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 7 | wt_med | lvlh5_tft | 0.75 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 10 | wt_med | lvlh5_naive | 0.75 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 14 | wt_med | lvlh5_naive | 0.74 | 0.02 |
| dayPeak pairwise ordering (headliners) | D4 | 21 | wt_med | lvlh5_naive | 0.73 | 0.02 |
| dayPeak pairwise ordering (headliners) | D4 | 30 | wt_med | lvlh5_naive | 0.73 | 0.02 |
| dayPeak pairwise ordering (headliners) | D4 | 45 | wt_med | lvlh5_tft | 0.72 | 0.03 |
| dayPeak pairwise ordering (headliners) | D4 | 60 | clim | lvlh5_tft | 0.73 | 0.07 |
| dayPeak pairwise ordering (headliners) | D4 | 90 | wt_med | lvlh5_naive | 0.71 | 0.02 |
| first-hour MAE (opening-aligned) | D5 | 0 | wt_med | lvlh5_tft | 3.86 | -0.20 |
| first-hour MAE (opening-aligned) | D5 | 1 | wt_med | lvlh5_tft | 3.91 | -0.21 |
| first-hour MAE (opening-aligned) | D5 | 2 | wt_med | lvlh5_tft | 3.93 | -0.21 |
| first-hour MAE (opening-aligned) | D5 | 3 | wt_med | lvlh5_tft | 3.95 | -0.22 |
| first-hour MAE (opening-aligned) | D5 | 4 | wt_med | lvlh5_tft | 3.96 | -0.23 |
| first-hour MAE (opening-aligned) | D5 | 5 | wt_med | lvlh5_tft | 3.98 | -0.23 |
| first-hour MAE (opening-aligned) | D5 | 6 | wt_med | lvlh5_tft | 4.01 | -0.24 |
| first-hour MAE (opening-aligned) | D5 | 7 | wt_med | lvlh5_tft | 4.06 | -0.25 |
| first-hour MAE (opening-aligned) | D5 | 10 | wt_med | lvlh5_tft | 4.12 | -0.26 |
| first-hour MAE (opening-aligned) | D5 | 14 | wt_med | lvlh5_tft | 4.21 | -0.28 |
| first-hour MAE (opening-aligned) | D5 | 21 | wt_med | lvlh5_tft | 4.32 | -0.31 |
| first-hour MAE (opening-aligned) | D5 | 30 | wt_med | lvlh5_tft | 4.50 | -0.35 |
| first-hour MAE (opening-aligned) | D5 | 45 | wt_med | lvlh5_cbd | 4.53 | -0.29 |
| first-hour MAE (opening-aligned) | D5 | 60 | clim | lvlh5_tft | 1.34 | -0.82 |
| first-hour MAE (opening-aligned) | D5 | 90 | wt_med | h5 | 4.83 | -0.23 |
| live-window MAE (headliners) | D2 | w0-45 | persistence | persistence | 6.29 | 0.00 |
| next-best precision | D1 | 0-60 | snaive7 | snaive7 | 0.37 | 0.00 |
| next-best precision | D1 | 60-120 | snaive7 | lvlh5_naive | 0.51 | 0.03 |
| next-best precision (top-3) | D1 | 0-60 | snaive7 | snaive7 | 0.43 | 0.00 |
| next-best precision (top-3) | D1 | 60-120 | snaive7 | lvlh5_naive | 0.57 | 0.01 |
| next-best recall | D1 | 0-60 | snaive7 | snaive7 | 0.65 | 0.00 |
| next-best recall | D1 | 60-120 | snaive7 | snaive7 | 0.62 | 0.00 |
| optimiser regret (min per park-day) | D4 | 1 | wt_med | lvlh5_tft | 27.85 | -0.68 |
| optimiser regret (min per park-day) | D4 | 3 | wt_med | lvlh5_tft | 27.86 | -1.22 |
| optimiser regret (min per park-day) | D4 | 7 | wt_med | h5 | 29.19 | -0.83 |
| optimiser regret (min per park-day) | D4 | 14 | wt_med | h5 | 29.67 | -1.18 |
| optimiser regret (min per park-day) | D4 | 30 | clim | lvlh5_tft | 29.87 | -0.81 |
| rope-drop worth agreement | D5 | 0 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 1 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 2 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 3 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 4 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 5 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 6 | wt_med | wt_med | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 7 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 10 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 14 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 21 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 30 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 45 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 60 | clim | clim | 0.91 | 0.00 |
| rope-drop worth agreement | D5 | 90 | clim | clim | 0.91 | 0.00 |
| UC4 Spearman within park-month | UC4 | 1 | lvl_snaive7 | lvl_snaive7 | 0.28 | 0.00 |
| UC4 Spearman within park-month | UC4 | 2 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 |
| UC4 Spearman within park-month | UC4 | 3 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 |
| UC4 Spearman within park-month | UC4 | 4 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 |
| UC4 Spearman within park-month | UC4 | 5 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 |
| UC4 Spearman within park-month | UC4 | 6 | lvl_snaive7 | lvl_snaive7 | 0.27 | 0.00 |
| UC4 Spearman within park-month | UC4 | 7 | lvl_naive4 | lvl_naive4 | 0.17 | 0.00 |
| UC4 Spearman within park-month | UC4 | 10 | lvl_naive4 | lvl_naive4 | 0.18 | 0.00 |
| UC4 Spearman within park-month | UC4 | 14 | lvl_naive4 | lvl_naive4 | 0.14 | 0.00 |
| UC4 Spearman within park-month | UC4 | 21 | lvl_naive4 | lvl_naive4 | 0.13 | 0.00 |
| UC4 Spearman within park-month | UC4 | 30 | lvl_clim | lvl_clim | 0.15 | 0.00 |
| UC4 Spearman within park-month | UC4 | 45 | lvl_naive4 | lvl_naive4 | 0.16 | 0.00 |
| UC4 Spearman within park-month | UC4 | 60 | lvl_naive4 | lvl_naive4 | 0.16 | 0.00 |
| UC4 Spearman within park-month | UC4 | 90 | lvl_wt56 | lvl_wt56 | 0.16 | 0.00 |
| UC4 Spearman within park-month | UC4 | 120 | lvl_naive4 | lvl_naive4 | 0.21 | 0.00 |
| UC4 Spearman within park-month | UC4 | 180 | lvl_naive4 | lvl_naive4 | 0.33 | 0.00 |

### Level vs shape by lead (MAE, all rides)

| lead | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | h5 | wt_med |
|---|---|---|---|---|---|---|
| 1 | 7.85 [7.77, 7.93] | 7.50 [7.42, 7.60] | 5.43 [5.39, 5.48] | 6.54 [6.46, 6.62] | 7.58 [7.50, 7.65] | 7.65 [7.58, 7.73] |
| 2 | 7.85 [7.78, 7.93] | 7.57 [7.48, 7.67] | 5.44 [5.39, 5.48] | 6.53 [6.46, 6.61] | 7.61 [7.54, 7.69] | 7.70 [7.62, 7.77] |
| 3 | 7.86 [7.78, 7.94] | 7.66 [7.57, 7.75] | 5.44 [5.39, 5.49] | 6.53 [6.45, 6.61] | 7.64 [7.56, 7.72] | 7.73 [7.65, 7.81] |
| 4 | 7.87 [7.79, 7.94] | 7.70 [7.62, 7.80] | 5.45 [5.40, 5.49] | 6.53 [6.45, 6.60] | 7.67 [7.59, 7.74] | 7.76 [7.68, 7.83] |
| 5 | 7.87 [7.79, 7.95] | 7.77 [7.68, 7.88] | 5.46 [5.41, 5.51] | 6.53 [6.45, 6.61] | 7.70 [7.62, 7.78] | 7.79 [7.71, 7.87] |
| 6 | 7.90 [7.82, 7.97] | 7.85 [7.75, 7.94] | 5.48 [5.43, 5.53] | 6.53 [6.45, 6.61] | 7.75 [7.68, 7.84] | 7.85 [7.77, 7.93] |
| 7 | 8.20 [8.11, 8.28] | 8.03 [7.94, 8.13] | 5.51 [5.46, 5.56] | 6.99 [6.90, 7.06] | 7.86 [7.77, 7.94] | 7.96 [7.87, 8.04] |
| 10 | 8.24 [8.16, 8.32] | 8.19 [8.09, 8.29] | 5.54 [5.49, 5.59] | 6.98 [6.90, 7.07] | 7.96 [7.88, 8.04] | 8.07 [7.99, 8.15] |
| 14 | 8.48 [8.40, 8.57] | 8.44 [8.34, 8.54] | 5.61 [5.56, 5.66] | 7.30 [7.21, 7.39] | 8.10 [8.03, 8.19] | 8.22 [8.15, 8.31] |
| 21 | 8.69 [8.61, 8.79] | 8.65 [8.55, 8.76] | 5.67 [5.62, 5.73] | 7.54 [7.46, 7.64] | 8.26 [8.18, 8.35] | 8.40 [8.31, 8.49] |
| 30 | 8.81 [8.72, 8.90] | 9.01 [8.89, 9.14] | 5.71 [5.65, 5.76] | 7.66 [7.57, 7.75] | 8.40 [8.31, 8.49] | 8.56 [8.47, 8.65] |
| 45 | 8.93 [8.83, 9.03] | 9.27 [9.11, 9.43] | 5.73 [5.67, 5.79] | 7.82 [7.71, 7.92] | 8.51 [8.41, 8.60] | 8.69 [8.59, 8.78] |
| 60 | 9.07 [8.97, 9.17] | 4.48 [3.09, 6.00] | 5.76 [5.69, 5.82] | 7.99 [7.88, 8.10] | 8.63 [8.53, 8.73] | 8.82 [8.71, 8.92] |
| 90 | 9.25 [9.14, 9.37] |  | 5.79 [5.72, 5.85] | 8.10 [7.98, 8.22] | 8.71 [8.60, 8.82] | 8.91 [8.80, 9.02] |

`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE shape scaled to the naive level (perfect shape). Where the gap lvlh5 → oracle_level stays large, the level is what is not forecastable.

## UC1 — live / next 2 h (intraday origins)

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | 7.97 [7.89, 8.05] | 7.39 [7.31, 7.47] | 7.71 [7.63, 7.79] | 7.24 [7.16, 7.33] | 2.46 [2.43, 2.49] | 8.60 [8.51, 8.69] | 7.45 [7.37, 7.52] |
| m030 | 7.95 [7.87, 8.03] | 7.36 [7.28, 7.43] | 7.70 [7.62, 7.78] | 7.25 [7.16, 7.34] | 4.15 [4.11, 4.19] | 8.57 [8.47, 8.67] | 7.44 [7.36, 7.52] |
| m045 | 7.98 [7.90, 8.07] | 7.39 [7.31, 7.46] | 7.73 [7.65, 7.81] | 7.29 [7.20, 7.37] | 5.07 [5.02, 5.12] | 8.59 [8.50, 8.68] | 7.47 [7.39, 7.55] |
| m060 | 8.06 [7.97, 8.14] | 7.46 [7.38, 7.54] | 7.77 [7.69, 7.85] | 7.35 [7.26, 7.44] | 5.70 [5.65, 5.75] | 8.67 [8.57, 8.77] | 7.54 [7.46, 7.61] |
| m075 | 8.12 [8.03, 8.21] | 7.56 [7.48, 7.64] | 7.91 [7.83, 8.00] | 7.44 [7.35, 7.53] | 6.28 [6.22, 6.35] | 8.76 [8.67, 8.86] | 7.61 [7.53, 7.70] |
| m090 | 8.16 [8.07, 8.25] | 7.57 [7.49, 7.65] | 7.95 [7.87, 8.03] | 7.48 [7.39, 7.57] | 6.79 [6.73, 6.86] | 8.79 [8.69, 8.89] | 7.64 [7.56, 7.73] |
| m105 | 8.22 [8.13, 8.31] | 7.61 [7.53, 7.69] | 8.00 [7.92, 8.08] | 7.52 [7.43, 7.62] | 7.16 [7.10, 7.23] | 8.85 [8.75, 8.95] | 7.69 [7.61, 7.78] |
| m120 | 8.27 [8.18, 8.35] | 7.64 [7.56, 7.72] | 8.00 [7.92, 8.08] | 7.52 [7.44, 7.62] | 7.48 [7.41, 7.55] | 8.87 [8.78, 8.97] | 7.73 [7.64, 7.81] |

**Paired difference vs the per-lead reference, all**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| m015 | 5.98 [5.90, 6.07] | 5.41 [5.34, 5.48] | 5.75 [5.68, 5.83] | 5.33 [5.26, 5.41] | 6.58 [6.49, 6.66] | 5.46 [5.38, 5.53] | persistence |
| m030 | 4.31 [4.24, 4.39] | 3.70 [3.64, 3.77] | 4.08 [4.01, 4.15] | 3.71 [3.63, 3.78] | 4.93 [4.84, 5.02] | 3.78 [3.72, 3.85] | persistence |
| m045 | 3.41 [3.34, 3.48] | 2.77 [2.70, 2.83] | 3.17 [3.10, 3.24] | 2.79 [2.73, 2.86] | 4.00 [3.91, 4.09] | 2.85 [2.78, 2.91] | persistence |
| m060 | 2.78 [2.70, 2.85] | 2.12 [2.06, 2.18] | 2.49 [2.42, 2.55] | 2.12 [2.05, 2.19] | 3.35 [3.27, 3.44] | 2.19 [2.12, 2.25] | persistence |
| m075 | 2.39 [2.31, 2.47] | 1.69 [1.63, 1.76] | 2.06 [2.00, 2.14] | 1.64 [1.57, 1.72] | 2.97 [2.89, 3.06] | 1.75 [1.69, 1.82] | persistence |
| m090 | 1.86 [1.79, 1.94] | 1.12 [1.06, 1.19] | 1.54 [1.46, 1.61] | 1.13 [1.06, 1.20] | 2.43 [2.35, 2.53] | 1.21 [1.15, 1.28] | persistence |
| m105 | 1.50 [1.42, 1.57] | 0.71 [0.65, 0.78] | 1.15 [1.08, 1.22] | 0.75 [0.68, 0.81] | 2.06 [1.98, 2.15] | 0.81 [0.75, 0.88] | persistence |
| m120 | 1.14 [1.07, 1.21] | 0.34 [0.28, 0.40] | 0.75 [0.69, 0.82] | 0.36 [0.29, 0.43] | 1.69 [1.61, 1.78] | 0.44 [0.38, 0.50] | persistence |

**Bias, all**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | -1.17 | -1.60 | -1.10 | -1.24 | -0.19 | 0.10 | -1.22 |
| m030 | -1.24 | -1.52 | -1.03 | -1.19 | -0.27 | 0.10 | -1.27 |
| m045 | -1.27 | -1.48 | -0.98 | -1.15 | -0.20 | 0.09 | -1.30 |
| m060 | -1.30 | -1.54 | -1.05 | -1.20 | -0.10 | 0.09 | -1.34 |
| m075 | -1.24 | -1.63 | -1.09 | -1.22 | -0.23 | 0.08 | -1.29 |
| m090 | -1.27 | -1.59 | -1.06 | -1.19 | -0.08 | 0.08 | -1.30 |
| m105 | -1.28 | -1.58 | -1.05 | -1.19 | 0.04 | 0.09 | -1.30 |
| m120 | -1.27 | -1.63 | -1.10 | -1.24 | 0.23 | 0.09 | -1.31 |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | 15.52 [15.36, 15.68] | 14.30 [14.16, 14.43] | 14.60 [14.45, 14.75] | 13.58 [13.42, 13.75] | 4.53 [4.48, 4.58] | 16.60 [16.42, 16.79] | 14.47 [14.33, 14.61] |
| m030 | 15.51 [15.36, 15.66] | 14.29 [14.15, 14.43] | 14.63 [14.48, 14.77] | 13.64 [13.48, 13.80] | 7.74 [7.67, 7.80] | 16.58 [16.39, 16.76] | 14.49 [14.35, 14.63] |
| m045 | 15.52 [15.38, 15.69] | 14.30 [14.17, 14.45] | 14.64 [14.50, 14.79] | 13.66 [13.52, 13.83] | 9.45 [9.37, 9.52] | 16.57 [16.39, 16.75] | 14.49 [14.35, 14.63] |
| m060 | 15.57 [15.41, 15.73] | 14.35 [14.22, 14.49] | 14.65 [14.49, 14.79] | 13.70 [13.55, 13.85] | 10.62 [10.53, 10.70] | 16.61 [16.44, 16.77] | 14.54 [14.39, 14.67] |
| m075 | 15.58 [15.42, 15.74] | 14.41 [14.27, 14.55] | 14.70 [14.54, 14.84] | 13.68 [13.52, 13.83] | 11.57 [11.47, 11.66] | 16.68 [16.49, 16.86] | 14.57 [14.42, 14.71] |
| m090 | 15.70 [15.55, 15.84] | 14.47 [14.33, 14.59] | 14.80 [14.66, 14.94] | 13.78 [13.63, 13.93] | 12.52 [12.42, 12.62] | 16.72 [16.56, 16.91] | 14.65 [14.52, 14.78] |
| m105 | 15.77 [15.62, 15.93] | 14.51 [14.38, 14.65] | 14.86 [14.73, 15.01] | 13.82 [13.67, 13.97] | 13.15 [13.05, 13.26] | 16.79 [16.61, 16.97] | 14.69 [14.56, 14.84] |
| m120 | 15.82 [15.66, 15.97] | 14.51 [14.38, 14.65] | 14.82 [14.68, 14.97] | 13.79 [13.63, 13.95] | 13.76 [13.65, 13.86] | 16.78 [16.60, 16.96] | 14.73 [14.59, 14.86] |

**Paired difference vs the per-lead reference, busy**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| m015 | 11.46 [11.29, 11.64] | 10.29 [10.14, 10.43] | 10.59 [10.43, 10.74] | 9.66 [9.49, 9.83] | 12.45 [12.25, 12.65] | 10.43 [10.28, 10.58] | persistence |
| m030 | 8.23 [8.07, 8.40] | 7.04 [6.89, 7.20] | 7.38 [7.22, 7.54] | 6.48 [6.32, 6.64] | 9.27 [9.08, 9.46] | 7.22 [7.07, 7.38] | persistence |
| m045 | 6.58 [6.42, 6.75] | 5.34 [5.21, 5.49] | 5.71 [5.56, 5.88] | 4.78 [4.63, 4.96] | 7.56 [7.38, 7.77] | 5.49 [5.36, 5.65] | persistence |
| m060 | 5.38 [5.21, 5.56] | 4.11 [3.97, 4.26] | 4.44 [4.28, 4.59] | 3.50 [3.34, 3.67] | 6.35 [6.16, 6.53] | 4.27 [4.13, 4.42] | persistence |
| m075 | 4.69 [4.52, 4.86] | 3.34 [3.18, 3.48] | 3.70 [3.54, 3.86] | 2.66 [2.50, 2.83] | 5.70 [5.50, 5.89] | 3.49 [3.32, 3.64] | persistence |
| m090 | 3.71 [3.55, 3.89] | 2.28 [2.13, 2.44] | 2.72 [2.57, 2.88] | 1.67 [1.50, 1.83] | 4.68 [4.50, 4.89] | 2.46 [2.31, 2.62] | persistence |
| m105 | 3.12 [2.96, 3.28] | 1.58 [1.44, 1.73] | 2.05 [1.90, 2.21] | 0.98 [0.82, 1.13] | 4.07 [3.88, 4.26] | 1.80 [1.65, 1.95] | persistence |
| m120 | 2.47 [2.29, 2.63] | 0.86 [0.72, 1.01] | 1.29 [1.13, 1.45] | 0.24 [0.06, 0.41] | 3.37 [3.18, 3.57] | 1.11 [0.96, 1.26] | persistence |

**Bias, busy**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| m015 | -0.65 | -1.45 | -0.98 | -1.94 | -0.32 | 0.72 | -0.67 |
| m030 | -0.78 | -1.30 | -0.82 | -1.83 | -0.41 | 0.73 | -0.77 |
| m045 | -0.85 | -1.15 | -0.67 | -1.69 | -0.17 | 0.69 | -0.82 |
| m060 | -0.89 | -1.25 | -0.76 | -1.74 | 0.10 | 0.69 | -0.86 |
| m075 | -0.74 | -1.39 | -0.89 | -1.88 | -0.16 | 0.69 | -0.74 |
| m090 | -0.77 | -1.30 | -0.82 | -1.81 | 0.20 | 0.68 | -0.74 |
| m105 | -0.72 | -1.27 | -0.78 | -1.80 | 0.47 | 0.69 | -0.73 |
| m120 | -0.72 | -1.35 | -0.87 | -1.88 | 0.90 | 0.70 | -0.74 |

## UC2 — rest of today from intraday origins

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | 8.12 [8.04, 8.20] | 7.52 [7.45, 7.60] | 7.88 [7.81, 7.96] | 7.41 [7.33, 7.49] | 8.72 [8.63, 8.80] | 8.73 [8.65, 8.83] | 7.59 [7.52, 7.67] |
| h4-8 | 8.42 [8.33, 8.51] | 7.79 [7.71, 7.87] | 8.18 [8.10, 8.27] | 7.72 [7.63, 7.82] | 11.35 [11.24, 11.47] | 9.05 [8.94, 9.15] | 7.88 [7.80, 7.96] |
| h8+ | 8.10 [8.00, 8.21] | 7.48 [7.38, 7.57] | 7.94 [7.84, 8.04] | 7.73 [7.61, 7.84] | 14.19 [13.97, 14.40] | 8.80 [8.70, 8.92] | 7.58 [7.48, 7.68] |

**Paired difference vs the per-lead reference, all**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| h2-4 | 0.59 [0.56, 0.62] | -0.07 [-0.07, -0.06] | 0.33 [0.30, 0.36] | 0.05 [-0.00, 0.09] | 0.68 [0.61, 0.75] | 1.16 [1.10, 1.23] | wt_med |
| h4-8 | 0.61 [0.59, 0.64] | -0.09 [-0.09, -0.08] | 0.34 [0.31, 0.37] | 0.06 [0.01, 0.11] | 3.25 [3.16, 3.34] | 1.22 [1.15, 1.29] | wt_med |
| h8+ | 0.56 [0.53, 0.59] | -0.10 [-0.11, -0.09] | 0.39 [0.36, 0.42] | 0.29 [0.24, 0.35] | 6.71 [6.56, 6.87] | 1.31 [1.24, 1.39] | wt_med |

**Bias, all**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | -1.28 | -1.60 | -1.07 | -1.22 | 0.21 | 0.08 | -1.31 |
| h4-8 | -1.33 | -1.68 | -1.11 | -1.28 | 0.44 | 0.07 | -1.42 |
| h8+ | -1.46 | -1.93 | -1.35 | -1.45 | 1.43 | -0.01 | -1.57 |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | 15.72 [15.57, 15.88] | 14.49 [14.35, 14.63] | 14.83 [14.69, 14.98] | 13.79 [13.65, 13.95] | 15.81 [15.69, 15.93] | 16.75 [16.59, 16.94] | 14.67 [14.53, 14.82] |
| h4-8 | 15.94 [15.79, 16.09] | 14.68 [14.55, 14.82] | 15.04 [14.90, 15.18] | 14.07 [13.92, 14.23] | 19.67 [19.51, 19.85] | 17.00 [16.83, 17.18] | 14.88 [14.75, 15.02] |
| h8+ | 14.70 [14.55, 14.87] | 13.56 [13.41, 13.72] | 14.00 [13.85, 14.17] | 13.59 [13.41, 13.78] | 23.26 [22.95, 23.57] | 15.99 [15.82, 16.19] | 13.78 [13.63, 13.93] |

**Paired difference vs the per-lead reference, busy**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | ref |
|---|---|---|---|---|---|---|---|
| h2-4 | 1.15 [1.07, 1.22] | -0.18 [-0.19, -0.17] | 0.24 [0.17, 0.32] | -0.56 [-0.68, -0.44] | 0.97 [0.82, 1.13] | 2.13 [1.97, 2.28] | wt_med |
| h4-8 | 1.16 [1.08, 1.23] | -0.20 [-0.21, -0.19] | 0.24 [0.16, 0.32] | -0.53 [-0.65, -0.40] | 5.32 [5.14, 5.50] | 2.21 [2.06, 2.38] | wt_med |
| h8+ | 1.01 [0.94, 1.07] | -0.24 [-0.26, -0.22] | 0.27 [0.20, 0.34] | -0.11 [-0.22, 0.01] | 10.88 [10.58, 11.17] | 2.32 [2.17, 2.48] | wt_med |

**Bias, busy**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| h2-4 | -0.78 | -1.33 | -0.83 | -1.85 | 0.99 | 0.70 | -0.76 |
| h4-8 | -0.83 | -1.41 | -0.87 | -1.91 | 1.69 | 0.68 | -0.90 |
| h8+ | -1.35 | -2.13 | -1.52 | -2.28 | 4.43 | 0.40 | -1.45 |

## UC2 — today and tomorrow from the 06:00 origin

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.07 [7.99, 8.16] | 7.47 [7.40, 7.55] | 10.26 [10.13, 10.42] | 7.82 [7.74, 7.90] | 7.37 [7.28, 7.45] | 5.41 [5.36, 5.45] | 6.54 [6.46, 6.63] | 8.69 [8.59, 8.79] | 7.55 [7.47, 7.63] |
| 1 | 8.10 [8.02, 8.19] | 7.58 [7.50, 7.65] | 10.21 [10.08, 10.35] | 7.85 [7.77, 7.93] | 7.50 [7.42, 7.60] | 5.43 [5.39, 5.48] | 6.54 [6.46, 6.62] | 8.69 [8.59, 8.79] | 7.65 [7.58, 7.73] |

**Paired difference vs the per-lead reference, all**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.59 [0.56, 0.62] | -0.07 [-0.07, -0.06] | 2.86 [2.77, 2.95] | 0.32 [0.29, 0.35] | 0.04 [-0.00, 0.09] | -2.16 [-2.21, -2.11] | -0.98 [-1.02, -0.93] | 1.16 [1.09, 1.23] | wt_med |
| 1 | 0.52 [0.50, 0.55] | -0.07 [-0.08, -0.07] | 2.69 [2.60, 2.78] | 0.24 [0.21, 0.27] | 0.09 [0.04, 0.14] | -2.24 [-2.29, -2.19] | -1.08 [-1.13, -1.03] | 1.06 [0.99, 1.13] | wt_med |

**Bias, all**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | -1.26 | -1.57 | -5.85 | -1.06 | -1.20 | -0.48 | -0.01 | 0.09 | -1.29 |
| 1 | -1.21 | -1.57 | -5.71 | -1.04 | -1.29 | -0.48 | -0.00 | 0.09 | -1.28 |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 15.59 [15.45, 15.74] | 14.36 [14.24, 14.50] | 19.20 [18.91, 19.50] | 14.68 [14.54, 14.82] | 13.69 [13.53, 13.84] | 9.67 [9.60, 9.75] | 11.69 [11.53, 11.84] | 16.63 [16.46, 16.80] | 14.55 [14.42, 14.68] |
| 1 | 15.61 [15.47, 15.77] | 14.52 [14.39, 14.67] | 18.96 [18.67, 19.23] | 14.69 [14.55, 14.84] | 13.91 [13.75, 14.05] | 9.69 [9.61, 9.77] | 11.65 [11.49, 11.79] | 16.60 [16.41, 16.79] | 14.72 [14.59, 14.86] |

**Paired difference vs the per-lead reference, busy**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 1.14 [1.07, 1.21] | -0.18 [-0.19, -0.17] | 4.95 [4.70, 5.19] | 0.22 [0.16, 0.30] | -0.56 [-0.67, -0.45] | -4.89 [-5.01, -4.79] | -2.77 [-2.87, -2.67] | 2.12 [1.97, 2.29] | wt_med |
| 1 | 1.01 [0.93, 1.08] | -0.19 [-0.20, -0.18] | 4.52 [4.28, 4.74] | 0.06 [-0.01, 0.14] | -0.49 [-0.60, -0.37] | -5.05 [-5.16, -4.94] | -2.97 [-3.08, -2.87] | 1.93 [1.77, 2.08] | wt_med |

**Bias, busy**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.77 | -1.31 | -14.57 | -0.83 | -1.82 | -0.82 | 0.58 | 0.69 | -0.77 |
| 1 | -0.59 | -1.19 | -14.12 | -0.72 | -1.90 | -0.83 | 0.69 | 0.79 | -0.64 |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | Asia | 9.25 | 8.57 | 14.33 | 9.02 | 8.71 | 5.77 | 7.76 | 9.74 | 8.62 |
| 0 | EU | 6.31 | 5.77 | 8.06 | 6.01 | 5.60 | 4.13 | 4.99 | 6.53 | 5.82 |
| 0 | NA | 9.08 | 8.50 | 10.31 | 8.89 | 8.38 | 6.49 | 7.31 | 10.31 | 8.61 |
| 0 | all | 8.07 | 7.47 | 10.26 | 7.82 | 7.37 | 5.41 | 6.54 | 8.69 | 7.55 |

## UC3 — planner day 1…90

**MAE, all** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 8.10 [8.02, 8.19] | 7.58 [7.50, 7.65] | 10.21 [10.08, 10.35] | 7.85 [7.77, 7.93] | 7.50 [7.42, 7.60] | 5.43 [5.39, 5.48] | 6.54 [6.46, 6.62] |  | 8.69 [8.59, 8.79] | 7.65 [7.58, 7.73] |
| 2 | 8.12 [8.03, 8.20] | 7.61 [7.54, 7.69] | 10.20 [10.06, 10.35] | 7.85 [7.78, 7.93] | 7.57 [7.48, 7.67] | 5.44 [5.39, 5.48] | 6.53 [6.46, 6.61] |  | 8.68 [8.59, 8.78] | 7.70 [7.62, 7.77] |
| 3 | 8.14 [8.05, 8.22] | 7.64 [7.56, 7.72] | 10.22 [10.08, 10.35] | 7.86 [7.78, 7.94] | 7.66 [7.57, 7.75] | 5.44 [5.39, 5.49] | 6.53 [6.45, 6.61] | 7.87 [7.78, 7.97] | 8.68 [8.58, 8.77] | 7.73 [7.65, 7.81] |
| 4 | 8.15 [8.07, 8.23] | 7.67 [7.59, 7.74] | 10.24 [10.11, 10.38] | 7.87 [7.79, 7.94] | 7.70 [7.62, 7.80] | 5.45 [5.40, 5.49] | 6.53 [6.45, 6.60] | 7.94 [7.85, 8.03] | 8.67 [8.57, 8.76] | 7.76 [7.68, 7.83] |
| 5 | 8.17 [8.08, 8.25] | 7.70 [7.62, 7.78] | 10.26 [10.13, 10.40] | 7.87 [7.79, 7.95] | 7.77 [7.68, 7.88] | 5.46 [5.41, 5.51] | 6.53 [6.45, 6.61] | 8.01 [7.92, 8.11] | 8.67 [8.57, 8.77] | 7.79 [7.71, 7.87] |
| 6 | 8.20 [8.12, 8.29] | 7.75 [7.68, 7.84] | 10.59 [10.45, 10.75] | 7.90 [7.82, 7.97] | 7.85 [7.75, 7.94] | 5.48 [5.43, 5.53] | 6.53 [6.45, 6.61] | 8.04 [7.94, 8.13] | 8.66 [8.57, 8.77] | 7.85 [7.77, 7.93] |
| 7 | 8.23 [8.14, 8.31] | 7.86 [7.77, 7.94] | 10.75 [10.60, 10.91] | 8.20 [8.11, 8.28] | 8.03 [7.94, 8.13] | 5.51 [5.46, 5.56] | 6.99 [6.90, 7.06] | 8.25 [8.15, 8.34] |  | 7.96 [7.87, 8.04] |
| 10 | 8.29 [8.21, 8.38] | 7.96 [7.88, 8.04] | 10.86 [10.71, 11.02] | 8.24 [8.16, 8.32] | 8.19 [8.09, 8.29] | 5.54 [5.49, 5.59] | 6.98 [6.90, 7.07] | 8.42 [8.31, 8.51] |  | 8.07 [7.99, 8.15] |
| 14 | 8.39 [8.30, 8.47] | 8.10 [8.03, 8.19] | 10.99 [10.84, 11.15] | 8.48 [8.40, 8.57] | 8.44 [8.34, 8.54] | 5.61 [5.56, 5.66] | 7.30 [7.21, 7.39] | 8.69 [8.59, 8.80] |  | 8.22 [8.15, 8.31] |
| 21 | 8.52 [8.43, 8.61] | 8.26 [8.18, 8.35] | 11.10 [10.94, 11.27] | 8.69 [8.61, 8.79] | 8.65 [8.55, 8.76] | 5.67 [5.62, 5.73] | 7.54 [7.46, 7.64] | 8.97 [8.86, 9.10] |  | 8.40 [8.31, 8.49] |
| 30 | 8.63 [8.54, 8.72] | 8.40 [8.31, 8.49] | 11.25 [11.07, 11.44] | 8.81 [8.72, 8.90] | 9.01 [8.89, 9.14] | 5.71 [5.65, 5.76] | 7.66 [7.57, 7.75] | 9.48 [9.35, 9.62] |  | 8.56 [8.47, 8.65] |
| 45 | 8.70 [8.60, 8.79] | 8.51 [8.41, 8.60] | 11.42 [11.14, 11.72] | 8.93 [8.83, 9.03] | 9.27 [9.11, 9.43] | 5.73 [5.67, 5.79] | 7.82 [7.71, 7.92] | 9.92 [9.76, 10.09] |  | 8.69 [8.59, 8.78] |
| 60 | 8.75 [8.64, 8.85] | 8.63 [8.53, 8.73] |  | 9.07 [8.97, 9.17] | 4.48 [3.09, 6.00] | 5.76 [5.69, 5.82] | 7.99 [7.88, 8.10] | 4.60 [3.22, 6.12] |  | 8.82 [8.71, 8.92] |
| 90 | 8.97 [8.86, 9.09] | 8.71 [8.60, 8.82] |  | 9.25 [9.14, 9.37] |  | 5.79 [5.72, 5.85] | 8.10 [7.98, 8.22] |  |  | 8.91 [8.80, 9.02] |

**Paired difference vs the per-lead reference, all**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.52 [0.50, 0.55] | -0.07 [-0.08, -0.07] | 2.69 [2.60, 2.78] | 0.24 [0.21, 0.27] | 0.09 [0.04, 0.14] | -2.24 [-2.29, -2.19] | -1.08 [-1.13, -1.03] |  | 1.06 [0.99, 1.13] |  | wt_med |
| 2 | 0.50 [0.48, 0.53] | -0.08 [-0.08, -0.07] | 2.64 [2.55, 2.73] | 0.21 [0.18, 0.24] | 0.12 [0.07, 0.17] | -2.28 [-2.33, -2.23] | -1.12 [-1.17, -1.08] |  | 1.01 [0.95, 1.08] |  | wt_med |
| 3 | 0.49 [0.46, 0.52] | -0.08 [-0.09, -0.08] | 2.62 [2.53, 2.71] | 0.18 [0.15, 0.21] | 0.18 [0.13, 0.23] | -2.31 [-2.36, -2.26] | -1.16 [-1.21, -1.11] | 0.56 [0.50, 0.62] | 0.98 [0.91, 1.04] |  | wt_med |
| 4 | 0.48 [0.45, 0.51] | -0.08 [-0.09, -0.08] | 2.61 [2.51, 2.70] | 0.15 [0.12, 0.19] | 0.19 [0.14, 0.24] | -2.33 [-2.37, -2.28] | -1.20 [-1.25, -1.15] | 0.59 [0.53, 0.65] | 0.94 [0.87, 1.00] |  | wt_med |
| 5 | 0.47 [0.44, 0.50] | -0.08 [-0.09, -0.08] | 2.59 [2.50, 2.67] | 0.13 [0.10, 0.16] | 0.23 [0.17, 0.27] | -2.35 [-2.40, -2.30] | -1.23 [-1.28, -1.18] | 0.64 [0.58, 0.70] | 0.90 [0.83, 0.97] |  | wt_med |
| 6 | 0.45 [0.42, 0.48] | -0.09 [-0.09, -0.08] | 2.84 [2.75, 2.93] | 0.09 [0.06, 0.12] | 0.24 [0.19, 0.29] | -2.39 [-2.44, -2.34] | -1.29 [-1.34, -1.24] | 0.60 [0.54, 0.66] | 0.83 [0.76, 0.90] |  | wt_med |
| 7 | 0.37 [0.34, 0.40] | -0.09 [-0.10, -0.09] | 2.89 [2.81, 2.99] | 0.33 [0.30, 0.36] | 0.32 [0.27, 0.37] | -2.47 [-2.52, -2.41] | -0.90 [-0.94, -0.84] | 0.71 [0.65, 0.78] |  |  | wt_med |
| 10 | 0.33 [0.29, 0.36] | -0.10 [-0.10, -0.09] | 2.88 [2.78, 2.98] | 0.25 [0.22, 0.28] | 0.37 [0.32, 0.42] | -2.55 [-2.60, -2.50] | -1.01 [-1.06, -0.96] | 0.77 [0.70, 0.83] |  |  | wt_med |
| 14 | 0.26 [0.23, 0.29] | -0.10 [-0.11, -0.10] | 2.82 [2.71, 2.92] | 0.37 [0.34, 0.40] | 0.43 [0.37, 0.49] | -2.64 [-2.69, -2.58] | -0.82 [-0.88, -0.77] | 0.85 [0.78, 0.92] |  |  | wt_med |
| 21 | 0.20 [0.16, 0.23] | -0.11 [-0.12, -0.11] | 2.70 [2.60, 2.81] | 0.38 [0.35, 0.42] | 0.41 [0.36, 0.47] | -2.75 [-2.81, -2.70] | -0.78 [-0.83, -0.72] | 0.86 [0.78, 0.93] |  |  | wt_med |
| 30 | 0.16 [0.12, 0.19] | -0.13 [-0.13, -0.12] | 2.63 [2.52, 2.74] | 0.35 [0.32, 0.38] | 0.44 [0.38, 0.50] | -2.87 [-2.93, -2.82] | -0.80 [-0.86, -0.75] | 0.99 [0.90, 1.08] |  |  | wt_med |
| 45 | 0.13 [0.10, 0.17] | -0.14 [-0.14, -0.13] | 1.86 [1.68, 2.04] | 0.39 [0.35, 0.42] | 0.44 [0.37, 0.53] | -2.99 [-3.05, -2.92] | -0.74 [-0.80, -0.68] | 1.15 [1.04, 1.25] |  |  | wt_med |
| 60 |  | -0.20 [-0.24, -0.16] |  | 0.38 [0.32, 0.44] | 0.52 [-0.03, 1.15] | -3.10 [-3.17, -3.03] | -0.73 [-0.80, -0.65] | 0.41 [-0.16, 1.05] |  | -0.07 [-0.11, -0.04] | clim |
| 90 | 0.20 [0.16, 0.24] | -0.14 [-0.15, -0.13] |  | 0.51 [0.46, 0.56] |  | -3.16 [-3.23, -3.08] | -0.64 [-0.71, -0.56] |  |  |  | wt_med |

**Bias, all**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -1.21 | -1.57 | -5.71 | -1.04 | -1.29 | -0.48 | -0.00 |  | 0.09 | -1.28 |
| 2 | -1.16 | -1.55 | -5.75 | -1.03 | -1.29 | -0.49 | 0.01 |  | 0.10 | -1.26 |
| 3 | -1.14 | -1.53 | -5.77 | -1.01 | -1.26 | -0.48 | 0.02 | 0.69 | 0.10 | -1.24 |
| 4 | -1.12 | -1.52 | -5.80 | -1.00 | -1.22 | -0.48 | 0.04 | 0.76 | 0.10 | -1.23 |
| 5 | -1.10 | -1.50 | -5.80 | -0.99 | -1.22 | -0.48 | 0.04 | 0.78 | 0.10 | -1.21 |
| 6 | -1.07 | -1.48 | -6.19 | -0.97 | -1.34 | -0.47 | 0.04 | 0.65 | 0.09 | -1.19 |
| 7 | -1.03 | -1.47 | -6.15 | -0.93 | -1.30 | -0.46 | 0.13 | 0.68 |  | -1.17 |
| 10 | -0.97 | -1.45 | -6.27 | -0.92 | -1.22 | -0.46 | 0.13 | 0.76 |  | -1.15 |
| 14 | -0.97 | -1.44 | -6.34 | -0.88 | -1.14 | -0.43 | 0.19 | 0.89 |  | -1.13 |
| 21 | -0.90 | -1.38 | -6.38 | -0.82 | -1.01 | -0.39 | 0.24 | 1.01 |  | -1.06 |
| 30 | -0.67 | -1.23 | -6.50 | -0.70 | -1.21 | -0.36 | 0.35 | 0.77 |  | -0.91 |
| 45 | -0.24 | -0.96 | -5.50 | -0.54 | -0.82 | -0.31 | 0.46 | 0.89 |  | -0.64 |
| 60 | 0.10 | -0.73 |  | -0.40 | -1.90 | -0.27 | 0.55 | -1.58 |  | -0.40 |
| 90 | 0.57 | -0.35 |  | -0.10 |  | -0.23 | 0.82 |  |  | -0.02 |

**MAE, busy** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 15.61 [15.47, 15.77] | 14.52 [14.39, 14.67] | 18.96 [18.67, 19.23] | 14.69 [14.55, 14.84] | 13.91 [13.75, 14.05] | 9.69 [9.61, 9.77] | 11.65 [11.49, 11.79] |  | 16.60 [16.41, 16.79] | 14.72 [14.59, 14.86] |
| 2 | 15.63 [15.48, 15.78] | 14.58 [14.44, 14.72] | 19.00 [18.70, 19.28] | 14.68 [14.53, 14.82] | 14.05 [13.89, 14.21] | 9.68 [9.61, 9.75] | 11.62 [11.47, 11.77] |  | 16.59 [16.40, 16.76] | 14.78 [14.64, 14.92] |
| 3 | 15.63 [15.48, 15.79] | 14.62 [14.48, 14.75] | 19.04 [18.74, 19.34] | 14.67 [14.53, 14.82] | 14.25 [14.08, 14.41] | 9.68 [9.61, 9.75] | 11.60 [11.44, 11.75] | 15.07 [14.89, 15.24] | 16.56 [16.38, 16.75] | 14.83 [14.69, 14.96] |
| 4 | 15.64 [15.49, 15.79] | 14.65 [14.52, 14.79] | 19.11 [18.83, 19.41] | 14.66 [14.53, 14.81] | 14.33 [14.17, 14.49] | 9.69 [9.61, 9.76] | 11.58 [11.43, 11.72] | 15.17 [15.00, 15.34] | 16.53 [16.36, 16.70] | 14.87 [14.73, 15.01] |
| 5 | 15.66 [15.51, 15.82] | 14.70 [14.58, 14.85] | 19.13 [18.83, 19.43] | 14.67 [14.54, 14.82] | 14.50 [14.34, 14.68] | 9.70 [9.63, 9.78] | 11.57 [11.42, 11.74] | 15.34 [15.18, 15.52] | 16.51 [16.34, 16.71] | 14.93 [14.80, 15.08] |
| 6 | 15.71 [15.56, 15.87] | 14.80 [14.67, 14.95] | 19.69 [19.40, 20.00] | 14.70 [14.55, 14.85] | 14.61 [14.44, 14.80] | 9.73 [9.65, 9.81] | 11.57 [11.40, 11.73] | 15.36 [15.17, 15.55] | 16.48 [16.29, 16.66] | 15.03 [14.90, 15.18] |
| 7 | 15.75 [15.60, 15.92] | 14.99 [14.86, 15.14] | 20.00 [19.69, 20.30] | 15.35 [15.20, 15.51] | 14.95 [14.78, 15.13] | 9.78 [9.70, 9.85] | 12.42 [12.25, 12.58] | 15.78 [15.60, 15.96] |  | 15.23 [15.10, 15.38] |
| 10 | 15.84 [15.70, 16.01] | 15.17 [15.03, 15.32] | 20.26 [19.95, 20.60] | 15.38 [15.23, 15.54] | 15.28 [15.09, 15.44] | 9.81 [9.73, 9.89] | 12.40 [12.25, 12.57] | 16.13 [15.96, 16.32] |  | 15.42 [15.28, 15.57] |
| 14 | 15.97 [15.82, 16.13] | 15.43 [15.28, 15.57] | 20.48 [20.16, 20.81] | 15.83 [15.69, 15.99] | 15.73 [15.54, 15.92] | 9.92 [9.84, 9.99] | 12.91 [12.75, 13.09] | 16.64 [16.45, 16.83] |  | 15.70 [15.55, 15.85] |
| 21 | 16.15 [16.00, 16.32] | 15.68 [15.52, 15.82] | 20.68 [20.34, 21.01] | 16.16 [16.00, 16.32] | 16.10 [15.89, 16.28] | 10.00 [9.92, 10.08] | 13.27 [13.10, 13.45] | 17.14 [16.93, 17.35] |  | 15.98 [15.82, 16.13] |
| 30 | 16.31 [16.15, 16.48] | 15.86 [15.70, 16.02] | 20.84 [20.46, 21.22] | 16.27 [16.10, 16.44] | 16.77 [16.55, 17.01] | 9.99 [9.90, 10.07] | 13.41 [13.22, 13.58] | 18.02 [17.78, 18.27] |  | 16.20 [16.03, 16.36] |
| 45 | 16.49 [16.31, 16.65] | 16.08 [15.90, 16.24] | 17.96 [17.53, 18.43] | 16.49 [16.31, 16.67] | 17.35 [17.03, 17.64] | 10.00 [9.92, 10.09] | 13.65 [13.45, 13.84] | 18.86 [18.55, 19.15] |  | 16.44 [16.26, 16.60] |
| 60 | 16.61 [16.42, 16.80] | 16.37 [16.20, 16.53] |  | 16.77 [16.58, 16.94] | 19.57 [14.00, 23.79] | 10.04 [9.95, 10.13] | 13.99 [13.77, 14.19] | 20.67 [15.20, 24.79] |  | 16.73 [16.56, 16.90] |
| 90 | 16.93 [16.71, 17.16] | 16.51 [16.31, 16.72] |  | 17.07 [16.85, 17.30] |  | 10.12 [10.02, 10.23] | 14.26 [14.02, 14.53] |  |  | 16.90 [16.68, 17.11] |

**Paired difference vs the per-lead reference, busy**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.01 [0.93, 1.08] | -0.19 [-0.20, -0.18] | 4.52 [4.28, 4.74] | 0.06 [-0.01, 0.14] | -0.49 [-0.60, -0.37] | -5.05 [-5.16, -4.94] | -2.97 [-3.08, -2.87] |  | 1.93 [1.77, 2.08] |  | wt_med |
| 2 | 0.96 [0.89, 1.03] | -0.20 [-0.21, -0.19] | 4.47 [4.22, 4.70] | -0.01 [-0.08, 0.07] | -0.41 [-0.53, -0.29] | -5.12 [-5.24, -5.01] | -3.06 [-3.15, -2.95] |  | 1.84 [1.69, 2.00] |  | wt_med |
| 3 | 0.92 [0.85, 1.00] | -0.21 [-0.22, -0.19] | 4.44 [4.20, 4.70] | -0.07 [-0.14, 0.01] | -0.27 [-0.37, -0.15] | -5.17 [-5.28, -5.06] | -3.13 [-3.24, -3.03] | 0.65 [0.51, 0.80] | 1.76 [1.60, 1.93] |  | wt_med |
| 4 | 0.90 [0.82, 0.97] | -0.21 [-0.22, -0.20] | 4.44 [4.22, 4.70] | -0.12 [-0.19, -0.04] | -0.25 [-0.37, -0.13] | -5.20 [-5.31, -5.09] | -3.19 [-3.29, -3.09] | 0.69 [0.54, 0.83] | 1.69 [1.53, 1.85] |  | wt_med |
| 5 | 0.88 [0.80, 0.95] | -0.21 [-0.23, -0.20] | 4.40 [4.16, 4.63] | -0.17 [-0.25, -0.09] | -0.13 [-0.26, -0.01] | -5.25 [-5.37, -5.14] | -3.26 [-3.37, -3.15] | 0.83 [0.68, 0.98] | 1.61 [1.45, 1.77] |  | wt_med |
| 6 | 0.82 [0.75, 0.90] | -0.22 [-0.23, -0.21] | 4.84 [4.57, 5.08] | -0.25 [-0.33, -0.17] | -0.12 [-0.24, -0.00] | -5.33 [-5.45, -5.21] | -3.37 [-3.48, -3.27] | 0.76 [0.62, 0.91] | 1.47 [1.31, 1.62] |  | wt_med |
| 7 | 0.69 [0.62, 0.76] | -0.23 [-0.24, -0.22] | 4.95 [4.69, 5.21] | 0.29 [0.21, 0.36] | 0.02 [-0.11, 0.14] | -5.48 [-5.61, -5.36] | -2.63 [-2.74, -2.52] | 1.00 [0.85, 1.15] |  |  | wt_med |
| 10 | 0.60 [0.52, 0.68] | -0.24 [-0.26, -0.23] | 4.99 [4.72, 5.26] | 0.12 [0.04, 0.19] | 0.16 [0.02, 0.28] | -5.64 [-5.76, -5.52] | -2.85 [-2.96, -2.75] | 1.17 [1.01, 1.32] |  |  | wt_med |
| 14 | 0.47 [0.39, 0.54] | -0.26 [-0.27, -0.25] | 4.89 [4.61, 5.18] | 0.37 [0.29, 0.44] | 0.30 [0.16, 0.42] | -5.81 [-5.94, -5.69] | -2.54 [-2.66, -2.43] | 1.36 [1.19, 1.52] |  |  | wt_med |
| 21 | 0.36 [0.28, 0.44] | -0.28 [-0.30, -0.27] | 4.68 [4.38, 4.98] | 0.40 [0.32, 0.47] | 0.23 [0.09, 0.37] | -6.02 [-6.14, -5.88] | -2.48 [-2.59, -2.37] | 1.39 [1.22, 1.56] |  |  | wt_med |
| 30 | 0.30 [0.21, 0.38] | -0.31 [-0.32, -0.30] | 4.49 [4.17, 4.79] | 0.31 [0.23, 0.39] | 0.34 [0.18, 0.50] | -6.25 [-6.38, -6.11] | -2.55 [-2.67, -2.43] | 1.68 [1.47, 1.88] |  |  | wt_med |
| 45 | 0.29 [0.20, 0.38] | -0.33 [-0.35, -0.31] | 2.45 [2.05, 2.83] | 0.35 [0.27, 0.43] | 0.28 [0.08, 0.47] | -6.49 [-6.63, -6.34] | -2.49 [-2.63, -2.36] | 2.09 [1.85, 2.31] |  |  | wt_med |
| 60 |  | -0.50 [-0.60, -0.41] |  | 0.28 [0.13, 0.42] | 7.08 [4.45, 9.00] | -6.76 [-6.92, -6.59] | -2.48 [-2.67, -2.30] | 7.15 [5.06, 9.22] |  | -0.19 [-0.30, -0.10] | clim |
| 90 | 0.34 [0.24, 0.44] | -0.33 [-0.35, -0.31] |  | 0.65 [0.54, 0.77] |  | -6.83 [-7.02, -6.63] | -2.12 [-2.28, -1.95] |  |  |  | wt_med |

**Bias, busy**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | -0.59 | -1.19 | -14.12 | -0.72 | -1.90 | -0.83 | 0.69 |  | 0.79 | -0.64 |
| 2 | -0.42 | -1.08 | -14.14 | -0.64 | -1.82 | -0.83 | 0.77 |  | 0.85 | -0.52 |
| 3 | -0.32 | -0.99 | -14.16 | -0.58 | -1.71 | -0.81 | 0.82 | 1.96 | 0.87 | -0.43 |
| 4 | -0.23 | -0.91 | -14.25 | -0.51 | -1.61 | -0.80 | 0.87 | 2.13 | 0.89 | -0.34 |
| 5 | -0.14 | -0.84 | -14.26 | -0.48 | -1.55 | -0.80 | 0.89 | 2.27 | 0.88 | -0.27 |
| 6 | -0.01 | -0.73 | -14.81 | -0.43 | -1.72 | -0.78 | 0.93 | 2.08 | 0.87 | -0.16 |
| 7 | 0.11 | -0.63 | -14.73 | -0.11 | -1.51 | -0.77 | 1.30 | 2.30 |  | -0.04 |
| 10 | 0.34 | -0.49 | -14.94 | -0.02 | -1.15 | -0.76 | 1.37 | 2.68 |  | 0.11 |
| 14 | 0.49 | -0.28 | -14.94 | 0.31 | -0.81 | -0.70 | 1.72 | 3.09 |  | 0.33 |
| 21 | 0.83 | 0.06 | -14.89 | 0.66 | -0.24 | -0.62 | 2.05 | 3.72 |  | 0.69 |
| 30 | 1.50 | 0.67 | -14.92 | 1.11 | -0.12 | -0.57 | 2.47 | 3.32 |  | 1.30 |
| 45 | 2.49 | 1.40 | -10.37 | 1.56 | 1.20 | -0.47 | 2.81 | 3.69 |  | 2.04 |
| 60 | 3.18 | 1.95 |  | 1.88 | -5.88 | -0.45 | 3.09 | -5.81 |  | 2.58 |
| 90 | 3.88 | 2.58 |  | 2.30 |  | -0.33 | 3.49 |  |  | 3.21 |

**MAE by region (all rides)**

| lead | region | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Asia | 9.26 | 8.70 | 14.04 | 9.04 | 8.85 | 5.79 | 7.75 |  | 9.74 | 8.76 |
| 1 | EU | 6.33 | 5.87 | 8.00 | 6.03 | 5.74 | 4.16 | 4.99 |  | 6.53 | 5.92 |
| 1 | NA | 9.14 | 8.59 | 10.46 | 8.91 | 8.51 | 6.51 | 7.31 |  | 10.31 | 8.70 |
| 1 | all | 8.10 | 7.58 | 10.21 | 7.85 | 7.50 | 5.43 | 6.54 |  | 8.69 | 7.65 |
| 3 | Asia | 9.22 | 8.77 | 14.16 | 9.05 | 9.10 | 5.79 | 7.73 | 8.88 | 9.74 | 8.84 |
| 3 | EU | 6.38 | 5.95 | 8.02 | 6.06 | 5.84 | 4.19 | 4.99 | 6.13 | 6.53 | 6.01 |
| 3 | NA | 9.22 | 8.64 | 10.39 | 8.93 | 8.68 | 6.51 | 7.31 | 9.23 | 10.30 | 8.76 |
| 3 | all | 8.14 | 7.64 | 10.22 | 7.86 | 7.66 | 5.44 | 6.53 | 7.87 | 8.68 | 7.73 |
| 7 | Asia | 9.22 | 9.00 | 14.56 | 9.42 | 9.52 | 5.86 | 8.24 | 9.16 |  | 9.08 |
| 7 | EU | 6.48 | 6.16 | 8.75 | 6.38 | 6.24 | 4.27 | 5.41 | 6.50 |  | 6.24 |
| 7 | NA | 9.39 | 8.86 | 10.81 | 9.27 | 9.01 | 6.58 | 7.76 | 9.66 |  | 9.00 |
| 7 | all | 8.23 | 7.86 | 10.75 | 8.20 | 8.03 | 5.51 | 6.99 | 8.25 |  | 7.96 |
| 14 | Asia | 9.31 | 9.25 | 15.17 | 9.66 | 10.11 | 5.95 | 8.49 | 9.89 |  | 9.34 |
| 14 | EU | 6.60 | 6.39 | 8.98 | 6.69 | 6.57 | 4.36 | 5.75 | 6.79 |  | 6.48 |
| 14 | NA | 9.62 | 9.14 | 10.95 | 9.56 | 9.42 | 6.69 | 8.10 | 10.14 |  | 9.30 |
| 14 | all | 8.39 | 8.10 | 10.99 | 8.48 | 8.44 | 5.61 | 7.30 | 8.69 |  | 8.22 |
| 30 | Asia | 9.45 | 9.41 | 15.31 | 9.97 | 10.75 | 6.03 | 8.91 | 10.96 |  | 9.54 |
| 30 | EU | 6.87 | 6.71 | 9.35 | 7.10 | 7.37 | 4.52 | 6.17 | 7.63 |  | 6.84 |
| 30 | NA | 9.95 | 9.53 | 11.21 | 9.83 | 9.78 | 6.76 | 8.40 | 10.74 |  | 9.74 |
| 30 | all | 8.63 | 8.40 | 11.25 | 8.81 | 9.01 | 5.71 | 7.66 | 9.48 |  | 8.56 |

## UC3 — by season (MAE, all rides)

| L | season | n | snaive7 | wt_med | clim | h5 | lvlh5_naive | lvlh5_tft | lvlh5_cbd | prod_served | oracle_level | oracle_shape |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | autumn | 1800199.00 | 8.26 | 8.17 | 8.07 | 7.97 | 8.15 | 7.60 | 10.26 |  | 5.12 | 7.16 |
| 0 | spring | 3863936.00 | 9.47 | 7.99 | 8.61 | 7.91 | 8.38 | 8.06 | 9.39 |  | 5.64 | 7.10 |
| 0 | summer | 6202500.00 | 8.26 | 7.06 | 7.69 | 7.03 | 7.36 | 7.25 | 10.27 |  | 5.31 | 6.00 |
| 0 | winter | 256346.00 | 10.58 | 8.39 | 9.89 | 8.39 | 8.97 |  |  |  | 6.19 | 7.46 |
| 1 | autumn | 1796586.00 | 8.26 | 8.28 | 8.15 | 8.07 | 8.19 | 7.76 | 9.98 |  | 5.13 | 7.16 |
| 1 | spring | 3835735.00 | 9.47 | 8.13 | 8.63 | 8.04 | 8.41 | 8.05 |  |  | 5.67 | 7.10 |
| 1 | summer | 6188203.00 | 8.26 | 7.15 | 7.72 | 7.12 | 7.38 | 7.40 | 10.28 |  | 5.34 | 6.00 |
| 1 | winter | 230254.00 | 10.65 | 8.49 | 9.75 | 8.49 | 8.91 |  |  |  | 6.18 | 7.35 |
| 3 | autumn | 1791629.00 | 8.26 | 8.37 | 8.19 | 8.16 | 8.21 | 7.90 | 9.94 | 8.16 | 5.14 | 7.16 |
| 3 | spring | 3796568.00 | 9.47 | 8.23 | 8.66 | 8.13 | 8.43 | 8.04 |  | 7.72 | 5.69 | 7.10 |
| 3 | summer | 6167419.00 | 8.26 | 7.23 | 7.79 | 7.19 | 7.41 | 7.57 | 10.30 | 7.79 | 5.37 | 6.00 |
| 3 | winter | 172421.00 | 10.88 | 8.28 | 9.52 | 8.27 | 8.66 |  |  |  | 5.89 | 7.19 |
| 7 | autumn | 1783013.00 |  | 8.60 | 8.30 | 8.37 | 8.73 | 8.33 | 10.39 | 8.65 | 5.19 | 7.88 |
| 7 | spring | 3708385.00 |  | 8.50 | 8.73 | 8.38 | 8.82 | 9.10 |  | 10.45 | 5.77 | 7.57 |
| 7 | summer | 6124268.00 |  | 7.46 | 7.93 | 7.40 | 7.69 | 7.94 | 10.87 | 8.11 | 5.45 | 6.38 |
| 7 | winter | 75587.00 |  | 8.04 | 8.90 | 7.97 | 8.51 |  |  |  | 5.82 | 7.03 |
| 14 | autumn | 1774797.00 |  | 8.85 | 8.30 | 8.61 | 9.16 | 8.79 | 10.60 | 9.20 | 5.23 | 8.45 |
| 14 | spring | 3459271.00 |  | 8.75 | 8.92 | 8.62 | 9.08 |  |  |  | 5.92 | 7.80 |
| 14 | summer | 6039659.00 |  | 7.74 | 8.13 | 7.67 | 7.96 | 8.33 | 11.12 | 8.52 | 5.55 | 6.68 |
| 30 | autumn | 1760630.00 |  | 8.94 | 8.24 | 8.69 | 9.45 | 9.39 | 10.62 | 10.19 | 5.28 | 8.87 |
| 30 | spring | 2774276.00 |  | 9.05 | 9.27 | 8.88 | 9.31 |  |  |  | 6.03 | 8.03 |
| 30 | summer | 5832721.00 |  | 8.22 | 8.48 | 8.09 | 8.38 | 8.81 | 11.54 | 9.14 | 5.69 | 7.12 |

## UC4 — crowd calendar (daily park level = mean headliner P90)

| lead | model | park_months | spearman_within_month | busy_day_hit_rate | spread_iqr_pred | spread_iqr_true |
|---|---|---|---|---|---|---|
| 1 | lvl_naive4 | 614 | 0.25 [0.23, 0.28] | 0.41 [0.39, 0.44] | 9.64 | 14.51 |
| 1 | lvl_wt56 | 615 | 0.13 [0.11, 0.16] | 0.40 [0.38, 0.43] | 6.01 | 14.50 |
| 1 | lvl_snaive7 | 599 | 0.28 [0.25, 0.30] | 0.44 [0.41, 0.46] | 14.86 | 14.50 |
| 1 | lvl_clim | 615 | 0.17 [0.15, 0.20] | 0.51 [0.49, 0.54] | 6.25 | 14.50 |
| 1 | lvl_tft | 433 | 0.30 [0.27, 0.33] | 0.42 [0.40, 0.43] | 10.53 | 13.67 |
| 1 | lvl_cbd | 374 | 0.15 [0.12, 0.18] | 0.41 [0.38, 0.44] | 5.00 | 13.37 |
| 2 | lvl_naive4 | 562 | 0.24 [0.21, 0.26] | 0.41 [0.39, 0.43] | 9.87 | 14.52 |
| 2 | lvl_wt56 | 563 | 0.12 [0.09, 0.15] | 0.39 [0.37, 0.42] | 6.77 | 14.50 |
| 2 | lvl_snaive7 | 551 | 0.27 [0.25, 0.29] | 0.43 [0.41, 0.46] | 14.91 | 14.59 |
| 2 | lvl_clim | 563 | 0.17 [0.14, 0.20] | 0.50 [0.48, 0.53] | 5.93 | 14.50 |
| 2 | lvl_tft | 381 | 0.29 [0.26, 0.32] | 0.42 [0.40, 0.44] | 11.24 | 13.89 |
| 2 | lvl_cbd | 329 | 0.16 [0.13, 0.18] | 0.40 [0.37, 0.43] | 4.83 | 13.58 |
| 3 | lvl_naive4 | 533 | 0.23 [0.21, 0.26] | 0.41 [0.38, 0.43] | 9.86 | 14.50 |
| 3 | lvl_wt56 | 534 | 0.11 [0.08, 0.14] | 0.39 [0.36, 0.41] | 6.67 | 14.50 |
| 3 | lvl_snaive7 | 522 | 0.27 [0.24, 0.29] | 0.43 [0.41, 0.46] | 14.98 | 14.55 |
| 3 | lvl_clim | 534 | 0.15 [0.13, 0.18] | 0.49 [0.47, 0.52] | 5.82 | 14.50 |
| 3 | lvl_tft | 381 | 0.27 [0.25, 0.30] | 0.41 [0.39, 0.43] | 11.02 | 13.89 |
| 3 | lvl_cbd | 329 | 0.17 [0.14, 0.21] | 0.41 [0.38, 0.44] | 5.00 | 13.43 |
| 4 | lvl_naive4 | 533 | 0.23 [0.21, 0.26] | 0.41 [0.38, 0.43] | 9.88 | 14.59 |
| 4 | lvl_wt56 | 534 | 0.11 [0.08, 0.14] | 0.39 [0.36, 0.41] | 6.71 | 14.51 |
| 4 | lvl_snaive7 | 522 | 0.27 [0.24, 0.29] | 0.43 [0.41, 0.46] | 15.00 | 14.60 |
| 4 | lvl_clim | 534 | 0.15 [0.12, 0.18] | 0.49 [0.47, 0.52] | 5.65 | 14.51 |
| 4 | lvl_tft | 381 | 0.28 [0.25, 0.30] | 0.40 [0.38, 0.42] | 11.52 | 13.91 |
| 4 | lvl_cbd | 329 | 0.19 [0.16, 0.22] | 0.42 [0.38, 0.45] | 5.42 | 13.58 |
| 5 | lvl_naive4 | 528 | 0.23 [0.21, 0.26] | 0.41 [0.38, 0.43] | 9.86 | 14.56 |
| 5 | lvl_wt56 | 529 | 0.10 [0.07, 0.13] | 0.38 [0.35, 0.41] | 6.52 | 14.50 |
| 5 | lvl_snaive7 | 521 | 0.27 [0.25, 0.30] | 0.43 [0.41, 0.46] | 14.96 | 14.60 |
| 5 | lvl_clim | 529 | 0.14 [0.11, 0.17] | 0.49 [0.46, 0.51] | 5.42 | 14.50 |
| 5 | lvl_tft | 377 | 0.25 [0.23, 0.28] | 0.40 [0.38, 0.42] | 11.71 | 13.93 |
| 5 | lvl_cbd | 327 | 0.22 [0.19, 0.25] | 0.43 [0.40, 0.46] | 5.80 | 13.58 |
| 6 | lvl_naive4 | 526 | 0.23 [0.21, 0.26] | 0.41 [0.38, 0.43] | 9.81 | 14.62 |
| 6 | lvl_wt56 | 527 | 0.10 [0.07, 0.13] | 0.38 [0.35, 0.41] | 6.62 | 14.53 |
| 6 | lvl_snaive7 | 521 | 0.27 [0.24, 0.29] | 0.43 [0.41, 0.46] | 14.86 | 14.52 |
| 6 | lvl_clim | 527 | 0.13 [0.11, 0.16] | 0.48 [0.45, 0.50] | 5.44 | 14.53 |
| 6 | lvl_tft | 375 | 0.25 [0.22, 0.27] | 0.40 [0.38, 0.42] | 11.14 | 13.94 |
| 6 | lvl_cbd | 327 | 0.13 [0.10, 0.16] | 0.40 [0.37, 0.44] | 4.51 | 13.65 |
| 7 | lvl_naive4 | 526 | 0.17 [0.14, 0.20] | 0.39 [0.36, 0.41] | 10.09 | 14.62 |
| 7 | lvl_wt56 | 527 | 0.07 [0.04, 0.10] | 0.37 [0.34, 0.40] | 6.42 | 14.45 |
| 7 | lvl_clim | 527 | 0.13 [0.10, 0.16] | 0.47 [0.45, 0.50] | 5.60 | 14.45 |
| 7 | lvl_tft | 375 | 0.19 [0.16, 0.22] | 0.37 [0.35, 0.39] | 11.68 | 13.94 |
| 7 | lvl_cbd | 327 | 0.08 [0.05, 0.12] | 0.37 [0.33, 0.40] | 4.17 | 13.65 |
| 10 | lvl_naive4 | 526 | 0.18 [0.15, 0.20] | 0.39 [0.37, 0.41] | 10.09 | 14.50 |
| 10 | lvl_wt56 | 527 | 0.07 [0.04, 0.10] | 0.39 [0.36, 0.41] | 6.64 | 14.37 |
| 10 | lvl_clim | 527 | 0.12 [0.09, 0.16] | 0.44 [0.42, 0.46] | 6.74 | 14.37 |
| 10 | lvl_tft | 375 | 0.19 [0.16, 0.21] | 0.37 [0.35, 0.39] | 11.33 | 13.82 |
| 10 | lvl_cbd | 326 | 0.08 [0.05, 0.11] | 0.38 [0.35, 0.41] | 4.13 | 13.62 |
| 14 | lvl_naive4 | 522 | 0.14 [0.12, 0.17] | 0.37 [0.34, 0.40] | 10.08 | 14.29 |
| 14 | lvl_wt56 | 524 | 0.06 [0.03, 0.09] | 0.36 [0.34, 0.39] | 6.33 | 14.24 |
| 14 | lvl_clim | 524 | 0.12 [0.09, 0.15] | 0.43 [0.41, 0.45] | 7.09 | 14.24 |
| 14 | lvl_tft | 373 | 0.15 [0.12, 0.18] | 0.33 [0.31, 0.35] | 11.60 | 13.65 |
| 14 | lvl_cbd | 295 | 0.04 [0.01, 0.07] | 0.34 [0.31, 0.38] | 3.94 | 13.82 |
| 21 | lvl_naive4 | 497 | 0.13 [0.10, 0.16] | 0.37 [0.34, 0.40] | 9.83 | 14.31 |
| 21 | lvl_wt56 | 510 | 0.06 [0.03, 0.09] | 0.35 [0.33, 0.38] | 6.45 | 14.17 |
| 21 | lvl_clim | 510 | 0.12 [0.09, 0.15] | 0.45 [0.43, 0.48] | 6.29 | 14.17 |
| 21 | lvl_tft | 366 | 0.13 [0.09, 0.16] | 0.35 [0.33, 0.37] | 11.28 | 13.66 |
| 21 | lvl_cbd | 288 | 0.02 [-0.02, 0.05] | 0.34 [0.31, 0.38] | 4.00 | 13.70 |
| 30 | lvl_naive4 | 451 | 0.14 [0.11, 0.17] | 0.37 [0.34, 0.39] | 9.67 | 14.28 |
| 30 | lvl_wt56 | 451 | 0.05 [0.01, 0.08] | 0.36 [0.33, 0.39] | 6.76 | 14.25 |
| 30 | lvl_clim | 451 | 0.15 [0.12, 0.18] | 0.49 [0.46, 0.52] | 6.00 | 14.25 |
| 30 | lvl_tft | 269 | 0.10 [0.06, 0.14] | 0.33 [0.30, 0.35] | 10.40 | 13.58 |
| 30 | lvl_cbd | 235 | 0.01 [-0.03, 0.05] | 0.34 [0.30, 0.38] | 4.50 | 13.45 |
| 45 | lvl_naive4 | 417 | 0.16 [0.13, 0.19] | 0.38 [0.36, 0.41] | 10.04 | 13.47 |
| 45 | lvl_wt56 | 418 | 0.10 [0.07, 0.13] | 0.38 [0.35, 0.41] | 6.38 | 13.63 |
| 45 | lvl_clim | 418 | 0.12 [0.08, 0.15] | 0.43 [0.41, 0.46] | 6.18 | 13.63 |
| 45 | lvl_tft | 180 | 0.17 [0.13, 0.21] | 0.35 [0.33, 0.38] | 10.96 | 14.12 |
| 45 | lvl_cbd | 82 | 0.13 [0.06, 0.20] | 0.39 [0.33, 0.46] | 4.48 | 14.45 |
| 60 | lvl_naive4 | 354 | 0.16 [0.13, 0.20] | 0.39 [0.36, 0.42] | 9.98 | 13.18 |
| 60 | lvl_wt56 | 354 | 0.12 [0.08, 0.16] | 0.39 [0.36, 0.42] | 6.03 | 13.16 |
| 60 | lvl_clim | 354 | 0.16 [0.13, 0.20] | 0.49 [0.45, 0.53] | 4.73 | 13.16 |
| 60 | lvl_tft | 1 | 0.14 [0.14, 0.14] | 0.20 [0.20, 0.20] | 9.56 | 9.90 |
| 90 | lvl_naive4 | 264 | 0.15 [0.11, 0.19] | 0.37 [0.34, 0.40] | 10.27 | 12.80 |
| 90 | lvl_wt56 | 264 | 0.16 [0.12, 0.21] | 0.42 [0.39, 0.46] | 6.82 | 12.80 |
| 90 | lvl_clim | 264 | 0.15 [0.11, 0.20] | 0.49 [0.45, 0.53] | 5.77 | 12.80 |
| 120 | lvl_naive4 | 181 | 0.21 [0.16, 0.26] | 0.40 [0.36, 0.44] | 10.55 | 12.46 |
| 120 | lvl_wt56 | 181 | 0.14 [0.09, 0.20] | 0.44 [0.39, 0.49] | 7.02 | 12.58 |
| 120 | lvl_clim | 181 | 0.13 [0.08, 0.19] | 0.52 [0.47, 0.57] | 6.33 | 12.58 |
| 180 | lvl_naive4 | 64 | 0.33 [0.25, 0.40] | 0.44 [0.37, 0.51] | 11.10 | 12.02 |
| 180 | lvl_wt56 | 64 | 0.31 [0.20, 0.41] | 0.56 [0.47, 0.65] | 8.20 | 12.02 |
| 180 | lvl_clim | 64 | 0.26 [0.16, 0.36] | 0.63 [0.54, 0.70] | 9.16 | 12.02 |

## D1 — next-best ride

Suggest a ride when the forecast maximum in the next 120 min is ≥ 10 min above the live wait (`lib/planner/next-best-ride.ts`, walking time 0). Correct when the TRUE maximum in that window is also ≥ 10 min above the live wait. `lead` = minutes until the predicted peak. False-suggestion rate = 1 − precision.

**next-best precision**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.30 [0.29, 0.30] | 0.28 [0.27, 0.28] | 0.31 [0.30, 0.31] | 0.28 [0.27, 0.29] | 0.37 [0.36, 0.37] | 0.29 [0.29, 0.30] |
| 60-120 | 0.47 [0.47, 0.48] | 0.48 [0.47, 0.48] | 0.51 [0.50, 0.51] | 0.46 [0.45, 0.46] | 0.51 [0.50, 0.51] | 0.47 [0.47, 0.48] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|
| 0-60 | -0.04 [-0.05, -0.04] | -0.05 [-0.05, -0.04] | -0.02 [-0.03, -0.02] | -0.03 [-0.04, -0.03] | -0.04 [-0.04, -0.04] | snaive7 |
| 60-120 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.02] | 0.03 [0.03, 0.03] | 0.01 [0.00, 0.01] | 0.00 [-0.00, 0.01] | snaive7 |

**next-best precision (top-3)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.37 [0.36, 0.37] | 0.36 [0.35, 0.36] | 0.38 [0.37, 0.38] | 0.35 [0.34, 0.35] | 0.43 [0.42, 0.43] | 0.37 [0.37, 0.38] |
| 60-120 | 0.56 [0.56, 0.57] | 0.56 [0.56, 0.57] | 0.57 [0.57, 0.58] | 0.53 [0.52, 0.54] | 0.59 [0.59, 0.60] | 0.57 [0.56, 0.57] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|
| 0-60 | -0.03 [-0.04, -0.03] | -0.03 [-0.04, -0.03] | -0.02 [-0.03, -0.02] | -0.03 [-0.03, -0.02] | -0.02 [-0.03, -0.02] | snaive7 |
| 60-120 | 0.00 [-0.00, 0.01] | 0.02 [0.01, 0.02] | 0.01 [0.01, 0.02] | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.02] | snaive7 |

**next-best recall**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med |
|---|---|---|---|---|---|---|
| 0-60 | 0.49 [0.48, 0.50] | 0.37 [0.36, 0.37] | 0.36 [0.35, 0.37] | 0.36 [0.35, 0.36] | 0.65 [0.64, 0.65] | 0.50 [0.49, 0.50] |
| 60-120 | 0.53 [0.53, 0.54] | 0.46 [0.46, 0.47] | 0.44 [0.44, 0.45] | 0.44 [0.43, 0.44] | 0.62 [0.62, 0.63] | 0.54 [0.53, 0.54] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | wt_med | ref |
|---|---|---|---|---|---|---|
| 0-60 | -0.16 [-0.17, -0.15] | -0.29 [-0.29, -0.28] | -0.30 [-0.30, -0.29] | -0.30 [-0.31, -0.29] | -0.16 [-0.16, -0.15] | snaive7 |
| 60-120 | -0.09 [-0.09, -0.08] | -0.16 [-0.17, -0.15] | -0.18 [-0.19, -0.18] | -0.19 [-0.20, -0.19] | -0.08 [-0.09, -0.08] | snaive7 |

## D2 — plan-block live correction (0–45 min, headliners)

Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`), i.e. `persistence` is what the frontend serves in this window.

**live-window MAE (headliners)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| w0-45 | 13.27 [13.15, 13.40] | 12.19 [12.08, 12.31] | 12.49 [12.36, 12.61] | 11.53 [11.41, 11.66] | 6.29 [6.24, 6.35] | 14.15 [14.00, 14.28] | 12.33 [12.22, 12.45] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|
| w0-45 | 6.96 [6.85, 7.08] | 5.89 [5.77, 5.99] | 6.18 [6.07, 6.29] | 5.36 [5.25, 5.47] | 7.80 [7.65, 7.93] | 6.03 [5.92, 6.14] | persistence |

**live-window bias (headliners)**

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|
| w0-45 | -1.43 [-1.64, -1.22] | -1.96 [-2.14, -1.77] | -1.45 [-1.64, -1.27] | -2.01 [-2.21, -1.81] | -0.20 [-0.23, -0.17] | 0.18 [-0.04, 0.38] | -1.52 [-1.70, -1.32] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_naive | lvlh5_tft | persistence | wt_med | ref |
|---|---|---|---|---|---|---|---|
| w0-45 | -1.66 [-1.88, -1.45] | -2.16 [-2.33, -1.97] | -1.66 [-1.82, -1.49] | -1.95 [-2.16, -1.72] | -0.34 [-0.57, -0.13] | -1.71 [-1.89, -1.53] | snaive7 |

## D3 — best time of the day

Ride-days with ≥ 8 truth slots and a non-flat truth. Hit = a true minimum slot is among the predicted 2 lowest slots / within ±30 min of the predicted best; regret = true wait at the predicted best slot − true minimum.

**best-time hit rate (top-2)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.78, 0.79] |  | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] |
| 1 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.74 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.79] |  | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] |
| 2 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.74 [0.74, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.79] |  | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] |
| 3 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.74 [0.74, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.79] | 0.75 [0.75, 0.75] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] |
| 4 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.74 [0.74, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.75] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] |
| 5 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.74 [0.74, 0.74] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.75] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] |
| 6 | 0.80 [0.80, 0.80] | 0.82 [0.81, 0.82] | 0.73 [0.73, 0.73] | 0.80 [0.79, 0.80] | 0.78 [0.78, 0.78] | 0.75 [0.75, 0.75] | 0.75 [0.75, 0.76] | 0.81 [0.80, 0.81] |
| 7 | 0.80 [0.80, 0.80] | 0.82 [0.81, 0.82] | 0.73 [0.72, 0.73] | 0.80 [0.79, 0.80] | 0.78 [0.77, 0.78] | 0.75 [0.75, 0.75] |  | 0.81 [0.80, 0.81] |
| 10 | 0.80 [0.80, 0.80] | 0.82 [0.81, 0.82] | 0.72 [0.72, 0.73] | 0.79 [0.79, 0.80] | 0.77 [0.77, 0.78] | 0.75 [0.75, 0.75] |  | 0.81 [0.80, 0.81] |
| 14 | 0.80 [0.79, 0.80] | 0.81 [0.81, 0.82] | 0.72 [0.72, 0.72] | 0.79 [0.79, 0.79] | 0.77 [0.77, 0.77] | 0.75 [0.74, 0.75] |  | 0.80 [0.80, 0.81] |
| 21 | 0.79 [0.79, 0.80] | 0.81 [0.81, 0.81] | 0.72 [0.71, 0.72] | 0.79 [0.78, 0.79] | 0.77 [0.76, 0.77] | 0.75 [0.74, 0.75] |  | 0.80 [0.80, 0.80] |
| 30 | 0.79 [0.79, 0.79] | 0.81 [0.81, 0.81] | 0.71 [0.71, 0.72] | 0.78 [0.78, 0.79] | 0.77 [0.76, 0.77] | 0.74 [0.74, 0.75] |  | 0.80 [0.80, 0.80] |
| 45 | 0.79 [0.78, 0.79] | 0.81 [0.80, 0.81] | 0.71 [0.70, 0.72] | 0.78 [0.78, 0.78] | 0.77 [0.77, 0.78] | 0.74 [0.74, 0.75] |  | 0.79 [0.79, 0.80] |
| 60 | 0.79 [0.79, 0.79] | 0.80 [0.80, 0.81] |  | 0.78 [0.77, 0.78] | 0.72 [0.65, 0.79] | 0.75 [0.67, 0.81] |  | 0.79 [0.79, 0.79] |
| 90 | 0.78 [0.78, 0.79] | 0.80 [0.80, 0.81] |  | 0.77 [0.77, 0.78] |  |  |  | 0.79 [0.79, 0.79] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.02, -0.01] | 0.01 [0.01, 0.01] | -0.06 [-0.06, -0.06] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.01] |  | -0.06 [-0.06, -0.06] | wt_med |
| 1 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.06 [-0.06, -0.05] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.01] |  | -0.06 [-0.06, -0.05] | wt_med |
| 2 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.06 [-0.06, -0.05] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.02] |  | -0.06 [-0.06, -0.05] | wt_med |
| 3 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.06 [-0.06, -0.05] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.02] | -0.06 [-0.06, -0.05] | -0.06 [-0.06, -0.05] | wt_med |
| 4 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.06 [-0.06, -0.05] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.02] | -0.06 [-0.06, -0.05] | -0.06 [-0.06, -0.05] | wt_med |
| 5 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.06 [-0.06, -0.05] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.02] | -0.06 [-0.06, -0.06] | -0.06 [-0.06, -0.05] | wt_med |
| 6 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.07 [-0.07, -0.06] | -0.01 [-0.01, -0.01] | -0.02 [-0.02, -0.02] | -0.06 [-0.06, -0.06] | -0.05 [-0.06, -0.05] | wt_med |
| 7 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.07 [-0.07, -0.07] | -0.01 [-0.02, -0.01] | -0.02 [-0.03, -0.02] | -0.06 [-0.06, -0.05] |  | wt_med |
| 10 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.07 [-0.07, -0.07] | -0.01 [-0.02, -0.01] | -0.02 [-0.03, -0.02] | -0.06 [-0.06, -0.05] |  | wt_med |
| 14 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.07 [-0.07, -0.07] | -0.02 [-0.02, -0.01] | -0.02 [-0.03, -0.02] | -0.05 [-0.06, -0.05] |  | wt_med |
| 21 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.07 [-0.08, -0.07] | -0.02 [-0.02, -0.02] | -0.02 [-0.03, -0.02] | -0.05 [-0.05, -0.05] |  | wt_med |
| 30 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.08 [-0.08, -0.07] | -0.02 [-0.02, -0.02] | -0.02 [-0.03, -0.02] | -0.05 [-0.05, -0.05] |  | wt_med |
| 45 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.02] | -0.05 [-0.06, -0.05] | -0.02 [-0.02, -0.02] | -0.02 [-0.02, -0.01] | -0.05 [-0.05, -0.05] |  | wt_med |
| 60 | -0.00 [-0.01, -0.00] | 0.01 [0.01, 0.02] |  | -0.02 [-0.02, -0.02] | -0.09 [-0.15, -0.04] | -0.07 [-0.12, -0.02] |  | wt_med |
| 90 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] |  | -0.02 [-0.03, -0.02] |  |  |  | wt_med |

**best-time hit rate (±30 min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.81 [0.81, 0.81] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.77] | 0.82 [0.81, 0.82] | 0.80 [0.80, 0.81] |  | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] |
| 1 | 0.81 [0.80, 0.81] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] |  | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] |
| 2 | 0.81 [0.80, 0.81] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] |  | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] |
| 3 | 0.81 [0.80, 0.81] | 0.82 [0.82, 0.83] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] |
| 4 | 0.81 [0.80, 0.81] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.77 [0.76, 0.77] | 0.82 [0.81, 0.82] |
| 5 | 0.81 [0.80, 0.81] | 0.83 [0.82, 0.83] | 0.77 [0.77, 0.77] | 0.81 [0.81, 0.82] | 0.80 [0.80, 0.80] | 0.78 [0.78, 0.78] | 0.77 [0.76, 0.77] | 0.81 [0.81, 0.82] |
| 6 | 0.81 [0.80, 0.81] | 0.82 [0.82, 0.83] | 0.76 [0.76, 0.76] | 0.81 [0.81, 0.82] | 0.80 [0.79, 0.80] | 0.78 [0.78, 0.78] | 0.77 [0.76, 0.77] | 0.81 [0.81, 0.82] |
| 7 | 0.81 [0.80, 0.81] | 0.82 [0.82, 0.83] | 0.76 [0.76, 0.76] | 0.81 [0.81, 0.81] | 0.80 [0.79, 0.80] | 0.78 [0.78, 0.78] |  | 0.81 [0.81, 0.82] |
| 10 | 0.81 [0.80, 0.81] | 0.82 [0.82, 0.82] | 0.76 [0.75, 0.76] | 0.81 [0.81, 0.81] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] |  | 0.81 [0.81, 0.81] |
| 14 | 0.80 [0.80, 0.81] | 0.82 [0.82, 0.82] | 0.75 [0.75, 0.76] | 0.81 [0.81, 0.81] | 0.79 [0.79, 0.80] | 0.78 [0.77, 0.78] |  | 0.81 [0.81, 0.81] |
| 21 | 0.80 [0.80, 0.81] | 0.82 [0.82, 0.82] | 0.75 [0.75, 0.76] | 0.81 [0.80, 0.81] | 0.79 [0.79, 0.79] | 0.78 [0.77, 0.78] |  | 0.81 [0.81, 0.81] |
| 30 | 0.80 [0.80, 0.80] | 0.82 [0.82, 0.82] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.81] | 0.79 [0.79, 0.79] | 0.77 [0.77, 0.78] |  | 0.81 [0.80, 0.81] |
| 45 | 0.80 [0.79, 0.80] | 0.82 [0.81, 0.82] | 0.75 [0.74, 0.75] | 0.80 [0.80, 0.80] | 0.79 [0.79, 0.80] | 0.77 [0.77, 0.78] |  | 0.80 [0.80, 0.80] |
| 60 | 0.80 [0.79, 0.80] | 0.81 [0.81, 0.82] |  | 0.80 [0.79, 0.80] | 0.73 [0.66, 0.80] | 0.76 [0.69, 0.82] |  | 0.80 [0.80, 0.80] |
| 90 | 0.79 [0.79, 0.79] | 0.81 [0.81, 0.82] |  | 0.79 [0.79, 0.80] |  |  |  | 0.80 [0.80, 0.80] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|
| 0 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.04 [-0.04, -0.03] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] |  | -0.05 [-0.05, -0.05] | wt_med |
| 1 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.03 [-0.04, -0.03] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] |  | -0.05 [-0.05, -0.05] | wt_med |
| 2 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.03 [-0.04, -0.03] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] |  | -0.05 [-0.05, -0.05] | wt_med |
| 3 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.03 [-0.04, -0.03] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.04, -0.03] | -0.05 [-0.05, -0.05] | wt_med |
| 4 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.03 [-0.04, -0.03] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.04, -0.03] | -0.05 [-0.05, -0.05] | wt_med |
| 5 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.03 [-0.04, -0.03] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.04 [-0.04, -0.03] | -0.05 [-0.05, -0.05] | wt_med |
| 6 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.04 [-0.05, -0.04] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.04, -0.03] | -0.05 [-0.05, -0.05] | wt_med |
| 7 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.04 [-0.05, -0.04] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.04, -0.03] |  | wt_med |
| 10 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.05 [-0.05, -0.04] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.04, -0.03] |  | wt_med |
| 14 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.05 [-0.05, -0.04] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.03, -0.03] |  | wt_med |
| 21 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.05 [-0.05, -0.04] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.03 [-0.03, -0.03] |  | wt_med |
| 30 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] | -0.05 [-0.05, -0.05] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] | -0.03 [-0.03, -0.03] |  | wt_med |
| 45 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.02] | -0.03 [-0.03, -0.02] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.03 [-0.03, -0.03] |  | wt_med |
| 60 | -0.01 [-0.01, -0.00] | 0.01 [0.01, 0.02] |  | -0.01 [-0.01, -0.01] | -0.07 [-0.13, -0.02] | -0.05 [-0.10, -0.01] |  | wt_med |
| 90 | -0.01 [-0.01, -0.01] | 0.01 [0.01, 0.01] |  | -0.01 [-0.02, -0.01] |  |  |  | wt_med |

**best-time regret (min)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0 | 3.50 [3.44, 3.56] | 2.94 [2.89, 2.99] | 4.12 [4.05, 4.19] | 3.25 [3.20, 3.30] | 3.38 [3.32, 3.44] |  | 4.65 [4.58, 4.72] | 3.28 [3.23, 3.34] |
| 1 | 3.51 [3.45, 3.56] | 2.96 [2.91, 3.01] | 4.12 [4.05, 4.19] | 3.26 [3.21, 3.31] | 3.43 [3.37, 3.49] |  | 4.64 [4.58, 4.71] | 3.30 [3.24, 3.35] |
| 2 | 3.51 [3.46, 3.57] | 2.96 [2.92, 3.01] | 4.14 [4.07, 4.21] | 3.26 [3.22, 3.31] | 3.46 [3.39, 3.52] |  | 4.64 [4.57, 4.71] | 3.29 [3.24, 3.35] |
| 3 | 3.52 [3.46, 3.58] | 2.97 [2.93, 3.02] | 4.15 [4.08, 4.22] | 3.27 [3.22, 3.33] | 3.47 [3.41, 3.53] | 4.33 [4.25, 4.41] | 4.64 [4.57, 4.71] | 3.31 [3.26, 3.36] |
| 4 | 3.52 [3.46, 3.59] | 2.97 [2.93, 3.03] | 4.17 [4.10, 4.25] | 3.28 [3.23, 3.33] | 3.48 [3.42, 3.55] | 4.35 [4.27, 4.43] | 4.64 [4.57, 4.71] | 3.31 [3.26, 3.37] |
| 5 | 3.52 [3.47, 3.59] | 2.98 [2.93, 3.03] | 4.19 [4.11, 4.26] | 3.29 [3.24, 3.35] | 3.50 [3.44, 3.56] | 4.35 [4.27, 4.43] | 4.64 [4.57, 4.72] | 3.32 [3.27, 3.38] |
| 6 | 3.53 [3.48, 3.60] | 3.00 [2.95, 3.05] | 4.37 [4.30, 4.44] | 3.31 [3.25, 3.36] | 3.55 [3.49, 3.61] | 4.36 [4.28, 4.44] | 4.64 [4.57, 4.71] | 3.35 [3.30, 3.41] |
| 7 | 3.54 [3.48, 3.60] | 3.03 [2.98, 3.08] | 4.46 [4.38, 4.53] | 3.33 [3.28, 3.38] | 3.61 [3.55, 3.68] | 4.36 [4.28, 4.44] |  | 3.38 [3.33, 3.44] |
| 10 | 3.57 [3.50, 3.62] | 3.05 [3.00, 3.10] | 4.51 [4.44, 4.59] | 3.36 [3.31, 3.41] | 3.66 [3.59, 3.72] | 4.39 [4.31, 4.47] |  | 3.42 [3.36, 3.48] |
| 14 | 3.60 [3.53, 3.67] | 3.09 [3.04, 3.15] | 4.63 [4.54, 4.71] | 3.43 [3.38, 3.49] | 3.70 [3.64, 3.78] | 4.41 [4.33, 4.50] |  | 3.46 [3.40, 3.52] |
| 21 | 3.65 [3.59, 3.72] | 3.11 [3.06, 3.17] | 4.72 [4.63, 4.82] | 3.51 [3.45, 3.57] | 3.81 [3.74, 3.88] | 4.49 [4.40, 4.57] |  | 3.52 [3.45, 3.58] |
| 30 | 3.73 [3.66, 3.80] | 3.13 [3.07, 3.18] | 4.84 [4.75, 4.94] | 3.57 [3.51, 3.63] | 3.87 [3.79, 3.96] | 4.65 [4.54, 4.76] |  | 3.56 [3.50, 3.63] |
| 45 | 3.78 [3.71, 3.85] | 3.20 [3.14, 3.25] | 4.86 [4.70, 5.02] | 3.68 [3.62, 3.74] | 3.89 [3.79, 4.00] | 4.75 [4.61, 4.88] |  | 3.70 [3.63, 3.76] |
| 60 | 3.76 [3.69, 3.83] | 3.23 [3.17, 3.29] |  | 3.76 [3.69, 3.82] | 4.41 [2.93, 5.94] | 4.32 [2.99, 5.53] |  | 3.74 [3.66, 3.80] |
| 90 | 3.92 [3.83, 4.01] | 3.27 [3.20, 3.34] |  | 3.82 [3.75, 3.91] |  |  |  | 3.75 [3.67, 3.83] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | ref |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.31 [0.28, 0.34] | -0.34 [-0.37, -0.31] | 0.76 [0.70, 0.82] | 0.02 [-0.02, 0.05] | 0.05 [0.00, 0.09] |  | 1.42 [1.37, 1.47] | wt_med |
| 1 | 0.30 [0.26, 0.33] | -0.34 [-0.37, -0.31] | 0.73 [0.68, 0.79] | 0.01 [-0.03, 0.04] | 0.08 [0.03, 0.12] |  | 1.39 [1.34, 1.44] | wt_med |
| 2 | 0.30 [0.27, 0.34] | -0.33 [-0.37, -0.30] | 0.75 [0.69, 0.81] | 0.01 [-0.03, 0.05] | 0.09 [0.05, 0.14] |  | 1.38 [1.33, 1.44] | wt_med |
| 3 | 0.30 [0.26, 0.33] | -0.34 [-0.37, -0.31] | 0.74 [0.68, 0.80] | 0.01 [-0.03, 0.04] | 0.09 [0.04, 0.13] | 1.10 [1.05, 1.16] | 1.37 [1.31, 1.42] | wt_med |
| 4 | 0.29 [0.25, 0.33] | -0.34 [-0.37, -0.31] | 0.75 [0.69, 0.81] | 0.00 [-0.04, 0.04] | 0.09 [0.04, 0.13] | 1.10 [1.05, 1.17] | 1.36 [1.30, 1.41] | wt_med |
| 5 | 0.29 [0.25, 0.32] | -0.34 [-0.38, -0.31] | 0.74 [0.68, 0.81] | 0.00 [-0.04, 0.04] | 0.10 [0.06, 0.15] | 1.10 [1.04, 1.16] | 1.34 [1.29, 1.39] | wt_med |
| 6 | 0.27 [0.23, 0.30] | -0.36 [-0.39, -0.33] | 0.89 [0.82, 0.96] | -0.01 [-0.05, 0.02] | 0.12 [0.08, 0.17] | 1.07 [1.01, 1.13] | 1.31 [1.26, 1.36] | wt_med |
| 7 | 0.25 [0.22, 0.28] | -0.35 [-0.38, -0.32] | 0.95 [0.88, 1.02] | 0.03 [-0.01, 0.06] | 0.16 [0.11, 0.21] | 1.06 [1.00, 1.11] |  | wt_med |
| 10 | 0.23 [0.19, 0.27] | -0.37 [-0.40, -0.34] | 0.97 [0.90, 1.03] | 0.00 [-0.04, 0.04] | 0.16 [0.11, 0.20] | 1.03 [0.97, 1.10] |  | wt_med |
| 14 | 0.22 [0.18, 0.25] | -0.37 [-0.41, -0.34] | 1.02 [0.95, 1.09] | 0.06 [0.02, 0.10] | 0.15 [0.10, 0.20] | 1.00 [0.92, 1.06] |  | wt_med |
| 21 | 0.20 [0.16, 0.23] | -0.41 [-0.44, -0.37] | 1.05 [0.96, 1.13] | 0.08 [0.03, 0.12] | 0.17 [0.10, 0.22] | 0.95 [0.88, 1.01] |  | wt_med |
| 30 | 0.20 [0.16, 0.24] | -0.44 [-0.48, -0.40] | 1.08 [1.00, 1.17] | 0.07 [0.02, 0.12] | 0.12 [0.05, 0.19] | 0.94 [0.87, 1.02] |  | wt_med |
| 45 | 0.16 [0.12, 0.21] | -0.50 [-0.54, -0.46] | 0.72 [0.59, 0.86] | 0.06 [0.01, 0.11] | 0.02 [-0.08, 0.12] | 0.92 [0.82, 1.00] |  | wt_med |
| 60 | 0.10 [0.06, 0.15] | -0.50 [-0.55, -0.46] |  | 0.11 [0.05, 0.16] | 2.08 [0.82, 3.48] | 2.03 [1.06, 2.97] |  | wt_med |
| 90 | 0.23 [0.17, 0.28] | -0.48 [-0.53, -0.42] |  | 0.17 [0.10, 0.23] |  |  |  | wt_med |

## D4 — planner optimiser

Fixed set of the park's top 4 rides (headliners first, by ex-ante level). Objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; regret = true cost of the forecast's plan − true cost of the truth-optimal plan. Leads [1, 3, 7, 14, 30].

**optimiser regret (min per park-day)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 1 | 30.28 [29.72, 30.87] | 28.68 [28.15, 29.21] | 28.97 [28.21, 29.66] | 28.89 [28.33, 29.44] | 27.85 [27.24, 28.48] |  | 34.46 [33.79, 35.14] | 29.39 [28.86, 29.90] |
| 3 | 30.52 [29.97, 31.11] | 28.87 [28.38, 29.41] | 29.36 [28.67, 30.12] | 28.86 [28.31, 29.38] | 27.86 [27.24, 28.47] | 32.16 [31.55, 32.82] | 34.29 [33.71, 34.90] | 29.79 [29.25, 30.36] |
| 7 | 30.53 [29.93, 31.14] | 29.19 [28.66, 29.76] | 29.90 [29.14, 30.66] | 29.42 [28.85, 29.95] | 28.64 [28.00, 29.31] | 32.92 [32.26, 33.56] |  | 30.04 [29.49, 30.56] |
| 14 | 31.04 [30.47, 31.62] | 29.67 [29.10, 30.23] | 30.91 [30.13, 31.65] | 29.96 [29.37, 30.52] | 29.31 [28.64, 29.95] | 33.57 [32.88, 34.28] |  | 30.88 [30.30, 31.46] |
| 30 | 31.53 [30.93, 32.18] | 30.24 [29.71, 30.80] | 30.96 [30.07, 31.86] | 30.86 [30.27, 31.47] | 29.87 [29.09, 30.64] | 34.79 [33.97, 35.65] |  | 31.66 [31.10, 32.28] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.87 [0.46, 1.28] | -0.73 [-1.08, -0.38] | -0.14 [-0.73, 0.49] | -0.54 [-0.89, -0.16] | -0.68 [-1.11, -0.23] |  | 4.80 [4.21, 5.39] |  | wt_med |
| 3 | 0.61 [0.19, 1.04] | -0.91 [-1.24, -0.58] | -0.40 [-1.02, 0.21] | -1.02 [-1.43, -0.62] | -1.22 [-1.65, -0.73] | 2.77 [2.17, 3.34] | 4.12 [3.53, 4.70] |  | wt_med |
| 7 | 0.53 [0.12, 0.93] | -0.83 [-1.18, -0.46] | -0.12 [-0.78, 0.48] | -0.53 [-0.93, -0.13] | -0.46 [-0.93, 0.06] | 3.62 [3.04, 4.21] |  |  | wt_med |
| 14 | 0.26 [-0.17, 0.70] | -1.18 [-1.56, -0.79] | 0.01 [-0.67, 0.62] | -0.85 [-1.23, -0.42] | -0.55 [-1.07, 0.00] | 3.52 [2.88, 4.16] |  |  | wt_med |
| 30 |  | -1.29 [-1.76, -0.80] | -0.63 [-1.43, 0.10] | -0.58 [-1.10, -0.11] | -0.81 [-1.54, -0.11] | 3.86 [3.10, 4.63] |  | 0.05 [-0.42, 0.49] | clim |

**dayPeak pairwise ordering (headliners)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0 | 0.72 [0.71, 0.72] | 0.76 [0.76, 0.76] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.77 [0.76, 0.77] |  | 0.71 [0.71, 0.71] | 0.74 [0.73, 0.74] |
| 1 | 0.72 [0.71, 0.72] | 0.75 [0.75, 0.76] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.77] |  | 0.71 [0.71, 0.71] | 0.73 [0.73, 0.73] |
| 2 | 0.72 [0.71, 0.72] | 0.75 [0.75, 0.76] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] |  | 0.71 [0.71, 0.71] | 0.73 [0.73, 0.73] |
| 3 | 0.71 [0.71, 0.72] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.76, 0.76] | 0.71 [0.71, 0.72] | 0.71 [0.71, 0.71] | 0.73 [0.73, 0.73] |
| 4 | 0.71 [0.71, 0.72] | 0.75 [0.75, 0.75] | 0.70 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.75, 0.76] | 0.71 [0.71, 0.71] | 0.71 [0.71, 0.71] | 0.73 [0.73, 0.73] |
| 5 | 0.71 [0.71, 0.71] | 0.75 [0.75, 0.75] | 0.69 [0.69, 0.70] | 0.76 [0.76, 0.76] | 0.76 [0.75, 0.76] | 0.71 [0.71, 0.71] | 0.71 [0.71, 0.71] | 0.73 [0.73, 0.73] |
| 6 | 0.71 [0.71, 0.71] | 0.75 [0.75, 0.75] | 0.68 [0.68, 0.69] | 0.76 [0.76, 0.76] | 0.75 [0.75, 0.76] | 0.71 [0.70, 0.71] | 0.71 [0.71, 0.71] | 0.73 [0.72, 0.73] |
| 7 | 0.71 [0.71, 0.71] | 0.74 [0.74, 0.75] | 0.68 [0.67, 0.68] | 0.75 [0.75, 0.75] | 0.75 [0.75, 0.75] | 0.70 [0.70, 0.71] |  | 0.72 [0.72, 0.72] |
| 10 | 0.71 [0.70, 0.71] | 0.74 [0.74, 0.74] | 0.68 [0.67, 0.68] | 0.75 [0.75, 0.75] | 0.75 [0.74, 0.75] | 0.70 [0.69, 0.70] |  | 0.72 [0.72, 0.72] |
| 14 | 0.70 [0.70, 0.71] | 0.74 [0.73, 0.74] | 0.67 [0.67, 0.68] | 0.74 [0.74, 0.74] | 0.74 [0.74, 0.74] | 0.69 [0.69, 0.70] |  | 0.71 [0.71, 0.72] |
| 21 | 0.70 [0.70, 0.70] | 0.73 [0.73, 0.73] | 0.67 [0.67, 0.67] | 0.73 [0.73, 0.74] | 0.73 [0.73, 0.73] | 0.69 [0.68, 0.69] |  | 0.71 [0.71, 0.71] |
| 30 | 0.70 [0.69, 0.70] | 0.72 [0.72, 0.73] | 0.67 [0.66, 0.67] | 0.73 [0.73, 0.73] | 0.73 [0.72, 0.73] | 0.68 [0.67, 0.68] |  | 0.70 [0.70, 0.71] |
| 45 | 0.69 [0.69, 0.69] | 0.71 [0.71, 0.72] | 0.66 [0.66, 0.67] | 0.72 [0.72, 0.72] | 0.72 [0.72, 0.73] | 0.67 [0.67, 0.68] |  | 0.69 [0.69, 0.70] |
| 60 | 0.69 [0.69, 0.69] | 0.71 [0.70, 0.71] |  | 0.72 [0.71, 0.72] | 0.73 [0.68, 0.80] | 0.64 [0.58, 0.71] |  | 0.69 [0.69, 0.69] |
| 90 | 0.69 [0.68, 0.69] | 0.71 [0.70, 0.71] |  | 0.71 [0.71, 0.72] |  |  |  | 0.69 [0.68, 0.69] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.02 [-0.02, -0.02] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.04] | 0.02 [0.02, 0.02] | 0.04 [0.03, 0.04] |  | -0.03 [-0.03, -0.02] |  | wt_med |
| 1 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.03] | 0.03 [0.02, 0.03] | 0.04 [0.03, 0.04] |  | -0.02 [-0.03, -0.02] |  | wt_med |
| 2 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.03] | 0.03 [0.03, 0.03] | 0.04 [0.03, 0.04] |  | -0.02 [-0.02, -0.02] |  | wt_med |
| 3 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.03] | 0.03 [0.03, 0.03] | 0.03 [0.03, 0.04] | -0.01 [-0.02, -0.01] | -0.02 [-0.02, -0.02] |  | wt_med |
| 4 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.03 [-0.04, -0.03] | 0.03 [0.03, 0.03] | 0.03 [0.03, 0.04] | -0.01 [-0.02, -0.01] | -0.02 [-0.02, -0.02] |  | wt_med |
| 5 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.03] | 0.03 [0.03, 0.03] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.02] |  | wt_med |
| 6 | -0.02 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.05, -0.04] | 0.03 [0.03, 0.03] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.01] | -0.02 [-0.02, -0.02] |  | wt_med |
| 7 | -0.01 [-0.02, -0.01] | 0.02 [0.02, 0.02] | -0.05 [-0.05, -0.04] | 0.02 [0.02, 0.03] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.01] |  |  | wt_med |
| 10 | -0.01 [-0.01, -0.01] | 0.02 [0.02, 0.02] | -0.05 [-0.05, -0.04] | 0.03 [0.02, 0.03] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.02] |  |  | wt_med |
| 14 | -0.01 [-0.01, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.05, -0.04] | 0.02 [0.02, 0.03] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.02] |  |  | wt_med |
| 21 | -0.01 [-0.01, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.04] | 0.02 [0.02, 0.03] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.02] |  |  | wt_med |
| 30 | -0.01 [-0.01, -0.01] | 0.02 [0.02, 0.02] | -0.04 [-0.04, -0.04] | 0.02 [0.02, 0.02] | 0.03 [0.03, 0.03] | -0.02 [-0.02, -0.02] |  |  | wt_med |
| 45 | -0.01 [-0.01, -0.00] | 0.02 [0.02, 0.02] | -0.05 [-0.05, -0.04] | 0.02 [0.02, 0.02] | 0.03 [0.03, 0.04] | -0.02 [-0.02, -0.01] |  |  | wt_med |
| 60 |  | 0.02 [0.02, 0.02] |  | 0.02 [0.02, 0.03] | 0.07 [0.00, 0.15] | -0.02 [-0.09, 0.05] |  | 0.00 [-0.00, 0.00] | clim |
| 90 | -0.00 [-0.01, -0.00] | 0.02 [0.02, 0.02] |  | 0.02 [0.02, 0.02] |  |  |  |  | wt_med |

## D5 — rope drop

worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`), per ride-day. `prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).

**first-hour MAE (opening-aligned)**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|
| 0 | 4.55 [4.47, 4.62] | 4.10 [4.04, 4.16] | 3.98 [3.90, 4.06] | 4.10 [4.04, 4.16] | 3.86 [3.80, 3.93] |  | 5.01 [4.93, 5.10] | 4.26 [4.19, 4.33] |
| 1 | 4.56 [4.49, 4.63] | 4.16 [4.10, 4.22] | 4.05 [3.97, 4.13] | 4.16 [4.10, 4.22] | 3.91 [3.84, 3.98] |  | 5.01 [4.92, 5.10] | 4.33 [4.27, 4.40] |
| 2 | 4.58 [4.50, 4.65] | 4.19 [4.12, 4.25] | 4.07 [3.99, 4.15] | 4.19 [4.12, 4.25] | 3.93 [3.86, 4.00] |  | 5.01 [4.92, 5.09] | 4.36 [4.30, 4.44] |
| 3 | 4.58 [4.50, 4.66] | 4.20 [4.13, 4.27] | 4.09 [4.01, 4.17] | 4.20 [4.13, 4.27] | 3.95 [3.87, 4.02] | 5.70 [5.61, 5.78] | 5.01 [4.92, 5.10] | 4.38 [4.31, 4.45] |
| 4 | 4.59 [4.51, 4.67] | 4.21 [4.16, 4.28] | 4.11 [4.03, 4.19] | 4.21 [4.16, 4.28] | 3.96 [3.90, 4.04] | 5.73 [5.65, 5.82] | 5.00 [4.91, 5.09] | 4.40 [4.34, 4.48] |
| 5 | 4.60 [4.52, 4.68] | 4.23 [4.17, 4.30] | 4.13 [4.05, 4.21] | 4.23 [4.17, 4.30] | 3.98 [3.91, 4.05] | 5.77 [5.69, 5.87] | 4.99 [4.91, 5.08] | 4.42 [4.36, 4.50] |
| 6 | 4.61 [4.54, 4.68] | 4.26 [4.20, 4.33] | 4.17 [4.10, 4.26] | 4.26 [4.20, 4.33] | 4.01 [3.94, 4.09] | 5.74 [5.65, 5.84] | 4.99 [4.90, 5.08] | 4.46 [4.39, 4.53] |
| 7 | 4.62 [4.55, 4.70] | 4.32 [4.25, 4.39] | 4.23 [4.14, 4.32] | 4.32 [4.25, 4.39] | 4.06 [3.98, 4.14] | 5.83 [5.73, 5.94] |  | 4.53 [4.46, 4.60] |
| 10 | 4.64 [4.57, 4.71] | 4.36 [4.29, 4.43] | 4.30 [4.21, 4.38] | 4.36 [4.29, 4.43] | 4.12 [4.04, 4.19] | 5.91 [5.81, 6.00] |  | 4.59 [4.51, 4.66] |
| 14 | 4.70 [4.62, 4.78] | 4.44 [4.37, 4.50] | 4.40 [4.31, 4.49] | 4.44 [4.37, 4.50] | 4.21 [4.13, 4.28] | 6.05 [5.95, 6.15] |  | 4.67 [4.59, 4.74] |
| 21 | 4.77 [4.69, 4.85] | 4.51 [4.44, 4.58] | 4.55 [4.45, 4.64] | 4.51 [4.44, 4.58] | 4.32 [4.24, 4.41] | 6.21 [6.10, 6.32] |  | 4.75 [4.67, 4.82] |
| 30 | 4.84 [4.76, 4.93] | 4.57 [4.50, 4.65] | 4.68 [4.57, 4.79] | 4.57 [4.50, 4.65] | 4.50 [4.40, 4.61] | 6.63 [6.51, 6.76] |  | 4.82 [4.74, 4.90] |
| 45 | 4.87 [4.79, 4.96] | 4.62 [4.55, 4.69] | 4.53 [4.35, 4.72] | 4.62 [4.55, 4.69] | 4.63 [4.51, 4.75] | 6.88 [6.73, 7.02] |  | 4.86 [4.78, 4.94] |
| 60 | 4.92 [4.82, 5.01] | 4.69 [4.61, 4.77] |  | 4.69 [4.61, 4.77] | 1.34 [0.96, 1.80] | 1.80 [1.37, 2.29] |  | 4.93 [4.84, 5.02] |
| 90 | 5.09 [4.99, 5.19] | 4.83 [4.74, 4.92] |  | 4.83 [4.74, 4.92] |  |  |  | 5.06 [4.97, 5.16] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_served | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.30 [0.27, 0.33] | -0.17 [-0.19, -0.14] | -0.21 [-0.24, -0.17] | -0.17 [-0.19, -0.14] | -0.20 [-0.23, -0.17] |  | 0.76 [0.70, 0.81] |  | wt_med |
| 1 | 0.25 [0.22, 0.28] | -0.18 [-0.20, -0.15] | -0.22 [-0.25, -0.18] | -0.18 [-0.20, -0.15] | -0.21 [-0.24, -0.18] |  | 0.69 [0.63, 0.75] |  | wt_med |
| 2 | 0.24 [0.21, 0.26] | -0.18 [-0.20, -0.16] | -0.22 [-0.26, -0.19] | -0.18 [-0.20, -0.16] | -0.21 [-0.25, -0.18] |  | 0.66 [0.61, 0.72] |  | wt_med |
| 3 | 0.22 [0.20, 0.25] | -0.18 [-0.21, -0.16] | -0.23 [-0.27, -0.19] | -0.18 [-0.21, -0.16] | -0.22 [-0.25, -0.19] | 1.58 [1.52, 1.63] | 0.64 [0.58, 0.69] |  | wt_med |
| 4 | 0.22 [0.19, 0.25] | -0.19 [-0.21, -0.17] | -0.24 [-0.27, -0.20] | -0.19 [-0.21, -0.17] | -0.23 [-0.26, -0.20] | 1.59 [1.53, 1.65] | 0.61 [0.56, 0.67] |  | wt_med |
| 5 | 0.21 [0.18, 0.23] | -0.19 [-0.22, -0.17] | -0.24 [-0.28, -0.20] | -0.19 [-0.22, -0.17] | -0.23 [-0.27, -0.20] | 1.62 [1.56, 1.68] | 0.59 [0.53, 0.64] |  | wt_med |
| 6 | 0.18 [0.15, 0.21] | -0.20 [-0.22, -0.18] | -0.25 [-0.29, -0.21] | -0.20 [-0.22, -0.18] | -0.24 [-0.27, -0.21] | 1.55 [1.49, 1.61] | 0.54 [0.48, 0.60] |  | wt_med |
| 7 | 0.13 [0.10, 0.16] | -0.21 [-0.24, -0.19] | -0.26 [-0.30, -0.22] | -0.21 [-0.24, -0.19] | -0.25 [-0.29, -0.22] | 1.58 [1.51, 1.64] |  |  | wt_med |
| 10 | 0.08 [0.06, 0.11] | -0.22 [-0.25, -0.20] | -0.28 [-0.32, -0.24] | -0.22 [-0.25, -0.20] | -0.26 [-0.30, -0.23] | 1.59 [1.53, 1.65] |  |  | wt_med |
| 14 | 0.06 [0.03, 0.09] | -0.23 [-0.26, -0.20] | -0.29 [-0.34, -0.25] | -0.23 [-0.26, -0.20] | -0.28 [-0.32, -0.25] | 1.62 [1.55, 1.69] |  |  | wt_med |
| 21 | 0.04 [0.02, 0.08] | -0.24 [-0.26, -0.21] | -0.32 [-0.37, -0.27] | -0.24 [-0.26, -0.21] | -0.31 [-0.34, -0.27] | 1.62 [1.56, 1.70] |  |  | wt_med |
| 30 | 0.03 [-0.00, 0.06] | -0.25 [-0.27, -0.22] | -0.35 [-0.40, -0.31] | -0.25 [-0.27, -0.22] | -0.35 [-0.40, -0.30] | 1.80 [1.72, 1.87] |  |  | wt_med |
| 45 | 0.02 [-0.01, 0.05] | -0.24 [-0.27, -0.21] | -0.29 [-0.39, -0.21] | -0.24 [-0.27, -0.21] | -0.39 [-0.45, -0.33] | 1.90 [1.81, 1.99] |  |  | wt_med |
| 60 |  | -0.24 [-0.28, -0.20] |  | -0.24 [-0.28, -0.20] | -0.82 [-1.32, -0.43] | -0.33 [-0.84, 0.10] |  | 0.03 [-0.01, 0.06] | clim |
| 90 | 0.03 [-0.01, 0.07] | -0.23 [-0.26, -0.20] |  | -0.23 [-0.26, -0.20] |  |  |  |  | wt_med |

**rope-drop worth agreement**

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] |  | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 1 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] |  | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 2 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] |  | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 3 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 4 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 5 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 6 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.91 [0.91, 0.91] |
| 7 | 0.91 [0.91, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.91] |
| 10 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.91] | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.91] |  | 0.91 [0.91, 0.91] |
| 14 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.91] |
| 21 | 0.91 [0.91, 0.91] | 0.90 [0.90, 0.91] | 0.90 [0.89, 0.90] | 0.90 [0.90, 0.90] | 0.91 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.90] |  | 0.91 [0.91, 0.91] |
| 30 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.90 [0.89, 0.90] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.89 [0.89, 0.90] |  | 0.91 [0.91, 0.91] |
| 45 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] | 0.86 [0.85, 0.86] | 0.90 [0.90, 0.90] | 0.90 [0.90, 0.91] | 0.90 [0.90, 0.90] | 0.89 [0.89, 0.90] |  | 0.91 [0.91, 0.91] |
| 60 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] |  | 0.90 [0.90, 0.90] | 0.98 [0.96, 0.99] | 0.90 [0.90, 0.90] | 0.98 [0.96, 0.99] |  | 0.91 [0.90, 0.91] |
| 90 | 0.91 [0.91, 0.91] | 0.91 [0.90, 0.91] |  | 0.90 [0.90, 0.90] |  | 0.89 [0.89, 0.90] |  |  | 0.90 [0.90, 0.91] |

paired difference vs reference:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | prod_ropedrop_hist | prod_served | snaive7 | wt_med | ref |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] |  | -0.01 [-0.01, -0.01] |  | wt_med |
| 1 | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] |  | -0.01 [-0.01, -0.01] |  | wt_med |
| 2 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] |  | -0.01 [-0.01, -0.01] |  | wt_med |
| 3 | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] |  | wt_med |
| 4 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] |  | wt_med |
| 5 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] |  | wt_med |
| 6 | -0.00 [-0.00, 0.00] | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.00, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] |  | wt_med |
| 7 |  | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] |  | -0.00 [-0.00, 0.00] | clim |
| 10 |  | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.00, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] |  | 0.00 [-0.00, 0.00] | clim |
| 14 |  | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] |  | -0.00 [-0.00, 0.00] | clim |
| 21 |  | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] |  | -0.00 [-0.00, 0.00] | clim |
| 30 |  | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] |  | -0.00 [-0.00, 0.00] | clim |
| 45 |  | -0.00 [-0.00, -0.00] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, -0.00] | -0.00 [-0.01, -0.00] | -0.01 [-0.01, -0.01] | -0.01 [-0.01, -0.01] |  | -0.00 [-0.00, 0.00] | clim |
| 60 |  | -0.00 [-0.00, -0.00] |  | -0.01 [-0.01, -0.00] | -0.00 [-0.01, 0.01] | -0.01 [-0.01, -0.01] | -0.00 [-0.01, 0.01] |  | -0.00 [-0.00, 0.00] | clim |
| 90 |  | -0.00 [-0.00, 0.00] |  | -0.01 [-0.01, -0.00] |  | -0.01 [-0.01, -0.01] |  |  | -0.00 [-0.00, -0.00] | clim |

| L | model | tp | fp | fn | tn | precision | recall |
|---|---|---|---|---|---|---|---|
| 0 | clim | 10392.00 | 4354.00 | 25097.00 | 289438.00 | 0.70 | 0.29 |
| 0 | h5 | 8732.00 | 2456.00 | 30240.00 | 313426.00 | 0.78 | 0.22 |
| 0 | lvlh5_cbd | 2157.00 | 686.00 | 19398.00 | 181459.00 | 0.76 | 0.10 |
| 0 | lvlh5_naive | 9499.00 | 3338.00 | 27833.00 | 289386.00 | 0.74 | 0.25 |
| 0 | lvlh5_tft | 6045.00 | 2108.00 | 19800.00 | 221068.00 | 0.74 | 0.23 |
| 0 | prod_ropedrop_hist | 18702.00 | 12188.00 | 19815.00 | 302641.00 | 0.61 | 0.49 |
| 0 | snaive7 | 17441.00 | 15813.00 | 15442.00 | 267137.00 | 0.52 | 0.53 |
| 0 | wt_med | 11744.00 | 4379.00 | 26773.00 | 310450.00 | 0.73 | 0.30 |
| 1 | clim | 10325.00 | 4377.00 | 24903.00 | 287726.00 | 0.70 | 0.29 |
| 1 | h5 | 8608.00 | 2512.00 | 30095.00 | 310990.00 | 0.77 | 0.22 |
| 1 | lvlh5_cbd | 2119.00 | 706.00 | 19167.00 | 178507.00 | 0.75 | 0.10 |
| 1 | lvlh5_naive | 9427.00 | 3399.00 | 27778.00 | 288826.00 | 0.73 | 0.25 |
| 1 | lvlh5_tft | 5817.00 | 2034.00 | 19769.00 | 218802.00 | 0.74 | 0.23 |
| 1 | prod_ropedrop_hist | 18417.00 | 12317.00 | 19780.00 | 300158.00 | 0.60 | 0.48 |
| 1 | snaive7 | 17374.00 | 15784.00 | 15418.00 | 266788.00 | 0.52 | 0.53 |
| 1 | wt_med | 11528.00 | 4478.00 | 26669.00 | 307997.00 | 0.72 | 0.30 |
| 3 | clim | 10195.00 | 4430.00 | 24537.00 | 284260.00 | 0.70 | 0.29 |
| 3 | h5 | 8448.00 | 2555.00 | 29701.00 | 307288.00 | 0.77 | 0.22 |
| 3 | lvlh5_cbd | 2091.00 | 694.00 | 18689.00 | 173870.00 | 0.75 | 0.10 |
| 3 | lvlh5_naive | 9364.00 | 3439.00 | 27541.00 | 287649.00 | 0.73 | 0.25 |
| 3 | lvlh5_tft | 5758.00 | 2201.00 | 19372.00 | 214791.00 | 0.72 | 0.23 |
| 3 | prod_ropedrop_hist | 17989.00 | 12474.00 | 19642.00 | 296333.00 | 0.59 | 0.48 |
| 3 | prod_served | 4072.00 | 1278.00 | 19683.00 | 198064.00 | 0.76 | 0.17 |
| 3 | snaive7 | 17219.00 | 15695.00 | 15335.00 | 265805.00 | 0.52 | 0.53 |
| 3 | wt_med | 11281.00 | 4511.00 | 26350.00 | 304296.00 | 0.71 | 0.30 |
| 7 | clim | 9860.00 | 4462.00 | 23997.00 | 277807.00 | 0.69 | 0.29 |
| 7 | h5 | 8196.00 | 2662.00 | 28976.00 | 299621.00 | 0.75 | 0.22 |
| 7 | lvlh5_cbd | 2036.00 | 774.00 | 18085.00 | 166152.00 | 0.72 | 0.10 |
| 7 | lvlh5_naive | 8848.00 | 3584.00 | 26585.00 | 275032.00 | 0.71 | 0.25 |
| 7 | lvlh5_tft | 5324.00 | 2350.00 | 18864.00 | 205977.00 | 0.69 | 0.22 |
| 7 | prod_ropedrop_hist | 17312.00 | 12621.00 | 19321.00 | 288565.00 | 0.58 | 0.47 |
| 7 | prod_served | 3864.00 | 1479.00 | 19040.00 | 190325.00 | 0.72 | 0.17 |
| 7 | wt_med | 10799.00 | 4709.00 | 25834.00 | 296477.00 | 0.70 | 0.29 |

## D6 — crowd bucket per park-day (Prognose heute / trip assistant)

Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's month, ≥ 30 days) → the `determineCrowdLevel` ladder. Busy = high or above.

| lead | model | park_days | bucket_acc | pm1_acc | busy_recall | busy_precision |
|---|---|---|---|---|---|---|
| 1 | lvl_cbd | 9488 | 0.20 [0.20, 0.21] | 0.57 [0.56, 0.58] | 0.15 [0.14, 0.16] | 0.64 [0.61, 0.67] |
| 1 | lvl_clim | 15812 | 0.27 [0.27, 0.28] | 0.70 [0.69, 0.71] | 0.38 [0.37, 0.39] | 0.60 [0.59, 0.62] |
| 1 | lvl_naive4 | 15663 | 0.37 [0.36, 0.37] | 0.77 [0.76, 0.78] | 0.61 [0.60, 0.62] | 0.63 [0.62, 0.64] |
| 1 | lvl_snaive7 | 15240 | 0.40 [0.39, 0.41] | 0.78 [0.77, 0.78] | 0.66 [0.65, 0.67] | 0.65 [0.64, 0.66] |
| 1 | lvl_tft | 11680 | 0.40 [0.39, 0.41] | 0.81 [0.80, 0.82] | 0.65 [0.64, 0.67] | 0.69 [0.68, 0.71] |
| 1 | lvl_wt56 | 15812 | 0.32 [0.31, 0.32] | 0.75 [0.74, 0.75] | 0.50 [0.49, 0.52] | 0.62 [0.60, 0.63] |
| 2 | lvl_cbd | 9274 | 0.21 [0.20, 0.22] | 0.57 [0.56, 0.58] | 0.15 [0.14, 0.16] | 0.65 [0.62, 0.69] |
| 2 | lvl_clim | 15644 | 0.27 [0.26, 0.28] | 0.70 [0.69, 0.71] | 0.38 [0.37, 0.39] | 0.60 [0.59, 0.62] |
| 2 | lvl_naive4 | 15506 | 0.37 [0.36, 0.37] | 0.77 [0.77, 0.78] | 0.61 [0.60, 0.63] | 0.63 [0.62, 0.65] |
| 2 | lvl_snaive7 | 15085 | 0.40 [0.39, 0.41] | 0.78 [0.77, 0.79] | 0.66 [0.65, 0.68] | 0.65 [0.64, 0.66] |
| 2 | lvl_tft | 11516 | 0.38 [0.37, 0.39] | 0.80 [0.79, 0.81] | 0.65 [0.63, 0.66] | 0.68 [0.67, 0.69] |
| 2 | lvl_wt56 | 15644 | 0.31 [0.31, 0.32] | 0.74 [0.74, 0.75] | 0.50 [0.49, 0.52] | 0.61 [0.60, 0.62] |
| 3 | lvl_cbd | 9189 | 0.21 [0.20, 0.21] | 0.57 [0.56, 0.58] | 0.15 [0.14, 0.17] | 0.65 [0.62, 0.69] |
| 3 | lvl_clim | 15546 | 0.27 [0.26, 0.28] | 0.70 [0.69, 0.70] | 0.38 [0.37, 0.39] | 0.60 [0.58, 0.61] |
| 3 | lvl_naive4 | 15414 | 0.37 [0.36, 0.38] | 0.77 [0.77, 0.78] | 0.62 [0.60, 0.63] | 0.63 [0.62, 0.64] |
| 3 | lvl_snaive7 | 14994 | 0.40 [0.39, 0.41] | 0.78 [0.77, 0.79] | 0.67 [0.65, 0.68] | 0.65 [0.64, 0.66] |
| 3 | lvl_tft | 11420 | 0.38 [0.37, 0.39] | 0.80 [0.79, 0.80] | 0.64 [0.62, 0.65] | 0.67 [0.66, 0.69] |
| 3 | lvl_wt56 | 15546 | 0.31 [0.31, 0.32] | 0.74 [0.73, 0.75] | 0.50 [0.49, 0.51] | 0.61 [0.59, 0.62] |
| 4 | lvl_cbd | 9098 | 0.21 [0.20, 0.22] | 0.57 [0.56, 0.58] | 0.16 [0.15, 0.17] | 0.66 [0.63, 0.69] |
| 4 | lvl_clim | 15459 | 0.27 [0.26, 0.28] | 0.70 [0.69, 0.70] | 0.38 [0.37, 0.39] | 0.60 [0.58, 0.61] |
| 4 | lvl_naive4 | 15334 | 0.37 [0.36, 0.38] | 0.77 [0.77, 0.78] | 0.62 [0.61, 0.63] | 0.64 [0.62, 0.65] |
| 4 | lvl_snaive7 | 14913 | 0.40 [0.39, 0.41] | 0.78 [0.77, 0.79] | 0.67 [0.66, 0.68] | 0.65 [0.64, 0.67] |
| 4 | lvl_tft | 11320 | 0.37 [0.36, 0.38] | 0.79 [0.78, 0.80] | 0.64 [0.62, 0.65] | 0.67 [0.66, 0.68] |
| 4 | lvl_wt56 | 15460 | 0.31 [0.30, 0.32] | 0.74 [0.73, 0.74] | 0.50 [0.48, 0.51] | 0.61 [0.59, 0.62] |
| 5 | lvl_cbd | 8990 | 0.21 [0.20, 0.22] | 0.57 [0.56, 0.58] | 0.17 [0.15, 0.18] | 0.68 [0.64, 0.71] |
| 5 | lvl_clim | 15354 | 0.27 [0.26, 0.28] | 0.70 [0.69, 0.70] | 0.38 [0.36, 0.39] | 0.60 [0.58, 0.61] |
| 5 | lvl_naive4 | 15232 | 0.37 [0.36, 0.38] | 0.77 [0.77, 0.78] | 0.62 [0.61, 0.63] | 0.64 [0.63, 0.65] |
| 5 | lvl_snaive7 | 14816 | 0.40 [0.39, 0.41] | 0.78 [0.78, 0.79] | 0.67 [0.66, 0.68] | 0.66 [0.65, 0.67] |
| 5 | lvl_tft | 11201 | 0.37 [0.36, 0.37] | 0.79 [0.78, 0.79] | 0.63 [0.62, 0.65] | 0.67 [0.65, 0.68] |
| 5 | lvl_wt56 | 15356 | 0.31 [0.30, 0.32] | 0.73 [0.73, 0.74] | 0.49 [0.48, 0.51] | 0.61 [0.59, 0.62] |
| 6 | lvl_cbd | 8887 | 0.20 [0.19, 0.20] | 0.53 [0.52, 0.54] | 0.11 [0.10, 0.12] | 0.59 [0.55, 0.63] |
| 6 | lvl_clim | 15253 | 0.27 [0.26, 0.27] | 0.69 [0.69, 0.70] | 0.38 [0.36, 0.39] | 0.60 [0.58, 0.61] |
| 6 | lvl_naive4 | 15134 | 0.37 [0.36, 0.38] | 0.78 [0.77, 0.78] | 0.62 [0.61, 0.64] | 0.64 [0.63, 0.65] |
| 6 | lvl_snaive7 | 14722 | 0.40 [0.39, 0.41] | 0.78 [0.78, 0.79] | 0.67 [0.66, 0.68] | 0.66 [0.65, 0.67] |
| 6 | lvl_tft | 11086 | 0.36 [0.35, 0.37] | 0.78 [0.77, 0.79] | 0.62 [0.60, 0.63] | 0.66 [0.65, 0.68] |
| 6 | lvl_wt56 | 15255 | 0.31 [0.30, 0.31] | 0.73 [0.72, 0.74] | 0.48 [0.47, 0.50] | 0.60 [0.59, 0.61] |
| 7 | lvl_cbd | 8793 | 0.19 [0.18, 0.20] | 0.52 [0.51, 0.53] | 0.10 [0.09, 0.12] | 0.56 [0.52, 0.59] |
| 7 | lvl_clim | 15155 | 0.26 [0.26, 0.27] | 0.69 [0.68, 0.70] | 0.37 [0.36, 0.39] | 0.59 [0.58, 0.61] |
| 7 | lvl_naive4 | 14958 | 0.34 [0.33, 0.34] | 0.73 [0.72, 0.74] | 0.57 [0.56, 0.58] | 0.59 [0.58, 0.61] |
| 7 | lvl_tft | 10982 | 0.34 [0.33, 0.35] | 0.76 [0.75, 0.77] | 0.59 [0.57, 0.60] | 0.64 [0.62, 0.65] |
| 7 | lvl_wt56 | 15157 | 0.30 [0.29, 0.30] | 0.72 [0.72, 0.73] | 0.48 [0.46, 0.49] | 0.59 [0.58, 0.60] |
| 10 | lvl_cbd | 8519 | 0.19 [0.18, 0.20] | 0.52 [0.50, 0.53] | 0.11 [0.10, 0.12] | 0.59 [0.55, 0.63] |
| 10 | lvl_clim | 14863 | 0.26 [0.25, 0.27] | 0.69 [0.68, 0.70] | 0.37 [0.36, 0.39] | 0.59 [0.58, 0.61] |
| 10 | lvl_naive4 | 14696 | 0.34 [0.33, 0.35] | 0.73 [0.73, 0.74] | 0.58 [0.57, 0.59] | 0.60 [0.59, 0.61] |
| 10 | lvl_tft | 10703 | 0.33 [0.33, 0.34] | 0.74 [0.73, 0.75] | 0.59 [0.57, 0.60] | 0.62 [0.60, 0.63] |
| 10 | lvl_wt56 | 14868 | 0.29 [0.29, 0.30] | 0.72 [0.71, 0.72] | 0.47 [0.46, 0.48] | 0.58 [0.57, 0.59] |
| 14 | lvl_cbd | 8119 | 0.18 [0.17, 0.19] | 0.51 [0.50, 0.52] | 0.10 [0.09, 0.11] | 0.57 [0.53, 0.61] |
| 14 | lvl_clim | 14478 | 0.26 [0.25, 0.27] | 0.69 [0.68, 0.69] | 0.37 [0.36, 0.38] | 0.60 [0.58, 0.61] |
| 14 | lvl_naive4 | 14254 | 0.31 [0.30, 0.31] | 0.70 [0.69, 0.71] | 0.54 [0.52, 0.55] | 0.56 [0.55, 0.58] |
| 14 | lvl_tft | 10272 | 0.31 [0.31, 0.32] | 0.72 [0.71, 0.73] | 0.58 [0.56, 0.59] | 0.60 [0.58, 0.61] |
| 14 | lvl_wt56 | 14483 | 0.28 [0.28, 0.29] | 0.70 [0.70, 0.71] | 0.44 [0.43, 0.46] | 0.56 [0.55, 0.58] |
| 21 | lvl_cbd | 7485 | 0.18 [0.17, 0.18] | 0.49 [0.48, 0.51] | 0.10 [0.09, 0.11] | 0.56 [0.51, 0.60] |
| 21 | lvl_clim | 13842 | 0.25 [0.25, 0.26] | 0.68 [0.67, 0.69] | 0.36 [0.35, 0.38] | 0.59 [0.58, 0.61] |
| 21 | lvl_naive4 | 13598 | 0.29 [0.28, 0.30] | 0.68 [0.67, 0.69] | 0.52 [0.51, 0.53] | 0.54 [0.53, 0.55] |
| 21 | lvl_tft | 9593 | 0.29 [0.28, 0.30] | 0.70 [0.69, 0.71] | 0.57 [0.56, 0.59] | 0.58 [0.56, 0.59] |
| 21 | lvl_wt56 | 13847 | 0.27 [0.27, 0.28] | 0.69 [0.68, 0.69] | 0.43 [0.41, 0.44] | 0.55 [0.54, 0.56] |
| 30 | lvl_cbd | 6638 | 0.17 [0.16, 0.18] | 0.49 [0.48, 0.51] | 0.10 [0.09, 0.11] | 0.55 [0.51, 0.59] |
| 30 | lvl_clim | 12928 | 0.25 [0.24, 0.26] | 0.68 [0.67, 0.69] | 0.36 [0.35, 0.37] | 0.59 [0.58, 0.61] |
| 30 | lvl_naive4 | 12695 | 0.28 [0.27, 0.29] | 0.67 [0.67, 0.68] | 0.52 [0.51, 0.53] | 0.53 [0.52, 0.55] |
| 30 | lvl_tft | 7278 | 0.27 [0.26, 0.28] | 0.66 [0.65, 0.67] | 0.58 [0.56, 0.59] | 0.55 [0.54, 0.57] |
| 30 | lvl_wt56 | 12935 | 0.27 [0.26, 0.27] | 0.67 [0.66, 0.68] | 0.42 [0.41, 0.44] | 0.54 [0.53, 0.56] |
| 45 | lvl_cbd | 2014 | 0.23 [0.22, 0.25] | 0.63 [0.61, 0.65] | 0.11 [0.09, 0.14] | 0.42 [0.34, 0.51] |
| 45 | lvl_clim | 11429 | 0.25 [0.24, 0.25] | 0.68 [0.67, 0.68] | 0.36 [0.34, 0.37] | 0.58 [0.56, 0.60] |
| 45 | lvl_naive4 | 11205 | 0.27 [0.26, 0.28] | 0.66 [0.65, 0.67] | 0.50 [0.48, 0.51] | 0.51 [0.50, 0.53] |
| 45 | lvl_tft | 4861 | 0.26 [0.25, 0.27] | 0.64 [0.62, 0.65] | 0.57 [0.55, 0.60] | 0.52 [0.49, 0.54] |
| 45 | lvl_wt56 | 11429 | 0.26 [0.25, 0.27] | 0.67 [0.67, 0.68] | 0.42 [0.40, 0.43] | 0.54 [0.53, 0.56] |
| 60 | lvl_clim | 10100 | 0.24 [0.23, 0.24] | 0.68 [0.67, 0.68] | 0.34 [0.33, 0.36] | 0.57 [0.55, 0.59] |
| 60 | lvl_naive4 | 9884 | 0.27 [0.26, 0.28] | 0.66 [0.65, 0.66] | 0.49 [0.47, 0.50] | 0.51 [0.49, 0.53] |
| 60 | lvl_tft | 63 | 0.21 [0.11, 0.32] | 0.59 [0.46, 0.71] | 0.68 [0.54, 0.82] | 0.68 [0.53, 0.83] |
| 60 | lvl_wt56 | 10099 | 0.26 [0.25, 0.27] | 0.67 [0.67, 0.68] | 0.40 [0.39, 0.42] | 0.55 [0.53, 0.57] |
| 90 | lvl_clim | 7522 | 0.23 [0.22, 0.24] | 0.67 [0.66, 0.68] | 0.36 [0.34, 0.38] | 0.59 [0.57, 0.62] |
| 90 | lvl_naive4 | 7373 | 0.28 [0.27, 0.29] | 0.68 [0.66, 0.69] | 0.51 [0.49, 0.53] | 0.55 [0.53, 0.57] |
| 90 | lvl_wt56 | 7516 | 0.26 [0.25, 0.27] | 0.70 [0.69, 0.71] | 0.40 [0.39, 0.42] | 0.58 [0.56, 0.60] |
| 120 | lvl_clim | 5133 | 0.22 [0.21, 0.23] | 0.67 [0.66, 0.68] | 0.39 [0.37, 0.41] | 0.56 [0.54, 0.59] |
| 120 | lvl_naive4 | 4995 | 0.28 [0.27, 0.29] | 0.68 [0.67, 0.69] | 0.53 [0.50, 0.55] | 0.49 [0.47, 0.51] |
| 120 | lvl_wt56 | 5114 | 0.25 [0.23, 0.26] | 0.67 [0.65, 0.68] | 0.43 [0.40, 0.45] | 0.53 [0.50, 0.56] |
| 180 | lvl_clim | 1759 | 0.21 [0.19, 0.23] | 0.65 [0.63, 0.67] | 0.51 [0.47, 0.55] | 0.50 [0.45, 0.54] |
| 180 | lvl_naive4 | 1726 | 0.26 [0.24, 0.28] | 0.64 [0.62, 0.67] | 0.61 [0.56, 0.66] | 0.40 [0.36, 0.43] |
| 180 | lvl_wt56 | 1759 | 0.24 [0.22, 0.26] | 0.68 [0.66, 0.70] | 0.52 [0.48, 0.57] | 0.50 [0.45, 0.54] |

Cross-park ordering on the same date (true gap ≥ 10 points):

| L | model | pairs | correct | accuracy |
|---|---|---|---|---|
| 1 | lvl_cbd | 309422 | 190691 | 0.62 |
| 1 | lvl_clim | 518286 | 315421 | 0.61 |
| 1 | lvl_naive4 | 510202 | 358132 | 0.70 |
| 1 | lvl_snaive7 | 481216 | 349823 | 0.73 |
| 1 | lvl_tft | 435678 | 318624 | 0.73 |
| 1 | lvl_wt56 | 518286 | 347917 | 0.67 |
| 2 | lvl_cbd | 299549 | 186645 | 0.62 |
| 2 | lvl_clim | 512248 | 309738 | 0.60 |
| 2 | lvl_naive4 | 504791 | 355094 | 0.70 |
| 2 | lvl_snaive7 | 475959 | 346625 | 0.73 |
| 2 | lvl_tft | 430038 | 311360 | 0.72 |
| 2 | lvl_wt56 | 512248 | 342455 | 0.67 |
| 3 | lvl_cbd | 296249 | 185189 | 0.63 |
| 3 | lvl_clim | 508153 | 305973 | 0.60 |
| 3 | lvl_naive4 | 501101 | 353460 | 0.71 |
| 3 | lvl_snaive7 | 472292 | 344745 | 0.73 |
| 3 | lvl_tft | 426298 | 306808 | 0.72 |
| 3 | lvl_wt56 | 508153 | 338477 | 0.67 |
| 4 | lvl_cbd | 292880 | 183324 | 0.63 |
| 4 | lvl_clim | 504695 | 302908 | 0.60 |
| 4 | lvl_naive4 | 498006 | 351998 | 0.71 |
| 4 | lvl_snaive7 | 469265 | 343503 | 0.73 |
| 4 | lvl_tft | 422273 | 301439 | 0.71 |
| 4 | lvl_wt56 | 504747 | 335160 | 0.66 |
| 5 | lvl_cbd | 288226 | 180581 | 0.63 |
| 5 | lvl_clim | 499579 | 298697 | 0.60 |
| 5 | lvl_naive4 | 493069 | 349656 | 0.71 |
| 5 | lvl_snaive7 | 464689 | 341281 | 0.73 |
| 5 | lvl_tft | 416464 | 295499 | 0.71 |
| 5 | lvl_wt56 | 499699 | 330902 | 0.66 |
| 6 | lvl_cbd | 283925 | 165492 | 0.58 |
| 6 | lvl_clim | 494891 | 293966 | 0.59 |
| 6 | lvl_naive4 | 488488 | 347535 | 0.71 |
| 6 | lvl_snaive7 | 460445 | 339103 | 0.74 |
| 6 | lvl_tft | 410934 | 289228 | 0.70 |
| 6 | lvl_wt56 | 495011 | 325543 | 0.66 |
| 7 | lvl_cbd | 280428 | 157119 | 0.56 |
| 7 | lvl_clim | 491060 | 290028 | 0.59 |
| 7 | lvl_naive4 | 480665 | 321110 | 0.67 |
| 7 | lvl_snaive7 | 0 | 0 |  |
| 7 | lvl_tft | 406693 | 277360 | 0.68 |
| 7 | lvl_wt56 | 491180 | 318394 | 0.65 |
| 10 | lvl_cbd | 270078 | 151463 | 0.56 |
| 10 | lvl_clim | 479643 | 281042 | 0.59 |
| 10 | lvl_naive4 | 470953 | 316601 | 0.67 |
| 10 | lvl_snaive7 | 0 | 0 |  |
| 10 | lvl_tft | 396213 | 265907 | 0.67 |
| 10 | lvl_wt56 | 479928 | 306809 | 0.64 |
| 14 | lvl_cbd | 254157 | 140658 | 0.55 |
| 14 | lvl_clim | 463323 | 269101 | 0.58 |
| 14 | lvl_naive4 | 451604 | 289015 | 0.64 |
| 14 | lvl_snaive7 | 0 | 0 |  |
| 14 | lvl_tft | 377443 | 245298 | 0.65 |
| 14 | lvl_wt56 | 463609 | 288934 | 0.62 |
| 21 | lvl_cbd | 230515 | 127165 | 0.55 |
| 21 | lvl_clim | 437697 | 251399 | 0.57 |
| 21 | lvl_naive4 | 424898 | 263637 | 0.62 |
| 21 | lvl_snaive7 | 0 | 0 |  |
| 21 | lvl_tft | 350094 | 222314 | 0.64 |
| 21 | lvl_wt56 | 438000 | 265018 | 0.61 |
| 30 | lvl_cbd | 198317 | 109087 | 0.55 |
| 30 | lvl_clim | 398256 | 224344 | 0.56 |
| 30 | lvl_naive4 | 386101 | 236674 | 0.61 |
| 30 | lvl_snaive7 | 0 | 0 |  |
| 30 | lvl_tft | 263045 | 164867 | 0.63 |
| 30 | lvl_wt56 | 398676 | 235638 | 0.59 |
| 45 | lvl_cbd | 21333 | 10660 | 0.50 |
| 45 | lvl_clim | 333968 | 185999 | 0.56 |
| 45 | lvl_naive4 | 322077 | 192644 | 0.60 |
| 45 | lvl_snaive7 | 0 | 0 |  |
| 45 | lvl_tft | 164443 | 100866 | 0.61 |
| 45 | lvl_wt56 | 333982 | 194458 | 0.58 |
| 60 | lvl_cbd | 0 | 0 |  |
| 60 | lvl_clim | 283099 | 157906 | 0.56 |
| 60 | lvl_naive4 | 271807 | 160529 | 0.59 |
| 60 | lvl_snaive7 | 0 | 0 |  |
| 60 | lvl_tft | 24 | 19 | 0.79 |
| 60 | lvl_wt56 | 283060 | 163507 | 0.58 |
| 90 | lvl_cbd | 0 | 0 |  |
| 90 | lvl_clim | 187545 | 107128 | 0.57 |
| 90 | lvl_naive4 | 181049 | 109197 | 0.60 |
| 90 | lvl_snaive7 | 0 | 0 |  |
| 90 | lvl_tft | 0 | 0 |  |
| 90 | lvl_wt56 | 187238 | 111572 | 0.60 |
| 120 | lvl_cbd | 0 | 0 |  |
| 120 | lvl_clim | 107619 | 63260 | 0.59 |
| 120 | lvl_naive4 | 102710 | 60937 | 0.59 |
| 120 | lvl_snaive7 | 0 | 0 |  |
| 120 | lvl_tft | 0 | 0 |  |
| 120 | lvl_wt56 | 106857 | 62254 | 0.58 |
| 180 | lvl_cbd | 0 | 0 |  |
| 180 | lvl_clim | 25916 | 16243 | 0.63 |
| 180 | lvl_naive4 | 24916 | 14964 | 0.60 |
| 180 | lvl_snaive7 | 0 | 0 |  |
| 180 | lvl_tft | 0 | 0 |  |
| 180 | lvl_wt56 | 25916 | 15580 | 0.60 |

## D7 — day comparison and calendar star

rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and park with a true rank gap ≥ 0.5; a predicted gap < 0.1 is a tie (counted wrong). Star = rank ≤ month median − 0.5 among ≥ 4 candidate days of the month ~30 days ahead (origins on the 1st and 15th).

| bucket | model | pairs | correct | ties | winner_accuracy | tie_rate |
|---|---|---|---|---|---|---|
| d1-7 | lvl_cbd | 107401 | 24595 | 69686 | 0.23 | 0.65 |
| d1-7 | lvl_clim | 188818 | 50284 | 115933 | 0.27 | 0.61 |
| d1-7 | lvl_naive4 | 185793 | 82950 | 68243 | 0.45 | 0.37 |
| d1-7 | lvl_snaive7 | 127681 | 60662 | 38507 | 0.48 | 0.30 |
| d1-7 | lvl_tft | 137423 | 56755 | 57542 | 0.41 | 0.42 |
| d1-7 | lvl_wt56 | 188855 | 51207 | 116083 | 0.27 | 0.61 |
| d31-90 | lvl_cbd | 1956122 | 287679 | 1451556 | 0.15 | 0.74 |
| d31-90 | lvl_clim | 22769210 | 5488988 | 13893083 | 0.24 | 0.61 |
| d31-90 | lvl_naive4 | 22044866 | 7437824 | 9782509 | 0.34 | 0.44 |
| d31-90 | lvl_tft | 4240053 | 1497634 | 1877115 | 0.35 | 0.44 |
| d31-90 | lvl_wt56 | 22766415 | 4994766 | 14751873 | 0.22 | 0.65 |
| d8-30 | lvl_cbd | 1857958 | 304463 | 1347068 | 0.16 | 0.73 |
| d8-30 | lvl_clim | 3539785 | 845571 | 2265043 | 0.24 | 0.64 |
| d8-30 | lvl_naive4 | 3463475 | 1253433 | 1528280 | 0.36 | 0.44 |
| d8-30 | lvl_tft | 2403527 | 874778 | 1045961 | 0.36 | 0.44 |
| d8-30 | lvl_wt56 | 3542183 | 806845 | 2295614 | 0.23 | 0.65 |

| model | pred_star | true_star | both | park_months | star_precision | star_recall |
|---|---|---|---|---|---|---|
| lvl_cbd | 965 | 3620 | 313 | 687 | 0.32 | 0.09 |
| lvl_clim | 1707 | 7538 | 637 | 1142 | 0.37 | 0.08 |
| lvl_naive4 | 5492 | 7454 | 2170 | 1138 | 0.40 | 0.29 |
| lvl_snaive7 | 543 | 522 | 174 | 345 | 0.32 | 0.33 |
| lvl_tft | 2830 | 4118 | 1015 | 750 | 0.36 | 0.25 |
| lvl_wt56 | 1867 | 7549 | 671 | 1142 | 0.36 | 0.09 |

## D8 — uncertainty

Coverage of the empirical q80/q95 (should read 0.80/0.95) and the share of truth slots that HAVE an interval (`field_coverage`; production composed days have none today):

| L | wt_med cov_q80 | wt_med cov_q95 | wt_med field_coverage |
|---|---|---|---|
| 0 | 0.84 | 0.92 | 0.97 |
| 1 | 0.84 | 0.92 | 0.97 |
| 2 | 0.84 | 0.92 | 0.97 |
| 3 | 0.84 | 0.92 | 0.96 |
| 4 | 0.84 | 0.92 | 0.96 |
| 5 | 0.84 | 0.92 | 0.96 |
| 6 | 0.84 | 0.92 | 0.96 |
| 7 | 0.83 | 0.91 | 0.96 |
| 10 | 0.83 | 0.91 | 0.96 |
| 14 | 0.83 | 0.91 | 0.95 |
| 21 | 0.82 | 0.90 | 0.94 |
| 30 | 0.82 | 0.90 | 0.94 |
| 45 | 0.83 | 0.90 | 0.92 |
| 60 | 0.83 | 0.91 | 0.91 |
| 90 | 0.84 | 0.91 | 0.89 |

Stated typical error = the trailing 28-day MAE at the same lead and park (what `expectedError` reports); ratio > 1 = the stated error is optimistic:

| model | L | realised | stated_weighted | ratio_realised_to_stated |
|---|---|---|---|---|
| snaive7 | 0 | 8.68 | 8.80 | 0.99 |
| snaive7 | 1 | 8.68 | 8.81 | 0.98 |
| snaive7 | 3 | 8.64 | 8.82 | 0.98 |
| wt_med | 0 | 7.54 | 7.60 | 0.99 |
| wt_med | 1 | 7.64 | 7.72 | 0.99 |
| wt_med | 3 | 7.72 | 7.79 | 0.99 |
| wt_med | 7 | 7.92 | 8.02 | 0.99 |
| wt_med | 14 | 8.09 | 8.25 | 0.98 |
| wt_med | 30 | 8.30 | 8.66 | 0.96 |
| wt_med | 60 | 8.42 | 8.60 | 0.98 |
| wt_med | 90 | 7.99 | 8.97 | 0.89 |
| clim | 0 | 8.06 | 8.16 | 0.99 |
| clim | 1 | 8.08 | 8.18 | 0.99 |
| clim | 3 | 8.11 | 8.20 | 0.99 |
| clim | 7 | 8.19 | 8.28 | 0.99 |
| clim | 14 | 8.29 | 8.43 | 0.98 |
| clim | 30 | 8.36 | 8.75 | 0.96 |
| clim | 60 | 8.26 | 8.63 | 0.96 |
| clim | 90 | 8.26 | 8.91 | 0.93 |
| h5 | 0 | 7.47 | 7.53 | 0.99 |
| h5 | 1 | 7.57 | 7.64 | 0.99 |
| h5 | 3 | 7.63 | 7.70 | 0.99 |
| h5 | 7 | 7.82 | 7.91 | 0.99 |
| h5 | 14 | 7.97 | 8.12 | 0.98 |
| h5 | 30 | 8.14 | 8.48 | 0.96 |
| h5 | 60 | 8.25 | 8.31 | 0.99 |
| h5 | 90 | 7.88 | 8.64 | 0.91 |
| lvlh5_naive | 0 | 7.82 | 7.90 | 0.99 |
| lvlh5_naive | 1 | 7.84 | 7.92 | 0.99 |
| lvlh5_naive | 3 | 7.85 | 7.93 | 0.99 |
| lvlh5_naive | 7 | 8.18 | 8.24 | 0.99 |
| lvlh5_naive | 14 | 8.41 | 8.45 | 1.00 |
| lvlh5_naive | 30 | 8.61 | 8.80 | 0.98 |
| lvlh5_naive | 60 | 8.75 | 8.74 | 1.00 |
| lvlh5_naive | 90 | 8.43 | 8.81 | 0.96 |
| lvlh5_tft | 0 | 7.35 | 7.41 | 0.99 |
| lvlh5_tft | 1 | 7.49 | 7.51 | 1.00 |
| lvlh5_tft | 3 | 7.65 | 7.64 | 1.00 |
| lvlh5_tft | 7 | 8.00 | 7.96 | 1.01 |
| lvlh5_tft | 14 | 8.40 | 8.24 | 1.02 |
| lvlh5_tft | 30 | 9.05 | 8.69 | 1.04 |
| lvlh5_cbd | 0 | 10.27 | 10.11 | 1.02 |
| lvlh5_cbd | 1 | 10.22 | 10.10 | 1.01 |
| lvlh5_cbd | 3 | 10.26 | 10.07 | 1.02 |
| lvlh5_cbd | 7 | 10.79 | 10.60 | 1.02 |
| lvlh5_cbd | 14 | 11.05 | 10.74 | 1.03 |
| lvlh5_cbd | 30 | 11.23 | 11.25 | 1.00 |
| prod_served | 3 | 7.87 | 7.82 | 1.01 |
| prod_served | 7 | 8.23 | 8.16 | 1.01 |
| prod_served | 14 | 8.69 | 8.50 | 1.02 |
| prod_served | 30 | 9.64 | 9.18 | 1.05 |

## D9 — coverage and openness

Share of operating truth slots with a forecast, per lead:

| lead | clim | h5 | lvlh5_cbd | lvlh5_naive | lvlh5_tft | oracle_level | oracle_shape | prod_served | snaive7 | wt_med |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.89 | 0.98 | 0.57 | 0.93 | 0.68 | 0.97 | 0.92 |  | 0.83 | 0.97 |
| 1 | 0.89 | 0.98 | 0.56 | 0.93 | 0.68 | 0.97 | 0.93 |  | 0.83 | 0.97 |
| 2 | 0.89 | 0.98 | 0.56 | 0.93 | 0.68 | 0.97 | 0.93 |  | 0.83 | 0.97 |
| 3 | 0.89 | 0.97 | 0.55 | 0.93 | 0.68 | 0.97 | 0.93 | 0.63 | 0.83 | 0.96 |
| 4 | 0.89 | 0.97 | 0.55 | 0.93 | 0.67 | 0.97 | 0.93 | 0.63 | 0.84 | 0.96 |
| 5 | 0.89 | 0.97 | 0.55 | 0.94 | 0.67 | 0.97 | 0.94 | 0.63 | 0.84 | 0.96 |
| 6 | 0.89 | 0.97 | 0.55 | 0.94 | 0.67 | 0.97 | 0.94 | 0.62 | 0.84 | 0.96 |
| 7 | 0.89 | 0.97 | 0.54 | 0.92 | 0.66 | 0.97 | 0.91 | 0.62 |  | 0.96 |
| 10 | 0.89 | 0.97 | 0.53 | 0.92 | 0.66 | 0.96 | 0.92 | 0.62 |  | 0.96 |
| 14 | 0.88 | 0.97 | 0.52 | 0.91 | 0.64 | 0.96 | 0.91 | 0.61 |  | 0.95 |
| 21 | 0.88 | 0.96 | 0.50 | 0.90 | 0.62 | 0.96 | 0.90 | 0.59 |  | 0.94 |
| 30 | 0.87 | 0.96 | 0.46 | 0.90 | 0.49 | 0.95 | 0.90 | 0.50 |  | 0.94 |
| 45 | 0.85 | 0.95 | 0.13 | 0.88 | 0.36 | 0.95 | 0.88 | 0.37 |  | 0.92 |
| 60 | 0.83 | 0.94 |  | 0.86 | 0.01 | 0.94 | 0.87 | 0.00 |  | 0.91 |
| 90 | 0.79 | 0.92 |  | 0.84 |  | 0.92 | 0.86 |  |  | 0.89 |

Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):

| L | lvl_naive4 | lvl_wt56 | lvl_snaive7 | lvl_clim | lvl_tft | lvl_cbd | n_ride_days |
|---|---|---|---|---|---|---|---|
| 1 | 0.93 | 0.98 | 0.91 | 0.92 | 0.70 | 0.57 | 145986 |
| 2 | 0.93 | 0.98 | 0.91 | 0.92 | 0.69 | 0.56 | 144715 |
| 3 | 0.93 | 0.98 | 0.91 | 0.92 | 0.69 | 0.56 | 143994 |
| 4 | 0.94 | 0.98 | 0.92 | 0.92 | 0.69 | 0.56 | 143309 |
| 5 | 0.94 | 0.98 | 0.92 | 0.91 | 0.68 | 0.55 | 142638 |
| 6 | 0.94 | 0.98 | 0.92 | 0.91 | 0.68 | 0.55 | 141930 |
| 7 | 0.91 | 0.98 | 0.00 | 0.91 | 0.68 | 0.55 | 141084 |
| 10 | 0.92 | 0.98 | 0.00 | 0.92 | 0.67 | 0.54 | 138660 |
| 14 | 0.91 | 0.98 | 0.00 | 0.91 | 0.66 | 0.52 | 135709 |
| 21 | 0.90 | 0.97 | 0.00 | 0.91 | 0.64 | 0.50 | 130683 |
| 30 | 0.90 | 0.97 | 0.00 | 0.91 | 0.50 | 0.47 | 123609 |
| 45 | 0.89 | 0.97 | 0.00 | 0.90 | 0.37 | 0.15 | 113174 |
| 60 | 0.87 | 0.96 | 0.00 | 0.89 | 0.00 | 0.00 | 102285 |
| 90 | 0.86 | 0.96 | 0.00 | 0.87 | 0.00 | 0.00 | 79830 |
| 120 | 0.82 | 0.94 | 0.00 | 0.85 | 0.00 | 0.00 | 57445 |
| 180 | 0.79 | 0.91 | 0.00 | 0.87 | 0.00 | 0.00 | 19826 |

Forecast present vs ride operated (ride-days with readings; a ride-day without any reading is UNKNOWN and not counted):

| L | snaive7 P(fc|operated) | snaive7 P(fc|not operated) | wt_med P(fc|operated) | wt_med P(fc|not operated) | clim P(fc|operated) | clim P(fc|not operated) | h5 P(fc|operated) | h5 P(fc|not operated) | lvlh5_naive P(fc|operated) | lvlh5_naive P(fc|not operated) | lvlh5_tft P(fc|operated) | lvlh5_tft P(fc|not operated) | lvlh5_cbd P(fc|operated) | lvlh5_cbd P(fc|not operated) | prod_served P(fc|operated) | prod_served P(fc|not operated) | operated_ride_days | not_operated_ride_days |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0.81 | 0.25 | 0.94 | 0.75 | 0.86 | 0.71 | 0.94 | 0.76 | 0.88 | 0.65 | 0.67 | 0.53 | 0.55 | 0.40 | 0.00 | 0.00 | 435371.00 | 34758.00 |
| 1 | 0.81 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.88 | 0.65 | 0.66 | 0.53 | 0.54 | 0.40 | 0.00 | 0.00 | 432343.00 | 34647.00 |
| 2 | 0.81 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.88 | 0.65 | 0.66 | 0.53 | 0.54 | 0.39 | 0.00 | 0.00 | 429640.00 | 34483.00 |
| 3 | 0.82 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.66 | 0.53 | 0.53 | 0.39 | 0.58 | 0.45 | 427074.00 | 34312.00 |
| 4 | 0.82 | 0.25 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.66 | 0.53 | 0.53 | 0.39 | 0.58 | 0.44 | 424518.00 | 34130.00 |
| 5 | 0.82 | 0.26 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.66 | 0.52 | 0.53 | 0.39 | 0.58 | 0.44 | 422064.00 | 34014.00 |
| 6 | 0.83 | 0.26 | 0.93 | 0.76 | 0.86 | 0.71 | 0.94 | 0.77 | 0.89 | 0.66 | 0.65 | 0.52 | 0.53 | 0.39 | 0.58 | 0.44 | 419640.00 | 33894.00 |
| 7 | 0.00 | 0.00 | 0.93 | 0.77 | 0.86 | 0.70 | 0.94 | 0.77 | 0.88 | 0.66 | 0.65 | 0.52 | 0.52 | 0.38 | 0.57 | 0.44 | 416862.00 | 33811.00 |
| 10 | 0.00 | 0.00 | 0.93 | 0.76 | 0.86 | 0.70 | 0.94 | 0.77 | 0.89 | 0.67 | 0.64 | 0.51 | 0.51 | 0.38 | 0.57 | 0.43 | 408905.00 | 33427.00 |
| 14 | 0.00 | 0.00 | 0.93 | 0.77 | 0.86 | 0.70 | 0.94 | 0.78 | 0.88 | 0.67 | 0.63 | 0.50 | 0.50 | 0.37 | 0.56 | 0.42 | 399433.00 | 32769.00 |
| 21 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.71 | 0.94 | 0.79 | 0.88 | 0.68 | 0.61 | 0.48 | 0.48 | 0.36 | 0.54 | 0.41 | 383417.00 | 31395.00 |
| 30 | 0.00 | 0.00 | 0.93 | 0.78 | 0.86 | 0.71 | 0.93 | 0.79 | 0.88 | 0.69 | 0.49 | 0.41 | 0.45 | 0.35 | 0.46 | 0.37 | 362041.00 | 29930.00 |
| 45 | 0.00 | 0.00 | 0.93 | 0.79 | 0.85 | 0.71 | 0.93 | 0.80 | 0.88 | 0.70 | 0.36 | 0.34 | 0.13 | 0.11 | 0.35 | 0.30 | 326652.00 | 27344.00 |
| 60 | 0.00 | 0.00 | 0.93 | 0.81 | 0.85 | 0.72 | 0.93 | 0.81 | 0.88 | 0.72 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 289193.00 | 24515.00 |
| 90 | 0.00 | 0.00 | 0.93 | 0.83 | 0.84 | 0.72 | 0.93 | 0.84 | 0.88 | 0.75 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 223863.00 | 19451.00 |
