# Reference baseline run — results and decisions (PAR-827)

What the reference run `ml-bench/results/20261009-baselines-v2/` measured for the naive
baselines, H5, the TFT level × H5 and production's composed planner tier, and which serving
decisions rest on it. Open it before you change the planner's level routing
(`TFT_LEVEL_MAX_LEAD_DAYS`), the H5 composer, or quote a baseline figure. Rules for reading a
cell are in [harness.md](harness.md#when-a-model-wins); the generated tables are the source for
every figure below.

## The run

Export of 2026-10-09 (289 UTC days, 44.2 M change-log rows → 13,455,954 truth slots, 403,317
ride-days, 3,601 rides, 157 parks), 231 daily origins 2026-02-19 → 2026-10-07, 3 shards,
54.4 shard-minutes. Scoring sums: git `ea9a8438`, `code_sha256` `25ed0c3b26d3accd`; verdict
tables: git `ca29858b`, `code_sha256` `efd8088545a5852a` (`report-*.json`). Truth **kept
heartbeats** (the old default, see below).

Three report passes:

| Pass | Files | What it fixes |
| --- | --- | --- |
| Headline (common target window 2026-08-15 … 10-07) | `summary_tables_2026-08-15_2026-10-07.md`, `tables_2026-08-15_2026-10-07/` | the target days: every lead scored on the same 54 days (17 summer, 37 autumn) |
| Full period | `summary.md`, `tables/` | nothing: year-round, lead mixed with target season |
| Origin-constant (origins 2026-08-15 … 10-07) | `summary_tables_o2026-08-15_2026-10-07.md`, `tables_o…/` | the origin days: a good control over d0–d10, degenerate past d14 |

The earlier `20261009-baselines/` is superseded (`SUPERSEDED.md`): figures quoted from it or from
the first revision of PR #445 do not match. The headline window has no Easter, Pentecost, peak
summer or Christmas; the full period is the year-round reading. **Where the two disagree, neither
alone is the answer** — a year-round serving change rests only on what survives both.

## Decisions this run drove

- **TFT level × H5 only to d2** (PAR-901): `TFT_LEVEL_MAX_LEAD_DAYS` in
  `src/common/utils/h5-profile.util.ts` went 6 → 2. See [the TFT level](#the-tft-level-on-busy-rides).
- **Plain H5 for the planner's 15-minute curve**: the one profile result that is not seasonal.
  Served behind `PLAN_DAY_H5_COMPOSER` (decision `planner_h5_slots`, judged 2026-12-22).
- **CatBoost's daily level is never applied to the planner curve** (it makes it worse).
- **Persistence stays the live answer for the first ~2 h**; the hand-over to a level-scaled profile
  is inside 2–4 h.

## What survives both passes

Of the 296 hand-over rows present in both passes, 189 name the same winner and 103 the same
non-naive model; those 103 are what a year-round change may rest on.

| Surface | Robust in both passes | Headline window only (autumn) | Full period only |
| --- | --- | --- | --- |
| UC3 slot MAE, all rides | `h5` d2–d5 and d90 | `lvlh5_tft` d0–d1 | `h5` to d45 |
| UC3 slot MAE, ex-ante busy | `lvlh5_tft` **d0–d2**; `h5` d90 | `lvlh5_tft` d3–d10 | `h5` to d45 |
| UC2 intraday | `lvlh5_tft` busy h2–4 and h4–8; `h5` all h8+ | `lvlh5_tft` all h2–8, busy h8+ | `h5` all/busy h8+ |
| D5 first hour | `lvlh5_tft` d0–d7 | — | to d14–d45 |
| D4 dayPeak ordering | `lvlh5_tft` d0–d45 | — | — |
| D4 optimiser regret | `h5` d1, `lvlh5_tft` d3 | — | `h5` to d30 |
| D3 best time (regret, both hit rates) | `h5` d0–d45 | — | to d60 |
| Within-ride-day Spearman | `h5` d0–d14 and d90; `prod_served_lin` d21–d45 | — | `h5` to d90 |
| UC3 dayPeak abs error | `lvlh5_tft` d0; `prod_served_lin` d3 | `lvlh5_tft` d1–d2 | to d5 |
| D6 crowd bucket | nothing | `lvl_tft` d7–d21 (+0.023 … +0.037) | — |
| D7 day comparison | `lvl_tft` d31–90 (+0.026 [+0.007, +0.045]) | `lvl_tft` d8–30 (+0.071) | — |
| UC4 daily ranking | nothing wins in either pass | — | — |
| UC1 live 15–120 min, D2 live window | `persistence` | — | — |
| D1 next-best ride, D5 rope-drop worth | nothing beats the naives | — | — |

## Findings by surface

### Live (UC1, D2) and the hand-over to profiles

- `persistence` beats every naive and every profile at every lead to 2 h (MAE 2.46 min at 15 min,
  7.58 at 120 min) and owns the live window 0–45 min on headliners (6.05 vs ≥ 11.90 for any
  forecast). The best profile is still +0.64 [+0.30, +0.96] behind at 120 min (busy +0.86).
- The hand-over happens inside **2–4 h**: there `lvlh5_tft` beats `persistence` by −0.43
  [−0.77, −0.12] (busy −1.03 [−1.59, −0.46]) while `persistence` is level with the weekday median;
  by 4–8 h `persistence` is +3.02 [+2.37, +3.75] behind.
- Heartbeat contamination flatters `persistence` (see below), so these live margins are
  conservative for every other model.

### The TFT level on busy rides

**The load-bearing correction.** In the headline window `lvlh5_tft` beats every naive on busy
rides at every lead d0–d10 (d10: −0.56 [−1.11, −0.05] vs `wt_med`, −0.59 [−1.01, −0.15] vs `clim`).
On the full period it beats the weekday median only to **d2** (d2 −0.415 [−0.699, −0.149]; d3
−0.267 [−0.553, +0.002]; d30 +0.420 [+0.116, +0.708], significantly worse). Widening the routing to
d10 would have been a year-round change justified by 37 autumn days — hence d0–d2 (PAR-901).

The origin-constant pass shows the d10 band is real for **autumn-shoulder origins**, not an
origin-drift artefact: busy rides, `lvlh5_tft`, paired share 0.97–0.98 throughout:

| Lead | vs `wt_med` (park-cluster) | vs `clim` (park-cluster) | Beats envelope |
| --- | --- | --- | --- |
| d0 | −1.361 [−1.908, −0.814] | −2.363 [−2.800, −1.835] | yes |
| d3 | −1.151 [−1.735, −0.550] | −1.718 [−2.163, −1.224] | yes |
| d7 | −0.897 [−1.494, −0.324] | −0.974 [−1.461, −0.452] | yes |
| d10 | −0.988 [−1.640, −0.376] | −0.632 [−1.175, −0.036] | yes |
| d14 | −0.708 [−1.479, +0.045] | +0.201 [−0.439, +0.906] | no |

Its origin mix stays 61–69 % autumn over d0–d10 (the headline window drifts 69 → 50 %), and the
harness itself marks d30 (24 origin days) and d45 (9) LOW-N. The full period's d0–d2 is a
year-round average that weights the season where the level helps at about one sixth.

So the band is **d0–d10 for late-summer/autumn origins and d0–d2 year-round**. A seasonal gate is a
**hypothesis**, not shippable: its non-autumn arm was never measured on its own (only as a blend),
and truth starts 2025-12-24, so "autumn" and "Sep–Oct 2026" are the same rows. Tests that would
settle it: one origin-constant pass over the existing `parts/` with a non-autumn origin window
(e.g. `--origin-from 2026-06-01 --origin-to 2026-07-31`, ~12 min CPU) and a second autumn
(~2027-09). Flat ≤ d6 (the pre-PAR-901 scope) is the one choice the evidence does not support.

On **all rides** the TFT level survives only d0–d1 in the headline window, nothing on the full
period, and is significantly worse than climatology from d14.

### H5 beats the weekday median everywhere

`h5` beats `wt_med` at every lead d0–d90 in all three passes (all rides −0.14 … −0.06, busy
−0.34 … −0.14, every CI excluding 0; mean margins −0.125 / −0.083 / −0.170 headline / full /
origin-constant). What caps its envelope win is the other naives: `snaive7` at d6 (−0.36
[−0.74, +0.00]) and climatology from d7 — but only in autumn-heavy data, because **climatology
beats the weekday median only in autumn** (`clim − wt_med` d1 −0.135, d7 −0.302, d14 −0.565,
d30 −0.702 in autumn; +0.16 … +1.23 in winter, spring and summer). On the full period `h5` beats
the whole envelope at every lead d0–d45 and again at d90 (failing only d60 against climatology,
−0.165 [−0.394, +0.023]). Its margin over `wt_med` holds in autumn, spring and summer and is
slightly negative only in winter (2 % of the data, +0.036 at d0 shrinking to +0.007 at d7).

So between d6 and d60 no model separates from the best naive at 95 % park-cluster confidence in
the headline window, and climatology itself is only +0.01 … +0.12 min better than H5 there, never
significantly.

### Level, not shape, is the error

At d1 a perfect level takes H5 from 7.98 to **5.34**; a perfect shape on the naive level gives
6.96. It holds at every lead (d30: 5.51 vs 8.70). Nothing in the benchmark closes the level gap;
[level drivers](../level-drivers.md) measures how much of it is explainable.

### Production's composed tier and CatBoost's daily level

- `prod_served` loses at every lead from d3 against the per-lead reference (+0.41 [+0.14, +0.71]
  d3, +0.70 d7, +1.00 d14, +1.41 [+1.07, +1.76] d30) and is worst in the opening hour (6.21 vs
  `h5` 4.32). Exception: `prod_served_lin` wins the within-ride-day Spearman at d21–d45
  (+0.015 … +0.026) and the dayPeak absolute error at d3 — good at long-lead ordering, bad at
  levels.
- `lvlh5_cbd` (CatBoost daily level) reads 10.21 at d1 against `h5` 7.98, +2.12 [+1.62, +2.68]
  against `wt_med`: a P50-like level against a P90 target. The table it read was dropped in PAR-870.

### Day ranking (UC4, D6, D7)

`lvl_snaive7` is the best level at d1–d6 (UC4 Spearman 0.33, crowd bucket exact 0.434, day
comparison 0.519). `lvl_tft` loses UC4 at every lead in both passes (175–189 park-months per lead
in the window, 264–410 on the full period); its significant loss at d30 (−0.081
[−0.144, −0.011]) is window-only (full period −0.036 [−0.077, +0.007]). Within-month ranking is
weak everywhere: the best source reaches Spearman 0.33 at d1 and 0.20–0.24 from d14 (0.28 at d90,
`lvl_wt56`), `lvl_tft` 0.13–0.17. The only robust TFT ranking win is D7 at d31–90.

### D8 — production's stated error, D9 — openness

- In the headline window production's `expectedError` under-states the error on busy rides and
  over-states it on quiet ones, more so with lead: realised ÷ stated runs 1.03–1.08 at d1, then busy
  1.09 (d3) → 1.18 (d30) → 1.13 (d60) and quiet 1.02 (d3) → 0.88 (d30) → 0.79 (d60). `wt_med`'s
  empirical q80 covers 0.846 and q95 0.920 at d1. (Until critic S13 the "headline" D8 table was a
  copy of the full-period one; only the window table is quoted here.)
- "Has a forecast" is not an open/closed decision: the weekday median publishes a curve for 83 % of
  ride-days the ride never operated (76 % on the full period). Coverage is scored apart from error.

## The season split

`mae_by_season.csv`, headline window, all rides, 06:00 origin (MAE minutes, each model on its own
coverage):

| Lead | Season | `wt_med` | `clim` | `h5` | `lvlh5_tft` |
| --- | --- | --- | --- | --- | --- |
| d0 | summer | 7.752 | 8.495 | **7.719** | 7.892 |
| d0 | autumn | 8.176 | 8.076 | 7.983 | **7.629** |
| d7 | summer | 8.224 | 8.494 | **8.198** | 8.737 |
| d7 | autumn | 8.605 | 8.304 | 8.396 | **8.355** |
| d30 | summer | 8.661 | 9.241 | **8.622** | 9.115 |
| d30 | autumn | 8.946 | **8.244** | 8.751 | 9.442 |

The TFT level's advantage is autumn-only (it is worse than `wt_med` in summer at every lead; on the
full period +0.23 worse in summer at d1, +0.62 in spring at d7), and so is climatology's. H5's gain
over the weekday median is the one that transfers across seasons — a profile-shape improvement,
not a level improvement.

## Heartbeat contamination of the target

Measured on the old target (heartbeats kept), by rebuilding the truth from the same raw export with
the filter on (export `20261010-nohb`) and a matched run pair `results/20261010-sens-hb` /
`results/20261010-sens-nohb` (68 origins 2026-08-01 … 10-07, leads 0–14, headline window;
`results/20261009-baselines-v2/heartbeat_sensitivity.py` re-derives every number):

- The column was empty before ~2026-09-06. Heartbeat-only truth slots: 0 % of the window's summer
  half, 3.88 % of its autumn half, 12.78 % on 2026-10-05 alone; 0.52 % of the whole export.
- They do not move the level (mean ride-day P90 24.7096 with, 24.7340 without) but make every
  autumn MAE 0.13–0.19 min optimistic.
- They do not explain the season reversal: `lvlh5_tft`'s autumn advantage grows (−0.547 → −0.604 at
  d0) and its summer deficit is unchanged.
- They flatter `persistence`: dropping them costs it +0.061 at 15 min and +0.275 at 120 min against
  +0.101 … +0.108 for `h5`; two regional UC1 wins at m120 disappear.
- They change 6 of 213 hand-over winners, **every one gaining significance when heartbeats are
  dropped** (UC3 all rides: `lvlh5_tft` wins d2, `h5` wins d6; `prod_served_lin` wins dayPeak abs
  error d4–d6). The published bands are therefore the conservative end: the queue-based truth can
  only widen them.

The owner decision BENCH-SPEC #6 left open is settled in favour of the queue: since PAR-1054
`build` drops heartbeats by default, like the forward archive (PAR-1024). Every table in this
run is the old target; a rebuild of the same export gives different figures.

## Sanity check against an independent implementation

Restricted to the 2026-10 review's 33 sample parks and its two target windows, a query over
`parts/slot` reproduces the review's own 15-minute numbers within 0.03, except TFT×H5 at d7 (7.58
vs 7.69, because the review scaled the rope-drop hour and BENCH-SPEC does not):

| Window, lead | `wt_med` / review | `h5` / review | `lvlh5_tft` / review TFT×H5 |
| --- | --- | --- | --- |
| 08-15 → 09-10, d1 | 7.04 / 7.05 | 6.95 / 6.96 | 7.05 / 7.06 |
| 08-15 → 09-10, d7 | 7.24 / 7.26 | 7.15 / 7.17 | 7.58 / 7.69 |
| 09-11 → 10-07, d1 | 7.22 / 7.24 | 7.06 / 7.09 | 7.11 / 7.13 |

The all-park population reads ~0.9 higher (8.11 vs 7.04 at d1) because it adds US and Asian parks
with longer queues. The query is `results/20261009-baselines-v2/sanity_33parks.sql` (33 park ids
inlined, container command in its header); its output CSV is not committed, so re-run it to check.

## Open questions

- The non-autumn origin-constant pass (above) and a second autumn for the seasonal TFT gate.
- A `mae_by_weekday` split ([harness.md](harness.md#confounds-every-reader-needs)).
- A re-run on the heartbeat-free target, to replace the conservative bands.
