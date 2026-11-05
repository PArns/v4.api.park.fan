# Chronos-2 in ml-bench (PAR-828, PAR-869, PAR-1063)

What the offline benchmark measured for Amazon's Chronos-2 and its variants, and how that led to
the production shadow and the prepared serving path. Open it before quoting a Chronos-2 offline
figure or changing the Chronos-2 adapter. Shared adapter, licences and GPU rules:
[README.md](README.md).

| | |
| --- | --- |
| Weights | `amazon/chronos-2`, `chronos-forecasting==2.3.2` |
| Licence | Apache-2.0, code and weights — benchmarking, fine-tuning and serving allowed |
| Covariates | native past + known-future, per task |
| Multivariate | one park = one task, every ride a target variate (group attention across the rides of a park) |
| Quantiles | arbitrary levels (`predict_quantiles`), incl. q0.95 |
| Horizon / context | 64 output patches × 16 = 1,024 steps in one shot / up to 8,192 steps |

| Variant | What it tests |
| --- | --- |
| `chronos2` | the honest candidate: compressed layout, covariates, no weather; the only variant with intraday origins |
| `chronos2_grid` | regular grid with NaN instead of compressed |
| `chronos2_nocov` | what the covariates are worth |
| `chronos2_owx` | ORACLE weather upper bound |
| `chronos2_ft` | full fine-tune on pre-origin history: 400 steps, lr 1e-5, context 2,048, refit every 28 days — **not run** |
| `chronos2_lora` | LoRA adapter, otherwise identical to `chronos2_ft` (PAR-1063) — **not run** |

`chronos2_x_h5` (H5 shape × Chronos-2's daily level) and `lvl_chronos2` come from the daily path.

## Production status

Chronos-2 runs as a shadow in the forward archive (`chronos_live` from `chronos2`, `chronos_grid`
from `chronos2_grid` — [chronos-shadow.md](../forward-archive/chronos-shadow.md)); serving on the
45–120 min slots is prepared behind `SERVE_CHRONOS_INTRADAY`
([chronos-serving.md](../forward-archive/chronos-serving.md)). The ML overview's decisions
`live_chronos` and `planner_chronos` are judged on 2026-11-09. Offline verdicts in
`offline-model-verdicts.json`: `live_h1_2` better and `plan_d0_1` better (both against a
non-served reference, so they cannot make a cell `better` on their own), `live_h0_1` pending,
`plan_d2_5` unclear, `plan_besttime` worse, `today_h2_6` / `today_h6_12` pending (PCN is served
there and is not in the bench, so only the live archive can decide).

## Runs

- **`20261010-par828-subset-chronos-v2`** — 30 parks (`ml-bench/subsets/par828-30parks.txt`),
  `--shard 0/7` = 33 origins 2026-02-19 … 10-01, `chronos2`, `chronos2_grid`, `chronos2_nocov`,
  `chronos2_owx`, paired with `chronos2,chronos2_grid,h5,lvlh5_tft,persistence`. 5,658 s, VRAM peak
  1.86 GB. Scoring `code_sha256` `d5acedf6…` (git `cdbc95a3`), tables `4b2a41ce…` (git `3107d7c9`).
  Coverage parity with the baselines: 0 missing slots in all 48 cells over d0–d7.
  **Every origin is a Thursday** (stride 7 from a Thursday), so each planner lead is one weekday —
  see below. `20261009-par828-subset-chronos` is VOID (`SUPERSEDED.md`): it had 6,767 slots of
  coverage mismatch.
- **Mixed-weekday re-scoring.** The 47-origin `--shard 0/5` sample (all seven weekdays) re-scored
  `chronos2` / `chronos2_grid` in [`20261012-tide-vs-chronos`](../ml-bench/results-neuralforecast.md#tide-vs-chronos-2-paired-on-identical-rows-par-869)
  and `chronos2` / `chronos2_nocov` in [`20261011-par853-toto-tirex-shard5`](tirex-toto.md). The
  offline verdict cells were filled from the mixed-weekday result.

The full-period pass has ≥ 30 origin days at d0–d21 and 29–30 park units everywhere; LOW-N begins
at d30. Its `lead_availability.csv` reports the export's availability, not the shard's — use the
per-cell `n_origin_days`. The headline window holds only 7–8 origin days, so its hand-over and
usable-horizon tables say "nothing tested".

## UC1 — the live surface

Full period, 33 origin days, 30 parks, paired share ≥ 0.998, park-cluster 95 % CI; negative =
Chronos-2 better:

| Lead | − `persistence` | − `h5` | − `lvlh5_tft` |
| --- | --- | --- | --- |
| 15 min | **+0.263 [+0.179, +0.356]** | −4.017 [−4.777, −3.222] | −3.751 [−4.549, −3.002] |
| 30 min | −0.416 [−0.610, −0.212] | −3.050 [−3.663, −2.419] | −2.887 [−3.575, −2.296] |
| 60 min | −1.262 [−1.597, −0.927] | −2.534 [−3.060, −1.975] | −2.341 [−2.919, −1.839] |
| 90 min | −1.882 [−2.386, −1.396] | −2.154 [−2.658, −1.653] | −2.016 [−2.554, −1.541] |
| 120 min | −2.268 [−2.920, −1.714] | −2.048 [−2.537, −1.557] | −1.868 [−2.351, −1.408] |

Busy rides vs `persistence`: +0.440 at 15 min, then −0.917 … −4.511 [−5.727, −3.541] at 120 min.
**Chronos-2 beats both servable profile incumbents at every lead 15–120 min and `persistence` from
30 minutes.** Its usable horizon is a band (m030–m120), not a prefix, so `usable_horizon.csv`'s
empty cell is not "no horizon". Heartbeat contamination flatters `persistence`
([results-baselines.md](../ml-bench/results-baselines.md#heartbeat-contamination-of-the-target)),
so these wins are a lower bound and part of the 15-minute loss may be contamination.

- **D2** (plan-block live correction, headliners, 0–45 min): `chronos2` 6.769 vs `persistence`
  7.147 (−0.645 [−0.941, −0.333]), `h5` 12.890, `lvlh5_tft` 11.738.
- **D1** (next-best ride): precision at top-3 0.708, +0.085 [+0.017, +0.147] — the only model that
  wins there — but recall 0.240 vs `snaive7` −0.351: fewer suggestions, more of them right.

## UC2 / UC3 — the planner

On the Thursday-origin shard, `chronos2_grid` − `h5` (all rides) is −0.983 at d0 and −0.574 at d1
(wins), **+0.577 at d2** (a loss), +0.034 at d3, then −0.389 … −0.360 at d4–d6 (wins); against
`lvlh5_tft` it wins d1 and d3–d7. That "d2 hole" is the **Saturday** cell and d3 the Sunday one —
every flat profile has its minimum MAE on d2 while the level-driven models have their maximum. The
lead-indexed routing rule drawn from it was withdrawn. The weekday-controlled contrast d0 vs d7
(both Thursday) shows a real horizon effect of +0.82 … +0.84 min over seven days against `h5`.

On the mixed-weekday sample (`20261012-tide-vs-chronos`, both passes) Chronos-2 beats every scored
incumbent on planner d0 (`chronos2_grid`), on the slot curve d1–d2 and d6 for all rides
(`chronos2_grid`) and d1, d3–d4, d6–d7 for busy rides (`chronos2_x_h5`); nothing clears the
incumbents at d3–d5 and d7 on all rides, and nothing wins D3 best time.

- **Layout: the grid wins.** `chronos2_grid` − `chronos2` is better at all eight leads (UC2 d0 all
  −0.157 [−0.241, −0.068], busy −0.262; UC3 d4–d7 −0.178 … −0.293, all significant; d1–d3 only on
  the park-day unit). The compressed layout buys ~170 days of context but destroys the opening ramp.
  It costs 2–3× the GPU time (18.6–23.4 s vs 6.5–11.3 s per daily-origin call). The grid claim does
  not extend to the live surface: `chronos2_grid` has no intraday origins.
- **Covariates are decisive.** `chronos2_nocov` is worse at every lead (UC2 d0 +0.283 all / +0.653
  busy; UC3 d1…d7 +0.39 … +0.82, all significant).
- **ORACLE weather is worth < 0.12 min** (`chronos2_owx` − `chronos2` −0.086 at d1, 0.000 at d5).
  With perfect foreknowledge that is two orders of magnitude below the routing margins — so a weather
  forecast archive is not justified by this model family (PAR-830's level work is a separate
  question).
- **Hand-over** (full period): 0 of 280 rows won by an `_owx` arm; 79 go to a servable Chronos-2
  configuration (`chronos2_grid` 28, `chronos2` 27, `chronos2_x_h5` 22, `chronos2_nocov` 2) against
  `wt_med` 112, `h5` 38, `lvlh5_tft` 14, `snaive7` 11. `chronos2_x_h5` reaches furthest
  (`max_significant_lead` d14 on MAE with the schedule known, d21 on D4 dayPeak ordering and busy
  MAE); its D9 level coverage is 0.95 of operating ride-days.
- **UC4 was not tested**: with weekly origins a park-month holds at most 5 target days at a fixed
  lead, below the 8 `report.uc4()` needs to build a unit. Only a dense-origin run can measure it.
  `lvl_chronos2` still feeds D6 (`max_significant_lead` d7) and D7.

## Uncertainty (D8) and openness (D9)

| Lead | `chronos2` q80 / q95 | `chronos2_grid` q80 / q95 | `wt_med` q80 / q95 |
| --- | --- | --- | --- |
| d0 | 0.791 / 0.939 | 0.756 / 0.923 | 0.868 / 0.946 |
| d1 | 0.815 / 0.953 | 0.771 / 0.932 | 0.869 / 0.945 |
| d3 | 0.824 / 0.957 | 0.775 / 0.944 | 0.886 / 0.939 |
| d7 | 0.819 / 0.958 | 0.784 / 0.963 | 0.859 / 0.936 |

`chronos2`'s q95 sits within ±0.01 of target and is better calibrated than `wt_med`'s; its q80 is
mildly under-covered. Field coverage is 0.99+ at d0–d7 and 0 from d10 (`max_lead_days = 7`).

Chronos-2 produces a curve for every slot of the origin-known window and **never abstains**
(P(forecast | not operated) = 1.000, vs `h5` 0.890): it contributes nothing to `ridesUnavailable` /
`opensAt` and must not be routed to them.

## Runtime

| | Measured |
| --- | --- |
| VRAM peak, four variants | 1.86 GB |
| weight load | 3.7 s per process |
| per origin, 30 parks, 4 variants | 71 s (Feb) … 276 s (Oct) |
| report (CPU, 2 cpu / 4 GB) | 1,266 s full period, 769 s headline window |

Full-run cost scales roughly with ride count: all 157 parks × 231 origins ≈ 67 h for the four
variants, ≈ 52 h for `chronos2` + `chronos2_grid`; all 231 origins on the 30-park subset ≈ 12.7 h,
one night — the cheapest way to get a gradeable headline window and a measurable UC4.

## Chronos-2 LoRA (PAR-1063)

Implemented and tested, not run. chronos-forecasting 2.3.2's own
`Chronos2Pipeline.fit(finetune_mode="lora")` (peft defaults: r = 8, alpha = 16, on
`self_attention.q/k/v/o` and `output_patch_embedding.output_layer`); a CPU smoke trained 1.21 M of
120.7 M parameters. Identical to `chronos2_ft` in everything but the adaptation (a test asserts it):
400 steps, lr 1e-5, context 2,048, horizon 384, batch 64, 28-day cadence — so the pair isolates full
vs low-rank adaptation. The fit runs inside `predict()` at a daily origin on the already-cut
context; intraday origins and the next 27 days reuse the adapter
(`test_lora_fit_sees_only_truth_before_its_origin`). Without peft, chronos would silently run a
full fine-tune under the LoRA name; the backend imports peft first and fails instead.
