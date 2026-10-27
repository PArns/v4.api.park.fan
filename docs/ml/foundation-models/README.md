# Foundation models in ml-bench

Pretrained time-series foundation models plugged into [ml-bench](../ml-bench/README.md) through
the same restricted plug-in interface as every other candidate (DataFrames only, truth strictly
before the origin, schedules as known at the origin, weather only as an opt-in ORACLE). The
question for each is the one BENCH-SPEC asks: up to which lead does it beat the best naive
baseline, and where does it hand over? Open this folder before adding, running or quoting a
foundation model, and always before proposing to serve one (licences below).

All adapters live in `ml-bench/mlbench/models/foundation.py` (`VARIANTS`), tests in
`ml-bench/tests/test_foundation*.py`.

| Family | Variants | Licence | Status | Page |
| --- | --- | --- | --- | --- |
| Chronos-2 (PAR-828, PAR-869, PAR-1063) | `chronos2`, `chronos2_grid`, `chronos2_nocov`, `chronos2_owx`, `chronos2_ft`, `chronos2_lora` | Apache-2.0 — servable | Measured: wins UC1 30–120 min and parts of the planner; shadow in production, serving prepared. `chronos2_ft` / `chronos2_lora` not run | [chronos-2.md](chronos-2.md) |
| TimesFM 3.0 / 2.5 (PAR-828, PAR-1064) | `timesfm3`, `timesfm3_owx`, `timesfm25`, `timesfm25_grid` | 3.0 benchmark-only; 2.5 Apache-2.0 | 3.0 ruled out, never scored; 2.5 not run | [timesfm.md](timesfm.md) |
| TiRex-2, Toto 2.0 (PAR-853) | `tirex2`, `tirex2_grid`, `toto2`, `toto2_nocov`, `toto2_grid` | Apache-2.0 | Measured: drop for serving, keep TiRex-2 as a plug-in | [tirex-toto.md](tirex-toto.md) |
| Granite PatchTST-FM-r2, TTM-r2, TabPFN-TS (PAR-1067) | `patchtst_fm`, `ttm_r2`, `ttm_r2_ft`, `tabpfn_ts` | Granite servable; TabPFN-TS benchmark-only | Implemented and tested, not run | [granite-tabpfn.md](granite-tabpfn.md) |

The queued combined run (PAR-1063 / 1064 / 1067) is in
[running.md](../ml-bench/running.md#queued-the-combined-par-1063--1064--1067-run).

## Shared adapter

- **Context layout** (`context_mode`). `compressed`: only the 15-minute slots inside the published
  operating windows, day after day — nights and closed days left out, 8,192 steps ≈ 170 operating
  days; the day boundary is carried by covariates. `grid`: a regular 15-minute UTC grid with NaN
  outside the windows — 8,192 steps = 85 days. Inside a window a slot without truth is NaN in both.
  A model that interpolates NaN linearly (TimesFM) is wrong across a night on the grid.
- **Covariates** per slot: minutes since published opening, minutes until published closing,
  in-window flag (grid only), local time of day. Per park-day: weekend, weekday (sin/cos), public
  and school holiday of the own region, holiday and school-holiday counts including neighbour
  regions (ml-service `holiday_features.py`, OR semantics), any school holiday, bridge day
  (re-derived from the public-holiday flag so past and future days share one definition — the
  schedule's own flags can carry post-origin information), and the published day length
  `day_len_h` (0 = published closed, NaN beyond the publishing horizon — an absent schedule never
  becomes a closed day). `Model.covariate_history_days` hands the model the covariates of the
  context days too.
- **ORACLE weather.** `uses_oracle_weather = True` adds daily max temperature and precipitation
  **actuals** (no forecast archive exists); every `*_owx` arm is an upper bound under its own name,
  excluded from the hand-over and never a serving result.
- **Daily level.** `predict_daily` runs the model on the ride-day P90 series (≥ 8 truth slots per
  ride-day, NaN on closed days) over calendar days with the day covariates. The harness composes it
  with H5 as `<name>_x_h5` at every slot lead and scores the level itself as `lvl_<name>` (UC4, D6,
  D7). The slot path is capped at `max_lead_days = 7`; beyond d7 only `<name>_x_h5` is scored.
- **Quantiles.** D8 needs q0.8 and q0.95; `qn95__<model>` is a separate q95 denominator so a model
  without a q95 head reports it missing instead of dividing by the q80 count. Only Chronos-2 and
  PatchTST-FM-r2 have a q95.

## Licences

| Model | Verdict | Basis |
| --- | --- | --- |
| Chronos-2 | servable | Apache-2.0, code and weights |
| TimesFM 3.0 | **benchmark-only** | weights under the TimesFM Non-Commercial License v1.0, which grants "non-commercial **and non-production** use" and excludes use "in direct or indirect interactions with end users or production systems" — park.fan being non-commercial does not help; serving would need a commercial licence from Google |
| TimesFM 2.5 | servable | model card `license: apache-2.0`; the timesfm README: weights up to 2.5 remain Apache-2.0 |
| TiRex-2, Toto 2.0 | servable | Apache-2.0, code and weights (TiRex v1 is under the NXAI Community License, which is why TiRex-2 is benchmarked) |
| Granite PatchTST-FM-r2, TTM-r2 | servable | dual OpenMDW-1.0 / Apache-2.0, resp. Apache-2.0; not gated |
| TabPFN-TS | **benchmark-only** | the TabPFN-3.5 weights it downloads are non-commercial and non-production; gated |

Exact quotes and URLs for the PAR-1067 verdicts: [granite-tabpfn.md](granite-tabpfn.md#licence-verdicts).

**Enforcement.** Every foundation plug-in declares `licence` (`servable` / `benchmark-only` /
`unclear`) and `serving_candidate`; `FoundationModel.__init_subclass__` refuses a serving
candidate that is not `servable`, and a class whose name matches a prefix in
`ml-bench/mlbench/benchmark_only_models.json` (`timesfm3`, `tabpfn`) but declares another licence.
Every `run-*.json` records both per arm under `licences`. `mlbench.licences.refuse_benchmark_only`
must be called by every verdict writer (`served_verdicts` does), and
`src/ml/utils/benchmark-only-models.spec.ts` fails CI if `src/ml/data/*.json` or the model
comparison matrix names a benchmark-only model.

## GPU and runtime

Runs go through `ml-bench/scripts-par828/run.sh` (GPU lock, quiet window 00:55–09:30 UTC, HF cache
`/data/parkfan/ml-bench/par-828/hf` mounted as `/hf`) — [running.md](../ml-bench/running.md#sharing-the-gpu-with-production).
VRAM is capped with `torch.cuda.set_per_process_memory_fraction` (`MLBENCH_FM_VRAM_FRACTION`,
default 0.6 of 16 GB); every `[fm]` log line carries the running peak. Inference never came close:
the peak of all seven PAR-853 arms in one origin was 3.29 GB.

## Harness lessons from these runs

- **The history cap raises, and must.** Chronos-2 asks for ~343 days of compressed context and 400
  for the daily head; on a 56-day cap every origin raised. Truncating instead would have published
  numbers from a shortened context with nothing in the output to show it. A model declares its need
  (`truth_history_days`, read off the class).
- **The intraday history cache was keyed on the origin table's name**, which the intraday path
  reuses, so every intraday origin after 08:00 forecast from a context frozen at the first hour.
  The key now includes the cut, and `test_information_cut_plugins` runs one pass per cut. The old
  assertion had passed *because of* the bug — the third guard in this review that could not fail
  (with the two-park fixture and an `--origin-from` path never executed). Give every guard a case
  in which it can fail.
- **Information budgets are read off the class** (`type(m)`), never the instance, because
  `scored_name()` is a classmethod: an instance attribute would widen the budget and keep the honest
  name. The guard is mutation-tested through `plugin_predict` / `plugin_predict_daily`.
- **Provenance.** Record the measured `code_sha256`, not a transcribed git SHA: branches rebased
  several times a day rewrite SHAs while the tree stays identical, and two published tables have
  carried a wrong SHA.
