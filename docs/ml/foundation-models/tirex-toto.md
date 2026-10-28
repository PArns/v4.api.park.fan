# TiRex-2 and Toto 2.0 in ml-bench (PAR-853)

Two more pretrained models, scored on the same inputs as `chronos2` and inside the same run, so
every margin against Chronos-2 is paired. **Verdict: drop both for serving; keep TiRex-2 as a
plug-in** — the first choice if Chronos-2 ever falls away for licence or operational reasons.
Shared adapter and licences: [README.md](README.md).

| | TiRex-2 | Toto 2.0 |
| --- | --- | --- |
| Weights | `NX-AI/TiRex-2`, revision `05e5b26d` | `Datadog/Toto-2.0-313m`, revision `a7bab288` |
| Package | `tirex-2==0.2.1` | `toto-2==2.0.0` |
| Licence | Apache-2.0, code and weights | Apache-2.0, code and weights |
| Covariates | native past + known-future | a `known_dynamic` hook, but exogenous input is not a trained feature of 2.0 |
| Multivariate | one park = one task | one park = one task, `series_ids` shared |
| Quantiles | fixed head 0.1 … 0.9 — no q95 (NaN, D8 reports it missing) | 9 knots, q50 / q80 read off — no q95 |
| Horizon | 320-step head, rolled forward by feeding the median back | one `forecast(horizon=…)` call |
| Context | 2,048 steps (`MLBENCH_TIREX2_CONTEXT`) | 4,096 steps (`MLBENCH_TOTO2_CONTEXT`), left-padded to whole patches |

TiRex v1 (`NX-AI/TiRex`) is under the NXAI Community License and univariate; TiRex-2 adds
covariates under Apache-2.0, so it is the one benchmarked.

| Variant | Context | Covariates | Origins |
| --- | --- | --- | --- |
| `tirex2` | compressed, 2,048 | yes | daily + intraday |
| `tirex2_grid` | regular grid, 2,048 steps = 21 days | yes | daily slots |
| `toto2` | compressed, 4,096 | through `known_dynamic` | daily + intraday |
| `toto2_nocov` | compressed, 4,096 | none (the official Toto 2.0) | daily slots |
| `toto2_grid` | regular grid, 4,096 steps = 42 days | through `known_dynamic` | daily slots |

## Construction traps

- **`CUDA_HOME` is required to import TiRex-2** on an image without a CUDA toolkit: `xlstm`'s sLSTM
  module resolves kernel paths at import time and raises, aborting the whole run at the first origin
  with nothing written. `TiRex2Backend.__init__` sets `os.environ.setdefault("CUDA_HOME",
  "/usr/local")` first; TiRex-2 never instantiates the sLSTM cell and runs the pure-PyTorch recurrent
  path, so the value is never used to build anything.
- **`torch.compile` has no compiler** in the slim image: `torch._dynamo.config.disable = True` runs
  it eagerly with the same numbers.
- A CPU smoke of `toto2` took 568 s per origin-task; on the GPU it is 24.7 s.

## Result

Run `20261011-par853-toto-tirex-shard5`: 47 origins (`--shard 0/5`, all seven weekdays) 2026-02-19 …
10-07 on the 30-park subset, paired with `h5`, `lvlh5_tft`, `persistence` and all other arms; git
`474e3932`, `code_sha256` `8ee46789…`; 5.0 h, 3.4 GB VRAM peak. Every figure comes out of
`ml-bench/scripts-par853/summarise.py` over the run's `cells_vs_*.csv`.

Full period, all rides, paired against `chronos2` (park-cluster CI):

| Band | `tirex2` | `toto2` |
| --- | --- | --- |
| m015 | **−0.070** [−0.124, −0.025] | **−0.176** [−0.242, −0.106] |
| m030 | +0.159 [+0.085, +0.234] | +0.014 [−0.036, +0.071] (tie) |
| m060 | +0.411 [+0.272, +0.560] | +0.154 [+0.097, +0.212] |
| m120 | +0.582 [+0.417, +0.777] | +0.300 [+0.209, +0.399] |
| h2–4 | +0.576 [+0.431, +0.738] | +0.317 [+0.222, +0.430] |
| h8+ | +0.443 [+0.265, +0.642] | +0.318 [+0.200, +0.459] |

- Both beat Chronos-2 at 15 minutes only; from 30 minutes Chronos-2 wins everywhere. Against `h5`
  both are clearly better in the live bands (`toto2` −4.19 … −1.65, `tirex2` −4.08 … −1.36 over
  m015–m120) and worse on the planner from d1 (+0.32 … +0.88).
- **Planner.** `tirex2_grid` is the only variant that keeps up with `chronos2`: a tie at every lead
  d0–d7 on all rides and better than `chronos2_nocov` throughout (−0.364 … −0.536), but worse on busy
  rides at d0, d2–d5 and d7. `tirex2_x_h5` reaches d21 on busy rides and wins the d10 hand-over cell
  (−0.453 [−0.710, −0.254]) — the one lead a PAR-853 arm takes off Chronos-2, whose `chronos2_x_h5`
  wins d1–d7, d14, d21 and d30. `toto2_x_h5` reaches d3. The composite arms were not in `--pair-with`,
  so they were not paired with each other.
- **`toto2_grid` is unusable**: +4.25 … +4.90 min behind `chronos2` on all rides (+10.07 … +11.58
  busy), q80 coverage 0.54. The cause is Toto 2.0, not the grid builder: `tirex2_grid` uses the same
  path and ties Chronos-2, the penalty appears in 30 of 30 parks, and the same weights on the
  compressed context are only +0.3 … +0.8 behind. Toto 2.0 does not cope with a 42-day grid that
  includes closed hours. The variant stays registered so nobody tries it twice.
- **Toto's `known_dynamic` covariates do nothing**: `toto2` and `toto2_nocov` differ by 0.03–0.04
  min, both tied with `chronos2_nocov`.
- **Uncertainty.** TiRex-2's q80 coverage is 0.80–0.83 (Chronos-2 0.79–0.81); neither model has a
  q95, and D8 leaves the field empty for all five variants.

Runtime (GPU smoke, RTX 5080, 30 parks, one origin): daily slots `tirex2` 16.7 s, `toto2` 24.7 s,
`chronos2` 28.2 s; intraday slot task 6.0–7.3 / 11.7–12.8 / 14.0–14.7 s; all seven arms in one origin
431.4 s, peak 3.29 GB. Model load: TiRex-2 3.7 s, Toto 2.0 ~8 s, Chronos-2 10.5 s.

In the admin matrix the two are the `fm` column, offline only (no archived curve):
`offline-model-verdicts.json` carries their cells from this run.
