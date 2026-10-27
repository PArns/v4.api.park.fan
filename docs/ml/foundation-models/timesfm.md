# TimesFM 3.0 and 2.5 in ml-bench (PAR-828, PAR-1064)

Google's TimesFM: version 3.0 is ruled out and was never scored; version 2.5 is implemented and
waiting for the combined run. Open it before running or proposing either. Shared adapter and
licence rules: [README.md](README.md).

| | TimesFM 3.0 (`timesfm3`, `timesfm3_owx`) | TimesFM 2.5 (`timesfm25`, `timesfm25_grid`) |
| --- | --- | --- |
| Weights | `google/timesfm-3.0-pytorch` | `google/timesfm-2.5-200m-pytorch`, revision `1d952420` (`MLBENCH_TFM25_REVISION`) |
| Package | `timesfm==3.0.2` | same wheel (`timesfm.TimesFM_2p5_200M_torch`) |
| Licence | code Apache-2.0, **weights non-commercial and non-production — benchmark-only** | Apache-2.0 — servable |
| Covariates | native past + future | none in the network; added by in-context regression (XReg) |
| Multivariate | variate attention capped at 32 — each ride is a univariate task | univariate; the XReg regression is shared by a park's rides |
| Quantiles | fixed head 0.1 … 0.9 — **no q95** | continuous head 0.1 … 0.9 — **no q95** |
| Status | never scored | not run |

## TimesFM 3.0 — ruled out, never measured

No TimesFM-3 number exists. This is a decision, not a result:

1. **No q95.** BENCH-SPEC requires q0.8 and q0.95 coverage, and q0.95 drives the frontend's
   `uncertaintyMinutes` / `accuracy.typicalError`. TimesFM-3 cannot serve that field without an
   added head.
2. **Licence.** The weights' licence excludes production and end-user use outright
   ([licences](README.md#licences)); serving would need a commercial licence from Google.

Further handicaps: 32-variate cap (40–70 rides per park), and it interpolates NaN linearly, so it
could only run on the compressed context — the layout Chronos-2 lost on. The adapter is
implemented, registered and tested with the stub backend; `--model timesfm3` scores it as a
benchmark-only reference arm. `timesfm3` is a listed prefix in
`ml-bench/mlbench/benchmark_only_models.json`, so no verdict writer may emit it.

## TimesFM 2.5 (PAR-1064)

- **Covariates via XReg.** `forecast_with_covariates`, mode `xreg + timesfm`: a linear fit (OLS
  through `pinv` after per-series normalisation) of the target on the covariates over the context,
  then TimesFM forecasts the residual. Called once per park. Needs jax + scikit-learn
  (`timesfm[xreg]`); the image uses CPU jax (`force_on_cpu=True`; 0.10.2 since the PAR-1066
  dependency upgrade). Covariates are `chronos2`'s, except `day_len_h` on the daily series: XReg
  cannot mask, so NaN ("beyond the publishing horizon") would read as a closed day. Other NaN
  covariates enter as 0.
- **NaN in the target** is cut at the front and interpolated linearly up front (the regression
  target may not hold NaN). On the grid that turns every night into a ramp; `timesfm25_grid` exists
  to measure what that costs.
- **Quantiles:** q0.5 and q0.8 read at columns 5 and 8; q95 is NaN and D8 reports it missing. The
  head covers 1,024 steps; the longest request is 768 (8 days on the grid) or 366 (daily path), and
  a longer one raises.
- **Knobs:** `MLBENCH_TFM25_CONTEXT` (8,192), `MLBENCH_TFM25_BATCH` (32); `torch_compile=False`
  (inductor needs a C compiler the slim image lacks).
- **CPU smoke with the real weights:** plain and XReg paths return finite q50/q80 for rides that ran
  and NaN for one that never did; a 366-step daily horizon works.
- `timesfm25` covers daily + intraday slots and the daily level (`timesfm25_x_h5`);
  `timesfm25_grid` daily slots only and is left out of the queued run to keep the night.
