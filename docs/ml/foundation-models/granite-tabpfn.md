# Granite PatchTST-FM-r2, Granite TTM-r2 and TabPFN-TS in ml-bench (PAR-1067)

Three more candidates, implemented and tested but **not run yet** — they wait for the combined run,
PAR-1141 (arms in [running.md](../ml-bench/running.md#the-foundation-model-arms-par-1063--1064--1067); image extra
`requirements-par1067.txt`, which includes `requirements-par1063.txt`, so they pair with
`chronos2`, `chronos2_ft`, `chronos2_lora` and `timesfm25` inside one run). Open it before running
them or quoting their licences. Shared adapter: [README.md](README.md).

| Variant | What it is | Origins | Quantiles |
| --- | --- | --- | --- |
| `patchtst_fm` | PatchTST-FM-r2 zero-shot, compressed context, no covariates | daily + intraday slots, daily level (→ `patchtst_fm_x_h5`) | q50 / q80 / **q95** |
| `ttm_r2` | TTM-r2 `1536-720-r2` zero-shot, compressed context, no covariates | daily + intraday slots | point only |
| `ttm_r2_ft` | TTM-r2 + the `chronos2` slot covariates as exogenous channels, fine-tuned every 28 days | daily + intraday slots | point only |
| `tabpfn_ts` | TabPFN-TS on the ride-day P90 series, day covariates + `day_len_h` | daily level only (UC4 / D6 / D7 / PEAK; `tabpfn_ts_x_h5` at slot leads) | — |

`patchtst_fm` and `ttm_r2` take no known-future covariates, so they are covariate-free reference
arms (the role of `chronos2_nocov`). The arms that can actually win are `ttm_r2_ft` (slots) and
`tabpfn_ts` (daily level, benchmark-only).

## Licence verdicts

Read from the primary sources on 2026-10-24.

| Model | Verdict | The line it rests on | Source |
| --- | --- | --- | --- |
| Granite PatchTST-FM-r2 | **servable** | metadata `license: openmdw-1.0`; README: "This model is dual‑licensed under the OpenMDW License version 1.0 and the Apache License 2.0. Users may choose to use the model under either license." Not gated. | <https://huggingface.co/ibm-granite/granite-timeseries-patchtst-fm-r2> (revision `b125275f`) |
| Granite TTM-r2 | **servable** | metadata `license: apache-2.0`. Not gated. | <https://huggingface.co/ibm-granite/granite-timeseries-ttm-r2> (branch `1536-720-r2` = commit `3ca910a8`) |
| granite-tsfm (code of both) | Apache-2.0 | PyPI `license_expression: Apache-2.0` | <https://pypi.org/project/granite-tsfm/0.3.10/> |
| TabPFN-TS → TabPFN-3.5 weights | **benchmark-only** | "TABPFN-3.5 License v1.0 — Last Revised: September 9, 2026": weights "for your non-commercial and non-production use"; "use (a) for any revenue-generating activity, (b) in direct or indirect interactions with end users or production systems, or (c) to train, fine-tune, or distill other models for commercial use, in each case is not a Non-Commercial Purpose."; "Any use of Outputs in production systems … requires a separate commercial license." Gated. | <https://huggingface.co/Prior-Labs/tabpfn_3_5/blob/main/LICENSE> |
| tabpfn-time-series / tabpfn (code) | Apache-2.0 | repository licence; PyPI `license_expression: Apache-2.0` | <https://github.com/PriorLabs/tabpfn-time-series> |

TabPFN-TS is the TimesFM 3.0 case again: open code, non-servable default weights
(tabpfn-time-series 1.3.0 pins `TABPFN_MODEL_VERSION = "v3.5"`). Whatever `tabpfn_ts` scores, it is a
reference arm; `tabpfn` is a listed prefix in `benchmark_only_models.json`.

## Covariate support

| Model | Known-future covariates | Evidence |
| --- | --- | --- |
| PatchTST-FM-r2 | none | `PatchTSTFMForPrediction.forward(past_values, past_observed_mask, …)` — the covariate argument is commented out (`# future_values … # future use`, granite-tsfm 0.3.10) |
| TTM-r2, zero-shot | none (channel-independent) | model card: exogenous use only under "Finetuning and Forecasting with Exogenous/Control Variables" |
| TTM-r2, fine-tuned | yes, via forecast channel mixing | `TinyTimeMixerConfig(exogenous_channel_indices=…, enable_forecast_channel_mixing=True)`; the r2 weights load every `head.fcm_block.*` parameter as newly initialised — the exogenous path exists only once trained |
| TabPFN-TS | yes, natively | `predict_df`: "Additional columns are treated as known covariates …" (only columns present in both context and future frame) |

## PatchTST-FM-r2 (`patchtst_fm`)

- Weights `ibm-granite/granite-timeseries-patchtst-fm-r2`, revision `b125275f`
  (`MLBENCH_PATCHTSTFM_REVISION`), ~385 M parameters, granite-tsfm 0.3.10.
- Context plus forecast must fit 8,192 steps and the model decodes at least 128 steps, so the
  adapter hands over the last `8192 − max(H, 128)` compressed slots. NaN is masked natively; a ride
  that never ran is not sent.
- The head holds 99 quantiles 0.01 … 0.99, so q0.95 is read directly — a real q95 for D8.
- `MLBENCH_PATCHTSTFM_BATCH` (16 rides per forward). CPU smoke with the real weights: MAE 0.35 over
  200 steps on a synthetic sine, quantiles monotone.

## TTM-r2 (`ttm_r2`, `ttm_r2_ft`)

- Weights `ibm-granite/granite-timeseries-ttm-r2`, branch `1536-720-r2` pinned to commit `3ca910a8`
  (`MLBENCH_TTM_REVISION`): context 1,536, horizon 720; longer requests raise. r2 is the "minutely to
  hourly" release, so there is no daily path (r2.1 adds daily data and is not benchmarked).
- The adapter z-scores each ride over its context and inverts the forecast; gaps are 0 with
  `past_observed_mask = False`; a short context is left-padded as unobserved.
- Point forecast only: q80 / q95 stay NaN (the shared output step used to copy q50 into a missing
  q80 via `np.fmax`; it now keeps it missing, tested).
- **Fine-tune `ttm_r2_ft`:** `1 + k` channels (`prediction_channel_indices=[0]`,
  `exogenous_channel_indices=[1..k]`, forecast channel mixing, `decoder_mode="mix_channel"`),
  backbone frozen, decoder + head + mixer trained with AdamW at lr 1e-3 (`MLBENCH_TTM_FT_LR`; the
  mixer starts from random weights), 400 steps (`MLBENCH_FT_STEPS`), batch 64. Exogenous channels are
  `chronos2`'s slot covariates, z-scored with their past statistics on both sides of the origin.
  Training windows are cut from the already-cut context (`ttm_windows` keeps the future end ≤ the
  context length); tested in the backend and the harness
  (`test_ttm_finetune_harness_sees_only_truth_before_its_origin`). Refit every 28 days like the other
  fine-tunes; a fit is cheap (~3 s for 20 steps on CPU), so `finetune_every_days = 1` is a one-line
  subclass if the probe makes the case.
- CPU smoke: on a clean synthetic sine the zero-shot model missed a period-40 cycle (z-MAE 1.0) but
  tracked period 96 (z-MAE 0.14); the compressed operating day (~40–50 slots) is close to the period
  it missed. The run will say whether that matters.

## TabPFN-TS (`tabpfn_ts`)

- Packages: tabpfn-time-series 1.3.0, tabpfn 9.1.0, tabpfn-client 0.6.1 (imported, never used),
  datasets 4.8.5.
- Daily level only, against `driver_level` and `wt_med`. One series per ride: the ride-day P90 (≥ 8
  slots) over the last 400 days; closed or unknown days are left out, never filled. Known-future
  columns: the `chronos2` day covariates plus `day_len_h` (NaN beyond the publishing horizon is
  TabPFN's own missing value).
- The backend receives real context and target dates (`wants_days`); the pipeline's
  `RunningIndexFeature` and `CalendarFeature` are used, `AutoSeasonalFeature` is not (closed days make
  the series irregular).
- **Local mode only** — `TabPFNMode.CLIENT` would send data to Prior Labs' cloud API. Telemetry is
  disabled (`TABPFN_DISABLE_TELEMETRY=1`) before import.
- **Tokens from the environment only, never committed or baked into the image:** `HF_TOKEN` (an
  account that accepted the gate) and `TABPFN_TOKEN` (Prior Labs, after accepting the licence at
  ux.priorlabs.ai), plus `TABPFN_MODEL_CACHE_DIR` on a mounted volume. `scripts-par828/run.sh` passes
  them through. Without them the first forecast fails loudly.
- Wiring smoke (CPU, with the ungated TabPFN v2 checkpoint): a weekday-driven synthetic level was
  forecast exactly over 14 days, a 5-day context gap skipped, a ride without data stayed NaN.
- No slot path (`slot_path = False`); the information-cut test checks the level path instead.

## Dependencies

`requirements-par1067.txt` resolves on top of the base with torch 2.11.0 (the newest cu128 wheel on
2026-10-24; granite-tsfm needs `torch<2.12,>=2.10`). It still changes scikit-learn 1.9.1 → 1.7.2
(granite-tsfm needs < 1.8) and pyarrow 26.0.0 → 25.0.1 (tabpfn-client caps it), and keeps datasets at
4.8.5. `python -m mlbench gpu-check` after the build confirms torch kept its cu128 build. Details:
[python-dependencies.md](../../development/python-dependencies.md).
