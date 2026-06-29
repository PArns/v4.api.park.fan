# TFT GPU Tuning Plan — nf-service

## Context

celestrial (RTX 5080, 16 GB VRAM) is now the production host, and nf-service/TFT
**already sees the GPU** (`torch.cuda.is_available() == True`, CUDA 12.8 / Blackwell
sm_120 build, `accelerator: "auto"`). CatBoost was proven to NOT benefit from GPU
(high-cardinality categoricals collapse — see `project_catboost_gpu`), so it stays on
CPU. **TFT is the workload the GPU was bought for** — a real neural net, GPU-bound.

The catch: almost every current TFT setting in `nf-service/config.py` is an **OOM
workaround** from when training ran CPU-only on a RAM-starved host. Comments literally
say so: `windows_batch_size=128` is "THE memory lever … keep it small"; `max_steps=500`
is "PoC training steps (tune up later)"; `hidden_size=64`; `batch_size=16`;
`NF_PARK_CHUNK_SIZE=10` ("small per-chunk panel keeps the shared-memory footprint
bounded"). These caps throttle model capacity. On a 16 GB GPU most of them can be
lifted — **this is where the real boost from the new system comes from.**

Authoritative targets (Nixtla AutoTFT default search space + the TFT tutorial):

| Param | Current (OOM-capped) | AutoTFT search space | GPU target |
|---|---|---|---|
| `hidden_size` | 64 | 64 / 128 / 256 | **128** (→256 if VRAM allows) |
| `windows_batch_size` | 128 | 128 / 256 / 512 / 1024 | **512** |
| `batch_size` | 16 | 32 / 64 / 128 / 256 | **64** |
| `max_steps` | 500 | 500 / 1000 / 2000 | **2000** (+ early stop) |
| `n_head` | 4 (default) | 4 / 8 | **8** |
| `input_size` | 90 | h×{1..5}=30..150 | keep 90 |
| `learning_rate` | 1e-3 | loguniform(1e-4,1e-1) | keep 1e-3, sweep later |

Sources: [AutoTFT search space](https://nixtlaverse.nixtla.io/neuralforecast/), [TFT tutorial](https://nixtlaverse.nixtla.io/neuralforecast/docs/tutorials/forecasting_tft.html), [pytorch-forecasting TFT tuning](https://pytorch-forecasting.readthedocs.io/en/v1.4.0/tutorials/stallion.html).

## Goal

Measurably better TFT daily-peak forecasts (especially busy-day MAE/bias on headliners),
exploiting the GPU's VRAM + speed — validated against the current champion before
anything goes live. Concretely: beat the current TFT and stay ≥ CatBoost on the busy
subset that TFT is meant to win.

---

## Levers (priority order)

### 1. Scale model capacity (the headline change)
`config.py`: `hidden_size 64→128`, `windows_batch_size 128→512`, `batch_size 16→64`,
`n_head 4→8`. All already env-vars (`NF_*`) → tune without code changes.
- *Why:* TFT capacity was deliberately starved for CPU RAM. VRAM is now the budget and
  attention parallelizes on GPU, so bigger batches also train faster, not just better.

### 2. Train longer + early stopping (avoid the 500-step PoC underfit)
`max_steps 500→2000`, and add a validation set + early stopping so longer training
can't overfit. `nf.fit()` in `forecast.py:212` is called **without `val_size`** →
no validation, no early stop. Add `val_size = NF_HORIZON` and pass
`early_stop_patience_steps` to the model (NeuralForecast supports it via the trainer).
- *Why:* 500 steps was a time-saving PoC cap; on GPU 2000 steps is cheap, and early
  stopping makes it safe.

### 3. Revisit chunking (`NF_PARK_CHUNK_SIZE=10`)
Chunked training was a **CPU shared-memory** bound (dataloader workers). On GPU the
constraint is VRAM, not host shared-mem. Test larger chunks or full-panel training —
fewer chunks = the static encoder and cross-series attention see more series at once
(the TFT paper's #1 quality lever is exactly cross-series static covariates).
- *Caveat:* keep `num_workers`/shared-mem in mind; raise chunk size empirically while
  watching both CPU-RAM and VRAM.

### 4. (Later) learning-rate / loss sweep
Once 1–3 land, sweep `learning_rate` (1e-4..3e-3) and re-test `studentt` vs the
rejected quantile losses now that capacity is higher — the earlier rejection was at
hidden_size=64, so it's worth re-checking at 128.

---

## Validation (must pass before promoting anything)

Reuse the existing tooling — do **not** build new:
- **`nf-service/backtest_headliners.py`** — OOS backtest, prints TFT vs CatBoost MAE+bias
  split by `quiet<40 / busy>=40 / busy>=70`. This is the scoreboard.
- **`docs/ml/neuralforecast-tft-evaluation.md`** — log each challenger's numbers there.

Success criteria (from the challenger discipline already in the docs):
1. **Primary:** busy-day MAE (`busy>=40`, `busy>=70`) on headliners improves vs the
   current TFT *and* stays ≤ CatBoost on those buckets (TFT's whole reason to exist).
2. **Guardrail:** overall/quiet MAE must not regress materially.
3. Only after passing: persist + serve. nf-service has its own serving path; mirror the
   CatBoost discipline — keep the prior forecast for rollback.

## Safe iteration loop (no prod risk, mirrors the CatBoost tuning we just did)

1. Edit `config.py` / `forecast.py` locally; lint (`ruff`).
2. `scp` + `docker cp` into the running nf-service container, clear `__pycache__`.
3. Run an **isolated** training+backtest in-container (its own version tag / output
   path), so production's `nf_forecast.parquet` is untouched until validated.
4. Read GPU use live: `nvidia-smi` during fit (expect VRAM climb + GPU-util > 0 —
   today's idle baseline was 2 MiB / 0 %).
5. Compare backtest numbers; if better → commit + push (Coolify redeploy). If not →
   iterate. env-gating means most tuning needs no rebuild.

## Critical files

- `nf-service/config.py` — all `NF_*` levers (hidden_size, windows_batch_size,
  batch_size, n_head?, max_steps, park_chunk_size, learning_rate, loss).
- `nf-service/forecast.py` — `_build_models()` (`~125`, TFT params + `trainer_kw`),
  `nf.fit()` (`212`, add `val_size` + early stopping), `n_head` is not currently passed
  → add it as `NF_N_HEAD`.
- `nf-service/backtest_headliners.py` — validation scoreboard (run as-is).
- `docs/ml/neuralforecast-tft-evaluation.md` — record results.
- `docker-compose.production.yml` — nf-service `mem_limit: 14g`; revisit if larger
  chunks need more CPU RAM (host has 29 GB + 32 GB swap; nf+ml training peaks must
  still coexist — budget before raising).

## Open questions for sign-off

1. **VRAM headroom:** 16 GB only. hidden_size=128 + windows_batch_size=512 must fit
   alongside anything else on the GPU. First isolated run is a VRAM probe (`nvidia-smi`)
   before committing to the biggest config.
2. **n_head plumbing:** currently not exposed — add `NF_N_HEAD` (small code change).
3. **Chunk size vs mem_limit:** how aggressively to lift chunking vs the 14 g container
   cap and coexistence with the CatBoost CPU training window.
