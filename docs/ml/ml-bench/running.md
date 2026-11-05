# Running ml-bench — adding a model, runs, GPU rules

How to plug a model into [the harness](harness.md), run and report it on celestrial without
disturbing production's GPU jobs, pair it against other models, and precompute NeuralForecast
models. Also holds the commands of runs that are implemented but not run yet.

## Adding a model

1. Copy `ml-bench/mlbench/models/example_level_h5.py`, rename the class and `name`, implement
   `predict` (and `fit` / `predict_daily` if useful). The example reproduces baseline 5a through
   the plug-in path and a test asserts the two score identically — keep that pattern.
2. Register it in `REGISTRY` (`mlbench/models/__init__.py`) or pass `--model package.module:Class`.
   A registered model is automatically covered by `test_information_cut_plugins`; run `pytest`
   before any run you report.
3. What a plug-in gets (`mlbench/models/base.py`): DataFrames only, never a database connection
   or an object from which one is reachable. `origin.history.df(days)` is truth ending before the
   origin; `train_panel` in `fit`; the horizon grid of the window known at the origin; covariates
   without weather unless the **class** sets `uses_oracle_weather = True` (it is then scored as
   `<name>_owx`). Rows outside the grid are dropped.
4. **Information budgets are part of the contract and are read off the class**, never the
   instance (an instance attribute must not be able to widen one): `uses_oracle_weather`,
   `truth_history_days` (0 = `--history-days`, default 56; the runner takes the max over the
   registered models) and `covariate_history_days`. `history.df(days)` **raises** above the
   budget rather than returning a short frame — a long-context model (Chronos-2 asks for ~343
   days) must declare its need instead of silently publishing numbers from a truncated context.
5. Foundation plug-ins also declare `licence` (`servable` / `benchmark-only` / `unclear`) and
   `serving_candidate`; see [licences](../foundation-models/README.md#licences).

## Build and run

```bash
cd ~/claude-worktrees/<your-checkout>
docker build -t ml-bench:<tag> --build-arg GIT_SHA=$(git rev-parse HEAD) \
  [--build-arg EXTRA_REQUIREMENTS=requirements-<extra>.txt] \
  --build-context mlsvc=ml-service ml-bench
docker run --rm --gpus all ml-bench:<tag> gpu-check          # must list sm_120
IMAGE_ID=$(docker image inspect -f '{{.Id}}' ml-bench:<tag>)
docker run -d --name mlbench-<model> --gpus all --user 1000:1000 -e HOME=/tmp \
  -e MLBENCH_IMAGE_ID=$IMAGE_ID --cpus 6 --memory 8g --cpu-shares 256 --entrypoint nice \
  -v /data/parkfan/ml-bench:/data -v $PWD/ml-bench/results:/app/results \
  ml-bench:<tag> -n 10 python -m mlbench run --export /data/exports/20261009 \
  --out /app/results/<run-id> --model <name> --memory 5GB --threads 4 --reference
# report, twice: the common target window, then the full period
R="docker run --rm --user 1000:1000 -e HOME=/tmp -v /data/parkfan/ml-bench:/data \
   -v $PWD/ml-bench/results:/app/results ml-bench:<tag> report --run /app/results/<run-id>"
$R --target-from 2026-08-15 --target-to 2026-10-07
$R
```

- `--from/--to` limit the origins, `--leads 0,1,3,7` the slot leads, `--shard i/n` splits
  origins across containers (never `n` a multiple of 7 — see
  [weekday aliasing](harness.md#confounds-every-reader-needs)), `--parks <ids|@file>` restricts
  the run to a park subset (`ml-bench/subsets/par828-30parks.txt`: 12 EU / 10 NA / 8 Asia). The
  park filter applies to the `parks` table, so baselines are re-scored on the same subset; an id
  that matches no park stops the run.
- Every run re-scores the baselines, so a model is always paired against them on the same rows.
  To compare two **models**, put both in one run and name the pairs with
  `--pair-with a,b,…`, then `report --vs a,b,…`.
- `--reference` refuses to start without a git SHA and an image id; every run records both plus
  the measured `code_sha256`.
- `run --probe` runs only the first origin and writes a `probe` block into `run-*.json` (wall
  time, peak allocated / reserved GPU memory, host peak RSS, every plug-in's counters such as
  `slot_seconds`, `daily_seconds`, `finetune_seconds`). It is a sizing tool; its scores are never
  a result.
- The Dockerfile's base is `python:3.11-slim` with torch from the cu128 index (sm_120 for the
  RTX 5080). Extras are layered with `EXTRA_REQUIREMENTS`: `requirements-foundation.txt`
  (Chronos-2, timesfm 3.0.2), `requirements-par853.txt`, `requirements-par1063.txt`,
  `requirements-par1067.txt` (includes par1063); `requirements-neuralforecast.txt` goes through
  `Dockerfile.neuralforecast`. Version holds and conflicts:
  [python-dependencies.md](../../development/python-dependencies.md).

## Sharing the GPU with production

The RTX 5080 (16 GB) also runs nf-service (TFT training 03:00 UTC, Chronos-2 shadow) and
pcn-service, which do not take the bench's lock.

- **`ml-bench/scripts-par828/run.sh <run-id> <run args…>`** is the wrapper for GPU runs. It
  refuses a dirty `ml-bench/` or an image built from another SHA, requires the harness
  coverage-parity fix (`MLBENCH_NEEDS_COMMIT`, default `a1d20bf8`) as an ancestor, takes
  `/data/parkfan/ml-bench/gpu.lock` atomically (`noclobber`), passes `HF_TOKEN` / `TABPFN_TOKEN`
  through from the caller's environment, mounts the HF cache `/data/parkfan/ml-bench/par-828/hf`
  as `/hf`, and marks the run VOID if `run-*.json` records another SHA. Point it at a checkout
  with `MLBENCH_REPO`, `MLBENCH_CODE`, `MLBENCH_IMAGE`.
- **Quiet window 00:55–09:30 UTC**: `run.sh` refuses to start inside it (exit 3; also checked
  while waiting for the lock; `MLBENCH_IGNORE_WINDOW=1` overrides) and
  `scripts-par828/quiet_guard.sh` stops `mlbench-par828-*` containers in it. Only the start is
  guarded — size the run with `--probe` so it ends before 00:55.
- **VRAM caps**: `MLBENCH_FM_VRAM_FRACTION` (foundation models) and `MLBENCH_NF_VRAM_FRACTION`
  (NeuralForecast), both default 0.6 of 16 GB, so a breach of the 10 GB bench budget fails the
  bench process instead of squeezing production. Container RAM is capped at 8 GB.

## NeuralForecast precompute (PAR-829)

TiDE, NHITS and TSMixerx are trained and forecast on the GPU into a parquet cache
(`mlbench/models/nf_precompute.py`, wrapper `run_nf_precompute.sh`, bounded run
`par829_subset.sh`), and the harness then scores the cache on CPU through the ordinary plug-in
path (`NFCached` subclasses `nf_tide`, `nf_nhits`, `nf_tsmixerx`, `nf_tide_wx` in
`mlbench/models/neuralforecast_models.py`; `MLBENCH_NF_CACHE` points at the cache).

**Panel** (`mlbench/models/nf_panel.py`): one series per ride on a service-day hour grid, K = 18
steps per day (hours 08–25 park-local; 99.99 % of truth slots fall in them). The model owns the
level and the hourly shape, H5 the 15-minute shape inside the hour
(`neuralforecast_models.hourly_to_slots`). Closed hours, closed days and missing readings stay on
the grid with `available_mask = 0`, so a week is always 7 × K steps. Known-future covariates:
opening-relative position, weekday, day of year and the ml-service holiday flags — not the
schedule's own `isHoliday` / `isBridgeDay`, which can carry post-origin information. Statics:
headliner flag, region, coordinates, ride and park median daily P90 over the 56 days before the
cutoff. Weather only for `nf_tide_wx` (ORACLE).

**Information cut.** Origins are grouped into month blocks; each block's model is trained once on
the panel before the block's first origin (~9 fits instead of ~230, weights up to a month stale
at the block's end), and every origin is forecast from the `input_size` steps just before it.
`tests/test_nf_models.py::test_precompute_information_cut` poisons every truth slot and schedule
row after an origin and asserts its forecast does not move. Training windows use the final
schedule, forecasts the schedule as known at the origin.

**Sizing — the traps that each cost a failed block:**

- Peak VRAM is set by the training-window pool, not `windows_batch_size`: NeuralForecast's
  `_create_windows` copies `batch_size × ceil(T_train / step_size) × (input_size + h) × n_cols ×
  4 B` before sampling (14 GiB at `step_size = 1` for a September block). `train_step_size()`
  derives the step from `MLBENCH_NF_POOL_BUDGET` (default 1 GiB) and keeps it coprime with K.
  Measured: step 5–17, peak device VRAM 1.7–2.8 GB on all parks. Training-only; forecasts pass
  `step_size = h` explicitly.
- Rides with under a day of measured history (`MIN_TRAIN_STEPS`) are dropped from the fit and kept
  in the forecasts; otherwise a batch raises `No windows available for training`.
- Container RAM: the cached `attraction_id` is a parquet dictionary column (a `<U36` array took
  ~800 MB) and park covariates are scattered park by park (a fancy index materialised 1.26 GB).
  Measured peak 3.2–4.0 GiB of 8. Pass `--memory 2500MB` on all parks; `window_chunk()` sizes the
  predict array against `WINDOW_ARRAY_BUDGET`.
- TSMixerx is multivariate per region (1,177–1,234 rides as channels on all parks): one window
  batch is ~1.1 GB before activations, and it covers daily origins only.
- `MIN_INSAMPLE_HOURS = 12` drops a (ride, origin) with too little input — quantify the D9 share
  it drops next to the error numbers.
- Score all parks over fewer origins rather than a park subset: the 20 busiest parks hold only
  32–40 % of truth slots and would be gated out by `MIN_COVERAGE`.

A full precompute (all parks, 2026-02-19 → 10-07) is 9 month blocks × 4 variants = 36 blocks, on
the order of 30 h of exclusive GPU; the bounded run covers August + September (all 54 days of the
headline target window, 30 origin days per lead from September alone).

```bash
PROBE=1 ml-bench/par829_subset.sh                     # one short block: fit time, VRAM
MODELS="nf_tide" ml-bench/par829_subset.sh            # August + September, all parks
docker run --rm --user 1000:1000 -e HOME=/tmp -e MLBENCH_NF_CACHE=/data/par-829/cache/subset \
  -v /data/parkfan/ml-bench:/data -v $PWD/ml-bench/results:/app/results \
  --cpus 3 --memory 2560m --cpu-shares 256 --entrypoint nice ml-bench:par-829 -n 10 \
  python -m mlbench run --export /data/exports/20261009 --out /app/results/<run-id> \
  --model mlbench.models.neuralforecast_models:NFTiDE \
  --from 2026-08-01 --to 2026-09-30 --shard 0/3 --memory 2.5GB --threads 3
```

`run_nf_precompute.sh` takes the GPU lock, releases it only if it is still its own, and samples
`nvidia-smi` every 15 s (`torch.cuda.max_memory_allocated()` misses the CUDA context and
pcn-service). `--quiet` (default `00:30-09:30` UTC) refuses a block whose **projected finish**
reaches the window (`--block-estimate` before the first block). The cache's `meta.json` records
`git_sha`, `code_sha256`, image id and `earlier_passes`; before reporting from a cache,
`git cat-file -t <git_sha>` must resolve and the coverage-parity fixes must be its ancestors. The
cheap runtime check: score `example_level_h5` alongside and compare its slot count per lead with
the built-in `lvlh5_naive`.

## The foundation-model arms (PAR-1063 / 1064 / 1067)

- `tabpfn_ts` is benchmark-only and needs `HF_TOKEN` and `TABPFN_TOKEN` exported in the shell
  (`read -s`), never written to a file.
- `driver_level` goes in as a `--model`, not only `--pair-with`, when a run reports daily-level
  cells: they compare every `lvl_*` source of the run.
- The fine-tunes (`chronos2_ft`, `chronos2_lora`, `ttm_r2_ft`) refit once per 28 days of origin
  date; a per-origin refit is a subclass with `finetune_every_days = 1`, not the default, because
  it would no longer pair on cadence with the others.
- Every run records `licences` per arm in `run-*.json`; a step that turns a run into an offline
  verdict must call `mlbench.licences.refuse_benchmark_only` on the model names it writes.

Open work: the combined run of these arms next to `chronos2` and `driver_level` has not run; its
commands and expected cost are in PAR-1141.
