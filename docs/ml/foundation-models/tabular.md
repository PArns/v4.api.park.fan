# Tabular foundation models in ml-bench (PAR-1405)

In-context tabular foundation models (the TabArena family: Kumo-Tabular, Inpriora, TabPFN, …)
as the learner of the daily level. Open this page before adding, running or quoting one, and
before proposing to serve one. Shared licence rules: [README.md](README.md#licences).

## Why only the daily level

[TabArena](https://github.com/autogluon/tabarena), the AutoGluon team's living benchmark, ranks
these models far above tuned gradient-boosted trees on regression. Its evidence does not cover
our data, though:

- **TabArena regression is small and IID.** It has 13 datasets. The largest has ~36k training
  rows (`diamonds`, 53,940 rows of which 35,960 train), and every split is random.
- **BeyondArena** ([arXiv 2606.30410](https://arxiv.org/abs/2606.30410), NeurIPS 2026) adds
  temporal and grouped splits up to 1 M rows. It finds the models competitive on temporal data up
  to ~100k training rows. On large or grouped data, tree-based and deep-learning models still win.
  It scored only older models (TabPFN-2.6, TabICLv2, TabDPT), none of the ones benchmarked here.

Our hourly CatBoost frame is 4.44 M rows, temporal and grouped by ride and park
([CatBoost training](../catboost-training.md)), which is outside that regime. The daily level of
[`driver_level`](../level-drivers.md#6-the-harness-plug-in-driver_level) is inside it: ride-day
rows, at most 300,000 per fit, refit every 28 days.

## TabArena on 2026-11-21

Source: the live leaderboard's data file (`data/pages.sqlite` of the Hugging Face Space
`TabArena/leaderboard`, version v0.1.9.22). The view is models plus open-source systems without
leakage warnings, no imputation, regression over all datasets. A 400-point Elo gap is about a 91 %
pairwise win rate.

| Model | Variant | Elo [95 % CI] | Weights licence |
| --- | --- | --- | --- |
| Kumo-Tabular (NVIDIA) | default | 2475 [2298, 2723] | OpenMDW-1.1 — servable |
| LimiX-2 | default | 2442 | non-commercial |
| Kumo-Tabular-Medium | default | 2329 [2184, 2538] | OpenMDW-1.1 — servable |
| Inpriora (Ant Group) | default | 2258 [2111, 2495] | Apache-2.0 — servable |
| Seldon | default | 2242 | own licence, gated, not read |
| TabPFN-3.5 | default | 2190 | non-commercial, non-production |
| Lattice | default | 2183 | CC-BY-NC-4.0 |
| Mitra-v2 (Amazon) | default | 2023 [1913, 2206] | Apache-2.0 — servable |
| Xiaomi-TabLDM | default | 1816 [1657, 2023] | Apache-2.0 — servable |
| Nori-30M | default | 1802 [1730, 1945] | Apache-2.0 — servable |
| TabICLv2 | default | 1721 [1590, 1931] | BSD-3-Clause — servable |
| TabDPT-1.3 | default | 1699 [1577, 1906] | Apache-2.0 — servable |
| CatBoost | tuned + ensembled | 1518 [1453, 1593] | — |

On the five largest regression datasets, Kumo-Tabular's RMSE is 2–18 % below that of tuned and
ensembled CatBoost. For example, `houses` is 0.180 against 0.211, `physiochemical_protein` 2.77
against 3.38 and `Food_Delivery_Time` 7.20 against 7.37.

## Which models are benchmarked

The rule has two parts:

1. **The licence must allow serving.** A benchmark-only model can never reach production, so its
   score would decide nothing.
2. **Its regression Elo CI must overlap the CI of the best servable model**, Kumo-Tabular
   [2298, 2723]. A model whose upper bound lies below 2298 is already decided worse.

| Model | Verdict |
| --- | --- |
| Kumo-Tabular, `large` | **benchmarked** as `kumo_level` |
| Inpriora | **benchmarked** as `inpriora_level` |
| Kumo-Tabular-Medium / -Small | not run: size variants of the same model; `MLBENCH_KUMO_SIZE` exists for CPU smokes only |
| Mitra-v2 | dropped: upper bound 2206 < 2298, and every fit is an 8-fold bagged fine-tune on a CUDA GPU |
| Xiaomi-TabLDM, Nori-30M, TabICLv2, TabDPT(-1.3), TabPFN-2.6, RealTabPFN-2.5 | dropped: upper bounds 2023 and lower, all below 2298 |
| LimiX-2, TabPFN-3.5, Lattice, TabFM, TabAbax, TabH2O | dropped: the weights' licence excludes commercial or production use |
| Causilo | dropped: its non-commercial purpose excludes "business decision-making" and "production deployment", so even a benchmark run that decides what we serve falls outside it |
| Seldon, EXAONE-Tabular | dropped: own licences, not read; benchmark only after a licence read |
| TabPFN-TS (`tabpfn_ts`, PAR-1067) | retired: runs on the TabPFN-3.5 weights (benchmark-only), and two servable models rank above TabPFN-3.5 |

## Licence verdicts

Read on 2026-11-21.

| Model | Verdict | Basis |
| --- | --- | --- |
| Kumo-Tabular | **servable** | model card `license: openmdw-1.1`, "Kumo Tabular weights are released under OpenMDW 1.1"; NVIDIA's blog: released under OpenMDW-1.1 "for commercial use". Not gated. Code (structured-data-models) Apache-2.0. <https://huggingface.co/nvidia/Kumo-Tabular> |
| Inpriora | **servable** | model card `license: apache-2.0`, "The model weights are released under the Apache License 2.0". Code repository Apache-2.0. <https://huggingface.co/ai-for-data/inpriora> |
| Mitra-v2 | servable (not benchmarked) | model card `license: apache-2.0`. <https://huggingface.co/autogluon/mitra-regressor-2> |
| LimiX-2 | benchmark-only | `license_name: stableai-limix-non-commercial-license-v1.0`. <https://huggingface.co/stable-ai/LimiX-2-1016> |
| Lattice | benchmark-only | weights `cc-by-nc-4.0` (code Apache-2.0). <https://huggingface.co/hachure-ai/Lattice> |
| TabFM (Google) | benchmark-only | `license_name: tabfm-non-commercial-v1.0` (code Apache-2.0). <https://huggingface.co/google/tabfm-1.0.0-pytorch> |
| Causilo | benchmark-only | Causilo License v1.0, §2(b): "commercial or production use of the Model, Derivatives, or Outputs … requires a separate license". <https://huggingface.co/nums-ai/causilo> |
| TabAbax, TabH2O | benchmark-only | `cc-by-nc-4.0`; `h2o-model-non-commercial-licence-v1.0` |
| TabPFN-3.5 | benchmark-only | see [granite-tabpfn.md](granite-tabpfn.md#licence-verdicts) |

## The arms

`ml-bench/mlbench/models/tabular_fm.py`, tests in `ml-bench/tests/test_tabular_fm.py`.

Each arm is `driver_level` with one change, the learner of `log(true P90 / naive)`. It keeps the
same training rows (ride-day × lead 1–90), the same features, the same naive reference and the same
H5 composition. A margin against `driver_level` is therefore the learner alone. A test swaps
`driver_level`'s own learner back in and asserts identical levels.

- **Context:** a seeded sample of the training rows, at most `MLBENCH_TFM_CONTEXT_ROWS` (default
  50,000). Kumo-Tabular was pretrained on contexts of up to 60,000 rows and 100 columns. Features
  that never vary in the rows are left out, as in `fit_model`.
- **`kumo_level`:** Kumo-Tabular `large` (215.7 M parameters) through
  `structured-data-models==0.2.1`, which pins the weights to revision `v1.0.1`. It uses 4
  estimators (`MLBENCH_KUMO_ESTIMATORS`) and fp16 autocast on CUDA. Regression returns 999 quantiles
  `q001` … `q999`, and the level uses `q500`: the median, like `driver_level`'s absolute-error
  loss. Categoricals (`dow`, `region`) go in as strings, because sdm encodes a category by its
  value.
- **`inpriora_level`:** Inpriora's regressor (121 M) with its default 8 estimators
  (`MLBENCH_INPRIORA_ESTIMATORS`) and fp16 autocast on CUDA. The weights are pinned to Hugging Face
  revision `8c41fb1a` (`MLBENCH_INPRIORA_REVISION`), and the code to the reviewed commit `dde547d0`.
  It gets the frame as `design` builds it, so it encodes the category columns itself. The point
  forecast is the regressor's own `predict`.
- **Query chunks:** `MLBENCH_TFM_QUERY_CHUNK` (20,000 rows per forward pass). VRAM is capped by
  `MLBENCH_FM_VRAM_FRACTION`, like the other foundation arms.
- **Per-arm counters** (`stats`, in a `--probe` run's json): fits, fit seconds, context rows,
  predicted rows, predict seconds.
- **Licence gate:** both arms declare `licence = "servable"` and `serving_candidate = True`.
  `TabularFMLevel` refuses the same mislabels as `FoundationModel`.
- **Stub:** `MLBENCH_TFM_BACKEND=stub` swaps the learner for a deterministic k-nearest-neighbour
  stand-in. `tests/conftest.py` sets it, so the information-cut test covers both arms without
  weights.

## Status

- **Implemented and tested.** The CPU wiring smoke ran both arms with their real weights on the
  synthetic export, over three origins: 904 context rows and 360 scored levels each. Kumo-Tabular
  (`small`, 2 estimators) took 2.5 s to fit and 0.9 s to predict; Inpriora (8 estimators,
  `INPRIORA_DISABLE_TRITON=1` on CPU) took 10.6 s and 3.2 s. `test_kumo_backend_real_weights` and
  `test_inpriora_backend_real_weights` run the backends against the real weights when
  `MLBENCH_FM_REAL=1`. The synthetic export says nothing about accuracy.
- **Inpriora's code comes from GitHub.** It has no PyPI release, so `requirements-par1405.txt` pins
  the commit that was reviewed on 2026-11-21, with the findings. The repository's first commit is
  from 2026-11-13, so no pin of it can be two weeks old.
- **Not scored on the production export yet.** The run belongs in the combined run (PAR-1141),
  next to `driver_level` (pass `driver_level` as a `--model`, not only `--pair-with`, see
  [running.md](../ml-bench/running.md#the-foundation-model-arms-par-1063--1064--1067)). Build the
  image with `--build-arg EXTRA_REQUIREMENTS=requirements-par1405.txt` and size it with `--probe`
  first. On TabArena's GPU Kumo-Tabular predicted 0.24 s and Inpriora 0.06 s per 1,000 rows.
- **In the admin:** the model comparison has an offline-only column „Kumo-Tabular / Inpriora“
  (`tfm`) on the four calendar questions. It reads „not measured“ until the run's verdicts are in
  `src/ml/data/offline-model-verdicts.json`; the arms run in no production shadow.
- **How it is judged:** against `driver_level` (paired, same rows) and `lvl_tft` on UC4 / D6 / D7,
  per segment and lead, with the rules on [model-overview](../model-overview.md#how-a-model-change-is-judged).
  A win on overall MAE alone does not count.
