# ml-bench — the offline forecasting benchmark

`ml-bench/` is the one harness every model comparison goes through (PAR-827): a read-only
production export, the 15-minute truth built once, a rolling-origin backtest of every baseline
and plug-in model under a strict information cut, and a score per use case (UC1–UC4) and per
frontend decision (D1–D9) with bootstrap CIs. Open this folder before you benchmark a model,
read a result under `ml-bench/results/`, or quote an offline verdict.

It is **not deployed**: not in either compose file, no service, no port. It runs by hand on
celestrial in its own container, with CPU, memory and GPU caps. The measurement spec it
implements is BENCH-SPEC (PAR-826); the spec text is not in this repository, its recorded
changes #1–#7 are quoted where they matter.

| Page | What it covers |
| --- | --- |
| [harness.md](harness.md) | Export, truth, protocol and information cut, baselines, decision metrics D1–D9, report outputs and the win / usable-horizon rules |
| [running.md](running.md) | Adding a plug-in model, running and reporting on celestrial, the GPU lock and quiet window, NeuralForecast precompute, queued runs |
| [served-arm.md](served-arm.md) | The `served` arm (PAR-1043): candidates against the curves production actually served, and the offline verdict generator |
| [results-baselines.md](results-baselines.md) | The reference baseline run `20261009-baselines-v2` and what it decided (H5 planner, TFT level only d0–d2) |
| [results-neuralforecast.md](results-neuralforecast.md) | TiDE / NHITS / TSMixerx (PAR-829) and the TiDE-vs-Chronos-2 head to head (PAR-869) |

Other model families have their own pages:
[foundation models](../foundation-models/README.md) (Chronos-2, TimesFM, TiRex-2, Toto 2.0,
Granite, TabPFN-TS) and [level drivers](../level-drivers.md) (`driver_level`, PAR-830).

## Layout

```
ml-bench/
  Dockerfile, Dockerfile.neuralforecast, Dockerfile.headtohead
  requirements*.txt          base + per-workstream extras (EXTRA_REQUIREMENTS build arg)
  mlbench/export.py          read-only export (stdlib only, runs on the DB host)
  mlbench/build.py           raw CSV -> Parquet + 15-min truth + covariates + bench.duckdb
  mlbench/baselines.py       baselines as DuckDB SQL, one origin at a time
  mlbench/runner.py          rolling origins, plug-in calls, per-origin aggregates
  mlbench/decisions.py       planner optimiser simulation (D4)
  mlbench/report.py          bootstrap, horizon curves, hand-over table, summary.md
  mlbench/drivers.py, driver_analysis.py   level-driver table and analysis (PAR-830)
  mlbench/served_verdicts.py offline verdicts for the admin matrix (PAR-1043)
  mlbench/licences.py, benchmark_only_models.json   licence gate (PAR-1067)
  mlbench/models/            plug-in interface (base.py) and every plug-in
  scripts/accumulate-archive-curves.sh             weekly copy of the served curves
  scripts-par828/run.sh, quiet_guard.sh            GPU lock + quiet-window wrapper
  run_nf_precompute.sh, par829_*.py/.sh            NeuralForecast precompute and tables
  tide_vs_chronos.py, splice_results.py, verify_cells_from_parts.py   PAR-869 tables
  tests/                     synthetic export; truth, information cut, optimiser, plug-in parity
  results/<run-id>/          generated outputs (see below)
```

Data lives outside the repo: `/data/parkfan/ml-bench/exports/<id>/` on celestrial (`raw/` CSV,
`parquet/`, `bench.duckdb`, `manifest.json`). A run writes `ml-bench/results/<run-id>/`:
`summary*.md`, `tables*/` and `run-*.json` / `report-*.json` (config, git SHA, measured source
hash, image id) are small and committed for reference runs; `parts/` (per-origin aggregates,
re-scorable with `report`) and `work/` are git-ignored. Everything under `results/` is generated
— never edit it by hand.

## What has been benchmarked, and the verdict

| Family | Run(s) | Verdict | Page |
| --- | --- | --- | --- |
| Baselines, TFT level × H5, production's composed tier | `20261009-baselines-v2` | Plain H5 beats the planner's composed tier; TFT level only d0–d2 on busy rides (`TFT_LEVEL_MAX_LEAD_DAYS = 2`, PAR-901); persistence owns the first 2 h. The H5 composer is behind `PLAN_DAY_H5_COMPOSER` (default off), decision `planner_h5_slots` | [results-baselines.md](results-baselines.md) |
| `driver_level` (PAR-830) | `20261009-par830-drivers` | Shadow on four day-ranking surfaces, serve nothing; running as a production shadow (PAR-1058) | [level-drivers.md](../level-drivers.md) |
| Chronos-2 (PAR-828, PAR-869) | `20261010-par828-subset-chronos-v2`, `20261012-tide-vs-chronos` | Live candidate at 30–120 min and on the planner's d0–d1; runs in the forward archive's shadow, serving prepared behind `SERVE_CHRONOS_INTRADAY` (default off) | [chronos-2.md](../foundation-models/chronos-2.md) |
| TiRex-2, Toto 2.0 (PAR-853) | `20261011-par853-toto-tirex-shard5` | Drop for serving; TiRex-2 kept as a plug-in | [tirex-toto.md](../foundation-models/tirex-toto.md) |
| TiDE, NHITS (PAR-829) | `20261011-par829-stageA`, `20261011-par829-xseason` | Beat the incumbents on live / intraday leads, lose every head to head against Chronos-2; not pursued | [results-neuralforecast.md](results-neuralforecast.md) |
| Chronos-2 LoRA, TimesFM 2.5, PatchTST-FM-r2, TTM-r2, TabPFN-TS | not run yet | Implemented and tested; the combined run is PAR-1141 | [running.md](running.md#the-foundation-model-arms-par-1063--1064--1067) |
| TimesFM 3.0 | never scored | Ruled out: no q95 head, weights licence excludes production | [timesfm.md](../foundation-models/timesfm.md) |

Offline verdicts reach the product through `src/ml/data/offline-model-verdicts.json`, read by
the admin model comparison ([forward-archive/model-comparison.md](../forward-archive/model-comparison.md)).
Since PAR-1038 an offline `better` only counts when it was measured against the served model
(`reference: "served"`), which only the [served arm](served-arm.md) can provide.
