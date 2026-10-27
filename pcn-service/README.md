# pcn-service

The Park-Crowd Nowcaster: a FastAPI service that trains one graph model per park every night and forecasts
every ride's next 12 hours in 15-minute slots. This page maps the directory and how to run it; the model,
its targets, the `pcn_forecasts` table, the shadow boards and the serving switch are in
[docs/ml/pcn.md](../docs/ml/pcn.md).

## Endpoints and schedule

| Endpoint                    | Called by (queue `pcn-shadow` in the API)        | Does                                                                         |
| --------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------- |
| `POST /train`               | `train-pcn`, daily 08:30 UTC                     | Trains every park in a subprocess (`train_runner.py`), saves `/app/models/pcn_<park>.pt` |
| `POST /forecast`            | every 15 minutes                                 | Re-infers each park and writes `pcn_forecasts` (`forecast.py`)               |
| `POST /score`               | hourly                                           | Scores matured forecasts against actuals and CatBoost into `pcn_intraday_comparisons` (`score.py`) |
| `GET /health`, `/gpu`, `/status`, `/train/status` | the API's health checks, operators | Liveness, GPU memory, the train/forecast/score job state                     |

The API serves PCN's forecast only with `SERVE_PCN_INTRADAY=true`; otherwise it runs in the shadow.

## Files

| File                                            | Role                                                                                     |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `config.py`                                     | `DB_*` connection and every `PCN_*` setting, each with its reason                       |
| `db.py`                                         | Park-local 15-minute panel (`date_bin`), median wait, `n_obs`, DOWN count; owns the `pcn_forecasts` DDL |
| `tensor.py`                                     | Pure pandas/numpy assembly of the `[rides × slots]` tensor (unit-testable without a DB) |
| `pipeline.py`                                   | The shared DB → tensor step for training, forecasting and the bake-off                  |
| `windowing.py`, `metrics.py`                    | Supervised windows and baselines; masked MAE and bias by busy segment and lead          |
| `torch_model.py`, `backbones.py`, `gp_stgnn.py` | The model base class, Graph WaveNet / local GRU backbones, the GP-STGNN                 |
| `backtest.py`, `run_bakeoff.py`                 | Offline rolling-origin bake-off against persistence and yesterday-same-slot             |
| `build_cross_ride_tensor.py`                    | CLI: build and inspect a park's tensor, `--save` to `.npz`                              |
| `train.py`, `train_runner.py`, `forecast.py`, `score.py` | The nightly train, the forecast tick, the scorer                               |
| `uuid_params.py`                                | Binds uuid ids as `CAST(:ids AS uuid[])`, never casting the indexed column (PAR-871)   |
| `main.py`                                       | The FastAPI app                                                                          |
| `test_*.py`                                     | Unit tests; the torch-dependent ones skip without torch                                 |

## Run

```bash
cd pcn-service && python3 -m pytest -q                                  # tests, no DB needed
python3 build_cross_ride_tensor.py --park <PARK_UUID>                  # needs DB_* set
python3 build_cross_ride_tensor.py --limit 5 --save ./models
python3 backtest.py --tensor models/crt_<park>.npz --input-size 192 --horizon 48   # persistence sanity run
```

Python 3.11 with the cu128 torch build from the Dockerfile (Blackwell, sm_120); pins and their reasons:
[Python dependencies](../docs/development/python-dependencies.md). Lint: `ruff check .` (rules in
`ruff.toml`).
