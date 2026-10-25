# Python dependencies: how versions are chosen

Applies to `ml-service`, `nf-service`, `pcn-service` and `ml-bench`. Last full upgrade:
PAR-1066 (2026-10-24).

## The rules

- **Exact pins (`==`)** in every `requirements*.txt`, and the torch pin in each Dockerfile.
  A rebuild must never pick up a release nobody has tested, because the nightly retrain
  runs on whatever the image holds.
- **Newest release that is at least two weeks old.** A release younger than that is
  skipped, even if it is the newest (on 2026-10-24: fastapi 0.162, pandas 3.1,
  sqlalchemy 2.1.6, python-dateutil 3.0, duckdb 2.0, granite-tsfm 0.3.11, tabpfn 9.2).
- **One numpy and one pandas in every image** (numpy 2.4.6, pandas 2.3.3), so a pickle or a
  Parquet file written by one service reads the same in another, and an ml-bench result
  transfers to the service it benchmarks.
- **Every hold gets a comment** next to the pin saying why. When the reason goes away, the
  hold goes too.

## The holds as of PAR-1066

| Pin | Why | Lifted when |
|---|---|---|
| numpy 2.4.x | 2.5 needs Python >= 3.12; nf/pcn/ml-bench run 3.11 | those images move to 3.12+ |
| pandas 2.3.3 (last 2.x) | 3.x turns on copy-on-write and the string dtype, which changes the feature code's behaviour; gluonts (PAR-853 extra) needs < 3 | a separate migration with the train/serve parity tests |
| sqlalchemy 2.0.x | pandas 2.3.3 predates SQLAlchemy 2.1 | pandas moves |
| torch 2.11.0 (cu128) | the newest cu128 build; 2.12+ ship as cu130 only, and the RTX 5080 host's driver has to be checked for CUDA 13 first. The RTX 5080 (sm_120) needs cu128 or newer | the host driver is confirmed for cu130 |
| catboost 1.2.10 | already the newest; stays CPU (GPU training collapses the high-cardinality categoricals) | — |
| jax 0.10.2 (ml-bench PAR-1063 extra) | the newest jax that runs on Python 3.11 | ml-bench moves to 3.12 |
| scikit-learn 1.7.2, pyarrow 25.0.1, datasets 4.8.5 (ml-bench PAR-1067 extra only, `requirements-par1067.txt`) | granite-tsfm needs sklearn < 1.8; tabpfn-client caps pyarrow <= 25.0.1; fev (via tabpfn-time-series) needs datasets < 5 | those packages lift their caps |

## Stored artefacts across an upgrade

Old artefacts must load in new code (deploy), and new artefacts must load in old code
(rollback). The artefacts are:

- **ml-service**: CatBoost `.cbm` + a joblib metadata pickle holding numpy scalars and
  arrays and pandas objects.
- **pcn-service**: torch checkpoints (`torch.save` of a dict with the state dict, numpy
  scales and ride ids; loaded with `weights_only=False`, so the torch 2.6 default change
  does not apply) and tensor caches (`.npz`, `allow_pickle=True`).
- **nf-service**: no TFT checkpoint — it fits and predicts per chunk (`forecast.py`,
  "No nf.save()"). The Chronos truth cache is Parquet + JSON.

PAR-1066 result (numpy 1.26.4 ↔ 2.4.6, pandas 2.2.3 ↔ 2.3.3): both directions load and
predict **bit-identically** (max abs diff 0.0) for the CatBoost model + metadata and for a
GP-STGNN checkpoint + tensor cache. numpy 1.26 reads numpy-2 pickles (it ships the
`numpy._core` alias), so a rollback does not strand a model retrained after the deploy.
The TFT call pattern gives the same columns and the same forecast on neuralforecast 3.1.8
and 3.3.0.

## Re-running the check

The scripts are in `scripts/python-deps/`. Make one venv with the old pins and one with
the new (`uv venv -p 3.13` for ml-service, `-p 3.11` for the others; for nf/pcn install
`torch==<pin>` first), then:

```bash
# ml-service: CatBoost + joblib metadata
old/bin/python scripts/python-deps/artefact_compat.py write /tmp/c-old
new/bin/python scripts/python-deps/artefact_compat.py read  /tmp/c-old   # deploy
new/bin/python scripts/python-deps/artefact_compat.py write /tmp/c-new
old/bin/python scripts/python-deps/artefact_compat.py read  /tmp/c-new   # rollback

# pcn-service: run from pcn-service/
(cd pcn-service && old/bin/python ../scripts/python-deps/pcn_compat.py write /tmp/p-old \
  && new/bin/python ../scripts/python-deps/pcn_compat.py read /tmp/p-old)

# nf-service: same TFT call pattern in both venvs, compare the printed output
old/bin/python scripts/python-deps/tft_smoke.py; new/bin/python scripts/python-deps/tft_smoke.py
```

Then each service's suite (`python -m pytest -q`; nf-service needs
`MODEL_DIR=<writable dir>`, since `/app/models` only exists in the image), one service at a
time. That every ml-bench extra resolves against the base pins can be checked without
installing anything:

```bash
uv pip compile --python-version 3.11 --python-platform x86_64-manylinux_2_28 \
  ml-bench/requirements.txt ml-bench/requirements-par1063.txt torch.txt
```

(`torch.txt` holds the Dockerfile's torch pin. The PAR-1067 extra (`requirements-par1067.txt`) deliberately downgrades
scikit-learn and pyarrow, so compile it against the base pins without those two.)

## Lint

ruff 0.17 widened its default rule set. `pcn-service` and `ml-bench` select their rules in
`ruff.toml` and stay clean; `ml-service` and `nf-service` have no config, so a bare
`ruff check` there reports hundreds of style findings that ruff 0.14 did not.
