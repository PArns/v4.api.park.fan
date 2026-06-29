# PCN Service — Park-Crowd Nowcaster (Phase 0)

The third modelling track from
[`docs/ml/custom-intraday-model-design.md`](../docs/ml/custom-intraday-model-design.md):
an intraday wait-time model that explicitly learns the **shared park-wide crowd state**
across all rides of a park (the cross-series signal CatBoost and TFT both miss).

This directory currently contains **Phase 0 only**: the data foundation that every later
step (the STG4Traffic bake-off and the GP-STGNN itself) needs — the **cross-ride tensor**.

## What the cross-ride tensor is

For one park it assembles all rides onto a common park-local 15-min grid and produces
aligned matrices `[R rides × T slots]`:

| array | shape | meaning |
|---|---|---|
| `wait_raw` | R×T | median STANDBY wait, `NaN` where unobserved |
| `wait_ffill` | R×T | per-ride forward-filled level (model context input) |
| `obs_mask` | R×T | 1 where a real operating wait existed (the loss target mask) |
| `down` | R×T | DOWN-status count per slot (pent-up-demand / reopening signal) |
| `park_open` | T | 1 where the park is open (≥`MIN_RIDES_OPEN` rides reporting) |
| `park_occ` | T | mean wait over **observed** rides = the *scalar* crowd baseline |
| `features` | R×T×C | the stacked node-feature tensor (channels in `tensor.CHANNELS`) |

The key idea (design doc §11.6): nf-service's intraday backtest collapses the park into
the **scalar** `park_occ`. Phase 0 keeps the **whole `[R × T]` matrix** so an
adaptive-graph STGNN (AGCRN / Graph WaveNet) can *learn* the ride×ride coupling instead of
being handed a hand-rolled average. `loss_mask = obs_mask × park_open` keeps closed slots
and sensor gaps out of the loss (no spurious-zero training — the flaw that broke the
2026-05-23 hourly PoC).

## Files

| file | role |
|---|---|
| `config.py` | DB settings (shared `DB_*` env) + tensor scope/knobs |
| `db.py` | park-local 15-min panel fetch (`date_bin`), median wait + `n_obs` + `down_count` |
| `tensor.py` | **pure pandas/numpy** tensor assembly (no DB → unit-testable) |
| `build_cross_ride_tensor.py` | CLI: build + print shape/density/occupancy per park; `--save` to `.npz` |
| `test_tensor.py` | synthetic-panel unit tests (grid, masks, `park_occ`, ffill) |

## Run

```bash
# unit tests (no DB)
cd pcn-service && python3 -m pytest test_tensor.py -q

# against the live DB (inside the image / with DB_* env set)
python3 build_cross_ride_tensor.py --park <PARK_UUID>          # inspect one park
python3 build_cross_ride_tensor.py --limit 5 --save ./models   # build + save a few
```

## Configuration (env, all optional)

| var | default | meaning |
|---|---|---|
| `DB_HOST/PORT/USER/PASSWORD/NAME` | localhost/5432/parkfan/…/parkfan | Postgres (same as ml/nf) |
| `PCN_PARK_IDS` | "" (all) | comma-separated park UUIDs to build |
| `PCN_WINDOW_DAYS` | 548 | history window (caps at what exists; auto-grows) |
| `PCN_SLOT_MINUTES` | 15 | slot grid (matches CatBoost serving) |
| `PCN_MIN_WAIT` | 5 | min STANDBY wait counted as a real observation |
| `PCN_MIN_RIDES_OPEN` | 3 | rides reporting to call a slot "park open" |

## Next (not in this phase)

1. **STG4Traffic bake-off** — feed this tensor to AGCRN / Graph WaveNet / MTGNN / DeepGLO +
   a Chronos-Bolt zero-shot baseline, scored vs CatBoost-intraday + naive baselines.
2. **GP-STGNN v1** — winning backbone + a peak-aware probabilistic head (SPADE / Tweedie).
3. Shadow-serving behind the existing busy/headliner gate.
