# shape-service — Tageskurven-Expansion (Level × Shape)

The **Shape** half of the two-model architecture
([design doc](../docs/ml/shape-model-design.md), and §10 of
[custom-intraday-model-design.md](../docs/ml/custom-intraday-model-design.md)).

It learns the **normalised daily form** of each ride — `Profil(ride, dow, slot)` from the
PCN decomposition — and renders any predicted **daily level** into a servable per-slot
wait curve:

```
wait(ride, day, slot) ≈ level(ride, day) × shape(ride, slot | crowd(level), dow)
```

This is **shared infrastructure**: it expands *any* day-level forecast (the long-term LCM
= TFT-Daily, **or** the daily-crowd forecast) into a 15-min curve, replacing what CatBoost
patches today for day 61–365 + intraday shape. It is small, data-sparse, and **not**
data-gated (the *form* needs operating days, not a full seasonal year).

## Phase 0 (this) — data foundation

Pure, DB-verified profile assembly. No serving / persistence yet (that's Phase 1).

| File | Role |
|---|---|
| `config.py` | settings (DB_*, slot grid, day-qualify + bucket + fallback knobs) |
| `db.py` | park-local per-(ride, day, slot-of-day) median-wait panel (same conventions as pcn/nf) |
| `profiles.py` | **PURE** core: normalise by daily peak, crowd/DOW buckets, fallback hierarchy, render |
| `pipeline.py` | DB → profiles glue |
| `build_profiles.py` | CLI: build + coverage summary (`--sample` renders an example curve) |
| `test_profiles.py` | unit tests for the pure logic |

```bash
cd shape-service && python3 -m pytest -q          # pure-logic tests (no DB)
python3 build_profiles.py --sample <PARK_UUID>    # build + render a sample curve
```

### Method

1. Park-local 15-min panel from `queue_data` (STANDBY, OPERATING, `waitTime ≥ 5`).
2. Per operating day: **level** = daily peak; keep days with a real peak + enough slots.
3. **Normalise** each day's curve by its level → level-free form.
4. Average the normalised curves per `(ride, crowd-bucket, dow-bucket, slot)`; crowd buckets
   are terciles of each ride's own daily-peak distribution (self-calibrating).
5. **Fallback hierarchy** for sparse cells:
   `(ride,crowd,dow) → (ride,crowd) → (ride,dow) → (ride) → (park)`.
6. **Render:** `level × shape`, crowd bucket derived from the predicted level.

## Next phases

- **Phase 1:** persist (`shape_profiles`), render API, serving-merge, shape-backtest vs CatBoost.
- **Phase 2:** learned-shape challenger + bake-off.
- **Phase 3:** Blend/handoff PCN ⨯ (Level×Shape).
