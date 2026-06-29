"""Intraday bake-off driver — the offline instrument (mirrors the discipline of
nf-service/backtest_intraday_nowcast.py, generalized to ANY model + the cross-ride
tensor).

Rolling-origin, leakage-free: train ONCE on bases strictly before the eval window,
then for each eval base predict forward using only the context up to that base ("train
nightly, predict live"). Every candidate is scored on the matched (ride, 15-min slot)
population against the naive baselines it must beat (persistence, yesterday-same-slot),
by busy segment and lead bucket.

Any model conforming to the `Model` protocol plugs in — the GP-STGNN (gp_stgnn.py) and
STG4Traffic backbones (AGCRN / Graph WaveNet / MTGNN / DeepGLO) alike. `PersistenceModel`
makes the harness runnable end-to-end without torch (it ties the persistence baseline,
proving the wiring).

    python3 backtest.py --tensor models/crt_<park>.npz        # persistence sanity run
"""

from __future__ import annotations

import argparse
from typing import Protocol

import numpy as np
import pandas as pd

import metrics
import windowing


class Model(Protocol):
    """A bake-off candidate. `fit` trains on the pre-eval history; `predict` returns
    [S, R, H] point forecasts for the given eval bases (the median/expected wait —
    a probabilistic model still returns its central forecast here, with quantiles
    served separately downstream)."""

    name: str

    def fit(self, t, train_bases: np.ndarray, L: int, H: int) -> None: ...

    def predict(self, t, eval_bases: np.ndarray, L: int, H: int) -> np.ndarray: ...


class PersistenceModel:
    """Reference candidate: hold the last observed wait flat over the horizon. Needs no
    training; exists so the harness runs end-to-end without torch."""

    name = "persist_model"

    def fit(self, t, train_bases, L, H):  # noqa: D401 - nothing to learn
        return None

    def predict(self, t, eval_bases, L, H):
        wait_raw, observed, _ = _arrays(t)
        last = np.stack([
            windowing.last_observed(wait_raw, observed, int(b)) for b in eval_bases
        ])  # [S, R]
        return np.repeat(last[:, :, None], H, axis=2)  # [S, R, H]


def _arrays(t) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(wait_raw, observed=obs_mask, target_mask=obs_mask*park_open) from a tensor."""
    wait_raw = np.asarray(t.wait_raw, dtype=float)
    observed = np.asarray(t.obs_mask, dtype=float)
    target_mask = observed * np.asarray(t.park_open, dtype=float)[None, :]
    return wait_raw, observed, target_mask


def _slots_index(t) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(np.asarray(t.slots)))


def grid_cadence(t) -> tuple[int, int]:
    """(slot_minutes, slots_per_day) inferred from the tensor's regular grid."""
    sl = _slots_index(t)
    step = sl[1] - sl[0]
    slot_minutes = int(step.total_seconds() // 60)
    spd = int(round(86400 / step.total_seconds()))
    return slot_minutes, spd


def rolling_origin_split(
    t, L: int, H: int, eval_days: int = 5, base_hour: int = 11
) -> tuple[np.ndarray, np.ndarray]:
    """(train_bases, eval_bases). Eval bases are the last `eval_days` anchors at
    `base_hour` local that have a full horizon ahead; train bases are every valid base
    strictly before the earliest eval base (no leakage)."""
    sl = _slots_index(t)
    T = len(sl)
    valid = set(windowing.valid_bases(T, L, H).tolist())
    anchors = [
        i for i in range(T)
        if sl[i].hour == base_hour and sl[i].minute == 0 and i in valid
    ]
    eval_bases = np.array(anchors[-eval_days:], dtype=int)
    if eval_bases.size == 0:
        # Fallback: no clean local-hour anchor (short/odd grid) → use the last
        # `eval_days` valid bases so the harness still produces a comparison.
        vb = windowing.valid_bases(T, L, H)
        eval_bases = vb[-eval_days:]
    cutoff = int(eval_bases.min()) if eval_bases.size else T
    train_bases = windowing.valid_bases(T, L, H)
    train_bases = train_bases[train_bases < cutoff]
    return train_bases, eval_bases


def run_backtest(
    t, model: Model, L: int = 480, H: int = 48, eval_days: int = 5, base_hour: int = 11
) -> dict:
    """Train `model`, predict the eval bases, and score it vs persistence + yesterday
    by busy segment and lead bucket. Returns the score dict + run stats."""
    slot_minutes, spd = grid_cadence(t)
    train_bases, eval_bases = rolling_origin_split(t, L, H, eval_days, base_hour)
    if eval_bases.size == 0:
        raise RuntimeError("no eval bases — tensor too short for the given L/H")

    model.fit(t, train_bases, L, H)
    pred = model.predict(t, eval_bases, L, H)  # [S, R, H]

    features = np.asarray(t.features, dtype=float)
    wait_raw, observed, target_mask = _arrays(t)
    ev = windowing.materialize(
        features, wait_raw, observed, target_mask, eval_bases, L, H, spd
    )
    persist = np.repeat(ev["last_obs"][:, :, None], H, axis=2)
    preds = {model.name: pred, "persist": persist, "yest": ev["yest"]}
    scores = metrics.evaluate(preds, ev["actual"], ev["mask"], slot_minutes)
    return {
        "scores": scores,
        "n_train_bases": int(train_bases.size),
        "n_eval_bases": int(eval_bases.size),
        "L": L, "H": H, "slot_minutes": slot_minutes,
        "table": metrics.format_table(scores),
    }


def main():
    import tensor as tns

    ap = argparse.ArgumentParser(description="PCN intraday bake-off (offline)")
    ap.add_argument("--tensor", required=True, help="path to a crt_<park>.npz")
    ap.add_argument("--input-size", type=int, default=480, help="context slots (L)")
    ap.add_argument("--horizon", type=int, default=48, help="forecast slots (H)")
    ap.add_argument("--eval-days", type=int, default=5)
    ap.add_argument("--base-hour", type=int, default=11)
    args = ap.parse_args()

    t = tns.CrossRideTensor.from_npz(args.tensor)
    res = run_backtest(
        t, PersistenceModel(), L=args.input_size, H=args.horizon,
        eval_days=args.eval_days, base_hour=args.base_hour,
    )
    print(f"park={t.park_id} rides={len(t.ride_ids)} slots={len(t.slots)} "
          f"train_bases={res['n_train_bases']} eval_bases={res['n_eval_bases']}")
    print(res["table"])


if __name__ == "__main__":
    main()
