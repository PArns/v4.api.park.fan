"""Supervised windowing over the cross-ride tensor (pure numpy, no torch).

Turns the aligned park matrices into nowcast samples: at a base slot `b` the model
sees the context window [b-L+1 .. b] (all rides, all channels) and predicts the next
H slots [b+1 .. b+H] for all rides — exactly how the live nowcast re-infers every
15 min with the current state (mirrors nf-service/backtest_intraday_nowcast.py).

Memory note: a dense [S, R, L, C] array over ALL training bases is multi-GB
(S~thousands × R~100 × L~480 × C). So this module is **index-based and lazy** —
`gather_context`/`gather_targets` fetch ONE sample on demand (the torch Dataset in
the model uses these), and `materialize` densifies only the handful of EVAL bases.
"""

from __future__ import annotations

import numpy as np


def valid_bases(T: int, L: int, H: int) -> np.ndarray:
    """Base slot indices b with a full context [b-L+1..b] and horizon [b+1..b+H]."""
    if T < L + H:
        return np.empty(0, dtype=int)
    return np.arange(L - 1, T - H, dtype=int)


def gather_context(features: np.ndarray, b: int, L: int) -> np.ndarray:
    """[R, L, C] context ending at (and including) base slot b."""
    return features[:, b - L + 1 : b + 1, :]


def gather_targets(
    wait_raw: np.ndarray, target_mask: np.ndarray, b: int, H: int
) -> tuple[np.ndarray, np.ndarray]:
    """([R, H] actual wait, [R, H] eval mask) for slots (b, b+H]."""
    sl = slice(b + 1, b + 1 + H)
    return wait_raw[:, sl], target_mask[:, sl]


def last_observed(wait_raw: np.ndarray, observed: np.ndarray, b: int) -> np.ndarray:
    """[R] last real wait at/before base b per ride; NaN if the ride was never seen
    (the persistence baseline = this value held flat over the horizon)."""
    R = wait_raw.shape[0]
    out = np.full(R, np.nan)
    obs = observed[:, : b + 1] > 0
    vals = wait_raw[:, : b + 1]
    for r in range(R):
        idx = np.flatnonzero(obs[r])
        if idx.size:
            out[r] = vals[r, idx[-1]]
    return out


def yesterday(
    wait_raw: np.ndarray, observed: np.ndarray, b: int, H: int, spd: int
) -> np.ndarray:
    """[R, H] seasonal-naive baseline: the actual wait `spd` slots (one day) before
    each target slot; NaN where that slot is out of range or unobserved."""
    R = wait_raw.shape[0]
    out = np.full((R, H), np.nan)
    for h in range(H):
        src = (b + 1 + h) - spd
        if src < 0:
            continue
        col = np.where(observed[:, src] > 0, wait_raw[:, src], np.nan)
        out[:, h] = col
    return out


def materialize(
    features: np.ndarray,
    wait_raw: np.ndarray,
    observed: np.ndarray,
    target_mask: np.ndarray,
    bases: np.ndarray,
    L: int,
    H: int,
    spd: int,
) -> dict:
    """Densify a SMALL set of bases (eval only — never all training bases) into
    stacked arrays the metrics/baselines consume.

    Returns:
      X        [S, R, L, C]  context features
      actual   [S, R, H]     realised wait (NaN where unobserved)
      mask     [S, R, H]     eval mask (obs AND park-open at the target slot)
      last_obs [S, R]        persistence source (NaN where ride unseen)
      yest     [S, R, H]     yesterday-same-slot baseline (NaN where unavailable)
      base_idx [S]
    """
    X, actual, mask, last_obs, yest = [], [], [], [], []
    for b in bases:
        b = int(b)
        X.append(gather_context(features, b, L))
        a, m = gather_targets(wait_raw, target_mask, b, H)
        actual.append(a)
        mask.append(m)
        last_obs.append(last_observed(wait_raw, observed, b))
        yest.append(yesterday(wait_raw, observed, b, H, spd))
    return {
        "X": np.stack(X) if X else np.empty((0,)),
        "actual": np.stack(actual) if actual else np.empty((0,)),
        "mask": np.stack(mask) if mask else np.empty((0,)),
        "last_obs": np.stack(last_obs) if last_obs else np.empty((0,)),
        "yest": np.stack(yest) if yest else np.empty((0,)),
        "base_idx": np.asarray(bases, dtype=int),
    }
