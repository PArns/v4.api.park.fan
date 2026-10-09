"""Cluster bootstrap over park-days (BENCH-SPEC: "Bootstrap 95 % CIs over park-days").

Every metric here is a ratio of sums over park-days (MAE = Σ|e| / n, a hit rate =
Σhits / Σtrials, …). The bootstrap resamples park-days with Poisson(1) weights —
the standard large-sample equivalent of resampling with replacement, and it lets
all metrics and both sides of a paired difference share the same draw.
"""

from __future__ import annotations

from functools import lru_cache

import numpy as np


@lru_cache(maxsize=4)
def poisson_weights(n_units: int, reps: int, seed: int) -> np.ndarray:
    """Cached: every metric over the same set of units reuses the same draws (cheaper,
    and the CIs of models scored on the same park-days come from one resampling)."""
    rng = np.random.default_rng(seed)
    w = rng.poisson(1.0, size=(reps, n_units)).astype(np.float32)
    w.setflags(write=False)
    return w


def ratio_ci(num: np.ndarray, den: np.ndarray, reps: int = 1000, seed: int = 827,
             alpha: float = 0.05) -> tuple[float, float, float]:
    """Point estimate and percentile CI of Σnum / Σden over resampled units."""
    num = np.nan_to_num(np.asarray(num, dtype=np.float64))
    den = np.nan_to_num(np.asarray(den, dtype=np.float64))
    if den.sum() <= 0:
        return (np.nan, np.nan, np.nan)
    point = num.sum() / den.sum()
    w = poisson_weights(len(num), reps, seed)
    with np.errstate(invalid="ignore", divide="ignore"):
        v = (w @ num.astype(np.float32)) / (w @ den.astype(np.float32))
    v = v[np.isfinite(v)]
    if v.size == 0:
        return (point, np.nan, np.nan)
    return (float(point), float(np.quantile(v, alpha / 2)), float(np.quantile(v, 1 - alpha / 2)))


def paired_diff_ci(num_m: np.ndarray, den_m: np.ndarray, num_r: np.ndarray, den_r: np.ndarray,
                   reps: int = 1000, seed: int = 827, alpha: float = 0.05) -> tuple[float, float, float]:
    """CI of (Σnum_m/Σden_m) − (Σnum_r/Σden_r), both sides on the same resampled units."""
    arrs = [np.nan_to_num(np.asarray(a, dtype=np.float64)) for a in (num_m, den_m, num_r, den_r)]
    nm, dm, nr, dr = arrs
    if dm.sum() <= 0 or dr.sum() <= 0:
        return (np.nan, np.nan, np.nan)
    point = nm.sum() / dm.sum() - nr.sum() / dr.sum()
    w = poisson_weights(len(nm), reps, seed)
    f = np.float32
    with np.errstate(invalid="ignore", divide="ignore"):
        v = (w @ nm.astype(f)) / (w @ dm.astype(f)) - (w @ nr.astype(f)) / (w @ dr.astype(f))
    v = v[np.isfinite(v)]
    if v.size == 0:
        return (point, np.nan, np.nan)
    return (float(point), float(np.quantile(v, alpha / 2)), float(np.quantile(v, 1 - alpha / 2)))


def significant(lo: float, hi: float, lower_is_better: bool = True) -> bool:
    """True when the paired-difference CI excludes 0 in the model's favour."""
    if not (np.isfinite(lo) and np.isfinite(hi)):
        return False
    return hi < 0 if lower_is_better else lo > 0
