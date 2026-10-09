"""Cluster bootstrap over park-days or parks (BENCH-SPEC: "Bootstrap 95 % CIs over park-days").

Every metric here is a ratio of sums over units (MAE = Σ|e| / n, a hit rate =
Σhits / Σtrials, …). The bootstrap resamples units with Poisson(1) weights — the
standard large-sample equivalent of resampling with replacement.

``Bootstrapper`` gives every unit ONE fixed column of weights for the whole
report: a park-day is resampled identically in every metric, both sides of a
paired difference share the draw, the result does not depend on row order, and
the weights are generated once instead of per call.
"""

from __future__ import annotations

import numpy as np


class Bootstrapper:
    def __init__(self, reps: int = 1000, seed: int = 827):
        self.reps = reps
        self.rng = np.random.default_rng(seed)
        self.index: dict[str, int] = {}
        self.W = np.zeros((reps, 0), dtype=np.float32)

    def _idx(self, keys) -> np.ndarray:
        keys = [str(k) for k in keys]
        new = [k for k in dict.fromkeys(keys) if k not in self.index]
        if new:
            # assign in sorted order so the draw a unit gets does not depend on which
            # metric happened to meet it first
            start = len(self.index)
            for i, k in enumerate(sorted(new)):
                self.index[k] = start + i
            add = self.rng.poisson(1.0, size=(self.reps, len(new))).astype(np.float32)
            self.W = np.concatenate([self.W, add], axis=1)
        return np.fromiter((self.index[k] for k in keys), dtype=np.int64, count=len(keys))

    def _sums(self, idx: np.ndarray, x: np.ndarray) -> np.ndarray:
        full = np.zeros(self.W.shape[1], dtype=np.float32)
        np.add.at(full, idx, np.nan_to_num(np.asarray(x, dtype=np.float64)).astype(np.float32))
        return self.W @ full

    def ratio(self, keys, num, den, alpha: float = 0.05) -> tuple[float, float, float]:
        num = np.nan_to_num(np.asarray(num, dtype=np.float64))
        den = np.nan_to_num(np.asarray(den, dtype=np.float64))
        if den.sum() <= 0:
            return (np.nan, np.nan, np.nan)
        point = num.sum() / den.sum()
        idx = self._idx(keys)
        with np.errstate(invalid="ignore", divide="ignore"):
            v = self._sums(idx, num) / self._sums(idx, den)
        return _ci(point, v, alpha)

    def diff(self, keys, nm, dm, nr, dr, alpha: float = 0.05) -> tuple[float, float, float]:
        nm, dm, nr, dr = (np.nan_to_num(np.asarray(a, dtype=np.float64)) for a in (nm, dm, nr, dr))
        if dm.sum() <= 0 or dr.sum() <= 0:
            return (np.nan, np.nan, np.nan)
        point = nm.sum() / dm.sum() - nr.sum() / dr.sum()
        idx = self._idx(keys)
        with np.errstate(invalid="ignore", divide="ignore"):
            v = self._sums(idx, nm) / self._sums(idx, dm) - self._sums(idx, nr) / self._sums(idx, dr)
        return _ci(point, v, alpha)


def _ci(point: float, v: np.ndarray, alpha: float) -> tuple[float, float, float]:
    v = v[np.isfinite(v)]
    if v.size == 0:
        return (float(point), np.nan, np.nan)
    return (float(point), float(np.quantile(v, alpha / 2)), float(np.quantile(v, 1 - alpha / 2)))


def ratio_ci(num, den, reps: int = 1000, seed: int = 827, alpha: float = 0.05) -> tuple[float, float, float]:
    """Point estimate and percentile CI of Σnum / Σden; units = positions."""
    return Bootstrapper(reps, seed).ratio(range(len(np.asarray(num))), num, den, alpha)


def paired_diff_ci(num_m, den_m, num_r, den_r, reps: int = 1000, seed: int = 827,
                   alpha: float = 0.05) -> tuple[float, float, float]:
    """CI of (Σnum_m/Σden_m) − (Σnum_r/Σden_r), both sides on the same resampled units."""
    return Bootstrapper(reps, seed).diff(range(len(np.asarray(num_m))), num_m, den_m, num_r, den_r, alpha)


def significant(lo: float, hi: float, lower_is_better: bool = True) -> bool:
    """True when the paired-difference CI excludes 0 in the model's favour."""
    if not (np.isfinite(lo) and np.isfinite(hi)):
        return False
    return hi < 0 if lower_is_better else lo > 0
