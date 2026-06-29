"""Pure shape-profile assembly + render (no DB — unit-testable on synthetic panels).

From a per-(ride, day, slot) median-wait panel, build the normalised daily FORM per
(ride, crowd-bucket, dow-bucket, slot) with a graceful fallback hierarchy, and render a
predicted daily LEVEL into a servable per-slot wait curve:

    wait_curve(ride, day, slot) = level × shape(ride, slot | crowd(level), dow)

See docs/ml/shape-model-design.md. This module is deliberately framework-light (pandas +
numpy) so it carries no DB / torch dependency and runs in CI.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# A curve is a float array of length slots_per_day; NaN at a slot = "ride not typically
# operating in that slot for this condition" (no zero-fill — closed slots stay absent).
Curve = np.ndarray
Cell = tuple[Curve, int]  # (normalised curve, n_days that informed it)


def dow_bucket(dow: int, mode: str) -> str:
    """0=Mon … 6=Sun (pandas convention). 'wend' = weekend/weekday; 'full' = per-DOW."""
    if mode == "full":
        return f"d{dow}"
    return "wend" if dow >= 5 else "week"


def crowd_thresholds(levels: np.ndarray, n_buckets: int) -> np.ndarray:
    """Inner quantile cut points of a ride's daily-peak distribution → (n_buckets-1) edges.
    Empty if there are too few distinct levels to bucket (→ everything is bucket 0)."""
    u = np.unique(levels)
    if u.size < n_buckets or n_buckets < 2:
        return np.array([], dtype=float)
    qs = [i / n_buckets for i in range(1, n_buckets)]
    return np.quantile(levels, qs)


def crowd_bucket(level: float, thresholds: np.ndarray) -> int:
    """Bucket index 0..n-1 (0 = quietest) for a level, via the ride's threshold edges."""
    if thresholds.size == 0:
        return 0
    return int(np.searchsorted(thresholds, level, side="right"))


@dataclass
class ShapeProfiles:
    """Built profiles for ONE park + the render logic."""

    park_id: str
    slot_count: int
    dow_mode: str
    min_obs: int
    thresholds: dict[str, np.ndarray] = field(default_factory=dict)  # ride -> crowd edges
    g_rcd: dict[tuple, Cell] = field(default_factory=dict)  # (ride, crowd, dow)
    g_rc: dict[tuple, Cell] = field(default_factory=dict)   # (ride, crowd)
    g_rd: dict[tuple, Cell] = field(default_factory=dict)   # (ride, dow)
    g_r: dict[tuple, Cell] = field(default_factory=dict)    # (ride,)
    g_park: Curve | None = None                             # park-wide ultimate fallback

    # -- serve helpers ------------------------------------------------------
    def level_to_crowd(self, ride: str, level: float) -> int:
        return crowd_bucket(level, self.thresholds.get(ride, np.array([])))

    def pick_curve(self, ride: str, crowd: int, dow: str) -> tuple[Curve, str]:
        """Finest trusted normalised curve for (ride, crowd, dow), with fallback. Returns
        (curve, source-tag). Source tag names which granularity was used (diagnostics)."""
        for dct, key, tag in (
            (self.g_rcd, (ride, crowd, dow), "rcd"),
            (self.g_rc, (ride, crowd), "rc"),
            (self.g_rd, (ride, dow), "rd"),
            (self.g_r, (ride,), "r"),
        ):
            cell = dct.get(key)
            if cell is not None and cell[1] >= self.min_obs and np.isfinite(cell[0]).any():
                return cell[0], tag
        if self.g_park is not None:
            return self.g_park, "park"
        return np.full(self.slot_count, np.nan), "none"

    def shape(self, ride: str, level: float, dow_index: int) -> tuple[Curve, str]:
        """Normalised form for the (level-derived) crowd bucket + day-of-week."""
        crowd = self.level_to_crowd(ride, level)
        return self.pick_curve(ride, crowd, dow_bucket(dow_index, self.dow_mode))

    def render(self, ride: str, level: float, dow_index: int) -> Curve:
        """Servable per-slot wait curve = predicted level × normalised shape. NaN slots
        (ride not typically open then) stay NaN."""
        curve, _ = self.shape(ride, level, dow_index)
        return level * curve

    def coverage(self) -> dict:
        """Diagnostic summary: how many rides, and the fallback-tag mix over (ride, crowd,
        dow) combinations actually present in the build."""
        rides = list(self.g_r.keys())
        return {
            "park_id": self.park_id,
            "rides": len(rides),
            "cells_rcd": len(self.g_rcd),
            "cells_rc": len(self.g_rc),
            "cells_rd": len(self.g_rd),
            "has_park_fallback": self.g_park is not None,
            "slot_count": self.slot_count,
        }


def _curves(df: pd.DataFrame, keys: list[str], slot_count: int) -> dict[tuple, Cell]:
    """{key_tuple: (mean-normalised curve [slot_count], n_distinct_days)} for a grouping."""
    means = df.groupby(keys + ["slot"], observed=True)["y_norm"].mean().reset_index()
    ndays = df.groupby(keys, observed=True)["day"].nunique()
    out: dict[tuple, Cell] = {}
    for key_vals, sub in means.groupby(keys, observed=True):
        k = key_vals if isinstance(key_vals, tuple) else (key_vals,)
        curve = np.full(slot_count, np.nan)
        curve[sub["slot"].to_numpy(dtype=int)] = sub["y_norm"].to_numpy(dtype=float)
        out[k] = (curve, int(ndays.loc[key_vals]))
    return out


def build_profiles(
    df: pd.DataFrame,
    *,
    park_id: str,
    slot_count: int,
    dow_mode: str = "wend",
    n_buckets: int = 3,
    min_day_peak: float = 5.0,
    min_day_slots: int = 8,
    min_obs: int = 5,
) -> ShapeProfiles | None:
    """Build the normalised-form profiles for one park's panel.

    df columns: unique_id, day (datetime), slot (int), y (median wait). Returns None if no
    day qualifies as shape-defining.
    """
    if df.empty:
        return None
    df = df[["unique_id", "day", "slot", "y"]].copy()

    # 1. Per (ride, day): daily level (peak) + operating-slot count; keep real operating days.
    day_stats = (
        df.groupby(["unique_id", "day"], observed=True)["y"]
        .agg(level="max", n_slots="size")
        .reset_index()
    )
    day_stats = day_stats[
        (day_stats["level"] >= min_day_peak) & (day_stats["n_slots"] >= min_day_slots)
    ]
    if day_stats.empty:
        return None

    # 2. Normalise each kept day's curve by its level (level-free form).
    df = df.merge(day_stats[["unique_id", "day", "level"]], on=["unique_id", "day"])
    df["y_norm"] = df["y"] / df["level"]
    df["dowb"] = df["day"].dt.dayofweek.map(lambda d: dow_bucket(int(d), dow_mode))

    # 3. Per-ride crowd thresholds (terciles of that ride's daily peaks) + per-row bucket.
    thresholds: dict[str, np.ndarray] = {}
    for ride, sub in day_stats.groupby("unique_id", observed=True):
        thresholds[ride] = crowd_thresholds(sub["level"].to_numpy(dtype=float), n_buckets)
    df["crowd"] = [
        crowd_bucket(lv, thresholds[r])
        for r, lv in zip(df["unique_id"], df["level"])
    ]

    # 4. Averaged normalised curves at each fallback granularity (+ park-wide ultimate).
    prof = ShapeProfiles(
        park_id=park_id, slot_count=slot_count, dow_mode=dow_mode, min_obs=min_obs,
        thresholds=thresholds,
        g_rcd=_curves(df, ["unique_id", "crowd", "dowb"], slot_count),
        g_rc=_curves(df, ["unique_id", "crowd"], slot_count),
        g_rd=_curves(df, ["unique_id", "dowb"], slot_count),
        g_r=_curves(df, ["unique_id"], slot_count),
    )
    park_curve = np.full(slot_count, np.nan)
    park_mean = df.groupby("slot", observed=True)["y_norm"].mean()
    park_curve[park_mean.index.to_numpy(dtype=int)] = park_mean.to_numpy(dtype=float)
    prof.g_park = park_curve
    return prof
