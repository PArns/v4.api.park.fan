"""Cross-ride tensor assembly — the Phase-0 foundation for the GP-STGNN / bake-off.

This is the one new data artifact the design doc (§11.6) calls for: the full
**[rides × slots] park matrix**, not the scalar park-mean that nf-service's
`add_occupancy` collapses it to. Keeping the whole matrix is the point — it lets an
adaptive-graph STGNN (AGCRN / Graph WaveNet) LEARN the ride×ride coupling (the
park-wide crowd state) instead of being handed a hand-rolled average.

Pure pandas/numpy, NO database — so it is unit-testable on synthetic panels
(`test_tensor.py`). The DB fetch lives in `db.py`; the caller wires them together
in `build_cross_ride_tensor.py`.

Axis convention: matrices are [R, T] (ride, slot); the stacked feature tensor is
[R, T, C] (ride, slot, channel). STG4Traffic-style models usually want [B, T, N, C];
transpose at the model boundary, not here.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

# Feature channels stacked on the last axis of `features` [R, T, C], in this order.
# wait_ffill = forward-filled context level (mirrors the nf backtest's y.ffill().fillna(0));
# obs_mask   = 1 where a real operating wait existed (the loss target mask);
# down       = DOWN-status count in the slot (pent-up-demand / reopening signal);
# slot/hour sin·cos = time-of-day (shared across rides, broadcast per node);
# park_occ   = the SCALAR cross-ride busyness baseline (what the STGNN must beat).
CHANNELS = [
    "wait_ffill",
    "obs_mask",
    "down",
    "slot_sin",
    "slot_cos",
    "hour_sin",
    "hour_cos",
    "park_occ",
]


@dataclass
class CrossRideTensor:
    """Aligned park matrices on a common 15-min grid.

    All [R, T] matrices share `ride_ids` (axis 0) and `slots` (axis 1)."""

    park_id: str
    ride_ids: list[str]            # [R]
    slots: pd.DatetimeIndex        # [T] park-local 15-min slots
    wait_raw: np.ndarray           # [R, T] median wait, NaN where unobserved
    wait_ffill: np.ndarray         # [R, T] per-ride ffill then 0 (model context input)
    obs_mask: np.ndarray           # [R, T] 1.0 where a real operating wait existed
    down: np.ndarray               # [R, T] DOWN-status count per slot
    park_open: np.ndarray          # [T]    1.0 where the park is open (slot heuristic)
    park_occ: np.ndarray           # [T]    mean wait over observed rides (scalar crowd)
    features: np.ndarray           # [R, T, C] stacked channels (see CHANNELS)
    channel_names: list[str]

    @property
    def shape(self) -> tuple[int, int, int]:
        return self.features.shape  # (R, T, C)

    @property
    def loss_mask(self) -> np.ndarray:
        """[R, T] slots a loss should be scored on: a real observation AND the park
        open. Closed slots and sensor gaps are excluded (no spurious-zero training)."""
        return self.obs_mask * self.park_open[None, :]

    def summary(self) -> dict:
        R, T, C = self.features.shape
        return {
            "park_id": self.park_id,
            "rides": R,
            "slots": T,
            "channels": C,
            "span": (str(self.slots[0]), str(self.slots[-1])) if T else (None, None),
            "obs_density": float(self.obs_mask.mean()) if self.obs_mask.size else 0.0,
            "open_slot_frac": float(self.park_open.mean()) if T else 0.0,
            "loss_slots": int(self.loss_mask.sum()),
            "park_occ_mean": float(self.park_occ[self.park_open > 0].mean())
            if self.park_open.any() else 0.0,
        }

    def to_npz(self, path: str) -> None:
        np.savez_compressed(
            path,
            park_id=self.park_id,
            ride_ids=np.array(self.ride_ids, dtype=object),
            slots=self.slots.astype("datetime64[ns]").to_numpy(),
            wait_raw=self.wait_raw,
            wait_ffill=self.wait_ffill,
            obs_mask=self.obs_mask,
            down=self.down,
            park_open=self.park_open,
            park_occ=self.park_occ,
            features=self.features,
            channel_names=np.array(self.channel_names, dtype=object),
        )


def regularize(
    panel: pd.DataFrame, freq: str = "15min", end: pd.Timestamp | None = None
) -> pd.DataFrame:
    """Put every ride of a park onto ONE common regular slot grid.

    A tensor needs all rides aligned on the SAME time axis, so (unlike the nf
    backtest, which regularizes each series from its own start) the grid here spans
    the park-wide min..max (or ..end), and every ride is reindexed onto it.

    Returns a long frame with one row per (ride, slot): unique_id, ds, y (NaN where
    unobserved), n_obs, down_count, obs_mask. No forward-fill here — that's a tensor
    channel, not a property of the panel.
    """
    if panel.empty:
        return panel.assign(obs_mask=pd.Series(dtype=float))
    start = panel["ds"].min()
    stop = end if end is not None else panel["ds"].max()
    grid = pd.date_range(start, stop, freq=freq)

    out = []
    for uid, g in panel.groupby("unique_id"):
        g = g.set_index("ds").sort_index()
        # Collapse any accidental duplicate slots (defensive; the SQL GROUP BY already
        # dedupes, but a caller could concat windows).
        if g.index.has_duplicates:
            g = g[~g.index.duplicated(keep="last")]
        g = g.reindex(grid)
        obs = g["y"].notna().astype(float)
        out.append(
            pd.DataFrame(
                {
                    "unique_id": uid,
                    "ds": grid,
                    "y": g["y"].to_numpy(),
                    "n_obs": g["n_obs"].fillna(0).to_numpy(),
                    "down_count": g["down_count"].fillna(0).to_numpy(),
                    "obs_mask": obs.to_numpy(),
                }
            )
        )
    return pd.concat(out, ignore_index=True)


def _slot_time_features(slots: pd.DatetimeIndex) -> dict[str, np.ndarray]:
    """Cyclic time-of-day at slot (96/day) and hour granularity — same for every ride
    at a given slot, broadcast across nodes in the tensor."""
    slot_of_day = slots.hour * 4 + slots.minute // 15  # 0..95
    h = slots.hour.to_numpy().astype(float)
    return {
        "slot_sin": np.sin(2 * np.pi * slot_of_day / 96).to_numpy(),
        "slot_cos": np.cos(2 * np.pi * slot_of_day / 96).to_numpy(),
        "hour_sin": np.sin(2 * np.pi * h / 24),
        "hour_cos": np.cos(2 * np.pi * h / 24),
    }


def assemble_tensor(
    panel_reg: pd.DataFrame, park_id: str = "", min_rides_open: int = 3
) -> CrossRideTensor:
    """Pivot a regularized long panel into aligned [R, T] matrices + the [R, T, C]
    feature tensor.

    `park_open[t]` heuristic (mirrors ml-service's operating-day rule at slot
    granularity): the park is open in slot t if at least `min_rides_open` rides report
    a real observation OR a DOWN signal in that slot. This distinguishes "park open,
    ride down/quiet" from "no data at all", so closed slots stay out of the loss.
    """
    ride_ids = sorted(panel_reg["unique_id"].unique().tolist())
    slots = pd.DatetimeIndex(sorted(panel_reg["ds"].unique()))
    R, T = len(ride_ids), len(slots)

    def _pivot(col: str, fill: float | None) -> np.ndarray:
        m = panel_reg.pivot(index="unique_id", columns="ds", values=col)
        m = m.reindex(index=ride_ids, columns=slots)
        if fill is not None:
            m = m.fillna(fill)
        return m.to_numpy(dtype=float)

    wait_raw = _pivot("y", None)               # keep NaN where unobserved
    obs_mask = _pivot("obs_mask", 0.0)
    down = _pivot("down_count", 0.0)
    n_obs = _pivot("n_obs", 0.0)

    # Per-ride forward-fill context level, then 0 for leading gaps (start-padding
    # analog). Done in pandas for correct row-wise ffill.
    wait_ffill = (
        pd.DataFrame(wait_raw, index=ride_ids, columns=slots)
        .ffill(axis=1)
        .fillna(0.0)
        .to_numpy(dtype=float)
    )

    # park_occ[t] = mean real wait across observing rides (the SCALAR crowd baseline).
    masked = np.where(obs_mask > 0, np.nan_to_num(wait_raw, nan=0.0), 0.0)
    den = obs_mask.sum(axis=0)
    park_occ = np.divide(
        masked.sum(axis=0), den, out=np.zeros(T), where=den > 0
    )

    # park_open[t] = enough rides reporting (obs OR down) to call the park open.
    reporting = ((obs_mask > 0) | (down > 0)).sum(axis=0)
    park_open = (reporting >= min_rides_open).astype(float)

    tfeat = _slot_time_features(slots)
    occ_bcast = np.broadcast_to(park_occ, (R, T))
    ch = {
        "wait_ffill": wait_ffill,
        "obs_mask": obs_mask,
        "down": down,
        "slot_sin": np.broadcast_to(tfeat["slot_sin"], (R, T)),
        "slot_cos": np.broadcast_to(tfeat["slot_cos"], (R, T)),
        "hour_sin": np.broadcast_to(tfeat["hour_sin"], (R, T)),
        "hour_cos": np.broadcast_to(tfeat["hour_cos"], (R, T)),
        "park_occ": occ_bcast,
    }
    features = np.stack([ch[name] for name in CHANNELS], axis=-1).astype(float)

    # n_obs is fetched + carried for diagnostics/quality gating even though it isn't a
    # default channel; reference it so linters/readers see it's intentional.
    _ = n_obs

    return CrossRideTensor(
        park_id=park_id,
        ride_ids=ride_ids,
        slots=slots,
        wait_raw=wait_raw,
        wait_ffill=wait_ffill,
        obs_mask=obs_mask,
        down=down,
        park_open=park_open,
        park_occ=park_occ,
        features=features,
        channel_names=list(CHANNELS),
    )


def build(
    panel: pd.DataFrame,
    park_id: str = "",
    freq: str = "15min",
    min_rides_open: int = 3,
    end: pd.Timestamp | None = None,
) -> CrossRideTensor:
    """Convenience: regularize a raw park panel and assemble the tensor in one call."""
    return assemble_tensor(
        regularize(panel, freq=freq, end=end),
        park_id=park_id,
        min_rides_open=min_rides_open,
    )
