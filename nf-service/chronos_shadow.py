"""Chronos-2 as a SHADOW forecast for the forward archive (PAR-852).

Zero-shot ``amazon/chronos-2`` (Apache-2.0, code and weights), ported from the
offline benchmark's plug-in (``ml-bench/mlbench/models/foundation.py``, PAR-828)
for the two variants that won there:

``live``  (bench ``chronos2``)       compressed context — the park's 15-min slots
          inside its PUBLISHED operating windows only, day after day, the last
          8192 of them (~170 operating days). Asked at the intraday origins for
          every window slot of today and tomorrow, exactly as the bench's
          intraday origins asked; the caller keeps the next four hours.
``grid``  (bench ``chronos2_grid``)  a regular 15-min UTC grid, the last 8192
          steps (~85 days), NaN outside the windows (Chronos-2 masks NaN).
          Asked once a day at the 06:00 origin for d0 … d7.

A park is ONE multivariate task: every ride is a target variate and the
covariates are shared, so the group attention looks across the rides.

**Covariates** — the bench's, unchanged (``nocov`` lost 0.39–0.82 min):

======================= =====================================================
``ko_min`` / ``kc_min`` minutes since the published opening / until the
                        published closing (``schedule_entries``, park-wide
                        OPERATING rows; NaN off-window in ``grid``)
``tod``                 park-local time of day in hours
``in_win``              ``grid`` only: 1 inside a published window, else 0
``is_weekend``          pandas ``dayofweek`` ∈ {5, 6} (0 = Monday)
``dow_sin/cos``         2π · dayofweek / 7
holiday flags           ``holiday_features.park_day_holiday_features`` — THE
                        shared ml-service implementation (OR semantics,
                        neighbour regions), vendored byte-identical (a test
                        compares the files): ``is_holiday_primary``,
                        ``is_school_holiday_primary``, ``holiday_count_total``,
                        ``school_holiday_count_total``, ``is_school_holiday_any``
``is_bridge_day``       Mon before / Fri after a primary public holiday
======================= =====================================================

No weather: in the bench it was oracle-only (actuals) and worth < 0.12 min.

**Information cut.** Truth slots enter the context only when the slot ENDED at
or before the origin and its in-force reading is not newer than the origin. The
published schedule and the holiday calendar are known-future inputs by
definition, read at the origin.

**Post-processing** (bench): quantiles 0.5 / 0.8 / 0.95, clipped at 0 and made
monotone; only future slots inside a published window are returned — a closed
hour gets no value, never a 0.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import threading
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd

from holiday_features import HolidayIndex, park_day_holiday_features

logger = logging.getLogger("nf.chronos")

QUANTILES = (0.5, 0.8, 0.95)
SLOT_COVS = ["ko_min", "kc_min", "tod"]
DAY_COVS = [
    "is_weekend",
    "dow_sin",
    "dow_cos",
    "is_holiday_primary",
    "is_school_holiday_primary",
    "holiday_count_total",
    "school_holiday_count_total",
    "is_school_holiday_any",
    "is_bridge_day",
]
SLOT = pd.Timedelta(minutes=15)
#: queue_data is a change log: a reading older than this is not in force.
STALE = pd.Timedelta(hours=3)
MIN_TRUTH_WAIT = 5
#: Chronos-2 decodes 64 patches × 16 steps without autoregressive unrolling.
MAX_HORIZON_STEPS = 1024
#: The rides forecast are those with a truth slot this many days before the
#: origin (the bench forecast the rides it was asked about; this is the ex-ante
#: equivalent and keeps retired rides out of the group attention).
RIDE_RECENT_DAYS = 14

VARIANTS = {
    "live": {"bench": "chronos2", "mode": "compressed", "lead_days": 1},
    "grid": {"bench": "chronos2_grid", "mode": "grid", "lead_days": 7},
}


def cov_names(mode: str) -> list[str]:
    return SLOT_COVS + (["in_win"] if mode == "grid" else []) + DAY_COVS


def history_days_needed(mode: str, context_slots: int) -> int:
    """Days of truth before the origin the context can reach (bench: compressed
    walks back ``context_slots // 24 + 2`` days, grid ``// 96 + 2``)."""
    return context_slots // (24 if mode == "compressed" else 96) + 2


# --------------------------------------------------------------------------- windows


def _ceil_slot(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.ceil("15min")


def window_slots(windows: pd.DataFrame) -> pd.DataFrame:
    """One row per 15-min slot inside each service day's window.

    ``windows``: ``date`` (service day), ``open_utc``, ``close_utc`` — the day's
    earliest published opening and latest closing, as the bench's ``windows``
    table holds them. A slot belongs to a day when its START lies in
    [open, close); where two days' windows overlap (a midnight crossing), the
    LATER day keeps the slot (bench ``QUALIFY … ORDER BY date DESC``).

    ``ko_min`` = ⌊(slot − open) / 15⌋ · 15 and ``kc_min`` = ⌊(close − slot − 1 min)
    / 15⌋ · 15 — the bench's ``ko`` / ``kc`` in minutes, so the last slot of a
    window has ``kc_min`` = 0.
    """
    rows = []
    for w in windows.itertuples(index=False):
        o = pd.Timestamp(w.open_utc)
        c = pd.Timestamp(w.close_utc)
        if pd.isna(o) or pd.isna(c) or c <= o:
            continue
        s = _ceil_slot(o)
        while s < c:
            rows.append((w.date, s, ((s - o) // SLOT) * 15.0,
                         ((c - s - pd.Timedelta(minutes=1)) // SLOT) * 15.0))
            s += SLOT
    out = pd.DataFrame(rows, columns=["date", "slot_utc", "ko_min", "kc_min"])
    if out.empty:
        out["slot_utc"] = pd.to_datetime(out["slot_utc"], utc=True)
        return out
    out["slot_utc"] = pd.to_datetime(out["slot_utc"], utc=True)
    out = out.sort_values(["slot_utc", "date"]).drop_duplicates("slot_utc", keep="last")
    return out.reset_index(drop=True)


def truth_slots(readings: pd.DataFrame, windows: pd.DataFrame) -> pd.DataFrame:
    """BENCH-SPEC truth for every ride and window slot.

    ``readings``: ``attraction_id``, ``ts`` (UTC), ``status``, ``wait`` — STANDBY
    change-log rows. A slot's value is the reading in force at the slot
    MIDPOINT (newest row with ts ≤ midpoint, at most 3 h old), kept only when it
    says OPERATING with a wait ≥ 5. Returns ``attraction_id``, ``date``,
    ``slot_utc``, ``y``.
    """
    slots = window_slots(windows)
    cols = ["attraction_id", "date", "slot_utc", "y"]
    if slots.empty or readings.empty:
        return pd.DataFrame(columns=cols)
    mids = (slots["slot_utc"] + SLOT / 2).to_numpy(dtype="datetime64[ns]")
    frames = []
    r = readings.assign(ts=pd.to_datetime(readings["ts"], utc=True)).sort_values("ts")
    for aid, g in r.groupby("attraction_id", sort=False):
        ts = g["ts"].to_numpy(dtype="datetime64[ns]")
        idx = np.searchsorted(ts, mids, side="right") - 1
        ok = idx >= 0
        safe = np.where(ok, idx, 0)
        age = mids - ts[safe]
        status = g["status"].to_numpy()[safe]
        wait = pd.to_numeric(g["wait"], errors="coerce").to_numpy(dtype=float)[safe]
        keep = (ok & (age <= np.timedelta64(int(STALE.total_seconds()), "s"))
                & (status == "OPERATING") & np.isfinite(wait) & (wait >= MIN_TRUTH_WAIT))
        if not keep.any():
            continue
        frames.append(pd.DataFrame({
            "attraction_id": aid,
            "date": slots["date"].to_numpy()[keep],
            "slot_utc": slots["slot_utc"].to_numpy()[keep],
            "y": wait[keep],
        }))
    if not frames:
        return pd.DataFrame(columns=cols)
    out = pd.concat(frames, ignore_index=True)
    out["slot_utc"] = pd.to_datetime(out["slot_utc"], utc=True)
    return out[cols]


# --------------------------------------------------------------------------- covariates


@dataclass
class ParkMeta:
    park_id: str
    timezone: str
    country: str | None
    region: str | None
    influencing: list


def day_covariates(meta: ParkMeta, dates: list[dt.date], holidays: pd.DataFrame) -> pd.DataFrame:
    """DAY_COVS per park-local date (index), from the shared holiday module.

    Bridge days are derived from the primary public-holiday flag of the
    neighbouring CALENDAR days (Mon before a Tue holiday, Fri after a Thu
    holiday, never on a holiday itself), so past and future carry one
    definition — the bench's ``day_covariates``. Weekday is pandas' (0 = Monday).
    """
    if not dates:
        return pd.DataFrame(columns=DAY_COVS)
    d0, d1 = min(dates) - dt.timedelta(days=1), max(dates) + dt.timedelta(days=1)
    days = [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]
    index = HolidayIndex(holidays)
    recs = [park_day_holiday_features(meta.country, meta.region, meta.influencing, day, index)
            for day in days]
    d = pd.DataFrame(recs, index=pd.Index(days, name="date"))
    dow = pd.Series([day.weekday() for day in days], index=d.index)  # 0 = Monday
    d["is_weekend"] = dow.isin([5, 6]).astype(float)
    d["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    d["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    hol = d["is_holiday_primary"].astype(float)
    nxt, prv = hol.shift(-1).fillna(0), hol.shift(1).fillna(0)
    d["is_bridge_day"] = (((dow == 0) & (nxt > 0)) | ((dow == 4) & (prv > 0))).astype(float) * (1 - hol)
    for c in DAY_COVS:
        d[c] = pd.to_numeric(d[c], errors="coerce").astype(float)
    return d.loc[[x for x in days if min(dates) <= x <= max(dates)], DAY_COVS]


def holiday_coverage(holidays_through: dt.date | None, target: dt.date) -> str:
    """Whether the holiday covariates of a target day rest on data: ``none`` when
    the holidays table has no row at all for the park's country (every flag
    reads 0 — absence, not "no holiday"), ``short`` when its last row lies
    before the target day (the sync has not reached it yet), else ``full``."""
    if holidays_through is None:
        return "none"
    return "full" if holidays_through >= target else "short"


# --------------------------------------------------------------------------- the task


@dataclass
class Task:
    rides: list[str]
    target: np.ndarray            # (n_rides, T) float32, NaN = no truth
    past_cov: np.ndarray          # (k, T)
    future_cov: np.ndarray        # (k, H)
    future: pd.DataFrame          # slot_utc, date, in_win — one row per step
    names: list[str]


def _tod(slot_utc: pd.Series, tz: str) -> np.ndarray:
    loc = pd.to_datetime(slot_utc, utc=True).dt.tz_convert(tz)
    return (loc.dt.hour + loc.dt.minute / 60.0).to_numpy(dtype=float)


def build_task(
    mode: str,
    origin: pd.Timestamp,
    tz: str,
    truth: pd.DataFrame,
    windows: pd.DataFrame,
    future_dates: list[dt.date],
    daycov: pd.DataFrame,
    rides: list[str],
    context_slots: int,
) -> Task | None:
    """The bench's series construction for one park at one origin.

    ``truth`` must already be cut at the origin (``cut_truth``); ``windows``
    holds past AND future service days (future ones = published at the origin).
    """
    origin = pd.Timestamp(origin).tz_convert("UTC")
    if not rides:
        return None
    wslots = window_slots(windows)
    if wslots.empty:
        return None
    truth_days = set(truth["date"]) if len(truth) else set()
    # Past window slots: days on which some ride has truth (bench
    # windows_from_history), slots that ended by the origin.
    past = wslots[wslots["date"].isin(truth_days)
                  & (wslots["slot_utc"] + SLOT <= origin)]
    fut = wslots[wslots["date"].isin(set(future_dates)) & (wslots["slot_utc"] >= origin)]
    if fut.empty:
        return None

    if mode == "compressed":
        ctx = past.sort_values("slot_utc").tail(context_slots).assign(in_win=1.0)
        fseq = fut.sort_values("slot_utc").head(MAX_HORIZON_STEPS).assign(in_win=1.0)
    else:
        anchor = _ceil_slot(origin)
        ctx_t = pd.date_range(anchor - SLOT * context_slots, anchor - SLOT, freq="15min", tz="UTC")
        last = min(fut["slot_utc"].max(), anchor + SLOT * (MAX_HORIZON_STEPS - 1))
        fut_t = pd.date_range(anchor, last, freq="15min", tz="UTC")

        def fill(times, w):
            g = pd.DataFrame({"slot_utc": times})
            g = g.merge(w[["slot_utc", "date", "ko_min", "kc_min"]], on="slot_utc", how="left")
            g["in_win"] = g["ko_min"].notna().astype(float)
            loc_date = pd.Series(g["slot_utc"].dt.tz_convert(tz).dt.date, index=g.index)
            g["date"] = g["date"].where(g["date"].notna(), loc_date)
            return g

        ctx, fseq = fill(ctx_t, past), fill(fut_t, fut)
    if ctx.empty or fseq.empty:
        return None
    ctx = ctx.assign(tod=_tod(ctx["slot_utc"], tz)).reset_index(drop=True)
    fseq = fseq.assign(tod=_tod(fseq["slot_utc"], tz)).reset_index(drop=True)

    y = np.full((len(rides), len(ctx)), np.nan, dtype=np.float32)
    if len(truth):
        ri = pd.Series(np.arange(len(rides)), index=rides)
        ci = pd.Series(np.arange(len(ctx)), index=pd.DatetimeIndex(ctx["slot_utc"]))
        r_ix = ri.reindex(truth["attraction_id"].to_numpy()).to_numpy()
        c_ix = ci.reindex(pd.DatetimeIndex(pd.to_datetime(truth["slot_utc"], utc=True))).to_numpy()
        ok = np.isfinite(r_ix) & np.isfinite(c_ix)
        y[r_ix[ok].astype(int), c_ix[ok].astype(int)] = truth["y"].to_numpy(dtype=np.float32)[ok]
    if not np.isfinite(y).any():
        return None

    names = cov_names(mode)

    def cov_matrix(seq: pd.DataFrame) -> np.ndarray:
        dd = daycov.reindex(seq["date"].to_numpy())
        return np.vstack([
            seq[n].to_numpy(dtype=np.float32) if n in seq else dd[n].to_numpy(dtype=np.float32)
            for n in names
        ])

    return Task(rides=list(rides), target=y, past_cov=cov_matrix(ctx),
                future_cov=cov_matrix(fseq), future=fseq[["slot_utc", "date", "in_win"]], names=names)


def cut_truth(truth: pd.DataFrame, origin: pd.Timestamp) -> pd.DataFrame:
    """Only slots that ENDED by the origin (their in-force reading is then
    older than the origin too, since it is read at the midpoint)."""
    if truth.empty:
        return truth
    s = pd.to_datetime(truth["slot_utc"], utc=True)
    return truth[s + SLOT <= pd.Timestamp(origin).tz_convert("UTC")]


def recent_rides(truth: pd.DataFrame, origin: pd.Timestamp, known: list[str]) -> list[str]:
    if truth.empty:
        return []
    s = pd.to_datetime(truth["slot_utc"], utc=True)
    since = pd.Timestamp(origin).tz_convert("UTC") - pd.Timedelta(days=RIDE_RECENT_DAYS)
    ids = set(truth.loc[s >= since, "attraction_id"])
    return sorted(a for a in known if a in ids)


def postprocess(task: Task, q: np.ndarray, keep_until: pd.Timestamp | None) -> pd.DataFrame:
    """Quantiles (n, H, 3) → rows ``attraction_id, date, slot_utc, q50, q80, q95``
    for future steps inside a published window only (bench: clip at 0, q80 ≥ q50,
    q95 ≥ q80, drop NaN q50)."""
    n, h = len(task.rides), len(task.future)
    q = np.asarray(q, dtype=float)[:, :h, :]
    df = pd.DataFrame({
        "attraction_id": np.repeat(np.asarray(task.rides, dtype=object), h),
        "slot_utc": np.tile(task.future["slot_utc"].to_numpy(), n),
        "date": np.tile(task.future["date"].to_numpy(), n),
        "in_win": np.tile(task.future["in_win"].to_numpy(), n),
        "q50": q[:, :, 0].reshape(-1),
        "q80": q[:, :, 1].reshape(-1),
        "q95": q[:, :, 2].reshape(-1),
    })
    for k in ("q50", "q80", "q95"):
        df[k] = df[k].clip(lower=0)
    df["q80"] = np.fmax(df["q80"], df["q50"])
    df["q95"] = np.where(np.isfinite(df["q95"]), np.fmax(df["q95"], df["q80"]), np.nan)
    df["slot_utc"] = pd.to_datetime(df["slot_utc"], utc=True)
    df = df[(df["in_win"] > 0) & np.isfinite(df["q50"])]
    if keep_until is not None:
        df = df[df["slot_utc"] < pd.Timestamp(keep_until).tz_convert("UTC")]
    return df.drop(columns=["in_win"]).reset_index(drop=True)


def to_curves(rows: pd.DataFrame, coverage_of) -> list[dict]:
    """One curve per ride and service day: contiguous 15-min arrays from the
    first to the last window slot, ``None`` where a slot has no value."""
    out = []
    if rows.empty:
        return out
    for (aid, date), g in rows.groupby(["attraction_id", "date"], sort=True):
        g = g.sort_values("slot_utc")
        first = g["slot_utc"].iloc[0]
        n = int((g["slot_utc"].iloc[-1] - first) / SLOT) + 1
        arr = {k: [None] * n for k in ("q50", "q80", "q95")}
        for s, a, b, c in zip(g["slot_utc"], g["q50"], g["q80"], g["q95"]):
            i = int((s - first) / SLOT)
            arr["q50"][i] = round(float(a), 2)
            arr["q80"][i] = round(float(b), 2)
            arr["q95"][i] = None if not np.isfinite(c) else round(float(c), 2)
        out.append({
            "attractionId": aid,
            "date": date.isoformat(),
            "slotStart": first.isoformat().replace("+00:00", "Z"),
            "slotMinutes": 15,
            "covariates": coverage_of(date),
            **arr,
        })
    return out


# --------------------------------------------------------------------------- engine


class ChronosEngine:
    """The resident Chronos-2 pipeline: loaded lazily ONCE, inference under one
    lock (a single GPU, a single uvicorn worker — two concurrent captures must
    neither load the model twice nor interleave on the device)."""

    def __init__(self, model_id: str, revision: str, cache_dir: str, vram_fraction: float):
        self.model_id = model_id
        self.revision = revision
        self.cache_dir = cache_dir
        self.vram_fraction = vram_fraction
        self.pipe = None
        self.device = "cpu"
        self.peak_vram_gb = 0.0
        self.load_seconds = None
        self.lock = threading.Lock()

    def _load(self):
        import torch
        from chronos import BaseChronosPipeline

        os.makedirs(self.cache_dir, exist_ok=True)
        t0 = time.monotonic()
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        if self.device == "cuda":
            torch.cuda.set_per_process_memory_fraction(self.vram_fraction)
        self.pipe = BaseChronosPipeline.from_pretrained(
            self.model_id, revision=self.revision, cache_dir=self.cache_dir,
            device_map=self.device, torch_dtype=torch.float32,
        )
        self.load_seconds = round(time.monotonic() - t0, 1)
        logger.info("Chronos-2 loaded on %s in %.1fs", self.device, self.load_seconds)

    def unload(self) -> None:
        """Frees the weights (before a TFT training run needs the GPU)."""
        with self.lock:
            if self.pipe is None:
                return
            self.pipe = None
            try:
                import torch

                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except Exception as e:  # noqa: BLE001 — freeing is best effort
                logger.debug("empty_cache after unload failed: %s", e)

    @property
    def loaded(self) -> bool:
        return self.pipe is not None

    def forecast(self, task: Task) -> np.ndarray:
        """(n_rides, H, 3) quantiles — the bench's ``ChronosBackend.forecast``."""
        with self.lock:
            if self.pipe is None:
                self._load()
            import torch

            ctx_len = getattr(self.pipe, "model_context_length", task.target.shape[1])
            y = task.target[:, -ctx_len:].astype(np.float32)
            t = y.shape[1]
            inp = {
                "target": y,
                "past_covariates": {n: task.past_cov[i, -t:].astype(np.float32)
                                    for i, n in enumerate(task.names)},
                "future_covariates": {n: task.future_cov[i].astype(np.float32)
                                      for i, n in enumerate(task.names)},
            }
            with torch.inference_mode():
                q, _ = self.pipe.predict_quantiles(
                    [inp], prediction_length=task.future_cov.shape[1],
                    quantile_levels=list(QUANTILES), batch_size=512,
                )
            if self.device == "cuda":
                self.peak_vram_gb = max(self.peak_vram_gb, torch.cuda.max_memory_allocated() / 1e9)
            return q[0].float().cpu().numpy()
