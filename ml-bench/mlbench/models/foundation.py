"""Time-series foundation models as zero-shot plug-ins (PAR-828).

Two models, one series construction:

* **Chronos-2** (``amazon/chronos-2``, Apache-2.0). Native past and known-future
  covariates; a park is ONE multivariate task — every ride of the park is a target
  variate and the covariates are shared — so the group attention can look across
  the rides of a park. Context up to 8192 steps, 64 output patches × 16 = 1024 steps
  without autoregressive unrolling.
* **TimesFM 3.0** (``google/timesfm-3.0-pytorch``; weights under the TimesFM
  Non-Commercial License v1.0 — fine for park.fan, which is non-commercial, and for
  this offline benchmark; NOT for production serving). Past-and-future covariates
  are native in 3.0. Its variate attention is capped at 32 variates (targets AND
  covariates), which a park with 40–70 rides does not fit, so each ride is a
  univariate task with the covariates as extra variates. Quantile head 0.1 … 0.9:
  there is no q95, so the adapter returns q95 = NaN (D8 reports it as missing).

**Context layout** (``context_mode``):

``compressed``  the ride's 15-min slots inside the PUBLISHED operating windows only,
                day after day — nights and closed days are left out, which stretches
                the 8192-step context to ~170 operating days. Day boundaries are told
                to the model by the covariates (minutes since opening / until closing,
                local time of day, weekday).
``grid``        a regular 15-min UTC grid, NaN outside the windows (8192 steps =
                85 days). Chronos-2 masks NaN natively; TimesFM interpolates NaN
                linearly (which is wrong across nights), so TimesFM is run compressed only.

Inside the window a slot without truth (ride down, closed, wait < 5) is NaN in both
layouts. The forecast is asked for every future window slot (compressed) or every
future grid slot up to the last requested window close (grid), and then joined to
the harness's ``horizon_slots``.

**Covariates** (BENCH-SPEC "Known-future covariates"), per slot:
minutes since published opening, minutes until published closing, in-window flag
(grid only), local time of day; per park-day: weekend, weekday (sin/cos), public /
school holiday of the park's own region, holiday / school-holiday counts incl. the
neighbour regions (ml-service ``holiday_features.py`` OR-semantics), any school
holiday, schedule holiday and bridge day. ``weather=True`` adds the daily max
temperature and precipitation — these are ACTUALS (no forecast archive exists), so
a ``*_wx`` model is an ORACLE upper bound and is reported separately.

**Daily variant** (``predict_daily``): the same model on the ride-day P90 level
series (the truth level, ``>= 8`` slots, NaN on closed days), calendar days, with
day covariates (+ published day length: 0 = published closed, NaN = beyond the
operator's publishing horizon — an absent schedule never becomes a closed day).
The runner composes it with H5 as ``<name>_x_h5`` at every slot lead, so long leads
(d10–d90) and UC4 (to d365) are scored without producing 15-min slots there.

The information cut is the runner's: truth comes only from ``origin.history``
(slots that end before the park's origin); covariates and windows are known-future
inputs by definition (``windows`` is the published schedule, flagged
``published_final`` — the export has no schedule history).
"""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import ClassVar

import numpy as np
import pandas as pd

from .base import Model, Origin

QUANTILES = (0.5, 0.8, 0.95)
SLOT_COVS = ["ko_min", "kc_min", "tod"]
DAY_COVS = ["is_weekend", "dow_sin", "dow_cos", "is_holiday_primary", "is_school_holiday_primary",
            "holiday_count_total", "school_holiday_count_total", "is_school_holiday_any",
            "is_bridge_day"]
WX_COVS = ["temp_max", "precip_sum"]
EMPTY = pd.DataFrame(columns=["attraction_id", "slot_start_utc", "q50", "q80", "q95"])

_BACKENDS: dict[str, object] = {}


# --------------------------------------------------------------------------- backends

class _Backend:
    """``forecast(tasks)``: tasks = [(target (n, T), past_cov (k, T), future_cov (k, H))];
    returns one array (n, H, 3) per task with the quantiles QUANTILES."""

    name = "backend"
    peak_vram_gb = 0.0

    def forecast(self, tasks: list[tuple[np.ndarray, np.ndarray | None, np.ndarray | None]],
                 cov_names: list[str]) -> list[np.ndarray]:
        raise NotImplementedError


class StubBackend(_Backend):
    """Deterministic stand-in for tests: the nan-mean of the last 64 context steps."""

    name = "stub"

    def forecast(self, tasks, cov_names):
        out = []
        for y, _pc, fc in tasks:
            H = fc.shape[1] if fc is not None else 1
            with np.errstate(all="ignore"):
                m = np.nanmean(y[:, -64:], axis=1) if y.shape[1] else np.full(y.shape[0], np.nan)
            m = np.where(np.isfinite(m), m, np.nan)
            out.append(np.repeat(np.repeat(m[:, None, None], H, axis=1), 3, axis=2))
        return out


class ChronosBackend(_Backend):
    name = "chronos2"

    def __init__(self, model_id: str = "amazon/chronos-2", batch_size: int = 512):
        import torch
        from chronos import BaseChronosPipeline

        self.torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.pipe = BaseChronosPipeline.from_pretrained(model_id, device_map=self.device,
                                                        torch_dtype=torch.float32)
        self.batch_size = batch_size
        self.context_length = self.pipe.model_context_length

    def forecast(self, tasks, cov_names):
        out = []
        for y, pc, fc in tasks:
            H = fc.shape[1]
            inp = {"target": y[:, -self.context_length:].astype(np.float32)}
            if pc is not None and len(cov_names):
                T = inp["target"].shape[1]
                inp["past_covariates"] = {n: pc[i, -T:].astype(np.float32) for i, n in enumerate(cov_names)}
                inp["future_covariates"] = {n: fc[i].astype(np.float32) for i, n in enumerate(cov_names)}
            with self.torch.inference_mode():
                q, _ = self.pipe.predict_quantiles([inp], prediction_length=H,
                                                   quantile_levels=list(QUANTILES),
                                                   batch_size=self.batch_size)
            out.append(q[0].float().cpu().numpy())  # (n, H, 3)
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, self.torch.cuda.max_memory_allocated() / 1e9)
        return out


class TimesFMBackend(_Backend):
    name = "timesfm3"

    def __init__(self, model_id: str = "google/timesfm-3.0-pytorch", batch_size: int = 64,
                 context_length: int = 8192):
        import torch
        from timesfm3.torch import TimesFM3Forecaster

        self.torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.fm = TimesFM3Forecaster.from_pretrained(model_id, device=self.device,
                                                     per_core_batch_size=batch_size)
        qs = list(self.fm.config.quantiles)
        self.qi = [qs.index(0.5), qs.index(0.8), None]          # no q95 in the head
        self.context_length = context_length

    def forecast(self, tasks, cov_names):
        ctx, pf, owner = [], [], []
        Hmax = max(fc.shape[1] for _y, _p, fc in tasks)
        for ti, (y, pc, fc) in enumerate(tasks):
            T = min(y.shape[1], self.context_length)
            cov = None
            if pc is not None and len(cov_names):
                fpad = fc
                if fc.shape[1] < Hmax:   # one horizon per call: pad the future covariates
                    fpad = np.concatenate([fc, np.repeat(fc[:, -1:], Hmax - fc.shape[1], axis=1)], axis=1)
                cov = np.concatenate([pc[:, -T:], fpad], axis=1).astype(np.float32)
            for i in range(y.shape[0]):
                ctx.append(y[i, -T:].astype(np.float32))
                pf.append(cov)
                owner.append((ti, i))
        res = list(self.fm.predict_batch(ctx, horizon=Hmax,
                                         past_future_covariates=pf if pf[0] is not None else None,
                                         return_quantiles=True, make_positive=True))
        out = [np.full((y.shape[0], fc.shape[1], 3), np.nan) for y, _p, fc in tasks]
        for (ti, i), r in zip(owner, res):
            H = out[ti].shape[1]
            qq = np.asarray(r.quantiles)[:H]
            out[ti][i, :, 0] = qq[:, self.qi[0]]
            out[ti][i, :, 1] = qq[:, self.qi[1]]
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, self.torch.cuda.max_memory_allocated() / 1e9)
        return out


def get_backend(kind: str) -> _Backend:
    if kind not in _BACKENDS:
        t0 = time.monotonic()
        _BACKENDS[kind] = {"chronos2": ChronosBackend, "timesfm3": TimesFMBackend,
                           "stub": StubBackend}[kind]()
        print(f"[foundation] loaded {kind} in {time.monotonic() - t0:.1f}s", flush=True)
    return _BACKENDS[kind]


# --------------------------------------------------------------------------- inputs

def day_covariates(cov: pd.DataFrame, weather: bool) -> pd.DataFrame:
    """Per (park_id, date): the numeric day covariates the models read. Bridge days are
    derived here from the public-holiday flag (Mon before a Tue holiday, Fri after a Thu
    holiday) so past and future days carry the same definition — the schedule's own
    bridge flag exists only where the schedule was known at the origin."""
    d = cov.copy()
    d["date"] = pd.to_datetime(d["date"]).dt.date
    dow = pd.to_datetime(d["date"]).dt.dayofweek          # 0 = Monday
    d["is_weekend"] = dow.isin([5, 6]).astype(float)
    d["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    d["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    d = d.sort_values(["park_id", "date"])
    hol = d["is_holiday_primary"].fillna(0).astype(float)
    g = d.assign(h=hol).groupby("park_id")["h"]
    nxt, prv = g.shift(-1).fillna(0), g.shift(1).fillna(0)
    d["is_bridge_day"] = (((dow == 0) & (nxt > 0)) | ((dow == 4) & (prv > 0))).astype(float) * (1 - hol)
    for col in DAY_COVS + (WX_COVS if weather else []):
        if col not in d:
            d[col] = np.nan
        d[col] = pd.to_numeric(d[col], errors="coerce").astype(float)
    # published day length: hours where the schedule was known at the origin, 0 for a
    # known closed day, NaN where the window is projected (never a fabricated fact)
    known = d.get("schedule_known_at_origin")
    hours = (pd.to_datetime(d["open_utc"], utc=True).rsub(pd.to_datetime(d["close_utc"], utc=True))
             .dt.total_seconds() / 3600) if "open_utc" in d else pd.Series(np.nan, index=d.index)
    if known is not None:
        known = known.fillna(False).astype(bool)
        d["day_len_h"] = np.where(known, hours.fillna(0.0), np.nan)
    else:
        d["day_len_h"] = np.nan
    return d


def windows_from_history(h: pd.DataFrame) -> pd.DataFrame:
    """Past operating windows per (park_id, date), recovered from the truth slots' ko/kc
    (minutes since the published opening / to the published closing). A day on which no
    ride of the park has a truth slot has no window — exactly the days the context skips."""
    s = pd.to_datetime(h["slot_start_utc"], utc=True)
    w = pd.DataFrame({"park_id": h["park_id"].to_numpy(), "date": pd.to_datetime(h["date"]).dt.date,
                      "open_utc": s - pd.to_timedelta(h["ko"].to_numpy() * 15, unit="min"),
                      "close_utc": s + pd.to_timedelta((h["kc"].to_numpy() + 1) * 15, unit="min")})
    return w.groupby(["park_id", "date"]).agg(open_utc=("open_utc", "min"),
                                              close_utc=("close_utc", "max")).reset_index()


def _expand(w: pd.DataFrame) -> pd.DataFrame:
    """Window rows -> one row per 15-min slot (park_id, date, slot_utc, ko_min, kc_min)."""
    n = ((w["close_utc"] - w["open_utc"]).dt.total_seconds() // 900).astype(int).clip(lower=0)
    rep = w.loc[w.index.repeat(n)].copy()
    k = rep.groupby(level=0).cumcount().to_numpy()
    rep["slot_utc"] = rep["open_utc"] + pd.to_timedelta(k * 15, unit="min")
    rep["ko_min"] = k * 15.0
    rep["kc_min"] = (rep["close_utc"] - rep["slot_utc"]).dt.total_seconds() / 60 - 15
    return rep[["park_id", "date", "slot_utc", "ko_min", "kc_min"]].reset_index(drop=True)


def _tod(slot_utc: pd.Series, tz: str) -> np.ndarray:
    loc = slot_utc.dt.tz_convert(tz)
    return (loc.dt.hour + loc.dt.minute / 60.0).to_numpy(dtype=float)


# --------------------------------------------------------------------------- the model

class FoundationModel(Model):
    """Zero-shot foundation model adapter. Subclasses pick backend / layout / covariates."""

    name: ClassVar[str] = "foundation"
    backend: ClassVar[str] = "chronos2"
    context_mode: ClassVar[str] = "compressed"
    use_covariates: ClassVar[bool] = True
    daily: ClassVar[bool] = True
    provides_quantiles = True
    intraday = True
    max_lead_days = 7
    refit_every_days = None          # zero-shot: nothing to fit (fit() is NOT overridden)
    context_slots: ClassVar[int] = int(os.environ.get("MLBENCH_FM_CONTEXT", "8192"))
    daily_context_days: ClassVar[int] = 400
    covariate_history_days = 400

    def __init__(self):
        self.stats = {"slot_calls": 0, "slot_seconds": 0.0, "daily_calls": 0, "daily_seconds": 0.0}
        self._hkey = None
        self._hist = None

    def _engine(self) -> _Backend:
        return get_backend(os.environ.get("MLBENCH_FM_BACKEND", self.backend))

    def _cov_names(self, slot: bool) -> list[str]:
        if not self.use_covariates:
            return []
        names = (SLOT_COVS + (["in_win"] if self.context_mode == "grid" else [])) if slot else []
        names = names + DAY_COVS + (WX_COVS if self.uses_oracle_weather else [])
        if not slot and self.backend == "chronos2":
            names = names + ["day_len_h"]   # NaN beyond the known schedule: Chronos masks NaN
        return names

    def _log(self, origin: Origin, what: str, t0: float, rows: int) -> None:
        sec = time.monotonic() - t0
        k = "daily" if what == "daily" else "slot"
        self.stats[f"{k}_calls"] += 1
        self.stats[f"{k}_seconds"] += sec
        eng = _BACKENDS.get(os.environ.get("MLBENCH_FM_BACKEND", self.backend))
        print(f"[fm] {self.scored_name()} {origin.date} {origin.kind}@{origin.hour_local} {what} "
              f"{sec:.1f}s rows={rows} vram_peak={getattr(eng, 'peak_vram_gb', 0):.2f}GB", flush=True)

    def _history(self, origin: Origin, days: int) -> pd.DataFrame:
        """Truth before the origin. The days before the origin date are cached per date
        (the daily call and the intraday calls of one date share them); the origin day's
        own slots are always taken from THIS origin's view (its cut)."""
        c = origin.date
        if self._hkey != (c, days):
            full = origin.history.df(days)
            full = full[pd.to_datetime(full["date"]).dt.date < c]
            self._hist, self._hkey = full, (c, days)
        today = origin.history.df(0)
        return pd.concat([self._hist, today], ignore_index=True) if len(today) else self._hist

    # -- 15-min slots
    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        if horizon_slots.empty:
            return EMPTY
        t0 = time.monotonic()
        per_day = 24 if self.context_mode == "compressed" else 96
        days_back = self.context_slots // per_day + 2
        hist = self._history(origin, days_back)
        if hist.empty:
            return EMPTY
        hist = hist.assign(slot_start_utc=pd.to_datetime(hist["slot_start_utc"], utc=True))
        hz = horizon_slots.copy()
        hz["slot_start_utc"] = pd.to_datetime(hz["slot_start_utc"], utc=True)
        hz["date"] = pd.to_datetime(hz["date"]).dt.date
        ou = origin.origin_utc.set_index("park_id")
        names = self._cov_names(slot=True)
        dc = day_covariates(known_future_covariates, self.uses_oracle_weather).set_index(["park_id", "date"]) \
            if names else None

        # past window slots (from the truth) + future window slots (the harness's grid)
        past = _expand(windows_from_history(hist))
        fut_all = hz.drop_duplicates(["park_id", "slot_start_utc"])[["park_id", "date", "slot_start_utc", "ko", "kc"]]
        fut_all = fut_all.rename(columns={"slot_start_utc": "slot_utc"})
        fut_all["ko_min"] = fut_all["ko"] * 15.0
        fut_all["kc_min"] = fut_all["kc"] * 15.0
        hist_by_park = dict(tuple(hist.groupby("park_id")))
        past_by_park = dict(tuple(past.groupby("park_id")))
        rides_by_park = hz.groupby("park_id")["attraction_id"].unique()

        tasks, meta = [], []
        for pid, fut in fut_all.groupby("park_id", sort=False):
            org = pd.Timestamp(ou.at[pid, "origin_utc"])
            org = org.tz_localize("UTC") if org.tzinfo is None else org.tz_convert("UTC")
            tz = ou.at[pid, "timezone"]
            pw = past_by_park.get(pid)
            if pw is None:
                continue
            pw = pw[pw["slot_utc"] + pd.Timedelta(minutes=15) <= org]
            fut = fut[fut["slot_utc"] >= org].sort_values("slot_utc")
            if self.context_mode == "compressed":
                ctx = pw.sort_values("slot_utc").drop_duplicates("slot_utc", keep="last").tail(self.context_slots)
                ctx = ctx.assign(in_win=1.0)
                fseq = fut[["date", "slot_utc", "ko_min", "kc_min"]].assign(in_win=1.0)
            else:
                ctx, fseq = self._grid(pw, fut, org, tz)
            if ctx.empty or fseq.empty:
                continue
            ctx = ctx.assign(tod=_tod(ctx["slot_utc"], tz))
            fseq = fseq.assign(tod=_tod(fseq["slot_utc"], tz))
            rides = sorted(rides_by_park[pid])
            Y = np.full((len(rides), len(ctx)), np.nan, dtype=np.float32)
            h = hist_by_park.get(pid)
            if h is not None and len(h):
                ri = pd.Series(np.arange(len(rides)), index=rides)
                ci = pd.Series(np.arange(len(ctx)), index=pd.DatetimeIndex(ctx["slot_utc"]))
                r_ix = ri.reindex(h["attraction_id"].to_numpy()).to_numpy()
                c_ix = ci.reindex(pd.DatetimeIndex(h["slot_start_utc"])).to_numpy()
                ok = np.isfinite(r_ix) & np.isfinite(c_ix)
                Y[r_ix[ok].astype(int), c_ix[ok].astype(int)] = h["y"].to_numpy(dtype=np.float32)[ok]
            if not np.isfinite(Y).any():
                continue
            if names:
                def cov_matrix(seq):
                    idx = pd.MultiIndex.from_arrays([np.full(len(seq), pid, dtype=object), seq["date"].to_numpy()])
                    dd = dc.reindex(idx)
                    cols = [seq[n].to_numpy(dtype=np.float32) if n in seq else dd[n].to_numpy(dtype=np.float32)
                            for n in names]
                    return np.vstack(cols)
                pc, fc = cov_matrix(ctx), cov_matrix(fseq)
            else:
                pc, fc = None, np.zeros((0, len(fseq)), np.float32)
            tasks.append((Y, pc, fc))
            meta.append((rides, fseq["slot_utc"].to_numpy()))
        if not tasks:
            return EMPTY
        preds = self._engine().forecast(tasks, names)
        frames = []
        for (rides, fslots), q in zip(meta, preds):
            n, H = len(rides), len(fslots)
            frames.append(pd.DataFrame({
                "attraction_id": np.repeat(np.asarray(rides, dtype=object), H),
                "slot_start_utc": np.tile(fslots, n),
                "q50": q[:, :, 0].reshape(-1), "q80": q[:, :, 1].reshape(-1),
                "q95": q[:, :, 2].reshape(-1)}))
        out = pd.concat(frames, ignore_index=True)
        out["slot_start_utc"] = pd.to_datetime(out["slot_start_utc"], utc=True)
        for k in ("q50", "q80", "q95"):
            out[k] = out[k].clip(lower=0)
        out["q80"] = np.fmax(out["q80"], out["q50"])
        out["q95"] = np.where(np.isfinite(out["q95"]), np.fmax(out["q95"], out["q80"]), np.nan)
        out = out.merge(hz[["attraction_id", "slot_start_utc"]], on=["attraction_id", "slot_start_utc"])
        out = out[np.isfinite(out["q50"])]
        self._log(origin, "slots", t0, len(out))
        return out[["attraction_id", "slot_start_utc", "q50", "q80", "q95"]]

    def _grid(self, pw: pd.DataFrame, fut: pd.DataFrame, org: pd.Timestamp, tz: str):
        """Regular 15-min grid: context = the last ``context_slots`` grid steps before the
        origin, future = every step up to the last requested slot. Off-window steps carry
        in_win = 0 and NaN for the window-relative minutes (Chronos-2 masks NaN)."""
        t0 = org - pd.Timedelta(minutes=15 * self.context_slots)
        ctx_t = pd.date_range(t0, org - pd.Timedelta(minutes=15), freq="15min", tz="UTC")
        fut_t = pd.date_range(org, fut["slot_utc"].max(), freq="15min", tz="UTC")

        def fill(times, w):
            g = pd.DataFrame({"slot_utc": times})
            g = g.merge(w[["slot_utc", "date", "ko_min", "kc_min"]], on="slot_utc", how="left")
            g["in_win"] = g["ko_min"].notna().astype(float)
            loc_date = g["slot_utc"].dt.tz_convert(tz).dt.date
            g["date"] = g["date"].where(g["date"].notna(), pd.Series(loc_date, index=g.index))
            return g

        return fill(ctx_t, pw.drop_duplicates("slot_utc", keep="last")), fill(fut_t, fut)

    # -- daily levels (UC4 + level × H5 at long leads)
    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        if not self.daily or horizon_days.empty:
            return None
        t0 = time.monotonic()
        c = origin.date
        h = origin.history.df(self.daily_context_days)
        if h.empty:
            return None
        h["date"] = pd.to_datetime(h["date"]).dt.date
        h = h[h["date"] < c]
        g = h.groupby(["park_id", "attraction_id", "date"])["y"]
        rd = pd.concat([g.quantile(0.9).rename("p90"), g.size().rename("n")], axis=1).reset_index()
        rd = rd[rd["n"] >= 8]
        if rd.empty:
            return None
        max_lead = int(horizon_days["lead_days"].max())
        ctx_days = list(pd.date_range(rd["date"].min(), c - dt.timedelta(days=1)).date)
        fut_days = list(pd.date_range(c, c + dt.timedelta(days=max_lead)).date)
        if len(ctx_days) < 14:
            return None
        names = self._cov_names(slot=False)
        dc = day_covariates(known_future_covariates, self.uses_oracle_weather) if names else None
        if dc is not None:
            # a past day's length comes from the truth windows (NaN when nothing ran)
            pw = windows_from_history(h)
            pw["len_past"] = (pw["close_utc"] - pw["open_utc"]).dt.total_seconds() / 3600
            dc = dc.merge(pw[["park_id", "date", "len_past"]], on=["park_id", "date"], how="left")
            past = pd.to_datetime(dc["date"]) < pd.Timestamp(c)
            dc.loc[past, "day_len_h"] = dc.loc[past, "len_past"]
        rides_by_park = horizon_days.groupby("park_id")["attraction_id"].unique()
        rd_by_park = dict(tuple(rd.groupby("park_id")))
        dc_by_park = dict(tuple(dc.groupby("park_id"))) if dc is not None else {}
        tasks, meta = [], []
        all_days = ctx_days + fut_days
        for pid in sorted(rides_by_park.index):
            rides = sorted(rides_by_park[pid])
            r = rd_by_park.get(pid)
            if r is None or r.empty:
                continue
            piv = r.pivot(index="attraction_id", columns="date", values="p90")
            Y = piv.reindex(index=rides, columns=ctx_days).to_numpy(dtype=np.float32)
            if not np.isfinite(Y).any():
                continue
            if names:
                cv = dc_by_park[pid].set_index("date").reindex(all_days)[names].to_numpy(dtype=np.float32).T
                pc, fc = cv[:, :len(ctx_days)], cv[:, len(ctx_days):]
            else:
                pc, fc = None, np.zeros((0, len(fut_days)), np.float32)
            tasks.append((Y, pc, fc))
            meta.append(rides)
        if not tasks:
            return None
        preds = self._engine().forecast(tasks, names)
        frames = []
        for rides, q in zip(meta, preds):
            frames.append(pd.DataFrame({
                "attraction_id": np.repeat(np.asarray(rides, dtype=object), len(fut_days)),
                "date": np.tile(np.asarray(fut_days, dtype=object), len(rides)),
                "level": np.clip(q[:, :, 0].reshape(-1), 0, None)}))
        out = pd.concat(frames, ignore_index=True)
        want = horizon_days[["attraction_id", "date"]].copy()
        want["date"] = pd.to_datetime(want["date"]).dt.date
        out = out.merge(want, on=["attraction_id", "date"])
        out = out[np.isfinite(out["level"])]
        self._log(origin, "daily", t0, len(out))
        return out


# --------------------------------------------------------------------------- registered variants

class Chronos2(FoundationModel):
    """Chronos-2 zero-shot, compressed operating-slot context, no weather (the honest variant)."""
    name = "chronos2"
    backend = "chronos2"


class Chronos2Owx(Chronos2):
    """+ daily weather ACTUALS — ORACLE upper bound, scored as chronos2_owx."""
    uses_oracle_weather = True
    intraday = False


class Chronos2Grid(Chronos2):
    """Regular 15-min grid context with NaN outside the windows (layout test)."""
    name = "chronos2_grid"
    context_mode = "grid"
    intraday = False
    daily = False


class Chronos2NoCov(Chronos2):
    """No covariates at all (what the covariates are worth)."""
    name = "chronos2_nocov"
    use_covariates = False
    intraday = False
    daily = False


class TimesFM3(FoundationModel):
    """TimesFM 3.0 zero-shot, compressed context, no weather. NC-licensed weights."""
    name = "timesfm3"
    backend = "timesfm3"
    intraday = False


class TimesFM3Owx(TimesFM3):
    uses_oracle_weather = True
    daily = False


VARIANTS = [Chronos2, Chronos2Owx, Chronos2Grid, Chronos2NoCov, TimesFM3, TimesFM3Owx]
