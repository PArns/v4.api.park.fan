"""Walk-forward training and forecasting of the NeuralForecast candidates (PAR-829).

    python -m mlbench.models.nf_precompute --export /data/exports/20261009 \
        --model nf_tide --cache /data/par-829/cache/<run> [--from 2026-02-19 --to 2026-10-07]

GPU work and scoring are split on purpose: this step trains and forecasts (one
GPU process, holding the shared GPU lock), the harness then scores the cached
forecasts on CPU through the ordinary plug-in path (``neuralforecast_models``),
sharded like any other run. Nothing is scored here.

Information cut (BENCH-SPEC "Protocol")
---------------------------------------
* Origins are grouped into **month blocks**. For a block with first origin c0 the
  model is trained ONCE on the panel steps before service day c0 — NeuralForecast
  ``fit(test_size=…)`` holds back every step from c0 on, and the training windows
  (insample + horizon) must end before it. One model per block, not per origin:
  ~9 fits instead of ~230, and a model that is up to a month stale at the end of
  its block, which is what a monthly retrain in production would be.
* Every origin of the block is then forecast with that model: the forecast window
  starts at the origin's hour and its insample is the ``input_size`` steps just
  before it — truth of service days < c (daily origin, 06:00) or, for an intraday
  origin at H:00, also the hours of day c that END by H:00. Nothing after the
  origin enters the input; the window's future part carries only known-future
  covariates. ``tests/test_nf_models.py::test_precompute_information_cut``
  poisons every truth slot after an origin and asserts that origin's forecast
  does not move.
* Opening hours are the published (final) schedule — d0–d7 is inside every
  operator's publishing horizon (median 39 d). Weather is ORACLE (daily actuals)
  and only used by the ``*_wx`` variants, which are reported separately.

Output: ``<cache>/<model>/b<block>_h<HH>.parquet`` — one row per (ride, origin,
service-day hour inside the published window): ``aid, origin_date, origin_hour,
date, hr, q50, q80, q95``. ``origin_hour`` 6 = the daily origin (all of d0–d7);
8…20 = the intraday origins (rest of d0 only). Plus ``<model>/meta.json`` (config,
per-block timings, panel size and torch's peak VRAM per fitted group).

Host safety (celestrial is the production host, see MODEL-AGENT-RULES)
----------------------------------------------------------------------
* ``--quiet`` refuses to START a new block when the block's PROJECTED FINISH would
  reach into the nightly window, not just when the window has already begun — see
  ``quiet_block_reason``. Blocks are cached, so stopping is cheap.
* The predict window array is sized from the number of series (``window_chunk``)
  so the same settings hold for a 20-park subset and for all 157 parks.
* ``MLBENCH_NF_VRAM_FRACTION`` (default 0.6 = 9.8 GB of the 16 GB card) caps torch's
  allocator, so breaching the 10 GB bench budget OOMs this process instead of
  squeezing pcn-service, which shares the GPU.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .nf_panel import HOUR0, K, Panel, build_panel

QUANTILES = [0.5, 0.8, 0.95]
INPUT_DAYS = 28            # input window: 4 weeks (the naive level uses the last 4 same weekdays)
HORIZON_DAYS = 8           # d0 … d7
MIN_INSAMPLE_HOURS = 12    # fewer measured hours in the input window -> no forecast
INTRADAY_HOURS = [8, 10, 12, 14, 16, 18, 20]
# Target size of the predict window array (see forecast_block/_windows_dataset). The
# container cap is 8 GB and the block already holds the panel (3601 series x ~4700
# steps x 22 cols float32 = 1.5 GB on all parks) plus DuckDB's buffers, and
# NeuralForecast copies the array into a torch tensor — so keep one chunk small.
WINDOW_ARRAY_BUDGET = 400 * 1024 ** 2
DEFAULT_BLOCK_ESTIMATE = 5400.0   # s; conservative until a block has been measured
# The GPU is shared with pcn-service and MODEL-AGENT-RULES caps a bench job at 10 GB
# of the 16 GB card. A fraction turns a breach into an OOM in THIS process instead of
# a squeeze on production; 0.6 x 16 GB = 9.8 GB. Override with MLBENCH_NF_VRAM_FRACTION.
VRAM_FRACTION = 0.6

# Model specs. Sizes were set on the subset run (see docs/ml/ml-bench.md, PAR-829).
SPECS: dict[str, dict] = {
    "nf_tide": dict(cls="TiDE", weather=False, kw=dict(
        hidden_size=256, decoder_output_dim=16, temporal_decoder_dim=64, num_encoder_layers=2,
        num_decoder_layers=2, dropout=0.1, layernorm=True, learning_rate=1e-3,
        batch_size=64, windows_batch_size=512, inference_windows_batch_size=1024)),
    "nf_tide_wx": dict(cls="TiDE", weather=True, kw=dict(
        hidden_size=256, decoder_output_dim=16, temporal_decoder_dim=64, num_encoder_layers=2,
        num_decoder_layers=2, dropout=0.1, layernorm=True, learning_rate=1e-3,
        batch_size=64, windows_batch_size=512, inference_windows_batch_size=1024)),
    "nf_nhits": dict(cls="NHITS", weather=False, kw=dict(
        learning_rate=1e-3, batch_size=64, windows_batch_size=512,
        inference_windows_batch_size=1024)),
    # multivariate: one model per region and block, every ride of the region is a channel
    "nf_tsmixerx": dict(cls="TSMixerx", weather=False, multivariate=True, kw=dict(
        n_block=2, ff_dim=64, dropout=0.1, revin=False, learning_rate=1e-3,
        batch_size=1, windows_batch_size=16, inference_windows_batch_size=16)),
}


def window_chunk(n_series: int, n_cols: int, budget: int = WINDOW_ARRAY_BUDGET) -> int:
    """Origins per predict call so [chunk, n_series, L+h, n_cols] float32 fits ``budget``."""
    per_origin = n_series * (INPUT_DAYS + HORIZON_DAYS) * K * n_cols * 4
    return max(1, budget // per_origin)


def month_blocks(first: dt.date, last: dt.date) -> list[tuple[dt.date, dt.date]]:
    out, c = [], first
    while c <= last:
        nxt = (c.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
        out.append((c, min(last, nxt - dt.timedelta(days=1))))
        c = nxt
    return out


def make_model(name: str, panel: Panel, max_steps: int, n_series: int | None, scale: float = 1.0,
               seed: int = 829):
    import torch
    from neuralforecast import models as M
    from neuralforecast.losses.pytorch import MQLoss

    spec = SPECS[name]
    kw = dict(spec["kw"])
    if scale != 1.0:   # tests: tiny networks
        for k in ("hidden_size", "temporal_decoder_dim", "ff_dim"):
            if k in kw:
                kw[k] = max(4, int(kw[k] * scale))
        if spec["cls"] == "NHITS":
            kw["mlp_units"] = [[64, 64]] * 3
    gpu = torch.cuda.is_available() and os.environ.get("MLBENCH_NF_CPU") != "1"
    common = dict(
        h=HORIZON_DAYS * K, input_size=INPUT_DAYS * K,
        futr_exog_list=panel.futr_cols, stat_exog_list=panel.static_cols,
        loss=MQLoss(quantiles=QUANTILES), scaler_type="robust", max_steps=max_steps,
        start_padding_enabled=True, random_seed=seed,
        accelerator="gpu" if gpu else "cpu", devices=1,
        enable_progress_bar=False, enable_model_summary=False, logger=False,
        enable_checkpointing=False, deterministic=not gpu,
    )
    if spec.get("multivariate"):
        common["n_series"] = n_series
        common["valid_batch_size"] = n_series
    return getattr(M, spec["cls"])(**common, **kw)


def to_dataset(panel: Panel, rows: np.ndarray):
    from neuralforecast.tsdataset import TimeSeriesDataset

    temporal = np.ascontiguousarray(panel.temporal[rows]).reshape(-1, len(panel.cols))
    n = len(rows)
    indptr = np.arange(0, (n + 1) * panel.T, panel.T, dtype=np.int64)
    return TimeSeriesDataset(temporal=temporal, temporal_cols=pd.Index(panel.cols), indptr=indptr,
                             y_idx=0, static=panel.static[rows], static_cols=pd.Index(panel.static_cols))


def _windows_dataset(panel: Panel, rows: np.ndarray, starts: np.ndarray, fut: np.ndarray):
    """One NeuralForecast series per (ride, origin): the ``input_size`` steps before the
    origin's first forecast step plus the ``h`` forecast steps. The forecast steps carry
    no target (masked) and the covariates as known at the origin (``fut``: [O, P, L+h, F]
    per origin and park, NaN = keep the panel's value)."""
    from neuralforecast.tsdataset import TimeSeriesDataset

    L, h = INPUT_DAYS * K, HORIZON_DAYS * K
    n, nf = len(rows), len(panel.futr_cols)
    park_ix = {p: i for i, p in enumerate(sorted(set(panel.parks)))}
    pidx = np.array([park_ix[panel.parks[r]] for r in rows])
    arr = np.empty((len(starts), n, L + h, len(panel.cols)), dtype=np.float32)
    for o, st in enumerate(starts):
        lo = st - L
        src = panel.temporal[rows, max(lo, 0):st + h]
        if lo < 0:
            arr[o, :, :-lo] = 0
            arr[o, :, -lo:] = src
        else:
            arr[o] = src
        arr[o, :, L:, 0] = 0
        arr[o, :, L:, -1] = 0
        f = fut[o][pidx]                                       # [n, L+h, F]
        sl = arr[o, :, :, 1:1 + nf]
        np.copyto(sl, f, where=np.isfinite(f))
    temporal = arr.reshape(-1, len(panel.cols))
    m = len(starts) * n
    indptr = np.arange(0, (m + 1) * (L + h), L + h, dtype=np.int64)
    static = np.tile(panel.static[rows], (len(starts), 1))
    ds = TimeSeriesDataset(temporal=temporal, temporal_cols=pd.Index(panel.cols), indptr=indptr,
                           y_idx=0, static=static, static_cols=pd.Index(panel.static_cols))
    return ds, arr


def forecast_block(name: str, panel: Panel, c0: dt.date, c_last: dt.date, max_steps: int,
                   hours: list[int], scale: float = 1.0, log=print, con=None,
                   chunk: int | None = None, stats: dict | None = None) -> dict[int, pd.DataFrame]:
    """Train on steps < c0, forecast every origin c0..c_last. Returns {origin_hour: rows}.

    ``con`` provides the schedule as known at each origin (``origin_covariates``); with
    ``con=None`` the panel's published-final covariates are used (unit tests only).

    ``chunk`` = origins per predict call. ``None`` sizes it from the number of series so
    the window array stays near ``WINDOW_ARRAY_BUDGET``: it is [chunk, series, L+h, cols]
    float32, so a fixed chunk that fits a 20-park subset (737 series) is 5x bigger on all
    157 parks (3601 series) and pushes the container past its 8 GB cap.
    ``stats`` is filled with fit seconds and peak VRAM per group."""
    import torch

    from .nf_panel import hour_features, origin_covariates

    spec = SPECS[name]
    multivariate = bool(spec.get("multivariate"))
    n_win = (c_last - c0).days + 1
    first = panel.idx(c0)
    assert panel.T == first + K * (n_win + HORIZON_DAYS), "panel must end at c_last + 8 days"
    L, h = INPUT_DAYS * K, HORIZON_DAYS * K
    nf = len(panel.futr_cols)
    parks = sorted(set(panel.parks))
    p_ix = {p: i for i, p in enumerate(parks)}
    origins = [c0 + dt.timedelta(days=i) for i in range(n_win)]
    # covariates as known at each origin for its days c .. c+8 (the window reaches into c+8)
    fut_days = np.full((n_win, len(parks), HORIZON_DAYS + 1, K, nf), np.nan, dtype=np.float32)
    if con is not None:
        oc = origin_covariates(con, origins, HORIZON_DAYS + 1)
        oc = oc[oc["park_id"].isin(p_ix)]
        oi = (oc["origin"].to_numpy().astype("datetime64[D]") - np.datetime64(c0, "D")).astype(int)
        di = (oc["date"].to_numpy().astype("datetime64[D]")
              - oc["origin"].to_numpy().astype("datetime64[D]")).astype(int)
        fut_days[oi, oc["park_id"].map(p_ix).to_numpy(), di] = hour_features(oc, panel.futr_cols)
        if spec["weather"]:     # ORACLE variant: weather stays the actuals of the panel
            w = [panel.futr_cols.index(c) for c in panel.futr_cols if c.startswith("wx_")]
            fut_days[..., w] = np.nan
    fut_days = fut_days.reshape(n_win, len(parks), (HORIZON_DAYS + 1) * K, nf)
    in_win_i = panel.cols.index("in_win")

    groups = sorted(set(panel.groups)) if multivariate else ["all"]
    if multivariate:
        hours = [6]          # one predict call per origin and region: daily origins only
    out: dict[int, list[pd.DataFrame]] = {H: [] for H in hours}
    aids = np.array(panel.aids)
    if stats is not None:
        stats.setdefault("groups", [])
    for g in groups:
        rows = (np.arange(len(aids)) if g == "all" else np.flatnonzero(np.array(panel.groups) == g))
        if len(rows) == 0:
            continue
        t0 = time.monotonic()
        model = make_model(name, panel, max_steps, len(rows), scale)
        model.fit(dataset=to_dataset(panel, rows), val_size=0, test_size=panel.T - first)
        t_fit = time.monotonic() - t0
        model.set_test_size(h)
        for H in hours:
            s = max(0, H - HOUR0)                    # first forecast step inside day c
            keep_steps = h if H == 6 else K - s      # intraday: rest of day c only
            step = 1 if multivariate else (chunk or window_chunk(len(rows), len(panel.cols)))
            for i0 in range(0, n_win, step):
                oo = np.arange(i0, min(n_win, i0 + step))
                starts = first + K * oo + s
                # covariates of the window [start-L, start+h): days >= c come from fut_days
                fut = np.full((len(oo), len(parks), L + h, nf), np.nan, dtype=np.float32)
                fut[:, :, L - s:] = fut_days[oo][:, :, :h + s]
                ds, arr = _windows_dataset(panel, rows, starts, fut)
                fc = np.asarray(model.predict(dataset=ds, step_size=h), dtype=np.float32)
                fc = np.sort(fc.reshape(len(oo), len(rows), h, -1), axis=-1)   # no quantile crossing
                ins = arr[:, :, :L, -1].sum(axis=2)                           # [O, n]
                ok = (arr[:, :, L:L + keep_steps, in_win_i] > 0) & (ins[:, :, None] >= MIN_INSAMPLE_HOURS)
                o_i, r_i, k_i = np.nonzero(ok)
                t_abs = starts[o_i] + k_i
                out[H].append(pd.DataFrame({
                    "aid": aids[rows][r_i],
                    "origin_date": np.datetime64(c0, "D") + oo[o_i].astype("timedelta64[D]"),
                    "origin_hour": np.int8(H),
                    "date": np.datetime64(panel.day0, "D") + (t_abs // K).astype("timedelta64[D]"),
                    "hr": (HOUR0 + t_abs % K).astype(np.int8),
                    "q50": np.maximum(fc[o_i, r_i, k_i, 0], 0), "q80": np.maximum(fc[o_i, r_i, k_i, 1], 0),
                    "q95": np.maximum(fc[o_i, r_i, k_i, 2], 0)}))
                del ds, arr
        vram = torch.cuda.max_memory_allocated() / 1e9 if torch.cuda.is_available() else 0.0
        t_total = time.monotonic() - t0
        log(f"  {name} group={g} series={len(rows)} fit={t_fit:.0f}s "
            f"total={t_total:.0f}s peak_vram={vram:.2f}GB chunk={step}")
        if stats is not None:
            # NOTE: max_memory_allocated() is torch's allocator only — it excludes the
            # CUDA context (~0.3-0.5 GB) and every other process on the shared GPU.
            # run_nf_precompute.sh samples nvidia-smi for the device-level peak.
            stats["groups"].append({"group": g, "series": len(rows), "chunk": int(step),
                                    "fit_seconds": round(t_fit, 1), "seconds": round(t_total, 1),
                                    "peak_vram_gb_torch": round(vram, 2)})
        del model
    return {H: pd.concat(v, ignore_index=True) if v else pd.DataFrame() for H, v in out.items()}


def in_quiet_window(spec: str | None, now: dt.datetime | None = None) -> bool:
    """``spec`` = 'HH:MM-HH:MM' UTC (celestrial: no new block from 00:30, nightly jobs 01:00-09:30)."""
    if not spec:
        return False
    a, b = (dt.time.fromisoformat(v) for v in spec.split("-"))
    t = (now or dt.datetime.now(dt.timezone.utc)).time()
    return (a <= t < b) if a < b else (t >= a or t < b)


def quiet_block_reason(spec: str | None, est_seconds: float,
                       now: dt.datetime | None = None) -> str | None:
    """Why a new block must not start now, or ``None`` if it may.

    Checking only the START time is not enough: a block that starts at 00:29 with
    the window at 00:30-09:30 passes the check and then runs for hours straight
    through the nightly jobs (generate-daily 01:00, TFT 03:00, CatBoost 06:00) on
    the production host. So a block is also refused when its PROJECTED FINISH
    reaches into the window. ``est_seconds`` is the expected block duration — the
    longest block measured so far in this run, or ``--block-estimate`` before the
    first one is done.
    """
    if not spec:
        return None
    now = now or dt.datetime.now(dt.timezone.utc)
    if in_quiet_window(spec, now):
        return f"inside the quiet window {spec} UTC"
    a = dt.time.fromisoformat(spec.split("-")[0])
    start = dt.datetime.combine(now.date(), a, tzinfo=dt.timezone.utc)
    if start <= now:
        start += dt.timedelta(days=1)
    if now + dt.timedelta(seconds=est_seconds) >= start:
        return (f"a block takes ~{est_seconds / 60:.0f} min, which would run past "
                f"{start:%H:%M} UTC into the quiet window {spec}")
    return None


def default_origins(con, export: Path, window_days: int = 56) -> tuple[dt.date, dt.date]:
    """The runner's origin range (``Runner.__init__``)."""
    d0, d1 = con.execute("SELECT min(date), max(date) FROM truth").fetchone()
    mf = export / "manifest.json"
    if mf.exists():
        m = json.loads(mf.read_text())
        if m.get("queue_to"):
            d1 = min(d1, dt.date.fromisoformat(m["queue_to"]) - dt.timedelta(days=1))
        if m.get("queue_from"):
            d0 = max(d0, dt.date.fromisoformat(m["queue_from"]) + dt.timedelta(days=1))
    return d0 + dt.timedelta(days=window_days), d1


def run(export: Path, cache: Path, name: str, origin_from: str | None, origin_to: str | None,
        max_steps: int, parks: list[str] | None = None, intraday: bool = True, scale: float = 1.0,
        memory: str = "3GB", threads: int = 4, log=print, quiet: str | None = None,
        block_estimate: float = DEFAULT_BLOCK_ESTIMATE) -> dict:
    import torch

    from ..build import connect
    from ..runner import code_hash, git_sha, load_tables

    # torch defaults to one thread per HOST core: 24 threads in a 4-6 CPU container
    # throttle each other to a crawl (measured: 300 TiDE steps > 20 min on celestrial).
    torch.set_num_threads(threads)
    frac = float(os.environ.get("MLBENCH_NF_VRAM_FRACTION", VRAM_FRACTION))
    if torch.cuda.is_available() and 0 < frac < 1:
        # hard cap instead of a convention: the other process on this GPU is production
        torch.cuda.set_per_process_memory_fraction(frac)
        total = torch.cuda.get_device_properties(0).total_memory / 1e9
        log(f"VRAM cap {frac:.2f} x {total:.1f}GB = {frac * total:.1f}GB")
    con = connect(memory, threads)
    load_tables(con, export)
    first, last = default_origins(con, export)
    if origin_from:
        first = max(first, dt.date.fromisoformat(origin_from))
    if origin_to:
        last = min(last, dt.date.fromisoformat(origin_to))
    day0 = con.execute("SELECT min(date) FROM truth").fetchone()[0]
    out_dir = cache / name
    out_dir.mkdir(parents=True, exist_ok=True)
    hours = [6] + (INTRADAY_HOURS if intraday else [])
    # Provenance of the CACHE, not just of the scoring run: a cache built from a dirty or
    # pre-rebase tree makes every number scored from it incomparable, and the cache
    # outlives the container that wrote it. `git_sha` must be a real commit that contains
    # the harness's plug-in coverage-parity fixes - see docs/ml/ml-bench.md.
    # A resumed run keeps the blocks and the PROVENANCE of the earlier passes: a cache
    # whose blocks come from two different code versions is exactly the hazard here, and
    # it must be visible in the file rather than inferred from timestamps.
    prior = {}
    mf0 = out_dir / "meta.json"
    if mf0.exists():
        try:
            prior = json.loads(mf0.read_text())
        except ValueError:
            prior = {}
    meta = {"model": name, "spec": SPECS[name], "max_steps": max_steps, "input_days": INPUT_DAYS,
            "horizon_days": HORIZON_DAYS, "K": K, "hour0": HOUR0, "quantiles": QUANTILES,
            "parks": parks, "export": str(export), "git_sha": git_sha(),
            "code_sha256": code_hash(), "image_id": os.environ.get("MLBENCH_IMAGE_ID", "unknown"),
            "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "blocks": list(prior.get("blocks", [])),
            "earlier_passes": list(prior.get("earlier_passes", []))
            + ([{k: prior.get(k) for k in ("git_sha", "code_sha256", "image_id",
                                           "started_utc", "max_steps")}] if prior else [])}
    log(f"provenance: git_sha={meta['git_sha']} image={meta['image_id'][:19]} "
        f"earlier_passes={len(meta['earlier_passes'])}")
    mf0.write_text(json.dumps(meta, indent=2, default=str))
    est = block_estimate
    for c0, c1 in month_blocks(first, last):
        tag = c0.strftime("%Y%m%d")
        if all((out_dir / f"b{tag}_h{H:02d}.parquet").exists() for H in hours):
            log(f"block {c0}..{c1}: cached, skip")
            continue
        # checked per block, not once at startup: the run is resumable, so stopping
        # here only costs the blocks that are left (cached blocks are kept).
        reason = quiet_block_reason(quiet, est, None)
        if reason:
            log(f"stop before block {c0}: {reason} (resume after the window, cached blocks are kept)")
            meta["stopped_for_quiet_window"] = {"before_block": str(c0), "reason": reason}
            break
        tb = time.monotonic()
        panel = build_panel(con, day0, c1 + dt.timedelta(days=HORIZON_DAYS + 1), c0,
                            SPECS[name]["weather"], parks)
        t_panel = time.monotonic() - tb
        log(f"block {c0}..{c1}: {len(panel.aids)} series x {panel.T} steps ({t_panel:.0f}s panel)")
        stats: dict = {}
        panel_mb = round(panel.temporal.nbytes / 1024 ** 2, 1)
        res = forecast_block(name, panel, c0, c1, max_steps, hours, scale, log, con=con, stats=stats)
        del panel
        for H, df in res.items():
            df.sort_values(["origin_date", "aid"]).to_parquet(
                out_dir / f"b{tag}_h{H:02d}.parquet", index=False, row_group_size=200_000)
        meta["blocks"].append({"from": str(c0), "to": str(c1),
                               "seconds": round(time.monotonic() - tb, 1),
                               "panel_seconds": round(t_panel, 1), "panel_mb": panel_mb,
                               "rows": {H: len(df) for H, df in res.items()}, **stats})
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str))
        # the quiet guard projects the next block from the longest one measured here
        est = max(b["seconds"] for b in meta["blocks"]) * 1.25
    return meta


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mlbench.models.nf_precompute")
    p.add_argument("--export", required=True)
    p.add_argument("--cache", required=True)
    p.add_argument("--model", required=True, choices=sorted(SPECS))
    p.add_argument("--from", dest="origin_from")
    p.add_argument("--to", dest="origin_to")
    p.add_argument("--max-steps", type=int, default=2000)
    p.add_argument("--parks", default=None, help="comma-separated park ids (subset run)")
    p.add_argument("--no-intraday", action="store_true")
    p.add_argument("--memory", default="3GB", help="DuckDB memory limit; the panel and the "
                   "predict windows live on top of it inside the 8 GB container cap")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--quiet", default="00:30-09:30", help="UTC window in which no new block starts ('' = off)")
    p.add_argument("--block-estimate", type=float, default=DEFAULT_BLOCK_ESTIMATE,
                   help="seconds a block is assumed to take before one has been measured; "
                        "the quiet guard refuses a block whose projected finish falls in the window")
    a = p.parse_args(argv)
    run(Path(a.export), Path(a.cache), a.model, a.origin_from, a.origin_to, a.max_steps,
        a.parks.split(",") if a.parks else None, not a.no_intraday, memory=a.memory, threads=a.threads,
        quiet=a.quiet or None, block_estimate=a.block_estimate,
        log=lambda s: print(f"[{dt.datetime.now(dt.timezone.utc):%H:%M:%S}] {s}", flush=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
