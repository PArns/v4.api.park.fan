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
timings, peak VRAM per block).
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


def forecast_block(name: str, panel: Panel, c0: dt.date, c_last: dt.date, max_steps: int,
                   hours: list[int], scale: float = 1.0, log=print) -> dict[int, pd.DataFrame]:
    """Train on steps < c0, forecast every origin c0..c_last. Returns {origin_hour: rows}."""
    import torch

    spec = SPECS[name]
    n_win = (c_last - c0).days + 1
    first = panel.idx(c0)
    assert panel.T == first + K * (n_win + HORIZON_DAYS), "panel must end at c_last + 8 days"
    test_size = panel.T - first                     # every step from c0 on is held back
    h = HORIZON_DAYS * K
    groups = (sorted(set(panel.groups)) if spec.get("multivariate") else ["all"])
    mask = panel.mask
    csum = np.concatenate([np.zeros((mask.shape[0], 1), np.float32), np.cumsum(mask, axis=1)], axis=1)
    inwin = panel.temporal[:, :, panel.cols.index("in_win")] > 0
    out: dict[int, list[pd.DataFrame]] = {H: [] for H in hours}
    for g in groups:
        rows = (np.arange(len(panel.aids)) if g == "all"
                else np.flatnonzero(np.array(panel.groups) == g))
        if len(rows) == 0:
            continue
        t0 = time.monotonic()
        ds = to_dataset(panel, rows)
        model = make_model(name, panel, max_steps, len(rows), scale)
        model.fit(dataset=ds, val_size=0, test_size=test_size)
        t_fit = time.monotonic() - t0
        for H in hours:
            s = max(0, H - HOUR0)                    # first forecast step inside day c
            model.set_test_size(test_size - s)
            fc = model.predict(dataset=ds, step_size=K)
            fc = np.asarray(fc, dtype=np.float32)
            nw = fc.shape[0] // (len(rows) * h)
            fc = fc.reshape(len(rows), nw, h, -1)[:, :n_win]
            fc = np.sort(fc, axis=-1)                # MQLoss may cross: enforce q50 <= q80 <= q95
            keep_steps = h if H == 6 else K - s      # intraday: rest of day c only
            starts = first + s + K * np.arange(n_win)                     # [W]
            tt = starts[None, :, None] + np.arange(keep_steps)[None, None, :]   # [1,W,S]
            ins = csum[rows][:, starts] - csum[rows][:, np.maximum(starts - INPUT_DAYS * K, 0)]
            ok = inwin[rows][:, tt[0]] & (ins[:, :, None] >= MIN_INSAMPLE_HOURS)
            si, wi, ki = np.nonzero(ok)
            t_abs = tt[0][wi, ki]
            df = pd.DataFrame({
                "aid": np.array(panel.aids)[rows][si],
                "origin_date": (np.datetime64(c0, "D") + wi.astype("timedelta64[D]")),
                "origin_hour": np.int8(H),
                "date": np.datetime64(panel.day0, "D") + (t_abs // K).astype("timedelta64[D]"),
                "hr": (HOUR0 + t_abs % K).astype(np.int8),
                "q50": np.maximum(fc[si, wi, ki, 0], 0), "q80": np.maximum(fc[si, wi, ki, 1], 0),
                "q95": np.maximum(fc[si, wi, ki, 2], 0)})
            out[H].append(df)
        vram = torch.cuda.max_memory_allocated() / 1e9 if torch.cuda.is_available() else 0.0
        log(f"  {name} group={g} series={len(rows)} fit={t_fit:.0f}s "
            f"total={time.monotonic() - t0:.0f}s peak_vram={vram:.2f}GB")
        del model, ds
    return {H: pd.concat(v, ignore_index=True) if v else pd.DataFrame() for H, v in out.items()}


def in_quiet_window(spec: str | None, now: dt.datetime | None = None) -> bool:
    """``spec`` = 'HH:MM-HH:MM' UTC (celestrial: no new block from 00:30, nightly jobs 01:00-09:30)."""
    if not spec:
        return False
    a, b = (dt.time.fromisoformat(v) for v in spec.split("-"))
    t = (now or dt.datetime.now(dt.timezone.utc)).time()
    return (a <= t < b) if a < b else (t >= a or t < b)


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
        memory: str = "4GB", threads: int = 4, log=print, quiet: str | None = None) -> dict:
    from ..build import connect
    from ..runner import load_tables

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
    meta = {"model": name, "spec": SPECS[name], "max_steps": max_steps, "input_days": INPUT_DAYS,
            "horizon_days": HORIZON_DAYS, "K": K, "hour0": HOUR0, "quantiles": QUANTILES,
            "parks": parks, "export": str(export), "blocks": []}
    for c0, c1 in month_blocks(first, last):
        if in_quiet_window(quiet):
            log(f"quiet window {quiet} UTC reached: stop before block {c0} (resume later, cached blocks are kept)")
            break
        tag = c0.strftime("%Y%m%d")
        if all((out_dir / f"b{tag}_h{H:02d}.parquet").exists() for H in hours):
            log(f"block {c0}..{c1}: cached, skip")
            continue
        tb = time.monotonic()
        panel = build_panel(con, day0, c1 + dt.timedelta(days=HORIZON_DAYS + 1), c0,
                            SPECS[name]["weather"], parks)
        t_panel = time.monotonic() - tb
        log(f"block {c0}..{c1}: {len(panel.aids)} series x {panel.T} steps ({t_panel:.0f}s panel)")
        res = forecast_block(name, panel, c0, c1, max_steps, hours, scale, log)
        del panel
        for H, df in res.items():
            df.sort_values(["origin_date", "aid"]).to_parquet(
                out_dir / f"b{tag}_h{H:02d}.parquet", index=False, row_group_size=200_000)
        meta["blocks"].append({"from": str(c0), "to": str(c1), "seconds": round(time.monotonic() - tb, 1),
                               "rows": {H: len(df) for H, df in res.items()}})
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str))
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
    p.add_argument("--memory", default="4GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--quiet", default="00:30-09:30", help="UTC window in which no new block starts ('' = off)")
    a = p.parse_args(argv)
    run(Path(a.export), Path(a.cache), a.model, a.origin_from, a.origin_to, a.max_steps,
        a.parks.split(",") if a.parks else None, not a.no_intraday, memory=a.memory, threads=a.threads, quiet=a.quiet or None,
        log=lambda s: print(f"[{dt.datetime.now(dt.timezone.utc):%H:%M:%S}] {s}", flush=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
