"""NeuralForecast plug-ins (PAR-829): panel grid, information cut of the precompute,
hourly -> 15-min composition, and the plug-in path through the runner."""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("neuralforecast")

C0, C1 = dt.date(2026, 4, 1), dt.date(2026, 4, 4)
ORIGIN = dt.date(2026, 4, 3)


@pytest.fixture()
def con(synth_export):
    from mlbench.build import connect
    from mlbench.runner import load_tables

    c = connect("1GB", 2)
    load_tables(c, synth_export, materialize=True)
    return c


def _forecast(con, hours=(6, 12)):
    import os

    from mlbench.models.nf_panel import build_panel
    from mlbench.models.nf_precompute import HORIZON_DAYS, forecast_block

    os.environ["MLBENCH_NF_CPU"] = "1"
    day0 = con.execute("SELECT min(date) FROM truth").fetchone()[0]
    panel = build_panel(con, day0, C1 + dt.timedelta(days=HORIZON_DAYS + 1), C0, weather=False)
    return panel, forecast_block("nf_tide", panel, C0, C1, max_steps=3, hours=list(hours), scale=0.05,
                                 log=lambda s: None, con=con)


def test_panel_grid(con):
    from mlbench.models.nf_panel import HOUR0, K, build_panel

    day0 = con.execute("SELECT min(date) FROM truth").fetchone()[0]
    p = build_panel(con, day0, C1, C0, weather=False)
    assert p.temporal.shape == (len(p.aids), p.n_days * K, len(p.cols))
    # every truth hour is on the grid and available; nothing outside the window is
    t = con.execute(f"""SELECT aid, date, ws // 4 hr, avg(y) y FROM truth
                        WHERE date < DATE '{C1}' GROUP BY ALL""").df()
    row = t.iloc[len(t) // 2]
    i, ti = p.aids.index(row["aid"]), p.idx(pd.Timestamp(row["date"]).date(), int(row["hr"]) - HOUR0)
    assert p.mask[i, ti] == 1 and abs(p.temporal[i, ti, 0] - row["y"]) < 1e-3
    assert p.mask.sum() == len(t)
    inwin = p.temporal[:, :, p.cols.index("in_win")]
    assert (p.mask[inwin == 0] == 0).all()
    # closed hours stay on the grid: every day has K steps, the night is masked, not dropped
    assert (p.mask.reshape(len(p.aids), p.n_days, K)[:, :, 0] == 0).all()   # 08:00, park opens 10:00


def test_schedule_holiday_flags_are_not_consumed():
    """S11 (baseline review): ``sched_is_holiday`` / ``sched_is_bridge_day`` are built in
    ``build.py`` with no ``updated_us`` filter but masked by the OPERATING row's
    ``updated_utc``, so an operator annotation written after the origin can pass through
    unmasked. The export carries no ``schedule`` table, so the exposure is not even
    measurable here -- the flags are therefore kept out of the model's inputs entirely.
    This test is the lock: re-adding them must be a deliberate, visible change."""
    from mlbench.models.nf_panel import BASE_FUTR, WEATHER_FUTR

    leaky = {"sched_is_holiday", "sched_is_bridge_day"}
    assert not leaky & set(BASE_FUTR + WEATHER_FUTR), (
        "schedule-derived holiday flags are back in the futr exog; S11 says they are "
        "only safe once build.py filters them by updated_us")
    # the holiday signal is still present, from the genuine calendar
    assert "is_holiday_primary" in BASE_FUTR and "is_school_holiday_any" in BASE_FUTR


def test_precompute_information_cut(con):
    """Poison every truth slot after the origin: the origin's forecasts must not move."""
    _, base = _forecast(con)
    # positive control first: poisoning BEFORE the origin must move them (test is not vacuous)
    con.execute("CREATE TABLE truth_keep AS SELECT * FROM truth")
    con.execute(f"UPDATE truth SET y = y + 300 WHERE date = DATE '{ORIGIN}' - 1")
    _, moved = _forecast(con)
    con.execute("DROP TABLE truth; CREATE TABLE truth AS SELECT * FROM truth_keep")
    # daily origin (06:00): everything from the origin's service day on is future;
    # intraday origin 12:00: the hours of that day that end after 12:00 too
    con.execute(f"""UPDATE truth SET y = y * 7 + 400
                    WHERE date > DATE '{ORIGIN}' OR (date = DATE '{ORIGIN}' AND ws >= 48)""")
    # schedules written after the origin: a later opening, a new holiday flag
    o_utc = f"timezone((SELECT timezone FROM parks WHERE id = w.park_id), TIMESTAMP '{ORIGIN} 06:00')"
    late = f"FROM windows w WHERE w.date >= DATE '{ORIGIN}' AND w.updated_utc >= {o_utc}"
    assert con.execute(f"SELECT count(*) {late}").fetchone()[0] > 0
    con.execute(f"""UPDATE park_day_cov c SET open_local = open_local + INTERVAL 2 HOUR, sched_is_holiday = true
                    WHERE (c.park_id, c.date) IN (SELECT (w.park_id, w.date) {late})""")
    con.execute(f"""UPDATE windows w SET open_local = open_local + INTERVAL 2 HOUR,
                    open_utc = open_utc + INTERVAL 2 HOUR WHERE w.date >= DATE '{ORIGIN}' AND w.updated_utc >= {o_utc}""")
    _, poisoned = _forecast(con, hours=(12,))
    con.execute(f"UPDATE truth SET y = y * 3 + 100 WHERE date = DATE '{ORIGIN}'")
    _, poisoned_d = _forecast(con, hours=(6,))

    def at(d, H):
        x = d[H]
        x = x[x["origin_date"] == pd.Timestamp(ORIGIN)].sort_values(["aid", "date", "hr"])
        return x.reset_index(drop=True)

    assert len(at(base, 6)) > 0 and len(at(base, 12)) > 0
    pd.testing.assert_frame_equal(at(base, 6), at(poisoned_d, 6))
    pd.testing.assert_frame_equal(at(base, 12), at(poisoned, 12))
    assert not np.allclose(at(base, 6)["q50"], at(moved, 6)["q50"])
    # the intraday forecast covers only the rest of the origin's day, from 12:00
    x = at(base, 12)
    assert (x["date"] == pd.Timestamp(ORIGIN)).all() and x["hr"].min() >= 12


def test_hourly_to_slots_keeps_the_hour_mean():
    from mlbench.models.neuralforecast_models import hourly_to_slots

    g = pd.DataFrame({"attraction_id": "a", "date": dt.date(2026, 4, 1),
                      "slot_start_utc": pd.date_range("2026-04-01 08:00", periods=8, freq="15min", tz="UTC"),
                      "ws": np.arange(40, 48)})
    hourly = pd.DataFrame({"aid": "a", "date": [dt.date(2026, 4, 1)] * 2, "hr": [10, 11],
                           "q50": [20.0, 40.0], "q80": [30.0, 50.0], "q95": [40.0, 60.0]})
    h5 = pd.Series([5, 10, 15, 30, np.nan, 40, 40, 40], dtype=float)
    s = hourly_to_slots(g, hourly, h5)
    assert np.isclose(s["q50"].iloc[:4].mean(), 20.0)
    assert s["q50"].iloc[0] < s["q50"].iloc[3]                 # the ramp survives
    assert np.allclose(s["q50"].iloc[4:], 40.0)                # missing H5 -> flat hour


def test_plugin_through_runner(synth_export, tmp_path, monkeypatch):
    from mlbench.config import BenchConfig
    from mlbench.models.nf_precompute import run
    from mlbench.runner import Runner

    monkeypatch.setenv("MLBENCH_NF_CPU", "1")
    cache = tmp_path / "cache"
    run(synth_export, cache, "nf_tide", str(C0), str(C1), max_steps=3, scale=0.05, memory="1GB",
        threads=2, log=lambda s: None)
    monkeypatch.setenv("MLBENCH_NF_CACHE", str(cache))
    from mlbench.models.neuralforecast_models import NFTiDE

    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    Runner(synth_export, out, cfg, [NFTiDE()], origins=(str(C0), str(C1))).run()
    slot = pd.concat(pd.read_parquet(f) for f in (out / "parts" / "slot").glob("*.parquet"))
    assert slot["n__nf_tide"].sum() > 0 and slot["qn__nf_tide"].sum() > 0
    lv = pd.concat(pd.read_parquet(f) for f in (out / "parts" / "levels").glob("*.parquet"))
    assert lv["lvl_nf_tide"].notna().any()


@pytest.mark.parametrize("name", ["nf_tide", "nf_nhits", "nf_tsmixerx"])
def test_series_layout(con, name, monkeypatch):
    """Forecast rows land on the right ride: the robust scaler alone carries each ride's
    level, so after a few dozen steps any net must rank headliners (~60) above the rest (~25).
    Guards the [series, window, step] unpacking, incl. the multivariate (TSMixerx) layout."""
    from mlbench.models.nf_panel import build_panel
    from mlbench.models.nf_precompute import HORIZON_DAYS, forecast_block

    monkeypatch.setenv("MLBENCH_NF_CPU", "1")
    day0 = con.execute("SELECT min(date) FROM truth").fetchone()[0]
    panel = build_panel(con, day0, C1 + dt.timedelta(days=HORIZON_DAYS + 1), C0, weather=False)
    res = forecast_block(name, panel, C0, C1, max_steps=60, hours=[6], scale=0.05, log=lambda s: None,
                         con=con)[6]
    m = res.groupby("aid")["q50"].median()
    hl = m[m.index.str.endswith(("r0", "r1"))]
    rest = m[m.index.str.endswith(("r2", "r3"))]
    assert len(hl) == 4 and len(rest) == 4 and hl.min() > rest.max()
    assert (res["q50"] <= res["q80"]).all() and (res["q80"] <= res["q95"]).all()


def test_quiet_window_guard():
    """The guard protects celestrial's nightly jobs, so test the gap that matters:
    a block that STARTS before the window but would finish inside it."""
    from mlbench.models.nf_precompute import in_quiet_window, quiet_block_reason

    def at(h, m):
        return dt.datetime(2026, 10, 9, h, m, tzinfo=dt.timezone.utc)

    q = "00:30-09:30"
    # the window itself (wraps midnight)
    assert in_quiet_window(q, at(0, 30)) and in_quiet_window(q, at(3, 0))
    assert in_quiet_window(q, at(9, 29)) and not in_quiet_window(q, at(9, 30))
    assert not in_quiet_window(q, at(23, 0)) and not in_quiet_window(q, at(0, 29))
    assert not in_quiet_window("", at(3, 0))                      # disabled

    # a 30-min block: fine at 23:00, refused at 00:29 because it would run to 00:59
    assert quiet_block_reason(q, 1800, at(23, 0)) is None
    assert quiet_block_reason(q, 1800, at(0, 29)) is not None
    assert quiet_block_reason(q, 1800, at(0, 10)) is not None
    # inside the window nothing starts, whatever the estimate
    assert quiet_block_reason(q, 60, at(3, 0)) is not None
    # right after the window there is a whole day of headroom
    assert quiet_block_reason(q, 3 * 3600, at(9, 30)) is None
    # ... but a 20-hour block is refused even then: it would cross the next window
    assert quiet_block_reason(q, 20 * 3600, at(9, 30)) is not None
    assert quiet_block_reason("", 20 * 3600, at(3, 0)) is None    # disabled


def test_window_chunk_scales_with_series():
    """The predict array is [chunk, series, L+h, cols] float32: more series -> fewer
    origins per call, so the same settings hold for a subset and for all parks."""
    from mlbench.models.nf_precompute import (
        HORIZON_DAYS,
        INPUT_DAYS,
        K,
        WINDOW_ARRAY_BUDGET,
        window_chunk,
    )

    cols, steps = 22, (INPUT_DAYS + HORIZON_DAYS) * K
    for n_series in (268, 737, 3601):
        c = window_chunk(n_series, cols)
        assert c >= 1
        assert c * n_series * steps * cols * 4 <= max(WINDOW_ARRAY_BUDGET,
                                                      n_series * steps * cols * 4)
    assert window_chunk(268, cols) > window_chunk(3601, cols)
    assert window_chunk(10 ** 7, cols) == 1                       # never zero


def test_predict_daily_never_serves_a_previous_origin(tmp_path, monkeypatch):
    """``predict_daily`` reads the curve ``predict`` built for the SAME origin.

    Two consecutive daily origins overlap in d1..d6, so if the cache has no rows for
    an origin (outside a precomputed block) and the previous origin's curve survived,
    the runner would score a forecast made a day earlier as this origin's d1 — an
    honest-looking but wrongly labelled lead. The curve is therefore dropped at the
    start of every daily ``predict``.
    """
    from mlbench.models.base import HistoryView, Origin
    from mlbench.models.neuralforecast_models import NFTiDE

    d0, d1 = dt.date(2026, 5, 10), dt.date(2026, 5, 11)
    cache = tmp_path / "c" / "nf_tide"
    cache.mkdir(parents=True)
    hrs = range(10, 19)
    pd.DataFrame({"aid": "a", "origin_date": pd.Timestamp(d0), "origin_hour": np.int8(6),
                  "date": pd.Timestamp(d0 + dt.timedelta(days=1)), "hr": list(hrs),
                  "q50": 20.0, "q80": 30.0, "q95": 40.0}).to_parquet(cache / "b20260501_h06.parquet")
    monkeypatch.setenv("MLBENCH_NF_CACHE", str(tmp_path / "c"))
    m = NFTiDE()

    def grid(day):
        n = 4 * len(hrs)
        return pd.DataFrame({
            "attraction_id": "a", "park_id": "p", "date": day,
            "slot_start_utc": pd.date_range(f"{day} 10:00", periods=n, freq="15min", tz="UTC"),
            "ws": np.arange(40, 40 + n), "ko": np.arange(n), "kc": np.arange(n)[::-1],
            "lead_days": 1})

    hist = pd.DataFrame({"attraction_id": "a", "date": [d0] * 4, "ws": [40, 41, 42, 43],
                         "ko": [0, 1, 2, 3], "kc": [3, 2, 1, 0], "y": [10.0, 20.0, 30.0, 40.0]})
    days = pd.DataFrame({"attraction_id": ["a"], "park_id": ["p"],
                         "date": [d0 + dt.timedelta(days=1)], "lead_days": [1]})

    def origin(d):
        return Origin(date=d, kind="daily", hour_local=6, origin_utc=pd.DataFrame(),
                      history=HistoryView(hist, None, 56))

    # origin d0 is in the cache: a curve and a daily level
    assert not m.predict(origin(d0), grid(d0 + dt.timedelta(days=1)), pd.DataFrame()).empty
    assert m.predict_daily(origin(d0), days, pd.DataFrame()) is not None
    # origin d1 is not: no curve, and no daily level inherited from d0
    assert m.predict(origin(d1), grid(d1 + dt.timedelta(days=1)), pd.DataFrame()).empty
    assert m.predict_daily(origin(d1), days, pd.DataFrame()) is None


def test_train_step_size_reproduces_the_measured_oom_allocation():
    """Calibration lock for the fix to the 2026-10-10 OOM.

    NeuralForecast materialises the WHOLE unfolded training window pool before
    ``windows_batch_size`` samples from it, so the peak is independent of
    ``windows_batch_size``.  With the probe's settings the formula must reproduce the
    allocation the OOM actually reported (14.02 GiB), which is what makes the budget
    below a measurement rather than a guess.
    """
    import math

    from mlbench.models.nf_precompute import (
        HORIZON_DAYS,
        INPUT_DAYS,
        K,
        SPECS,
        train_step_size,
    )

    lh = (INPUT_DAYS + HORIZON_DAYS) * K
    t_train, cols = 4536, 20                      # the 2026-09-01..09-03 probe block
    batch = SPECS["nf_tide"]["kw"]["batch_size"]
    unthinned = batch * t_train * lh * cols * 4
    assert round(unthinned / 1024 ** 3, 2) == 14.02, unthinned

    budget = 1024 ** 3
    step, facts = train_step_size("nf_tide", 3364, cols, t_train, budget)
    assert facts["pool_bytes"] <= budget                      # the pool now fits
    assert facts["pool_bytes"] * step >= unthinned / 2        # and is not absurdly small
    assert math.gcd(step, K) == 1, "window starts must cycle through every hour phase"
    assert facts["pool_windows_per_series"] == math.ceil(t_train / step)

    # Monotone in the budget, and never degenerate.
    assert train_step_size("nf_tide", 3364, cols, t_train, budget * 8)[0] <= step
    assert train_step_size("nf_tide", 3364, cols, t_train, 1)[0] >= 1
    assert train_step_size("nf_tide", 3364, cols, 1, budget)[0] == 1


def test_train_step_size_scales_the_multivariate_pool_by_series():
    """TSMixerx is multivariate: the pool keeps every ride of the group as a channel,
    so its budget scales with the group's series count, not with batch_size."""
    from mlbench.models.nf_precompute import train_step_size

    small = train_step_size("nf_tsmixerx", 200, 20, 4536)[0]
    large = train_step_size("nf_tsmixerx", 2000, 20, 4536)[0]
    assert large > small


def test_make_model_thins_training_windows_but_not_the_forecast():
    """``step_size`` must reach the model as a TRAIN-only knob: predict() uses
    ``predict_step_size``, which forecast_block sets via predict(step_size=h), so a
    thinned training pool cannot change the forecast layout."""
    import inspect

    from mlbench.models.nf_precompute import HORIZON_DAYS, K, make_model

    class _P:
        futr_cols = ["f0"]
        static_cols = ["s0"]
        cols = ["y"] + [f"c{i}" for i in range(18)] + ["available_mask"]

    m = make_model("nf_tide", _P(), max_steps=1, n_series=None, scale=0.05, t_train=4536)
    assert m.step_size > 1, "the training pool must be thinned"

    # The forecast side is untouched, and the reason is stronger than "it is set to 1":
    # the constructor never creates `predict_step_size` at all. It comes into existence
    # only inside predict(), assigned from predict()'s own argument -- which is what
    # forecast_block passes as step_size=h. So no constructor value can reach it.
    assert not hasattr(m, "predict_step_size")
    m.set_test_size(HORIZON_DAYS * K)
    assert not hasattr(m, "predict_step_size")
    sig = inspect.signature(type(m).predict).parameters["step_size"]
    assert sig.default == 1
    src = inspect.getsource(type(m).predict)
    assert "self.predict_step_size = step_size" in src, src

    # And the unthinned model still differs only in that one knob.
    plain = make_model("nf_tide", _P(), max_steps=1, n_series=None, scale=0.05)
    assert plain.step_size == 1
    assert plain.input_size == m.input_size and plain.h == m.h


def test_cache_write_is_atomic_and_the_block_resume_is_honest(synth_export, tmp_path,
                                                              monkeypatch):
    """A killed precompute must never leave a block that LOOKS complete.

    `run()` treats a block as finished when all of its hour files exist, so the write has
    to be temp-then-rename: otherwise a kill during the last file leaves all paths present
    with one truncated and a later pass accepts it. Checks the contract from both ends --
    nothing partial survives a success, and a missing hour file really does re-run the
    block rather than being skipped.
    """
    monkeypatch.setenv("MLBENCH_NF_CPU", "1")
    from mlbench.models.nf_precompute import run

    cache = tmp_path / "cache"
    kw = dict(export=synth_export, cache=cache, name="nf_tide", origin_from="2026-04-01",
              origin_to="2026-04-02", max_steps=2, scale=0.05, memory="1GB", threads=2,
              log=lambda s: None)
    meta = run(**kw)
    out = cache / "nf_tide"
    hours = [6, 8, 10, 12, 14, 16, 18, 20]
    assert len(meta["blocks"]) == 1
    assert not list(out.glob("*.tmp")), "no partial file may survive a successful block"
    assert not list(out.glob(".*tmp")), "no hidden partial file either"
    for h in hours:
        assert (out / f"b20260401_h{h:02d}.parquet").exists()

    # A stale temp file from an earlier kill must NOT make the block look cached, and
    # must not be mistaken for output.
    (out / ".b20260401_h06.parquet.tmp").write_bytes(b"truncated garbage")
    (out / "b20260401_h20.parquet").unlink()
    meta2 = run(**kw)
    assert len(meta2["blocks"]) == 1, "the block must be recomputed, not skipped"
    assert (out / "b20260401_h20.parquet").exists()
    assert meta2["earlier_passes"], "a resumed pass must record the earlier pass's provenance"

    # Every file the run calls final must be a readable parquet with the rows it claims.
    import pyarrow.parquet as pq
    for h in hours:
        f = out / f"b20260401_h{h:02d}.parquet"
        assert pq.ParquetFile(f).metadata.num_rows == meta2["blocks"][0]["rows"][h]


def test_a_lone_available_step_cannot_form_a_training_window():
    """Why MIN_TRAIN_STEPS exists, as arithmetic rather than a comment.

    neuralforecast keeps a training window only if it has at least one available step in
    the insample AND at least one in the horizon (both minima are 1 because
    `training_data_availability_threshold` defaults to 0.0). A series with a single
    available step therefore yields NO usable window -- the one point cannot be in both
    halves of the same window -- and if a whole batch is that sparse the library raises
    "No windows available for training" and the block dies. Two points far enough apart
    are sufficient, which is why the threshold is a count and not a span.
    """
    from mlbench.models.nf_precompute import HORIZON_DAYS, INPUT_DAYS, K, MIN_TRAIN_STEPS

    L, h = INPUT_DAYS * K, HORIZON_DAYS * K

    def usable(points: set[int], span: int) -> bool:
        return any(any(k <= r < k + L for r in points) and any(k + L <= q < k + L + h
                                                               for q in points)
                   for k in range(0, span))

    span = 3000
    for p in range(0, span, 137):                     # a lone point: never usable
        assert not usable({p}, span)
    # two points separated by at least L and less than L + h: always usable
    for p in range(0, span - L - h, 211):
        assert usable({p, p + L}, span)
        assert usable({p, p + L + h - 1}, span)
    # and the guard is at least 2, so the degenerate case cannot reach the fit
    assert MIN_TRAIN_STEPS >= 2
