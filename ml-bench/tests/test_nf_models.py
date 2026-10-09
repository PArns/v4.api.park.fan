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
