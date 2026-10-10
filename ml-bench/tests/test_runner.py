"""Runner: strict information cut (baselines AND every registered plug-in), plug-in path ==
built-in path, provenance, report end to end."""

import argparse
import datetime as dt
import json

import duckdb
import numpy as np
import pandas as pd
import pytest

from mlbench import baselines as B
from mlbench.config import BenchConfig
from mlbench.models import REGISTRY, LevelH5Example
from mlbench.models.base import HistoryView
from mlbench.report import build_report
from mlbench.runner import Runner

PRED_COLS = ["snaive7", "wt_med", "wt_q80", "wt_q95", "clim", "h5", "lvlh5_naive", "lvlh5_tft",
             "lvlh5_cbd", "prod_served", "prod_served_lin", "busy", "ko", "kc", "sk"]
C = dt.date(2026, 4, 20)


def _cfg():
    return BenchConfig(memory_limit="1GB", threads=2)


def _setup(r: Runner, c: dt.date) -> pd.DataFrame:
    B.snapshot_tables(r.con, c.replace(day=1).isoformat(), r.cfg)
    B.window_tables(r.con, c.isoformat(), r.cfg)
    B.target_tables(r.con, c.isoformat(), r.slot_leads(c), r.cfg)
    return r.con.execute(f"SELECT aid, slot_utc, L, {', '.join(PRED_COLS)} FROM tg ORDER BY aid, slot_utc").df()


def _poison(r: Runner, c: dt.date, weather: bool = True) -> None:
    """Change everything a forecast made at ``c`` must not know.

    ``weather=False`` leaves the weather columns alone, because an ORACLE-weather
    model (``uses_oracle_weather``, scored ``<name>_owx``) is ALLOWED to see the
    target day's actuals — the test has to be able to say "oracle weather yes,
    post-origin truth no" instead of failing every honest `_owx` model (critic S10).
    """
    x = r.con.execute
    cut = "(SELECT o.origin_utc FROM o WHERE o.park_id = {t}.park_id)"
    x(f"UPDATE truth SET y = y * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE > {cut.format(t='truth')}")
    x(f"UPDATE slots SET wait = wait * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE > {cut.format(t='slots')}")
    x(f"UPDATE ride_day SET p90 = p90 * 10, peak_h90 = peak_h90 * 10 WHERE date >= DATE '{c}'")
    x(f"UPDATE hour_stats SET p50 = p50 * 10, p90 = p90 * 10 WHERE date >= DATE '{c}'")
    x("UPDATE tft SET peak = peak * 10 WHERE created_utc >= (SELECT min(origin_utc) FROM o)")
    x("UPDATE cbd SET peak = peak * 10 WHERE created_utc >= (SELECT min(origin_utc) FROM o)")
    # a schedule written after the origin moves: nothing may follow it. Both ends of
    # the window and the publication stamp itself, because a late CLOSING time leaks
    # through kc / the close-aligned H5 hour / prod_served's hours just as a late
    # opening leaks through ko.
    x(f"""UPDATE windows SET open_utc = open_utc + INTERVAL 2 HOUR,
              open_local = open_local + INTERVAL 2 HOUR, close_utc = close_utc - INTERVAL 2 HOUR,
              close_local = close_local - INTERVAL 2 HOUR, n_windows = n_windows + 1,
              updated_utc = updated_utc + INTERVAL 1 HOUR
          WHERE updated_utc >= {cut.format(t='windows')}""")
    # every covariate a plug-in can read, not only two of them
    cov = ("sched_is_holiday = NOT coalesce(sched_is_holiday, false), "
           "sched_is_bridge_day = NOT coalesce(sched_is_bridge_day, false)")
    if weather:
        cov += (", temp_max = 99, temp_min = -99, precip_sum = 99, wind_max = 99, weather_code = 99")
    x(f"UPDATE park_day_cov SET {cov} WHERE date >= DATE '{c}'")


def test_information_cut_baselines(synth_export, tmp_path):
    """Poisoning everything at/after the origin must not move any forecast made at it."""
    r = Runner(synth_export, tmp_path / "run", _cfg(), [], materialize=True)
    before = _setup(r, C)
    assert (~before["sk"]).any() and before["sk"].any()      # both schedule regimes are exercised
    _poison(r, C)
    after = _setup(r, C)
    assert len(before) == len(after) > 0
    for col in PRED_COLS:
        a, b = before[col].to_numpy(dtype=float), after[col].to_numpy(dtype=float)
        assert np.allclose(a, b, equal_nan=True), col


def _predict_all(r: Runner, m) -> dict[str, pd.DataFrame]:
    """Every surface a plug-in is scored on: the daily slot path, the daily LEVEL path
    (UC4 / D6 / D7 / the `_x_h5` curve) and the intraday path."""
    out = {}
    out["daily"] = r.plugin_predict(m, C, [0, 1, 3, 7]).sort_values(
        ["aid", "slot_utc"]).reset_index(drop=True)
    lvl = r.plugin_predict_daily(m, C)
    if lvl is not None and len(lvl):
        out["level"] = lvl.sort_values(list(lvl.columns[:2])).reset_index(drop=True)
    for hour in (8, 16):
        r.con.execute(f"CREATE OR REPLACE TEMP TABLE oh AS {B.origin_sql(C.isoformat(), hour)}")
        out[f"intraday{hour}"] = r.plugin_predict(m, C, [0, 1], kind="intraday", hour=hour,
                                                  origin_table="oh").sort_values(
            ["aid", "slot_utc"]).reset_index(drop=True)
    return out


@pytest.mark.parametrize("name", sorted(REGISTRY))
def test_information_cut_plugins(name, synth_export, tmp_path):
    """Every registered model, on every path it is scored on: the same forecasts after
    the future is poisoned.

    Covers the daily slot path, ``predict_daily`` and the intraday origins (the three
    paths the runner actually calls), and poisons both ends of a late-published
    window plus every covariate column — see ``_poison``. An ORACLE-weather model is
    allowed the target day's weather, so for those the weather columns are left alone
    and everything else still has to hold (critic S10).

    Known limit: the poison is in the attached tables (``materialize=True``). A
    plug-in that opens the export's Parquet files itself is not caught here — what
    rules that out is that no path to the export and no connection is reachable from
    anything the model is handed (``test_plugins_get_no_connection``).
    """
    r = Runner(synth_export, tmp_path / "run", _cfg(), [REGISTRY[name]()], materialize=True)
    m = r.models[0]
    _setup(r, C)
    r.maybe_fit(m, C)
    before = _predict_all(r, m)
    _poison(r, C, weather=not r.oracle_weather(m))
    _setup(r, C)
    # the history view is cached per (origin, origin table) — correct in production,
    # where truth cannot change inside one origin, but it has to be dropped here or
    # the poisoned truth would never be read back
    r.clear_caches()
    r.set_models([REGISTRY[name]()])
    m2 = r.models[0]
    r._last_fit.clear()
    r.maybe_fit(m2, C)
    after = _predict_all(r, m2)
    assert len(before["daily"]) > 0
    assert set(before) == set(after)
    for path, b in before.items():
        pd.testing.assert_frame_equal(b, after[path], obj=f"{name} / {path}")


def _reachable_connection(obj, depth: int = 4) -> str | None:
    """Walk attributes, closures, defaults and containers looking for a DuckDB
    connection. ``vars()`` alone is not enough: the old ``HistoryView`` stored a
    closure and the connection sat in ``_fetch.__closure__[0].cell_contents``
    (critic S9)."""
    seen: set[int] = set()

    def walk(o, path: str, d: int):
        if d < 0 or id(o) in seen:
            return None
        seen.add(id(o))
        if isinstance(o, duckdb.DuckDBPyConnection):
            return path
        for attr in ("__closure__", "__defaults__", "__kwdefaults__", "__self__", "__func__",
                     "cell_contents"):
            v = getattr(o, attr, None)
            if v is None:
                continue
            hit = walk(v, f"{path}.{attr}", d - 1)
            if hit:
                return hit
        if isinstance(o, (list, tuple, set, frozenset)):
            for i, v in enumerate(o):
                hit = walk(v, f"{path}[{i}]", d - 1)
                if hit:
                    return hit
        if isinstance(o, dict):
            for k, v in o.items():
                hit = walk(v, f"{path}[{k!r}]", d - 1)
                if hit:
                    return hit
        for k, v in list(vars(o).items()) if hasattr(o, "__dict__") else []:
            hit = walk(v, f"{path}.{k}", d - 1)
            if hit:
                return hit
        return None

    return walk(obj, "origin.history", depth)


def test_plugins_get_no_connection(synth_export, tmp_path):
    r = Runner(synth_export, tmp_path / "run", _cfg(), [], materialize=True)
    _setup(r, C)
    hv = r.history_view(C, "o")
    assert isinstance(hv, HistoryView)
    assert not any(isinstance(v, duckdb.DuckDBPyConnection) for v in vars(hv).values())
    # ... and no connection anywhere in the object graph, not just the top level
    assert _reachable_connection(hv) is None
    assert not hasattr(hv, "_fetch")
    h = hv.df(56)
    o = r.con.execute("SELECT park_id, origin_utc FROM o").df()
    j = h.merge(o, on="park_id")
    assert len(j) > 0
    assert (j["slot_start_utc"] + pd.Timedelta(minutes=15) <= j["origin_utc"]).all()
    # a window longer than the runner materialised is refused, not silently truncated
    with pytest.raises(ValueError):
        hv.df(hv.max_days + 1)
    cov = r.covariates(C, 7, oracle_weather=False)
    assert "temp_max" not in cov.columns and cov["weather_source"].isna().all()
    assert "schedule_known_at_origin" in cov.columns
    assert "temp_max" in r.covariates(C, 7, oracle_weather=True).columns
    # the two schedule-derived flags are cut at THEIR OWN publication time
    assert bool(cov["holiday_flags_cut_at_their_own_publish_time"].iloc[0])
    late = r.con.execute(f"""
        SELECT count(*) FROM park_day_cov cov JOIN o ON o.park_id = cov.park_id
        WHERE cov.date >= DATE '{C}' AND cov.sched_flags_updated_utc >= o.origin_utc
          AND cov.sched_is_holiday""").fetchone()[0]
    assert late > 0, "fixture must contain a day whose holiday flag is published late"
    assert "sched_flags_updated_utc" not in cov.columns
    late_days = r.con.execute(f"""
        SELECT cov.date FROM park_day_cov cov JOIN o ON o.park_id = cov.park_id
        WHERE cov.date >= DATE '{C}' AND cov.sched_flags_updated_utc >= o.origin_utc""").df()["date"]
    masked = cov[cov["date"].isin(set(late_days))]
    assert len(masked) > 0
    assert masked["sched_is_holiday"].isna().all()
    assert masked["sched_is_bridge_day"].isna().all()


def test_oracle_weather_cannot_hide_behind_an_instance_attribute(synth_export, tmp_path):
    """Setting the flag on the INSTANCE must not buy oracle weather, because
    ``scored_name()`` reads the CLASS and the model would keep its honest name
    (critic S8)."""
    class Sneaky(LevelH5Example):
        name = "sneaky"

        def __init__(self):
            super().__init__()
            self.uses_oracle_weather = True

    r = Runner(synth_export, tmp_path / "run", _cfg(), [Sneaky()], materialize=True)
    m = r.models[0]
    assert r.name_of(m) == "sneaky"           # honest name ...
    assert r.oracle_weather(m) is False       # ... and therefore no oracle weather
    _setup(r, C)
    assert "temp_max" not in r.covariates(C, 7, r.oracle_weather(m)).columns

    class Honest(LevelH5Example):
        name = "honest"
        uses_oracle_weather = True

    r2 = Runner(synth_export, tmp_path / "run2", _cfg(), [Honest()], materialize=True)
    assert r2.name_of(r2.models[0]) == "honest_owx"
    assert r2.oracle_weather(r2.models[0]) is True


def test_plugin_example_equals_builtin(synth_export, tmp_path):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    r = Runner(synth_export, out, cfg, [LevelH5Example()], origins=("2026-04-10", "2026-04-12"))
    r.run()
    s = duckdb.connect().execute(f"""SELECT sum(n__lvlh5_naive) a, sum(n__example_level_h5) b,
        sum(sae__lvlh5_naive) c, sum(sae__example_level_h5) d FROM '{out}/parts/slot/*.parquet'""").fetchone()
    assert s[0] == s[1] and s[0] > 0
    assert abs(s[2] - s[3]) < 1e-6


def test_oracle_weather_model_never_wins_a_handover_row(synth_export, tmp_path):
    """An ORACLE-weather model is an upper bound, not a servable configuration. It may
    appear in the value tables but must never win a hand-over row or a usable horizon
    — BENCH-SPEC says oracle weather is "reported separately" and the hand-over table
    is the serving router's input. PAR-828 measured 34 of 280 full-period hand-over
    rows going to `chronos2_owx` before this gate existed.

    Depends on S8: if the weather gate reads the instance while `scored_name()` reads
    the class, an oracle model never gets the `_owx` suffix and this test cannot see
    it — the two fixes only work together.
    """
    from mlbench.report import is_oracle

    class Oracle(LevelH5Example):
        name = "oracle_wx"
        uses_oracle_weather = True

    assert is_oracle("oracle_wx_owx") and is_oracle("oracle_wx_owx_x_h5")
    assert not is_oracle("lvlh5_tft") and not is_oracle("snowx_level")   # no false positives

    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    r = Runner(synth_export, out, cfg, [Oracle()], origins=("2026-04-01", "2026-04-20"))
    assert r.name_of(r.models[0]) == "oracle_wx_owx"
    r.run()
    tdir = build_report(out, reps=50)
    cells = pd.read_csv(tdir / "cells.csv")
    owx = {m for m in cells["model"].unique() if is_oracle(m) and "owx" in str(m)}
    assert owx, "the oracle model must still be scored and visible"
    for name in ("handover", "usable_horizon"):
        t = pd.read_csv(tdir / f"{name}.csv")
        col = "winner" if name == "handover" else "model"
        assert not set(t[col]) & owx, f"{name}.csv awards a row to {owx}"


def test_end_to_end_report(synth_export, tmp_path):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7, 14])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-20")).run()
    tdir = build_report(out, reps=50)
    summary = (out / "summary.md").read_text()
    for section in ("## Horizon", "Hand-over", "## UC1", "## UC3", "## UC4", "## D1", "## D2", "## D3",
                    "## D4", "## D5", "## D6", "## D7", "## D8", "## D9"):
        assert section in summary, section
    curve = pd.read_csv(tdir / "horizon_curve_mae.csv")
    assert {"UC1", "UC2", "UC3"} <= set(curve["uc"])
    # MAE of a model is never pooled across leads: one row per (uc, lead, segment, model)
    assert not curve.duplicated(["uc", "lead", "segment", "model"]).any()
    # pairing: in the first hour the level-scaled curves ARE h5 by construction, so the
    # first-hour scores must coincide exactly (they did not when pairing was per park-day)
    cells = pd.read_csv(tdir / "cells.csv")
    fh = cells[cells["metric"] == "first-hour MAE (opening-aligned)"].pivot_table(
        index="lead", columns="model", values="value")
    both = fh[["h5", "lvlh5_tft"]].dropna()
    assert len(both) > 0
    assert np.allclose(both["h5"], both["lvlh5_tft"])


def test_reference_run_refuses_unknown_provenance(synth_export, tmp_path, monkeypatch):
    from mlbench.runner import add_args, main

    p = argparse.ArgumentParser()
    add_args(p)
    monkeypatch.delenv("MLBENCH_IMAGE_ID", raising=False)
    monkeypatch.setattr("mlbench.runner.git_sha", lambda: "unknown")
    with pytest.raises(SystemExit):
        main(p.parse_args(["--export", str(synth_export), "--out", str(tmp_path / "r"), "--reference"]))
