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


def _poison(r: Runner, c: dt.date) -> None:
    """Change everything a forecast made at c must not know."""
    x = r.con.execute
    cut = "(SELECT o.origin_utc FROM o WHERE o.park_id = {t}.park_id)"
    x(f"UPDATE truth SET y = y * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE > {cut.format(t='truth')}")
    x(f"UPDATE slots SET wait = wait * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE > {cut.format(t='slots')}")
    x(f"UPDATE ride_day SET p90 = p90 * 10, peak_h90 = peak_h90 * 10 WHERE date >= DATE '{c}'")
    x(f"UPDATE hour_stats SET p50 = p50 * 10, p90 = p90 * 10 WHERE date >= DATE '{c}'")
    x("UPDATE tft SET peak = peak * 10 WHERE created_utc >= (SELECT min(origin_utc) FROM o)")
    x("UPDATE cbd SET peak = peak * 10 WHERE created_utc >= (SELECT min(origin_utc) FROM o)")
    # a schedule written after the origin moves: nothing may follow it
    x(f"""UPDATE windows SET open_utc = open_utc + INTERVAL 2 HOUR, open_local = open_local + INTERVAL 2 HOUR
          WHERE updated_utc >= {cut.format(t='windows')}""")
    x("UPDATE park_day_cov SET temp_max = 99, precip_sum = 99")


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


@pytest.mark.parametrize("name", sorted(REGISTRY))
def test_information_cut_plugins(name, synth_export, tmp_path):
    """Every registered model: the same forecasts after the future is poisoned."""
    r = Runner(synth_export, tmp_path / "run", _cfg(), [], materialize=True)
    m = REGISTRY[name]()
    _setup(r, C)
    r.maybe_fit(m, C)
    before = r.plugin_predict(m, C, [0, 1, 3, 7]).sort_values(["aid", "slot_utc"]).reset_index(drop=True)
    _poison(r, C)
    _setup(r, C)
    m2 = REGISTRY[name]()
    r._last_fit.clear()
    r.maybe_fit(m2, C)
    after = r.plugin_predict(m2, C, [0, 1, 3, 7]).sort_values(["aid", "slot_utc"]).reset_index(drop=True)
    assert len(before) > 0
    pd.testing.assert_frame_equal(before, after)


def test_plugins_get_no_connection(synth_export, tmp_path):
    r = Runner(synth_export, tmp_path / "run", _cfg(), [], materialize=True)
    _setup(r, C)
    hv = r.history_view(C, "o")
    assert isinstance(hv, HistoryView)
    assert not any(isinstance(v, duckdb.DuckDBPyConnection) for v in vars(hv).values())
    h = hv.df(56)
    o = r.con.execute("SELECT park_id, origin_utc FROM o").df()
    j = h.merge(o, on="park_id")
    assert len(j) > 0
    assert (j["slot_start_utc"] + pd.Timedelta(minutes=15) <= j["origin_utc"]).all()
    cov = r.covariates(C, 7, oracle_weather=False)
    assert "temp_max" not in cov.columns and cov["weather_source"].isna().all()
    assert "schedule_known_at_origin" in cov.columns
    assert "temp_max" in r.covariates(C, 7, oracle_weather=True).columns


def test_plugin_example_equals_builtin(synth_export, tmp_path):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    r = Runner(synth_export, out, cfg, [LevelH5Example()], origins=("2026-04-10", "2026-04-12"))
    r.run()
    s = duckdb.connect().execute(f"""SELECT sum(n__lvlh5_naive) a, sum(n__example_level_h5) b,
        sum(sae__lvlh5_naive) c, sum(sae__example_level_h5) d FROM '{out}/parts/slot/*.parquet'""").fetchone()
    assert s[0] == s[1] and s[0] > 0
    assert abs(s[2] - s[3]) < 1e-6


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
