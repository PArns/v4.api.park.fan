"""Level drivers (PAR-830): the reference is baseline 5a, schedules are as-of, the
plug-in obeys the information cut and is scored through the runner."""

import datetime as dt

import duckdb
import numpy as np
import pandas as pd

from mlbench import driver_analysis as A
from mlbench import drivers as D
from mlbench.config import BenchConfig
from mlbench.models import DriverLevel
from mlbench.runner import Runner


def test_reference_is_naive_median_of_last_four_same_weekday(synth_export):
    con = A.load(synth_export, leads=[1, 10])
    d = A.frame(con)
    rd = pd.read_parquet(synth_export / "parquet" / "ride_day.parquet")
    rd["date"] = pd.to_datetime(rd["date"]).dt.date
    row = d[(d["L"] == 10)].iloc[len(d[d["L"] == 10]) // 2]
    origin = pd.Timestamp(row["origin"]).date()
    target = pd.Timestamp(row["date"]).date()
    h = rd[(rd["aid"] == row["aid"]) & (rd["date"] < origin) & (rd["date"] >= origin - dt.timedelta(days=56))
           & (pd.to_datetime(rd["date"]).dt.dayofweek == pd.Timestamp(target).dayofweek)]
    expect = h.sort_values("date", ascending=False)["p90"].head(4).median()
    assert np.isclose(row["ref"], expect)
    assert np.isclose(row["y"], np.log(row["true_p90"] / expect))
    assert (pd.to_datetime(d["origin"]) == pd.to_datetime(d["date"]) - pd.to_timedelta(d["L"], unit="D")).all()


def test_schedule_drivers_are_masked_when_written_after_the_origin(synth_export):
    d = A.frame(A.load(synth_export, leads=[1, 10]))
    late = d["win_upd_us"] >= d["origin_us"]
    assert late.any()
    assert d.loc[late, [f"{c}_a" for c in D.WINDOW_COLS]].isna().all().all()
    early = ~late & d["win_upd_us"].notna()
    if early.any():
        assert np.allclose(d.loc[early, "hours_a"], d.loc[early, "hours"])
    # the final (upper-bound) columns are never masked
    assert d["hours"].notna().all()


def test_univariate_and_scores_run(synth_export):
    d = A.frame(A.load(synth_export, leads=[1, 7]))
    cfg = BenchConfig(bootstrap_reps=50)
    u = A.univariate(d, 1, cfg, reps=50)
    assert "effect_log" in u.columns
    p = A.walk_forward(d, ["hgb", "gam"], {"cal+hol+sched": ["cal", "hol", "sched"]}, min_train=200,
                       log=lambda s: None)
    cols = ["yhat__none__bias", "yhat__hgb__cal+hol+sched", "yhat__gam__cal+hol+sched"]
    ride, park = A.score(p, cols, cfg)
    assert set(ride["lead"]) <= {1, 7} and len(ride) and len(park)
    # out of sample only: every scored origin month lies after its training data
    assert (pd.to_datetime(p["origin"]) >= pd.Timestamp("2026-03-01")).all()


class _SmallDriverLevel(DriverLevel):
    min_train_rows = 200
    settled_rows = 200


def test_plugin_scored_through_runner(synth_export, tmp_path):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    Runner(synth_export, out, cfg, [_SmallDriverLevel()], origins=("2026-04-20", "2026-04-21")).run()
    con = duckdb.connect()
    n = con.execute(f"SELECT sum(n__driver_level_x_h5) FROM '{out}/parts/slot/*.parquet'").fetchone()[0]
    assert n and n > 0
    lv = con.execute(f"SELECT count(lvl_driver_level), count(lvl_naive4) FROM '{out}/parts/levels/*.parquet'").fetchone()
    assert lv[0] > 0 and lv[0] <= lv[1]
