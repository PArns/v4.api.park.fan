"""Level drivers (PAR-830): the reference is baseline 5a, schedules are as-of, the
plug-in obeys the information cut and is scored through the runner."""

import datetime as dt

import duckdb
import numpy as np
import pandas as pd

import pytest

from mlbench import baselines as B
from mlbench import driver_analysis as A
from mlbench import drivers as D
from mlbench.config import BenchConfig
from mlbench.models import DriverLevel
from mlbench.models.base import Model
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


def test_plugin_caches_accumulate_across_origins_without_leaking(synth_export, tmp_path):
    """The multi-origin accumulation path (``_advance``/``_cov``/``_win``) is where a
    cross-origin leak would live: the caches are kept BETWEEN origins, so a row that
    was in the future at origin 1 is in the past at origin 3. Drive one instance over
    three consecutive origins and assert the caches hold the past only, once each."""
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3])
    model = _SmallDriverLevel()
    origins = ("2026-04-20", "2026-04-21", "2026-04-22")
    Runner(synth_export, tmp_path / "run", cfg, [model], origins=(origins[0], origins[-1])).run()

    last = dt.date.fromisoformat(origins[-1])
    assert model._seen == last, "the caches must be advanced to the final origin"

    # ride-day levels: strictly before the origin (the `h[h["date"] < c]` cut), no dupes
    rd_dates = pd.to_datetime(model._rd["date"])
    assert len(rd_dates) and rd_dates.max().date() < last
    assert not model._rd.duplicated(["aid", "date"]).any()

    # past windows are read off observed truth, so they too are strictly past
    win_dates = pd.to_datetime(model._win["date"])
    assert len(win_dates) and win_dates.max().date() < last
    assert not model._win.duplicated(["park_id", "date"]).any()

    # covariates DO carry future dates (that is their job) but only one row per
    # park-day: origin 3's version must overwrite the one accumulated at origin 1
    # rather than sit beside it.
    assert not model._cov.duplicated(["park_id", "date"]).any()
    assert pd.to_datetime(model._cov["date"]).max().date() >= last

    # and the leaky flags are never cached at all (critic S11: the export builds
    # park_day_cov.sched_is_holiday / sched_is_bridge_day without an as-of filter)
    for col in ("sched_is_holiday", "sched_is_bridge_day"):
        assert col not in model._cov.columns


def test_runner_materialises_the_history_the_plugin_declares(synth_export, tmp_path):
    """``origin.history`` is materialised and ``df(days)`` raises above the cap rather
    than returning a short frame, while ``_advance`` primes its caches with 400 days at
    the first origin -- ten times ``BenchConfig.history_days``. The runner must therefore
    widen the frame to the largest ``truth_history_days`` of its registered models;
    without that every driver_level origin is a ``ValueError``, and a cap that truncated
    instead would score the published levels on a quietly shortened history."""
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1])
    c = dt.date(2026, 4, 20)
    r = Runner(synth_export, tmp_path / "run", cfg, [_SmallDriverLevel()], materialize=True,
               origins=(str(c), str(c)))
    r.con.execute(f"CREATE OR REPLACE TEMP TABLE o AS {B.origin_sql(c.isoformat(), 6)}")

    assert DriverLevel.truth_history_days >= 400
    assert r.history_days_needed() == DriverLevel.truth_history_days
    hv = r.history_view(c, "o")
    assert hv.max_days == DriverLevel.truth_history_days
    hv.df(400)                                   # the first origin's priming ask must not raise

    # a model that declares nothing still gets exactly the config window, and the need is
    # read off the CLASS, so an instance attribute cannot widen an information budget
    narrow = _Narrow()
    narrow.truth_history_days = 400
    r.set_models([narrow])
    r.clear_caches()
    assert r.history_days_needed() == cfg.history_days
    with pytest.raises(ValueError):
        r.history_view(c, "o").df(cfg.history_days + 1)


class _Narrow(Model):
    name = "narrow"
