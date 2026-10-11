"""Foundation-model adapters (PAR-828), run with the deterministic stub backend.

The real backends need GPU weights; what can break without them is the series
construction: the information cut, the slot bookkeeping and the join back onto
the harness's horizon slots."""

import datetime as dt

import duckdb
import numpy as np
import pandas as pd
import pytest

from mlbench import baselines as B
from mlbench.config import BenchConfig
from mlbench.models.base import Model
from mlbench.models.foundation import EMPTY, Chronos2, Chronos2Grid, FoundationModel
from mlbench.runner import Runner


class Recorder(FoundationModel):
    name = "fm_stub"
    backend = "stub"
    intraday = True
    seen: list

    def __init__(self):
        super().__init__()
        self.seen = []

    def predict(self, origin, horizon_slots, cov):
        out = super().predict(origin, horizon_slots, cov)
        self.seen.append((origin.kind, origin.hour_local, out.sort_values(
            ["attraction_id", "slot_start_utc"]).reset_index(drop=True)))
        return out

    def predict_daily(self, origin, horizon_days, cov):
        out = super().predict_daily(origin, horizon_days, cov)
        if out is not None:
            self.seen.append(("level", 0, out.sort_values(["attraction_id", "date"]).reset_index(drop=True)))
        return out


class GridRecorder(Recorder):
    name = "fm_stub_grid"
    context_mode = "grid"
    intraday = False
    daily = False


def _run(export, tmp, model, poison: bool):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    c = dt.date(2026, 4, 20)
    r = Runner(export, tmp, cfg, [model], materialize=True, origins=(str(c), str(c)))
    if poison:
        # everything at/after the 06:00 daily origin is replaced by garbage; the intraday
        # origins legitimately see the origin day up to THEIR cut, so only test the daily ones
        r.con.execute("""CREATE OR REPLACE TEMP TABLE o0 AS SELECT p.id park_id,
            timezone(p.timezone, TIMESTAMP '2026-04-20 06:00:00') o FROM parks p""")
        r.con.execute("""UPDATE truth SET y = y * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE >
            (SELECT o FROM o0 WHERE o0.park_id = truth.park_id)""")
    r.run()
    return r


@pytest.mark.parametrize("cls", [Recorder, GridRecorder])
def test_information_cut_and_coverage(synth_export, tmp_path, cls):
    a, b = cls(), cls()
    ra = _run(synth_export, tmp_path / "a", a, poison=False)
    _run(synth_export, tmp_path / "b", b, poison=True)
    daily_a = [s for s in a.seen if s[0] in ("daily", "level")]
    daily_b = [s for s in b.seen if s[0] in ("daily", "level")]
    assert daily_a and len(daily_a) == len(daily_b)
    for (_, _, x), (_, _, y) in zip(daily_a, daily_b):
        assert len(x) > 0
        pd.testing.assert_frame_equal(x, y)
    # the model's column reaches the slot aggregates and covers most target slots
    s = duckdb.connect().execute(f"""SELECT sum(n__{a.name}) n, sum(n__wt_med) w, sum(qn__{a.name}) q
        FROM '{tmp_path / "a"}/parts/slot/*.parquet'""").fetchone()
    assert s[0] > 0.5 * s[1] and s[2] > 0
    if a.intraday:
        assert any(k == "intraday" for k, _, _ in a.seen)
    assert ra.origins


def test_intraday_sees_origin_day(synth_export, tmp_path):
    """An intraday origin's context contains that day's slots up to the origin."""
    m = Recorder()
    _run(synth_export, tmp_path / "a", m, poison=False)
    intr = {h: df for k, h, df in m.seen if k == "intraday"}
    assert len(intr) >= 2
    # the stub forecasts the mean of the last 64 context steps: later origins see more of
    # the day, so their forecasts for the same slots differ somewhere
    h0, h1 = sorted(intr)[0], sorted(intr)[-1]
    j = intr[h0].merge(intr[h1], on=["attraction_id", "slot_start_utc"])
    assert len(j) and not np.allclose(j["q50_x"], j["q50_y"])


def test_variants_registered():
    from mlbench.models import REGISTRY
    assert {"chronos2", "chronos2_owx", "chronos2_grid", "chronos2_nocov", "timesfm3",
            "timesfm3_owx"} <= set(REGISTRY)
    assert Chronos2.provides_quantiles and Chronos2Grid.context_mode == "grid"


def test_parse_park_ids_cuts_the_comment_before_the_commas():
    """A subset file's header is prose with commas in it. Splitting on commas first
    turns "# 12 EU, 10 NA" into the ids " 10 NA" — which is how the first PAR-828
    subset run reported 32 parks for a 30-park file."""
    from mlbench.runner import parse_park_ids

    raw = ("# PAR-828 representative subset: 12 EU, 10 NA, 8 Asia (large and small)\n"
           "# EU\n"
           "park-a\n"
           "park-b, park-c   # two on one line, with a trailing comment\n"
           "\n")
    assert parse_park_ids(raw) == ["park-a", "park-b", "park-c"]
    assert parse_park_ids("x,y") == ["x", "y"]


def test_unknown_park_id_is_refused(synth_export, tmp_path):
    """An id in no export must stop the run, not quietly shrink the subset."""
    from mlbench.runner import load_tables
    from mlbench.build import connect

    con = connect("1GB", 2)
    real = duckdb.connect().execute(
        f"SELECT id FROM '{synth_export}/parquet/parks.parquet' LIMIT 1").fetchone()[0]
    load_tables(con, synth_export, materialize=True, park_ids=[real])
    assert con.execute("SELECT count(*) FROM parks").fetchone()[0] == 1
    with pytest.raises(SystemExit, match="in no export"):
        load_tables(connect("1GB", 2), synth_export, materialize=True,
                    park_ids=[real, "not-a-park"])


def test_pair_with_enables_a_model_against_model_comparison(synth_export, tmp_path):
    """`run --pair-with h5` + `report --vs h5` compares the plug-in against H5 PAIRED.

    Without it only the three reference candidates have paired sums, and the per-slot
    forecasts are gone by report time — "is the compressed layout better than the grid"
    would have no CI at all."""
    import json

    from mlbench.report import build_report

    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    r = Runner(synth_export, out, cfg, [Recorder()], origins=("2026-04-10", "2026-04-14"))
    r.pair_with = ["h5"]
    r.run()
    cols = duckdb.connect().execute(
        f"DESCRIBE SELECT * FROM '{out}/parts/slot/*.parquet'").df()["column_name"].tolist()
    assert "pn__fm_stub__h5" in cols and "psr__fm_stub__h5" in cols
    tdir = build_report(out, reps=50, vs=["h5", "lvlh5_tft"])
    vs = pd.read_csv(tdir / "cells_vs_h5.csv")
    row = vs[(vs["model"] == "fm_stub") & (vs["metric"] == "MAE") & (vs["segment"] == "all")
             & (vs["region"] == "all")]
    assert len(row) and row["ref"].eq("h5").all()
    assert row["diff_vs_ref"].notna().any() and row["diff_lo_park"].notna().any()
    assert "Paired against `h5`" in (out / "summary.md").read_text()
    # a comparator nobody paired against says so rather than disappearing
    assert not (tdir / "cells_vs_lvlh5_tft.csv").exists()
    assert "re-run with `--pair-with lvlh5_tft`" in (out / "summary.md").read_text()


def test_runner_materialises_the_history_a_plugin_declares(synth_export, tmp_path):
    """`HistoryView` raises above what the runner materialised (critic S9), and the
    foundation plug-ins ask for ~343 days of compressed context and 400 for the daily
    head — far beyond `BenchConfig.history_days` (56, the profile window). So the
    runner must widen the materialised frame to the largest `truth_history_days` of
    its registered models; otherwise every Chronos-2 origin is a `ValueError`, and a
    cap that truncated instead would silently shorten the scored context."""
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1])
    c = dt.date(2026, 4, 20)
    fm = Chronos2()
    r = Runner(synth_export, tmp_path, cfg, [fm], materialize=True, origins=(str(c), str(c)))
    r.con.execute(f"CREATE OR REPLACE TEMP TABLE o AS {B.origin_sql(c.isoformat(), 6)}")

    assert Chronos2.truth_history_days >= 400
    assert r.history_days_needed() == Chronos2.truth_history_days
    hv = r.history_view(c, "o")
    assert hv.max_days == Chronos2.truth_history_days
    hv.df(400)                                     # the daily head's ask must not raise
    hv.df(FoundationModel.context_slots // 24 + 2)  # the compressed context's ask

    # a model that declares nothing still gets exactly the config window, and the cap
    # is read off the CLASS, so an instance attribute cannot widen it (same reasoning as
    # critic S8's oracle-weather gate)
    narrow = _Narrow()
    narrow.truth_history_days = 400
    r.set_models([narrow])
    assert r.history_days_needed() == cfg.history_days
    with pytest.raises(ValueError):
        r.history_view(c, "o").df(cfg.history_days + 1)


class _Narrow(Model):
    name = "narrow"


def test_history_view_cache_follows_the_origin_hour(synth_export, tmp_path):
    """The intraday path reuses the table name `oh` for every origin hour, so a history
    cache keyed on `(date, table name)` hands hour 8's context to hours 10…20 and every
    intraday origin forecasts from a context frozen at the first hour of the day. The key
    has to follow the information CUT, not the label."""
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0])
    c = dt.date(2026, 4, 20)
    r = Runner(synth_export, tmp_path, cfg, [Recorder()], materialize=True,
               origins=(str(c), str(c)))
    seen = {}
    for h in (8, 20):
        r.con.execute(f"CREATE OR REPLACE TEMP TABLE oh AS {B.origin_sql(c.isoformat(), h)}")
        d = r.history_view(c, "oh").df(7)
        seen[h] = len(d[pd.to_datetime(d["date"]).dt.date == c])
    assert seen[8] < seen[20], f"origin-day slots did not grow with the hour: {seen}"


def test_an_instance_attribute_cannot_widen_an_information_budget(synth_export, tmp_path):
    """Both per-model history budgets are read off the CLASS, at the CALL SITES.

    `scored_name()` is a classmethod, so a model that sets a budget in `__init__` would get
    the wider window and keep its honest name — critic S8's hole, which this harness closed
    for `uses_oracle_weather` and must close for every budget of the same shape.
    `covariate_history_days` was read off the instance when `truth_history_days` was added
    beside it, which is the bug this test exists to make impossible.

    It therefore drives `plugin_predict` / `plugin_predict_daily` and inspects the frame the
    MODEL is actually handed. An earlier version of this test called `covariates()` directly
    with `type(m).covariate_history_days` already resolved, so it passed with the fix
    reverted — a guard that cannot fail. Mutating `type(m).covariate_history_days` back to
    `m.covariate_history_days` in `runner.py` must make this test fail.
    """
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1])
    c = dt.date(2026, 4, 20)

    class _Greedy(Model):
        name = "greedy"
        seen: list

        def __init__(self):
            self.seen = []
            self.truth_history_days = 400          # instance only — must be ignored
            self.covariate_history_days = 400      # instance only — must be ignored

        def predict(self, origin, horizon_slots, known_future_covariates):
            self.seen.append(known_future_covariates)
            return EMPTY

        def predict_daily(self, origin, horizon_days, known_future_covariates):
            self.seen.append(known_future_covariates)
            return None

    m = _Greedy()
    r = Runner(synth_export, tmp_path, cfg, [m], materialize=True, origins=(str(c), str(c)))
    B.snapshot_tables(r.con, c.replace(day=1).isoformat(), cfg)
    B.window_tables(r.con, c.isoformat(), cfg)
    B.target_tables(r.con, c.isoformat(), r.slot_leads(c), cfg)

    assert r.history_days_needed() == cfg.history_days, "truth budget widened by an instance"

    r.plugin_predict(m, c, [0, 1])
    r.plugin_predict_daily(m, c)
    assert m.seen, "the model was never called, so this test proves nothing"
    for cov in m.seen:
        earliest = pd.to_datetime(cov["date"]).dt.date.min()
        assert earliest >= c, (
            f"covariates reach back to {earliest}; the CLASS budget is "
            f"{_Greedy.covariate_history_days} days, so no row before {c} may be handed "
            f"over — the call site read the instance")
