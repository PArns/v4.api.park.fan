"""Runner: strict information cut (baselines AND every registered plug-in), plug-in path ==
built-in path, provenance, report end to end."""

import argparse
import datetime as dt
import json
from pathlib import Path

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

    # Poisoning from the 06:00 cut proves nothing about the intraday paths: an origin at
    # 08:00 or 16:00 is ENTITLED to the origin day's truth up to its own cut, which is
    # poisoned ground. Asserting those two frames unchanged passed only while the history
    # view was cached per (date, table name) and so served 08:00's context at 16:00 — i.e.
    # the assertion was carried by a staleness bug, and the honest version of it is that
    # the intraday paths MUST move. Each cut therefore gets its own pass.
    daily_paths = [p for p in before if p in ("daily", "level")]
    intraday_paths = [p for p in before if p.startswith("intraday")]
    assert daily_paths and intraday_paths
    after = _repredict(synth_export, tmp_path / "p06", name, r, 6)
    for path in daily_paths:
        pd.testing.assert_frame_equal(before[path], after[path], obj=f"{name} / {path} / cut 06")
    assert set(before) == set(after)
    if getattr(m, "slot_path", True):
        assert len(before["daily"]) > 0
        moved = [p for p in intraday_paths
                 if not before[p]["q50"].equals(after[p]["q50"])]
        assert moved, (f"{name}: no intraday path moved when the origin day's truth was "
                       "poisoned — the intraday context is not following its own cut")
    else:   # a daily-level-only plug-in (tabpfn_ts): nothing on the slot paths, a level
        assert all(before[p].empty for p in ["daily", *intraday_paths]) and len(before["level"])

    # Second pass: poison from the LAST intraday cut, so every path's own history is
    # clean and all three must hold. This is what actually covers the intraday leak.
    late = max(int(p.removeprefix("intraday")) for p in intraday_paths)
    after = _repredict(synth_export, tmp_path / "plate", name, r, late)
    for path in before:
        pd.testing.assert_frame_equal(before[path], after[path], obj=f"{name} / {path} / cut {late}")


def _repredict(export, tmp, name: str, ref: Runner, poison_hour: int) -> dict[str, pd.DataFrame]:
    """`_predict_all` again on a fresh runner whose future is poisoned from the
    park-local `poison_hour` cut. A fresh runner rather than a re-`_setup` of the first
    one: the poison is cumulative, so two cuts cannot share a connection."""
    r = Runner(export, tmp, _cfg(), [REGISTRY[name]()], materialize=True)
    m = r.models[0]
    _setup(r, C)
    if poison_hour != r.cfg.origin_hour_local:
        r.con.execute(f"CREATE OR REPLACE TEMP TABLE o AS {B.origin_sql(C.isoformat(), poison_hour)}")
    _poison(r, C, weather=not r.oracle_weather(m))
    _setup(r, C)
    r.clear_caches()
    r._last_fit.clear()
    r.maybe_fit(m, C)
    return _predict_all(r, m)


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
    # `usable_horizon.csv` is filtered by `is_oracle` before the groupby, so an owx
    # model is absent from it outright — that half is a real check of the filter.
    # The `handover.winner` half is **weak on this fixture**: with two parks nothing
    # reaches `wins = True`, so the winner is always the fallback reference and an
    # owx model could not have been named anyway. The observable version of that
    # assertion is `test_handover_excludes_naive_candidates_and_oracles_from_the_winner_column`.
    for name in ("handover", "usable_horizon"):
        t = pd.read_csv(tdir / f"{name}.csv")
        col = "winner" if name == "handover" else "model"
        assert not set(t[col]) & owx, f"{name}.csv awards a row to {owx}"


def test_handover_and_usable_horizon_share_one_gate(synth_export, tmp_path):
    """`cells.csv`, `handover.csv` and `usable_horizon.csv` must apply ONE gate.

    Critic B4's tautology was a gate that only some tables used: UC4's unit is the
    park-MONTH, and gating it on origin days made every UC4 cell untested while the
    PR quoted the resulting emptiness as "no level source wins". A sibling later
    reported the same shape in `handover.csv`. The invariant below is what would
    have caught both halves at once, and it is deliberately **data-independent** —
    it does not need any cell to win.

    Note what this fixture cannot do: `synth.py` builds **two** parks, so
    `n_units_park <= 2` and `min_bootstrap_units = 10` makes `wins = True`
    unreachable here. Any test that asserts something about winners on this
    fixture is vacuous unless it lowers that floor, which is why the win-side
    check below is reported rather than required.
    """
    from mlbench.report import is_oracle, tested

    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-20")).run()
    tdir = build_report(out, reps=50)

    cells = pd.read_csv(tdir / "cells.csv")
    hand = pd.read_csv(tdir / "handover.csv")
    uh = pd.read_csv(tdir / "usable_horizon.csv")
    assert len(cells) and len(hand) and len(uh)

    key = ["metric", "uc", "segment", "lead"]
    scored = cells[(cells["region"] == "all") & ~cells["model"].map(lambda m: is_oracle(str(m)))]

    # 1. THE invariant: `handover.n_tested` must equal the number of rows the same
    #    gate admits in `cells.csv`. If either table used a different gate — a
    #    day-unit threshold on a park-month metric, say — these would diverge.
    checked = 0
    for k, g in scored.groupby(key):
        row = hand[(hand["metric"] == k[0]) & (hand["uc"] == k[1])
                   & (hand["segment"] == k[2]) & (hand["lead"].astype(str) == str(k[3]))]
        if row.empty:
            continue
        expect = int(sum(tested(r, cfg, str(r["model"]))[0] for _, r in g.iterrows()))
        assert int(row.iloc[0]["n_tested"]) == expect, (
            f"{k}: handover says n_tested={int(row.iloc[0]['n_tested'])}, recomputing the "
            f"same gate over cells.csv gives {expect} — the two tables are not sharing a gate")
        checked += 1
    assert checked > 10, f"only {checked} hand-over rows cross-checked"

    # 2. the unit a metric is gated on must be identical in both summary tables
    hu = {(str(r["metric"]), str(r["uc"]), str(r["segment"])): r["unit"] for _, r in hand.iterrows()}
    for _, r in uh.iterrows():
        k = (str(r["metric"]), str(r["uc"]), str(r["segment"]))
        if k in hu:
            assert hu[k] == r["unit"], f"{k}: handover unit {hu[k]} != usable_horizon unit {r['unit']}"

    # 3. `leads_tested = 0` must always carry its reason, so it can never again be
    #    read as "nothing won"
    empty = uh[uh["leads_tested"] == 0]
    assert empty["not_tested_because"].fillna("").str.len().gt(0).all()

    # 4. the win side, where it is reachable: a non-candidate, non-oracle model that
    #    wins AND passes the gate must be counted. Vacuous on this fixture (two
    #    parks), so it is not asserted to be non-empty.
    for k, g in scored.groupby(key):
        row = hand[(hand["metric"] == k[0]) & (hand["uc"] == k[1])
                   & (hand["segment"] == k[2]) & (hand["lead"].astype(str) == str(k[3]))]
        if row.empty:
            continue
        elig = [r for _, r in g.iterrows()
                if bool(r.get("wins")) and not bool(r.get("is_candidate"))
                and tested(r, cfg, str(r["model"]))[0]]
        if elig:
            assert int(row.iloc[0]["significant_models"]) > 0, (
                f"{k}: {[r['model'] for r in elig]} win and pass the gate but "
                f"significant_models = 0")


def test_cells_all_refs_is_always_emitted(synth_export, tmp_path):
    """BENCH-SPEC change #5 makes `cells_all_refs.csv` the artefact every
    per-baseline claim rests on, and three PRs cite it. A sibling workstream
    reported it missing, so assert it is emitted for a windowed pass as well as a
    full one, and that it really carries one row per (model, candidate) pair.
    """
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-20")).run()

    for kwargs in ({}, {"target_from": "2026-04-02", "target_to": "2026-04-18"}):
        tdir = build_report(out, reps=50, **kwargs)
        p = tdir / "cells_all_refs.csv"
        assert p.exists(), f"cells_all_refs.csv missing for {kwargs or 'full period'}"
        ar = pd.read_csv(p)
        assert len(ar) and {"model", "ref_candidate", "is_selected_ref"} <= set(ar.columns)
        # strictly more rows than cells.csv: every model against every candidate
        assert len(ar) > len(pd.read_csv(tdir / "cells.csv")) * 0.5
        assert ar["is_selected_ref"].any(), "no row is marked as the selected reference"
        assert ar.groupby(["metric", "uc", "segment", "region", "lead", "model"]).size().max() >= 2


def test_weekday_aliasing_is_refused_and_the_census_is_recorded():
    """PAR-828's subset took every 7th origin from a Thursday, so all 33 origins
    were Thursdays and every planner lead was one fixed weekday — d2 = Saturday,
    d3 = Sunday. Its "unexplained d2 hole" was the Saturday cell. Four workstreams
    and three critic passes missed it for two days because **no table in the
    harness carries a weekday column**, so the aliasing was invisible in every
    artefact a reviewer reads.

    Mutation-checked in both directions: the refusal must fire on a stride of 7
    and on a one-weekday window arrived at another way, and must NOT fire on a
    stride of 3 or on consecutive origins.
    """
    import datetime as dt

    from mlbench.runner import check_weekday_aliasing, origin_weekday_census

    consecutive = [dt.date(2026, 2, 19) + dt.timedelta(days=i) for i in range(231)]
    census = origin_weekday_census(consecutive)
    assert sum(census.values()) == 231
    assert all(v > 0 for v in census.values()), "231 consecutive days must hit every weekday"

    # every 7th origin from a Thursday — PAR-828's exact construction
    every7 = consecutive[0::7]
    assert all(c.weekday() == 3 for c in every7), "fixture must reproduce the all-Thursday subset"
    c7 = origin_weekday_census(every7)
    assert c7["Thu"] == len(every7) and sum(1 for v in c7.values() if v) == 1

    # 1. a stride that is a multiple of 7 is refused
    for n in (7, 14, 21):
        try:
            check_weekday_aliasing(consecutive[0::n], n, allow=False)
        except SystemExit as e:
            assert "aliased with day-of-week" in str(e)
            assert "multiple of 7" in str(e)
        else:
            raise AssertionError(f"--shard i/{n} must be refused")

    # 2. the same pathology reached another way: a window of one weekday
    mondays = [dt.date(2026, 3, 2) + dt.timedelta(weeks=i) for i in range(20)]
    try:
        check_weekday_aliasing(mondays, 1, allow=False)
    except SystemExit as e:
        assert "distinct weekday" in str(e)
    else:
        raise AssertionError("a one-weekday origin set must be refused even at stride 1")

    # 3. the escape hatch works, and still returns the census
    got = check_weekday_aliasing(every7, 7, allow=True)
    assert got["Thu"] == len(every7)

    # 4. it must NOT fire on legitimate sharding or on consecutive origins
    for n in (1, 2, 3, 4, 5, 6, 8):
        out = check_weekday_aliasing(consecutive[0::n], n, allow=False)
        assert sum(out.values()) == len(consecutive[0::n])


def test_origin_window_filter_actually_runs(synth_export, tmp_path):
    """`--origin-from/--origin-to` must EXECUTE, not merely exist.

    This flag shipped, was reviewed, was cited in the docs as the answer to the
    lead-season confound, and then raised `Binder Error: No function matches
    '-(TIMESTAMP_NS, INTEGER)'` the first time it was pointed at a real run:
    `date` is a timestamp in the parquet, so `(date - L)` has no overload. Nothing
    in the suite called it, so "the code path is there" was mistaken for "it
    works". That is the same failure mode as a test that cannot observe what it
    guards, one level up.
    """
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-20")).run()

    full = build_report(out, reps=50)
    n_full = len(pd.read_csv(full / "cells.csv"))

    tdir = build_report(out, origin_from="2026-04-05", origin_to="2026-04-12", reps=50)
    assert tdir.name == "tables_o2026-04-05_2026-04-12"
    cells = pd.read_csv(tdir / "cells.csv")
    assert len(cells) > 0, "the origin window filtered everything out"
    assert len(cells) < n_full, "the origin window changed nothing — is the filter wired up?"

    rec = json.loads((out / f"report-{tdir.name}.json").read_text())
    assert (rec["origin_from"], rec["origin_to"]) == ("2026-04-05", "2026-04-12")
    assert rec["target_from"] is None

    # every origin-bearing table must respect it: the widest lead still scored
    # cannot imply an origin outside the window
    summary = (out / f"summary_{tdir.name}.md").read_text()
    assert "Origin window 2026-04-05 … 2026-04-12" in summary

    # and it composes with a target window
    both = build_report(out, target_from="2026-04-06", target_to="2026-04-14",
                        origin_from="2026-04-05", origin_to="2026-04-12", reps=50)
    assert both.name == "tables_2026-04-06_2026-04-14_o2026-04-05_2026-04-12"
    assert len(pd.read_csv(both / "cells.csv")) > 0


def test_report_records_its_own_code_identity(synth_export, tmp_path):
    """A `tables_*/` dir must carry the identity of the code that WROTE it.

    `run-*.json` describes the code that produced `parts/` (the scoring sums); every
    verdict on top of them — reference choice, envelope win, LOW-N gate, hand-over
    winner — is recomputed by the report, routinely at a later commit. On
    `20261009-baselines-v2` the `parts/` are from `ea9a8438` and the published tables
    from three fix commits later, and the only evidence of that was a file mtime.

    `code_sha256` must be MEASURED (recomputable from the sources), not asserted:
    `git_sha` comes from the launcher's environment and has been stale in this repo.
    """
    import hashlib

    import mlbench
    from mlbench.provenance import code_hash

    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps(
        {"export": str(synth_export), "git_sha": "deadbeef", "git_sha_source": "env",
         "code_sha256": "not-the-report-hash"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-12")).run()

    tdir = build_report(out, target_from="2026-04-02", target_to="2026-04-12", reps=50)
    rec = json.loads((out / f"report-{tdir.name}.json").read_text())

    # recompute the hash the way a reviewer would, independently of the module
    h = hashlib.sha256()
    root = Path(mlbench.__file__).resolve().parent
    for f in sorted(root.rglob("*.py")):
        h.update(str(f.relative_to(root)).encode())
        h.update(f.read_bytes())
    assert rec["code_sha256"] == h.hexdigest() == code_hash()

    assert rec["kind"] == "report"
    assert rec["tables_dir"] == tdir.name == "tables_2026-04-02_2026-04-12"
    assert (rec["target_from"], rec["target_to"]) == ("2026-04-02", "2026-04-12")
    assert "cells.csv" in rec["tables"] and "handover.csv" in rec["tables"]
    # the report's identity must be kept SEPARATE from the run's, not overwritten by it
    assert rec["run_shards"][0]["code_sha256"] == "not-the-report-hash"
    assert rec["run_shards"][0]["git_sha"] == "deadbeef"
    assert rec["code_sha256"] != rec["run_shards"][0]["code_sha256"]
    # and the summary must say which code wrote the tables, flagging an asserted SHA
    summary = (out / f"summary_{tdir.name}.md").read_text()
    assert "code that produced THESE tables (the report)" in summary
    assert rec["code_sha256"][:16] in summary
    assert "code that produced `parts/` (the scoring)" in summary


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


def _cells_frame(rows):
    """A minimal `cells.csv`-shaped frame for unit-testing `handover()` directly.

    The synthetic export has **two** parks, so `n_units_park <= 2` and the
    `min_bootstrap_units = 10` guard makes `wins = True` unreachable end-to-end.
    Every assertion about a *winner* on that fixture is therefore vacuous. Two
    tests in this file were: the naive-exclusion test below (its `bad` frame
    filters on `significant_models > 0`, which is never true there) and the
    hand-over half of the ORACLE-weather test. `handover()` is a pure function of
    a DataFrame, so the honest fix is to hand it a frame that *does* contain a
    win and assert on the mechanism, rather than hope the fixture produces one.
    """
    base = dict(metric="MAE", uc="UC3", segment="all", region="all", lead=1, unit="park-day",
                lower=True, ref="wt_med", value=8.0, wins=False, wins_park_day=False,
                is_candidate=False, n_env=3, env_min_share=0.9, env_min_units_park_day=100,
                env_min_units_park=40, n_origin_days=54, envelope="snaive7/wt_med/clim",
                hardest_naive="wt_med", diff_vs_ref=0.0, diff_vs_hardest=0.0,
                diff_lo_park=-1.0, diff_hi_park=-0.5, diff_lo=-1.0, diff_hi=-0.5,
                hardest_lo_park=-1.0, hardest_hi_park=-0.5, paired_share=0.9)
    return pd.DataFrame([{**base, **r} for r in rows])


def test_handover_excludes_naive_candidates_and_oracles_from_the_winner_column():
    """The 1313bc61 fix and the `_owx` suffix gate, asserted where they can be seen.

    A candidate that beats the envelope of the OTHER candidates is a real finding
    and `usable_horizon.csv` keeps it — but the hand-over table must not rank it
    against a model, because the two margins are measured against different (and
    for a candidate, weaker) pools. An ORACLE-weather model must never win a row
    at all. Both are checked here against a frame that actually contains wins.
    """
    from mlbench.report import handover

    cfg = BenchConfig()
    cells = _cells_frame([
        # a naive candidate with the BIGGEST margin ...
        dict(model="wt_med", is_candidate=True, wins=True, diff_vs_hardest=-0.90,
             hardest_naive="clim", envelope="snaive7/clim", n_env=2),
        # ... an ORACLE-weather model with the second biggest ...
        dict(model="chronos2_owx", wins=True, diff_vs_hardest=-0.60),
        dict(model="chronos2_owx_x_h5", wins=True, diff_vs_hardest=-0.55),
        # ... and a servable model with the smallest. The servable one must win.
        dict(model="lvlh5_tft", wins=True, diff_vs_hardest=-0.30),
    ])
    ho = handover(cells, cfg)
    assert len(ho) == 1
    row = ho.iloc[0]
    assert row["winner"] == "lvlh5_tft", (
        f"winner is {row['winner']!r} — a naive candidate or an oracle took the row")
    assert row["significant_models"] == 1, "only the servable model may be counted"
    assert row["reference"] == "wt_med"

    # with no servable winner the row falls back to the reference, and says so
    only_naive = _cells_frame([
        dict(model="wt_med", is_candidate=True, wins=True, diff_vs_hardest=-0.90),
        dict(model="lvlh5_tft", wins=False),
    ])
    ho2 = handover(only_naive, cfg)
    assert ho2.iloc[0]["winner"] == "wt_med"
    assert ho2.iloc[0]["significant_models"] == 0
    assert float(ho2.iloc[0]["paired_margin"]) == 0.0


def test_handover_respects_the_coverage_gate_even_when_a_model_wins():
    """A win on 21 % of the reference's paired rows must not reach the serving
    router. This is the real cell behind the one apparent violation of the naive
    invariant: `lvlh5_cbd` wins D5 first-hour at d45 with `env_min_share = 0.212`
    and is correctly suppressed, appearing in `low_coverage_cells.csv` instead.
    """
    from mlbench.report import handover

    cfg = BenchConfig()
    cells = _cells_frame([
        dict(model="lvlh5_cbd", wins=True, diff_vs_hardest=-0.46, env_min_share=0.212,
             paired_share=0.212),
        dict(model="wt_med", is_candidate=True, wins=False),
    ])
    ho = handover(cells, cfg)
    assert ho.iloc[0]["significant_models"] == 0, "a 21 %-coverage win must not be counted"
    assert ho.iloc[0]["winner"] == "wt_med"


def test_handover_never_names_a_naive_candidate_as_the_winner(synth_export, tmp_path):
    """End-to-end companion to `test_handover_excludes_naive_candidates_...`.

    NOTE this one is **weak by construction**: the fixture has two parks, so no cell
    can reach `wins = True` and the `bad` frame below is empty for that reason
    rather than because the exclusion works. It is kept because it would still
    catch a crash or a schema change in the real pipeline; the assertion that the
    exclusion itself works lives in the unit test above, which hands `handover()`
    a frame that does contain wins."""
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-20")).run()
    tdir = build_report(out, reps=50)
    ho = pd.read_csv(tdir / "handover.csv")
    cells = pd.read_csv(tdir / "cells.csv")
    cands = set(cells.loc[cells["is_candidate"].fillna(False).astype(bool), "model"])
    assert cands, "the fixture must have reference candidates"
    # a candidate may only appear as the winner when it IS the fallback reference
    bad = ho[ho["winner"].isin(cands) & (ho["significant_models"] > 0)]
    assert bad.empty, bad[["metric", "uc", "lead", "reference", "winner"]].to_string()
