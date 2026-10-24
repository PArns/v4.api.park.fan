"""The served arm (PAR-1043): the archive's own information cut, the nearest-origin rule,
the export -> build path, the --pair-with served wiring and the verdict generator."""

import datetime as dt
import gzip
import json
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

from mlbench import served_verdicts as SV
from mlbench.build import connect, served_curves
from mlbench.config import BenchConfig
from mlbench.models.base import HistoryView, Origin
from mlbench.models.served import Served, ServedIncumbent
from mlbench.runner import Runner

UTC = dt.timezone.utc
COLS = ["origin_us", "attraction_id", "park_id", "surface", "origin_kind", "lead_days", "target_date",
        "slot_start_us", "slot_minutes", "waits", "sources"]


def _us(t: dt.datetime) -> int:
    return int(pd.Timestamp(t).value // 1000)


def _row(origin, surface, kind, target, start, minutes, waits, aid="r1", park="p1", lead=0):
    return {"origin_us": _us(origin), "attraction_id": aid, "park_id": park, "surface": surface,
            "origin_kind": kind, "lead_days": lead, "target_date": target.isoformat(),
            "slot_start_us": _us(start), "slot_minutes": minutes,
            "waits": ",".join("" if w is None else str(w) for w in waits), "sources": "c" * len(waits)}


def _write_curves(tmp, rows) -> "Path":  # noqa: F821
    """Rows in the exact CSV format `mlbench export` writes, through `build`'s conversion."""
    raw, out = tmp / "raw", tmp / "parquet"
    raw.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    with gzip.open(raw / "archive_curves_2026-10-15.csv.gz", "wt") as f:
        pd.DataFrame(rows, columns=COLS).to_csv(f, index=False)
    assert served_curves(connect("1GB", 1), raw, out) == len(rows)
    return out / "served_curves.parquet"


def _predict(model, kind: str, bench: dt.datetime, slots: list[dt.datetime]) -> dict:
    origin = Origin(bench.date(), kind, bench.hour, pd.DataFrame(
        {"park_id": ["p1"], "timezone": ["UTC"], "origin_utc": [pd.Timestamp(bench)]}),
        HistoryView(pd.DataFrame()))
    grid = pd.DataFrame({"attraction_id": "r1", "park_id": "p1",
                         "slot_start_utc": pd.to_datetime(slots, utc=True)})
    p = model.predict(origin, grid, pd.DataFrame())
    return {pd.Timestamp(t).to_pydatetime(): v for t, v in zip(p["slot_start_utc"], p["q50"])}


D = dt.date(2026, 10, 15)


def T(h: int, m: int = 0, day: dt.date = D) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(h, m), UTC)


def test_intraday_cut_never_reads_a_later_capture(tmp_path):
    """A capture whose nominal origin is after the bench origin is never used, nor is a
    slot that starts before the capture instant (the capture may have partly seen it)."""
    path = _write_curves(tmp_path, [
        _row(T(6, 5), 1, 1, D, T(6, 15), 15, [10] * 48),                 # 06:00 capture, 12 h
        _row(T(10, 7), 1, 2, D, T(10, 0), 15, [20] * 17),                # 10:00 capture, 4 h
        _row(T(11, 0), 1, 2, D, T(11, 0), 15, [99] * 16),                # 11:00 nominal: after
        _row(T(12, 5), 1, 2, D, T(12, 15), 15, [99] * 16),               # 12:00: after
        _row(T(10, 7), 2, 2, D, T(10, 0), 60, [77] * 4),                 # planner: not intraday
    ])
    got = _predict(Served(path), "intraday", T(10), [T(10), T(10, 15), T(13), T(14, 15), T(15)])
    assert 99 not in got.values() and 77 not in got.values()
    # 10:00 started before the 10:07 capture; 14:15 and 15:00 are past the 4 h it archived —
    # and NOT filled from the older 06:00 curve: the nearest origin answers or nothing does
    assert got == {T(10, 15): 20, T(13): 20}


def test_nearest_origin_and_its_age_cap(tmp_path):
    path = _write_curves(tmp_path, [
        _row(T(6, 5), 1, 1, D, T(6, 15), 15, [10] * 56),
        _row(T(5, 59), 1, 1, D, T(6, 15), 15, [11] * 56, aid="r2"),      # 2 h 01 before 08:00
    ])
    # 08:00 has no capture of its own: the 06:00 one is the nearest, ≤ 2 h old
    assert _predict(Served(path), "intraday", T(8), [T(8), T(9)]) == {T(8): 10, T(9): 10}
    # ten o'clock: the 06:05 capture is now too old to stand for what was served
    assert _predict(Served(path), "intraday", T(10), [T(10), T(11)]) == {}
    origin = Origin(D, "intraday", 8, pd.DataFrame({"park_id": ["p1"], "timezone": ["UTC"],
                                                    "origin_utc": [pd.Timestamp(T(8))]}),
                    HistoryView(pd.DataFrame()))
    grid = pd.DataFrame({"attraction_id": "r2", "park_id": "p1",
                         "slot_start_utc": pd.to_datetime([T(9)], utc=True)})
    assert Served(path).predict(origin, grid, pd.DataFrame()).empty


def test_daily_reads_the_planner_as_a_step_with_its_long_leads(tmp_path):
    d10 = D + dt.timedelta(days=10)
    path = _write_curves(tmp_path, [
        _row(T(6, 5), 2, 1, D, T(9), 60, [30, None, 40]),                # hourly, one gap
        _row(T(7, 5), 2, 3, d10, T(9, day=d10), 60, [50], lead=10),      # long lead, 07:00 capture
        _row(T(6, 5), 1, 1, D, T(9), 15, [5] * 16),                      # park_hourly: not planner
        _row(T(5, 5, D - dt.timedelta(days=1)), 2, 1, D, T(9), 60, [66] * 3, lead=1),  # yesterday's
    ])
    got = _predict(Served(path), "daily", T(6), [T(9), T(9, 45), T(10, 15), T(11), T(12),
                                                 T(9, day=d10), T(9, 45, d10)])
    assert got == {T(9): 30, T(9, 45): 30, T(11): 40, T(9, day=d10): 50, T(9, 45, d10): 50}


def test_incumbent_prefers_surface_8_at_the_same_capture(tmp_path):
    path = _write_curves(tmp_path, [_row(T(10, 5), 1, 2, D, T(10, 15), 15, [12] * 4),
                                    _row(T(10, 5), 8, 2, D, T(10, 15), 15, [21] * 4)])
    assert _predict(Served(path), "intraday", T(10), [T(10, 30)]) == {T(10, 30): 12}
    assert _predict(ServedIncumbent(path), "intraday", T(10), [T(10, 30)]) == {T(10, 30): 21}


def test_missing_archive_is_an_error_not_an_empty_arm(tmp_path):
    with pytest.raises(FileNotFoundError, match="archive_curves"):
        _predict(Served(tmp_path / "nope.parquet"), "daily", T(6), [T(9)])


# --------------------------------------------------------------------- end to end (synth export)

def _synth_curves(export, tmp) -> "Path":  # noqa: F821
    """Archived curves for the synth parks, built from the truth (+3 min) so the arm has
    values: a 06:05 planner (15-min, d0–d3) and 4-h park_hourly at 10/12/14/16:05."""
    con = duckdb.connect()
    truth = con.execute(f"""SELECT t.aid, t.park_id, CAST(t.date AS DATE) AS date, t.slot_utc, t.y, p.timezone
        FROM read_parquet('{export}/parquet/truth.parquet') t
        JOIN read_parquet('{export}/parquet/parks.parquet') p ON p.id = t.park_id
        WHERE t.date BETWEEN DATE '2026-04-08' AND DATE '2026-04-18'""").df()
    truth["slot_utc"] = pd.to_datetime(truth["slot_utc"], utc=True)
    rows = []
    for (aid, park, date), g in truth.groupby(["aid", "park_id", "date"]):
        date = pd.Timestamp(date).date()
        z = ZoneInfo(g["timezone"].iloc[0])
        g = g.set_index("slot_utc")["y"]
        start = g.index.min()
        n = int((g.index.max() - start) / pd.Timedelta(minutes=15)) + 1
        grid = [start + pd.Timedelta(minutes=15 * i) for i in range(n)]
        waits = [None if s not in g.index else int(g[s]) + 3 for s in grid]
        for lead in range(4):
            c = date - dt.timedelta(days=lead)
            rows.append(_row(dt.datetime.combine(c, dt.time(6, 5), z), 2, 1, date, start, 15, waits,
                             aid=aid, park=park, lead=lead))
        for h in (10, 12, 14, 16):
            o = pd.Timestamp(dt.datetime.combine(date, dt.time(h, 5), z)).tz_convert("UTC")
            sub = [(s, w) for s, w in zip(grid, waits) if o < s <= o + pd.Timedelta(hours=4)]
            if sub:
                rows.append(_row(o, 1, 2, date, sub[0][0], 15, [w for _, w in sub], aid=aid, park=park))
    return _write_curves(tmp, rows)


def test_pair_with_served_end_to_end_and_the_generator(synth_export, tmp_path, capsys):
    from mlbench.report import build_report

    path = _synth_curves(synth_export, tmp_path / "arch")
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3])
    out = tmp_path / "20261108-served-test"
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test",
                                                   "origins": ["2026-04-10", "2026-04-14"]}))
    r = Runner(synth_export, out, cfg, [Served(path)], origins=("2026-04-10", "2026-04-14"))
    r.pair_with = ["served"]
    r.run()

    def cols(t):
        return duckdb.connect().execute(
            f"DESCRIBE SELECT * FROM '{out}/parts/{t}/*.parquet'").df()["column_name"].tolist()

    assert {"pn__h5__served", "pn__lvlh5_tft__served", "pn__wt_med__served"} <= set(cols("slot"))
    assert {"pn__persistence__served", "pn__h5__served"} <= set(cols("intraday"))
    rd = duckdb.connect().execute(f"SELECT DISTINCT ref FROM '{out}/parts/rideday/*.parquet'").df()
    assert "served" in set(rd["ref"])

    tdir = build_report(out, reps=50, vs=["served"])
    vs = pd.read_csv(tdir / "cells_vs_served.csv")
    for uc, model in (("UC3", "h5"), ("UC1", "persistence"), ("D3", "h5")):
        row = vs[(vs["uc"] == uc) & (vs["model"] == model) & (vs["segment"] == "all")
                 & (vs["region"] == "all")]
        assert len(row) and row["ref"].eq("served").all(), (uc, model)
        assert row["diff_vs_ref"].notna().any(), (uc, model)

    verdicts = tmp_path / "verdicts.json"
    src = SV.DEFAULT_VERDICTS.read_text()
    verdicts.write_text(src)
    assert SV.main(["--run", str(out), "--verdicts", str(verdicts), "--min-origin-days", "1"]) == 0
    assert "+++" in capsys.readouterr().out and verdicts.read_text() == src     # dry run
    assert SV.main(["--run", str(out), "--verdicts", str(verdicts), "--min-origin-days", "1",
                    "--write"]) == 0
    e = json.loads(verdicts.read_text())["questions"]["plan_d0_1"]["h5"]
    assert (e["reference"], e["metric"], e["runId"]) == ("served", "slot_mae", out.name)
    assert e["ciLow"] <= e["diff"] <= e["ciHigh"]
    assert e["period"] == "2026-04-10..2026-04-14"


# --------------------------------------------------------------------- generator rules (fixture)

def _cell(lead, diff, lo, hi, wins, model="h5", uc="UC3", days=20, parks=12, metric="MAE"):
    return {"metric": metric, "uc": uc, "lead": str(lead), "segment": "all", "region": "all",
            "model": model, "ref": "served", "diff_vs_ref": str(diff), "diff_lo_park": str(lo),
            "diff_hi_park": str(hi), "wins": str(wins), "n_paired_origin_days": str(days),
            "n_units_park": str(parks)}


@pytest.mark.parametrize("cells,state,worst", [
    # every lead wins in both passes -> better; the quoted figure is the weakest cell
    ({"a": [_cell(0, -1.0, -1.5, -0.5, True), _cell(1, -0.4, -0.7, -0.1, True)],
      "b": [_cell(0, -0.9, -1.4, -0.4, True), _cell(1, -0.6, -0.9, -0.3, True)]}, "better", -0.4),
    # one cell's CI reaches 0 -> unclear
    ({"a": [_cell(0, -1.0, -1.5, -0.5, True), _cell(1, -0.2, -0.5, 0.1, False)]}, "unclear", -0.2),
    # a lead missing from one pass -> unclear, however good the rest
    ({"a": [_cell(0, -1.0, -1.5, -0.5, True), _cell(1, -0.4, -0.7, -0.1, True)],
      "b": [_cell(0, -0.9, -1.4, -0.4, True)]}, "unclear", -0.4),
    # every CI above 0 -> worse
    ({"a": [_cell(0, 1.0, 0.5, 1.5, False), _cell(1, 0.4, 0.1, 0.7, False)]}, "worse", 1.0),
    # wins, but on too few origin days -> unclear
    ({"a": [_cell(0, -1.0, -1.5, -0.5, True, days=5), _cell(1, -0.4, -0.7, -0.1, True)]},
     "unclear", -0.4),
])
def test_cell_verdict_rules(cells, state, worst):
    v = SV.cell_verdict(cells, "MAE", ("UC2", "UC3"), ["0", "1"], "h5", 14)
    assert v["state"] == state and v["diff"] == worst
    assert v["ciLow"] <= v["diff"] <= v["ciHigh"]


def test_cell_without_rows_is_left_alone(tmp_path):
    cells = {"a": [_cell(0, -1.0, -1.5, -0.5, True, model="chronos2_grid")]}
    assert SV.cell_verdict(cells, "MAE", ("UC2", "UC3"), ["0", "1"], "h5", 14) is None
    new = SV.entries(tmp_path, cells, 14)              # no run-*.json -> no period
    assert set(new) == {("plan_d0_1", "chronos")}
    out = SV.apply({"questions": {"plan_d0_1": {"h5": {"state": "pending"}}}}, new)
    assert out["questions"]["plan_d0_1"]["h5"] == {"state": "pending"}
    assert "period" not in out["questions"]["plan_d0_1"]["chronos"]
