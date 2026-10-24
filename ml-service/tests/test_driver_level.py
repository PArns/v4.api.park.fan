"""PAR-1058: the production port of ml-bench's ``driver_level`` gives the plug-in's
features and levels on the same inputs.

The fixture holds a small synthetic history (two parks, three rides each) and what
the plug-in made of it: its training-row count, the driver rows it predicted from
and the levels. Regenerate it — after a change to either side — with ml-bench and
DuckDB importable (``ml-bench/requirements.txt``) and scikit-learn at the
ml-service pin, from the repository root:

    docker run --rm -v "$PWD":/repo -w /repo/ml-service <image with duckdb> \\
        python tests/test_driver_level.py

The fixture stays under 10 000 training rows on purpose: from there on
HistGradientBoosting splits off an early-stopping set by row order, and DuckDB's
row order is not defined, so the plug-in itself is only reproducible below it.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import driver_level as DL

FIXTURE = Path(__file__).parent / "fixtures" / "driver_level_parity.json"
ORIGIN = dt.date(2026, 5, 20)


def _inputs(seed: int = 1058):
    """Synthetic rdh / windows / cov / parks, deterministic in ``seed``."""
    rng = np.random.default_rng(seed)
    parks = pd.DataFrame({"id": ["p-eu", "p-na"], "timezone": ["Europe/Berlin", "America/New_York"]})
    d0, d1 = dt.date(2026, 1, 5), ORIGIN + dt.timedelta(days=95)
    days = [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]
    cov, win, rdh = [], [], []
    for pid in parks["id"]:
        breaks = {d for d in days if (d.month == 4 and d.day <= 12) or (d.month == 7 and d.day >= 20)}
        public = {d for d in days if rng.random() < 0.03}
        for d in days:
            dow = (d.weekday() + 1) % 7
            hol = int(d in public)
            school = int(d in breaks)
            nb = [int(rng.random() < 0.04) for _ in range(3)]
            nsc = int(rng.integers(0, 3)) if school else int(rng.random() < 0.1)
            cov.append({"park_id": pid, "date": d, "dow": dow, "is_holiday_primary": hol,
                        "is_school_holiday_primary": school, "is_holiday_neighbor_1": nb[0],
                        "is_holiday_neighbor_2": nb[1], "is_holiday_neighbor_3": nb[2],
                        "neighbor_school_holiday_count": nsc, "holiday_count_total": hol + sum(nb),
                        "is_school_holiday_any": int(school or nsc > 0)})
            if pid == "p-na" and dow == 2 and d.month < 3:
                continue  # closed Tuesdays in winter
            close = 20 if dow in (0, 6) else 18
            close += 2 if rng.random() < 0.08 else 0
            opens = 9.5 if rng.random() < 0.1 else 10.0
            base = pd.Timestamp(d)
            win.append({"park_id": pid, "date": d,
                        "open_local": base + pd.Timedelta(hours=opens),
                        "close_local": base + pd.Timedelta(hours=close)})
            if d >= ORIGIN:
                continue
            for k in range(3):
                if rng.random() < 0.05:
                    continue
                lvl = (25 + 15 * k) * (1.35 if dow in (0, 6) else 1.0) * (1.3 if hol else 1.0) \
                    * (1.25 if school else 1.0) * (1 + (close - 18) * 0.05) * float(rng.lognormal(0, 0.15))
                rdh.append({"aid": f"{pid}-r{k}", "park_id": pid, "date": d, "p90": round(lvl, 2)})
    return (pd.DataFrame(rdh), pd.DataFrame(win), pd.DataFrame(cov), parks)


def _queries(rdh: pd.DataFrame) -> pd.DataFrame:
    rides = rdh[["aid", "park_id"]].drop_duplicates("aid")
    q = pd.concat([rides.assign(L=L) for L in range(1, 92)], ignore_index=True)
    q["origin"] = ORIGIN
    q["date"] = [ORIGIN + dt.timedelta(days=int(L)) for L in q["L"]]
    return q


def _fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def test_port_matches_the_ml_bench_plugin():
    rdh, win, cov, parks = _inputs()
    exp = _fixture()
    pdf = DL.park_day_features(cov, win)
    fitted = DL.fit(rdh, pdf, parks, ORIGIN)
    assert fitted is not None and fitted.rows == exp["train_rows"]
    got = DL.predict_levels(fitted, _queries(rdh), rdh, pdf, parks)
    want = pd.DataFrame(exp["levels"])
    m = want.merge(got.assign(date=got["date"].astype(str)), on=["aid", "date"], how="outer",
                   suffixes=("_want", "_got"), indicator=True)
    assert (m["_merge"] == "both").all(), m[m["_merge"] != "both"]
    np.testing.assert_allclose(m["level_got"], m["level_want"], rtol=1e-9)


def test_port_builds_the_plugins_driver_rows():
    rdh, win, cov, parks = _inputs()
    split = _fixture()["rows"]
    exp = pd.DataFrame(split["data"], columns=split["columns"])
    rows = DL.driver_rows(_queries(rdh), rdh, DL.park_day_features(cov, win), parks, with_truth=False)
    rows = rows.assign(date=rows["date"].astype(str))
    m = exp.merge(rows, on=["aid", "date"], how="outer", suffixes=("_want", ""), indicator=True)
    assert (m["_merge"] == "both").all()
    for f in ["ref", *DL.FEATURES]:
        a, b = m[f"{f}_want"], m[f]
        if f == "region":
            assert (a == b).all()
            continue
        np.testing.assert_allclose(b.astype("float64"), a.astype("float64"), rtol=1e-12, equal_nan=True,
                                   err_msg=f)


def test_unpublished_days_get_the_runners_projection():
    d = dt.date(2026, 6, 1)  # a Monday
    base = [d - dt.timedelta(days=i) for i in range(1, 15)]
    w = pd.DataFrame({"park_id": "p", "date": base,
                      "open_local": [pd.Timestamp(x) + pd.Timedelta(hours=10) for x in base],
                      "close_local": [pd.Timestamp(x) + pd.Timedelta(hours=18 if x.weekday() < 5 else 20,
                                                                    minutes=7) for x in base]})
    pub = pd.DataFrame({"park_id": ["p"], "date": [d + dt.timedelta(days=1)],
                        "open_local": [pd.Timestamp(d) + pd.Timedelta(days=1, hours=9)],
                        "close_local": [pd.Timestamp(d) + pd.Timedelta(days=1, hours=17)]})
    out = DL.project_windows(pd.concat([w, pub], ignore_index=True), {"p": d}, horizon=6)
    by = out.set_index("date")
    # published wins; a weekday reads the weekday median, rounded to the quarter hour
    assert by.loc[d + dt.timedelta(days=1), "close_local"].hour == 17
    assert by.loc[d, "close_local"] == pd.Timestamp(d) + pd.Timedelta(hours=18, minutes=0)
    sat = d + dt.timedelta(days=5)
    assert by.loc[sat, "close_local"] == pd.Timestamp(sat) + pd.Timedelta(hours=20, minutes=0)
    assert len(out) == len(w) + 1 + 6  # origin .. origin+6, one of them published


def test_forecast_covers_every_lead_and_no_more():
    rdh, win, cov, parks = _inputs()
    out = DL.forecast(rdh, win, cov, parks, {"p-eu": ORIGIN, "p-na": ORIGIN}, horizon=91)
    assert set(out["L"]) == set(range(1, 92))
    assert (pd.to_datetime(out["date"]) - pd.Timestamp(ORIGIN)).dt.days.eq(out["L"]).all()
    assert (out["level"] > 0).all()


def test_covariates_come_from_holiday_features_with_sunday_zero():
    meta = pd.DataFrame({"park_id": ["p"], "country": ["DE"], "region_code": ["DE-BW"],
                         "influencingRegions": ['[{"countryCode": "FR", "regionCode": null}]']})
    hol = pd.DataFrame({"date": pd.to_datetime(["2026-10-03", "2026-10-04"]), "country": ["DE", "FR"],
                        "region": [None, None], "holiday_type": ["public", "public"],
                        "is_nationwide": [True, True]})
    cov = DL.build_cov(meta, hol, dt.date(2026, 10, 3), dt.date(2026, 10, 5)).set_index("date")
    assert list(cov["dow"]) == [6, 0, 1]  # Sat, Sun, Mon
    assert cov.loc[dt.date(2026, 10, 3), "is_holiday_primary"] == 1
    assert cov.loc[dt.date(2026, 10, 4), "is_holiday_neighbor_1"] == 1


def test_a_second_start_is_refused_while_the_first_run_lives(tmp_path):
    lock = str(tmp_path / "driver_level.lock")
    sleeper = [sys.executable, "-c", "import time; time.sleep(30)"]
    first = DL.start_single_flight(sleeper, lock)
    try:
        assert first is not None
        assert DL.start_single_flight(sleeper, lock) is None
    finally:
        first.kill()
        first.wait()
    # The lock dies with the child: no stale lock after a kill (an OOM looks the same).
    again = DL.start_single_flight([sys.executable, "-c", "pass"], lock)
    assert again is not None
    assert again.wait() == 0


def _generate() -> None:  # pragma: no cover — needs ml-bench + DuckDB
    from types import SimpleNamespace

    root = Path(__file__).resolve().parents[2] / "ml-bench"
    sys.path.insert(0, str(root))
    from mlbench import drivers as D
    from mlbench.models.driver_level import COV_KEEP, DriverLevel

    rdh, win, cov, parks = _inputs()
    m = DriverLevel()
    m._rd = rdh[["aid", "park_id", "date", "p90"]]
    m._win = win[win["date"] < ORIGIN]
    fut = win[win["date"] >= ORIGIN][["park_id", "date", "open_local", "close_local"]]
    m._cov = cov.merge(fut, on=["park_id", "date"], how="left")[COV_KEEP]
    m._parks = parks
    m._seen = ORIGIN
    q = _queries(rdh)
    days = q.rename(columns={"aid": "attraction_id", "L": "lead_days"})
    lv = m.predict_daily(SimpleNamespace(date=ORIGIN), days, m._cov)
    m._tables(ORIGIN)
    m._db.register("q_in", q.assign(origin=ORIGIN))
    m._db.execute("""CREATE OR REPLACE TABLE q AS SELECT aid, park_id, CAST(origin AS DATE) origin,
                     CAST(date AS DATE) date, CAST(L AS INTEGER) L FROM q_in""")
    rows = D.add_asof(m._db.execute(D.driver_rows_sql(m.window_days, with_truth=False)).df())
    rows["date"] = pd.to_datetime(rows["date"]).dt.date.astype(str)
    keep = ["aid", "date", "ref", *DL.FEATURES]
    out = {
        "_about": "PAR-1058 parity fixture: generated by ml-service/tests/test_driver_level.py "
                  "from the ml-bench driver_level plug-in; do not edit by hand.",
        "train_rows": int(m._fit_rows),
        "levels": [{"aid": a, "date": str(d), "level": float(v)}
                   for a, d, v in zip(lv["attraction_id"], lv["date"], lv["level"])],
        "rows": json.loads(rows[keep].sort_values(["aid", "date"])
                           .to_json(orient="split", index=False, double_precision=15)),
    }
    FIXTURE.parent.mkdir(exist_ok=True)
    FIXTURE.write_text(json.dumps(out, separators=(",", ":")) + "\n")
    print(f"wrote {FIXTURE}: {out['train_rows']} training rows, {len(out['levels'])} levels")


if __name__ == "__main__":
    _generate()
