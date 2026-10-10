"""Truth construction (BENCH-SPEC "Truth")."""

import datetime as dt
import gzip
import json
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from mlbench.build import build, connect


def _raw(tmp_path, rows, windows, tz="Europe/Berlin"):
    raw = tmp_path / "raw"
    raw.mkdir()

    def csv(name, df):
        with gzip.open(raw / f"{name}.csv.gz", "wt") as f:
            df.to_csv(f, index=False)

    csv("parks", pd.DataFrame([{"id": "p", "slug": "p", "name": "p", "timezone": tz, "continent": "x",
                                "country_code": "DE", "region_code": "DE-BW",
                                "influencing_regions": json.dumps([]), "latitude": 0.0, "longitude": 0.0,
                                "park_type": "THEME_PARK"}]))
    csv("attractions", pd.DataFrame([{"id": "a", "park_id": "p", "name": "a", "slug": "a",
                                      "attraction_type": "x", "attraction_kind": "RIDE", "is_seasonal": False,
                                      "open_with_park": False, "latitude": 0.0, "longitude": 0.0, "land": "l",
                                      "retired_at_us": None, "headliner_tier": "A", "is_headliner": True}]))
    csv("schedule", pd.DataFrame([{"park_id": "p", "date": d, "schedule_type": "OPERATING",
                                   "opening_us": o, "closing_us": c, "is_holiday": False,
                                   "is_bridge_day": False, "updated_us": 0} for d, o, c in windows]))
    csv("holidays", pd.DataFrame([{"country": "DE", "date": "2026-01-01", "region": None,
                                   "holiday_type": "public", "is_nationwide": True, "name": "x"}]))
    csv("weather", pd.DataFrame([{"park_id": "p", "date": "2026-01-01", "data_type": "historical",
                                  "temp_max": 1.0, "temp_min": 0.0, "precip_sum": 0.0, "rain_sum": 0.0,
                                  "snow_sum": 0.0, "weather_code": 1, "wind_max": 1.0, "updated_us": 0}]))
    csv("queue_2026-03-28", pd.DataFrame(rows))
    con = connect("1GB", 2)
    build(raw, tmp_path / "parquet", con)
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{tmp_path}/parquet/slots.parquet') ORDER BY slot_utc").df()


def us(t):
    return int(pd.Timestamp(t).value // 1000)


def test_value_in_force_at_midpoint_and_staleness(tmp_path):
    z = ZoneInfo("Europe/Berlin")
    op = dt.datetime(2026, 3, 28, 10, 0, tzinfo=z)
    cl = dt.datetime(2026, 3, 28, 18, 0, tzinfo=z)
    rows = [
        # 10:05 -> 20 ; 10:20 -> 35 ; 10:22:30 is exactly a midpoint -> the 10:22:30 row is in force
        {"attraction_id": "a", "ts_us": us(op + dt.timedelta(minutes=5)), "status": "OPERATING", "wait": 20,
         "is_heartbeat": False},
        {"attraction_id": "a", "ts_us": us(op + dt.timedelta(minutes=22, seconds=30)), "status": "OPERATING",
         "wait": 35, "is_heartbeat": False},
        {"attraction_id": "a", "ts_us": us(op + dt.timedelta(minutes=50)), "status": "DOWN", "wait": None,
         "is_heartbeat": False},
        {"attraction_id": "a", "ts_us": us(op + dt.timedelta(hours=1)), "status": "OPERATING", "wait": 3,
         "is_heartbeat": False},
        # last row at 12:00; stays in force for 3 h (until 15:00), not until closing
        {"attraction_id": "a", "ts_us": us(op + dt.timedelta(hours=2)), "status": "OPERATING", "wait": 40,
         "is_heartbeat": False},
    ]
    s = _raw(tmp_path, rows, [("2026-03-28", us(op), us(cl))])
    s["local"] = s["slot_local"].dt.strftime("%H:%M")
    v = dict(zip(s["local"], s["wait"]))
    st = dict(zip(s["local"], s["status"]))
    assert v["10:00"] == 20          # midpoint 10:07:30, row 10:05
    assert v["10:15"] == 35          # midpoint 10:22:30 == row timestamp -> in force
    assert st["10:45"] == "DOWN"     # midpoint 10:52:30
    assert v["11:00"] == 3           # kept in slots (status data), excluded from truth (< 5)
    assert v["14:45"] == 40          # midpoint 14:52:30 -> 2 h 52 m old
    assert "15:00" not in v          # midpoint 15:07:30 -> older than 3 h -> no value
    assert s["ko"].min() == 0 and int(s.loc[s["local"] == "10:15", "ko"].iloc[0]) == 1
    truth = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{tmp_path}/parquet/truth.parquet')").df()
    assert set(truth["y"]) == {20, 35, 40}


def test_window_crossing_midnight_belongs_to_service_day(tmp_path):
    z = ZoneInfo("Europe/Berlin")
    op = dt.datetime(2026, 3, 28, 18, 0, tzinfo=z)
    cl = dt.datetime(2026, 3, 29, 1, 0, tzinfo=z)   # crosses midnight AND the DST switch night
    rows = [{"attraction_id": "a", "ts_us": us(op + dt.timedelta(minutes=10 + 20 * k)), "status": "OPERATING",
             "wait": 10 + k, "is_heartbeat": False} for k in range(20)]
    s = _raw(tmp_path, rows, [("2026-03-28", us(op), us(cl))])
    assert set(s["date"].astype(str)) == {"2026-03-28"}
    after = s[s["slot_local"].dt.date == dt.date(2026, 3, 29)]
    assert len(after) == 4                             # 00:00 .. 00:45 local
    assert after["ws"].min() == 96                     # continues past 95
    assert s["kc"].min() == 0


def test_drop_heartbeats_removes_only_carry_forward_slots(tmp_path):
    """`build --drop-heartbeats` must remove truth slots that exist only because a
    heartbeat row reset the 3 h staleness clock, and nothing else: every remaining
    slot keeps its value, and the dropped ones are all stale carry-forward
    (PAR-827 critic B3).
    """
    import synth
    from mlbench.build import build, connect

    raw = tmp_path / "raw"
    synth.write(raw)
    out = {}
    for tag, drop in (("with", False), ("without", True)):
        con = connect("1GB", 2)
        st = build(raw, tmp_path / f"pq-{tag}", con, drop_heartbeats=drop)
        assert st["drop_heartbeats"] is drop
        out[tag] = con.execute(
            "SELECT aid, slot_utc, y, (SELECT max(age_min) FROM slots) m FROM truth").df()
        out[f"{tag}_n"] = st["truth"]
        con.close()
    assert out["with_n"] > out["without_n"] > 0, (out["with_n"], out["without_n"])
    a = out["with"].set_index(["aid", "slot_utc"])["y"]
    b = out["without"].set_index(["aid", "slot_utc"])["y"]
    removed, added = set(a.index) - set(b.index), set(b.index) - set(a.index)
    assert removed, "dropping heartbeats must remove the carried-forward slots"
    # It is a near-subset, not a strict one: removing a row LENGTHENS the preceding
    # real row's forward fill (its `m_end` is the next row's ts, capped at 3 h), so a
    # midpoint can survive under a different parent row. On the 2026-10-09 export this
    # is 2 slots out of 13.5 M. The value never changes either way, because a heartbeat
    # carries the SAME wait as the row before it.
    assert len(added) <= max(1, len(removed) // 100), (len(added), len(removed))
    both = a.index.intersection(b.index)
    assert (a.loc[both].to_numpy() == b.loc[both].to_numpy()).all()
