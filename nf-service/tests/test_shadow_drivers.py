"""PAR-1057: the PAR-830 level drivers and the time-weighted target of the grid
shadow TFT.

    cd nf-service && python3 -m pytest tests/test_shadow_drivers.py -q

Pure pandas: no database, no torch.
"""

import importlib.util
import os
import sys

import pandas as pd
import pytest

NF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, NF_DIR)

# Other test modules replace `db` in sys.modules with a stub; load the real file.
_spec = importlib.util.spec_from_file_location("nf_db_drivers", os.path.join(NF_DIR, "db.py"))
db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(db)

T = pd.Timestamp


def _win(ds, open_hm, close, asof=1, park="p1"):
    day = T(ds)
    return {"park_id": park, "ds": day, "sched_open": 1, "sched_closed": 0,
            "open_local": day + pd.Timedelta(open_hm),
            "close_local": day + pd.Timedelta(close), "sched_asof": asof}


def _by_day(out):
    return out.set_index("ds")


# --- opening hours / late close (drivers 1 and 4) ---------------------------

def test_hours_and_close_against_the_last_four_same_weekdays():
    sat = ["2026-09-26", "2026-10-03", "2026-10-10", "2026-10-17"]
    rows = [_win(d, "10h", "18h") for d in sat]
    rows.append(_win("2026-10-24", "10h", "22h"))         # Halloween night: 12 h, +4 h
    rows.append(_win("2026-10-31", "10h", "25h"))         # closes 01:00 the next day
    out = _by_day(db.schedule_drivers(pd.DataFrame(rows)))
    d = out.loc[T("2026-10-24")]
    assert d.hours_ratio == pytest.approx(1.5) and d.close_vs_ref == pytest.approx(4.0)
    assert d.sched_known == 1
    # The reference of 10-31 is 10-24 + the three 8 h Saturdays before it.
    late = out.loc[T("2026-10-31")]
    assert late.close_vs_ref == pytest.approx(25 - (22 + 18 * 3) / 4)
    # The first Saturday has no reference day: neutral.
    first = out.loc[T("2026-09-26")]
    assert (first.hours_ratio, first.close_vs_ref) == (1.0, 0.0)


def test_a_window_published_after_the_origin_is_unknown_and_neutral():
    rows = [_win("2026-10-17", "10h", "18h"), _win("2026-10-24", "10h", "22h", asof=0)]
    d = _by_day(db.schedule_drivers(pd.DataFrame(rows))).loc[T("2026-10-24")]
    assert (d.sched_known, d.hours_ratio, d.close_vs_ref, d.closed_days_before) == (0, 1.0, 0.0, 0)


# --- first operating day after a closure (driver 3) -------------------------

def test_closed_days_before_counts_gaps_between_published_windows_only():
    days = ["2026-10-01", "2026-10-02", "2026-10-05", "2026-11-30"]
    out = _by_day(db.schedule_drivers(pd.DataFrame([_win(d, "10h", "18h") for d in days])))
    assert out["closed_days_before"].tolist() == [0, 0, 2, db.CLOSED_DAYS_CAP]


def test_drivers_reach_the_frame_and_unknown_days_stay_neutral():
    meta = pd.DataFrame([{"unique_id": "r1", "park_id": "p1", "timezone": "Europe/Berlin",
                          "country": "DE", "region": None, "influencing": None,
                          "latitude": 48.0}])
    sched = pd.DataFrame([_win("2026-10-17", "10h", "18h"), _win("2026-10-24", "9h", "21h")])
    frame = pd.DataFrame({"unique_id": "r1",
                          "ds": pd.to_datetime(["2026-10-24", "2026-10-25"])})
    out = db.add_calendar_covariates(frame, meta, pd.DataFrame(), schedule=sched)
    assert out["hours_ratio"].tolist() == [1.5, 1.0]
    assert out["sched_known"].tolist() == [1, 0]
    assert not out[db.DRIVER_EXOG].isna().any().any()


# --- school-break changes (driver 2) -----------------------------------------

def _school_frame(days):
    meta = pd.DataFrame([{"unique_id": "r1", "park_id": "p1", "timezone": "Europe/Berlin",
                          "country": "DE", "region": "DE-NW", "influencing": None,
                          "latitude": 51.0}])
    frame = pd.DataFrame({"unique_id": "r1", "ds": pd.to_datetime(days)})
    return frame, meta


def _school(*dates):
    return pd.DataFrame([{"date": T(d), "country": "DE", "region": "DE-NW",
                          "holiday_type": "school"} for d in dates])


def test_first_day_last_day_and_day_after_a_break():
    frame, meta = _school_frame(pd.date_range("2026-07-09", "2026-07-13"))
    hol = _school("2026-07-10", "2026-07-11", "2026-07-12", "2026-10-20")  # coverage to 10-20
    out = db.add_calendar_covariates(frame, meta, hol)
    assert out["school_break_first_day"].tolist() == [0, 1, 0, 0, 0]
    assert out["school_break_last_day"].tolist() == [0, 0, 0, 1, 0]
    assert out["school_break_day_after"].tolist() == [0, 0, 0, 0, 1]


def test_no_change_is_claimed_past_the_school_coverage():
    frame, meta = _school_frame(pd.date_range("2026-07-11", "2026-07-13"))
    out = db.add_calendar_covariates(frame, meta, _school("2026-07-10", "2026-07-11", "2026-07-12"))
    # Coverage ends 07-12: whether 07-12 is the last day, or 07-13 the day after, is unknown.
    assert out["school_break_last_day"].tolist() == [0, 0, 0]
    assert out["school_break_day_after"].tolist() == [0, 0, 0]
    assert out["school_holiday_known"].tolist() == [1, 1, 0]


def test_drivers_are_inputs_of_the_shadow_only(monkeypatch):
    assert db.futr_exog() == db.FUTR_EXOG
    monkeypatch.setattr(db, "DRIVERS", True)
    assert db.futr_exog() == db.FUTR_EXOG  # never without the grid
    monkeypatch.setattr(db.settings, "NF_CALENDAR_GRID", True)
    assert db.futr_exog() == db.FUTR_EXOG_GRID + db.DRIVER_EXOG
    monkeypatch.setattr(db, "DRIVERS", False)
    assert db.futr_exog() == db.FUTR_EXOG_GRID


# --- the time-weighted target (5) --------------------------------------------

def _readings(rows, uid="r1"):
    return pd.DataFrame([{"unique_id": uid, "park_id": "p1", "ts": T(f"2026-10-20 {hm}"),
                          "status": st, "wait": float(w)} for hm, st, w in rows])


WINDOW = pd.DataFrame([_win("2026-10-20", "10h", "18h")])


def test_a_jittery_busy_hour_does_not_outweigh_a_calm_day():
    rows = [("10:00", "OPERATING", 10)]
    # Six rows in half an hour: two of 22 counted quarter-hours.
    rows += [(f"12:{m:02d}", "OPERATING", 60 + m // 5) for m in range(0, 30, 5)]
    rows += [("12:30", "OPERATING", 10)]
    r = _readings(rows)
    change_log_p90 = r["wait"].quantile(0.9)
    out = db.time_weighted_p90(r, WINDOW, 0.9, 5)
    assert change_log_p90 > 60
    assert out["y"].tolist() == [10.0]


def test_a_reading_is_carried_for_at_most_three_hours_and_a_down_row_ends_it():
    # 10:00 wait 40 → slots ending 10:15 … 13:00 (12); nothing after the 3 h.
    stale = _readings([("10:00", "OPERATING", 40)], "stale")
    # DOWN at 10:30: only the slots ending 10:15 and 10:30 carry the 40 → < 3 slots.
    down = _readings([("10:00", "OPERATING", 40), ("10:30", "DOWN", 0)], "down")
    late = _readings([("10:00", "OPERATING", 20), ("16:00", "OPERATING", 80)], "late")
    out = db.time_weighted_p90(pd.concat([stale, down, late]), WINDOW, 0.9, 5).set_index("unique_id")
    assert "down" not in out.index
    assert out.loc["stale", "y"] == 40.0
    # late: 12 slots of 20 (to 13:00) and 8 of 80 (16:15 … 18:00).
    assert out.loc["late", "y"] == pytest.approx(80.0)


def test_without_a_published_window_the_operating_span_is_the_window():
    r = _readings([("10:00", "OPERATING", 30), ("11:00", "OPERATING", 50)])
    out = db.time_weighted_p90(r, None, 0.9, 5)
    # Slots ending 10:15 … 11:15: 30, 30, 30, 30, 50 → P90 = 30 + 0.6 × 20.
    assert out["y"].tolist() == [pytest.approx(42.0)]


def test_categorical_ids_and_status_give_the_same_target():
    """fetch_time_weighted_panel casts ids/status to category for memory; the
    result must equal the plain-str one and hold no empty id × day groups."""
    stale = _readings([("10:00", "OPERATING", 40)], "stale")
    down = _readings([("10:00", "OPERATING", 40), ("10:30", "DOWN", 0)], "down")
    late = _readings([("10:00", "OPERATING", 20), ("16:00", "OPERATING", 80)], "late")
    r = pd.concat([stale, down, late], ignore_index=True)
    plain = db.time_weighted_p90(r, WINDOW, 0.9, 5)
    cat = r.astype({"unique_id": "category", "park_id": "category", "status": "category"})
    out = db.time_weighted_p90(cat, WINDOW, 0.9, 5)
    out["unique_id"] = out["unique_id"].astype(str)
    pd.testing.assert_frame_equal(
        out.sort_values("unique_id").reset_index(drop=True),
        plain.sort_values("unique_id").reset_index(drop=True),
    )
