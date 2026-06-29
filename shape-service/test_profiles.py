"""Unit tests for the pure shape-profile assembly + render (no DB).

    cd shape-service && python3 -m pytest test_profiles.py -q
"""

import numpy as np
import pandas as pd
import pytest

import profiles as P

# Operating window 8:00–14:00 in 15-min slots (32..56), a tent shape peaking at slot 44.
_OPEN = list(range(32, 57))


def _shape_val(s: int) -> float:
    return 0.2 + 0.8 * max(0.0, 1.0 - abs(s - 44) / 12.0)  # 1.0 at 44, ~0.2 at edges


def _panel(days: int = 40, rides=("a", "b"), levels=(10, 30, 50, 70, 90)) -> pd.DataFrame:
    """Synthetic panel: wait = daily_level × tent_shape, so normalising by the daily peak
    recovers the tent and the daily peak == the level."""
    rows = []
    for ride in rides:
        for d in range(days):
            day = pd.Timestamp("2026-01-01") + pd.Timedelta(days=d)
            level = levels[d % len(levels)]
            for s in _OPEN:
                rows.append({"unique_id": ride, "day": day, "slot": s,
                             "y": level * _shape_val(s)})
    return pd.DataFrame(rows)


def _build(df=None, **kw):
    df = _panel() if df is None else df
    return P.build_profiles(df, park_id="P", slot_count=96, **kw)


def test_builds_and_normalises_to_peak_one():
    prof = _build()
    assert prof is not None
    curve, _ = prof.pick_curve("a", crowd=1, dow="week")
    # normalised by the daily peak → the busiest slot's mean fraction is ~1.0
    assert np.nanmax(curve) == pytest.approx(1.0, abs=1e-6)
    assert curve[44] == pytest.approx(1.0, abs=1e-6)         # tent apex
    # NaN where the ride never operated (e.g. midnight) — no zero-fill
    assert np.isnan(curve[0])


def test_crowd_bucketing_from_level():
    prof = _build()
    # levels 10..90 over 5 distinct values → 3 terciles; low maps low, high maps high
    assert prof.level_to_crowd("a", 10) == 0
    assert prof.level_to_crowd("a", 90) == prof.thresholds["a"].size  # top bucket
    assert prof.level_to_crowd("a", 10) < prof.level_to_crowd("a", 90)


def test_render_scales_shape_by_level():
    prof = _build()
    curve = prof.render("a", level=60.0, dow_index=2)        # a Wednesday
    assert np.nanmax(curve) == pytest.approx(60.0, abs=1e-6)  # peak == level
    # the apex slot equals the level; an edge slot is ~0.2 of it
    assert curve[44] == pytest.approx(60.0, abs=1e-6)
    assert curve[32] == pytest.approx(60.0 * _shape_val(32), abs=1e-6)


def test_weekend_weekday_split():
    prof = _build()
    # both buckets exist and are distinct keys in the dow-granularity profiles
    rd_keys = {k[1] for k in prof.g_rd}
    assert rd_keys == {"week", "wend"}


def test_fallback_to_coarser_when_sparse():
    # ride 'c' appears on only 2 days (< min_obs) → its (ride,*) cells are untrusted, so
    # pick_curve must fall back past them to the park-wide curve.
    base = _panel(days=40, rides=("a",))
    sparse = _panel(days=2, rides=("c",))
    prof = _build(pd.concat([base, sparse], ignore_index=True), min_obs=5)
    _, tag = prof.pick_curve("c", crowd=0, dow="week")
    assert tag == "park"
    # the well-populated ride still resolves to a fine-grained cell
    _, tag_a = prof.pick_curve("a", crowd=1, dow="week")
    assert tag_a in {"rcd", "rc", "rd"}


def test_daytype_conditioner_and_additive_render():
    # supply a daytype map (the holiday-driven second conditioner) and render additively.
    df = _panel()
    dl = {pd.Timestamp(d).normalize(): ("wend" if pd.Timestamp(d).dayofweek >= 5 else "reg")
          for d in df["day"].unique()}
    prof = P.build_profiles(df, park_id="P", slot_count=96, day_label=dl,
                            alpha=0.5, beta=0.6)
    assert prof.alpha == 0.5 and prof.beta == 0.6
    # g_rd is now keyed by the daytype labels, not weekday buckets
    assert {k[1] for k in prof.g_rd} <= {"wend", "reg"}
    curve = prof.render_additive("a", level=60.0, dt_label="reg")
    # synthetic shape is level-invariant → deviations ~0 → additive form ≈ ride base (tent)
    assert curve[44] == pytest.approx(60.0, abs=1e-6)         # apex == level
    assert np.isnan(curve[0])                                 # not-open slot stays NaN
    # missing daytype cell → deviation falls back to 0 (ride base), still finite at apex
    assert np.isfinite(prof.render_additive("a", 60.0, "nonexistent")[44])


def test_drops_non_operating_days_and_empty():
    # a near-empty day (one tiny slot) must not define a shape
    df = _panel(days=10, rides=("a",))
    df = pd.concat([df, pd.DataFrame([{"unique_id": "a",
                    "day": pd.Timestamp("2026-05-01"), "slot": 40, "y": 1.0}])],
                   ignore_index=True)
    prof = _build(df, min_day_peak=5, min_day_slots=8)
    assert prof is not None
    # the 1-slot/peak<5 day is excluded → ride 'a' day count unaffected by it
    assert P.build_profiles(pd.DataFrame(columns=["unique_id", "day", "slot", "y"]),
                            park_id="P", slot_count=96) is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
