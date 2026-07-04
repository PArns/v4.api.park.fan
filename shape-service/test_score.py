"""Unit tests for the shape scorer's freeze-cutoff logic (no DB).

    cd shape-service && python3 -m pytest test_score.py -q
"""

import datetime as _dt

import pandas as pd

import score


def test_freeze_cutoff_freezes_days_that_left_the_easternmost_window():
    """A local date ages out of the per-park window east→west; the pooled board must stop
    (re)writing it once the easternmost park sheds it, else it collapses to the western
    subset. With the shape 96h lookback the last ~3 full days + today stay writable."""
    now = pd.Timestamp("2026-07-04 10:30", tz="UTC")
    # 96h window (+14h east margin) → cutoff 3 days back; today (07-04) stays writable.
    cutoff = score.freeze_cutoff_date(96, now_utc=now)
    assert cutoff <= _dt.date(2026, 7, 4)
    assert cutoff == _dt.date(2026, 7, 2)


def test_freeze_old_days_filter():
    """_freeze_old_days drops rows below the live cutoff, keeps cutoff-and-newer."""
    cutoff = score.freeze_cutoff_date(96)
    old = cutoff - _dt.timedelta(days=5)
    rows = [
        {"target_date": cutoff, "n": 1},
        {"target_date": cutoff + _dt.timedelta(days=1), "n": 1},
        {"target_date": old, "n": 1},
    ]
    kept = {r["target_date"] for r in score._freeze_old_days(rows, 96)}
    assert old not in kept and cutoff in kept


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-q"]))
