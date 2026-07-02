"""Unit tests for the shadow scorer's pure aggregation (no DB).

    cd pcn-service && python3 -m pytest test_score.py -q
"""

import pandas as pd
import pytest

import score


def _row(rows, model, segment, lead):
    for r in rows:
        if r["model"] == model and r["segment"] == segment and r["lead_bucket"] == lead:
            return r
    return None


def _df():
    return pd.DataFrame([
        # 2026-06-01
        {"target_slot": pd.Timestamp("2026-06-01 10:00"), "lead_h": 1.0,
         "actual": 10.0, "pcn": 12.0, "catboost": 9.0},     # quiet
        {"target_slot": pd.Timestamp("2026-06-01 10:15"), "lead_h": 2.0,
         "actual": 70.0, "pcn": 60.0, "catboost": 50.0},    # busy
        # 2026-06-02
        {"target_slot": pd.Timestamp("2026-06-02 10:00"), "lead_h": 5.0,
         "actual": 40.0, "pcn": 44.0, "catboost": 30.0},    # mid
    ])


def test_aggregate_overall_mae_bias():
    rows = score.aggregate_comparison(_df(), models=["pcn", "catboost"])
    r = _row(rows, "pcn", "all", "all")  # only the 2026-06-01 group has 2 rows
    # grouped per target_date: 06-01 has the two slots
    assert r["target_date"] == pd.Timestamp("2026-06-01").date()
    assert r["n"] == 2
    assert r["mae"] == pytest.approx(6.0)    # mean(|12-10|, |60-70|)
    assert r["bias"] == pytest.approx(-4.0)  # mean(+2, -10)


def test_aggregate_segments_and_models():
    rows = score.aggregate_comparison(_df(), models=["pcn", "catboost"])
    assert _row(rows, "pcn", "quiet", "all")["n"] == 1
    assert _row(rows, "pcn", "quiet", "all")["mae"] == pytest.approx(2.0)
    assert _row(rows, "pcn", "busy", "all")["mae"] == pytest.approx(10.0)
    # CatBoost on the same matched 06-01 population
    cb = _row(rows, "catboost", "all", "all")
    assert cb["n"] == 2
    assert cb["mae"] == pytest.approx(10.5)   # mean(|9-10|, |50-70|)
    assert cb["bias"] == pytest.approx(-10.5)


def test_aggregate_lead_buckets():
    rows = score.aggregate_comparison(_df(), models=["pcn"])
    d1, d2 = pd.Timestamp("2026-06-01").date(), pd.Timestamp("2026-06-02").date()
    # 06-01 slots are lead 1h and 2h → both <=3h, none in 3-6h
    r1_le3 = [x for x in rows if x["target_date"] == d1
              and x["lead_bucket"] == "<=3h" and x["segment"] == "all"][0]
    assert r1_le3["n"] == 2
    assert not [x for x in rows if x["target_date"] == d1 and x["lead_bucket"] == "3-6h"]
    # 06-02 slot is lead 5h → 3-6h bucket
    r2 = [x for x in rows if x["target_date"] == d2
          and x["lead_bucket"] == "3-6h" and x["segment"] == "all"][0]
    assert r2["n"] == 1


def test_aggregate_drops_nan_actual_and_empty():
    df = _df()
    df.loc[0, "actual"] = float("nan")
    rows = score.aggregate_comparison(df, models=["pcn"])
    # the dropped quiet row → no quiet segment on 06-01
    assert _row(rows, "pcn", "quiet", "all") is None
    assert score.aggregate_comparison(pd.DataFrame(), ["pcn"]) == []


def test_serve_round_mirrors_catboost_serving():
    """Parity with ml-service round_to_nearest_5 + min-10 (predict.py:1959-1969):
    half-up 5er steps, floor of 10 for positive values, 0 stays 0. The board scores
    PCN through the same boundary that serves it."""
    import numpy as np

    x = np.array([0.0, 2.4, 2.5, 7.2, 7.5, 12.4, 23.0, 34.7])
    out = score.serve_round(x)
    np.testing.assert_array_equal(out, [0, 0, 10, 10, 10, 10, 25, 35])


def test_full_day_window_covers_only_full_days():
    """The full-day contract: with a 48h lookback the window starts at YESTERDAY 00:00
    (the first fully-covered local day) — never mid-day — so per-target_date upserts
    are always supersets of the previous write instead of shrinking slices."""
    now = pd.Timestamp("2026-07-02 14:37")
    lo, hi = score.full_day_window(now, 48, "15min")
    assert lo == pd.Timestamp("2026-07-01 00:00")      # yesterday, complete
    assert hi == pd.Timestamp("2026-07-02 14:30")      # current partial slot excluded
    # 96h lookback (shape default) → last 3 full days
    lo96, _ = score.full_day_window(now, 96, "15min")
    assert lo96 == pd.Timestamp("2026-06-29 00:00")


def test_full_day_window_never_starts_after_today():
    """A short lookback (<24h) can't cover any full past day — the window degrades to
    today-so-far instead of starting in the future."""
    now = pd.Timestamp("2026-07-02 06:10")
    lo, hi = score.full_day_window(now, 6, "15min")
    assert lo == pd.Timestamp("2026-07-02 00:00")
    assert hi == pd.Timestamp("2026-07-02 06:00")


def test_full_day_window_at_midnight_run():
    """Run right after midnight: yesterday just became a matured date and is still
    fully covered by the 48h window (its final, complete rewrite)."""
    now = pd.Timestamp("2026-07-03 00:05")
    lo, hi = score.full_day_window(now, 48, "15min")
    assert lo == pd.Timestamp("2026-07-02 00:00")
    assert hi == pd.Timestamp("2026-07-03 00:00")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
