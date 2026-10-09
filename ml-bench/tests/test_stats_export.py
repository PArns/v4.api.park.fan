import datetime as dt
import re

import numpy as np

from mlbench import export
from mlbench.stats import paired_diff_ci, ratio_ci, significant


def test_ratio_ci_contains_point_and_shrinks():
    rng = np.random.default_rng(0)
    num = rng.normal(10, 2, 500)
    den = np.ones(500)
    p, lo, hi = ratio_ci(num, den, reps=400)
    assert lo < p < hi
    p2, lo2, hi2 = ratio_ci(np.tile(num, 4), np.tile(den, 4), reps=400)
    assert (hi2 - lo2) < (hi - lo)


def test_paired_diff_detects_a_real_gap_only():
    rng = np.random.default_rng(1)
    base = rng.gamma(2, 5, 800)
    n = np.full(800, 10.0)
    _, lo, hi = paired_diff_ci(base * 10, n, (base + 1) * 10, n, reps=400)
    assert significant(lo, hi)              # model is 1 min better on every park-day
    _, lo, hi = paired_diff_ci(base * 10, n, base * 10, n, reps=400)
    assert not significant(lo, hi)


def test_export_queries_are_bounded_selects():
    q = export.queue_query(dt.date(2026, 7, 15))
    assert "2026-07-15 00:00:00+00" in q and "2026-07-16 00:00:00+00" in q
    for sql in list(export.STATIC_QUERIES.values()) + [q, export.forecast_query("tft_forecasts",
                                                                              dt.date(2026, 6, 1),
                                                                              dt.date(2026, 7, 1), 120)]:
        head = sql.strip().split()[0].upper()
        assert head == "SELECT"
        for verb in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"):
            assert not re.search(rf"\b{verb}\b", sql.upper())
    assert export.month_starts(dt.date(2026, 3, 5), dt.date(2026, 5, 2)) == [
        (dt.date(2026, 3, 1), dt.date(2026, 4, 1)), (dt.date(2026, 4, 1), dt.date(2026, 5, 1)),
        (dt.date(2026, 5, 1), dt.date(2026, 6, 1))]
