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


def test_one_unit_gives_a_zero_width_ci_that_is_not_a_win():
    """With a single bootstrap unit the resampled ratio is weight-independent, so every
    replicate equals the point estimate: the CI collapses and `significant` says yes
    for free. `significant_diff` must refuse it (critic S5)."""
    from mlbench.stats import Bootstrapper, significant_diff

    b = Bootstrapper(reps=200, seed=3)
    v, lo, hi, n = b.diff(["p1"], [90.0], [10.0], [100.0], [10.0])
    assert n == 1
    assert lo == hi == v < 0                       # zero width, "excludes 0"
    assert significant(lo, hi)                     # the raw rule is fooled
    assert not significant_diff(lo, hi, True, n)   # the guarded rule is not
    # a real sample with real between-unit spread is still a win
    rng = np.random.default_rng(5)
    base = rng.gamma(2, 5, 60)
    keys = [f"p{i}" for i in range(60)]
    v, lo, hi, n = b.diff(keys, base * 10, np.full(60, 10.0), (base + 1) * 10, np.full(60, 10.0))
    assert n == 60 and lo < hi and significant_diff(lo, hi, True, n)
    # ... but the same gap on 5 units is refused for too few units
    v, lo, hi, n = b.diff(keys[:5], base[:5] * 10, np.full(5, 10.0), (base[:5] + 1) * 10,
                          np.full(5, 10.0))
    assert n == 5 and not significant_diff(lo, hi, True, n)


def _ref_base(mae: dict[str, list[float]]) -> "object":
    """One cell: 40 park-days, one row per model (own figure) and one per ordered
    (model, ref) pair, all on the same rows so everything is perfectly paired."""
    import pandas as pd

    parks = [f"p{i}" for i in range(40)]
    rows = []
    for m, vals in mae.items():
        for p, v in zip(parks, vals):
            rows.append(dict(park_id=p, date="2026-08-15", model=m, ref=m,
                             num_m=v * 10, den_m=10.0, num_r=0.0, den_r=0.0))
    for m, mv in mae.items():
        for r, rv in mae.items():
            if m == r:
                continue
            for p, a, b in zip(parks, mv, rv):
                rows.append(dict(park_id=p, date="2026-08-15", model=m, ref=r,
                                 num_m=a * 10, den_m=10.0, num_r=b * 10, den_r=10.0))
    return pd.DataFrame(rows)


def test_reference_is_the_ladder_unless_a_candidate_significantly_beats_it():
    """B1: a 0.07 min gap inside the CI must NOT move the display reference off the
    ladder; a consistent, significant gap must. And a win is against the whole
    envelope, so it cannot be bought by the reference moving to a weaker candidate."""
    from mlbench.config import BenchConfig
    from mlbench.report import ALL_REF_ROWS, REF_CHOICES, evaluate

    cfg = BenchConfig(bootstrap_reps=300)
    rng = np.random.default_rng(11)
    noise = rng.normal(0, 2.0, 40)
    keys = {"uc": "UC3", "lead": 7, "segment": "all", "region": "all", "metric": "MAE"}

    # near-tie: clim is 0.07 better on average, swamped by between-park spread
    near = {"wt_med": list(8.45 + noise), "clim": list(8.38 + noise + rng.normal(0, 2.0, 40)),
            "h5": list(8.31 + noise)}
    ALL_REF_ROWS.clear(), REF_CHOICES.clear()
    rows = evaluate(_ref_base(near), cfg, ["snaive7", "wt_med", "clim"], True, keys)
    assert {r["ref"] for r in rows} == {"wt_med"}, "a near-tie must not move the reference"
    assert "ladder order" in REF_CHOICES[0]["reason"]
    assert next(r for r in rows if r["model"] == "h5")["envelope"] == "wt_med/clim"
    # every (model, candidate) pair is published, not just the selected one
    pairs = {(r["model"], r["ref_candidate"]) for r in ALL_REF_ROWS}
    assert ("h5", "wt_med") in pairs and ("h5", "clim") in pairs
    assert ("wt_med", "clim") in pairs and ("clim", "wt_med") in pairs
    # the mirrored pair is the exact negation
    fwd = next(r for r in ALL_REF_ROWS if (r["model"], r["ref_candidate"]) == ("wt_med", "clim"))
    rev = next(r for r in ALL_REF_ROWS if (r["model"], r["ref_candidate"]) == ("clim", "wt_med"))
    assert fwd["diff_vs_ref"] == -rev["diff_vs_ref"]
    assert fwd["diff_lo_park"] == -rev["diff_hi_park"]

    # real gap: clim is 1 min better on EVERY park-day -> it takes the reference
    clear = {"wt_med": list(8.45 + noise), "clim": list(7.45 + noise), "h5": list(8.31 + noise)}
    ALL_REF_ROWS.clear(), REF_CHOICES.clear()
    rows = evaluate(_ref_base(clear), cfg, ["snaive7", "wt_med", "clim"], True, keys)
    assert {r["ref"] for r in rows} == {"clim"}
    assert "significantly beaten by" in REF_CHOICES[0]["reason"]
    # h5 is 0.14 better than wt_med but 0.86 WORSE than clim, so it must not win:
    # the envelope is a conjunction, and the display reference cannot buy a win
    h5 = next(r for r in rows if r["model"] == "h5")
    assert h5["envelope"] == "wt_med/clim"
    assert h5["hardest_naive"] == "clim" and h5["diff_vs_hardest"] > 0
    assert h5["wins"] is False
    # clim itself beats the envelope of the OTHER candidates, which is a real finding
    cl = next(r for r in rows if r["model"] == "clim")
    assert cl["envelope"] == "wt_med" and cl["wins"] is True


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
