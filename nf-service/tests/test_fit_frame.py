"""Series that cannot form a training window stay out of the fit.

    cd nf-service && python3 -m pytest tests/test_fit_frame.py -q
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import forecast


def _panel(lengths, mask=None):
    rows = []
    for uid, n in lengths.items():
        for d in range(n):
            row = {"unique_id": uid, "ds": pd.Timestamp("2026-10-01") + pd.Timedelta(days=d), "y": 1.0}
            if mask is not None:
                row["available_mask"] = mask[uid][d]
            rows.append(row)
    return pd.DataFrame(rows)


def test_one_day_series_is_left_out_of_the_fit():
    out = forecast.fit_frame(_panel({"new": 1, "two": 2, "old": 30}))
    assert sorted(out["unique_id"].unique()) == ["old", "two"]
    assert len(out) == 32


def test_grid_panel_counts_observed_days_not_rows():
    # Grid rows exist for every calendar day; only available_mask days are observed.
    mask = {"gappy": [0, 0, 1, 0], "ok": [1, 0, 1, 0]}
    out = forecast.fit_frame(_panel({"gappy": 4, "ok": 4}, mask))
    assert list(out["unique_id"].unique()) == ["ok"]
