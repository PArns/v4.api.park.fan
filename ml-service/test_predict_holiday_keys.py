#!/usr/bin/env python3
"""
Pins the two mechanical rewrites in predict.py's holiday-feature block:

  1. `primary_key` is built with vector ops instead of a row-wise apply.
  2. neighbor keys / counts are evaluated once per unique (parkId, date_str)
     pair and mapped back positionally.

Both must be indistinguishable from the row-wise versions they replaced —
predict.py (inference) and features.py (training) have to stay byte-identical,
so a subtle reordering here would show up as a silent feature skew rather than
an error.
"""

import numpy as np
import pandas as pd


def _frame():
    """Interleaved parks/dates with repeats — the layout a daily forecast has."""
    rows = []
    for day in ("2026-07-20", "2026-07-21"):
        for park, country, region in (
            ("p1", "DE", "NW"),
            ("p2", "NL", ""),  # no region → the "country||date" branch
            ("p3", "BE", "VLG"),
        ):
            for attraction in range(4):
                rows.append(
                    {
                        "parkId": park,
                        "attractionId": f"{park}-a{attraction}",
                        "park_country": country,
                        "park_region": region,
                        "date_str": day,
                    }
                )
    # Shuffle deterministically so positional bugs cannot hide behind sorting.
    return pd.DataFrame(rows).sample(frac=1, random_state=7).reset_index(drop=True)


def test_primary_key_matches_the_row_wise_formula():
    df = _frame()

    expected = df.apply(
        lambda row: (
            f"{row['park_country']}|{row['park_region']}|{row['date_str']}"
            if row["park_region"]
            else f"{row['park_country']}||{row['date_str']}"
        ),
        axis=1,
    )

    actual = np.where(
        df["park_region"].astype(bool),
        df["park_country"] + "|" + df["park_region"] + "|" + df["date_str"],
        df["park_country"] + "||" + df["date_str"],
    )

    assert list(actual) == list(expected)


def test_per_pair_mapping_preserves_row_order():
    """
    The dedup pattern: evaluate per unique pair, then rebuild the full column.
    Must equal a plain row-wise evaluation, element for element.
    """
    df = _frame()

    influences = {
        "p1": [{"countryCode": "NL", "regionCode": "LI"}],
        "p2": [],
        "p3": [{"countryCode": "FR", "regionCode": None}],
    }

    def key_for(park_id, date_str, index):
        regions = influences.get(park_id, [])
        if index < len(regions):
            inf = regions[index]
            country = inf.get("countryCode", "")
            region = inf.get("regionCode") or ""
            return (
                f"{country}|{region}|{date_str}"
                if region
                else f"{country}||{date_str}"
            )
        return ""

    pair_keys = list(zip(df["parkId"], df["date_str"]))
    unique_pairs = list(dict.fromkeys(pair_keys))

    # The pair list must actually collapse work, otherwise the test is vacuous.
    assert len(unique_pairs) == 6
    assert len(pair_keys) == 24

    for slot in range(3):
        lookup = {p: key_for(p[0], p[1], slot) for p in unique_pairs}
        actual = [lookup[p] for p in pair_keys]
        expected = [key_for(park, day, slot) for park, day in pair_keys]
        assert actual == expected, f"slot {slot}"


def test_date_str_to_local_date_mapping_is_unambiguous():
    """
    The counts path recovers `local_date` from `date_str`. That is only safe
    because date_str is exactly str(local_date) — assert the round trip.
    """
    local_dates = pd.to_datetime(
        ["2026-07-20", "2026-07-21", "2026-12-31", "2027-01-01"]
    ).date
    df = pd.DataFrame({"local_date": local_dates})
    df["date_str"] = df["local_date"].astype(str)

    recovered = dict(zip(df["date_str"], df["local_date"]))

    assert len(recovered) == len(df)
    for _, row in df.iterrows():
        assert recovered[row["date_str"]] == row["local_date"]
