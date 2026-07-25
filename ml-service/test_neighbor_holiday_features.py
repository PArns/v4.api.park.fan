#!/usr/bin/env python3
"""
Equivalence tests for the influencing-region ("neighbor") holiday features.

Why this exists: the neighbor aggregation is evaluated once per unique
(parkId, date_local) pair instead of once per row. That is only valid because
the result depends on nothing else — `influencingRegions` comes from the park.
These tests pin that invariant, plus the exact per-case values, so the
train-time path cannot silently drift from what it produced before.

Related: the 2026-07-17 bug where influencing regions were capped at 3 and
country-level (null-region) entries were dropped entirely.
"""

from datetime import date, datetime

import pandas as pd

from features import add_holiday_features

# --- Fixtures -------------------------------------------------------------

PARK_BORDER = "park-border"  # 4 influencing regions incl. a null-region country
PARK_PLAIN = "park-plain"  # no influencing regions
PARK_DUPES = "park-dupes"  # duplicate influencing regions (must dedupe)

D1 = date(2026, 7, 20)  # NL school holiday + BE (any-region) school holiday
D2 = date(2026, 7, 21)  # nothing anywhere

PARKS_METADATA = pd.DataFrame(
    [
        {
            "park_id": PARK_BORDER,
            "country": "DE",
            "region_code": "NW",
            "influencingRegions": [
                {"countryCode": "NL", "regionCode": "LI"},
                {"countryCode": "NL", "regionCode": "GE"},
                {"countryCode": "BE", "regionCode": None},  # country-wide check
                {"countryCode": "FR", "regionCode": "IDF"},  # 4th slot
            ],
        },
        {
            "park_id": PARK_PLAIN,
            "country": "DE",
            "region_code": "NW",
            "influencingRegions": [],
        },
        {
            "park_id": PARK_DUPES,
            "country": "DE",
            "region_code": "NW",
            "influencingRegions": [
                {"countryCode": "NL", "regionCode": "LI"},
                {"countryCode": "NL", "regionCode": "LI"},  # exact duplicate
                {"countryCode": "NL", "regionCode": "NL-LI"},  # same after normalize
            ],
        },
    ]
)

HOLIDAYS = pd.DataFrame(
    [
        # NL-LI + NL-GE school holiday on D1
        {
            "date": D1,
            "country": "NL",
            "region": "NL-LI",
            "holiday_type": "school",
            "is_nationwide": False,
        },
        {
            "date": D1,
            "country": "NL",
            "region": "NL-GE",
            "holiday_type": "school",
            "is_nationwide": False,
        },
        # Belgium: regional only — matched via the country-wide "any region" set
        {
            "date": D1,
            "country": "BE",
            "region": "BE-VLG",
            "holiday_type": "school",
            "is_nationwide": False,
        },
        # France: a public holiday in the 4th slot — the old [:3] cap dropped it
        {
            "date": D1,
            "country": "FR",
            "region": "FR-IDF",
            "holiday_type": "public",
            "is_nationwide": False,
        },
    ]
)


def build_df(rows_per_pair: int) -> pd.DataFrame:
    """A frame with `rows_per_pair` identical rows per (park, date) pair."""
    records = []
    for park in (PARK_BORDER, PARK_PLAIN, PARK_DUPES):
        for day in (D1, D2):
            for i in range(rows_per_pair):
                records.append(
                    {
                        "parkId": park,
                        "attractionId": f"{park}-attr-{i}",
                        "timestamp": datetime(day.year, day.month, day.day, 10, i % 60),
                        "local_timestamp": datetime(
                            day.year, day.month, day.day, 10, i % 60
                        ),
                    }
                )
    return pd.DataFrame(records)


def run(rows_per_pair: int = 1) -> pd.DataFrame:
    df = build_df(rows_per_pair)
    return add_holiday_features(
        df,
        PARKS_METADATA,
        datetime(2026, 7, 1),
        datetime(2026, 7, 31),
        cached_holidays_df=HOLIDAYS.copy(),
    )


def pair_values(out: pd.DataFrame, park: str, day: date, column: str):
    """All values of `column` for one (park, date) pair."""
    mask = (out["parkId"] == park) & (out["date_local"] == day)
    return out.loc[mask, column].tolist()


# --- Tests ----------------------------------------------------------------


def test_counts_aggregate_over_all_influencing_regions():
    """NL-LI + NL-GE + BE(any-region) = 3 school; FR in slot 4 = 1 public."""
    out = run()
    assert pair_values(out, PARK_BORDER, D1, "neighbor_school_holiday_count") == [3]
    # holiday_count_total = primary public (0) + neighbor public (FR, 4th slot)
    assert pair_values(out, PARK_BORDER, D1, "holiday_count_total") == [1]
    assert pair_values(out, PARK_BORDER, D1, "is_school_holiday_any") == [1]


def test_duplicate_influencing_regions_are_counted_once():
    """Three entries all resolving to NL-LI must count as one."""
    out = run()
    assert pair_values(out, PARK_DUPES, D1, "neighbor_school_holiday_count") == [1]


def test_park_without_influencing_regions_stays_zero():
    out = run()
    assert pair_values(out, PARK_PLAIN, D1, "neighbor_school_holiday_count") == [0]
    assert pair_values(out, PARK_PLAIN, D1, "holiday_count_total") == [0]


def test_day_without_any_holiday_is_zero_everywhere():
    out = run()
    for park in (PARK_BORDER, PARK_PLAIN, PARK_DUPES):
        assert pair_values(out, park, D2, "neighbor_school_holiday_count") == [0]
        assert pair_values(out, park, D2, "holiday_count_total") == [0]
        assert pair_values(out, park, D2, "is_school_holiday_any") == [0]


def test_legacy_slot_flags_track_the_first_three_regions_only():
    """The three per-slot columns stay positional (slot 4 is counted, not flagged)."""
    out = run()
    # slots 1-3 are NL-LI, NL-GE, BE — all school, so no PUBLIC flag is set
    assert pair_values(out, PARK_BORDER, D1, "is_holiday_neighbor_1") == [0]
    assert pair_values(out, PARK_BORDER, D1, "is_holiday_neighbor_2") == [0]
    assert pair_values(out, PARK_BORDER, D1, "is_holiday_neighbor_3") == [0]


NEIGHBOR_COLUMNS = [
    "neighbor_school_holiday_count",
    "holiday_count_total",
    "school_holiday_count_total",
    "is_school_holiday_any",
    "is_holiday_neighbor_1",
    "is_holiday_neighbor_2",
    "is_holiday_neighbor_3",
]


def test_result_depends_only_on_park_and_date():
    """
    The invariant the per-pair evaluation relies on: every row of a
    (parkId, date_local) pair gets the SAME values, no matter how many rows
    that pair has.
    """
    single = run(rows_per_pair=1)
    many = run(rows_per_pair=25)

    for park in (PARK_BORDER, PARK_PLAIN, PARK_DUPES):
        for day in (D1, D2):
            for column in NEIGHBOR_COLUMNS:
                expected = pair_values(single, park, day, column)[0]
                actual = pair_values(many, park, day, column)
                assert len(actual) == 25, f"{park}/{day}/{column}"
                assert set(actual) == {expected}, (
                    f"{park}/{day}/{column}: expected all {expected}, got {set(actual)}"
                )


def test_row_order_is_preserved():
    """Mapping results back must not reorder or misalign rows."""
    out = run(rows_per_pair=3)
    expected_ids = build_df(3)["attractionId"].tolist()
    assert out["attractionId"].tolist() == expected_ids
