"""PAR-1049: a school break nobody has published yet is unknown, not "no break".

Past a location's last stored school-holiday row the school columns are NaN
(CatBoost reads NaN as missing); inside the coverage nothing changes, and
without a SchoolCoverage the features are what they always were.
"""

import datetime as dt
import math

import pandas as pd

from holiday_features import (
    SCHOOL_FEATURE_COLUMNS,
    HolidayIndex,
    SchoolCoverage,
    assign_holiday_features,
    park_day_holiday_features,
)

HOLIDAYS = pd.DataFrame(
    [
        # Spain: the summer break ends 2026-09-11, the public calendar reaches 2028.
        ("ES", "2026-09-11", "ES-CT", "school", False),
        ("ES", "2028-01-01", None, "public", True),
        ("FR", "2026-12-24", "FR-IDF", "school", False),
        ("SE", "2026-12-25", None, "public", True),  # Sweden: no school row at all
    ],
    columns=["country", "date", "region", "holiday_type", "is_nationwide"],
)
INDEX = HolidayIndex(HOLIDAYS)
COVERAGE = SchoolCoverage(HOLIDAYS)


def features(country, region, day, influencing=(), coverage=COVERAGE):
    return park_day_holiday_features(
        country, region, list(influencing), dt.date.fromisoformat(day), INDEX, coverage
    )


def test_coverage_is_per_region_and_school_only():
    assert COVERAGE.through("ES", "CT") == dt.date(2026, 9, 11)  # not the 2028 public row
    assert COVERAGE.through("ES", "MD") == dt.date(2026, 9, 11)  # unmatched region: country's
    assert COVERAGE.through("SE", None) is None


def test_inside_the_coverage_nothing_changes():
    on = features("ES", "ES-CT", "2026-09-11")
    after = features("ES", "ES-CT", "2026-09-11", coverage=None)
    assert on == after
    assert on["is_school_holiday_primary"] == 1


def test_past_the_coverage_school_columns_are_unknown():
    out = features("ES", "ES-CT", "2026-10-01")
    for col in ("is_school_holiday_primary", "school_holiday_count_total", "is_school_holiday_any"):
        assert math.isnan(out[col]), col
    assert out["is_holiday_primary"] == 0  # public flags stay facts


def test_a_country_without_school_data_is_unknown_every_day():
    assert math.isnan(features("SE", None, "2026-03-01")["is_school_holiday_primary"])


def test_an_unknown_neighbour_does_not_hide_a_known_break():
    neighbour = [{"countryCode": "ES", "regionCode": None}]
    # FR-IDF is covered and on a break; Spain is not covered on 2026-12-24.
    out = features("FR", "FR-IDF", "2026-12-24", neighbour)
    assert out["is_school_holiday_primary"] == 1
    assert math.isnan(out["neighbor_school_holiday_count"])
    assert math.isnan(out["school_holiday_count_total"])
    assert out["is_school_holiday_any"] == 1
    # Not on a break: the unknown neighbour makes "any" unknown too.
    assert math.isnan(features("FR", "FR-IDF", "2026-12-01", neighbour)["is_school_holiday_any"])


def test_assign_keeps_nan_with_coverage_and_ints_without():
    parks = pd.DataFrame(
        [{"park_id": "p", "country": "ES", "region_code": "ES-CT", "influencingRegions": []}]
    )
    df = pd.DataFrame({"parkId": ["p", "p"], "date_local": ["2026-09-11", "2026-10-01"]})
    out = assign_holiday_features(df.copy(), parks, HOLIDAYS, coverage=COVERAGE)
    assert out["is_school_holiday_primary"].iloc[0] == 1
    assert out["is_school_holiday_primary"].isna().tolist() == [False, True]
    legacy = assign_holiday_features(df.copy(), parks, HOLIDAYS)
    for col in SCHOOL_FEATURE_COLUMNS:
        assert legacy[col].dtype.kind == "i"
    assert legacy["is_school_holiday_primary"].tolist() == [1, 0]


def test_us_states_without_rows_are_unknown_not_covered_by_florida():
    # PAR-1071: US school data is curated per state; Ohio is not on Orlando's calendar.
    us = SchoolCoverage(
        pd.DataFrame(
            [("US", "2028-08-09", "US-FL", "school", False)],
            columns=["country", "date", "region", "holiday_type", "is_nationwide"],
        )
    )
    assert us.through("US", "FL") == dt.date(2028, 8, 9)
    assert us.through("US", "OH") is None
    assert us.through_any("US") is None
