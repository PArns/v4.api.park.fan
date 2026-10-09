#!/usr/bin/env python3
"""
PAR-816: the holidays table holds several rows for one (country, region, date)
— CH-ZH 2025-04-21 five times in production — and one (country, date) can carry
several national rows (KR, HK, CN, JP, US). The training path merged that table
onto the feature frame and assigned the merge result back by index label; the
merge came back longer than the frame, so every row after the first duplicate
read ANOTHER row's holiday.

These tests pin the replacement (holiday_features.assign_holiday_features, used
by training AND inference): one flag pair per key, OR-combined, assigned by
position. `test_rows_after_a_duplicate_keep_their_own_holiday` fails on the old
features.py.
"""

from datetime import date, datetime

import pandas as pd
from features import add_holiday_features
from holiday_features import (
    HOLIDAY_FEATURE_COLUMNS,
    HolidayIndex,
    assign_holiday_features,
)
from holiday_utils import normalize_region_code

PARK_ZH = "park-zh"  # CH / ZH — the duplicated regional key
PARK_NW = "park-nw"  # DE / NW — plain, one row per key
PARK_KR = "park-kr"  # KR, no region — duplicated national key

DUP_DAY = date(2025, 4, 21)
SCHOOL_DAY = date(2025, 4, 22)
NW_DAY = date(2025, 4, 23)
KR_DAY = date(2025, 5, 5)
QUIET_DAY = date(2025, 4, 24)

PARKS = pd.DataFrame(
    [
        {"park_id": PARK_ZH, "country": "CH", "region_code": "ZH", "influencingRegions": []},
        {
            "park_id": PARK_NW,
            "country": "DE",
            "region_code": "NW",
            # A regional neighbour with no regional row that day: the national
            # row must still count (inference used to skip that fallback).
            "influencingRegions": [{"countryCode": "NL", "regionCode": "NL-LI"}],
        },
        {"park_id": PARK_KR, "country": "KR", "region_code": None, "influencingRegions": "[]"},
    ]
)


def _h(day, country, region, kind, nationwide=False):
    return {
        "date": day,
        "country": country,
        "region": region,
        "holiday_type": kind,
        "is_nationwide": nationwide,
    }


HOLIDAYS = pd.DataFrame(
    [
        # CH-ZH: three rows for ONE key, as production has it.
        _h(DUP_DAY, "CH", "CH-ZH", "observance"),
        _h(DUP_DAY, "CH", "CH-ZH", "public"),
        _h(DUP_DAY, "CH", "CH-ZH", "school"),
        _h(SCHOOL_DAY, "CH", "CH-ZH", "school"),
        # DE-NW: a bank holiday (training counted it, inference did not).
        _h(NW_DAY, "DE", "DE-NW", "bank"),
        # NL nationwide public holiday on NW_DAY, no NL-LI row.
        _h(NW_DAY, "NL", None, "public", nationwide=True),
        # KR: two national rows for one day.
        _h(KR_DAY, "KR", None, "public", nationwide=True),
        _h(KR_DAY, "KR", None, "observance", nationwide=True),
    ]
)

EXPECTED = {
    # (park, day): (is_holiday_primary, is_school_holiday_primary, is_holiday_neighbor_1)
    (PARK_ZH, DUP_DAY): (1, 1, 0),
    (PARK_ZH, SCHOOL_DAY): (0, 1, 0),
    (PARK_ZH, QUIET_DAY): (0, 0, 0),
    (PARK_NW, NW_DAY): (1, 0, 1),
    (PARK_NW, QUIET_DAY): (0, 0, 0),
    (PARK_KR, KR_DAY): (1, 0, 0),
    (PARK_KR, QUIET_DAY): (0, 0, 0),
}


def _frame(rows_per_pair: int = 3) -> pd.DataFrame:
    records = []
    for (park, day) in EXPECTED:
        for i in range(rows_per_pair):
            ts = datetime(day.year, day.month, day.day, 10 + i)
            records.append(
                {
                    "parkId": park,
                    "attractionId": f"{park}-{i}",
                    "timestamp": ts,
                    "local_timestamp": ts,
                }
            )
    return pd.DataFrame(records)


def _run_training(df: pd.DataFrame, holidays: pd.DataFrame = HOLIDAYS) -> pd.DataFrame:
    return add_holiday_features(
        df,
        PARKS,
        datetime(2025, 4, 1),
        datetime(2025, 5, 31),
        cached_holidays_df=holidays.copy(),
    )


def _assert_expected(out: pd.DataFrame):
    days = pd.to_datetime(out["local_timestamp"]).dt.date
    for (park, day), (pub, school, n1) in EXPECTED.items():
        rows = out[(out["parkId"] == park).to_numpy() & (days == day).to_numpy()]
        assert len(rows) > 0, (park, day)
        got = set(
            zip(
                rows["is_holiday_primary"],
                rows["is_school_holiday_primary"],
                rows["is_holiday_neighbor_1"],
            )
        )
        assert got == {(pub, school, n1)}, f"{park} {day}: {got}"


def test_rows_after_a_duplicate_keep_their_own_holiday():
    """Fails on the old merge-and-assign-by-label code: the CH-ZH triple shifts
    every later row onto another row's holiday."""
    df = _frame()
    out = _run_training(df)
    assert len(out) == len(df)
    _assert_expected(out)


def test_row_order_and_count_are_preserved():
    df = _frame()
    out = _run_training(df)
    assert out["attractionId"].tolist() == df["attractionId"].tolist()
    assert out["local_timestamp"].tolist() == df["local_timestamp"].tolist()


def test_holiday_row_order_does_not_change_a_feature():
    """Old inference kept whichever duplicate came last from the database."""
    df = _frame()
    forward = _run_training(df.copy())
    backward = _run_training(df.copy(), HOLIDAYS.iloc[::-1].reset_index(drop=True))
    for col in HOLIDAY_FEATURE_COLUMNS:
        assert forward[col].tolist() == backward[col].tolist(), col


def test_inference_entry_point_matches_training_on_a_non_range_index():
    """predict.py calls assign_holiday_features on a frame whose index is not a
    RangeIndex (it is restored via df.loc[original_index]). Shuffled labels
    must not move a value onto another row, and the values must equal training."""
    df = _frame()
    training = _run_training(df.copy())

    shuffled = df.sample(frac=1, random_state=3)
    shuffled.index = shuffled.index * 7 + 100  # labels that are not positions
    shuffled["local_date"] = shuffled["local_timestamp"].dt.date
    inference = assign_holiday_features(
        shuffled, PARKS, HOLIDAYS.copy(), park_col="parkId", date_col="local_date"
    )

    training = training.set_index("attractionId").assign(ts=training["local_timestamp"].to_numpy())
    for _, row in inference.iterrows():
        match = training[
            (training.index == row["attractionId"]) & (training["ts"] == row["local_timestamp"])
        ]
        assert len(match) == 1
        for col in HOLIDAY_FEATURE_COLUMNS:
            assert match[col].iloc[0] == row[col], (row["attractionId"], col)


def test_easter_sunday_counts_on_both_paths():
    easter = date(2025, 4, 20)
    df = pd.DataFrame(
        [
            {
                "parkId": PARK_NW,
                "attractionId": "a",
                "timestamp": datetime(2025, 4, 20, 12),
                "local_timestamp": datetime(2025, 4, 20, 12),
            }
        ]
    )
    out = _run_training(df)
    assert out["is_holiday_primary"].tolist() == [1]
    assert out["holiday_count_total"].tolist() == [1]
    df["local_date"] = [easter]
    out = assign_holiday_features(df, PARKS, HOLIDAYS.copy(), date_col="local_date")
    assert out["is_holiday_primary"].tolist() == [1]


def test_index_collapses_duplicates_with_or():
    index = HolidayIndex(HOLIDAYS)
    assert index.flags("CH", "ZH", DUP_DAY) == (True, True)
    assert index.flags("KR", None, KR_DAY) == (True, False)
    assert index.flags("NL", "LI", NW_DAY) == (True, False)  # national fallback


def test_region_aliases_match_the_api():
    """Same table as src/common/utils/region.util.ts."""
    assert normalize_region_code("NRW") == "NW"
    assert normalize_region_code("NDS") == "NI"
    assert normalize_region_code("England") == "ENG"
    assert normalize_region_code("GB-ENG") == "ENG"
    assert normalize_region_code("DE-NW") == "NW"
    assert normalize_region_code(None) is None
