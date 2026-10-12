"""Day-type region matching (no DB).

    cd shape-service && python3 -m pytest test_daytypes.py -q

PAR-816: the holidays table stores regions as 'DE-NW', parks.regionCode as 'NW'.
Compared raw they never matched, so a regional school break never produced the
'school' day type for any park.
"""

import datetime as _dt

import daytypes
import pandas as pd


def _holidays():
    return pd.DataFrame(
        [
            {"date": pd.Timestamp("2026-07-14"), "region": "DE-NW", "holiday_type": "school"},
            {"date": pd.Timestamp("2026-07-15"), "region": "DE-BY", "holiday_type": "school"},
            {"date": pd.Timestamp("2026-10-03"), "region": None, "holiday_type": "public"},
        ]
    )


def test_prefixed_holiday_region_matches_the_parks_short_code():
    fn = daytypes.build_daytype_fn(_holidays(), "NW")
    assert fn(_dt.date(2026, 7, 14)) == "school"


def test_other_regions_do_not_leak_in():
    fn = daytypes.build_daytype_fn(_holidays(), "NW")
    assert fn(_dt.date(2026, 7, 15)) == "peak"  # a Wednesday in July


def test_nationwide_rows_apply_without_a_region():
    fn = daytypes.build_daytype_fn(_holidays(), None)
    assert fn(_dt.date(2026, 7, 14)) == "peak"
    assert fn(_dt.date(2026, 10, 3)) in ("pubhol", "wend")


def test_aliases_match_the_api():
    assert daytypes.normalize_region("NRW") == "NW"
    assert daytypes.normalize_region("DE-NW") == "NW"
    assert daytypes.normalize_region("England") == "ENG"
    assert daytypes.normalize_region(None) is None
