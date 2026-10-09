"""Day-type archetypes — collapse the calendar factors (weekend, public holiday, school
holiday/ferien, bridge day, peak season) into a SMALL number of buckets, so the shape's
second conditioner stays data-dense instead of exploding multiplicatively (the data-wall,
design §8a). Validated: this `daytype` beats a raw weekday/weekend split on busy/mid.

Priority order (a day gets the strongest archetype that applies):
    school > public/bridge holiday > weekend > peak-season weekday > regular weekday
"""

from __future__ import annotations

import pandas as pd

DAYTYPES = ("school", "pubhol", "wend", "peak", "reg")
PEAK_MONTHS = (6, 7, 8, 12)  # summer + Christmas; refined once a full year of data exists


# Non-ISO codes geocoding returns -> ISO 3166-2 suffix. Mirrors REGION_ALIASES in
# src/common/utils/region.util.ts and ml-service/holiday_utils.py.
REGION_ALIASES = {"NRW": "NW", "NDS": "NI", "England": "ENG", "Scotland": "SCT", "Wales": "WLS"}


def normalize_region(code: str | None) -> str | None:
    """'DE-NW' / 'NW' / 'NRW' -> 'NW'. The holidays table stores 'DE-NW' while
    parks.regionCode stores 'NW', so the two never compare equal raw (PAR-816)."""
    if not code or not isinstance(code, str):
        return None
    short = code.split("-")[-1]
    return REGION_ALIASES.get(short, short) or None


def build_daytype_fn(holidays: pd.DataFrame, region: str | None):
    """Return a function day -> daytype label, using nationwide + the park's-region holidays."""
    if holidays is None or holidays.empty:
        school = pubbr = set()
    else:
        park_region = normalize_region(region)
        holiday_region = holidays["region"].map(normalize_region)
        own_region = (
            holiday_region == park_region if park_region else pd.Series(False, index=holidays.index)
        )
        reg = holidays[holiday_region.isna() | own_region]
        school = set(reg.loc[reg["holiday_type"] == "school", "date"])
        pubbr = set(reg.loc[reg["holiday_type"].isin(["public", "bridge"]), "date"])

    def daytype(day) -> str:
        d = pd.Timestamp(day).normalize()
        if d in school:
            return "school"
        if d in pubbr:
            return "pubhol"
        if d.dayofweek >= 5:
            return "wend"
        if d.month in PEAK_MONTHS:
            return "peak"
        return "reg"

    return daytype


def daytype_map(holidays: pd.DataFrame, region: str | None, days) -> dict:
    """{day(Timestamp) -> label} for a set/iterable of park-local days."""
    fn = build_daytype_fn(holidays, region)
    return {pd.Timestamp(d): fn(d) for d in pd.unique(pd.Index(days))}
