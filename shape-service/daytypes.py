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


def build_daytype_fn(holidays: pd.DataFrame, region: str | None):
    """Return a function day -> daytype label, using nationwide + the park's-region holidays."""
    if holidays is None or holidays.empty:
        school = pubbr = set()
    else:
        reg = holidays[
            holidays["region"].isna()
            | (holidays["region"] == "")
            | (holidays["region"] == region)
        ]
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
