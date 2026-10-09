"""
Holiday features for one (park, local date) — the ONE implementation that both
training (features.py `add_holiday_features`) and inference (predict.py
`create_prediction_features`) call.

Why one implementation: the two paths used to be separate re-implementations
and had drifted apart (PAR-816):

* training merged the holidays table onto the frame and assigned the result
  back BY INDEX LABEL. The holidays table holds several rows for one
  (country, region, date) — CH-ZH 2025-04-21 five times, 4,569 such regional
  keys and 1,577 national ones in production — so the merge came back longer
  than the frame and every row after the first duplicate read ANOTHER row's
  holiday;
* inference built a dict and kept whichever row came last (database order);
* inference counted only `public` as a holiday where training counted
  public/bank/bridge, its neighbour slots skipped the national fallback, and
  the Easter Sunday fallback existed only in training.

The rules below are what both paths now share.

Semantics
---------
The holidays table is collapsed to FLAGS, never to one "type":

* ``public`` — any row of type public, bank or bridge (PUBLIC_HOLIDAY_TYPES);
* ``school`` — any row of type school.

Several rows for one key are combined with OR, so the row order the database
returns cannot change a feature, and a day that is both a public holiday and
inside a school break carries both flags (the old single-type pick had to drop
one of them). ``observance`` rows set neither flag.

For a location with a region the flags are regional OR national: a nationwide
public holiday that falls inside a regional school break is both. A location
without a region reads the national rows only. An influencing region given at
country level (``regionCode`` null, e.g. Belgium, whose school breaks are
regional) matches if ANY region of that country has the flag that day.

Easter Sunday is a public holiday for parks in EASTER_COUNTRIES: Nager.Date
lists it only for DE-BB, and it is one of the busiest days of the year.
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Iterable

import pandas as pd
from holiday_utils import normalize_region_code

PUBLIC_HOLIDAY_TYPES = frozenset({"public", "bank", "bridge"})
SCHOOL_HOLIDAY_TYPES = frozenset({"school"})
EASTER_COUNTRIES = frozenset({"DE", "AT", "CH", "NL", "BE", "FR", "PL", "CZ", "GB", "US"})

HOLIDAY_FEATURE_COLUMNS = [
    "is_holiday_primary",
    "is_school_holiday_primary",
    "is_holiday_neighbor_1",
    "is_holiday_neighbor_2",
    "is_holiday_neighbor_3",
    "neighbor_school_holiday_count",
    "holiday_count_total",
    "school_holiday_count_total",
    "is_school_holiday_any",
]

Flags = tuple[bool, bool]  # (public, school)
_NO_FLAGS: Flags = (False, False)


class HolidayIndex:
    """One (public, school) flag pair per key — duplicates already OR-combined."""

    def __init__(self, holidays_df: pd.DataFrame | None):
        self.regional: dict[tuple[str, str, _dt.date], Flags] = {}
        self.national: dict[tuple[str, _dt.date], Flags] = {}
        self.country_any_public: set[tuple[str, _dt.date]] = set()
        self.country_any_school: set[tuple[str, _dt.date]] = set()
        if holidays_df is None or holidays_df.empty:
            return

        h = pd.DataFrame(
            {
                "country": holidays_df["country"].to_numpy(),
                "date": pd.to_datetime(holidays_df["date"]).dt.date.to_numpy(),
                "region": holidays_df["region"].to_numpy(),
                "public": holidays_df["holiday_type"]
                .isin(PUBLIC_HOLIDAY_TYPES)
                .to_numpy(),
                "school": holidays_df["holiday_type"]
                .isin(SCHOOL_HOLIDAY_TYPES)
                .to_numpy(),
            }
        )
        region_present = h["region"].notna() & (h["region"].astype(str) != "")
        if "is_nationwide" in holidays_df.columns:
            nationwide = holidays_df["is_nationwide"].fillna(False).astype(bool).to_numpy()
        else:
            nationwide = ~region_present.to_numpy()
        # A row without a region is nationwide whatever the flag says.
        national_mask = nationwide | ~region_present.to_numpy()

        regional = h[region_present.to_numpy()].copy()
        if not regional.empty:
            regional["region"] = regional["region"].map(normalize_region_code)
            agg = regional.groupby(["country", "region", "date"], sort=False)[
                ["public", "school"]
            ].max()
            self.regional = {
                key: (bool(p), bool(s))
                for key, p, s in zip(agg.index, agg["public"], agg["school"])
            }

        national = h[national_mask]
        if not national.empty:
            agg = national.groupby(["country", "date"], sort=False)[
                ["public", "school"]
            ].max()
            self.national = {
                key: (bool(p), bool(s))
                for key, p, s in zip(agg.index, agg["public"], agg["school"])
            }

        any_country = h.groupby(["country", "date"], sort=False)[["public", "school"]].max()
        self.country_any_public = set(any_country.index[any_country["public"].to_numpy()])
        self.country_any_school = set(any_country.index[any_country["school"].to_numpy()])

    def flags(self, country, region_norm: str | None, day: _dt.date) -> Flags:
        """Regional OR national flags for one location and day."""
        national = self.national.get((country, day), _NO_FLAGS)
        if not region_norm:
            return national
        regional = self.regional.get((country, region_norm, day), _NO_FLAGS)
        return (regional[0] or national[0], regional[1] or national[1])

    def country_any(self, country, day: _dt.date) -> Flags:
        """Flags if ANY region of the country (or the country itself) has them."""
        return (
            (country, day) in self.country_any_public,
            (country, day) in self.country_any_school,
        )


def parse_influencing_regions(raw) -> list[dict]:
    """`influencingRegions` arrives as a list or as its JSON text."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if not isinstance(raw, list):
        return []
    return raw


def easter_sunday(year: int) -> _dt.date:
    """Anonymous Gregorian algorithm."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    ll = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ll) // 451
    month = (h + ll - 7 * m + 114) // 31
    day = ((h + ll - 7 * m + 114) % 31) + 1
    return _dt.date(year, month, day)


def park_day_holiday_features(
    country, region_code, influencing_regions: Iterable, day, index: HolidayIndex
) -> dict[str, int]:
    """All HOLIDAY_FEATURE_COLUMNS for one park on one local date."""
    out = dict.fromkeys(HOLIDAY_FEATURE_COLUMNS, 0)
    if day is None or pd.isna(day) or country is None or pd.isna(country):
        return out

    primary_public, primary_school = index.flags(
        country, normalize_region_code(region_code if isinstance(region_code, str) else None), day
    )
    if country in EASTER_COUNTRIES and day == easter_sunday(day.year):
        primary_public = True

    public_count = 0
    school_count = 0
    seen = set()
    for i, region_def in enumerate(influencing_regions):
        if not isinstance(region_def, dict):
            continue
        n_country = region_def.get("countryCode")
        n_region = normalize_region_code(region_def.get("regionCode") or None)
        if (n_country, n_region) in seen:
            continue
        seen.add((n_country, n_region))
        if n_region:
            n_public, n_school = index.flags(n_country, n_region, day)
        else:
            n_public, n_school = index.country_any(n_country, day)
        public_count += int(n_public)
        school_count += int(n_school)
        if i < 3:
            out[f"is_holiday_neighbor_{i + 1}"] = int(n_public)

    out["is_holiday_primary"] = int(primary_public)
    out["is_school_holiday_primary"] = int(primary_school)
    out["neighbor_school_holiday_count"] = school_count
    out["holiday_count_total"] = int(primary_public) + public_count
    out["school_holiday_count_total"] = int(primary_school) + school_count
    out["is_school_holiday_any"] = int(primary_school or school_count > 0)
    return out


def assign_holiday_features(
    df: pd.DataFrame,
    parks_metadata: pd.DataFrame,
    holidays_df: pd.DataFrame | None,
    park_col: str = "parkId",
    date_col: str = "date_local",
) -> pd.DataFrame:
    """Write HOLIDAY_FEATURE_COLUMNS onto `df`, row for row.

    Evaluated once per distinct (park, local date) — the features depend on
    nothing else — and mapped back by that key. The result is assigned by
    POSITION from a left merge on a de-duplicated right side, so the frame's
    index (RangeIndex or not) cannot shift a value onto another row.
    """
    days = pd.to_datetime(df[date_col], errors="coerce").dt.date
    keys = pd.DataFrame({"_park": df[park_col].to_numpy(), "_day": days.to_numpy()})
    pairs = keys.drop_duplicates()

    index = HolidayIndex(holidays_df)
    meta = {}
    if parks_metadata is not None and not parks_metadata.empty:
        for row in parks_metadata.drop_duplicates("park_id").itertuples(index=False):
            meta[row.park_id] = (
                row.country,
                row.region_code,
                parse_influencing_regions(getattr(row, "influencingRegions", None)),
            )

    records = []
    for park, day in zip(pairs["_park"], pairs["_day"]):
        country, region, influences = meta.get(park, (None, None, []))
        rec = park_day_holiday_features(country, region, influences, day, index)
        rec["_park"] = park
        rec["_day"] = day
        records.append(rec)
    per_pair = pd.DataFrame(records, columns=["_park", "_day", *HOLIDAY_FEATURE_COLUMNS])

    merged = keys.merge(per_pair, on=["_park", "_day"], how="left", validate="many_to_one")
    # A left merge onto a unique right side keeps the left row count and order.
    # If this ever fails, assigning by position below would mislabel rows.
    assert len(merged) == len(df), (
        f"holiday merge changed the row count: {len(df)} -> {len(merged)}"
    )
    for col in HOLIDAY_FEATURE_COLUMNS:
        df[col] = merged[col].fillna(0).astype(int).to_numpy()
    return df
