"""The weekend of a park's country, for every weekday/weekend split the Python
services make (PAR-1379).

The table is the API's ``CUSTOM_WEEKEND_RULES``
(``src/date-features/constants/weekend-rules.constant.ts``, a test compares the
two) and counts the days the same way, which is also Postgres'
``EXTRACT(DOW …)``: 0 = Sunday … 6 = Saturday. pandas' ``dayofweek`` and
``date.weekday()`` count from Monday; ``is_weekend`` takes those. A country the
table does not list, and a park without a country, has Saturday + Sunday.

ml-service owns this file. nf-service and pcn-service carry byte-identical
copies, because each service builds its own image; a test in each compares them.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

#: 0 = Sunday, 6 = Saturday.
DEFAULT_WEEKEND_DAYS: tuple[int, ...] = (0, 6)

#: Only the countries whose weekend is not Saturday + Sunday. The UAE moved to
#: Saturday + Sunday in 2022 and is not listed.
CUSTOM_WEEKEND_DAYS: dict[str, tuple[int, ...]] = {
    "SA": (5, 6),
    "KW": (5, 6),
    "QA": (5, 6),
    "BH": (5, 6),
    "OM": (5, 6),
    "EG": (5, 6),
    "JO": (5, 6),
    "IQ": (5, 6),
    "SY": (5, 6),
    "YE": (5, 6),
    "IL": (5, 6),
    "IR": (5,),
    "AF": (4, 5),
    "BN": (5, 0),
    "NP": (6,),
}


def weekend_days(country) -> tuple[int, ...]:
    """The country's weekend days, 0 = Sunday."""
    if not isinstance(country, str):
        return DEFAULT_WEEKEND_DAYS
    return CUSTOM_WEEKEND_DAYS.get(country.strip().upper(), DEFAULT_WEEKEND_DAYS)


def is_weekend(weekday, country=None) -> np.ndarray:
    """Whether each ``weekday`` (0 = Monday) is a weekend day, as a bool array.

    ``country`` is one ISO code for every row, or one per row (array-like of the
    same length; None / NaN reads as no country). A missing weekday is not a
    weekend day."""
    day = np.asarray(weekday, dtype=float)
    sunday0 = np.where(np.isfinite(day), (np.nan_to_num(day) + 1) % 7, -1).astype(int)
    if country is None or isinstance(country, str):
        return np.isin(sunday0, weekend_days(country))
    codes = pd.Categorical(np.asarray(country, dtype=object))
    out = np.isin(sunday0, DEFAULT_WEEKEND_DAYS)
    for i, code in enumerate(codes.categories):
        rows = codes.codes == i
        out[rows] = np.isin(sunday0[rows], weekend_days(code))
    return out


def weekend_sql(country: str, dow: str) -> str:
    """A Postgres boolean: is ``dow`` (an ``EXTRACT(DOW …)`` expression,
    0 = Sunday) a weekend day in ``country`` (an expression holding the ISO code)?
    Rendered from the table above only, never from input."""
    by_days: dict[tuple[int, ...], list[str]] = {}
    for code, days in CUSTOM_WEEKEND_DAYS.items():
        by_days.setdefault(tuple(sorted(days)), []).append(code)

    def days_in(days) -> str:
        listed = ", ".join(str(d) for d in sorted(days))
        return f"{dow} IN ({listed})"

    def codes_in(codes) -> str:
        listed = ", ".join(f"'{c}'" for c in codes)
        return f"UPPER({country}) IN ({listed})"

    whens = " ".join(
        f"WHEN {codes_in(codes)} THEN {days_in(days)}" for days, codes in by_days.items()
    )
    return f"(CASE {whens} ELSE {days_in(DEFAULT_WEEKEND_DAYS)} END)"
