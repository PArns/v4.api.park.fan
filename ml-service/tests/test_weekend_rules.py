"""PAR-1379: one weekend table for the Python services, the API's.

    cd ml-service && python3 -m pytest tests/test_weekend_rules.py -q
"""

import os
import re
import sqlite3
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import weekend_rules as wr

API_RULES = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "src", "date-features", "constants", "weekend-rules.constant.ts",
)


def _api_table():
    with open(API_RULES, encoding="utf-8") as fh:
        ts = fh.read()
    custom = {
        code: frozenset(int(d) for d in days.split(","))
        for code, days in re.findall(
            r'countryCode:\s*"([A-Z]{2})".*?weekendDays:\s*\[([\d,\s]+)\]', ts, re.DOTALL
        )
    }
    default = re.search(r"DEFAULT_WEEKEND_DAYS\s*=\s*\[([\d,\s]+)\]", ts).group(1)
    return custom, frozenset(int(d) for d in default.split(","))


def test_the_table_is_the_apis():
    if not os.path.exists(API_RULES):
        pytest.skip("the API source is not checked out next to ml-service")
    custom, default = _api_table()
    assert custom == {c: frozenset(d) for c, d in wr.CUSTOM_WEEKEND_DAYS.items()}
    assert default == frozenset(wr.DEFAULT_WEEKEND_DAYS)


def test_gulf_weekend_is_friday_and_saturday():
    # 2026-11-13 is a Friday; pandas counts it 4 (0 = Monday).
    fri, sat, sun = 4, 5, 6
    assert wr.is_weekend([fri, sat, sun], "SA").tolist() == [True, True, False]
    assert wr.is_weekend([fri, sat, sun], "DE").tolist() == [False, True, True]
    assert wr.is_weekend([fri, sat, sun], "AE").tolist() == [False, True, True]
    assert wr.is_weekend([fri, sat, sun], None).tolist() == [False, True, True]


def test_one_country_per_row():
    days = [4, 4, 4, 6, 6, np.nan]
    countries = ["SA", "il", None, "QA", float("nan"), "SA"]
    assert wr.is_weekend(days, countries).tolist() == [True, True, False, False, True, False]


@pytest.mark.parametrize("country", [*wr.CUSTOM_WEEKEND_DAYS, "DE", "US", None])
def test_sql_agrees_with_the_table(country):
    expr = wr.weekend_sql("c", "d")
    con = sqlite3.connect(":memory:")
    for sunday0 in range(7):
        (got,) = con.execute(f"SELECT {expr} FROM (SELECT ? AS c, ? AS d)", (country, sunday0)).fetchone()
        monday0 = (sunday0 + 6) % 7
        assert bool(got) == bool(wr.is_weekend([monday0], country)[0]), (country, sunday0)
