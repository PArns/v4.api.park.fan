"""Holiday covariates: region matching (no DB — the engine is created lazily-connected).

    cd nf-service && python3 -m pytest test_calendar_covariates.py -q

PAR-816: influencingRegions carries 'DE-NW' while the holiday side was normalised
to 'NW', so no regional neighbour ever matched and is_holiday_neighbor only fired
for country-wide entries.
"""

import db
import pandas as pd


def _frame():
    return pd.DataFrame(
        {"unique_id": ["a1", "a1"], "ds": pd.to_datetime(["2026-07-14", "2026-07-15"])}
    )


def _meta(influencing):
    return pd.DataFrame(
        [
            {
                "unique_id": "a1",
                "park_id": "p1",
                "country": "DE",
                "region": "RP",
                "influencing": influencing,
            }
        ]
    )


HOLIDAYS = pd.DataFrame(
    [
        {"date": pd.Timestamp("2026-07-14"), "country": "DE", "region": "DE-NW", "holiday_type": "school"},
        {"date": pd.Timestamp("2026-07-15"), "country": "DE", "region": "DE-RP", "holiday_type": "school"},
    ]
)


def test_prefixed_influencing_region_matches():
    out = db.add_calendar_covariates(
        _frame(), _meta([{"countryCode": "DE", "regionCode": "DE-NW"}]), HOLIDAYS
    )
    assert out["is_holiday_neighbor"].tolist() == [1, 0]
    assert out["is_school_holiday"].tolist() == [0, 1]


def test_alias_and_short_codes_match_too():
    for code in ("NW", "NRW"):
        out = db.add_calendar_covariates(
            _frame(), _meta([{"countryCode": "DE", "regionCode": code}]), HOLIDAYS
        )
        assert out["is_holiday_neighbor"].tolist() == [1, 0], code


def test_regional_neighbour_picks_up_its_countrys_national_holidays():
    """NL-LI must see Koningsdag, which is stored nationally as (NL, None)."""
    holidays = pd.DataFrame(
        [
            {"date": pd.Timestamp("2026-04-27"), "country": "NL", "region": None, "holiday_type": "public"},
            {"date": pd.Timestamp("2026-04-28"), "country": "BE", "region": None, "holiday_type": "public"},
        ]
    )
    frame = pd.DataFrame(
        {"unique_id": ["a1", "a1"], "ds": pd.to_datetime(["2026-04-27", "2026-04-28"])}
    )
    out = db.add_calendar_covariates(
        frame, _meta([{"countryCode": "NL", "regionCode": "NL-LI"}]), holidays
    )
    # NL national counts for the NL-LI neighbour; BE is not a neighbour.
    assert out["is_holiday_neighbor"].tolist() == [1, 0]


def test_norm_region():
    assert db._norm_region("DE-NW") == "NW"
    assert db._norm_region("NDS") == "NI"
    assert db._norm_region("") is None
    assert db._norm_region(None) is None
