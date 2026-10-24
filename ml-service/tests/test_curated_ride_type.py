"""PAR-1045: curated attraction_kind / indoor_outdoor overrule the name
regexes where set, behind CATBOOST_CURATED_RIDE_TYPE, the same on both paths.

    cd ml-service && python3 -m pytest tests/test_curated_ride_type.py -q
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pandas as pd

import db
import features
import predict

FACTS = pd.DataFrame(
    {
        "attractionId": ["a", "b", "c", "d"],
        "attraction_kind": [None, "TRANSPORT", "RIDE", None],
        "indoor_outdoor": ["indoor", "outdoor", None, "covered_queue"],
    }
)


def _rows():
    return pd.DataFrame(
        {
            "attractionId": ["a", "b", "c", "d", "e"],
            "attractionName": [
                "Big Thunder Mountain",  # regex: coaster, not indoor
                "Haunted Railroad Express",  # regex: coaster + indoor
                "Wild Water Coaster",  # regex: coaster + water
                "Dark Ride Adventure",  # regex: indoor
                "Haunted Mansion",  # no curated row at all
            ],
        }
    )


def test_serving_and_training_share_one_definition():
    assert predict.add_attraction_type_features is features.add_attraction_type_features


def test_flag_off_is_the_name_heuristic(monkeypatch):
    monkeypatch.setattr(db, "fetch_curated_attraction_facts", lambda ids: FACTS)
    off = features.add_attraction_type_features(_rows())
    assert off["is_indoor"].tolist() == [0, 1, 0, 1, 1]
    assert off["is_coaster"].tolist() == [1, 1, 1, 0, 0]


def test_curated_values_overrule_only_where_set(monkeypatch):
    monkeypatch.setattr(db, "fetch_curated_attraction_facts", lambda ids: FACTS)
    on = features.add_attraction_type_features(_rows(), curated=True)
    # a: indoor curated; b: outdoor + transport; c: RIDE keeps the name's
    # flags; d: covered queue is not indoor; e: unjudged keeps the regex.
    assert on["is_indoor"].tolist() == [1, 0, 0, 0, 1]
    assert on["is_coaster"].tolist() == [1, 0, 1, 0, 0]
    assert on["is_water_ride"].tolist() == [0, 0, 1, 0, 0]


def test_coverage_counts_null_as_unjudged():
    assert features.curated_attraction_coverage(FACTS) == {
        "attractions": 4,
        "attraction_kind": 2,
        "indoor_outdoor": 3,
    }
