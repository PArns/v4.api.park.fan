"""Park chunk membership is stable and holds only parks with a series (PAR-1054).

    cd nf-service && python3 -m pytest tests/test_park_chunks.py -q
"""

import importlib.util
import inspect
import os
import sys

NF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, NF_DIR)

# Other test modules replace `db` in sys.modules with a stub; load the real file.
_spec = importlib.util.spec_from_file_location("nf_db_chunks", os.path.join(NF_DIR, "db.py"))
db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(db)


def _chunks(ids, size=2):
    return [ids[i:i + size] for i in range(0, len(ids), size)]


def test_parks_are_ordered_by_country_then_id_unknown_country_last():
    rows = [
        {"pid": "f", "country": "DE"},
        {"pid": "a", "country": "US"},
        {"pid": "c", "country": None},
        {"pid": "b", "country": "DE"},
    ]
    assert db.order_parks(rows) == ["b", "f", "a", "c"]


def test_a_new_park_does_not_move_parks_of_earlier_countries():
    base = [{"pid": p, "country": c} for p, c in
            [("0a", "AT"), ("9z", "AT"), ("1b", "DE"), ("5e", "DE"), ("2c", "US")]]
    before = _chunks(db.order_parks(base))
    # In UUID order "00" would lead and shift every chunk; by country it lands in US.
    after = _chunks(db.order_parks(base + [{"pid": "00", "country": "US"}]))
    assert before[:2] == after[:2]


def test_the_park_list_requires_a_row_the_panel_could_keep():
    src = inspect.getsource(db.fetch_park_ids)
    assert "EXISTS" in src
    for predicate in ("qd.status = 'OPERATING'", "qd.\"queueType\" = 'STANDBY'",
                      "qd.\"waitTime\" >=", ":window"):
        assert predicate in src, predicate
