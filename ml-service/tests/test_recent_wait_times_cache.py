"""The in-process cache behind `fetch_recent_wait_times`.

The ml-service image ships no pytest, so this file doubles as a plain script:
`python3 tests/test_recent_wait_times_cache.py` runs the same assertions.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import predict


class VanishingDict(dict):
    """A cache that reports an entry a parallel worker has already removed.

    Gunicorn serves /predict from several workers over one module-level dict,
    so the list of expired keys can go stale between being collected and being
    acted on. This models exactly that window.
    """

    def __init__(self, *args, vanished_key="gone-by-now", **kwargs):
        super().__init__(*args, **kwargs)
        self._vanished_key = vanished_key

    def items(self):
        return list(super().items()) + [(self._vanished_key, (None, 0.0))]


def test_drops_expired_entries_and_keeps_fresh_ones():
    now = 1000.0
    cache = {"stale": ("frame-a", now - 500), "fresh": ("frame-b", now - 5)}

    predict._evict_expired_entries(cache, now, 60)

    assert "stale" not in cache
    assert "fresh" in cache


def test_survives_an_entry_that_vanishes_mid_eviction():
    """The production bug (2026-08-31): `del cache[k]` on a key another worker
    had already dropped raised KeyError out of `fetch_recent_wait_times` and
    killed the whole prediction request, which the API logged as
    `Failed to get predictions from ML service`.
    """
    now = 1000.0
    cache = VanishingDict({"stale": ("frame-a", now - 500)})

    predict._evict_expired_entries(cache, now, 60)

    assert "stale" not in cache


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    sys.exit(1 if failures else 0)
