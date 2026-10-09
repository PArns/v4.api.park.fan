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


def test_concurrent_reads_never_see_the_cache_change_size_mid_eviction():
    """PAR-815: the sync `def predict` handlers run in FastAPI's threadpool, so
    one thread could insert a bucket while another iterated `cache.items()` to
    evict, and the request died with "dictionary changed size during iteration"
    (five HTTP 500s in the logs). Two threads hammer `_cached_read` with fresh
    keys over a large cache, with the interpreter switching threads as often as
    it can, so the old code loses that race within a few calls.
    """
    import threading
    import time

    import pandas as pd

    now = time.time()
    cache = {f"seed-{i}": (None, now) for i in range(20_000)}
    frame = pd.DataFrame({"x": [1]})
    errors = []

    def hammer(tag):
        try:
            for i in range(60):
                predict._cached_read(cache, f"{tag}-{i}", lambda: frame)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        threads = [threading.Thread(target=hammer, args=(t,)) for t in "abcd"]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(old_interval)

    assert errors == [], errors
    assert all(f"{t}-59" in cache for t in "abcd")


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
