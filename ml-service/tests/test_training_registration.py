"""PAR-815: a training run may only announce a version that exists, and the
row cap that bounds training time may not drop a season.

The ml-service image ships no pytest, so this file doubles as a plain script:
`python3 tests/test_training_registration.py` runs the same assertions.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import joblib
import numpy as np
import pandas as pd

import model as model_module
import train
import train_standalone


class _ModelDir:
    """Points MODEL_DIR at a fresh temp directory for the duration of a test."""

    def __enter__(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old = model_module.settings.MODEL_DIR
        model_module.settings.MODEL_DIR = self._tmp.name
        return self._tmp.name

    def __exit__(self, *exc):
        model_module.settings.MODEL_DIR = self._old
        self._tmp.cleanup()


def _save_fake_model(model_dir, version, mae=4.433):
    with open(os.path.join(model_dir, f"catboost_{version}.cbm"), "wb") as f:
        f.write(b"cbm")
    joblib.dump(
        {
            "metrics": {"mae": mae},
            "train_samples": 10,
            "val_samples": 2,
            "training_timings": {"fetch": {"seconds": 1.0, "rows": 12}},
        },
        os.path.join(model_dir, f"metadata_{version}.pkl"),
    )


# --- load_saved_metadata ---------------------------------------------------


def test_saved_metadata_is_read_from_the_versions_own_files():
    with _ModelDir() as d:
        _save_fake_model(d, "v20261006_0600", mae=4.433)
        _save_fake_model(d, "v20261003_0600", mae=5.06)
        meta = model_module.load_saved_metadata("v20261006_0600")
        assert meta["metrics"]["mae"] == 4.433


def test_saved_metadata_needs_both_files():
    with _ModelDir() as d:
        joblib.dump({"metrics": {"mae": 4.4}}, os.path.join(d, "metadata_v1.pkl"))
        assert model_module.load_saved_metadata("v1") is None
        assert model_module.load_saved_metadata("v-missing") is None


def test_saved_metadata_refuses_path_like_versions():
    with _ModelDir():
        for bad in ["../etc/passwd", "a/b", "..", "", ".hidden"]:
            assert model_module.load_saved_metadata(bad) is None, bad


# --- train_standalone ------------------------------------------------------


def _run_standalone(fake_train, version="v20261009_0600"):
    """Run train_standalone.main() with train.train_model replaced."""
    with _ModelDir() as d:
        status_file = os.path.join(d, "training_status.json")
        sentinel_file = os.path.join(d, "active_version.txt")
        original = train.train_model
        train.train_model = lambda version: fake_train(d, version)
        old_argv = sys.argv
        sys.argv = ["train_standalone.py", version, status_file, sentinel_file]
        try:
            code = train_standalone.main()
        finally:
            sys.argv = old_argv
            train.train_model = original
        with open(status_file) as f:
            status = json.load(f)
        return code, status, os.path.exists(sentinel_file)


def test_early_return_is_a_failure_and_writes_no_sentinel():
    """train_model returns None when it finds no data or an empty training set.
    That used to be reported as "completed" with a sentinel for a version that
    has no file — a phantom every worker retried loading on each request."""
    code, status, sentinel = _run_standalone(lambda d, v: None)
    assert code == 1
    assert status["status"] == "failed"
    assert "stopped early" in status["error"]
    assert sentinel is False


def test_missing_model_file_is_a_failure_and_writes_no_sentinel():
    code, status, sentinel = _run_standalone(lambda d, v: {"mae": 4.4})
    assert code == 1
    assert status["status"] == "failed"
    assert sentinel is False


def test_saved_model_completes_without_activating_itself():
    """Training only saves files and status. The sentinel is written by
    /model/reload once the API has registered the version and it passed the
    gate, so a rejected or timed-out run never changes what is served."""

    def fake_train(d, v):
        _save_fake_model(d, v)
        return {"mae": 4.433}

    code, status, sentinel = _run_standalone(fake_train)
    assert code == 0
    assert status["status"] == "completed"
    assert sentinel is False
    # The per-phase timings saved with the model reach the training status.
    assert status["timings"] == {"fetch": {"seconds": 1.0, "rows": 12}}


# --- cap_training_rows -----------------------------------------------------


def _pool(days=300, rows_per_day=100):
    ts = pd.date_range("2025-12-24", periods=days, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": np.repeat(ts, rows_per_day),
            "waitTime": np.arange(days * rows_per_day),
        }
    )


def test_cap_is_off_by_default():
    """The owner's call: train on the full pool. The cap is opt-in until an
    out-of-time A/B (full vs capped) shows it costs no accuracy."""
    from config import Settings

    assert Settings.model_fields["TRAIN_MAX_ROWS"].default == 0


def test_cap_is_a_no_op_under_the_budget_or_when_disabled():
    df = _pool(days=10)
    assert train.cap_training_rows(df, 10_000, 90, 42) is df
    assert train.cap_training_rows(df, 0, 90, 42) is df


def test_cap_keeps_recent_days_whole_and_every_month_represented():
    df = _pool(days=300, rows_per_day=100)  # 30 000 rows
    capped = train.cap_training_rows(df, 15_000, 90, 42)

    assert len(capped) == 15_000
    cutoff = df["timestamp"].max() - pd.Timedelta(days=90)
    assert (capped["timestamp"] >= cutoff).sum() == (df["timestamp"] >= cutoff).sum()

    months_before = set(df["timestamp"].dt.strftime("%Y-%m"))
    months_after = set(capped["timestamp"].dt.strftime("%Y-%m"))
    assert months_after == months_before

    # Same seed, same rows: two runs on one day train on the same pool.
    again = train.cap_training_rows(df, 15_000, 90, 42)
    assert again["waitTime"].tolist() == capped["waitTime"].tolist()


def test_cap_samples_everything_when_the_recent_block_alone_is_over_budget():
    df = _pool(days=300, rows_per_day=100)
    capped = train.cap_training_rows(df, 5_000, 90, 42)  # 91 recent days = 9 100
    assert len(capped) == 5_000
    assert capped["timestamp"].min() < df["timestamp"].max() - pd.Timedelta(days=90)


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
