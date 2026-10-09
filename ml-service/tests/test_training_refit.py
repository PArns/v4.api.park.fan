"""PAR-815: the served model is refitted on ALL rows (train pool + the 30-day
hold-out) after the early-stopped fit has been scored on that hold-out.

The ml-service image ships no pytest, so this file doubles as a plain script:
`python3 tests/test_training_refit.py` runs the same assertions.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import numpy as np
import pandas as pd

import model as model_module
import train
from features import get_categorical_features, get_feature_columns


def _frame(n, seed):
    rng = np.random.default_rng(seed)
    cats = set(get_categorical_features())
    data = {}
    for col in get_feature_columns():
        if col in cats:
            data[col] = rng.choice(["a", "b", "c"], size=n)
        else:
            data[col] = rng.random(n)
    X = pd.DataFrame(data)
    y = pd.Series(10 + 50 * X[get_feature_columns()[-1]] + rng.normal(0, 1, n))
    return X, y


class _FewIterations:
    def __enter__(self):
        s = model_module.settings
        self._old = (s.CATBOOST_ITERATIONS, s.CATBOOST_THREAD_COUNT)
        s.CATBOOST_ITERATIONS = 300
        s.CATBOOST_THREAD_COUNT = 2

    def __exit__(self, *exc):
        s = model_module.settings
        s.CATBOOST_ITERATIONS, s.CATBOOST_THREAD_COUNT = self._old


def test_refit_uses_the_early_stopped_tree_count_on_all_rows():
    with _FewIterations():
        X_tr, y_tr = _frame(600, 1)
        X_val, y_val = _frame(200, 2)
        m = model_module.WaitTimeModel("vtest")
        metrics = m.train(X_tr, y_tr, X_val, y_val)
        trees = m.best_iteration_count()
        assert trees == m.model.get_best_iteration() + 1

        X_all = pd.concat([X_tr, X_val], ignore_index=True)
        y_all = pd.concat([y_tr, y_val], ignore_index=True)
        m.refit(X_all, y_all, iterations=trees)

        assert m.model.tree_count_ == trees
        assert m.metadata["refit"]["rows"] == 800
        assert m.metadata["final_train_samples"] == 800
        # The gate still reads the early-stopped fit's validation metrics.
        assert m.metadata["metrics"]["mae"] == metrics["mae"]
        assert m.metadata["train_samples"] == 600
        assert len(m.predict(X_val)) == 200


def test_holdout_summary_reports_overall_and_busy_mae():
    y_true = [10, 20, 60, 90]
    y_pred = [12, 20, 50, 80]
    s = train.holdout_summary(y_true, y_pred)
    assert s["mae"] == (2 + 0 + 10 + 10) / 4
    assert s["busy_mae"] == 10
    assert s["samples"] == 4
    assert s["busy_samples"] == 2


def test_holdout_summary_without_busy_rows():
    s = train.holdout_summary([5, 10], [6, 9])
    assert s["busy_mae"] is None
    assert s["busy_samples"] == 0


def test_sample_weights_match_the_accuracy_and_busy_formulas():
    s = train.settings
    old = (s.ENABLE_SAMPLE_WEIGHTS, s.CATBOOST_BUSY_WEIGHT, s.SAMPLE_WEIGHT_FACTOR)
    s.ENABLE_SAMPLE_WEIGHTS, s.CATBOOST_BUSY_WEIGHT, s.SAMPLE_WEIGHT_FACTOR = (
        True,
        True,
        0.5,
    )
    try:
        df = pd.DataFrame(
            {"attractionId": ["r1", "r2", "r3"], "waitTime": [20.0, 80.0, 5.0]}
        )
        stats = pd.DataFrame(
            {"attraction_id": ["r1", "r1", "r2"], "mae": [40.0, 40.0, 4.0]}
        )
        w = train.compute_sample_weights(df, stats)
        accuracy = np.array([2.0, 1.1, 1.25])  # r3 unknown → MAE 10
        busy = np.clip((df["waitTime"].values / 20.0) ** 0.5, 0.4, 2.5)
        assert np.allclose(w, accuracy * busy)
        # Same rows, same weights, whichever frame they sit in.
        assert np.allclose(train.compute_sample_weights(df.iloc[::-1], stats), w[::-1])
    finally:
        s.ENABLE_SAMPLE_WEIGHTS, s.CATBOOST_BUSY_WEIGHT, s.SAMPLE_WEIGHT_FACTOR = old


def test_refit_is_on_by_default():
    from config import Settings

    assert Settings.model_fields["TRAIN_REFIT_ON_ALL_ROWS"].default is True


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
