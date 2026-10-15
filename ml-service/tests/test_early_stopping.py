"""PAR-902: early stopping is wired up, and the metadata says whether it fired.

Every nightly run grew the full CATBOOST_ITERATIONS trees with its best
iteration on the last one, and the only record of that was a log line that a
redeploy deletes. These tests pin the wiring (an eval set that is not the
training set, the overfitting detector on the training loss, use_best_model) and
the `early_stopping` block in the model metadata.

The ml-service image ships no pytest, so this file doubles as a plain script:
`python3 tests/test_early_stopping.py` runs the same assertions.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import numpy as np
import pandas as pd

import model as model_module
from config import Settings
from features import get_categorical_features, get_feature_columns


def _frame(n, seed, signal=True):
    rng = np.random.default_rng(seed)
    cats = set(get_categorical_features())
    data = {}
    for col in get_feature_columns():
        if col in cats:
            data[col] = rng.choice(["a", "b", "c"], size=n)
        else:
            data[col] = rng.random(n)
    X = pd.DataFrame(data)
    if signal:
        y = 10 + 50 * X[get_feature_columns()[-1]] + rng.normal(0, 1, n)
    else:
        # Nothing to learn: every tree past the first few only fits noise.
        y = 30 + rng.normal(0, 10, n)
    return X, pd.Series(y)


class _Settings:
    def __init__(self, **overrides):
        self.overrides = overrides

    def __enter__(self):
        s = model_module.settings
        self.old = {k: getattr(s, k) for k in self.overrides}
        for k, v in {"CATBOOST_THREAD_COUNT": 2, **self.overrides}.items():
            self.old.setdefault(k, getattr(s, k))
            setattr(s, k, v)

    def __exit__(self, *exc):
        for k, v in self.old.items():
            setattr(model_module.settings, k, v)


def test_fit_watches_a_separate_eval_set_with_the_training_loss():
    with _Settings(CATBOOST_ITERATIONS=60, CATBOOST_EARLY_STOPPING_ROUNDS=17):
        X_tr, y_tr = _frame(400, 1)
        X_val, y_val = _frame(150, 2)
        m = model_module.WaitTimeModel("vtest")
        m.train(X_tr, y_tr, X_val, y_val)

        params = m.model.get_all_params()
        assert params["od_type"] == "Iter"
        assert params["od_wait"] == 17
        assert params["use_best_model"] is True
        # The detector watches the loss the model is trained on.
        assert params["eval_metric"] == params["loss_function"]
        # A validation curve exists, so fit() received an eval set.
        assert "validation" in m.model.get_evals_result()
        assert m.metadata["hyperparameters"]["early_stopping_rounds"] == 17


def test_early_stopping_fires_when_the_validation_loss_stops_improving():
    with _Settings(
        CATBOOST_ITERATIONS=1000,
        CATBOOST_LEARNING_RATE=0.3,
        CATBOOST_EARLY_STOPPING_ROUNDS=20,
    ):
        X_tr, y_tr = _frame(400, 3, signal=False)
        X_val, y_val = _frame(200, 4, signal=False)
        m = model_module.WaitTimeModel("vtest")
        m.train(X_tr, y_tr, X_val, y_val)

        es = m.metadata["early_stopping"]
        assert es["stopped_early"] is True
        assert es["trees_built"] < es["iterations_cap"] == 1000
        # Stopped exactly od_wait rounds after the best one ...
        assert es["trees_built"] == es["best_iteration"] + 1 + 20
        # ... and use_best_model cut the model back to that best one, which is
        # also the tree count the refit on all rows uses.
        assert es["tree_count"] == es["best_iteration"] + 1
        assert m.best_iteration_count() == es["tree_count"]
        assert es["best_validation_loss"] is not None


def test_metadata_says_so_when_the_cap_ends_the_fit():
    with _Settings(
        CATBOOST_ITERATIONS=30,
        CATBOOST_LEARNING_RATE=0.01,
        CATBOOST_EARLY_STOPPING_ROUNDS=100,
    ):
        X_tr, y_tr = _frame(400, 5)
        X_val, y_val = _frame(150, 6)
        m = model_module.WaitTimeModel("vtest")
        m.train(X_tr, y_tr, X_val, y_val)

        es = m.metadata["early_stopping"]
        assert es["stopped_early"] is False
        assert es["trees_built"] == es["iterations_cap"] == 30
        assert es["learning_rate"] == 0.01


def test_refit_never_early_stops():
    with _Settings(CATBOOST_ITERATIONS=40):
        X_tr, y_tr = _frame(300, 7)
        X_val, y_val = _frame(100, 8)
        m = model_module.WaitTimeModel("vtest")
        m.train(X_tr, y_tr, X_val, y_val)
        X_all = pd.concat([X_tr, X_val], ignore_index=True)
        y_all = pd.concat([y_tr, y_val], ignore_index=True)
        m.refit(X_all, y_all, iterations=25)
        # Exactly the tree count it was handed: no eval set, nothing to stop on.
        assert m.model.tree_count_ == 25
        # The early-stopping record describes the fit, not the refit.
        assert m.metadata["early_stopping"]["iterations_cap"] == 40


def test_defaults_leave_room_for_fit_and_refit():
    fields = Settings.model_fields
    lr = fields["CATBOOST_LEARNING_RATE"].default
    iterations = fields["CATBOOST_ITERATIONS"].default
    assert fields["CATBOOST_EARLY_STOPPING_ROUNDS"].default == 100
    # 0.03 x 2000 never converged (PAR-902); the cap times the step size is
    # what decides how far the model gets.
    assert lr == 0.1
    assert iterations == 1500
    # Both are paid twice (fit + refit); production measured ~0.84 s per tree
    # for the fit and ~1.07 s for the refit at 2000 trees.
    assert iterations * (0.84 + 1.07) < 60 * 60


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok {name}")
