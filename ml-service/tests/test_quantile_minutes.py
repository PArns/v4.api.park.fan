"""`quantile_minutes` hands the API the q0.8 / q0.95 it stores for live coverage (PAR-912).

The API measures P(actual <= predicted_q80) on these values, so the function must
return the quantile the model was TRAINED for and nothing standing in for it: a
neighbouring alpha would report the wrong coverage under the right name.

Like test_prediction_response_fields.py this runs as a plain script too
(`python3 tests/test_quantile_minutes.py`) and does not import predict.py, which
pulls in catboost and a database connection. It lifts the one function out with
`ast` and compiles it on its own.
"""

import __future__

import ast
import os
from typing import Any

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PREDICT = os.path.join(HERE, "..", "predict.py")


def _load():
    with open(PREDICT, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "quantile_minutes"
    )
    module = ast.Module(body=[fn], type_ignores=[])
    # Postponed annotations, so `int | None` compiles on an older local Python too.
    code = compile(module, PREDICT, "exec", flags=__future__.annotations.compiler_flag)
    ns = {"np": np, "Any": Any}
    exec(code, ns)  # noqa: S102 — our own source file, compiled in isolation
    return ns["quantile_minutes"]


quantile_minutes = _load()


def test_picks_the_trained_alphas_rounded_to_whole_minutes():
    q = {0.5: np.array([10.0, 3.2]), 0.8: np.array([17.4, 6.6]), 0.95: np.array([31.5, 9.49])}
    assert quantile_minutes(q, 0) == (17, 32)
    assert quantile_minutes(q, 1) == (7, 9)


def test_no_quantile_head_gives_none():
    assert quantile_minutes({}, 0) == (None, None)


def test_a_missing_alpha_is_none_never_a_neighbour():
    q = {0.5: np.array([10.0]), 0.9: np.array([20.0])}
    assert quantile_minutes(q, 0) == (None, None)


def test_non_finite_values_are_dropped():
    q = {0.8: np.array([np.nan]), 0.95: np.array([np.inf])}
    assert quantile_minutes(q, 0) == (None, None)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok — quantile_minutes")
