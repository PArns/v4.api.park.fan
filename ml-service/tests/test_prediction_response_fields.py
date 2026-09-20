"""Every key `predict.py` puts in a prediction must be declared on `PredictionResponse`.

`/predict` is served with `response_model=BulkPredictionResponse`, and each row goes
through `PredictionResponse(**p)`. Pydantic v2 defaults to `extra="ignore"`, so a key
that predict.py sets and this class does not declare is dropped from the response
without an error, a warning or a failing test. The NestJS side then reads `undefined`
and writes NULL, and nothing anywhere says so.

That is not hypothetical. `status` went that way for months: set on every row
(`predict.py`, the `results.append` block), undeclared here, so it never reached the
API and `MLService.storePredictions` wrote NULL into `wait_time_predictions.status` on
all 3157154 hourly rows. PAR-117 declared it and widened the feedback filter to keep
UNKNOWN, so the column now carries a value and the filter says what it means.

The ml-service image ships no pytest, so this file doubles as a plain script:
`python3 tests/test_prediction_response_fields.py` runs the same assertions. It parses
both files with `ast` rather than importing them, because importing predict.py pulls in
catboost, pandas and a database connection to check a question about field names.
"""

import ast
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
sys.path.insert(0, ROOT)

# Keys predict.py sets that PredictionResponse deliberately does not carry.
#
# Empty since PAR-117: `status` was the last entry and is now declared. Add a key
# here only with a reason, and delete the entry rather than the test once it is
# declared.
KNOWN_DROPPED = set()


def _prediction_dict_keys(path):
    """The literal keys of the dict appended to `results` in predict.py."""
    tree = ast.parse(open(path, encoding="utf-8").read())
    found = []
    for node in ast.walk(tree):
        # results.append({...})
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "append":
            continue
        if not isinstance(func.value, ast.Name) or func.value.id != "results":
            continue
        for arg in node.args:
            if isinstance(arg, ast.Dict):
                keys = [
                    k.value
                    for k in arg.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                ]
                if "predictedWaitTime" in keys:
                    found.append(keys)
    return found


def _class_field_names(path, class_name):
    """Annotated field names on a class, in declaration order."""
    tree = ast.parse(open(path, encoding="utf-8").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return [
                stmt.target.id
                for stmt in node.body
                if isinstance(stmt, ast.AnnAssign)
                and isinstance(stmt.target, ast.Name)
            ]
    raise AssertionError(f"class {class_name} not found in {path}")


def main():
    predict_py = os.path.join(ROOT, "predict.py")
    main_py = os.path.join(ROOT, "main.py")

    dicts = _prediction_dict_keys(predict_py)
    assert len(dicts) == 1, (
        f"expected exactly one prediction dict in predict.py, found {len(dicts)}. "
        "A second one would need the same check."
    )
    produced = set(dicts[0])
    declared = set(_class_field_names(main_py, "PredictionResponse"))

    dropped = produced - declared - KNOWN_DROPPED
    assert not dropped, (
        f"predict.py sets {sorted(dropped)}, which PredictionResponse does not "
        "declare. Pydantic drops them from the response silently — declare them "
        "or add them to KNOWN_DROPPED with a reason."
    )

    # The uncertainty band is the reason this test exists; pin it by name so a
    # refactor cannot quietly take it back out of either side.
    assert "uncertaintyMinutes" in produced, "predict.py stopped emitting the band"
    assert "uncertaintyMinutes" in declared, "PredictionResponse stopped carrying it"

    # `status` was the hole this test was written around (PAR-117). Both sides are
    # pinned by name now, because dropping it again would silently sharpen the
    # feedback filter in MLService.storePredictions — the same failure in reverse.
    assert "status" in produced, "predict.py stopped setting status"
    assert "status" in declared, "PredictionResponse stopped carrying status"

    print(f"ok — {len(produced)} keys produced, {len(declared)} declared")
    print(f"     band present on both sides, {sorted(KNOWN_DROPPED)} knowingly dropped")


if __name__ == "__main__":
    main()
