"""Every key `predict.py` puts in a prediction must be declared on `PredictionResponse`.

`/predict` is served with `response_model=BulkPredictionResponse`, and each row goes
through `PredictionResponse(**p)`. Pydantic v2 defaults to `extra="ignore"`, so a key
that predict.py sets and this class does not declare is dropped from the response
without an error, a warning or a failing test. The NestJS side then reads `undefined`
and writes NULL, and nothing anywhere says so.

That is not hypothetical. `status` is set on every row (`predict.py`, the
`results.append` block) and is NOT declared on `PredictionResponse`, so it never
reaches the API. `MLService.storePredictions` writes `pred.status || null` into a
column that is therefore always NULL, and the feedback filter one screen below it —
`pred.status === "OPERATING" || pred.status === null`, commented "excluding scheduled
closures" — passes everything. See the KNOWN_DROPPED note below.

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
# `parkId` IS declared and does reach the client. `status` is the open bug described
# above: declaring it would silently activate the feedback filter and drop UNKNOWN
# rows from accuracy scoring, which is a metrics change and not this test's business.
# It is recorded in the frontend repo's todo.md; when it is fixed, delete the entry
# rather than the test.
KNOWN_DROPPED = {"status"}


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

    # And guard the known hole itself: if `status` ever gets declared, this test
    # should be updated deliberately rather than keep an obsolete exception.
    assert "status" in produced, "predict.py no longer sets status — drop KNOWN_DROPPED"
    assert "status" not in declared, (
        "status is now declared on PredictionResponse. That activates the feedback "
        "filter in MLService.storePredictions; remove it from KNOWN_DROPPED and "
        "check what it does to accuracy coverage."
    )

    print(f"ok — {len(produced)} keys produced, {len(declared)} declared")
    print(f"     band present on both sides, {sorted(KNOWN_DROPPED)} knowingly dropped")


if __name__ == "__main__":
    main()
