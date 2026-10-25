"""Write a CatBoost model + joblib metadata pickle with one venv, read it with another.

usage: python artefact_compat.py write DIR | read DIR  (see docs/development/python-dependencies.md)
"""
import json, sys
from pathlib import Path
import joblib, numpy as np, pandas as pd, sklearn
from catboost import CatBoostRegressor, Pool

mode, out = sys.argv[1], Path(sys.argv[2])
rng = np.random.default_rng(7)
n = 2000
X = pd.DataFrame({
    "hour": rng.integers(8, 22, n), "dow": rng.integers(0, 7, n),
    "temp": rng.normal(18, 6, n).round(1),
    "attractionId": rng.choice([f"a{i}" for i in range(40)], n),
    "parkId": rng.choice(["p1", "p2", "p3"], n),
})
y = 10 + 3 * X["hour"] + 5 * (X["dow"] >= 5) + rng.normal(0, 4, n)
cat = ["attractionId", "parkId"]
probe = X.iloc[:200]
if mode == "write":
    out.mkdir(parents=True, exist_ok=True)
    m = CatBoostRegressor(iterations=60, depth=4, loss_function="MultiQuantile:alpha=0.5,0.8,0.95",
                          random_seed=1, verbose=False, thread_count=2)
    m.fit(Pool(X, y, cat_features=cat))
    m.save_model(str(out / "model.cbm"))
    meta = {
        "features_used": list(X.columns), "categorical_features": cat,
        "metrics": {"mae": np.float64(4.2), "n": np.int64(n), "q": np.array([0.5, 0.8, 0.95])},
        "feature_stats": X.describe(), "means": X[["hour", "temp"]].mean(),
        "trained_at": pd.Timestamp("2026-10-24T06:00:00Z"), "dtypes": X.dtypes,
        "versions": {"numpy": np.__version__, "pandas": pd.__version__, "sklearn": sklearn.__version__},
    }
    joblib.dump(meta, out / "metadata.pkl")
    np.save(out / "pred.npy", m.predict(probe))
    print("wrote with", meta["versions"])
else:
    m = CatBoostRegressor(); m.load_model(str(out / "model.cbm"))
    meta = joblib.load(out / "metadata.pkl")
    ref = np.load(out / "pred.npy")
    got = m.predict(probe[meta["features_used"]])
    assert type(meta["metrics"]["mae"]) is np.float64 and meta["metrics"]["n"] == n
    assert isinstance(meta["feature_stats"], pd.DataFrame) and meta["trained_at"].tz is not None
    print(json.dumps({"written_with": meta["versions"],
                      "read_with": {"numpy": np.__version__, "pandas": pd.__version__},
                      "max_abs_diff": float(np.abs(got - ref).max()), "identical": bool(np.array_equal(got, ref))}))
