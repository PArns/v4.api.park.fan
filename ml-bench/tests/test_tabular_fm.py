"""PAR-1405: tabular foundation models as the daily-level learner, and the retired arms."""

import importlib.util
import os

import duckdb
import numpy as np
import pandas as pd
import pytest

from mlbench.config import BenchConfig
from mlbench.models import REGISTRY, RETIRED, DriverLevel
from mlbench.models.neuralforecast_models import NFCached
from mlbench.models.tabular_fm import (BACKENDS, InprioraLevel, KumoBackend, KumoLevel, StubBackend,
                                       TabularFMLevel, _codes)
from mlbench.runner import Runner, load_model, retired_reason


class _Small:
    min_train_rows = 200
    settled_rows = 200


class _SmallKumo(_Small, KumoLevel):
    pass


class _SmallDriver(_Small, DriverLevel):
    pass


def test_arms_registered_and_servable():
    assert {"kumo_level", "inpriora_level"} <= set(REGISTRY)
    for cls in (KumoLevel, InprioraLevel):
        assert issubclass(cls, DriverLevel) and cls.licence == "servable" and cls.serving_candidate
        assert cls.scored_name() not in RETIRED
    assert set(BACKENDS) == {"stub", "kumo", "inpriora"}


def test_licence_gate_matches_the_foundation_arms():
    with pytest.raises(TypeError, match="serving candidate"):
        type("Bad", (TabularFMLevel,), {"name": "x_level", "licence": "unclear", "serving_candidate": True})
    with pytest.raises(TypeError, match="benchmark-only"):
        type("Relabelled", (TabularFMLevel,), {"name": "tabpfn_level", "licence": "servable"})


def test_codes_follow_the_context_categories():
    X = pd.DataFrame({"region": pd.Categorical(["EU", "NA", "MARS"]), "a": [1, None, 3]})
    Z = _codes(X, {"region": ["EU", "NA"]})
    assert Z["region"].tolist()[:2] == [0.0, 1.0] and np.isnan(Z["region"].iloc[2])
    assert np.isnan(Z["a"].iloc[1]) and Z["a"].dtype == "float64"


def test_stub_backend_learns_a_driver_shape():
    rng = np.random.default_rng(0)
    X = pd.DataFrame({"L": rng.integers(1, 90, 3000).astype(float),
                      "region": pd.Categorical(rng.choice(["EU", "NA"], 3000)),
                      "hol": rng.integers(0, 2, 3000).astype(float)})
    y = 0.4 * X["hol"].to_numpy() + np.where(X["region"] == "EU", 0.2, -0.2)
    b = StubBackend()
    b.fit(X.iloc[:2500], y[:2500], seed=830)
    assert np.abs(b.predict(X.iloc[2500:]) - y[2500:]).mean() < 0.05


def test_only_the_learner_differs_from_driver_level(synth_export, tmp_path, monkeypatch):
    """With driver_level's own learner swapped back in, the arm reproduces driver_level exactly:
    same rows, features, reference and H5 composition, so a margin is the learner alone."""
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    monkeypatch.setattr(_SmallKumo, "_learner", DriverLevel._learner)
    runs = {}
    for cls in (_SmallDriver, _SmallKumo):
        out = tmp_path / cls.__name__
        Runner(synth_export, out, cfg, [cls()], origins=("2026-04-20", "2026-04-20")).run()
        lv = duckdb.connect().execute(
            f"SELECT aid, date, L, lvl_{cls.name} AS lvl FROM '{out}/parts/levels/*.parquet' "
            f"WHERE lvl_{cls.name} IS NOT NULL ORDER BY 1, 2").df()
        runs[cls.name] = lv
    a, b = runs["driver_level"], runs["kumo_level"]
    assert len(a) > 0
    pd.testing.assert_frame_equal(a, b)


def test_context_is_a_seeded_capped_sample(synth_export, tmp_path):
    class Capped(_SmallKumo):
        context_rows = 150

    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1])
    m = Capped()
    out = tmp_path / "run"
    Runner(synth_export, out, cfg, [m], origins=("2026-04-20", "2026-04-20")).run()
    assert m.stats["fits"] == 1 and m.stats["context_rows"] == 150 and m._fit_rows > 150
    assert isinstance(m._engine(), StubBackend) and len(m._engine()._y) == 150
    assert m.stats["predict_rows"] > 0
    n = duckdb.connect().execute(
        f"SELECT count(lvl_kumo_level) FROM '{out}/parts/levels/*.parquet'").fetchone()[0]
    assert n > 0


def test_each_arm_owns_its_backend():
    a, b = _SmallKumo(), _SmallKumo()
    assert a._engine() is not b._engine()


def test_retired_arms_are_refused_without_the_flag():
    assert {"timesfm3", "timesfm3_owx", "toto2", "toto2_nocov", "toto2_grid", "tabpfn_ts",
            "nf_tide", "nf_tide_wx", "nf_nhits", "nf_tsmixerx"} == set(RETIRED)
    # every retired name is a real arm (no typo silently retiring nothing)
    nf_names = {c.name for c in NFCached.__subclasses__()} | {
        c.name for s in NFCached.__subclasses__() for c in s.__subclasses__()}
    assert set(RETIRED) <= set(REGISTRY) | nf_names
    # TiRex-2 is the kept fallback, Chronos-2 and the new arms are live candidates
    assert not {"tirex2", "tirex2_grid", "chronos2", "kumo_level", "inpriora_level"} & set(RETIRED)
    for spec in ("timesfm3", "tabpfn_ts", "mlbench.models.neuralforecast_models:NFTiDE"):
        with pytest.raises(SystemExit, match="retired"):
            load_model(spec)
    assert load_model("tabpfn_ts", allow_retired=True).name == "tabpfn_ts"
    assert retired_reason(REGISTRY["toto2_grid"]).startswith("PAR-853")
    assert retired_reason(REGISTRY["tirex2"]) is None


@pytest.mark.skipif(not (os.environ.get("MLBENCH_FM_REAL") and importlib.util.find_spec("sdm")),
                    reason="real Kumo-Tabular weights: MLBENCH_FM_REAL=1 with structured-data-models")
def test_kumo_backend_real_weights():
    rng = np.random.default_rng(1)
    X = pd.DataFrame({"L": rng.integers(1, 90, 1200).astype(float),
                      "region": pd.Categorical(rng.choice(["EU", "NA", "ASIA"], 1200)),
                      "log_ref": rng.normal(3, 0.5, 1200)})
    X.loc[rng.random(1200) < 0.1, "log_ref"] = np.nan
    y = 0.3 * np.nan_to_num(X["log_ref"].to_numpy() - 3) + np.where(X["region"] == "EU", 0.2, -0.1)
    b = KumoBackend(size=os.environ.get("MLBENCH_KUMO_SIZE", "small"), estimators=2)
    b.fit(X.iloc[:1000], y[:1000], seed=830)
    p = b.predict(X.iloc[1000:])
    assert p.shape == (200,) and np.isfinite(p).all()
    assert np.abs(p - y[1000:]).mean() < np.abs(y[1000:] - np.median(y[:1000])).mean()
