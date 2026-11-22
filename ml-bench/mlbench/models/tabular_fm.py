"""Plug-in: tabular foundation models as the learner of the daily level (PAR-1405).

TabArena (live leaderboard v0.1.9.22, read 2026-11-21) ranks in-context tabular foundation
models far above tuned gradient-boosted trees on regression, but only on small IID datasets
(at most ~36k training rows, random splits). BeyondArena (NeurIPS 2026) finds them competitive on
temporal data up to ~100k rows and behind trees on large or grouped data. The one model in our
stack inside that regime is the daily level of ``driver_level``: ride-day rows, at most 300,000,
refit every 28 days. The hourly CatBoost frame (4.44 M rows) is not.

So each arm here is ``driver_level`` with ONE change, the learner of ``log(true P90 / naive)``:
the same training rows (ride-day x lead), the same features, the same naive reference and the
same H5 composition. The gradient-boosted model is replaced by an in-context forward pass whose
context is a seeded sample of those rows (``context_rows``, ``MLBENCH_TFM_CONTEXT_ROWS``, default
50,000; Kumo-Tabular was pretrained on contexts up to 60,000 rows and 100 columns). A margin
against ``driver_level`` is therefore the learner and nothing else.

Arms (licences and the selection rule: docs/ml/foundation-models/tabular.md):

* ``kumo_level``: NVIDIA Kumo-Tabular, ``large`` (215.7 M parameters). Code Apache-2.0, weights
  OpenMDW-1.1, so it is servable. structured-data-models 0.3.0 pins the weights to revision
  ``v1.0.1``. A regression returns 999 quantiles ``q001`` ... ``q999``, and the level uses
  ``q500``: the median, as ``driver_level``'s absolute-error loss.
* ``inpriora_level``: Ant Group's Inpriora regressor (121 M). Code and weights Apache-2.0, so
  servable; weights pinned to Hugging Face revision ``MLBENCH_INPRIORA_REVISION``. Its inference
  code has no PyPI release (GitHub only); the image extra pins the commit that was reviewed.

Both run on the GPU when there is one (VRAM capped like the other foundation arms,
``MLBENCH_FM_VRAM_FRACTION``) and fall back to CPU. ``MLBENCH_TFM_BACKEND=stub`` swaps the learner
for a deterministic k-nearest-neighbour stand-in. The test suite does that (``tests/conftest.py``),
so the information-cut test covers these arms without weights.
"""

from __future__ import annotations

import os
import time
from typing import ClassVar

import numpy as np
import pandas as pd

from .. import drivers as D
from ..licences import is_benchmark_only
from .driver_level import FEATURES, DriverLevel

LICENCES = ("servable", "benchmark-only", "unclear")
#: Hugging Face revision of ai-for-data/inpriora (2026-11-17)
INPRIORA_REVISION = os.environ.get("MLBENCH_INPRIORA_REVISION", "8c41fb1a17dc775cf5c2d76bbe91097d55a040c9")
#: query rows per forward pass (bounds GPU memory; the context is encoded once per fit).
#: 20,000 ran Inpriora out of the 9.3 GB cap on the RTX 5080 at the April origins (PAR-1141).
QUERY_CHUNK = int(os.environ.get("MLBENCH_TFM_QUERY_CHUNK", "4000"))


# --------------------------------------------------------------------------- backends

class _Backend:
    """``fit(X, y, seed)`` stores the context, ``predict(X)`` returns the point forecast."""

    name = "backend"

    def fit(self, X: pd.DataFrame, y: np.ndarray, seed: int) -> None:
        raise NotImplementedError

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        raise NotImplementedError


def _codes(X: pd.DataFrame, cats: dict[str, list]) -> pd.DataFrame:
    """Categoricals as integer codes of the context's categories (unseen = NaN), floats else."""
    out = pd.DataFrame(index=X.index)
    for c in X.columns:
        if c in cats:
            out[c] = pd.Categorical(X[c].astype(str), categories=cats[c]).codes.astype("float64")
            out.loc[out[c] < 0, c] = np.nan
        else:
            out[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    return out


class StubBackend(_Backend):
    """Deterministic stand-in for tests and CPU wiring: k-nearest neighbours on z-scored
    features (categoricals as codes, NaN as the context median), median of the neighbours."""

    name = "stub"
    k = 15

    def fit(self, X, y, seed):
        self._cats = {c: sorted(X[c].astype(str).unique()) for c in X.columns if c in D.CATEGORICAL}
        Z = _codes(X, self._cats)
        self._med = Z.median()
        Z = Z.fillna(self._med)
        self._mu, self._sd = Z.mean(), Z.std().replace(0, 1).fillna(1)
        self._Z = ((Z - self._mu) / self._sd).to_numpy()
        self._y = np.asarray(y, dtype=float)

    def predict(self, X):
        Z = ((_codes(X, self._cats).fillna(self._med) - self._mu) / self._sd).to_numpy()
        k = min(self.k, len(self._y))
        out = np.empty(len(Z))
        for a in range(0, len(Z), 2048):
            d = ((Z[a:a + 2048, None, :] - self._Z[None, :, :]) ** 2).sum(-1)
            idx = np.argsort(d, axis=1, kind="stable")[:, :k]
            out[a:a + 2048] = np.median(self._y[idx], axis=1)
        return out


class KumoBackend(_Backend):
    """NVIDIA Kumo-Tabular through ``structured-data-models`` (``sdm``)."""

    name = "kumo"

    def __init__(self, size: str = "large", estimators: int = 4):
        import sdm
        import torch

        from .foundation import _cap_vram

        self.sdm, self.torch = sdm, torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        _cap_vram(torch)
        self.estimators = int(os.environ.get("MLBENCH_KUMO_ESTIMATORS", estimators))
        # `small` / `medium` only for a CPU wiring smoke: the arm is scored with `large`
        self.size = os.environ.get("MLBENCH_KUMO_SIZE", size)
        self.model = sdm.models.KumoTabular(task="regression", size=self.size, device=self.device)

    def _stypes(self, cols) -> dict:
        st = self.sdm.Stype
        return {c: (st.categorical if c in D.CATEGORICAL else st.numerical) for c in cols}

    def _table(self, df: pd.DataFrame):
        # categoricals go in as strings: sdm encodes a category by its value, so a query frame
        # whose categories differ from the context's still lines up (checked in the smoke)
        df = df.copy()
        for c in df.columns:
            if c in D.CATEGORICAL:
                df[c] = df[c].astype(str)
        return self.sdm.TableTensor.from_pandas(df=df, stypes=self._stypes(df.columns), device=self.device)

    def _autocast(self):
        return self.torch.amp.autocast(self.device, dtype=self.torch.float16, enabled=self.device == "cuda")

    def fit(self, X, y, seed):
        self.torch.manual_seed(seed)
        t = self._table(X.assign(__y=np.asarray(y, dtype=float)))
        with self._autocast():
            self.model.fit(t.drop_columns("__y"), t[:, "__y"], num_estimators=self.estimators)

    def predict(self, X):
        out = []
        for a in range(0, len(X), QUERY_CHUNK):
            with self._autocast():
                r = self.model.predict(self._table(X.iloc[a:a + QUERY_CHUNK]))
            j = r.columns[self.sdm.Stype.numerical].index("q500")
            out.append(r.numerical[:, j].float().cpu().numpy())
        return np.concatenate(out) if out else np.empty(0)


class InprioraBackend(_Backend):
    """Ant Group's Inpriora regressor (sklearn-style ``fit`` / ``predict``), code pinned to the
    reviewed commit in ``requirements-par1405.txt``."""

    name = "inpriora"

    def __init__(self, estimators: int = 8):
        import torch
        from inpriora import InprioraRegressor

        from .foundation import _cap_vram

        _cap_vram(torch)
        # weights from the pinned Hugging Face revision; CUDA when available, fp16 autocast there
        self.model = InprioraRegressor(
            revision=INPRIORA_REVISION,
            n_estimators=int(os.environ.get("MLBENCH_INPRIORA_ESTIMATORS", estimators)))

    def fit(self, X, y, seed):
        # the frame as `design` builds it: Inpriora encodes the category columns itself
        self.model.set_params(random_seed=seed)
        self.model.fit(X, np.asarray(y, dtype=float))

    def predict(self, X):
        out = [np.asarray(self.model.predict(X.iloc[a:a + QUERY_CHUNK]), dtype=float).reshape(-1)
               for a in range(0, len(X), QUERY_CHUNK)]
        return np.concatenate(out) if out else np.empty(0)


BACKENDS: dict[str, type[_Backend]] = {"stub": StubBackend, "kumo": KumoBackend,
                                        "inpriora": InprioraBackend}


# --------------------------------------------------------------------------- the learner

class _Fitted:
    """Mirrors ``driver_analysis.Fitted``: ``predict(rows)`` returns ŷ for a driver frame."""

    def __init__(self, backend: _Backend | None, feats: list[str], offset: float, stats: dict):
        self.backend, self.feats, self.offset, self.stats = backend, feats, offset, stats

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        if self.backend is None:
            return np.full(len(df), self.offset)
        t0 = time.monotonic()
        y = self.backend.predict(_design(df, self.feats))
        self.stats["predict_rows"] += len(df)
        self.stats["predict_seconds"] += time.monotonic() - t0
        return y


def _design(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    from ..driver_analysis import design

    return design(df, feats)


class TabularFMLevel(DriverLevel):
    """``driver_level`` with an in-context tabular foundation model as its learner."""

    name = "tfm_level"
    backend: ClassVar[str] = "stub"
    #: the weights' licence verdict, and whether the arm may ever be proposed for serving
    licence: ClassVar[str] = "unclear"
    serving_candidate: ClassVar[bool] = False
    context_rows: ClassVar[int] = int(os.environ.get("MLBENCH_TFM_CONTEXT_ROWS", "50000"))

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if cls.licence not in LICENCES:
            raise TypeError(f"{cls.__name__}: licence must be one of {LICENCES}, got {cls.licence!r}")
        if cls.serving_candidate and cls.licence != "servable":
            raise TypeError(f"{cls.__name__}: a {cls.licence} model can never be a serving candidate")
        if is_benchmark_only(getattr(cls, "name", "")) and cls.licence != "benchmark-only":
            raise TypeError(f"{cls.__name__}: {cls.name!r} is listed benchmark-only in "
                            f"benchmark_only_models.json but declares licence {cls.licence!r}")

    def __init__(self) -> None:
        super().__init__()
        self.stats = {"fits": 0, "fit_seconds": 0.0, "context_rows": 0,
                      "predict_rows": 0, "predict_seconds": 0.0}
        self._backend_obj: _Backend | None = None

    def _engine(self) -> _Backend:
        # one backend per arm instance, never shared: a fit stores its context inside the
        # backend, so two arms on one backend would predict from each other's context
        if self._backend_obj is None:
            self._backend_obj = BACKENDS[os.environ.get("MLBENCH_TFM_BACKEND", self.backend)]()
        return self._backend_obj

    def _learner(self, tr: pd.DataFrame) -> _Fitted:
        # like fit_model: a driver that never varies in the rows carries nothing
        feats = [f for f in FEATURES if tr[f].nunique(dropna=True) > 1]
        y = tr["y"].to_numpy(dtype=float)
        if not feats:
            return _Fitted(None, [], float(np.median(y)), self.stats)
        ctx = tr.sample(self.context_rows, random_state=self.seed) if len(tr) > self.context_rows else tr
        t0 = time.monotonic()
        engine = self._engine()
        engine.fit(_design(ctx, feats), ctx["y"].to_numpy(dtype=float), self.seed)
        self.stats["fits"] += 1
        self.stats["fit_seconds"] += time.monotonic() - t0
        self.stats["context_rows"] = len(ctx)
        return _Fitted(engine, feats, 0.0, self.stats)


class KumoLevel(TabularFMLevel):
    """Kumo-Tabular (large) as the daily-level learner."""

    name = "kumo_level"
    backend = "kumo"
    licence = "servable"              # code Apache-2.0, weights OpenMDW-1.1
    serving_candidate = True


class InprioraLevel(TabularFMLevel):
    """Inpriora as the daily-level learner."""

    name = "inpriora_level"
    backend = "inpriora"
    licence = "servable"              # weights Apache-2.0
    serving_candidate = True


VARIANTS = [KumoLevel, InprioraLevel]
