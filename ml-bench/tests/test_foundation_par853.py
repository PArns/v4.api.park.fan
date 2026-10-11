"""PAR-853 backends (TiRex-2, Toto 2.0) with the model mocked.

What can break without the weights is our side of the contract: the context window,
the patch padding, the covariate layout, the rollout past TiRex-2's 320-step head,
dropping all-NaN variates, and the q50/q80/q95 output (no q95 head -> NaN)."""

import sys
import types

import numpy as np
import pytest
import torch

from mlbench.models import REGISTRY
from mlbench.models.foundation import (NAN_MASKING_BACKENDS, TiRex2, TiRex2Backend, TiRex2Grid,
                                       Toto2, Toto2Backend, Toto2Grid, Toto2NoCov, _ffill_rows)

QS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]


def test_registered_and_covariates():
    assert {"tirex2", "tirex2_grid", "toto2", "toto2_nocov", "toto2_grid"} <= set(REGISTRY)
    assert TiRex2.intraday and TiRex2.daily and Toto2.intraday and Toto2.daily
    assert TiRex2Grid.context_mode == "grid" and Toto2Grid.context_mode == "grid"
    # same inputs as chronos2: every slot + day covariate, and day_len_h on the daily series
    assert TiRex2()._cov_names(slot=True) == REGISTRY["chronos2"]()._cov_names(slot=True)
    assert Toto2()._cov_names(slot=False) == REGISTRY["chronos2"]()._cov_names(slot=False)
    assert Toto2NoCov()._cov_names(slot=True) == []
    assert {"tirex2", "toto2"} <= set(NAN_MASKING_BACKENDS)


def test_ffill_rows_keeps_leading_nan():
    a = np.array([[np.nan, 1, np.nan, 3, np.nan]], dtype=np.float32)
    np.testing.assert_array_equal(np.isnan(_ffill_rows(a)), [[True, False, False, False, False]])
    np.testing.assert_array_equal(_ffill_rows(a)[0, 1:], [1, 1, 3, 3])


class FakeTiRexModel:
    quantiles = torch.tensor(QS)
    future_len = 4
    context_len = 8


class FakeTiRexFM:
    """forecast -> (n, 9, h); quantile k = last context value + k (so rollout is visible)."""

    def __init__(self):
        self.model = FakeTiRexModel()
        self.calls = []

    def forecast(self, ts, prediction_length, output_type):
        (t,) = ts
        self.calls.append((t.target.shape, None if t.future_covariates is None else t.future_covariates.shape,
                           prediction_length))
        assert not torch.isnan(t.target).all(dim=1).any(), "an all-NaN variate reached the model"
        last = t.target[:, -1].numpy()
        q = last[:, None, None] + np.arange(9)[None, :, None] + np.zeros((1, 1, prediction_length))
        return [q.astype(np.float32)]


@pytest.fixture
def fake_tirex(monkeypatch):
    mod = types.ModuleType("tirex2")

    class TimeseriesType:
        def __init__(self, target, past_covariates, future_covariates):
            self.target, self.past_covariates, self.future_covariates = target, past_covariates, future_covariates

    mod.TimeseriesType = TimeseriesType
    monkeypatch.setitem(sys.modules, "tirex2", mod)
    b = TiRex2Backend.__new__(TiRex2Backend)
    b.torch, b.device, b.peak_vram_gb = torch, "cpu", 0.0
    b.fm = FakeTiRexFM()
    b.qi, b.max_h, b.context_length = (4, 7), 4, 8
    return b


def test_tirex2_rollout_window_and_covariates(fake_tirex):
    y = np.vstack([np.arange(20, dtype=np.float32), np.full(20, np.nan, np.float32)])  # ride 2 never ran
    pc = np.ones((3, 20), np.float32)
    fc = np.ones((3, 10), np.float32) * 2
    (q,) = fake_tirex.forecast([(y, pc, fc)], ["a", "b", "c"])
    assert q.shape == (2, 10, 3)
    assert np.isnan(q[1]).all()                      # the empty variate: no forecast, not zero
    assert np.isnan(q[0, :, 2]).all()                # no q95 head
    # 10 steps with a 4-step head: 4 + 4 + 2, each call sees 8 context steps and
    # covariates for context + its own horizon
    assert [c[2] for c in fake_tirex.fm.calls] == [4, 4, 2]
    assert all(c[0] == (1, 8) for c in fake_tirex.fm.calls)
    assert [c[1] for c in fake_tirex.fm.calls] == [(3, 12), (3, 12), (3, 10)]
    # median fed back: the 2nd block starts from the 1st block's last median (19 + 4)
    np.testing.assert_allclose(q[0, :4, 0], 19 + 4)
    np.testing.assert_allclose(q[0, 4:8, 0], 23 + 4)
    np.testing.assert_allclose(q[0, :, 1] - q[0, :, 0], 3)


class FakeToto:
    class config:
        patch_size = 4

    class output_head:
        knots = QS

    def __init__(self):
        self.inputs = None

    def forecast(self, inputs, horizon, **kw):
        self.inputs, self.kw = inputs, kw
        n = inputs["target"].shape[1]
        base = torch.arange(n, dtype=torch.float32)[None, None, :, None]
        return base + torch.arange(9, dtype=torch.float32)[:, None, None, None] + torch.zeros(9, 1, n, horizon)


def _toto():
    b = Toto2Backend.__new__(Toto2Backend)
    b.torch, b.device, b.peak_vram_gb = torch, "cpu", 0.0
    b.model, b.qi, b.patch, b.context_length = FakeToto(), (4, 7), 4, 8
    return b


def test_toto2_padding_mask_and_known_dynamic():
    b = _toto()
    y = np.array([[np.nan, 1, 2, np.nan, 4, 5],
                  [np.nan] * 6,
                  [3, 3, 3, 3, 3, np.nan]], dtype=np.float32)
    pc = np.vstack([np.arange(6), np.full(6, np.nan)]).astype(np.float32)
    fc = np.ones((2, 5), np.float32)
    (q,) = b.forecast([(y, pc, fc)], ["a", "b"])
    i = b.model.inputs
    assert tuple(i["target"].shape) == (1, 2, 8)          # empty ride dropped, 6 -> 8 (patch 4)
    m = i["target_mask"][0].numpy()
    np.testing.assert_array_equal(m[0], [0, 0, 0, 1, 1, 0, 1, 1])
    assert np.isfinite(i["target"].numpy()).all()
    assert float(i["target"][0, 0, 5]) == 2.0            # forward-filled, masked
    assert tuple(i["known_dynamic"].shape) == (1, 2, 13)  # pad + context + horizon
    assert not i["known_dynamic_mask"][0, 1].any()        # an all-NaN covariate is unobserved
    assert b.model.kw["decode_block_size"] is None
    assert q.shape == (3, 5, 3) and np.isnan(q[1]).all() and np.isnan(q[:, :, 2]).all()
    np.testing.assert_allclose(q[0, :, 0], 4)             # variate 0, knot 0.5
    np.testing.assert_allclose(q[2, :, 1], 1 + 7)         # variate 1 of the kept rows, knot 0.8


def test_toto2_nocov_has_no_known_dynamic():
    b = _toto()
    y = np.arange(8, dtype=np.float32)[None]
    b.forecast([(y, None, np.zeros((0, 3), np.float32))], [])
    assert "known_dynamic" not in b.model.inputs
