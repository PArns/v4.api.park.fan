"""PAR-1063 (Chronos-2 LoRA) and PAR-1064 (TimesFM 2.5), without GPU or weights.

What can break on our side: the LoRA fit seeing truth from after its origin, the fit
cadence, a silent fall-back to a full fine-tune when peft is missing, TimesFM 2.5's
input cleaning / covariate alignment / output slicing, and the --probe bookkeeping."""

import datetime as dt
import json
import sys

import numpy as np
import pytest

from mlbench.__main__ import main
from mlbench.config import BenchConfig
from mlbench.models import REGISTRY
from mlbench.models.foundation import (NAN_MASKING_BACKENDS, ChronosBackend, Chronos2FT, Chronos2Lora,
                                       StubBackend, TimesFM25, TimesFM25Backend, TimesFM25Grid)
from mlbench.runner import Runner


def test_registered_and_config():
    assert {"chronos2_lora", "timesfm25", "timesfm25_grid"} <= set(REGISTRY)
    # chronos2_lora differs from chronos2_ft in the adaptation method ONLY
    assert Chronos2Lora.finetune_mode == "lora" and Chronos2FT.finetune_mode == "full"
    for k in ("finetune_every_days", "finetune_steps", "finetune_context", "finetune_horizon",
              "finetune_lr", "daily", "intraday", "context_mode", "use_covariates"):
        assert getattr(Chronos2Lora, k) == getattr(Chronos2FT, k), k
    assert Chronos2Lora.finetune_every_days == 28
    # TimesFM 2.5: chronos2's covariates, but never day_len_h (NaN = unknown would read 0)
    assert TimesFM25()._cov_names(slot=True) == REGISTRY["chronos2"]()._cov_names(slot=True)
    assert "day_len_h" not in TimesFM25()._cov_names(slot=False)
    assert "timesfm25" not in NAN_MASKING_BACKENDS
    assert TimesFM25.intraday and TimesFM25.daily            # daily -> timesfm25_x_h5
    assert TimesFM25Grid.context_mode == "grid" and not TimesFM25Grid.daily


# ---------------------------------------------------------------- LoRA: leakage + cadence

@pytest.fixture
def fits(monkeypatch):
    calls = []

    def finetune(self, tasks, cov_names, **kw):
        calls.append((kw.get("mode"), [y.copy() for y, _pc, _fc in tasks]))
        return None

    monkeypatch.setattr(StubBackend, "finetune", finetune)
    return calls


def _run(export, out, model, first, last, poison):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1])
    r = Runner(export, out, cfg, [model], materialize=True, origins=(str(first), str(last)))
    if poison:   # every truth slot ending after the first daily origin -> garbage
        r.con.execute("""CREATE OR REPLACE TEMP TABLE o0 AS SELECT p.id park_id,
            timezone(p.timezone, TIMESTAMP '%s 06:00:00') o FROM parks p""" % first)
        r.con.execute("""UPDATE truth SET y = y * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE >
            (SELECT o FROM o0 WHERE o0.park_id = truth.park_id)""")
    r.run()


def test_lora_fit_sees_only_truth_before_its_origin(synth_export, tmp_path, fits):
    c = dt.date(2026, 4, 20)
    _run(synth_export, tmp_path / "a", Chronos2Lora(), c, c, poison=False)
    clean = list(fits)
    fits.clear()
    _run(synth_export, tmp_path / "b", Chronos2Lora(), c, c, poison=True)
    # one fit per date: at the 06:00 origin, never again at the intraday ones
    assert len(clean) == 1 and len(fits) == 1
    assert clean[0][0] == fits[0][0] == "lora"
    a, b = clean[0][1], fits[0][1]
    assert len(a) == len(b) and all(np.isfinite(y).any() for y in a)
    for x, y in zip(a, b):
        np.testing.assert_array_equal(x, y)


def test_lora_refit_cadence(synth_export, tmp_path, fits):
    class Every2(Chronos2Lora):
        name = "chronos2_lora_e2"
        finetune_every_days = 2
        intraday = False

    c = dt.date(2026, 4, 20)
    _run(synth_export, tmp_path / "c", Every2(), c, c + dt.timedelta(days=4), poison=False)
    assert len(fits) == 3                                   # d0, d2, d4


def test_lora_refuses_to_fall_back_to_a_full_finetune(monkeypatch):
    class Pipe:
        called = False

        def fit(self, *a, **k):
            Pipe.called = True

    b = ChronosBackend.__new__(ChronosBackend)
    b.pipe = Pipe()
    monkeypatch.setitem(sys.modules, "peft", None)          # "not installed"
    y = np.ones((1, 8), np.float32)
    with pytest.raises(ImportError):
        b.finetune([(y, None, None)], [], prediction_length=4, steps=1, context_length=8,
                   learning_rate=1e-5, batch_size=2, mode="lora")
    assert not Pipe.called


# ---------------------------------------------------------------- TimesFM 2.5 backend

M = TimesFM25Backend.MAX_HORIZON


class FakeTFM:
    """forecast -> (n, backcast + M, 10); quantile column k = last input value + k."""

    def __init__(self):
        self.plain, self.xreg = [], []

    @staticmethod
    def _q(v, H):
        return v[-1] + np.arange(10)[None, :] + np.zeros((H, 1))

    def forecast(self, horizon, inputs):
        self.plain.append([np.array(v) for v in inputs])
        return None, np.stack([np.concatenate([np.zeros((5, 10)), self._q(v, horizon)]) for v in inputs])

    def forecast_with_covariates(self, inputs, dynamic_numerical_covariates, xreg_mode, force_on_cpu):
        self.xreg.append((inputs, dynamic_numerical_covariates, xreg_mode, force_on_cpu))
        H = len(next(iter(dynamic_numerical_covariates.values()))[0]) - len(inputs[0])
        return None, [self._q(v, H) for v in inputs]


def _tfm(context=6):
    b = TimesFM25Backend.__new__(TimesFM25Backend)
    b.torch, b.device, b.peak_vram_gb, b.context_length = None, "cpu", 0.0, context
    b.fm = FakeTFM()
    return b


def test_timesfm25_plain_cleans_inputs_and_slices_the_horizon():
    b = _tfm()
    y = np.array([[9, 9, np.nan, 1, np.nan, 3, 5, 7],      # context 6 -> [nan,1,nan,3,5,7]
                  [np.nan] * 8], dtype=np.float32)          # never ran
    (q,) = b.forecast([(y, None, np.zeros((0, 4), np.float32))], [])
    (inp,) = b.fm.plain[0]
    np.testing.assert_allclose(inp, [1, 2, 3, 5, 7])        # leading NaN cut, gap interpolated
    assert q.shape == (2, 4, 3) and np.isnan(q[1]).all() and np.isnan(q[:, :, 2]).all()
    np.testing.assert_allclose(q[0, :, 0], 7 + 5)           # column 5 = q0.5, backcast dropped
    np.testing.assert_allclose(q[0, :, 1], 7 + 8)           # column 8 = q0.8


def test_timesfm25_covariates_align_with_the_cleaned_input():
    b = _tfm()
    y = np.array([[np.nan, np.nan, 2, 4, np.nan, 6]], dtype=np.float32)
    pc = np.vstack([np.arange(6), np.full(6, np.nan)]).astype(np.float32)
    fc = np.vstack([np.full(3, 10), np.full(3, 20)]).astype(np.float32)
    (q,) = b.forecast([(y, pc, fc)], ["a", "b"])
    (inputs, cov, mode, cpu) = b.fm.xreg[0]
    assert mode == "xreg + timesfm" and cpu is True and not b.fm.plain
    np.testing.assert_allclose(inputs[0], [2, 4, 5, 6])
    np.testing.assert_allclose(cov["a"][0], [2, 3, 4, 5, 10, 10, 10])   # last 4 past + horizon
    np.testing.assert_allclose(cov["b"][0], [0, 0, 0, 0, 20, 20, 20])   # NaN -> 0
    np.testing.assert_allclose(q[0, :, 0], 6 + 5)


def test_timesfm25_refuses_a_horizon_beyond_the_quantile_head():
    y = np.ones((1, 4), np.float32)
    with pytest.raises(ValueError):
        _tfm().forecast([(y, None, np.zeros((0, M + 1), np.float32))], [])


# ---------------------------------------------------------------- --probe

def test_probe_runs_one_origin_and_records_timing(synth_export, tmp_path, fits):
    out = tmp_path / "probe"
    assert main(["run", "--export", str(synth_export), "--out", str(out), "--model", "chronos2_lora",
                 "--leads", "0,1", "--memory", "1GB", "--threads", "2", "--probe"]) == 0
    meta = json.loads((out / "run-0of1.json").read_text())
    assert meta["origins"][0] == meta["origins"][1]          # exactly one origin
    p = meta["probe"]
    assert p["seconds"] > 0 and "gpu_peak_allocated_gb" in p
    assert 0.01 < p["host_peak_rss_gb"] < 100 and p["host_peak_rss_children_gb"] >= 0   # GB, not KiB
    st = p["models"]["chronos2_lora"]
    assert st["finetune_calls"] == 1 and st["slot_calls"] >= 1
