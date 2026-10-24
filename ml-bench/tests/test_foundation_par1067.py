"""PAR-1067 (Granite PatchTST-FM-r2, Granite TTM-r2, TabPFN-TS), without GPU or weights.

What can break on our side: the licence guard, the context / horizon bookkeeping and
quantile columns of PatchTST-FM, TTM's scaling and exogenous channel layout, a TTM
fine-tune window reaching past the origin, TabPFN-TS's frames (dates, covariates, the
days without a level) and the daily-only wiring."""

import datetime as dt
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from mlbench.config import BenchConfig
from mlbench.models import REGISTRY
from mlbench.models.foundation import (NAN_MASKING_BACKENDS, Chronos2, FoundationModel, PatchTSTFM,
                                       PatchTSTFMBackend, StubBackend, TabPFNTS, TabPFNTSBackend, TimesFM3,
                                       TTMBackend, TTMR2, TTMR2FT, ttm_windows)
from mlbench.runner import Runner

torch = pytest.importorskip("torch")


def test_registered_and_config():
    assert {"patchtst_fm", "ttm_r2", "ttm_r2_ft", "tabpfn_ts"} <= set(REGISTRY)
    assert PatchTSTFM()._cov_names(slot=True) == [] and TTMR2()._cov_names(slot=True) == []
    # the fine-tune gets exactly chronos2's slot covariates as exogenous channels
    assert TTMR2FT()._cov_names(slot=True) == Chronos2()._cov_names(slot=True)
    assert TTMR2FT.finetune_every_days == 28 and not TTMR2.daily and not TTMR2.provides_quantiles
    # TabPFN-TS: daily level only, with the published day length (NaN = unknown, TabPFN's missing)
    assert not TabPFNTS.intraday and TabPFNTS.daily and not TabPFNTS.slot_path
    assert "tabpfn_ts" in NAN_MASKING_BACKENDS and "day_len_h" in TabPFNTS()._cov_names(slot=False)


# ---------------------------------------------------------------- licence guard

def test_licence_verdicts():
    lic = {n: (m.licence, m.serving_candidate) for n, m in REGISTRY.items() if issubclass(m, FoundationModel)}
    assert lic["tabpfn_ts"] == ("benchmark-only", False)
    assert lic["timesfm3"] == ("benchmark-only", False)
    for n in ("patchtst_fm", "ttm_r2", "ttm_r2_ft", "chronos2", "timesfm25"):
        assert lic[n] == ("servable", True), n


def test_a_benchmark_only_model_cannot_be_marked_servable():
    for base in (TabPFNTS, TimesFM3):
        with pytest.raises(TypeError, match="never be a serving candidate"):
            type("Sneaky", (base,), {"name": "sneaky", "serving_candidate": True})
    with pytest.raises(TypeError, match="never be a serving candidate"):
        type("Unclear", (FoundationModel,), {"name": "u", "licence": "unclear", "serving_candidate": True})
    with pytest.raises(TypeError, match="licence must be one of"):
        type("Typo", (FoundationModel,), {"name": "t", "licence": "apache"})


def test_run_metadata_labels_the_licence(synth_export, tmp_path):
    from mlbench.__main__ import main

    out = tmp_path / "probe"
    assert main(["run", "--export", str(synth_export), "--out", str(out), "--model", "tabpfn_ts",
                 "--model", "ttm_r2", "--leads", "0,1", "--memory", "1GB", "--threads", "2", "--probe"]) == 0
    lic = json.loads((out / "run-0of1.json").read_text())["licences"]
    assert lic["tabpfn_ts"] == {"licence": "benchmark-only", "serving_candidate": False}
    assert lic["ttm_r2"] == {"licence": "servable", "serving_candidate": True}


# ---------------------------------------------------------------- PatchTST-FM backend

class FakePFM:
    """quantile_outputs (B, 99, H, 1): quantile k = last context value + k."""

    def __init__(self):
        self.config = SimpleNamespace(quantile_levels=[round(i / 100, 2) for i in range(1, 100)],
                                      context_length=20, d_patch=2, pretrain_mask_cont=2)
        self.calls = []

    def __call__(self, past_values, prediction_length, return_dict):
        self.calls.append((past_values.numpy().copy(), prediction_length))
        last = past_values[:, -1]
        q = last[:, None, None] + torch.arange(99, dtype=torch.float32)[None, :, None]
        return SimpleNamespace(quantile_outputs=q.repeat(1, 1, prediction_length)[..., None])


def _pfm(batch=2):
    b = PatchTSTFMBackend.__new__(PatchTSTFMBackend)
    b.torch, b.device, b.peak_vram_gb, b.batch_size = torch, "cpu", 0.0, batch
    b.model = FakePFM()
    b.qi = [49, 79, 94]
    b.window, b.min_h = 20, 4
    return b


def test_patchtst_fm_context_horizon_and_quantiles():
    b = _pfm()
    y = np.vstack([np.arange(30, dtype=np.float32), np.full(30, np.nan, np.float32),
                   np.arange(30, dtype=np.float32) + 100, np.arange(30, dtype=np.float32) + 200])
    (q,) = b.forecast([(y, None, np.zeros((0, 6), np.float32))], [])
    # context + horizon fits the 20-step window: 20 - max(6, 4) = 14 context steps
    assert [c[0].shape for c in b.model.calls] == [(2, 14), (1, 14)]   # 3 rides, batch 2
    assert all(c[1] == 6 for c in b.model.calls)
    assert q.shape == (4, 6, 3) and np.isnan(q[1]).all()              # never-ran ride stays NaN
    np.testing.assert_allclose(q[0, :, :], [[29 + 49, 29 + 79, 29 + 94]] * 6)
    np.testing.assert_allclose(q[3, 0], [229 + 49, 229 + 79, 229 + 94])
    # a horizon below the decoder's minimum still leaves room for it
    b.model.calls.clear()
    b.forecast([(y[:1], None, np.zeros((0, 2), np.float32))], [])
    assert b.model.calls[0][0].shape == (1, 16)


# ---------------------------------------------------------------- TTM backend

class FakeTTM(torch.nn.Module):
    """prediction_outputs (B, 720, 1) = last observed past value of channel 0 + 1."""

    def __init__(self):
        super().__init__()
        self.backbone = torch.nn.Linear(1, 1)
        self.head = torch.nn.Linear(1, 1)
        self.calls = []

    def forward(self, past_values, past_observed_mask, future_values=None, future_observed_mask=None,
                return_loss=False, return_dict=True):
        self.calls.append({k: (v.detach().numpy().copy() if v is not None else None) for k, v in
                           (("pv", past_values), ("pm", past_observed_mask), ("fv", future_values),
                            ("fm", future_observed_mask))})
        last = past_values[:, -1:, :1]
        out = (last + 1).repeat(1, 720, 1) + 0 * self.head(last)
        loss = (out[:, :1, 0] ** 2).mean() if return_loss else None
        return SimpleNamespace(prediction_outputs=out, loss=loss)


def _ttm():
    b = TTMBackend.__new__(TTMBackend)
    b.torch, b.device, b.peak_vram_gb, b.batch_size = torch, "cpu", 0.0, 256
    b.model, b.C, b.max_h = FakeTTM(), 1536, 720
    b.cls = SimpleNamespace(from_pretrained=lambda *a, **k: b._ft.update(k) or b._ft_model)
    b._ft, b._ft_model = {}, FakeTTM()
    return b


def test_ttm_zero_shot_scales_pads_and_unscales():
    b = _ttm()
    y = np.array([[2.0, 4.0, np.nan, 6.0], [np.nan] * 4], dtype=np.float32)
    (q,) = b.forecast([(y, None, np.zeros((0, 3), np.float32))], [])
    c = b.model.calls[0]
    assert c["pv"].shape == (1, 1536, 1) and c["fv"] is None          # left-padded, no exog
    assert not c["pm"][0, :1532].any() and c["pm"][0, -4:, 0].tolist() == [True, True, False, True]
    mu, sd = np.nanmean(y[0]), np.nanstd(y[0])
    np.testing.assert_allclose(c["pv"][0, -4:, 0], [(2 - mu) / sd, (4 - mu) / sd, 0, (6 - mu) / sd], rtol=1e-6)
    # inverse scaling: (last z + 1) * sd + mu = 6 + sd ; q80 / q95 do not exist
    np.testing.assert_allclose(q[0, :, 0], 6 + sd, rtol=1e-5)
    assert np.isnan(q[0, :, 1:]).all() and np.isnan(q[1]).all()


def test_ttm_exogenous_channels_after_finetune():
    b = _ttm()
    y = np.array([[1.0, 3.0, 5.0]], dtype=np.float32)
    pc = np.array([[0.0, 10.0, 20.0], [5.0, 5.0, 5.0]], dtype=np.float32)
    fc = np.array([[30.0, 40.0], [5.0, 5.0]], dtype=np.float32)
    ft = FakeTTM()
    b.forecast([(y, pc, fc)], ["a", "b"], pipe=ft)
    c = ft.calls[0]
    assert c["pv"].shape == (1, 1536, 3) and c["fv"].shape == (1, 720, 3)
    # future covariates are scaled with the PAST mean / sd; the target channel is unknown (0)
    mu, sd = 10.0, np.std([0, 10, 20])
    np.testing.assert_allclose(c["fv"][0, :2, 1], [(30 - mu) / sd, (40 - mu) / sd], rtol=1e-6)
    np.testing.assert_allclose(c["fv"][0, :2, 2], 0.0)                 # constant covariate -> 0
    assert (c["fv"][0, :, 0] == 0).all()
    np.testing.assert_allclose(c["pv"][0, -3:, 1], [(0 - mu) / sd, 0, (20 - mu) / sd], rtol=1e-6)
    assert not b.model.calls                                            # the zero-shot model unused


def test_ttm_refuses_a_horizon_beyond_the_branch():
    with pytest.raises(ValueError):
        _ttm().forecast([(np.ones((1, 4), np.float32), None, np.zeros((0, 721), np.float32))], [])


def test_ttm_windows_never_reach_past_the_context():
    for T in (10, 600, 3000):
        w = ttm_windows(T, C=1536, H=720, min_past=384, stride=180)
        assert all(0 <= s < e < f <= T and e - s <= 1536 and f - e <= 720 for s, e, f in w)
    assert ttm_windows(100, 1536, 720, 384, 180) == []


def test_ttm_finetune_sees_only_the_context_and_builds_exogenous_config():
    b = _ttm()
    T = 3000
    y = np.vstack([np.arange(T, dtype=np.float32), np.full(T, np.nan, np.float32)])
    pc = np.vstack([np.arange(T, dtype=np.float32) % 7, np.ones(T, np.float32)])
    fc = np.full((2, 50), 1e6, np.float32)                               # never in a training window
    m = b.finetune([(y, pc, fc)], ["a", "b"], prediction_length=384, steps=3, context_length=2048,
                   learning_rate=1e-3, batch_size=4)
    assert m is b._ft_model and not m.training
    assert b._ft["num_input_channels"] == 3 and b._ft["prediction_channel_indices"] == [0]
    assert b._ft["exogenous_channel_indices"] == [1, 2] and b._ft["enable_forecast_channel_mixing"]
    assert len(m.calls) == 3
    zmax = (T - 1 - np.mean(np.arange(T))) / np.std(np.arange(T))
    for c in m.calls:
        assert c["pv"].shape == (4, 1536, 3) and c["fv"].shape == (4, 720, 3)
        assert np.abs(c["fv"]).max() <= zmax + 1e-4 and np.abs(c["pv"]).max() <= zmax + 1e-4


def test_ttm_finetune_without_any_window_fails_loudly():
    with pytest.raises(ValueError, match="no training window"):
        _ttm().finetune([(np.ones((1, 50), np.float32), None, None)], [], prediction_length=384, steps=1,
                        context_length=2048, learning_rate=1e-3, batch_size=4)


@pytest.fixture
def fits(monkeypatch):
    calls = []

    def finetune(self, tasks, cov_names, **kw):
        calls.append([(y.copy(), None if pc is None else pc.copy()) for y, pc, _fc in tasks])
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


def test_ttm_finetune_harness_sees_only_truth_before_its_origin(synth_export, tmp_path, fits):
    c = dt.date(2026, 4, 20)
    _run(synth_export, tmp_path / "a", TTMR2FT(), c, c, poison=False)
    clean = list(fits)
    fits.clear()
    _run(synth_export, tmp_path / "b", TTMR2FT(), c, c, poison=True)
    assert len(clean) == 1 and len(fits) == 1                   # one fit per date, never intraday
    for (ya, pa), (yb, pb) in zip(clean[0], fits[0]):
        np.testing.assert_array_equal(ya, yb)
        np.testing.assert_array_equal(pa, pb)
        assert pa.shape[0] == len(TTMR2FT()._cov_names(slot=True))


# ---------------------------------------------------------------- TabPFN-TS

def test_tabpfn_frames_align_dates_and_covariates():
    ctx_days = [dt.date(2026, 4, 1) + dt.timedelta(days=i) for i in range(3)]
    fut_days = [dt.date(2026, 4, 4), dt.date(2026, 4, 5)]
    y = np.array([[10, np.nan, 30], [np.nan, np.nan, np.nan], [1, 2, 3]], dtype=np.float32)
    pc = np.array([[0, 1, 0], [8, np.nan, 9]], dtype=np.float32)
    fc = np.array([[1, 1], [np.nan, 10]], dtype=np.float32)
    ctx, fut = TabPFNTSBackend.frames(y, pc, fc, ["hol", "day_len_h"], ctx_days, fut_days)
    a = ctx[ctx["item_id"] == 0]
    assert a["timestamp"].dt.date.tolist() == [ctx_days[0], ctx_days[2]]       # the NaN day is left out
    assert a["target"].tolist() == [10, 30] and a["hol"].tolist() == [0, 0] and a["day_len_h"].tolist() == [8, 9]
    assert 1 not in set(ctx["item_id"]) and 1 not in set(fut["item_id"])      # a ride that never ran
    f = fut[fut["item_id"] == 2]
    assert f["timestamp"].dt.date.tolist() == fut_days and f["hol"].tolist() == [1, 1]
    assert np.isnan(f["day_len_h"].iloc[0]) and f["day_len_h"].iloc[1] == 10
    assert "target" not in fut


class FakePipe:
    def __init__(self):
        self.calls = []

    def predict_df(self, ctx, future_df, quantiles):
        self.calls.append((ctx, future_df, quantiles))
        last = ctx.groupby("item_id")["target"].last()
        p = future_df.assign(target=future_df["item_id"].map(last) + future_df["timestamp"].dt.day)
        return p.set_index(["item_id", "timestamp"])[["target"]]


def test_tabpfn_forecast_maps_back_and_needs_the_dates():
    b = TabPFNTSBackend.__new__(TabPFNTSBackend)
    b.pipe = FakePipe()
    ctx_days = [dt.date(2026, 4, 1), dt.date(2026, 4, 2)]
    fut_days = [dt.date(2026, 4, 3), dt.date(2026, 4, 4)]
    y = np.array([[np.nan, np.nan], [5, 7]], dtype=np.float32)
    task = (y, np.zeros((1, 2), np.float32), np.zeros((1, 2), np.float32))
    (q,) = b.forecast([task], ["x"], days=(ctx_days, fut_days))
    assert np.isnan(q[0]).all() and q[1, :, 0].tolist() == [7 + 3, 7 + 4] and np.isnan(q[1, :, 1:]).all()
    assert b.pipe.calls[0][2] == [0.5]
    with pytest.raises(ValueError, match="daily levels only"):
        b.forecast([task], ["x"])


def test_tabpfn_daily_wiring_gets_the_calendar(synth_export, tmp_path, monkeypatch):
    seen = []

    def forecast(self, tasks, cov_names, pipe=None, days=None):
        seen.append((days, cov_names))
        return StubBackend.forecast(self, tasks, cov_names)

    class DayStub(StubBackend):
        wants_days = True

    monkeypatch.setattr(DayStub, "forecast", forecast)
    monkeypatch.setitem(__import__("mlbench.models.foundation", fromlist=["_BACKENDS"])._BACKENDS, "stub",
                        DayStub())
    c = dt.date(2026, 4, 20)
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1])
    r = Runner(synth_export, tmp_path / "t", cfg, [TabPFNTS()], materialize=True, origins=(str(c), str(c)))
    r.run()
    (days, names), = seen                                       # daily path only, once
    ctx_days, fut_days = days
    assert ctx_days[-1] == c - dt.timedelta(days=1) and fut_days[0] == c
    assert len(set(ctx_days)) == len(ctx_days) and "day_len_h" in names
    lv = pd.concat(pd.read_parquet(p) for p in (tmp_path / "t" / "parts" / "levels").rglob("*.parquet"))
    assert lv["lvl_tabpfn_ts"].notna().any()                    # reaches UC4 / D6 / D7


def test_point_only_backend_keeps_q80_missing(synth_export, tmp_path, monkeypatch):
    stub = StubBackend.forecast

    def forecast(self, tasks, cov_names, pipe=None):
        out = stub(self, tasks, cov_names)
        for q in out:
            q[:, :, 1:] = np.nan
        return out

    class Rec(TTMR2):
        name = "ttm_rec"
        out: list = []

        def predict(self, origin, horizon_slots, cov):
            o = super().predict(origin, horizon_slots, cov)
            Rec.out.append(o)
            return o

    monkeypatch.setattr(StubBackend, "forecast", forecast)
    c = dt.date(2026, 4, 20)
    Runner(synth_export, tmp_path / "q", BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1]),
           [Rec()], materialize=True, origins=(str(c), str(c))).run()
    o = pd.concat(Rec.out)
    assert len(o) and o["q50"].notna().all() and o["q80"].isna().all() and o["q95"].isna().all()
