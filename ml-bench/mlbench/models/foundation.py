"""Time-series foundation models as zero-shot plug-ins (PAR-828).

Two models, one series construction:

* **Chronos-2** (``amazon/chronos-2``, Apache-2.0). Native past and known-future
  covariates; a park is ONE multivariate task — every ride of the park is a target
  variate and the covariates are shared — so the group attention can look across
  the rides of a park. Context up to 8192 steps, 64 output patches × 16 = 1024 steps
  without autoregressive unrolling.
* **TimesFM 3.0** (``google/timesfm-3.0-pytorch``; weights under the TimesFM
  Non-Commercial License v1.0 — "non-commercial and NON-PRODUCTION use"; use "in
  direct or indirect interactions with end users or production systems" is excluded
  by the licence text, so ``timesfm3`` is a BENCHMARK-ONLY arm and can never be
  served on park.fan without a commercial licence from Google). Past-and-future covariates
  are native in 3.0. Its variate attention is capped at 32 variates (targets AND
  covariates), which a park with 40–70 rides does not fit, so each ride is a
  univariate task with the covariates as extra variates. Quantile head 0.1 … 0.9:
  there is no q95, so the adapter returns q95 = NaN (D8 reports it as missing).

**Context layout** (``context_mode``):

``compressed``  the ride's 15-min slots inside the PUBLISHED operating windows only,
                day after day — nights and closed days are left out, which stretches
                the 8192-step context to ~170 operating days. Day boundaries are told
                to the model by the covariates (minutes since opening / until closing,
                local time of day, weekday).
``grid``        a regular 15-min UTC grid, NaN outside the windows (8192 steps =
                85 days). Chronos-2 masks NaN natively; TimesFM interpolates NaN
                linearly (which is wrong across nights), so TimesFM is run compressed only.

Inside the window a slot without truth (ride down, closed, wait < 5) is NaN in both
layouts. The forecast is asked for every future window slot (compressed) or every
future grid slot up to the last requested window close (grid), and then joined to
the harness's ``horizon_slots``.

**Covariates** (BENCH-SPEC "Known-future covariates"), per slot:
minutes since published opening, minutes until published closing, in-window flag
(grid only), local time of day; per park-day: weekend, weekday (sin/cos), public /
school holiday of the park's own region, holiday / school-holiday counts incl. the
neighbour regions (ml-service ``holiday_features.py`` OR-semantics), any school
holiday, schedule holiday and bridge day. ``weather=True`` adds the daily max
temperature and precipitation — these are ACTUALS (no forecast archive exists), so
a ``*_wx`` model is an ORACLE upper bound and is reported separately.

**Daily variant** (``predict_daily``): the same model on the ride-day P90 level
series (the truth level, ``>= 8`` slots, NaN on closed days), calendar days, with
day covariates (+ published day length: 0 = published closed, NaN = beyond the
operator's publishing horizon — an absent schedule never becomes a closed day).
The runner composes it with H5 as ``<name>_x_h5`` at every slot lead, so long leads
(d10–d90) and UC4 (to d365) are scored without producing 15-min slots there.

**PAR-853 additions** (same series construction, same covariates):

* **TiRex-2** (``NX-AI/TiRex-2``, Apache-2.0; package ``tirex-2``). Native past and
  known-future covariates and native multivariate targets, so a park is one task
  exactly as for Chronos-2 and every covariate above is passed as known-future. Its
  window is 2048 context steps and 320 forecast steps; longer horizons are rolled
  out with the median fed back (the rollout is ours, not the package's). The fused
  FlashRNN kernel needs ``nvcc`` at runtime, so the model is loaded with the
  pure-PyTorch ("vanilla") recurrent kernels and moved to the GPU.
* **Toto 2.0** (``Datadog/Toto-2.0-313m``, Apache-2.0; package ``toto-2``). Rides of
  a park are variates of one task. The release notes say exogenous-variable support
  is NOT part of 2.0 ("planned for a future 2.0 release"); the model's own forecast
  entry point nevertheless takes ``known_dynamic`` series (appended as extra
  variates whose future is observed — the package's GluonTS adapter wires
  ``feat_dynamic_real`` there). ``toto2`` passes the SAME covariates as ``chronos2``
  that way — an untrained use of a mechanical hook — and ``toto2_nocov`` is the
  official covariate-free zero-shot model. Context 4096 (its training context:
  ``residual_attn_ratio`` = sqrt(S/ln S) with S = 128 patches of 32).

Both heads are 0.1 … 0.9: there is no q95, so both return q95 = NaN like TimesFM.

**PAR-1064 — TimesFM 2.5** (``google/timesfm-2.5-200m-pytorch``, weights Apache-2.0 per
the model card; the class ``timesfm.TimesFM_2p5_200M_torch`` ships in the same
``timesfm==3.0.2`` wheel as TimesFM 3.0). Univariate: every ride is its own series.
Covariates go through the package's own ``forecast_with_covariates`` (in-context
linear regression, "XReg", mode ``xreg + timesfm``: a ridge/OLS fit of the target on the
covariates over the context, TimesFM forecasts the residual) — one regression per
``forecast_with_covariates`` call, and ``forecast()`` makes one call per task, so one per
PARK, shared by its rides (batching tasks into one call would silently pool parks). That is a covariate path bolted on beside the network, not
covariates the network attends to as Chronos-2's do. TimesFM interpolates NaN linearly
(also inside the XReg target, which would otherwise be NaN), so the grid layout turns
nights into ramps; ``timesfm25_grid`` measures what that costs rather than assuming it.
Quantile head 0.1 … 0.9 (continuous head, horizon <= 1024): q95 = NaN.

**PAR-1063 — Chronos-2 LoRA** (``chronos2_lora``): chronos-forecasting's own
``fit(finetune_mode="lora")`` (peft adapter on the attention q/k/v/o and the output
layer, the package default), on exactly the inputs and cadence of ``chronos2_ft`` — so
the two differ only in full vs low-rank adaptation.

**PAR-1067** — Granite PatchTST-FM-r2 (``patchtst_fm``, no covariate input, 99 quantiles),
Granite TTM-r2 (``ttm_r2`` zero-shot without covariates; ``ttm_r2_ft`` with the covariates
as exogenous channels, which exist only after fine-tuning) and TabPFN-TS (``tabpfn_ts``,
daily level only, BENCHMARK-ONLY weights). Every variant declares ``licence`` and
``serving_candidate``; a benchmark-only class marked as a serving candidate is refused.

The information cut is the runner's: truth comes only from ``origin.history``
(slots that end before the park's origin); covariates and windows are known-future
inputs by definition (``windows`` is the published schedule, flagged
``published_final`` — the export has no schedule history).
"""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import ClassVar

import numpy as np
import pandas as pd

from ..licences import is_benchmark_only
from .base import Model, Origin

QUANTILES = (0.5, 0.8, 0.95)
SLOT_COVS = ["ko_min", "kc_min", "tod"]
DAY_COVS = ["is_weekend", "dow_sin", "dow_cos", "is_holiday_primary", "is_school_holiday_primary",
            "holiday_count_total", "school_holiday_count_total", "is_school_holiday_any",
            "is_bridge_day"]
WX_COVS = ["temp_max", "precip_sum"]
#: backends that treat a NaN covariate as missing (TimesFM interpolates it instead)
NAN_MASKING_BACKENDS = ("chronos2", "tirex2", "toto2", "tabpfn_ts")
#: licence verdicts of the weights (docs/ml/foundation-models.md quotes the line each rests on)
LICENCES = ("servable", "benchmark-only", "unclear")
EMPTY = pd.DataFrame(columns=["attraction_id", "slot_start_utc", "q50", "q80", "q95"])

_BACKENDS: dict[str, object] = {}
#: VRAM cap (MODEL-AGENT-RULES: <= 10 GB on the shared 16 GB RTX 5080)
VRAM_FRACTION = float(os.environ.get("MLBENCH_FM_VRAM_FRACTION", "0.6"))


def _cap_vram(torch) -> None:
    if torch.cuda.is_available():
        torch.cuda.set_per_process_memory_fraction(VRAM_FRACTION)


# --------------------------------------------------------------------------- backends

class _Backend:
    """``forecast(tasks)``: tasks = [(target (n, T), past_cov (k, T), future_cov (k, H))];
    returns one array (n, H, 3) per task with the quantiles QUANTILES."""

    name = "backend"
    peak_vram_gb = 0.0

    def forecast(self, tasks: list[tuple[np.ndarray, np.ndarray | None, np.ndarray | None]],
                 cov_names: list[str], pipe=None) -> list[np.ndarray]:
        raise NotImplementedError


class StubBackend(_Backend):
    """Deterministic stand-in for tests: the nan-mean of the last 64 context steps."""

    name = "stub"

    def finetune(self, tasks, cov_names, **kw):
        return None

    def forecast(self, tasks, cov_names, pipe=None):
        out = []
        for y, _pc, fc in tasks:
            H = fc.shape[1] if fc is not None else 1
            with np.errstate(all="ignore"):
                m = np.nanmean(y[:, -64:], axis=1) if y.shape[1] else np.full(y.shape[0], np.nan)
            m = np.where(np.isfinite(m), m, np.nan)
            out.append(np.repeat(np.repeat(m[:, None, None], H, axis=1), 3, axis=2))
        return out


class ChronosBackend(_Backend):
    name = "chronos2"

    def __init__(self, model_id: str = "amazon/chronos-2", batch_size: int = 512):
        import torch
        from chronos import BaseChronosPipeline

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.pipe = BaseChronosPipeline.from_pretrained(model_id, device_map=self.device,
                                                        torch_dtype=torch.float32)
        self.batch_size = batch_size
        self.context_length = self.pipe.model_context_length

    def finetune(self, tasks, cov_names, prediction_length: int, steps: int, context_length: int,
                 learning_rate: float, batch_size: int, mode: str = "full"):
        """A fine-tuned COPY of the pipeline (chronos-forecasting's own trainer; ``mode``
        "full" or "lora") on the context windows of ``tasks`` — truth before the origin only."""
        if mode == "lora":
            # without peft, fit() warns and silently runs a FULL fine-tune — which would
            # then be scored as chronos2_lora
            import peft  # noqa: F401
        inputs = []
        for y, pc, _fc in tasks:
            d = {"target": y.astype(np.float32)}
            if pc is not None and len(cov_names):
                d["past_covariates"] = {n: pc[i].astype(np.float32) for i, n in enumerate(cov_names)}
                d["future_covariates"] = {n: None for n in cov_names}
            inputs.append(d)
        return self.pipe.fit(inputs, prediction_length=prediction_length, finetune_mode=mode,
                             context_length=context_length, learning_rate=learning_rate,
                             num_steps=steps, batch_size=batch_size,
                             output_dir=f"/tmp/chronos2-{mode}", remove_printer_callback=True,
                             report_to="none", save_strategy="no", logging_steps=50)

    def forecast(self, tasks, cov_names, pipe=None):
        pipe = pipe or self.pipe
        out = []
        for y, pc, fc in tasks:
            H = fc.shape[1]
            inp = {"target": y[:, -self.context_length:].astype(np.float32)}
            if pc is not None and len(cov_names):
                T = inp["target"].shape[1]
                inp["past_covariates"] = {n: pc[i, -T:].astype(np.float32) for i, n in enumerate(cov_names)}
                inp["future_covariates"] = {n: fc[i].astype(np.float32) for i, n in enumerate(cov_names)}
            with self.torch.inference_mode():
                q, _ = pipe.predict_quantiles([inp], prediction_length=H,
                                                   quantile_levels=list(QUANTILES),
                                                   batch_size=self.batch_size)
            out.append(q[0].float().cpu().numpy())  # (n, H, 3)
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, self.torch.cuda.max_memory_allocated() / 1e9)
        return out


class TimesFMBackend(_Backend):
    name = "timesfm3"

    def __init__(self, model_id: str = "google/timesfm-3.0-pytorch",
                 batch_size: int = int(os.environ.get("MLBENCH_TFM_BATCH", "8")),
                 context_length: int = int(os.environ.get("MLBENCH_TFM_CONTEXT", "4096"))):
        import torch
        from timesfm3.torch import TimesFM3Forecaster

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.fm = TimesFM3Forecaster.from_pretrained(model_id, device=self.device,
                                                     per_core_batch_size=batch_size)
        qs = list(self.fm.config.quantiles)
        self.qi = [qs.index(0.5), qs.index(0.8), None]          # no q95 in the head
        self.context_length = context_length

    def forecast(self, tasks, cov_names, pipe=None):
        ctx, pf, owner = [], [], []
        Hmax = max(fc.shape[1] for _y, _p, fc in tasks)
        for ti, (y, pc, fc) in enumerate(tasks):
            T = min(y.shape[1], self.context_length)
            cov = None
            if pc is not None and len(cov_names):
                fpad = fc
                if fc.shape[1] < Hmax:   # one horizon per call: pad the future covariates
                    fpad = np.concatenate([fc, np.repeat(fc[:, -1:], Hmax - fc.shape[1], axis=1)], axis=1)
                cov = np.concatenate([pc[:, -T:], fpad], axis=1).astype(np.float32)
            for i in range(y.shape[0]):
                ctx.append(y[i, -T:].astype(np.float32))
                pf.append(cov)
                owner.append((ti, i))
        res = list(self.fm.predict_batch(ctx, horizon=Hmax,
                                         past_future_covariates=pf if pf[0] is not None else None,
                                         return_quantiles=True, make_positive=True))
        out = [np.full((y.shape[0], fc.shape[1], 3), np.nan) for y, _p, fc in tasks]
        for (ti, i), r in zip(owner, res):
            H = out[ti].shape[1]
            qq = np.asarray(r.quantiles)[:H]
            out[ti][i, :, 0] = qq[:, self.qi[0]]
            out[ti][i, :, 1] = qq[:, self.qi[1]]
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, self.torch.cuda.max_memory_allocated() / 1e9)
        return out


class TimesFM25Backend(_Backend):
    """TimesFM 2.5 (200M, Apache-2.0): univariate per ride; covariates via the package's
    in-context regression (``forecast_with_covariates``), fitted once per task (= park)."""

    name = "timesfm25"
    #: Hugging Face revision of the weights (2025-10-02), pinned like TiRex-2 / Toto 2.0
    REVISION = os.environ.get("MLBENCH_TFM25_REVISION", "1d952420fba87f3c6dee4f240de0f1a0fbc790e3")
    #: the continuous quantile head covers 1024 steps (``output_quantile_len``); the
    #: longest request is 8 days on the 15-min grid (768) or 366 days on the daily path
    MAX_HORIZON = 1024
    QI = (5, 8)        # output column 0 is the mean, then 0.1 … 0.9 -> q50, q80

    def __init__(self, model_id: str = "google/timesfm-2.5-200m-pytorch",
                 context_length: int = int(os.environ.get("MLBENCH_TFM25_CONTEXT", "8192")),
                 batch_size: int = int(os.environ.get("MLBENCH_TFM25_BATCH", "32"))):
        import timesfm
        import torch

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        # torch_compile=False: inductor needs a C compiler the slim image does not ship
        # (the TiRex-2 trap); eager mode gives the same numbers
        self.fm = timesfm.TimesFM_2p5_200M_torch.from_pretrained(model_id, revision=self.REVISION,
                                                                 torch_compile=False)
        self.context_length = context_length
        # return_backcast is required by forecast_with_covariates; plain forecast()
        # output then carries the backcast in front, so every read takes the last
        # MAX_HORIZON steps
        self.fm.compile(timesfm.ForecastConfig(
            max_context=context_length, max_horizon=self.MAX_HORIZON, normalize_inputs=True,
            per_core_batch_size=batch_size, use_continuous_quantile_head=True,
            force_flip_invariance=True, infer_is_positive=True, fix_quantile_crossing=True,
            return_backcast=True))

    @staticmethod
    def _clean(y: np.ndarray) -> np.ndarray | None:
        """What the package does to every input (strip leading NaN, interpolate the rest),
        done up front because the XReg target must not contain NaN; None = never ran."""
        if not np.isfinite(y).any():
            return None
        v = y[int(np.argmax(np.isfinite(y))):].astype(np.float64)
        nan = ~np.isfinite(v)
        if nan.any():
            idx = np.arange(len(v))
            v[nan] = np.interp(idx[nan], idx[~nan], v[~nan])
        return v

    def forecast(self, tasks, cov_names, pipe=None):
        C, M = self.context_length, self.MAX_HORIZON
        out = [np.full((y.shape[0], fc.shape[1], 3), np.nan) for y, _p, fc in tasks]
        plain, plain_owner = [], []
        for ti, (y, pc, fc) in enumerate(tasks):
            H = fc.shape[1]
            if H > M:
                raise ValueError(f"timesfm25: horizon {H} > {M} (continuous quantile head)")
            rows, inputs, covs = [], [], []
            for i in range(y.shape[0]):
                v = self._clean(y[i, -C:])
                if v is None:
                    continue
                rows.append(i)
                inputs.append(v)
                if pc is not None and len(cov_names):
                    # a NaN covariate (grid: no window-relative minutes off-window, which
                    # in_win flags) is 0 for the regression; day_len_h is never passed here
                    covs.append(np.nan_to_num(np.concatenate([pc[:, -len(v):], fc], axis=1), nan=0.0))
            if not rows:
                continue
            if covs:
                _pts, qs = self.fm.forecast_with_covariates(
                    inputs=inputs,
                    dynamic_numerical_covariates={n: [c[k] for c in covs] for k, n in enumerate(cov_names)},
                    xreg_mode="xreg + timesfm", force_on_cpu=True)
                for i, q in zip(rows, qs):
                    q = np.asarray(q)[:H]
                    out[ti][i, :, 0], out[ti][i, :, 1] = q[:, self.QI[0]], q[:, self.QI[1]]
            else:
                plain += inputs
                plain_owner += [(ti, i) for i in rows]
        if plain:
            # one pooled call for tasks of different H, so always M (1024) steps; sliced to
            # [:H] below — an 8-step intraday task pays for 1024 steps (cost only)
            _pts, qs = self.fm.forecast(horizon=M, inputs=list(plain))
            for (ti, i), q in zip(plain_owner, qs):
                H = out[ti].shape[1]
                q = np.asarray(q)[-M:][:H]
                out[ti][i, :, 0], out[ti][i, :, 1] = q[:, self.QI[0]], q[:, self.QI[1]]
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, self.torch.cuda.max_memory_allocated() / 1e9)
        return out


def _ffill_rows(a: np.ndarray) -> np.ndarray:
    """Forward-fill NaN along the time axis (leading NaN stay NaN)."""
    idx = np.where(np.isfinite(a), np.arange(a.shape[1])[None, :], 0)
    np.maximum.accumulate(idx, axis=1, out=idx)
    return a[np.arange(a.shape[0])[:, None], idx]


class TiRex2Backend(_Backend):
    """TiRex-2: one multivariate task per park, every covariate as known-future."""

    name = "tirex2"
    #: Hugging Face revision of the weights (2026-07-21; the October commits add an
    #: ONNX export and re-uploaded files — pinned so a re-upload cannot move the run)
    REVISION = os.environ.get("MLBENCH_TIREX2_REVISION", "05e5b26db52bfb256f1ae1bdf785589850482de3")

    def __init__(self, model_id: str = "NX-AI/TiRex-2",
                 context_length: int = int(os.environ.get("MLBENCH_TIREX2_CONTEXT", "2048"))):
        import torch
        import torch._dynamo

        # tirex2 wraps blocks in @torch.compile; inductor needs a C/C++ compiler at
        # runtime that this slim image does not ship. Eager mode gives the same numbers.
        torch._dynamo.config.disable = True

        # `import tirex2` pulls in xlstm, whose sLSTM module computes the CUDA kernel's
        # include/lib paths AT IMPORT TIME (`xlstm/blocks/slstm/src/cuda_init.py`) through
        # `torch.utils.cpp_extension.include_paths("cuda")`, which raises when CUDA_HOME is
        # unset. On a GPU box with no CUDA toolkit that killed the whole run before the
        # first forecast. The value is only string arithmetic for a kernel TiRex-2 never
        # instantiates -- it is a BiXLSTM (mLSTM) model and the recurrent path is the pure
        # PyTorch one selected by `device="cpu"` below -- so any existing root gets the
        # import through. Should a future xlstm actually try to COMPILE that kernel, the
        # build fails loudly on the missing nvcc, which is the outcome we want: a visible
        # error, never silently different numbers.
        os.environ.setdefault("CUDA_HOME", "/usr/local")
        from tirex2 import load_model

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        # device="cpu" selects the pure-PyTorch recurrent kernels (no nvcc needed);
        # the module is then moved to the GPU, where those kernels run unchanged
        self.fm = load_model(model_id, device="cpu", hf_kwargs={"revision": self.REVISION})
        self.fm.model.to(self.device)
        qs = [round(float(q), 6) for q in self.fm.model.quantiles]
        self.qi = (qs.index(0.5), qs.index(0.8))
        self.max_h = int(self.fm.model.future_len)
        self.context_length = min(context_length, int(self.fm.model.context_len))

    def forecast(self, tasks, cov_names, pipe=None):
        from tirex2 import TimeseriesType

        torch = self.torch
        C = self.context_length
        out = []
        for y, pc, fc in tasks:
            H = fc.shape[1]
            res = np.full((y.shape[0], H, 3), np.nan)
            keep = np.isfinite(y).any(axis=1)        # an all-NaN variate would NaN the park
            if not keep.any():
                out.append(res)
                continue
            ctx = y[keep][:, -C:].astype(np.float32)
            T = ctx.shape[1]
            cov = None
            if pc is not None and len(cov_names):
                cov = np.concatenate([pc[:, -T:], fc], axis=1).astype(np.float32)   # (k, T+H)
            done = 0
            while done < H:
                h = min(self.max_h, H - done)
                L = ctx.shape[1]
                start = T + done - L
                c = None if cov is None else torch.from_numpy(np.ascontiguousarray(cov[:, start:T + done + h]))
                ts = TimeseriesType(target=torch.from_numpy(np.ascontiguousarray(ctx)),
                                    past_covariates=None, future_covariates=c)
                with torch.inference_mode():
                    q = self.fm.forecast([ts], prediction_length=h, output_type="numpy")[0]  # (n, 9, h)
                res[keep, done:done + h, 0] = q[:, self.qi[0], :]
                res[keep, done:done + h, 1] = q[:, self.qi[1], :]
                done += h
                if done < H:   # roll out: the median becomes context, the window slides
                    ctx = np.concatenate([ctx, q[:, self.qi[0], :].astype(np.float32)], axis=1)[:, -C:]
            out.append(res)
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, torch.cuda.max_memory_allocated() / 1e9)
        return out


class Toto2Backend(_Backend):
    """Toto 2.0: one multivariate task per park; covariates (if any) as ``known_dynamic``."""

    name = "toto2"
    REVISION = os.environ.get("MLBENCH_TOTO2_REVISION", "a7bab288f5e95f8606f8306f86659357e1c001ef")

    def __init__(self, model_id: str = os.environ.get("MLBENCH_TOTO2_MODEL", "Datadog/Toto-2.0-313m"),
                 context_length: int = int(os.environ.get("MLBENCH_TOTO2_CONTEXT", "4096"))):
        import torch
        from toto2 import Toto2Model

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = Toto2Model.from_pretrained(model_id, revision=self.REVISION).to(self.device).eval()
        knots = [round(float(k), 6) for k in self.model.output_head.knots]
        self.qi = (knots.index(0.5), knots.index(0.8))
        self.patch = int(self.model.config.patch_size)
        self.context_length = context_length

    def forecast(self, tasks, cov_names, pipe=None):
        torch = self.torch
        P = self.patch
        out = []
        for y, pc, fc in tasks:
            H = fc.shape[1]
            res = np.full((y.shape[0], H, 3), np.nan)
            keep = np.isfinite(y).any(axis=1)
            if not keep.any():
                out.append(res)
                continue
            ctx = y[keep][:, -self.context_length:].astype(np.float32)
            T = ctx.shape[1]
            pad = (-T) % P                           # whole patches: left-pad, unobserved
            obs = np.isfinite(ctx)
            # the package's own GluonTS adapter forward-fills the values and passes the
            # observed mask (its forecast() forces the last context patch to "observed")
            vals = np.nan_to_num(_ffill_rows(ctx), nan=0.0)
            vals = np.pad(vals, ((0, 0), (pad, 0)))
            obs = np.pad(obs, ((0, 0), (pad, 0)))
            dev = self.device
            inputs = {"target": torch.from_numpy(vals)[None].to(dev),
                      "target_mask": torch.from_numpy(obs)[None].to(dev),
                      "series_ids": torch.zeros((1, vals.shape[0]), dtype=torch.long, device=dev)}
            if pc is not None and len(cov_names):
                kd = np.concatenate([pc[:, -T:], fc], axis=1).astype(np.float32)
                kd = np.pad(kd, ((0, 0), (pad, 0)), constant_values=np.nan)
                kdm = np.isfinite(kd)
                inputs |= {"known_dynamic": torch.from_numpy(np.nan_to_num(kd, nan=0.0))[None].to(dev),
                           "known_dynamic_mask": torch.from_numpy(kdm)[None].to(dev),
                           "known_dynamic_series_ids": torch.zeros((1, kd.shape[0]), dtype=torch.long,
                                                                   device=dev)}
            with torch.inference_mode():
                q = self.model.forecast(inputs, horizon=H, decode_block_size=None, has_missing_values=True,
                                        scaler_fallback_min_obs=8, quantile_real_cap_k=1e4)  # (9, 1, n, H)
            q = q[:, 0].float().cpu().numpy()
            res[keep, :, 0] = q[self.qi[0]]
            res[keep, :, 1] = q[self.qi[1]]
            out.append(res)
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, torch.cuda.max_memory_allocated() / 1e9)
        return out


def _quantile_columns(levels, want=QUANTILES) -> list[int | None]:
    """Index of each wanted quantile in a model's quantile levels (None = not in the head)."""
    lv = [round(float(q), 6) for q in levels]
    return [lv.index(q) if q in lv else None for q in want]


class PatchTSTFMBackend(_Backend):
    """PAR-1067: Granite PatchTST-FM-r2 (385M; Apache-2.0 OR OpenMDW-1.0). Univariate: the
    rides of a park are a batch of independent series. ``forward`` has no covariate input
    (``future_values`` is commented out as "future use" in granite-tsfm 0.3.10), so the
    model never sees one. NaN is masked natively (``past_observed_mask`` from NaN). The
    99-quantile head (0.01 … 0.99) holds q0.5, q0.8 AND q0.95."""

    name = "patchtst_fm"
    #: Hugging Face revision of the weights (2026-09-09)
    REVISION = os.environ.get("MLBENCH_PATCHTSTFM_REVISION", "b125275f9204d37cbb81fe47b9cf5e08a521e829")

    def __init__(self, model_id: str = "ibm-granite/granite-timeseries-patchtst-fm-r2",
                 batch_size: int = int(os.environ.get("MLBENCH_PATCHTSTFM_BATCH", "16"))):
        import torch
        from tsfm_public import PatchTSTFMForPrediction

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = PatchTSTFMForPrediction.from_pretrained(model_id, revision=self.REVISION)
        self.model = self.model.to(self.device).eval()
        self.qi = _quantile_columns(self.model.config.quantile_levels)
        #: context + forecast must fit the model's 8192 steps
        self.window = int(self.model.config.context_length)
        #: the model always decodes at least this many steps (d_patch × pretrain_mask_cont = 128)
        self.min_h = int(self.model.config.d_patch) * max(int(self.model.config.pretrain_mask_cont), 2)
        self.batch_size = batch_size

    def forecast(self, tasks, cov_names, pipe=None):
        torch = self.torch
        out = []
        for y, _pc, fc in tasks:
            H = fc.shape[1]
            res = np.full((y.shape[0], H, 3), np.nan)
            keep = np.flatnonzero(np.isfinite(y).any(axis=1))
            ctx = y[keep][:, -(self.window - max(H, self.min_h)):].astype(np.float32)
            for b in range(0, len(keep), self.batch_size):
                x = torch.from_numpy(np.ascontiguousarray(ctx[b:b + self.batch_size])).to(self.device)
                with torch.inference_mode():
                    o = self.model(past_values=x, prediction_length=H, return_dict=True)
                q = o.quantile_outputs.float().cpu().numpy()[..., 0]        # (B, 99, H)
                for j, k in enumerate(self.qi):
                    res[keep[b:b + self.batch_size], :, j] = q[:, k, :]
            out.append(res)
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, torch.cuda.max_memory_allocated() / 1e9)
        return out


def _standardise(a: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per-row z-score over the finite values (TTM's card: scale every channel yourself)."""
    with np.errstate(all="ignore"):
        mu = np.nanmean(a, axis=-1, keepdims=True)
        sd = np.nanstd(a, axis=-1, keepdims=True)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    sd = np.where(np.isfinite(sd) & (sd > 1e-6), sd, 1.0)
    return (a - mu) / sd, mu, sd


def ttm_windows(T: int, C: int, H: int, min_past: int, stride: int) -> list[tuple[int, int, int]]:
    """Training windows inside a context of length T: (past start, cut, future end), the
    past ``[start, cut)`` at most C long, the future ``[cut, end)`` at most H long and
    ALWAYS ending at or before T — the context is truth before the origin, so no window can
    reach past it."""
    return [(max(0, e - C), e, min(e + H, T)) for e in range(min_past, T, stride)]


class TTMBackend(_Backend):
    """PAR-1067: Granite TinyTimeMixer r2 (Apache-2.0), branch ``1536-720-r2`` (context 1536,
    horizon 720, 15-min is in r2's "minutely to hourly" range). Point forecast only: q80 and
    q95 are NaN. Zero-shot it is channel-independent and takes no covariates; the
    exogenous path (``exogenous_channel_indices`` + forecast channel mixing) is a block the
    pretrained weights do not contain, so it exists only after ``finetune``."""

    name = "ttm_r2"
    MODEL_ID = "ibm-granite/granite-timeseries-ttm-r2"
    #: the commit of branch 1536-720-r2 (branches move; a commit does not)
    REVISION = os.environ.get("MLBENCH_TTM_REVISION", "3ca910a81018c6a6159a02e2505f3d62c39e91a4")

    def __init__(self, batch_size: int = int(os.environ.get("MLBENCH_TTM_BATCH", "256"))):
        import torch
        from tsfm_public import TinyTimeMixerForPrediction

        self.torch = torch
        self.cls = TinyTimeMixerForPrediction
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = self.cls.from_pretrained(self.MODEL_ID, revision=self.REVISION).to(self.device).eval()
        self.C = int(self.model.config.context_length)
        self.max_h = int(self.model.config.prediction_length)
        self.batch_size = batch_size

    @staticmethod
    def _channels(y: np.ndarray, cov: np.ndarray | None) -> tuple[np.ndarray, np.ndarray]:
        """(T,) target + (k, T) covariates -> values (T, 1+k) with NaN as 0, observed mask."""
        v = y[None] if cov is None else np.vstack([y[None], cov])
        return np.nan_to_num(v, nan=0.0).T.astype(np.float32), np.isfinite(v).T

    def _pad(self, v: np.ndarray, m: np.ndarray, L: int, left: bool):
        n = L - len(v)
        if n <= 0:
            return (v[-L:], m[-L:]) if left else (v[:L], m[:L])
        pv, pm = np.zeros((n, v.shape[1]), np.float32), np.zeros((n, v.shape[1]), bool)
        return (np.vstack([pv, v]), np.vstack([pm, m])) if left else (np.vstack([v, pv]), np.vstack([m, pm]))

    def finetune(self, tasks, cov_names, prediction_length: int, steps: int, context_length: int,
                 learning_rate: float, batch_size: int, mode: str = "full"):
        """A TTM with ``len(cov_names)`` exogenous channels and forecast channel mixing,
        backbone frozen, decoder + head + mixer trained on windows of the CONTEXT of
        ``tasks`` (truth before the origin only, see ``ttm_windows``)."""
        torch = self.torch
        k = len(cov_names)
        m = self.cls.from_pretrained(
            self.MODEL_ID, revision=self.REVISION, num_input_channels=1 + k, prediction_channel_indices=[0],
            exogenous_channel_indices=list(range(1, 1 + k)), enable_forecast_channel_mixing=k > 0,
            decoder_mode="mix_channel", fcm_use_mixer=True, fcm_mix_layers=2, fcm_context_length=1,
            fcm_prepend_past=True, ignore_mismatched_sizes=True).to(self.device)
        for p in m.backbone.parameters():
            p.requires_grad = False
        prep = [(_standardise(y)[0], _standardise(pc)[0] if pc is not None and k else None)
                for y, pc, _fc in tasks]
        samples = [(ti, i, s, e, f)
                   for ti, (z, _cz) in enumerate(prep) for i in range(z.shape[0])
                   for s, e, f in ttm_windows(z.shape[1], self.C, self.max_h, min_past=self.C // 4,
                                              stride=max(self.max_h // 4, 1))
                   if np.isfinite(z[i, e:f]).any() and np.isfinite(z[i, s:e]).sum() >= 32]
        if not samples:
            raise ValueError("ttm_r2_ft: no training window with truth before the origin")
        rng = np.random.default_rng(1066)
        opt = torch.optim.AdamW([p for p in m.parameters() if p.requires_grad], lr=learning_rate)
        m.train()
        for _step in range(steps):
            pick = rng.integers(0, len(samples), size=min(batch_size, len(samples)))
            pv, pm, fv, fm = [], [], [], []
            for j in pick:
                ti, i, s, e, f = samples[j]
                z, cz = prep[ti]
                a, am = self._pad(*self._channels(z[i, s:e], None if cz is None else cz[:, s:e]), self.C, True)
                b, bm = self._pad(*self._channels(z[i, e:f], None if cz is None else cz[:, e:f]), self.max_h, False)
                pv.append(a), pm.append(am), fv.append(b), fm.append(bm)
            t = lambda xs, dt_=torch.float32: torch.from_numpy(np.stack(xs)).to(self.device, dt_)  # noqa: E731
            fvt, fmt = t(fv), t(fm, torch.bool)
            target, tmask = fvt[..., 0].clone(), fmt[..., 0]
            # the model gets exactly what forecast() gives it: future covariates, 0 on the
            # target channel and no future mask, so the label is never an input; the masked
            # MSE on channel 0 is computed here instead of by the model from future_values
            fvt[..., 0] = 0
            pred = m(past_values=t(pv), past_observed_mask=t(pm, torch.bool),
                     future_values=fvt if k else None, return_loss=False,
                     return_dict=True).prediction_outputs[..., 0]
            loss = ((pred - target) ** 2)[tmask].mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
        return m.eval()

    def forecast(self, tasks, cov_names, pipe=None):
        torch = self.torch
        model = pipe or self.model
        exog = pipe is not None and len(cov_names) > 0
        out = []
        for y, pc, fc in tasks:
            H = fc.shape[1]
            if H > self.max_h:
                raise ValueError(f"ttm_r2: horizon {H} > {self.max_h} (branch 1536-720-r2)")
            res = np.full((y.shape[0], H, 3), np.nan)
            keep = np.flatnonzero(np.isfinite(y).any(axis=1))
            z, mu, sd = _standardise(y)
            if exog:
                # the covariates are scaled with their PAST statistics on both sides
                _, cmu, csd = _standardise(pc)
                cz, czf = (pc - cmu) / csd, (fc - cmu) / csd
            batch = []
            for i in keep:
                a, am = self._pad(*self._channels(z[i], cz if exog else None), self.C, True)
                if exog:
                    b, bm = self._pad(*self._channels(np.full(H, np.nan), czf), self.max_h, False)
                else:
                    b = bm = None
                batch.append((i, a, am, b, bm))
            for s in range(0, len(batch), self.batch_size):
                chunk = batch[s:s + self.batch_size]
                kw = {"past_values": torch.from_numpy(np.stack([c[1] for c in chunk])).to(self.device),
                      "past_observed_mask": torch.from_numpy(np.stack([c[2] for c in chunk])).to(self.device)}
                if exog:
                    kw["future_values"] = torch.from_numpy(np.stack([c[3] for c in chunk])).to(self.device)
                with torch.inference_mode():
                    p = model(**kw, return_loss=False, return_dict=True).prediction_outputs
                p = p.float().cpu().numpy()[:, :H, 0]
                rows = [c[0] for c in chunk]
                res[rows, :, 0] = p * sd[rows] + mu[rows]
            out.append(res)
        if self.device == "cuda":
            self.peak_vram_gb = max(self.peak_vram_gb, torch.cuda.max_memory_allocated() / 1e9)
        return out


class TabPFNTSBackend(_Backend):
    """PAR-1067: TabPFN-TS 1.3.0 on TabPFN-3.5 weights (NON-COMMERCIAL, NON-PRODUCTION
    licence: benchmark-only). Daily levels only (``wants_days``: the pipeline builds its
    calendar features from real dates). Every ride is its own series, the day covariates
    are known-future columns, NaN is TabPFN's own missing value. LOCAL mode only — the
    CLIENT mode would send our data to Prior Labs' cloud. The gated weights need
    ``HF_TOKEN`` (Hugging Face) and ``TABPFN_TOKEN`` (Prior Labs' licence acceptance) in
    the environment at run time; neither is ever written to the repository or the image."""

    name = "tabpfn_ts"
    wants_days = True

    def __init__(self, max_context: int = int(os.environ.get("MLBENCH_TABPFN_CONTEXT", "4096"))):
        # anonymous usage telemetry (posthog) is on by default in tabpfn-common-utils
        os.environ.setdefault("TABPFN_DISABLE_TELEMETRY", "1")
        import torch
        from tabpfn_time_series import TabPFNMode, TabPFNTSPipeline
        from tabpfn_time_series.features import CalendarFeature, RunningIndexFeature

        self.torch = torch
        _cap_vram(torch)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        # no AutoSeasonalFeature: closed days leave the daily series irregular, and the
        # package recommends opting out of it for irregular timestamps
        self.pipe = TabPFNTSPipeline(max_context_length=max_context, tabpfn_mode=TabPFNMode.LOCAL,
                                     temporal_features=[RunningIndexFeature(), CalendarFeature()])

    @staticmethod
    def frames(y, pc, fc, cov_names, ctx_days, fut_days) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Long context / future frames (item_id = row of y); a day without a level is
        left out of the context, never filled."""
        n, T = y.shape
        H = len(fut_days)
        ctx = pd.DataFrame({"item_id": np.repeat(np.arange(n), T),
                            "timestamp": np.tile(pd.to_datetime(ctx_days), n),
                            "target": y.reshape(-1).astype(float)})
        fut = pd.DataFrame({"item_id": np.repeat(np.arange(n), H),
                            "timestamp": np.tile(pd.to_datetime(fut_days), n)})
        for k, c in enumerate(cov_names):
            ctx[c] = np.tile(pc[k], n)
            fut[c] = np.tile(fc[k], n)
        ctx = ctx[np.isfinite(ctx["target"])]
        fut = fut[fut["item_id"].isin(ctx["item_id"].unique())]
        return ctx.reset_index(drop=True), fut.reset_index(drop=True)

    def forecast(self, tasks, cov_names, pipe=None, days=None):
        import warnings

        if days is None:
            raise ValueError("tabpfn_ts: daily levels only (the backend needs the calendar dates)")
        ctx_days, fut_days = days
        out = []
        for y, pc, fc in tasks:
            res = np.full((y.shape[0], len(fut_days), 3), np.nan)
            ctx, fut = self.frames(y, pc, fc, cov_names, ctx_days, fut_days)
            if len(ctx):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)   # NaN future covariates are expected
                    p = self.pipe.predict_df(ctx, future_df=fut, quantiles=[0.5]).reset_index()
                pos = pd.Series(np.arange(len(fut_days)), index=pd.to_datetime(fut_days))
                res[p["item_id"].to_numpy(int), pos.reindex(pd.to_datetime(p["timestamp"])).to_numpy(int), 0] = \
                    p["target"].to_numpy(float)
            out.append(res)
        return out


def get_backend(kind: str) -> _Backend:
    if kind not in _BACKENDS:
        t0 = time.monotonic()
        _BACKENDS[kind] = {"chronos2": ChronosBackend, "timesfm3": TimesFMBackend,
                           "timesfm25": TimesFM25Backend,
                           "tirex2": TiRex2Backend, "toto2": Toto2Backend,
                           "patchtst_fm": PatchTSTFMBackend, "ttm_r2": TTMBackend,
                           "tabpfn_ts": TabPFNTSBackend,
                           "stub": StubBackend}[kind]()
        print(f"[foundation] loaded {kind} in {time.monotonic() - t0:.1f}s", flush=True)
    return _BACKENDS[kind]


# --------------------------------------------------------------------------- inputs

def day_covariates(cov: pd.DataFrame, weather: bool) -> pd.DataFrame:
    """Per (park_id, date): the numeric day covariates the models read. Bridge days are
    derived here from the public-holiday flag (Mon before a Tue holiday, Fri after a Thu
    holiday) so past and future days carry the same definition — the schedule's own
    bridge flag exists only where the schedule was known at the origin."""
    d = cov.copy()
    d["date"] = pd.to_datetime(d["date"]).dt.date
    dow = pd.to_datetime(d["date"]).dt.dayofweek          # 0 = Monday
    d["is_weekend"] = dow.isin([5, 6]).astype(float)
    d["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    d["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    d = d.sort_values(["park_id", "date"])
    hol = d["is_holiday_primary"].fillna(0).astype(float)
    g = d.assign(h=hol).groupby("park_id")["h"]
    nxt, prv = g.shift(-1).fillna(0), g.shift(1).fillna(0)
    d["is_bridge_day"] = (((dow == 0) & (nxt > 0)) | ((dow == 4) & (prv > 0))).astype(float) * (1 - hol)
    for col in DAY_COVS + (WX_COVS if weather else []):
        if col not in d:
            d[col] = np.nan
        d[col] = pd.to_numeric(d[col], errors="coerce").astype(float)
    # published day length: hours where the schedule was known at the origin, 0 for a
    # known closed day, NaN where the window is projected (never a fabricated fact)
    known = d.get("schedule_known_at_origin")
    hours = (pd.to_datetime(d["open_utc"], utc=True).rsub(pd.to_datetime(d["close_utc"], utc=True))
             .dt.total_seconds() / 3600) if "open_utc" in d else pd.Series(np.nan, index=d.index)
    if known is not None:
        known = known.fillna(False).astype(bool)
        d["day_len_h"] = np.where(known, hours.fillna(0.0), np.nan)
    else:
        d["day_len_h"] = np.nan
    return d


def windows_from_history(h: pd.DataFrame) -> pd.DataFrame:
    """Past operating windows per (park_id, date), recovered from the truth slots' ko/kc
    (minutes since the published opening / to the published closing). A day on which no
    ride of the park has a truth slot has no window — exactly the days the context skips."""
    s = pd.to_datetime(h["slot_start_utc"], utc=True)
    w = pd.DataFrame({"park_id": h["park_id"].to_numpy(), "date": pd.to_datetime(h["date"]).dt.date,
                      "open_utc": s - pd.to_timedelta(h["ko"].to_numpy() * 15, unit="min"),
                      "close_utc": s + pd.to_timedelta((h["kc"].to_numpy() + 1) * 15, unit="min")})
    return w.groupby(["park_id", "date"]).agg(open_utc=("open_utc", "min"),
                                              close_utc=("close_utc", "max")).reset_index()


def _expand(w: pd.DataFrame) -> pd.DataFrame:
    """Window rows -> one row per 15-min slot (park_id, date, slot_utc, ko_min, kc_min)."""
    n = ((w["close_utc"] - w["open_utc"]).dt.total_seconds() // 900).astype(int).clip(lower=0)
    rep = w.loc[w.index.repeat(n)].copy()
    k = rep.groupby(level=0).cumcount().to_numpy()
    rep["slot_utc"] = rep["open_utc"] + pd.to_timedelta(k * 15, unit="min")
    rep["ko_min"] = k * 15.0
    rep["kc_min"] = (rep["close_utc"] - rep["slot_utc"]).dt.total_seconds() / 60 - 15
    return rep[["park_id", "date", "slot_utc", "ko_min", "kc_min"]].reset_index(drop=True)


def _tod(slot_utc: pd.Series, tz: str) -> np.ndarray:
    loc = slot_utc.dt.tz_convert(tz)
    return (loc.dt.hour + loc.dt.minute / 60.0).to_numpy(dtype=float)


# --------------------------------------------------------------------------- the model

class FoundationModel(Model):
    """Zero-shot foundation model adapter. Subclasses pick backend / layout / covariates."""

    name: ClassVar[str] = "foundation"
    backend: ClassVar[str] = "chronos2"
    context_mode: ClassVar[str] = "compressed"
    use_covariates: ClassVar[bool] = True
    daily: ClassVar[bool] = True
    provides_quantiles = True
    intraday = True
    max_lead_days = 7
    refit_every_days = None          # zero-shot: nothing to fit (fit() is NOT overridden)
    context_slots: ClassVar[int] = int(os.environ.get("MLBENCH_FM_CONTEXT", "8192"))
    daily_context_days: ClassVar[int] = 400
    covariate_history_days = 400
    #: `predict()` walks back `context_slots // 24 + 2` days (compressed) and
    #: `predict_daily()` `daily_context_days`; the runner has to materialise the larger
    #: of the two or `HistoryView.df()` raises (critic S9 capped it at 56).
    truth_history_days: ClassVar[int] = max(400, context_slots // 24 + 2)

    #: the weights' licence verdict, and whether the arm may ever be proposed for serving
    licence: ClassVar[str] = "unclear"
    serving_candidate: ClassVar[bool] = False

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if cls.licence not in LICENCES:
            raise TypeError(f"{cls.__name__}: licence must be one of {LICENCES}, got {cls.licence!r}")
        if cls.serving_candidate and cls.licence != "servable":
            raise TypeError(f"{cls.__name__}: a {cls.licence} model can never be a serving candidate")
        # the class and mlbench/benchmark_only_models.json may not drift apart
        if is_benchmark_only(getattr(cls, "name", "")) and cls.licence != "benchmark-only":
            raise TypeError(f"{cls.__name__}: {cls.name!r} is listed benchmark-only in "
                            f"benchmark_only_models.json but declares licence {cls.licence!r}")

    def __init__(self):
        self.stats = {"slot_calls": 0, "slot_seconds": 0.0, "daily_calls": 0, "daily_seconds": 0.0}
        self._hkey = None
        self._hist = None

    def _engine(self) -> _Backend:
        return get_backend(os.environ.get("MLBENCH_FM_BACKEND", self.backend))

    def _cov_names(self, slot: bool) -> list[str]:
        if not self.use_covariates:
            return []
        names = (SLOT_COVS + (["in_win"] if self.context_mode == "grid" else [])) if slot else []
        names = names + DAY_COVS + (WX_COVS if self.uses_oracle_weather else [])
        if not slot and self.backend in NAN_MASKING_BACKENDS:
            names = names + ["day_len_h"]   # NaN beyond the known schedule: these models mask NaN
        return names

    def _log(self, origin: Origin, what: str, t0: float, rows: int) -> None:
        sec = time.monotonic() - t0
        k = "daily" if what == "daily" else "slot"
        self.stats[f"{k}_calls"] += 1
        self.stats[f"{k}_seconds"] += sec
        eng = _BACKENDS.get(os.environ.get("MLBENCH_FM_BACKEND", self.backend))
        print(f"[fm] {self.scored_name()} {origin.date} {origin.kind}@{origin.hour_local} {what} "
              f"{sec:.1f}s (model {getattr(self, '_t_engine', 0):.1f}s) rows={rows} vram_peak={getattr(eng, 'peak_vram_gb', 0):.2f}GB", flush=True)

    #: fine-tuning (Chronos-2 only): None = zero-shot
    finetune_every_days: ClassVar[int | None] = None
    finetune_steps: ClassVar[int] = int(os.environ.get("MLBENCH_FT_STEPS", "400"))
    finetune_context: ClassVar[int] = 2048
    finetune_horizon: ClassVar[int] = 384      # ~8 operating days of 15-min slots
    finetune_lr: ClassVar[float] = float(os.environ.get("MLBENCH_FT_LR", "1e-5"))
    #: "full" or "lora" (chronos-forecasting's own fit(finetune_mode=...))
    finetune_mode: ClassVar[str] = "full"

    def _maybe_finetune(self, origin: Origin, tasks, names):
        """Fine-tune on the series the model is about to forecast from (truth before
        this origin only), at the first daily origin and every ``finetune_every_days``;
        intraday origins reuse the last fine-tuned weights."""
        if self.finetune_every_days is None:
            return None
        last = getattr(self, "_ft_date", None)
        due = last is None or (origin.date - last).days >= self.finetune_every_days
        if origin.kind == "daily" and due:
            t0 = time.monotonic()
            eng = self._engine()
            self._ft_pipe = eng.finetune(
                tasks, names, prediction_length=self.finetune_horizon, steps=self.finetune_steps,
                context_length=self.finetune_context, learning_rate=self.finetune_lr, batch_size=64,
                mode=self.finetune_mode)
            self._ft_date = origin.date
            sec = time.monotonic() - t0
            self.stats["finetune_calls"] = self.stats.get("finetune_calls", 0) + 1
            self.stats["finetune_seconds"] = self.stats.get("finetune_seconds", 0.0) + sec
            torch = getattr(eng, "torch", None)
            vram = torch.cuda.max_memory_allocated() / 1e9 if torch is not None and torch.cuda.is_available() else 0.0
            print(f"[fm] {self.scored_name()} fine-tuned ({self.finetune_mode}) at {origin.date} in {sec:.0f}s "
                  f"vram_peak={vram:.2f}GB", flush=True)
        return getattr(self, "_ft_pipe", None)

    def _history(self, origin: Origin, days: int) -> pd.DataFrame:
        """Truth before the origin. The days before the origin date are cached per date
        (the daily call and the intraday calls of one date share them); the origin day's
        own slots are always taken from THIS origin's view (its cut)."""
        c = origin.date
        if self._hkey != (c, days):
            full = origin.history.df(days)
            full = full[pd.to_datetime(full["date"]).dt.date < c]
            self._hist, self._hkey = full, (c, days)
        today = origin.history.df(0)
        return pd.concat([self._hist, today], ignore_index=True) if len(today) else self._hist

    # -- 15-min slots
    def predict(self, origin: Origin, horizon_slots: pd.DataFrame,
                known_future_covariates: pd.DataFrame) -> pd.DataFrame:
        if horizon_slots.empty:
            return EMPTY
        t0 = time.monotonic()
        per_day = 24 if self.context_mode == "compressed" else 96
        days_back = self.context_slots // per_day + 2
        hist = self._history(origin, days_back)
        if hist.empty:
            return EMPTY
        hist = hist.assign(slot_start_utc=pd.to_datetime(hist["slot_start_utc"], utc=True))
        hz = horizon_slots.copy()
        hz["slot_start_utc"] = pd.to_datetime(hz["slot_start_utc"], utc=True)
        hz["date"] = pd.to_datetime(hz["date"]).dt.date
        ou = origin.origin_utc.set_index("park_id")
        names = self._cov_names(slot=True)
        dc = day_covariates(known_future_covariates, self.uses_oracle_weather).set_index(["park_id", "date"]) \
            if names else None

        # past window slots (from the truth) + future window slots (the harness's grid)
        past = _expand(windows_from_history(hist))
        fut_all = hz.drop_duplicates(["park_id", "slot_start_utc"])[["park_id", "date", "slot_start_utc", "ko", "kc"]]
        fut_all = fut_all.rename(columns={"slot_start_utc": "slot_utc"})
        fut_all["ko_min"] = fut_all["ko"] * 15.0
        fut_all["kc_min"] = fut_all["kc"] * 15.0
        hist_by_park = dict(tuple(hist.groupby("park_id")))
        past_by_park = dict(tuple(past.groupby("park_id")))
        rides_by_park = hz.groupby("park_id")["attraction_id"].unique()

        tasks, meta = [], []
        for pid, fut in fut_all.groupby("park_id", sort=False):
            org = pd.Timestamp(ou.at[pid, "origin_utc"])
            org = org.tz_localize("UTC") if org.tzinfo is None else org.tz_convert("UTC")
            tz = ou.at[pid, "timezone"]
            pw = past_by_park.get(pid)
            if pw is None:
                continue
            pw = pw[pw["slot_utc"] + pd.Timedelta(minutes=15) <= org]
            fut = fut[fut["slot_utc"] >= org].sort_values("slot_utc")
            if self.context_mode == "compressed":
                ctx = pw.sort_values("slot_utc").drop_duplicates("slot_utc", keep="last").tail(self.context_slots)
                ctx = ctx.assign(in_win=1.0)
                fseq = fut[["date", "slot_utc", "ko_min", "kc_min"]].assign(in_win=1.0)
            else:
                ctx, fseq = self._grid(pw, fut, org, tz)
            if ctx.empty or fseq.empty:
                continue
            ctx = ctx.assign(tod=_tod(ctx["slot_utc"], tz))
            fseq = fseq.assign(tod=_tod(fseq["slot_utc"], tz))
            rides = sorted(rides_by_park[pid])
            Y = np.full((len(rides), len(ctx)), np.nan, dtype=np.float32)
            h = hist_by_park.get(pid)
            if h is not None and len(h):
                ri = pd.Series(np.arange(len(rides)), index=rides)
                ci = pd.Series(np.arange(len(ctx)), index=pd.DatetimeIndex(ctx["slot_utc"]))
                r_ix = ri.reindex(h["attraction_id"].to_numpy()).to_numpy()
                c_ix = ci.reindex(pd.DatetimeIndex(h["slot_start_utc"])).to_numpy()
                ok = np.isfinite(r_ix) & np.isfinite(c_ix)
                Y[r_ix[ok].astype(int), c_ix[ok].astype(int)] = h["y"].to_numpy(dtype=np.float32)[ok]
            if not np.isfinite(Y).any():
                continue
            if names:
                def cov_matrix(seq):
                    idx = pd.MultiIndex.from_arrays([np.full(len(seq), pid, dtype=object), seq["date"].to_numpy()])
                    dd = dc.reindex(idx)
                    cols = [seq[n].to_numpy(dtype=np.float32) if n in seq else dd[n].to_numpy(dtype=np.float32)
                            for n in names]
                    return np.vstack(cols)
                pc, fc = cov_matrix(ctx), cov_matrix(fseq)
            else:
                pc, fc = None, np.zeros((0, len(fseq)), np.float32)
            tasks.append((Y, pc, fc))
            meta.append((rides, fseq["slot_utc"].to_numpy()))
        if not tasks:
            return EMPTY
        te = time.monotonic()
        preds = self._engine().forecast(tasks, names, pipe=self._maybe_finetune(origin, tasks, names))
        self._t_engine = time.monotonic() - te
        frames = []
        for (rides, fslots), q in zip(meta, preds):
            n, H = len(rides), len(fslots)
            frames.append(pd.DataFrame({
                "attraction_id": np.repeat(np.asarray(rides, dtype=object), H),
                "slot_start_utc": np.tile(fslots, n),
                "q50": q[:, :, 0].reshape(-1), "q80": q[:, :, 1].reshape(-1),
                "q95": q[:, :, 2].reshape(-1)}))
        out = pd.concat(frames, ignore_index=True)
        out["slot_start_utc"] = pd.to_datetime(out["slot_start_utc"], utc=True)
        for k in ("q50", "q80", "q95"):
            out[k] = out[k].clip(lower=0)
        # a point-only backend (TTM) leaves q80 NaN: fmax alone would turn it into q50
        out["q80"] = np.where(np.isfinite(out["q80"]), np.fmax(out["q80"], out["q50"]), np.nan)
        out["q95"] = np.where(np.isfinite(out["q95"]), np.fmax(out["q95"], out["q80"]), np.nan)
        out = out.merge(hz[["attraction_id", "slot_start_utc"]], on=["attraction_id", "slot_start_utc"])
        out = out[np.isfinite(out["q50"])]
        self._log(origin, "slots", t0, len(out))
        return out[["attraction_id", "slot_start_utc", "q50", "q80", "q95"]]

    def _grid(self, pw: pd.DataFrame, fut: pd.DataFrame, org: pd.Timestamp, tz: str):
        """Regular 15-min grid: context = the last ``context_slots`` grid steps before the
        origin, future = every step up to the last requested slot. Off-window steps carry
        in_win = 0 and NaN for the window-relative minutes (Chronos-2 masks NaN)."""
        t0 = org - pd.Timedelta(minutes=15 * self.context_slots)
        ctx_t = pd.date_range(t0, org - pd.Timedelta(minutes=15), freq="15min", tz="UTC")
        fut_t = pd.date_range(org, fut["slot_utc"].max(), freq="15min", tz="UTC")

        def fill(times, w):
            g = pd.DataFrame({"slot_utc": times})
            g = g.merge(w[["slot_utc", "date", "ko_min", "kc_min"]], on="slot_utc", how="left")
            g["in_win"] = g["ko_min"].notna().astype(float)
            loc_date = g["slot_utc"].dt.tz_convert(tz).dt.date
            g["date"] = g["date"].where(g["date"].notna(), pd.Series(loc_date, index=g.index))
            return g

        return fill(ctx_t, pw.drop_duplicates("slot_utc", keep="last")), fill(fut_t, fut)

    # -- daily levels (UC4 + level × H5 at long leads)
    def predict_daily(self, origin: Origin, horizon_days: pd.DataFrame,
                      known_future_covariates: pd.DataFrame) -> pd.DataFrame | None:
        if not self.daily or horizon_days.empty:
            return None
        t0 = time.monotonic()
        c = origin.date
        h = origin.history.df(self.daily_context_days)
        if h.empty:
            return None
        h["date"] = pd.to_datetime(h["date"]).dt.date
        h = h[h["date"] < c]
        g = h.groupby(["park_id", "attraction_id", "date"])["y"]
        rd = pd.concat([g.quantile(0.9).rename("p90"), g.size().rename("n")], axis=1).reset_index()
        rd = rd[rd["n"] >= 8]
        if rd.empty:
            return None
        max_lead = int(horizon_days["lead_days"].max())
        ctx_days = list(pd.date_range(rd["date"].min(), c - dt.timedelta(days=1)).date)
        fut_days = list(pd.date_range(c, c + dt.timedelta(days=max_lead)).date)
        if len(ctx_days) < 14:
            return None
        names = self._cov_names(slot=False)
        dc = day_covariates(known_future_covariates, self.uses_oracle_weather) if names else None
        if dc is not None:
            # a past day's length comes from the truth windows (NaN when nothing ran)
            pw = windows_from_history(h)
            pw["len_past"] = (pw["close_utc"] - pw["open_utc"]).dt.total_seconds() / 3600
            dc = dc.merge(pw[["park_id", "date", "len_past"]], on=["park_id", "date"], how="left")
            past = pd.to_datetime(dc["date"]) < pd.Timestamp(c)
            dc.loc[past, "day_len_h"] = dc.loc[past, "len_past"]
        rides_by_park = horizon_days.groupby("park_id")["attraction_id"].unique()
        rd_by_park = dict(tuple(rd.groupby("park_id")))
        dc_by_park = dict(tuple(dc.groupby("park_id"))) if dc is not None else {}
        tasks, meta = [], []
        all_days = ctx_days + fut_days
        for pid in sorted(rides_by_park.index):
            rides = sorted(rides_by_park[pid])
            r = rd_by_park.get(pid)
            if r is None or r.empty:
                continue
            piv = r.pivot(index="attraction_id", columns="date", values="p90")
            Y = piv.reindex(index=rides, columns=ctx_days).to_numpy(dtype=np.float32)
            if not np.isfinite(Y).any():
                continue
            if names:
                cv = dc_by_park[pid].set_index("date").reindex(all_days)[names].to_numpy(dtype=np.float32).T
                pc, fc = cv[:, :len(ctx_days)], cv[:, len(ctx_days):]
            else:
                pc, fc = None, np.zeros((0, len(fut_days)), np.float32)
            tasks.append((Y, pc, fc))
            meta.append(rides)
        if not tasks:
            return None
        te = time.monotonic()
        eng = self._engine()
        kw = {"days": (ctx_days, fut_days)} if getattr(eng, "wants_days", False) else {}
        preds = eng.forecast(tasks, names, **kw)
        self._t_engine = time.monotonic() - te
        frames = []
        for rides, q in zip(meta, preds):
            frames.append(pd.DataFrame({
                "attraction_id": np.repeat(np.asarray(rides, dtype=object), len(fut_days)),
                "date": np.tile(np.asarray(fut_days, dtype=object), len(rides)),
                "level": np.clip(q[:, :, 0].reshape(-1), 0, None)}))
        out = pd.concat(frames, ignore_index=True)
        want = horizon_days[["attraction_id", "date"]].copy()
        want["date"] = pd.to_datetime(want["date"]).dt.date
        out = out.merge(want, on=["attraction_id", "date"])
        out = out[np.isfinite(out["level"])]
        self._log(origin, "daily", t0, len(out))
        return out


# --------------------------------------------------------------------------- registered variants

class Chronos2(FoundationModel):
    """Chronos-2 zero-shot, compressed operating-slot context, no weather (the honest variant)."""
    name = "chronos2"
    backend = "chronos2"
    licence = "servable"              # Apache-2.0 code and weights
    serving_candidate = True


class Chronos2Owx(Chronos2):
    """+ daily weather ACTUALS — ORACLE upper bound, scored as chronos2_owx."""
    uses_oracle_weather = True
    intraday = False


class Chronos2Grid(Chronos2):
    """Regular 15-min grid context with NaN outside the windows (layout test)."""
    name = "chronos2_grid"
    context_mode = "grid"
    intraday = False
    daily = False


class Chronos2NoCov(Chronos2):
    """No covariates at all (what the covariates are worth)."""
    name = "chronos2_nocov"
    use_covariates = False
    intraday = False
    daily = False


class TimesFM3(FoundationModel):
    """TimesFM 3.0 zero-shot, compressed context, no weather. BENCHMARK-ONLY, NON-PRODUCTION:
    the weights' licence excludes production and end-user use (see the module docstring)."""
    name = "timesfm3"
    backend = "timesfm3"
    intraday = False
    licence = "benchmark-only"        # TimesFM Non-Commercial License v1.0: non-production


class TimesFM3Owx(TimesFM3):
    uses_oracle_weather = True
    daily = False


class Chronos2FT(Chronos2):
    """Chronos-2 fully fine-tuned (time-boxed: 400 steps, lr 1e-5, context 2048) on the
    benchmark's own history before each origin, refreshed every 28 days."""
    name = "chronos2_ft"
    finetune_every_days = 28
    daily = False


class Chronos2Lora(Chronos2FT):
    """PAR-1063: a LoRA adapter (peft, r=8, alpha=16, the package default targets) instead
    of a full fine-tune, everything else as ``chronos2_ft``. Refit every 28 days, not per
    origin: 231 per-origin fits would add 231 × fit time to a ~12.7 h inference night
    (see ``--probe``), while 28 days is ~9 fits and the SAME cadence as chronos2_ft, so
    the pair isolates the adaptation method. Each fit sees truth before its own origin."""
    name = "chronos2_lora"
    finetune_mode = "lora"


class TimesFM25(FoundationModel):
    """PAR-1064: TimesFM 2.5 (200M, Apache-2.0) zero-shot, compressed context, the chronos2
    covariates through XReg (minus day_len_h, whose NaN means "unknown" and would read 0)."""
    name = "timesfm25"
    backend = "timesfm25"
    licence = "servable"              # Apache-2.0 weights
    serving_candidate = True


class TimesFM25Grid(TimesFM25):
    """Regular 15-min grid (8192 steps = 85 days); TimesFM interpolates the NaN nights."""
    name = "timesfm25_grid"
    context_mode = "grid"
    intraday = False
    daily = False


# ---- PAR-853: TiRex-2 and Toto 2.0 (same inputs as chronos2 where the model takes them)

class TiRex2(FoundationModel):
    """TiRex-2 zero-shot, compressed context, the chronos2 covariates as known-future."""
    name = "tirex2"
    backend = "tirex2"
    licence = "servable"              # Apache-2.0
    serving_candidate = True
    context_slots = int(os.environ.get("MLBENCH_TIREX2_CONTEXT", "2048"))


class TiRex2Grid(TiRex2):
    """Regular 15-min grid (2048 steps = 21 days), NaN outside the windows."""
    name = "tirex2_grid"
    context_mode = "grid"
    intraday = False
    daily = False


class Toto2(FoundationModel):
    """Toto 2.0 (313m) zero-shot, compressed context, the chronos2 covariates through the
    ``known_dynamic`` hook — exogenous inputs are NOT a trained feature of 2.0."""
    name = "toto2"
    backend = "toto2"
    licence = "servable"              # Apache-2.0
    serving_candidate = True
    context_slots = int(os.environ.get("MLBENCH_TOTO2_CONTEXT", "4096"))


class Toto2NoCov(Toto2):
    """The official, covariate-free Toto 2.0 (pairs against chronos2_nocov)."""
    name = "toto2_nocov"
    use_covariates = False
    intraday = False
    daily = False


class Toto2Grid(Toto2):
    """Regular 15-min grid (4096 steps = 42 days), NaN outside the windows."""
    name = "toto2_grid"
    context_mode = "grid"
    intraday = False
    daily = False


# ---- PAR-1067: Granite PatchTST-FM-r2, Granite TTM-r2, TabPFN-TS

class PatchTSTFM(FoundationModel):
    """PatchTST-FM-r2 zero-shot, compressed context (8192 steps minus the horizon). The
    model has NO covariate input, so this is a covariate-free arm like chronos2_nocov;
    the daily path gives patchtst_fm_x_h5. Real q0.95 from the 99-quantile head."""
    name = "patchtst_fm"
    backend = "patchtst_fm"
    use_covariates = False
    licence = "servable"              # Apache-2.0 OR OpenMDW-1.0, the user's choice
    serving_candidate = True


class TTMR2(FoundationModel):
    """TTM-r2 (branch 1536-720-r2) zero-shot, compressed context, no covariates (its
    exogenous path needs fine-tuning). Point forecast: no q80/q95. r2 covers minutely to
    hourly data, so there is no daily path (r2.1 adds daily; not benchmarked here)."""
    name = "ttm_r2"
    backend = "ttm_r2"
    use_covariates = False
    provides_quantiles = False
    daily = False
    context_slots = 1536
    licence = "servable"              # Apache-2.0
    serving_candidate = True


class TTMR2FT(TTMR2):
    """TTM-r2 fine-tuned with the chronos2 slot + day covariates as EXOGENOUS channels
    (forecast channel mixing), backbone frozen, on windows of the truth before the origin
    — refit every 28 days at a daily origin, like chronos2_ft / chronos2_lora."""
    name = "ttm_r2_ft"
    use_covariates = True
    finetune_every_days = 28
    finetune_lr = float(os.environ.get("MLBENCH_TTM_FT_LR", "1e-3"))
    #: the fit draws its windows from 8192 compressed slots (~170 operating days); the
    #: forecast itself reads the last 1536
    context_slots = 8192


class TabPFNTS(FoundationModel):
    """TabPFN-TS on the ride-day P90 level only (UC4 / D6 / D7 / PEAK, and tabpfn_ts_x_h5
    at the slot leads), with the day covariates + published day length as known-future
    columns. BENCHMARK-ONLY: the TabPFN-3.5 weights' licence excludes production use."""
    name = "tabpfn_ts"
    backend = "tabpfn_ts"
    intraday = False
    provides_quantiles = False
    licence = "benchmark-only"        # TABPFN-3.5 License v1.0: non-commercial, non-production
    #: no 15-min slots of its own (tests/test_runner.py reads this)
    slot_path = False

    def predict(self, origin, horizon_slots, known_future_covariates):
        return EMPTY


VARIANTS = [Chronos2, Chronos2Owx, Chronos2Grid, Chronos2NoCov, TimesFM3, TimesFM3Owx, Chronos2FT,
            TiRex2, TiRex2Grid, Toto2, Toto2NoCov, Toto2Grid, Chronos2Lora, TimesFM25, TimesFM25Grid,
            PatchTSTFM, TTMR2, TTMR2FT, TabPFNTS]
