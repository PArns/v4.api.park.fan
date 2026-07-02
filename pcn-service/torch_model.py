"""Shared training/inference base for the torch bake-off candidates.

Every neural candidate (GP-STGNN, the LocalGRU ablation, future STG4Traffic backbones)
shares the same plumbing: robust wait-scaling, lazy windowing into [B, L, N, C] batches,
a masked probabilistic loss (quantile / Tweedie), save/load, and CUDA-first device
selection. Subclasses implement ONLY `build_net()` — the spatio-temporal architecture.

This keeps the bake-off honest: candidates differ by architecture, not by training
harness, so a win is a win on the model, not the loop.
"""

from __future__ import annotations

import logging

import numpy as np
import torch
import torch.nn as nn

import metrics
import windowing
from tensor import CHANNELS

logger = logging.getLogger("pcn.model")

# Wait-valued channels scaled by the robust scale; masks / time sin·cos stay as-is.
_SCALE_CHANNELS = ("wait_ffill", "park_occ")


def masked_pinball(pred, target, mask, quantiles):
    """pred [.., H, Q]; target/mask [.., H]. Mean pinball over masked entries."""
    t = target.unsqueeze(-1)
    m = mask.unsqueeze(-1)
    q = torch.tensor(quantiles, device=pred.device).view(*([1] * (pred.dim() - 1)), -1)
    e = t - pred
    loss = torch.maximum(q * e, (q - 1) * e) * m
    return loss.sum() / (m.sum() * len(quantiles)).clamp_min(1.0)


def masked_tweedie(mu, target, mask, p: float):
    """Tweedie negative log-likelihood (up to const), mu>0, 1<p<2 — the research-
    favoured right-skew likelihood for spiky positive demand."""
    mu = mu.clamp_min(1e-6)
    nll = -target * mu.pow(1 - p) / (1 - p) + mu.pow(2 - p) / (2 - p)
    return (nll * mask).sum() / mask.sum().clamp_min(1.0)


class TorchSeqModel:
    """backtest.Model base. Subclass and implement `build_net(N, C, H)`."""

    arch = "base"

    def __init__(
        self, loss: str = "quantile", quantiles=(0.5, 0.8, 0.9), tweedie_p: float = 1.5,
        hidden: int = 64, max_steps: int = 500, batch_size: int = 16, lr: float = 1e-3,
        max_train_windows: int = 6000, seed: int = 0, **arch_kwargs,
    ):
        self.loss = loss
        self.quantiles = list(quantiles)
        self.tweedie_p = tweedie_p
        self.hidden = hidden
        self.max_steps = max_steps
        self.batch_size = batch_size
        self.lr = lr
        self.max_train_windows = max_train_windows
        self.seed = seed
        self.arch_kwargs = arch_kwargs
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.name = f"{self.arch}-{loss}"
        self._scale = 1.0
        # The channel list this model was TRAINED on (fixed at fit/load). Kept so a
        # tensor built with newer, appended channels still serves an older checkpoint:
        # predict paths select the trained channels BY NAME (see _model_features).
        self.channels: list[str] = list(CHANNELS)
        self._scale_idx = [self.channels.index(c) for c in _SCALE_CHANNELS]
        self._median_q = self.quantiles.index(0.5) if 0.5 in self.quantiles else 0
        self.net: nn.Module | None = None
        self._dims: tuple[int, int, int] | None = None  # (N, C, H)

    # -- subclass hook ------------------------------------------------------
    def build_net(self, N: int, C: int, H: int) -> nn.Module:
        raise NotImplementedError

    # -- helpers ------------------------------------------------------------
    def head_out(self) -> int:
        return len(self.quantiles) if self.loss == "quantile" else 1

    def _scaled(self, features: np.ndarray) -> np.ndarray:
        f = features.copy()
        f[..., self._scale_idx] = f[..., self._scale_idx] / self._scale
        return f

    def _set_channels(self, channels: list[str]) -> None:
        self.channels = list(channels)
        self._scale_idx = [self.channels.index(c) for c in _SCALE_CHANNELS]

    def _model_features(self, t) -> np.ndarray:
        """Tensor features reordered/sliced to the channels this model was trained on.
        Channel evolution contract: new channels are APPENDED to tensor.CHANNELS, so an
        older checkpoint keeps serving (it selects its own channels by name) until the
        nightly retrain picks the new ones up. A trained channel missing from the tensor
        is a hard error — silently zero-filling would skew the inputs."""
        f = np.asarray(t.features, dtype=float)
        names = list(getattr(t, "channel_names", CHANNELS))
        if self.channels == names:
            return f
        missing = [c for c in self.channels if c not in names]
        if missing:
            raise RuntimeError(
                f"{self.name}: tensor lacks trained channels {missing} — retrain needed")
        return f[..., [names.index(c) for c in self.channels]]

    def _batch(self, feats_scaled, bases, L):
        xs = [windowing.gather_context(feats_scaled, int(b), L).transpose(1, 0, 2)
              for b in bases]  # each [L, N, C]
        return torch.tensor(np.stack(xs), dtype=torch.float32, device=self.device)

    # VRAM budget for keeping the precomputed window stack resident on the GPU. One park
    # is tiny vs the 16 GiB card; above this we fall back to pinned-host (still removes the
    # per-step numpy gather). 4 GiB ≈ 6000 windows × 480 × 96 × C × 4B worst case.
    _GPU_WINDOW_BUDGET = 4 * 1024 ** 3

    def _precompute_windows(self, feats_scaled, wait_raw, target_mask, bases, L, H):
        """Materialise the FULL training set once: context [W,L,N,C] + target/mask [W,N,H].

        The recurrent encoder already pays L sequential GPU kernels per step; re-gathering
        16 windows from CPU numpy AND transferring them every step on top of that forced a
        host↔device sync that left the GPU ~half-idle. Building the stack once (vectorised,
        same as the CatBoost feature prep) and keeping it GPU-resident makes each step a
        pure on-device gather → the GPU stays fed. Falls back to pinned host memory if the
        stack would blow the VRAM budget."""
        X = torch.tensor(
            np.stack([windowing.gather_context(feats_scaled, int(b), L).transpose(1, 0, 2)
                      for b in bases]), dtype=torch.float32)        # [W, L, N, C]
        Y = torch.tensor(
            np.nan_to_num(np.stack([wait_raw[:, b + 1:b + 1 + H] for b in bases]))
            / self._scale, dtype=torch.float32)                    # [W, N, H]
        M = torch.tensor(
            np.stack([target_mask[:, b + 1:b + 1 + H] for b in bases]),
            dtype=torch.float32)                                   # [W, N, H]
        if self.device == "cuda":
            if X.element_size() * X.nelement() <= self._GPU_WINDOW_BUDGET:
                return X.to(self.device), Y.to(self.device), M.to(self.device)
            # Too big to stay resident → pin so per-step batch transfers are fast/async.
            return X.pin_memory(), Y.pin_memory(), M.pin_memory()
        return X, Y, M

    def _loss(self, out, y_t, m_t):
        if self.loss == "tweedie":
            mu = torch.nn.functional.softplus(out[..., 0])
            return masked_tweedie(mu, y_t, m_t, self.tweedie_p)
        return masked_pinball(out, y_t, m_t, self.quantiles)

    # -- Model protocol -----------------------------------------------------
    def fit(self, t, train_bases: np.ndarray, L: int, H: int) -> None:
        torch.manual_seed(self.seed)
        wait_raw = np.asarray(t.wait_raw, dtype=float)
        observed = np.asarray(t.obs_mask, dtype=float)
        target_mask = observed * np.asarray(t.park_open, dtype=float)[None, :]
        self._set_channels(list(getattr(t, "channel_names", CHANNELS)))
        features = np.asarray(t.features, dtype=float)
        N, T, C = features.shape
        self._dims = (N, C, H)

        seen = wait_raw[observed > 0]
        self._scale = max(float(np.median(np.abs(seen))) if seen.size else 1.0, 1.0)
        feats_scaled = self._scaled(features)

        if train_bases.size == 0:
            logger.warning("%s: no training bases — left untrained", self.name)
            return
        rng = np.random.default_rng(self.seed)
        bases = train_bases
        if bases.size > self.max_train_windows:
            bases = rng.choice(bases, self.max_train_windows, replace=False)

        X, Y, M = self._precompute_windows(
            feats_scaled, wait_raw, target_mask, bases, L, H)
        W = X.shape[0]

        self.net = self.build_net(N, C, H).to(self.device)
        opt = torch.optim.Adam(self.net.parameters(), lr=self.lr)
        self.net.train()
        logger.info("%s: training on %s (%d windows, N=%d C=%d H=%d, data on %s)",
                    self.name, self.device, W, N, C, H, X.device.type)

        bs = min(self.batch_size, W)
        for step in range(self.max_steps):
            sel = torch.as_tensor(rng.integers(0, W, size=bs), device=X.device)
            x = X[sel].to(self.device, non_blocking=True)        # no-op if already resident
            y_t = Y[sel].to(self.device, non_blocking=True)
            m_t = M[sel].to(self.device, non_blocking=True)
            loss = self._loss(self.net(x), y_t, m_t)
            opt.zero_grad()
            loss.backward()
            opt.step()
            if (step + 1) % max(1, self.max_steps // 5) == 0:
                logger.info("  step %d/%d loss=%.4f", step + 1, self.max_steps,
                            loss.item())

    @torch.no_grad()
    def predict(self, t, eval_bases: np.ndarray, L: int, H: int) -> np.ndarray:
        if self.net is None:
            raise RuntimeError(f"{self.name} not fitted")
        self.net.eval()
        x = self._batch(self._scaled(self._model_features(t)), eval_bases, L)
        out = self.net(x)
        if self.loss == "tweedie":
            med = torch.nn.functional.softplus(out[..., 0])
        else:
            med = out[..., self._median_q]
        return med.cpu().numpy() * self._scale

    @torch.no_grad()
    def predict_quantiles(self, t, bases: np.ndarray, L: int, H: int) -> dict:
        """{quantile: [S, R, H]} for the served quantiles. For per-purpose serving:
        q0.5 = displayed wait, q0.8 = busy/crowd signal. Tweedie has no quantiles → it
        returns {0.5: mean} (the expected value). Quantiles are forced monotonic
        (running max over ascending alpha) — the pinball heads are trained
        independently and can cross, which would put the crowd signal below the
        displayed wait."""
        if self.net is None:
            raise RuntimeError(f"{self.name} not fitted")
        self.net.eval()
        x = self._batch(self._scaled(self._model_features(t)), bases, L)
        out = self.net(x)
        if self.loss == "tweedie":
            return {0.5: (torch.nn.functional.softplus(out[..., 0]).cpu().numpy()
                          * self._scale)}
        return metrics.enforce_quantile_monotonicity(
            {q: (out[..., i].cpu().numpy() * self._scale)
             for i, q in enumerate(self.quantiles)})

    # -- persistence --------------------------------------------------------
    def save(self, path: str, ride_ids=None) -> None:
        if self.net is None or self._dims is None:
            raise RuntimeError("nothing to save — fit first")
        torch.save({
            "arch": self.arch,
            "state_dict": self.net.state_dict(),
            "scale": self._scale,
            "dims": self._dims,
            "config": {
                "loss": self.loss, "quantiles": self.quantiles,
                "tweedie_p": self.tweedie_p, "hidden": self.hidden,
                "arch_kwargs": self.arch_kwargs,
            },
            "ride_ids": list(ride_ids) if ride_ids is not None else None,
            "channels": list(self.channels),
        }, path)

    def load(self, path: str) -> "TorchSeqModel":
        ckpt = torch.load(path, map_location=self.device, weights_only=False)
        # Restore the TRAINED config before rebuilding the net, so build_net() and the
        # head match the checkpoint even if this instance was constructed with different
        # defaults (otherwise load_state_dict would mismatch silently).
        cfg = ckpt.get("config", {})
        self.loss = cfg.get("loss", self.loss)
        self.quantiles = cfg.get("quantiles", self.quantiles)
        self.tweedie_p = cfg.get("tweedie_p", self.tweedie_p)
        self.hidden = cfg.get("hidden", self.hidden)
        self.arch_kwargs = cfg.get("arch_kwargs", self.arch_kwargs)
        self._median_q = self.quantiles.index(0.5) if 0.5 in self.quantiles else 0
        # Restore the TRAINED channel list (older checkpoints predate appended channels;
        # _model_features selects them by name from whatever tensor is passed in).
        self._set_channels(ckpt.get("channels") or list(CHANNELS))
        N, C, H = ckpt["dims"]
        self._scale = ckpt["scale"]
        self._dims = (N, C, H)
        self.net = self.build_net(N, C, H).to(self.device)
        self.net.load_state_dict(ckpt["state_dict"])
        self.net.eval()
        self.ride_ids = ckpt.get("ride_ids")
        return self
