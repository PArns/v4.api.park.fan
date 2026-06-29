"""GP-STGNN — the intraday candidate ("Ansatz 3", design doc §11.6).

An AGCRN-style adaptive-graph spatio-temporal net with a probabilistic, peak-aware
head. Why this shape (vs the hand-rolled PCN cross-attention):

  * **Adaptive graph (AGCRN DAGG):** A = softmax(ReLU(E·Eᵀ)) is LEARNED from node
    embeddings — no predefined ride graph. This learned adjacency *is* the shared
    park-wide crowd state (DeepGLO's insight: standard models won't learn it
    otherwise; the graph forces a shared low-rank coupling across rides).
  * **Node-Adaptive Parameters (AGCRN NAPL):** per-ride conv weights generated from
    each ride's embedding (E·W_pool) = our `Sensitivität(ride)` — quiet walk-ons and
    busy headliners get different dynamics from one global model.
  * **Probabilistic, peak-aware head:** quantile (per-purpose serving: q0.5 display,
    q0.8 crowd) or Tweedie (the research-favoured right-skew likelihood) instead of
    median regression — the honest fix for the busy-tail under-prediction.

CUDA-first: training/inference run on the GPU when available (the RTX 5080 / Blackwell
host — the Dockerfile installs the cu128 torch wheel). The model is small (R ≤ ~100
nodes/park, AGCRN is cheap) so 16 GB is ample with per-park batching.

Conforms to backtest.Model — drops straight into the bake-off harness.
"""

from __future__ import annotations

import logging

import numpy as np
import torch
import torch.nn as nn

import windowing
from tensor import CHANNELS

logger = logging.getLogger("pcn.gpstgnn")

# Channels scaled by the robust wait-scale (the rest — masks, time sin/cos — are left
# as-is). Keeping it to the wait-valued channels mirrors nf-service's robust scaler.
_SCALE_CHANNELS = ("wait_ffill", "park_occ")


# ----------------------------------------------------------------- AGCRN core

class AVWGCN(nn.Module):
    """Adaptive Vertex-Wise Graph Convolution (AGCRN). Builds the adjacency from node
    embeddings each forward pass and applies per-node (NAPL) Chebyshev weights."""

    def __init__(self, dim_in: int, dim_out: int, cheb_k: int, embed_dim: int):
        super().__init__()
        self.cheb_k = cheb_k
        self.weights_pool = nn.Parameter(
            torch.empty(embed_dim, cheb_k, dim_in, dim_out)
        )
        self.bias_pool = nn.Parameter(torch.empty(embed_dim, dim_out))
        nn.init.xavier_normal_(self.weights_pool)
        nn.init.zeros_(self.bias_pool)

    def forward(self, x: torch.Tensor, node_emb: torch.Tensor) -> torch.Tensor:
        # x [B, N, dim_in], node_emb [N, embed]
        N = node_emb.shape[0]
        supports = torch.softmax(torch.relu(node_emb @ node_emb.T), dim=1)  # [N,N]
        # Chebyshev set [I, A, 2A·A−I, …]
        sset = [torch.eye(N, device=x.device), supports]
        for _ in range(2, self.cheb_k):
            sset.append(2 * supports @ sset[-1] - sset[-2])
        supports = torch.stack(sset, dim=0)                       # [K, N, N]
        x_g = torch.einsum("knm,bmi->bkni", supports, x)         # [B,K,N,dim_in]
        x_g = x_g.permute(0, 2, 1, 3)                            # [B,N,K,dim_in]
        weights = torch.einsum("nd,dkio->nkio", node_emb, self.weights_pool)
        bias = node_emb @ self.bias_pool                         # [N, dim_out]
        out = torch.einsum("bnki,nkio->bno", x_g, weights) + bias
        return out                                              # [B, N, dim_out]


class AGCRNCell(nn.Module):
    """GRU cell whose linear maps are AVWGCNs (graph-aware, node-adaptive)."""

    def __init__(self, dim_in: int, hidden: int, cheb_k: int, embed_dim: int):
        super().__init__()
        self.hidden = hidden
        self.gate = AVWGCN(dim_in + hidden, 2 * hidden, cheb_k, embed_dim)
        self.update = AVWGCN(dim_in + hidden, hidden, cheb_k, embed_dim)

    def forward(self, x, state, node_emb):
        # x [B,N,dim_in], state [B,N,hidden]
        combined = torch.cat([x, state], dim=-1)
        z_r = torch.sigmoid(self.gate(combined, node_emb))
        z, r = torch.split(z_r, self.hidden, dim=-1)
        candidate = torch.cat([x, r * state], dim=-1)
        hc = torch.tanh(self.update(candidate, node_emb))
        return z * state + (1 - z) * hc


class GPSTGNN(nn.Module):
    """Encoder (AGCRN over the context window) + a per-horizon probabilistic head."""

    def __init__(
        self, n_nodes: int, dim_in: int, hidden: int, embed_dim: int,
        horizon: int, head_out: int, cheb_k: int = 2,
    ):
        super().__init__()
        self.node_emb = nn.Parameter(torch.randn(n_nodes, embed_dim) * 0.05)
        self.cell = AGCRNCell(dim_in, hidden, cheb_k, embed_dim)
        self.hidden = hidden
        # End-conv maps the final hidden state to horizon × head_out per node.
        self.end = nn.Conv2d(1, horizon * head_out, kernel_size=(1, hidden), bias=True)
        self.horizon = horizon
        self.head_out = head_out

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x [B, L, N, dim_in]
        B, L, N, _ = x.shape
        state = torch.zeros(B, N, self.hidden, device=x.device)
        for t in range(L):
            state = self.cell(x[:, t], state, self.node_emb)
        h = state.unsqueeze(1)                       # [B,1,N,hidden]
        out = self.end(h)                            # [B, H*head_out, N, 1]
        out = out.squeeze(-1).permute(0, 2, 1)       # [B, N, H*head_out]
        return out.reshape(B, N, self.horizon, self.head_out)


# ----------------------------------------------------------------- losses

def _masked_pinball(pred, target, mask, quantiles):
    # pred [.., H, Q], target/mask [.., H]
    t = target.unsqueeze(-1)
    m = mask.unsqueeze(-1)
    q = torch.tensor(quantiles, device=pred.device).view(*([1] * (pred.dim() - 1)), -1)
    e = t - pred
    loss = torch.maximum(q * e, (q - 1) * e) * m
    denom = m.sum() * len(quantiles)
    return loss.sum() / denom.clamp_min(1.0)


def _masked_tweedie(mu, target, mask, p: float):
    # Tweedie negative log-likelihood (up to const), mu>0, 1<p<2. mu/target [..,H].
    mu = mu.clamp_min(1e-6)
    nll = -target * mu.pow(1 - p) / (1 - p) + mu.pow(2 - p) / (2 - p)
    return (nll * mask).sum() / mask.sum().clamp_min(1.0)


# ----------------------------------------------------------------- Model wrapper

class GPSTGNNModel:
    """backtest.Model adapter: builds windows from the cross-ride tensor, trains the
    AGCRN net on the GPU, and returns the median forecast for the eval bases."""

    def __init__(
        self, loss: str = "quantile", quantiles=(0.5, 0.8, 0.9), tweedie_p: float = 1.5,
        hidden: int = 64, embed_dim: int = 10, max_steps: int = 500,
        batch_size: int = 16, lr: float = 1e-3, max_train_windows: int = 6000,
        seed: int = 0,
    ):
        self.loss = loss
        self.quantiles = list(quantiles)
        self.tweedie_p = tweedie_p
        self.hidden = hidden
        self.embed_dim = embed_dim
        self.max_steps = max_steps
        self.batch_size = batch_size
        self.lr = lr
        self.max_train_windows = max_train_windows
        self.seed = seed
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.name = f"gpstgnn-{loss}"
        self._scale = 1.0
        self._scale_idx = [CHANNELS.index(c) for c in _SCALE_CHANNELS]
        self._median_q = self.quantiles.index(0.5) if 0.5 in self.quantiles else 0
        self.net: GPSTGNN | None = None

    # -- helpers ------------------------------------------------------------
    def _head_out(self) -> int:
        return len(self.quantiles) if self.loss == "quantile" else 1

    def _scaled_features(self, features: np.ndarray) -> np.ndarray:
        f = features.copy()
        f[..., self._scale_idx] = f[..., self._scale_idx] / self._scale
        return f

    def _context_batch(self, feats_scaled, bases, L):
        # [B, L, N, C] from per-sample [N,L,C] → transpose to [L,N,C]
        xs = [windowing.gather_context(feats_scaled, int(b), L).transpose(1, 0, 2)
              for b in bases]
        return torch.tensor(np.stack(xs), dtype=torch.float32, device=self.device)

    # -- Model protocol -----------------------------------------------------
    def fit(self, t, train_bases: np.ndarray, L: int, H: int) -> None:
        torch.manual_seed(self.seed)
        wait_raw = np.asarray(t.wait_raw, dtype=float)
        observed = np.asarray(t.obs_mask, dtype=float)
        target_mask = observed * np.asarray(t.park_open, dtype=float)[None, :]
        features = np.asarray(t.features, dtype=float)
        N, T, C = features.shape

        # Robust wait-scale from observed training waits (median-abs; ≈ RobustScaler).
        seen = wait_raw[observed > 0]
        self._scale = float(np.median(np.abs(seen))) if seen.size else 1.0
        self._scale = max(self._scale, 1.0)
        feats_scaled = self._scaled_features(features)

        if train_bases.size == 0:
            logger.warning("%s: no training bases — model left untrained", self.name)
            return
        rng = np.random.default_rng(self.seed)
        bases = train_bases
        if bases.size > self.max_train_windows:
            bases = rng.choice(bases, self.max_train_windows, replace=False)

        self.net = GPSTGNN(
            n_nodes=N, dim_in=C, hidden=self.hidden, embed_dim=self.embed_dim,
            horizon=H, head_out=self._head_out(), cheb_k=2,
        ).to(self.device)
        opt = torch.optim.Adam(self.net.parameters(), lr=self.lr)
        self.net.train()
        logger.info("%s: training on %s (%d windows, N=%d, C=%d, H=%d)",
                    self.name, self.device, bases.size, N, C, H)

        for step in range(self.max_steps):
            idx = rng.choice(bases, min(self.batch_size, bases.size), replace=False)
            x = self._context_batch(feats_scaled, idx, L)            # [B,L,N,C]
            y = np.stack([wait_raw[:, b + 1:b + 1 + H] for b in idx])   # [B,N,H]
            m = np.stack([target_mask[:, b + 1:b + 1 + H] for b in idx])
            y_t = torch.tensor(np.nan_to_num(y) / self._scale,
                               dtype=torch.float32, device=self.device)
            m_t = torch.tensor(m, dtype=torch.float32, device=self.device)
            out = self.net(x)                                       # [B,N,H,head_out]
            if self.loss == "tweedie":
                mu = torch.nn.functional.softplus(out[..., 0])
                loss = _masked_tweedie(mu, y_t, m_t, self.tweedie_p)
            else:
                loss = _masked_pinball(out, y_t, m_t, self.quantiles)
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
        feats_scaled = self._scaled_features(np.asarray(t.features, dtype=float))
        x = self._context_batch(feats_scaled, eval_bases, L)
        out = self.net(x)                                          # [S,N,H,head_out]
        if self.loss == "tweedie":
            med = torch.nn.functional.softplus(out[..., 0])
        else:
            med = out[..., self._median_q]
        return (med.cpu().numpy() * self._scale)                  # [S,N,H], unscaled
