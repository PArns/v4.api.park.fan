"""GP-STGNN — the intraday candidate ("Ansatz 3", design doc §11.6).

An AGCRN-style adaptive-graph net. Why this shape:
  * **Adaptive graph (AGCRN DAGG):** A = softmax(ReLU(E·Eᵀ)) is LEARNED from node
    embeddings — no predefined ride graph. This learned adjacency *is* the shared
    park-wide crowd state (DeepGLO's insight: standard models won't learn it otherwise).
  * **Node-Adaptive Parameters (AGCRN NAPL):** per-ride conv weights from each ride's
    embedding (E·W_pool) = our `Sensitivität(ride)`.
  * **Probabilistic, peak-aware head** (quantile / Tweedie via the shared base) — the
    honest fix for busy-tail under-prediction instead of a loss thumb.

The training/scaling/serving plumbing lives in torch_model.TorchSeqModel; this file is
just the architecture + a thin model class. CUDA-first (the base auto-selects the GPU).
"""

from __future__ import annotations

import torch
import torch.nn as nn

from torch_model import TorchSeqModel


def build_supports(node_emb: torch.Tensor, cheb_k: int) -> torch.Tensor:
    """Chebyshev support stack [K,N,N] from the adaptive adjacency A = softmax(ReLU(E·Eᵀ)).
    Depends only on node_emb, and the gate/update convs share it, so the recurrent encoder
    computes it ONCE per forward rather than at every one of the L timesteps."""
    N = node_emb.shape[0]
    a = torch.softmax(torch.relu(node_emb @ node_emb.T), dim=1)   # [N,N]
    sset = [torch.eye(N, device=node_emb.device), a]
    for _ in range(2, cheb_k):
        sset.append(2 * a @ sset[-1] - sset[-2])
    return torch.stack(sset, dim=0)                               # [K,N,N]


class AVWGCN(nn.Module):
    """Adaptive Vertex-Wise Graph Convolution (AGCRN): adjacency from node embeddings;
    per-node (NAPL) Chebyshev weights. The adjacency (`supports`) and the node-adaptive
    weights/bias depend only on node_emb — constant across a forward — so they are
    precomputed once (see `node_params` / `build_supports`) and passed into `forward`,
    instead of being rebuilt at every recurrent timestep."""

    def __init__(self, dim_in: int, dim_out: int, cheb_k: int, embed_dim: int):
        super().__init__()
        self.cheb_k = cheb_k
        self.weights_pool = nn.Parameter(torch.empty(embed_dim, cheb_k, dim_in, dim_out))
        self.bias_pool = nn.Parameter(torch.empty(embed_dim, dim_out))
        nn.init.xavier_normal_(self.weights_pool)
        nn.init.zeros_(self.bias_pool)

    def node_params(self, node_emb: torch.Tensor):
        """Per-node Chebyshev weights [N,K,Ci,Co] + bias [N,Co] from the embeddings."""
        weights = torch.einsum("nd,dkio->nkio", node_emb, self.weights_pool)
        bias = node_emb @ self.bias_pool
        return weights, bias

    def forward(self, x, supports, weights, bias):
        # supports [K,N,N], weights [N,K,Ci,Co], bias [N,Co] — all precomputed per forward.
        x_g = torch.einsum("knm,bmi->bkni", supports, x).permute(0, 2, 1, 3)
        return torch.einsum("bnki,nkio->bno", x_g, weights) + bias  # [B,N,dim_out]


class AGCRNCell(nn.Module):
    """GRU cell whose linear maps are AVWGCNs (graph-aware, node-adaptive)."""

    def __init__(self, dim_in: int, hidden: int, cheb_k: int, embed_dim: int):
        super().__init__()
        self.hidden = hidden
        self.gate = AVWGCN(dim_in + hidden, 2 * hidden, cheb_k, embed_dim)
        self.update = AVWGCN(dim_in + hidden, hidden, cheb_k, embed_dim)

    def node_params(self, node_emb):
        """Precompute (per-forward) the gate + update node-adaptive weights/bias."""
        return self.gate.node_params(node_emb), self.update.node_params(node_emb)

    def forward(self, x, state, supports, gate_p, update_p):
        combined = torch.cat([x, state], dim=-1)
        gw, gb = gate_p
        z, r = torch.split(
            torch.sigmoid(self.gate(combined, supports, gw, gb)), self.hidden, -1)
        uw, ub = update_p
        hc = torch.tanh(self.update(torch.cat([x, r * state], dim=-1), supports, uw, ub))
        return z * state + (1 - z) * hc


class GPSTGNN(nn.Module):
    """AGCRN encoder over the context window + a per-horizon probabilistic head."""

    def __init__(self, n_nodes, dim_in, hidden, embed_dim, horizon, head_out, cheb_k=2):
        super().__init__()
        self.node_emb = nn.Parameter(torch.randn(n_nodes, embed_dim) * 0.05)
        self.cell = AGCRNCell(dim_in, hidden, cheb_k, embed_dim)
        self.hidden = hidden
        self.cheb_k = cheb_k
        self.end = nn.Conv2d(1, horizon * head_out, kernel_size=(1, hidden), bias=True)
        self.horizon = horizon
        self.head_out = head_out

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        B, L, N, _ = x.shape
        # Adjacency + node-adaptive conv weights depend only on node_emb → compute ONCE
        # per forward, not inside every one of the L recurrent steps (the hot loop).
        supports = build_supports(self.node_emb, self.cheb_k)
        gate_p, update_p = self.cell.node_params(self.node_emb)
        state = torch.zeros(B, N, self.hidden, device=x.device)
        for tstep in range(L):
            state = self.cell(x[:, tstep], state, supports, gate_p, update_p)
        out = self.end(state.unsqueeze(1))            # [B, H*head_out, N, 1]
        out = out.squeeze(-1).permute(0, 2, 1)        # [B, N, H*head_out]
        return out.reshape(B, N, self.horizon, self.head_out)


class GPSTGNNModel(TorchSeqModel):
    arch = "gpstgnn"

    def __init__(self, embed_dim: int = 10, cheb_k: int = 2, **kw):
        super().__init__(embed_dim=embed_dim, cheb_k=cheb_k, **kw)

    def build_net(self, N: int, C: int, H: int) -> nn.Module:
        return GPSTGNN(
            n_nodes=N, dim_in=C, hidden=self.hidden,
            embed_dim=self.arch_kwargs["embed_dim"], horizon=H,
            head_out=self.head_out(), cheb_k=self.arch_kwargs["cheb_k"],
        )
