"""Bake-off candidates other than the GP-STGNN — chiefly the ABLATION that decides
whether the cross-ride graph earns its keep.

`LocalGRUModel` is GP-STGNN with the graph removed: each ride is encoded by the SAME
GRU independently (no AVWGCN, no learned adjacency, no cross-ride coupling). It is the
honest control — if GP-STGNN does not beat LocalGRU on the busy tail, the learned
park-crowd graph is not buying anything and we should not pay for it. Same training
harness (TorchSeqModel), same loss, same scaling → a clean A/B on architecture alone.

Future STG4Traffic backbones (Graph WaveNet, MTGNN, DeepGLO) drop in the same way:
subclass TorchSeqModel, implement build_net returning [B, N, H, head_out].
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F

from torch_model import TorchSeqModel


class LocalGRU(nn.Module):
    """Per-ride GRU with shared weights — no cross-ride structure (the ablation)."""

    def __init__(self, dim_in: int, hidden: int, horizon: int, head_out: int):
        super().__init__()
        self.gru = nn.GRU(dim_in, hidden, batch_first=True)
        self.head = nn.Linear(hidden, horizon * head_out)
        self.horizon = horizon
        self.head_out = head_out

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        B, L, N, C = x.shape
        # Fold nodes into the batch so each ride is encoded independently.
        xn = x.permute(0, 2, 1, 3).reshape(B * N, L, C)
        _, h = self.gru(xn)                          # h [1, B*N, hidden]
        out = self.head(h[-1])                       # [B*N, H*head_out]
        return out.reshape(B, N, self.horizon, self.head_out)


class LocalGRUModel(TorchSeqModel):
    arch = "localgru"

    def build_net(self, N: int, C: int, H: int) -> nn.Module:
        return LocalGRU(dim_in=C, hidden=self.hidden, horizon=H, head_out=self.head_out())


class GraphWaveNet(nn.Module):
    """Graph WaveNet (compact): a SELF-ADAPTIVE adjacency A = softmax(ReLU(E1·E2ᵀ))
    learned from two node-embedding dictionaries (no predefined graph), stacked
    gated dilated causal temporal convs, and graph diffusion (A and Aᵀ) per layer.

    A second learned-graph candidate alongside GP-STGNN — different inductive bias
    (dilated TCN vs recurrent), same shared-crowd-state idea. Causal left-padding keeps
    the time length constant so it works for any context length L (tiny in tests, 480 in
    prod). Returns [B, N, H, head_out]."""

    def __init__(self, n_nodes, dim_in, hidden, horizon, head_out,
                 embed_dim=10, layers=2, kernel=2):
        super().__init__()
        self.E1 = nn.Parameter(torch.randn(n_nodes, embed_dim) * 0.05)
        self.E2 = nn.Parameter(torch.randn(n_nodes, embed_dim) * 0.05)
        self.start = nn.Conv2d(dim_in, hidden, (1, 1))
        self.kernel = kernel
        self.dilations = [2 ** i for i in range(layers)]
        self.filter_convs = nn.ModuleList(
            nn.Conv2d(hidden, hidden, (1, kernel), dilation=(1, d)) for d in self.dilations)
        self.gate_convs = nn.ModuleList(
            nn.Conv2d(hidden, hidden, (1, kernel), dilation=(1, d)) for d in self.dilations)
        # graph conv mixes [x, A·x, Aᵀ·x] back to hidden.
        self.gconvs = nn.ModuleList(
            nn.Conv2d(hidden * 3, hidden, (1, 1)) for _ in self.dilations)
        self.end1 = nn.Conv2d(hidden, hidden, (1, 1))
        self.end2 = nn.Conv2d(hidden, horizon * head_out, (1, 1))
        self.horizon = horizon
        self.head_out = head_out

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        B, L, N, _ = x.shape
        x = x.permute(0, 3, 2, 1)                      # [B, C, N, L]
        x = self.start(x)                              # [B, hidden, N, L]
        A = torch.softmax(torch.relu(self.E1 @ self.E2.T), dim=1)  # [N, N]
        skip = 0.0
        for d, fc, gc, gconv in zip(self.dilations, self.filter_convs,
                                    self.gate_convs, self.gconvs):
            res = x
            pad = d * (self.kernel - 1)                # causal left-pad → length preserved
            xp = F.pad(x, (pad, 0))
            x = torch.tanh(fc(xp)) * torch.sigmoid(gc(xp))         # gated TCN [B,hid,N,L]
            ax = torch.einsum("vw,bcwl->bcvl", A, x)              # diffusion forward
            axt = torch.einsum("vw,bcwl->bcvl", A.T, x)           # diffusion reverse
            x = gconv(torch.cat([x, ax, axt], dim=1))
            x = x + res                                          # residual (same length)
            skip = skip + x
        x = torch.relu(skip)
        x = torch.relu(self.end1(x))
        x = self.end2(x)[..., -1]                      # last step [B, H*head_out, N]
        return x.permute(0, 2, 1).reshape(B, N, self.horizon, self.head_out)


class GraphWaveNetModel(TorchSeqModel):
    arch = "graphwavenet"

    def __init__(self, embed_dim: int = 10, layers: int = 2, **kw):
        super().__init__(embed_dim=embed_dim, layers=layers, **kw)

    def build_net(self, N: int, C: int, H: int) -> nn.Module:
        return GraphWaveNet(
            n_nodes=N, dim_in=C, hidden=self.hidden, horizon=H,
            head_out=self.head_out(), embed_dim=self.arch_kwargs["embed_dim"],
            layers=self.arch_kwargs["layers"],
        )


# Registry for the bake-off runner: name -> factory(**model_kwargs).
def build_registry(**model_kwargs) -> dict:
    """The neural candidates to bake off. Naive baselines (persistence,
    yesterday-same-slot) are added by the harness itself."""
    import gp_stgnn

    return {
        "gpstgnn": lambda: gp_stgnn.GPSTGNNModel(**model_kwargs),
        "graphwavenet": lambda: GraphWaveNetModel(**model_kwargs),
        "localgru": lambda: LocalGRUModel(**model_kwargs),
    }
