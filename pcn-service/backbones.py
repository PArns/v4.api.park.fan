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


# Registry for the bake-off runner: name -> factory(**model_kwargs).
def build_registry(**model_kwargs) -> dict:
    """The neural candidates to bake off. Naive baselines (persistence,
    yesterday-same-slot) are added by the harness itself."""
    import gp_stgnn

    return {
        "gpstgnn": lambda: gp_stgnn.GPSTGNNModel(**model_kwargs),
        "localgru": lambda: LocalGRUModel(**model_kwargs),
    }
