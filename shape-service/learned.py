"""Learned shape correction (Phase-2 upgrade) — validated +2.8% busy over the additive+smooth
NP model (design §8d), at scale (≥18 parks).

The NP additive+smooth form (profiles.render_additive / serve_curve) is a strong, robust,
dependency-light baseline. A small global net can shave a further ~2.8% off the busy-tail by
learning a CORRECTION on top of it that pools the calendar-factor effects across all rides:

    final_norm = clip(anchor + 0.5·tanh(MLP(ride_emb, slot_emb, [crowd, school, pub, wend,
                                                                  peak])), 0, 2)

Key lessons (the first attempt failed by ignoring all three):
  * Train GLOBALLY over MANY parks (a 7-park model is data-starved); the ride embedding +
    shared day-feature effects need scale.
  * EARLY-STOP on a held-out validation split — fixed-epoch training overfits the correction.
  * Anchor on the NP form (feed it in, predict a residual) — the net cannot reproduce per-ride
    forms from scratch, but it can correct a strong anchor.
  * Keep it SIMPLE: adding the NP component curves (base/crowd/daytype) as extra features
    OVERFITS (val improves, eval worsens) → richer features need more data.

This module is the model + fit/predict on prepared arrays; the shadow wiring (build training
rows from the profiles + panel, persist one global checkpoint, render at serve) lands with the
shape shadow service. torch is an optional dependency — imported lazily.
"""

from __future__ import annotations

import numpy as np

# Feature layout for the prepared arrays (one row per (ride, day, operating slot)):
#   ride_idx, slot, crowd, f_school, f_pub, f_wend, f_peak, anchor, y_norm, level
N_DAYFEATS = 5  # crowd + school/pub/wend/peak


def _make_net(n_rides: int, slot_count: int, emb: int = 16, hidden: int = 128):
    import torch.nn as nn

    net = nn.Sequential(
        nn.Linear(2 * emb + N_DAYFEATS, hidden),
        nn.ReLU(),
        nn.Dropout(0.1),
        nn.Linear(hidden, hidden),
        nn.ReLU(),
        nn.Linear(hidden, 1),
    )
    nn.init.zeros_(net[-1].weight)
    nn.init.zeros_(net[-1].bias)  # start at the anchor (delta = 0)
    return net


class LearnedShape:
    """Global anchor-and-correct shape model. `fit` early-stops on a validation array;
    `predict` returns the corrected normalised form per row."""

    def __init__(
        self,
        n_rides: int,
        slot_count: int = 96,
        emb: int = 16,
        lr: float = 2e-3,
        weight_decay: float = 1e-5,
        patience: int = 15,
        max_epochs: int = 300,
        batch: int = 16384,
        seed: int = 0,
    ):
        import torch
        import torch.nn as nn

        self.torch = torch
        torch.manual_seed(seed)
        self.dev = "cuda" if torch.cuda.is_available() else "cpu"
        self.slot_count = slot_count
        self.r_emb = nn.Embedding(n_rides, emb).to(self.dev)
        self.s_emb = nn.Embedding(slot_count, emb).to(self.dev)
        self.net = _make_net(n_rides, slot_count, emb).to(self.dev)
        self.lr, self.wd, self.patience = lr, weight_decay, patience
        self.max_epochs, self.batch = max_epochs, batch

    def _forward(self, ri, sl, fx, anc):
        x = self.torch.cat([self.r_emb(ri), self.s_emb(sl), fx], -1)
        return (anc + 0.5 * self.torch.tanh(self.net(x).squeeze(-1))).clamp(0, 2)

    def _split(self, A):
        t = self.torch
        return (
            t.tensor(A[:, 0], dtype=t.long, device=self.dev),
            t.tensor(A[:, 1], dtype=t.long, device=self.dev),
            t.tensor(A[:, 2:7], dtype=t.float32, device=self.dev),  # 5 day-features
            t.tensor(A[:, 7], dtype=t.float32, device=self.dev),  # anchor
            t.tensor(A[:, 8], dtype=t.float32, device=self.dev),  # y_norm
            t.tensor(A[:, 9], dtype=t.float32, device=self.dev),
        )  # level (loss weight)

    def fit(self, train: np.ndarray, val: np.ndarray) -> dict:
        t = self.torch
        ri, sl, fx, anc, yn, lv = self._split(train)
        vri, vsl, vfx, vanc, vyn, vlv = self._split(val)
        params = (
            list(self.net.parameters())
            + list(self.r_emb.parameters())
            + list(self.s_emb.parameters())
        )
        opt = t.optim.Adam(params, lr=self.lr, weight_decay=self.wd)
        n, best, bad, best_sd = len(train), 1e18, 0, None
        for ep in range(self.max_epochs):
            self.net.train()
            perm = t.randperm(n, device=self.dev)
            for i in range(0, n, self.batch):
                idx = perm[i : i + self.batch]
                loss = (
                    t.abs(self._forward(ri[idx], sl[idx], fx[idx], anc[idx]) - yn[idx]) * lv[idx]
                ).mean()
                opt.zero_grad()
                loss.backward()
                opt.step()
            self.net.eval()
            with t.no_grad():
                vl = float((t.abs(self._forward(vri, vsl, vfx, vanc) - vyn) * vlv).mean())
            if vl < best - 1e-4:
                best, bad = vl, 0
                best_sd = {
                    g: {k: v.detach().clone() for k, v in sd.items()}
                    for g, sd in self._state().items()
                }
            else:
                bad += 1
                if bad >= self.patience:
                    break
        if best_sd is not None:
            self._load(best_sd)
        return {"best_val": best, "epochs": ep + 1}

    def predict(self, A: np.ndarray) -> np.ndarray:
        t = self.torch
        ri, sl, fx, anc, _, _ = self._split(A)
        self.net.eval()
        with t.no_grad():
            return self._forward(ri, sl, fx, anc).cpu().numpy()

    def _state(self):
        return {
            "net": self.net.state_dict(),
            "r": self.r_emb.state_dict(),
            "s": self.s_emb.state_dict(),
        }

    def _load(self, sd):
        # sd holds cloned tensors of the three sub-state-dicts
        self.net.load_state_dict(sd["net"])
        self.r_emb.load_state_dict(sd["r"])
        self.s_emb.load_state_dict(sd["s"])
