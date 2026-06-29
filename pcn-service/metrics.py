"""Segmented MAE / bias for the intraday bake-off (pure numpy).

The whole discipline of the project: judge on the BUSY tail, never overall MAE (the
quiet majority hides the signal). So every model is scored by busy segment AND lead
bucket, masked to real-observed + park-open slots, against the naive baselines it must
beat (mirrors nf-service/backtest_intraday_nowcast.py's `seg`).
"""

from __future__ import annotations

import numpy as np

# Busy segments by realised wait (minutes), mirroring the existing intraday backtest.
BUSY_SEGMENTS = [
    ("quiet <30", lambda a: a < 30),
    ("mid 30-59", lambda a: (a >= 30) & (a < 60)),
    ("busy >=60", lambda a: a >= 60),
]


def mae_bias(
    pred: np.ndarray, actual: np.ndarray, keep: np.ndarray
) -> tuple[float, float, int]:
    """(MAE, bias=mean(pred-actual), n) over entries where `keep` and both finite."""
    k = keep & np.isfinite(pred) & np.isfinite(actual)
    n = int(k.sum())
    if n == 0:
        return float("nan"), float("nan"), 0
    d = pred[k] - actual[k]
    return float(np.abs(d).mean()), float(d.mean()), n


def lead_hours(H: int, slot_minutes: int) -> np.ndarray:
    """[H] lead time in hours for each horizon step (step 0 = +1 slot)."""
    return (np.arange(1, H + 1) * slot_minutes) / 60.0


def evaluate(
    preds: dict[str, np.ndarray],
    actual: np.ndarray,
    mask: np.ndarray,
    slot_minutes: int = 15,
) -> dict:
    """Score every named prediction array against `actual` on the eval `mask`,
    broken out by overall / busy segment / lead bucket.

    preds/actual/mask are all [S, R, H]. Returns
    {segment_label: {model_name: (mae, bias, n)}}.
    """
    H = actual.shape[-1]
    lh = lead_hours(H, slot_minutes)
    base = mask > 0

    # Lead-bucket masks broadcast over [S, R, H] via the H axis.
    lead_buckets = {
        "lead <=3h": lh <= 3,
        "lead 3-6h": (lh > 3) & (lh <= 6),
        "lead >6h": lh > 6,
    }

    out: dict[str, dict] = {}

    def _row(label: str, keep: np.ndarray):
        out[label] = {
            name: mae_bias(p, actual, keep) for name, p in preds.items()
        }

    _row("ALL", base)
    for label, fn in BUSY_SEGMENTS:
        _row(label, base & fn(actual))
    for label, hmask in lead_buckets.items():
        _row(label, base & np.broadcast_to(hmask, actual.shape))
    return out


def format_table(scores: dict) -> str:
    """Pretty-print the evaluate() result as MAE/bias per segment per model."""
    if not scores:
        return "(no scores)"
    models = list(next(iter(scores.values())).keys())
    width = max(len(m) for m in models)
    lines = []
    header = "  ".join(f"{m:>{width}}" for m in models)
    lines.append(f"{'segment':<12}  {header}")
    for seg, by_model in scores.items():
        cells = []
        for m in models:
            mae, bias, n = by_model[m]
            cells.append(f"{mae:5.1f}/{bias:+5.1f}".rjust(width))
        lines.append(f"{seg:<12}  " + "  ".join(cells))
    return "\n".join(lines)
