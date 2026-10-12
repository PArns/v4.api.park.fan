"""Planner-optimiser simulation for D4 (frontend ``lib/planner/optimize.ts``).

What is ported, and what is simplified (documented in docs/ml/ml-bench.md):

* **Objective — ported:** ``cost = Σ wait + IDLE_WEIGHT · Σ idle`` with
  IDLE_WEIGHT = 0.5. Idle of a stop = its start minus the earliest start the
  previous stop allowed (``idleFor``); the walk in from the gates is not charged.
* **Time model — ported:** starts snap up to 15 minutes (``SNAP_MIN_FINE``);
  a stop occupies ``[start, start + wait]``; the next earliest start is
  ``snapUp(freeAt + transfer, 15)``; delay options 0, 15 … 120 minutes
  (``MAX_DELAY_MIN``); a stop fits iff it STARTS before closing; the first stop
  can start at ``snapUp(open + 15)`` (``GATE_TO_FIRST_RIDE_MIN``).
* **Transfer — ported (ceiling):** ``EXIT_MIN (3) + rideMin (5) + walkCeil``,
  ``walkCeil = ceil(metres · 1.6 / 67)``; without coordinates 3 min in the same
  land and 8 across lands (``leg.ts``).
* **Search — replaced by the exact optimum.** The frontend runs a beam search
  (width 192, ≤ 4 Pareto labels per key) plus local search. On the fixed set of
  ``optimiser_rides`` (4) rides used here the exact dynamic programme over
  (visited set, last ride, earliest-start slot) is cheap, and it is what the
  beam converges to on so few rides — so this measures the forecast, not the
  search heuristic. No new heuristic is introduced.
* **Wait lookup:** the value of the 15-minute slot containing the start (the
  owner's 15-min requirement; the frontend reads the hourly point today).
* **Not simulated:** overflow / headliner dropping (all rides fit or the
  park-day is skipped), fixed blocks, opensAt floors, early entry, live
  corrections.

Regret = true cost of the plan built from the forecast (same order, same
planned starts, a stop starts later only if the truth makes the guest late)
minus the true cost of the plan built from truth.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

SNAP = 15
EXIT_MIN = 3
RIDE_MIN = 5
WALK_DETOUR = 1.6
WALK_M_PER_MIN = 67.0
SAME_LAND_MIN = 3
CROSS_LAND_MIN = 8
GATE_TO_FIRST_RIDE_MIN = 15


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def transfer_ceiling(a: dict, b: dict) -> int:
    """leg.ts ceiling: EXIT_MIN + rideMin + walkCeil."""
    if all(v is not None and not (isinstance(v, float) and math.isnan(v))
           for v in (a.get("lat"), a.get("lng"), b.get("lat"), b.get("lng"))):
        m = haversine_m(a["lat"], a["lng"], b["lat"], b["lng"])
        walk = math.ceil(m * WALK_DETOUR / WALK_M_PER_MIN) if m > 0 else 0
    else:
        walk = SAME_LAND_MIN if a.get("land") and a.get("land") == b.get("land") else CROSS_LAND_MIN
    return EXIT_MIN + RIDE_MIN + walk


@dataclass
class Plan:
    order: list[int]
    starts: list[int]       # slot index (minutes = SNAP * index after opening)
    cost: float


def optimise(waits: np.ndarray, transfer: np.ndarray, n_slots: int,
             idle_weight: float = 0.5, max_delay_min: int = 120) -> Plan | None:
    """Exact minimum of Σwait + w·Σidle over orders and delay options.

    ``waits[r, k]`` = wait (minutes) when starting ride r at slot k after opening;
    ``transfer[i, j]`` = transfer minutes from ride i to j; slot 0 is the opening
    slot, a stop must start at a slot < ``n_slots``. Returns None if the rides
    cannot all start before closing.

    State: (visited set, last ride, freeAt minute of the last ride). The next
    stop's earliest start is snapUp(freeAt + transfer(last, next)).
    """
    R = waits.shape[0]
    K = n_slots
    first = math.ceil(GATE_TO_FIRST_RIDE_MIN / SNAP)
    if first >= K:
        return None
    finite_w = waits[np.isfinite(waits)]
    M = SNAP * K + int(math.ceil(finite_w.max() if finite_w.size else 0)) + 1
    delays = np.arange(0, max_delay_min // SNAP + 1)
    full = (1 << R) - 1
    fs = np.arange(M + 1)
    cost: dict[tuple[int, int], np.ndarray] = {}
    back: dict[tuple[int, int], tuple[np.ndarray, np.ndarray, np.ndarray]] = {}

    def relax(mask: int, last: int, prev_f: np.ndarray, prev_c: np.ndarray) -> None:
        for j in range(R):
            if mask >> j & 1:
                continue
            if last < 0:
                earliest = np.full(prev_f.shape, first)
            else:
                earliest = np.ceil((prev_f + transfer[last, j]) / SNAP).astype(int)
            s = (earliest[:, None] + delays[None, :]).ravel()
            f0 = np.repeat(prev_f, len(delays))
            c0 = np.repeat(prev_c, len(delays)) + idle_weight * SNAP * np.tile(delays, len(prev_f))
            ok = s < K
            s, f0, c0 = s[ok], f0[ok], c0[ok]
            w = waits[j, s]
            good = np.isfinite(w)
            s, f0, c0, w = s[good], f0[good], c0[good], w[good]
            if s.size == 0:
                continue
            c = c0 + w
            nf = np.ceil(SNAP * s + w).astype(int)
            nm = mask | (1 << j)
            best = cost.setdefault((nm, j), np.full(M + 1, np.inf))
            bp = back.setdefault((nm, j), (np.full(M + 1, -2), np.full(M + 1, -1), np.full(M + 1, -1)))
            o = np.lexsort((s, c))
            s, f0, c, nf = s[o], f0[o], c[o], nf[o]
            uniq, idx = np.unique(nf, return_index=True)
            upd = c[idx] < best[uniq]
            tgt, src = uniq[upd], idx[upd]
            best[tgt] = c[src]
            bp[0][tgt] = last
            bp[1][tgt] = f0[src]
            bp[2][tgt] = s[src]

    relax(0, -1, np.array([0]), np.array([0.0]))
    for mask in range(1, full + 1):
        for last in range(R):
            arr = cost.get((mask, last))
            if arr is None:
                continue
            live = np.isfinite(arr)
            if live.any():
                relax(mask, last, fs[live], arr[live])
    fin = [(float(cost[(full, j)].min()), j) for j in range(R) if (full, j) in cost]
    fin = [f for f in fin if np.isfinite(f[0])]
    if not fin:
        return None
    total, j = min(fin)
    f = int(np.argmin(cost[(full, j)]))
    order, starts, mask = [], [], full
    while j >= 0:
        prev, pf, ss = (int(v[f]) for v in back[(mask, j)])
        order.append(j)
        starts.append(ss)
        mask &= ~(1 << j)
        j, f = prev, pf
    return Plan(order[::-1], starts[::-1], float(total))


def execute(plan: Plan, true_waits: np.ndarray, transfer: np.ndarray, n_slots: int,
             idle_weight: float = 0.5) -> float:
    """True cost of following ``plan``: keep its order and planned starts; a stop
    starts later only when the truth makes the guest late for it. NaN when a
    stop has no true value, inf when the plan runs past closing."""
    base = math.ceil(GATE_TO_FIRST_RIDE_MIN / SNAP)
    total = 0.0
    prev = -1
    free = 0.0
    for j, planned in zip(plan.order, plan.starts):
        if prev >= 0:
            base = math.ceil(math.ceil(free + transfer[prev, j]) / SNAP)
        start = max(planned, base)
        if start >= n_slots:
            return math.inf
        w = true_waits[j, start]
        if not np.isfinite(w):
            return math.nan
        total += w + idle_weight * SNAP * (start - base)
        free = math.ceil(SNAP * start + w)
        prev = j
    return total
