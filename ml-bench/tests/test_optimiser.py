"""D4: the dynamic programme is the exact optimum of the frontend objective."""

import itertools
import math

import numpy as np

from mlbench.decisions import Plan, execute, optimise


def brute(W, T, K):
    best = math.inf
    for order in itertools.permutations(range(W.shape[0])):
        for ds in itertools.product(range(9), repeat=W.shape[0]):
            starts, base, free, prev, ok = [], 1, 0, -1, True
            for j, d in zip(order, ds):
                if prev >= 0:
                    base = math.ceil(math.ceil(free + T[prev, j]) / 15)
                s = base + d
                if s >= K:
                    ok = False
                    break
                starts.append(s)
                free = math.ceil(15 * s + W[j, s])
                prev = j
            if ok:
                best = min(best, execute(Plan(list(order), starts, 0), W, T, K))
    return best


def test_dp_matches_brute_force():
    rng = np.random.default_rng(3)
    for _ in range(4):
        K = 30
        W = rng.integers(5, 90, size=(3, K)).astype(float)
        T = rng.integers(8, 20, size=(3, 3)).astype(float)
        plan = optimise(W, T, K)
        assert plan is not None
        assert math.isclose(plan.cost, brute(W, T, K))
        assert math.isclose(execute(plan, W, T, K), plan.cost)


def test_waiting_for_a_dip_is_used_when_it_pays():
    K = 20
    W = np.full((1, K), 60.0)
    W[0, 5] = 5.0                               # waiting 4 slots (60 min idle -> 30 cost) saves 55
    plan = optimise(W, np.zeros((1, 1)), K)
    assert plan.starts == [5]
    assert math.isclose(plan.cost, 5 + 0.5 * 60)


def test_regret_of_a_wrong_forecast_is_positive():
    K = 20
    truth = np.full((2, K), 30.0)
    truth[0, 1] = 5.0
    fc = np.full((2, K), 30.0)
    fc[1, 1] = 5.0                              # forecast puts the dip on the other ride
    T = np.full((2, 2), 10.0)
    best = optimise(truth, T, K)
    plan = optimise(fc, T, K)
    assert execute(plan, truth, T, K) > best.cost
