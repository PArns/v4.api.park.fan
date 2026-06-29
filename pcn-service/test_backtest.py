"""Unit tests for the bake-off harness (windowing + metrics + driver), no torch/DB.

    cd pcn-service && python3 -m pytest test_backtest.py -q
"""

import numpy as np
import pandas as pd
import pytest

import backtest
import metrics
import tensor
import windowing


# ---------------------------------------------------------------- windowing

def test_valid_bases():
    np.testing.assert_array_equal(windowing.valid_bases(10, 3, 2), [2, 3, 4, 5, 6, 7])
    assert windowing.valid_bases(4, 3, 2).size == 0  # T < L+H


def test_gather_context_and_targets():
    R, T, C = 2, 10, 3
    feats = np.arange(R * T * C).reshape(R, T, C).astype(float)
    ctx = windowing.gather_context(feats, b=4, L=3)        # slots 2,3,4
    assert ctx.shape == (R, 3, C)
    np.testing.assert_array_equal(ctx[:, -1, :], feats[:, 4, :])

    wait = np.arange(R * T).reshape(R, T).astype(float)
    mask = np.ones((R, T))
    a, m = windowing.gather_targets(wait, mask, b=4, H=2)   # slots 5,6
    np.testing.assert_array_equal(a, wait[:, 5:7])
    assert m.shape == (R, 2)


def test_last_observed_picks_most_recent():
    wait = np.array([[10.0, 20.0, 30.0, 40.0]])
    obs = np.array([[1.0, 0.0, 1.0, 0.0]])  # last obs at idx 2 → 30
    np.testing.assert_array_equal(windowing.last_observed(wait, obs, b=3), [30.0])
    # ride never observed → NaN
    assert np.isnan(windowing.last_observed(wait, np.zeros((1, 4)), b=3)[0])


def test_yesterday_picks_one_day_back():
    # spd=4: target slot t uses t-4. wait increases by 1 per slot.
    wait = np.arange(12).reshape(1, 12).astype(float)
    obs = np.ones((1, 12))
    y = windowing.yesterday(wait, obs, b=5, H=2, spd=4)  # targets 6,7 → src 2,3
    np.testing.assert_array_equal(y, [[2.0, 3.0]])
    # out-of-range source → NaN
    y0 = windowing.yesterday(wait, obs, b=2, H=2, spd=4)  # targets 3,4 → src -1,0
    assert np.isnan(y0[0, 0]) and y0[0, 1] == 0.0


# ---------------------------------------------------------------- metrics

def test_mae_bias_basic():
    pred = np.array([[[12.0, 22.0]]])
    actual = np.array([[[10.0, 20.0]]])
    keep = np.ones((1, 1, 2), dtype=bool)
    mae, bias, n = metrics.mae_bias(pred, actual, keep)
    assert (mae, bias, n) == (2.0, 2.0, 2)


def test_evaluate_segments():
    actual = np.array([[[10.0, 70.0]]])          # one quiet, one busy
    pred = actual + 5.0
    mask = np.ones_like(actual)
    sc = metrics.evaluate({"m": pred}, actual, mask, slot_minutes=15)
    assert sc["ALL"]["m"] == (5.0, 5.0, 2)
    assert sc["quiet <30"]["m"][2] == 1
    assert sc["busy >=60"]["m"][2] == 1
    # masked-out entries are excluded
    sc2 = metrics.evaluate({"m": pred}, actual, np.zeros_like(actual))
    assert sc2["ALL"]["m"][2] == 0


# ---------------------------------------------------------------- driver

def _make_tensor(days=8, freq="6h", rides=("a", "b", "c")):
    """Synthetic park: `rides` over `days` days on a coarse grid (small spd) so the
    rolling-origin/anchor logic is exercised without 96 slots/day."""
    start = pd.Timestamp("2026-06-01 00:00")
    spd = int(round(86400 / pd.Timedelta(freq).total_seconds()))
    n = days * spd
    grid = pd.date_range(start, periods=n, freq=freq)
    rows = []
    for ri, uid in enumerate(rides):
        for i, ds in enumerate(grid):
            # deterministic wait with a per-ride level + daily ramp; small gaps.
            if i % 7 == 3 and uid == "a":
                continue  # a sensor gap
            y = 10.0 + 5 * ri + (i % spd) * 3
            rows.append({"unique_id": uid, "ds": ds, "y": float(y),
                         "n_obs": 5, "down_count": 0})
    panel = pd.DataFrame(rows)
    return tensor.build(panel, park_id="P", freq=freq, min_rides_open=1)


def test_grid_cadence():
    t = _make_tensor(freq="6h")
    slot_minutes, spd = backtest.grid_cadence(t)
    assert slot_minutes == 360 and spd == 4


def test_rolling_origin_split_no_leakage():
    t = _make_tensor(days=8, freq="6h")
    train, ev = backtest.rolling_origin_split(t, L=4, H=2, eval_days=2, base_hour=12)
    assert ev.size >= 1
    # every training base is strictly before the earliest eval base (leakage-free)
    assert train.max() < ev.min()


def test_run_backtest_persistence_identity():
    t = _make_tensor(days=10, freq="6h")
    res = backtest.run_backtest(
        t, backtest.PersistenceModel(), L=4, H=2, eval_days=2, base_hour=12
    )
    sc = res["scores"]
    # the PersistenceModel candidate must reproduce the persistence baseline exactly
    assert sc["ALL"]["persist_model"] == sc["ALL"]["persist"]
    assert res["n_eval_bases"] >= 1
    assert "persist" in sc["ALL"] and "yest" in sc["ALL"]


def test_pool_scores_is_n_weighted():
    import run_bakeoff
    parkA = {"ALL": {"m": (10.0, -2.0, 100), "persist": (12.0, -3.0, 100)}}
    parkB = {"ALL": {"m": (20.0, +4.0, 300), "persist": (18.0, +1.0, 300)}}
    pooled = run_bakeoff.pool_scores([parkA, parkB])
    mae, bias, n = pooled["ALL"]["m"]
    assert n == 400
    # n-weighted: (10*100 + 20*300)/400 = 17.5 ; bias (-2*100 + 4*300)/400 = 2.5
    assert mae == pytest.approx(17.5)
    assert bias == pytest.approx(2.5)


def test_format_table_handles_missing_columns():
    # pooled scores can drop a (segment, model) cell → a later segment may carry a model
    # the first lacks; format_table must not KeyError.
    scores = {
        "ALL": {"a": (1.0, 0.0, 5), "b": (2.0, 0.0, 5)},
        "busy >=60": {"a": (3.0, 0.0, 2)},  # 'b' absent here
    }
    out = metrics.format_table(scores)
    assert "a" in out and "b" in out
    assert "—" in out  # placeholder for the missing cell


def test_pool_scores_skips_empty_and_nan():
    import run_bakeoff
    parks = [
        {"busy >=60": {"m": (float("nan"), float("nan"), 0)}},
        {"busy >=60": {"m": (15.0, -5.0, 50)}},
    ]
    pooled = run_bakeoff.pool_scores(parks)
    assert pooled["busy >=60"]["m"] == (15.0, -5.0, 50)


def test_npz_roundtrip_then_backtest(tmp_path):
    t = _make_tensor(days=10, freq="6h")
    p = tmp_path / "crt.npz"
    t.to_npz(str(p))
    t2 = tensor.CrossRideTensor.from_npz(str(p))
    res = backtest.run_backtest(
        t2, backtest.PersistenceModel(), L=4, H=2, eval_days=2, base_hour=12
    )
    assert res["n_eval_bases"] >= 1


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
