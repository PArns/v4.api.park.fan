"""Smoke tests for the GP-STGNN model. Skipped where torch is absent (CI/CPU dev);
runs on CPU here and on CUDA on the RTX 5080 host (the model auto-selects the device).

    cd pcn-service && python3 -m pytest test_gp_stgnn.py -q
"""

import numpy as np
import pandas as pd
import pytest

torch = pytest.importorskip("torch")

import backtest  # noqa: E402
import gp_stgnn  # noqa: E402
import tensor  # noqa: E402


def _tensor(days=12, freq="6h", rides=("a", "b", "c", "d")):
    grid = pd.date_range("2026-06-01", periods=days * 4, freq=freq)
    rows = []
    for ri, uid in enumerate(rides):
        for i, ds in enumerate(grid):
            rows.append({"unique_id": uid, "ds": ds,
                         "y": 10.0 + 6 * ri + (i % 4) * 8, "n_obs": 5, "down_count": 0})
    return tensor.build(pd.DataFrame(rows), park_id="P", freq=freq, min_rides_open=1)


@pytest.mark.parametrize("loss", ["quantile", "tweedie"])
def test_fit_predict_shapes(loss):
    t = _tensor()
    m = gp_stgnn.GPSTGNNModel(
        loss=loss, hidden=8, embed_dim=4, max_steps=3, batch_size=2
    )
    train, ev = backtest.rolling_origin_split(t, L=4, H=2, eval_days=2, base_hour=12)
    m.fit(t, train, L=4, H=2)
    pred = m.predict(t, ev, L=4, H=2)
    assert pred.shape == (ev.size, len(t.ride_ids), 2)
    assert np.isfinite(pred).all()
    assert (pred >= 0).all() or loss == "quantile"  # tweedie mean is non-negative


def test_runs_through_backtest_harness():
    t = _tensor()
    m = gp_stgnn.GPSTGNNModel(loss="quantile", hidden=8, embed_dim=4,
                              max_steps=3, batch_size=2)
    res = backtest.run_backtest(t, m, L=4, H=2, eval_days=2, base_hour=12)
    assert m.name in res["scores"]["ALL"]
    assert "persist" in res["scores"]["ALL"]


def test_adaptive_adjacency_is_row_stochastic():
    # softmax(ReLU(E·Eᵀ)) rows sum to 1 — the learned park-crowd coupling.
    g = gp_stgnn.GPSTGNN(n_nodes=5, dim_in=3, hidden=4, embed_dim=4,
                         horizon=2, head_out=3)
    A = torch.softmax(torch.relu(g.node_emb @ g.node_emb.T), dim=1)
    assert torch.allclose(A.sum(1), torch.ones(5), atol=1e-5)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
