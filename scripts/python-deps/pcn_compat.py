"""PCN checkpoint + tensor cache written by one venv, read by another. Run from pcn-service/.
usage: python pcn_compat.py write DIR | read DIR"""
import sys
from pathlib import Path
import numpy as np, pandas as pd, torch
sys.path.insert(0, ".")
import backtest, gp_stgnn, tensor

torch.manual_seed(0); np.random.seed(0)
mode, out = sys.argv[1], Path(sys.argv[2])
if mode == "write":
    out.mkdir(parents=True, exist_ok=True)
    grid = pd.date_range("2026-06-01", periods=48, freq="6h")
    rows = [{"unique_id": u, "ds": ds, "y": 10.0 + 6 * ri + (i % 4) * 8, "n_obs": 5, "down_count": 0}
            for ri, u in enumerate("abcd") for i, ds in enumerate(grid)]
    t = tensor.build(pd.DataFrame(rows), park_id="P", freq="6h", min_rides_open=1)
    t.to_npz(str(out / "t.npz"))
    train, ev = backtest.rolling_origin_split(t, L=4, H=2, eval_days=2, base_hour=12)
    m = gp_stgnn.GPSTGNNModel(loss="quantile", hidden=8, embed_dim=4, max_steps=3, batch_size=2)
    m.fit(t, train, L=4, H=2)
    m.save(str(out / "m.pt"), ride_ids=t.ride_ids)
    np.save(out / "ev.npy", ev); np.save(out / "pred.npy", m.predict(t, ev, 4, 2))
    print("wrote with numpy", np.__version__, "torch", torch.__version__)
else:
    t = tensor.CrossRideTensor.from_npz(str(out / "t.npz"))
    m = gp_stgnn.GPSTGNNModel().load(str(out / "m.pt"))
    got = m.predict(t, np.load(out / "ev.npy"), 4, 2)
    ref = np.load(out / "pred.npy")
    print("read with numpy", np.__version__, "torch", torch.__version__,
          "max_abs_diff", float(np.abs(got - ref).max()), "ride_ids", m.ride_ids == t.ride_ids)
